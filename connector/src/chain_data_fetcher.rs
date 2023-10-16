//! Client utility to interact with Solana chain via RPC.
//!
//! The retrieved data will be deserialized using anchor types.
//! No dependency to Mango Types are allowed; use `mango_chain_data_fetcher.rs` instead.


use anchor_lang::prelude::*;
use std::cell::{Ref, RefMut};
use std::sync::{Arc, RwLock};
use std::{mem, thread};
use std::time::{Duration, Instant};
use anchor_lang::{Owner, ZeroCopy};
use anchor_lang::__private::bytemuck;
use anchor_lang::error::ErrorCode;


use anyhow::Context;
use arrayref::array_ref;

use solana_client::nonblocking::rpc_client::RpcClient as RpcClientAsync;
use solana_sdk::account::{AccountSharedData, ReadableAccount};
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
// use mango_v4::accounts_zerocopy::LoadZeroCopy;
use crate::account_fetcher_trait::{AccountFetcher, AccountFetcherSync};
use crate::chain_data::{AccountData, ChainData, SlotData, SlotStatus};

/// A complex account fetcher that mostly depends on an external job keeping
/// the chain_data up to date.
///
/// In addition to the usual async fetching interface, it also has synchronous
/// functions to access some kinds of data with less overhead.
///
/// Also, there's functions for fetching up to date data via rpc.
/// was renamed from "struct AccountFetcher" to avoid conflict with the trait
pub struct ChainDataFetcher {
    pub chain_data: Arc<RwLock<ChainData>>,
    pub rpc: RpcClientAsync,
}

#[async_trait::async_trait]
impl AccountFetcher for ChainDataFetcher {

    async fn fetch_raw_account(
        &self,
        address: &Pubkey,
    ) -> anyhow::Result<solana_sdk::account::AccountSharedData> {
        let result = self.fetch_raw_account_sync(address);
        return result;
    }

    async fn fetch_raw_account_lookup_table(
        &self,
        address: &Pubkey,
    ) -> anyhow::Result<AccountSharedData> {
        // Fetch data via RPC if missing: the chain data updater doesn't know about all the
        // lookup talbes we may need.
        if let Ok(alt) = self.fetch_raw_account_sync(address) {
            return Ok(alt);
        }
        self.refresh_account_via_rpc(address).await?;
        self.fetch_raw_account_sync(address)
    }

    async fn fetch_program_accounts(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>> {
        self.fetch_program_accounts_sync(program, discriminator)
    }

}

impl AccountFetcherSync for ChainDataFetcher {
    // renamed from fetch_raw
    fn fetch_raw_account_sync(&self, address: &Pubkey) -> anyhow::Result<AccountSharedData> {
        let chain_data = self.chain_data.read().unwrap();
        Ok(chain_data
            .account(address)
            .map(|d| d.account.clone())
            .with_context(|| format!("fetch account {} via chain_data ({} elements)", address, chain_data.accounts_count()))?)
    }


    // TODO remove this duplication
    fn fetch_program_accounts_sync(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>> {
        let chain_data = self.chain_data.read().unwrap();
        Ok(chain_data
            .iter_accounts()
            .filter_map(|(pk, data)| {
                if data.account.owner() != program {
                    return None;
                }
                let acc_data = data.account.data();
                if acc_data.len() < 8 || acc_data[..8] != discriminator {
                    return None;
                }
                Some((*pk, data.account.clone()))
            })
            .collect::<Vec<_>>())
    }
}


impl ChainDataFetcher {

    // note: Generic methods cannot be used in a trait because it is not "object safe"
    // note: cannot be in connector because .load depends on ZeroCopy
    // pub fn fetch<T: anchor_lang::ZeroCopy + anchor_lang::Owner>(
    //     &self,
    //     address: &Pubkey,
    // ) -> anyhow::Result<T> {
    //     Ok(*self
    //         .fetch_raw_account_sync(address)?
    //         .load::<T>()
    //         .with_context(|| format!("loading account {}", address))?)
    // }

    // fetches via RPC, stores in ChainData, returns new version
    // pub async fn fetch_fresh<T: anchor_lang::ZeroCopy + anchor_lang::Owner>(
    //     &self,
    //     address: &Pubkey,
    // ) -> anyhow::Result<T> {
    //     self.refresh_account_via_rpc(address).await?;
    //     self.fetch(address)
    // }

    pub async fn refresh_account_via_rpc(&self, address: &Pubkey) -> anyhow::Result<Slot> {
        let response = self
            .rpc
            .get_account_with_commitment(address, self.rpc.commitment())
            .await
            .with_context(|| format!("refresh account {} via rpc", address))?;
        let slot = response.context.slot;
        let account = response
            .value
            .ok_or(anchor_client::ClientError::AccountNotFound)
            .with_context(|| format!("refresh account {} via rpc", address))?;

        let mut chain_data = self.chain_data.write().unwrap();
        let best_chain_slot = chain_data.best_chain_slot();

        // The RPC can get information for slots that haven't been seen yet on chaindata. That means
        // that the rpc thinks that slot is valid. Make it so by telling chain data about it.
        if best_chain_slot < slot {
            chain_data.update_slot(SlotData {
                slot,
                parent: Some(best_chain_slot),
                status: SlotStatus::Processed,
                chain: 0,
            });
        }

        chain_data.update_account(
            *address,
            AccountData {
                slot,
                account: account.into(),
                write_version: 1,
            },
        );

        Ok(slot)
    }

    // /// Return the maximum slot reported for the processing of the signatures
    // pub async fn transaction_max_slot(&self, signatures: &[Signature]) -> anyhow::Result<Slot> {
    //     let statuses = self.rpc.get_signature_statuses(signatures).await?.value;
    //     Ok(statuses
    //         .iter()
    //         .map(|status_opt| status_opt.as_ref().map(|status| status.slot).unwrap_or(0))
    //         .max()
    //         .unwrap_or(0))
    // }

    /// Return success once all addresses have data >= min_slot
    pub async fn refresh_accounts_via_rpc_until_slot(
        &self,
        addresses: &[Pubkey],
        min_slot: Slot,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        for address in addresses {
            loop {
                if start.elapsed() > timeout {
                    anyhow::bail!(
                        "timeout while waiting for data for {} that's newer than slot {}",
                        address,
                        min_slot
                    );
                }
                let data_slot = self.refresh_account_via_rpc(address).await?;
                if data_slot >= min_slot {
                    break;
                }
                thread::sleep(Duration::from_millis(500));
            }
        }
        Ok(())
    }
}
