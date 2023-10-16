use std::cell::RefCell;
use std::sync::Arc;
use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;

#[async_trait::async_trait]
pub trait AccountFetcher: Sync + Send {

    async fn fetch_raw_account(
        &self,
       address: &Pubkey,
    ) -> anyhow::Result<AccountSharedData>;

    async fn fetch_raw_account_lookup_table(
        &self,
        address: &Pubkey,
    ) -> anyhow::Result<AccountSharedData> {
        self.fetch_raw_account(address).await
    }

    async fn fetch_program_accounts(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>>;

}


pub trait AccountFetcherSync: Sync + Send {

    fn fetch_raw_account_sync(
        &self,
        address: &Pubkey,
    ) -> anyhow::Result<AccountSharedData>;

    fn fetch_program_accounts_sync(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>>;
}

struct WrappedAccountFetcher<'a> {
    pub sync: &'a dyn AccountFetcherSync
}

#[async_trait::async_trait]
impl<'a> AccountFetcher for WrappedAccountFetcher<'a> {

    async fn fetch_raw_account(
        &self,
        address: &Pubkey,
    ) -> anyhow::Result<AccountSharedData> {
        self.sync.fetch_raw_account_sync(address)
    }

    async fn fetch_program_accounts(
        &self,
        program: &Pubkey,
        discriminator: [u8; 8],
    ) -> anyhow::Result<Vec<(Pubkey, AccountSharedData)>> {
        self.sync.fetch_program_accounts_sync(program, discriminator)
    }


}

pub fn wrap_account_fetcher_async<'a>(account_fetcher_sync: &'a dyn AccountFetcherSync) -> Box<dyn AccountFetcher + 'a> {
    // wrap to async
    Box::new(WrappedAccountFetcher {
        sync: account_fetcher_sync,
    })
}
