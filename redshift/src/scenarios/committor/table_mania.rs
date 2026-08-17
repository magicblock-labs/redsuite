use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, check_ne, prep, BaseCtx, ChainCtx, ErCtx, Result,
    Scenario, ScenarioReport,
};
use signer::Signer;
use solana_address_lookup_table_interface::{
    instruction::{
        create_lookup_table, deactivate_lookup_table, extend_lookup_table,
    },
    state::{AddressLookupTable, LOOKUP_TABLE_MAX_ADDRESSES},
};

const AIRDROP_LAMPORTS: u64 = 50_000_000_000;
const TOTAL_PUBKEYS: usize = 300;
const EXTEND_CHUNK: usize = 20;
const NOT_DEACTIVATED: u64 = u64::MAX;

pub struct TableManiaScenario;

#[async_trait(?Send)]
impl Scenario for TableManiaScenario {
    fn name(&self) -> &str {
        "redshift/table_mania"
    }

    async fn run(&self, base: &BaseCtx, _er: &ErCtx) -> Result<ScenarioReport> {
        let report = ScenarioReport::ok(self.name());

        // Part 1: Lookup table creation, extension & meta verification
        run_lookup_table_lifecycle(base).await?;

        // Part 2: Address cap enforcement & spill into a second table
        run_multi_table_allocation(base).await?;

        // Part 3: Deactivation marks the table on chain
        run_deactivation_lifecycle(base).await?;

        Ok(report)
    }
}

struct TableState {
    deactivation_slot: u64,
    authority: Option<Pubkey>,
    addresses: Vec<Pubkey>,
}

async fn read_table(base: &BaseCtx, table_pda: &Pubkey) -> Result<TableState> {
    let account = base
        .account(table_pda)
        .await?
        .ok_or("lookup table account is missing on base")?;
    let table =
        AddressLookupTable::deserialize(&account.data).map_err(|err| {
            format!("lookup table account does not decode: {err:?}")
        })?;
    Ok(TableState {
        deactivation_slot: table.meta.deactivation_slot,
        authority: table.meta.authority,
        addresses: table.addresses.to_vec(),
    })
}

fn unique_pubkeys(count: usize) -> Vec<Pubkey> {
    (0..count).map(|_| Pubkey::new_unique()).collect()
}

fn sorted(pubkeys: &[Pubkey]) -> Vec<Pubkey> {
    let mut copy = pubkeys.to_vec();
    copy.sort();
    copy
}

async fn run_lookup_table_lifecycle(base: &BaseCtx) -> Result<()> {
    let authority = prep::funded_payer(base, AIRDROP_LAMPORTS).await?;
    let recent_slot = base.api().get_slot().await?;

    let (create_ix, table_pda) = create_lookup_table(
        authority.pubkey(),
        authority.pubkey(),
        recent_slot,
    );
    base.send(&authority, &[create_ix]).await?;

    let created = read_table(base, &table_pda).await?;
    check_eq!(
        created.authority,
        Some(authority.pubkey()),
        "the new lookup table does not carry the expected authority"
    )?;
    check_eq!(
        created.deactivation_slot,
        NOT_DEACTIVATED,
        "the new lookup table is already deactivated"
    )?;
    check!(
        created.addresses.is_empty(),
        "the new lookup table already holds addresses"
    )?;

    let first_batch = unique_pubkeys(10);
    extend_table_in_chunks(base, &authority, table_pda, &first_batch).await?;

    let after_first = read_table(base, &table_pda).await?;
    check_eq!(
        sorted(&after_first.addresses),
        sorted(&first_batch),
        "the lookup table does not hold exactly the first batch of addresses"
    )?;

    let second_batch = unique_pubkeys(50);
    extend_table_in_chunks(base, &authority, table_pda, &second_batch).await?;

    let mut expected = first_batch;
    expected.extend_from_slice(&second_batch);
    let after_second = read_table(base, &table_pda).await?;
    check_eq!(
        sorted(&after_second.addresses),
        sorted(&expected),
        "the lookup table does not hold exactly both batches of addresses"
    )?;
    check_eq!(
        after_second.deactivation_slot,
        NOT_DEACTIVATED,
        "extending the lookup table deactivated it"
    )?;

    Ok(())
}

async fn extend_table_in_chunks(
    base: &BaseCtx,
    authority: &keypair::Keypair,
    table_pda: Pubkey,
    pubkeys: &[Pubkey],
) -> Result<()> {
    for chunk in pubkeys.chunks(EXTEND_CHUNK) {
        let ix = extend_lookup_table(
            table_pda,
            authority.pubkey(),
            Some(authority.pubkey()),
            chunk.to_vec(),
        );
        base.send(authority, &[ix]).await?;
    }
    Ok(())
}

async fn run_multi_table_allocation(base: &BaseCtx) -> Result<()> {
    let authority = prep::funded_payer(base, AIRDROP_LAMPORTS).await?;

    let first_slot = base.api().get_slot().await?;
    let (first_create_ix, first_table) =
        create_lookup_table(authority.pubkey(), authority.pubkey(), first_slot);
    base.send(&authority, &[first_create_ix]).await?;

    let first_keys = unique_pubkeys(LOOKUP_TABLE_MAX_ADDRESSES);
    extend_table_in_chunks(base, &authority, first_table, &first_keys).await?;

    let filled = read_table(base, &first_table).await?;
    check_eq!(
        sorted(&filled.addresses),
        sorted(&first_keys),
        "the filled lookup table does not hold exactly the addresses that were added"
    )?;
    check_eq!(
        filled.addresses.len(),
        LOOKUP_TABLE_MAX_ADDRESSES,
        "the filled lookup table does not hold the maximum address count"
    )?;

    let overflow_ix = extend_lookup_table(
        first_table,
        authority.pubkey(),
        Some(authority.pubkey()),
        unique_pubkeys(1),
    );
    check!(
        base.send(&authority, &[overflow_ix]).await.is_err(),
        "the lookup table accepted an address past the maximum count"
    )?;

    let second_slot = base.api().get_slot().await?;
    let (second_create_ix, second_table) = create_lookup_table(
        authority.pubkey(),
        authority.pubkey(),
        second_slot,
    );
    base.send(&authority, &[second_create_ix]).await?;

    let second_keys =
        unique_pubkeys(TOTAL_PUBKEYS - LOOKUP_TABLE_MAX_ADDRESSES);
    extend_table_in_chunks(base, &authority, second_table, &second_keys)
        .await?;

    let spilled = read_table(base, &second_table).await?;
    check_eq!(
        sorted(&spilled.addresses),
        sorted(&second_keys),
        "the second lookup table does not hold exactly the spilled addresses"
    )?;
    check_eq!(
        filled.addresses.len() + spilled.addresses.len(),
        TOTAL_PUBKEYS,
        "the two lookup tables do not hold all the addresses"
    )?;

    Ok(())
}

async fn run_deactivation_lifecycle(base: &BaseCtx) -> Result<()> {
    let authority = prep::funded_payer(base, AIRDROP_LAMPORTS).await?;
    let recent_slot = base.api().get_slot().await?;

    let (create_ix, table_pda) = create_lookup_table(
        authority.pubkey(),
        authority.pubkey(),
        recent_slot,
    );
    base.send(&authority, &[create_ix]).await?;

    let before = read_table(base, &table_pda).await?;
    check_eq!(
        before.deactivation_slot,
        NOT_DEACTIVATED,
        "the lookup table is deactivated before the deactivate instruction"
    )?;

    let deactivate_ix = deactivate_lookup_table(table_pda, authority.pubkey());
    base.send(&authority, &[deactivate_ix]).await?;

    let after = read_table(base, &table_pda).await?;
    check_ne!(
        after.deactivation_slot,
        NOT_DEACTIVATED,
        "the lookup table did not record a deactivation slot"
    )?;
    check_eq!(
        after.authority,
        Some(authority.pubkey()),
        "the deactivation changed the lookup table authority"
    )?;

    Ok(())
}
