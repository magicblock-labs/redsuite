use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::{
    check, check_eq, check_ne, prep, BaseCtx, ChainCtx, PrivateErScenario,
    Result,
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
impl PrivateErScenario for TableManiaScenario {
    fn name(&self) -> &str {
        "redshift/table_mania"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let authority = prep::funded_payer(base, AIRDROP_LAMPORTS).await?;
        let first = create_table(base, &authority).await?;
        let created = read_table(base, &first).await?;
        check_eq!(
            created.authority,
            Some(authority.pubkey()),
            "new table {first} authority"
        )?;
        check_eq!(
            created.deactivation_slot,
            NOT_DEACTIVATED,
            "new table {first} is active"
        )?;
        check!(created.addresses.is_empty(), "new table {first} is empty")?;

        let keys = unique_pubkeys(TOTAL_PUBKEYS);
        let mut start = 0;
        for end in [10, 60, LOOKUP_TABLE_MAX_ADDRESSES] {
            extend_table_in_chunks(base, &authority, first, &keys[start..end])
                .await?;
            let table = read_table(base, &first).await?;
            check_eq!(
                sorted(&table.addresses),
                sorted(&keys[..end]),
                "table {first}: exactly {end} staged addresses"
            )?;
            check_eq!(
                table.deactivation_slot,
                NOT_DEACTIVATED,
                "extending table {first} leaves it active"
            )?;
            start = end;
        }
        let overflow = extend_lookup_table(
            first,
            authority.pubkey(),
            Some(authority.pubkey()),
            unique_pubkeys(1),
        );
        check!(
            base.submit_and_confirm(&authority, &[overflow])
                .await
                .is_err(),
            "table {first} rejects an address past capacity"
        )?;

        let second = create_table(base, &authority).await?;
        check_ne!(second, first, "overflow uses a distinct second table")?;
        extend_table_in_chunks(
            base,
            &authority,
            second,
            &keys[LOOKUP_TABLE_MAX_ADDRESSES..],
        )
        .await?;
        let spilled = read_table(base, &second).await?;
        check_eq!(
            sorted(&spilled.addresses),
            sorted(&keys[LOOKUP_TABLE_MAX_ADDRESSES..]),
            "table {second} contains exactly the spilled addresses"
        )?;
        check_eq!(
            LOOKUP_TABLE_MAX_ADDRESSES + spilled.addresses.len(),
            TOTAL_PUBKEYS,
            "the two tables hold all addresses"
        )?;

        // Empty-table deactivation is a separate contract from filled tables.
        let empty = create_table(base, &authority).await?;
        let before = read_table(base, &empty).await?;
        check!(
            before.addresses.is_empty(),
            "deactivation starts with an empty table {empty}"
        )?;
        check_eq!(
            before.deactivation_slot,
            NOT_DEACTIVATED,
            "table {empty} is active before deactivation"
        )?;
        base.submit_and_confirm(
            &authority,
            &[deactivate_lookup_table(empty, authority.pubkey())],
        )
        .await?;
        let after = read_table(base, &empty).await?;
        check_ne!(
            after.deactivation_slot,
            NOT_DEACTIVATED,
            "table {empty} records its deactivation slot"
        )?;
        check_eq!(
            after.authority,
            Some(authority.pubkey()),
            "deactivation preserves table {empty}'s authority"
        )?;
        Ok(())
    }
}

async fn create_table(
    base: &BaseCtx,
    authority: &keypair::Keypair,
) -> Result<Pubkey> {
    let (instruction, table) = create_lookup_table(
        authority.pubkey(),
        authority.pubkey(),
        base.api().get_slot().await?,
    );
    base.submit_and_confirm(authority, &[instruction]).await?;
    Ok(table)
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
        base.submit_and_confirm(authority, &[ix]).await?;
    }
    Ok(())
}
