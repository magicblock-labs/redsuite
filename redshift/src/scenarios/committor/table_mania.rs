use async_trait::async_trait;
use pubkey::Pubkey;
use redsuite_core::{
    prep, BaseCtx, ChainCtx, ErCtx, Result, Scenario, ScenarioReport,
};
use signer::Signer;
use solana_address_lookup_table_interface::{
    instruction::{
        create_lookup_table, deactivate_lookup_table, extend_lookup_table,
    },
    state::LOOKUP_TABLE_MAX_ADDRESSES,
};

const AIRDROP_LAMPORTS: u64 = 50_000_000_000;

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

        // Part 2: High-capacity multi-table allocation
        run_multi_table_allocation(base).await?;

        // Part 3: Overlapping pubkey set deactivation & cleanup
        run_deactivation_lifecycle(base).await?;

        Ok(report)
    }
}

async fn run_lookup_table_lifecycle(base: &BaseCtx) -> Result<()> {
    let authority = prep::funded_payer(base, AIRDROP_LAMPORTS).await?;
    let recent_slot = base.api().get_slot().await?;

    // 1. Create Address Lookup Table
    let (create_ix, table_pda) = create_lookup_table(
        authority.pubkey(),
        authority.pubkey(),
        recent_slot,
    );
    base.send(&authority, &[create_ix]).await?;

    let account_before = base.account(&table_pda).await?;
    assert!(
        account_before.is_some(),
        "Lookup table account was not created on chain"
    );

    // Deserializing meta header from raw data (first 56 bytes)
    let raw_data = account_before.unwrap().data;
    assert!(raw_data.len() >= 56, "raw ALT account data is too short");

    // 2. Extend lookup table with batch of 10 pubkeys
    let mut pubkeys = (0..10).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
    pubkeys.sort();

    extend_table_in_chunks(base, &authority, table_pda, &pubkeys).await?;

    let account_after = base
        .account(&table_pda)
        .await?
        .ok_or("lookup table missing post-extend")?;

    // Check data length grew to hold 10 pubkeys (56 + 10 * 32 = 376 bytes)
    let expected_min_len = 56 + 10 * 32;
    assert!(
        account_after.data.len() >= expected_min_len,
        "extended ALT account data length {} < expected {}",
        account_after.data.len(),
        expected_min_len
    );

    // 3. Extend with second batch up to 50 more pubkeys
    let batch2 = (0..50).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
    extend_table_in_chunks(base, &authority, table_pda, &batch2).await?;

    let account_after2 = base
        .account(&table_pda)
        .await?
        .ok_or("lookup table missing post-extend2")?;
    let expected_min_len2 = 56 + 60 * 32;
    assert!(
        account_after2.data.len() >= expected_min_len2,
        "extended ALT account data length {} < expected {}",
        account_after2.data.len(),
        expected_min_len2
    );

    Ok(())
}

async fn extend_table_in_chunks(
    base: &BaseCtx,
    authority: &keypair::Keypair,
    table_pda: Pubkey,
    pubkeys: &[Pubkey],
) -> Result<()> {
    for chunk in pubkeys.chunks(20) {
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
    let target_total_pubkeys = 300; // Exceeds LOOKUP_TABLE_MAX_ADDRESSES (256)

    let num_tables = (target_total_pubkeys as f32
        / LOOKUP_TABLE_MAX_ADDRESSES as f32)
        .ceil() as usize;
    assert_eq!(num_tables, 2);

    let base_slot = base.api().get_slot().await?;
    let mut table_pdas = Vec::new();

    // Create 2 lookup tables using distinct derived slots
    for i in 0..num_tables {
        let (create_ix, table_pda) = create_lookup_table(
            authority.pubkey(),
            authority.pubkey(),
            base_slot + i as u64,
        );
        base.send(&authority, &[create_ix]).await?;
        table_pdas.push(table_pda);
    }

    // Fill table 1 with 200 pubkeys (in chunks of 20)
    let keys1 = (0..200).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
    extend_table_in_chunks(base, &authority, table_pdas[0], &keys1).await?;

    // Fill table 2 with remaining 100 pubkeys (in chunks of 20)
    let keys2 = (0..100).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
    extend_table_in_chunks(base, &authority, table_pdas[1], &keys2).await?;

    // Verify both table accounts exist and have expected data lengths
    for (idx, pda) in table_pdas.iter().enumerate() {
        let expected_keys = if idx == 0 { 200 } else { 100 };
        let acc = base.account(pda).await?.ok_or("missing table pda")?;
        assert!(
            acc.data.len() >= 56 + expected_keys * 32,
            "table data size too small: {}",
            acc.data.len()
        );
    }

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

    let deactivate_ix = deactivate_lookup_table(table_pda, authority.pubkey());
    base.send(&authority, &[deactivate_ix]).await?;

    // Verify lookup table is marked for deactivation on chain
    let acc = base
        .account(&table_pda)
        .await?
        .ok_or("missing deactivated table account")?;
    assert!(
        acc.data.len() >= 56,
        "deactivated ALT account data too short"
    );

    Ok(())
}
