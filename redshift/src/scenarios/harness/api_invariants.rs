use async_trait::async_trait;
use keypair::Keypair;
use redsuite_core::{
    check, check_eq, mdp, BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use signer::Signer;

const DELEGATED_LAMPORTS: u64 = 1_000_000_000;

pub struct ApiInvariants;

#[async_trait(?Send)]
impl PrivateErScenario for ApiInvariants {
    fn name(&self) -> &str {
        "redshift/api_invariants"
    }

    async fn run(&self, base: &BaseCtx) -> Result<()> {
        let validator = Keypair::new();
        base.airdrop(&validator.pubkey(), DELEGATED_LAMPORTS)
            .await?;
        let record = mdp::DomainRecord {
            identity: validator.pubkey(),
            status: mdp::STATUS_ACTIVE,
            block_time_ms: 101,
            base_fee: 102,
            features: [0u8; 32],
            load_average: 222,
            country_code: *b"BOL",
            addr: "1.1.1.0:1010".to_owned(),
        };
        let record_pda = mdp::record_pda(&validator.pubkey());

        base.submit_and_confirm(&validator, &[mdp::register(&record)])
            .await?;
        let registered = base
            .account(&record_pda)
            .await?
            .ok_or("domain record missing after register")?;
        check_eq!(
            registered.owner,
            mdp::mdp_id(),
            "domain record not owned by mdp"
        )?;
        check_eq!(
            registered.data,
            record.encode(),
            "registered record bytes diverge from the submitted record"
        )?;

        let mut mutated = record.clone();
        mutated.status = mdp::STATUS_DRAINING;
        mutated.base_fee = 0;
        mutated.addr = "this.is.very.long.string.to.test.sync".to_owned();
        base.submit_and_confirm(&validator, &[mdp::sync(&mutated)])
            .await?;
        let synced = base
            .account(&record_pda)
            .await?
            .ok_or("domain record missing after sync")?;
        check_eq!(
            synced.data,
            mutated.encode(),
            "synced record bytes diverge from the mutated record"
        )?;
        check!(
            synced.data.len() > registered.data.len(),
            "sync with a longer addr should have grown the record account"
        )?;

        base.submit_and_confirm(
            &validator,
            &[mdp::unregister(&validator.pubkey())],
        )
        .await?;
        let unregistered = base.account(&record_pda).await?;
        check!(
            unregistered.is_none(),
            "domain record still present after unregister"
        )?;

        Ok(())
    }
}
