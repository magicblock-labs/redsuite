use std::{rc::Rc, time::Duration};

use anyhow::ensure;
use magicblock_config::config::AllowedProgram;
use redsuite_core::{
    topology,
    topology::{ErOptions, PrivateEr},
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use tokio::time;

const CLONE_TIMEOUT: Duration = Duration::from_secs(20);
const BLOCKED_SETTLE: Duration = Duration::from_secs(2);

/// An allowlisted program clones while an existing unlisted program stays absent.
pub struct ProgramAllowlist {
    private: PrivateEr,
}

impl PrivateErScenario for ProgramAllowlist {
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        ensure!(
            base.account(&topology::COMMITTOR_ID)
                .await?
                .is_some_and(|program| program.executable),
            "unlisted program exists on base"
        );
        let private = topology::private_er(
            &base,
            ErOptions {
                label: "program-allowlist".to_owned(),
                settings: Box::new(|params| {
                    params.chainlink.allowed_programs =
                        Some(vec![AllowedProgram { id: redshift_interface::ID }]);
                }),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self { private })
    }

    async fn run(self) -> Result<()> {
        let er = self.private.ctx();
        er.await_program_clone(&redshift_interface::ID, CLONE_TIMEOUT).await?;
        ensure!(
            er.account(&topology::COMMITTOR_ID).await?.is_none(),
            "unlisted program initially absent on ER"
        );
        time::sleep(BLOCKED_SETTLE).await;
        ensure!(
            er.account(&topology::COMMITTOR_ID).await?.is_none(),
            "unlisted program remains absent on ER"
        );
        self.private.finish().await
    }
}
