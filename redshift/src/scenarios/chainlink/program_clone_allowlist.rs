use std::{rc::Rc, time::Duration};

use anyhow::ensure;
use magicblock_config::config::AllowedProgram;
use redsuite_core::{
    topology,
    topology::{ErOptions, PrivateEr},
    BaseCtx, ChainCtx, PrivateErScenario, Result,
};
use tokio::time;

const BLOCKED_SETTLE: Duration = Duration::from_secs(2);

/// Checks that an allowlisted program materializes while an existing
/// unlisted executable stays absent.
/// Uses a private ER so the clone policy is exercised from startup, not after cache hydration.
pub struct ProgramCloneAllowlist {
    private: PrivateEr,
}

impl PrivateErScenario for ProgramCloneAllowlist {
    /// Proves the blocked executable exists on base before starting the restricted ER.
    async fn setup(base: Rc<BaseCtx>, _: ()) -> Result<Self> {
        ensure!(base
            .account(&topology::COMMITTOR_ID)
            .await?
            .is_some_and(|program| program.executable));
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

    /// Requires allowed cloning and sustained absence of the blocked program
    /// after a settling window.
    async fn run(self) -> Result<()> {
        let er = self.private.ctx();
        er.await_program_clone(&redshift_interface::ID).await?;
        ensure!(er.account(&topology::COMMITTOR_ID).await?.is_none());
        time::sleep(BLOCKED_SETTLE).await;
        ensure!(er.account(&topology::COMMITTOR_ID).await?.is_none());
        self.private.finish().await
    }
}
