use std::{rc::Rc, time::Duration};

use magicblock_config::config::AllowedProgram;
use redsuite_core::{
    check, topology,
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
        check!(
            base.account(&topology::COMMITTOR_ID)
                .await?
                .is_some_and(|program| program.executable),
            "the unlisted program must exist on L1 before testing cloning policy"
        )?;
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
        check!(
            er.account(&topology::COMMITTOR_ID).await?.is_none(),
            "the unlisted program must be absent before the settle window"
        )?;
        time::sleep(BLOCKED_SETTLE).await;
        check!(
            er.account(&topology::COMMITTOR_ID).await?.is_none(),
            "the restricted ER must not clone the unlisted program"
        )?;
        self.private.finish().await
    }
}
