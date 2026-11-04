use std::{env, path::PathBuf, result};

use anyhow::ensure;

use clap::ValueEnum;
use futures_util::future::LocalBoxFuture;
use magicblock_config::LeaderParams;
use pubkey::Pubkey;

use crate::{
    config::ExecutionConfig,
    context::{BaseCtx, ErCtx},
    scenario::{
        run_private_er_scenario, run_shared_scenario, PrivateErScenario, RunRecord, Scenario,
        Verdict,
    },
    topology::ErOptions,
    Result,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum Family {
    Redline,
    Redshift,
    Redhat,
}

impl Family {
    pub const fn prefix(self) -> &'static str {
        match self {
            Family::Redline => "redline",
            Family::Redshift => "redshift",
            Family::Redhat => "redhat",
        }
    }
}

#[derive(Debug)]
pub struct ErGroup {
    pub label: &'static str,
    pub settings: fn(&mut LeaderParams),
    pub proxy: bool,
    /// Identities admitted at boot; followers are launched only by replication coverage.
    pub followers: usize,
}

pub static DEFAULT: ErGroup = ErGroup {
    label: "default",
    settings: |_| {},
    proxy: false,
    followers: 0,
};

impl ErGroup {
    pub fn allowed_followers(&self) -> Result<Vec<Pubkey>> {
        use signer::Signer;
        (0..self.followers)
            .map(|index| {
                crate::topology::identity_for_label(&format!("{}-verifier{index}", self.label))
                    .map(|key| key.pubkey())
            })
            .collect()
    }

    pub fn options(&self) -> ErOptions {
        ErOptions {
            label: self.label.to_owned(),
            settings: Box::new(self.settings),
            ..ErOptions::default()
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Er {
    Shared(&'static ErGroup),
    Exclusive(&'static ErGroup),
    Fresh,
}

impl Er {
    pub fn group(self) -> Option<&'static ErGroup> {
        match self {
            Er::Shared(group) | Er::Exclusive(group) => Some(group),
            Er::Fresh => None,
        }
    }
}

pub use crate::fixtures::Fixture;

pub const FIXTURES_DIR_ENV: &str = "REDSUITE_FIXTURES_DIR";

impl Fixture {
    pub fn resolve(self) -> Result<PathBuf> {
        let dir = env::var_os(FIXTURES_DIR_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| crate::topology::workspace_root().join("target/deploy"));
        let path = dir.join(self.so_name());
        ensure!(
            path.is_file(),
            "fixture {} is missing; run `cargo xtask programs` and select target/deploy \
             with {FIXTURES_DIR_ENV}, or use the matching release fixtures",
            path.display()
        );
        Ok(path)
    }

    /// Fixed address for base-startup fixtures; dynamically deployed probes have none.
    pub const fn program_id(self) -> Option<Pubkey> {
        match self {
            Fixture::RedlineProgram => Some(redline_interface::ID),
            Fixture::RedshiftProgram => Some(redshift_interface::ID),
            Fixture::RedhatProgram => Some(redhat_interface::ID),
            Fixture::UpgradeProbe | Fixture::UpgradeProbeUpgraded => None,
        }
    }
}

pub type ScenarioFuture = LocalBoxFuture<'static, RunRecord>;

/// Register the same static preparation to run it once per suite.
pub struct BasePreparation {
    pub run: for<'a> fn(&'a BaseCtx) -> LocalBoxFuture<'a, Result<()>>,
}
type ScenarioRunner = Box<
    dyn FnOnce(
        String,
        &'static [Fixture],
        ExecutionConfig,
        Option<result::Result<ErCtx, String>>,
    ) -> ScenarioFuture,
>;

/// Registration is side-effect-free; setup only runs when the entry is consumed.
pub struct ScenarioEntry {
    pub short_name: &'static str,
    pub er: Er,
    pub explicit_only: bool,
    pub order: u16,
    /// Base-wide setup, deduplicated across selected entries before ER groups start.
    pub prepare: Option<&'static BasePreparation>,
    fixtures: &'static [Fixture],
    run: ScenarioRunner,
}

/// Registers a scenario using its snake-cased type name. Use `named` to override it.
#[macro_export]
macro_rules! scenario {
    ($scenario:path, $kind:ident($($args:expr),* $(,)?)) => {{
        const PARTS: &[&str] = &$crate::__const_str::split!(stringify!($scenario), "::");
        const NAME: &str = $crate::__const_str::convert_ascii_case!(
            snake,
            $crate::__const_str::trim_ascii!(PARTS[PARTS.len() - 1])
        );
        <$scenario>::$kind(NAME, $($args),*)
    }};
}

impl ScenarioEntry {
    pub fn explicit_only(mut self) -> Self {
        self.explicit_only = true;
        self
    }

    pub fn ordered(mut self, order: u16) -> Self {
        self.order = order;
        self
    }

    pub fn prepare_with(mut self, prepare: &'static BasePreparation) -> Self {
        self.prepare = Some(prepare);
        self
    }

    pub fn named(mut self, name: &'static str) -> Self {
        self.short_name = name;
        self
    }

    pub fn name(&self, family: Family) -> String {
        format!("{}/{}", family.prefix(), self.short_name)
    }

    pub fn run(
        self,
        family: Family,
        config: ExecutionConfig,
        er: Option<result::Result<ErCtx, String>>,
    ) -> ScenarioFuture {
        let name = self.name(family);
        (self.run)(name, self.fixtures, config, er)
    }

    pub(crate) fn shared<S, V, Options>(
        short_name: &'static str,
        er: Er,
        fixtures: &'static [Fixture],
        options: Options,
    ) -> Self
    where
        S: Scenario<V, Options>,
        V: Verdict + 'static,
        Options: 'static,
    {
        Self {
            short_name,
            er,
            explicit_only: false,
            order: 0,
            prepare: None,
            fixtures,
            run: Box::new(move |name, fixtures, config, er| {
                let er = er.unwrap_or_else(|| Err("shared scenario requires an ER group".into()));
                Box::pin(run_shared_scenario(
                    name,
                    fixtures,
                    config,
                    er,
                    async move |base, er, stage| {
                        let scenario = S::setup(base, er, options).await?;
                        *stage = "scenario";
                        scenario.run().await
                    },
                ))
            }),
        }
    }

    pub(crate) fn private<S, V, Options>(
        short_name: &'static str,
        fixtures: &'static [Fixture],
        options: Options,
    ) -> Self
    where
        S: PrivateErScenario<V, Options>,
        V: Verdict + 'static,
        Options: 'static,
    {
        Self {
            short_name,
            er: Er::Fresh,
            explicit_only: false,
            order: 0,
            prepare: None,
            fixtures,
            run: Box::new(move |name, fixtures, config, _er| {
                Box::pin(run_private_er_scenario(
                    name,
                    fixtures,
                    config,
                    async move |base, stage| {
                        let scenario = S::setup(base, options).await?;
                        *stage = "scenario";
                        scenario.run().await
                    },
                ))
            }),
        }
    }
}
