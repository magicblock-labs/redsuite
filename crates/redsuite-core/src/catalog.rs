use std::{path::PathBuf, result};

use anyhow::anyhow;
use clap::ValueEnum;
use futures_util::future::LocalBoxFuture;
use magicblock_config::LeaderParams;
use pubkey::Pubkey;

use crate::{
    context::ErCtx,
    profile::ExecutionConfig,
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
    pub proxied: bool,
}

pub static DEFAULT: ErGroup = ErGroup {
    label: "default",
    settings: |_| {},
    proxied: false,
};

impl ErGroup {
    pub fn options(&self) -> ErOptions {
        let settings = self.settings;
        ErOptions {
            label: self.label.to_owned(),
            settings: Box::new(settings),
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fixture {
    RedlineProgram,
    RedshiftProgram,
    RedhatProgram,
    UpgradeProbe,
    UpgradeProbeUpgraded,
}

impl Fixture {
    pub fn resolve(self) -> Result<PathBuf> {
        let path = crate::topology::workspace_root().join("target/deploy").join(self.so_name());
        if !path.is_file() {
            return Err(anyhow!(
                "fixture {} is missing; run `cargo xtask programs`",
                path.display()
            ));
        }
        Ok(path)
    }

    pub const ALL: [Fixture; 5] = [
        Fixture::RedlineProgram,
        Fixture::RedshiftProgram,
        Fixture::RedhatProgram,
        Fixture::UpgradeProbe,
        Fixture::UpgradeProbeUpgraded,
    ];

    pub const fn so_name(self) -> &'static str {
        match self {
            Fixture::RedlineProgram => "redline_program.so",
            Fixture::RedshiftProgram => "redshift_program.so",
            Fixture::RedhatProgram => "redhat_program.so",
            Fixture::UpgradeProbe => "upgrade_probe.so",
            Fixture::UpgradeProbeUpgraded => "upgrade_probe_upgraded.so",
        }
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
    fixtures: &'static [Fixture],
    run: ScenarioRunner,
}

/// Registers a scenario using its snake-cased type name. Use `named` for aliases.
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
