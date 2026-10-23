use std::{future::Future, path::PathBuf, pin::Pin};

use clap::ValueEnum;
use magicblock_config::LeaderParams;
use pubkey::Pubkey;

use crate::{
    context::ErCtx, profile::ExecutionConfig, scenario::RunRecord, topology::ErOptions, Result,
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
    None,
}

impl Er {
    pub fn group(self) -> Option<&'static ErGroup> {
        match self {
            Er::Shared(group) | Er::Exclusive(group) => Some(group),
            Er::Fresh | Er::None => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fixture {
    RedlineProgram,
    RedshiftProgram,
    RedhatProgram,
    RedshiftProgramSlim,
    RedshiftProgramSlimUpgraded,
}

impl Fixture {
    pub fn resolve(self) -> Result<PathBuf> {
        let path = crate::topology::workspace_root().join("target/deploy").join(self.so_name());
        if !path.is_file() {
            return Err(format!(
                "fixture {} is missing; run `cargo xtask programs`",
                path.display()
            )
            .into());
        }
        Ok(path)
    }

    pub const ALL: [Fixture; 5] = [
        Fixture::RedlineProgram,
        Fixture::RedshiftProgram,
        Fixture::RedhatProgram,
        Fixture::RedshiftProgramSlim,
        Fixture::RedshiftProgramSlimUpgraded,
    ];

    pub const fn so_name(self) -> &'static str {
        match self {
            Fixture::RedlineProgram => "redline_program.so",
            Fixture::RedshiftProgram => "redshift_program.so",
            Fixture::RedhatProgram => "redhat_program.so",
            Fixture::RedshiftProgramSlim => "redshift_program_slim.so",
            Fixture::RedshiftProgramSlimUpgraded => "redshift_program_slim_upgraded.so",
        }
    }

    pub const fn loaded_at_base_boot(self) -> bool {
        matches!(
            self,
            Fixture::RedlineProgram | Fixture::RedshiftProgram | Fixture::RedhatProgram
        )
    }

    pub const fn program_id(self) -> Pubkey {
        match self {
            Fixture::RedlineProgram => redline_interface::ID,
            Fixture::RedshiftProgram
            | Fixture::RedshiftProgramSlim
            | Fixture::RedshiftProgramSlimUpgraded => redshift_interface::ID,
            Fixture::RedhatProgram => redhat_interface::ID,
        }
    }
}

pub type ScenarioFuture = Pin<Box<dyn Future<Output = RunRecord>>>;

pub struct ScenarioEntry {
    pub family: Family,
    pub short_name: &'static str,
    pub er: Er,
    pub run: fn(ExecutionConfig, std::result::Result<ErCtx, String>) -> ScenarioFuture,
}

impl ScenarioEntry {
    pub fn name(&self) -> String {
        format!("{}/{}", self.family.prefix(), self.short_name)
    }
}

#[macro_export]
macro_rules! scenario_catalog {
    (@execute $grouped:ident($group:expr), $scenario:expr, $fixtures:expr,
     $config:expr, $er:expr) => {
        $crate::run_shared_scenario(
            $scenario, $fixtures, $config, $er,
        )
    };
    (@execute $base_only:ident, $scenario:expr, $fixtures:expr,
     $config:expr, $er:expr) => {
        $crate::run_private_er_scenario(
            $scenario, $fixtures, $config,
        )
    };
    (
        family: $family:ident,
        $($short_name:ident => $($segment:ident)::+ {
            er: $er:ident $(($group:expr))?,
            fixtures: [$($fixture:expr),* $(,)?]
            $(,)?
        }),* $(,)?
    ) => {
        pub const SCENARIOS: &[$crate::catalog::ScenarioEntry] = &[
            $($crate::catalog::ScenarioEntry {
                family: $crate::catalog::Family::$family,
                short_name: stringify!($short_name),
                er: $crate::catalog::Er::$er $((&$group))?,
                run: |config, _group_er| {
                    Box::pin($crate::scenario_catalog!(
                        @execute $er $(($group))?,
                        scenarios::$($segment)::+,
                        &[$($fixture),*],
                        config,
                        _group_er
                    ))
                },
            },)*
        ];

    };
}
