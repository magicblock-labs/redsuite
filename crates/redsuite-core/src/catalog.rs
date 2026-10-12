use std::{future::Future, pin::Pin};

use pubkey::Pubkey;

use crate::{
    context::ErCtx, profile::ExecutionConfig, scenario::RunRecord,
    topology::ErOptions,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

#[derive(Debug, PartialEq, Eq)]
pub struct ErGroup {
    pub label: &'static str,
    pub env: &'static [(&'static str, &'static str)],
    pub proxied: bool,
}

impl ErGroup {
    pub fn options(&self) -> ErOptions {
        ErOptions {
            label: self.label.to_owned(),
            env: self
                .env
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            ..ErOptions::default()
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
            Fixture::RedshiftProgramSlimUpgraded => {
                "redshift_program_slim_upgraded.so"
            }
        }
    }

    pub const fn loaded_at_base_boot(self) -> bool {
        matches!(
            self,
            Fixture::RedlineProgram
                | Fixture::RedshiftProgram
                | Fixture::RedhatProgram
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

    pub const fn package(self) -> &'static str {
        match self {
            Fixture::RedlineProgram => "redline-program",
            Fixture::RedshiftProgram
            | Fixture::RedshiftProgramSlim
            | Fixture::RedshiftProgramSlimUpgraded => "redshift-program",
            Fixture::RedhatProgram => "redhat-program",
        }
    }

    pub const fn features(self) -> &'static [&'static str] {
        match self {
            Fixture::RedlineProgram
            | Fixture::RedshiftProgram
            | Fixture::RedhatProgram => &["default"],
            Fixture::RedshiftProgramSlim => &[],
            Fixture::RedshiftProgramSlimUpgraded => &["upgraded"],
        }
    }
}

pub type ScenarioFuture = Pin<Box<dyn Future<Output = RunRecord>>>;

pub struct ScenarioEntry {
    pub family: Family,
    pub short_name: &'static str,
    pub er: Er,
    pub host_exclusive: bool,
    pub fixtures: &'static [Fixture],
    pub optional_fixtures: &'static [Fixture],
    pub run: fn(
        ExecutionConfig,
        std::result::Result<ErCtx, String>,
    ) -> ScenarioFuture,
}

impl ScenarioEntry {
    pub fn name(&self) -> String {
        format!("{}/{}", self.family.prefix(), self.short_name)
    }
}

#[macro_export]
macro_rules! scenario_catalog {
    (@execute Shared($group:expr), $scenario:expr, $fixtures:expr,
     $optional:expr, $config:expr, $er:expr) => {
        $crate::run_shared_scenario(
            $scenario, $fixtures, $optional, $config, $er,
        )
    };
    (@execute Exclusive($group:expr), $scenario:expr, $fixtures:expr,
     $optional:expr, $config:expr, $er:expr) => {
        $crate::run_shared_scenario(
            $scenario, $fixtures, $optional, $config, $er,
        )
    };
    (@execute $base_only:ident, $scenario:expr, $fixtures:expr,
     $optional:expr, $config:expr, $er:expr) => {
        $crate::run_private_er_scenario(
            $scenario, $fixtures, $optional, $config,
        )
    };
    (
        family: $family:ident,
        $($short_name:ident => $($segment:ident)::+ {
            er: $er:ident $(($group:expr))?,
            $(host_exclusive: $host_exclusive:literal,)?
            fixtures: [$($fixture:expr),* $(,)?]
            $(, optional_fixtures: [$($optional:expr),* $(,)?])?
            $(,)?
        }),* $(,)?
    ) => {
        pub const SCENARIOS: &[$crate::catalog::ScenarioEntry] = &[
            $($crate::catalog::ScenarioEntry {
                family: $crate::catalog::Family::$family,
                short_name: stringify!($short_name),
                er: $crate::catalog::Er::$er $((&$group))?,
                host_exclusive: false $(| $host_exclusive)?,
                fixtures: &[$($fixture),*],
                optional_fixtures: &[$($($optional),*)?],
                run: |config, _group_er| {
                    Box::pin($crate::scenario_catalog!(
                        @execute $er $(($group))?,
                        scenarios::$($segment)::+,
                        &[$($fixture),*],
                        &[$($($optional),*)?],
                        config,
                        _group_er
                    ))
                },
            },)*
        ];

    };
}
