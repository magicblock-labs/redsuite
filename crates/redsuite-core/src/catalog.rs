use std::{future::Future, pin::Pin};

use pubkey::Pubkey;

use crate::{profile::ExecutionConfig, scenario::RunRecord};

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Topology {
    Shared,
    PrivateEr,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Resource {
    Er,
    HostExclusive,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lane {
    Exclusive,
    PrivateEr,
    Shared,
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
    pub topology: Topology,
    pub resources: &'static [Resource],
    pub fixtures: &'static [Fixture],
    pub optional_fixtures: &'static [Fixture],
    pub run: fn(ExecutionConfig) -> ScenarioFuture,
}

impl ScenarioEntry {
    pub fn name(&self) -> String {
        format!("{}/{}", self.family.prefix(), self.short_name)
    }

    pub fn lane(&self) -> Lane {
        if self.resources.contains(&Resource::HostExclusive) {
            Lane::Exclusive
        } else if self.topology == Topology::PrivateEr {
            Lane::PrivateEr
        } else {
            Lane::Shared
        }
    }
}

#[macro_export]
macro_rules! scenario_catalog {
    (@execute Shared, $scenario:expr, $fixtures:expr, $optional:expr,
     $config:expr) => {
        $crate::run_shared_scenario($scenario, $fixtures, $optional, $config)
    };
    (@execute PrivateEr, $scenario:expr, $fixtures:expr, $optional:expr,
     $config:expr) => {
        $crate::run_private_er_scenario(
            $scenario, $fixtures, $optional, $config,
        )
    };
    (
        family: $family:ident,
        $($short_name:ident => $($segment:ident)::+ {
            topology: $topology:ident,
            resources: [$($resource:expr),* $(,)?],
            fixtures: [$($fixture:expr),* $(,)?]
            $(, optional_fixtures: [$($optional:expr),* $(,)?])?
            $(,)?
        }),* $(,)?
    ) => {
        pub const SCENARIOS: &[$crate::catalog::ScenarioEntry] = &[
            $($crate::catalog::ScenarioEntry {
                family: $crate::catalog::Family::$family,
                short_name: stringify!($short_name),
                topology: $crate::catalog::Topology::$topology,
                resources: &[$($resource),*],
                fixtures: &[$($fixture),*],
                optional_fixtures: &[$($($optional),*)?],
                run: |config| {
                    Box::pin($crate::scenario_catalog!(@execute $topology,
                        scenarios::$($segment)::+,
                        &[$($fixture),*],
                        &[$($($optional),*)?],
                        config
                    ))
                },
            },)*
        ];

    };
}
