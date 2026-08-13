use std::future::Future;
use std::pin::Pin;

use crate::profile;
use crate::report::ScenarioReport;

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
    BaseAlt,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fixture {
    RedlineProgram,
    RedshiftProgram,
    RedhatProgram,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProfileSet(pub &'static [&'static str]);

impl ProfileSet {
    pub const ALL: ProfileSet = ProfileSet(profile::ALL);

    pub fn contains(&self, name: &str) -> bool {
        self.0.contains(&name)
    }
}

pub type ScenarioFuture = Pin<Box<dyn Future<Output = ScenarioReport>>>;

pub struct ScenarioEntry {
    pub family: Family,
    pub short_name: &'static str,
    pub profiles: ProfileSet,
    pub topology: Topology,
    pub resources: &'static [Resource],
    pub fixtures: &'static [Fixture],
    pub run: fn() -> ScenarioFuture,
}

impl ScenarioEntry {
    pub fn name(&self) -> String {
        format!("{}/{}", self.family.prefix(), self.short_name)
    }
}

#[macro_export]
macro_rules! scenario_catalog {
    (@profiles) => {
        $crate::catalog::ProfileSet::ALL
    };
    (@profiles $profiles:expr) => {
        $profiles
    };
    (
        family: $family:ident,
        $($short_name:ident => $($segment:ident)::+ {
            $(profiles: $profiles:expr,)?
            topology: $topology:expr,
            resources: [$($resource:expr),* $(,)?],
            fixtures: [$($fixture:expr),* $(,)?] $(,)?
        }),* $(,)?
    ) => {
        pub const SCENARIOS: &[$crate::catalog::ScenarioEntry] = &[
            $($crate::catalog::ScenarioEntry {
                family: $crate::catalog::Family::$family,
                short_name: stringify!($short_name),
                profiles: $crate::scenario_catalog!(@profiles $($profiles)?),
                topology: $topology,
                resources: &[$($resource),*],
                fixtures: &[$($fixture),*],
                run: || {
                    Box::pin($crate::run_scenario(
                        scenarios::$($segment)::+,
                    ))
                },
            },)*
        ];

        $(
            #[cfg(test)]
            #[tokio::test]
            async fn $short_name() {
                $crate::run_scenario(scenarios::$($segment)::+).await;
            }
        )*

        #[cfg(test)]
        #[test]
        fn catalog_names_match_the_scenarios() {
            use $crate::Scenario as _;
            $(
                assert_eq!(
                    scenarios::$($segment)::+.name(),
                    format!(
                        "{}/{}",
                        $crate::catalog::Family::$family.prefix(),
                        stringify!($short_name)
                    ),
                );
            )*
        }
    };
}
