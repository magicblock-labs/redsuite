use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};

use crate::Result;

const PROFILE_ENV: &str = "REDSUITE_PROFILE";
const LOOP_ENV: &str = "REDSUITE_LOOP";

pub const ALL: &[&str] = &["lite", "full", "soak", "deep"];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Profile {
    Lite,
    Full,
    Soak,
    Deep,
}

impl Profile {
    pub const ALL: [Profile; 4] =
        [Profile::Lite, Profile::Full, Profile::Soak, Profile::Deep];

    pub fn name(self) -> &'static str {
        match self {
            Profile::Lite => "lite",
            Profile::Full => "full",
            Profile::Soak => "soak",
            Profile::Deep => "deep",
        }
    }

    pub fn parse(text: &str) -> Option<Profile> {
        Profile::ALL
            .into_iter()
            .find(|profile| profile.name() == text)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LoopMode {
    Open,
    Closed,
}

impl LoopMode {
    pub fn name(self) -> &'static str {
        match self {
            LoopMode::Open => "open",
            LoopMode::Closed => "closed",
        }
    }

    pub fn parse(text: &str) -> Option<LoopMode> {
        match text {
            "open" => Some(LoopMode::Open),
            "closed" => Some(LoopMode::Closed),
            _ => None,
        }
    }
}

// The run's frontend inputs, parsed and validated once. The CLI builds it
// from arguments (environment as fallback); nextest-spawned tests parse it
// from the environment before calling the executor, which records a bad
// value as a preflight failure — scenario code never reads the variables.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionConfig {
    pub profile: Profile,
    pub loop_mode: LoopMode,
}

impl ExecutionConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            profile: parse_env(
                PROFILE_ENV,
                Profile::Lite,
                Profile::parse,
                "lite|full|soak|deep",
            )?,
            loop_mode: parse_env(
                LOOP_ENV,
                LoopMode::Open,
                LoopMode::parse,
                "open|closed",
            )?,
        })
    }
}

fn parse_env<T>(
    env: &str,
    default: T,
    parse: fn(&str) -> Option<T>,
    expected: &str,
) -> Result<T> {
    match std::env::var(env) {
        Err(_) => Ok(default),
        Ok(value) => parse(&value).ok_or_else(|| {
            format!("unknown {env} `{value}` (expected {expected})").into()
        }),
    }
}

pub struct ProfileValues<T> {
    pub lite: T,
    pub full: T,
    pub soak: Option<T>,
    pub deep: Option<T>,
}

impl<T> ProfileValues<T> {
    // soak and deep never substitute for each other; both step down to full
    fn resolve(&self, requested: Profile) -> (&T, Profile) {
        match requested {
            Profile::Lite => (&self.lite, Profile::Lite),
            Profile::Full => (&self.full, Profile::Full),
            Profile::Soak => self
                .soak
                .as_ref()
                .map(|values| (values, Profile::Soak))
                .unwrap_or((&self.full, Profile::Full)),
            Profile::Deep => self
                .deep
                .as_ref()
                .map(|values| (values, Profile::Deep))
                .unwrap_or((&self.full, Profile::Full)),
        }
    }
}

pub fn select<'v, T>(
    scenario: &str,
    config: ExecutionConfig,
    values: &'v ProfileValues<T>,
) -> (&'v T, Profile) {
    let (selected, level) = values.resolve(config.profile);
    if level != config.profile {
        announce(scenario, config.profile, level);
    }
    (selected, level)
}

fn announce(scenario: &str, requested: Profile, selected: Profile) {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let mut seen = SEEN
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if seen.insert(format!("{scenario}:{}", requested.name())) {
        eprintln!(
            "[redsuite] {scenario}: profile `{}` is not defined here, \
             running `{}`",
            requested.name(),
            selected.name()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(soak: bool, deep: bool) -> ProfileValues<&'static str> {
        ProfileValues {
            lite: "lite values",
            full: "full values",
            soak: soak.then_some("soak values"),
            deep: deep.then_some("deep values"),
        }
    }

    #[test]
    fn a_defined_profile_is_used_as_asked() {
        assert_eq!(
            values(false, true).resolve(Profile::Deep),
            (&"deep values", Profile::Deep)
        );
        assert_eq!(
            values(false, false).resolve(Profile::Lite),
            (&"lite values", Profile::Lite)
        );
    }

    #[test]
    fn a_missing_profile_steps_down_to_full() {
        assert_eq!(
            values(false, false).resolve(Profile::Deep),
            (&"full values", Profile::Full)
        );
        assert_eq!(
            values(false, false).resolve(Profile::Soak),
            (&"full values", Profile::Full)
        );
    }

    #[test]
    fn soak_and_deep_never_substitute_for_each_other() {
        assert_eq!(
            values(false, true).resolve(Profile::Soak),
            (&"full values", Profile::Full)
        );
        assert_eq!(
            values(true, false).resolve(Profile::Deep),
            (&"full values", Profile::Full)
        );
    }

    #[test]
    fn parsing_covers_every_level_and_rejects_unknown_names() {
        for profile in Profile::ALL {
            assert_eq!(Profile::parse(profile.name()), Some(profile));
        }
        assert!(ALL
            .iter()
            .zip(Profile::ALL)
            .all(|(name, profile)| *name == profile.name()));
        assert_eq!(Profile::parse("xyzzy"), None);
        assert_eq!(LoopMode::parse("closed"), Some(LoopMode::Closed));
        assert_eq!(LoopMode::parse("xyzzy"), None);
    }
}
