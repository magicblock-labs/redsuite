use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};

const ENV: &str = "REDSUITE_PROFILE";

pub const DEFAULT: &str = "lite";
pub const ALL: &[&str] = &["lite", "full", "soak", "deep"];

const CHAINS: &[(&str, &[&str])] = &[
    ("lite", &["lite"]),
    ("full", &["full", "lite"]),
    ("soak", &["soak", "full", "lite"]),
    ("deep", &["deep", "full", "lite"]),
];

pub fn is_known(name: &str) -> bool {
    ALL.contains(&name)
}

pub fn requested() -> String {
    std::env::var(ENV).unwrap_or_else(|_| DEFAULT.to_owned())
}

pub fn select(scenario: &str, available: &[&str]) -> &'static str {
    let requested = requested();
    if !is_known(&requested) {
        panic!(
            "unknown REDSUITE_PROFILE `{requested}` (expected {})",
            ALL.join("|")
        );
    }
    let selected = resolve(&requested, available).unwrap_or_else(|| {
        panic!("{scenario} defines no profile at or below `{requested}`")
    });
    if selected != requested {
        announce(scenario, &requested, selected);
    }
    selected
}

fn resolve(requested: &str, available: &[&str]) -> Option<&'static str> {
    CHAINS
        .iter()
        .find(|(name, _)| *name == requested)?
        .1
        .iter()
        .copied()
        .find(|candidate| available.iter().any(|name| name == candidate))
}

fn announce(scenario: &str, requested: &str, selected: &str) {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let mut seen = SEEN
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if seen.insert(format!("{scenario}:{requested}")) {
        eprintln!(
            "[redsuite] {scenario}: profile `{requested}` is not defined \
             here, running `{selected}`"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_defined_profile_is_used_as_asked() {
        assert_eq!(resolve("deep", &["lite", "full", "deep"]), Some("deep"));
        assert_eq!(resolve("lite", &["lite", "full"]), Some("lite"));
    }

    #[test]
    fn a_missing_profile_steps_down_to_the_next_defined_one() {
        assert_eq!(resolve("deep", &["lite", "full"]), Some("full"));
        assert_eq!(resolve("full", &["lite"]), Some("lite"));
    }

    #[test]
    fn soak_and_deep_never_substitute_for_each_other() {
        assert_eq!(resolve("soak", &["lite", "full", "deep"]), Some("full"));
        assert_eq!(resolve("deep", &["lite", "full", "soak"]), Some("full"));
    }

    #[test]
    fn an_unknown_name_resolves_to_nothing() {
        assert!(!is_known("xyzzy"));
        assert_eq!(resolve("xyzzy", &["lite", "full"]), None);
    }
}
