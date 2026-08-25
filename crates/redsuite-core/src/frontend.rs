use crate::{profile, report, topology, Result};

pub const ENV_VARS: &[&str] = &[
    topology::ER_BIN_ENV,
    topology::ROOT_ENV,
    topology::CLONE_URL_ENV,
    profile::PROFILE_ENV,
    profile::LOOP_ENV,
];

pub fn dispatch(args: &[String]) -> Option<Result<()>> {
    let arg = |index: usize| args.get(index).map(String::as_str);
    match (arg(0), arg(1)) {
        (Some("stack"), Some("status")) => Some(topology::status()),
        (Some("stack"), Some("down")) => Some(topology::down()),
        (Some("report"), Some("list")) => Some(report::list()),
        (Some("report"), Some("compare")) => {
            let rest = &args[2..];
            let strict = rest.iter().any(|flag| flag == "--strict");
            let brief = rest.iter().any(|flag| flag == "--brief");
            let filter = rest.iter().find(|value| !value.starts_with("--"));
            Some(report::compare(filter.map(String::as_str), strict, brief))
        }
        (Some("report"), Some("bmf")) => match (arg(2), arg(3)) {
            (Some("--out"), Some(path)) => Some(report::bmf(Some(path))),
            (None, _) => Some(report::bmf(None)),
            _ => None,
        },
        _ => None,
    }
}

pub fn usage(invocation: &str) -> String {
    [
        ("stack status", "show the shared base+ER stack (booted on demand by tests)"),
        ("stack down", "stop the shared stack and clear its state"),
        ("report list", "list persisted scenario reports (target/redsuite-reports/)"),
        (
            "report compare [scenario] [--strict] [--brief]",
            "diff the latest run per scenario against its nearest comparable baseline (--strict: fail on regressions; --brief: changed metrics only)",
        ),
        ("report bmf [--out <path>]", "export the latest campaign as Bencher Metric Format JSON"),
    ]
    .into_iter()
    .map(|(command, description)| {
        format!("  {invocation} {command:<46} {description}\n")
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn unknown_commands_are_not_dispatched() {
        assert!(dispatch(&args(&["frobnicate"])).is_none());
        assert!(dispatch(&args(&["stack"])).is_none());
        assert!(dispatch(&args(&["stack", "restart"])).is_none());
        assert!(dispatch(&args(&["report"])).is_none());
        assert!(dispatch(&args(&["report", "bmf", "--out"])).is_none());
        assert!(dispatch(&args(&["report", "bmf", "extra"])).is_none());
    }

    #[test]
    fn usage_names_the_invocation() {
        let text = usage("redsuite");
        assert!(text.contains("redsuite stack down"));
        assert!(text.contains("redsuite report bmf"));
        assert!(text.contains("redsuite report compare"));
    }
}
