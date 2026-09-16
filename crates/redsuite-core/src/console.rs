use std::{fmt::Arguments, sync::OnceLock};

pub const VERBOSE_ENV: &str = "REDSUITE_VERBOSE";

static VERBOSE: OnceLock<bool> = OnceLock::new();

pub fn verbose() -> bool {
    *VERBOSE.get_or_init(|| {
        std::env::var_os(VERBOSE_ENV)
            .is_some_and(|value| value != "0" && !value.is_empty())
    })
}

pub fn line(args: Arguments<'_>) {
    eprintln!("[redsuite] {args}");
}

pub fn detail(args: Arguments<'_>) {
    eprintln!("[redsuite]   {args}");
}

pub fn debug(args: Arguments<'_>) {
    if verbose() {
        eprintln!("[redsuite] {args}");
    }
}
