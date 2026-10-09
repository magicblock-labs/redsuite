use crate::Result;

pub const PROFILE_ENV: &str = "REDSUITE_PROFILE";
pub const LOOP_ENV: &str = "REDSUITE_LOOP";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Profile {
    Lite,
    Full,
}

impl Profile {
    pub fn name(self) -> &'static str {
        match self {
            Profile::Lite => "lite",
            Profile::Full => "full",
        }
    }

    pub fn parse(text: &str) -> Option<Profile> {
        match text {
            "lite" => Some(Profile::Lite),
            "full" => Some(Profile::Full),
            _ => None,
        }
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

// The run's frontend inputs, parsed and validated once by the CLI from its
// arguments (environment as fallback) — scenario code never reads the
// variables.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionConfig {
    pub profile: Profile,
    pub loop_mode: LoopMode,
}

impl ExecutionConfig {
    pub fn from_env(redline: bool) -> Result<Self> {
        Ok(Self {
            profile: if redline {
                parse_env(
                    PROFILE_ENV,
                    Profile::Lite,
                    Profile::parse,
                    "lite|full",
                )?
            } else {
                Profile::Lite
            },
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
}

impl<T> ProfileValues<T> {
    pub fn select(&self, profile: Profile) -> &T {
        match profile {
            Profile::Lite => &self.lite,
            Profile::Full => &self.full,
        }
    }
}
