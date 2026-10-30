use clap::ValueEnum;
use std::{path::PathBuf, rc::Rc, time::Duration};
use url::Url;

/// An existing devnet-backed environment whose processes are not owned by RedSuite.
#[derive(Debug)]
pub struct Environment {
    pub base_rpc: Url,
    pub base_ws: Url,
    pub er: Option<ErEndpoints>,
    pub payer: PathBuf,
}

#[derive(Debug)]
pub struct ErEndpoints {
    pub rpc: Url,
    pub ws: Url,
    pub metrics: Url,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, ValueEnum)]
pub enum Profile {
    #[default]
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
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, ValueEnum)]
pub enum LoopMode {
    #[default]
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
}

#[derive(Clone, Debug, Default)]
pub struct ExecutionConfig {
    pub profile: Profile,
    pub loop_mode: LoopMode,
    pub seed: u64,
    pub duration: Option<Duration>,
    pub stop: Option<tokio::sync::watch::Receiver<bool>>,
    pub environment: Option<Rc<Environment>>,
    pub continuous: bool,
}

impl ExecutionConfig {
    pub fn stopped(&self) -> bool {
        self.stop.as_ref().is_some_and(|stop| *stop.borrow())
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
