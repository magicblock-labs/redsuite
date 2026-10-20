use clap::ValueEnum;

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

#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutionConfig {
    pub profile: Profile,
    pub loop_mode: LoopMode,
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
