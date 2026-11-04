use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Runtime {
    pub agave: Agave,
    pub yellowstone: Yellowstone,
}

#[derive(Deserialize, Serialize)]
pub struct Agave {
    pub version: String,
    pub revision: String,
}

#[derive(Deserialize, Serialize)]
pub struct Yellowstone {
    pub version: String,
    pub revision: String,
}

impl Runtime {
    pub fn yellowstone_tag(&self) -> String {
        format!(
            "v{}+solana.{}",
            self.yellowstone.version, self.agave.version
        )
    }
}
