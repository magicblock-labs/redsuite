#[derive(Debug)]
pub struct ScenarioReport {
    pub scenario: String,
    pub passed: bool,
}

impl ScenarioReport {
    pub fn ok(name: &str) -> Self {
        Self {
            scenario: name.to_owned(),
            passed: true,
        }
    }
}
