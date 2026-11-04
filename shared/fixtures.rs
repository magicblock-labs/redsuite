#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fixture {
    RedlineProgram,
    RedshiftProgram,
    RedhatProgram,
    UpgradeProbe,
    UpgradeProbeUpgraded,
}

impl Fixture {
    pub const ALL: [Fixture; 5] = [
        Fixture::RedlineProgram,
        Fixture::RedshiftProgram,
        Fixture::RedhatProgram,
        Fixture::UpgradeProbe,
        Fixture::UpgradeProbeUpgraded,
    ];

    pub const fn so_name(self) -> &'static str {
        match self {
            Fixture::RedlineProgram => "redline_program.so",
            Fixture::RedshiftProgram => "redshift_program.so",
            Fixture::RedhatProgram => "redhat_program.so",
            Fixture::UpgradeProbe => "upgrade_probe.so",
            Fixture::UpgradeProbeUpgraded => "upgrade_probe_upgraded.so",
        }
    }

    pub const fn build(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::RedlineProgram => ("redline", None),
            Self::RedshiftProgram => ("redshift", None),
            Self::RedhatProgram => ("redhat", None),
            Self::UpgradeProbe => ("upgrade", Some("v1")),
            Self::UpgradeProbeUpgraded => ("upgrade", Some("v2")),
        }
    }
}
