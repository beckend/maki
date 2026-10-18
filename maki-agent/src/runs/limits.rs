use maki_config::RunsConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitLimits {
    pub max_depth: u8,
    pub max_children_per_run: u32,
    pub session_max_runs: u32,
    pub siblings: bool,
}

impl Default for UnitLimits {
    fn default() -> Self {
        Self::from(&RunsConfig::default())
    }
}

impl From<&RunsConfig> for UnitLimits {
    fn from(config: &RunsConfig) -> Self {
        Self {
            max_depth: u8::try_from(config.max_depth).unwrap_or(u8::MAX),
            max_children_per_run: config.max_children_per_run,
            session_max_runs: config.session_max_runs,
            siblings: config.siblings,
        }
    }
}
