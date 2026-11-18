use std::time::Duration;

use maki_config::{RunsConfig, UnattendedMode};

/// Timeouts above this are lowered, so `Instant + timeout` cannot overflow.
const MAX_TIMEOUT_SECS: u64 = 365 * 24 * 3600;

/// Caps of one hub, from `[agent.runs]`. A cap of [`u32::MAX`] is no cap: the
/// defaults leave every limit open, and a host that wants a ceiling sets one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitLimits {
    pub max_agents: u32,
    pub max_shells: u32,
    pub max_shells_per_owner: u32,
    pub max_depth: u32,
    pub max_children_per_run: u32,
    pub max_queue: u32,
    pub spawn_per_minute: u32,
    pub session_max_runs: u32,
    pub default_timeout: Duration,
    pub max_timeout: Duration,
    pub siblings: bool,
    pub permission_timeout: Duration,
    pub unattended: UnattendedMode,
    pub max_retained: u32,
    pub max_idle: u32,
    pub idle_timeout: Duration,
}

impl Default for UnitLimits {
    fn default() -> Self {
        Self::from(&RunsConfig::default())
    }
}

impl From<&RunsConfig> for UnitLimits {
    fn from(config: &RunsConfig) -> Self {
        Self {
            max_agents: config.max_agents,
            max_shells: config.max_shells,
            max_shells_per_owner: config.max_shells_per_owner,
            max_depth: config.max_depth,
            max_children_per_run: config.max_children_per_run,
            max_queue: config.max_queue,
            spawn_per_minute: config.spawn_per_minute,
            session_max_runs: config.session_max_runs,
            default_timeout: Duration::from_secs(config.default_timeout_secs.min(MAX_TIMEOUT_SECS)),
            max_timeout: Duration::from_secs(config.max_timeout_secs.min(MAX_TIMEOUT_SECS)),
            siblings: config.siblings,
            permission_timeout: Duration::from_secs(
                config.permission_timeout_secs.min(MAX_TIMEOUT_SECS),
            ),
            unattended: config.unattended_permissions,
            max_retained: config.max_retained,
            max_idle: config.max_idle,
            idle_timeout: Duration::from_secs(config.idle_timeout_secs.min(MAX_TIMEOUT_SECS)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_design() {
        let l = UnitLimits::default();
        assert_eq!(
            (
                l.max_agents,
                l.max_shells,
                l.max_shells_per_owner,
                l.max_depth,
                l.max_children_per_run,
                l.max_queue,
                l.spawn_per_minute,
                l.session_max_runs,
                l.siblings
            ),
            (
                u32::MAX,
                u32::MAX,
                u32::MAX,
                u32::MAX,
                u32::MAX,
                u32::MAX,
                u32::MAX,
                u32::MAX,
                false
            )
        );
        assert_eq!(l.default_timeout, Duration::from_secs(86_400));
        assert_eq!(l.max_timeout, Duration::from_secs(86_400));
        assert_eq!(
            (l.max_retained, l.max_idle, l.idle_timeout),
            (u32::MAX, u32::MAX, Duration::from_secs(1800))
        );
    }

    #[test]
    fn huge_timeouts_are_lowered() {
        let config = RunsConfig {
            default_timeout_secs: u64::MAX,
            max_timeout_secs: u64::MAX,
            ..RunsConfig::default()
        };
        let l = UnitLimits::from(&config);
        assert_eq!(l.max_timeout, Duration::from_secs(MAX_TIMEOUT_SECS));
        assert!(
            std::time::Instant::now()
                .checked_add(l.default_timeout)
                .is_some()
        );
    }

    #[test]
    fn a_cap_is_taken_as_it_is() {
        let config = RunsConfig {
            max_agents: 1000,
            max_queue: 1_000_000,
            ..RunsConfig::default()
        };
        let l = UnitLimits::from(&config);
        assert_eq!((l.max_agents, l.max_queue), (1000, 1_000_000));
    }
}
