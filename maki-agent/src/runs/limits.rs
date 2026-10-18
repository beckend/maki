use std::time::Duration;

use maki_config::{RunsConfig, UnattendedMode};

/// Timeouts above this are lowered, so `Instant + timeout` cannot overflow.
const MAX_TIMEOUT_SECS: u64 = 365 * 24 * 3600;

/// Caps of one hub, from `[agent.runs]`. Values above the field's range are
/// saturated; zero is rejected by the config validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitLimits {
    pub max_agents: u8,
    pub max_shells: u8,
    pub max_shells_per_owner: u8,
    pub max_depth: u8,
    pub max_children_per_run: u8,
    pub max_queue: u16,
    pub spawn_per_minute: u8,
    pub session_max_runs: u16,
    pub default_timeout: Duration,
    pub max_timeout: Duration,
    pub siblings: bool,
    pub permission_timeout: Duration,
    pub unattended: UnattendedMode,
    pub max_retained: u16,
    pub max_idle: u16,
    pub idle_timeout: Duration,
}

impl Default for UnitLimits {
    fn default() -> Self {
        Self::from(&RunsConfig::default())
    }
}

impl From<&RunsConfig> for UnitLimits {
    fn from(config: &RunsConfig) -> Self {
        let small = |v: u32| u8::try_from(v).unwrap_or(u8::MAX);
        let wide = |v: u32| u16::try_from(v).unwrap_or(u16::MAX);
        Self {
            max_agents: small(config.max_agents),
            max_shells: small(config.max_shells),
            max_shells_per_owner: small(config.max_shells_per_owner),
            max_depth: small(config.max_depth),
            max_children_per_run: small(config.max_children_per_run),
            max_queue: wide(config.max_queue),
            spawn_per_minute: small(config.spawn_per_minute),
            session_max_runs: wide(config.session_max_runs),
            default_timeout: Duration::from_secs(config.default_timeout_secs.min(MAX_TIMEOUT_SECS)),
            max_timeout: Duration::from_secs(config.max_timeout_secs.min(MAX_TIMEOUT_SECS)),
            siblings: config.siblings,
            permission_timeout: Duration::from_secs(
                config.permission_timeout_secs.min(MAX_TIMEOUT_SECS),
            ),
            unattended: config.unattended_permissions,
            max_retained: wide(config.max_retained),
            max_idle: wide(config.max_idle),
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
            (4, 8, 4, 2, 8, 16, 10, 64, false)
        );
        assert_eq!(l.default_timeout, Duration::from_secs(1800));
        assert_eq!(l.max_timeout, Duration::from_secs(86_400));
        assert_eq!(
            (l.max_retained, l.max_idle, l.idle_timeout),
            (64, 16, Duration::from_secs(1800))
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
    fn oversized_values_saturate() {
        let config = RunsConfig {
            max_agents: 1000,
            max_queue: 1_000_000,
            ..RunsConfig::default()
        };
        let l = UnitLimits::from(&config);
        assert_eq!((l.max_agents, l.max_queue), (u8::MAX, u16::MAX));
    }
}
