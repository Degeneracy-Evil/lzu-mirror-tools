use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::parser::{CronParser, Seconds, Year};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AttemptState, RunState};

const MIN_INTERVAL_SECONDS: u64 = 60;
const MAX_INTERVAL_SECONDS: u64 = 365 * 24 * 60 * 60;
const MONTH_NAMES: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const WEEKDAY_NAMES: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged, deny_unknown_fields)]
pub enum ScheduleConfig {
    Interval { interval: String },
    Cron { cron: String, timezone: String },
}

impl ScheduleConfig {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Interval { interval } => parse_interval(interval).map(|_| ()),
            Self::Cron { cron, timezone } => {
                validate_cron(cron)?;
                timezone
                    .parse::<Tz>()
                    .map(|_| ())
                    .map_err(|_| format!("unknown IANA timezone {timezone:?}"))
            }
        }
    }

    pub fn canonicalized(self) -> Result<Self, String> {
        match self {
            Self::Interval { interval } => Ok(Self::Interval {
                interval: format!("{}s", parse_interval(&interval)?),
            }),
            Self::Cron { cron, timezone } => {
                validate_cron(&cron)?;
                timezone
                    .parse::<Tz>()
                    .map_err(|_| format!("unknown IANA timezone {timezone:?}"))?;
                Ok(Self::Cron {
                    cron: cron
                        .split_whitespace()
                        .map(str::to_uppercase)
                        .collect::<Vec<_>>()
                        .join(" "),
                    timezone,
                })
            }
        }
    }

    pub fn semantic_hash(&self) -> String {
        let bytes = toml::to_string(self).expect("schedule serialization cannot fail");
        format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
    }

    pub fn interval_seconds(&self) -> Option<u64> {
        match self {
            Self::Interval { interval } => parse_interval(interval).ok(),
            Self::Cron { .. } => None,
        }
    }

    pub fn next_after_ms(&self, after_ms: i64) -> Result<i64, String> {
        match self {
            Self::Interval { interval } => {
                let millis = i64::try_from(parse_interval(interval)? * 1_000)
                    .map_err(|_| "interval timestamp overflow".to_owned())?;
                after_ms
                    .checked_add(millis)
                    .ok_or_else(|| "interval timestamp overflow".to_owned())
            }
            Self::Cron { cron, timezone } => {
                let timezone = timezone
                    .parse::<Tz>()
                    .map_err(|_| format!("unknown IANA timezone {timezone:?}"))?;
                let start = DateTime::<Utc>::from_timestamp_millis(after_ms)
                    .ok_or_else(|| "timestamp outside supported range".to_owned())?
                    .with_timezone(&timezone);
                let parsed = parse_cron(cron)?;
                parsed
                    .find_next_occurrence(&start, false)
                    .map(|next| next.timestamp_millis())
                    .map_err(|error| error.to_string())
            }
        }
    }
}

fn parse_interval(value: &str) -> Result<u64, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.subsec_nanos() != 0 {
        return Err("interval must use whole seconds".into());
    }
    let seconds = duration.as_secs();
    if !(MIN_INTERVAL_SECONDS..=MAX_INTERVAL_SECONDS).contains(&seconds) {
        return Err("interval must be between 1m and 365d".into());
    }
    Ok(seconds)
}

fn parse_cron(expression: &str) -> Result<croner::Cron, String> {
    CronParser::builder()
        .seconds(Seconds::Disallowed)
        .year(Year::Disallowed)
        .build()
        .parse(expression)
        .map_err(|error| error.to_string())
}

fn validate_cron(expression: &str) -> Result<(), String> {
    let fields = expression.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err("cron must contain exactly five fields".into());
    }
    if expression.contains(['@', '?', '#', '+']) {
        return Err("extended cron syntax is not supported".into());
    }
    for word in expression
        .split(|character: char| !character.is_ascii_alphabetic())
        .filter(|word| !word.is_empty())
    {
        let word = word.to_uppercase();
        if !MONTH_NAMES.contains(&word.as_str()) && !WEEKDAY_NAMES.contains(&word.as_str()) {
            return Err(format!("unsupported cron token {word:?}"));
        }
    }
    parse_cron(expression).map(|_| ())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunTrigger {
    Manual,
    Scheduled,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct ScheduleRuntime {
    pub next_due_at_ms: Option<i64>,
    pub last_evaluated_at_ms: Option<i64>,
    pub catch_up_pending: bool,
    pub catch_up_since_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DueEvaluation {
    pub runtime: ScheduleRuntime,
    pub became_due: bool,
    pub skipped_while_active: bool,
}

pub fn activate_schedule(schedule: &ScheduleConfig, now_ms: i64) -> Result<ScheduleRuntime, String> {
    Ok(ScheduleRuntime {
        next_due_at_ms: Some(schedule.next_after_ms(now_ms)?),
        last_evaluated_at_ms: Some(now_ms),
        catch_up_pending: false,
        catch_up_since_ms: None,
    })
}

pub fn evaluate_schedule_due(
    schedule: &ScheduleConfig,
    mut runtime: ScheduleRuntime,
    now_ms: i64,
    has_active_run: bool,
) -> Result<DueEvaluation, String> {
    let Some(due_at) = runtime.next_due_at_ms else {
        return Ok(DueEvaluation {
            runtime,
            became_due: false,
            skipped_while_active: false,
        });
    };
    if due_at > now_ms {
        return Ok(DueEvaluation {
            runtime,
            became_due: false,
            skipped_while_active: false,
        });
    }

    runtime.last_evaluated_at_ms = Some(now_ms);
    if has_active_run {
        runtime.catch_up_pending = false;
        runtime.catch_up_since_ms = None;
    } else {
        runtime.catch_up_pending = true;
        runtime.catch_up_since_ms = Some(
            runtime
                .catch_up_since_ms
                .map_or(due_at, |existing| existing.min(due_at)),
        );
    }
    runtime.next_due_at_ms = match schedule {
        ScheduleConfig::Interval { .. } => None,
        ScheduleConfig::Cron { .. } => Some(schedule.next_after_ms(now_ms)?),
    };
    Ok(DueEvaluation {
        runtime,
        became_due: !has_active_run,
        skipped_while_active: has_active_run,
    })
}

pub fn rearm_interval(schedule: &ScheduleConfig, terminal_at_ms: i64) -> Result<Option<i64>, String> {
    match schedule {
        ScheduleConfig::Interval { .. } => schedule.next_after_ms(terminal_at_ms).map(Some),
        ScheduleConfig::Cron { .. } => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RetryContext {
    pub outcome: AttemptState,
    pub attempt_no: u32,
    pub max_attempts: u32,
    pub retry_delay_seconds: u64,
    pub cancel_requested: bool,
    pub mirror_eligible: bool,
    pub owner_unchanged: bool,
    pub server_now_ms: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryDecision {
    Schedule { retry_due_at_ms: i64 },
    Final(RunState),
}

pub fn decide_retry(context: RetryContext) -> RetryDecision {
    let retryable = matches!(
        context.outcome,
        AttemptState::Failed | AttemptState::TimedOut | AttemptState::Interrupted
    );
    if retryable
        && context.attempt_no < context.max_attempts
        && !context.cancel_requested
        && context.mirror_eligible
        && context.owner_unchanged
    {
        let delay_ms = i64::try_from(context.retry_delay_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX);
        return RetryDecision::Schedule {
            retry_due_at_ms: context.server_now_ms.saturating_add(delay_ms),
        };
    }
    RetryDecision::Final(match context.outcome {
        AttemptState::Succeeded => RunState::Succeeded,
        AttemptState::TimedOut => RunState::TimedOut,
        AttemptState::Cancelled => RunState::Cancelled,
        AttemptState::Queued
        | AttemptState::Accepted
        | AttemptState::Running
        | AttemptState::Failed
        | AttemptState::Interrupted
        | AttemptState::Rejected => RunState::Failed,
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn intervals_are_bounded_and_canonical() {
        assert!(ScheduleConfig::Interval { interval: "59s".into() }.validate().is_err());
        assert!(
            ScheduleConfig::Interval {
                interval: "366d".into()
            }
            .validate()
            .is_err()
        );
        let hour = ScheduleConfig::Interval { interval: "60m".into() }
            .canonicalized()
            .expect("interval");
        let equivalent = ScheduleConfig::Interval { interval: "1h".into() }
            .canonicalized()
            .expect("interval");
        assert_eq!(hour, equivalent);
        assert_eq!(hour.semantic_hash(), equivalent.semantic_hash());
    }

    #[test]
    fn strict_cron_and_timezone_validation() {
        for invalid in [
            "@hourly",
            "0 0 * *",
            "0 0 L * *",
            "0 0 1W * *",
            "0 0 * * MON#2",
            "0 0 ? * *",
        ] {
            assert!(
                ScheduleConfig::Cron {
                    cron: invalid.into(),
                    timezone: "UTC".into()
                }
                .validate()
                .is_err(),
                "accepted {invalid}"
            );
        }
        assert!(
            ScheduleConfig::Cron {
                cron: "15 * * JAN-MAR MON-FRI".into(),
                timezone: "Asia/Shanghai".into()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn cron_dst_gap_and_overlap_follow_contract() {
        let fixed = ScheduleConfig::Cron {
            cron: "30 2 * * *".into(),
            timezone: "America/New_York".into(),
        };
        let before_gap = Utc.with_ymd_and_hms(2025, 3, 9, 6, 59, 0).unwrap().timestamp_millis();
        let after_gap = fixed.next_after_ms(before_gap).expect("gap occurrence");
        assert_eq!(
            DateTime::<Utc>::from_timestamp_millis(after_gap).expect("time"),
            Utc.with_ymd_and_hms(2025, 3, 9, 7, 0, 0).unwrap()
        );

        let overlap = ScheduleConfig::Cron {
            cron: "30 1 * * *".into(),
            timezone: "America/New_York".into(),
        };
        let before_overlap = Utc.with_ymd_and_hms(2025, 11, 2, 4, 59, 0).unwrap().timestamp_millis();
        let first = overlap.next_after_ms(before_overlap).expect("overlap occurrence");
        assert_eq!(
            DateTime::<Utc>::from_timestamp_millis(first).expect("time"),
            Utc.with_ymd_and_hms(2025, 11, 2, 5, 30, 0).unwrap()
        );
        assert!(
            overlap.next_after_ms(first).expect("next occurrence")
                > Utc.with_ymd_and_hms(2025, 11, 2, 6, 30, 0).unwrap().timestamp_millis()
        );
    }

    #[test]
    fn due_intent_coalesces_and_retry_stays_in_run() {
        let cron = ScheduleConfig::Cron {
            cron: "* * * * *".into(),
            timezone: "UTC".into(),
        };
        let runtime = activate_schedule(&cron, 0).expect("activate");
        let first = evaluate_schedule_due(&cron, runtime, 600_000, false).expect("due");
        assert!(first.runtime.catch_up_pending);
        assert_eq!(first.runtime.catch_up_since_ms, Some(60_000));
        let again = evaluate_schedule_due(&cron, first.runtime, 1_200_000, false).expect("coalesce");
        assert_eq!(again.runtime.catch_up_since_ms, Some(60_000));

        assert_eq!(
            decide_retry(RetryContext {
                outcome: AttemptState::Failed,
                attempt_no: 1,
                max_attempts: 3,
                retry_delay_seconds: 5,
                cancel_requested: false,
                mirror_eligible: true,
                owner_unchanged: true,
                server_now_ms: 10_000,
            }),
            RetryDecision::Schedule {
                retry_due_at_ms: 15_000
            }
        );
    }
}
