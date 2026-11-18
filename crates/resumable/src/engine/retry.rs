use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Backoff {
    None,
    Fixed(Duration),
    Exponential {
        initial: Duration,
        factor: f64,
        max: Duration,
        jitter: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: Backoff,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: Backoff::None,
        }
    }
}

impl RetryPolicy {
    pub(super) fn record(&self, retryable: bool) -> crate::RetryPolicyRecord {
        use crate::BackoffRecord;
        let backoff = match self.backoff {
            Backoff::None => BackoffRecord::None,
            Backoff::Fixed(delay) => BackoffRecord::Fixed {
                delay_ms: delay.as_millis().min(u64::MAX as u128) as u64,
            },
            Backoff::Exponential {
                initial,
                factor,
                max,
                jitter,
            } => BackoffRecord::Exponential {
                initial_ms: initial.as_millis().min(u64::MAX as u128) as u64,
                factor_bits: factor.to_bits(),
                max_ms: max.as_millis().min(u64::MAX as u128) as u64,
                jitter,
            },
        };
        crate::RetryPolicyRecord {
            max_attempts: self.max_attempts,
            backoff,
            retryable,
        }
    }

    pub fn validate(&self) -> Result<(), RetryPolicyError> {
        if self.max_attempts == 0 {
            return Err(RetryPolicyError::ZeroAttempts);
        }
        if let Backoff::Exponential { factor, .. } = self.backoff {
            if !factor.is_finite() || factor < 1.0 {
                return Err(RetryPolicyError::InvalidFactor);
            }
        }
        Ok(())
    }
    pub(super) fn delay(&self, attempt: u32, seed: u64) -> Duration {
        match self.backoff {
            Backoff::None => Duration::ZERO,
            Backoff::Fixed(delay) => delay,
            Backoff::Exponential {
                initial,
                factor,
                max,
                jitter,
            } => {
                let exponent = attempt.saturating_sub(1).min(1024) as i32;
                let raw = (initial.as_secs_f64() * factor.powi(exponent)).min(max.as_secs_f64());
                let value = if jitter {
                    raw * (0.5 + (seed % 10001) as f64 / 20000.0)
                } else {
                    raw
                };
                Duration::from_secs_f64(value.min(max.as_secs_f64()).max(0.0))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RetryPolicyError {
    #[error("max_attempts must be greater than zero")]
    ZeroAttempts,
    #[error("exponential backoff factor must be finite and at least 1")]
    InvalidFactor,
}

#[derive(Debug, Clone, Default)]
pub struct StepOptions {
    pub retry_policy: RetryPolicy,
    pub retryable: bool,
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    #[test]
    fn policy_rejects_zero_attempts_and_invalid_factors() {
        assert_eq!(RetryPolicy::default().validate(), Ok(()));
        assert_eq!(
            RetryPolicy {
                max_attempts: 0,
                backoff: Backoff::None
            }
            .validate(),
            Err(RetryPolicyError::ZeroAttempts)
        );
        assert_eq!(
            RetryPolicy {
                max_attempts: 2,
                backoff: Backoff::Exponential {
                    initial: Duration::from_secs(1),
                    factor: f64::NAN,
                    max: Duration::from_secs(8),
                    jitter: false
                }
            }
            .validate(),
            Err(RetryPolicyError::InvalidFactor)
        );
    }

    #[test]
    fn exponential_delay_is_bounded_and_jitter_stays_within_bounds() {
        let policy = RetryPolicy {
            max_attempts: u32::MAX,
            backoff: Backoff::Exponential {
                initial: Duration::from_secs(2),
                factor: 2.0,
                max: Duration::from_secs(10),
                jitter: true,
            },
        };
        for seed in 0..1000 {
            let delay = policy.delay(u32::MAX, seed);
            assert!(delay <= Duration::from_secs(10));
            assert!(delay.as_millis() >= 4_999);
        }
    }
}
