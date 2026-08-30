use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("invalid {kind}: {value:?}")]
pub struct InvalidIdentifier {
    kind: &'static str,
    value: String,
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

macro_rules! name_id {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidIdentifier> {
                let value = value.into();
                if valid_name(&value) {
                    Ok(Self(value))
                } else {
                    Err(InvalidIdentifier { kind: $kind, value })
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidIdentifier;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

name_id!(MirrorName, "mirror name");
name_id!(NodeName, "node name");
name_id!(AgentInstanceId, "agent instance id");
name_id!(RequestId, "request id");

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct AttemptNo(u32);

impl AttemptNo {
    pub fn new(value: u32) -> Result<Self, InvalidIdentifier> {
        if value == 0 {
            Err(InvalidIdentifier {
                kind: "attempt number",
                value: value.to_string(),
            })
        } else {
            Ok(Self(value))
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(Ulid);

impl RunId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for RunId {
    type Err = ulid::DecodeError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_path_and_api_safe() {
        assert!(MirrorName::new("ubuntu-24.04").is_ok());
        for invalid in ["", "../x", "/root", "a/b", " space"] {
            assert!(MirrorName::new(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn attempt_numbers_start_at_one() {
        assert!(AttemptNo::new(0).is_err());
        assert_eq!(AttemptNo::new(1).expect("one is valid").get(), 1);
    }
}
