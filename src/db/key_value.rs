use core::fmt;
use std::borrow::Borrow;

use crate::command::common::CommandError;
use crate::resp::RespBody;

// Key and Value are both plain byte-string wrappers with the identical set of
// RESP bulk-string conversions — generated once here instead of by hand per
// type, so the two can't drift out of sync with each other.
macro_rules! byte_newtype {
    ($ty:ident) => {
        impl From<Vec<u8>> for $ty {
            fn from(value: Vec<u8>) -> Self {
                Self { value }
            }
        }

        impl From<&[u8]> for $ty {
            fn from(value: &[u8]) -> Self {
                Self {
                    value: value.into(),
                }
            }
        }

        impl From<$ty> for Vec<u8> {
            fn from(value: $ty) -> Vec<u8> {
                value.value
            }
        }

        impl From<&$ty> for Vec<u8> {
            fn from(value: &$ty) -> Vec<u8> {
                value.value.clone()
            }
        }

        impl From<$ty> for RespBody {
            fn from(value: $ty) -> Self {
                RespBody::Bulk(Some(value.into()))
            }
        }

        impl From<&$ty> for RespBody {
            fn from(value: &$ty) -> Self {
                RespBody::Bulk(Some(value.into()))
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&String::from_utf8_lossy(&self.value))
            }
        }
    };
}

#[derive(Eq, Default, Debug, Hash, PartialEq, Clone)]
pub struct Key {
    value: Vec<u8>,
}

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Eq, Default, Debug, PartialEq, Clone)]
pub struct Value {
    value: Vec<u8>,
}

impl Value {
    pub fn from_int(num: i64) -> Self {
        Self {
            value: num.to_string().as_bytes().into(),
        }
    }

    pub fn parse_int(&self) -> Result<i64, CommandError> {
        str::from_utf8(&self.value)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(CommandError::NotAnInteger)
    }
}

byte_newtype!(Key);
byte_newtype!(Value);
