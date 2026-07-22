use core::fmt;

use crate::command::common::CommandError;
use crate::resp::RespBody;

// (ms, seq); field order == redis id order, so a BTreeMap stays sorted for XRANGE
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct StreamId {
    ms: u64,
    seq: u64,
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl StreamId {
    pub const MAX: Self = Self {
        ms: u64::MAX,
        seq: u64::MAX,
    };
    pub const ZERO: Self = Self { ms: 0, seq: 0 };

    pub const fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    pub const fn ms(&self) -> u64 {
        self.ms
    }

    pub const fn seq(&self) -> u64 {
        self.seq
    }

    pub const fn incr_seq(&mut self) {
        self.seq += 1;
    }

    pub fn parse(ms: &str, seq: &str) -> Result<Self, CommandError> {
        let ms: u64 = ms
            .parse()
            .map_err(|_| CommandError::ParseStream(ms.into()))?;
        let seq: u64 = seq
            .parse()
            .map_err(|_| CommandError::ParseStream(seq.into()))?;
        Ok(Self { ms, seq })
    }

    pub fn parse_opt_seq(string: &str, last: Option<Self>) -> Result<Self, CommandError> {
        if string.len() == 1 {
            return match (string, last) {
                ("+", _) => Ok(Self::MAX),
                ("$", Some(other)) => Ok(other),
                ("-", _) | ("$", None) => Ok(Self::ZERO),
                _ => Err(CommandError::ParseStream(string.into())),
            };
        }
        if string.contains('-') {
            let Some((ms, seq)) = string.split_once('-') else {
                return Err(CommandError::ParseStream(string.into()));
            };
            return Self::parse(ms, seq);
        }
        let ms: u64 = string
            .parse()
            .map_err(|_| CommandError::ParseStream(string.into()))?;

        Ok(Self { ms, seq: 0 })
    }

    pub const fn is_valid(&self, other: &Self) -> bool {
        let ms_greater = self.ms > other.ms;
        let ms_equal_and_seq_greater = self.ms == other.ms && self.seq > other.seq;
        ms_greater || ms_equal_and_seq_greater
    }
}

// XADD accepts an explicit id, a ms part with an auto-generated sequence
// (<ms>-*), or a fully auto-generated id (*).
#[derive(Copy, Clone)]
pub enum StreamIdSpec {
    Explicit(StreamId),
    AutoSeq(u64),
    Auto,
}

impl StreamIdSpec {
    pub fn parse(string: &str) -> Result<Self, CommandError> {
        if string == "*" {
            return Ok(Self::Auto);
        }

        let Some((ms, seq)) = string.split_once('-') else {
            return Err(CommandError::ParseStream(string.into()));
        };

        if seq == "*" {
            let ms: u64 = ms
                .parse()
                .map_err(|_| CommandError::ParseStream(ms.into()))?;
            return Ok(Self::AutoSeq(ms));
        }

        Ok(Self::Explicit(StreamId::parse(ms, seq)?))
    }
}

impl From<StreamId> for Vec<u8> {
    fn from(value: StreamId) -> Self {
        value.to_string().into_bytes()
    }
}

impl From<StreamId> for String {
    fn from(value: StreamId) -> Self {
        value.to_string()
    }
}

impl From<StreamId> for RespBody {
    fn from(value: StreamId) -> Self {
        Self::Bulk(Some(value.into()))
    }
}
