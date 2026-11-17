//! Timestamps as Kache writes them.
//!
//! Kache stores and prints instants as RFC 3339 text. Earlier releases wrote
//! that text with chrono, and event logs, reports and exports keep those
//! bytes: the fraction is padded to 3, 6 or 9 digits, and only the digits it
//! needs. [`rfc3339`] and [`rfc3339_z`] write exactly that, so a reader that
//! compares or diffs the text sees no change. Reading accepts any RFC 3339
//! instant.

use std::fmt::Write as _;

pub use jiff::{SignedDuration, Timestamp};

/// The current instant.
pub fn now() -> Timestamp {
    Timestamp::now()
}

/// `ts` as RFC 3339 with a `+00:00` offset, as chrono's `to_rfc3339` wrote it.
pub fn rfc3339(ts: Timestamp) -> String {
    format(ts, "+00:00")
}

/// `ts` as RFC 3339 with a `Z` offset, as chrono's serde impl wrote it.
pub fn rfc3339_z(ts: Timestamp) -> String {
    format(ts, "Z")
}

fn format(ts: Timestamp, offset: &str) -> String {
    let mut out = ts.strftime("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = ts.subsec_nanosecond().unsigned_abs();
    let _ = if nanos == 0 {
        Ok(())
    } else if nanos.is_multiple_of(1_000_000) {
        write!(out, ".{:03}", nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        write!(out, ".{:06}", nanos / 1_000)
    } else {
        write!(out, ".{nanos:09}")
    };
    out.push_str(offset);
    out
}

/// Parse an RFC 3339 instant with any offset.
pub fn parse_rfc3339(text: &str) -> Option<Timestamp> {
    text.parse().ok()
}

/// Serde for a [`Timestamp`] field, in the text [`rfc3339_z`] writes.
pub mod serde_rfc3339 {
    use super::Timestamp;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(ts: &Timestamp, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::rfc3339_z(*ts))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Timestamp, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What chrono 0.4.45 wrote for these instants: `to_rfc3339`, then serde.
    const CHRONO: [(i64, i32, &str, &str); 7] = [
        (
            1700000000,
            0,
            "2023-11-14T22:13:20+00:00",
            "2023-11-14T22:13:20Z",
        ),
        (
            1700000000,
            500000000,
            "2023-11-14T22:13:20.500+00:00",
            "2023-11-14T22:13:20.500Z",
        ),
        (
            1700000000,
            120000000,
            "2023-11-14T22:13:20.120+00:00",
            "2023-11-14T22:13:20.120Z",
        ),
        (
            1700000000,
            123000,
            "2023-11-14T22:13:20.000123+00:00",
            "2023-11-14T22:13:20.000123Z",
        ),
        (
            1700000000,
            1000,
            "2023-11-14T22:13:20.000001+00:00",
            "2023-11-14T22:13:20.000001Z",
        ),
        (
            1700000000,
            123456789,
            "2023-11-14T22:13:20.123456789+00:00",
            "2023-11-14T22:13:20.123456789Z",
        ),
        (
            1700000000,
            1,
            "2023-11-14T22:13:20.000000001+00:00",
            "2023-11-14T22:13:20.000000001Z",
        ),
    ];

    #[test]
    fn text_matches_what_chrono_wrote_and_reads_back() {
        for (secs, nanos, offset, zulu) in CHRONO {
            let ts = Timestamp::new(secs, nanos).unwrap();
            assert_eq!(rfc3339(ts), offset);
            assert_eq!(rfc3339_z(ts), zulu);
            assert_eq!(parse_rfc3339(offset), Some(ts));
            assert_eq!(parse_rfc3339(zulu), Some(ts));
        }
        assert_eq!(
            parse_rfc3339("2023-11-14T23:13:20+01:00"),
            Timestamp::new(1700000000, 0).ok()
        );
        assert_eq!(parse_rfc3339("not a time"), None);
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Event {
        #[serde(with = "serde_rfc3339")]
        ts: Timestamp,
    }

    #[test]
    fn serde_writes_chrono_text_and_reads_any_offset() {
        let (secs, nanos, _, zulu) = CHRONO[2];
        let event = Event {
            ts: Timestamp::new(secs, nanos).unwrap(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(json, format!("{{\"ts\":\"{zulu}\"}}"));
        assert_eq!(serde_json::from_str::<Event>(&json).unwrap(), event);
        let shifted = r#"{"ts":"2023-11-14T23:13:20.12+01:00"}"#;
        assert_eq!(serde_json::from_str::<Event>(shifted).unwrap(), event);
        assert!(serde_json::from_str::<Event>(r#"{"ts":"yesterday"}"#).is_err());
    }
}
