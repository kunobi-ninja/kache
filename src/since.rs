//! The `--since` lookback window shared by `kache stats`, `kache report`, and
//! `kache monitor` (kunobi-ninja/kache#897).
//!
//! The window used to be parsed straight to whole hours, so `--since 15m` did
//! not parse, fell back to the 24h default, and was then labelled "last 24h".
//! Keeping the parsed value here, in seconds plus the unit the user typed,
//! means every consumer filters by the same cutoff and prints the same label.

use chrono::{DateTime, Utc};

/// The unit a window was written in. Kept so the label says `15m` when the
/// user typed `15m` and `24h` when they typed `24h` (or a bare `24`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Secs,
    Mins,
    Hours,
    Days,
}

impl Unit {
    fn secs(self) -> u64 {
        match self {
            Unit::Secs => 1,
            Unit::Mins => 60,
            Unit::Hours => 3600,
            Unit::Days => 86_400,
        }
    }

    fn suffix(self) -> char {
        match self {
            Unit::Secs => 's',
            Unit::Mins => 'm',
            Unit::Hours => 'h',
            Unit::Days => 'd',
        }
    }
}

/// A lookback window: "events from the last N seconds".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SinceWindow {
    secs: u64,
    unit: Unit,
}

impl SinceWindow {
    /// The default window every windowed command used before #897 and still
    /// uses when `--since` is not given.
    pub(crate) const DEFAULT: SinceWindow = SinceWindow {
        secs: 24 * 3600,
        unit: Unit::Hours,
    };

    /// A window from a raw second count, as carried over the daemon protocol.
    /// The label unit is the largest one that divides the count evenly.
    pub(crate) fn from_secs(secs: u64) -> Self {
        let unit = [Unit::Days, Unit::Hours, Unit::Mins]
            .into_iter()
            .find(|unit| secs > 0 && secs.is_multiple_of(unit.secs()))
            .unwrap_or(Unit::Secs);
        SinceWindow { secs, unit }
    }

    /// A whole-hour window. `None` on overflow.
    pub(crate) fn from_hours(hours: u64) -> Option<Self> {
        Some(SinceWindow {
            secs: hours.checked_mul(3600)?,
            unit: Unit::Hours,
        })
    }

    /// Parse `15m`, `2h`, `7d`, `90s`, or a bare integer (hours, kept for
    /// compatibility with the pre-#897 flag). Whitespace around the value is
    /// ignored. `None` for anything else, including overflow and an empty
    /// number.
    pub(crate) fn parse(input: &str) -> Option<Self> {
        let input = input.trim();
        let (digits, unit) = match input.chars().last()? {
            's' => (&input[..input.len() - 1], Unit::Secs),
            'm' => (&input[..input.len() - 1], Unit::Mins),
            'h' => (&input[..input.len() - 1], Unit::Hours),
            'd' => (&input[..input.len() - 1], Unit::Days),
            // No unit suffix: the whole value is the count, in hours. A
            // trailing character that is not a digit is rejected by the
            // all-digits check below, so this arm needs no guard of its own.
            _ => (input, Unit::Hours),
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let count: u64 = digits.parse().ok()?;
        Some(SinceWindow {
            secs: count.checked_mul(unit.secs())?,
            unit,
        })
    }

    pub(crate) fn secs(self) -> u64 {
        self.secs
    }

    /// Whole hours, rounded down. `0` for a sub-hour window. Kept for the
    /// `since_hours` / `hours` fields older consumers read; `secs()` is the
    /// authoritative value.
    pub(crate) fn hours(self) -> u64 {
        self.secs / 3600
    }

    /// The window written in the unit it was requested in: `15m`, `2h`, `24h`.
    pub(crate) fn label(self) -> String {
        format!("{}{}", self.secs / self.unit.secs(), self.unit.suffix())
    }

    /// The instant events must be at or after to fall inside the window.
    /// Saturates at the earliest representable time rather than wrapping.
    pub(crate) fn cutoff(self, now: DateTime<Utc>) -> DateTime<Utc> {
        let secs = i64::try_from(self.secs).unwrap_or(i64::MAX);
        chrono::Duration::try_seconds(secs)
            .and_then(|d| now.checked_sub_signed(d))
            .unwrap_or(DateTime::<Utc>::MIN_UTC)
    }

    /// [`Self::cutoff`] as Unix seconds, for the transfer log whose timestamps
    /// are stored that way. Clamped at zero for a window older than the epoch.
    pub(crate) fn cutoff_unix_secs(self, now: DateTime<Utc>) -> u64 {
        u64::try_from(self.cutoff(now).timestamp()).unwrap_or(0)
    }
}

impl std::fmt::Display for SinceWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parses_every_unit() {
        assert_eq!(SinceWindow::parse("90s").unwrap().secs(), 90);
        assert_eq!(SinceWindow::parse("15m").unwrap().secs(), 900);
        assert_eq!(SinceWindow::parse("2h").unwrap().secs(), 7200);
        assert_eq!(SinceWindow::parse("7d").unwrap().secs(), 604_800);
    }

    #[test]
    fn bare_integer_means_hours() {
        let window = SinceWindow::parse("48").unwrap();
        assert_eq!(window.secs(), 172_800);
        assert_eq!(window.hours(), 48);
        assert_eq!(window.label(), "48h");
    }

    #[test]
    fn surrounding_whitespace_is_ignored() {
        assert_eq!(SinceWindow::parse(" 15m \n").unwrap().secs(), 900);
    }

    #[test]
    fn rejects_invalid_input() {
        for bad in [
            "", " ", "invalid", "m", "h", "15x", "1.5h", "-2h", "2 h", "15mm", "+3h",
        ] {
            assert!(SinceWindow::parse(bad).is_none(), "{bad:?} must not parse");
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(SinceWindow::parse("18446744073709551615d").is_none());
        assert!(SinceWindow::parse("18446744073709551615h").is_none());
        assert!(SinceWindow::parse("18446744073709551615m").is_none());
        assert!(SinceWindow::parse("18446744073709551616s").is_none());
        assert!(SinceWindow::from_hours(u64::MAX).is_none());
    }

    #[test]
    fn label_keeps_the_requested_unit() {
        assert_eq!(SinceWindow::parse("15m").unwrap().label(), "15m");
        assert_eq!(SinceWindow::parse("2h").unwrap().label(), "2h");
        assert_eq!(SinceWindow::parse("24h").unwrap().label(), "24h");
        assert_eq!(SinceWindow::parse("1d").unwrap().label(), "1d");
        assert_eq!(SinceWindow::parse("90s").unwrap().label(), "90s");
        assert_eq!(SinceWindow::DEFAULT.label(), "24h");
        assert_eq!(SinceWindow::DEFAULT.to_string(), "24h");
        assert_eq!(SinceWindow::from_hours(3).unwrap().label(), "3h");
    }

    #[test]
    fn from_secs_picks_the_largest_even_unit() {
        assert_eq!(SinceWindow::from_secs(900).label(), "15m");
        assert_eq!(SinceWindow::from_secs(7200).label(), "2h");
        assert_eq!(SinceWindow::from_secs(86_400).label(), "1d");
        assert_eq!(SinceWindow::from_secs(90).label(), "90s");
        assert_eq!(SinceWindow::from_secs(0).label(), "0s");
        assert_eq!(SinceWindow::from_secs(900).secs(), 900);
        assert_eq!(
            SinceWindow::from_secs(900),
            SinceWindow::parse("15m").unwrap()
        );
    }

    #[test]
    fn default_is_twenty_four_hours() {
        assert_eq!(SinceWindow::DEFAULT.secs(), 86_400);
        assert_eq!(SinceWindow::DEFAULT.hours(), 24);
        assert_eq!(SinceWindow::DEFAULT, SinceWindow::parse("24h").unwrap());
    }

    #[test]
    fn sub_hour_windows_floor_to_zero_hours() {
        assert_eq!(SinceWindow::parse("15m").unwrap().hours(), 0);
        assert_eq!(SinceWindow::parse("90m").unwrap().hours(), 1);
    }

    #[test]
    fn cutoff_subtracts_the_window() {
        let now = Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0).unwrap();
        let window = SinceWindow::parse("15m").unwrap();
        assert_eq!(
            window.cutoff(now),
            Utc.with_ymd_and_hms(2026, 9, 1, 11, 45, 0).unwrap()
        );
        assert_eq!(window.cutoff_unix_secs(now), now.timestamp() as u64 - 900);
    }

    #[test]
    fn cutoff_saturates_for_absurd_windows() {
        let now = Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0).unwrap();
        let window = SinceWindow::parse("18446744073709551615s").unwrap();
        assert_eq!(window.cutoff(now), DateTime::<Utc>::MIN_UTC);
        assert_eq!(window.cutoff_unix_secs(now), 0);
    }
}
