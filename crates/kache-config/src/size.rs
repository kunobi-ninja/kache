//! Byte-valued settings accept integer bytes or a string with a size unit.

use serde::{Deserialize, Deserializer};

/// Parse sizes exactly, rejecting overflow instead of saturating a budget.
pub fn parse_size(value: &str) -> Option<u64> {
    let value = value.trim();
    if let Ok(bytes) = value.parse::<u64>() {
        return Some(bytes);
    }
    let end = value
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(end);
    if number.is_empty() || number == "." {
        return None;
    }
    let unit = suffix.trim().parse::<bytesize::Unit>().ok()?;
    let factor = u128::from(1_u64 * unit);
    let bytes = if let Some((whole, fraction)) = number.split_once('.') {
        let whole = if whole.is_empty() {
            0
        } else {
            whole.parse::<u128>().ok()?
        };
        let fractional = if fraction.is_empty() {
            0
        } else {
            fraction.parse::<u128>().ok()?
        };
        let scale = 10_u128.checked_pow(fraction.len().try_into().ok()?)?;
        whole
            .checked_mul(factor)?
            .checked_add(fractional.checked_mul(factor)? / scale)?
    } else {
        number.parse::<u128>().ok()?.checked_mul(factor)?
    };
    bytes.try_into().ok()
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SizeInput {
    Bytes(u64),
    Text(String),
}

pub(crate) fn optional_text<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(
        Option::<SizeInput>::deserialize(deserializer)?.map(|value| match value {
            SizeInput::Bytes(bytes) => bytes.to_string(),
            SizeInput::Text(text) => text,
        }),
    )
}

pub(crate) fn optional_bytes<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<SizeInput>::deserialize(deserializer)? {
        Some(SizeInput::Bytes(bytes)) => Ok(Some(bytes)),
        Some(SizeInput::Text(text)) => parse_size(&text).map(Some).ok_or_else(|| {
            serde::de::Error::custom("expected byte count or size such as 10GiB or 500MB")
        }),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_distinguish_decimal_and_binary_and_preserve_exact_bytes() {
        for (text, bytes) in [
            ("1GB", 1_000_000_000),
            ("1GiB", 1_073_741_824),
            (" 1.5 GiB ", 1_610_612_736),
            ("500mb", 500_000_000),
            (".5MiB", 524_288),
            ("1.KiB", 1024),
            ("64K", 64_000),
            ("1024", 1024),
            ("0B", 0),
            ("9007199254740993B", 9_007_199_254_740_993),
            ("18446744073709551615B", u64::MAX),
        ] {
            assert_eq!(parse_size(text), Some(bytes), "{text}");
        }
        for text in [
            "",
            "-1GiB",
            "1_000",
            "1.2.3GiB",
            ".GiB",
            "NaNGB",
            "2frogs",
            "18446744073709551616",
            "18446744073709551616B",
            "16EiB",
            "99999999999999999999999999999999999999999999999999GB",
        ] {
            assert_eq!(parse_size(text), None, "{text:?}");
        }
    }
}
