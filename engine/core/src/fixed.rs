use crate::{Error, Result};
use serde_json::Value;
use std::{borrow::Cow, fmt, str::FromStr};

/// Signed price or quantity, in units of 10^-8. Parsing never rounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fixed(pub i64);

impl FromStr for Fixed {
    type Err = Error;
    fn from_str(text: &str) -> Result<Self> {
        let plain = if text.contains(['e', 'E']) {
            Cow::Owned(expand(text)?)
        } else {
            Cow::Borrowed(text)
        };
        let (negative, unsigned) = match plain.strip_prefix('-') {
            Some(s) => (true, s),
            None => (false, plain.as_ref()),
        };
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if fraction.len() > 8 {
            return Err(Error(format!("more than 8 decimals: {text}")));
        }
        if whole.is_empty()
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
            || unsigned.ends_with('.')
        {
            return Err(Error("invalid decimal".into()));
        }
        let scale = 10i128.pow(8 - fraction.len() as u32);
        let whole: i128 = whole
            .parse()
            .map_err(|_| Error("decimal overflow".into()))?;
        let fraction: i128 = if fraction.is_empty() {
            0
        } else {
            fraction
                .parse()
                .map_err(|_| Error("invalid fraction".into()))?
        };
        let atoms = whole
            .checked_mul(100_000_000)
            .and_then(|n| n.checked_add(fraction * scale))
            .ok_or_else(|| Error("decimal overflow".into()))?;
        let atoms = if negative { -atoms } else { atoms };
        Ok(Self(
            i64::try_from(atoms).map_err(|_| Error("decimal overflow".into()))?,
        ))
    }
}

impl fmt::Display for Fixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = i128::from(self.0);
        write!(
            f,
            "{}{}.{:08}",
            if n < 0 { "-" } else { "" },
            n.abs() / 100_000_000,
            n.abs() % 100_000_000
        )
    }
}

/// Midpoint in 10^-9 units: the extra decimal represents half a Fixed atom exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mid(pub i128);

/// Expand scientific notation while preserving decimal scale, like Decimal's format(value, "f").
pub(crate) fn expand(text: &str) -> Result<String> {
    let (negative, text) = if let Some(s) = text.strip_prefix('-') {
        (true, s)
    } else {
        (false, text)
    };
    let (mantissa, exponent) = if let Some(i) = text.find(['e', 'E']) {
        (
            &text[..i],
            text[i + 1..]
                .parse::<i32>()
                .map_err(|_| Error("invalid exponent".into()))?,
        )
    } else {
        (text, 0)
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || mantissa.ends_with('.')
    {
        return Err(Error("invalid decimal".into()));
    }
    // Bound allocations for malformed inputs; all accepted fixed-point values fit this bound.
    let point = whole.len() as i64 + i64::from(exponent);
    let digits = format!("{whole}{fraction}");
    if !(-128..=128).contains(&point) || digits.len() > 256 {
        return Err(Error("decimal overflow or excessive precision".into()));
    }
    let plain = if point <= 0 {
        format!("0.{}{}", "0".repeat((-point) as usize), digits)
    } else if point as usize >= digits.len() {
        format!("{}{}", digits, "0".repeat(point as usize - digits.len()))
    } else {
        format!(
            "{}.{}",
            &digits[..point as usize],
            &digits[point as usize..]
        )
    };
    Ok(if negative { format!("-{plain}") } else { plain })
}

pub(crate) fn decimal(value: &Value) -> Result<String> {
    match value {
        Value::String(s) => {
            s.parse::<Fixed>()?;
            Ok(s.clone())
        }
        Value::Number(n) => {
            let s = n.to_string();
            s.parse::<Fixed>()?;
            Ok(s)
        }
        _ => Err(Error("expected decimal string or JSON number".into())),
    }
}

pub(crate) fn kraken_fmt(text: &str) -> String {
    // Text was already validated by Fixed parsing.
    let plain = if text.contains(['e', 'E']) {
        Cow::Owned(expand(text).expect("validated decimal"))
    } else {
        Cow::Borrowed(text)
    };
    plain.replace('.', "").trim_start_matches('0').to_owned()
}
