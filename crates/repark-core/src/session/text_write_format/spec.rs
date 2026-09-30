use std::collections::HashMap;

use datafusion::common::{DataFusionError, Result};

use super::PatternKind;
use super::udf::{FormatSpecs, spec_from_value};
use crate::session_time_zone::{canonical_session_zone_id, java_display_zone_id};

pub(crate) const SPEC_ZONE_KEY: &str = "repark.text.zone";
pub(crate) const SPEC_TIMESTAMP_FORMAT_KEY: &str = "repark.text.timestamp_format_hex";
pub(crate) const SPEC_NTZ_FORMAT_KEY: &str = "repark.text.timestamp_ntz_format_hex";
pub(crate) const SPEC_DATE_FORMAT_KEY: &str = "repark.text.date_format_hex";
const SPEC_PREFIX: &str = "repark.text.";

#[derive(Debug)]
pub(crate) struct TextWriteSpec {
    pub zone_raw: String,
    pub canonical: String,
    pub display: String,
    pub specs: FormatSpecs,
}

impl TextWriteSpec {
    pub(crate) fn from_parts(
        zone_raw: &str,
        timestamp: Option<&str>,
        ntz: Option<&str>,
        date: Option<&str>,
    ) -> Result<Self> {
        Ok(Self {
            zone_raw: zone_raw.to_string(),
            canonical: canonical_session_zone_id(zone_raw).clone(),
            display: java_display_zone_id(zone_raw).clone(),
            specs: FormatSpecs {
                timestamp: spec_from_value(timestamp, PatternKind::Timestamp)?,
                ntz: spec_from_value(ntz, PatternKind::TimestampNtz)?,
                date: spec_from_value(date, PatternKind::Date)?,
            },
        })
    }

    pub(crate) fn from_format_options(
        format_options: &HashMap<String, String>,
    ) -> Result<(Self, HashMap<String, String>)> {
        let zone_raw = format_options.get(SPEC_ZONE_KEY).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "text timestamp write is missing its '{SPEC_ZONE_KEY}' option"
            ))
        })?;
        let timestamp = Self::decode_option(format_options, SPEC_TIMESTAMP_FORMAT_KEY)?;
        let ntz = Self::decode_option(format_options, SPEC_NTZ_FORMAT_KEY)?;
        let date = Self::decode_option(format_options, SPEC_DATE_FORMAT_KEY)?;
        let spec = Self::from_parts(
            zone_raw,
            timestamp.as_deref(),
            ntz.as_deref(),
            date.as_deref(),
        )?;
        let rest = format_options
            .iter()
            .filter(|(key, _)| !key.starts_with(SPEC_PREFIX))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok((spec, rest))
    }

    fn decode_option(
        format_options: &HashMap<String, String>,
        key: &str,
    ) -> Result<Option<String>> {
        let Some(hex) = format_options.get(key) else {
            return Ok(None);
        };
        Self::hex_decode(hex).map(Some).map_err(|()| {
            DataFusionError::Execution(format!(
                "text timestamp write option '{key}' is not valid hex"
            ))
        })
    }

    fn hex_value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    fn hex_decode(hex: &str) -> std::result::Result<String, ()> {
        let bytes = hex.as_bytes();
        if !bytes.len().is_multiple_of(2) {
            return Err(());
        }
        let mut raw = Vec::with_capacity(bytes.len() / 2);
        for pair in bytes.chunks(2) {
            let (Some(high), Some(low)) = (Self::hex_value(pair[0]), Self::hex_value(pair[1]))
            else {
                return Err(());
            };
            raw.push(high * 16 + low);
        }
        String::from_utf8(raw).map_err(|_| ())
    }

    fn hex_encode(text: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(text.len() * 2);
        for byte in text.bytes() {
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 15)] as char);
        }
        out
    }

    pub(crate) fn options_sql(
        zone_id: &str,
        timestamp: Option<&str>,
        ntz: Option<&str>,
        date: Option<&str>,
    ) -> String {
        let mut pairs = vec![format!(
            "'{SPEC_ZONE_KEY}' '{}'",
            zone_id.replace('\'', "''")
        )];
        for (key, pattern) in [
            (SPEC_TIMESTAMP_FORMAT_KEY, timestamp),
            (SPEC_NTZ_FORMAT_KEY, ntz),
            (SPEC_DATE_FORMAT_KEY, date),
        ] {
            if let Some(pattern) = pattern {
                pairs.push(format!("'{key}' '{}'", Self::hex_encode(pattern)));
            }
        }
        pairs.join(", ")
    }
}
