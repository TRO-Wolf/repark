use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::catalog::Session;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::common::{DataFusionError, Result};
use object_store::path::Path as ObjectPath;

use super::PatternKind;
use super::udf::{FormatSpecs, spec_from_value};
use crate::session_time_zone::{canonical_session_zone_id, java_display_zone_id};

pub(crate) const SPEC_ZONE_KEY: &str = "repark.text.zone";
pub(crate) const SPEC_TIMESTAMP_FORMAT_KEY: &str = "repark.text.timestamp_format_hex";
pub(crate) const SPEC_NTZ_FORMAT_KEY: &str = "repark.text.timestamp_ntz_format_hex";
pub(crate) const SPEC_DATE_FORMAT_KEY: &str = "repark.text.date_format_hex";
pub(crate) const SPEC_WRITE_ID_KEY: &str = "repark.text.write_id";
pub(crate) const SPEC_DISPLAY_HEADER_KEY: &str = "repark.text.display_header";
const SPEC_PREFIX: &str = "repark.text.";

type TextWriteCollectors = Arc<Mutex<HashMap<String, Arc<Mutex<Vec<ObjectPath>>>>>>;

#[derive(Debug, Default)]
pub(crate) struct TextWritePathRegistry {
    paths: TextWriteCollectors,
}

impl TextWritePathRegistry {
    pub(crate) fn register(&self, write_id: &str) -> Arc<Mutex<Vec<ObjectPath>>> {
        let collector = Arc::new(Mutex::new(Vec::new()));
        if let Ok(mut guard) = self.paths.lock() {
            guard.insert(write_id.to_string(), Arc::clone(&collector));
        }
        collector
    }

    pub(crate) fn handle(&self, write_id: &str) -> Option<Arc<Mutex<Vec<ObjectPath>>>> {
        self.paths
            .lock()
            .ok()
            .and_then(|guard| guard.get(write_id).cloned())
    }

    pub(crate) fn take(&self, write_id: &str) -> Vec<ObjectPath> {
        let removed = self
            .paths
            .lock()
            .ok()
            .and_then(|mut guard| guard.remove(write_id));
        match removed {
            Some(collector) => {
                let mut guard = collector
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                std::mem::take(&mut *guard)
            }
            None => Vec::new(),
        }
    }
}

impl ConfigExtension for TextWritePathRegistry {
    const PREFIX: &'static str = "repark.textwrite";
}

impl ExtensionOptions for TextWritePathRegistry {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(Self {
            paths: Arc::clone(&self.paths),
        })
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the text write registry is owned by the \
             running s3a write and never set by configuration",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

pub(crate) fn collector_for_write(
    state: &dyn Session,
    write_id: Option<&str>,
) -> Arc<Mutex<Vec<ObjectPath>>> {
    if let Some(id) = write_id
        && let Some(registry) = state
            .config_options()
            .extensions
            .get::<TextWritePathRegistry>()
        && let Some(collector) = registry.handle(id)
    {
        collector
    } else {
        Arc::new(Mutex::new(Vec::new()))
    }
}

#[derive(Debug)]
pub(crate) struct TextWriteSpec {
    pub zone_raw: String,
    pub canonical: String,
    pub display: String,
    pub specs: FormatSpecs,
    pub write_id: Option<String>,
    pub display_header: bool,
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
            write_id: None,
            display_header: false,
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
        let mut spec = Self::from_parts(
            zone_raw,
            timestamp.as_deref(),
            ntz.as_deref(),
            date.as_deref(),
        )?;
        spec.write_id = format_options.get(SPEC_WRITE_ID_KEY).cloned();
        spec.display_header = format_options
            .get(SPEC_DISPLAY_HEADER_KEY)
            .is_some_and(|value| value == "true");
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
        display_header: bool,
    ) -> String {
        let mut pairs = vec![format!(
            "'{SPEC_ZONE_KEY}' '{}'",
            zone_id.replace('\'', "''")
        )];
        if display_header {
            pairs.push(format!("'{SPEC_DISPLAY_HEADER_KEY}' 'true'"));
        }
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

#[cfg(test)]
mod spec_tests {
    use super::*;

    #[test]
    fn spec_write_id_parses_and_strips_from_rest() {
        let mut format_options = HashMap::new();
        format_options.insert("repark.text.zone".to_string(), "UTC".to_string());
        format_options.insert("repark.text.write_id".to_string(), "write-1".to_string());
        format_options.insert("format.has_header".to_string(), "false".to_string());
        let (spec, rest) =
            TextWriteSpec::from_format_options(&format_options).expect("spec builds");
        assert_eq!(spec.write_id.as_deref(), Some("write-1"));
        assert_eq!(
            rest,
            HashMap::from([("format.has_header".to_string(), "false".to_string())])
        );
        let plain = HashMap::from([("repark.text.zone".to_string(), "UTC".to_string())]);
        let (spec, _) = TextWriteSpec::from_format_options(&plain).expect("spec builds");
        assert_eq!(spec.write_id, None);
    }

    #[test]
    fn write_path_registry_holds_one_collector_per_write() {
        let registry = TextWritePathRegistry::default();
        assert!(registry.handle("missing").is_none());
        assert!(registry.take("missing").is_empty());
        let first = registry.register("write-1");
        let second = registry.register("write-2");
        first
            .lock()
            .unwrap()
            .push(ObjectPath::from("prefix/first.json"));
        assert_eq!(registry.take("write-1").len(), 1);
        assert!(registry.take("write-1").is_empty());
        assert!(registry.handle("write-2").is_some());
        assert!(second.lock().unwrap().is_empty());
        assert!(registry.take("write-2").is_empty());
    }
}
