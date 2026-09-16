use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const SPARK_CATALYST_ATTR: &str = "spark.sql.catalyst.type";

const MAX_POSTSCRIPT: u64 = 512;

const CODEC_NONE: u64 = 0;

const CODEC_ZLIB: u64 = 1;

const CODEC_SNAPPY: u64 = 2;

const CODEC_LZO: u64 = 3;

const CODEC_LZ4: u64 = 4;

const CODEC_ZSTD: u64 = 5;

const FIELD_FOOTER_LENGTH: u32 = 1;

const FIELD_COMPRESSION: u32 = 2;

const FIELD_BLOCK_SIZE: u32 = 3;

const FIELD_TYPES: u32 = 4;

const FIELD_STRIPES: u32 = 3;

const FIELD_STRIPE_OFFSET: u32 = 1;

const FIELD_STRIPE_INDEX: u32 = 2;

const FIELD_STRIPE_DATA: u32 = 3;

const FIELD_STRIPE_FOOTER: u32 = 4;

const FIELD_WRITER_TZ: u32 = 3;

const FIELD_ATTRIBUTES: u32 = 7;

const FIELD_ATTR_KEY: u32 = 1;

const FIELD_ATTR_VALUE: u32 = 2;

const DEFAULT_BLOCK_SIZE: usize = 262_144;

pub(crate) fn spark_catalyst_types(path: &Path) -> HashMap<usize, String> {
    read_catalyst_types(path).unwrap_or_default()
}

pub(crate) fn file_writer_tz(path: &Path) -> Option<String> {
    read_writer_tz(path).unwrap_or(None)
}

fn read_writer_tz(path: &Path) -> Result<Option<String>, String> {
    let mut file = File::open(path).map_err(|error| format!("open: {error}"))?;
    let (footer, codec, cap) = read_footer_section(&mut file)?;
    let top = parse_fields(&footer)?;
    let mut zones: Vec<String> = Vec::new();
    for (_, span) in top.delimited(FIELD_STRIPES) {
        let stripe = parse_fields(span)?;
        let offset = stripe
            .varint(FIELD_STRIPE_OFFSET)
            .ok_or("no stripe offset")?;
        let index_length = stripe.varint(FIELD_STRIPE_INDEX).unwrap_or(0);
        let data_length = stripe.varint(FIELD_STRIPE_DATA).unwrap_or(0);
        let footer_length = stripe.varint(FIELD_STRIPE_FOOTER).unwrap_or(0);
        file.seek(SeekFrom::Start(offset + data_length + index_length))
            .map_err(|error| format!("seek stripe: {error}"))?;
        let footer_bytes =
            usize::try_from(footer_length).map_err(|error| format!("long footer: {error}"))?;
        let mut raw = vec![0; footer_bytes];
        file.read_exact(&mut raw)
            .map_err(|error| format!("read stripe: {error}"))?;
        let decoded = expand_blocks(&raw, codec, cap)?;
        let footer = parse_fields(&decoded)?;
        if let Some(zone) = footer.string(FIELD_WRITER_TZ)
            && !zones.contains(&zone)
        {
            zones.push(zone);
        }
    }
    if zones.len() == 1 {
        return Ok(Some(zones.remove(0)));
    }
    Ok(None)
}

fn read_footer_section(file: &mut File) -> Result<(Vec<u8>, u64, usize), String> {
    let length = file
        .metadata()
        .map_err(|error| format!("stat: {error}"))?
        .len();
    let tail_length = length.min(MAX_POSTSCRIPT);
    let tail_back = i64::try_from(tail_length).map_err(|error| format!("long tail: {error}"))?;
    file.seek(SeekFrom::End(-tail_back))
        .map_err(|error| format!("seek tail: {error}"))?;
    let mut tail = vec![0; tail_length as usize];
    file.read_exact(&mut tail)
        .map_err(|error| format!("read tail: {error}"))?;
    let script_length = tail[tail.len() - 1] as usize;
    let script_end = tail.len() - 1;
    let script_start = script_end
        .checked_sub(script_length)
        .ok_or_else(|| "short postscript".to_string())?;
    let script = parse_fields(&tail[script_start..script_end])?;
    let footer_length = script
        .varint(FIELD_FOOTER_LENGTH)
        .ok_or_else(|| "no footer length".to_string())?;
    let codec = script.varint(FIELD_COMPRESSION).unwrap_or(CODEC_NONE);
    let block_cap = usize::try_from(script.varint(FIELD_BLOCK_SIZE).unwrap_or(0))
        .map_err(|error| format!("wide block size: {error}"))?;
    let footer_cap = if block_cap == 0 {
        DEFAULT_BLOCK_SIZE
    } else {
        block_cap
    };
    let footer_start = length
        .checked_sub(1 + script_length as u64 + footer_length)
        .ok_or_else(|| "short file".to_string())?;
    file.seek(SeekFrom::Start(footer_start))
        .map_err(|error| format!("seek footer: {error}"))?;
    let footer_bytes =
        usize::try_from(footer_length).map_err(|error| format!("long footer: {error}"))?;
    let mut raw = vec![0; footer_bytes];
    file.read_exact(&mut raw)
        .map_err(|error| format!("read footer: {error}"))?;
    let footer = expand_blocks(&raw, codec, footer_cap)?;
    Ok((footer, codec, footer_cap))
}

fn read_catalyst_types(path: &Path) -> Result<HashMap<usize, String>, String> {
    let mut file = File::open(path).map_err(|error| format!("open: {error}"))?;
    let (footer, _, _) = read_footer_section(&mut file)?;
    let top = parse_fields(&footer)?;
    let mut out: HashMap<usize, String> = HashMap::new();
    for (position, span) in top.delimited(FIELD_TYPES) {
        let ty = parse_fields(span)?;
        for (_, attr_span) in ty.delimited(FIELD_ATTRIBUTES) {
            let attr = parse_fields(attr_span)?;
            if attr.string(FIELD_ATTR_KEY).as_deref() == Some(SPARK_CATALYST_ATTR)
                && let Some(value) = attr.string(FIELD_ATTR_VALUE)
            {
                out.insert(position, value);
            }
        }
    }
    Ok(out)
}

fn expand_blocks(raw: &[u8], codec: u64, cap: usize) -> Result<Vec<u8>, String> {
    let mut out: Vec<u8> = Vec::new();
    let mut at = 0usize;
    while at < raw.len() {
        let header = raw
            .get(at..at + 3)
            .ok_or_else(|| "short block header".to_string())?;
        let word = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        at += 3;
        let length = (word >> 1) as usize;
        let chunk = raw
            .get(at..at + length)
            .ok_or_else(|| "short block body".to_string())?;
        at += length;
        if word & 1 == 1 {
            out.extend_from_slice(chunk);
            continue;
        }
        match codec {
            CODEC_NONE => return Err("compressed block without a codec".to_string()),
            CODEC_ZLIB => {
                let mut decoder = flate2::read::DeflateDecoder::new(chunk);
                decoder
                    .read_to_end(&mut out)
                    .map_err(|error| format!("zlib footer: {error}"))?;
            }
            CODEC_SNAPPY => {
                let size = snap::raw::decompress_len(chunk)
                    .map_err(|error| format!("snappy footer: {error}"))?;
                let start = out.len();
                out.resize(start + size, 0);
                snap::raw::Decoder::new()
                    .decompress(chunk, &mut out[start..])
                    .map_err(|error| format!("snappy footer: {error}"))?;
            }
            CODEC_LZO => {
                let decoded = lzokay_native::decompress_all(chunk, None)
                    .map_err(|error| format!("lzo footer: {error}"))?;
                out.extend_from_slice(&decoded);
            }
            CODEC_LZ4 => {
                let decoded = lz4_flex::block::decompress(chunk, cap)
                    .map_err(|error| format!("lz4 footer: {error}"))?;
                out.extend_from_slice(&decoded);
            }
            CODEC_ZSTD => {
                let mut decoder =
                    zstd::Decoder::new(chunk).map_err(|error| format!("zstd footer: {error}"))?;
                decoder
                    .read_to_end(&mut out)
                    .map_err(|error| format!("zstd footer: {error}"))?;
            }
            other => return Err(format!("unsupported footer codec {other}")),
        }
    }
    Ok(out)
}

#[derive(Debug, Default)]
struct ProtoFields<'a> {
    vars: Vec<(u32, u64)>,
    blobs: Vec<(u32, &'a [u8])>,
}

impl<'a> ProtoFields<'a> {
    fn varint(&self, field: u32) -> Option<u64> {
        self.vars
            .iter()
            .find_map(|(id, value)| (*id == field).then_some(*value))
    }

    fn delimited(&self, field: u32) -> Vec<(usize, &'a [u8])> {
        let mut position = 0usize;
        let mut out = Vec::new();
        for (id, span) in &self.blobs {
            if *id == field {
                out.push((position, *span));
                position += 1;
            }
        }
        out
    }

    fn string(&self, field: u32) -> Option<String> {
        self.blobs
            .iter()
            .find_map(|(id, span)| (*id == field).then(|| String::from_utf8(span.to_vec()).ok())?)
    }
}

fn read_varint(bytes: &[u8], mut at: usize) -> Result<(u64, usize), String> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(at).ok_or_else(|| "short varint".to_string())?;
        at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, at));
        }
        shift += 7;
        if shift > 63 {
            return Err("wide varint".to_string());
        }
    }
}

fn parse_fields(bytes: &[u8]) -> Result<ProtoFields<'_>, String> {
    let mut fields = ProtoFields::default();
    let mut at = 0usize;
    while at < bytes.len() {
        let (tag, next) = read_varint(bytes, at)?;
        at = next;
        let id = u32::try_from(tag >> 3).map_err(|error| format!("wide tag: {error}"))?;
        let wire = (tag & 7) as u8;
        match wire {
            0 => {
                let (value, next) = read_varint(bytes, at)?;
                at = next;
                fields.vars.push((id, value));
            }
            1 => {
                at = at
                    .checked_add(8)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| "short fixed64".to_string())?;
            }
            2 => {
                let (length, next) = read_varint(bytes, at)?;
                at = next;
                let span =
                    usize::try_from(length).map_err(|error| format!("wide blob: {error}"))?;
                let end = at
                    .checked_add(span)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| "short blob".to_string())?;
                fields.blobs.push((id, &bytes[at..end]));
                at = end;
            }
            5 => {
                at = at
                    .checked_add(4)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| "short fixed32".to_string())?;
            }
            other => return Err(format!("bad wire type {other}")),
        }
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tagged_varint(field: u32, value: u64) -> Vec<u8> {
        let mut out = encode_varint((field << 3) as u64);
        out.extend(encode_varint(value));
        out
    }

    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    #[test]
    fn footer_varint_round_trip() {
        let bytes = tagged_varint(1, 618);
        let fields = parse_fields(&bytes).unwrap();
        assert_eq!(fields.varint(1), Some(618));
        assert_eq!(fields.varint(2), None);
    }

    #[test]
    fn footer_blob_and_skip_arms() {
        let mut bytes = tagged_varint(12, 7);
        bytes.push((4 << 3 | 2) as u8);
        bytes.extend(encode_varint(3));
        bytes.extend_from_slice(b"abc");
        bytes.push((6 << 3 | 1) as u8);
        bytes.extend_from_slice(&[9; 8]);
        bytes.push((7 << 3 | 5) as u8);
        bytes.extend_from_slice(&[8; 4]);
        let fields = parse_fields(&bytes).unwrap();
        assert_eq!(fields.varint(12), Some(7));
        assert_eq!(fields.string(4), Some("abc".to_string()));
    }

    #[test]
    fn footer_short_inputs_refuse() {
        assert!(parse_fields(&[0x80]).is_err());
        assert!(read_varint(&[0x80], 0).is_err());
        assert!(expand_blocks(&[1, 2], CODEC_NONE, 8).is_err());
        assert!(expand_blocks(&[2, 0, 0, 9], CODEC_NONE, 8).is_err());
    }

    #[test]
    fn footer_stored_block_passes_through() {
        let mut raw = vec![7, 0, 0];
        raw.extend_from_slice(b"abc");
        assert_eq!(expand_blocks(&raw, CODEC_ZSTD, 8).unwrap(), b"abc");
    }
}
