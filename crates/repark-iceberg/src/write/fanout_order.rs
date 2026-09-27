use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;
use iceberg::spec::{DataFile, Literal, PrimitiveLiteral, Struct};
use iceberg_datafusion::DataFileCommitOrder;

const SHUFFLE_PARTITIONS_KEY: &str = "spark.sql.shuffle.partitions";
const DEFAULT_SHUFFLE_PARTITIONS: u32 = 200;
const FIRST_CAPACITY: usize = 16;
const TREEIFY_KEYS: usize = 8;

fn java_field_hash(literal: &Literal) -> Option<i32> {
    match literal {
        Literal::Primitive(PrimitiveLiteral::Int(value)) => Some(*value),
        Literal::Primitive(PrimitiveLiteral::Long(value)) => {
            let bits = u64::from_ne_bytes(value.to_ne_bytes());
            let low = u32::try_from(bits & 0xffff_ffff).ok()?;
            let high = u32::try_from((bits >> 32) & 0xffff_ffff).ok()?;
            Some(i32::from_ne_bytes((low ^ high).to_ne_bytes()))
        }
        Literal::Primitive(PrimitiveLiteral::String(value)) => {
            let mut hash = 177i32;
            for unit in value.encode_utf16() {
                hash = hash.wrapping_mul(31).wrapping_add(i32::from(unit));
            }
            Some(hash)
        }
        Literal::Primitive(PrimitiveLiteral::Boolean(value)) => {
            Some(if *value { 1231 } else { 1237 })
        }
        Literal::Primitive(
            PrimitiveLiteral::Float(_)
            | PrimitiveLiteral::Double(_)
            | PrimitiveLiteral::Binary(_)
            | PrimitiveLiteral::Int128(_)
            | PrimitiveLiteral::UInt128(_)
            | PrimitiveLiteral::AboveMax
            | PrimitiveLiteral::BelowMin,
        )
        | Literal::Struct(_)
        | Literal::List(_)
        | Literal::Map(_) => None,
    }
}

fn java_struct_hash(partition: &Struct) -> Option<i32> {
    let mut hash = 97i32
        .wrapping_mul(41)
        .wrapping_add(i32::try_from(partition.fields().len()).ok()?);
    for field in partition.iter() {
        let field_hash = match field {
            None => 0,
            Some(literal) => java_field_hash(literal)?,
        };
        hash = hash.wrapping_mul(41).wrapping_add(field_hash);
    }
    Some(hash)
}

fn mix_key(key: u32) -> u32 {
    key.wrapping_mul(0xcc9e_2d51)
        .rotate_left(15)
        .wrapping_mul(0x1b87_3593)
}

fn mix_hash(hash: u32, key: u32) -> u32 {
    (hash ^ key)
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe654_6b64)
}

fn finish_mix(hash: u32, length: u32) -> u32 {
    let mut mixed = hash ^ length;
    mixed ^= mixed >> 16;
    mixed = mixed.wrapping_mul(0x85eb_ca6b);
    mixed ^= mixed >> 13;
    mixed = mixed.wrapping_mul(0xc2b2_ae35);
    mixed ^= mixed >> 16;
    mixed
}

fn hash_int(value: u32, seed: u32) -> u32 {
    finish_mix(mix_hash(seed, mix_key(value)), 4)
}

fn hash_long(value: i64, seed: u32) -> Option<u32> {
    let bits = u64::from_ne_bytes(value.to_ne_bytes());
    let low = u32::try_from(bits & 0xffff_ffff).ok()?;
    let high = u32::try_from((bits >> 32) & 0xffff_ffff).ok()?;
    Some(finish_mix(
        mix_hash(mix_hash(seed, mix_key(low)), mix_key(high)),
        8,
    ))
}

fn hash_bytes(bytes: &[u8], seed: u32) -> Option<u32> {
    let mut hash = seed;
    let chunks = bytes.chunks_exact(4);
    let tail = chunks.remainder();
    for chunk in chunks {
        let word = <[u8; 4]>::try_from(chunk).ok()?;
        hash = mix_hash(hash, mix_key(u32::from_le_bytes(word)));
    }
    for byte in tail {
        let signed = i32::from(i8::from_ne_bytes([*byte]));
        hash = mix_hash(hash, mix_key(u32::from_ne_bytes(signed.to_ne_bytes())));
    }
    Some(finish_mix(hash, u32::try_from(bytes.len()).ok()?))
}

fn spark_murmur3(partition: &Struct) -> Option<i32> {
    let mut hash = 42u32;
    for field in partition.iter() {
        let Some(literal) = field else {
            continue;
        };
        hash = match literal {
            Literal::Primitive(PrimitiveLiteral::Int(value)) => {
                hash_int(u32::from_ne_bytes(value.to_ne_bytes()), hash)
            }
            Literal::Primitive(PrimitiveLiteral::Long(value)) => hash_long(*value, hash)?,
            Literal::Primitive(PrimitiveLiteral::String(value)) => {
                hash_bytes(value.as_bytes(), hash)?
            }
            Literal::Primitive(PrimitiveLiteral::Boolean(value)) => {
                hash_int(u32::from(*value), hash)
            }
            Literal::Primitive(
                PrimitiveLiteral::Float(_)
                | PrimitiveLiteral::Double(_)
                | PrimitiveLiteral::Binary(_)
                | PrimitiveLiteral::Int128(_)
                | PrimitiveLiteral::UInt128(_)
                | PrimitiveLiteral::AboveMax
                | PrimitiveLiteral::BelowMin,
            )
            | Literal::Struct(_)
            | Literal::List(_)
            | Literal::Map(_) => return None,
        };
    }
    Some(i32::from_ne_bytes(hash.to_ne_bytes()))
}

#[must_use]
pub fn spark_fanout_commit_order(files: Vec<DataFile>, shuffle_partitions: u32) -> Vec<DataFile> {
    if shuffle_partitions == 0 {
        return files;
    }
    let mut index_of: HashMap<Struct, usize> = HashMap::new();
    let mut keys: Vec<Struct> = Vec::new();
    let mut key_of_file: Vec<usize> = Vec::with_capacity(files.len());
    for file in &files {
        if file.sort_order_id().is_some_and(|id| id != 0) {
            return files;
        }
        let partition = file.partition().clone();
        let key = *index_of.entry(partition.clone()).or_insert_with(|| {
            keys.push(partition);
            keys.len() - 1
        });
        key_of_file.push(key);
    }
    if keys.len() < 2 {
        return files;
    }
    let mut capacity = FIRST_CAPACITY;
    while keys.len() > capacity * 3 / 4 {
        capacity *= 2;
    }
    let Ok(mask) = u32::try_from(capacity - 1) else {
        return files;
    };
    let mut specs: Vec<(u32, i64)> = Vec::with_capacity(keys.len());
    let mut bucket_counts: HashMap<u32, usize> = HashMap::new();
    for key in &keys {
        let Some(hash) = java_struct_hash(key) else {
            return files;
        };
        let bits = u32::from_ne_bytes(hash.to_ne_bytes());
        let bucket = (bits ^ (bits >> 16)) & mask;
        let Some(murmur) = spark_murmur3(key) else {
            return files;
        };
        let reducer = i64::from(murmur).rem_euclid(i64::from(shuffle_partitions));
        let count = bucket_counts.entry(bucket).or_insert(0);
        *count += 1;
        if *count >= TREEIFY_KEYS {
            return files;
        }
        specs.push((bucket, reducer));
    }
    let mut keyed: Vec<(u32, i64, usize)> = specs
        .into_iter()
        .enumerate()
        .map(|(key, (bucket, reducer))| (bucket, reducer, key))
        .collect();
    keyed.sort_by_key(|keyed| (keyed.0, keyed.1));
    let mut position_of: HashMap<usize, usize> = HashMap::with_capacity(keyed.len());
    for (position, (_, _, key)) in keyed.iter().enumerate() {
        position_of.insert(*key, position);
    }
    let mut ranks: Vec<usize> = Vec::with_capacity(files.len());
    for key in &key_of_file {
        let Some(position) = position_of.get(key).copied() else {
            return files;
        };
        ranks.push(position);
    }
    if ranks.len() != files.len() {
        return files;
    }
    let mut ranked: Vec<(usize, DataFile)> = files
        .into_iter()
        .zip(ranks)
        .map(|(file, rank)| (rank, file))
        .collect();
    ranked.sort_by_key(|ranked| ranked.0);
    ranked.into_iter().map(|(_, file)| file).collect()
}

#[allow(clippy::missing_errors_doc, clippy::implicit_hasher)]
pub fn shuffle_partitions_from_config_map(
    conf: &HashMap<String, String>,
) -> datafusion::error::Result<u32> {
    let Some(raw) = conf.get(SHUFFLE_PARTITIONS_KEY) else {
        return Ok(DEFAULT_SHUFFLE_PARTITIONS);
    };
    match raw.parse::<u32>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(DataFusionError::Configuration(format!(
            "invalid value {raw} for {SHUFFLE_PARTITIONS_KEY}: expected a positive integer"
        ))),
    }
}

#[must_use]
pub fn with_spark_fanout_commit_order(
    config: SessionConfig,
    shuffle_partitions: u32,
) -> SessionConfig {
    config.with_extension(Arc::new(DataFileCommitOrder::new(move |files, _| {
        spark_fanout_commit_order(files, shuffle_partitions)
    })))
}

#[cfg(test)]
#[path = "fanout_order_tests.rs"]
mod tests;
