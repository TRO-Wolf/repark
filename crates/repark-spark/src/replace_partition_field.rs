use datafusion::error::{DataFusionError, Result};
use iceberg::spec::Transform;
use repark_iceberg::write::alter::PartitionSpecChange;

use crate::alter::{
    IcebergAlterDdl, Sig, parse_partition_field_term, partition_field_spec_parts, render_sig_at,
    word_at, word_eq,
};

enum OldFieldTerm {
    ByName {
        name: String,
    },
    ByTransform {
        source_name: String,
        transform: Transform,
    },
}

pub(crate) fn parse(
    significant: &[Sig],
    start: usize,
    table_parts: Vec<String>,
) -> Result<IcebergAlterDdl> {
    let (old_term, next) = parse_old_field_term(significant, start)?;
    if !word_eq(significant, next, "WITH") {
        return Err(DataFusionError::Plan(
            "ALTER TABLE REPLACE PARTITION FIELD expects `WITH <transform>(col) [AS name]`".into(),
        ));
    }
    let (field_spec, name, next) = parse_partition_field_term(significant, next + 1)?;
    let (new_name, next) = if name.is_some() {
        (name, next)
    } else if word_eq(significant, next, "AS") {
        let alias = word_at(significant, next + 1).ok_or_else(|| {
            DataFusionError::Plan(
                "ALTER TABLE REPLACE PARTITION FIELD … AS expects a partition field name".into(),
            )
        })?;
        (Some(alias.to_string()), next + 2)
    } else {
        (None, next)
    };
    if next < significant.len() {
        return Err(DataFusionError::Plan(format!(
            "trailing tokens after REPLACE PARTITION FIELD (starting at `{}`)",
            render_sig_at(significant, next)
        )));
    }
    let (source_name, transform) = partition_field_spec_parts(&field_spec);
    let change = match old_term {
        OldFieldTerm::ByName { name: old_name } => PartitionSpecChange::ReplaceField {
            old_name,
            source_name,
            transform,
            new_name,
        },
        OldFieldTerm::ByTransform {
            source_name: old_source_name,
            transform: old_transform,
        } => PartitionSpecChange::ReplaceFieldByTransform {
            old_source_name,
            old_transform,
            source_name,
            transform,
            new_name,
        },
    };
    Ok(IcebergAlterDdl::PartitionSpec {
        table_parts,
        changes: vec![change],
    })
}

fn parse_old_field_term(significant: &[Sig], start: usize) -> Result<(OldFieldTerm, usize)> {
    if matches!(significant.get(start + 1), Some(Sig::LParen)) {
        let (spec, _alias, next) = parse_partition_field_term(significant, start)?;
        let (source_name, transform) = partition_field_spec_parts(&spec);
        return Ok((
            OldFieldTerm::ByTransform {
                source_name,
                transform,
            },
            next,
        ));
    }
    let name = word_at(significant, start).ok_or_else(|| {
        DataFusionError::Plan(
            "ALTER TABLE REPLACE PARTITION FIELD expects the existing partition field name".into(),
        )
    })?;
    Ok((
        OldFieldTerm::ByName {
            name: name.to_string(),
        },
        start + 1,
    ))
}
