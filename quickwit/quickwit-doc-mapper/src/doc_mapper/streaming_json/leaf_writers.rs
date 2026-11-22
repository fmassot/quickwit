// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Primitive leaf-value writers for single-pass streaming JSON deserialization.

use std::net::IpAddr;
use std::str::FromStr;

use tantivy::TantivyDocument as Document;
use tantivy::schema::IntoIpv6Addr;

use crate::DocParsingError;
use crate::doc_mapper::mapping_tree::{LeafType, MappingLeaf, check_supplied_tie_breaker_value};

#[inline]
pub(super) fn add_leaf_str(
    leaf: &MappingLeaf,
    key: &str,
    strict_mode: bool,
    val: &str,
    document: &mut Document,
) -> Result<(), DocParsingError> {
    match &leaf.typ {
        LeafType::Text(_) => {
            document.add_text(leaf.field, val);
            Ok(())
        }
        LeafType::DateTime(opts) => {
            let dt = opts
                .parse_str(val)
                .map_err(|msg| DocParsingError::ValueError(key.to_string(), msg))?;
            document.add_date(leaf.field, dt);
            Ok(())
        }
        LeafType::I64(opts) => parse_coerced_num::<i64>(val, opts.coerce, key)
            .map(|parsed| document.add_i64(leaf.field, parsed)),
        LeafType::U64(opts) => parse_coerced_num::<u64>(val, opts.coerce, key)
            .map(|parsed| document.add_u64(leaf.field, parsed)),
        LeafType::F64(opts) => parse_coerced_num::<f64>(val, opts.coerce, key)
            .map(|parsed| document.add_f64(leaf.field, parsed)),
        LeafType::IpAddr(_) => {
            let ipv6 = IpAddr::from_str(val)
                .map_err(|err| {
                    DocParsingError::ValueError(
                        key.to_string(),
                        format!("failed to parse IP address `{val}`: {err}"),
                    )
                })?
                .into_ipv6_addr();
            document.add_ip_addr(leaf.field, ipv6);
            Ok(())
        }
        LeafType::Bytes(opts) => {
            let payload = opts
                .input_format
                .parse_str(val)
                .map_err(|msg| DocParsingError::ValueError(key.to_string(), msg))?;
            document.add_bytes(leaf.field, &payload);
            Ok(())
        }
        LeafType::Bool(_) => Err(DocParsingError::ValueError(
            key.to_string(),
            format!("expected boolean, got `\"{val}\"`"),
        )),
        LeafType::Json(_) => Err(DocParsingError::ValueError(
            key.to_string(),
            format!("expected object, got `\"{val}\"`"),
        )),
        LeafType::TieBreaker => {
            check_supplied_tie_breaker_value(false, strict_mode, key.to_string())
        }
    }
}

#[inline]
pub(super) fn add_leaf_i64(
    leaf: &MappingLeaf,
    key: &str,
    strict_mode: bool,
    val: i64,
    document: &mut Document,
) -> Result<(), DocParsingError> {
    match &leaf.typ {
        LeafType::I64(_) => {
            document.add_i64(leaf.field, val);
            Ok(())
        }
        LeafType::U64(_) => {
            if let Ok(u64_val) = u64::try_from(val) {
                document.add_u64(leaf.field, u64_val);
                Ok(())
            } else {
                Err(DocParsingError::ValueError(
                    key.to_string(),
                    format!("expected u64, got inconvertible JSON number `{val}`"),
                ))
            }
        }
        LeafType::F64(_) => {
            document.add_f64(leaf.field, val as f64);
            Ok(())
        }
        LeafType::DateTime(opts) => {
            let dt = opts
                .parse_i64(val)
                .map_err(|msg| DocParsingError::ValueError(key.to_string(), msg))?;
            document.add_date(leaf.field, dt);
            Ok(())
        }
        LeafType::TieBreaker => {
            check_supplied_tie_breaker_value(false, strict_mode, key.to_string())
        }
        _ => Err(DocParsingError::ValueError(
            key.to_string(),
            format!("unexpected integer `{val}`"),
        )),
    }
}

#[inline]
pub(super) fn add_leaf_u64(
    leaf: &MappingLeaf,
    key: &str,
    strict_mode: bool,
    val: u64,
    document: &mut Document,
) -> Result<(), DocParsingError> {
    match &leaf.typ {
        LeafType::U64(_) => {
            document.add_u64(leaf.field, val);
            Ok(())
        }
        LeafType::I64(_) => {
            if let Ok(i64_val) = i64::try_from(val) {
                document.add_i64(leaf.field, i64_val);
                Ok(())
            } else {
                Err(DocParsingError::ValueError(
                    key.to_string(),
                    format!("expected i64, got inconvertible JSON number `{val}`"),
                ))
            }
        }
        LeafType::F64(_) => {
            document.add_f64(leaf.field, val as f64);
            Ok(())
        }
        LeafType::DateTime(opts) => {
            let dt = if let Ok(i64_val) = i64::try_from(val) {
                opts.parse_i64(i64_val)
            } else {
                opts.parse_f64(val as f64)
            }
            .map_err(|msg| DocParsingError::ValueError(key.to_string(), msg))?;
            document.add_date(leaf.field, dt);
            Ok(())
        }
        LeafType::TieBreaker => {
            check_supplied_tie_breaker_value(false, strict_mode, key.to_string())
        }
        _ => Err(DocParsingError::ValueError(
            key.to_string(),
            format!("unexpected integer `{val}`"),
        )),
    }
}

#[inline]
pub(super) fn add_leaf_f64(
    leaf: &MappingLeaf,
    key: &str,
    strict_mode: bool,
    val: f64,
    document: &mut Document,
) -> Result<(), DocParsingError> {
    match &leaf.typ {
        LeafType::F64(_) => {
            document.add_f64(leaf.field, val);
            Ok(())
        }
        LeafType::DateTime(opts) => {
            let dt = opts
                .parse_f64(val)
                .map_err(|msg| DocParsingError::ValueError(key.to_string(), msg))?;
            document.add_date(leaf.field, dt);
            Ok(())
        }
        LeafType::TieBreaker => {
            check_supplied_tie_breaker_value(false, strict_mode, key.to_string())
        }
        _ => Err(DocParsingError::ValueError(
            key.to_string(),
            format!("unexpected float `{val}`"),
        )),
    }
}

#[inline]
pub(super) fn add_leaf_bool(
    leaf: &MappingLeaf,
    key: &str,
    strict_mode: bool,
    val: bool,
    document: &mut Document,
) -> Result<(), DocParsingError> {
    match &leaf.typ {
        LeafType::Bool(_) => {
            document.add_bool(leaf.field, val);
            Ok(())
        }
        LeafType::TieBreaker => {
            check_supplied_tie_breaker_value(false, strict_mode, key.to_string())
        }
        _ => Err(DocParsingError::ValueError(
            key.to_string(),
            format!("unexpected boolean `{val}`"),
        )),
    }
}

#[inline]
fn parse_coerced_num<T: FromStr>(
    str_val: &str,
    coerce: bool,
    key: &str,
) -> Result<T, DocParsingError> {
    if coerce {
        str_val.parse::<T>().map_err(|_| {
            DocParsingError::ValueError(
                key.to_string(),
                format!(
                    "failed to coerce JSON string `\"{str_val}\"` to {}",
                    std::any::type_name::<T>()
                ),
            )
        })
    } else {
        Err(DocParsingError::ValueError(
            key.to_string(),
            format!(
                "expected JSON number, got string `\"{str_val}\"`. enable coercion to {} with the \
                 `coerce` parameter in the field mapping",
                std::any::type_name::<T>()
            ),
        ))
    }
}
