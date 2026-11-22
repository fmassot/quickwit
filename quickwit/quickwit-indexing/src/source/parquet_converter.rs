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

//! Zero-JSON Arrow `RecordBatch` to Quickwit [`ProcessedDoc`] converter.
//!
//! Pre-binds Arrow column indices to Tantivy [`Field`]s and typed slice readers once per
//! `RecordBatch` so the per-row inner loop performs direct typed array indexing with zero
//! JSON parsing, zero field-name hashing, and zero per-row dynamic downcasts.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;

use anyhow::{Context, bail};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, StringViewArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit};
use quickwit_doc_mapper::{DocMapper, ModeType};
use tantivy::schema::{Field, FieldType, OwnedValue, Schema as TantivySchema};
use tantivy::{DateTime, TantivyDocument};

use crate::models::ProcessedDoc;

const SECS_PER_DAY: i64 = 86_400;

/// Target Tantivy field kind bound to an Arrow column index.
#[derive(Clone, Copy, Debug)]
enum TargetKind {
    I64,
    U64,
    F64,
    Bool,
    Str,
    Bytes,
    Date,
    IpAddr,
}

#[derive(Clone, Debug)]
struct BoundColumn {
    /// Column index within the projected `RecordBatch`.
    batch_col_idx: usize,
    field: Field,
    target_kind: TargetKind,
    is_timestamp: bool,
}

#[derive(Clone, Debug)]
struct DynamicColumn {
    batch_col_idx: usize,
    name: String,
}

/// Pre-bound mapping from a Parquet/Arrow schema to a Quickwit [`DocMapper`].
pub(crate) struct BoundParquetSchema {
    /// Root column indices in the original Parquet schema to read (projection pushdown).
    pub(crate) root_projection_indices: Vec<usize>,
    mapped_columns: Vec<BoundColumn>,
    dynamic_columns: Vec<DynamicColumn>,
    num_tantivy_fields: usize,
    has_timestamp_field: bool,
    dynamic_field: Option<Field>,
    document_size_field: Option<Field>,
    index_field_presence: bool,
}

impl BoundParquetSchema {
    /// Binds an Arrow schema (from a Parquet file footer) against `doc_mapper`.
    ///
    /// Contract: when `doc_mapper.mode_type() == ModeType::Strict`, any Parquet column absent
    /// from the Tantivy schema returns an error. When `Lenient`, unmapped columns are pruned
    /// via Parquet projection pushdown so they are neither read nor decompressed.
    pub(crate) fn try_new(
        doc_mapper: &DocMapper,
        arrow_schema: &ArrowSchema,
    ) -> anyhow::Result<Self> {
        if doc_mapper.source_field().is_some() {
            bail!(
                "native parquet ingest currently requires `store_source: false` in the index \
                 doc_mapping"
            );
        }
        let tantivy_schema: TantivySchema = doc_mapper.schema();
        let mode_type = doc_mapper.mode_type();
        let timestamp_field_opt = doc_mapper
            .timestamp_field_name()
            .and_then(|name| tantivy_schema.get_field(name).ok());

        let mut root_projection_indices = Vec::with_capacity(arrow_schema.fields().len());
        let mut mapped_columns = Vec::with_capacity(arrow_schema.fields().len());
        let mut dynamic_columns = Vec::new();

        for (orig_idx, arrow_field) in arrow_schema.fields().iter().enumerate() {
            let col_name = arrow_field.name();
            if let Ok(tantivy_field) = tantivy_schema.get_field(col_name) {
                let target_kind = match tantivy_schema.get_field_entry(tantivy_field).field_type() {
                    FieldType::I64(_) => TargetKind::I64,
                    FieldType::U64(_) => TargetKind::U64,
                    FieldType::F64(_) => TargetKind::F64,
                    FieldType::Bool(_) => TargetKind::Bool,
                    FieldType::Str(_) => TargetKind::Str,
                    FieldType::Bytes(_) => TargetKind::Bytes,
                    FieldType::Date(_) => TargetKind::Date,
                    FieldType::IpAddr(_) => TargetKind::IpAddr,
                    other => {
                        bail!(
                            "unsupported Tantivy field type `{other:?}` for Parquet column \
                             `{col_name}`"
                        );
                    }
                };
                let batch_col_idx = root_projection_indices.len();
                root_projection_indices.push(orig_idx);
                mapped_columns.push(BoundColumn {
                    batch_col_idx,
                    field: tantivy_field,
                    target_kind,
                    is_timestamp: timestamp_field_opt == Some(tantivy_field),
                });
            } else {
                match mode_type {
                    ModeType::Strict => {
                        bail!("Parquet column `{col_name}` is not defined in strict index schema");
                    }
                    ModeType::Lenient => {
                        // Pruned via Parquet projection pushdown.
                    }
                    ModeType::Dynamic => {
                        let batch_col_idx = root_projection_indices.len();
                        root_projection_indices.push(orig_idx);
                        dynamic_columns.push(DynamicColumn {
                            batch_col_idx,
                            name: col_name.clone(),
                        });
                    }
                }
            }
        }

        Ok(Self {
            root_projection_indices,
            mapped_columns,
            dynamic_columns,
            num_tantivy_fields: tantivy_schema.num_fields(),
            has_timestamp_field: timestamp_field_opt.is_some(),
            dynamic_field: doc_mapper.dynamic_field(),
            document_size_field: doc_mapper.document_size_field(),
            index_field_presence: doc_mapper.index_field_presence(),
        })
    }

    /// Converts one projected Arrow `RecordBatch` into [`ProcessedDoc`]s ready for `Indexer`.
    pub(crate) fn convert_batch(
        &self,
        batch: &RecordBatch,
        doc_mapper: &DocMapper,
    ) -> anyhow::Result<Vec<ProcessedDoc>> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        let readers: Vec<BoundColumnReader<'_>> = self
            .mapped_columns
            .iter()
            .map(|col| {
                let array = batch.column(col.batch_col_idx);
                let typed =
                    TypedArrayReader::try_new(array, col.target_kind).with_context(|| {
                        format!(
                            "failed to bind Parquet column `{}` (Arrow type {:?}) to {:?}",
                            batch.schema().field(col.batch_col_idx).name(),
                            array.data_type(),
                            col.target_kind
                        )
                    })?;
                Ok(BoundColumnReader {
                    field: col.field,
                    is_timestamp: col.is_timestamp,
                    has_nulls: array.null_count() > 0,
                    array: array.as_ref(),
                    typed,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let est_bytes_per_row = (batch.get_array_memory_size() / num_rows.max(1)).max(64);
        let mut docs = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let mut document = TantivyDocument::with_capacity_and_num_fields(
                est_bytes_per_row + 64,
                self.num_tantivy_fields,
            );
            let mut timestamp_opt: Option<DateTime> = None;
            let mut row_bytes: usize = 0;

            for reader in &readers {
                if reader.has_nulls && reader.array.is_null(row_idx) {
                    continue;
                }
                let (bytes_written, dt_opt) =
                    reader
                        .typed
                        .write_row(row_idx, reader.field, &mut document)?;
                row_bytes += bytes_written;
                if reader.is_timestamp {
                    timestamp_opt = dt_opt;
                }
            }

            if let Some(dynamic_field) = self.dynamic_field
                && !self.dynamic_columns.is_empty()
            {
                let mut dyn_map = BTreeMap::new();
                for dyn_col in &self.dynamic_columns {
                    let array = batch.column(dyn_col.batch_col_idx);
                    if array.is_null(row_idx) {
                        continue;
                    }
                    if let Some((owned_val, val_bytes)) = arrow_value_to_owned(array, row_idx) {
                        row_bytes += dyn_col.name.len() + val_bytes;
                        dyn_map.insert(dyn_col.name.clone(), owned_val);
                    }
                }
                if !dyn_map.is_empty() {
                    document.add_object(dynamic_field, dyn_map);
                }
            }

            if self.has_timestamp_field && timestamp_opt.is_none() {
                bail!("missing required timestamp value at Parquet row {row_idx}");
            }
            if let Some(doc_size_field) = self.document_size_field {
                document.add_u64(doc_size_field, row_bytes as u64);
            }
            if self.index_field_presence {
                doc_mapper.populate_field_presence(&mut document);
            }

            docs.push(ProcessedDoc {
                doc: document,
                timestamp_opt,
                fingerprint_opt: None,
                partition: 0,
                num_bytes: row_bytes.max(1),
            });
        }

        Ok(docs)
    }
}

struct BoundColumnReader<'a> {
    field: Field,
    is_timestamp: bool,
    has_nulls: bool,
    array: &'a dyn Array,
    typed: TypedArrayReader<'a>,
}

/// Pre-downcast Arrow column reader eliminating per-row dynamic dispatch.
enum TypedArrayReader<'a> {
    // I64 targets
    I64FromI64(&'a [i64]),
    I64FromI32(&'a [i32]),
    I64FromI16(&'a [i16]),
    I64FromI8(&'a [i8]),
    I64FromU64(&'a [u64]),
    I64FromU32(&'a [u32]),
    I64FromU16(&'a [u16]),
    I64FromU8(&'a [u8]),
    // U64 targets
    U64FromU64(&'a [u64]),
    U64FromU32(&'a [u32]),
    U64FromU16(&'a [u16]),
    U64FromU8(&'a [u8]),
    U64FromI64(&'a [i64]),
    U64FromI32(&'a [i32]),
    U64FromI16(&'a [i16]),
    U64FromI8(&'a [i8]),
    // F64 targets
    F64FromF64(&'a [f64]),
    F64FromF32(&'a [f32]),
    F64FromI64(&'a [i64]),
    F64FromI32(&'a [i32]),
    // Bool target
    Bool(&'a BooleanArray),
    // Str targets
    StrFromUtf8(&'a StringArray),
    StrFromLargeUtf8(&'a LargeStringArray),
    StrFromUtf8View(&'a StringViewArray),
    StrFromBinary(&'a BinaryArray),
    // Bytes targets
    BytesFromBinary(&'a BinaryArray),
    BytesFromLargeBinary(&'a LargeBinaryArray),
    BytesFromUtf8(&'a StringArray),
    // Date targets
    DateFromTsSecs(&'a [i64]),
    DateFromTsMillis(&'a [i64]),
    DateFromTsMicros(&'a [i64]),
    DateFromTsNanos(&'a [i64]),
    DateFromDate32(&'a [i32]),
    DateFromDate64(&'a [i64]),
    DateFromI64Secs(&'a [i64]),
    DateFromI32Secs(&'a [i32]),
    DateFromU64Secs(&'a [u64]),
    DateFromU32Secs(&'a [u32]),
    DateFromU16Days(&'a [u16]),
    DateFromI16Days(&'a [i16]),
    // IpAddr targets
    IpFromUtf8(&'a StringArray),
    IpFromU32(&'a [u32]),
}

impl<'a> TypedArrayReader<'a> {
    fn try_new(array: &'a ArrayRef, target_kind: TargetKind) -> anyhow::Result<Self> {
        let reader = match (target_kind, array.data_type()) {
            (TargetKind::I64, DataType::Int64) => {
                Self::I64FromI64(array.as_primitive::<Int64Type>().values())
            }
            (TargetKind::I64, DataType::Int32) => {
                Self::I64FromI32(array.as_primitive::<Int32Type>().values())
            }
            (TargetKind::I64, DataType::Int16) => {
                Self::I64FromI16(array.as_primitive::<Int16Type>().values())
            }
            (TargetKind::I64, DataType::Int8) => {
                Self::I64FromI8(array.as_primitive::<Int8Type>().values())
            }
            (TargetKind::I64, DataType::UInt64) => {
                Self::I64FromU64(array.as_primitive::<UInt64Type>().values())
            }
            (TargetKind::I64, DataType::UInt32) => {
                Self::I64FromU32(array.as_primitive::<UInt32Type>().values())
            }
            (TargetKind::I64, DataType::UInt16) => {
                Self::I64FromU16(array.as_primitive::<UInt16Type>().values())
            }
            (TargetKind::I64, DataType::UInt8) => {
                Self::I64FromU8(array.as_primitive::<UInt8Type>().values())
            }

            (TargetKind::U64, DataType::UInt64) => {
                Self::U64FromU64(array.as_primitive::<UInt64Type>().values())
            }
            (TargetKind::U64, DataType::UInt32) => {
                Self::U64FromU32(array.as_primitive::<UInt32Type>().values())
            }
            (TargetKind::U64, DataType::UInt16) => {
                Self::U64FromU16(array.as_primitive::<UInt16Type>().values())
            }
            (TargetKind::U64, DataType::UInt8) => {
                Self::U64FromU8(array.as_primitive::<UInt8Type>().values())
            }
            (TargetKind::U64, DataType::Int64) => {
                Self::U64FromI64(array.as_primitive::<Int64Type>().values())
            }
            (TargetKind::U64, DataType::Int32) => {
                Self::U64FromI32(array.as_primitive::<Int32Type>().values())
            }
            (TargetKind::U64, DataType::Int16) => {
                Self::U64FromI16(array.as_primitive::<Int16Type>().values())
            }
            (TargetKind::U64, DataType::Int8) => {
                Self::U64FromI8(array.as_primitive::<Int8Type>().values())
            }

            (TargetKind::F64, DataType::Float64) => {
                Self::F64FromF64(array.as_primitive::<Float64Type>().values())
            }
            (TargetKind::F64, DataType::Float32) => {
                Self::F64FromF32(array.as_primitive::<Float32Type>().values())
            }
            (TargetKind::F64, DataType::Int64) => {
                Self::F64FromI64(array.as_primitive::<Int64Type>().values())
            }
            (TargetKind::F64, DataType::Int32) => {
                Self::F64FromI32(array.as_primitive::<Int32Type>().values())
            }

            (TargetKind::Bool, DataType::Boolean) => Self::Bool(array.as_boolean()),

            (TargetKind::Str, DataType::Utf8) => Self::StrFromUtf8(array.as_string::<i32>()),
            (TargetKind::Str, DataType::LargeUtf8) => {
                Self::StrFromLargeUtf8(array.as_string::<i64>())
            }
            (TargetKind::Str, DataType::Utf8View) => Self::StrFromUtf8View(array.as_string_view()),
            (TargetKind::Str, DataType::Binary) => Self::StrFromBinary(array.as_binary::<i32>()),

            (TargetKind::Bytes, DataType::Binary) => {
                Self::BytesFromBinary(array.as_binary::<i32>())
            }
            (TargetKind::Bytes, DataType::LargeBinary) => {
                Self::BytesFromLargeBinary(array.as_binary::<i64>())
            }
            (TargetKind::Bytes, DataType::Utf8) => Self::BytesFromUtf8(array.as_string::<i32>()),

            (TargetKind::Date, DataType::Timestamp(TimeUnit::Second, _)) => {
                Self::DateFromTsSecs(array.as_primitive::<TimestampSecondType>().values())
            }
            (TargetKind::Date, DataType::Timestamp(TimeUnit::Millisecond, _)) => {
                Self::DateFromTsMillis(array.as_primitive::<TimestampMillisecondType>().values())
            }
            (TargetKind::Date, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                Self::DateFromTsMicros(array.as_primitive::<TimestampMicrosecondType>().values())
            }
            (TargetKind::Date, DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
                Self::DateFromTsNanos(array.as_primitive::<TimestampNanosecondType>().values())
            }
            (TargetKind::Date, DataType::Date32) => {
                Self::DateFromDate32(array.as_primitive::<Date32Type>().values())
            }
            (TargetKind::Date, DataType::Date64) => {
                Self::DateFromDate64(array.as_primitive::<Date64Type>().values())
            }
            (TargetKind::Date, DataType::Int64) => {
                Self::DateFromI64Secs(array.as_primitive::<Int64Type>().values())
            }
            (TargetKind::Date, DataType::Int32) => {
                Self::DateFromI32Secs(array.as_primitive::<Int32Type>().values())
            }
            (TargetKind::Date, DataType::UInt64) => {
                Self::DateFromU64Secs(array.as_primitive::<UInt64Type>().values())
            }
            (TargetKind::Date, DataType::UInt32) => {
                Self::DateFromU32Secs(array.as_primitive::<UInt32Type>().values())
            }
            (TargetKind::Date, DataType::UInt16) => {
                Self::DateFromU16Days(array.as_primitive::<UInt16Type>().values())
            }
            (TargetKind::Date, DataType::Int16) => {
                Self::DateFromI16Days(array.as_primitive::<Int16Type>().values())
            }

            (TargetKind::IpAddr, DataType::Utf8) => Self::IpFromUtf8(array.as_string::<i32>()),
            (TargetKind::IpAddr, DataType::UInt32) => {
                Self::IpFromU32(array.as_primitive::<UInt32Type>().values())
            }

            (kind, arrow_ty) => {
                bail!("unsupported Arrow type `{arrow_ty:?}` for target field kind `{kind:?}`");
            }
        };
        Ok(reader)
    }

    #[inline(always)]
    fn write_row(
        &self,
        row_idx: usize,
        field: Field,
        doc: &mut TantivyDocument,
    ) -> anyhow::Result<(usize, Option<DateTime>)> {
        match self {
            Self::I64FromI64(vals) => {
                doc.add_i64(field, vals[row_idx]);
                Ok((8, None))
            }
            Self::I64FromI32(vals) => {
                doc.add_i64(field, i64::from(vals[row_idx]));
                Ok((4, None))
            }
            Self::I64FromI16(vals) => {
                doc.add_i64(field, i64::from(vals[row_idx]));
                Ok((2, None))
            }
            Self::I64FromI8(vals) => {
                doc.add_i64(field, i64::from(vals[row_idx]));
                Ok((1, None))
            }
            Self::I64FromU64(vals) => {
                doc.add_i64(field, vals[row_idx] as i64);
                Ok((8, None))
            }
            Self::I64FromU32(vals) => {
                doc.add_i64(field, i64::from(vals[row_idx]));
                Ok((4, None))
            }
            Self::I64FromU16(vals) => {
                doc.add_i64(field, i64::from(vals[row_idx]));
                Ok((2, None))
            }
            Self::I64FromU8(vals) => {
                doc.add_i64(field, i64::from(vals[row_idx]));
                Ok((1, None))
            }

            Self::U64FromU64(vals) => {
                doc.add_u64(field, vals[row_idx]);
                Ok((8, None))
            }
            Self::U64FromU32(vals) => {
                doc.add_u64(field, u64::from(vals[row_idx]));
                Ok((4, None))
            }
            Self::U64FromU16(vals) => {
                doc.add_u64(field, u64::from(vals[row_idx]));
                Ok((2, None))
            }
            Self::U64FromU8(vals) => {
                doc.add_u64(field, u64::from(vals[row_idx]));
                Ok((1, None))
            }
            Self::U64FromI64(vals) => {
                let val =
                    u64::try_from(vals[row_idx]).context("negative Int64 value in u64 field")?;
                doc.add_u64(field, val);
                Ok((8, None))
            }
            Self::U64FromI32(vals) => {
                let val =
                    u64::try_from(vals[row_idx]).context("negative Int32 value in u64 field")?;
                doc.add_u64(field, val);
                Ok((4, None))
            }
            Self::U64FromI16(vals) => {
                let val =
                    u64::try_from(vals[row_idx]).context("negative Int16 value in u64 field")?;
                doc.add_u64(field, val);
                Ok((2, None))
            }
            Self::U64FromI8(vals) => {
                let val =
                    u64::try_from(vals[row_idx]).context("negative Int8 value in u64 field")?;
                doc.add_u64(field, val);
                Ok((1, None))
            }

            Self::F64FromF64(vals) => {
                doc.add_f64(field, vals[row_idx]);
                Ok((8, None))
            }
            Self::F64FromF32(vals) => {
                doc.add_f64(field, f64::from(vals[row_idx]));
                Ok((4, None))
            }
            Self::F64FromI64(vals) => {
                doc.add_f64(field, vals[row_idx] as f64);
                Ok((8, None))
            }
            Self::F64FromI32(vals) => {
                doc.add_f64(field, f64::from(vals[row_idx]));
                Ok((4, None))
            }

            Self::Bool(arr) => {
                doc.add_bool(field, arr.value(row_idx));
                Ok((1, None))
            }

            Self::StrFromUtf8(arr) => {
                let text = arr.value(row_idx);
                doc.add_text(field, text);
                Ok((text.len(), None))
            }
            Self::StrFromLargeUtf8(arr) => {
                let text = arr.value(row_idx);
                doc.add_text(field, text);
                Ok((text.len(), None))
            }
            Self::StrFromUtf8View(arr) => {
                let text = arr.value(row_idx);
                doc.add_text(field, text);
                Ok((text.len(), None))
            }
            Self::StrFromBinary(arr) => {
                let bytes = arr.value(row_idx);
                let text = std::str::from_utf8(bytes)
                    .context("non-UTF-8 bytes in Binary column mapped to text field")?;
                doc.add_text(field, text);
                Ok((text.len(), None))
            }

            Self::BytesFromBinary(arr) => {
                let bytes = arr.value(row_idx);
                doc.add_bytes(field, bytes);
                Ok((bytes.len(), None))
            }
            Self::BytesFromLargeBinary(arr) => {
                let bytes = arr.value(row_idx);
                doc.add_bytes(field, bytes);
                Ok((bytes.len(), None))
            }
            Self::BytesFromUtf8(arr) => {
                let bytes = arr.value(row_idx).as_bytes();
                doc.add_bytes(field, bytes);
                Ok((bytes.len(), None))
            }

            Self::DateFromTsSecs(vals) | Self::DateFromI64Secs(vals) => {
                let dt = DateTime::from_timestamp_secs(vals[row_idx]);
                doc.add_date(field, dt);
                Ok((8, Some(dt)))
            }
            Self::DateFromTsMillis(vals) | Self::DateFromDate64(vals) => {
                let dt = DateTime::from_timestamp_millis(vals[row_idx]);
                doc.add_date(field, dt);
                Ok((8, Some(dt)))
            }
            Self::DateFromTsMicros(vals) => {
                let dt = DateTime::from_timestamp_micros(vals[row_idx]);
                doc.add_date(field, dt);
                Ok((8, Some(dt)))
            }
            Self::DateFromTsNanos(vals) => {
                let dt = DateTime::from_timestamp_nanos(vals[row_idx]);
                doc.add_date(field, dt);
                Ok((8, Some(dt)))
            }
            Self::DateFromDate32(vals) => {
                let dt = DateTime::from_timestamp_secs(i64::from(vals[row_idx]) * SECS_PER_DAY);
                doc.add_date(field, dt);
                Ok((4, Some(dt)))
            }
            Self::DateFromI32Secs(vals) => {
                let dt = DateTime::from_timestamp_secs(i64::from(vals[row_idx]));
                doc.add_date(field, dt);
                Ok((4, Some(dt)))
            }
            Self::DateFromU64Secs(vals) => {
                let dt = DateTime::from_timestamp_secs(vals[row_idx] as i64);
                doc.add_date(field, dt);
                Ok((8, Some(dt)))
            }
            Self::DateFromU32Secs(vals) => {
                let dt = DateTime::from_timestamp_secs(i64::from(vals[row_idx]));
                doc.add_date(field, dt);
                Ok((4, Some(dt)))
            }
            Self::DateFromU16Days(vals) => {
                let dt = DateTime::from_timestamp_secs(i64::from(vals[row_idx]) * SECS_PER_DAY);
                doc.add_date(field, dt);
                Ok((2, Some(dt)))
            }
            Self::DateFromI16Days(vals) => {
                let dt = DateTime::from_timestamp_secs(i64::from(vals[row_idx]) * SECS_PER_DAY);
                doc.add_date(field, dt);
                Ok((2, Some(dt)))
            }

            Self::IpFromUtf8(arr) => {
                let text = arr.value(row_idx);
                let ip = IpAddr::from_str(text).context("invalid IP address in Parquet column")?;
                let ipv6 = match ip {
                    IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                    IpAddr::V6(v6) => v6,
                };
                doc.add_ip_addr(field, ipv6);
                Ok((text.len(), None))
            }
            Self::IpFromU32(vals) => {
                let ipv6 = Ipv4Addr::from(vals[row_idx]).to_ipv6_mapped();
                doc.add_ip_addr(field, ipv6);
                Ok((4, None))
            }
        }
    }
}

fn arrow_value_to_owned(array: &ArrayRef, row_idx: usize) -> Option<(OwnedValue, usize)> {
    match array.data_type() {
        DataType::Boolean => Some((OwnedValue::Bool(array.as_boolean().value(row_idx)), 1)),
        DataType::Int8 => Some((
            OwnedValue::I64(i64::from(array.as_primitive::<Int8Type>().value(row_idx))),
            1,
        )),
        DataType::Int16 => Some((
            OwnedValue::I64(i64::from(array.as_primitive::<Int16Type>().value(row_idx))),
            2,
        )),
        DataType::Int32 => Some((
            OwnedValue::I64(i64::from(array.as_primitive::<Int32Type>().value(row_idx))),
            4,
        )),
        DataType::Int64 => Some((
            OwnedValue::I64(array.as_primitive::<Int64Type>().value(row_idx)),
            8,
        )),
        DataType::UInt8 => Some((
            OwnedValue::U64(u64::from(array.as_primitive::<UInt8Type>().value(row_idx))),
            1,
        )),
        DataType::UInt16 => Some((
            OwnedValue::U64(u64::from(array.as_primitive::<UInt16Type>().value(row_idx))),
            2,
        )),
        DataType::UInt32 => Some((
            OwnedValue::U64(u64::from(array.as_primitive::<UInt32Type>().value(row_idx))),
            4,
        )),
        DataType::UInt64 => Some((
            OwnedValue::U64(array.as_primitive::<UInt64Type>().value(row_idx)),
            8,
        )),
        DataType::Float32 => Some((
            OwnedValue::F64(f64::from(
                array.as_primitive::<Float32Type>().value(row_idx),
            )),
            4,
        )),
        DataType::Float64 => Some((
            OwnedValue::F64(array.as_primitive::<Float64Type>().value(row_idx)),
            8,
        )),
        DataType::Utf8 => {
            let text = array.as_string::<i32>().value(row_idx);
            Some((OwnedValue::Str(text.to_owned()), text.len()))
        }
        DataType::LargeUtf8 => {
            let text = array.as_string::<i64>().value(row_idx);
            Some((OwnedValue::Str(text.to_owned()), text.len()))
        }
        DataType::Utf8View => {
            let text = array.as_string_view().value(row_idx);
            Some((OwnedValue::Str(text.to_owned()), text.len()))
        }
        _ => None,
    }
}
