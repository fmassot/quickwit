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

//! Zero-copy adapters bridging `serde_json_borrow` values directly into Tantivy's [`Value`]
//! trait so documents can be encoded straight into [`tantivy::TantivyDocument`] without
//! intermediate `serde_json::Value`, `BTreeMap`, or `OwnedValue` trees.

use serde_json_borrow::{Map as BorrowedJsonMap, Value as BorrowedJsonValue};
use tantivy::TantivyDocument as Document;
use tantivy::schema::document::{ReferenceValue, ReferenceValueLeaf, try_parse_rfc3339_date_time};
use tantivy::schema::{Field, Value};

/// An entry in the dynamically-mapped portion of a borrowed JSON document.
#[derive(Debug)]
pub(crate) enum DynamicEntry<'a> {
    /// An entire unmapped subtree borrowed directly from the parsed JSON.
    Borrowed(&'a BorrowedJsonValue<'a>),
    /// A partially-mapped object node whose unmapped subfields are stored here.
    NestedObject(Vec<(&'a str, DynamicEntry<'a>)>),
}

/// Zero-copy [`tantivy::schema::Value`] view over borrowed JSON values and dynamic subtrees.
#[derive(Clone, Copy, Debug)]
pub(crate) enum BorrowedDocValue<'a> {
    Json(&'a BorrowedJsonValue<'a>),
    JsonObject(&'a BorrowedJsonMap<'a>),
    DynamicObject(&'a [(&'a str, DynamicEntry<'a>)]),
}

pub(crate) struct BorrowedDocArrayIter<'a>(std::slice::Iter<'a, BorrowedJsonValue<'a>>);

impl<'a> Iterator for BorrowedDocArrayIter<'a> {
    type Item = BorrowedDocValue<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(BorrowedDocValue::Json)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for BorrowedDocArrayIter<'_> {}

pub(crate) enum BorrowedDocObjectIter<'a> {
    JsonMap {
        map: &'a BorrowedJsonMap<'a>,
        idx: usize,
    },
    DynamicEntries(std::slice::Iter<'a, (&'a str, DynamicEntry<'a>)>),
}

impl<'a> Iterator for BorrowedDocObjectIter<'a> {
    type Item = (&'a str, BorrowedDocValue<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BorrowedDocObjectIter::JsonMap { map, idx } => {
                let (key, val) = map.get_key_value_at(*idx)?;
                *idx += 1;
                Some((key, BorrowedDocValue::Json(val)))
            }
            BorrowedDocObjectIter::DynamicEntries(iter) => {
                let (key, entry) = iter.next()?;
                let val = match entry {
                    DynamicEntry::Borrowed(json_val) => BorrowedDocValue::Json(json_val),
                    DynamicEntry::NestedObject(entries) => {
                        BorrowedDocValue::DynamicObject(entries.as_slice())
                    }
                };
                Some((*key, val))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            BorrowedDocObjectIter::JsonMap { map, idx } => {
                let remaining = map.len().saturating_sub(*idx);
                (remaining, Some(remaining))
            }
            BorrowedDocObjectIter::DynamicEntries(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for BorrowedDocObjectIter<'_> {}

impl<'a> Value<'a> for BorrowedDocValue<'a> {
    type ArrayIter = BorrowedDocArrayIter<'a>;
    type ObjectIter = BorrowedDocObjectIter<'a>;

    #[inline]
    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match *self {
            BorrowedDocValue::Json(json_val) => match json_val {
                BorrowedJsonValue::Null => ReferenceValueLeaf::Null.into(),
                BorrowedJsonValue::Bool(bool_val) => ReferenceValueLeaf::Bool(*bool_val).into(),
                BorrowedJsonValue::Number(number) => {
                    if let Some(val) = number.as_i64() {
                        ReferenceValueLeaf::I64(val).into()
                    } else if let Some(val) = number.as_u64() {
                        ReferenceValueLeaf::U64(val).into()
                    } else if let Some(val) = number.as_f64() {
                        ReferenceValueLeaf::F64(val).into()
                    } else {
                        panic!("unsupported serde_json_borrow number {number}");
                    }
                }
                BorrowedJsonValue::Str(text) => {
                    let text_ref: &'a str = text.as_ref();
                    if let Some(date_time) = try_parse_rfc3339_date_time(text_ref) {
                        ReferenceValueLeaf::Date(date_time).into()
                    } else {
                        ReferenceValueLeaf::Str(text_ref).into()
                    }
                }
                BorrowedJsonValue::Array(elements) => {
                    ReferenceValue::Array(BorrowedDocArrayIter(elements.iter()))
                }
                BorrowedJsonValue::Object(map) => {
                    ReferenceValue::Object(BorrowedDocObjectIter::JsonMap { map, idx: 0 })
                }
            },
            BorrowedDocValue::JsonObject(map) => {
                ReferenceValue::Object(BorrowedDocObjectIter::JsonMap { map, idx: 0 })
            }
            BorrowedDocValue::DynamicObject(entries) => {
                ReferenceValue::Object(BorrowedDocObjectIter::DynamicEntries(entries.iter()))
            }
        }
    }
}

/// Inserts an unmapped leaf at `path` inside a borrowed dynamic entries tree.
pub(crate) fn insert_borrowed_dynamic_path<'a>(
    path: &[&'a str],
    field_name: &'a str,
    val: &'a BorrowedJsonValue<'a>,
    mut entries: &mut Vec<(&'a str, DynamicEntry<'a>)>,
) {
    for &segment in path {
        let pos = if let Some(pos) = entries.iter().position(|(key, _)| *key == segment) {
            pos
        } else {
            entries.push((segment, DynamicEntry::NestedObject(Vec::new())));
            entries.len() - 1
        };
        entries = match &mut entries[pos].1 {
            DynamicEntry::NestedObject(child_entries) => child_entries,
            DynamicEntry::Borrowed(_) => panic!("expected nested dynamic object"),
        };
    }
    entries.push((field_name, DynamicEntry::Borrowed(val)));
}

/// Emits all primitive values from a borrowed JSON subtree into every field in `concat_fields`,
/// matching [`crate::doc_mapper::mapping_tree::map_primitive_json_to_concatenate_value`] (no
/// RFC3339 date auto-parsing).
pub(crate) fn emit_borrowed_json_concatenate(
    json_val: &BorrowedJsonValue<'_>,
    concat_fields: &[Field],
    document: &mut Document,
) {
    match json_val {
        BorrowedJsonValue::Null => {}
        BorrowedJsonValue::Str(text) => {
            for &field in concat_fields {
                document.add_text(field, text.as_ref());
            }
        }
        BorrowedJsonValue::Bool(bool_val) => {
            for &field in concat_fields {
                document.add_bool(field, *bool_val);
            }
        }
        BorrowedJsonValue::Number(number) => {
            if let Some(val) = number.as_i64() {
                for &field in concat_fields {
                    document.add_i64(field, val);
                }
            } else if let Some(val) = number.as_u64() {
                for &field in concat_fields {
                    document.add_u64(field, val);
                }
            } else if let Some(val) = number.as_f64() {
                for &field in concat_fields {
                    document.add_f64(field, val);
                }
            }
        }
        BorrowedJsonValue::Array(elements) => {
            for element in elements {
                emit_borrowed_json_concatenate(element, concat_fields, document);
            }
        }
        BorrowedJsonValue::Object(map) => {
            for (_key, val) in map.iter() {
                emit_borrowed_json_concatenate(val, concat_fields, document);
            }
        }
    }
}

/// Emits all primitive values from a dynamic entry tree into `concat_fields`.
pub(crate) fn emit_dynamic_entries_concatenate(
    entries: &[(&str, DynamicEntry<'_>)],
    concat_fields: &[Field],
    document: &mut Document,
) {
    for (_key, entry) in entries {
        match entry {
            DynamicEntry::Borrowed(json_val) => {
                emit_borrowed_json_concatenate(json_val, concat_fields, document);
            }
            DynamicEntry::NestedObject(children) => {
                emit_dynamic_entries_concatenate(children, concat_fields, document);
            }
        }
    }
}
