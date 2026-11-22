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

//! Single-pass streaming JSON deserializer that writes mapped fields and dynamic subtrees
//! directly into a [`tantivy::TantivyDocument`] (`CompactDoc`) without building an intermediate
//! `serde_json::Value` or `serde_json_borrow::Value` DOM.

mod dynamic_seed;
mod leaf_writers;

use std::borrow::Cow;
use std::fmt;

use serde::de::{self, Deserialize, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use tantivy::TantivyDocument as Document;
use tantivy::schema::Field;
use tantivy::schema::document::CompactDocValueWriter;

use self::dynamic_seed::{
    ArrayElemLeafSeed, DynamicValueSeed, ObserveOnlySeed, stream_map_into_compact_doc,
};
use self::leaf_writers::{add_leaf_bool, add_leaf_f64, add_leaf_i64, add_leaf_str, add_leaf_u64};
use super::mapping_tree::{
    LeafType, MappingLeaf, MappingNode, MappingTree, check_supplied_tie_breaker_value,
};
use crate::{Cardinality, DocParsingError, ModeType};

/// Zero-copy string wrapper for serde deserialization.
///
/// Unlike serde's standard `Deserialize` implementation for `Cow<'de, str>` (which always
/// allocates an owned `String`), this wrapper borrows directly from the JSON input buffer
/// whenever the string contains no escape sequences.
pub(super) struct BorrowedStr<'de>(pub(super) Cow<'de, str>);

impl BorrowedStr<'_> {
    #[inline(always)]
    pub(super) fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'de> Deserialize<'de> for BorrowedStr<'de> {
    #[inline(always)]
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BorrowedStrVisitor;

        impl<'de> Visitor<'de> for BorrowedStrVisitor {
            type Value = BorrowedStr<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON string")
            }

            #[inline(always)]
            fn visit_borrowed_str<E: de::Error>(self, val: &'de str) -> Result<Self::Value, E> {
                Ok(BorrowedStr(Cow::Borrowed(val)))
            }

            #[inline]
            fn visit_str<E: de::Error>(self, val: &str) -> Result<Self::Value, E> {
                Ok(BorrowedStr(Cow::Owned(val.to_owned())))
            }

            #[inline]
            fn visit_string<E: de::Error>(self, val: String) -> Result<Self::Value, E> {
                Ok(BorrowedStr(Cow::Owned(val)))
            }
        }

        deserializer.deserialize_str(BorrowedStrVisitor)
    }
}

/// Primitive JSON value observed during streaming deserialization.
#[derive(Clone, Copy, Debug)]
pub enum ObservedPrimitive<'a> {
    /// JSON `null`.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// 64-bit floating-point number.
    F64(f64),
    /// String slice (borrowed from JSON input when unescaped).
    Str(&'a str),
}

/// Optional observer notified during single-pass streaming JSON parsing (used by document
/// clustering fingerprinting to avoid a second pass over a materialized DOM).
pub trait StreamingJsonObserver {
    /// Returns `true` if the observer needs exact values for the given root-level key.
    fn wants_root_value(&self, _key: &str) -> bool {
        false
    }

    /// Called when a root-level non-object primitive value is parsed.
    fn on_root_primitive(&mut self, _key: &str, _value: ObservedPrimitive<'_>) {}

    /// Called when a root-level array is parsed.
    fn on_root_array(&mut self, _key: &str) {}

    /// Called when entering a JSON object child under `parent_hash` with `key`.
    /// Returns the child path hash passed to nested calls.
    fn on_object_enter(&mut self, _parent_hash: u64, _key: &str) -> u64 {
        0
    }

    /// Called when a leaf value is encountered inside a nested object.
    fn on_nested_leaf(&mut self, _leaf_hash: u64) {}

    /// Initial seed hash for the root object.
    fn root_hash_seed(&self) -> u64 {
        0
    }
}

impl StreamingJsonObserver for () {}

/// Outcome of a streaming JSON parse attempt.
pub(crate) enum StreamingParseOutcome {
    /// Successfully parsed directly into `document`.
    Ok,
    /// Encountered a valid JSON structure requiring the DOM fallback (e.g., an object/array on a
    /// root key observed for raw fingerprinting).
    FallbackRequired,
    /// Document failed schema validation or JSON syntax validation.
    Err(DocParsingError),
}

/// Streams a JSON object string directly into `document` while notifying `observer`, reusing
/// `scratch` across documents to avoid per-document scratch buffer setup.
///
/// Precondition: caller verified that the `DocMapper` has no partition key, no `_source` field,
/// no concatenate fields, and no nested mapped `Object` nodes.
pub(crate) fn stream_json_str_into_doc<O: StreamingJsonObserver>(
    json_str: &str,
    field_mappings: &MappingNode,
    mode: ModeType,
    dynamic_field: Option<Field>,
    document: &mut Document,
    scratch: &mut Vec<u8>,
    observer: &mut O,
) -> StreamingParseOutcome {
    let mut deserializer = serde_json::Deserializer::from_str(json_str);
    let mut writer = document.value_writer_with_scratch(scratch);
    let mut state = StreamState {
        doc_error: None,
        fallback_required: false,
    };
    let seed = RootObjectSeed {
        field_mappings,
        mode,
        dynamic_field,
        writer: &mut writer,
        observer,
        state: &mut state,
    };
    match seed.deserialize(&mut deserializer) {
        Ok(()) => {
            if deserializer.end().is_err() {
                let sample: String = json_str.chars().take(20).chain("...".chars()).collect();
                return StreamingParseOutcome::Err(DocParsingError::NotJsonObject(sample));
            }
            StreamingParseOutcome::Ok
        }
        Err(_) => {
            if state.fallback_required {
                return StreamingParseOutcome::FallbackRequired;
            }
            if let Some(doc_err) = state.doc_error.take() {
                return StreamingParseOutcome::Err(doc_err);
            }
            let sample: String = json_str.chars().take(20).chain("...".chars()).collect();
            StreamingParseOutcome::Err(DocParsingError::NotJsonObject(sample))
        }
    }
}

pub(super) struct StreamState {
    doc_error: Option<DocParsingError>,
    fallback_required: bool,
}

impl StreamState {
    #[cold]
    pub(super) fn fail<E: de::Error>(&mut self, err: DocParsingError) -> E {
        self.doc_error = Some(err);
        de::Error::custom("doc mapping error")
    }

    #[cold]
    pub(super) fn request_fallback<E: de::Error>(&mut self) -> E {
        self.fallback_required = true;
        de::Error::custom("fallback required")
    }
}

struct RootObjectSeed<'a, 'doc, 's, O> {
    field_mappings: &'a MappingNode,
    mode: ModeType,
    dynamic_field: Option<Field>,
    writer: &'a mut CompactDocValueWriter<'doc, 's>,
    observer: &'a mut O,
    state: &'a mut StreamState,
}

impl<'de, O: StreamingJsonObserver> DeserializeSeed<'de> for RootObjectSeed<'_, '_, '_, O> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de, O: StreamingJsonObserver> Visitor<'de> for RootObjectSeed<'_, '_, '_, O> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let dynamic_start = self.writer.begin_object();
        let root_seed = self.observer.root_hash_seed();
        let strict_mode = self.mode == ModeType::Strict;

        while let Some(key) = map.next_key::<BorrowedStr<'de>>()? {
            let key_str: &str = key.as_str();
            if let Some(child_tree) = self.field_mappings.branches.get(key_str) {
                let MappingTree::Leaf(leaf) = child_tree else {
                    return Err(self.state.request_fallback());
                };
                map.next_value_seed(MappedLeafSeed {
                    key: key_str,
                    leaf,
                    strict_mode,
                    root_seed,
                    writer: self.writer,
                    observer: self.observer,
                    state: self.state,
                })?;
            } else {
                match self.mode {
                    ModeType::Strict => {
                        return Err(self
                            .state
                            .fail(DocParsingError::NoSuchFieldInSchema(key_str.to_string())));
                    }
                    ModeType::Lenient => {
                        if self.observer.wants_root_value(key_str) {
                            return Err(self.state.request_fallback());
                        }
                        map.next_value_seed(ObserveOnlySeed {
                            parent_hash: root_seed,
                            key: Some(key_str),
                            is_root: true,
                            observer: self.observer,
                        })?;
                    }
                    ModeType::Dynamic => {
                        let key_addr = self.writer.write_key(key_str);
                        let val_addr = map.next_value_seed(DynamicValueSeed {
                            parent_hash: root_seed,
                            key: Some(key_str),
                            is_root: true,
                            writer: self.writer,
                            observer: self.observer,
                            state: self.state,
                        })?;
                        self.writer.push_object_entry(key_addr, val_addr);
                    }
                }
            }
        }

        if let Some(dynamic_field) = self.dynamic_field
            && !self.writer.frame_is_empty(dynamic_start)
        {
            let dynamic_addr = self.writer.end_object(dynamic_start);
            self.writer.finish_field(dynamic_field, dynamic_addr);
        }
        Ok(())
    }
}

/// Streams a value for a mapped top-level [`MappingLeaf`] straight into `CompactDoc`.
struct MappedLeafSeed<'a, 'doc, 's, O> {
    key: &'a str,
    leaf: &'a MappingLeaf,
    strict_mode: bool,
    root_seed: u64,
    writer: &'a mut CompactDocValueWriter<'doc, 's>,
    observer: &'a mut O,
    state: &'a mut StreamState,
}

impl<'de, O: StreamingJsonObserver> DeserializeSeed<'de> for MappedLeafSeed<'_, '_, '_, O> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de, O: StreamingJsonObserver> Visitor<'de> for MappedLeafSeed<'_, '_, '_, O> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a field value")
    }

    #[inline]
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.observer
            .on_root_primitive(self.key, ObservedPrimitive::Null);
        if matches!(self.leaf.typ, LeafType::TieBreaker) {
            return check_supplied_tie_breaker_value(true, self.strict_mode, self.key.to_string())
                .map_err(|err| self.state.fail(err));
        }
        Ok(())
    }

    #[inline]
    fn visit_bool<E: de::Error>(self, val: bool) -> Result<(), E> {
        self.observer
            .on_root_primitive(self.key, ObservedPrimitive::Bool(val));
        add_leaf_bool(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    #[inline]
    fn visit_i64<E: de::Error>(self, val: i64) -> Result<(), E> {
        self.observer
            .on_root_primitive(self.key, ObservedPrimitive::I64(val));
        add_leaf_i64(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    #[inline]
    fn visit_u64<E: de::Error>(self, val: u64) -> Result<(), E> {
        self.observer
            .on_root_primitive(self.key, ObservedPrimitive::U64(val));
        add_leaf_u64(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    #[inline]
    fn visit_f64<E: de::Error>(self, val: f64) -> Result<(), E> {
        self.observer
            .on_root_primitive(self.key, ObservedPrimitive::F64(val));
        add_leaf_f64(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    #[inline]
    fn visit_borrowed_str<E: de::Error>(self, val: &'de str) -> Result<(), E> {
        self.observer
            .on_root_primitive(self.key, ObservedPrimitive::Str(val));
        add_leaf_str(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    #[inline]
    fn visit_str<E: de::Error>(self, val: &str) -> Result<(), E> {
        self.observer
            .on_root_primitive(self.key, ObservedPrimitive::Str(val));
        add_leaf_str(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        if self.observer.wants_root_value(self.key) {
            return Err(self.state.request_fallback());
        }
        self.observer.on_root_array(self.key);
        if matches!(self.leaf.typ, LeafType::TieBreaker) {
            return check_supplied_tie_breaker_value(false, self.strict_mode, self.key.to_string())
                .map_err(|err| self.state.fail(err));
        }
        if self.leaf.cardinality == Cardinality::SingleValued {
            return Err(self.state.fail(DocParsingError::MultiValuesNotSupported(
                self.key.to_string(),
            )));
        }
        while seq
            .next_element_seed(ArrayElemLeafSeed {
                key: self.key,
                leaf: self.leaf,
                strict_mode: self.strict_mode,
                writer: self.writer,
                state: self.state,
            })?
            .is_some()
        {}
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        if self.observer.wants_root_value(self.key) {
            return Err(self.state.request_fallback());
        }
        if matches!(self.leaf.typ, LeafType::TieBreaker) {
            return check_supplied_tie_breaker_value(false, self.strict_mode, self.key.to_string())
                .map_err(|err| self.state.fail(err));
        }
        if !matches!(self.leaf.typ, LeafType::Json(_)) {
            return Err(self.state.request_fallback());
        }
        let child_hash = self.observer.on_object_enter(self.root_seed, self.key);
        let obj_addr =
            stream_map_into_compact_doc(map, child_hash, self.writer, self.observer, self.state)?;
        self.writer.finish_field(self.leaf.field, obj_addr);
        Ok(())
    }
}
