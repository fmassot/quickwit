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

//! Serde seeds for streaming dynamic JSON subtrees, multivalued leaf arrays, and lenient-mode
//! skipped subtrees into [`CompactDocValueWriter`].

use std::fmt;

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use tantivy::schema::document::{
    CompactDocValueAddr, CompactDocValueWriter, ReferenceValueLeaf, try_parse_rfc3339_date_time,
};

use super::leaf_writers::{add_leaf_bool, add_leaf_f64, add_leaf_i64, add_leaf_str, add_leaf_u64};
use super::{BorrowedStr, ObservedPrimitive, StreamState, StreamingJsonObserver};
use crate::doc_mapper::mapping_tree::{LeafType, MappingLeaf};

/// Deserializes a single element inside a multivalued mapped leaf array.
pub(super) struct ArrayElemLeafSeed<'a, 'doc, 's> {
    pub(super) key: &'a str,
    pub(super) leaf: &'a MappingLeaf,
    pub(super) strict_mode: bool,
    pub(super) writer: &'a mut CompactDocValueWriter<'doc, 's>,
    pub(super) state: &'a mut StreamState,
}

impl<'de> DeserializeSeed<'de> for ArrayElemLeafSeed<'_, '_, '_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ArrayElemLeafSeed<'_, '_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a leaf array element")
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_bool<E: de::Error>(self, val: bool) -> Result<(), E> {
        add_leaf_bool(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    fn visit_i64<E: de::Error>(self, val: i64) -> Result<(), E> {
        add_leaf_i64(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    fn visit_u64<E: de::Error>(self, val: u64) -> Result<(), E> {
        add_leaf_u64(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    fn visit_f64<E: de::Error>(self, val: f64) -> Result<(), E> {
        add_leaf_f64(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    fn visit_str<E: de::Error>(self, val: &str) -> Result<(), E> {
        add_leaf_str(
            self.leaf,
            self.key,
            self.strict_mode,
            val,
            self.writer.doc_mut(),
        )
        .map_err(|err| self.state.fail(err))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, _seq: A) -> Result<(), A::Error> {
        Err(self.state.request_fallback())
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        if !matches!(self.leaf.typ, LeafType::Json(_)) {
            return Err(self.state.request_fallback());
        }
        let mut noop = ();
        let obj_addr = stream_map_into_compact_doc(map, 0, self.writer, &mut noop, self.state)?;
        self.writer.finish_field(self.leaf.field, obj_addr);
        Ok(())
    }
}

/// Streams any dynamic JSON value straight into `CompactDocValueWriter` while notifying `observer`.
pub(super) struct DynamicValueSeed<'a, 'doc, 's, O> {
    pub(super) parent_hash: u64,
    pub(super) key: Option<&'a str>,
    pub(super) is_root: bool,
    pub(super) writer: &'a mut CompactDocValueWriter<'doc, 's>,
    pub(super) observer: &'a mut O,
    pub(super) state: &'a mut StreamState,
}

impl<'de, O: StreamingJsonObserver> DeserializeSeed<'de> for DynamicValueSeed<'_, '_, '_, O> {
    type Value = CompactDocValueAddr;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<CompactDocValueAddr, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<O: StreamingJsonObserver> DynamicValueSeed<'_, '_, '_, O> {
    #[inline(always)]
    fn notify_primitive(&mut self, val: ObservedPrimitive<'_>) {
        if self.is_root {
            if let Some(key) = self.key {
                self.observer.on_root_primitive(key, val);
            }
        } else if let Some(key) = self.key {
            let leaf_hash = self.observer.on_object_enter(self.parent_hash, key);
            self.observer.on_nested_leaf(leaf_hash);
        }
    }
}

impl<'de, O: StreamingJsonObserver> Visitor<'de> for DynamicValueSeed<'_, '_, '_, O> {
    type Value = CompactDocValueAddr;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    #[inline]
    fn visit_unit<E: de::Error>(mut self) -> Result<CompactDocValueAddr, E> {
        self.notify_primitive(ObservedPrimitive::Null);
        Ok(self.writer.write_leaf(ReferenceValueLeaf::Null))
    }

    #[inline]
    fn visit_bool<E: de::Error>(mut self, val: bool) -> Result<CompactDocValueAddr, E> {
        self.notify_primitive(ObservedPrimitive::Bool(val));
        Ok(self.writer.write_leaf(ReferenceValueLeaf::Bool(val)))
    }

    #[inline]
    fn visit_i64<E: de::Error>(mut self, val: i64) -> Result<CompactDocValueAddr, E> {
        self.notify_primitive(ObservedPrimitive::I64(val));
        Ok(self.writer.write_leaf(ReferenceValueLeaf::I64(val)))
    }

    #[inline]
    fn visit_u64<E: de::Error>(mut self, val: u64) -> Result<CompactDocValueAddr, E> {
        self.notify_primitive(ObservedPrimitive::U64(val));
        // Match `OwnedValue::from(serde_json::Value)` which prefers `I64` when it fits.
        if val <= i64::MAX as u64 {
            Ok(self.writer.write_leaf(ReferenceValueLeaf::I64(val as i64)))
        } else {
            Ok(self.writer.write_leaf(ReferenceValueLeaf::U64(val)))
        }
    }

    #[inline]
    fn visit_f64<E: de::Error>(mut self, val: f64) -> Result<CompactDocValueAddr, E> {
        self.notify_primitive(ObservedPrimitive::F64(val));
        Ok(self.writer.write_leaf(ReferenceValueLeaf::F64(val)))
    }

    #[inline]
    fn visit_borrowed_str<E: de::Error>(mut self, val: &'de str) -> Result<CompactDocValueAddr, E> {
        self.notify_primitive(ObservedPrimitive::Str(val));
        let leaf = match try_parse_rfc3339_date_time(val) {
            Some(dt) => ReferenceValueLeaf::Date(dt),
            None => ReferenceValueLeaf::Str(val),
        };
        Ok(self.writer.write_leaf(leaf))
    }

    #[inline]
    fn visit_str<E: de::Error>(mut self, val: &str) -> Result<CompactDocValueAddr, E> {
        self.notify_primitive(ObservedPrimitive::Str(val));
        let leaf = match try_parse_rfc3339_date_time(val) {
            Some(dt) => ReferenceValueLeaf::Date(dt),
            None => ReferenceValueLeaf::Str(val),
        };
        Ok(self.writer.write_leaf(leaf))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<CompactDocValueAddr, A::Error> {
        if self.is_root {
            if let Some(key) = self.key {
                if self.observer.wants_root_value(key) {
                    return Err(self.state.request_fallback());
                }
                self.observer.on_root_array(key);
            }
        } else if let Some(key) = self.key {
            let leaf_hash = self.observer.on_object_enter(self.parent_hash, key);
            self.observer.on_nested_leaf(leaf_hash);
        }
        let arr_start = self.writer.begin_array();
        let mut noop = ();
        while let Some(elem_addr) = seq.next_element_seed(DynamicValueSeed {
            parent_hash: 0,
            key: None,
            is_root: false,
            writer: self.writer,
            observer: &mut noop,
            state: self.state,
        })? {
            self.writer.push_array_elem(elem_addr);
        }
        Ok(self.writer.end_array(arr_start))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<CompactDocValueAddr, A::Error> {
        let child_hash = if let Some(key) = self.key {
            if self.is_root && self.observer.wants_root_value(key) {
                return Err(self.state.request_fallback());
            }
            self.observer.on_object_enter(self.parent_hash, key)
        } else {
            self.parent_hash
        };
        stream_map_into_compact_doc(map, child_hash, self.writer, self.observer, self.state)
    }
}

#[inline]
pub(super) fn stream_map_into_compact_doc<'de, A: MapAccess<'de>, O: StreamingJsonObserver>(
    mut map: A,
    obj_hash: u64,
    writer: &mut CompactDocValueWriter<'_, '_>,
    observer: &mut O,
    state: &mut StreamState,
) -> Result<CompactDocValueAddr, A::Error> {
    let obj_start = writer.begin_object();
    while let Some(child_key) = map.next_key::<BorrowedStr<'de>>()? {
        let child_key_str = child_key.as_str();
        let key_addr = writer.write_key(child_key_str);
        let val_addr = map.next_value_seed(DynamicValueSeed {
            parent_hash: obj_hash,
            key: Some(child_key_str),
            is_root: false,
            writer,
            observer,
            state,
        })?;
        writer.push_object_entry(key_addr, val_addr);
    }
    Ok(writer.end_object(obj_start))
}

/// Skips an unmapped value in lenient mode while still reporting its structure to `observer`.
pub(super) struct ObserveOnlySeed<'a, O> {
    pub(super) parent_hash: u64,
    pub(super) key: Option<&'a str>,
    pub(super) is_root: bool,
    pub(super) observer: &'a mut O,
}

impl<'de, O: StreamingJsonObserver> DeserializeSeed<'de> for ObserveOnlySeed<'_, O> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<O: StreamingJsonObserver> ObserveOnlySeed<'_, O> {
    #[inline]
    fn notify_leaf(&mut self) {
        if let Some(key) = self.key {
            if self.is_root {
                self.observer
                    .on_root_primitive(key, ObservedPrimitive::Null);
            } else {
                let leaf_hash = self.observer.on_object_enter(self.parent_hash, key);
                self.observer.on_nested_leaf(leaf_hash);
            }
        }
    }
}

impl<'de, O: StreamingJsonObserver> Visitor<'de> for ObserveOnlySeed<'_, O> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_unit<E: de::Error>(mut self) -> Result<(), E> {
        self.notify_leaf();
        Ok(())
    }
    fn visit_bool<E: de::Error>(mut self, _: bool) -> Result<(), E> {
        self.notify_leaf();
        Ok(())
    }
    fn visit_i64<E: de::Error>(mut self, _: i64) -> Result<(), E> {
        self.notify_leaf();
        Ok(())
    }
    fn visit_u64<E: de::Error>(mut self, _: u64) -> Result<(), E> {
        self.notify_leaf();
        Ok(())
    }
    fn visit_f64<E: de::Error>(mut self, _: f64) -> Result<(), E> {
        self.notify_leaf();
        Ok(())
    }
    fn visit_str<E: de::Error>(mut self, _: &str) -> Result<(), E> {
        self.notify_leaf();
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        self.notify_leaf();
        while seq.next_element::<de::IgnoredAny>()?.is_some() {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let obj_hash = match self.key {
            Some(key) => self.observer.on_object_enter(self.parent_hash, key),
            None => self.parent_hash,
        };
        while let Some(child_key) = map.next_key::<BorrowedStr<'de>>()? {
            map.next_value_seed(ObserveOnlySeed {
                parent_hash: obj_hash,
                key: Some(child_key.as_str()),
                is_root: false,
                observer: self.observer,
            })?;
        }
        Ok(())
    }
}
