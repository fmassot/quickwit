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

//! Fingerprint computation.
//!
//! A fingerprint contains one hash for each configured fingerprint policy, in configuration order.
//! Each policy can combine:
//! 1. structure fields, represented by sorted sets of leaf JSON paths;
//! 2. raw fields, represented by their exact JSON values;
//! 3. tokenized fields, represented by their token types.
//!
//! ```text
//! JSON document
//!     |
//!     +--> policy 0 fields --> hash 0
//!     +--> policy 1 fields --> hash 1
//!     +--> ...
//!     +--> policy N fields --> hash N
//!     |
//!     v
//! Fingerprint [hash 0, hash 1, ..., hash N]
//! ```
//!
//! Tokenized fields use the sequence of token types as a lightweight message template. This keeps
//! the stable structure of a value while ignoring volatile literals such as IDs, ports, UUIDs, or
//! IP addresses. By default, the first 50 tokens contribute to the fingerprint; `max_tokens` can
//! override this limit for each tokenized method.
//!
//! Examples:
//! ```text
//! "server started at 8080"
//!     -> Word Gap Word Gap Word Gap Number
//!
//! "server started at 9090"
//!     -> Word Gap Word Gap Word Gap Number
//! ```
//!
//! These two values produce the same tokenized signature and policy hash, so they can be ordered
//! together. A different shape produces a different signature:
//!
//! ```text
//! "connection from 1.2.3.4"
//!     -> Word Gap Word Gap IPv4
//! ```
//!
//! Raw fields preserve JSON types and nested structure, which is useful for dimensions such as
//! `service` or numeric status codes. Missing fields are encoded as absent so configured field
//! positions remain distinct within a policy hash.
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::Arc;

use fnv::FnvHasher;
use quickwit_config::{
    ClusteringMethod, ClusteringPolicy, DocsClusteringConfig, FingerprintPolicy, JsonPath,
};
use quickwit_doc_mapper::{ObservedPrimitive, StreamingJsonObserver};
use serde_json::Value as JsonValue;
use serde_json_borrow::{Map as BorrowedJsonMap, Value as BorrowedJsonValue};
use smallvec::SmallVec;

use super::tokenize;

const PATH_COMPONENT_SEPARATOR: u8 = 0xF9;
const FIELD_ABSENT: u8 = 0xFA;
const FIELD_PRESENT: u8 = 0xFB;
const PATH_SEPARATOR: u8 = 0xFC;
const FIELD_BOUNDARY: u8 = 0xFD;
const TOKENIZED_TOKEN_SEPARATOR: u8 = 0xFE;
const DEFAULT_MAX_GROUPING_TOKENS: usize = 50;

// Inline 4 hashes to avoid heap allocations.
// This is usually enough for most use cases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fingerprint(SmallVec<[u64; 4]>);

impl Deref for Fingerprint {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
impl Fingerprint {
    pub(crate) fn new<const N: usize>(hashes: [u64; N]) -> Self {
        let mut fingerprint = SmallVec::new();
        fingerprint.extend_from_slice(&hashes);
        Self(fingerprint)
    }
}

#[derive(Clone)]
enum StreamingPolicySpec {
    Structure,
    RawRoot { field: String },
    TokenizedRoot { field: String, max_tokens: usize },
}

#[derive(Clone)]
pub struct Fingerprinter {
    config: Arc<DocsClusteringConfig>,
    policies: Arc<[FingerprintPolicy]>,
    streaming_specs_opt: Option<Arc<[StreamingPolicySpec]>>,
}

impl Fingerprinter {
    pub fn new(config: &DocsClusteringConfig) -> Self {
        let mut policies = Vec::new();
        let mut streaming_specs = Vec::new();
        let mut all_streamable = true;
        for policy in &config.policies {
            match policy {
                ClusteringPolicy::Fingerprint { fingerprint } => {
                    if fingerprint.fingerprint.len() == 1 {
                        match &fingerprint.fingerprint[0] {
                            ClusteringMethod::Structure { exclude } if exclude.is_empty() => {
                                streaming_specs.push(StreamingPolicySpec::Structure);
                            }
                            ClusteringMethod::Raw { path } if path.len() == 1 => {
                                streaming_specs.push(StreamingPolicySpec::RawRoot {
                                    field: path[0].clone(),
                                });
                            }
                            ClusteringMethod::Tokenized { path, max_tokens } if path.len() == 1 => {
                                streaming_specs.push(StreamingPolicySpec::TokenizedRoot {
                                    field: path[0].clone(),
                                    max_tokens: max_tokens.unwrap_or(DEFAULT_MAX_GROUPING_TOKENS),
                                });
                            }
                            _ => all_streamable = false,
                        }
                    } else {
                        all_streamable = false;
                    }
                    policies.push(fingerprint.clone());
                }
            }
        }

        let streaming_specs_opt = all_streamable.then(|| streaming_specs.into_boxed_slice().into());
        Self {
            config: Arc::new(config.clone()),
            policies: policies.into_boxed_slice().into(),
            streaming_specs_opt,
        }
    }

    /// Creates a single-pass streaming observer if every configured policy can be evaluated
    /// directly during streaming JSON deserialization.
    pub fn streaming_observer(&self) -> Option<StreamingFingerprintObserver<'_>> {
        let specs = self.streaming_specs_opt.as_deref()?;
        let mut has_structure = false;
        let mut policy_hashes = SmallVec::with_capacity(specs.len());
        for spec in specs {
            if matches!(spec, StreamingPolicySpec::Structure) {
                has_structure = true;
            }
            policy_hashes.push(None);
        }
        Some(StreamingFingerprintObserver {
            specs,
            has_structure,
            structure_acc: StructureSetHash::default(),
            policy_hashes,
        })
    }

    pub fn config(&self) -> &DocsClusteringConfig {
        &self.config
    }

    pub fn fingerprint(&self, json_value: &JsonValue) -> Fingerprint {
        let mut fingerprint = SmallVec::new();
        for policy in self.policies.iter() {
            let mut hasher = FnvHasher::default();

            for method in policy.fingerprint.iter() {
                match method {
                    ClusteringMethod::Structure { exclude } => {
                        self.hash_structure(json_value, exclude, &mut hasher);
                    }
                    ClusteringMethod::Raw { path } => {
                        self.hash_raw_value(json_value, path, &mut hasher);
                    }
                    ClusteringMethod::Tokenized { path, max_tokens } => {
                        self.hash_string_tokenized(json_value, path, *max_tokens, &mut hasher);
                    }
                }
            }

            fingerprint.push(hasher.finish());
        }

        Fingerprint(fingerprint)
    }

    /// Computes the exact same [`Fingerprint`] directly on a borrowed JSON object without
    /// allocating an owned `serde_json::Value` tree.
    pub fn fingerprint_borrowed(&self, json_obj: &BorrowedJsonMap<'_>) -> Fingerprint {
        let mut fingerprint = SmallVec::new();
        for policy in self.policies.iter() {
            let mut hasher = FnvHasher::default();

            for method in policy.fingerprint.iter() {
                match method {
                    ClusteringMethod::Structure { exclude } => {
                        self.hash_structure_borrowed(json_obj, exclude, &mut hasher);
                    }
                    ClusteringMethod::Raw { path } => {
                        self.hash_raw_value_borrowed(json_obj, path, &mut hasher);
                    }
                    ClusteringMethod::Tokenized { path, max_tokens } => {
                        self.hash_string_tokenized_borrowed(
                            json_obj,
                            path,
                            *max_tokens,
                            &mut hasher,
                        );
                    }
                }
            }

            fingerprint.push(hasher.finish());
        }

        Fingerprint(fingerprint)
    }

    fn hash_structure(&self, value: &JsonValue, exclude: &[JsonPath], hasher: &mut FnvHasher) {
        fn walk_no_exclude(json_value: &JsonValue, parent_hash: u64, acc: &mut StructureSetHash) {
            match json_value {
                JsonValue::Object(obj) => {
                    for (key, child_value) in obj.iter() {
                        let child_hash = hash_path_component(parent_hash, key.as_str());
                        walk_no_exclude(child_value, child_hash, acc);
                    }
                }
                _ => acc.insert(parent_hash),
            }
        }

        fn walk_with_exclude<'a>(
            json_value: &'a JsonValue,
            exclude: &[JsonPath],
            parent_hash: u64,
            current: &mut SmallVec<[&'a str; 16]>,
            acc: &mut StructureSetHash,
        ) {
            match json_value {
                JsonValue::Object(obj) => {
                    for (key, child_value) in obj.iter() {
                        let key_str = key.as_str();
                        current.push(key_str);
                        if !is_path_excluded(exclude, current) {
                            let child_hash = hash_path_component(parent_hash, key_str);
                            walk_with_exclude(child_value, exclude, child_hash, current, acc);
                        }
                        current.pop();
                    }
                }
                _ => acc.insert(parent_hash),
            }
        }

        let mut acc = StructureSetHash::default();
        if exclude.is_empty() {
            walk_no_exclude(value, STRUCTURE_HASH_SEED, &mut acc);
        } else {
            let mut current: SmallVec<[&str; 16]> = SmallVec::new();
            walk_with_exclude(value, exclude, STRUCTURE_HASH_SEED, &mut current, &mut acc);
        }
        acc.finish_into(hasher);
    }

    fn hash_structure_borrowed(
        &self,
        json_obj: &BorrowedJsonMap<'_>,
        exclude: &[JsonPath],
        hasher: &mut FnvHasher,
    ) {
        fn walk_obj_no_exclude(
            obj: &BorrowedJsonMap<'_>,
            parent_hash: u64,
            acc: &mut StructureSetHash,
        ) {
            for (key, child_value) in obj.iter() {
                let child_hash = hash_path_component(parent_hash, key);
                match child_value {
                    BorrowedJsonValue::Object(child_obj) => {
                        walk_obj_no_exclude(child_obj, child_hash, acc);
                    }
                    _ => acc.insert(child_hash),
                }
            }
        }

        fn walk_obj_with_exclude<'a>(
            obj: &'a BorrowedJsonMap<'a>,
            exclude: &[JsonPath],
            parent_hash: u64,
            current: &mut SmallVec<[&'a str; 16]>,
            acc: &mut StructureSetHash,
        ) {
            for (key, child_value) in obj.iter() {
                current.push(key);
                if !is_path_excluded(exclude, current) {
                    let child_hash = hash_path_component(parent_hash, key);
                    match child_value {
                        BorrowedJsonValue::Object(child_obj) => {
                            walk_obj_with_exclude(child_obj, exclude, child_hash, current, acc);
                        }
                        _ => acc.insert(child_hash),
                    }
                }
                current.pop();
            }
        }

        let mut acc = StructureSetHash::default();
        if exclude.is_empty() {
            walk_obj_no_exclude(json_obj, STRUCTURE_HASH_SEED, &mut acc);
        } else {
            let mut current: SmallVec<[&str; 16]> = SmallVec::new();
            walk_obj_with_exclude(
                json_obj,
                exclude,
                STRUCTURE_HASH_SEED,
                &mut current,
                &mut acc,
            );
        }
        acc.finish_into(hasher);
    }

    fn hash_raw_value_borrowed(
        &self,
        json_obj: &BorrowedJsonMap<'_>,
        path: &JsonPath,
        hasher: &mut FnvHasher,
    ) {
        fn hash_raw_borrowed_inner(json_value: &BorrowedJsonValue<'_>, hasher: &mut FnvHasher) {
            const RAW_NULL: u8 = 0;
            const RAW_BOOL: u8 = 1;
            const RAW_NUMBER: u8 = 2;
            const RAW_STRING: u8 = 3;
            const RAW_ARRAY: u8 = 4;
            const RAW_OBJECT: u8 = 5;

            match json_value {
                BorrowedJsonValue::Null => hasher.write_u8(RAW_NULL),
                BorrowedJsonValue::Bool(value) => {
                    hasher.write_u8(RAW_BOOL);
                    hasher.write_u8(*value as u8);
                }
                BorrowedJsonValue::Number(value) => {
                    hasher.write_u8(RAW_NUMBER);
                    let number = value.to_string();
                    hasher.write(number.as_bytes());
                }
                BorrowedJsonValue::Str(value) => {
                    hasher.write_u8(RAW_STRING);
                    hasher.write_usize(value.len());
                    hasher.write(value.as_bytes());
                }
                BorrowedJsonValue::Array(values) => {
                    hasher.write_u8(RAW_ARRAY);
                    hasher.write_usize(values.len());
                    for value in values {
                        hash_raw_borrowed_inner(value, hasher);
                    }
                }
                BorrowedJsonValue::Object(map) => {
                    let mut entries: SmallVec<[(&str, &BorrowedJsonValue<'_>); 16]> =
                        map.iter().collect();
                    entries.sort_unstable_by_key(|(key, _)| *key);
                    hasher.write_u8(RAW_OBJECT);
                    hasher.write_usize(entries.len());
                    for (key, value) in entries {
                        hasher.write_usize(key.len());
                        hasher.write(key.as_bytes());
                        hash_raw_borrowed_inner(value, hasher);
                    }
                }
            }
        }

        let Some(json_value) = get_leaf_borrowed_value(json_obj, path) else {
            hasher.write_u8(FIELD_ABSENT);
            hasher.write_u8(FIELD_BOUNDARY);
            return;
        };
        hasher.write_u8(FIELD_PRESENT);
        hash_raw_borrowed_inner(json_value, hasher);
        hasher.write_u8(FIELD_BOUNDARY);
    }

    fn hash_string_tokenized_borrowed(
        &self,
        json_obj: &BorrowedJsonMap<'_>,
        path: &JsonPath,
        max_tokens: Option<usize>,
        hasher: &mut FnvHasher,
    ) {
        let Some(value) = get_leaf_borrowed_value(json_obj, path).and_then(|val| val.as_str())
        else {
            hasher.write_u8(FIELD_ABSENT);
            hasher.write_u8(FIELD_BOUNDARY);
            return;
        };
        hasher.write_u8(FIELD_PRESENT);

        let max_tokens = max_tokens.unwrap_or(DEFAULT_MAX_GROUPING_TOKENS);
        for span in tokenize(value).take(max_tokens) {
            hasher.write_u8(span.token_type as u8);
            hasher.write_u8(TOKENIZED_TOKEN_SEPARATOR);
        }

        hasher.write_u8(FIELD_BOUNDARY);
    }

    fn hash_raw_value(&self, json_value: &JsonValue, path: &JsonPath, hasher: &mut FnvHasher) {
        fn hash_raw_value_inner(json_value: &JsonValue, hasher: &mut FnvHasher) {
            const RAW_NULL: u8 = 0;
            const RAW_BOOL: u8 = 1;
            const RAW_NUMBER: u8 = 2;
            const RAW_STRING: u8 = 3;
            const RAW_ARRAY: u8 = 4;
            const RAW_OBJECT: u8 = 5;

            match json_value {
                JsonValue::Null => hasher.write_u8(RAW_NULL),
                JsonValue::Bool(value) => {
                    hasher.write_u8(RAW_BOOL);
                    hasher.write_u8(*value as u8);
                }
                JsonValue::Number(value) => {
                    hasher.write_u8(RAW_NUMBER);
                    let number = value.to_string();
                    hasher.write(number.as_bytes());
                }
                JsonValue::String(value) => {
                    hasher.write_u8(RAW_STRING);
                    hasher.write_usize(value.len());
                    hasher.write(value.as_bytes());
                }
                JsonValue::Array(values) => {
                    hasher.write_u8(RAW_ARRAY);
                    hasher.write_usize(values.len());
                    for value in values {
                        hash_raw_value_inner(value, hasher);
                    }
                }
                JsonValue::Object(map) => {
                    hasher.write_u8(RAW_OBJECT);
                    hasher.write_usize(map.len());
                    for (key, value) in map.iter() {
                        hasher.write_usize(key.len());
                        hasher.write(key.as_bytes());
                        hash_raw_value_inner(value, hasher);
                    }
                }
            }
        }

        let Some(json_value) = get_leaf_json_value(json_value, path) else {
            hasher.write_u8(FIELD_ABSENT);
            hasher.write_u8(FIELD_BOUNDARY);
            return;
        };
        hasher.write_u8(FIELD_PRESENT);
        hash_raw_value_inner(json_value, hasher);
        hasher.write_u8(FIELD_BOUNDARY);
    }

    fn hash_string_tokenized(
        &self,
        json_value: &JsonValue,
        path: &JsonPath,
        max_tokens: Option<usize>,
        hasher: &mut FnvHasher,
    ) {
        let Some(value) = get_leaf_string(json_value, path) else {
            hasher.write_u8(FIELD_ABSENT);
            hasher.write_u8(FIELD_BOUNDARY);
            return;
        };
        hasher.write_u8(FIELD_PRESENT);

        let max_tokens = max_tokens.unwrap_or(DEFAULT_MAX_GROUPING_TOKENS);
        for span in tokenize(value).take(max_tokens) {
            hasher.write_u8(span.token_type as u8);
            hasher.write_u8(TOKENIZED_TOKEN_SEPARATOR);
        }

        hasher.write_u8(FIELD_BOUNDARY);
    }
}

/// Single-pass streaming fingerprint observer that computes the exact same [`Fingerprint`] as
/// [`Fingerprinter::fingerprint`] while [`quickwit_doc_mapper::DocMapper`] streams JSON directly
/// into a `CompactDoc`.
pub struct StreamingFingerprintObserver<'a> {
    specs: &'a [StreamingPolicySpec],
    has_structure: bool,
    structure_acc: StructureSetHash,
    policy_hashes: SmallVec<[Option<u64>; 4]>,
}

impl StreamingFingerprintObserver<'_> {
    /// Finalizes all policy hashes into a [`Fingerprint`].
    pub fn finish(self) -> Fingerprint {
        let mut structure_hash_opt: Option<u64> = None;
        let mut absent_hash_opt: Option<u64> = None;
        let mut fingerprint = SmallVec::with_capacity(self.specs.len());
        for (idx, spec) in self.specs.iter().enumerate() {
            let hash = match spec {
                StreamingPolicySpec::Structure => *structure_hash_opt.get_or_insert_with(|| {
                    let mut hasher = FnvHasher::default();
                    self.structure_acc.finish_into(&mut hasher);
                    hasher.finish()
                }),
                StreamingPolicySpec::RawRoot { .. } | StreamingPolicySpec::TokenizedRoot { .. } => {
                    self.policy_hashes[idx].unwrap_or_else(|| {
                        *absent_hash_opt.get_or_insert_with(|| {
                            let mut hasher = FnvHasher::default();
                            hasher.write_u8(FIELD_ABSENT);
                            hasher.write_u8(FIELD_BOUNDARY);
                            hasher.finish()
                        })
                    })
                }
            };
            fingerprint.push(hash);
        }
        Fingerprint(fingerprint)
    }
}

impl StreamingJsonObserver for StreamingFingerprintObserver<'_> {
    #[inline]
    fn root_hash_seed(&self) -> u64 {
        STRUCTURE_HASH_SEED
    }

    #[inline]
    fn wants_root_value(&self, key: &str) -> bool {
        self.specs.iter().any(|spec| match spec {
            StreamingPolicySpec::RawRoot { field } => field == key,
            StreamingPolicySpec::TokenizedRoot { field, .. } => field == key,
            StreamingPolicySpec::Structure => false,
        })
    }

    #[inline]
    fn on_root_primitive(&mut self, key: &str, value: ObservedPrimitive<'_>) {
        if self.has_structure {
            let leaf_hash = hash_path_component(STRUCTURE_HASH_SEED, key);
            self.structure_acc.insert(leaf_hash);
        }
        for (idx, spec) in self.specs.iter().enumerate() {
            match spec {
                StreamingPolicySpec::RawRoot { field } if field == key => {
                    let mut hasher = FnvHasher::default();
                    hasher.write_u8(FIELD_PRESENT);
                    hash_observed_raw_primitive(value, &mut hasher);
                    hasher.write_u8(FIELD_BOUNDARY);
                    self.policy_hashes[idx] = Some(hasher.finish());
                }
                StreamingPolicySpec::TokenizedRoot { field, max_tokens } if field == key => {
                    let mut hasher = FnvHasher::default();
                    if let ObservedPrimitive::Str(text) = value {
                        hasher.write_u8(FIELD_PRESENT);
                        for span in tokenize(text).take(*max_tokens) {
                            hasher.write_u8(span.token_type as u8);
                            hasher.write_u8(TOKENIZED_TOKEN_SEPARATOR);
                        }
                    } else {
                        hasher.write_u8(FIELD_ABSENT);
                    }
                    hasher.write_u8(FIELD_BOUNDARY);
                    self.policy_hashes[idx] = Some(hasher.finish());
                }
                _ => {}
            }
        }
    }

    #[inline]
    fn on_root_array(&mut self, key: &str) {
        if self.has_structure {
            let leaf_hash = hash_path_component(STRUCTURE_HASH_SEED, key);
            self.structure_acc.insert(leaf_hash);
        }
    }

    #[inline]
    fn on_object_enter(&mut self, parent_hash: u64, key: &str) -> u64 {
        if self.has_structure {
            hash_path_component(parent_hash, key)
        } else {
            0
        }
    }

    #[inline]
    fn on_nested_leaf(&mut self, leaf_hash: u64) {
        if self.has_structure {
            self.structure_acc.insert(leaf_hash);
        }
    }
}

#[inline]
fn hash_observed_raw_primitive(value: ObservedPrimitive<'_>, hasher: &mut FnvHasher) {
    const RAW_NULL: u8 = 0;
    const RAW_BOOL: u8 = 1;
    const RAW_NUMBER: u8 = 2;
    const RAW_STRING: u8 = 3;

    match value {
        ObservedPrimitive::Null => hasher.write_u8(RAW_NULL),
        ObservedPrimitive::Bool(bool_val) => {
            hasher.write_u8(RAW_BOOL);
            hasher.write_u8(bool_val as u8);
        }
        ObservedPrimitive::I64(num) => {
            hasher.write_u8(RAW_NUMBER);
            let number = num.to_string();
            hasher.write(number.as_bytes());
        }
        ObservedPrimitive::U64(num) => {
            hasher.write_u8(RAW_NUMBER);
            let number = num.to_string();
            hasher.write(number.as_bytes());
        }
        ObservedPrimitive::F64(num) => {
            hasher.write_u8(RAW_NUMBER);
            let number = serde_json::Number::from_f64(num)
                .map(|n| n.to_string())
                .unwrap_or_else(|| num.to_string());
            hasher.write(number.as_bytes());
        }
        ObservedPrimitive::Str(text) => {
            hasher.write_u8(RAW_STRING);
            hasher.write_usize(text.len());
            hasher.write(text.as_bytes());
        }
    }
}

const STRUCTURE_HASH_SEED: u64 = 0x517cc1b727220a95;
const FX_HASH_K: u64 = 0x517cc1b727220a95;

/// Commutative, order-independent accumulator for the set of leaf JSON paths in a document.
///
/// Each leaf path is hashed incrementally during DFS (reusing each parent object's prefix hash
/// across all its children), finalized with `splitmix64`, and folded into both a wrapping sum and
/// an XOR accumulator along with the leaf count. This avoids allocating or sorting path lists
/// while producing identical hashes regardless of JSON key ordering.
#[derive(Default)]
struct StructureSetHash {
    sum: u64,
    xor: u64,
    count: u64,
}

impl StructureSetHash {
    #[inline(always)]
    fn insert(&mut self, leaf_hash: u64) {
        let mixed = splitmix64(leaf_hash);
        self.sum = self.sum.wrapping_add(mixed);
        self.xor ^= mixed;
        self.count += 1;
    }

    #[inline(always)]
    fn finish_into(&self, hasher: &mut FnvHasher) {
        hasher.write_u64(self.sum);
        hasher.write_u64(self.xor);
        hasher.write_u64(self.count);
        hasher.write_u8(PATH_SEPARATOR);
    }
}

#[inline(always)]
fn splitmix64(mut val: u64) -> u64 {
    val = val.wrapping_add(0x9e3779b97f4a7c15);
    val = (val ^ (val >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    val = (val ^ (val >> 27)).wrapping_mul(0x94d049bb133111eb);
    val ^ (val >> 31)
}

/// Extends `parent_hash` with one path component using 8-byte word hashing and a component
/// separator so dotted keys (`"a.b"`) hash differently from nested paths (`["a", "b"]`).
#[inline(always)]
fn hash_path_component(mut state: u64, component: &str) -> u64 {
    let mut bytes = component.as_bytes();
    while bytes.len() >= 8 {
        let word = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        state = (state.rotate_left(5) ^ word).wrapping_mul(FX_HASH_K);
        bytes = &bytes[8..];
    }
    if bytes.len() >= 4 {
        let word = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64;
        state = (state.rotate_left(5) ^ word).wrapping_mul(FX_HASH_K);
        bytes = &bytes[4..];
    }
    if bytes.len() >= 2 {
        let word = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as u64;
        state = (state.rotate_left(5) ^ word).wrapping_mul(FX_HASH_K);
        bytes = &bytes[2..];
    }
    if let Some(&last_byte) = bytes.first() {
        state = (state.rotate_left(5) ^ (last_byte as u64)).wrapping_mul(FX_HASH_K);
    }
    // Mix component length and separator to keep component boundaries unambiguous.
    let boundary = ((component.len() as u64) << 8) | (PATH_COMPONENT_SEPARATOR as u64);
    (state.rotate_left(5) ^ boundary).wrapping_mul(FX_HASH_K)
}

#[inline]
fn is_path_excluded(exclude: &[JsonPath], path: &[&str]) -> bool {
    if exclude.is_empty() {
        return false;
    }
    exclude.iter().any(|excluded_path| {
        excluded_path.len() == path.len()
            && excluded_path
                .iter()
                .zip(path.iter())
                .all(|(excluded_component, component)| excluded_component.as_str() == *component)
    })
}

fn get_leaf_borrowed_value<'a>(
    json_obj: &'a BorrowedJsonMap<'a>,
    path: &[String],
) -> Option<&'a BorrowedJsonValue<'a>> {
    let (first, rest) = path.split_first()?;
    let mut current = json_obj.get(first.as_str())?;
    for segment in rest {
        let BorrowedJsonValue::Object(obj) = current else {
            return None;
        };
        current = obj.get(segment.as_str())?;
    }
    Some(current)
}

fn get_leaf_json_value<'a>(json_value: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    if path.is_empty() {
        return Some(json_value);
    }
    let JsonValue::Object(obj) = json_value else {
        return None;
    };
    get_leaf_json_value(obj.get(path.first()?)?, &path[1..])
}

fn get_leaf_string<'a>(json_value: &'a JsonValue, path: &[String]) -> Option<&'a str> {
    get_leaf_json_value(json_value, path)?.as_str()
}

#[cfg(test)]
mod tests {
    use quickwit_config::DocsClusteringConfig;
    use serde_json::Value as JsonValue;

    use super::Fingerprinter;

    fn parse(s: &str) -> JsonValue {
        serde_json::from_str(s).unwrap()
    }

    fn test_docs_clustering_config() -> DocsClusteringConfig {
        docs_clustering_config(serde_json::json!([
            {
                "fingerprint": [{
                    "kind": "structure",
                    "exclude": ["tag", "custom"]
                }]
            },
            {
                "fingerprint": [{
                    "path": "message",
                    "kind": "tokenized"
                },
                {
                    "path": "service",
                    "kind": "raw"
                }]
            }
        ]))
    }

    fn test_fingerprinter() -> Fingerprinter {
        let docs_clustering_config = test_docs_clustering_config();
        Fingerprinter::new(&docs_clustering_config)
    }

    fn docs_clustering_config(json_value: JsonValue) -> DocsClusteringConfig {
        serde_json::from_value(json_value).unwrap()
    }

    #[test]
    fn configured_fingerprinter_returns_config() {
        let docs_clustering_config = test_docs_clustering_config();
        let fingerprinter = test_fingerprinter();
        assert_eq!(fingerprinter.config(), &docs_clustering_config);
    }

    #[test]
    fn identical_logs_have_equal_fingerprint() {
        let fingerprinter = test_fingerprinter();
        let doc = parse(r#"{"message":"server started at 8080","service":"api"}"#);
        assert_eq!(
            fingerprinter.fingerprint(&doc),
            fingerprinter.fingerprint(&doc)
        );
    }

    #[test]
    fn dotted_key_and_nested_path_have_different_schema_fingerprints() {
        let fingerprinter = test_fingerprinter();
        let dotted_key_doc = parse(r#"{"a.b":1}"#);
        let nested_path_doc = parse(r#"{"a":{"b":1}}"#);
        let dotted_key_fingerprint = fingerprinter.fingerprint(&dotted_key_doc);
        let nested_path_fingerprint = fingerprinter.fingerprint(&nested_path_doc);

        assert_ne!(dotted_key_fingerprint[0], nested_path_fingerprint[0]);
        assert_eq!(dotted_key_fingerprint[1], nested_path_fingerprint[1]);
    }

    #[test]
    fn same_message_template_has_equal_fingerprint() {
        let fingerprinter = test_fingerprinter();
        let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
        let doc2 = parse(r#"{"message":"server started at 9090","service":"api"}"#);
        assert_eq!(
            fingerprinter.fingerprint(&doc1),
            fingerprinter.fingerprint(&doc2)
        );
    }

    #[test]
    fn different_message_template_changes_grouping_fingerprint_only() {
        let fingerprinter = test_fingerprinter();
        let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
        let doc2 = parse(r#"{"message":"connection from 1.2.3.4","service":"api"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
        assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
        assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
    }

    #[test]
    fn tokenized_field_respects_hardcoded_grouping_token_limit() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!([
            {
                "fingerprint": [{
                    "kind": "structure"
                }]
            },
            {
                "fingerprint": [{
                    "path": "message",
                    "kind": "tokenized"
                }]
            }
        ]));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config);
        let prefix = "alpha ".repeat(25);
        let doc1 = parse(&format!(r#"{{"message":"{prefix}123"}}"#));
        let doc2 = parse(&format!(r#"{{"message":"{prefix}beta"}}"#));
        assert_eq!(
            fingerprinter.fingerprint(&doc1)[1],
            fingerprinter.fingerprint(&doc2)[1]
        );
    }

    #[test]
    fn different_service_changes_grouping_fingerprint_only() {
        let fingerprinter = test_fingerprinter();
        let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
        let doc2 = parse(r#"{"message":"server started at 8080","service":"worker"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
        assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
        assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
    }

    #[test]
    fn ignored_custom_shape_does_not_change_fingerprint() {
        let fingerprinter = test_fingerprinter();
        let doc1 =
            parse(r#"{"message":"server started at 8080","service":"api","custom":{"a":1}}"#);
        let doc2 =
            parse(r#"{"message":"server started at 8080","service":"api","custom":{"b":2}}"#);
        assert_eq!(
            fingerprinter.fingerprint(&doc1),
            fingerprinter.fingerprint(&doc2)
        );
    }

    #[test]
    fn extra_non_ignored_shape_changes_schema_fingerprint_only() {
        let fingerprinter = test_fingerprinter();
        let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
        let doc2 = parse(r#"{"message":"server started at 8080","service":"api","host":"web-1"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
        assert_ne!(doc1_fingerprint[0], doc2_fingerprint[0]);
        assert_eq!(doc1_fingerprint[1], doc2_fingerprint[1]);
    }

    #[test]
    fn configured_raw_field_changes_grouping_fingerprint_only() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!([
            {
                "fingerprint": [{
                    "kind": "structure"
                }]
            },
            {
                "fingerprint": [{
                    "path": "host",
                    "kind": "raw"
                }]
            }
        ]));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config);
        let doc1 = parse(r#"{"message":"same","host":"web-1"}"#);
        let doc2 = parse(r#"{"message":"same","host":"web-2"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
        assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
        assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
    }

    #[test]
    fn raw_field_hashes_non_string_values() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!([
            {
                "fingerprint": [{
                    "kind": "structure"
                }]
            },
            {
                "fingerprint": [{
                    "path": "status",
                    "kind": "raw"
                }]
            }
        ]));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config);
        let doc1 = parse(r#"{"status":200}"#);
        let doc2 = parse(r#"{"status":500}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
        assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
        assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
    }

    #[test]
    fn raw_field_preserves_json_type_and_structure_boundaries() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!([
            {
                "fingerprint": [{
                    "path": "value",
                    "kind": "raw"
                }]
            }
        ]));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config);
        let docs = [
            parse(r#"{"value":"null"}"#),
            parse(r#"{"value":null}"#),
            parse(r#"{"value":""}"#),
            parse(r#"{"value":[]}"#),
            parse(r#"{"value":{}}"#),
            parse(r#"{"value":false}"#),
            parse(r#"{"value":-1}"#),
            parse(r#"{"value":18446744073709551615}"#),
        ];
        let fingerprints: Vec<_> = docs
            .iter()
            .map(|doc| fingerprinter.fingerprint(doc))
            .collect();

        for (left_idx, left_fingerprint) in fingerprints.iter().enumerate() {
            for right_fingerprint in &fingerprints[left_idx + 1..] {
                assert_ne!(left_fingerprint, right_fingerprint);
            }
        }
    }

    #[test]
    fn absent_grouping_values_preserve_field_position() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!([
            {
                "fingerprint": [{
                    "kind": "structure"
                }]
            },
            {
                "fingerprint": [{
                    "path": "a",
                    "kind": "raw"
                },
                {
                    "path": "b",
                    "kind": "raw"
                }]
            }
        ]));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config);
        let doc1 = parse(r#"{"a":"x","b":null}"#);
        let doc2 = parse(r#"{"a":null,"b":"x"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);

        assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
        assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
    }

    #[test]
    fn borrowed_and_owned_fingerprints_match_across_key_orders() {
        let fingerprinter = test_fingerprinter();
        let json1 = r#"{"message":"server started at 8080","service":"api","attrs":{"z":1,"a":"v","nested":{"k":true}},"custom":{"ignored":1}}"#;
        let json2 = r#"{"service":"api","attrs":{"nested":{"k":true},"a":"v","z":1},"message":"server started at 9090","custom":{"other":2}}"#;

        let owned1 = parse(json1);
        let owned2 = parse(json2);
        let borrowed1: serde_json_borrow::Value = serde_json::from_str(json1).unwrap();
        let borrowed2: serde_json_borrow::Value = serde_json::from_str(json2).unwrap();

        let fp_owned1 = fingerprinter.fingerprint(&owned1);
        let fp_owned2 = fingerprinter.fingerprint(&owned2);
        let fp_borrowed1 = fingerprinter.fingerprint_borrowed(borrowed1.as_object().unwrap());
        let fp_borrowed2 = fingerprinter.fingerprint_borrowed(borrowed2.as_object().unwrap());

        assert_eq!(fp_owned1, fp_borrowed1);
        assert_eq!(fp_owned2, fp_borrowed2);
        assert_eq!(fp_borrowed1, fp_borrowed2);
    }

    #[test]
    fn streaming_observer_matches_owned_and_borrowed_fingerprints() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!([
            { "fingerprint": [{ "kind": "structure" }] },
            { "fingerprint": [{ "path": "ServiceName", "kind": "raw" }] },
            { "fingerprint": [{ "path": "SeverityText", "kind": "raw" }] },
            { "fingerprint": [{ "path": "Body", "kind": "tokenized" }] }
        ]));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config);
        let doc_mapper: quickwit_doc_mapper::DocMapper =
            serde_json::from_value(serde_json::json!({
                "mode": "dynamic",
                "dynamic_mapping": { "indexed": false, "stored": true, "fast": false },
                "field_mappings": [
                    { "name": "Timestamp", "type": "datetime", "fast": true, "indexed": false },
                    { "name": "Body", "type": "text" },
                    { "name": "ServiceName", "type": "text", "tokenizer": "raw", "fast": true },
                    { "name": "SeverityText", "type": "text", "tokenizer": "raw", "fast": true }
                ],
                "timestamp_field": "Timestamp"
            }))
            .unwrap();
        assert!(doc_mapper.can_stream_json());

        let samples = [
            r#"{"Timestamp":1758585600000,"ServiceName":"frontend","SeverityText":"error","Body":"Failed to place order 42","ResourceAttributes":{"host.name":"node-1","pid":"17"},"ScopeAttributes":{},"LogAttributes":{"trace_id":"abc","num":13,"flag":true}}"#,
            r#"{"Timestamp":1758585600000,"ServiceName":"checkout","Body":"Timeout from 10.0.0.1","Extra":[1,2,3]}"#,
        ];
        for json_str in samples {
            let owned = parse(json_str);
            let borrowed: serde_json_borrow::Value = serde_json::from_str(json_str).unwrap();
            let mut observer = fingerprinter.streaming_observer().unwrap();
            let (stream_part, stream_doc) = doc_mapper
                .try_doc_from_json_str_streaming(json_str, &mut observer)
                .unwrap()
                .unwrap();
            let fp_stream = observer.finish();

            assert_eq!(fp_stream, fingerprinter.fingerprint(&owned));
            assert_eq!(
                fp_stream,
                fingerprinter.fingerprint_borrowed(borrowed.as_object().unwrap())
            );
            let (dom_part, dom_doc) = doc_mapper
                .doc_from_json_obj(owned.as_object().unwrap().clone(), json_str.len() as u64)
                .unwrap();
            assert_eq!(stream_part, dom_part);
            let schema = doc_mapper.schema();
            let stream_json = doc_mapper
                .doc_to_json(tantivy::Document::to_named_doc(&stream_doc, &schema).0)
                .unwrap();
            let dom_json = doc_mapper
                .doc_to_json(tantivy::Document::to_named_doc(&dom_doc, &schema).0)
                .unwrap();
            assert_eq!(stream_json, dom_json);
        }
    }
}
