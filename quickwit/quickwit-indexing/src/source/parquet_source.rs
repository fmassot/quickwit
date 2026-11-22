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

//! Native Parquet work-stealing source for `local-ingest --input-format parquet`.
//!
//! Splits input Parquet file(s) at row-group granularity across `N` independent indexing
//! pipelines. Each pipeline decodes its assigned row group on the blocking runtime using
//! Parquet projection pushdown and streams pre-converted [`ProcessedDocBatch`]es directly
//! through `DocProcessor` to `Indexer`, bypassing JSON encoding/decoding completely.

use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_common::runtimes::RuntimeType;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::checkpoint::SourceCheckpointDelta;

use super::parquet_converter::BoundParquetSchema;
use super::{Source, SourceContext};
use crate::actors::DocProcessor;
use crate::models::ProcessedDocBatch;

/// Number of rows converted per `ProcessedDocBatch` streamed from a Parquet row group.
const PARQUET_RECORD_BATCH_ROWS: usize = 4_096;

#[derive(Clone, Debug)]
struct ParquetWorkItem {
    file_path: Arc<PathBuf>,
    row_group_idx: usize,
}

/// Cloneable MPMC work-stealing queue of Parquet row groups.
#[derive(Clone, Debug)]
pub struct LocalParquetReceiver {
    work_rx: flume::Receiver<ParquetWorkItem>,
}

/// Expands `input_path` (single `.parquet` file, directory of `.parquet` files, or glob
/// pattern) and enqueues every `(file_path, row_group_idx)` work item into a lock-free
/// MPMC channel.
///
/// Returns the work-stealing receiver and the total number of row groups discovered.
pub fn open_local_parquet_work_queue(
    input_path: &str,
) -> anyhow::Result<(LocalParquetReceiver, usize)> {
    let local_path_str = input_path.strip_prefix("file://").unwrap_or(input_path);
    let file_paths = resolve_parquet_paths(local_path_str)?;
    if file_paths.is_empty() {
        bail!("no Parquet files matched `{local_path_str}`");
    }

    let (work_tx, work_rx) = flume::unbounded();
    let mut total_row_groups = 0usize;

    for path in file_paths {
        let file = File::open(&path)
            .with_context(|| format!("failed to open Parquet file `{}`", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("failed to read Parquet footer of `{}`", path.display()))?;
        let num_row_groups = builder.metadata().num_row_groups();
        let shared_path = Arc::new(path);
        for row_group_idx in 0..num_row_groups {
            let _ = work_tx.send(ParquetWorkItem {
                file_path: Arc::clone(&shared_path),
                row_group_idx,
            });
            total_row_groups += 1;
        }
    }
    drop(work_tx);

    Ok((LocalParquetReceiver { work_rx }, total_row_groups))
}

fn resolve_parquet_paths(path_str: &str) -> anyhow::Result<Vec<PathBuf>> {
    if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
        let mut matches = Vec::new();
        for entry in glob::glob(path_str)
            .with_context(|| format!("invalid Parquet glob pattern `{path_str}`"))?
        {
            let matched = entry?;
            if matched.is_file() {
                matches.push(matched);
            }
        }
        matches.sort();
        return Ok(matches);
    }

    let path = Path::new(path_str);
    if path.is_dir() {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(path)
            .with_context(|| format!("failed to read directory `{path_str}`"))?
        {
            let entry_path = entry?.path();
            if entry_path.is_file()
                && entry_path.extension().and_then(|ext| ext.to_str()) == Some("parquet")
            {
                files.push(entry_path);
            }
        }
        files.sort();
        return Ok(files);
    }

    if !path.is_file() {
        bail!("Parquet input path does not exist or is not a file: `{path_str}`");
    }
    Ok(vec![path.to_path_buf()])
}

/// Source that steals Parquet row groups from a shared [`LocalParquetReceiver`], decodes
/// Arrow batches on the blocking runtime with projection pushdown, and emits
/// [`ProcessedDocBatch`]es directly to `DocProcessor`.
pub struct ParquetSource {
    receiver: LocalParquetReceiver,
    doc_mapper: Arc<DocMapper>,
    num_docs_processed: u64,
    num_row_groups_processed: u64,
}

impl ParquetSource {
    pub fn new(receiver: LocalParquetReceiver, doc_mapper: Arc<DocMapper>) -> Self {
        Self {
            receiver,
            doc_mapper,
            num_docs_processed: 0,
            num_row_groups_processed: 0,
        }
    }
}

impl fmt::Debug for ParquetSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ParquetSource")
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let work_opt = ctx
            .protect_future(self.receiver.work_rx.recv_async())
            .await
            .ok();
        let Some(work_item) = work_opt else {
            ctx.send_exit_with_success(doc_processor_mailbox).await?;
            return Err(ActorExitStatus::Success);
        };

        let doc_mapper = Arc::clone(&self.doc_mapper);
        let (batch_tx, batch_rx) = flume::bounded::<anyhow::Result<ProcessedDocBatch>>(2);
        let blocking_handle = RuntimeType::Blocking.get_runtime_handle();

        let decode_task = blocking_handle.spawn_blocking(move || {
            if let Err(err) = decode_row_group_into_channel(&work_item, &doc_mapper, &batch_tx) {
                let _ = batch_tx.send(Err(err));
            }
        });

        while let Ok(batch_res) = ctx.protect_future(batch_rx.recv_async()).await {
            let processed_batch = batch_res?;
            self.num_docs_processed += processed_batch.docs.len() as u64;
            ctx.send_message(doc_processor_mailbox, processed_batch)
                .await?;
        }

        ctx.protect_future(decode_task)
            .await
            .context("Parquet row group decode task panicked")?;
        self.num_row_groups_processed += 1;
        Ok(Duration::ZERO)
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::json!({
            "num_docs_processed": self.num_docs_processed,
            "num_row_groups_processed": self.num_row_groups_processed,
        })
    }
}

fn decode_row_group_into_channel(
    work_item: &ParquetWorkItem,
    doc_mapper: &DocMapper,
    batch_tx: &flume::Sender<anyhow::Result<ProcessedDocBatch>>,
) -> anyhow::Result<()> {
    let file = File::open(work_item.file_path.as_ref()).with_context(|| {
        format!(
            "failed to open Parquet file `{}`",
            work_item.file_path.display()
        )
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let bound_schema = BoundParquetSchema::try_new(doc_mapper, builder.schema())?;
    let projection = ProjectionMask::roots(
        builder.parquet_schema(),
        bound_schema.root_projection_indices.iter().copied(),
    );
    let reader = builder
        .with_row_groups(vec![work_item.row_group_idx])
        .with_projection(projection)
        .with_batch_size(PARQUET_RECORD_BATCH_ROWS)
        .build()?;

    for record_batch_res in reader {
        let record_batch = record_batch_res?;
        let docs = bound_schema.convert_batch(&record_batch, doc_mapper)?;
        if docs.is_empty() {
            continue;
        }
        let processed_batch = ProcessedDocBatch::new(docs, SourceCheckpointDelta::default(), false);
        if batch_tx.send(Ok(processed_batch)).is_err() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray, UInt16Array};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use tantivy::schema::Value;

    use super::*;

    #[test]
    fn test_parquet_roundtrip_strict_and_lenient() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("EventTime", DataType::Int64, false),
            ArrowField::new("EventDate", DataType::UInt16, false),
            ArrowField::new("Title", DataType::Utf8, true),
            ArrowField::new("ExtraIgnored", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(Int64Array::from(vec![1_373_889_600i64, 1_373_889_605i64])),
                Arc::new(UInt16Array::from(vec![15_901u16, 15_901u16])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(Int64Array::from(vec![99, 100])),
            ],
        )
        .unwrap();

        let temp_file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        {
            let mut writer =
                ArrowWriter::try_new(temp_file.reopen().unwrap(), arrow_schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let doc_mapper: DocMapper = serde_json::from_str(
            r#"{
                "mode": "lenient",
                "store_source": false,
                "timestamp_field": "EventTime",
                "field_mappings": [
                    {"name": "EventTime", "type": "datetime", "fast": true},
                    {"name": "EventDate", "type": "datetime", "fast": true},
                    {"name": "Title", "type": "text", "fast": true}
                ]
            }"#,
        )
        .unwrap();

        let (receiver, num_row_groups) =
            open_local_parquet_work_queue(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(num_row_groups, 1);
        let work_item = receiver.work_rx.recv().unwrap();

        let (tx, rx) = flume::bounded(4);
        decode_row_group_into_channel(&work_item, &doc_mapper, &tx).unwrap();
        drop(tx);

        let processed = rx.recv().unwrap().unwrap();
        assert_eq!(processed.docs.len(), 2);
        let schema = doc_mapper.schema();
        let title_field = schema.get_field("Title").unwrap();
        let date_field = schema.get_field("EventDate").unwrap();
        assert_eq!(
            processed.docs[0]
                .doc
                .get_first(title_field)
                .and_then(|val| val.as_str()),
            Some("hello")
        );
        assert!(processed.docs[1].doc.get_first(title_field).is_none());
        assert_eq!(
            processed.docs[0]
                .doc
                .get_first(date_field)
                .and_then(|val| val.as_datetime())
                .map(|dt| dt.into_timestamp_secs()),
            Some(15_901 * 86_400)
        );
    }
}
