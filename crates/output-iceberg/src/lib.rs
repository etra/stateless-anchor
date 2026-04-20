use std::collections::HashMap;
use std::sync::Arc;

use anchor_config::IcebergConfig;
use anchor_core::{PushReceipt, Sink, SinkError};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_json::ReaderBuilder;
use arrow_select::take::take;
use async_trait::async_trait;
use bytes::Bytes;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataFileFormat, Literal, PartitionKey, PartitionSpecRef, SchemaRef, Struct, Transform,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::file::properties::WriterProperties;

pub struct IcebergSink {
    catalog: Arc<dyn Catalog>,
}

/// Walk the error's `source()` chain and join each link with " | caused by: ".
/// iceberg::Error's Display only prints the top-level message, so TLS / DNS /
/// HTTP status details sit one or two levels deep and are otherwise invisible.
fn err_chain<E: std::error::Error>(e: &E) -> String {
    use std::fmt::Write;
    let mut s = format!("{e}");
    let mut src: Option<&dyn std::error::Error> = e.source();
    while let Some(inner) = src {
        let _ = write!(s, " | caused by: {inner}");
        src = inner.source();
    }
    s
}

impl IcebergSink {
    pub async fn connect(cfg: &IcebergConfig) -> Result<Self, SinkError> {
        let mut props = cfg.storage.clone();
        props.insert(REST_CATALOG_PROP_URI.to_string(), cfg.catalog_uri.clone());
        props.insert(
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            cfg.warehouse.clone(),
        );
        if let Some(token) = &cfg.token {
            // iceberg-rust's REST catalog reads the bearer token from the
            // "token" property and sends `Authorization: Bearer <token>`.
            props.insert("token".to_string(), token.clone());
        }
        // iceberg 0.9 requires an explicit StorageFactory — core only ships
        // LocalFs/Memory. For S3-compatible warehouses (AWS, R2, MinIO) we
        // inject the opendal-backed S3 factory; `configured_scheme` matches
        // the scheme prefix of the warehouse URI (`s3://...`). The same
        // `s3.endpoint` / `s3.region` / `s3.access-key-id` / etc. props are
        // forwarded via the catalog load call below — FileIO picks them up
        // from the REST response / catalog properties.
        let storage_factory = Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        });
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load("rest", props)
            .await
            .map_err(|e| SinkError::Catalog(err_chain(&e)))?;
        Ok(Self {
            catalog: Arc::new(catalog),
        })
    }

    /// Probe the catalog at startup: list top-level namespaces and their
    /// tables, logging each at INFO. Fails loud if the catalog is
    /// unreachable — same spirit as config parse errors.
    pub async fn log_inventory(&self) -> Result<(), SinkError> {
        let namespaces = self
            .catalog
            .list_namespaces(None)
            .await
            .map_err(|e| SinkError::Catalog(err_chain(&e)))?;
        tracing::info!(count = namespaces.len(), "catalog connected");
        for ns in &namespaces {
            match self.catalog.list_tables(ns).await {
                Ok(tables) => {
                    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
                    tracing::info!(namespace = ?ns.as_ref(), tables = ?names, "namespace");
                }
                Err(e) => {
                    tracing::warn!(
                        namespace = ?ns.as_ref(),
                        error = %e,
                        "failed to list tables in namespace"
                    );
                }
            }
        }
        Ok(())
    }
}

// Dedup note: iceberg-rust 0.9 has EqualityDeleteFileWriter but its
// Transaction API (fast_append) accepts data files only — there is no
// public RowDelta / add_delete_files action yet. Until iceberg-rust exposes
// one, anchor cannot emit the equality-delete file that would let readers
// reconcile cross-batch duplicates by identifier field. This sink is
// therefore append-only; duplicate event_ids submitted across batches will
// all land in the table until compaction or a MERGE query collapses them.
// When the RowDelta action lands upstream, wire up EqualityDeleteFileWriter
// alongside the data write below and commit both via that action.

#[async_trait]
impl Sink for IcebergSink {
    async fn push_ndjson(
        &self,
        namespace: &str,
        table: &str,
        body: Bytes,
    ) -> Result<PushReceipt, SinkError> {
        let ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table.to_string(),
        );
        let iceberg_table = self.catalog.load_table(&ident).await.map_err(|e| {
            // Only the "not found" kinds should surface as 404; everything
            // else (auth, network, server error) must propagate with its
            // real message — otherwise we mask real failures as TableNotFound.
            if matches!(
                e.kind(),
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
            ) {
                SinkError::TableNotFound {
                    namespace: namespace.to_string(),
                    table: table.to_string(),
                }
            } else {
                SinkError::Catalog(err_chain(&e))
            }
        })?;

        let iceberg_schema: SchemaRef = iceberg_table.metadata().current_schema().clone();
        let arrow_schema = Arc::new(
            schema_to_arrow_schema(&iceberg_schema)
                .map_err(|e| SinkError::SchemaMismatch(err_chain(&e)))?,
        );

        // Parse NDJSON strictly against the table's schema. First bad row
        // fails the whole request — no partial writes.
        let reader = ReaderBuilder::new(arrow_schema)
            .build(std::io::Cursor::new(body))
            .map_err(|e| SinkError::BadPayload(err_chain(&e)))?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total = 0usize;
        for batch in reader {
            let batch = batch.map_err(|e| SinkError::SchemaMismatch(err_chain(&e)))?;
            total += batch.num_rows();
            batches.push(batch);
        }
        if total == 0 {
            return Ok(PushReceipt { records_written: 0 });
        }

        let file_io = iceberg_table.file_io().clone();
        let location_gen = DefaultLocationGenerator::new(iceberg_table.metadata().clone())
            .map_err(|e| SinkError::Storage(err_chain(&e)))?;
        let name_gen = DefaultFileNameGenerator::new(
            "part".to_string(),
            Some(uuid::Uuid::new_v4().to_string()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            iceberg_schema.clone(),
        );
        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            file_io,
            location_gen,
            name_gen,
        );
        let data_writer_builder = DataFileWriterBuilder::new(rolling_builder);

        let partition_spec = iceberg_table.metadata().default_partition_spec();
        let data_files = if partition_spec.is_unpartitioned() {
            let mut writer = data_writer_builder
                .build(None)
                .await
                .map_err(|e| SinkError::Storage(err_chain(&e)))?;
            for batch in batches {
                writer
                    .write(batch)
                    .await
                    .map_err(|e| SinkError::Storage(err_chain(&e)))?;
            }
            writer
                .close()
                .await
                .map_err(|e| SinkError::Storage(err_chain(&e)))?
        } else {
            // FanoutWriter keeps one open writer per partition key and
            // returns all resulting DataFiles on close. We compute the key
            // per row and slice the batch into per-partition sub-batches.
            let sources = resolve_partition_sources(partition_spec, &iceberg_schema, &batches[0])?;
            let mut fanout: FanoutWriter<_> = FanoutWriter::new(data_writer_builder);
            for batch in &batches {
                for (literals, rows) in group_rows_by_partition(batch, &sources)? {
                    let sub_batch = take_rows(batch, &rows)?;
                    let key = PartitionKey::new(
                        (**partition_spec).clone(),
                        iceberg_schema.clone(),
                        Struct::from_iter(literals.into_iter().map(Some)),
                    );
                    fanout
                        .write(key, sub_batch)
                        .await
                        .map_err(|e| SinkError::Storage(err_chain(&e)))?;
                }
            }
            fanout
                .close()
                .await
                .map_err(|e| SinkError::Storage(err_chain(&e)))?
        };

        // Atomic append. If commit fails, orphaned Parquet files remain in
        // object storage but are never referenced by the table — readers
        // won't see them. Running a periodic orphan-file GC against the
        // warehouse is the caller's concern.
        let tx = Transaction::new(&iceberg_table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .map_err(|e| SinkError::Catalog(err_chain(&e)))?;
        tx.commit(&*self.catalog)
            .await
            .map_err(|e| SinkError::Catalog(err_chain(&e)))?;

        Ok(PushReceipt {
            records_written: total,
        })
    }
}

struct PartitionSource {
    name: String,
    col_idx: usize,
}

fn resolve_partition_sources(
    spec: &PartitionSpecRef,
    schema: &SchemaRef,
    sample_batch: &RecordBatch,
) -> Result<Vec<PartitionSource>, SinkError> {
    let mut sources = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        if !matches!(field.transform, Transform::Identity) {
            return Err(SinkError::Storage(format!(
                "partition transform {:?} on `{}` is not supported yet; \
                 IcebergSink currently handles Transform::Identity only",
                field.transform, field.name
            )));
        }
        let source_field = schema.field_by_id(field.source_id).ok_or_else(|| {
            SinkError::Storage(format!(
                "partition source_id {} not found in schema",
                field.source_id
            ))
        })?;
        let col_idx = sample_batch
            .schema()
            .index_of(source_field.name.as_str())
            .map_err(|e| SinkError::Storage(format!("column lookup failed: {e}")))?;
        sources.push(PartitionSource {
            name: source_field.name.clone(),
            col_idx,
        });
    }
    Ok(sources)
}

fn group_rows_by_partition(
    batch: &RecordBatch,
    sources: &[PartitionSource],
) -> Result<Vec<(Vec<Literal>, Vec<u32>)>, SinkError> {
    let mut by_key: HashMap<String, (Vec<Literal>, Vec<u32>)> = HashMap::new();
    for row in 0..batch.num_rows() {
        let literals: Vec<Literal> = sources
            .iter()
            .map(|s| arrow_literal_at_row(batch.column(s.col_idx).as_ref(), row, &s.name))
            .collect::<Result<_, _>>()?;
        let key = literals
            .iter()
            .map(|l| format!("{l:?}"))
            .collect::<Vec<_>>()
            .join("\x1f");
        by_key
            .entry(key)
            .or_insert_with(|| (literals, Vec::new()))
            .1
            .push(row as u32);
    }
    Ok(by_key.into_values().collect())
}

fn take_rows(batch: &RecordBatch, rows: &[u32]) -> Result<RecordBatch, SinkError> {
    let indices = UInt32Array::from(rows.to_vec());
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None).map_err(|e| SinkError::Storage(err_chain(&e))))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| SinkError::Storage(err_chain(&e)))
}

fn arrow_literal_at_row(col: &dyn Array, row: usize, name: &str) -> Result<Literal, SinkError> {
    if col.is_null(row) {
        return Err(SinkError::BadPayload(format!(
            "partition column `{name}` is null at row {row}"
        )));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        Ok(Literal::string(a.value(row).to_string()))
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        Ok(Literal::int(a.value(row)))
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        Ok(Literal::long(a.value(row)))
    } else {
        Err(SinkError::Storage(format!(
            "unsupported partition column type for `{name}`: {:?}",
            col.data_type()
        )))
    }
}
