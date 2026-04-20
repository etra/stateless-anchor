use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("table not found: {namespace}.{table}")]
    TableNotFound { namespace: String, table: String },
    #[error("payload does not match table schema: {0}")]
    SchemaMismatch(String),
    #[error("malformed NDJSON payload: {0}")]
    BadPayload(String),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Debug, Clone)]
pub struct PushReceipt {
    pub records_written: usize,
}

/// Atomic write of NDJSON records into a named table.
///
/// Either every record in `body` is committed, or the call fails and nothing
/// is written. Durability — retries, buffering, delivery guarantees — is the
/// caller's problem, not this component's.
#[async_trait]
pub trait Sink: Send + Sync {
    async fn push_ndjson(
        &self,
        namespace: &str,
        table: &str,
        body: Bytes,
    ) -> Result<PushReceipt, SinkError>;
}
