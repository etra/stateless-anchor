use std::sync::Arc;

use anchor_core::{Sink, SinkError};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use bytes::Bytes;
use serde_json::json;

#[derive(Clone)]
struct AppState {
    sink: Arc<dyn Sink>,
}

pub fn router(sink: Arc<dyn Sink>) -> Router {
    Router::new()
        .route(
            "/api/schema/{namespace}/table/{table}/push",
            post(push_handler),
        )
        .with_state(AppState { sink })
}

async fn push_handler(
    Path((namespace, table)): Path<(String, String)>,
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    let started = std::time::Instant::now();
    let bytes = body.len();
    tracing::info!(%namespace, %table, bytes, "push received");
    match state.sink.push_ndjson(&namespace, &table, body).await {
        Ok(receipt) => {
            tracing::info!(
                %namespace,
                %table,
                records = receipt.records_written,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "push committed"
            );
            (
                StatusCode::OK,
                Json(json!({ "records_written": receipt.records_written })),
            )
                .into_response()
        }
        Err(err) => {
            tracing::warn!(
                %namespace,
                %table,
                elapsed_ms = started.elapsed().as_millis() as u64,
                error = %err,
                "push failed"
            );
            error_response(err)
        }
    }
}

fn error_response(err: SinkError) -> Response {
    let status = match &err {
        SinkError::TableNotFound { .. } => StatusCode::NOT_FOUND,
        SinkError::SchemaMismatch(_) | SinkError::BadPayload(_) => StatusCode::BAD_REQUEST,
        SinkError::Catalog(_) | SinkError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": err.to_string() }))).into_response()
}
