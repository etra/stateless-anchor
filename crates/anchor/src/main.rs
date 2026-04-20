use std::path::PathBuf;
use std::sync::Arc;

use anchor_config::Config;
use anchor_input_http::router;
use anchor_output_iceberg::IcebergSink;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config_path: PathBuf = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yaml".to_string())
        .into();
    let cfg = Config::load_from_path(&config_path)?;

    let sink = IcebergSink::connect(&cfg.iceberg).await?;
    sink.log_inventory().await?;
    let app = router(Arc::new(sink));
    let listener = tokio::net::TcpListener::bind(cfg.http.bind).await?;
    tracing::info!(bind = %cfg.http.bind, config = %config_path.display(), "anchor listening");
    axum::serve(listener, app).await?;
    Ok(())
}
