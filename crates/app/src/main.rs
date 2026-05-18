use anyhow::{Context, Result};
use common::AppConfig;
use indexer::IndexerService;
use rpc::RpcClient;
use storage::Store;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let config = AppConfig::from_env().context("failed to read environment")?;
    let store = Store::connect(&config.database)
        .await
        .context("failed to connect database")?;
    let rpc = RpcClient::new(&config.chain).context("failed to initialize rpc client")?;
    let indexer = IndexerService::new(config.chain.clone(), rpc.clone(), store.clone());

    let router = api::router(store.clone(), rpc.clone(), &config.api);
    let listener = TcpListener::bind(&config.api.bind_addr)
        .await
        .with_context(|| format!("failed to bind api listener at {}", config.api.bind_addr))?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let indexer_shutdown = shutdown_rx.clone();
    let api_shutdown = shutdown_rx.clone();

    let mut indexer_task = tokio::spawn(async move { indexer.run(indexer_shutdown).await });
    let mut api_task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(wait_for_shutdown(api_shutdown))
            .await
    });

    info!(bind_addr = %config.api.bind_addr, "aether-indexer is running");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown requested via ctrl-c");
        }
        result = &mut indexer_task => {
            match result {
                Ok(Ok(())) => info!("indexer task completed"),
                Ok(Err(err)) => error!(error = %err, "indexer task failed"),
                Err(err) => error!(error = %err, "indexer task panicked"),
            }
        }
        result = &mut api_task => {
            match result {
                Ok(Ok(())) => info!("api task completed"),
                Ok(Err(err)) => error!(error = %err, "api task failed"),
                Err(err) => error!(error = %err, "api task panicked"),
            }
        }
    }

    let _ = shutdown_tx.send(true);

    if !indexer_task.is_finished() {
        let indexer_join = indexer_task.await;
        if let Ok(Err(err)) = indexer_join {
            error!(error = %err, "indexer task returned error during shutdown");
        }
    }
    if !api_task.is_finished() {
        let api_join = api_task.await;
        if let Ok(Err(err)) = api_join {
            error!(error = %err, "api task returned error during shutdown");
        }
    }

    Ok(())
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aether_indexer=info,app=info,api=info,indexer=info".into()),
        )
        .try_init();
}
