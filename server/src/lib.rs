//! The server pulls together the individual components of plateau and exposes
//! an HTTP interface to the [catalog].

use std::sync::Arc;

use futures::{future, stream};
use tokio::signal::unix::{signal, SignalKind};
use tokio_stream::wrappers::SignalStream;

mod axum_util;
pub mod config;
pub mod http;
pub mod metrics;
pub mod replication;

// Re-export plateau modules at the top level
pub use plateau_catalog as catalog;
pub use plateau_client as client;
pub use plateau_data as data;
pub use plateau_transport as transport;
// Re-export arrow from plateau_transport
pub use transport::arrow;

// Re-export commonly used types from the modules
pub use crate::config::PlateauConfig as Config;
pub use catalog::Catalog;
pub use data::DEFAULT_BYTE_LIMIT;

#[cfg(test)]
pub use plateau_test as test;

/// Future that resolves when an exit signal (SIGINT / SIGTERM / SIGQUIT) is
/// received.
pub fn exit_signal<'a>() -> future::BoxFuture<'a, ()> {
    use future::FutureExt;
    use stream::StreamExt;

    fn signal_stream(k: SignalKind) -> impl stream::Stream<Item = ()> {
        SignalStream::new(signal(k).unwrap())
    }

    let signal_stream = stream::select_all(vec![
        signal_stream(SignalKind::interrupt()),
        signal_stream(SignalKind::terminate()),
        signal_stream(SignalKind::quit()),
    ]);

    signal_stream.into_future().map(|_| ()).boxed()
}

/// Async task that runs the full plateau server stack from a user-provided
/// [config::PlateauConfig].
///
/// Attempts a clean shutdown when the provided `stop` signal is received (i.e.
/// [exit_signal]).
pub async fn task_from_config(
    config: config::PlateauConfig,
    stop: future::BoxFuture<'_, ()>,
) -> bool {
    let catalog = Arc::new(
        Catalog::attach(config.data_path.clone(), config.catalog.clone())
            .await
            .expect("error opening catalog"),
    );

    // Reclaim disk before any of the main loops start. Normal retention writes
    // to the manifest before deleting backing data, which cannot make progress
    // when the disk is already full; this startup pass deletes data first so the
    // subsequent manifest writes have room. Must run before the catalog and
    // retention loops below.
    catalog.retain_startup().await;

    task_from_catalog_config(catalog, config, stop).await
}

/// Async task that runs the full plateau server stack from a user-provided
/// [Catalog] and [config::PlateauConfig]
///
/// Attempts a clean shutdown when the provided `stop` signal is received (i.e.
/// [exit_signal]).
pub async fn task_from_catalog_config(
    catalog: Arc<Catalog>,
    config: config::PlateauConfig,
    stop: future::BoxFuture<'_, ()>,
) -> bool {
    let (addr, end_tx, server) = http::serve(config.clone(), catalog.clone()).await;

    {
        use futures::future::FutureExt;
        let mut tasks = vec![Catalog::checkpoints(catalog.clone()).boxed(), stop, server];

        // Run reconciliation continuously if configured. The loop runs passes
        // back-to-back (paced by the config), publishing each completed report
        // into the catalog so /info reflects the freshest consistency view. It
        // joins the shutdown race here: when `stop` (or the server) wins the
        // `select_all`, this future is dropped, cleanly cancelling the loop.
        if let Some(reconcile_config) = config.reconcile.clone() {
            tracing::info!(
                "starting continuous reconciliation loop with config: {:?}",
                reconcile_config
            );
            tasks.push(catalog::ReconcileJob::run_loop(catalog.clone(), reconcile_config).boxed());
        }

        if config.catalog.storage.monitor {
            tasks.push(catalog.monitor_disk_storage().boxed());
        }

        if let Some(replicate) = config.replication {
            tasks.push(Box::pin(replication::run(replicate, addr)));
        }

        future::select_all(tasks.into_iter()).await;
    }

    tracing::info!("shutting down");
    end_tx.send(()).ok();
    Catalog::close_arc(catalog).await
}
