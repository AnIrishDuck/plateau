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

pub use crate::config::PlateauConfig as Config;
pub use catalog::Catalog;
pub use data::DEFAULT_BYTE_LIMIT;
pub use plateau_catalog as catalog;
pub use plateau_data as data;
pub use plateau_transport as transport;
pub use plateau_transport::arrow2;

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

    // Start reconciliation task if configured
    if let Some(reconcile_config) = &config.reconcile {
        tracing::info!(
            "starting reconciliation task with config: {:?}",
            reconcile_config
        );
        let mut reconciler =
            catalog::ReconcileJob::with_config(catalog.clone(), reconcile_config.clone());

        tokio::spawn(async move {
            // Run reconciliation once and exit
            match reconciler.run(None).await {
                Ok(_) => {
                    tracing::info!("reconciliation completed successfully");
                }
                Err(e) => {
                    tracing::error!("reconciliation error: {:?}", e);
                }
            }
        });
    }

    {
        use futures::future::FutureExt;
        let mut tasks = vec![Catalog::checkpoints(catalog.clone()).boxed(), stop, server];

        if config.catalog.storage.monitor {
            tasks.push(catalog.monitor_disk_storage().boxed());
        }

        if let Some(emergency_reconcile) = config.emergency_reconcile {
            tasks.push(
                Catalog::emergency_reconciliations(catalog.clone(), emergency_reconcile).boxed(),
            );
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
