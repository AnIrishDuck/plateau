use std::net::SocketAddr;
use std::ops::{Deref, Range, RangeInclusive};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, FromRef, Path, State},
    http::{header::ACCEPT, HeaderMap, Request},
    routing::{get, post},
    Json, Router, Server,
};

use chrono::{DateTime, Utc};
use futures::{Future, FutureExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::oneshot;
use tower_http::classify::{StatusInRangeAsFailures, StatusInRangeFailureClass};
use tower_http::trace::TraceLayer;
use tracing::Instrument;
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::PlateauConfig;
use crate::transport::{
    ActiveSegmentReport, DataFocus, InfoResponse, Inserted, PartitionInfo, Partitions,
    ReconcileStats, RecordQuery, RecordStatus, Span, Topic, TopicInfo, TopicIterationOrder,
    TopicIterationQuery, TopicIterationStatus, TopicIterator, Topics,
};

pub use crate::axum_util::{query::Query, Response};
use crate::catalog::manifest::PartitionId;
use crate::catalog::reconcile::ReconcileJob;
use crate::catalog::slog::SlogError;
use crate::catalog::Catalog;
use crate::data::{
    limit::{BatchStatus, RowLimit},
    Ordering, RecordIndex,
};
use crate::http::chunk::SchemaChunkRequest;

mod chunk;
mod error;

pub use self::error::ErrorReply;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub bind: SocketAddr,
    pub max_append_bytes: usize,
    pub max_page: RowLimit,
}

impl Config {
    pub fn localhost() -> Self {
        Self::with_socket(SocketAddr::from(([127, 0, 0, 1], 0)))
    }

    pub fn with_socket(bind: SocketAddr) -> Self {
        Self::default().bind(bind)
    }

    pub fn bind(self, bind: SocketAddr) -> Self {
        Self { bind, ..self }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([0, 0, 0, 0], 3030)),
            max_append_bytes: crate::DEFAULT_BYTE_LIMIT,
            max_page: RowLimit::default(),
        }
    }
}

trait FromRange {
    fn from_range(r: Range<RecordIndex>) -> Self;
}

impl FromRange for Span {
    fn from_range(r: Range<RecordIndex>) -> Self {
        Self {
            start: r.start.0,
            end: r.end.0,
        }
    }
}

trait IntoRecordStatus {
    fn into_record_status(self) -> RecordStatus;
}

impl IntoRecordStatus for BatchStatus {
    fn into_record_status(self) -> RecordStatus {
        match self {
            Self::Open { .. } => RecordStatus::All,
            Self::SchemaChanged => RecordStatus::SchemaChange,
            Self::BytesExceeded => RecordStatus::ByteLimited,
            Self::RecordsExceeded => RecordStatus::RecordLimited,
        }
    }
}

#[derive(Clone)]
struct AppState(Arc<Catalog>, Arc<PlateauConfig>);

impl FromRef<AppState> for PlateauConfig {
    fn from_ref(state: &AppState) -> Self {
        state.1.deref().clone()
    }
}

pub async fn serve(
    config: PlateauConfig,
    catalog: Arc<Catalog>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    Pin<Box<dyn Future<Output = ()> + Send>>,
) {
    let config = Arc::new(config);

    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();

    // By default tower_http only logs 5xx errors, we want to log 4xx as well
    let log_codes = StatusInRangeAsFailures::new(400..=599);

    let filter = Router::new()
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
        .route("/ok", get(healthcheck))
        .route("/topics", get(get_topics))
        .route(
            "/topic/:topic_name/partition/:partition_name/records",
            get(partition_get_records),
        )
        .route(
            "/topic/:topic_name/partition/:partition_name",
            post(topic_append).layer(DefaultBodyLimit::max(config.http.max_append_bytes)),
        )
        .route("/topic/:topic_name/records", post(topic_iterate_route))
        .route("/topic/:topic_name", get(topic_get_info))
        .route("/info", get(get_info))
        .layer(
            TraceLayer::new(log_codes.into_make_classifier())
                .make_span_with(|request: &Request<Body>| {
                    tracing::span!(
                        target: "plateau::http",
                        tracing::Level::INFO,
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        version = ?request.version(),
                    )
                })
                .on_failure(
                    |err: StatusInRangeFailureClass, _latency: Duration, _span: &tracing::Span| {
                        error!(?err);
                    },
                ),
        )
        .with_state(AppState(catalog, Arc::clone(&config)));

    let server = Server::bind(&config.http.bind).serve(filter.into_make_service());
    let addr = server.local_addr();

    let fut = server.with_graceful_shutdown(FutureExt::map(rx_shutdown, |_| ()));
    let span = tracing::info_span!("Server::run", ?addr);
    tracing::info!(parent: &span, "listening on http://{}", addr);

    (
        addr,
        tx_shutdown,
        Box::pin(async move { fut.instrument(span).await.unwrap_or(()) }),
    )
}

#[utoipa::path(
    get,
    operation_id = "healthcheck",
    path = "/ok",
    responses(
        (status = 200, description = "Healthcheck", body = serde_json::Value),
    ),
  )]
async fn healthcheck(
    State(AppState(catalog, config)): State<AppState>,
) -> Result<Response<serde_json::Value>, ErrorReply> {
    let duration = SystemTime::now().duration_since(catalog.last_checkpoint().await);
    let healthy = duration
        .map(|d| d < config.catalog.checkpoint_interval * 10)
        .unwrap_or(true);
    if healthy {
        Ok(Response::ok(json!({"ok": "true"})))
    } else {
        Err(ErrorReply::NoHeartbeat)
    }
}

#[utoipa::path(
    get,
    operation_id = "get_topics",
    path = "/topics",
    responses(
        (status = 200, description = "List of topics", body = Topics),
    ),
  )]
async fn get_topics(
    State(AppState(catalog, _config)): State<AppState>,
) -> Result<Response<Topics>, ErrorReply> {
    let topics = catalog.list_topics().await;
    Ok(Response::ok(Topics {
        topics: topics.into_iter().map(|name| Topic { name }).collect(),
    }))
}

#[utoipa::path(
    post,
    operation_id = "topic.append",
    path = "/topic/{topic_name}/partition/{partition_name}",
    params(
        ("topic_name", Path, description = "Topic name"),
        ("partition_name", Path, description = "Partition name"),
    ),
    responses(
        (status = 200, description = "Span of inserted records", body = Inserted),
    ),
    request_body(content = SchemaChunk<crate::transport::ArrowSchema>, content_type = "application/vnd.apache.arrow.file"),
  )]
async fn topic_append(
    State(AppState(catalog, _config)): State<AppState>,
    Path((topic_name, partition_name)): Path<(String, String)>,
    chunk: SchemaChunkRequest,
) -> Result<Response<Inserted>, ErrorReply> {
    topic_append_internal(topic_name, partition_name, catalog, chunk).await
}
async fn topic_append_internal(
    topic_name: String,
    partition_name: String,
    catalog: Arc<Catalog>,
    chunk: SchemaChunkRequest,
) -> Result<Response<Inserted>, ErrorReply> {
    if catalog.is_readonly() {
        return Err(ErrorReply::InsufficientDiskSpace);
    }

    if chunk.0.contains_null_type() {
        return Err(ErrorReply::NullTypes);
    }

    catalog.record_write();

    let topic = catalog.get_topic(&topic_name).await;
    info!(
        "appending {} to {}/{}",
        chunk.0.len(),
        topic_name,
        partition_name
    );
    let r = topic.extend(&partition_name, chunk.0).await;

    Ok(Response::ok(Inserted {
        span: Span::from_range(r.map_err(|e| match e.downcast_ref::<SlogError>() {
            Some(SlogError::WriterThreadBusy) => ErrorReply::WriterBusy,
            None => ErrorReply::Unknown,
        })?),
    }))
}

#[utoipa::path(
    get,
    operation_id = "topic.get_info",
    path = "/topic/{topic_name}",
    params(
        ("topic_name", Path, description = "Topic name"),
    ),
    responses(
        (status = 200, description = "List of partitions for topic", body = Partitions),
    ),
  )]
async fn topic_get_info(
    State(AppState(catalog, _config)): State<AppState>,
    Path(topic_name): Path<String>,
) -> Result<Response<Partitions>, ErrorReply> {
    let topic = catalog.get_topic(&topic_name).await;
    let indices = topic.readable_ids(None).await;

    Ok(Response::ok(Partitions {
        partitions: indices
            .into_iter()
            .map(|(partition, range)| (partition, Span::from_range(range)))
            .collect(),
        bytes: topic.byte_size().await,
    }))
}

#[utoipa::path(
    post,
    operation_id = "topic.iterate",
    path = "/topic/{topic_name}/records",
    params(
        ("topic_name", Path, description = "Topic name"),
        TopicIterationQuery,
    ),
    responses(
        (status = 200, description = "Topic's partitions with records", body = serde_json::Value),
    ),
    request_body(content = TopicIterator, content_type = "application/json"),
  )]
async fn topic_iterate_route(
    State(AppState(catalog, config)): State<AppState>,
    Path(topic_name): Path<String>,
    query: Option<Query<TopicIterationQuery>>,
    headers: HeaderMap,
    position: Option<Json<TopicIterator>>,
) -> Result<axum::response::Response, ErrorReply> {
    let max_page = config.http.max_page;
    topic_iterate(topic_name, query, headers, position, catalog, max_page).await
}

pub async fn topic_iterate(
    topic_name: String,
    query: Option<Query<TopicIterationQuery>>,
    headers: HeaderMap,
    position: Option<Json<TopicIterator>>,
    catalog: Arc<Catalog>,
    max_page: RowLimit,
) -> Result<axum::response::Response, ErrorReply> {
    let query = query.map(|Query(query)| query).unwrap_or_default();
    let content = headers.get(ACCEPT).and_then(|header| header.to_str().ok());
    let position = position.map(|Json(value)| value);

    let topic = catalog.get_topic(&topic_name).await;
    let page_size = RowLimit::records(query.page_size.unwrap_or(1000)).min(max_page);
    let position = position.unwrap_or_default();
    let partition_filter = query.partition_filter;
    let order: Ordering = query.order.unwrap_or(TopicIterationOrder::Asc).into();

    let mut result = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        if order == Ordering::Reverse {
            Err(ErrorReply::InvalidQuery)?
        }
        topic
            .get_records_by_time(position, times, page_size, partition_filter)
            .await
    } else {
        topic
            .get_records(position, page_size, order, partition_filter)
            .await
    };

    let status = TopicIterationStatus {
        next: result.iter,
        status: result.batch.status.into_record_status(),
    };

    // WARNING !!!  DO NOT ADD MORE ITEMS TO THE METADATA.
    if let Some(schema) = result.batch.schema.as_mut() {
        schema.metadata.insert(
            "status".to_string(),
            serde_json::to_string(&status).unwrap(),
        );
    }

    chunk::to_reply(content, result.batch, query.data_focus)
}

#[utoipa::path(
    get,
    operation_id = "partition.get_records",
    path = "/topic/{topic_name}/partition/{partition_name}/records",
    params(
        ("topic_name", Path, description = "Topic name"),
        ("partition_name", Path, description = "Partition name"),
        RecordQuery,
    ),
    responses(
        (status = 200, description = "List of records for partition", body = serde_json::Value),
    ),
  )]
async fn partition_get_records(
    State(AppState(catalog, config)): State<AppState>,
    Path((topic_name, partition_name)): Path<(String, String)>,
    Query(query): Query<RecordQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ErrorReply> {
    let max_page = config.http.max_page;
    let topic = catalog.get_topic(&topic_name).await;
    let start_record = RecordIndex(query.start);
    let page_size = RowLimit::records(query.page_size.unwrap_or(1000)).min(max_page);
    let mut result = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        topic
            .get_partition(&partition_name)
            .await
            .get_records_by_time(start_record, times, page_size)
            .await
    } else {
        topic
            .get_partition(&partition_name)
            .await
            .get_records(start_record, page_size, Ordering::Forward)
            .await
    };

    let start = result.chunks.first().and_then(|i| i.start());
    let end = result
        .chunks
        .iter()
        .next_back()
        .and_then(|i| i.end().map(|ix| ix + 1));
    let range = start.zip(end).map(|(start, end)| start..end);

    // WARNING !!!  DO NOT ADD MORE ITEMS TO THE METADATA.
    let status = result.status.into_record_status();
    if let Some(schema) = result.schema.as_mut() {
        schema.metadata.insert(
            "status".to_string(),
            serde_json::to_string(&status).unwrap(),
        );
        schema.metadata.insert(
            "span".to_string(),
            serde_json::to_string(&range.clone().map(Span::from_range)).unwrap(),
        );
    }

    chunk::to_reply(
        headers.get(ACCEPT).and_then(|header| header.to_str().ok()),
        result,
        query.data_focus,
    )
}

fn parse_time_range(
    start: String,
    end: Option<String>,
) -> Result<RangeInclusive<DateTime<Utc>>, ErrorReply> {
    let end = match end {
        Some(end_time) => end_time,
        None => return Err(ErrorReply::InvalidQuery),
    };

    let start = DateTime::parse_from_rfc3339(&start);
    let end = DateTime::parse_from_rfc3339(&end);
    if let (Ok(start), Ok(end)) = (start, end) {
        Ok(start.with_timezone(&Utc)..=end.with_timezone(&Utc))
    } else {
        Err(ErrorReply::InvalidQuery)
    }
}

#[utoipa::path(
    get,
    operation_id = "get_info",
    path = "/info",
    responses(
        (status = 200, description = "System information including topics, partitions, and retention stats", body = InfoResponse),
    ),
  )]
async fn get_info(
    State(AppState(catalog, _config)): State<AppState>,
) -> Result<Response<InfoResponse>, ErrorReply> {
    use futures::StreamExt;

    // Run retention checks
    catalog.retain().await;

    // Get all topics
    let topic_names = catalog.list_topics().await;

    // Collect topic information with their partitions
    let mut topics = Vec::new();

    for topic_name in &topic_names {
        let topic = catalog.get_topic(topic_name).await;

        // Get all partition names for this topic from the manifest
        let partition_names = catalog.manifest().get_partitions(topic_name).await;

        // Collect partition information for this topic
        let mut partitions = Vec::new();

        for partition_name in partition_names {
            let partition = topic.get_partition(&partition_name).await;

            // Get partition stats
            let byte_size = partition.byte_size().await;
            let readable_ids = partition.readable_ids().await;

            // Get segment data to determine time range and indices
            let records = readable_ids.as_ref().map(|ids| Span {
                start: ids.start.0,
                end: ids.end.0,
            });

            // Get time range from manifest
            let partition_id = PartitionId::new(topic_name, &partition_name);
            let mut oldest_time = None;
            let mut newest_time = None;

            // Get all segments for this partition to find time range
            let segments_stream = catalog.manifest().stream_segments(
                &partition_id,
                RecordIndex(0),
                Ordering::Forward,
            );

            let segments: Vec<_> = segments_stream.collect().await;
            let segments = segments.first().zip(segments.last()).map(|(first, last)| {
                oldest_time = Some(*first.time.start());
                newest_time = Some(*last.time.end());

                Span {
                    start: first.index.0,
                    end: last.index.0,
                }
            });

            partitions.push(PartitionInfo {
                name: partition_name, // Just the partition name, not the full path
                oldest_time,
                newest_time,
                total_byte_size: byte_size,
                records,
                segments,
            });
        }

        topics.push(TopicInfo {
            name: topic_name.clone(),
            partitions,
        });
    }

    // Run retention job to get stats
    let mut reconciler = ReconcileJob::new(catalog.clone());
    // Run a reconciliation pass to get current stats
    let _ = reconciler
        .run(None)
        .await
        .map_err(|_| ErrorReply::Unknown)?;

    let report = reconciler.report();
    let sealed = &report.sealed;
    let retention_stats = ReconcileStats {
        files_checked: sealed.files_checked.len(),
        untracked_files: sealed.untracked_files.len(),
        size_mismatches: sealed.size_mismatches.len(),
        missing_files: sealed.missing_files.len(),
        expected_size: sealed.expected_size.as_u64() as usize,
        actual_size: sealed.actual_size.as_u64() as usize,
    };

    let active_segments = report
        .active
        .iter()
        .map(|a| ActiveSegmentReport {
            topic: a.topic.clone(),
            partition: a.partition.clone(),
            manifest_size: a.manifest_size,
            disk_size: a.disk_size,
            delta: a.delta,
        })
        .collect();

    Ok(Response::ok(InfoResponse {
        topics,
        retention_stats,
        active_segments,
    }))
}

#[derive(OpenApi)]
#[openapi(
    paths(
        healthcheck,
        get_topics,
        topic_append,
        topic_get_info,
        topic_iterate_route,
        partition_get_records,
        get_info,
    ),
    components(
        schemas(
            DataFocus,
            Inserted,
            Partitions,
            // PartitionFilter,
            crate::transport::ArrowSchemaChunk,
            Span,
            Topic,
            Topics,
            TopicIterationOrder,
            // TopicIterator,
            InfoResponse,
            TopicInfo,
            PartitionInfo,
            ReconcileStats,
            ActiveSegmentReport,
        )
    ),
    tags(
        (name = "Plateau", description = "Plateau API")
    )
)]
struct ApiDoc;

#[cfg(test)]
mod test {
    use crate::transport::{TopicIterationOrder, TopicIterationQuery};

    #[test]
    fn can_parse_order_query() {
        use serde_qs as qs;

        let q = qs::from_str::<TopicIterationQuery>("order=desc").unwrap();
        assert_eq!(TopicIterationOrder::Desc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("order=DESC").unwrap();
        assert_eq!(TopicIterationOrder::Desc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("order=Asc").unwrap();
        assert_eq!(TopicIterationOrder::Asc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("order=AsC").unwrap();
        assert_eq!(TopicIterationOrder::Asc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("").unwrap();
        assert!(q.order.is_none());
    }
}
