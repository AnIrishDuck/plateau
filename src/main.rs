mod catalog;
mod manifest;
mod partition;
mod segment;
mod slog;
mod topic;

use ::log::info;
use parquet::data_type::ByteArray;
use rweb::*;
use serde::{Deserialize, Serialize};
use slog::RecordIndex;
use std::ops::Range;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::catalog::Catalog;
use crate::topic::Record;

#[derive(Deserialize)]
struct Insert {
    records: Vec<String>,
}

#[derive(Schema, Serialize)]
struct Span {
    start: usize,
    end: usize,
}

impl Span {
    fn from_range(r: Range<RecordIndex>) -> Self {
        Span {
            start: r.start.0,
            end: r.end.0,
        }
    }
}

#[derive(Schema, Serialize)]
struct Inserted {
    span: Span,
}

#[derive(Schema, Serialize)]
struct Partitions {
    partitions: Vec<String>,
}

#[derive(Schema, Serialize)]
struct Records {
    span: Span,
    records: Vec<String>,
}

#[derive(Deserialize)]
struct RecordQuery {
    start: usize,
    limit: Option<usize>,
}

#[derive(Debug)]
struct InvalidQuery;
impl warp::reject::Reject for InvalidQuery {}

#[tokio::main]
async fn main() {
    let catalog = Catalog::attach(PathBuf::from("./data")).await;

    pretty_env_logger::init();
    let log = warp::log("plateau::http");

    let (spec, filter) = openapi::spec().build(move || topic_append(catalog));

    serve(filter.or(openapi_docs(spec)).with(log))
        .run(([127, 0, 0, 1], 3030))
        .await;
}

#[post("/topic/{topic_name}/{partition_name}")]
#[openapi(id = "topic.append")]
#[body_size(max = "10240000")]
async fn topic_append(
    topic_name: String,
    partition_name: String,
    #[data] catalog: Catalog,
    #[json] request: Insert,
) -> Result<Json<Inserted>, Rejection> {
    let time = SystemTime::now();
    let rs: Vec<_> = request
        .records
        .into_iter()
        .map(|m| Record {
            time,
            message: ByteArray::from(m.as_str()),
        })
        .collect();

    let topic = catalog.get_topic(&topic_name).await;
    info!(
        "appending {} to {}/{}",
        rs.len(),
        topic_name,
        partition_name
    );
    let r = topic.append(&partition_name, &rs).await;

    Ok(Json::from(Inserted {
        span: Span::from_range(r),
    }))
}

#[get("/topic/{topic_name}")]
#[openapi(id = "topic.get_partitions")]
async fn topic_get_partitions(
    topic_name: String,
    #[data] catalog: Catalog,
) -> Result<Json<Partitions>, Rejection> {
    let topic = catalog.get_topic(&topic_name).await;
    Ok(Json::from(Partitions {
        partitions: topic.get_partitions().await,
    }))
}

#[get("/topic/{topic_name}/{partition_name}/records")]
#[openapi(id = "topic.get_records")]
async fn topic_get_records(
    topic_name: String,
    partition_name: String,
    #[query] query: String,
    #[data] catalog: Catalog,
) -> Result<Json<Records>, Rejection> {
    if let Ok(q) = serde_json::from_str::<RecordQuery>(&query) {
        let topic = catalog.get_topic(&topic_name).await;
        let limit = std::cmp::min(q.limit.unwrap_or(1000), 10000);
        let (range, rs) = topic
            .get_records(&partition_name, RecordIndex(q.start), limit)
            .await;
        Ok(Json::from(Records {
            span: Span::from_range(range),
            records: vec![],
        }))
    } else {
        Err(rweb::reject::custom(InvalidQuery {}))
    }
}
