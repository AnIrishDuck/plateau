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
struct Records {
    records: Vec<String>,
}

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
