//! The catalog is the storage format that organizes individual segments.
//!
//! It uses the [manifest] to record metadata about all stored segments in the
//! system.
//!
//! The catalog breaks down into:
//!
//! - A [slog] to order segments.
//! - A [partition] that wraps the [slog] to enforce size constraints and handle
//!   writes to the [manifest].
//! - A [topic] that is a collection of multiple different partitions that supports
//!   read and write operations.
//! - A [catalog] that is a collection of topics.
//!
//! The catalog also by default runs a [storage] monitor to ensure that we fall back
//! to a read-only mode if the disk is too full.

pub mod catalog;
pub mod manifest;
pub mod reconcile;
pub mod storage;

pub mod partition;
pub mod slog;
pub mod topic;

pub use plateau_client as client;
pub use plateau_data as data;
pub use plateau_transport as transport;
pub use plateau_transport::arrow2;

#[cfg(test)]
pub use plateau_test as test;

pub use catalog::{Catalog, Config};
pub use reconcile::{EmergencyReconcileConfig, ReconcileConfig, ReconcileJob, ReconcileStats};
