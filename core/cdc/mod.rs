//! CDC (Change Data Capture) streaming sink infrastructure.
//!
//! This module provides trait-based abstractions for streaming CDC events
//! to external systems like NATS, Kafka, Redis, etc.
//!
//! # Feature Flags
//!
//! - `cdc_nats`: Enables NATS/JetStream sink support (requires async-nats, tokio, base64)
//!
//! # Usage
//!
//! CDC streaming is enabled via the `PRAGMA change_data_capture_sink` command:
//!
//! ```sql
//! -- Enable NATS sink (fire-and-forget)
//! PRAGMA change_data_capture_sink('nats,nats://localhost:4222,cdc.events');
//!
//! -- Enable JetStream sink (persistent, acknowledged)
//! PRAGMA change_data_capture_sink('jetstream,nats://localhost:4222,cdc.db1');
//!
//! -- Disable sink
//! PRAGMA change_data_capture_sink('off');
//!
//! -- Query current sink status
//! PRAGMA change_data_capture_sink;
//! ```
//!
//! Note: CDC events are only generated when CDC is enabled via
//! `PRAGMA unstable_capture_data_changes_conn`.

mod config;
mod sink;

#[cfg(feature = "cdc_nats")]
mod nats_sink;

// Re-export public types
pub use config::{CdcSinkConfig, JetStreamSinkConfig, NatsSinkConfig};
pub use sink::{CdcChangeType, CdcEvent, CdcSink, CdcSinkError};

#[cfg(feature = "cdc_nats")]
pub use nats_sink::{JetStreamSink, NatsSink};

use std::sync::Arc;

/// Create a CDC sink from configuration.
///
/// This is the main factory function for creating sinks based on configuration.
#[cfg(feature = "cdc_nats")]
pub fn create_sink(config: CdcSinkConfig) -> Result<Arc<dyn CdcSink>, CdcSinkError> {
    match config {
        CdcSinkConfig::Nats(nats_config) => {
            let sink = NatsSink::new(nats_config)?;
            Ok(Arc::new(sink))
        }
        CdcSinkConfig::JetStream(js_config) => {
            let sink = JetStreamSink::new(js_config)?;
            Ok(Arc::new(sink))
        }
    }
}

/// Create a CDC sink from configuration (stub when no sink features are enabled).
#[cfg(not(feature = "cdc_nats"))]
pub fn create_sink(_config: CdcSinkConfig) -> Result<Arc<dyn CdcSink>, CdcSinkError> {
    Err(CdcSinkError::ConfigError(
        "No CDC sink backends available. Enable the 'cdc_nats' feature.".to_string(),
    ))
}
