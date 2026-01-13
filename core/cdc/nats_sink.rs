//! NATS CDC sink implementations.
//!
//! This module provides two NATS-based sinks for CDC events:
//! - `NatsSink`: Core NATS, fire-and-forget publishing
//! - `JetStreamSink`: NATS JetStream, persistent with acknowledgments

use super::config::{JetStreamSinkConfig, NatsSinkConfig};
use super::sink::{CdcEvent, CdcSink, CdcSinkError};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client;
use parking_lot::RwLock;
use std::sync::{Arc, OnceLock};
use tokio::runtime::{Handle, Runtime};

/// Lazily-initialized tokio runtime for CDC operations.
///
/// This runtime is shared across all NATS sinks and is created on first use.
/// It uses a single worker thread to minimize resource usage.
static CDC_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_or_create_runtime() -> Handle {
    // Try to use an existing tokio runtime if we're already in one
    if let Ok(handle) = Handle::try_current() {
        return handle;
    }

    // Otherwise, create/get the dedicated CDC runtime
    CDC_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("cdc-nats")
                .enable_all()
                .build()
                .expect("Failed to create CDC tokio runtime")
        })
        .handle()
        .clone()
}

/// NATS CDC sink implementation.
///
/// Uses fire-and-forget publishing for non-blocking operation.
/// The sink maintains a persistent connection to the NATS server
/// and spawns async tasks for each publish operation.
pub struct NatsSink {
    /// NATS client wrapped in RwLock for thread-safe access.
    /// Option allows for graceful close by taking ownership.
    client: Arc<RwLock<Option<Client>>>,
    /// NATS subject to publish events to.
    subject: String,
    /// Original URL (for debugging/info).
    #[allow(dead_code)]
    url: String,
    /// Tokio runtime handle for async operations.
    runtime_handle: Handle,
}

impl std::fmt::Debug for NatsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsSink")
            .field("subject", &self.subject)
            .field("url", &self.url)
            .field("connected", &self.is_connected())
            .finish()
    }
}

impl NatsSink {
    /// Create a new NATS sink with the given configuration.
    ///
    /// This will establish a connection to the NATS server.
    /// Returns an error if the connection cannot be established.
    pub fn new(config: NatsSinkConfig) -> Result<Self, CdcSinkError> {
        let runtime_handle = get_or_create_runtime();

        // Connect to NATS server
        let client = runtime_handle.block_on(async {
            async_nats::connect(&config.url)
                .await
                .map_err(|e| CdcSinkError::ConnectionError(e.to_string()))
        })?;

        tracing::info!(
            "CDC NATS sink connected to {} (subject: {})",
            config.url,
            config.subject
        );

        Ok(Self {
            client: Arc::new(RwLock::new(Some(client))),
            subject: config.subject,
            url: config.url,
            runtime_handle,
        })
    }
}

impl CdcSink for NatsSink {
    fn sink_type(&self) -> &'static str {
        "nats"
    }

    fn publish(&self, event: CdcEvent) -> Result<(), CdcSinkError> {
        let client_guard = self.client.read();
        let Some(client) = client_guard.as_ref() else {
            return Err(CdcSinkError::ConnectionError("Sink closed".to_string()));
        };

        // Serialize event to JSON
        let json = event.to_json();
        let subject = self.subject.clone();
        let client_clone = client.clone();

        tracing::debug!("CDC NATS publishing to subject '{}'", subject);

        // Use block_on for publish. The async-nats publish() is fast as it just
        // queues the message to the client's internal buffer. This ensures messages
        // are queued before close() is called, avoiding the race condition where
        // spawned tasks might not execute before drain().
        self.runtime_handle.block_on(async move {
            if let Err(e) = client_clone.publish(subject, json.into()).await {
                tracing::warn!("CDC NATS publish failed: {}", e);
            }
        });

        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client
            .read()
            .as_ref()
            .map(|c| c.connection_state() == async_nats::connection::State::Connected)
            .unwrap_or(false)
    }

    fn close(&self) -> Result<(), CdcSinkError> {
        let mut client_guard = self.client.write();
        if let Some(client) = client_guard.take() {
            tracing::info!("Closing CDC NATS sink");
            // Drain the client to ensure all pending messages are sent
            self.runtime_handle.block_on(async {
                if let Err(e) = client.drain().await {
                    tracing::warn!("Error draining NATS client: {}", e);
                }
            });
        }
        Ok(())
    }
}

impl Drop for NatsSink {
    fn drop(&mut self) {
        // Attempt graceful close on drop
        let _ = self.close();
    }
}

/// NATS JetStream CDC sink implementation.
///
/// Uses JetStream for persistent, acknowledged message delivery.
/// Each publish waits for server acknowledgment before returning.
pub struct JetStreamSink {
    /// NATS client wrapped in RwLock for thread-safe access.
    client: Arc<RwLock<Option<Client>>>,
    /// JetStream context for publishing.
    jetstream: Arc<RwLock<Option<JetStreamContext>>>,
    /// JetStream subject to publish events to.
    subject: String,
    /// Original URL (for debugging/info).
    #[allow(dead_code)]
    url: String,
    /// Tokio runtime handle for async operations.
    runtime_handle: Handle,
}

impl std::fmt::Debug for JetStreamSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JetStreamSink")
            .field("subject", &self.subject)
            .field("url", &self.url)
            .field("connected", &self.is_connected())
            .finish()
    }
}

impl JetStreamSink {
    /// Create a new JetStream sink with the given configuration.
    ///
    /// This will establish a connection to the NATS server and create a JetStream context.
    /// Returns an error if the connection cannot be established.
    pub fn new(config: JetStreamSinkConfig) -> Result<Self, CdcSinkError> {
        let runtime_handle = get_or_create_runtime();

        // Connect to NATS server
        let client = runtime_handle.block_on(async {
            async_nats::connect(&config.url)
                .await
                .map_err(|e| CdcSinkError::ConnectionError(e.to_string()))
        })?;

        // Create JetStream context
        let jetstream = async_nats::jetstream::new(client.clone());

        tracing::info!(
            "CDC JetStream sink connected to {} (subject: {})",
            config.url,
            config.subject
        );

        Ok(Self {
            client: Arc::new(RwLock::new(Some(client))),
            jetstream: Arc::new(RwLock::new(Some(jetstream))),
            subject: config.subject,
            url: config.url,
            runtime_handle,
        })
    }
}

impl CdcSink for JetStreamSink {
    fn sink_type(&self) -> &'static str {
        "jetstream"
    }

    fn publish(&self, event: CdcEvent) -> Result<(), CdcSinkError> {
        let js_guard = self.jetstream.read();
        let Some(jetstream) = js_guard.as_ref() else {
            return Err(CdcSinkError::ConnectionError("Sink closed".to_string()));
        };

        // Serialize event to JSON
        let json = event.to_json();
        let subject = self.subject.clone();
        let js_clone = jetstream.clone();

        tracing::debug!(
            "CDC JetStream publishing to subject '{}': {}",
            subject,
            json
        );

        // Publish with JetStream and wait for acknowledgment
        self.runtime_handle.block_on(async move {
            match js_clone.publish(subject.clone(), json.into()).await {
                Ok(ack_future) => {
                    // Wait for server acknowledgment
                    match ack_future.await {
                        Ok(ack) => tracing::debug!(
                            "CDC JetStream message acknowledged (seq: {}, stream: {})",
                            ack.sequence,
                            ack.stream
                        ),
                        Err(e) => tracing::warn!("CDC JetStream ack failed: {}", e),
                    }
                }
                Err(e) => tracing::warn!("CDC JetStream publish failed: {}", e),
            }
        });

        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client
            .read()
            .as_ref()
            .map(|c| c.connection_state() == async_nats::connection::State::Connected)
            .unwrap_or(false)
    }

    fn close(&self) -> Result<(), CdcSinkError> {
        // Clear JetStream context first
        let _ = self.jetstream.write().take();

        // Then close the client
        let mut client_guard = self.client.write();
        if let Some(client) = client_guard.take() {
            tracing::info!("Closing CDC JetStream sink");
            self.runtime_handle.block_on(async {
                if let Err(e) = client.drain().await {
                    tracing::warn!("Error draining NATS client: {}", e);
                }
            });
        }
        Ok(())
    }
}

impl Drop for JetStreamSink {
    fn drop(&mut self) {
        // Attempt graceful close on drop
        let _ = self.close();
    }
}
