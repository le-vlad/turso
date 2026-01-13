//! CDC (Change Data Capture) sink trait and types.
//!
//! This module defines the core abstraction for CDC streaming sinks.

use crate::types::Value;
use std::fmt::Debug;
use thiserror::Error;

/// Error type for CDC sink operations.
#[derive(Debug, Error)]
pub enum CdcSinkError {
    #[error("Connection failed: {0}")]
    ConnectionError(String),
    #[error("Publish failed: {0}")]
    PublishError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Type of change in a CDC event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcChangeType {
    Insert = 1,
    Update = 0,
    Delete = -1,
}

impl CdcChangeType {
    /// Create from integer value (as stored in turso_cdc table).
    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(CdcChangeType::Insert),
            0 => Some(CdcChangeType::Update),
            -1 => Some(CdcChangeType::Delete),
            _ => None,
        }
    }

    /// Get string representation for JSON serialization.
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcChangeType::Insert => "INSERT",
            CdcChangeType::Update => "UPDATE",
            CdcChangeType::Delete => "DELETE",
        }
    }
}

/// A CDC event containing all change information.
///
/// This struct represents a single change captured from the database,
/// ready to be published to an external sink.
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// Auto-incrementing change identifier.
    pub change_id: i64,
    /// Unix timestamp of the change.
    pub change_time: i64,
    /// Type of change (INSERT, UPDATE, DELETE).
    pub change_type: CdcChangeType,
    /// Name of the table that was modified.
    pub table_name: String,
    /// Primary key/rowid of the changed row.
    pub id: Value,
    /// Binary record before the change (for UPDATE/DELETE).
    pub before: Option<Vec<u8>>,
    /// Binary record after the change (for INSERT/UPDATE).
    pub after: Option<Vec<u8>>,
    /// Update details (for UPDATE in full mode).
    pub updates: Option<Vec<u8>>,
}

impl CdcEvent {
    /// Serialize the event to JSON format.
    pub fn to_json(&self) -> String {
        let change_type_str = self.change_type.as_str();

        let id_json = match &self.id {
            Value::Null => "null".to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(t) => format!("\"{}\"", escape_json_string(t.as_str())),
            Value::Blob(b) => format!("\"{}\"", base64_encode(b)),
        };

        let before_json = self
            .before
            .as_ref()
            .map(|b| format!("\"{}\"", base64_encode(b)))
            .unwrap_or_else(|| "null".to_string());
        let after_json = self
            .after
            .as_ref()
            .map(|a| format!("\"{}\"", base64_encode(a)))
            .unwrap_or_else(|| "null".to_string());
        let updates_json = self
            .updates
            .as_ref()
            .map(|u| format!("\"{}\"", base64_encode(u)))
            .unwrap_or_else(|| "null".to_string());

        format!(
            r#"{{"change_id":{},"change_time":{},"change_type":"{}","table_name":"{}","id":{},"before":{},"after":{},"updates":{}}}"#,
            self.change_id,
            self.change_time,
            change_type_str,
            escape_json_string(&self.table_name),
            id_json,
            before_json,
            after_json,
            updates_json
        )
    }
}

/// Escape special characters in a string for JSON.
fn escape_json_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
}

/// Base64 encode bytes (simple implementation to avoid dependency when feature is off).
#[cfg(feature = "cdc_nats")]
fn base64_encode(data: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data)
}

#[cfg(not(feature = "cdc_nats"))]
fn base64_encode(data: &[u8]) -> String {
    // Fallback: use hex encoding when base64 crate not available
    hex::encode(data)
}

/// Trait for CDC sinks that receive change events.
///
/// All implementations must be Send + Sync for thread-safe connection sharing.
/// Sinks should implement fire-and-forget semantics where publish operations
/// do not block the calling thread.
pub trait CdcSink: Send + Sync + Debug {
    /// Returns the sink type identifier (e.g., "nats", "kafka").
    fn sink_type(&self) -> &'static str;

    /// Publish a CDC event asynchronously (fire-and-forget).
    ///
    /// This method should return immediately without waiting for confirmation.
    /// Errors should be logged but not propagated to avoid blocking database operations.
    fn publish(&self, event: CdcEvent) -> Result<(), CdcSinkError>;

    /// Check if the sink connection is healthy.
    fn is_connected(&self) -> bool;

    /// Close the sink connection gracefully.
    fn close(&self) -> Result<(), CdcSinkError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_change_type_from_i64() {
        assert_eq!(CdcChangeType::from_i64(1), Some(CdcChangeType::Insert));
        assert_eq!(CdcChangeType::from_i64(0), Some(CdcChangeType::Update));
        assert_eq!(CdcChangeType::from_i64(-1), Some(CdcChangeType::Delete));
        assert_eq!(CdcChangeType::from_i64(42), None);
    }

    #[test]
    fn test_cdc_event_to_json() {
        let event = CdcEvent {
            change_id: 1,
            change_time: 1234567890,
            change_type: CdcChangeType::Insert,
            table_name: "users".to_string(),
            id: Value::Integer(42),
            before: None,
            after: Some(vec![1, 2, 3]),
            updates: None,
        };

        let json = event.to_json();
        assert!(json.contains("\"change_type\":\"INSERT\""));
        assert!(json.contains("\"table_name\":\"users\""));
        assert!(json.contains("\"change_id\":1"));
        assert!(json.contains("\"id\":42"));
    }

    #[test]
    fn test_escape_json_string() {
        assert_eq!(escape_json_string("hello"), "hello");
        assert_eq!(escape_json_string("hello\"world"), "hello\\\"world");
        assert_eq!(escape_json_string("hello\nworld"), "hello\\nworld");
        assert_eq!(escape_json_string("tab\there"), "tab\\there");
    }
}
