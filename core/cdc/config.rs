//! CDC sink configuration types.

use super::sink::CdcSinkError;

/// Configuration for a NATS CDC sink (core NATS, fire-and-forget).
#[derive(Debug, Clone)]
pub struct NatsSinkConfig {
    /// NATS server URL (e.g., "nats://localhost:4222").
    pub url: String,
    /// NATS subject to publish CDC events to.
    pub subject: String,
}

/// Configuration for a JetStream CDC sink (persistent, acknowledged).
#[derive(Debug, Clone)]
pub struct JetStreamSinkConfig {
    /// NATS server URL (e.g., "nats://localhost:4222").
    pub url: String,
    /// JetStream subject to publish CDC events to.
    pub subject: String,
}

/// Configuration for a CDC sink.
///
/// This enum allows for extensibility - new sink types can be added
/// as new variants without breaking existing code.
#[derive(Debug, Clone)]
pub enum CdcSinkConfig {
    /// NATS core streaming sink (fire-and-forget).
    Nats(NatsSinkConfig),
    /// NATS JetStream sink (persistent, acknowledged).
    JetStream(JetStreamSinkConfig),
}

impl CdcSinkConfig {
    /// Parse a CDC sink configuration from PRAGMA parameters.
    ///
    /// Expected format: `sink_type,url,subject`
    /// Examples:
    /// - `nats,nats://localhost:4222,cdc.events` (core NATS, fire-and-forget)
    /// - `jetstream,nats://localhost:4222,cdc.db1` (JetStream, persistent)
    pub fn parse(sink_type: &str, url: &str, subject: &str) -> Result<Self, CdcSinkError> {
        // Validate common parameters
        if url.is_empty() {
            return Err(CdcSinkError::ConfigError(
                "URL cannot be empty".to_string(),
            ));
        }
        if subject.is_empty() {
            return Err(CdcSinkError::ConfigError(
                "Subject cannot be empty".to_string(),
            ));
        }

        match sink_type.to_lowercase().as_str() {
            "nats" => Ok(CdcSinkConfig::Nats(NatsSinkConfig {
                url: url.to_string(),
                subject: subject.to_string(),
            })),
            "jetstream" => Ok(CdcSinkConfig::JetStream(JetStreamSinkConfig {
                url: url.to_string(),
                subject: subject.to_string(),
            })),
            _ => Err(CdcSinkError::ConfigError(format!(
                "Unknown sink type: '{sink_type}'. Supported types: nats, jetstream"
            ))),
        }
    }

    /// Get the sink type name.
    pub fn sink_type(&self) -> &'static str {
        match self {
            CdcSinkConfig::Nats(_) => "nats",
            CdcSinkConfig::JetStream(_) => "jetstream",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nats_config() {
        let config = CdcSinkConfig::parse("nats", "nats://localhost:4222", "test.subject")
            .expect("Should parse valid NATS config");

        match config {
            CdcSinkConfig::Nats(nats_config) => {
                assert_eq!(nats_config.url, "nats://localhost:4222");
                assert_eq!(nats_config.subject, "test.subject");
            }
            _ => panic!("Expected Nats config"),
        }
    }

    #[test]
    fn test_parse_nats_config_case_insensitive() {
        let config = CdcSinkConfig::parse("NATS", "nats://localhost:4222", "test.subject")
            .expect("Should parse NATS in uppercase");

        assert_eq!(config.sink_type(), "nats");
    }

    #[test]
    fn test_parse_jetstream_config() {
        let config = CdcSinkConfig::parse("jetstream", "nats://localhost:4222", "cdc.db1")
            .expect("Should parse valid JetStream config");

        match config {
            CdcSinkConfig::JetStream(js_config) => {
                assert_eq!(js_config.url, "nats://localhost:4222");
                assert_eq!(js_config.subject, "cdc.db1");
            }
            _ => panic!("Expected JetStream config"),
        }
    }

    #[test]
    fn test_parse_jetstream_config_case_insensitive() {
        let config = CdcSinkConfig::parse("JETSTREAM", "nats://localhost:4222", "cdc.db1")
            .expect("Should parse JETSTREAM in uppercase");

        assert_eq!(config.sink_type(), "jetstream");
    }

    #[test]
    fn test_parse_unknown_sink_type() {
        let result = CdcSinkConfig::parse("kafka", "url", "topic");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CdcSinkError::ConfigError(_)));
    }

    #[test]
    fn test_parse_empty_url() {
        let result = CdcSinkConfig::parse("nats", "", "subject");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty_subject() {
        let result = CdcSinkConfig::parse("nats", "nats://localhost:4222", "");
        assert!(result.is_err());
    }
}
