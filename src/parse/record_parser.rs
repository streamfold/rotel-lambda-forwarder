use aws_lambda_events::cloudwatch_logs::LogEntry;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::LogRecord,
};

use crate::parse::{
    cwlogs::{LogPlatform, ParserType},
    json::parse_json_log_entry,
    keyvalue::parse_keyvalue_log_entry,
};

pub(crate) struct RecordParser {
    platform: LogPlatform,
    parser_type: ParserType,
}

impl RecordParser {
    pub(crate) fn new(platform: LogPlatform, parser_type: ParserType) -> Self {
        Self {
            platform,
            parser_type,
        }
    }

    pub(crate) fn parse(&self, now_nanos: u64, log_entry: LogEntry) -> LogRecord {
        let mut lr = LogRecord {
            time_unix_nano: (log_entry.timestamp * 1_000_000) as u64,
            observed_time_unix_nano: now_nanos,
            attributes: vec![KeyValue {
                key: "cloudwatch.id".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(log_entry.id)),
                }),
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        match self.parser_type {
            ParserType::Json => {
                parse_json_log_entry(self.platform, log_entry.message, &mut lr);
            }
            ParserType::KeyValue => {
                parse_keyvalue_log_entry(self.platform, log_entry.message, &mut lr);
            }
            ParserType::Unknown => {
                // Auto-detect: try JSON first, otherwise plain text
                if log_entry.message.len() > 2 && log_entry.message.starts_with("{") {
                    parse_json_log_entry(self.platform, log_entry.message, &mut lr);
                } else {
                    lr.body = Some(AnyValue {
                        value: Some(Value::StringValue(log_entry.message)),
                    });
                }
            }
        }

        lr
    }
}
