//! JSON Blob Parser Module
//!
//! This module handles parsing of S3 log files that contain a single JSON blob
//! with a top-level "Records" key pointing to an array of JSON records.
//!
//! This format is commonly used by AWS CloudTrail and other AWS services.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// A JSON blob structure with a Records array.
/// Each record in the array is a JSON object representing a single log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonLogRecords {
    /// Array of log records, where each record is a JSON object
    #[serde(rename = "Records")]
    pub records: Vec<serde_json::Map<String, JsonValue>>,
}

impl JsonLogRecords {
    /// Parse a JSON blob from a string into JsonLogRecords
    pub fn from_str(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// Get the number of records in this blob
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if there are no records
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_blob() {
        let json_data = r#"{
            "Records": [
                {"eventName": "CreateBucket", "eventTime": "2024-01-01T12:00:00Z"},
                {"eventName": "DeleteBucket", "eventTime": "2024-01-01T13:00:00Z"}
            ]
        }"#;

        let blob = JsonLogRecords::from_str(json_data).unwrap();
        assert_eq!(blob.len(), 2);
        assert!(!blob.is_empty());

        // Verify first record
        let first_record = &blob.records[0];
        assert_eq!(
            first_record.get("eventName").unwrap().as_str().unwrap(),
            "CreateBucket"
        );

        // Verify second record
        let second_record = &blob.records[1];
        assert_eq!(
            second_record.get("eventName").unwrap().as_str().unwrap(),
            "DeleteBucket"
        );
    }

    #[test]
    fn test_parse_empty_records() {
        let json_data = r#"{"Records": []}"#;

        let blob = JsonLogRecords::from_str(json_data).unwrap();
        assert_eq!(blob.len(), 0);
        assert!(blob.is_empty());
    }

    #[test]
    fn test_parse_complex_record() {
        let json_data = r#"{
            "Records": [
                {
                    "eventVersion": "1.09",
                    "userIdentity": {
                        "type": "AssumedRole",
                        "principalId": "EXAMPLE123"
                    },
                    "eventTime": "2024-01-01T12:00:00Z",
                    "eventSource": "s3.amazonaws.com",
                    "eventName": "GetObject",
                    "awsRegion": "us-east-1",
                    "requestParameters": {
                        "bucketName": "my-bucket",
                        "key": "my-file.txt"
                    }
                }
            ]
        }"#;

        let blob = JsonLogRecords::from_str(json_data).unwrap();
        assert_eq!(blob.len(), 1);

        let record = &blob.records[0];
        assert_eq!(
            record.get("eventName").unwrap().as_str().unwrap(),
            "GetObject"
        );
        assert_eq!(
            record.get("eventSource").unwrap().as_str().unwrap(),
            "s3.amazonaws.com"
        );

        // Verify nested object
        let user_identity = record.get("userIdentity").unwrap().as_object().unwrap();
        assert_eq!(
            user_identity.get("type").unwrap().as_str().unwrap(),
            "AssumedRole"
        );
    }

    #[test]
    fn test_parse_invalid_json() {
        let json_data = r#"{"Records": [invalid json}"#;

        let result = JsonLogRecords::from_str(json_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_records_field() {
        let json_data = r#"{"OtherField": []}"#;

        let result = JsonLogRecords::from_str(json_data);
        assert!(result.is_err());
    }
}
