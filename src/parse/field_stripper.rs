//! Field Stripper Module
//!
//! This module provides functionality to strip sensitive fields from parsed log data
//! based on JSON path identifiers. It supports recursive traversal of nested JSON
//! structures to remove sensitive information like credentials, tokens, and keys.
//!
//! # Overview
//!
//! The field stripper uses a tree visitor pattern to recursively navigate through
//! `serde_json::Map` structures and remove fields at specified JSON paths. This is
//! particularly useful for sanitizing AWS CloudTrail logs that may contain sensitive
//! credential information in response elements.
//!

use serde_json::Value as JsonValue;
use tracing::debug;

use crate::parse::cwlogs::LogPlatform;

/// Strips sensitive fields from parsed log data based on the log platform.
pub struct FieldStripper {
    paths_to_strip: Vec<JsonPath>,
}

impl FieldStripper {
    pub fn new(platform: LogPlatform) -> Self {
        // TODO: Add paths here by log platform, support configurable overrides in the future
        let paths_to_strip = match platform {
            LogPlatform::Cloudtrail => vec![
                // Strip session tokens from AWS credentials
                JsonPath::parse("responseElements.credentials.sessionToken"),
            ],
            _ => vec![],
        };

        Self { paths_to_strip }
    }

    /// Strip sensitive fields from a JSON map
    pub fn strip_fields(&self, map: &mut serde_json::Map<String, JsonValue>) {
        for path in &self.paths_to_strip {
            if path.remove_from_map(map) {
                debug!("Stripped field at path: {}", path);
            }
        }
    }
}

/// Represents a JSON path like "responseElements.credentials.sessionToken"
#[derive(Debug, Clone)]
pub struct JsonPath {
    segments: Vec<String>,
}

impl JsonPath {
    /// Parse a dot-separated path string into a JsonPath
    pub fn parse(path: &str) -> Self {
        let segments = path.split('.').map(|s| s.to_string()).collect();
        Self { segments }
    }

    /// Remove the field at this path from the given map
    /// Returns true if the field was found and removed
    pub fn remove_from_map(&self, map: &mut serde_json::Map<String, JsonValue>) -> bool {
        if self.segments.is_empty() {
            return false;
        }

        if self.segments.len() == 1 {
            // Base case: remove directly from this map
            return map.remove(&self.segments[0]).is_some();
        }

        // Recursive case: navigate to the parent object
        let first = &self.segments[0];
        let remaining = &self.segments[1..];

        if let Some(JsonValue::Object(child_map)) = map.get_mut(first) {
            let child_path = JsonPath {
                segments: remaining.to_vec(),
            };
            return child_path.remove_from_map(child_map);
        }

        false
    }
}

impl std::fmt::Display for JsonPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.segments.join("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_path_parse() {
        let path = JsonPath::parse("responseElements.credentials.sessionToken");
        assert_eq!(path.segments.len(), 3);
        assert_eq!(path.segments[0], "responseElements");
        assert_eq!(path.segments[1], "credentials");
        assert_eq!(path.segments[2], "sessionToken");
    }

    #[test]
    fn test_remove_from_map_simple() {
        let mut map = serde_json::Map::new();
        map.insert(
            "field1".to_string(),
            JsonValue::String("value1".to_string()),
        );
        map.insert(
            "field2".to_string(),
            JsonValue::String("value2".to_string()),
        );

        let path = JsonPath::parse("field1");
        assert!(path.remove_from_map(&mut map));
        assert!(!map.contains_key("field1"));
        assert!(map.contains_key("field2"));
    }

    #[test]
    fn test_remove_from_map_nested() {
        let mut map = serde_json::Map::new();
        let mut credentials = serde_json::Map::new();
        credentials.insert(
            "accessKeyId".to_string(),
            JsonValue::String("AKIAIOSFODNN7EXAMPLE".to_string()),
        );
        credentials.insert(
            "secretAccessKey".to_string(),
            JsonValue::String("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
        );
        credentials.insert(
            "sessionToken".to_string(),
            JsonValue::String("AQoDYXdzEJr...".to_string()),
        );

        let mut response_elements = serde_json::Map::new();
        response_elements.insert("credentials".to_string(), JsonValue::Object(credentials));

        map.insert(
            "responseElements".to_string(),
            JsonValue::Object(response_elements),
        );
        map.insert(
            "eventName".to_string(),
            JsonValue::String("AssumeRole".to_string()),
        );

        // Remove sessionToken
        let path = JsonPath::parse("responseElements.credentials.sessionToken");
        assert!(path.remove_from_map(&mut map));

        // Verify it was removed
        if let Some(JsonValue::Object(response)) = map.get("responseElements") {
            if let Some(JsonValue::Object(creds)) = response.get("credentials") {
                assert!(!creds.contains_key("sessionToken"));
                assert!(creds.contains_key("accessKeyId"));
                assert!(creds.contains_key("secretAccessKey"));
            } else {
                panic!("credentials not found");
            }
        } else {
            panic!("responseElements not found");
        }

        // eventName should still be there
        assert!(map.contains_key("eventName"));
    }

    #[test]
    fn test_remove_from_map_nonexistent() {
        let mut map = serde_json::Map::new();
        map.insert(
            "field1".to_string(),
            JsonValue::String("value1".to_string()),
        );

        let path = JsonPath::parse("nonexistent.path.here");
        assert!(!path.remove_from_map(&mut map));
        assert!(map.contains_key("field1"));
    }

    #[test]
    fn test_remove_from_map_deep_nested() {
        let mut map = serde_json::Map::new();

        let mut level3 = serde_json::Map::new();
        level3.insert(
            "secret".to_string(),
            JsonValue::String("password123".to_string()),
        );
        level3.insert("public".to_string(), JsonValue::String("open".to_string()));

        let mut level2 = serde_json::Map::new();
        level2.insert("level3".to_string(), JsonValue::Object(level3));

        let mut level1 = serde_json::Map::new();
        level1.insert("level2".to_string(), JsonValue::Object(level2));

        map.insert("level1".to_string(), JsonValue::Object(level1));

        let path = JsonPath::parse("level1.level2.level3.secret");
        assert!(path.remove_from_map(&mut map));

        // Verify the secret was removed but public remains
        if let Some(JsonValue::Object(l1)) = map.get("level1") {
            if let Some(JsonValue::Object(l2)) = l1.get("level2") {
                if let Some(JsonValue::Object(l3)) = l2.get("level3") {
                    assert!(!l3.contains_key("secret"));
                    assert!(l3.contains_key("public"));
                } else {
                    panic!("level3 not found");
                }
            } else {
                panic!("level2 not found");
            }
        } else {
            panic!("level1 not found");
        }
    }

    #[test]
    fn test_field_stripper_cloudtrail() {
        let stripper = FieldStripper::new(LogPlatform::Cloudtrail);
        assert!(!stripper.paths_to_strip.is_empty());

        let mut map = serde_json::Map::new();
        let mut credentials = serde_json::Map::new();
        credentials.insert(
            "accessKeyId".to_string(),
            JsonValue::String("AKIAIOSFODNN7EXAMPLE".to_string()),
        );
        credentials.insert(
            "secretAccessKey".to_string(),
            JsonValue::String("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
        );
        credentials.insert(
            "sessionToken".to_string(),
            JsonValue::String("AQoDYXdzEJr...".to_string()),
        );

        let mut response_elements = serde_json::Map::new();
        response_elements.insert("credentials".to_string(), JsonValue::Object(credentials));

        map.insert(
            "responseElements".to_string(),
            JsonValue::Object(response_elements),
        );
        map.insert(
            "eventName".to_string(),
            JsonValue::String("AssumeRole".to_string()),
        );

        stripper.strip_fields(&mut map);

        // Verify sensitive fields were removed
        if let Some(JsonValue::Object(response)) = map.get("responseElements") {
            if let Some(JsonValue::Object(creds)) = response.get("credentials") {
                assert!(
                    !creds.contains_key("sessionToken"),
                    "sessionToken should be removed"
                );
                assert!(
                    creds.contains_key("accessKeyId"),
                    "accessKeyId should not be removed"
                );
                assert!(
                    creds.contains_key("secretAccessKey"),
                    "secretAccessKey should not be removed"
                );
            } else {
                panic!("credentials not found");
            }
        } else {
            panic!("responseElements not found");
        }

        // eventName should still be there
        assert!(map.contains_key("eventName"));
    }
}
