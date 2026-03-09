use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

/// Construct a [`KeyValue`] with a [`Value::StringValue`] from a key and any value
/// that can be converted into a [`String`].
///
/// # Example
///
/// ```ignore
/// let kv = string_kv("cloud.provider", "aws");
/// let kv = string_kv("http.status_code", 200.to_string());
/// ```
pub(crate) fn string_kv(key: &str, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_kv_str_value() {
        let kv = string_kv("cloud.provider", "aws");
        assert_eq!(kv.key, "cloud.provider");
        let inner = kv.value.expect("value should be Some");
        match inner.value {
            Some(Value::StringValue(s)) => assert_eq!(s, "aws"),
            other => panic!("expected StringValue, got {:?}", other),
        }
    }
}
