# S3 Log Processing

This document describes the S3 event notification processing feature in rotel-lambda-forwarder, which enables automated ingestion and parsing of log files stored in S3 buckets.

## Overview

The S3 log processor allows you to automatically process log files uploaded to S3 by:
1. Receiving S3 event notifications via Lambda
2. Loading log files from S3 (with automatic decompression)
3. Parsing log lines into OpenTelemetry (OTLP) log records
4. Forwarding logs to your configured OTLP endpoint

This is particularly useful for:
- Processing application logs written to S3
- Ingesting logs from services that only support S3 output
- Batch processing of historical logs
- Processing large log files that exceed CloudWatch Logs limits

## Features

- **Automatic Format Detection**: Detects JSON and key-value log formats automatically
- **Compression Support**: Automatically decompresses `.gz` and `.gzip` files
- **Parallel Processing**: Processes multiple S3 objects concurrently with configurable limits
- **Memory Management**: Controls memory usage by limiting concurrent object loading
- **Efficient Batching**: Batches log records to optimize network usage
- **Rich Metadata**: Attaches S3 bucket, key, and AWS resource attributes to all logs

## Configuration

### Environment Variables

Configure the S3 log processor using these environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `FORWARDER_S3_MAX_PARALLEL_OBJECTS` | Maximum number of S3 objects to process concurrently | `5` |
| `FORWARDER_S3_MAX_MEMORY_MB` | Maximum total memory (in MB) for loaded objects | `100` |
| `FORWARDER_S3_BATCH_SIZE` | Number of log records to batch before sending | `1000` |

### Example Configuration

```bash
export FORWARDER_S3_MAX_PARALLEL_OBJECTS=10
export FORWARDER_S3_MAX_MEMORY_MB=200
export FORWARDER_S3_BATCH_SIZE=500
```

## Setting Up S3 Event Notifications

### 1. Create an S3 Bucket Event Notification

Configure your S3 bucket to send event notifications to your Lambda function:

```json
{
  "LambdaFunctionConfigurations": [
    {
      "Id": "LogProcessing",
      "LambdaFunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:rotel-forwarder",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "logs/"
            },
            {
              "Name": "suffix",
              "Value": ".log"
            }
          ]
        }
      }
    }
  ]
}
```

### 2. Grant S3 Permissions

Ensure your Lambda function's IAM role has permission to read from the S3 bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::my-log-bucket/*"
    }
  ]
}
```

### 3. Grant Lambda Invocation Permission

Allow S3 to invoke your Lambda function:

```bash
aws lambda add-permission \
  --function-name rotel-forwarder \
  --statement-id s3-trigger \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::my-log-bucket
```

## Supported Log Formats

### JSON Logs

Each line should be a valid JSON object:

```json
{"timestamp":"2025-01-15T10:30:00Z","level":"info","msg":"Application started","service":"web-api"}
{"timestamp":"2025-01-15T10:30:01Z","level":"error","msg":"Connection failed","error":"timeout"}
```

**Parsed Fields:**
- `level` → Mapped to OTLP severity
- `msg` or `message` → Used as log body
- `timestamp` or `time` → Parsed as log timestamp
- All other fields → Added as log attributes

### Key-Value Logs

Each line should contain space-separated key=value pairs:

```
time="2025-01-15T10:30:00Z" level=info msg="Application started" service=web-api
time="2025-01-15T10:30:01Z" level=error msg="Connection failed" error=timeout
```

**Parsed Fields:**
- `level` → Mapped to OTLP severity
- `msg` or `message` → Used as log body
- `time` or `timestamp` → Parsed as log timestamp
- All other fields → Added as log attributes

### Plain Text Logs

Lines that don't match JSON or key-value format are treated as plain text:

```
Application started successfully
Processing request from 192.168.1.100
Error: Connection timeout
```

Plain text logs are stored as-is in the log body with minimal parsing.

## Resource Attributes

All logs processed from S3 events include these resource attributes:

| Attribute | Description | Example |
|-----------|-------------|---------|
| `cloud.provider` | Cloud provider | `aws` |
| `cloud.region` | AWS region | `us-east-1` |
| `cloud.account.id` | AWS account ID | `123456789012` |
| `s3.bucket.name` | S3 bucket name | `my-log-bucket` |
| `s3.object.key` | S3 object key | `logs/app.log` |

## Event Notification Structure

The Lambda function receives S3 event notifications in this format:

```json
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-west-2",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": {
          "name": "my-log-bucket",
          "arn": "arn:aws:s3:::my-log-bucket"
        },
        "object": {
          "key": "logs/application.log",
          "size": 1024,
          "eTag": "d41d8cd98f00b204e9800998ecf8427e"
        }
      }
    }
  ]
}
```

## Processing Flow

1. **Event Reception**: Lambda receives S3 event notification
2. **Record Processing**: For each S3 record in the event:
   - Log event metadata (version, source, name, region, time)
   - Extract bucket name and object key
   - Acquire memory permit based on object size
3. **Object Loading**: Download object from S3
4. **Decompression**: Automatically decompress if file has `.gz` or `.gzip` extension
5. **Format Detection**: Analyze first few lines to detect log format
6. **Line Parsing**: Parse each log line using detected format
7. **Batching**: Group parsed logs into batches (default 1000 records)
8. **Resource Logs Creation**: Create OTLP ResourceLogs with metadata
9. **Forwarding**: Send batched logs to configured OTLP endpoint

## Performance Considerations

### Parallel Processing

The processor uses a semaphore-based approach to control parallelism:
- **Concurrency Limit**: Max number of S3 objects processed simultaneously
- **Memory Limit**: Total bytes of S3 objects loaded in memory at once

Example: With `max_parallel_objects=5` and `max_memory_bytes=100MB`:
- Up to 5 small files can be processed concurrently
- Only 2-3 large files (30-50MB each) may be processed at once
- Automatically balances throughput and memory usage

### Batching

Logs are batched into ResourceLogs structures before sending:
- **Batch Size**: Number of log records per ResourceLogs (default 1000)
- **Network Efficiency**: Reduces number of channel sends
- **Memory Efficiency**: Releases memory after each batch is sent

### Lambda Timeout

Ensure your Lambda timeout is sufficient for processing:
- Small files (<10MB): 30-60 seconds
- Medium files (10-100MB): 2-5 minutes
- Large files (>100MB): 5-15 minutes
- Multiple large files: Consider increasing timeout or reducing concurrency

## Example Usage

### Example 1: Application Logs

Store application logs in S3 with this structure:
```
s3://my-logs/
  app1/
    2025-01-15/
      app.log.gz
      app-errors.log.gz
  app2/
    2025-01-15/
      service.log.gz
```

Configure S3 event notification with prefix filter `app1/` to process only app1 logs.

### Example 2: ELB Access Logs

AWS ELB can write access logs directly to S3. Configure the forwarder to process these:

```bash
# S3 path: s3://my-logs/elb/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2025/01/15/
export FORWARDER_S3_MAX_PARALLEL_OBJECTS=10
export FORWARDER_S3_BATCH_SIZE=2000
```

### Example 3: CloudTrail Logs

Process CloudTrail logs stored in S3:

```bash
# CloudTrail logs are JSON format
export FORWARDER_S3_MAX_PARALLEL_OBJECTS=5
export FORWARDER_S3_BATCH_SIZE=500
```

## Troubleshooting

### High Memory Usage

**Symptom**: Lambda function running out of memory

**Solutions**:
- Reduce `FORWARDER_S3_MAX_MEMORY_MB`
- Reduce `FORWARDER_S3_MAX_PARALLEL_OBJECTS`
- Increase Lambda memory allocation
- Process smaller files or filter by prefix

### Timeout Issues

**Symptom**: Lambda function timing out

**Solutions**:
- Increase Lambda timeout
- Reduce `FORWARDER_S3_MAX_PARALLEL_OBJECTS`
- Increase `FORWARDER_S3_BATCH_SIZE` for faster processing
- Split large files into smaller chunks

### Failed S3 Object Access

**Symptom**: Error loading S3 objects

**Solutions**:
- Verify IAM permissions (s3:GetObject)
- Check S3 bucket policy
- Verify object key is correctly URL-decoded
- Ensure object exists and is not archived to Glacier

### Parse Errors

**Symptom**: Logs not parsing correctly

**Solutions**:
- Check log format matches JSON or key-value patterns
- Verify UTF-8 encoding
- Check for malformed JSON (missing quotes, trailing commas)
- Review logs for format detection output

## Testing

### Test with Example Event

Use the example S3 event notification:

```bash
# Located at examples/s3-event-notification.json
aws lambda invoke \
  --function-name rotel-forwarder \
  --payload file://examples/s3-event-notification.json \
  response.json
```

### Test with Sample Logs

Sample log files are provided in `examples/sample-logs/`:
- `json-logs.log`: JSON format example
- `keyvalue-logs.log`: Key-value format example

Upload these to S3 and trigger the Lambda function:

```bash
aws s3 cp examples/sample-logs/json-logs.log s3://my-log-bucket/logs/
```

## Monitoring

Key metrics to monitor:
- **Lambda Duration**: Time to process S3 events
- **Lambda Memory**: Peak memory usage
- **Lambda Errors**: Failed invocations
- **S3 GetObject Requests**: Number of objects downloaded
- **OTLP Export Success**: Logs successfully forwarded

CloudWatch Logs will contain:
- Event record details (bucket, key, size)
- Decompression information
- Line counts and batch sizes
- Parse errors and warnings

## Best Practices

1. **Use Compression**: Always compress log files (gzip) to reduce S3 storage and transfer costs
2. **Partition by Date**: Organize logs by date for easier management and filtering
3. **Filter Events**: Use S3 event filters (prefix/suffix) to only process relevant files
4. **Tune Batch Size**: Larger batches are more efficient but use more memory
5. **Monitor Costs**: S3 GetObject requests and data transfer have costs
6. **Use Lifecycle Policies**: Archive or delete old logs from S3 to reduce costs
7. **Test Formats**: Validate log formats before enabling automatic processing
8. **Set Appropriate Timeouts**: Match Lambda timeout to expected file sizes

## Limitations

- Maximum file size: Limited by Lambda memory (up to 10GB with max Lambda memory)
- Processing time: Limited by Lambda timeout (max 15 minutes)
- Line-based parsing: Each line must be a complete log entry
- UTF-8 encoding: Non-UTF-8 files may fail to parse
- No multi-line logs: Stack traces or multi-line messages must be in a single field

## See Also

- [AWS S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html)
- [OpenTelemetry Logs Specification](https://opentelemetry.io/docs/specs/otel/logs/)
- [Lambda with S3 Tutorial](https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html)