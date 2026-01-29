# S3 Log Processing - Quick Start Guide

## 5-Minute Setup

### 1. Update Lambda IAM Role

Add S3 read permissions to your Lambda function's IAM role:

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
      "Resource": "arn:aws:s3:::YOUR-LOG-BUCKET/*"
    }
  ]
}
```

### 2. Configure S3 Event Notification

Using AWS CLI:

```bash
# Create notification configuration
cat > notification.json <<EOF
{
  "LambdaFunctionConfigurations": [
    {
      "LambdaFunctionArn": "arn:aws:lambda:REGION:ACCOUNT:function:rotel-forwarder",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {"Name": "prefix", "Value": "logs/"},
            {"Name": "suffix", "Value": ".log"}
          ]
        }
      }
    }
  ]
}
EOF

# Apply notification
aws s3api put-bucket-notification-configuration \
  --bucket YOUR-LOG-BUCKET \
  --notification-configuration file://notification.json

# Grant S3 permission to invoke Lambda
aws lambda add-permission \
  --function-name rotel-forwarder \
  --statement-id s3-trigger \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::YOUR-LOG-BUCKET
```

### 3. Deploy Updated Lambda

```bash
# Build and deploy
make build
make deploy

# Or using AWS CLI
aws lambda update-function-code \
  --function-name rotel-forwarder \
  --zip-file fileb://target/lambda/bootstrap.zip
```

### 4. Test with Sample File

```bash
# Upload a test log file
echo '{"level":"info","msg":"test message"}' > test.log
aws s3 cp test.log s3://YOUR-LOG-BUCKET/logs/test.log

# Check Lambda logs
aws logs tail /aws/lambda/rotel-forwarder --follow
```

## Configuration (Optional)

Set environment variables on your Lambda function:

```bash
aws lambda update-function-configuration \
  --function-name rotel-forwarder \
  --environment Variables="{
    FORWARDER_S3_MAX_PARALLEL_OBJECTS=10,
    FORWARDER_S3_MAX_MEMORY_MB=200,
    FORWARDER_S3_BATCH_SIZE=1000
  }"
```

## Common Use Cases

### Use Case 1: Application Logs

**Scenario**: Your application writes JSON logs to S3

```bash
# Log format
{"timestamp":"2025-01-15T10:30:00Z","level":"info","msg":"Request processed","user_id":"123"}

# S3 structure
s3://my-logs/app/2025/01/15/app.log.gz

# Configuration
FORWARDER_S3_BATCH_SIZE=1000  # Good for JSON logs
```

### Use Case 2: ELB Access Logs

**Scenario**: Process AWS ELB access logs

```bash
# S3 structure (auto-created by ELB)
s3://my-logs/elb/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2025/01/15/

# Configuration
FORWARDER_S3_MAX_PARALLEL_OBJECTS=10  # ELB creates many small files
FORWARDER_S3_BATCH_SIZE=2000
```

### Use Case 3: CloudTrail Logs

**Scenario**: Process CloudTrail audit logs

```bash
# S3 structure (auto-created by CloudTrail)
s3://my-logs/cloudtrail/AWSLogs/123456789012/CloudTrail/us-east-1/2025/01/15/

# Configuration
FORWARDER_S3_MAX_PARALLEL_OBJECTS=5
FORWARDER_S3_BATCH_SIZE=500  # CloudTrail events are large
```

### Use Case 4: Compressed Application Logs

**Scenario**: Large compressed log files

```bash
# Log files
s3://my-logs/app/2025-01-15-app.log.gz  # 50MB compressed, 200MB uncompressed

# Configuration
FORWARDER_S3_MAX_PARALLEL_OBJECTS=2  # Large files
FORWARDER_S3_MAX_MEMORY_MB=250       # Handle decompressed size
FORWARDER_S3_BATCH_SIZE=2000         # Faster batching
```

## Troubleshooting

### Lambda Times Out

```bash
# Increase timeout
aws lambda update-function-configuration \
  --function-name rotel-forwarder \
  --timeout 300

# Or reduce concurrency
FORWARDER_S3_MAX_PARALLEL_OBJECTS=2
```

### Out of Memory

```bash
# Increase Lambda memory
aws lambda update-function-configuration \
  --function-name rotel-forwarder \
  --memory-size 2048

# Or reduce memory limit
FORWARDER_S3_MAX_MEMORY_MB=50
```

### S3 Access Denied

```bash
# Check IAM policy
aws iam get-role-policy \
  --role-name YOUR-LAMBDA-ROLE \
  --policy-name YOUR-POLICY-NAME

# Check bucket policy
aws s3api get-bucket-policy --bucket YOUR-LOG-BUCKET
```

### Logs Not Parsing

```bash
# Check format detection in CloudWatch Logs
aws logs tail /aws/lambda/rotel-forwarder --follow | grep "Detected log format"

# Validate log format
# JSON: Each line must be valid JSON object
{"level":"info","msg":"test"}

# Key-value: Space-separated key=value pairs
time="2025-01-15T10:30:00Z" level=info msg="test"
```

## Verification

### Check Event Notification

```bash
aws s3api get-bucket-notification-configuration \
  --bucket YOUR-LOG-BUCKET
```

### Check Lambda Permissions

```bash
aws lambda get-policy --function-name rotel-forwarder
```

### Test Manually

```bash
# Create test event
cat > test-event.json <<EOF
{
  "Records": [{
    "eventVersion": "2.1",
    "eventSource": "aws:s3",
    "awsRegion": "us-east-1",
    "eventTime": "2025-01-15T10:30:00.000Z",
    "eventName": "ObjectCreated:Put",
    "s3": {
      "bucket": {"name": "YOUR-LOG-BUCKET"},
      "object": {"key": "logs/test.log", "size": 1024}
    }
  }]
}
EOF

# Invoke Lambda
aws lambda invoke \
  --function-name rotel-forwarder \
  --payload file://test-event.json \
  response.json

cat response.json
```

## Monitoring

### Key CloudWatch Metrics

```bash
# View invocation count
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=rotel-forwarder \
  --start-time 2025-01-15T00:00:00Z \
  --end-time 2025-01-15T23:59:59Z \
  --period 3600 \
  --statistics Sum

# View error rate
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=rotel-forwarder \
  --start-time 2025-01-15T00:00:00Z \
  --end-time 2025-01-15T23:59:59Z \
  --period 3600 \
  --statistics Sum
```

### View Logs

```bash
# Tail logs in real-time
aws logs tail /aws/lambda/rotel-forwarder --follow

# Search for errors
aws logs tail /aws/lambda/rotel-forwarder --filter-pattern "ERROR"

# Search for S3 processing
aws logs tail /aws/lambda/rotel-forwarder --filter-pattern "S3"
```

## Performance Tuning

### For Small Files (< 1MB)

```bash
FORWARDER_S3_MAX_PARALLEL_OBJECTS=20
FORWARDER_S3_MAX_MEMORY_MB=100
FORWARDER_S3_BATCH_SIZE=1000
```

### For Medium Files (1-10MB)

```bash
FORWARDER_S3_MAX_PARALLEL_OBJECTS=10
FORWARDER_S3_MAX_MEMORY_MB=150
FORWARDER_S3_BATCH_SIZE=1000
```

### For Large Files (> 10MB)

```bash
FORWARDER_S3_MAX_PARALLEL_OBJECTS=3
FORWARDER_S3_MAX_MEMORY_MB=300
FORWARDER_S3_BATCH_SIZE=2000
```

### For Compressed Files

```bash
# Account for decompressed size
FORWARDER_S3_MAX_PARALLEL_OBJECTS=5
FORWARDER_S3_MAX_MEMORY_MB=200  # 5x compressed size
FORWARDER_S3_BATCH_SIZE=1000
```

## Best Practices

1. **Always use compression**: Saves S3 costs and transfer time
   ```bash
   gzip application.log
   aws s3 cp application.log.gz s3://bucket/logs/
   ```

2. **Organize by date**: Makes management easier
   ```
   s3://bucket/logs/2025/01/15/app.log.gz
   ```

3. **Use event filters**: Only process relevant files
   ```json
   {"Name": "prefix", "Value": "logs/production/"}
   {"Name": "suffix", "Value": ".log.gz"}
   ```

4. **Monitor costs**: Set up billing alerts
   ```bash
   # S3 GetObject: $0.0004/1000 requests
   # Lambda execution: $0.0000166667/GB-second
   ```

5. **Test formats**: Validate before enabling at scale
   ```bash
   head -10 your-log-file.log | jq .  # Test JSON
   ```

## Next Steps

- Read full documentation: [S3_LOG_PROCESSING.md](./S3_LOG_PROCESSING.md)
- Review implementation: [S3_IMPLEMENTATION_SUMMARY.md](./S3_IMPLEMENTATION_SUMMARY.md)
- Check examples: [../examples/](../examples/)
- Configure log processors: `FORWARDER_OTLP_LOG_PROCESSORS`

## Getting Help

- Check CloudWatch Logs for detailed error messages
- Review [S3_LOG_PROCESSING.md](./S3_LOG_PROCESSING.md) for troubleshooting
- Verify IAM permissions and bucket policies
- Test with small sample files first