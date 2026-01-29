#!/bin/bash

set -e

# Check arguments
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <bootstrap_zip_path> <function_name> [aws_region]"
    echo "Example: $0 bootstrap.zip my-lambda-function us-east-1"
    echo ""
    echo "Environment variables:"
    echo "  IAM_ROLE - Required for creating new functions"
    echo "  AWS_DEFAULT_REGION - AWS region to use (default: us-east-1)"
    exit 1
fi

BOOTSTRAP_ZIP="$1"
FUNCTION_NAME="$2"
AWS_REGION="${3:-${AWS_DEFAULT_REGION:-us-east-1}}"

# Verify AWS credentials are configured
echo "Verifying AWS credentials..."
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    echo "Error: AWS credentials are not properly configured"
    echo "Please configure your AWS credentials using one of the following methods:"
    echo "  - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
    echo "  - Run 'aws configure' to set up credentials"
    echo "  - Use an IAM role (if running on EC2/ECS/Lambda)"
    exit 1
fi

# Verify bootstrap zip exists
if [ ! -f "$BOOTSTRAP_ZIP" ]; then
    echo "Error: Bootstrap zip not found at: $BOOTSTRAP_ZIP"
    exit 1
fi

echo "Checking if Lambda function '$FUNCTION_NAME' exists in region $AWS_REGION..."

# Check if function exists
if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "Function exists. Updating function code..."

    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file "fileb://$BOOTSTRAP_ZIP" \
        --no-cli-pager \
        --publish \
        --region "$AWS_REGION" > /dev/null

    echo "Waiting for function to be updated..."
    aws lambda wait function-updated \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION"

    echo "Successfully updated Lambda function '$FUNCTION_NAME'"

    # Get the updated function info
    aws lambda get-function \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --query 'Configuration.[FunctionName,Runtime,LastModified,Version,CodeSize]' \
        --output table
else
    echo "Function does not exist. Creating new function..."

    # Check if IAM_ROLE is set
    if [ -z "$IAM_ROLE" ]; then
        echo "Error: IAM_ROLE environment variable is not set"
        echo "Please set IAM_ROLE to the ARN of the Lambda execution role"
        echo "Example: export IAM_ROLE=arn:aws:iam::123456789012:role/lambda-execution-role"
        exit 1
    fi

    echo "Creating Lambda function '$FUNCTION_NAME' with role: $IAM_ROLE"

    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --runtime provided.al2023 \
        --role "$IAM_ROLE" \
        --handler bootstrap \
        --zip-file "fileb://$BOOTSTRAP_ZIP" \
        --architectures x86_64 \
        --timeout 30 \
        --memory-size 256 \
        --region "$AWS_REGION"

    echo "Waiting for function to be active..."
    aws lambda wait function-active \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION"

    echo "Successfully created Lambda function '$FUNCTION_NAME'"

    # Get the new function info
    aws lambda get-function \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" \
        --query 'Configuration.[FunctionName,Runtime,LastModified,CodeSize]' \
        --output table
fi
