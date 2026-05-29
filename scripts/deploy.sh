#!/bin/bash
# Deploy the Kinesis DSM CloudFormation stack.
# Usage: ./scripts/deploy.sh [stack-name] [stream-name] [aws-region]
# Outputs StreamName and StreamARN -- copy these into your .env file.

set -euo pipefail

STACK_NAME="${1:-kinesis-dsm-demo}"
STREAM_NAME="${2:-datadog-dsm-demo}"
AWS_REGION="${3:-${AWS_REGION:-us-east-1}}"

echo "Deploying stack: $STACK_NAME (region: $AWS_REGION)"

aws cloudformation deploy \
  --template-file infrastructure/kinesis-dsm-stack.yaml \
  --stack-name "$STACK_NAME" \
  --parameter-overrides StreamName="$STREAM_NAME" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$AWS_REGION"

echo ""
echo "Stack deployed. Copy these values into your .env file:"
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs' \
  --output table
