#!/bin/bash
# Tear down the Kinesis DSM CloudFormation stack and all resources.
# Usage: ./scripts/teardown.sh [stack-name] [aws-region]
# WARNING: This deletes the Kinesis stream, S3 bucket, and IAM policies.

set -euo pipefail

STACK_NAME="${1:-kinesis-dsm-demo}"
AWS_REGION="${2:-${AWS_REGION:-us-east-1}}"

echo "WARNING: This will delete stack $STACK_NAME and all its resources."
read -p "Type 'yes' to confirm: " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
  echo "Aborted."
  exit 1
fi

# Empty the S3 bucket before deleting (CloudFormation cannot delete non-empty buckets)
BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='OutputBucketName'].OutputValue" \
  --output text 2>/dev/null || echo "")

if [ -n "$BUCKET" ]; then
  echo "Emptying S3 bucket: $BUCKET"
  aws s3 rm "s3://$BUCKET" --recursive --region "$AWS_REGION" || true
fi

echo "Deleting CloudFormation stack: $STACK_NAME"
aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$AWS_REGION"

aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$AWS_REGION"
echo "Stack deleted successfully."
