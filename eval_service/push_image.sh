#!/bin/bash
# 建置並推送 eval-service 到 ECR

set -e

AWS_PROFILE="lab"
AWS_REGION="ap-northeast-1"
AWS_ACCOUNT_ID="781160412246"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_NAME="aiot/text-to-sql-benchmark"

cd "$(dirname "$0")/.."

echo "=========================================="
echo "建置並推送 eval-service"
echo "=========================================="

aws ecr get-login-password --region ${AWS_REGION} --profile ${AWS_PROFILE} | \
  docker login --username AWS --password-stdin ${ECR_REGISTRY}

docker build --no-cache -f eval_service/Dockerfile -t eval-service:latest .
docker tag eval-service:latest ${ECR_REGISTRY}/${IMAGE_NAME}:latest
docker push ${ECR_REGISTRY}/${IMAGE_NAME}:latest

echo ""
echo "✅ eval-service 推送完成: ${ECR_REGISTRY}/${IMAGE_NAME}:latest"
