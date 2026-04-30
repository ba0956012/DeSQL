#!/bin/bash
# 建置並推送 chart-service 到 ECR

set -e

AWS_PROFILE="lab"
AWS_REGION="ap-northeast-1"
AWS_ACCOUNT_ID="781160412246"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_NAME="aiot/chart-service"

cd "$(dirname "$0")/.."

echo "=========================================="
echo "建置並推送 chart-service"
echo "=========================================="

aws ecr get-login-password --region ${AWS_REGION} --profile ${AWS_PROFILE} | \
  docker login --username AWS --password-stdin ${ECR_REGISTRY}

docker build --no-cache -f chart_service/Dockerfile -t chart-service:latest .
docker tag chart-service:latest ${ECR_REGISTRY}/${IMAGE_NAME}:latest
docker push ${ECR_REGISTRY}/${IMAGE_NAME}:latest

echo ""
echo "✅ chart-service 推送完成: ${ECR_REGISTRY}/${IMAGE_NAME}:latest"
