#!/bin/bash
set -e

CONTAINER_NAME="mingo-dev-minio"

echo "Stopping and removing existing $CONTAINER_NAME container if it exists..."
podman stop $CONTAINER_NAME 2>/dev/null || true
podman rm $CONTAINER_NAME 2>/dev/null || true

echo "Starting MinIO..."
podman run -d --name $CONTAINER_NAME -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  quay.io/minio/minio server /data --console-address ":9001"

echo "Waiting for MinIO to start..."
sleep 5

echo "Creating test buckets..."
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_DEFAULT_REGION="us-east-1"
ENDPOINT="http://localhost:9000"

# Using AWS CLI to create buckets, assuming awscli is installed or we use podman directly
# We can use podman exec with the minio client (mc) to create buckets inside the container securely
podman exec $CONTAINER_NAME mc alias set myminio http://localhost:9000 minioadmin minioadmin
podman exec $CONTAINER_NAME mc mb myminio/microbesng-plasmidseq --ignore-existing
podman exec $CONTAINER_NAME mc mb myminio/microbesng-data --ignore-existing
podman exec $CONTAINER_NAME mc mb myminio/dummy-bucket --ignore-existing

echo "MinIO is running at $ENDPOINT (Console: http://localhost:9001)"
echo "Test buckets created."
echo "Provide AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin locally, and use --s3-endpoint-url $ENDPOINT with mingo-upload."
