#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONTAINER_NAME="ecs152a-simulator"
IMAGE_NAME="ecs152a/simulator"

echo "[INFO] Stopping any existing simulator container..."
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "[INFO] Building simulator image ($IMAGE_NAME)..."
docker build -t "$IMAGE_NAME" .

echo "[INFO] Starting simulator container in the background..."
CONTAINER_ID="$(docker run -d \
    --name "$CONTAINER_NAME" \
    --cap-add=NET_ADMIN \
    --rm \
    -p 5001:5001/udp \
    -v "$SCRIPT_DIR/hdd":/hdd \
    "$IMAGE_NAME")"

echo "[SUCCESS] Simulator container is running (ID: $CONTAINER_ID)"
echo "          Training profile is applied automatically inside the container."
echo "          Use 'docker logs -f $CONTAINER_NAME' to follow output."
echo "          Run './test_sender.sh' or './test_fairness.sh' in another shell."