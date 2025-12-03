#!/bin/bash
# Unified test script for students to test their sender implementation
# Usage: ./test_sender.sh <your_sender.py> [payload_file]
# Optional: NUM_RUNS (env), RECEIVER_PORT (env, default 5001)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="ecs152a-simulator"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

print_header() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

abs_path() {
    python3 -c 'import os, sys; print(os.path.abspath(sys.argv[1]))' "$1"
}

resolve_payload_file() {
    local candidate="$1"
    local search_paths=()

    if [[ -n "$candidate" ]]; then
        search_paths+=("$candidate")
        if [[ "$candidate" != /* ]]; then
            search_paths+=("$SCRIPT_DIR/$candidate")
            search_paths+=("$SCRIPT_DIR/hdd/$candidate")
        fi
    fi

    for path in "${search_paths[@]}"; do
        if [ -f "$path" ]; then
            abs_path "$path"
            return 0
        fi
    done

    return 1
}

derive_received_name() {
    local filename="$1"
    if [[ "$filename" == *.* ]]; then
        local base="${filename%.*}"
        local ext="${filename##*.}"
        echo "${base}_received.${ext}"
    else
        echo "${filename}_received"
    fi
}

ensure_container_running() {
    print_info "Ensuring simulator container is available..."
    if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        print_warning "Simulator container not found. Launching a new one..."
        "$SCRIPT_DIR/start-simulator.sh"
        sleep 5
        return
    fi

    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        print_info "Starting existing simulator container..."
        docker start "$CONTAINER_NAME" >/dev/null
        sleep 3
    else
        print_info "Simulator container already running"
    fi
}

start_receiver() {
    print_info "Starting receiver on port $RECEIVER_PORT..."
    docker exec "$CONTAINER_NAME" pkill -f receiver.py >/dev/null 2>&1 || true
    docker exec "$CONTAINER_NAME" rm -f "$CONTAINER_OUTPUT_FILE" >/dev/null 2>&1 || true
    docker exec -d "$CONTAINER_NAME" env \
        RECEIVER_PORT="$RECEIVER_PORT" \
        TEST_FILE="$CONTAINER_PAYLOAD_FILE" \
        PAYLOAD_FILE="$CONTAINER_PAYLOAD_FILE" \
        RECEIVER_OUTPUT_FILE="$CONTAINER_OUTPUT_FILE" \
        python3 /app/receiver.py >/dev/null
    sleep 2
}

if [ $# -eq 0 ]; then
    print_error "No sender file specified"
    echo "Usage: ./test_sender.sh <your_sender.py> [payload_file]"
    echo "Example: ./test_sender.sh my_tcp_tahoe.py file.zip"
    exit 1
fi

SENDER_FILE=$1
PAYLOAD_ARG=${2:-file.zip}
NUM_RUNS="${NUM_RUNS:-10}"              # default 10 runs
RECEIVER_PORT="${RECEIVER_PORT:-5001}"  # default receiver port

if [ ! -f "$SENDER_FILE" ]; then
    print_error "Sender file '$SENDER_FILE' not found"
    exit 1
fi

PAYLOAD_SOURCE="$(resolve_payload_file "$PAYLOAD_ARG" || true)"
if [ -z "${PAYLOAD_SOURCE:-}" ]; then
    print_error "Could not locate payload file '$PAYLOAD_ARG'."
    echo "Looked for it relative to current dir, $SCRIPT_DIR, and $SCRIPT_DIR/hdd."
    exit 1
fi

PAYLOAD_BASENAME="$(basename "$PAYLOAD_SOURCE")"
RECEIVED_BASENAME="$(derive_received_name "$PAYLOAD_BASENAME")"
CONTAINER_PAYLOAD_FILE="/hdd/$PAYLOAD_BASENAME"
CONTAINER_OUTPUT_FILE="/hdd/$RECEIVED_BASENAME"

print_header "ECS 152A - Testing Your Sender Implementation"
print_info "Sender file: $SENDER_FILE"
print_info "Payload file: $PAYLOAD_SOURCE (copied as $CONTAINER_PAYLOAD_FILE)"
print_info "Number of runs: $NUM_RUNS"
print_info "Receiver port (inside container): $RECEIVER_PORT"

# -------------------------------
# Step 1: Pre-flight checks
# -------------------------------
print_header "Step 1/4: Pre-flight Checks"

print_info "Checking Docker installation..."
if ! command -v docker &>/dev/null; then
    print_error "Docker is not installed or not in PATH"
    echo "Please install Docker first. See SETUP.md for instructions."
    exit 1
fi
print_success "Docker is installed"

print_info "Checking if Docker daemon is running..."
if ! docker info &>/dev/null; then
    print_error "Docker daemon is not running"
    echo "Please start Docker Desktop (macOS/Windows) or Docker service (Linux)"
    exit 1
fi
print_success "Docker daemon is running"

# -------------------------------
# Step 2: Copy files into container
# -------------------------------
print_header "Step 2/4: Preparing Test Environment"

ensure_container_running

print_info "Copying your sender file into container as /app/sender.py..."
docker cp "$SENDER_FILE" "$CONTAINER_NAME":/app/sender.py 2>&1 | grep -v "deprecated" || true
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    print_error "Failed to copy sender file into container"
    exit 1
fi
print_success "Sender file copied"

print_info "Copying payload ($PAYLOAD_BASENAME) into container..."
docker cp "$PAYLOAD_SOURCE" "$CONTAINER_NAME:$CONTAINER_PAYLOAD_FILE" >/dev/null
print_success "Payload ready inside container"

# -------------------------------
# Step 3: Run sender multiple times
# -------------------------------
print_header "Step 3/4: Running Your Sender (multiple runs)"

ALL_METRICS=""

for ((run = 1; run <= NUM_RUNS; run++)); do
    print_header "Run $run/$NUM_RUNS"
    start_receiver

    print_info "Executing your sender implementation inside container..."
    echo ""

    set +e
    SENDER_OUTPUT=$(
        docker exec \
            -e RECEIVER_PORT="$RECEIVER_PORT" \
            -e TEST_FILE="$CONTAINER_PAYLOAD_FILE" \
            -e PAYLOAD_FILE="$CONTAINER_PAYLOAD_FILE" \
            "$CONTAINER_NAME" python3 /app/sender.py 2>&1
    )
    SENDER_EXIT_CODE=$?
    set -e

    echo "$SENDER_OUTPUT"
    echo ""

    if [ $SENDER_EXIT_CODE -ne 0 ]; then
        print_error "Sender exited with error code $SENDER_EXIT_CODE on run $run"
        print_warning "Check the output above for error messages"
        exit 1
    fi

    METRICS_LINE=$(echo "$SENDER_OUTPUT" \
        | grep -E '^[0-9]+\.?[0-9]*,[0-9]+\.?[0-9]*,[0-9]+\.?[0-9]*,[0-9]+\.?[0-9]*$' \
        | tail -n 1)

    if [ -n "$METRICS_LINE" ]; then
        ALL_METRICS+="$METRICS_LINE"$'\n'
    else
        print_warning "Could not parse metrics on run $run. Skipping this run in averages."
    fi

    sleep 1
done

# -------------------------------
# Step 4: Aggregate metrics
# -------------------------------
print_header "Step 4/4: Performance Metrics (Averaged)"

if [ -z "$ALL_METRICS" ]; then
    print_warning "No valid metrics were collected. Make sure your sender prints: throughput,delay,jitter,score"
    exit 0
fi

AVG_METRICS=$(python3 - <<EOF
data = """$ALL_METRICS""".strip().splitlines()
ths, delays, jits, scores = [], [], [], []
for line in data:
    t, d, j, s = map(float, line.split(","))
    ths.append(t); delays.append(d); jits.append(j); scores.append(s)

def avg(lst): return sum(lst) / len(lst) if lst else 0.0

print(f"{avg(ths):.3f},{avg(delays):.6f},{avg(jits):.6f},{avg(scores):.3f}")
EOF
)

IFS=',' read -r THROUGHPUT AVG_DELAY AVG_JITTER SCORE <<<"$AVG_METRICS"

echo "Results (averaged over $NUM_RUNS runs):"
echo "  Throughput:  ${THROUGHPUT} bytes/sec"
echo "  Avg Delay:   ${AVG_DELAY} sec"
echo "  Avg Jitter:  ${AVG_JITTER} sec"
echo "  Score:       ${SCORE}"
echo ""

print_success "Test completed successfully!"
echo ""