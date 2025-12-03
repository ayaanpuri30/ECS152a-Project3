#!/usr/bin/env python3

from __future__ import annotations

import os
import socket
import sys
import time
from typing import List, Tuple

PACKET_SIZE = 1024
SEQ_ID_SIZE = 4
MSS = PACKET_SIZE - SEQ_ID_SIZE
ACK_TIMEOUT = 1.0
MAX_TIMEOUTS = 5

HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("RECEIVER_PORT", "5001"))

delays = []
total_payload_bytes = 0


def load_payload_chunks() -> List[bytes]:
    """
    Reads the selected payload file (or falls back to file.zip) and returns
    up to two MSS-sized chunks for the demo transfer.
    """
    candidates = [
        os.environ.get("TEST_FILE"),
        os.environ.get("PAYLOAD_FILE"),
        "/hdd/file.zip",
        "file.zip",
    ]

    for path in candidates:
        if not path:
            continue
        expanded = os.path.expanduser(path)
        if os.path.exists(expanded):
            with open(expanded, "rb") as f:
                data = f.read()

            chunks = [data[i : i + MSS] for i in range(0, len(data), MSS)]
            return chunks

    return []


def make_packet(seq_id: int, payload: bytes) -> bytes:
    return int.to_bytes(seq_id, SEQ_ID_SIZE, byteorder="big", signed=True) + payload


def parse_ack(packet: bytes) -> Tuple[int, str]:
    seq = int.from_bytes(packet[:SEQ_ID_SIZE], byteorder="big", signed=True)
    msg = packet[SEQ_ID_SIZE:].decode(errors="ignore")
    return seq, msg


def print_metrics(duration, throughput, avg_delay, avg_jitter) -> None:
    """
    Print transfer metrics in the format expected by test scripts.

    TODO: Students should replace the hardcoded delay/jitter/score values
    with actual calculated metrics from their implementation.
    """
    jitter_term = 15 / avg_jitter if avg_jitter > 0 else 0.0
    delay_term = 35 / avg_delay if avg_delay > 0 else 0.0

    score = (throughput / 2000) + jitter_term + delay_term

    print("\nDemo transfer complete!")
    print(f"duration={duration:.3f}s throughput={throughput:.2f} bytes/sec")
    print(
        f"avg_delay={avg_delay:.6f}s avg_jitter={avg_jitter:.6f}s (TODO: Calculate actual values)"
    )
    print(f"{throughput:.7f},{avg_delay:.7f},{avg_jitter:.7f},{score:.7f}")


def record_packet(send_t, ack_t, payload_len) -> None:
    global total_payload_bytes
    delay = ack_t - send_t
    delays.append(delay)
    total_payload_bytes += payload_len


def main() -> None:
    chunks = load_payload_chunks()

    if not chunks:
        print("No payload file found", file=sys.stderr)
        sys.exit(1)

    transfers: List[Tuple[int, bytes]] = []

    seq = 0
    for chunk in chunks:
        transfers.append((seq, chunk))
        seq += len(chunk)

    # EOF marker
    transfers.append((seq, b""))
    total_bytes = sum(len(chunk) for chunk in chunks)

    print(f"Connecting to receiver at {HOST}:{PORT}")
    print(
        f"Demo transfer will send {total_bytes} bytes across {len(chunks)} packets (+EOF)."
    )

    start = time.time()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(ACK_TIMEOUT)
        addr = (HOST, PORT)

        for seq_id, payload in transfers:
            pkt = make_packet(seq_id, payload)
            print(f"Sending seq={seq_id}, bytes={len(payload)}")
            first_send_time = None

            retries = 0
            while True:
                if first_send_time is None:
                    first_send_time = time.time()

                sock.sendto(pkt, addr)

                try:
                    ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
                except socket.timeout:
                    retries += 1
                    if retries > MAX_TIMEOUTS:
                        raise RuntimeError(
                            "Receiver did not respond (max retries exceeded)"
                        )
                    print(
                        f"Timeout waiting for ACK (seq={seq_id}). Retrying ({retries}/{MAX_TIMEOUTS})..."
                    )
                    continue

                ack_id, msg = parse_ack(ack_pkt)
                print(f"Received {msg.strip()} for ack_id={ack_id}")

                if msg.startswith("fin"):
                    # Respond with FIN/ACK to let receiver exit cleanly
                    fin_ack = make_packet(ack_id, b"FIN/ACK")
                    sock.sendto(fin_ack, addr)
                    duration = max(time.time() - start, 1e-6)

                    throughput, avg_delay, avg_jitter = compute_stats(duration)
                    print_metrics(duration, throughput, avg_delay, avg_jitter)
                    return

                if msg.startswith("ack") and ack_id >= seq_id + len(payload):
                    if len(payload) > 0:
                        record_packet(first_send_time, time.time(), len(payload))
                    break
                # Else: duplicate/stale ACK, continue waiting

        # Wait for final FIN after EOF packet
        while True:
            ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
            ack_id, msg = parse_ack(ack_pkt)
            if msg.startswith("fin"):
                fin_ack = make_packet(ack_id, b"FIN/ACK")
                sock.sendto(fin_ack, addr)
                duration = max(time.time() - start, 1e-6)

                throughput, avg_delay, avg_jitter = compute_stats(duration)
                print_metrics(duration, throughput, avg_delay, avg_jitter)

                return


def compute_stats(duration):
    throughput = total_payload_bytes / duration
    avg_delay = (sum(delays) / len(delays)) if delays else 0.0

    if len(delays) < 2:
        return throughput, avg_delay, 0.0

    diffs = [abs(delays[i] - delays[i - 1]) for i in range(1, len(delays))]
    return throughput, avg_delay, sum(diffs) / len(diffs)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Skeleton sender hit an error: {exc}", file=sys.stderr)
        sys.exit(1)
