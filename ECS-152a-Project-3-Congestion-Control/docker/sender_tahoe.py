#!/usr/bin/env python3
"""
TCP Tahoe-style sender for ECS 152A Project 3.
"""

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

# reads the selected payload file and splits into MSS sized chunks
def load_payload_chunks() -> List[bytes]:
    candidates = [
        os.environ.get("TEST_FILE"),
        os.environ.get("PAYLOAD_FILE"),
        "/hdd/file.zip",
        "file.zip",
    ]

    data = None
    for path in candidates:
        if not path:
            continue
        expanded = os.path.expanduser(path)
        if os.path.exists(expanded):
            with open(expanded, "rb") as f:
                data = f.read()
            break

    if data is None:
        print(
            "Could not find payload file (tried TEST_FILE, PAYLOAD_FILE, file.zip)",
            file=sys.stderr,
        )
        sys.exit(1)

    if not data:
        return [b"EMPTY_FILE"]

    chunks: List[bytes] = []
    for i in range(0, len(data), MSS):
        chunks.append(data[i:i + MSS])
    return chunks


def make_packet(seq_id: int, payload: bytes) -> bytes:
    return int.to_bytes(seq_id, SEQ_ID_SIZE, byteorder="big", signed=True) + payload


def parse_ack(packet: bytes) -> Tuple[int, str]:
    seq = int.from_bytes(packet[:SEQ_ID_SIZE], byteorder="big", signed=True)
    msg = packet[SEQ_ID_SIZE:].decode(errors="ignore")
    return seq, msg

# compute throughput, average delay, average jitter, and final metric.
def compute_metrics(total_bytes: int, duration: float, delays: List[float]) -> Tuple[float, float, float, float]:
    if duration <= 0:
        duration = 1e-6

    throughput = float(total_bytes) / duration

    if delays:
        avg_delay = sum(delays) / len(delays)
        if len(delays) > 1:
            diffs = [abs(delays[i] - delays[i - 1]) for i in range(1, len(delays))]
            avg_jitter = sum(diffs) / len(diffs)
        else:
            avg_jitter = 0.0
    else:
        avg_delay = 0.0
        avg_jitter = 0.0

    eps = 1e-6
    metric = (throughput / 2000.0) + 15.0 / (avg_jitter + eps) + 35.0 / (avg_delay + eps)

    return throughput, avg_delay, avg_jitter, metric


def print_metrics(total_bytes: int, duration: float, delays: List[float]) -> None:
    throughput, avg_delay, avg_jitter, metric = compute_metrics(total_bytes, duration, delays)

    print("\nTahoe transfer complete.")
    print(f"duration={duration:.6f}s throughput={throughput:.2f} bytes/sec")
    print(f"avg_delay={avg_delay:.6f}s avg_jitter={avg_jitter:.6f}s metric={metric:.6f}")
    print(f"{throughput:.7f},{avg_delay:.7f},{avg_jitter:.7f},{metric:.7f}")



def main() -> None:
    # load file in MSS sized chunks
    data_chunks = load_payload_chunks()
    transfers: List[Tuple[int, bytes]] = []

    seq = 0
    for chunk in data_chunks:
        transfers.append((seq, chunk))
        seq += len(chunk)

    # EOF marker
    transfers.append((seq, b""))
    total_bytes = sum(len(chunk) for chunk in data_chunks)

    print(f"Connecting to receiver at {HOST}:{PORT}")
    print(
        f"Tahoe sender will send {total_bytes} bytes across "
        f"{len(data_chunks)} data packets (+EOF)."
    )

    cwnd = 1.0
    ssthresh = 64.0
    dup_ack_count = 0
    last_ack_id = -1

    send_times = {}
    delays: List[float] = []

    start = time.time()

    num_packets = len(transfers)
    base_idx = 0
    next_idx = 0

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(ACK_TIMEOUT)
        addr = (HOST, PORT)

        while base_idx < num_packets:
            # send up to cwnd packets
            window_limit = base_idx + max(int(cwnd), 1)
            while next_idx < num_packets and next_idx < window_limit:
                seq_id, payload = transfers[next_idx]
                pkt = make_packet(seq_id, payload)
                sock.sendto(pkt, addr)
                if len(payload) > 0 and seq_id not in send_times:
                    send_times[seq_id] = time.time()
                next_idx += 1

            try:
                ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
            except socket.timeout:
                #  retransmit
                ssthresh = max(cwnd / 2.0, 1.0)
                cwnd = 1.0
                dup_ack_count = 0

                if base_idx < num_packets:
                    seq_rtx, payload_rtx = transfers[base_idx]
                    pkt_rtx = make_packet(seq_rtx, payload_rtx)
                    sock.sendto(pkt_rtx, addr)
                continue

            ack_id, msg = parse_ack(ack_pkt)

            if msg.startswith("fin"):
                fin_ack = make_packet(ack_id, b"FIN/ACK")
                sock.sendto(fin_ack, addr)
                duration = max(time.time() - start, 1e-6)
                print_metrics(total_bytes, duration, delays)
                return

            if msg.startswith("ack"):
                if ack_id > last_ack_id:
                    dup_ack_count = 0

                    # slide window and record delays
                    while base_idx < num_packets:
                        seq_b, payload_b = transfers[base_idx]
                        if len(payload_b) > 0 and ack_id >= seq_b + len(payload_b):
                            if seq_b in send_times:
                                delay = time.time() - send_times[seq_b]
                                delays.append(delay)
                            base_idx += 1
                        elif len(payload_b) == 0 and ack_id >= seq_b:
                            base_idx += 1
                        else:
                            break

                    if cwnd < ssthresh:
                        cwnd += 1.0
                    else:
                        cwnd += 1.0 / max(cwnd, 1.0)

                    last_ack_id = ack_id

                elif ack_id == last_ack_id:
                    dup_ack_count += 1
                    if dup_ack_count == 3:
                        # Tahoe fast retransmit
                        ssthresh = max(cwnd / 2.0, 1.0)
                        cwnd = 1.0
                        dup_ack_count = 0
                        if base_idx < num_packets:
                            seq_rtx, payload_rtx = transfers[base_idx]
                            pkt_rtx = make_packet(seq_rtx, payload_rtx)
                            sock.sendto(pkt_rtx, addr)
                        continue

        # wait for final FIN if needed
        while True:
            ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
            ack_id, msg = parse_ack(ack_pkt)
            if msg.startswith("fin"):
                fin_ack = make_packet(ack_id, b"FIN/ACK")
                sock.sendto(fin_ack, addr)
                duration = max(time.time() - start, 1e-6)
                print_metrics(total_bytes, duration, delays)
                return



if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Tahoe sender hit an error: {exc}", file=sys.stderr)
        sys.exit(1)
