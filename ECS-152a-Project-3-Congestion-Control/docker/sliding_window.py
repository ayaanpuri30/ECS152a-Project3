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
MAX_TIMEOUTS = 50

HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("RECEIVER_PORT", "5001"))


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


def send_packet(sock, addr, packet):
    now = time.time()
    if packet["first_send_time"] is None:
        packet["first_send_time"] = now
    packet["last_send_time"] = now

    pkt = make_packet(packet["seq_id"], packet["payload"])
    # print(f"Sending seq={packet['seq_id']}, bytes={len(packet['payload'])}")
    sock.sendto(pkt, addr)


def handle_timeout(sock, addr, packets, base: int, RTO: float) -> None:
    now = time.time()
    if base >= len(packets):
        return

    oldest = packets[base]
    if oldest["acked"] or oldest["last_send_time"] is None:
        return

    if now - oldest["last_send_time"] >= RTO:
        # only retransmit the oldest unacked packet
        send_packet(sock, addr, oldest)


def handle_ack(ack_pkt: bytes, packets: list[dict]) -> bool:
    ack_id, msg = parse_ack(ack_pkt)

    if msg.startswith("fin"):
        return True

    if not msg.startswith("ack"):
        return False

    # Mark all packets with end_seq <= ack_id as acknowledged
    for p in packets:
        if p["acked"]:
            continue
        end_seq = p["seq_id"] + len(p["payload"])
        if end_seq <= ack_id:
            p["acked"] = True
            if len(p["payload"]) > 0 and p["first_send_time"] is not None:
                record_packet(p["first_send_time"], time.time(), len(p["payload"]))

    return False


def find_new_base(packets: list[dict], base: int) -> int:
    while base < len(packets) and packets[base]["acked"]:
        base += 1
    return base


def main() -> None:
    global delays, total_payload_bytes
    delays = []
    total_payload_bytes = 0
    chunks = load_payload_chunks()

    # chunks = chunks[:500]

    if not chunks:
        print("No payload file found", file=sys.stderr)
        sys.exit(1)

    packets: list[dict] = []
    seq = 0
    for chunk in chunks:
        packets.append(
            {
                "seq_id": seq,
                "payload": chunk,
                "first_send_time": None,
                "last_send_time": None,
                "acked": False,
            }
        )
        seq += len(chunk)

    packets.append(
        {
            "seq_id": seq,
            "payload": b"",
            "first_send_time": None,
            "last_send_time": None,
            "acked": False,
        }
    )

    total_bytes = sum(len(chunk) for chunk in chunks)

    print(f"Connecting to receiver at {HOST}:{PORT}")
    print(
        f"Demo transfer will send {total_bytes} bytes across {len(chunks)} packets (+EOF)."
    )

    start = time.time()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(ACK_TIMEOUT)
        addr = (HOST, PORT)

        WINDOW_SIZE = 100
        RTO = 1.0

        base = 0
        next_to_send = 0
        num_packets = len(packets)
        timeout_count = 0

        while base < num_packets:
            while next_to_send < num_packets and next_to_send < base + WINDOW_SIZE:
                p = packets[next_to_send]
                send_packet(sock, addr, p)
                next_to_send += 1

            try:
                ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
                timeout_count = 0
                fin_seen = handle_ack(ack_pkt, packets)
                base = find_new_base(packets, base)
                if fin_seen:
                    ack_id, _ = parse_ack(ack_pkt)
                    fin_ack = make_packet(ack_id, b"FIN/ACK")
                    sock.sendto(fin_ack, addr)
                    duration = max(time.time() - start, 1e-6)

                    throughput, avg_delay, avg_jitter = compute_stats(duration)
                    print_metrics(duration, throughput, avg_delay, avg_jitter)
                    return
            except socket.timeout:
                timeout_count += 1
                handle_timeout(sock, addr, packets, base, RTO)
                if timeout_count > MAX_TIMEOUTS:
                    print("Too many timeouts, aborting this run", file=sys.stderr)
                    break

        while True:
            try:
                ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
            except socket.timeout:
                break

            ack_id, msg = parse_ack(ack_pkt)
            if msg.startswith("fin"):
                fin_ack = make_packet(ack_id, b"FIN/ACK")
                sock.sendto(fin_ack, addr)
                break

    duration = max(time.time() - start, 1e-6)
    throughput, avg_delay, avg_jitter = compute_stats(duration)
    print_metrics(duration, throughput, avg_delay, avg_jitter)


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
