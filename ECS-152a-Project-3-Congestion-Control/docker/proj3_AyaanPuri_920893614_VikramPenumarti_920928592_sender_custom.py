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
MAX_CONSEC_TIMEOUTS = 50

HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
PORT = int(os.environ.get("RECEIVER_PORT", "5001"))


# load file and split into MSS-sized chunks
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
        chunks.append(data[i : i + MSS])
    return chunks


def make_packet(seq_id: int, payload: bytes) -> bytes:
    return int.to_bytes(seq_id, SEQ_ID_SIZE, byteorder="big", signed=True) + payload


def parse_ack(packet: bytes) -> Tuple[int, str]:
    seq = int.from_bytes(packet[:SEQ_ID_SIZE], byteorder="big", signed=True)
    msg = packet[SEQ_ID_SIZE:].decode(errors="ignore")
    return seq, msg


# compute throughput, average delay, average jitter, and final metric
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
    throughput, avg_delay, avg_jitter, metric = compute_metrics(
        total_bytes, duration, delays
    )

    print("\nCustom protocol transfer complete.")
    print(f"duration={duration:.6f}s throughput={throughput:.2f} bytes/sec")
    print(f"avg_delay={avg_delay:.6f}s avg_jitter={avg_jitter:.6f}s metric={metric:.6f}")
    print(f"{throughput:.7f},{avg_delay:.7f},{avg_jitter:.7f},{metric:.7f}")


def main() -> None:
    # load entire file as MSS-sized chunks
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
        f"Custom sender will send {total_bytes} bytes across "
        f"{len(data_chunks)} data packets (+EOF)."
    )

    cwnd = 4.0 # more aggressive start than Tahoe/Reno
    ssthresh = 64.0
    max_cwnd = 128.0
    dup_ack_count = 0
    last_ack_id = -1

    base_id = 0
    next_id = 0

    send_times = {}
    delays: List[float] = []

    start = time.time()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(ACK_TIMEOUT)
        addr = (HOST, PORT)

        consec_timeouts = 0

        while base_id < len(transfers):
            # send packets within current window
            while next_id < len(transfers) and (next_id - base_id) < int(cwnd):
                seq_id, payload = transfers[next_id]
                pkt = make_packet(seq_id, payload)
                sock.sendto(pkt, addr)

                if len(payload) > 0 and seq_id not in send_times:
                    send_times[seq_id] = time.time()

                next_id += 1

            # wait for ACK or timeout
            try:
                ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
            except socket.timeout:
                consec_timeouts += 1

                # gentle backoff
                ssthresh = max(cwnd / 2.0, 4.0)
                cwnd = max(ssthresh, 4.0)
                dup_ack_count = 0

                if consec_timeouts > MAX_CONSEC_TIMEOUTS:
                    raise RuntimeError(
                        "Receiver did not respond (max consecutive timeouts exceeded)"
                    )

                # retransmit oldest unacked packet
                if base_id < len(transfers):
                    seq_id, payload = transfers[base_id]
                    pkt = make_packet(seq_id, payload)
                    sock.sendto(pkt, addr)

                continue

            consec_timeouts = 0

            ack_id, msg = parse_ack(ack_pkt)
            msg_stripped = msg.strip()

            # receiver done
            if msg_stripped.startswith("fin"):
                fin_ack = make_packet(ack_id, b"FIN/ACK")
                sock.sendto(fin_ack, addr)
                duration = max(time.time() - start, 1e-6)
                print_metrics(total_bytes, duration, delays)
                return

            if not msg_stripped.startswith("ack"):
                continue

            # new ACK vs duplicate
            if ack_id > last_ack_id:
                dup_ack_count = 0

                # log delays for any newly acked packets
                now = time.time()
                while base_id < len(transfers):
                    seq_b, payload_b = transfers[base_id]
                    end_b = seq_b + len(payload_b)
                    if end_b <= ack_id:
                        if len(payload_b) > 0 and seq_b in send_times:
                            delay = now - send_times[seq_b]
                            delays.append(delay)
                            del send_times[seq_b]
                        base_id += 1
                    else:
                        break

                # window growth
                if cwnd < ssthresh:
                    # slow start
                    cwnd += 1.0
                else:
                    cwnd += 1.5 / cwnd

                if cwnd > max_cwnd:
                    cwnd = max_cwnd

                last_ack_id = ack_id

            elif ack_id == last_ack_id: # duplicate ACK
                dup_ack_count += 1

                if dup_ack_count == 3:
                    # fast retransmit + mild backoff
                    ssthresh = max(cwnd / 2.0, 4.0)
                    cwnd = max(ssthresh, 4.0)
                    dup_ack_count = 0

                    if base_id < len(transfers):
                        seq_id, payload = transfers[base_id]
                        pkt = make_packet(seq_id, payload)
                        sock.sendto(pkt, addr)


        # final FIN
        fin_timeouts = 0
        while True:
            try:
                ack_pkt, _ = sock.recvfrom(PACKET_SIZE)
            except socket.timeout:
                fin_timeouts += 1
                
                if fin_timeouts > 5:
                    duration = max(time.time() - start, 1e-6)
                    print_metrics(total_bytes, duration, delays)
                    return
                continue

            ack_id, msg = parse_ack(ack_pkt)
            if msg.strip().startswith("fin"):
                fin_ack = make_packet(ack_id, b"FIN/ACK")
                sock.sendto(fin_ack, addr)
                duration = max(time.time() - start, 1e-6)
                print_metrics(total_bytes, duration, delays)
                return


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Custom sender hit an error: {exc}", file=sys.stderr)
        sys.exit(1)