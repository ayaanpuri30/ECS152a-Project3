import os
import socket
import sys
import time

PACKET_SIZE = 1024
SEQ_ID_SIZE = 4
MESSAGE_SIZE = PACKET_SIZE - SEQ_ID_SIZE

TIMEOUT = 5
FIN_ACK_DELAY = 0.5


def create_acknowledgement(seq_id, message: str) -> bytes:
    return (
        int.to_bytes(seq_id, SEQ_ID_SIZE, signed=True, byteorder="big")
        + message.encode()
    )


def resolve_payload_path() -> tuple[str, str]:
    payload = (
        os.environ.get("TEST_FILE") or os.environ.get("PAYLOAD_FILE") or "/hdd/file.zip"
    )
    output = (
        os.environ.get("RECEIVER_OUTPUT_FILE")
        or os.environ.get("OUTPUT_FILE")
        or "/hdd/file2.zip"
    )
    return payload, output


def main():
    receiver_port = int(os.environ.get("RECEIVER_PORT", "5001"))
    payload_file, output_file = resolve_payload_path()

    os.makedirs(os.path.dirname(output_file) or "/hdd", exist_ok=True)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
        udp_socket.bind(("0.0.0.0", receiver_port))
        udp_socket.settimeout(TIMEOUT)

        timeouts = 0
        max_consecutive_timeouts = 3
        packets_received = 0
        duplicate_packets = 0
        last_activity = time.time()
        expected_seq_id = 0
        received_data: dict[int, bytes] = {}

        print(f"Receiver running on port {receiver_port}")
        print(f"Expecting payload: {payload_file} -> writing to {output_file}")
        print("Waiting for data...")

        while True:
            try:
                packet, client = udp_socket.recvfrom(PACKET_SIZE)
                timeouts = 0
                last_activity = time.time()
                packets_received += 1

                seq_id_bytes, message = packet[:SEQ_ID_SIZE], packet[SEQ_ID_SIZE:]

                if message == b"FIN/ACK":
                    print(f"\nReceived FIN/ACK from sender at {client}")
                    print(f"Total packets received: {packets_received}")
                    print(f"Duplicate packets: {duplicate_packets}")
                    print(f"Unique sequences: {len(received_data)}")
                    break

                seq_id = int.from_bytes(seq_id_bytes, signed=True, byteorder="big")

                if seq_id in received_data:
                    duplicate_packets += 1

                received_data[seq_id] = message

                while expected_seq_id in received_data:
                    if len(received_data[expected_seq_id]) == 0:
                        break
                    expected_seq_id += len(received_data[expected_seq_id])

                if packets_received % 100 == 0:
                    progress = (
                        (
                            expected_seq_id
                            / (expected_seq_id + len(received_data) * MESSAGE_SIZE)
                        )
                        * 100
                        if received_data
                        else 0
                    )
                    print(
                        f"Received {packets_received} packets, Expected seq: {expected_seq_id}, Duplicates: {duplicate_packets}"
                    )

                ack_id = expected_seq_id
                acknowledgement = create_acknowledgement(ack_id, "ack")
                udp_socket.sendto(acknowledgement, client)

                if (
                    expected_seq_id in received_data
                    and len(received_data[expected_seq_id]) == 0
                ):
                    print(f"\n✓ Transfer complete! Expected seq: {expected_seq_id}")
                    print(f"Total packets received: {packets_received}")
                    print(f"Duplicate packets: {duplicate_packets}")
                    print(f"Unique sequences: {len(received_data)}")

                    ack = create_acknowledgement(ack_id, "ack")
                    fin = create_acknowledgement(ack_id + 3, "fin")
                    udp_socket.sendto(ack, client)
                    udp_socket.sendto(fin, client)
                    time.sleep(FIN_ACK_DELAY)

            except socket.timeout:
                timeouts += 1
                print(
                    f"Timeout {timeouts}/{max_consecutive_timeouts} - No packets received for 10s"
                )

                if timeouts >= max_consecutive_timeouts:
                    print(
                        f"\n⚠ No packets for {max_consecutive_timeouts * 10}s, assuming transfer failed or completed"
                    )
                    print(f"Total packets received: {packets_received}")
                    print(f"Expected sequence ID: {expected_seq_id}")
                    print(f"Sequences stored: {len(received_data)}")

                    if (
                        expected_seq_id in received_data
                        and len(received_data[expected_seq_id]) == 0
                    ):
                        print("✓ Transfer appears complete (have end marker)")
                    else:
                        print("✗ Transfer incomplete (missing end marker or data)")
                    break

            except KeyboardInterrupt:
                print("\n\nReceiver interrupted by user")
                print(f"Received {packets_received} packets before interruption")
                break

            except Exception as e:
                print(f"Error receiving packet: {e}")
                continue

    print(f"\nWriting received data to {output_file}...")
    try:
        with open(output_file, "wb") as f:
            bytes_written = 0
            for sid in sorted(received_data.keys()):
                f.write(received_data[sid])
                bytes_written += len(received_data[sid])

        print(f"✓ Wrote {bytes_written:,} bytes to {output_file}")

        try:
            if os.path.exists(payload_file):
                original_size = os.path.getsize(payload_file)
                received_size = os.path.getsize(output_file)

                if original_size == received_size:
                    print(f"✓ File size matches original: {original_size:,} bytes")
                    with open(payload_file, "rb") as f1, open(output_file, "rb") as f2:
                        if f1.read() == f2.read():
                            print("✓ File content matches original perfectly!")
                        else:
                            print("✗ File size matches but content differs")
                else:
                    print(
                        f"✗ File size mismatch: original={original_size:,}, received={received_size:,}"
                    )
        except Exception as e:
            print(f"Could not verify file: {e}")

    except Exception as e:
        print(f"✗ Error writing file: {e}")
        sys.exit(1)

    print("\nReceiver exited successfully")


if __name__ == "__main__":
    main()
