# Troubleshooting Guide

Common issues and their solutions for the ECS 152A Congestion Control assignment.

## Docker Issues

### Docker daemon not running

**Symptoms:**

```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock
```

**Solutions:**

**macOS:**

- If using Docker Desktop: Start Docker Desktop from Applications
- If using Colima: Run `colima start`

**Linux:**

```bash
sudo systemctl start docker
sudo systemctl enable docker  # Start on boot
```

**Windows:**

- Start Docker Desktop from the Start menu
- Ensure WSL 2 is installed and enabled

### Permission denied errors (Linux)

**Symptoms:**

```
Got permission denied while trying to connect to the Docker daemon socket
```

**Solution:**

```bash
# Add your user to the docker group
sudo usermod -aG docker $USER

# Apply the new group membership
newgrp docker

# Log out and log back in for permanent effect
```

### Container not found

**Symptoms:**

```
Error: No such container: ecs152a-simulator
```

**Solution:** build and relaunch the detached simulator container:

```bash
cd docker
./start-simulator.sh      # macOS/Linux
start_sim.bat             # Windows
```

### Container exists but won't start

**Solution:**

```bash
# Remove the old container and start fresh
docker rm -f ecs152a-simulator
./start-simulator.sh
```

Remember: `./start-simulator.sh` runs the training profile in the background and keeps the container alive. When you are done testing, stop it explicitly with `docker stop ecs152a-simulator`.

## Network/Connection Issues

### Port 5001 already in use

**Symptoms:**

```
Bind for 0.0.0.0:5001 failed: port is already allocated
```

**Find what's using the port:**

**macOS/Linux:**

```bash
lsof -i :5001
# or
sudo netstat -tulpn | grep 5001
```

**Windows:**

```bash
netstat -ano | findstr :5001
```

**Solutions:**

1. Kill the process using the port
2. Or stop any other simulators running: `docker stop ecs152a-simulator`

### Receiver not responding

**Symptoms:**

- Sender times out waiting for ACKs
- No response from receiver

**Diagnostic steps:**

```bash
# Check if container is running
docker ps

# Check receiver logs
docker logs ecs152a-simulator

# Restart the simulator
docker restart ecs152a-simulator
```

**Common causes:**

- Receiver process from a prior run is still draining; re-run `./test_sender.sh ...` to spawn a new one or call `docker exec ecs152a-simulator pkill -f receiver.py`.
- Firewall blocking localhost connections
- Sending to wrong port (default 5001; fairness harness also uses 5002)

### Connection refused

**Symptoms:**

```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Solutions:**

1. Ensure receiver is running: `docker ps`
2. Check you're connecting to correct address: `localhost:5001`
3. Restart the simulator: `./start-simulator.sh`

## Testing Issues

### test_sender.sh: command not found (macOS/Linux)

**Solution:**

```bash
# Make script executable
chmod +x test_sender.sh

# Run with ./
./test_sender.sh my_sender.py
```

### Path issues on Windows

**Symptom:**

```
file.zip not found
```

**Solution:**

- Always run the scripts from the `docker` directory so relative payload paths resolve correctly.
- Remember each test script accepts an optional payload argument:

  ```batch
  cd docker
  test_sender.bat my_sender.py my_payload.zip
  ```

### "Docker not in PATH" error

**Solution:**

```bash
# Check if Docker is installed
which docker

# If installed but not in PATH, add to PATH
export PATH="/usr/local/bin:$PATH"  # macOS/Linux

# Or restart terminal after Docker installation
```

### Payload file not found

- Ensure the payload exists either in the repo root, inside `docker/`, or inside `docker/hdd/`.
- Pass the filename explicitly if you store multiple datasets:
  ```bash
  ./test_sender.sh my_sender.py datasets/file2.zip
  ./test_fairness.sh tahoe.py reno.py datasets/file2.zip
  ```
- The scripts copy the resolved file into `/hdd` as-is, so keep extensions consistent with what your receiver expects.

## Debugging Strategies

### Add verbose logging

```python
DEBUG = True

if DEBUG:
    print(f"Sent packet {seq_id}, window [{base}:{base+cwnd}]")
    print(f"Received ACK {ack_seq_id}")
    print(f"cwnd={cwnd:.2f}, ssthresh={ssthresh}, dup_acks={duplicate_acks}")
```

### Test with smaller file first

Replace file.zip with a smaller test file to iterate faster:

```bash
# Create small test file
head -c 10000 file.zip > test_small.mp3
docker cp test_small.mp3 ecs152a-simulator:/hdd/file.zip
```

### Check receiver logs

```bash
docker logs ecs152a-simulator
```

Shows:

- Which packets were received
- Current network phase
- Any errors from receiver

### Manual testing

Instead of using test script:

```bash
# Start simulator
./start-simulator.sh

# In another terminal, copy and run manually
docker cp my_sender.py ecs152a-simulator:/app/
docker exec ecs152a-simulator python3 /app/my_sender.py
```

### Verify packet format

```python
# Print packet contents
packet = create_packet(seq_id, message)
print(f"Packet: seq_id={seq_id}, size={len(packet)}")
print(f"  Seq bytes: {packet[:4].hex()}")
print(f"  Payload size: {len(packet[4:])}")
```

## Still Having Issues?

1. **Post on discussion board** - Include error messages and what you've tried
2. **Office hours** - Bring specific questions and code snippets

## Quick Reference Commands

```bash
# Start simulator
./start-simulator.sh

# Test your sender
./test_sender.sh my_sender.py [payload.zip]

# Compare two senders
./test_fairness.sh tahoe.py reno.py [payload.zip]

# Check Docker status
docker ps

# View receiver logs
docker logs ecs152a-simulator

# Restart receivers only (inside container)
docker exec ecs152a-simulator pkill -f receiver.py

# Stop simulator (also stops training profile)
docker stop ecs152a-simulator

# Remove container completely
docker rm -f ecs152a-simulator

# Copy file from container
docker cp ecs152a-simulator:/hdd/file2.zip ./received.mp3

# Run commands inside container
docker exec ecs152a-simulator ls -la /hdd/
```
