#!/bin/bash
set -e

# Make sure the training profile is executable
chmod +x training_profile.sh

# Start the network training profile in the background
./training_profile.sh &

echo "Training profile started in background."
echo "Container will stay alive; receivers will be started via docker exec."

# Keep the container running indefinitely
# (so test_sender.sh and test_fairness.sh can start/stop receiver.py as needed)
tail -f /dev/null