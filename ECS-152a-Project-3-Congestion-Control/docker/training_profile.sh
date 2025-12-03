#!/bin/bash

bandwidth=50000   # initial BW (kbit)
delay=50          # initial base delay (ms)
pl=0.1            # initial loss (%)
limit=10000       # queue size in packets

# Clean up qdisc on exit
trap "tc qdisc del dev lo root 2>/dev/null" EXIT

# Root HTB and netem under it
tc qdisc replace dev lo root handle 1: htb default 1
tc class replace dev lo parent 1: classid 1:1 htb rate ${bandwidth}kbit ceil ${bandwidth}kbit
tc qdisc replace dev lo parent 1:1 handle 10: netem \
    delay ${delay}ms \
    loss ${pl}% \
    limit ${limit}

# Random initial phase
phase=$((1 + RANDOM % 5))
counter=0
phase_duration=$((20 + RANDOM % 21))  # 20-40 seconds

echo "Starting network simulation with random initial phase: $phase"

while true; do
    counter=$((counter + 1))

    case $phase in
        1)
            # Moderate bottleneck with big queue - primary queuing phase
            bandwidth=$((1200 + RANDOM % 1300))     # 1.2-2.5 Mbps
            delay=$((60 + RANDOM % 50))             # 60-110 ms base
            pl=$(echo "scale=2; 0.2 + $(($RANDOM % 40)) / 100" | bc)  # 0.2-0.6% loss
            limit=40000  # Large queue for queuing delay
            ;;

        2)
            # Tighter bottleneck - more aggressive queuing
            bandwidth=$((600 + RANDOM % 800))       # 600-1400 kbps
            delay=$((80 + RANDOM % 60))             # 80-140 ms base
            pl=$(echo "scale=2; 0.25 + $(($RANDOM % 45)) / 100" | bc)  # 0.25-0.7% loss
            limit=50000  # Very deep queue
            ;;

        3)
            # Light congestion - less queuing
            bandwidth=$((2500 + RANDOM % 2000))     # 2.5-4.5 Mbps
            delay=$((40 + RANDOM % 40))             # 40-80 ms base
            pl=$(echo "scale=2; 0.3 + $(($RANDOM % 80)) / 100" | bc)  # 0.3-1.1% loss
            limit=25000
            ;;

        4)
            # Moderate capacity
            bandwidth=$((3500 + RANDOM % 2500))     # 3.5-6 Mbps
            delay=$((30 + RANDOM % 40))             # 30-70 ms
            pl=$(echo "scale=2; 0.25 + $(($RANDOM % 60)) / 100" | bc)  # 0.25-0.85% loss
            limit=20000
            ;;

        5)
            # Sudden tight squeeze - instant queue buildup
            bandwidth=$((800 + RANDOM % 600))       # 800-1400 kbps
            delay=$((70 + RANDOM % 50))             # 70-120 ms base
            pl=$(echo "scale=2; 0.2 + $(($RANDOM % 50)) / 100" | bc)  # 0.2-0.7% loss
            limit=45000
            ;;
    esac

    # Apply shaping
    tc class change dev lo parent 1: classid 1:1 htb \
        rate ${bandwidth}kbit ceil ${bandwidth}kbit

    tc qdisc change dev lo parent 1:1 handle 10: netem \
        delay ${delay}ms \
        loss ${pl}% \
        limit ${limit}

    echo "Phase: $phase, Time: $counter/$phase_duration, BW: ${bandwidth}kbit, Delay: ${delay}ms, Loss: ${pl}%, Limit: ${limit}"

    if [ $counter -ge $phase_duration ]; then
        next_phase=$phase
        while [ $next_phase -eq $phase ]; do
            next_phase=$((1 + RANDOM % 5))
        done

        echo "=== Transitioning from Phase $phase to Phase $next_phase ==="
        phase=$next_phase
        counter=0
        phase_duration=$((20 + RANDOM % 21))
    fi

    sleep 1
done