FROM python:3.11-slim

# Prevents Python from writing pyc files and enables unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=America/Los_Angeles

# Install system dependencies and clean up in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    iproute2 \
    bc \
    procps \
    tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir tqdm

# Set working directory
WORKDIR /app

# Copy required files
COPY training_profile.sh docker-script.sh receiver.py ./
RUN chmod +x training_profile.sh docker-script.sh

# Start receiver with network simulation
CMD ["./docker-script.sh"]