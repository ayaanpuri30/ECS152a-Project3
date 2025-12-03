# Docker Setup Guide

This guide will help you install Docker, which is required to run the network simulator for this assignment.

## Installation by Operating System

### macOS

**Option 1: Colima (Recommended - Easier and lighter)**

Colima is a lightweight alternative to Docker Desktop that runs Docker containers using Lima.

1. Install Homebrew (if not already installed):
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. Install Colima and Docker:
   ```bash
   brew install colima docker docker-compose
   ```

3. Start Colima:
   ```bash
   colima start
   ```

4. Verify installation:
   ```bash
   docker run hello-world
   ```

**Option 2: Docker Desktop**

1. Download Docker Desktop for Mac from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
2. Open the downloaded `.dmg` file
3. Drag Docker.app to your Applications folder
4. Launch Docker Desktop from Applications
5. Follow the setup wizard to complete installation
6. Verify installation by opening Terminal and running:
   ```bash
   docker run hello-world
   ```

### Linux (Ubuntu/Debian)

1. Update your package index:
   ```bash
   sudo apt-get update
   ```

2. Install required packages:
   ```bash
   sudo apt-get install ca-certificates curl gnupg lsb-release
   ```

3. Add Docker's official GPG key:
   ```bash
   sudo mkdir -p /etc/apt/keyrings
   curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
   ```

4. Set up the repository:
   ```bash
   echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
   ```

5. Install Docker Engine:
   ```bash
   sudo apt-get update
   sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
   ```

6. Verify installation:
   ```bash
   sudo docker run hello-world
   ```

### Windows

1. Download Docker Desktop for Windows from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
2. Run the installer (`Docker Desktop Installer.exe`)
3. Follow the installation wizard and ensure "Use WSL 2 instead of Hyper-V" is selected if you have WSL 2 installed
4. Click "Finish" when the installation completes
5. Launch Docker Desktop from the Start menu
6. Complete the initial setup wizard
7. Verify installation by opening PowerShell or Command Prompt and running:
   ```bash
   docker run hello-world
   ```

## Verification

After installation, verify Docker is working correctly:

```bash
docker --version
docker info
```

You should see version information and system details without errors.

## Start the simulator container once

With Docker running, build and launch the course simulator (it stays up in the background and applies the training profile automatically):

```bash
cd docker
./start-simulator.sh        # macOS/Linux
# or
start_sim.bat               # Windows
```

Once the container is running you can invoke `./test_sender.sh my_sender.py [payload.zip]` (or the `.bat` equivalent) as many times as you like; the scripts copy your latest sender/payload into `/app`/`/hdd` and restart only the in-container receivers between runs.

## Troubleshooting

### Docker daemon not running
- **macOS**: Start Docker Desktop from Applications or run `colima start`
- **Linux**: Run `sudo systemctl start docker`
- **Windows**: Start Docker Desktop from Start menu

### Permission denied (Linux)
If you get permission errors when running Docker commands:
```bash
sudo usermod -aG docker $USER
newgrp docker
```
Then log out and log back in.

### Port conflicts
If port 5001 is already in use:
```bash
# Find what's using the port
lsof -i :5001  # macOS/Linux
netstat -ano | findstr :5001  # Windows

# Kill the process or change the port in the assignment
```

## Additional Resources

For detailed platform-specific instructions and troubleshooting:
- [Linux Installation Guide](https://docs.docker.com/engine/install/ubuntu/)
- [macOS Installation Guide](https://docs.docker.com/desktop/install/mac-install/)
- [Windows Installation Guide](https://docs.docker.com/desktop/install/windows-install/)