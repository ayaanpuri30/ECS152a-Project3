@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%"

set "CONTAINER_NAME=ecs152a-simulator"
set "IMAGE_NAME=ecs152a/simulator"

echo [INFO] Stopping any existing simulator container...
docker rm -f %CONTAINER_NAME% >nul 2>&1

echo [INFO] Building simulator image (%IMAGE_NAME%)...
docker build -t %IMAGE_NAME% .

echo [INFO] Starting simulator container in the background...
for /f "tokens=*" %%I in ('
    docker run -d ^
        --name %CONTAINER_NAME% ^
        --cap-add=NET_ADMIN ^
        --rm ^
        -p 5001:5001/udp ^
        -v "%SCRIPT_DIR%hdd:/hdd" ^
        %IMAGE_NAME%
') do set "CONTAINER_ID=%%I"

echo [SUCCESS] Simulator container is running (ID: %CONTAINER_ID%)
echo           Training profile is applied automatically inside the container.
echo           Use "docker logs -f %CONTAINER_NAME%" to follow output.
echo           Run "test_sender.bat" or analogous scripts from another shell.

popd
endlocal
