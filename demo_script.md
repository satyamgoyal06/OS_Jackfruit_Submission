# OS Jackfruit - Live Demonstration Script

Use this script during your practical demonstration to run through all the features of the OS Jackfruit container runtime step-by-step. 

Run all of these commands from the `~/boilerplate` directory inside your Ubuntu VM.

## 1. Setup and Build
```bash
cd boilerplate
make clean
make

# Load the kernel monitor
sudo insmod monitor.ko
```

## 2. Prepare Container Filesystems
```bash
# Extract the Alpine base rootfs (ensure you have the aarch64 version for your VM)
mkdir -p rootfs-base
wget -qO- https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/aarch64/alpine-minirootfs-3.20.3-aarch64.tar.gz | tar -xz -C rootfs-base

# Make isolated copies for our various tests
cp -a rootfs-base rootfs-alpha
cp -a rootfs-base rootfs-beta
cp -a rootfs-base rootfs-memsoft
cp -a rootfs-base rootfs-memhard
cp -a rootfs-base rootfs-cpuA
cp -a rootfs-base rootfs-cpuB

# Copy our custom workloads directly into the rootfs copies
cp memory_hog rootfs-memsoft/
cp memory_hog rootfs-memhard/
cp cpu_hog rootfs-cpuA/
cp cpu_hog rootfs-cpuB/
```

## 3. Run Supervisor Daemon
```bash
# Start the supervisor daemon in the background to manage our containers
sudo nohup ./engine supervisor ./rootfs-base > supervisor.log 2>&1 &
```

## 4. Multi-Container Execution & Metadata
```bash
# Start two basic containers running a long sleep command
sudo ./engine start alpha ./rootfs-alpha "sleep 200"
sudo ./engine start beta ./rootfs-beta "sleep 200"

# Demonstrate that the supervisor is tracking them concurrently
sudo ./engine ps
```

## 5. Memory Soft-Limit Verification
```bash
# Clear the kernel ring buffer for a clean reading
sudo dmesg -c > /dev/null

# Run a container that allocates 45MB after booting (total ~95MB runtime footprint)
# Passing the 80MB soft limit, but well under the 150MB hard limit.
# Press 'Ctrl+C' to return to terminal after you let it run for ~2 seconds.
sudo ./engine run mem-soft ./rootfs-memsoft "/memory_hog 45 10000" --soft-mib 80 --hard-mib 150

# Inspect dmesg to demonstrate the soft-limit module warning!
sudo dmesg | tail -n 5
```

## 6. Memory Hard-Limit Verification
```bash
sudo dmesg -c > /dev/null

# Run a container that wildly allocates 60MB every loop, breaching 100MB instantly
sudo ./engine run mem-hard ./rootfs-memhard "/memory_hog 60 10" --soft-mib 80 --hard-mib 100

# The command will immediately exit automatically as it gets SIGKILL.
# Inspect dmesg to demonstrate the hard-limit kernel enforcement!
sudo dmesg | tail -n 5

# Demonstrate the `ps` command acknowledging it was explicitly 'killed' via exit_code 137
sudo ./engine ps
```

## 7. Scheduling Fair-Share CPU Demonstration
```bash
# Spawn two infinite-loop CPU hogs. Give 'cpuB' a massively penalized Priority (nice=19)
sudo ./engine start cpuA ./rootfs-cpuA "/cpu_hog"
sudo ./engine start cpuB ./rootfs-cpuB "/cpu_hog" --nice 19

# Both programs increment an accumulator in a while-loop for exactly 10 seconds.
# Wait exactly 11 seconds to guarantee they have finished.
sleep 11

# Compare the final outputs! cpuA should have advanced its math significantly more than cpuB!
echo "--- CPU A (Normal Priority) Output ---"
cat logs/cpuA.log | tail -n 3
echo "--- CPU B (Low Priority) Output ---"
cat logs/cpuB.log | tail -n 3
```

## 8. Clean Teardown
```bash
# Gracefully stop the background processes
sudo ./engine stop alpha
sudo ./engine stop beta

# Stop the supervisor daemon itself
sudo killall engine

# Verify there are absolutely no zombie engines running
ps aux | grep engine

# Remove the memory monitor
sudo rmmod monitor

# Final cleanup
make clean
```
