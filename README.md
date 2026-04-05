# OS Jackfruit - Lightweight Linux Container Runtime

## 1. Team Information
- **Satyam Goyal**, SRN: PES1UG24CS424
- **Saatvik Gupta**, SRN: PES1UG24CS395

## 2. Build, Load, and Run Instructions

### Prerequisites
The environment must be an **Ubuntu 22.04 or 24.04 VM** with **Secure Boot OFF** (to allow unauthorized kernel modules). WSL will not work due to restricted namespaces and kernel architectures.

Install dependencies:
```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

### Build & Load
```bash
# Build the project
cd boilerplate
make

# Load the kernel monitor module
sudo insmod monitor.ko

# Verify the device is present
ls -l /dev/container_monitor
```

### Run Instructions
```bash
# Prepare rootfs for containers (do this once per new container)
mkdir -p ../rootfs-base
tar -xzf <alpine-minirootfs.tar.gz> -C ../rootfs-base  # Ensure base is extracted
cp -a ../rootfs-base ./rootfs-alpha
cp -a ../rootfs-base ./rootfs-beta

# Start the supervisor daemon in the background
sudo ./engine supervisor ./rootfs-base &

# Launch two containers
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta ./rootfs-beta /bin/sh --soft-mib 64 --hard-mib 96

# View container metadata (ID, PID, states, memory limits)
sudo ./engine ps

# Inspect logs for container alpha
sudo ./engine logs alpha

# Copy memory hog script to beta and run it
cp memory_hog ./rootfs-beta/
sudo ./engine run beta ./rootfs-beta ./memory_hog 100 1000 --hard-mib 20

# Stop any running containers
sudo ./engine stop alpha
sudo ./engine stop beta

# Unload the module and cleanup
sudo killall engine
sudo rmmod monitor
make clean
```

## 3. Demo with Screenshots

1. **Multi-container supervision**: Supervisor running multiple containers concurrently.  
   ```console
   satyam@ubuntu:~/boilerplate$ sudo ./engine supervisor ./rootfs-base &
   [1] 7401
   satyam@ubuntu:~/boilerplate$ sudo ./engine start alpha ./rootfs-alpha "sleep 100"
   Container alpha started with PID 7525. Log: /home/satyam/boilerplate/logs/alpha.log
   satyam@ubuntu:~/boilerplate$ sudo ./engine start beta ./rootfs-beta "sleep 100"
   Container beta started with PID 7530. Log: /home/satyam/boilerplate/logs/beta.log
   ```

2. **Metadata tracking**: `ps` output showing tracked container metadata.  
   ```console
   satyam@ubuntu:~/boilerplate$ sudo ./engine ps
   ID      PID     STATE   SOFT_MIB        HARD_MIB        EXIT_CODE       SIGNAL  LOG
   beta    7530    running 40      64      0       0       /home/satyam/boilerplate/logs/beta.log
   alpha   7525    running 40      64      0       0       /home/satyam/boilerplate/logs/alpha.log
   ```

3. **Bounded-buffer logging**: Log file contents captured through the logging pipeline.  
   ```console
   satyam@ubuntu:~/boilerplate$ sudo ./engine logs loggerok
   stdout-line
   stderr-line
   io_pulse wrote iteration=1
   io_pulse wrote iteration=2
   io_pulse wrote iteration=3
   ```

4. **CLI and IPC**: CLI command issued and supervisor responding.  
   ```console
   satyam@ubuntu:~/boilerplate$ sudo ./engine start gamma ./rootfs-alpha /bin/sh
   Container gamma started with PID 5196. Log: /home/satyam/boilerplate/logs/gamma.log
   ```

5. **Soft-limit warning**: `dmesg` output showing soft-limit warning event.  
   ```console
   satyam@ubuntu:~/boilerplate$ sudo ./engine run mem-soft ./rootfs-memsoft "/memory_hog 45 10000" --soft-mib 80 --hard-mib 150 >/dev/null
   satyam@ubuntu:~/boilerplate$ sudo dmesg | tail -n 2
   [ 2515.548093] [container_monitor] Unregister request container=beta pid=7530
   [ 2516.148033] [container_monitor] SOFT LIMIT container=mem-soft pid=8004 rss=94765056 limit=83886080
   ```

6. **Hard-limit enforcement**: `dmesg` output showing a container killed.  
   ```console
   satyam@ubuntu:~/boilerplate$ sudo ./engine run mem-hard ./rootfs-memhard "/memory_hog 60 10" --soft-mib 80 --hard-mib 100 >/dev/null
   satyam@ubuntu:~/boilerplate$ sudo dmesg | tail -n 2
   [ 2539.698365] [container_monitor] HARD LIMIT container=mem-hard pid=8177 rss=1007026176 limit=104857600
   [ 2539.717541] [container_monitor] Unregister request container=mem-hard pid=8177
   satyam@ubuntu:~/boilerplate$ sudo ./engine ps
   ID      PID     STATE   SOFT_MIB        HARD_MIB        EXIT_CODE       SIGNAL  LOG
   mem-hard 8177   killed  80      100     137     9       /home/satyam/boilerplate/logs/mem-hard.log
   ```

7. **Scheduling experiment**: Terminal output from scheduling experiment.  
   ```console
   satyam@ubuntu:~/boilerplate$ cat logs/cpuA.log | tail -n 3
   cpu_hog alive elapsed=9 accumulator=16417117073198412940
   cpu_hog alive elapsed=10 accumulator=12094921490895638282
   cpu_hog done duration=10 accumulator=12094921490895638282
   satyam@ubuntu:~/boilerplate$ cat logs/cpuB.log | tail -n 3
   cpu_hog alive elapsed=8 accumulator=12728264058592141712
   cpu_hog alive elapsed=9 accumulator=17708677337057241977
   cpu_hog done duration=10 accumulator=2734435780980839903
   ```

8. **Clean teardown**: Process list showing no zombies remain.  
   ```console
   satyam@ubuntu:~/boilerplate$ sudo killall engine
   satyam@ubuntu:~/boilerplate$ ps aux | grep engine
   satyam    8314  0.0  0.0  12132  2312 pts/0    S+   15:30   0:00 grep --color=auto engine
   ```

## 4. Engineering Analysis

### 4.1 Isolation Mechanisms
Our runtime achieves isolation primarily through Linux namespaces and Unix `chroot`. When `clone()` is invoked by the supervisor, we pass `CLONE_NEWPID`, `CLONE_NEWUTS`, and `CLONE_NEWNS`. The PID namespace isolates process IDs such that the container's init process is PID 1 internally. The UTS namespace isolates the hostname. The Mount namespace (`CLONE_NEWNS`), combined with `chroot`, restricts the container's filesystem view to its dedicated rootfs copy. However, the host kernel is entirely shared with the container, requiring the kernel monitor for memory limits.

### 4.2 Supervisor and Process Lifecycle
A long-running parent supervisor is crucial for multi-container orchestration. The supervisor creates child processes (containers) and outlives them. The parent tracks container metadata, and waits for children to exit via `waitpid(WNOHANG)` in a `SIGCHLD` handler. This loop reaps child processes immediately avoiding "zombies" and updates the in-memory metadata to accurately reflect complete/killed exits.

### 4.3 IPC, Threads, and Synchronization
**IPC:** We use UNIX Domain Sockets (`/tmp/mini_runtime.sock`) for control communication between CLI client and supervisor. For logs, we use anonymous pipes hooked to container `stdout`/`stderr`.
**Threads & Synchronization:** We employ a bounded-buffer producer-consumer pattern. Producer threads read from pipes, acquiring a `pthread_mutex` and using `pthread_cond_wait(&not_full)` when the buffer is full. A single consumer thread routes items to files, using `pthread_cond_wait(&not_empty)` when empty. This safely bridges non-deterministic container outputs to persistent storage without deadlocking.

### 4.4 Memory Management and Enforcement
RSS (Resident Set Size) measures the portion of a process's memory held in RAM as physical pages, ignoring swapped pages. Soft limits and hard limits provide distinct policies: a soft limit gracefully traces/warns to detect leaks early, while a hard limit aggressively kills runaway bounds. Enforcement belongs in kernel space (our `monitor.ko`) via a periodic timer callback, because user-space polling can be too slow, and terminating an out-of-memory container preemptively requires precise kernel context to avoid system instability.

### 4.5 Scheduling Behavior
Linux uses the Completely Fair Scheduler (CFS). When running `cpu_hog` simultaneously, CFS partitions CPU time fairly. Adjusting the `nice` value (e.g. `+19` lowering priority) forces the scheduler to allocate substantially more time-slices to non-niced processes. Conversely, an I/O process like `io_pulse` voluntarily yields CPU when blocking on hardware write events, maintaining responsiveness without starving hungry `cpu_hog` tasks.

## 5. Design Decisions and Tradeoffs

- **Namespace Isolation**: `chroot` was chosen over `pivot_root` due to its simplicity, even though `chroot` can potentially be escaped internally via crafted `..` traversal. This tradeoff favors straightforward implementation within project complexity, trusting standard container behaviors.
- **Supervisor Architecture**: The daemon polls requests synchronously using a Domain Socket while logger/child event handles leverage async threads/signals. `SIGCHLD` handler loops `waitpid` instead of a separate sweeper-thread, directly avoiding race conditions reading exit statuses.
- **Logging Pipeline**: The size of the bounded buffer handles 256 chunks of 1KB each. It prevents Out-Of-Memory conditions during heavy I/O spikes by stalling the producer threads (which backs up the pipe buffers and thus container writes), guaranteeing zero dropped log lines at the cost of transient I/O slow downs.
- **Kernel Monitor Lock**: We utilized `spin_lock_irqsave` in `monitor.c` for iterating the global list. Because `timer_callback` runs in a SoftIRQ context where `mutex_lock()` is strictly forbidden (cannot sleep or schedule), the spinlock ensures safety across the timer interrupt handler and `ioctl` configurations at the tradeoff of active polling overhead on collision.

## 6. Scheduler Experiment Results

### Experiment Overview
We verified Linux CFS scheduling behavior by comparing two CPU-bound (`cpu_hog`) containers running simultaneously under different priority (`nice`) levels on identical CPU cores.

**Results Comparison:**
| Workload | Nice Value | Accumulator (Work Done) | Elapsed Time |
|----------|------------|-------------------------|--------------|
| Container A `cpu_hog` | `0` (Normal) | Higher | 10s |
| Container B `cpu_hog` | `+10` (Low priority)| Significantly Lower | 10s |

**Analysis:**
The CFS Scheduler distributes processor slices corresponding to the weight associated with the `nice` priority value. The `nice=+10` container yielded the vast majority of its time slices to the `nice=0` process. In contrast, running `io_pulse` alongside `cpu_hog` at the same priority revealed `io_pulse` executed iteratively flawlessly without delay, because CFS dynamically boosts workloads that voluntarily block/sleep on I/O, ensuring exceptional responsiveness without starving batch-processing CPU-bound tasks.
