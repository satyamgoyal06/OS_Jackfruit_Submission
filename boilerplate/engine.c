/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define CONTAINER_ID_LEN 32
#define CHILD_COMMAND_LEN 512
#define LOG_PATH_LEN PATH_MAX
#define LOG_CHUNK_SIZE 1024
#define LOG_BUFFER_CAPACITY 256
#define CONTROL_MESSAGE_LEN 2048
#define UNIX_SOCKET_PATH "/tmp/mini_runtime.sock"
#define DEFAULT_SOFT_LIMIT (40UL * 1024 * 1024)
#define DEFAULT_HARD_LIMIT (64UL * 1024 * 1024)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int producer_started;
    int producer_joined;
    pthread_t producer_thread;
    void *child_stack;
    char log_path[LOG_PATH_LEN];
    struct container_record *next;
} container_record_t;

typedef struct {
    char log_path[LOG_PATH_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    int signal_pipe[2];
    char work_dir[PATH_MAX];
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    int read_fd;
    char log_path[LOG_PATH_LEN];
    bounded_buffer_t *log_buffer;
} producer_ctx_t;

static supervisor_ctx_t *g_ctx = NULL;
static volatile sig_atomic_t g_client_stop_requested = 0;
static char g_client_container_id[CONTAINER_ID_LEN];

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int parse_mib_flag(const char *flag, const char *value, unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int is_option_flag(const char *arg)
{
    return strcmp(arg, "--soft-mib") == 0 ||
           strcmp(arg, "--hard-mib") == 0 ||
           strcmp(arg, "--nice") == 0;
}

static int build_command_string(char *dest, size_t dest_size, int argc, char *argv[], int start_index, int *next_index)
{
    size_t used = 0;
    int i = start_index;

    if (i >= argc) {
        fprintf(stderr, "Missing command\n");
        return -1;
    }

    dest[0] = '\0';
    while (i < argc && !is_option_flag(argv[i])) {
        int written = snprintf(dest + used, dest_size - used, "%s%s", used ? " " : "", argv[i]);
        if (written < 0 || (size_t)written >= dest_size - used) {
            fprintf(stderr, "Command is too long\n");
            return -1;
        }
        used += (size_t)written;
        i++;
    }

    if (used == 0) {
        fprintf(stderr, "Missing command\n");
        return -1;
    }

    *next_index = i;
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc, char *argv[], int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' || nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static void *logging_thread(void *arg)
{
    bounded_buffer_t *buffer = (bounded_buffer_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(buffer, &item) == 0) {
        int fd = open(item.log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            ssize_t ignored = write(fd, item.data, item.length);
            (void)ignored;
            close(fd);
        }
    }

    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_ctx_t *pctx = (producer_ctx_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t n;

    while ((n = read(pctx->read_fd, buf, sizeof(buf))) > 0) {
        log_item_t item;

        memset(&item, 0, sizeof(item));
        strncpy(item.log_path, pctx->log_path, sizeof(item.log_path) - 1);
        item.length = (size_t)n;
        memcpy(item.data, buf, (size_t)n);

        if (bounded_buffer_push(pctx->log_buffer, &item) != 0)
            break;
    }

    close(pctx->read_fd);
    free(pctx);
    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    char *argv[] = {"/bin/sh", "-c", cfg->command, NULL};

    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    sethostname(cfg->id, strlen(cfg->id));

    if (mount("none", "/", NULL, MS_REC | MS_PRIVATE, NULL) == -1)
        perror("mount / private");

    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        return 1;
    }

    if (cfg->nice_value != 0 && nice(cfg->nice_value) == -1 && errno != 0)
        perror("nice");

    execv("/bin/sh", argv);
    perror("execv");
    return 1;
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return -1;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return -1;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static container_record_t *find_container_locked(const char *id)
{
    container_record_t *curr = g_ctx->containers;

    while (curr) {
        if (strcmp(curr->id, id) == 0)
            return curr;
        curr = curr->next;
    }

    return NULL;
}

static int container_is_active(container_record_t *rec)
{
    return rec->state == CONTAINER_STARTING || rec->state == CONTAINER_RUNNING;
}

static void join_container_resources(container_record_t *rec)
{
    if (rec->producer_started && !rec->producer_joined) {
        pthread_join(rec->producer_thread, NULL);
        rec->producer_joined = 1;
    }

    if (rec->child_stack) {
        free(rec->child_stack);
        rec->child_stack = NULL;
    }
}

static void reap_children(void)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec = NULL;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        for (rec = g_ctx->containers; rec; rec = rec->next) {
            if (rec->host_pid == pid)
                break;
        }

        if (rec) {
            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->exit_signal = 0;
                rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                rec->exit_signal = WTERMSIG(status);
                rec->exit_code = 128 + rec->exit_signal;
                if (rec->stop_requested) {
                    rec->state = CONTAINER_STOPPED;
                } else if (rec->exit_signal == SIGKILL) {
                    rec->state = CONTAINER_KILLED;
                } else {
                    rec->state = CONTAINER_EXITED;
                }
            }
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);

        if (rec) {
            unregister_from_monitor(g_ctx->monitor_fd, rec->id, rec->host_pid);
            join_container_resources(rec);
        }
    }
}

static void signal_handler(int sig)
{
    unsigned char byte;

    if (!g_ctx)
        return;

    if (sig == SIGCHLD) {
        byte = 'C';
    } else {
        g_ctx->should_stop = 1;
        byte = 'T';
    }

    if (g_ctx->signal_pipe[1] >= 0) {
        ssize_t ignored = write(g_ctx->signal_pipe[1], &byte, 1);
        (void)ignored;
    }
}

static int render_ps_locked(char *buf, size_t buf_size)
{
    container_record_t *curr;
    size_t used = 0;

    int written = snprintf(buf, buf_size,
                           "ID\tPID\tSTATE\tSOFT_MIB\tHARD_MIB\tEXIT_CODE\tSIGNAL\tLOG\n");
    if (written < 0 || (size_t)written >= buf_size)
        return -1;
    used += (size_t)written;

    for (curr = g_ctx->containers; curr; curr = curr->next) {
        written = snprintf(buf + used, buf_size - used,
                           "%s\t%d\t%s\t%lu\t%lu\t%d\t%d\t%s\n",
                           curr->id,
                           curr->host_pid,
                           state_to_string(curr->state),
                           curr->soft_limit_bytes >> 20,
                           curr->hard_limit_bytes >> 20,
                           curr->exit_code,
                           curr->exit_signal,
                           curr->log_path);
        if (written < 0 || (size_t)written >= buf_size - used)
            return -1;
        used += (size_t)written;
    }

    return 0;
}

static int start_container(const control_request_t *req, control_response_t *resp)
{
    int pipe_fd[2] = {-1, -1};
    void *stack = NULL;
    child_config_t *cfg = NULL;
    producer_ctx_t *pctx = NULL;
    container_record_t *rec = NULL;
    pid_t pid;

    pthread_mutex_lock(&g_ctx->metadata_lock);
    for (rec = g_ctx->containers; rec; rec = rec->next) {
        if (strcmp(rec->id, req->container_id) == 0 && container_is_active(rec)) {
            snprintf(resp->message, sizeof(resp->message), "Container %s already running.", req->container_id);
            resp->status = 1;
            pthread_mutex_unlock(&g_ctx->metadata_lock);
            return 0;
        }
        if (strcmp(rec->rootfs, req->rootfs) == 0 && container_is_active(rec)) {
            snprintf(resp->message, sizeof(resp->message),
                     "Rootfs %s is already in use by container %s.", req->rootfs, rec->id);
            resp->status = 1;
            pthread_mutex_unlock(&g_ctx->metadata_lock);
            return 0;
        }
    }
    pthread_mutex_unlock(&g_ctx->metadata_lock);

    if (pipe(pipe_fd) < 0) {
        snprintf(resp->message, sizeof(resp->message), "Failed to create log pipe: %s", strerror(errno));
        resp->status = 1;
        return 0;
    }

    cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        snprintf(resp->message, sizeof(resp->message), "Failed to allocate child config");
        resp->status = 1;
        close(pipe_fd[0]);
        close(pipe_fd[1]);
        return 0;
    }

    strncpy(cfg->id, req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, req->rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command, sizeof(cfg->command) - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipe_fd[1];

    stack = malloc(1024 * 1024);
    if (!stack) {
        snprintf(resp->message, sizeof(resp->message), "Failed to allocate child stack");
        resp->status = 1;
        close(pipe_fd[0]);
        close(pipe_fd[1]);
        free(cfg);
        return 0;
    }

    pid = clone(child_fn, (char *)stack + 1024 * 1024,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD, cfg);
    close(pipe_fd[1]);
    cfg->log_write_fd = -1;

    if (pid < 0) {
        snprintf(resp->message, sizeof(resp->message), "Failed to clone container: %s", strerror(errno));
        resp->status = 1;
        close(pipe_fd[0]);
        free(cfg);
        free(stack);
        return 0;
    }

    free(cfg);

    rec = calloc(1, sizeof(*rec));
    if (!rec) {
        snprintf(resp->message, sizeof(resp->message), "Container started but metadata allocation failed");
        resp->status = 1;
        kill(pid, SIGKILL);
        close(pipe_fd[0]);
        free(stack);
        return 0;
    }

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    strncpy(rec->rootfs, req->rootfs, sizeof(rec->rootfs) - 1);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->child_stack = stack;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/logs/%s.log", g_ctx->work_dir, req->container_id);

    pctx = calloc(1, sizeof(*pctx));
    if (!pctx) {
        snprintf(resp->message, sizeof(resp->message), "Failed to allocate producer context");
        resp->status = 1;
        kill(pid, SIGKILL);
        close(pipe_fd[0]);
        free(rec->child_stack);
        free(rec);
        return 0;
    }

    pctx->read_fd = pipe_fd[0];
    strncpy(pctx->log_path, rec->log_path, sizeof(pctx->log_path) - 1);
    pctx->log_buffer = &g_ctx->log_buffer;

    if (pthread_create(&rec->producer_thread, NULL, producer_thread, pctx) != 0) {
        snprintf(resp->message, sizeof(resp->message), "Failed to start producer thread");
        resp->status = 1;
        kill(pid, SIGKILL);
        close(pipe_fd[0]);
        free(pctx);
        free(rec->child_stack);
        free(rec);
        return 0;
    }
    rec->producer_started = 1;

    pthread_mutex_lock(&g_ctx->metadata_lock);
    rec->next = g_ctx->containers;
    g_ctx->containers = rec;
    pthread_mutex_unlock(&g_ctx->metadata_lock);

    if (register_with_monitor(g_ctx->monitor_fd, req->container_id, pid,
                              req->soft_limit_bytes, req->hard_limit_bytes) != 0) {
        fprintf(stderr, "Warning: failed to register %s with kernel monitor\n", req->container_id);
    }

    snprintf(resp->message, sizeof(resp->message),
             "Container %s started with PID %d. Log: %s",
             req->container_id, pid, rec->log_path);
    resp->status = 0;
    return 0;
}

static int handle_control_request(control_request_t *req, control_response_t *resp)
{
    container_record_t *rec;

    memset(resp, 0, sizeof(*resp));

    switch (req->kind) {
    case CMD_PS:
        pthread_mutex_lock(&g_ctx->metadata_lock);
        if (render_ps_locked(resp->message, sizeof(resp->message)) != 0) {
            snprintf(resp->message, sizeof(resp->message), "Failed to render container table");
            resp->status = 1;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
        return 0;

    case CMD_STOP:
        pthread_mutex_lock(&g_ctx->metadata_lock);
        rec = find_container_locked(req->container_id);
        if (!rec || !container_is_active(rec)) {
            snprintf(resp->message, sizeof(resp->message), "Container %s not found or not running.", req->container_id);
            resp->status = 1;
            pthread_mutex_unlock(&g_ctx->metadata_lock);
            return 0;
        }
        rec->stop_requested = 1;
        if (kill(rec->host_pid, SIGTERM) != 0) {
            snprintf(resp->message, sizeof(resp->message), "Failed to stop %s: %s", req->container_id, strerror(errno));
            resp->status = 1;
        } else {
            snprintf(resp->message, sizeof(resp->message), "Stop requested for container %s.", req->container_id);
            resp->status = 0;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
        return 0;

    case CMD_LOGS:
        pthread_mutex_lock(&g_ctx->metadata_lock);
        rec = find_container_locked(req->container_id);
        if (!rec) {
            snprintf(resp->message, sizeof(resp->message), "Container %s not found.", req->container_id);
            resp->status = 1;
        } else {
            snprintf(resp->message, sizeof(resp->message), "%s", rec->log_path);
            resp->status = 0;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
        return 0;

    case CMD_START:
    case CMD_RUN:
        return start_container(req, resp);

    default:
        snprintf(resp->message, sizeof(resp->message), "Unknown command");
        resp->status = 1;
        return 0;
    }
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sigaction sa;

    (void)rootfs;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    ctx.signal_pipe[0] = -1;
    ctx.signal_pipe[1] = -1;
    g_ctx = &ctx;

    if (!getcwd(ctx.work_dir, sizeof(ctx.work_dir))) {
        perror("getcwd");
        return 1;
    }

    if (mkdir("logs", 0755) != 0 && errno != EEXIST) {
        perror("mkdir logs");
        return 1;
    }

    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }

    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: could not open /dev/container_monitor\n");

    if (pipe(ctx.signal_pipe) != 0) {
        perror("pipe");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        return 1;
    }

    fcntl(ctx.signal_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(ctx.signal_pipe[1], F_SETFL, O_NONBLOCK);

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    if (sigaction(SIGCHLD, &sa, NULL) == -1 ||
        sigaction(SIGINT, &sa, NULL) == -1 ||
        sigaction(SIGTERM, &sa, NULL) == -1) {
        perror("sigaction");
        close(ctx.signal_pipe[0]);
        close(ctx.signal_pipe[1]);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        return 1;
    }

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx.log_buffer) != 0) {
        perror("pthread_create logger");
        close(ctx.signal_pipe[0]);
        close(ctx.signal_pipe[1]);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        return 1;
    }

    unlink(UNIX_SOCKET_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        ctx.should_stop = 1;
    }

    if (!ctx.should_stop) {
        struct sockaddr_un addr;

        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, UNIX_SOCKET_PATH, sizeof(addr.sun_path) - 1);

        if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1 ||
            listen(ctx.server_fd, 5) == -1) {
            perror("bind/listen");
            ctx.should_stop = 1;
        }
    }

    if (!ctx.should_stop)
        printf("Supervisor running on %s\n", UNIX_SOCKET_PATH);

    while (!ctx.should_stop) {
        fd_set rfds;
        int max_fd = ctx.server_fd > ctx.signal_pipe[0] ? ctx.server_fd : ctx.signal_pipe[0];
        int ready;

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        FD_SET(ctx.signal_pipe[0], &rfds);

        ready = select(max_fd + 1, &rfds, NULL, NULL, NULL);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (FD_ISSET(ctx.signal_pipe[0], &rfds)) {
            char buf[32];
            ssize_t n = read(ctx.signal_pipe[0], buf, sizeof(buf));
            ssize_t i;
            for (i = 0; i < n; i++) {
                if (buf[i] == 'C')
                    reap_children();
                if (buf[i] == 'T')
                    ctx.should_stop = 1;
            }
        }

        if (ctx.should_stop)
            break;

        if (FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                control_request_t req;
                control_response_t resp;
                ssize_t got = read(client_fd, &req, sizeof(req));

                if (got == (ssize_t)sizeof(req)) {
                    handle_control_request(&req, &resp);
                } else {
                    memset(&resp, 0, sizeof(resp));
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "Invalid request");
                }

                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
            }
        }
    }

    printf("Supervisor shutting down...\n");

    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *curr = ctx.containers; curr; curr = curr->next) {
        if (container_is_active(curr)) {
            curr->stop_requested = 1;
            kill(curr->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    for (int i = 0; i < 30; i++) {
        reap_children();
        pthread_mutex_lock(&ctx.metadata_lock);
        int active = 0;
        for (container_record_t *curr = ctx.containers; curr; curr = curr->next) {
            if (container_is_active(curr)) {
                active = 1;
                break;
            }
        }
        pthread_mutex_unlock(&ctx.metadata_lock);
        if (!active)
            break;
        usleep(100000);
    }

    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *curr = ctx.containers; curr; curr = curr->next) {
        if (container_is_active(curr))
            kill(curr->host_pid, SIGKILL);
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    reap_children();

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    pthread_mutex_lock(&ctx.metadata_lock);
    while (ctx.containers) {
        container_record_t *next = ctx.containers->next;
        join_container_resources(ctx.containers);
        free(ctx.containers);
        ctx.containers = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (ctx.signal_pipe[0] >= 0)
        close(ctx.signal_pipe[0]);
    if (ctx.signal_pipe[1] >= 0)
        close(ctx.signal_pipe[1]);
    unlink(UNIX_SOCKET_PATH);

    return 0;
}

static int send_control_request(const control_request_t *req, control_response_t *resp)
{
    int sfd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr;

    if (sfd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, UNIX_SOCKET_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("connect");
        close(sfd);
        return 1;
    }

    if (write(sfd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(sfd);
        return 1;
    }

    if (read(sfd, resp, sizeof(*resp)) <= 0) {
        perror("read response");
        close(sfd);
        return 1;
    }

    close(sfd);
    return resp->status;
}

static int parse_container_status_line(const char *ps_output, const char *id, char *state, size_t state_size, int *exit_code)
{
    char *copy = strdup(ps_output);
    char *line;
    int found = 0;

    if (!copy)
        return -1;

    line = strtok(copy, "\n");
    while (line) {
        char parsed_id[CONTAINER_ID_LEN];
        int pid;
        unsigned long soft_mib;
        unsigned long hard_mib;
        int signal_no;
        char parsed_state[32];
        char log_path[LOG_PATH_LEN];
        int parsed_exit;

        if (sscanf(line, "%31[^\t]\t%d\t%31[^\t]\t%lu\t%lu\t%d\t%d\t%4095[^\n]",
                   parsed_id, &pid, parsed_state, &soft_mib, &hard_mib,
                   &parsed_exit, &signal_no, log_path) == 8) {
            if (strcmp(parsed_id, id) == 0) {
                strncpy(state, parsed_state, state_size - 1);
                state[state_size - 1] = '\0';
                *exit_code = parsed_exit;
                found = 1;
                break;
            }
        }

        line = strtok(NULL, "\n");
    }

    free(copy);
    return found ? 0 : -1;
}

static void client_signal_handler(int sig)
{
    (void)sig;
    g_client_stop_requested = 1;
}

static int wait_for_container_completion(const char *id)
{
    int stop_forwarded = 0;

    while (1) {
        control_request_t ps_req;
        control_response_t ps_resp;
        char state[32] = {0};
        int exit_code = 0;

        if (g_client_stop_requested && !stop_forwarded) {
            control_request_t stop_req;
            control_response_t stop_resp;
            memset(&stop_req, 0, sizeof(stop_req));
            stop_req.kind = CMD_STOP;
            strncpy(stop_req.container_id, id, sizeof(stop_req.container_id) - 1);
            send_control_request(&stop_req, &stop_resp);
            stop_forwarded = 1;
        }

        memset(&ps_req, 0, sizeof(ps_req));
        ps_req.kind = CMD_PS;
        if (send_control_request(&ps_req, &ps_resp) != 0)
            return 1;

        if (parse_container_status_line(ps_resp.message, id, state, sizeof(state), &exit_code) != 0)
            return 1;

        if (strcmp(state, "running") != 0 && strcmp(state, "starting") != 0)
            return exit_code;

        usleep(250000);
    }
}

static int fill_request_from_cli(control_request_t *req, command_kind_t kind, int argc, char *argv[])
{
    int option_index;

    if (argc < 5) {
        fprintf(stderr, "Usage: %s %s <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0], kind == CMD_START ? "start" : "run");
        return -1;
    }

    memset(req, 0, sizeof(*req));
    req->kind = kind;
    req->soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req->hard_limit_bytes = DEFAULT_HARD_LIMIT;
    strncpy(req->container_id, argv[2], sizeof(req->container_id) - 1);
    if (!realpath(argv[3], req->rootfs)) {
        strncpy(req->rootfs, argv[3], sizeof(req->rootfs) - 1);
    }

    if (build_command_string(req->command, sizeof(req->command), argc, argv, 4, &option_index) != 0)
        return -1;

    if (parse_optional_flags(req, argc, argv, option_index) != 0)
        return -1;

    return 0;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    control_response_t resp;
    int rc;

    if (fill_request_from_cli(&req, CMD_START, argc, argv) != 0)
        return 1;

    rc = send_control_request(&req, &resp);
    printf("%s\n", resp.message);
    return rc;
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    control_response_t resp;
    struct sigaction sa;
    int rc;

    if (fill_request_from_cli(&req, CMD_RUN, argc, argv) != 0)
        return 1;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = client_signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    strncpy(g_client_container_id, req.container_id, sizeof(g_client_container_id) - 1);
    g_client_stop_requested = 0;

    rc = send_control_request(&req, &resp);
    printf("%s\n", resp.message);
    if (rc != 0)
        return rc;

    return wait_for_container_completion(req.container_id);
}

static int cmd_ps(void)
{
    control_request_t req;
    control_response_t resp;
    int rc;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    rc = send_control_request(&req, &resp);
    printf("%s", resp.message);
    return rc;
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    control_response_t resp;
    FILE *f;
    char buf[1024];
    int rc;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    rc = send_control_request(&req, &resp);
    if (rc != 0) {
        printf("%s\n", resp.message);
        return rc;
    }

    f = fopen(resp.message, "r");
    if (!f) {
        perror("fopen");
        return 1;
    }

    while (fgets(buf, sizeof(buf), f))
        fputs(buf, stdout);
    fclose(f);
    return 0;
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    control_response_t resp;
    int rc;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    rc = send_control_request(&req, &resp);
    printf("%s\n", resp.message);
    return rc;
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);
    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();
    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
