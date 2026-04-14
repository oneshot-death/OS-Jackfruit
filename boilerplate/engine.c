/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Tasks implemented:
 *   Task 1 - fork()+unshare(), chroot() filesystem isolation, /proc mount,
 *            SIGCHLD reaping, per-container metadata.
 *   Task 2 - Named-pipe (FIFO) control plane.
 *   Task 3 - Bounded-buffer logging: mutex + condition-variable ring buffer.
 *   Task 4 - /proc/<pid>/status VmRSS polling (user-space fallback) +
 *            ioctl kernel path + stop_requested attribution.
 *   Task 5 - nice()-based CPU priority experiment wired through the CLI.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* =========================================================================
 * Constants
 * ========================================================================= */
#define CONTAINER_ID_LEN     32
#define CONTROL_MESSAGE_LEN  512
#define CHILD_COMMAND_LEN    256
#define LOG_CHUNK_SIZE       4096
#define LOG_BUFFER_CAPACITY  16
#define DEFAULT_SOFT_LIMIT   (40UL << 20)
#define DEFAULT_HARD_LIMIT   (64UL << 20)
#define LOG_DIR              "logs"

/* -------------------------------------------------------------------------
 * Task 2 – FIFO paths
 
 * ------------------------------------------------------------------------- */
#define FIFO_REQ_PATH    "/tmp/mini_runtime_req.fifo"
#define FIFO_RESP_PREFIX "/tmp/mini_runtime_resp_"

/* =========================================================================
 * Types
 * ========================================================================= */
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
    char              id[CONTAINER_ID_LEN];
    pid_t             host_pid;
    time_t            started_at;
    container_state_t state;
    unsigned long     soft_limit_bytes;
    unsigned long     hard_limit_bytes;
    int               exit_code;
    int               exit_signal;
    int               stop_requested;   /* Task 4: attribution flag */
    char              log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

/* -------------------------------------------------------------------------
 * Task 3 – Bounded buffer (mutex + condition variables)
 * ------------------------------------------------------------------------- */
typedef struct {
    log_item_t      items[LOG_BUFFER_CAPACITY];
    size_t          head;
    size_t          tail;
    size_t          count;
    int             shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    pid_t          client_pid;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           command[CHILD_COMMAND_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int  nice_value;
    int  log_write_fd;
} child_config_t;

typedef struct {
    int              pipe_read_fd;
    char             container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

typedef struct {
    int              req_fifo_fd;
    int              monitor_fd;
    volatile int     should_stop;
    pthread_t        logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t  metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* =========================================================================
 * Globals
 * ========================================================================= */
static supervisor_ctx_t *g_ctx = NULL;

/* =========================================================================
 * Utility helpers
 * ========================================================================= */
static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <base-rootfs>\n"
        "  %s start <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s run   <id> <rootfs> <cmd> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n"
        "  %s logs <id>\n"
        "  %s stop <id>\n",
        prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
                          unsigned long *out)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno || end == value || *end) {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value too large for %s\n", flag);
        return -1;
    }
    *out = mib << 20;
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc, char *argv[], int start)
{
    int i;
    for (i = start; i < argc; i += 2) {
        char *end = NULL; long nv;
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for %s\n", argv[i]); return -1;
        }
        if (!strcmp(argv[i], "--soft-mib")) {
            if (parse_mib_flag("--soft-mib", argv[i+1], &req->soft_limit_bytes)) return -1;
        } else if (!strcmp(argv[i], "--hard-mib")) {
            if (parse_mib_flag("--hard-mib", argv[i+1], &req->hard_limit_bytes)) return -1;
        } else if (!strcmp(argv[i], "--nice")) {
            errno = 0;
            nv = strtol(argv[i+1], &end, 10);
            if (errno || end == argv[i+1] || *end || nv < -20 || nv > 19) {
                fprintf(stderr, "Invalid --nice (expected -20..19): %s\n", argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nv;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]); return -1;
        }
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "soft limit cannot exceed hard limit\n"); return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t s)
{
    switch (s) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

static void generate_log_path(const char *id, char *log_path)
{
    mkdir(LOG_DIR, 0755);
    snprintf(log_path, PATH_MAX, "%s/%s.log", LOG_DIR, id);
}

static void make_resp_path(pid_t pid, char *buf, size_t len)
{
    snprintf(buf, len, "%s%d.fifo", FIFO_RESP_PREFIX, (int)pid);
}

/* =========================================================================
 * Task 3 – Bounded buffer implementation
 * ========================================================================= */
static int bounded_buffer_init(bounded_buffer_t *b)
{
    int rc;
    memset(b, 0, sizeof(*b));
    if ((rc = pthread_mutex_init(&b->mutex, NULL))) return rc;
    if ((rc = pthread_cond_init(&b->not_empty, NULL))) {
        pthread_mutex_destroy(&b->mutex); return rc;
    }
    if ((rc = pthread_cond_init(&b->not_full, NULL))) {
        pthread_cond_destroy(&b->not_empty);
        pthread_mutex_destroy(&b->mutex); return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *b)
{
    pthread_mutex_lock(&b->mutex);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
}

int bounded_buffer_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == LOG_BUFFER_CAPACITY && !b->shutting_down)
        pthread_cond_wait(&b->not_full, &b->mutex);

    if (b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }

    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;
    pthread_cond_signal(&b->not_empty);  /* wake one consumer */
    pthread_mutex_unlock(&b->mutex);
    return 0;
}


int bounded_buffer_pop(bounded_buffer_t *b, log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);
    while (b->count == 0 && !b->shutting_down)
        pthread_cond_wait(&b->not_empty, &b->mutex);

    if (b->count == 0) {          /* must be shutting_down */
        pthread_mutex_unlock(&b->mutex);
        return 1;
    }

    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
    b->count--;
    pthread_cond_signal(&b->not_full);   /* wake a blocked producer */
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/* Consumer thread: drains bounded buffer to per-container log files */
void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t item;

    while (1) {
        int rc = bounded_buffer_pop(buf, &item);
        if (rc == 1) break;   /* drained and shutting down */
        if (rc != 0) continue;

        char log_path[PATH_MAX];
        generate_log_path(item.container_id, log_path);

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) { perror("logging_thread open"); continue; }

        ssize_t written = 0;
        while ((size_t)written < item.length) {
            ssize_t n = write(fd, item.data + written,
                              item.length - (size_t)written);
            if (n < 0) { perror("logging_thread write"); break; }
            written += n;
        }
        close(fd);
    }
    return NULL;
}

/* Producer thread: one per container; reads container pipe -> bounded buffer */
static void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, parg->container_id, CONTAINER_ID_LEN - 1);

    while (1) {
        n = read(parg->pipe_read_fd, item.data, LOG_CHUNK_SIZE - 1);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            break;   /* container exited (EOF) or read error */
        }
        item.data[n] = '\0';
        item.length = (size_t)n;
        if (bounded_buffer_push(parg->buffer, &item) != 0) break;
    }

    close(parg->pipe_read_fd);
    free(parg);
    return NULL;
}

/* =========================================================================
 * Task 1 – child_fn: namespace isolation + chroot + exec
 * ========================================================================= */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (unshare(CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS) != 0) {
        perror("unshare"); return 1;
    }

    /* --- 2. Redirect stdout/stderr into the logging pipe ---------------- */
    if (cfg->log_write_fd >= 0) {
        if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) { perror("dup2 stdout"); return 1; }
        if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) { perror("dup2 stderr"); return 1; }
        if (cfg->log_write_fd != STDOUT_FILENO && cfg->log_write_fd != STDERR_FILENO)
            close(cfg->log_write_fd);
    }

    /* --- 3. Container hostname ------------------------------------------ */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0)
        perror("sethostname (non-fatal)");

    if (chroot(cfg->rootfs) != 0) { perror("chroot"); return 1; }
    if (chdir("/") != 0)          { perror("chdir /"); return 1; }

    /* --- 5. Mount /proc (private to this mount namespace) --------------- */
    if (mkdir("/proc", 0555) != 0 && errno != EEXIST)
        perror("mkdir /proc (non-fatal)");
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("mount /proc (non-fatal)");

    /* --- 6. nice() – Task 5 CPU priority hook ---------------------------
     * Applied before exec so the target command inherits the priority.
     * Positive = less CPU; negative = more (requires CAP_SYS_NICE).
     */
    if (cfg->nice_value != 0) {
        errno = 0;
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice (non-fatal)");
    }

    /* --- 7. exec -------------------------------------------------------- */
    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 127;
}

/* =========================================================================
 * Task 1 – Monitor ioctl wrappers
 * ========================================================================= */
int register_with_monitor(int fd, const char *id, pid_t pid,
                          unsigned long soft, unsigned long hard)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = pid; req.soft_limit_bytes = soft; req.hard_limit_bytes = hard;
    strncpy(req.container_id, id, sizeof(req.container_id) - 1);
    return (ioctl(fd, MONITOR_REGISTER, &req) < 0) ? -1 : 0;
}

int unregister_from_monitor(int fd, const char *id, pid_t pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = pid;
    strncpy(req.container_id, id, sizeof(req.container_id) - 1);
    return (ioctl(fd, MONITOR_UNREGISTER, &req) < 0) ? -1 : 0;
}

/* =========================================================================
 * Task 4 – /proc/<pid>/status VmRSS reader
 
 * ========================================================================= */
static long read_vmrss_kb(pid_t pid)
{
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/status", (int)pid);

    FILE *f = fopen(path, "r");
    if (!f) return -1;

    char line[256];
    long rss_kb = -1;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, "%ld", &rss_kb);
            break;
        }
    }
    fclose(f);
    return rss_kb;
}

static void *userspace_monitor_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;

    while (!ctx->should_stop) {
        sleep(1);

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        while (rec) {
            if (rec->state != CONTAINER_RUNNING) { rec = rec->next; continue; }

            long rss_kb = read_vmrss_kb(rec->host_pid);
            if (rss_kb < 0) { rec = rec->next; continue; }

            long rss_bytes = rss_kb * 1024L;

            if ((unsigned long)rss_bytes > rec->hard_limit_bytes) {
                fprintf(stderr,
                    "[monitor] HARD LIMIT container=%s pid=%d "
                    "rss=%ldB limit=%luB — sending SIGKILL\n",
                    rec->id, rec->host_pid, rss_bytes, rec->hard_limit_bytes);
                /* stop_requested stays 0: SIGCHLD classifies as KILLED */
                kill(rec->host_pid, SIGKILL);
            } else if ((unsigned long)rss_bytes > rec->soft_limit_bytes) {
                /* Warn only once per container lifetime */
                static char warned[CONTAINER_ID_LEN] = "";
                if (strcmp(warned, rec->id) != 0) {
                    fprintf(stderr,
                        "[monitor] SOFT LIMIT container=%s pid=%d "
                        "rss=%ldB limit=%luB\n",
                        rec->id, rec->host_pid, rss_bytes, rec->soft_limit_bytes);
                    strncpy(warned, rec->id, CONTAINER_ID_LEN - 1);
                }
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
    return NULL;
}

/* =========================================================================
 * Task 1 – Signal handlers
 * ========================================================================= */
static void sigchld_handler(int sig)
{
    (void)sig;
    if (!g_ctx) return;

    int status; pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code   = 128 + rec->exit_signal;
                    if (rec->state == CONTAINER_RUNNING) {
                        /*
                         * Task 4 attribution rule:
                         * stop_requested=1 => operator issued 'engine stop'
                         * stop_requested=0 + SIGKILL => hard-limit enforcement
                         */
                        rec->state = rec->stop_requested
                                   ? CONTAINER_STOPPED
                                   : CONTAINER_KILLED;
                    }
                }
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

/* =========================================================================
 * Task 1 – spawn_container
 * ========================================================================= */
static int spawn_container(supervisor_ctx_t *ctx,
                           const control_request_t *req)
{
    int pipefd[2];
    if (pipe(pipefd) != 0) { perror("pipe"); return -1; }
    fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);

    child_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    strncpy(cfg.id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg.rootfs,  req->rootfs,       PATH_MAX - 1);
    strncpy(cfg.command, req->command,      CHILD_COMMAND_LEN - 1);
    cfg.nice_value   = req->nice_value;
    cfg.log_write_fd = pipefd[1];

    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) { perror("calloc"); close(pipefd[0]); close(pipefd[1]); return -1; }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_STARTING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code        = -1;
    generate_log_path(req->container_id, rec->log_path);

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork"); free(rec); close(pipefd[0]); close(pipefd[1]);
        return -1;
    }
    if (pid == 0) {
        close(pipefd[0]);
        _exit(child_fn(&cfg));
    }

    /* Parent */
    close(pipefd[1]);
    rec->host_pid = pid;
    rec->state    = CONTAINER_RUNNING;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next        = ctx->containers;
    ctx->containers  = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0)
        if (register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                                  req->soft_limit_bytes, req->hard_limit_bytes) != 0)
            perror("register_with_monitor (non-fatal)");

    /* Task 3: launch producer thread for this container's pipe */
    producer_arg_t *parg = malloc(sizeof(*parg));
    if (parg) {
        parg->pipe_read_fd = pipefd[0];
        parg->buffer       = &ctx->log_buffer;
        strncpy(parg->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        pthread_t pt;
        if (pthread_create(&pt, NULL, producer_thread, parg) == 0)
            pthread_detach(pt);
        else { perror("pthread_create producer"); close(pipefd[0]); free(parg); }
    } else {
        close(pipefd[0]);
    }

    fprintf(stderr, "[supervisor] started id=%-16s host_pid=%-6d nice=%d\n",
            rec->id, pid, req->nice_value);
    return 0;
}

/* =========================================================================
 * Task 2 – Supervisor-side FIFO helpers
 * ========================================================================= */
static void supervisor_send_response(pid_t client_pid, int status,
                                     const char *msg)
{
    char resp_path[PATH_MAX];
    make_resp_path(client_pid, resp_path, sizeof(resp_path));

    int fd = -1, tries;
    for (tries = 0; tries < 30 && fd < 0; tries++) {
        fd = open(resp_path, O_WRONLY | O_NONBLOCK);
        if (fd < 0) usleep(10000);
    }
    if (fd < 0) { perror("supervisor_send_response open"); return; }

    control_response_t resp;
    memset(&resp, 0, sizeof(resp));
    resp.status = status;
    if (msg) strncpy(resp.message, msg, sizeof(resp.message) - 1);
    write(fd, &resp, sizeof(resp));
    close(fd);
}

static void supervisor_handle_request(supervisor_ctx_t *ctx,
                                      const control_request_t *req)
{
    char msg[CONTROL_MESSAGE_LEN];

    switch (req->kind) {

    case CMD_START:
    case CMD_RUN: {
        /* Reject duplicate running containers */
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c) {
            if (!strcmp(c->id, req->container_id) &&
                (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING)) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                snprintf(msg, sizeof(msg), "container '%s' already running",
                         req->container_id);
                supervisor_send_response(req->client_pid, 1, msg);
                return;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (spawn_container(ctx, req) != 0) {
            supervisor_send_response(req->client_pid, 1, "spawn failed");
            return;
        }

        if (req->kind == CMD_START) {
            snprintf(msg, sizeof(msg), "started %s", req->container_id);
            supervisor_send_response(req->client_pid, 0, msg);
        } else {
            /* CMD_RUN: ack the start, then block until child exits */
            snprintf(msg, sizeof(msg), "running %s", req->container_id);
            supervisor_send_response(req->client_pid, 0, msg);

            pid_t target = -1;
            pthread_mutex_lock(&ctx->metadata_lock);
            for (c = ctx->containers; c; c = c->next)
                if (!strcmp(c->id, req->container_id)) { target = c->host_pid; break; }
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (target > 0) {
                int ws;
                waitpid(target, &ws, 0);
                int code = WIFEXITED(ws) ? WEXITSTATUS(ws)
                         : (WIFSIGNALED(ws) ? 128 + WTERMSIG(ws) : -1);
                snprintf(msg, sizeof(msg), "exited code=%d", code);
                supervisor_send_response(req->client_pid, code, msg);
            }
        }
        break;
    }

    case CMD_PS: {
        char table[CONTROL_MESSAGE_LEN];
        int off = 0;
        off += snprintf(table + off, sizeof(table) - (size_t)off,
                        "%-16s %-7s %-10s %-8s %-8s %-5s\n",
                        "ID", "PID", "STATE", "SOFT_MiB", "HARD_MiB", "EXIT");
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *r = ctx->containers;
             r && off < (int)sizeof(table) - 1; r = r->next) {
            off += snprintf(table + off, sizeof(table) - (size_t)off,
                            "%-16s %-7d %-10s %-8lu %-8lu %-5d\n",
                            r->id, r->host_pid, state_to_string(r->state),
                            r->soft_limit_bytes >> 20, r->hard_limit_bytes >> 20,
                            r->exit_code);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        supervisor_send_response(req->client_pid, 0, table);
        break;
    }

    case CMD_LOGS: {
        char log_path[PATH_MAX];
        generate_log_path(req->container_id, log_path);
        int lfd = open(log_path, O_RDONLY);
        if (lfd < 0) {
            snprintf(msg, sizeof(msg), "no log for '%s'", req->container_id);
            supervisor_send_response(req->client_pid, 1, msg);
            break;
        }
        char buf[CONTROL_MESSAGE_LEN - 1];
        ssize_t n;
        while ((n = read(lfd, buf, sizeof(buf) - 1)) > 0) {
            buf[n] = '\0';
            supervisor_send_response(req->client_pid, 0, buf);
        }
        close(lfd);
        supervisor_send_response(req->client_pid, 0, ""); /* end sentinel */
        break;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r;
        for (r = ctx->containers; r; r = r->next) {
            if (!strcmp(r->id, req->container_id)) {
                if (r->state == CONTAINER_RUNNING) {
                    /*
                     * Task 4: set stop_requested BEFORE sending the signal.
                     * The SIGCHLD handler checks this flag to distinguish
                     * operator stop (CONTAINER_STOPPED) from hard-limit kill
                     * (CONTAINER_KILLED).
                     */
                    r->stop_requested = 1;
                    r->state          = CONTAINER_STOPPED;
                    kill(r->host_pid, SIGTERM);
                    if (ctx->monitor_fd >= 0)
                        unregister_from_monitor(ctx->monitor_fd, r->id, r->host_pid);
                    pthread_mutex_unlock(&ctx->metadata_lock);
                    snprintf(msg, sizeof(msg), "sent SIGTERM to %s (pid=%d)",
                             r->id, r->host_pid);
                    supervisor_send_response(req->client_pid, 0, msg);
                    return;
                }
                pthread_mutex_unlock(&ctx->metadata_lock);
                snprintf(msg, sizeof(msg), "container '%s' not running (state=%s)",
                         r->id, state_to_string(r->state));
                supervisor_send_response(req->client_pid, 1, msg);
                return;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(msg, sizeof(msg), "container '%s' not found", req->container_id);
        supervisor_send_response(req->client_pid, 1, msg);
        break;
    }

    default:
        supervisor_send_response(req->client_pid, 1, "unknown command");
        break;
    }
}

/* =========================================================================
 * Task 2 – run_supervisor: FIFO event loop
 * ========================================================================= */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.req_fifo_fd = -1;
    ctx.monitor_fd  = -1;
    g_ctx = &ctx;

    if (pthread_mutex_init(&ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init"); return 1;
    }
    if (bounded_buffer_init(&ctx.log_buffer) != 0) {
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock); return 1;
    }

    /* Signal handlers */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);
    sa.sa_handler = sigterm_handler;
    sa.sa_flags   = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Kernel monitor (optional) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] kernel monitor unavailable — using /proc fallback\n");

    mkdir(LOG_DIR, 0755);

    /* Task 3: logger consumer thread */
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx.log_buffer) != 0) {
        perror("pthread_create logger");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock); return 1;
    }

    pthread_t monitor_thread;
    int mon_thread_started = 0;
    if (ctx.monitor_fd < 0) {
        if (pthread_create(&monitor_thread, NULL,
                           userspace_monitor_thread, &ctx) == 0)
            mon_thread_started = 1;
        else
            perror("pthread_create userspace_monitor (non-fatal)");
    }

 
    unlink(FIFO_REQ_PATH);
    if (mkfifo(FIFO_REQ_PATH, 0600) != 0) {
        perror("mkfifo"); goto cleanup;
    }
    ctx.req_fifo_fd = open(FIFO_REQ_PATH, O_RDWR);
    if (ctx.req_fifo_fd < 0) {
        perror("open request FIFO"); unlink(FIFO_REQ_PATH); goto cleanup;
    }

    fprintf(stderr,
            "[supervisor] ready  rootfs=%s  fifo=%s  monitor=%s\n",
            rootfs, FIFO_REQ_PATH,
            ctx.monitor_fd >= 0 ? "kernel-module" : "/proc-fallback");

    /* ---- Main event loop ---- */
    while (!ctx.should_stop) {
        control_request_t req;
        ssize_t total = 0;
        char   *buf   = (char *)&req;

        while (total < (ssize_t)sizeof(req) && !ctx.should_stop) {
            ssize_t n = read(ctx.req_fifo_fd, buf + total,
                             sizeof(req) - (size_t)total);
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("read fifo"); goto shutdown;
            }
            if (n == 0) { usleep(1000); continue; }
            total += n;
        }

        if (total == (ssize_t)sizeof(req))
            supervisor_handle_request(&ctx, &req);
    }

shutdown:
    fprintf(stderr, "[supervisor] shutting down\n");

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *r = ctx.containers; r; r = r->next) {
        if (r->state == CONTAINER_RUNNING || r->state == CONTAINER_STARTING) {
            r->stop_requested = 1;
            r->state = CONTAINER_STOPPED;
            kill(r->host_pid, SIGTERM);
        }
        if (ctx.monitor_fd >= 0)
            unregister_from_monitor(ctx.monitor_fd, r->id, r->host_pid);
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    while (waitpid(-1, NULL, 0) > 0) ;

    /* Join threads */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    if (mon_thread_started) pthread_join(monitor_thread, NULL);

cleanup:
    /* Free metadata */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *r = ctx.containers;
        while (r) { container_record_t *nxt = r->next; free(r); r = nxt; }
        ctx.containers = NULL;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    if (ctx.req_fifo_fd >= 0) close(ctx.req_fifo_fd);
    unlink(FIFO_REQ_PATH);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    g_ctx = NULL;
    fprintf(stderr, "[supervisor] exited cleanly\n");
    return 0;
}

/* =========================================================================
 * Task 2 – Client: send_control_request via FIFOs
 * ========================================================================= */
static int send_control_request(const control_request_t *req)
{
    char resp_path[PATH_MAX];
    make_resp_path(req->client_pid, resp_path, sizeof(resp_path));

    /* Create response FIFO before writing request (avoids ENXIO race) */
    unlink(resp_path);
    if (mkfifo(resp_path, 0600) != 0) {
        perror("mkfifo response FIFO"); return 1;
    }

    int req_fd = open(FIFO_REQ_PATH, O_WRONLY);
    if (req_fd < 0) {
        perror("open request FIFO (is supervisor running?)");
        unlink(resp_path); return 1;
    }
    if (write(req_fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request FIFO"); close(req_fd); unlink(resp_path); return 1;
    }
    close(req_fd);

    int resp_fd = open(resp_path, O_RDONLY);
    if (resp_fd < 0) {
        perror("open response FIFO"); unlink(resp_path); return 1;
    }

    int exit_status = 0;
    if (req->kind == CMD_LOGS) {
        control_response_t resp;
        while (1) {
            ssize_t n = read(resp_fd, &resp, sizeof(resp));
            if (n <= 0 || resp.message[0] == '\0') break;
            printf("%s", resp.message);
            if (resp.status != 0) { exit_status = resp.status; break; }
        }
    } else {
        control_response_t resp;
        ssize_t n = read(resp_fd, &resp, sizeof(resp));
        if (n == (ssize_t)sizeof(resp)) {
            if (req->kind == CMD_PS)
                printf("%s", resp.message);
            else
                fprintf(stderr, "[engine] %s\n", resp.message);
            exit_status = resp.status;
        }
    }

    close(resp_fd);
    unlink(resp_path);
    return exit_status;
}

/* =========================================================================
 * CLI helpers
 * ========================================================================= */
static control_request_t make_base_req(command_kind_t kind)
{
    control_request_t r;
    memset(&r, 0, sizeof(r));
    r.kind             = kind;
    r.client_pid       = getpid();
    r.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    r.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    return r;
}

static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr, "Usage: %s start <id> <rootfs> <cmd> [...]\n", argv[0]);
        return 1;
    }
    control_request_t req = make_base_req(CMD_START);
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    if (parse_optional_flags(&req, argc, argv, 5)) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr, "Usage: %s run <id> <rootfs> <cmd> [...]\n", argv[0]);
        return 1;
    }
    control_request_t req = make_base_req(CMD_RUN);
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    if (parse_optional_flags(&req, argc, argv, 5)) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req = make_base_req(CMD_PS);
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    control_request_t req = make_base_req(CMD_LOGS);
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    control_request_t req = make_base_req(CMD_STOP);
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

/* =========================================================================
 * main
 * ========================================================================= */
int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }
    if (!strcmp(argv[1], "supervisor")) {
        if (argc < 3) { fprintf(stderr, "Usage: %s supervisor <rootfs>\n", argv[0]); return 1; }
        return run_supervisor(argv[2]);
    }
    if (!strcmp(argv[1], "start")) return cmd_start(argc, argv);
    if (!strcmp(argv[1], "run"))   return cmd_run(argc, argv);
    if (!strcmp(argv[1], "ps"))    return cmd_ps();
    if (!strcmp(argv[1], "logs"))  return cmd_logs(argc, argv);
    if (!strcmp(argv[1], "stop"))  return cmd_stop(argc, argv);
    usage(argv[0]); return 1;
}
