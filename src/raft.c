#define _XOPEN_SOURCE 800
#include "config.h"

#include <unistd.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/select.h>
#include <time.h>
#include <stdio.h>
#include <err.h>
#include <errno.h>
#ifdef HAVE_PTHREAD_H
# include <pthread.h>
#else
# error "pthread.h is required"
#endif
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <syslog.h>

#include "debug.h"
#include "raft.h"

#ifdef TESTS
# include "raft_test_api.h"
#endif

#define MAX(a,b) (((a)>(b)) ? (a) : (b))
#define MIN(a,b) (((a)<(b)) ? (a) : (b))

/* Global variables */
struct raft_state raft_state;
const struct raft_impl *raft_impl = NULL;

/* Magic Numbers */
static const unsigned  RAFT_MAX_PACKET_SIZE  = 0x1000000U;
static const unsigned  RAFT_PING_DELAY       = 50;
static const unsigned  RAFT_MIN_ELECTION     = 200;
static const unsigned  RAFT_MAX_ELECTION     = 500;
static const unsigned  RAFT_MAX_LOG_LENGTH   = 100;

/* Local variables */

/* raft_peers[0] is always "self" so is sometimes excluded
 * from iteration */
static _Atomic int raft_active_peers      = 0;
static int global_raft_fd                 = -1;
static struct raft_host_entry *raft_peers = NULL;
static unsigned raft_num_peers            = 1;
static pthread_t raft_thread_id;
static struct raft_client_state raft_client_state;

/* External variables */
extern uint8_t opt_raft_id;
extern bool opt_raft;
extern in_port_t opt_raft_port;
extern struct in_addr opt_raft_listen;
extern bool opt_database;
extern struct in_addr opt_listen;
extern in_port_t opt_port;
extern pthread_rwlock_t global_topics_lock;
extern _Atomic bool running;

/* External decl */
void close_socket(int *fd);
void sock_linger(int fd);
void sock_keepalive(int fd);
void sock_nodelay(int fd);
void sock_reuse(int fd, int reuse);
void sock_nonblock(int fd);
void logger(int priority, const struct client *client, const char *format, ...);
struct topic *find_topic(const uint8_t *name, bool active_only, bool need_lock);
struct message *find_message_by_uuid(const uint8_t uuid[static const UUID_SIZE]);
int attempt_save_all_topics(void);
int save_topic(const struct topic *topic);
struct topic *register_topic(const uint8_t *name,
        const uint8_t uuid[const UUID_SIZE]
#ifdef FEATURE_RAFT
        , bool source_self
#endif
        );

/* Forward decl */
static struct raft_log *raft_alloc_log(raft_conn_t, raft_log_t);
static int raft_free_log(struct raft_log *);
static int raft_append_log(struct raft_log *, struct raft_log **, struct raft_log **, _Atomic long *);
static int raft_commit_and_advance(void);
static int raft_remove_iobuf(struct io_buf *, struct io_buf **, struct io_buf **, unsigned *);

static long rnd(int from, int to)
{
    return random() % (to - from + 1) + from;
}

static int64_t timems(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
        return -1;

    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

enum {
    RSS_CURRENT_TERM = 0,
    RSS_VOTED_FOR,
    RSS_SIZE,
};

enum {
    RSS_INDEX = 0,
    RSS_TERM,
    RSS_EVENT,
    RSS_EVENT_BUF_LEN,
    RSS_HDR_SIZE
};

static int raft_save_state_vars(void)
{
    int state_fd = -1;

    const char *fn = raft_state.fn_vars_new;
    const uint32_t save_current_term = raft_state.current_term;
    const uint32_t save_voted_for    = raft_state.voted_for;

    const uint32_t save_block[RSS_SIZE] = {
        save_current_term,
        save_voted_for,
    };

    if (raft_state.self_id == 0) {
        errno = EALREADY;
        return -1;
    }

    if ((state_fd = open(fn, O_WRONLY|O_CREAT|O_TRUNC, 0600)) == -1) {
        warn("raft_save_state: open(%s)", fn);
        goto fail;
    }

    for (unsigned idx = 0; idx < RSS_SIZE; idx++)
        if (write(state_fd, &save_block[idx], sizeof(uint32_t)) == -1) {
            warn("raft_save_state: write(%s)", fn);
            goto fail;
        }

    fdatasync(state_fd);
    close(state_fd);
    state_fd = -1;

    if (rename(raft_state.fn_vars_new, raft_state.fn_vars) == -1)
        goto fail;

    return 0;

fail:
    if (state_fd != -1)
        close(state_fd);

    unlink(fn);
    rdbg_printf("RAFT raft_save_state_meta: FAIL\n");
    return -1;
}

static int raft_save_state_log(void)
{
    int log_fd = -1;
    uint8_t *event_buf = NULL;
    const char *fn = raft_state.fn_log;

    /*rdbg_printf("RAFT raft_save_state_log: last_saved_index=%d\n",
            raft_state.last_saved_index);*/

    if ((log_fd = open(fn, O_WRONLY|O_CREAT|O_APPEND, 0600)) == -1) {
        warn("raft_save_state_log: open(%s)", fn);
        goto fail;
    }

    for (struct raft_log *p = raft_state.log_head; p; p = p->next)
    {
        if (p->index <= raft_state.last_saved_index) /* skip already saved logs */
            continue;

        if (p->event > raft_impl->num_log_types)
            abort();

        int event_buf_len;

        if ((event_buf_len = raft_impl->handlers[p->event].save_log(p, &event_buf)) == -1) {
            warn("raft_save_state_log: save_log");
            goto fail;
        }

        if (event_buf_len < 0 || (size_t)event_buf_len > USHRT_MAX) {
            warnx("raft_save_state_log: event_buf_len is %d", event_buf_len);
            errno = EOVERFLOW;
            goto fail;
        }

        const uint32_t save_hdr[RSS_HDR_SIZE] = {
            p->index,
            p->term,
            p->event,
            event_buf_len,
            };

        /*rdbg_printf("RAFT savr_hdr[%u,%u,%u,%u]\n",
                save_hdr[0],
                save_hdr[1],
                save_hdr[2],
                save_hdr[3]
                )*/

        for (unsigned idx = 0; idx < RSS_HDR_SIZE; idx++) {
            if (write(log_fd, &save_hdr[idx], sizeof(uint32_t)) != sizeof(uint32_t)) {
                warn("raft_save_state_log: write(hdr,%s)", fn);
                goto fail;
            }
        }

        if (event_buf) {
            if (write(log_fd, event_buf, event_buf_len) != event_buf_len) {
                warn("raft_save_state: write(event_buf,%s)", fn);
                goto fail;
            }
            free(event_buf);
            event_buf = NULL;
        }

        raft_state.last_saved_index = p->index;

#ifdef FEATURE_RAFT_DEBUG
#if 0
        rdbg_printf("RAFT raft_save_state: [t:%02u-i:%02u/s:%02u vs ci:%02u|la:%02x]: %s:",
                p->term, p->index, p->index, raft_state.commit_index,
                raft_state.last_applied, raft_log_str[p->event]);
        switch(p->event)
        {
            case RAFT_LOG_REGISTER_TOPIC:
                rdbg_cprintf("<%s> ", p->register_topic.name);
                if ( (p->register_topic.flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED) )
                    rdbg_cprintf("retained ");
                break;

            default:
                break;
        }
        rdbg_cprintf("\n");
#endif
#endif
    }

    fdatasync(log_fd);
    close(log_fd);
    
    return 0;

fail:
    rdbg_printf("RAFT raft_save_state_log: fail on %s\n", fn);
    if (event_buf)
        free(event_buf);
    if (log_fd != -1)
        close(log_fd);

    return -1;
}

static int raft_load_state_vars(void)
{
    const char *fn = raft_state.fn_vars;
    int state_fd = -1;

    //rdbg_printf("RAFT raft_load_state_vars: start\n");

    if (raft_state.self_id == 0) {
        errno = EALREADY;
        return -1;
    }

    if ((state_fd = open(fn, O_RDONLY)) == -1) {
        warn("raft_load_state_vars: open(%s)", fn);
        if (errno == ENOENT)
            return 0;

        goto fail;
    }

    uint32_t read_block[RSS_SIZE];

    for (unsigned idx = 0; idx < RSS_SIZE; idx++)
        if (read(state_fd, &read_block[idx], sizeof(uint32_t)) == -1) {
            warn("raft_load_state_vars: read(%s)", fn);
            goto fail;
        }

    close(state_fd);

    raft_state.current_term = read_block[RSS_CURRENT_TERM];
    raft_state.voted_for = read_block[RSS_VOTED_FOR];
    return 0;

fail:
    if (state_fd != -1)
        close(state_fd);
    return -1;
}

static int raft_load_state_logs(void)
{
    int log_fd = -1;
    uint8_t *event_buf = NULL;
    struct raft_log *tmp_log = NULL;
    int rc;
    unsigned logs_read = 0;
    const char *fn = raft_state.fn_log;

    if ((log_fd = open(fn, O_RDONLY)) == -1) {
        warn("raft_load_state_logs: open(%s)", fn);
        if (errno != ENOENT)
            goto fail;
        return -1;
    }

    raft_state.log_index = 0;

    while (1)
    {
        uint32_t read_hdr[RSS_HDR_SIZE];

        for (unsigned idx = 0; idx < RSS_HDR_SIZE; idx++) {
            if ((rc = read(log_fd, &read_hdr[idx], sizeof(uint32_t))) != sizeof(uint32_t)) {
                /* EOF on the first read means we've reached the end of the logs,
                 * otherwise it's an error */
                if (rc == 0 && idx == 0)
                    goto logs_done;
                warn("raft_load_state: read(log)");
                goto fail;
            }
        }

        /*rdbg_printf("RAFT raft_load_state_logs: [%u,%u,%u,%u]\n",
                read_hdr[0],
                read_hdr[1],
                read_hdr[2],
                read_hdr[3]
                );*/

        if ((tmp_log = raft_alloc_log(RAFT_PEER, read_hdr[RSS_EVENT])) == NULL)
            goto fail;

        tmp_log->index = read_hdr[RSS_INDEX];
        tmp_log->term = read_hdr[RSS_TERM];

        if (read_hdr[RSS_EVENT_BUF_LEN]) {
            if ((event_buf = malloc(read_hdr[RSS_EVENT_BUF_LEN])) == NULL)
                goto fail;

            if ((size_t)(rc = read(log_fd, event_buf, read_hdr[RSS_EVENT_BUF_LEN])) != read_hdr[RSS_EVENT_BUF_LEN]) {
                if (rc == 0) {
                    errno = EPIPE;
                } else if (rc > 0) {
                    warnx("raft_load_state_logs: short read on log %d: %u/%u",
                            logs_read, rc, read_hdr[RSS_EVENT_BUF_LEN]);
                }
                goto fail;
            }

            if (raft_impl->handlers[read_hdr[RSS_EVENT]].read_log(tmp_log,
                        event_buf, read_hdr[RSS_EVENT_BUF_LEN]) == -1) {
                warn("raft_load_state: read_log");
                goto fail;
            }

            free(event_buf);
            event_buf = NULL;
        }

        if (raft_append_log(tmp_log, &raft_state.log_head,
                    &raft_state.log_tail, &raft_state.log_length) == -1) {
            warn("raft_load_state: raft_append_log");
            goto fail;
        }

        if (raft_commit_and_advance() == -1) {
            warn("raft_load_state: raft_commit_and_advance");
            goto fail;
        }

        logs_read++;
        if (tmp_log->index > raft_state.last_saved_index)
            raft_state.last_saved_index = tmp_log->index;
    }
logs_done:
    close(log_fd);

    if (logs_read) {
        assert(raft_state.log_tail);
        rdbg_printf("RAFT raft_load_state: tail=[%d/%d]\n",
                raft_state.log_tail->term,
                raft_state.log_tail->index);
        raft_state.log_index = raft_state.log_tail->index;
    }

    return 0;

fail:
    rdbg_printf("RAFT raft_load_state_logs: failed on %s\n", fn);
    if (log_fd != -1)
        close(log_fd);
    if (event_buf)
        free(event_buf);
    if (tmp_log)
        raft_free_log(tmp_log);
    return -1;
}

[[gnu::nonnull]]
int raft_parse_cmdline_host_list(const char *tmp, int extra)
{
    char *ptr, *ptr2;
    char *save = NULL, *save2 = NULL;
    char *arg = NULL;
    char *single = NULL;
    unsigned cnt = extra;
    struct raft_host_entry *entry = NULL;
    void *tmp_entry = NULL;
    struct raft_host_entry **list = &raft_peers;

    errno = EINVAL;

    if ((arg = strdup(tmp)) == NULL)
        goto fail;

    ptr = strtok_r(arg, "/", &save);

    struct in_addr addr;
    in_port_t port = 0;
    uint8_t id;

    if ((tmp_entry = calloc(extra, sizeof(struct raft_host_entry))) == NULL)
        goto fail;
    entry = tmp_entry;
    tmp_entry = NULL;

    while (ptr)
    {
        if ((single = strdup(ptr)) == NULL)
            goto fail;

        save2 = NULL;

        /* id: */
        if ((ptr2 = strtok_r(single, ":", &save2)) == NULL)
            goto fail;

        id = atoi(ptr2);
        if (id <= 0 || id >= 0xff)
            goto fail;

        /* host: */
        if ((ptr2 = strtok_r(NULL, ":", &save2)) == NULL)
            goto fail;

        if (inet_pton(AF_INET, ptr2, &addr) != 1)
            goto fail;

        /* port */
        if ((ptr2 = strtok_r(NULL, ":", &save2)) == NULL)
            goto fail;

        port = atoi(ptr2);
        if (port <= 0 || port >= USHRT_MAX)
            goto fail;
        port = htons(port);

        free(single);
        single = NULL;

        if (addr.s_addr == INADDR_ANY)
            goto fail;

        if ((tmp_entry = realloc(entry, sizeof(struct raft_host_entry) * (cnt + 1))) == NULL)
            goto fail;
        entry = tmp_entry;
        tmp_entry = NULL;

        memset(&entry[cnt], 0, sizeof(struct raft_host_entry));
        entry[cnt].port = port;
        entry[cnt].address = addr;
        entry[cnt].server_id = id;
        entry[cnt].peer_fd = -1;

        cnt++;
        ptr = strtok_r(NULL, "/", &save);
    }

    free(arg);
    arg = NULL;

    if ((tmp_entry = realloc(entry, sizeof(struct raft_host_entry) * (cnt + 1))) == NULL)
        goto fail;
    entry = tmp_entry;
    tmp_entry = NULL;

    memset(&entry[cnt], 0, sizeof(struct raft_host_entry));

    *list = entry;

    errno = 0;

    raft_num_peers = cnt;
    return 0;

fail:
    if (single)
        free(single);
    if (arg)
        free(arg);
    if (tmp_entry)
        free(tmp_entry);
    if (entry)
        free (entry);

    return -1;
}

static void free_iobuf(struct io_buf *iobuf)
{
    if (iobuf == NULL)
        return;

    if (iobuf->buf)
        free((void *)iobuf->buf);

    free(iobuf);
}

static void raft_reset_read_state(struct raft_host_entry *client)
{
    client->rd_state = RAFT_PCK_NEW;
    client->rd_offset = 0;
    client->rd_need = 0;
    if (client->rd_packet_buffer) {
        free(client->rd_packet_buffer);
        client->rd_packet_buffer = NULL;
    }
    client->rd_packet_length = 0;
}

static inline bool raft_has_pending_write(struct raft_host_entry *client)
{
    bool ret;
    pthread_rwlock_rdlock(&client->wr_lock);
    ret = (client->wr_active || client->wr_head);
    pthread_rwlock_unlock(&client->wr_lock);
    return ret;
}

[[gnu::nonnull]]
static int raft_clear_active_write(struct raft_host_entry *client)
{
    client->wr_need          = 0;
    client->wr_packet_buffer = NULL;
    client->wr_packet_length = 0;
    client->wr_offset        = 0;
    client->wr_active        = NULL;

    return 0;
}

[[gnu::nonnull]]
static int raft_reset_write_state(struct raft_host_entry *client, bool need_lock)
{
    struct io_buf *tmp, *next;

    if (need_lock)
        pthread_rwlock_wrlock(&client->wr_lock);

    free_iobuf(client->wr_active);
    raft_clear_active_write(client);

    for (tmp = client->wr_head; tmp; tmp = next)
    {
        next = tmp->next;
        free_iobuf(tmp);
    }

    client->wr_head = NULL;
    client->wr_tail = NULL;
    client->wr_active = NULL;
    client->wr_queue = 0;

    if (need_lock)
        pthread_rwlock_unlock(&client->wr_lock);

    return 0;
}

static int raft_reset_ss_state(struct raft_host_entry *client, bool need_lock)
{
    if (need_lock)
        pthread_rwlock_wrlock(&client->ss_lock);

    if (client->ss_data) {
        free((void *)client->ss_data);
        client->ss_data = NULL;
    }

    client->ss_last_index = 0;
    client->ss_last_term = 0;
    client->ss_need = 0;
    client->ss_offset = 0;
    client->ss_tried_offset = 0;
    client->ss_tried_length = 0;
    client->ss_tried_status = RAFT_ERR;

    if (need_lock)
        pthread_rwlock_unlock(&client->ss_lock);

    return 0;
}

static int raft_update_leader_id(uint32_t leader_id, bool need_lock)
{

    if (leader_id == NULL_ID) {
        if (need_lock) pthread_rwlock_wrlock(&raft_client_state.lock);
        raft_client_state.current_leader_id = NULL_ID;
        raft_client_state.current_leader = NULL;
        if (need_lock) pthread_rwlock_unlock(&raft_client_state.lock);
        return 0;
    }

    for (unsigned idx = 0; idx < raft_num_peers; idx++)
        if (raft_peers[idx].server_id == leader_id) {
            if (need_lock) pthread_rwlock_wrlock(&raft_client_state.lock);
            raft_client_state.current_leader_id = leader_id;
            raft_client_state.current_leader = &raft_peers[idx];
            if (need_lock) pthread_rwlock_unlock(&raft_client_state.lock);
            return 0;
        }

    rdbg_printf("RAFT raft_update_leader_id: no such leader %d\n", leader_id);
    errno = ENOENT;
    return -1;
}

static int raft_update_term(uint32_t new_term)
{
    if (new_term <= raft_state.current_term)
        return 0;

    raft_state.current_term = new_term;
    raft_state.voted_for = NULL_ID;

    return raft_save_state_vars();
}

static int raft_update_voted_for(uint32_t new_votee)
{
    raft_state.voted_for = new_votee;

    return raft_save_state_vars();
}

static struct raft_log *raft_log_at(uint32_t index)
{
    errno = 0;

    if (index == 0)
        return NULL;

    if (index == -1U) {
        errno = EINVAL;
        return NULL;
    }

    for (struct raft_log *tmp = raft_state.log_head; tmp; tmp = tmp->next)
        if (tmp->index == index)
            return tmp;

    errno = ENOENT;
    return NULL;
}

static uint32_t raft_term_at(uint32_t index)
{
    if (index == 0)
        return 0;

    if (index == -1U) {
        errno = EINVAL;
        return -1;
    }

    const struct raft_log *log_entry;

    if ((log_entry = raft_log_at(index)) != NULL)
        return log_entry->term;

    errno = ENOENT;
    return -1U;
}

[[gnu::nonnull]]
static int raft_close(struct raft_host_entry *client)
{
    rdbg_printf("RAFT raft_close: id=%u,fd=%u\n", client->server_id, client->peer_fd);

    /* this avoids double close, as close_socket sets to -1 */
    if (client->peer_fd == -1)
        return 0;

    close_socket(&client->peer_fd);
    client->next_conn_attempt = timems() + rnd(RAFT_MIN_ELECTION * 2, RAFT_MAX_ELECTION * 4);

    const uint32_t last = raft_state.log_tail ? raft_state.log_tail->index : 0;

    if (client->server_id != raft_state.self_id) {
        raft_active_peers--;
        client->match_index = 0;
        client->next_index = last + 1;
    }

    raft_reset_read_state(client);
    raft_reset_write_state(client, true);
    raft_reset_ss_state(client, true);

    return 0;
}

[[gnu::warn_unused_result, gnu::malloc]]
static struct raft_log *raft_alloc_log(raft_conn_t conn_type, raft_log_t event)
{
    struct raft_log *ret;
    bool mutex_init = false, cond_init = false, condattr_init = false;
    pthread_condattr_t attr;

    assert(conn_type < RAFT_MAX_CONN);
    assert(event < RAFT_MAX_LOG);

    if ((ret = calloc(1, sizeof(struct raft_log))) == NULL)
        return NULL;

    if (conn_type == RAFT_CLIENT) {
        if (pthread_condattr_init(&attr) != 0)
            goto fail;
        else
            condattr_init = true;

        if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0)
            goto fail;

        if (pthread_mutex_init(&ret->mutex, NULL) != 0)
            goto fail;
        else
            mutex_init = true;

        if (pthread_cond_init(&ret->cv, &attr) != 0)
            goto fail;
        else
            cond_init = true;

        pthread_condattr_destroy(&attr);
        condattr_init = false;
    }

    ret->done = false;
    ret->role = conn_type;
    ret->event = event;

    return ret;

fail:
    if (ret) {
        if (conn_type == RAFT_CLIENT) {
            if (cond_init)
                pthread_cond_destroy(&ret->cv);
            if (mutex_init)
                pthread_mutex_destroy(&ret->mutex);
            if (condattr_init)
                pthread_condattr_destroy(&attr);
        }

        free(ret);
    }

    return NULL;
}

[[gnu::nonnull]]
static int raft_free_log(struct raft_log *entry)
{
    int rc = 0;

    if (entry->event >= raft_impl->num_log_types) {
        errno = EINVAL;
        rc = -1;
        warnx("raft_free_log: attempt to free unknown event type %u", entry->event);
    } else {
        const struct raft_impl_entry *ent = &raft_impl->handlers[entry->event];
        if (ent->free_log)
            ent->free_log(entry);
    }

    if (entry->role == RAFT_CLIENT) {
        pthread_mutex_destroy(&entry->mutex);
        pthread_cond_destroy(&entry->cv);
    }

    free(entry);
    return rc;
}

[[gnu::nonnull(1,2)]]
static int raft_remove_iobuf(struct io_buf *entry, struct io_buf **head, struct io_buf **tail, unsigned *len)
{
    struct io_buf *prev = NULL;

    if (*head == entry) {
        *head = entry->next;
        goto found_head;
    }

    for (prev = *head; prev; prev = prev->next)
        if (prev->next == entry)
            goto found;

    errno = ENOENT;
    return -1;

found:
    prev->next = entry->next;

found_head:
    if (tail && *tail == entry)
        *tail = prev;

    entry->next = NULL;

    (*len)--;

    return 0;
}

[[gnu::nonnull(1,2)]]
static int raft_remove_log(struct raft_log *entry, struct raft_log **head,
        struct raft_log **tail, pthread_rwlock_t *lock, _Atomic long *log_len)
{
    struct raft_log *prev = NULL;

    if (lock)
        pthread_rwlock_wrlock(lock);

    if (*head == entry) {
        *head = entry->next;
        goto found_head;
    }

    for (prev = *head; prev; prev = prev->next)
        if (prev->next == entry)
            goto found;

    if (lock)
        pthread_rwlock_unlock(lock);
    errno = ENOENT;
    return -1;

found:
    prev->next = entry->next;

found_head:
    if (tail && *tail == entry)
        *tail = prev;

    entry->next = NULL;

    if (log_len)
        (*log_len)--;

    if (lock)
        pthread_rwlock_unlock(lock);
    return 0;
}

[[gnu::nonnull]]
static int raft_append_log(struct raft_log *entry, struct raft_log **head,
        struct raft_log **tail, _Atomic long *log_len)
{
    if (*head == NULL)
        *head = entry;
    if (*tail != NULL)
        (*tail)->next = entry;
    *tail = entry;

    (*log_len)++;

    return 0;
}

[[gnu::nonnull]]
static int raft_append_iobuf(struct io_buf *entry, struct io_buf **head,
        struct io_buf **tail, unsigned *iobuf_len)
{
    if (*head == NULL)
        *head = entry;
    if (*tail != NULL)
        (*tail)->next = entry;
    *tail = entry;

    (*iobuf_len)++;

    return 0;
}

[[maybe_unused, gnu::nonnull]]
static int raft_prepend_log(struct raft_log *entry, struct raft_log **head,
        struct raft_log **tail, _Atomic long *log_len)
{
    if (*tail == NULL)
        *tail = entry;

    entry->next = *head;
    *head = entry;

    (*log_len)++;

    return 0;
}

static int raft_commit_and_advance(void)
{
    struct raft_log *log_entry;

    if ((log_entry = raft_log_at(raft_state.commit_index + 1)) == NULL) {
        errno = ERANGE;
        return -1;
    }

    rdbg_printf(BWHT "RAFT raft_commit_and_advance: commiting: idx=%u/%u" CRESET "\n",
            log_entry->index,
            log_entry->term
            );

    if (log_entry->event >= raft_impl->num_log_types) {
        errno = EINVAL;
        warnx("raft_commit_and_advance: unknown log entry %u@%u/%u",
                log_entry->event,
                log_entry->index, log_entry->term);
        return -1;
    }

    const struct raft_impl_entry *ent = &raft_impl->handlers[log_entry->event];
    if (ent->commit_and_advance)
        if (ent->commit_and_advance(log_entry) == -1)
            goto fail;

    raft_state.commit_index++;
    return 0;

fail:
    return -1;
}

static int raft_check_commit_index(uint32_t best)
{
    if (best <= raft_state.commit_index)
        return 0;

    const unsigned majority = (raft_num_peers/2) + 1;

    for (uint32_t current_best = best; current_best > raft_state.commit_index; current_best--)
    {
        const uint32_t term = raft_term_at(current_best);

        /* Only commit entries from currentTerm */

        if (term == -1U)
            continue;
        if (term != raft_state.current_term)
            continue;

        unsigned cnt = 1;

        for (unsigned idx = 1; idx < raft_num_peers; idx++)
            if (raft_peers[idx].match_index >= current_best)
                cnt++;

        if (cnt >= majority) {
            while (raft_state.commit_index < current_best)
                if (raft_commit_and_advance() == -1)
                    break;
            rdbg_printf("RAFT raft_check_commit_index: bumped commit_index to %u (goal %u)\n",
                    raft_state.commit_index, current_best);
            return current_best;
        }
    }

    return 0;
}

static int raft_leader_log_appendv(raft_log_t event, struct raft_log *log_p, va_list ap)
{
    struct raft_log *new_log = log_p;

    if (event >= RAFT_MAX_LOG) {
        errno = EINVAL;
        return -1;
    }

    rdbg_printf("RAFT raft_leader_log_appendv: %s\n",
            raft_log_str[event]);

    if (log_p == NULL)
        if ((new_log = raft_alloc_log(RAFT_PEER, event)) == NULL)
            goto fail;

    new_log->term = raft_state.current_term;
    new_log->index = ++raft_state.log_index;

    if (log_p == NULL) {
        if (event >= raft_impl->num_log_types) {
            errno = EINVAL;
            goto fail;
        }

        const struct raft_impl_entry *ent = &raft_impl->handlers[event];

        if (ent->leader_append)
            if (ent->leader_append(new_log, ap) == -1)
                goto fail;

    }

    if (raft_append_log(new_log, &raft_state.log_head, &raft_state.log_tail, &raft_state.log_length) == -1)
        goto fail;

    raft_save_state_log();

    for (unsigned idx = 1; idx < raft_num_peers; idx++)
    {
        if (raft_peers[idx].peer_fd == -1)
            continue;

        const uint32_t prev_index = raft_peers[idx].next_index - 1;
        const uint32_t prev_term = raft_term_at(prev_index);

        if (prev_term == -1U) {
            warnx("raft_leader_log_appendv: can't find term for index %d", prev_index);
            continue;
        }

        if (raft_send(RAFT_PEER, &raft_peers[idx], RAFT_APPEND_ENTRIES,
                    raft_state.current_term, raft_state.self_id,
                    prev_index,
                    prev_term,
                    raft_state.commit_index,
                    1,
                    new_log) == -1) {
            warn("raft_leader_log_appendv: raft_send(id=%u)", raft_peers[idx].server_id);
        }
    }
    return 0;

fail:
    if (new_log)
        raft_free_log(new_log);

    return -1;
}

int raft_leader_log_append(raft_log_t event, ...)
{
    int rc;
    va_list ap;
    va_start(ap, event);
    rc = raft_leader_log_appendv(event, NULL, ap);
    va_end(ap);
    return rc;
}

/**
 * caller must have pending_event->mutex
 */
static int raft_await_client_response(struct raft_log *pending_event)
{
    int rc;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec += 5;


    while (!pending_event->done)
    {
        if ((rc = pthread_cond_timedwait(&pending_event->cv,
                        &pending_event->mutex, &ts)) != 0) {
            errno = rc;
            return -1;
        }
    }

    return 0;
}

/* sends a log event (as a client) to the leader to process */
static int raft_client_log_sendv(raft_log_t event, va_list ap)
{
    int rc = -1;
    struct raft_log *new_client_event = NULL;
    bool inserted = false;

    if (event >= RAFT_MAX_LOG) {
        errno = EINVAL;
        goto fail;
    }

    rdbg_printf("RAFT raft_client_log_sendv: %s\n",
            raft_log_str[event]);

    if ((new_client_event = raft_alloc_log(RAFT_CLIENT, event)) == NULL)
        goto fail;

    /* sequence_num is _Atomic */
    new_client_event->sequence_num = ++raft_client_state.sequence_num;

    if ( ((rc = pthread_mutex_trylock(&new_client_event->mutex)) != 0) ) {
        warnx("raft_client_log_sendv: pthread_init: %s", strerror(rc));
        errno = rc;
        goto fail;
    }

    pthread_rwlock_wrlock(&raft_client_state.log_pending_lock);
    //for (prev = raft_client_state.log_pending; prev && prev->next; prev = prev->next) {}

    if (raft_client_state.log_pending_tail == NULL) {
        raft_client_state.log_pending_tail = new_client_event;
        raft_client_state.log_pending_head = new_client_event;
    } else {
        raft_client_state.log_pending_tail->next = new_client_event;
        raft_client_state.log_pending_tail = new_client_event;
    }

    inserted = true;
    pthread_rwlock_unlock(&raft_client_state.log_pending_lock);

    if (event >= raft_impl->num_log_types) {
        errno = EINVAL;
        goto fail;
    }

    const struct raft_impl_entry *ent = &raft_impl->handlers[event];

    if (ent->client_append)
        if (ent->client_append(new_client_event, event, ap) == -1)
            goto fail;

    if ((rc = raft_send(RAFT_CLIENT, NULL,
                    RAFT_CLIENT_REQUEST,
                    raft_state.self_id, new_client_event->sequence_num,
                    event,
                    new_client_event
                    )) == -1) {
        warn("raft_client_log_sendv: raft_send");
    }

    if (rc == -1) {
        pthread_mutex_unlock(&new_client_event->mutex);
        goto fail;
    }

    if ((rc = raft_await_client_response(new_client_event)) == -1) {
        pthread_mutex_unlock(&new_client_event->mutex);
        warn("raft_client_log_sendv: raft_await_client_response");
        goto fail;
    }

    /* success */

    raft_remove_log(new_client_event, &raft_client_state.log_pending_head,
            &raft_client_state.log_pending_tail,
            &raft_client_state.log_pending_lock, NULL);
    pthread_mutex_unlock(&new_client_event->mutex);
    raft_free_log(new_client_event);
    return 0;

fail:
    if (new_client_event) {
        if (inserted)
            raft_remove_log(new_client_event, &raft_client_state.log_pending_head,
                    &raft_client_state.log_pending_tail,
                    &raft_client_state.log_pending_lock, NULL);
        raft_free_log(new_client_event);
    }

    return -1;
}

int raft_client_log_send(raft_log_t event, ...)
{
    int rc;
    va_list ap;

    //rdbg_printf("RAFT %lu vs %lu\n", pthread_self(), raft_thread_id);

    va_start(ap, event);

    /* Just checking that pthread_self() == raft_thread_id won't work,
     * as there is no easy solution for raft_client_log_sendv() to send
     * to "self" */

    pthread_mutex_lock(&raft_state.lock);
    if (raft_state.state == RAFT_STATE_LEADER) {
        /* we're in raft_thread so need to preserve our lock of raft_state,
         * so the main thread doesn't touch it */
        rc = raft_leader_log_appendv(event, NULL, ap);
        pthread_mutex_unlock(&raft_state.lock);
    } else {
        pthread_mutex_unlock(&raft_state.lock);
        /* we're not in raft_thread, so will not touch raft_state */
        rc = raft_client_log_sendv(event, ap);
    }
    va_end(ap);
    return rc;
}

/* appends a log (from the leader) to the local log list */
[[gnu::nonnull]]
static int raft_client_log_append_single(struct raft_log *raft_log, uint32_t leader_commit)
{

    for (struct raft_log *prev = NULL, *tmp = raft_state.log_head; tmp; tmp = tmp->next)
    {
        if (tmp->index == raft_log->index && tmp->term == raft_log->term) {
            /* 4. Append any new entries *not* already in the log */
            return 0; /* TODO should this be -1 ? */
        }

        if (tmp->index != raft_log->index) {
            prev = tmp;
            continue;
        }

        /* else same index, different term ... */

        /* If an existing entry conflicts (same index, different term)
         * delete the existing entry, and all that follow it
         */

        /* detach at the conflict point */
        if (prev) {
            prev->next = NULL;
            raft_state.log_tail = prev;
        } else {
            raft_state.log_head = NULL;
            raft_state.log_tail = NULL;
        }

        struct raft_log *next = NULL;

        /* free the conflicting entries */
        while(tmp)
        {
            next = tmp->next;
            raft_state.log_length--;
            //raft_remove_log(tmp, &raft_state.log_head, &raft_state.log_tail, NULL);
            raft_free_log(tmp);
            tmp = next;
        }

        //if (prev)
        //    prev->next = NULL;

        /* rollback to the last good point (incomplete state machine undo?) */
        if (raft_state.log_tail) {
            if (raft_state.commit_index > raft_state.log_tail->index)
                raft_state.commit_index = raft_state.log_tail->index;

            if (raft_state.last_applied > raft_state.log_tail->index) {
                warnx("raft_client_log_append_single: need to reverse last_applied!");
                raft_state.last_applied = raft_state.log_tail->index;
            }
        }

        break;
    }

    raft_append_log(raft_log, &raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);
    if (raft_state.log_tail)
        raft_state.log_index = raft_state.log_tail->index;
    raft_save_state_log();

    /* 5. If leaderCommit > commitIndex, set commitIndex =
     * min(leaderCommit, index of last new entry */
    if (leader_commit > raft_state.commit_index) {
        const uint32_t desired_commit_index = MIN(leader_commit, raft_log->index);
        while (raft_state.commit_index < desired_commit_index)
            if (raft_commit_and_advance() == -1)
                break;
        //raft_state.commit_index = MIN(leader_commit, raft_log->index);
    }

    return 1;
}

[[gnu::nonnull]]
static void raft_remove_and_free_unknown_host(struct raft_host_entry *entry)
{
    if (entry->peer_fd != -1)
        close_socket(&entry->peer_fd);

    struct raft_host_entry **ptr = &raft_client_state.unknown_clients;

    while (*ptr && *ptr != entry)
        ptr = &(*ptr)->unknown_next;

    if (*ptr != entry)
        return;

    *ptr = entry->unknown_next;

    entry->unknown_next = NULL;
    raft_reset_read_state(entry);
    raft_reset_write_state(entry, false);
    raft_reset_ss_state(entry, false);
    pthread_rwlock_destroy(&entry->wr_lock);
    pthread_rwlock_destroy(&entry->ss_lock);
    free(entry);
}

static int raft_new_conn(int new_fd, struct raft_host_entry *unknown_host,
        [[maybe_unused]] const struct sockaddr_in *sin, socklen_t /*sin_len*/)
{
    const uint8_t *ptr = NULL;
    raft_conn_t type;
    ssize_t rc;
    struct raft_host_entry *tmp_host = NULL;
    struct raft_packet packet;
    uint16_t mqtt_port;
    uint32_t id, mqtt_addr;
    unsigned idx;

    rdbg_printf("RAFT raft_new_conn called with new_fd=%d and unknown_host=%p\n",
            new_fd, unknown_host);

    errno = 0;
    assert( (unknown_host == NULL && new_fd != -1) || (unknown_host != NULL && new_fd == -1) );

    if (unknown_host != NULL) {
        tmp_host = unknown_host;
        new_fd = tmp_host->peer_fd;
        goto has_host_entry;
    }

    rdbg_printf("RAFT raft_new_conn: new on fd %u\n", new_fd);

    sock_nonblock(new_fd);
    sock_linger(new_fd);
    sock_nodelay(new_fd);
    sock_keepalive(new_fd);

    if ((tmp_host = calloc(1, sizeof(struct raft_host_entry))) == NULL)
        goto fail;

    if (pthread_rwlock_init(&tmp_host->wr_lock, NULL) != 0) {
pre_fail:
        free(tmp_host);
        close_socket(&new_fd);
        return -1;
    }

    if (pthread_rwlock_init(&tmp_host->ss_lock, NULL) != 0) {
        pthread_rwlock_destroy(&tmp_host->wr_lock);
        goto pre_fail;
    }

    tmp_host->peer_fd = new_fd;
    raft_reset_read_state(tmp_host);
    raft_reset_write_state(tmp_host, true);
    raft_reset_ss_state(tmp_host, true);
    tmp_host->rd_state = RAFT_PCK_HELLO;
    tmp_host->rd_need = RAFT_HELLO_SIZE + RAFT_HDR_SIZE;

    tmp_host->unknown_next = raft_client_state.unknown_clients;
    raft_client_state.unknown_clients = tmp_host;

has_host_entry:
    if ((rc = read(new_fd, tmp_host->rd_packet + tmp_host->rd_offset,
                    tmp_host->rd_need)) != tmp_host->rd_need) {
        if (rc == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                return -1;
            warn("raft_new_conn: read");
            goto fail;
        } else if (rc == 0) {
            goto fail;
        }
        errno = EAGAIN;

        tmp_host->rd_offset += rc;
        tmp_host->rd_need -= rc;

        return -1;
    }

    /* we have read enough */
    tmp_host->rd_state = RAFT_PCK_EMPTY;

    ptr = tmp_host->rd_packet;

    packet.rpc = *ptr++;
    packet.flags = *ptr++;
    packet.role = *ptr++;
    packet.res0 = *ptr++;

    if (packet.rpc != RAFT_HELLO) {
        warnx("raft_new_conn: not RAFT_HELLO");
        errno = EINVAL;
        goto fail;
    }

    memcpy(&packet.length, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    packet.length = ntohl(packet.length);
    packet.length -= RAFT_HDR_SIZE;

    if (packet.length != RAFT_HELLO_SIZE) {
        warnx("raft_new_conn: pck_len is %u not %u", packet.length, RAFT_HELLO_SIZE);
        errno = EFBIG;
        goto fail;
    }

    memcpy(&id, ptr, sizeof(id));
    id = ntohl(id);
    ptr += sizeof(id);

    type = *ptr++;

    memcpy(&mqtt_addr, ptr, sizeof(mqtt_addr));
    ptr += sizeof(mqtt_addr);

    memcpy(&mqtt_port, ptr, sizeof(mqtt_port));
    ptr += sizeof(mqtt_port);

    if (packet.role != RAFT_CLIENT && packet.role != RAFT_PEER) {
        warnx("raft_new_conn: attempt to connect as illegal role");
        errno = EINVAL;
        goto fail;
    }

    /* We connect to ourselves, but only as a RAFT_CLIENT */
    if (id == raft_state.self_id &&
            packet.role == RAFT_PEER) {
        warnx("raft_new_conn: attempt to read from self?");
        errno = EINVAL;
        goto fail;
    }

    if (type >= RAFT_MAX_CONN) {
        errno = EINVAL;
        goto fail;
    }

    rdbg_printf("RAFT raft_new_conn: read id %u type %s (addr=%08x:%u)\n",
            id, raft_conn_str[type], ntohl(mqtt_addr), ntohs(mqtt_port));

    for (idx = 0; idx < raft_num_peers; idx++)
    {
        if ( (raft_peers[idx].port == 0) ||
                (id != raft_peers[idx].server_id) )
            continue;

        goto found_one;
    }

    errno = ENOENT;
    goto fail;

found_one:

    rdbg_printf(NGRN "RAFT raft_new_conn: new (fd=%d,idx=%d,id=%d) from %08x:%u [%s] [%s]" CRESET "\n",
            new_fd, idx, raft_peers[idx].server_id,
            sin ? ntohl(sin->sin_addr.s_addr) : 0,
            sin ? ntohs(sin->sin_port) : 0,
            raft_conn_str[type],
            raft_peers[idx].peer_fd == -1 ? "NEW" : "EXISTING");

    if (raft_peers[idx].peer_fd != -1) {
        if (raft_peers[idx].server_id != raft_state.self_id) {
            rdbg_printf("RAFT raft_new_conn: already connected on fd %u, closing\n",
                    raft_peers[idx].peer_fd);
            close_socket(&new_fd);
            if (tmp_host)
                tmp_host->peer_fd = -1;
        }
        goto finish;
    }

    raft_peers[idx].peer_fd = new_fd;
    raft_reset_read_state(&raft_peers[idx]);
    raft_reset_write_state(&raft_peers[idx], true);
    raft_reset_ss_state(&raft_peers[idx], true);
    raft_peers[idx].mqtt_addr.s_addr = mqtt_addr;
    raft_peers[idx].mqtt_port = mqtt_port;
    if (tmp_host)
        tmp_host->peer_fd = -1;

    if (idx != 0) {
        const uint32_t last_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
        raft_active_peers++;
        raft_peers[idx].match_index = 0;
        raft_peers[idx].next_index = last_index + 1;
        rdbg_printf("RAFT raft_new_conn: setting next_index to %d\n", last_index + 1);
    }

finish:
    if (tmp_host)
        raft_remove_and_free_unknown_host(tmp_host);

    rdbg_printf("RAFT raft_new_conn: returning %d\n", idx);
    return idx;

fail:
    if (tmp_host)
        close_socket(&tmp_host->peer_fd);

    idx = -1;
    goto finish;
}

static const struct {
    const size_t min_size;
    const size_t max_size;
} raft_rpc_settings[RAFT_MAX_RPC] = {
    [RAFT_HELLO]                 = { RAFT_HELLO_SIZE                 , RAFT_HELLO_SIZE                 } ,
    [RAFT_CLIENT_REQUEST]        = { RAFT_CLIENT_REQUEST_SIZE        , RAFT_MAX_PACKET_SIZE            } ,
    [RAFT_CLIENT_REQUEST_REPLY]  = { RAFT_CLIENT_REQUEST_REPLY_SIZE  , RAFT_CLIENT_REQUEST_REPLY_SIZE  } ,
    [RAFT_APPEND_ENTRIES]        = { RAFT_APPEND_ENTRIES_FIXED_SIZE  , RAFT_MAX_PACKET_SIZE            } ,
    [RAFT_APPEND_ENTRIES_REPLY]  = { RAFT_APPEND_ENTRIES_REPLY_SIZE  , RAFT_APPEND_ENTRIES_REPLY_SIZE  } ,
    [RAFT_REQUEST_VOTE]          = { RAFT_REQUEST_VOTE_SIZE          , RAFT_REQUEST_VOTE_SIZE          } ,
    [RAFT_REQUEST_VOTE_REPLY]    = { RAFT_REQUEST_VOTE_REPLY_SIZE    , RAFT_REQUEST_VOTE_REPLY_SIZE    } ,
    [RAFT_REGISTER_CLIENT]       = { RAFT_REGISTER_CLIENT_SIZE       , RAFT_REGISTER_CLIENT_SIZE       } ,
    [RAFT_REGISTER_CLIENT_REPLY] = { RAFT_REGISTER_CLIENT_REPLY_SIZE , RAFT_REGISTER_CLIENT_REPLY_SIZE } ,
};


[[gnu::nonnull]]
/**
 * Adds a buffer to the pending writes for a client.
 *
 * @param client the client to add to
 * @param[in] buffer ownership of buffer is transferred
 * @param size length of buffer
 *
 * @return -1 on error, @c errno is set
 */
static int raft_add_write(struct raft_host_entry *client, uint8_t *buffer, ssize_t size)
{
    struct io_buf *io_buf;

    if (size <= 0) {
        errno = EINVAL;
        return -1;
    }

    if (client->peer_fd == -1) {
        errno = EBADF;
        return -1;
    }

    if (client->wr_queue > 10) {
        errno = ENOSPC;
        return -1;
    }

    if ((io_buf = malloc(sizeof(struct io_buf))) == NULL)
        return -1;

    io_buf->buf = buffer;
    io_buf->next = NULL;
    io_buf->size = size;

    pthread_rwlock_wrlock(&client->wr_lock);
    raft_append_iobuf(io_buf, &client->wr_head, &client->wr_tail, &client->wr_queue);
    pthread_rwlock_unlock(&client->wr_lock);
    return 0;
}

[[gnu::nonnull]]
static int raft_try_write(struct raft_host_entry *client)
{
    ssize_t rc;
    struct io_buf *cur;

    errno = 0;

    pthread_rwlock_wrlock(&client->wr_lock);

    if ((cur = client->wr_active) == NULL) {
        cur = client->wr_head;

        if (cur == NULL)
            goto done;

        raft_remove_iobuf(cur, &client->wr_head, &client->wr_tail, &client->wr_queue);
        client->wr_active = cur;

        client->wr_need = client->wr_packet_length = cur->size;
        client->wr_packet_buffer = (void *)cur->buf;
        client->wr_offset = 0;
    }

    if ((rc = write(client->peer_fd, client->wr_packet_buffer + client->wr_offset,
                    client->wr_need)) == client->wr_need) {
        free_iobuf(client->wr_active);
        /* subsequent calls to raft_try_write will advance to the next
         * pending io_buf */
        raft_clear_active_write(client);
        goto done;
    }

    if (rc > 0) {
        client->wr_offset += rc;
        client->wr_need -= rc;
done:
        pthread_rwlock_unlock(&client->wr_lock);
        return 0;
    }

    if (rc == 0)
        errno = EPIPE;

    if (rc == -1 && (errno == EWOULDBLOCK || errno == EINTR || errno == EAGAIN)) {
        pthread_rwlock_unlock(&client->wr_lock);
        errno = EAGAIN;
        return -1;
    }

    pthread_rwlock_unlock(&client->wr_lock);
    raft_close(client);
    return -1;
}


/* -1 = all */
int raft_send(raft_conn_t mode, struct raft_host_entry *client, raft_rpc_t rpc, ...)
{
    ssize_t sendsz;
    uint8_t *packet_buffer = NULL, *ptr;
    void *tmp_packet_buffer = NULL;
    struct raft_packet packet;

    uint16_t arg_req_len = 0;
    uint32_t arg_client_id = 0, arg_leader_hint = 0;
    uint32_t arg_id = 0; //, arg_flags = 0;
    uint32_t arg_leader_id = 0, arg_prev_log_index = 0, arg_prev_log_term = 0;
    uint32_t arg_num_entries = 0, arg_new_match_index = 0, arg_leader_commit = 0;
    uint32_t arg_sequence_num = 0, arg_voted_for = NULL_ID;
    uint32_t arg_term = 0, arg_candidate_id, arg_last_log_index, arg_last_log_term;
    //const uint8_t *arg_str = NULL, *arg_uuid = NULL, *arg_msg_uuid = NULL;
    uint8_t arg_status = 0, arg_conn_type = 0, arg_req_type = 0, arg_req_flags;
    uint8_t arg_log_type = 0;

    struct raft_log *arg_entries = NULL;
    struct raft_log *arg_event = NULL;

    struct send_state send_state;
    struct send_state *send_states = NULL;

    va_list ap;

    errno = EINVAL;
    memset(&packet, 0, sizeof(packet));

    if (mode == RAFT_PEER && client != NULL && client->peer_fd == -1) {
        errno = EBADF;
        return -1;
    } else if (mode == RAFT_PEER && client == NULL && raft_active_peers == 0) {
        return 0;
    } else if (mode == RAFT_CLIENT && raft_client_state.current_leader == NULL) {
        warnx("raft_send: current_leader == NULL");
        errno = EBADF;
        return -1;
    } else if (mode == RAFT_SERVER && (client == NULL || client->peer_fd == -1)) {
        warnx("raft_send: SERVER without client or peer_fd");
        errno = EBADF;
        return -1;
    }

    if (rpc >= RAFT_MAX_RPC) {
        errno = EINVAL;
        return -1;
    }

    if (rpc != RAFT_APPEND_ENTRIES && rpc != RAFT_APPEND_ENTRIES_REPLY)
        rdbg_printf("RAFT raft_send: %s to fd=%d,id=%d (active_peers=%d, leader_id=%d)\n",
                raft_rpc_str[rpc],
                client ? client->peer_fd : -1,
                client ? (int)client->server_id : -1,
                raft_active_peers, raft_client_state.current_leader_id);

    va_start(ap, rpc);

    packet.length = RAFT_HDR_SIZE;
    packet.length += raft_rpc_settings[rpc].min_size;

    switch (rpc)
    {
        case RAFT_REGISTER_CLIENT_REPLY:
            arg_status      = (uint8_t)va_arg(ap, raft_status_t);
            arg_client_id   = htonl(va_arg(ap, uint32_t));
            arg_leader_hint = htonl(va_arg(ap, uint32_t));

            if (arg_status >= RAFT_MAX_STATUS)
                goto fail;
            break;

        case RAFT_HELLO:
            arg_id        = htonl(va_arg(ap, uint32_t));
            arg_conn_type = (uint8_t)va_arg(ap, raft_conn_t);

            if (arg_conn_type >= RAFT_MAX_CONN)
                goto fail;
            break;

        case RAFT_REQUEST_VOTE:
            arg_term           = htonl(va_arg(ap, uint32_t));
            arg_candidate_id   = htonl(va_arg(ap, uint32_t));
            arg_last_log_index = htonl(va_arg(ap, uint32_t));
            arg_last_log_term  = htonl(va_arg(ap, uint32_t));
            break;

        case RAFT_REQUEST_VOTE_REPLY:
            arg_status    = (uint8_t)va_arg(ap, raft_status_t);
            arg_term      = htonl(va_arg(ap, uint32_t));
            arg_voted_for = htonl(va_arg(ap, uint32_t));

            if (arg_status >= RAFT_MAX_STATUS)
                goto fail;
            break;

        case RAFT_CLIENT_REQUEST_REPLY:
            arg_status       = (uint8_t)va_arg(ap, raft_status_t);
            arg_log_type     = (uint8_t)va_arg(ap, raft_log_t);
            arg_client_id    = htonl(va_arg(ap, uint32_t));
            arg_sequence_num = htonl(va_arg(ap, uint32_t));

            if (arg_status >= RAFT_MAX_STATUS)
                goto fail;

            if (arg_log_type >= RAFT_MAX_LOG)
                goto fail;

            break;

        case RAFT_CLIENT_REQUEST:
            arg_client_id    = htonl(va_arg(ap, uint32_t));
            arg_sequence_num = htonl(va_arg(ap, uint32_t));
            arg_req_type     = (uint8_t)va_arg(ap, raft_log_t);
            arg_req_flags    = 0;
            arg_req_len      = 0;

            if (arg_req_type >= raft_impl->num_log_types) {
                errno = EINVAL;
                warnx("raft_send: CLIENT_REQUEST: unknown type (%u)", arg_req_type);
                goto fail;
            }

            const struct raft_impl_entry *ent = &raft_impl->handlers[arg_req_type];

            arg_event = va_arg(ap, struct raft_log *);
            if (ent->pre_send)
                if (ent->pre_send(arg_event, &send_state) == -1)
                    goto fail;

            if ( ((size_t)packet.length) + send_state.arg_req_len > RAFT_MAX_PACKET_SIZE) {
                errno = EOVERFLOW;
                goto fail;
            }

            packet.length += send_state.arg_req_len;
            arg_req_len = htons(send_state.arg_req_len);
            break;

        case RAFT_APPEND_ENTRIES:
            arg_term           = htonl(va_arg(ap, uint32_t));
            arg_leader_id      = htonl(va_arg(ap, uint32_t));
            arg_prev_log_index = htonl(va_arg(ap, uint32_t));
            arg_prev_log_term  = htonl(va_arg(ap, uint32_t));
            arg_leader_commit  = htonl(va_arg(ap, uint32_t));
            arg_num_entries    = va_arg(ap, uint32_t);

            if (packet.length < (RAFT_HDR_SIZE + RAFT_APPEND_ENTRIES_FIXED_SIZE)) {
                warn("raft_send: APPEND_ENTRIES: packet.length too short");
                goto fail;
            }

            if (arg_num_entries) {
                arg_entries = va_arg(ap, struct raft_log *);
                if (arg_entries == NULL)
                    goto fail;

                struct raft_log *tmp = arg_entries;
                unsigned idx;
                arg_req_len = 0;

                if ((send_states = calloc(arg_num_entries, sizeof(struct send_state))) == NULL)
                    goto fail;

                /* FIXME need this to be an array? */

                for (idx = 0; tmp && idx < arg_num_entries; idx++)
                {
                    if ( ((size_t)packet.length) + RAFT_LOG_FIXED_SIZE > RAFT_MAX_PACKET_SIZE) {
                        errno = EOVERFLOW;
                        goto fail;
                    }

                    packet.length += RAFT_LOG_FIXED_SIZE;

                    if (tmp->event >= raft_impl->num_log_types) {
                        errno = EINVAL;
                        goto fail;
                    }

                    const struct raft_impl_entry *ent = &raft_impl->handlers[tmp->event];

                    if (ent->pre_send)
                        if (ent->pre_send(tmp, &send_states[idx]) == -1)
                            goto fail;

                    tmp = tmp->next;
                    arg_req_len += send_states[idx].arg_req_len;
                }

                //assert(tmp == NULL);
                assert(idx == arg_num_entries);

                if ( ((size_t)packet.length) + arg_req_len > RAFT_MAX_PACKET_SIZE) {
                    errno = EOVERFLOW;
                    goto fail;
                }

                packet.length += arg_req_len;
            }
            break;

        case RAFT_APPEND_ENTRIES_REPLY:
            arg_status = (uint8_t)(va_arg(ap, raft_status_t));
            arg_new_match_index = htonl(va_arg(ap, uint32_t));
            break;

        default:
            rdbg_printf("RAFT raft_send: unknown type %d\n", rpc);
            errno = EINVAL;
            goto fail;
    }

    if (packet.length > RAFT_MAX_PACKET_SIZE) {
        errno = EOVERFLOW;
        goto fail;
    }

    if ((ptr = packet_buffer = malloc(packet.length)) == NULL)
        goto fail;

    sendsz = packet.length;

    packet.rpc   = (uint8_t)rpc;
    packet.role  = mode;
    packet.res0  = 0;
    packet.flags = 0;

    packet.length = htonl(packet.length);

    *ptr++ = packet.rpc;
    *ptr++ = packet.flags;
    *ptr++ = packet.role;
    *ptr++ = packet.res0;

    memcpy(ptr, &packet.length, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    switch(rpc)
    {
        case RAFT_HELLO:
            memcpy(ptr, &arg_id, sizeof(arg_id))              ; ptr += sizeof(arg_id)   ;
            *ptr++ = arg_conn_type;
            memcpy(ptr, &opt_listen.s_addr, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
            uint16_t tmp_port = htons(opt_port);
            memcpy(ptr, &tmp_port, sizeof(uint16_t))          ; ptr += sizeof(uint16_t) ;
            break;

        case RAFT_CLIENT_REQUEST_REPLY:
            *ptr++ = arg_status;
            *ptr++ = arg_log_type;
            memcpy(ptr, &arg_client_id, 4); ptr += 4;
            memcpy(ptr, &arg_sequence_num, 4); ptr += 4;
            break;

        case RAFT_CLIENT_REQUEST:
            memcpy(ptr, &arg_client_id, sizeof(uint32_t))    ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_sequence_num, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
            *ptr++ = arg_req_type;
            *ptr++ = arg_req_flags;

            /* actual request payload */
            memcpy(ptr, &arg_req_len, sizeof(uint16_t)); ptr += sizeof(uint16_t);

            if (arg_req_type >= raft_impl->num_log_types) {
                errno = EINVAL;
                goto fail;
            }

            const struct raft_impl_entry *ent = &raft_impl->handlers[arg_req_type];

            send_state.ptr = ptr;

            if (ent->fill_send)
                if (ent->fill_send(&send_state, arg_event) == -1)
                    goto fail;

            ptr = send_state.ptr;
            break;

        case RAFT_REGISTER_CLIENT_REPLY:
            *ptr++ = arg_status;
            memcpy(ptr, &arg_client_id, sizeof(uint32_t))   ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_leader_hint, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
            break;

        case RAFT_REQUEST_VOTE:
            memcpy(ptr, &arg_term, sizeof(uint32_t))           ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_candidate_id, sizeof(uint32_t))   ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_last_log_index, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_last_log_term, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ;
            break;

        case RAFT_REQUEST_VOTE_REPLY:
            *ptr++ = arg_status;
            memcpy(ptr, &arg_term, sizeof(uint32_t))      ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_voted_for, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
            break;

        case RAFT_APPEND_ENTRIES:
            uint32_t tmp32 = htonl(arg_num_entries);

            memcpy(ptr, &arg_term, sizeof(uint32_t))           ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_leader_id, sizeof(uint32_t))      ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_prev_log_index, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_prev_log_term, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_leader_commit, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &tmp32, sizeof(uint32_t))              ; ptr += sizeof(uint32_t) ;

            if (arg_num_entries) {
                //rdbg_printf("RAFT raft_send: APPEND_ENTRIES: sending %d entries\n", arg_num_entries);
                const struct raft_log *tmp = arg_entries;
                uint32_t index, term;
                uint16_t entry_length;
                uint8_t *entry_length_ptr;

                for (unsigned idx = 0; tmp && idx < arg_num_entries; idx++)
                {
                    *ptr = (uint8_t)tmp->event; ptr++; /* type  */
                    *ptr = 0; ptr++;                   /* flags */
                    index = htonl(tmp->index);         /* index */
                    term = htonl(tmp->term);           /* term  */
                    memcpy(ptr, &index, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
                    memcpy(ptr, &term, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ;
                    entry_length_ptr = ptr;       /* save for later*/
                    ptr += sizeof(uint16_t);
                    entry_length = 0;

                    if (tmp->event >= raft_impl->num_log_types) {
                        errno = EINVAL;
                        warnx("raft_send: tmp->event (%u) unknown inside RAFT_APPEND_ENTRIES", tmp->event);
                        goto fail;
                    }

                    const struct raft_impl_entry *ent = &raft_impl->handlers[tmp->event];

                    ssize_t rc;
                    if (ent->fill_send) {
                        send_states[idx].ptr = ptr;
                        if ((rc = ent->fill_send(&send_states[idx], tmp)) == -1)
                            goto fail;
                        entry_length += rc;
                        ptr = send_states[idx].ptr;
                    }

                    tmp = tmp->next;
                    entry_length = htons(entry_length);
                    memcpy(entry_length_ptr, &entry_length, sizeof(uint16_t));
                }
            }
            if(ptr != packet_buffer + sendsz)
                goto fail;
            break;

        case RAFT_APPEND_ENTRIES_REPLY:
            *ptr++ = arg_status;
            uint32_t term = htonl(raft_state.current_term);
            memcpy(ptr, &term, sizeof(uint32_t))                ; ptr += sizeof(uint32_t) ;
            memcpy(ptr, &arg_new_match_index, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ;
            break;

        default:
            errno = EINVAL;
            rdbg_printf("RAFT raft_send: unknown type %d\n", rpc);
            goto fail;
    }

    unsigned sent = 0;

    if (client == NULL && mode != RAFT_CLIENT) {
        for (unsigned idx = 1; idx < raft_num_peers; idx++)
        {
            if (raft_peers[idx].peer_fd == -1)
                continue;
            if ((tmp_packet_buffer = malloc(sendsz)) == NULL)
                goto fail;

            memcpy(tmp_packet_buffer, packet_buffer, sendsz);

            if (raft_add_write(&raft_peers[idx], tmp_packet_buffer, sendsz) == -1) {
                warn("raft_send: bcast raft_add_write(%u)", idx);
                free(tmp_packet_buffer);
                tmp_packet_buffer = NULL;
                raft_close(&raft_peers[idx]);
            } else if (raft_try_write(&raft_peers[idx]) == -1) {
                if (errno != EAGAIN)
                    warn("raft_send: bcast raft_try_write(%u)", idx);
            } else {
                sent++;
            }
        }
    } else {
        int *fd;

        if (mode == RAFT_CLIENT) {
            pthread_rwlock_rdlock(&raft_client_state.lock);
            if (raft_client_state.current_leader == NULL) {
                pthread_rwlock_unlock(&raft_client_state.lock);
                warnx("raft_send: can't find leader?");
                goto fail;
            }
            fd = &raft_client_state.current_leader->peer_fd;
            client = raft_client_state.current_leader;
            pthread_rwlock_unlock(&raft_client_state.lock);
        } else /* RAFT_PEER || RAFT_SERVER */ {
            if (client == NULL) {
                errno = EINVAL;
                goto fail;
            }
            fd = &client->peer_fd;
        }

        assert(fd != NULL);

        if (*fd == -1) {
            errno = EBADF;
            raft_close(client);
            goto fail;
        }

        if (raft_add_write(client, packet_buffer, sendsz) == -1) {
            warn("raft_send: raft_add_write(%u)", client->server_id);
            if (errno != ENOSPC)
                raft_close(client);
            goto fail;
        }

        packet_buffer = NULL;

        if (raft_try_write(client) == -1) {
            if (errno != EAGAIN) {
                warn("raft_send: raft_try_write(%u)", client->server_id);
                goto fail;
            }
        } else {
            sent = 1;
        }
    }

    if (packet_buffer)
        free(packet_buffer);
    if (send_states)
        free(send_states);
    va_end(ap);
    return sent;

fail:
    if (send_states)
        free(send_states);
    if (packet_buffer)
        free(packet_buffer);
    if (tmp_packet_buffer)
        free(tmp_packet_buffer);
    va_end(ap);
    return -1;
}

static int raft_reset_election_timer(void)
{
    raft_state.election_timer = timems() + rnd(RAFT_MIN_ELECTION,
            RAFT_MAX_ELECTION) + 100;
    return 0;
}

static int raft_reset_next_ping(void)
{
    raft_state.next_ping = timems() + RAFT_PING_DELAY;
    return 0;
}

static int raft_change_to(raft_state_t mode)
{
    if (raft_state.state == mode)
        return 0;

    rdbg_printf(BBLU "RAFT raft_change_to: %s (from %s)"CRESET"\n",
            raft_mode_str[mode], raft_mode_str[raft_state.state]);

    switch (mode)
    {
        case RAFT_STATE_FOLLOWER:
            break;

        case RAFT_STATE_CANDIDATE:
            break;

        case RAFT_STATE_LEADER:
            pthread_rwlock_wrlock(&raft_client_state.lock);
            raft_update_leader_id(raft_state.self_id, false);

            const uint32_t last = raft_state.log_tail ? raft_state.log_tail->index : 0;

            for (unsigned idx = 1; idx < raft_num_peers; idx++)
            {
                raft_peers[idx].match_index = 0;
                raft_peers[idx].next_index = last + 1;
            }

            /* ensures a 'welcome' ping-style RAFT_APPEND_ENTRIES is sent swiftly */
            raft_state.next_ping = timems();

            logger(LOG_NOTICE, NULL, "raft: node is now leader of term %u", raft_state.current_term);
            break;

        default:
            errno = EINVAL;
            return -1;
    }

    raft_state.state = mode;

    if (mode == RAFT_STATE_LEADER)
        pthread_rwlock_unlock(&raft_client_state.lock);

    return mode;
}

static int raft_tick_connection_check(void)
{
    int new_fd = 0;
    struct sockaddr_in sin;
    socklen_t sin_len;
    errno = 0;

    for (unsigned idx = 1; idx < raft_num_peers; idx++)
    {
        if (raft_peers[idx].port == 0)
            continue;

        if (raft_peers[idx].peer_fd != -1)
            continue;

        if (timems() < raft_peers[idx].next_conn_attempt)
            continue;

        if ((new_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1) {
            warn("raft_tick_connection_check: socket");
            continue;
        }

        memset(&sin, 0, sizeof(sin));

        sin.sin_family = AF_INET;
        sin.sin_addr = raft_peers[idx].address;
        sin.sin_port = raft_peers[idx].port;

        sin_len = sizeof(sin);

        sock_linger(new_fd);
        sock_nodelay(new_fd);
        sock_keepalive(new_fd);

        if (connect(new_fd, (struct sockaddr *)&sin, sin_len) == -1) {
            close_socket(&new_fd);
            raft_peers[idx].next_conn_attempt = timems() +
                rnd(RAFT_MIN_ELECTION * 2, RAFT_MAX_ELECTION * 3);
            continue;
        }

        sock_nonblock(new_fd);

        raft_peers[idx].peer_fd = new_fd;
        raft_reset_read_state(&raft_peers[idx]);
        raft_reset_write_state(&raft_peers[idx], true);
        raft_reset_ss_state(&raft_peers[idx], true);

        if (idx)
            raft_active_peers++;

        rdbg_printf("RAFT raft_tick: connected to %08x:%u (idx=%u, fd=%d, id=%u)\n",
                ntohl(sin.sin_addr.s_addr), ntohs(sin.sin_port),
                idx, new_fd, raft_peers[idx].server_id);

        if (raft_send(idx == 0 ? RAFT_CLIENT : RAFT_PEER, &raft_peers[idx], RAFT_HELLO,
                    raft_state.self_id, idx == 0 ? RAFT_CLIENT : RAFT_PEER) == -1) {
            warn("raft_tick: connect: write(id), closing");
            raft_close(&raft_peers[idx]);
        }

    }

    return 0;
}

static int raft_stop_election(void)
{
    rdbg_printf(NMAG "RAFT raft_stop_election" CRESET "\n");
    raft_state.election = false;

    for (unsigned idx = 0; idx < raft_num_peers; idx++) {
        raft_peers[idx].vote_responded = false;
        raft_peers[idx].vote_granted = NULL_ID;
    }

    raft_reset_election_timer();
    return 0;
}

static int raft_request_votes(void)
{
    int rc;

    const uint32_t log_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
    const uint32_t log_term = log_index ? raft_term_at(log_index) : 0;

    if (log_term == -1U) {
        errno = ENOENT;
        warn("raft_request_votes: no term at index %u", log_index);
        return -1;
    }

    for (unsigned idx = 1; idx < raft_num_peers; idx++)
    {
        if (raft_peers[idx].peer_fd == -1)
            continue;
        if (raft_peers[idx].vote_responded)
            continue;

        if ((rc = raft_send(RAFT_PEER, &raft_peers[idx],
                        RAFT_REQUEST_VOTE,
                        raft_state.current_term,
                        raft_state.self_id,
                        log_index, log_term
                        )) == -1) {
            warn("raft_request_votes: raft_send");
        }
    }

    raft_state.next_request_vote = timems() + RAFT_PING_DELAY;

    return 0;
}

static int raft_start_election(void)
{
    rdbg_printf(BMAG "RAFT raft_start_election" CRESET "\n");

    /* On conversion to candidate, start election */

    /* Timeout(i) == */

    /* We assume:  /\ state' = [state EXCEPT ![i] = Candidate]
     * has happened by the caller */
    assert(raft_state.state == RAFT_STATE_CANDIDATE);

    raft_state.current_term++; /* ignore raft_update_term() as next sync's */

    /* Vote for self */
    raft_update_voted_for(raft_state.self_id);
    raft_peers[0].vote_responded = true;
    raft_peers[0].vote_granted = raft_state.self_id;

    /* Don't wipe our vote */
    for (unsigned idx = 1; idx < raft_num_peers; idx++)
    {
        raft_peers[idx].vote_responded = false;
        raft_peers[idx].vote_granted = NULL_ID;
    }

    /* Reset election timer */
    raft_reset_election_timer();

    /* leader unknown untill RAFT_APPEND_ENTRIES */
    raft_update_leader_id(NULL_ID, true);

    rdbg_printf(BWHT "RAFT raft_change_to: starting election (new term will be %u)" CRESET "\n",
            raft_state.current_term);

    if (raft_num_peers == 1) {
        raft_change_to(RAFT_STATE_LEADER);
        raft_stop_election();
        return 0;
    }

    /* Send RequestVote RPCs to all other servers */
    raft_state.election = true;

    return raft_request_votes();
}

static int raft_tick(void)
{
    const timems_t now = timems();

#ifdef FEATURE_RAFT_DEBUG
    static time_t last_run = 0;

    if (last_run == 0)
        last_run = time(NULL) - 1;

    if (time(NULL) != last_run) {
        const uint32_t log_index = raft_state.log_tail ?
            raft_state.log_tail->index : 0;

        rdbg_printf(
                "RAFT [%s]: trm=%u/log.idx=%u/comm=%u/idx=%u peers=%d ldr=%d vte=%d\n",
                raft_mode_str[raft_state.state],
                raft_state.current_term,
                log_index,
                raft_state.commit_index,
                raft_state.log_index,
                raft_active_peers, raft_client_state.current_leader_id,
                raft_state.voted_for);

#if 0
        static uint32_t last_idx = 0, last_term = 0;
        if (last_term != raft_state.current_term || log_index != last_idx) {
            last_term = raft_state.current_term;
            last_idx = log_index;

            for (const struct raft_log *ent = raft_state.log_head; ent; ent = ent->next)
            {
                const char *name;
                if (ent == raft_state.log_head)
                    name = "HD";
                else if (ent == raft_state.log_tail)
                    name = "TL";
                else
                    name = "--";

                rdbg_printf("RAFT raft_tick: [%2s] [%s] idx=%u/%u",
                        name, raft_log_str[ent->event], ent->index, ent->term);

                switch(ent->event)
                {
                    case RAFT_LOG_REGISTER_TOPIC:
                        rdbg_cprintf(" <%s>", ent->register_topic.name);
                        break;
                    default:
                        break;
                }
                rdbg_cprintf("\n");
            }
        }
#endif
    }
#endif

    switch(raft_state.state)
    {
        case RAFT_STATE_NONE:
            warnx("raft_tick: STATE_NONE");
            abort();
            break;

        case RAFT_STATE_FOLLOWER:
            /* Timeout(i) */
            if (now > raft_state.election_timer) {
                rdbg_printf("RAFT raft_tick: FOLLOWER election_timer expired\n");
                raft_change_to(RAFT_STATE_CANDIDATE);
                raft_start_election();
                logger(LOG_INFO, NULL, "raft: election timeout, starting election");
            }
            break;

        case RAFT_STATE_CANDIDATE:
            /* Timeout(i) */
            if (now > raft_state.election_timer) {
                rdbg_printf("RAFT raft_tick: CANDIDATE election_timer expired\n");
                raft_start_election();
            } else if (now > raft_state.next_request_vote && raft_state.election)
                raft_request_votes();
            break;

        case RAFT_STATE_LEADER:
            if ( now > raft_state.next_ping ) {
                for (unsigned idx = 1; idx < raft_num_peers; idx++)
                {
                    if (raft_peers[idx].peer_fd == -1)
                        continue;

                    const uint32_t log_index = raft_peers[idx].next_index - 1;
                    const uint32_t log_term = raft_term_at(log_index);

                    if (log_term == -1U) {
                        warnx("raft_tick: can't find term for index %d (log_index)", log_index);
                        continue;
                    }

                    if (raft_send(RAFT_PEER, &raft_peers[idx],
                                RAFT_APPEND_ENTRIES,
                                raft_state.current_term,
                                raft_state.self_id,
                                log_index, log_term,
                                raft_state.commit_index,
                                (uint32_t)0, NULL
                                ) == -1) {
                        warn("raft_tick: LEADER: raft_send(%u)",
                                raft_peers[idx].server_id);
                    }
                }

                raft_reset_next_ping();
            }

            for (unsigned idx = 1; idx < raft_num_peers; idx++)
            {
                uint32_t tmp_prev_index = 0, tmp_prev_term = 0;

                if (raft_peers[idx].peer_fd == -1)
                    continue;

                if (raft_state.log_tail == NULL ||
                        raft_state.log_tail->index < raft_peers[idx].next_index)
                    continue;
                /* else... [if] last logindex >= nextIndex[idx] for a follower */

                if (now < raft_peers[idx].last_leader_sync)
                    continue;

                if (raft_has_pending_write(&raft_peers[idx]))
                    continue;

                raft_peers[idx].last_leader_sync = timems() + RAFT_PING_DELAY;

                for (struct raft_log *tmp = raft_state.log_head; tmp; tmp = tmp->next)
                {
                    /* ... starting at nextIndex */
                    if (tmp->index < raft_peers[idx].next_index)
                        continue;

                    tmp_prev_index = raft_peers[idx].next_index - 1;
                    tmp_prev_term = raft_term_at(tmp_prev_index);

                    if (tmp_prev_term == -1U) {
                        warnx("raft_tick: can't find term for index %d (tmp_prev_index)", tmp_prev_index);
                        continue;
                    }

                    unsigned log_cnt = 0;
                    for (const struct raft_log *cnt_tmp = tmp; cnt_tmp && log_cnt < RAFT_MAX_LOG_LENGTH; cnt_tmp = cnt_tmp->next)
                        log_cnt++;

                    log_cnt = MIN(RAFT_MAX_LOG_LENGTH, log_cnt);

                    if (raft_state.log_head &&
                            raft_state.log_head->index <= raft_peers[idx].next_index) {

                        rdbg_printf("RAFT raft_tick: sending APPEND_ENTRIES to id=%u at idx=%u/%u\n",
                                raft_peers[idx].server_id,
                                tmp->index, tmp->term);

                        if (raft_send(RAFT_PEER, &raft_peers[idx],
                                    RAFT_APPEND_ENTRIES,
                                    raft_state.current_term,
                                    raft_state.self_id,
                                    tmp_prev_index, tmp_prev_term,
                                    raft_state.commit_index, log_cnt,
                                    (const struct raft_log *)tmp) == -1)
                            warn("raft_tick: LEADER: raft_send");
                        /* we have sent all the remaining ones so exit this loop */
                        break;
                    }
                }

            }

            /*
            if (raft_state.log_length > RAFT_MAX_LOG_LENGTH) {
                rdbg_printf("RAFT raft_tick: saving state and compacting log\n");
            }
            */

            break;

        default:
            break;
    }

    raft_tick_connection_check();

#ifdef FEATURE_RAFT_DEBUG
    last_run = time(NULL);
#endif

    return 0;
}

[[gnu::nonnull(1)]]
static raft_status_t process_append_entries(struct raft_host_entry *client,
        uint32_t term, uint32_t leader_id, uint32_t prev_log_index,
        uint32_t prev_log_term, struct raft_log *log_entry_head,
        uint32_t leader_commit, uint32_t new_match_index)
{
    raft_status_t reply = RAFT_TRUE;

    /* 1. reply false if term < currentTerm */
    if (term < raft_state.current_term) {
        reply = RAFT_FALSE;
        goto append_reply;
    }

    raft_update_leader_id(leader_id, true);

    /* 2. reply false if log doesn't contain an entry at
     * prev_log_index whose term matches prev_log_term */

    if (raft_state.log_head == NULL) {
        if (prev_log_index > 0) {
            new_match_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
            reply = RAFT_FALSE; /* signal we have nothing so we're at index=0 */
            goto append_reply;
        }
        goto got_prev_log;
    }

    for (const struct raft_log *tmp = raft_state.log_head; tmp; tmp = tmp->next)
    {
        if (prev_log_index == 0)
            goto got_prev_log;
        if (tmp->index == prev_log_index && tmp->term == prev_log_term)
            goto got_prev_log;
    }
    rdbg_printf("RAFT process_append_entries: no log matches for [%d/%d]\n",
            prev_log_term, prev_log_index);
    new_match_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
    reply = RAFT_FALSE;
    goto append_reply;

got_prev_log:
    if (log_entry_head) {
        /* we have a valid list, so append them all */
        for (struct raft_log *next = NULL, *tmp = log_entry_head; tmp; tmp = next)
        {
            int rc;

            next = tmp->next;
            tmp->next = NULL;

            if ((rc = raft_client_log_append_single(tmp, leader_commit)) == -1) {
                warn("raft_recv: APPEND_ENTRIES: raft_client_log_append");
                reply = RAFT_FALSE;
                tmp->next = next;     /* re-chain */
                log_entry_head = tmp;
                goto append_reply;
            } else if (rc == 0) {
                /* entry was a dupe */
                if (tmp->index > new_match_index)
                    new_match_index = tmp->index;

                if (log_entry_head == tmp)
                    log_entry_head = next;
                raft_free_log(tmp);
                tmp = NULL;
                /* TODO what should reply be here? */
            } else {
                if (tmp->index > new_match_index)
                    new_match_index = tmp->index;
            }
        }
        /* we've added them all OK, don't free */
        log_entry_head = NULL;
    }

    if (reply == RAFT_TRUE && leader_commit > raft_state.commit_index) {
        /* Advance commit index on a heart beat (if acceptable) */
        const uint32_t desired_commit_index = MIN(leader_commit,
                raft_state.log_tail ? raft_state.log_tail->index : 0);
        while (raft_state.commit_index < desired_commit_index)
            if (raft_commit_and_advance() == -1)
                break;
    }

    /*
     * ALL SERVERS:
     *
     * If RPC request or response contains term T > currentTerm,
     * set currentTerm to T, convert to follower. §3.3
     *
     * CANDIDATES: §3.4
     *
     * If AppendEntries RPC received from new leader: convert to follower.
     *
     * If the leader’s term (included in its RPC) is at least as large as
     * the candidate’s current term, then the candidate recognizes the
     * leader as legitimate and returns to follower state
     */

    /* exit events from Candidate status:
     *
     * receives majority of votes --> LEADER
     * election times out, new election --> CANDIDATE
     * discovers current leader OR new term --> FOLLOWER
     */

    if (term == raft_state.current_term &&
            raft_state.state == RAFT_STATE_CANDIDATE) {
        rdbg_printf("RAFT raft_recv: APPEND_ENTRIES: received term matched leader ping from %u, ceasing CANDIDATE.\n",
                client->server_id);
        goto step_down;
    } else if (term > raft_state.current_term) {
        rdbg_printf("RAFT raft_recv: APPEND_ENTRIES: received newer term from %u, stepping down.\n",
                client->server_id);
        raft_update_term(term);
step_down:
        raft_change_to(RAFT_STATE_FOLLOWER);
        raft_stop_election();
        raft_update_leader_id(leader_id, true);
    }

    /* This state may never happen, but just in case */
    if (reply == RAFT_FALSE)
        goto append_reply;

append_reply:
    if (reply == RAFT_TRUE) {
        /* ? */
    } else {
        rdbg_printf(BRED "RAFT raft_recv: APPEND_ENTRIES: sending reply of %s idx=%u " CRESET "\n",
                raft_status_str[reply], new_match_index);
    }

    raft_send(RAFT_PEER, client, RAFT_APPEND_ENTRIES_REPLY, reply,
            new_match_index);

    /* if we have any left (dupes) free them */
    while (log_entry_head) {
        struct raft_log *next = log_entry_head->next;
        raft_free_log(log_entry_head);
        log_entry_head = next;
    }

    return reply;
}

[[gnu::nonnull]]
/** the caller (e.g. raft_recv) needs to ensure that client->rd_packet_buffer
 * has been verified within sensible min/max bounds, e.g. raft_rpc_settings.
 * this function will check variable length elements
 */
static int raft_process_packet(struct raft_host_entry *client, raft_rpc_t rpc)
{
    const uint32_t log_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
    const uint32_t log_term = raft_state.log_tail ? raft_state.log_tail->term : 0;
    const uint8_t *ptr = client->rd_packet_buffer;
    size_t bytes_remaining = client->rd_packet_length;
    struct raft_log *log_entry = NULL, *log_entry_head = NULL;
    struct raft_log *prev_log_entry = NULL;
    uint32_t term = 0;
    uint32_t leader_id;
    //uint8_t *temp_string = NULL;
    struct raft_log *out = NULL;

    assert(rpc < RAFT_MAX_RPC);

    pthread_rwlock_rdlock(&raft_client_state.lock);
    leader_id = raft_client_state.current_leader_id;
    pthread_rwlock_unlock(&raft_client_state.lock);

    switch(rpc)
    {
        case RAFT_REGISTER_CLIENT:
            if (raft_state.state != RAFT_STATE_LEADER) {
                raft_send(RAFT_PEER, client, RAFT_REGISTER_CLIENT_REPLY,
                        RAFT_NOT_LEADER, 0, leader_id);
                break;
            }
            /* append register command to log, replicate and commit */
            /* TODO */
            /* apply command in log order, allocting session for new client */
            /* TODO */
            /* reply ok with unique client id, log index can be used */
            raft_send(RAFT_PEER, client, RAFT_REGISTER_CLIENT_REPLY, RAFT_OK,
                    raft_state.last_applied, leader_id);
            break;

        case RAFT_CLIENT_REQUEST_REPLY:
            {
                raft_status_t reply;
                [[maybe_unused]] raft_log_t log_type;
                uint32_t client_id, sequence_num;
                uint8_t tmp;

                tmp = *ptr++;
                reply = tmp;

                tmp = *ptr++;
                log_type = tmp;

                memcpy(&client_id, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                client_id = ntohl(client_id);
                memcpy(&sequence_num, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
                sequence_num = ntohl(sequence_num);

                if (reply >= RAFT_MAX_STATUS) {
                    errno = EINVAL;
                    goto fail;
                }

                if (log_type >= RAFT_MAX_LOG) {
                    errno = EINVAL;
                    goto fail;
                }

                if (reply == RAFT_OK) {
                    /* We treat a this is a valid communication from the leader */
                    raft_reset_election_timer();

                    struct raft_log *match = NULL;

                    pthread_rwlock_rdlock(&raft_client_state.log_pending_lock);
                    for (match = raft_client_state.log_pending_head; match; match = match->next)
                        /* this is for the CLIENT to get a RAFT_OK from the SERVER
                         * as we are the CLIENT, then we use our self_id */
                        if (match->sequence_num == sequence_num &&
                                raft_state.self_id == client_id)
                            break;
                    pthread_rwlock_unlock(&raft_client_state.log_pending_lock);

                    if (match == NULL) {
                        /* late replies are not this bad */
                        goto done;
                    }

                    pthread_mutex_lock(&match->mutex);
                    match->done = true;
                    pthread_cond_signal(&match->cv);
                    pthread_mutex_unlock(&match->mutex);
                    goto done;
                }

                warn("raft_process_packet: CLIENT_REQUEST_REPLY: got an error: <%s>",
                        reply < RAFT_MAX_STATUS ? raft_status_str[reply] : "ILLEGAL_CODE");
                goto fail;
            }
            break;

        case RAFT_CLIENT_REQUEST:
            {
                raft_status_t reply = RAFT_OK;
                uint32_t client_id, sequence_num;
                uint8_t type/*, flags*/;
                uint16_t len;

                memcpy(&client_id, ptr, sizeof(uint32_t));
                ptr += sizeof(uint32_t); client_id = ntohl(client_id);

                memcpy(&sequence_num, ptr, sizeof(uint32_t));
                ptr += sizeof(uint32_t); sequence_num = ntohl(sequence_num);

                type = *ptr++;
                /* flags = * */ ptr++;

                bytes_remaining -= RAFT_CLIENT_REQUEST_SIZE;

                /* TODO read the actual request */
                memcpy(&len, ptr, sizeof(len));
                ptr += sizeof(len); len = ntohs(len);

                if (len > bytes_remaining)
                    goto fail;

                if (raft_state.state != RAFT_STATE_LEADER) {
                    reply = RAFT_NOT_LEADER;
                    goto send_client_request_reply;
                }

                if (type >= raft_impl->num_log_types) {
                    errno = EINVAL;
                    goto fail;
                }

                const struct raft_impl_entry *ent = &raft_impl->handlers[type];

                if (ent->process_packet)
                    if ((out = raft_alloc_log(RAFT_CLIENT, type)) == NULL)
                        goto fail;

                if (ent->process_packet) {
                    size_t tmp_bytes_remaining = bytes_remaining;
                    if ((reply = ent->process_packet(&tmp_bytes_remaining, &ptr, rpc, type, out)) == -1)
                        goto fail;
                    assert(tmp_bytes_remaining == 0);

                    int rc = raft_leader_log_appendv(type, out, NULL);

                    if (rc == -1) {
                        reply = RAFT_ERR;
                    }
                } else
                    reply = RAFT_ERR;

send_client_request_reply:
                raft_send(RAFT_SERVER, client, RAFT_CLIENT_REQUEST_REPLY,
                        reply, type, client_id, sequence_num);
            }
            break;

        case RAFT_REQUEST_VOTE:
            {
                uint32_t candidate_id, last_log_index, last_log_term;
                raft_status_t reply;

                memcpy(&term, ptr, sizeof(uint32_t))           ; ptr += sizeof(uint32_t) ; term = ntohl(term)                     ;
                memcpy(&candidate_id, ptr, sizeof(uint32_t))   ; ptr += sizeof(uint32_t) ; candidate_id = ntohl(candidate_id)     ;
                memcpy(&last_log_index, ptr, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ; last_log_index = ntohl(last_log_index) ;
                memcpy(&last_log_term, ptr, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ; last_log_term = ntohl(last_log_term)   ;
                rdbg_printf("RAFT raft_process_packet: REQUEST_VOTE: term=%u candidate_id=%u last_log_index=%u/%u\n",
                        term, candidate_id, last_log_index, last_log_term);

                /* If RPC request or response contains term T > currentTerm:
                 * set currentTerm = T, convert to follower */
                if (term > raft_state.current_term) {
                    rdbg_printf("RAFT raft_process_packet: higher term received from id=%u (%u>%u)\n",
                            client->server_id, term, raft_state.current_term);
                    raft_update_term(term);
                    raft_change_to(RAFT_STATE_FOLLOWER);
                    raft_stop_election();
                    raft_update_leader_id(NULL_ID, true);
                }

                bool is_upto_date = (last_log_term > log_term) ||
                    (last_log_term == log_term && last_log_index >= log_index);

                if (term < raft_state.current_term) {
                    /* Reply false if term < currentTerm */
                    reply = RAFT_FALSE;
                } else if (
                        (raft_state.voted_for == NULL_ID || raft_state.voted_for == candidate_id)
                        && is_upto_date
                        && candidate_id != NULL_ID ) {
                    /* If votedFor is NULL or candidateId, and candidate's log is at
                     * least as up-to-date as receivers log, grant vote */
                    reply = RAFT_TRUE;
                    raft_update_voted_for(candidate_id);
                    raft_reset_election_timer();
                } else {
                    reply = RAFT_FALSE;
                }

                raft_send(RAFT_PEER, client, RAFT_REQUEST_VOTE_REPLY, reply,
                        raft_state.current_term, raft_state.voted_for);
            }
            break;

        case RAFT_APPEND_ENTRIES:
            {
                uint32_t leader_id, prev_log_index, prev_log_term, leader_commit, num_entries;

                memcpy(&term, ptr, sizeof(uint32_t))           ; ptr += sizeof(uint32_t) ; term = ntohl(term)                     ;
                memcpy(&leader_id, ptr, sizeof(uint32_t))      ; ptr += sizeof(uint32_t) ; leader_id = ntohl(leader_id)           ;
                memcpy(&prev_log_index, ptr, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ; prev_log_index = ntohl(prev_log_index) ;
                memcpy(&prev_log_term, ptr, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ; prev_log_term = ntohl(prev_log_term)   ;
                memcpy(&leader_commit, ptr, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ; leader_commit = ntohl(leader_commit)   ;
                memcpy(&num_entries, ptr, sizeof(uint32_t))    ; ptr += sizeof(uint32_t) ; num_entries = ntohl(num_entries)       ;

                if ( ((size_t)num_entries) * RAFT_LOG_FIXED_SIZE > RAFT_MAX_PACKET_SIZE) {
                    errno = EOVERFLOW;
                    goto fail;
                }

                bytes_remaining -= RAFT_APPEND_ENTRIES_FIXED_SIZE;

                uint32_t new_match_index = prev_log_index;
                uint32_t expected_index = prev_log_index + 1;

                if (num_entries) {
                    rdbg_printf("RAFT raft_process_packet: APPEND_ENTRIES: term=%u leader_id=%u prev_log_index=%u/%u leader_commit=%u num_entries=%u\n",
                            term, leader_id, prev_log_index, prev_log_term, leader_commit,
                            num_entries);
                }

                /* We treat a this is a valid communication from the leader */
                raft_reset_election_timer();

                /* 'discovers server with higher term' is valid exit from
                 * Leader state */

                /* go over each log entry, building a temporary list starting with
                 * log_entry_head */

                /* TODO chunking to avoid EOVERFLOW */
                for (unsigned idx = 0; idx < num_entries; idx++)
                {
                    uint8_t type, flags;
                    uint32_t index, term;
                    uint16_t entry_length;

                    if (bytes_remaining < RAFT_LOG_FIXED_SIZE)
                        goto fail;

                    type = *ptr; ptr++;
                    flags = *ptr; ptr++;

                    memcpy(&index, ptr, sizeof(uint32_t));
                    ptr += sizeof(uint32_t);
                    index = ntohl(index);

                    memcpy(&term, ptr, sizeof(uint32_t));
                    ptr += sizeof(uint32_t);
                    term = ntohl(term);

                    memcpy(&entry_length, ptr, sizeof(uint16_t));
                    ptr += sizeof(uint16_t);
                    entry_length = ntohs(entry_length);

                    if (type >= RAFT_MAX_LOG) {
                        errno = EINVAL;
                        goto fail;
                    }

                    /*
                       rdbg_printf("RAFT raft_process_packet: APPEND_ENTRIES: idx=%u type=%s flags=%u len=%u\n",
                       idx, raft_log_str[type], flags, entry_length);*/

                    bytes_remaining -= RAFT_LOG_FIXED_SIZE;

                    if (index != expected_index) {
                        rdbg_printf(BRED "RAFT raft_process_packet: sending RAFT_FALSE: index %d != expected_index %d " CRESET "\n",
                                index, expected_index);
                        raft_send(RAFT_PEER, client, RAFT_APPEND_ENTRIES_REPLY, RAFT_FALSE,
                                prev_log_index);
                        goto done;
                    }

                    if ((log_entry = raft_alloc_log(RAFT_PEER, type)) == NULL)
                        goto fail;

                    log_entry->flags = flags;
                    log_entry->index = index;
                    log_entry->term = term;

                    if (type >= raft_impl->num_log_types) {
                        errno = EINVAL;
                        goto fail;
                    }

                    const struct raft_impl_entry *ent = &raft_impl->handlers[type];

                    if (ent->process_packet) {
                        size_t tmp_bytes_remaining = entry_length;
                        if (ent->process_packet(&tmp_bytes_remaining,
                                    &ptr, rpc, (raft_log_t)type, log_entry) == -1)
                            goto fail;
                        assert(tmp_bytes_remaining == 0);
                    } else
                        ptr += entry_length;

                    bytes_remaining -= entry_length;

                    /* if we're the first one, set the head */
                    if (log_entry_head == NULL) {
                        log_entry_head = log_entry;
                        /* if we're not the first, link us in */
                    } else if (prev_log_entry) {
                        prev_log_entry->next = log_entry;
                    }

                    prev_log_entry = log_entry;
                    /* NULL the valid entry, so that fail doesn't double-free */
                    log_entry = NULL;
                    expected_index++;
                }

                if (process_append_entries(client, term, leader_id, prev_log_index,
                            prev_log_term, log_entry_head, leader_commit,
                            new_match_index) == RAFT_ERR)
                    goto fail;

                log_entry_head = NULL;
            }
            break;

        case RAFT_APPEND_ENTRIES_REPLY:
            {
                uint8_t tmp;
                uint32_t client_term, new_match_index;
                raft_status_t status;

                memcpy(&tmp, ptr, 1)                            ; ptr++                   ; status = tmp                             ;
                memcpy(&client_term, ptr, sizeof(uint32_t))     ; ptr += sizeof(uint32_t) ; client_term = ntohl(client_term)         ;
                memcpy(&new_match_index, ptr, sizeof(uint32_t)) ; ptr += sizeof(uint32_t) ; new_match_index = ntohl(new_match_index) ;

                if (client_term > raft_state.current_term) {
                    rdbg_printf("RAFT raft_process_packet: APPEND_ENTRIES_REPLY has higher term from id=%u, converting\n",
                            client->server_id);
                    raft_update_term(client_term);
                    raft_change_to(RAFT_STATE_FOLLOWER);
                    raft_stop_election();
                    raft_update_leader_id(NULL_ID, true);
                    break;
                }

                if (raft_state.state != RAFT_STATE_LEADER)
                    break;

                if (status == RAFT_TRUE) {
                    if (new_match_index > client->match_index) {
                        rdbg_printf("RAFT raft_process_packet: APPEND_ENTRIES_REPLY: updating match_index[%u] to %u\n",
                                client->server_id, new_match_index);
                        client->match_index = new_match_index;
                        raft_check_commit_index(new_match_index);
                    }
                    if (new_match_index >= client->next_index) {
                        client->next_index = new_match_index + 1;
                    }

                    break;
                }

                rdbg_printf(BRED "RAFT raft_process_packet: APPEND_ENTRIES_REPLY %s from %u new_match_index %d " CRESET "\n",
                      raft_status_str[status], client->server_id, new_match_index);

                const uint32_t old_next = client->next_index;
                const uint32_t leader_tail = raft_state.log_tail ? raft_state.log_tail->index : 0;
                /* Follower hint: earliest index worth retrying: its match + 1, min 1 */
                const uint32_t hinted_next = MAX(1, new_match_index + 1);
                /* Clamp hint so we never point past the leader's own log */
                const uint32_t target_next = MIN(leader_tail +1, hinted_next);

                /* Fast backtrack */
                if (target_next < old_next)
                    client->next_index = target_next;
                /* Slow backtrack */
                if (client->next_index == old_next && client->next_index > 1)
                    client->next_index--;

                if (raft_state.log_head && client->next_index > raft_state.log_head->index) {
                    /* leader has the entries needed at/after next_index */
                } else if (client->next_index > 1) {
                    /* TODO trigger RAFT_INSTALL_SNAPSHOT */
                    warnx("raft_process_packet: i don't have what the client needs!");
                } else {
                    client->next_index = 1;
                }

            }
            break;

        case RAFT_REQUEST_VOTE_REPLY:
            {
                uint8_t tmp;
                raft_status_t status;
                uint32_t voted_for;

                memcpy(&tmp, ptr, 1); ptr++;
                status = tmp;

                memcpy(&term, ptr, 4); ptr += 4;
                term = ntohl(term);

                memcpy(&voted_for, ptr, 4); ptr += 4;
                voted_for = ntohl(voted_for);

                /* ignore votes from older terms */
                if (term < raft_state.current_term)
                    break;

                if (term > raft_state.current_term) {
                    rdbg_printf("RAFT raft_process_packet: REQUEST_VOTE_REPLY has higher term from id=%u, converting.\n",
                            client->server_id);
                    raft_update_term(term);
                    raft_change_to(RAFT_STATE_FOLLOWER);
                    raft_stop_election(); /* triggers raft_reset_election_timer() */
                    raft_update_leader_id(NULL_ID, true);
                    break;
                }

                /* this must happen after the above check */
                if (raft_state.state != RAFT_STATE_CANDIDATE)
                    break;

                rdbg_printf("RAFT raft_process_packet: REQUEST_VOTE_REPLY: %u voted %s\n",
                        client->server_id, raft_status_str[status]);

                client->vote_responded = true;

                if (status == RAFT_TRUE) {
                    client->vote_granted = raft_state.self_id;
                } else if (voted_for != NULL_ID) {
                    client->vote_granted = voted_for;
                }

                unsigned votes = 0;
                const unsigned need = (raft_num_peers/2) + 1;
#ifdef FEATURE_RAFT_DEBUG
                const unsigned total = raft_num_peers;
                unsigned has_voted = 1;
#endif

                for (unsigned idx = 1; idx < raft_num_peers; idx++)
                {
#ifdef FEATURE_RAFT_DEBUG
                    if (raft_peers[idx].vote_responded == true)
                        has_voted++;
#endif
                    if (raft_peers[idx].vote_granted == raft_state.self_id)
                        votes++;
                }

                if (raft_state.self_id == raft_state.voted_for)
                    votes++;

                rdbg_printf(BCYN "RAFT raft_process_packet: REQUEST_VOTE_REPLY: %d/%d votes. %d voted. need %d" CRESET "\n",
                        votes, total, has_voted, need);

                if (votes >= need) {
                    rdbg_printf(BYEL "RAFT raft_process_packet: REQUEST_VOTE_REPLY: i have won!" CRESET "\n");
                    raft_update_leader_id(raft_state.self_id, true);
                    raft_change_to(RAFT_STATE_LEADER);
                    raft_stop_election();
                }
            }
            break;

        default:
            warnx("raft_process_packet: unknown type: %d", rpc);
            goto fail;
    }

done:
    if (log_entry) {
        raft_free_log(log_entry);
        log_entry = NULL;
    }

    while (log_entry_head)
    {
        struct raft_log *next = log_entry_head->next;
        raft_free_log(log_entry_head);
        log_entry_head = next;
    }

    return 0;

fail:
    if (out) {
        raft_free_log(out);
    }
    if (log_entry) {
        raft_free_log(log_entry);
        log_entry = NULL;
    }

    while (log_entry_head)
    {
        struct raft_log *next = log_entry_head->next;
        raft_free_log(log_entry_head);
        log_entry_head = next;
    }

    return -1;
}

/**
 * handles the (primary) raft state machine for reads.
 * TODO: consider merging this with the raft_new_conn() state machine
 */
[[gnu::nonnull]]
static int raft_recv(struct raft_host_entry *client)
{
    ssize_t rc;
    uint8_t *ptr = NULL;
    const int *fd = &client->peer_fd;

    if (*fd == -1) {
        errno = EBADF;
        raft_close(client);
        return -1;
    }

    /* raft_recv state machine */
    switch (client->rd_state)
    {
        /* Start a new state machine */
        case RAFT_PCK_NEW:
        case RAFT_PCK_EMPTY:
            client->rd_state = RAFT_PCK_HEADER;
            client->rd_offset = 0;
            client->rd_need = RAFT_HDR_SIZE;
            if (client->rd_packet_buffer) {
                free(client->rd_packet_buffer);
                client->rd_packet_buffer = NULL;
            }
            client->rd_packet_length = 0;

            /* fall-through */

        /* Read the fixed header */
        case RAFT_PCK_HEADER:
            ptr = client->rd_packet;

            if ((rc = read(*fd, ptr + client->rd_offset,
                            client->rd_need)) != client->rd_need) {
                if (rc == -1) {
                    if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR)
                        return 0;
                    warn("raft_recv: read(RAFT_PCK_HEADER)");
                    goto shit_packet;
                } else if (rc == 0) {
eof_recv:
                    raft_close(client);
                    rc = 0;
                    goto clean_up;
                }

                client->rd_need -= rc;
                client->rd_offset += rc;

                return 0;

shit_packet: /* common with the packet read */
                rdbg_printf("RAFT raft_recv: closing\n");
                raft_close(client);
                goto fail;
            }

            struct raft_packet packet;

            packet.rpc = *ptr++;
            packet.flags = *ptr++;
            packet.role = *ptr++;
            packet.res0 = *ptr++;
            memcpy(&packet.length, ptr, 4); ptr += 4;

            packet.length = ntohl(packet.length);

            if (packet.length < RAFT_HDR_SIZE) {
                errno = EINVAL;
                goto fail;
            }

            if (packet.length > RAFT_MAX_PACKET_SIZE) {
                errno = EOVERFLOW;
                goto fail;
            }

            if (packet.rpc >= RAFT_MAX_RPC) {
                errno = EINVAL;
                goto fail;
            }

            packet.length -= RAFT_HDR_SIZE;
            client->rd_packet_length = packet.length;

            /* This is very chatty */
            //rdbg_printf("RAFT raft_recv: type=%u length=%u\n", packet.rpc, packet.length);

            if (raft_rpc_settings[packet.rpc].min_size) {
                if (packet.length < raft_rpc_settings[packet.rpc].min_size) {
                    errno = EBADMSG;
                    goto fail;
                }
            }
            if (raft_rpc_settings[packet.rpc].max_size) {
                if (packet.length > raft_rpc_settings[packet.rpc].max_size) {
                    errno = EMSGSIZE;
                    goto fail;
                }
            }

            client->rd_rpc = packet.rpc;

            if (packet.length) {
                if ((ptr = client->rd_packet_buffer = malloc(packet.length)) == NULL)
                    goto fail;

                client->rd_state = RAFT_PCK_PACKET;
                client->rd_need = packet.length;
                client->rd_offset = 0;

                goto do_raft_pck_packet;
            }

            client->rd_state = RAFT_PCK_PROCESS;
            break;

        /* Read the variable length payload */
        case RAFT_PCK_PACKET:
do_raft_pck_packet:
            if ((rc = read(*fd, client->rd_packet_buffer + client->rd_offset,
                            client->rd_need)) != client->rd_need) {
                if (rc == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                        return 0;
                    warn("raft_recv: read(RAFT_PCK_PACKET)");
                    goto shit_packet;
                } else if (rc == 0) {
                    goto eof_recv;
                }
                client->rd_offset += rc;
                client->rd_need -= rc;
                return 0;
            }
            client->rd_state = RAFT_PCK_PROCESS;

            /* fall-through */

        /* Now skip to processing it */
        case RAFT_PCK_PROCESS:
            break;

        /* We should never be in another state */
        default:
            warnx("raft_recv: state is %u", client->rd_state);
            errno = EINVAL;
            goto fail;
    }
    assert(client->rd_state == RAFT_PCK_PROCESS);

    /* Now we've read everything, process it */
    if ((rc = raft_process_packet(client, client->rd_rpc)) == -1)
        goto fail;

    rc = 0;

clean_up:
    raft_reset_read_state(client);
    client->rd_state = RAFT_PCK_EMPTY;
    return rc;

fail:
    raft_close(client);
    rc = -1;
    goto clean_up;
}

static void raft_clean(void)
{
    struct raft_log *next, *tmp;

    rdbg_printf("RAFT raft_clean\n");

    if (raft_peers) {
        for (unsigned idx = 0; idx < raft_num_peers; idx++)
            if (raft_peers[idx].peer_fd != -1)
                close_socket(&raft_peers[idx].peer_fd);
        free(raft_peers);
    }

    for (next = NULL, tmp = raft_state.log_head; tmp; tmp = next)
    {
        next = tmp->next;
        raft_remove_log(tmp, &raft_state.log_head, &raft_state.log_tail, NULL, &raft_state.log_length);
        raft_free_log(tmp);
        tmp = NULL;
    }

    if (global_raft_fd != -1) {
        rdbg_printf("     close_socket: closing raft_fd %d\n", global_raft_fd);
        close_socket(&global_raft_fd);
    }
}

static int raft_init(void)
{
    static bool raft_init = false;
    struct sockaddr_in sin = {0};
    char bind_addr[INET_ADDRSTRLEN];

    if (raft_init == true)
        errx(EXIT_FAILURE, "raft_init: called twice");

    raft_init = true;

    errno = 0;
    logger(LOG_INFO, NULL, "raft_init: started");
    rdbg_printf("RAFT init\n");

    if (raft_peers == NULL || raft_num_peers == 0) {
        warnx("raft peers missing (single server?)");
        if ((raft_peers = malloc(sizeof(struct raft_host_entry))) == NULL)
            err(EXIT_FAILURE, "malloc(raft_host_entry)");
    }

    if ((global_raft_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
        logger(LOG_EMERG, NULL, "socket(raft): %s", strerror(errno));

    sock_linger(global_raft_fd);
    sock_reuse(global_raft_fd, 1);
    sock_nonblock(global_raft_fd);

    sin.sin_family = AF_INET;
    sin.sin_port = htons(opt_raft_port);
    sin.sin_addr.s_addr = opt_raft_listen.s_addr;

    inet_ntop(AF_INET, &sin.sin_addr, bind_addr, sizeof(bind_addr));
    logger(LOG_NOTICE, NULL, "main: raft binding to %s:%u", bind_addr, opt_raft_port);

    if (bind(global_raft_fd, (struct sockaddr *)&sin, sizeof(sin)) == -1)
        logger(LOG_EMERG, NULL, "bind(raft): %s", strerror(errno));

    if (listen(global_raft_fd, 5) == -1)
        logger(LOG_EMERG, NULL, "listen(raft): %s", strerror(errno));

    memset(&raft_state, 0, sizeof(raft_state));

    raft_state.self_id = opt_raft_id;

    raft_state.current_term = 1;            /* /\ currentTerm = [i \in Server |-> 1]        */
    raft_state.state = RAFT_STATE_FOLLOWER; /* /\ state       = [i \in Server |-> Follower] */
    raft_state.voted_for = NULL_ID;         /* /\ votedFor    = [i \in Server |-> Nil]      */

    raft_state.log_head = NULL;            /* /\ log         = [i \in Server |-> << >>]    */
    raft_state.log_tail = NULL;            /* /\ log         = [i \in Server |-> << >>]    */
    raft_state.log_length = 0;
    raft_state.last_saved_index = 0;
    raft_state.commit_index = 0;           /* /\ commitIndex = [i \in Server |-> 0]        */

    raft_client_state.current_leader_id = NULL_ID;

    for (unsigned idx = 0; idx < raft_num_peers; idx++)
    {
        raft_peers[idx].vote_responded = false;     /* /\ votesResponded = [i \in Server |-> {}] */
        raft_peers[idx].vote_granted = NULL_ID;     /* /\ votesGranted   = [i \in Server |-> {}] */
        raft_peers[idx].next_index = 1;             /* /\ nextIndex      = [i \in Server |-> [j \in Server |-> 1]] */
        raft_peers[idx].match_index = 0;            /* /\ matchIndex     = [i \in Server |-> [j \in Server |-> 0]] */

        raft_peers[idx].peer_fd = -1;
        raft_peers[idx].next_conn_attempt = timems() + rnd(RAFT_MIN_ELECTION * 2,
                RAFT_MAX_ELECTION * 3);
        if (pthread_rwlock_init(&raft_peers[idx].wr_lock, NULL) != 0)
            return -1;
        if (pthread_rwlock_init(&raft_peers[idx].ss_lock, NULL) != 0)
            return -1;
    }

    /* Index 0 is 'self' */
    raft_peers[0].server_id = opt_raft_id;
    raft_peers[0].address = opt_raft_listen;
    raft_peers[0].port = htons(opt_raft_port);
    raft_peers[0].next_conn_attempt = 0;
    raft_peers[0].mqtt_addr.s_addr = opt_listen.s_addr;
    raft_peers[0].mqtt_port = htons(opt_port);

    rdbg_printf("RAFT self   : id=%d url=%08x:%u\n", raft_state.self_id,
            htonl(opt_raft_listen.s_addr), htons(opt_raft_port));

# ifdef FEATURE_RAFT_DEBUG
    for (unsigned idx = 0; idx < raft_num_peers; idx++)
        rdbg_printf("RAFT peer[%d]: id=%d url=%08x:%u\n", idx, raft_peers[idx].server_id,
                ntohl(raft_peers[idx].address.s_addr), ntohs(raft_peers[idx].port));
# endif

    raft_reset_election_timer();

    if (pthread_mutex_init(&raft_state.lock, NULL) != 0)
        return -1;

    if (pthread_rwlock_init(&raft_client_state.lock, NULL) != 0)
        return -1;

    if (pthread_rwlock_init(&raft_client_state.log_pending_lock, NULL) != 0)
        return -1;

    rdbg_printf("RAFT init: raft_num_peers=%u\n", raft_num_peers);

    assert(raft_state.self_id > 0);

    snprintf((void *)raft_state.fn_prefix       , NAME_MAX/2      , "save_state_%u_" , raft_state.self_id);
    snprintf((void *)raft_state.fn_vars         , NAME_MAX-8      , "%svars.bin"    , raft_state.fn_prefix);
    snprintf((void *)raft_state.fn_log          , NAME_MAX-8      , "%slog.bin"     , raft_state.fn_prefix);
    snprintf((void *)raft_state.fn_vars_new     , NAME_MAX        , "%s.new"         , raft_state.fn_vars);

    if (raft_load_state_vars() == -1) {
        warn("raft_init: raft_load_state_vars");
        if (errno != ENOENT)
            return -1;
    }

    if (raft_load_state_logs() == -1) {
        warn("raft_init: raft_load_state_logs");
        if (errno != ENOENT)
            return -1;
    }

    logger(LOG_INFO, NULL, "raft_init: completed. %u peers registered", raft_num_peers);
    return global_raft_fd;
}

void *raft_loop(void *start_args)
{
    raft_thread_id = pthread_self();
    raft_impl = (const void *)start_args;
    const int raft_fd = raft_init();

    if (raft_fd == -1)
        exit(EXIT_FAILURE);

    pthread_mutex_lock(&raft_state.lock);
    while (running)
    {
        int rc;
        fd_set fds_in, fds_out, fds_exc;
        int max_fd = 0;

        FD_ZERO(&fds_in);
        FD_ZERO(&fds_out);
        FD_ZERO(&fds_exc);

        max_fd = raft_fd;

        FD_SET(raft_fd, &fds_in);

        for (unsigned idx = 0; idx < raft_num_peers; idx++)
        {
            if (raft_peers[idx].peer_fd != -1) {
                max_fd = MAX(max_fd, raft_peers[idx].peer_fd);
                FD_SET(raft_peers[idx].peer_fd, &fds_in);
                FD_SET(raft_peers[idx].peer_fd, &fds_exc);
                if (raft_has_pending_write(&raft_peers[idx]))
                    FD_SET(raft_peers[idx].peer_fd, &fds_out);
            }
        }
        for (struct raft_host_entry *tmp = raft_client_state.unknown_clients; tmp; tmp = tmp->unknown_next)
        {
            if (tmp->peer_fd != -1) {
                max_fd = MAX(max_fd, tmp->peer_fd);
                FD_SET(tmp->peer_fd, &fds_in);
                FD_SET(tmp->peer_fd, &fds_exc);
                /* no writes yet */
                //if (tmp->wr_packet_buffer)
                //    FD_SET(tmp->peer_fd, &fds_out);

            }
        }

        struct timeval timeout = {
            .tv_usec = 25000,
            .tv_sec = 0,
        };

        pthread_mutex_unlock(&raft_state.lock);
        if ((rc = select(max_fd + 1, &fds_in, &fds_out, &fds_exc, &timeout)) == -1) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                pthread_mutex_lock(&raft_state.lock);
                continue;
            }
            logger(LOG_WARNING, NULL, "raft_loop: select: %s", strerror(errno));
            sleep(1);
        }
        pthread_mutex_lock(&raft_state.lock);

        raft_tick();

        /* TODO:
         * rc == 0 needs to be checked, but needs more consideration
         * on if we want to check for write will/won't block */

        /* First, handle properly connected peers/clients */
        for (unsigned idx = 0; idx < raft_num_peers; idx++)
        {
            if (raft_peers[idx].peer_fd != -1) {
                if (FD_ISSET(raft_peers[idx].peer_fd, &fds_exc)) {
                    rdbg_printf("RAFT main_loop: fds_exc, closing peer\n");
                    raft_close(&raft_peers[idx]);
                    continue;
                }

                if (raft_has_pending_write(&raft_peers[idx]) &&
                        FD_ISSET(raft_peers[idx].peer_fd, &fds_out)) {
                    if (raft_try_write(&raft_peers[idx]) == -1)
                        if (errno != EAGAIN) {
                            rdbg_printf("RAFT main_loop: raft_try_write: %s\n", strerror(errno));
                            continue;
                        }
                }

                if (FD_ISSET(raft_peers[idx].peer_fd, &fds_in))
                    raft_recv(&raft_peers[idx]);
            }
        }

        /* Next, handle new connections we've not yet finished
         * RAFT_HELLO with */
        struct raft_host_entry *tmp_host, *next_host;
        next_host = NULL;

        for (tmp_host = raft_client_state.unknown_clients; tmp_host; tmp_host = next_host)
        {
            next_host = tmp_host->unknown_next;

            if (tmp_host->peer_fd != -1) {
                if (FD_ISSET(tmp_host->peer_fd, &fds_exc)) {
                    warnx("main_loop: tmp_host is in fds_exc");
                    raft_remove_and_free_unknown_host(tmp_host);
                    continue;
                    /* TODO
                     * currently there is no 'write' within this distinct state machine,
                     * so the below isn't needed. Probably better to merge the state machines
                     * than fix this?

                     } else if (tmp_host->wr_packet_buffer) {
                     if (raft_try_write(tmp_host) == -1)
                     if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                     rdbg_printf("RAFT main_loop: raft_try_write\n");
                     continue;
                     }
                     */
                }

                if (FD_ISSET(tmp_host->peer_fd, &fds_in)) {
                    if (raft_new_conn(-1, tmp_host, NULL, 0) == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                            continue;
                        warn("main_loop: tmp_host: raft_new_conn");
                        /* tmp_host might have been trashed by raft_new_conn() */
                    }
                }
            }
        }

        /* Finnally, handle new connections */
        if (FD_ISSET(raft_fd, &fds_in)) {
            int tmp_fd;
            struct sockaddr_in sin;
            socklen_t sin_len = sizeof(sin);

            if ((tmp_fd = accept(raft_fd, (struct sockaddr *)&sin, &sin_len)) != -1) {
                rdbg_printf("RAFT main_loop: accept fd %u\n", tmp_fd);
                if (raft_new_conn(tmp_fd, NULL, &sin, sin_len) == -1) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                        goto raft_new_out;
                    rdbg_printf("RAFT main_loop: raft_new_conn: failed: %s\n", strerror(errno));
                    close_socket(&tmp_fd);
                }
            } else
                warn("main_loop: raft accept failed");
        }
raft_new_out:
    }
    logger(LOG_INFO, NULL, "raft_loop: terminated normally");
    pthread_mutex_unlock(&raft_state.lock);

    raft_clean();
    return 0; /* this does pthread_exit() */
}

bool raft_is_leader(void)
{
    bool ret;
    pthread_mutex_lock(&raft_state.lock);
    ret = raft_state.state == RAFT_STATE_LEADER;
    pthread_mutex_unlock(&raft_state.lock);
    return ret;
}

int raft_get_leader_address(char tmpbuf[static INET_ADDRSTRLEN+1+5+1], size_t len)
{
    if (len < INET_ADDRSTRLEN+1+5+1) {
        errno = ENOSPC;
        return -1;
    }

    pthread_rwlock_rdlock(&raft_client_state.lock);
    if (raft_client_state.current_leader && raft_client_state.current_leader->mqtt_addr.s_addr) {

        char name[INET_ADDRSTRLEN];
        memset(tmpbuf, 0, len);

        if (inet_ntop(AF_INET, &raft_client_state.current_leader->mqtt_addr, name, INET_ADDRSTRLEN) != NULL) {
            snprintf(tmpbuf, len, "%s:%u", name, ntohs(raft_client_state.current_leader->mqtt_port));
            pthread_rwlock_unlock(&raft_client_state.lock);
            return 0;
        }
        warn("send_cp_connack: inet_ntop");
    }

    pthread_rwlock_unlock(&raft_client_state.lock);
    return -1;
}

#ifdef TESTS
static struct raft_host_entry **raft_test_peers_ptr(void)
{
    return &raft_peers;
}

static unsigned *raft_test_num_peers_ptr(void)
{
    return &raft_num_peers;
}

static struct raft_client_state *raft_test_client_state_ptr(void)
{
    return &raft_client_state;
}

const struct raft_test_api raft_test_api = {
    .rnd = rnd,
    .timems = timems,
    .raft_save_state = raft_save_state,
    .raft_load_state = raft_load_state,
    .raft_reset_read_state = raft_reset_read_state,
    .raft_has_pending_write = raft_has_pending_write,
    .raft_clear_active_write = raft_clear_active_write,
    .raft_reset_write_state = raft_reset_write_state,
    .raft_reset_ss_state = raft_reset_ss_state,
    .raft_update_leader_id = raft_update_leader_id,
    .raft_update_term = raft_update_term,
    .raft_log_at = raft_log_at,
    .raft_term_at = raft_term_at,
    .raft_close = raft_close,
    .raft_alloc_log = raft_alloc_log,
    .raft_free_log = raft_free_log,
    .raft_remove_log = raft_remove_log,
    .raft_append_log = raft_append_log,
    .raft_prepend_log = raft_prepend_log,
    .raft_commit_and_advance = raft_commit_and_advance,
    .raft_check_commit_index = raft_check_commit_index,
    .raft_await_client_response = raft_await_client_response,
    .raft_client_log_sendv = raft_client_log_sendv,
    .raft_client_log_append_single = raft_client_log_append_single,
    .raft_remove_and_free_unknown_host = raft_remove_and_free_unknown_host,
    .raft_new_conn = raft_new_conn,
    .raft_add_write = raft_add_write,
    .raft_try_write = raft_try_write,
    .raft_append_iobuf = raft_append_iobuf,
    .raft_remove_iobuf = raft_remove_iobuf,
    .raft_reset_election_timer = raft_reset_election_timer,
    .raft_reset_next_ping = raft_reset_next_ping,
    .raft_change_to = raft_change_to,
    .raft_tick_connection_check = raft_tick_connection_check,
    .raft_stop_election = raft_stop_election,
    .raft_request_votes = raft_request_votes,
    .raft_start_election = raft_start_election,
    .raft_tick = raft_tick,
    .process_append_entries = process_append_entries,
    .raft_process_packet = raft_process_packet,
    .raft_recv = raft_recv,
    .raft_clean = raft_clean,
    .raft_init = raft_init,
    .peers_ptr = raft_test_peers_ptr,
    .num_peers_ptr = raft_test_num_peers_ptr,
    .client_state_ptr = raft_test_client_state_ptr,
};
#endif
