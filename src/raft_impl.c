#define _XOPEN_SOURCE 800

#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <err.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>

#include "config.h"
#include "mqtt.h"

#if defined(FEATURE_RAFT_DEBUG)
# define CRESET "\x1b[0m"

# define BBLU "\x1b[1;34m"
# define BCYN "\x1b[1;36m"
# define BGRN "\x1b[1;32m"
# define BMAG "\x1b[1;35m"
# define BRED "\x1b[1;31m"
# define BWHT "\x1b[1;37m"
# define BYEL "\x1b[1;33m"

# define NBLU "\x1b[0;34m"
# define NCYN "\x1b[0;36m"
# define NGRN "\x1b[0;32m"
# define NMAG "\x1b[0;35m"
# define NRED "\x1b[0;31m"
# define NWHT "\x1b[0;37m"
# define NYEL "\x1b[0;33m"

#endif

#ifndef FEATURE_RAFT_DEBUG
# define rdbg_printf(...) { }
# define rdbg_cprintf(...) { }
#else
# define rdbg_printf(...) { long dbg_now = timems(); printf("%lu.%04lu: ", dbg_now / 1000, dbg_now % 1000); printf(__VA_ARGS__); }
# define rdbg_cprintf(...) { printf(__VA_ARGS__); }
extern const char *uuid_to_string(const uint8_t uuid[const static UUID_SIZE]);
#endif

extern struct message *find_message_by_uuid(const uint8_t uuid[static const UUID_SIZE]);
extern struct topic *find_topic(const uint8_t *name, bool active_only, bool need_lock);
extern int save_topic(const struct topic *topic);
[[gnu::nonnull(1),gnu::warn_unused_result]] struct topic *register_topic(const uint8_t *name,
        const uint8_t uuid[const UUID_SIZE]
#ifdef FEATURE_RAFT
        , bool source_self
#endif
        );
extern int raft_send(raft_conn_t mode, struct raft_host_entry *client, raft_rpc_t rpc, ...);
extern int raft_leader_log_append(raft_log_t event, ...);


extern pthread_rwlock_t global_topics_lock;
extern bool opt_database;
extern struct raft_state raft_state;


[[maybe_unused]] inline static int64_t timems(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
        return -1;

    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

static int free_log(struct raft_log *entry)
{
    if (entry->register_topic.name)
        free(entry->register_topic.name);

    return 0;
}

static int commit_and_advance(struct raft_log *log_entry)
{
    struct topic *topic = NULL;
    struct message *message = NULL;

    if (log_entry->register_topic.retained)
        if ((message = find_message_by_uuid(log_entry->register_topic.msg_uuid)) == NULL) {
            errno = ENOENT;
            warn("raft_commit_and_advance: can't find retained message for UUID");
            goto fail;
        }

    pthread_rwlock_wrlock(&global_topics_lock);
    if ((topic = find_topic(log_entry->register_topic.name, false, false)) != NULL) {
        if (topic->state != TOPIC_PREACTIVE)
            goto fail;
        topic->state = TOPIC_ACTIVE;
        pthread_rwlock_unlock(&global_topics_lock);
        rdbg_printf("RAFT raft_commit_and_advance: activated topic <%s>\n", log_entry->register_topic.name);
    } else {
        pthread_rwlock_unlock(&global_topics_lock);

        if ((topic = register_topic(log_entry->register_topic.name, log_entry->register_topic.uuid, false)) == NULL) {
            warn("raft_commit_and_advance: register_topic");
            goto fail;
        } else {
            rdbg_printf("RAFT raft_commit_and_advance: registered new topic <%s>\n", log_entry->register_topic.name);
        }
    }

    if (message && topic->retained_message == message) {
        warn("raft_commit_and_advance: somehow the topic already has the message");
        goto fail;
    }

    topic->retained_message = message;

    /* TODO what about message->topic? */

    if (opt_database)
        save_topic(topic);

    return 0;

fail:
    return -1;
}

static int leader_append(struct raft_log *new_log, va_list ap)
{
    new_log->register_topic.name = (void *)strdup((void *)va_arg(ap, uint8_t *));
    if (new_log->register_topic.name == NULL)
        goto fail;
    new_log->register_topic.length = strlen((void *)new_log->register_topic.name);

    memcpy(&new_log->register_topic.uuid, va_arg(ap, uint8_t *), UUID_SIZE);

    new_log->register_topic.flags = va_arg(ap, uint32_t);
    new_log->register_topic.retained = (new_log->register_topic.flags &
            RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED);

    if (new_log->register_topic.retained)
        memcpy(&new_log->register_topic.msg_uuid, va_arg(ap, uint8_t *), UUID_SIZE);
    else
        memset(&new_log->register_topic.msg_uuid, 0, UUID_SIZE);
    return 0;
fail:
    return -1;
}

static int client_append(struct raft_log *new_client_event, raft_log_t event, va_list ap)
{
    int rc = 0;

    uint8_t *str      = va_arg(ap, uint8_t *);
    uint8_t *uuid     = va_arg(ap, void *);
    uint32_t flags    = va_arg(ap, uint32_t);
    uint8_t *msg_uuid = NULL;
    
    if (flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED)
        msg_uuid      = va_arg(ap, void *);

    if ((new_client_event->register_topic.name = (void *)strdup((void *)str)) == NULL) {
        warn("raft_client_log_sendv: strdup");
        goto fail;
    }
    new_client_event->register_topic.length = strlen((void *)str);
    memcpy(&new_client_event->register_topic.uuid, uuid, UUID_SIZE);
    new_client_event->register_topic.flags = flags;
    if (msg_uuid)
        memcpy(&new_client_event->register_topic.msg_uuid, msg_uuid, UUID_SIZE);
    else
        memset(&new_client_event->register_topic.msg_uuid, 0, UUID_SIZE);

    if ((rc = raft_send(RAFT_CLIENT, NULL,
                    RAFT_CLIENT_REQUEST,
                    raft_state.self_id, new_client_event->sequence_num,
                    event,
                    new_client_event
                    )) == -1) {
        warn("raft_client_log_sendv: raft_send");
    }

    return rc;
fail:
    return -1;
}

static int pre_send(struct raft_log *arg_event, struct send_state *out)
{
    out->arg_req_len = 0;
    out->arg_str = arg_event->register_topic.name;
    out->arg_uuid = arg_event->register_topic.uuid;
    out->arg_flags = arg_event->register_topic.flags;

    out->arg_req_len += sizeof(uint16_t); /* strlen */
    out->arg_req_len += strlen((const void *)out->arg_str) + 1;
    out->arg_req_len += UUID_SIZE;
    out->arg_req_len += sizeof(uint32_t); /* flags */

    if (out->arg_flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED) {
        out->arg_msg_uuid = arg_event->register_topic.msg_uuid;
        out->arg_req_len += UUID_SIZE;
    } else
        out->arg_msg_uuid = NULL;

    return 0;
}

static int fill_send(struct send_state *out, const struct raft_log *tmp)
{
    uint16_t entry_length = 0;

    const size_t tmp_len     = strlen((void *)out->arg_str);
    const uint16_t len       = htons(tmp_len);
    const bool retained_uuid = (out->arg_flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED);
    out->arg_flags                = htonl(out->arg_flags);

    memcpy(out->ptr, &out->arg_flags, sizeof(uint32_t)) ; out->ptr += sizeof(uint32_t) ;
    memcpy(out->ptr, &len, sizeof(uint16_t))       ; out->ptr += sizeof(uint16_t) ;
    memcpy(out->ptr, out->arg_str, tmp_len)             ; out->ptr += tmp_len          ;
    *(out->ptr++) = '\0';
    memcpy(out->ptr, out->arg_uuid, UUID_SIZE)          ; out->ptr += UUID_SIZE        ;
    if (retained_uuid) {
        memcpy(out->ptr, out->arg_msg_uuid, UUID_SIZE)  ; out->ptr += UUID_SIZE        ;
    }

    rdbg_printf(BGRN "RAFT raft_send: CLIENT_REQUEST: REGISTER_TOPIC <%s, %ld, %s>" CRESET "\n",
            out->arg_str, tmp_len, uuid_to_string(out->arg_uuid));

    if (tmp) {
        entry_length += sizeof(uint32_t);
        entry_length += sizeof(uint16_t);
        entry_length += tmp->register_topic.length;
        entry_length++;
        entry_length += UUID_SIZE;
        if (tmp->register_topic.retained)
            entry_length += UUID_SIZE;
    }

    return entry_length;
}

static raft_status_t process_packet(size_t *bytes_remaining, uint8_t **ptr, raft_log_t type)
{
    uint8_t *temp_string = NULL;
    uint32_t flags;
    uint16_t tmp_len;
    uint8_t uuid[UUID_SIZE], msg_uuid[UUID_SIZE];
    raft_status_t reply = RAFT_OK;
    int rc = 0;

    if (*bytes_remaining < sizeof(uint32_t))
        goto fail;

    memcpy(&flags, *ptr, sizeof(uint32_t));
    *ptr += sizeof(uint32_t);
    *bytes_remaining -= sizeof(uint32_t);

    flags = ntohl(flags);

    if (*bytes_remaining < sizeof(uint16_t))
        goto fail;

    memcpy(&tmp_len, *ptr, sizeof(uint16_t));
    *ptr += sizeof(uint16_t);
    *bytes_remaining -= sizeof(uint16_t);

    tmp_len = ntohs(tmp_len);

    if (*bytes_remaining < (size_t)tmp_len + 1)
        goto fail;

    if (*ptr[tmp_len] != '\0')
        goto fail;

    if ((temp_string = (void *)strndup((void *)*ptr, tmp_len)) == NULL) {
        warnx("raft_recv: strndup");
        goto fail;
    }
    *ptr += tmp_len + 1;
    *bytes_remaining -= (tmp_len + 1);

    if (*bytes_remaining < UUID_SIZE)
        goto fail;

    memcpy(&uuid, *ptr, UUID_SIZE);
    *ptr += UUID_SIZE;
    *bytes_remaining -= UUID_SIZE;

    if (flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED) {
        if (*bytes_remaining < UUID_SIZE)
            goto fail;
        memcpy(&msg_uuid, *ptr, UUID_SIZE);
        *ptr += UUID_SIZE;
        *bytes_remaining -= UUID_SIZE;
    }

    if (*bytes_remaining != 0)
        goto fail;

    rdbg_printf("RAFT raft_recv: CLIENT_REQUEST: REGISTER_TOPIC(%s)\n", temp_string);
    rc = raft_leader_log_append(type,
            temp_string, uuid, flags,
            (flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED) ? msg_uuid : NULL);
    free(temp_string);

    if (rc == -1)
        reply = RAFT_ERR;

    return reply;

fail:
    if (temp_string)
        free(temp_string);

    return -1;
}

const struct raft_impl mqtt_raft_impl = {
    .free_log           = free_log,
    .commit_and_advance = commit_and_advance,
    .leader_append      = leader_append,
    .client_append      = client_append,
    .pre_send           = pre_send,
    .fill_send          = fill_send,
    .process_packet     = process_packet,

    .name = "fail-mqttd"
};
