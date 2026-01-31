#define _XOPEN_SOURCE 800

#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <err.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <assert.h>

#include "config.h"
#include "mqtt.h"
#include "raft.h"

#if defined(FEATURE_RAFT_IMPL_DEBUG)
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

#ifndef FEATURE_RAFT_IMPL_DEBUG
# define rdbg_printf(...) { }
# define rdbg_cprintf(...) { }
#else
# define rdbg_printf(...) { long dbg_now = timems(); printf("%lu.%04lu: ", dbg_now / 1000, dbg_now % 1000); printf(__VA_ARGS__); }
# define rdbg_cprintf(...) { printf(__VA_ARGS__); }
extern const char *uuid_to_string(const uint8_t uuid[const static UUID_SIZE]);
#endif

extern struct message *find_message_by_uuid(const uint8_t uuid[static const UUID_SIZE]);
extern struct topic *find_topic_by_uuid(const uint8_t uuid[static const UUID_SIZE]);
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

static int free_log(struct raft_log *lg)
{
    union raft_log_options *entry = &lg->opt;

    switch(lg->event)
    {
        case RAFT_LOG_REGISTER_TOPIC:
            if (entry->register_topic.name)
                free(entry->register_topic.name);
            break;

        default:
            break;
    }

    return 0;
}

static int unregister_topic_commit_and_advance(struct raft_log *lg)
{
    struct topic *topic = NULL;
    union raft_log_options *log_entry = &lg->opt;

    if ((topic = find_topic_by_uuid(log_entry->unregister_topic.uuid)) == NULL) {
        errno = ENOENT;
        warn("raft_commit_and_advance: can't find topic for UUID");
        goto fail;
    }
    
    rdbg_printf("IMPL unregister_topic_commit_and_advance: dunno how to unregstier a topic\n");

    return 0;

fail:
    return -1;
}

static int register_topic_commit_and_advance(struct raft_log *lg)
{
    struct topic *topic = NULL;
    struct message *message = NULL;
    union raft_log_options *log_entry = &lg->opt;

    if (log_entry->register_topic.retained)
        if ((message = find_message_by_uuid(log_entry->register_topic.msg_uuid)) == NULL) {
            errno = ENOENT;
            warn("raft_commit_and_advance: can't find retained message for UUID");
            goto fail;
        }

    pthread_rwlock_wrlock(&global_topics_lock);
    if ((topic = find_topic(log_entry->register_topic.name, false, false)) != NULL) {
        if (topic->state != TOPIC_PREACTIVE) {
            pthread_rwlock_unlock(&global_topics_lock);
            goto fail;
        }
        topic->state = TOPIC_ACTIVE;
        pthread_rwlock_unlock(&global_topics_lock);
        rdbg_printf("IMPL raft_commit_and_advance: activated topic <%s>\n", log_entry->register_topic.name);
    } else {
        pthread_rwlock_unlock(&global_topics_lock);

        if ((topic = register_topic(log_entry->register_topic.name, log_entry->register_topic.uuid, false)) == NULL) {
            warn("raft_commit_and_advance: register_topic");
            goto fail;
        } else {
            rdbg_printf("IMPL raft_commit_and_advance: registered new topic <%s>\n", log_entry->register_topic.name);
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

static int unregister_topic_leader_append(struct raft_log *lg, va_list ap)
{
    union raft_log_options *new_log = &lg->opt;

    memcpy(&new_log->unregister_topic.uuid, va_arg(ap, uint8_t *), UUID_SIZE);

    return 0;
}

static int register_topic_leader_append(struct raft_log *lg, va_list ap)
{
    size_t name_len;
    union raft_log_options *new_log = &lg->opt;

    new_log->register_topic.name = (void *)strdup((void *)va_arg(ap, uint8_t *));
    if (new_log->register_topic.name == NULL)
        goto fail;
    if ((name_len = strlen((void *)new_log->register_topic.name)) > UINT16_MAX) {
        errno = EOVERFLOW;
        goto fail;
    }
    new_log->register_topic.length = name_len;

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

static int unregister_topic_client_append(struct raft_log *lg, raft_log_t /* event */, va_list ap)
{
    union raft_log_options *new_client_event = &lg->opt;

    uint8_t *uuid = va_arg(ap, void *);

    memcpy(&new_client_event->unregister_topic.uuid, uuid, UUID_SIZE);

    return 0;
}

static int register_topic_client_append(struct raft_log *lg, raft_log_t /* event */, va_list ap)
{
    int rc = 0;
    size_t name_len;
    union raft_log_options *new_client_event = &lg->opt;

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
    
    if ((name_len = strlen((void *)str)) > UINT16_MAX) {
        errno = EOVERFLOW;
        goto fail;
    }
    
    new_client_event->register_topic.length = name_len;
    memcpy(&new_client_event->register_topic.uuid, uuid, UUID_SIZE);
    new_client_event->register_topic.flags = flags;
    if (msg_uuid)
        memcpy(&new_client_event->register_topic.msg_uuid, msg_uuid, UUID_SIZE);
    else
        memset(&new_client_event->register_topic.msg_uuid, 0, UUID_SIZE);

    return rc;
fail:
    return -1;
}

static int unregister_topic_pre_send(struct raft_log *lg, struct send_state *out)
{
    union raft_log_options *arg_event = &lg->opt;

    out->arg_uuid = arg_event->unregister_topic.uuid;

    return 0;
}

static int register_topic_pre_send(struct raft_log *lg, struct send_state *out)
{
    union raft_log_options *arg_event = &lg->opt;
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

static int unregister_topic_fill_send(struct send_state *out, const struct raft_log * /* lg */)
{
    memcpy(out->ptr, out->arg_uuid, UUID_SIZE) ; out->ptr += UUID_SIZE ;

    return UUID_SIZE;
}

static int register_topic_fill_send(struct send_state *out, const struct raft_log *lg)
{
    const union raft_log_options *tmp = &lg->opt;
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

    //rdbg_printf(BGRN "IMPL fill_send: CLIENT_REQUEST: REGISTER_TOPIC <%s, %ld, %s>" CRESET "\n",
    //        out->arg_str, tmp_len, uuid_to_string(out->arg_uuid));

    entry_length += sizeof(uint32_t);
    entry_length += sizeof(uint16_t);
    entry_length += tmp->register_topic.length;
    entry_length++;
    entry_length += UUID_SIZE;
    if (tmp->register_topic.retained)
        entry_length += UUID_SIZE;

    return entry_length;
}

static raft_status_t unregister_topic_process_packet(size_t *bytes_remaining, const uint8_t **ptr,
        [[maybe_unused]] raft_rpc_t rpc, raft_log_t /* type */, struct raft_log *lg)
{
    union raft_log_options *out = &lg->opt;
    uint8_t uuid[UUID_SIZE];

    if (*bytes_remaining < UUID_SIZE)
        goto fail;

    memcpy(&uuid, *ptr, UUID_SIZE);
    *ptr += UUID_SIZE;
    *bytes_remaining -= UUID_SIZE;

    if (out != NULL) {
        memcpy(out->unregister_topic.uuid, uuid, UUID_SIZE);
    }

    return 0;

fail:
    return -1;
}

static raft_status_t register_topic_process_packet(size_t *bytes_remaining, const uint8_t **ptr,
        [[maybe_unused]] raft_rpc_t rpc, raft_log_t /* type */, struct raft_log *lg)
{
    union raft_log_options *out = &lg->opt;
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

    if ((*ptr)[tmp_len] != '\0')
        goto fail;

    if ((temp_string = (void *)strndup((void *)*ptr, tmp_len)) == NULL) {
        warnx("process_packet: strndup");
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

    if (out != NULL) {
        out->register_topic.retained = (flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED);
        if ((out->register_topic.name = (void *)strdup((void *)temp_string)) == NULL)
            goto fail;

        out->register_topic.length = tmp_len;
        out->register_topic.flags = flags;
        memcpy(out->register_topic.uuid, uuid, UUID_SIZE);
        if (out->register_topic.retained)
            memcpy(out->register_topic.msg_uuid, msg_uuid, UUID_SIZE);
        else
            memset(out->register_topic.msg_uuid, 0, UUID_SIZE);
    }

    rdbg_printf("IMPL process_packet: %s: REGISTER_TOPIC(%s)\n", raft_rpc_str[rpc], temp_string);
    /*
    rc = raft_leader_log_append(type,
            temp_string, uuid, flags,
            (flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED) ? msg_uuid : NULL);
            */
    free(temp_string);

    if (rc == -1)
        reply = RAFT_ERR;

    return reply;

fail:
    if (temp_string)
        free(temp_string);

    return -1;
}

static int save_log(const struct raft_log *lg, uint8_t **event_buf)
{
    int rc = 0;
    errno = EINVAL;
    uint8_t *ret = NULL, *ptr = NULL;

    if (event_buf == NULL)
        goto fail;

    const union raft_log_options *l = &lg->opt;

    switch (lg->event)
    {
        case RAFT_LOG_NOOP:
            break;

        case RAFT_LOG_UNREGISTER_TOPIC:
            rc = UUID_SIZE;

            if ((ptr = ret = malloc(rc)) == NULL)
                goto fail;

            memcpy(ptr, &l->unregister_topic.uuid, UUID_SIZE) ; ptr += UUID_SIZE ;
            break;

        case RAFT_LOG_REGISTER_TOPIC:
            assert(l->register_topic.length > 0);

            rc = sizeof(uint32_t) + sizeof(uint16_t) + l->register_topic.length + UUID_SIZE;
            if (l->register_topic.retained)
                rc += UUID_SIZE;

            if ((ptr = ret = malloc(rc)) == NULL)
                goto fail;

            memcpy(ptr, &l->register_topic.flags, sizeof(uint32_t))       ; ptr += sizeof(uint32_t)         ; 
            memcpy(ptr, &l->register_topic.length, sizeof(uint16_t))      ; ptr += sizeof(uint16_t)         ; 
            memcpy(ptr, &l->register_topic.uuid, UUID_SIZE)               ; ptr += UUID_SIZE                ; 
            memcpy(ptr, l->register_topic.name, l->register_topic.length) ; ptr += l->register_topic.length ; 
            if (l->register_topic.retained) {
                memcpy(ptr, &l->register_topic.msg_uuid, UUID_SIZE)       ; ptr += UUID_SIZE                ;
            }
            
            assert(ptr == (ret + rc));
            break;

        default:
            warnx("save_log: unknown event %d", lg->event);
            goto fail;
    }

    *event_buf = ret;
    errno = 0;
    return rc;

fail:
    if (ret)
        free(ret);
    return -1;
}

static int read_log(struct raft_log *lg, const uint8_t *event_buf, int len)
{
    const uint8_t *ptr = event_buf;
    union raft_log_options *l = &lg->opt;
    errno = EINVAL;
    size_t bytes_remaining = len;

    //rdbg_printf("IMPL read_log: event_buf=%p len=%d\n", event_buf, len);

    if (event_buf == NULL || len == 0)
        goto fail;

    switch (lg->event)
    {
        case RAFT_LOG_NOOP:
            break;

        case RAFT_LOG_UNREGISTER_TOPIC:
            if (bytes_remaining < UUID_SIZE)
                goto fail;

            memcpy(&l->unregister_topic.uuid, ptr, UUID_SIZE) ; ptr += UUID_SIZE ; bytes_remaining -= UUID_SIZE;

            rdbg_printf("IMPL read_log: UNREGISTER_TOPIC <%s>\n", uuid_to_string(l->unregister_topic.uuid));
            break;

        case RAFT_LOG_REGISTER_TOPIC:
            if (bytes_remaining < sizeof(uint32_t) + sizeof(uint16_t) + UUID_SIZE)
                goto fail;

            memcpy(&l->register_topic.flags, ptr, sizeof(uint32_t))  ; ptr += sizeof(uint32_t) ; bytes_remaining -= sizeof(uint32_t) ;
            memcpy(&l->register_topic.length, ptr, sizeof(uint16_t)) ; ptr += sizeof(uint16_t) ; bytes_remaining -= sizeof(uint16_t) ;
            memcpy(&l->register_topic.uuid, ptr, UUID_SIZE)          ; ptr += UUID_SIZE        ; bytes_remaining -= UUID_SIZE        ;

            if (bytes_remaining < l->register_topic.length)
                goto fail;

            l->register_topic.name = (void *)strndup((const void *)ptr, l->register_topic.length);
            if (l->register_topic.name == NULL)
                goto fail;
            ptr += l->register_topic.length;
            bytes_remaining -= l->register_topic.length;

            if (l->register_topic.flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED) {
                l->register_topic.retained = true;
                if (bytes_remaining < UUID_SIZE)
                    goto fail;

                memcpy(&l->register_topic.msg_uuid, ptr, UUID_SIZE)  ; ptr += UUID_SIZE        ; bytes_remaining -= UUID_SIZE        ;
            }

            rdbg_printf("IMPL read_log: REGISTER_TOPIC [%d/%d] <%s>\n", l->index, l->term, l->register_topic.name);
            assert(ptr == (event_buf + len));
            break;

        default:
            warnx("read_log: unknown event %d", lg->event);
            goto fail;
    }

    return 0;

fail:
    if (l->register_topic.name) {
        free (l->register_topic.name);
        l->register_topic.name = NULL;
    }
    return -1;
}

const struct raft_impl mqtt_raft_impl = {
    .name = "fail-mqttd",
    .num_log_types = RAFT_MAX_LOG,
    .handlers = {
        [RAFT_LOG_NOOP] = {
            .free_log           = NULL,
            .commit_and_advance = NULL,
            .leader_append      = NULL,
            .client_append      = NULL,
            .pre_send           = NULL,
            .fill_send          = NULL,
            .process_packet     = NULL,
            .save_log           = save_log,
            .read_log           = read_log,
        },
        [RAFT_LOG_REGISTER_TOPIC] = {
            .free_log           = free_log,
            .commit_and_advance = register_topic_commit_and_advance,
            .leader_append      = register_topic_leader_append,
            .client_append      = register_topic_client_append,
            .pre_send           = register_topic_pre_send,
            .fill_send          = register_topic_fill_send,
            .process_packet     = register_topic_process_packet,
            .save_log           = save_log,
            .read_log           = read_log,
        },
        [RAFT_LOG_UNREGISTER_TOPIC] = {
            .free_log           = free_log,
            .commit_and_advance = unregister_topic_commit_and_advance,
            .leader_append      = unregister_topic_leader_append,
            .client_append      = unregister_topic_client_append,
            .pre_send           = unregister_topic_pre_send,
            .fill_send          = unregister_topic_fill_send,
            .process_packet     = unregister_topic_process_packet,
            .save_log           = save_log,
            .read_log           = read_log,

        },
        { NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL }
    },
};
