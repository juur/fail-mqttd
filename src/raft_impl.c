#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 800
#endif

#include "config.h"

#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <err.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <assert.h>

#include "mqtt.h"
#include "raft.h"
#include "debug.h"

#ifdef FEATURE_RAFT_IMPL_DEBUG
static int64_t timems(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
        return -1;

    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}
#endif

extern struct message *find_message_by_uuid(const uint8_t uuid[static const UUID_SIZE]);
extern struct topic *find_topic_by_uuid(const uint8_t uuid[static const UUID_SIZE]);
extern struct topic *find_topic(const uint8_t *name, bool active_only, bool need_lock);
extern int save_topic(const struct topic *topic);
extern struct topic *register_topic(const uint8_t *name, const uint8_t uuid[const UUID_SIZE] RAFT_API_SOURCE_SELF);
extern int raft_send(raft_conn_t mode, struct raft_host_entry *client, raft_rpc_t rpc, ...);
extern int raft_leader_log_append(raft_log_t event, ...);

extern pthread_rwlock_t global_topics_lock;
extern bool opt_database;
extern struct raft_state raft_state;

enum { RAFT_LOG_MAX_PAYLOAD_SIZE = 0x100000 };

static const struct raft_impl_limits raft_log_settings[RAFT_MAX_LOG] ={
    [RAFT_LOG_NOOP]               = { 0, 0 },
    [RAFT_LOG_REGISTER_TOPIC]     = { RAFT_LOG_REGISTER_TOPIC_SIZE     , RAFT_LOG_MAX_PAYLOAD_SIZE        } ,
    [RAFT_LOG_UNREGISTER_TOPIC]   = { RAFT_LOG_UNREGISTER_TOPIC_SIZE   , RAFT_LOG_UNREGISTER_TOPIC_SIZE   } ,
    [RAFT_LOG_REGISTER_SESSION]   = { RAFT_LOG_REGISTER_SESSION_SIZE   , RAFT_LOG_MAX_PAYLOAD_SIZE        } ,
    [RAFT_LOG_UNREGISTER_SESSION] = { RAFT_LOG_UNREGISTER_SESSION_SIZE , RAFT_LOG_UNREGISTER_SESSION_SIZE } ,
};

/* helper functions */

static inline void write_u32(struct send_state *out, const uint32_t val)
{
    memcpy(out->ptr, &val, sizeof(val));
    out->ptr += sizeof(val);
}

static inline void write_u16(struct send_state *out, const uint16_t val)
{
    memcpy(out->ptr, &val, sizeof(val));
    out->ptr += sizeof(val);
}

static inline void write_str(struct send_state *out, const void *src, size_t len)
{
    memcpy(out->ptr, src, len);
    out->ptr += len;
}

static inline void write_uuid(struct send_state *out, const uint8_t *src)
{
    memcpy(out->ptr, src, UUID_SIZE);
    out->ptr += UUID_SIZE;
}


static int read_u16(uint16_t *dest, const uint8_t **ptr, size_t *bytes_remaining)
{
    memcpy(dest, *ptr, sizeof(*dest));
    *dest = ntohs(*dest);
    (*ptr) += sizeof(*dest);
    (*bytes_remaining) -= sizeof(*dest);

    return 0;
}

static int read_u32(uint32_t *dest, const uint8_t **ptr, size_t *bytes_remaining)
{
    memcpy(dest, *ptr, sizeof(*dest));
    *dest = ntohl(*dest);
    (*ptr) += sizeof(*dest);
    (*bytes_remaining) -= sizeof(*dest);

    return 0;
}

static int read_uuid(uint8_t dest[static UUID_SIZE], const uint8_t **ptr, size_t *bytes_remaining)
{
    if (*bytes_remaining < UUID_SIZE) {
        errno = EMSGSIZE;
        return -1;
    }

    memcpy(dest, *ptr, UUID_SIZE);
    (*ptr) += UUID_SIZE;
    (*bytes_remaining) -= UUID_SIZE;

    return 0;
}

static int read_str(uint8_t **string, const uint8_t **ptr, size_t length, size_t *bytes_remaining)
{
    if (*bytes_remaining < length) {
        errno = EMSGSIZE;
        return -1;
    }

    if ((*string = (void *)strndup((void *)*ptr, length)) == NULL)
        return -1;

    *bytes_remaining -= length;
    *ptr += length;
    return 0;
}

/* implementation */

static int free_log(struct raft_log *lg, raft_log_t event)
{
    union raft_log_options *entry = &lg->opt;

    switch(event)
    {
        case RAFT_LOG_REGISTER_TOPIC:
            if (entry->register_topic.name) {
                free(entry->register_topic.name);
                entry->register_topic.name = NULL;
            }
            break;

        case RAFT_LOG_REGISTER_SESSION:
            if (entry->register_session.client_id) {
                free(entry->register_session.client_id);
                entry->register_session.client_id = NULL;
            }
            break;

        default:
            break;
    }

    return 0;
}

static int unregister_topic_apply(struct raft_log *lg/*, struct raft_reply *reply */, raft_log_t /*event*/)
{
    struct topic *topic = NULL;
    union raft_log_options *log_entry = &lg->opt;

    if ((topic = find_topic_by_uuid(log_entry->unregister_topic.uuid)) == NULL) {
        errno = ENOENT;
        warn("raft_apply: can't find topic for UUID");
        goto fail;
    }

    ridbg_printf("IMPL unregister_topic_apply: dunno how to unregstier a topic\n");

    return 0;

fail:
    return -1;
}

static int register_topic_apply(struct raft_log *lg /*, struct raft_reply *reply*/, raft_log_t /*event*/)
{
    struct topic *topic = NULL;
    struct message *message = NULL;
    union raft_log_options *log_entry = &lg->opt;

    if (log_entry->register_topic.retained)
        if ((message = find_message_by_uuid(log_entry->register_topic.msg_uuid)) == NULL) {
            errno = ENOENT;
            warn("raft_apply: can't find retained message for UUID");
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
        ridbg_printf("IMPL raft_apply: activated topic <%s>\n", log_entry->register_topic.name);
    } else {
        pthread_rwlock_unlock(&global_topics_lock);

        if ((topic = register_topic(log_entry->register_topic.name, log_entry->register_topic.uuid, false)) == NULL) {
            warn("raft_apply: register_topic");
            goto fail;
        } else {
            ridbg_printf("IMPL raft_apply: registered new topic <%s>\n", log_entry->register_topic.name);
        }
    }

    if (message && topic->retained_message == message) {
        warn("raft_apply: somehow the topic already has the message");
        goto fail;
    }

    topic->retained_message = message;

    /* TODO what about message->topic? */

    if (opt_database)
        save_topic(topic);

    //memcpy(&reply->opt.register_topic.uuid, &topic->uuid, UUID_SIZE);

    return 0;

fail:
    return -1;
}

static int unregister_topic_append(struct raft_log *lg, raft_log_t /* event */, va_list ap)
{
    union raft_log_options *new_log = &lg->opt;

    memcpy(&new_log->unregister_topic.uuid, va_arg(ap, uint8_t *), UUID_SIZE);

    return 0;
}

static int register_session_append(struct raft_log *lg, raft_log_t /* event */, va_list ap)
{
    union raft_log_options *new_event = &lg->opt;

    struct session *session = va_arg(ap, void *);

    new_event->register_session.flags = 0;
    if (session->request_problem_information)
        new_event->register_session.flags |= RAFT_LOG_REGISTER_SESSION_REQ_PROB_INFO;
    if (session->request_response_information)
        new_event->register_session.flags |= RAFT_LOG_REGISTER_SESSION_REQ_RESP_INFO;
    memcpy(&new_event->register_session.uuid, &session->uuid, UUID_SIZE);
    new_event->register_session.client_id = (void *)strdup((const void *)session->client_id);
    new_event->register_session.client_id_length = strlen((const void *)session->client_id);
    new_event->register_session.expiry_interval = session->expiry_interval;
    new_event->register_session.last_connected = session->last_connected;
    return 0;
}

static int register_topic_append(struct raft_log *lg, raft_log_t /* event */, va_list ap)
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

    ridbg_printf("IMPL register_topic_append: returning %u\n", rc);

    return rc;
fail:
    return -1;
}

static int register_session_size_send(struct raft_log *lg, struct send_state *out, raft_log_t /*event*/)
{
    union raft_log_options *arg_event = &lg->opt;

    out->arg_req_len += arg_event->register_session.client_id_length;

    return 0;
}

static int register_topic_size_send(struct raft_log *lg, struct send_state *out, raft_log_t /*event*/)
{
    union raft_log_options *arg_event = &lg->opt;

    out->arg_str = arg_event->register_topic.name;
    out->arg_uuid = arg_event->register_topic.uuid;
    out->arg_flags = arg_event->register_topic.flags;

    out->arg_req_len += strlen((const void *)out->arg_str);

    if (out->arg_flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED) {
        out->arg_msg_uuid = arg_event->register_topic.msg_uuid;
        out->arg_req_len += UUID_SIZE;
    } else
        out->arg_msg_uuid = NULL;

    ridbg_printf("IMPL register_topic_size_send: length now %u\n", out->arg_req_len);

    return 0;
}

static int unregister_topic_fill_send(struct send_state *out, const struct raft_log * /* lg */, raft_log_t /*event*/)
{
    write_uuid(out, out->arg_uuid);

    return UUID_SIZE;
}

static int register_session_fill_send(struct send_state * /* out */, const struct raft_log *lg, raft_log_t /*event*/)
{
    [[maybe_unused]] const union raft_log_options *tmp = &lg->opt;
    int entry_length = 0;

    return entry_length;
}

static int register_topic_fill_send(struct send_state *out, const struct raft_log *lg, raft_log_t /*event*/)
{
    const union raft_log_options *tmp = &lg->opt;
    const bool retained_uuid = (out->arg_flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED);
    uint16_t entry_length = 0;

    write_u32(out, htons(out->arg_flags));
    write_u16(out, htons(strlen((void *)out->arg_str)));
    write_uuid(out, out->arg_uuid);
    write_str(out, out->arg_str, strlen((void *)out->arg_str));
    if (retained_uuid)
        write_uuid(out, out->arg_msg_uuid);

    entry_length += sizeof(uint32_t);
    entry_length += sizeof(uint16_t);
    entry_length += tmp->register_topic.length;
    entry_length += UUID_SIZE;
    if (tmp->register_topic.retained)
        entry_length += UUID_SIZE;

    ridbg_printf("IMPL register_topic_fill_send: returning %u\n", entry_length);

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

static raft_status_t register_session_process_packet(size_t *bytes_remaining, const uint8_t **ptr,
        [[maybe_unused]] raft_rpc_t rpc, raft_log_t /* type */, struct raft_log *lg)
{
    if (lg == NULL) {
        errno = EINVAL;
        goto fail;
    }

    union raft_log_options *out = &lg->opt;
    raft_status_t reply = RAFT_OK;
    int rc = 0;

    read_u16(&out->register_session.flags, ptr, bytes_remaining);
    read_u32(&out->register_session.expiry_interval, ptr, bytes_remaining);
    read_uuid(out->register_session.uuid, ptr, bytes_remaining);
    read_u32(&out->register_session.last_connected, ptr, bytes_remaining);
    read_u16(&out->register_session.client_id_length, ptr, bytes_remaining);

    if (out->register_session.client_id_length > *bytes_remaining) {
        errno = EMSGSIZE;
        goto fail;
    }

    if (read_str(&out->register_session.client_id, ptr,
                out->register_session.client_id_length, bytes_remaining) == -1)
        goto fail;

    if (rc == -1)
        reply = RAFT_ERR;

    return reply;

fail:
    return -1;
}

static raft_status_t register_topic_process_packet(size_t *bytes_remaining, const uint8_t **ptr,
        [[maybe_unused]] raft_rpc_t rpc, raft_log_t /* type */, struct raft_log *lg)
{
    union raft_log_options *out = &lg->opt;
    uint8_t *temp_string = NULL;
    raft_status_t reply = RAFT_OK;
    int rc = 0;

    ridbg_printf("IMPL register_topic_process_packet: bytes_remaining=%lu\n",
            *bytes_remaining);

    read_u32(&out->register_topic.flags, ptr, bytes_remaining);
    read_u16(&out->register_topic.length, ptr, bytes_remaining);
    read_uuid(out->register_topic.uuid, ptr, bytes_remaining);

    if (read_str(&out->register_topic.name, ptr, out->register_topic.length,
            bytes_remaining) == -1)
        goto fail;

    out->register_topic.retained = (out->register_topic.flags & RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED);

    if (out->register_topic.retained)
        if (read_uuid(out->register_topic.msg_uuid, ptr, bytes_remaining) == -1)
            goto fail;

    ridbg_printf("IMPL process_packet: %s: REGISTER_TOPIC(%s)\n", raft_rpc_str[rpc], temp_string);
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

static int save_log(const struct raft_log *lg, uint8_t **event_buf, raft_log_t /*event*/)
{
    int rc = 0;
    errno = EINVAL;
    uint8_t *ret = NULL, *ptr = NULL;

    if (event_buf == NULL)
        goto fail;

    const union raft_log_options *l = &lg->opt;

    switch (lg->log_type)
    {
        case RAFT_LOG_NOOP:
            rc = 2+4+UUID_SIZE+4+2+l->register_session.client_id_length;
            if ((ptr = ret = malloc(rc)) == NULL)
                goto fail;
            memcpy(ptr, &l->register_session.flags, 2); ptr += 2 ;
            memcpy(ptr, &l->register_session.expiry_interval, 4); ptr += 4;
            memcpy(ptr, &l->register_session.uuid, UUID_SIZE); ptr += UUID_SIZE;
            memcpy(ptr, &l->register_session.last_connected, 4); ptr += 4;
            memcpy(ptr, &l->register_session.client_id_length, 2); ptr += 2;
            strncpy((void *)ptr, (const void *)&l->register_session.client_id, l->register_session.client_id_length);
            ptr += l->register_session.client_id_length ;
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
            warnx("save_log: unknown event %d", lg->log_type);
            goto fail;
    }

    *event_buf = ret;
    errno = 0;
    return rc;

fail:
    if (ret)
        free(ret);
    *event_buf = NULL;

    return -1;
}

static int read_log(struct raft_log *lg, const uint8_t *event_buf, int len, raft_log_t /*event*/)
{
    const uint8_t *ptr = event_buf;
    union raft_log_options *l = &lg->opt;
    errno = EINVAL;
    size_t bytes_remaining = len;

    if (event_buf == NULL || len == 0)
        goto fail;

    switch (lg->log_type)
    {
        case RAFT_LOG_NOOP:
            break;

        case RAFT_LOG_REGISTER_SESSION:
            if (bytes_remaining < (2+4+UUID_SIZE+4+2))
                goto fail;
            memcpy(&l->register_session.flags, ptr, 2)            ; ptr += 2 ; bytes_remaining -= 2 ;
            memcpy(&l->register_session.expiry_interval, ptr, 4)  ; ptr += 4 ; bytes_remaining -= 4 ;
            memcpy(&l->register_session.uuid, ptr, UUID_SIZE)     ; ptr += UUID_SIZE ; bytes_remaining -= UUID_SIZE ;
            memcpy(&l->register_session.expiry_interval, ptr, 4)  ; ptr += 4 ; bytes_remaining -= 4 ;
            memcpy(&l->register_session.client_id_length, ptr, 2) ; ptr += 2 ; bytes_remaining -= 2 ;

            if (bytes_remaining < l->register_session.client_id_length)
                goto fail;

            if ((l->register_session.client_id = (void *)strndup((const void *)ptr,
                            l->register_session.client_id_length)) == NULL)
                goto fail;

            if (l->register_session.client_id[l->register_session.client_id_length] != '\0')
                goto fail;
            break;

        case RAFT_LOG_UNREGISTER_SESSION:
            if (bytes_remaining < UUID_SIZE)
                goto fail;

            memcpy(&l->unregister_session.uuid, ptr, UUID_SIZE) ; ptr += UUID_SIZE ; bytes_remaining -= UUID_SIZE;
            break;

        case RAFT_LOG_UNREGISTER_TOPIC:
            if (bytes_remaining < UUID_SIZE)
                goto fail;

            memcpy(&l->unregister_topic.uuid, ptr, UUID_SIZE) ; ptr += UUID_SIZE ; bytes_remaining -= UUID_SIZE;

            ridbg_printf("IMPL read_log: UNREGISTER_TOPIC <%s>\n", uuid_to_string(l->unregister_topic.uuid));
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

            ridbg_printf("IMPL read_log: REGISTER_TOPIC <%s>\n",
                    l->register_topic.name);
            assert(ptr == (event_buf + len));
            break;

        default:
            warnx("read_log: unknown event %d", lg->log_type);
            goto fail;
    }

    return 0;

fail:
    switch (lg->log_type)
    {
        case RAFT_LOG_REGISTER_TOPIC:
            if (l->register_topic.name) {
                free(l->register_topic.name);
                l->register_topic.name = NULL;
                l->register_topic.length = 0;
            }
            break;

        case RAFT_LOG_REGISTER_SESSION:
            if (l->register_session.client_id) {
                free(l->register_session.client_id);
                l->register_session.client_id = NULL;
                l->register_session.client_id_length = 0;
            }

        default:
            break;
    }
    return -1;
}

const struct raft_impl mqtt_raft_impl = {
    .name          = "fail-mqttd",
    .num_log_types = RAFT_MAX_LOG,
    .limits        = raft_log_settings,
    .handlers      = {
        [RAFT_LOG_NOOP] = {
            .save_log           = save_log,
            .read_log           = read_log,
        },
        [RAFT_LOG_REGISTER_TOPIC] = {
            .free_log           = free_log,
            .apply              = register_topic_apply,
            .leader_append      = register_topic_append,
            .client_append      = register_topic_append,
            .size_send          = register_topic_size_send,
            .fill_send          = register_topic_fill_send,
            .process_packet     = register_topic_process_packet,
            .save_log           = save_log,
            .read_log           = read_log,
        },
        [RAFT_LOG_UNREGISTER_TOPIC] = {
            .free_log           = free_log,
            .apply              = unregister_topic_apply,
            .leader_append      = unregister_topic_append,
            .client_append      = unregister_topic_append,
            .fill_send          = unregister_topic_fill_send,
            .process_packet     = unregister_topic_process_packet,
            .save_log           = save_log,
            .read_log           = read_log,

        },
        [RAFT_LOG_REGISTER_SESSION] = {
            .leader_append      = register_session_append,
            .client_append      = register_session_append,
            .size_send          = register_session_size_send,
            .fill_send          = register_session_fill_send,
            .process_packet     = register_session_process_packet,
            .free_log           = free_log,
            .save_log           = save_log,
            .read_log           = read_log,
        },
        [RAFT_LOG_UNREGISTER_SESSION] = {
            .free_log           = free_log,
            .save_log           = save_log,
            .read_log           = read_log,
        },
        { NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL }
    },
};
