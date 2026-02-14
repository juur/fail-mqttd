#define _DUMP_C
#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 800
#endif

#include <time.h>
#include <stdio.h>
#include <pthread.h>

#include "config.h"
#include "debug.h"

[[maybe_unused]] static inline int64_t timems(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
        return -1U;

    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}


static const char *lookup_str(const char *const *table, unsigned max, unsigned val, const char *fallback)
{
    if (table && val < max && table[val])
        return table[val];
    return fallback;
}

void dump_client(const struct client *c)
{
    if (!c) {
        dbg_printf("client: <null>\n");
        return;
    }

    const char *state_str = lookup_str(client_state_str, CLIENT_STATE_MAX, c->state, "CLIENT_STATE_INVALID");
    const char *parse_str = lookup_str(read_state_str, READ_STATE_MAX, c->parse_state, "READ_STATE_INVALID");

    dbg_printf("client id=%d state=%s ref=%u session=%d fd=%d client_id=%s user=%s parse=%s\n",
            c->id,
            state_str,
            (unsigned)c->refcnt,
            c->session ? c->session->id : -1U,
            c->fd,
            c->client_id ? (const char *)c->client_id : "<null>",
            c->username ? (const char *)c->username : "<null>",
            parse_str);

    (void)state_str; (void)parse_str;
}

void dump_message(const struct message *m)
{
    if (!m) {
        dbg_printf("message: <null>\n");
        return;
    }

    const char *state_str = lookup_str(message_state_str, MSG_STATE_MAX, m->state, "MSG_STATE_INVALID");
    const char *type_str = lookup_str(message_type_str, MSG_TYPE_MAX, m->type, "MSG_TYPE_INVALID");

    dbg_printf("message id=%d state=%s ref=%u type=%s qos=%u retain=%u sender=%d topic=%d payload_len=%zu mds=%u\n",
            m->id,
            state_str,
            (unsigned)m->refcnt,
            type_str,
            m->qos,
            m->retain,
            m->sender ? m->sender->id : -1U,
            m->topic ? m->topic->id : -1U,
            m->payload_len,
            m->num_message_delivery_states);

    (void)state_str; (void)type_str;
}

void dump_mds(const struct message_delivery_state *mds)
{
    if (!mds) {
        dbg_printf("mds: <null>\n");
        return;
    }

    dbg_printf("mds id=%d state=%u ro=%u msg=%d sess=%d pid=%u last=%lu ack=%lu rel=%lu comp=%lu reason=%u\n",
            mds->id,
            mds->state,
            mds->read_only,
            mds->message ? mds->message->id : -1U,
            mds->session ? mds->session->id : -1U,
            mds->packet_identifier,
            (unsigned long)mds->last_sent,
            (unsigned long)mds->acknowledged_at,
            (unsigned long)mds->released_at,
            (unsigned long)mds->completed_at,
            (unsigned)mds->client_reason);
}

void dump_packet(const struct packet *p)
{
    if (!p) {
        dbg_printf("packet: <null>\n");
        return;
    }

    const char *type_str = lookup_str(control_packet_str, MQTT_CP_MAX, p->type, "CP_INVALID");
    const char *dir_str = lookup_str(packet_dir_str, PACKET_DIR_MAX, p->direction, "PACKET_DIR_INVALID");
    const char *reason_str = lookup_str(reason_codes_str, MQTT_REASON_CODE_MAX, p->reason_code, "REASON_INVALID");

    dbg_printf("packet id=%d dir=%s type=%s owner=%d msg=%d pid=%u rl=%zu flags=0x%x props=%u reason=%s\n",
            p->id,
            dir_str,
            type_str,
            p->owner ? p->owner->id : -1U,
            p->message ? p->message->id : -1U,
            p->packet_identifier,
            p->remaining_length,
            p->flags,
            p->property_count,
            reason_str);
    (void)type_str; (void)dir_str; (void)reason_str;
}

void dump_subscription(const struct subscription *sub)
{
    if (!sub) {
        dbg_printf(" subscription: <null>\n");
        return;
    }

    const char *type_str = lookup_str(subscription_type_str, SUB_TYPE_MAX, sub->type, "SUB_TYPE_INVALID");

    dbg_printf(" subscription id=%d type=%s topic_filter=%s opt=0x%02x sub_id=%u ",
            sub->id,
            type_str,
            sub->topic_filter ? (const char *)sub->topic_filter : "<null>",
            sub->option,
            sub->subscription_identifier);

    if (sub->type == SUB_NON_SHARED) {
        dbg_cprintf("session=%d\n",
                sub->non_shared.session ? sub->non_shared.session->id : -1U);
    } else if (sub->type == SUB_SHARED) {
        dbg_cprintf("share_name=%s sessions=%u [",
                sub->shared.share_name ? (const char *)sub->shared.share_name : "<null>",
                sub->shared.num_sessions);

        for (unsigned i = 0; i < sub->shared.num_sessions; i++) {
            struct session *s = sub->shared.sessions ? sub->shared.sessions[i] : NULL;
            dbg_cprintf("%s%d", (i ? "," : ""), s ? s->id : -1U);
            (void)s;
        }

        dbg_cprintf("]\n");
    } else {
        dbg_cprintf("session=<unknown>\n");
    }
    (void)type_str;
}

void dump_session(const struct session *s)
{
    if (!s) {
        dbg_printf("session: <null>\n");
        return;
    }

    const char *state_str = lookup_str(session_state_str, SESSION_STATE_MAX, s->state, "SESSION_STATE_INVALID");
    const char *client_state = (s->client && s->client->state < CLIENT_STATE_MAX)
        ? client_state_str[s->client->state]
        : (s->client ? "CLIENT_STATE_INVALID" : "null");

    dbg_printf("session id=%d state=%s ref=%u client=%d cstate=%s client_id=%s ",
            s->id,
            state_str,
            (unsigned)s->refcnt,
            s->client ? s->client->id : -1U,
            client_state,
            s->client_id ? (const char *)s->client_id : "<null>");

    dbg_cprintf("subs=%u mds=%u last_conn=%lu expires_at=%lu expiry_int=%u rri=%u rpi=%u ",
            s->num_subscriptions,
            s->num_message_delivery_states,
            (unsigned long)s->last_connected,
            (unsigned long)s->expires_at,
            s->expiry_interval,
            s->request_response_information,
            s->request_problem_information);

    dbg_cprintf("will: topic=%d props=%u payload_len=%zu fmt=%u qos=%u retain=%u at=%lu\n",
            s->will_topic ? s->will_topic->id : -1U,
            s->num_will_props,
            s->will_payload_len,
            s->will_payload_format,
            s->will_qos,
            s->will_retain,
            (unsigned long)s->will_at);

    pthread_rwlock_rdlock((void *)&s->subscriptions_lock);
    for (unsigned i = 0; i < s->num_subscriptions; i++) {
        if (s->subscriptions && s->subscriptions[i])
            dump_subscription(s->subscriptions[i]);
    }
    pthread_rwlock_unlock((void *)&s->subscriptions_lock);
    (void)state_str; (void)client_state;
}

void dump_topic(const struct topic *t)
{
    if (!t) {
        dbg_printf("topic: <null>\n");
        return;
    }

    const char *state_str = lookup_str(topic_state_str, TOPIC_STATE_MAX, t->state, "TOPIC_STATE_INVALID");

    dbg_printf("topic id=%d state=%s ref=%u name=%s retained=%d pending=%d\n",
            t->id,
            state_str,
            (unsigned)t->refcnt,
            t->name ? (const char *)t->name : "<null>",
            t->retained_message ? t->retained_message->id : -1U,
            t->pending_queue ? t->pending_queue->id : -1U);
    (void)state_str;
}

void dump_any(mqtt_type type, const void *ptr)
{
    switch (type) {
        case T_NULL:
            dbg_printf("null\n");
            break;
        case T_CLIENT:
            dump_client((const struct client *)ptr);
            break;
        case T_MESSAGE:
            dump_message((const struct message *)ptr);
            break;
        case T_MDS:
            dump_mds((const struct message_delivery_state *)ptr);
            break;
        case T_PACKET:
            dump_packet((const struct packet *)ptr);
            break;
        case T_SESSION:
            dump_session((const struct session *)ptr);
            break;
        case T_SUBSCRIPTION:
            dump_subscription((const struct subscription *)ptr);
            break;
        case T_TOPIC:
            dump_topic((const struct topic *)ptr);
            break;
        default:
            dbg_printf("unknown mqtt_type=%d\n", type);
            break;
    }
}
