#define _XOPEN_SOURCE 700
#include "config.h"

#include <stdlib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <err.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>
#include <sys/select.h>
#include <stdatomic.h>
#include <signal.h>
#include <assert.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <limits.h>

#include "mqtt.h"

#ifdef NDEBUG
# define dbg_printf(...)
#else
# define dbg_printf(...) printf(__VA_ARGS__)
#endif

#define CRESET "\x1b[0m"
#define BBLK "\x1b[1;30m"
#define BRED "\x1b[1;31m"
#define NRED "\x1b[0;31m"
#define BGRN "\x1b[1;32m"
#define NGRN "\x1b[0;32m"
#define BYEL "\x1b[1;33m"
#define BBLU "\x1b[1;34m"
#define BMAG "\x1b[1;35m"
#define BCYN "\x1b[1;36m"
#define NCYN "\x1b[1;36m"
#define BWHT "\x1b[1;37m"

typedef int (*control_func_t)(struct client *, struct packet *, const void *);

/*
 * misc. globals
 */

static int mother_fd = -1;
static bool running;

/*
 * unique ids
 */

static _Atomic id_t subscription_id = 1;
static _Atomic id_t session_id      = 1;
static _Atomic id_t topic_id        = 1;
static _Atomic id_t packet_id       = 1;
static _Atomic id_t message_id      = 1;
static _Atomic id_t client_id       = 1;
static _Atomic id_t mds_id          = 1;

/*
 * magic numbers
 */

static constexpr unsigned MAX_PACKETS           = 256;
static constexpr unsigned MAX_CLIETNS           = 64;
static constexpr unsigned MAX_TOPICS            = 1024;
static constexpr unsigned MAX_MESSAGES          = 16384;
static constexpr unsigned MAX_PACKET_LENGTH     = 0x1000000U;
static constexpr unsigned MAX_MESSAGES_PER_TICK = 100;
static constexpr unsigned MAX_PROPERTIES        = 32;
static constexpr unsigned MAX_RECEIVE_PUBS      = 8;
static constexpr unsigned MAX_SESSIONS          = 128;

/*
 * global lists and associated locks & counts
 */

static pthread_rwlock_t global_clients_lock  = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_sessions_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_messages_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_packets_lock  = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_topics_lock   = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_mds_lock      = PTHREAD_RWLOCK_INITIALIZER;

static struct client *global_client_list              = NULL;
static struct message *global_message_list            = NULL;
static struct packet *global_packet_list              = NULL;
static struct topic *global_topic_list                = NULL;
static struct session *global_session_list            = NULL;
static struct message_delivery_state *global_mds_list = NULL;

static unsigned num_clients  = 0;
static unsigned num_messages = 0;
static unsigned num_packets  = 0;
static unsigned num_topics   = 0;
static unsigned num_sessions = 0;
static unsigned num_mds      = 0;

/*
 * command line options
 */

static in_port_t opt_port = 1883;
static struct in_addr opt_listen = {
    .s_addr = INADDR_LOOPBACK
};

/*
 * forward declarations
 */

static int unsubscribe(struct subscription *sub);
static int dequeue_message(struct message *msg);

/*
 * command line stuff
 */

/**
 * show version to the specified FILE
 * @param fp FILE to output to.
 */
[[gnu::nonnull]]
static void show_version(FILE *fp)
{
    fprintf(fp, "fail-mqttd " VERSION "\n" "\n" "Written by http://github.com/juur");
}

/**
 * show usage information to the specified file
 * @param fp FILE to output to.
 * @param name typically argv[0] from main() to display
 */
[[gnu::nonnull]]
static void show_usage(FILE *fp, const char *name)
{
    fprintf(fp, "fail-mqttd -- a terrible implementation of MQTT\n" "\n"
            "Usage: %s [-hV] [-H ADDR] [-p PORT] [TOPIC..]\n"
            "Provides a MQTT broker, "
            "pre-creating topics per additional command line arguments, "
            "if provided.\n"
            "\n"
            "Options:\n"
            "  -h         show help\n"
            "  -p PORT    bind to TCP port PORT   (default 1883)\n"
            "  -H ADDR    bind to IP address ADDR (default 127.0.0.1)\n"
            "  -V         show version\n" "\n",
            name);
}

/*
 * debugging helpers
 */

#define log_io_error(m,r,e,d) _log_io_error(m,r,e,d,__FILE__,__func__,__LINE__);
static int _log_io_error(const char *msg, ssize_t rc, ssize_t expected, bool die,
        const char *file, const char *func, int line)
{
    if (rc == -1) {
        if (die)
            err(EXIT_FAILURE, "%s: read error at %s:%u: %s", func, file, line, msg ? msg : "");
        else
            warn("%s: read error at %s:%u: %s", func, file, line, msg ? msg : "");
        return -1;
    }

    if (die)
        errx(EXIT_FAILURE, "%s: short read (%lu < %lu) at %s:%u: %s",
                func, rc, expected, file, line, msg ? msg : "");
    else
        warnx("%s: short read (%lu < %lu) at %s:%u: %s",
                func, rc, expected, file, line, msg ? msg : "");
    errno = ERANGE;

    return -1;
}

[[maybe_unused]]
static void dump_topics(void)
{
    dbg_printf("   global_topics:\n");
    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
    {
        dbg_printf("  topic: <%s>\n", (char *)tmp->name);
    }
    pthread_rwlock_unlock(&global_topics_lock);
}

static void dump_clients(void)
{
    pthread_rwlock_rdlock(&global_clients_lock);
    for (struct client *client = global_client_list; client; client = client->next)
    {
        dbg_printf("{\"id\": %u, \"session\": {\"id\": %u}, \"client-id\": \"%s\"}%s\n",
                client->id,
                client->session->id,
                client->client_id,
                client->next ? "," : ""
              );
    }
    pthread_rwlock_unlock(&global_clients_lock);
}

static void dump_all(void)
{
    dbg_printf("{\"clients\":[\n");
    dump_clients();
    dbg_printf("]}\n");
}

/*
 * allocators / deallocators
 */

[[gnu::nonnull]]
static void close_socket(int *fd)
{
    if (*fd != -1) {
        shutdown(*fd, SHUT_RDWR);
        close(*fd);
        *fd = -1;
    }
}

[[gnu::nonnull]]
static void free_subscription(struct subscription *sub)
{
    free(sub);
}

[[gnu::nonnull]]
static void free_topic_subs(struct topic_sub_request *request)
{
    if (request->topics) {
        for (unsigned cnt = 0; cnt < request->num_topics; cnt++)
        {
            if (request->topics[cnt]) {
                free((void *)request->topics[cnt]);
                request->topics[cnt] = NULL;
            }
        }
        free(request->topics);
        request->topics = NULL;
    }

    if (request->options) {
        free(request->options);
        request->options = NULL;
    }

    if (request->response_codes) {
        free(request->response_codes);
        request->response_codes = NULL;
    }

    free(request);
}

[[gnu::nonnull]]
static void free_topic(struct topic *topic)
{
    dbg_printf("     free_topic: id=%u <%s>\n",
            topic->id,
            (topic->name == NULL) ? "" : (char *)topic->name
            );

    pthread_rwlock_wrlock(&global_topics_lock); {
        if (global_topic_list == topic) {
            global_topic_list = topic->next;
        } else for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
        {
            if (tmp->next == topic) {
                tmp->next = topic->next;
                break;
            }
        }
    } pthread_rwlock_unlock(&global_topics_lock);
    topic->next = NULL;

    /* TODO check if we should have a wrlock here,
     * not inside unsubscribe_from_topic */
    if (topic->subscribers) {
        dbg_printf("     free_topic: subscribers=%p num_subscribers=%u\n",
                (void *)topic->subscribers, topic->num_subscribers);

        /* keep going, but restart if we unsubscribe as the array
         * will be modified */
        while (topic->num_subscribers && topic->subscribers)
        {
            for(unsigned idx = 0; idx < topic->num_subscribers; idx++)
            {
                dbg_printf("     free_topic: subscriber[%u]\n", idx);
                if ((*topic->subscribers)[idx] == NULL)
                    continue;

                /* TODO should we handle the return code ? */
                (void)unsubscribe((*topic->subscribers)[idx]);
            }
        }

        /* Not sure this locking is useful */
        pthread_rwlock_wrlock(&topic->subscribers_lock); {
            free(topic->subscribers);
            topic->subscribers = NULL;
        } pthread_rwlock_unlock(&topic->subscribers_lock);
    }

    pthread_rwlock_wrlock(&topic->pending_queue_lock); {
        if (topic->pending_queue) {
            /* TODO persist */
            struct message *msg;
            while ((msg = topic->pending_queue)) {
                if (dequeue_message(msg) == -1) {
                    topic->pending_queue = msg->next_queue;
                    pthread_rwlock_unlock(&topic->pending_queue_lock);
                    err(EXIT_FAILURE, "free_topic: dequeue_message"); /* TODO y/n ? */
                }
                msg->state = MSG_DEAD;
            }
        }
    } pthread_rwlock_unlock(&topic->pending_queue_lock);

    if (topic->name) {
        free((void *)topic->name);
        topic->name = NULL;
    }

    pthread_rwlock_destroy(&topic->pending_queue_lock);
    pthread_rwlock_destroy(&topic->subscribers_lock);
    num_topics--;
    free(topic);
}

[[gnu::nonnull, gnu::access(read_write,1,2)]]
static void free_properties(
        struct property (*props)[], unsigned count)
{
    type_t type;

    for (unsigned i = 0; i < count; i++)
    {
        if ((*props)[i].ident > MQTT_MAX_PROPERTY_IDENT) /* TODO handle error */
            continue;

        type = property_to_type[(*props)[i].ident];

        switch (type)
        {
            case MQTT_TYPE_UTF8_STRING:
                if ((*props)[i].utf8_string)
                    free((*props)[i].utf8_string);
                break;

            case MQTT_TYPE_BINARY:
                if ((*props)[i].binary.data)
                    free((*props)[i].binary.data);
                break;

            default:
                break;
        }
    }
    free(props);
}

[[gnu::nonnull]]
static void free_message_delivery_state(struct message_delivery_state *mds)
{
    dbg_printf("     free_message_delivery_state: id=%u\n",
            mds->id);

    pthread_rwlock_wrlock(&global_mds_lock);
    if (global_mds_list == mds) {
        global_mds_list = mds->next;
    } else for (struct message_delivery_state *tmp = global_mds_list;
            tmp; tmp = tmp->next)
    {
        if (tmp->next == mds) {
            tmp->next = mds->next;
            break;
        }
    }
    pthread_rwlock_unlock(&global_mds_lock);
    mds->next = NULL;

    assert(mds->session == NULL);
    assert(mds->message == NULL);

    free(mds);
    num_mds--;
}

[[gnu::nonnull]]
static void free_packet(struct packet *pck, bool need_lock)
{
    struct packet *tmp;
    unsigned lck;

    dbg_printf("     free_packet: id=%u owner=%u <%s>\n",
            pck->id,
            pck->owner ? pck->owner->id : 0,
            pck->owner ? (char *)pck->owner->client_id : "");

    if ((lck = atomic_load_explicit(&pck->refcnt, memory_order_relaxed)) > 0) {
        warnx("free_packet: attempt to free with refcnt=%u", lck);
        abort();
        return;
    }

    if (pck->owner) {
        pthread_rwlock_wrlock(&pck->owner->active_packets_lock);
        if (pck->owner->active_packets == pck) {
            pck->owner->active_packets = pck->next_client;
        } else for (tmp = pck->owner->active_packets; tmp; tmp = tmp->next_client)
        {
            if (tmp->next_client == pck) {
                tmp->next_client = pck->next_client;
                break;
            }
        }
        pck->next_client = NULL;
        pthread_rwlock_unlock(&pck->owner->active_packets_lock);
        pck->owner = NULL;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_packets_lock);
    if (pck == global_packet_list) {
        global_packet_list = pck->next;
    } else for (tmp = global_packet_list; tmp; tmp = tmp->next)
    {
        if (tmp->next == pck) {
            tmp->next = pck->next;
            break;
        }
    }
    if (need_lock)
        pthread_rwlock_unlock(&global_packets_lock);
    pck->next = NULL;

    if (pck->payload) {
        free(pck->payload);
        pck->payload = NULL;
    }

    if (pck->will_props) {
        free_properties(pck->will_props, pck->num_will_props);

        pck->num_will_props = 0;
        pck->will_props = NULL;
    }

    if (pck->properties) {
        free_properties(pck->properties, pck->property_count);

        pck->property_count = 0;
        pck->properties = NULL;
    }

    if (pck->message) {
        atomic_fetch_sub_explicit(&pck->message->refcnt, 1, memory_order_acq_rel);
        pck->message = NULL;
    }

    num_packets--;
    free(pck);
}

[[gnu::nonnull]]
static void free_message(struct message *msg, bool need_lock)
{
    struct message *tmp;
    unsigned lck;

    dbg_printf("     free_message: id=%u [%s] lock=%s topic=%u <%s>\n",
            msg->id, message_state_str[msg->state],
            need_lock ? "yes" : "no",
            msg->topic ? msg->topic->id : 0,
            msg->topic ? (char *)msg->topic->name : "");

    if ((lck = atomic_load_explicit(&msg->refcnt, memory_order_relaxed)) > 0) {
        warn("free_message: attempt to free with refcnt=%u", lck);
        abort();
        return;
    }

    if (msg->topic) {
        warn("free_message: attempt to free with topic <%s> set",
                msg->topic->name);
        return;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_messages_lock);
    {
        if (global_message_list == msg) {
            global_message_list = msg->next;
        } else for (tmp = global_message_list; tmp; tmp = tmp->next) {
            if (tmp->next == msg) {
                tmp->next = msg->next;
                break;
            }
        }
    }
    if (need_lock)
        pthread_rwlock_unlock(&global_messages_lock);

    msg->next = NULL;

    if (msg->payload) {
        free((void *)msg->payload);
        msg->payload = NULL;
    }

    /* TODO do this properly */
    if (msg->delivery_states)
        free(msg->delivery_states);

    pthread_rwlock_destroy(&msg->delivery_states_lock);

    num_messages--;
    free(msg);
}

[[gnu::nonnull]]
static void free_client(struct client *client, bool needs_lock)
{
    struct client *tmp;

    dbg_printf("     free_client: id=%u [%s] lock=%s client_id=%s session=%u %s\n",
            client->id, client_state_str[client->state],
            needs_lock ? "yes" : "no",
            (char *)client->client_id,
            client->session ? client->session->id : 0,
            client->session ? (char *)client->session->client_id : "");

    if (atomic_load_explicit(&client->refcnt, memory_order_relaxed) > 0) {
        warn("free_client: attempt to free with refcnt");
        abort();
        return;
    }

    client->state = CS_CLOSED;

    if (needs_lock)
        pthread_rwlock_wrlock(&global_clients_lock);
    {

        if (global_client_list == client) {
            global_client_list = client->next;
        } else for (tmp = global_client_list; tmp; tmp = tmp->next) {
            if (tmp->next == client) {
                tmp->next = client->next;
                break;
            }
        }
    }
    if (needs_lock)
        pthread_rwlock_unlock(&global_clients_lock);

    client->next = NULL;

    pthread_rwlock_wrlock(&client->active_packets_lock);
    for (struct packet *p = client->active_packets, *next; p; p = next)
    {
        next = p->next_client;
        atomic_fetch_sub_explicit(&p->refcnt, 1, memory_order_acq_rel);
        free_packet(p, true);
    }
    client->active_packets = NULL;
    pthread_rwlock_unlock(&client->active_packets_lock);

    pthread_rwlock_wrlock(&global_packets_lock); {
        for (struct packet *p = global_packet_list; p; p = p->next)
        {
            if (p->owner == client)
                p->owner = NULL; /* TODO locking? */
        }
    } pthread_rwlock_unlock(&global_packets_lock);


    if (client->fd != -1)
        close_socket(&client->fd);

    if (client->client_id) {
        free ((void *)client->client_id);
        client->client_id = NULL;
    }

    if (client->username) {
        free ((void *)client->username);
        client->username = NULL;
    }

    if (client->password) {
        free ((void *)client->password);
        client->password = NULL;
    }



    if (client->packet_buf)
        free(client->packet_buf);

    if (client->session) {
        client->session->client = NULL;
        client->session = NULL;
    }

    pthread_rwlock_destroy(&client->active_packets_lock);

    num_clients--;
    free(client);
}

    [[gnu::nonnull]]
static void free_session(struct session *session, bool need_lock)
{
    struct session *tmp;

    dbg_printf("     free_session: session=%u <%s> [%s] client=%u <%s>\n",
            session->id,
            session->client_id, session_state_str[session->state],
            session->client ? session->client->id : 0,
            session->client ? (char *)session->client->client_id : "");

    if (atomic_load_explicit(&session->refcnt, memory_order_relaxed) > 0) {
        warn("free_session: attempt to free with refcnt");
        abort();
        return;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_sessions_lock);
    {
        if (global_session_list == session) {
            global_session_list = session->next;
        } else for (tmp = global_session_list; tmp; tmp = tmp->next) {
            if (tmp->next == session) {
                tmp->next = session->next;
                break;
            }
        }
        if (session->client) {
            warn("free_session: freeing session with connected client!");
            session->client->state = CS_CLOSED;
            close_socket(&session->client->fd);
            session->client->session = NULL;
            session->client = NULL;
        }
    }
    if (need_lock)
        pthread_rwlock_unlock(&global_sessions_lock);

    pthread_rwlock_wrlock(&session->subscriptions_lock);
    if (session->subscriptions) {
        while (session->num_subscriptions)
            for (unsigned idx = 0; idx < session->num_subscriptions; idx++) {
                if ((*session->subscriptions)[idx] == NULL)
                    continue;
                unsubscribe((*session->subscriptions)[idx]);
                //(*session->subscriptions)[idx] = NULL;
            }
        free(session->subscriptions);
        session->subscriptions = NULL;
        session->num_subscriptions = 0;
    }
    pthread_rwlock_unlock(&session->subscriptions_lock);

    /* TODO do this properly */
    pthread_rwlock_wrlock(&session->delivery_states_lock); {
        /* TODO iterate and free_message_delivery_state() */
        if (session->delivery_states)
            free (session->delivery_states);
    } pthread_rwlock_unlock(&session->delivery_states_lock);

    if (session->client_id)
        free((void *)session->client_id);

    pthread_rwlock_destroy(&session->subscriptions_lock);
    pthread_rwlock_destroy(&session->delivery_states_lock);

    free(session);
}

[[gnu::malloc, gnu::nonnull]]
static struct message_delivery_state *alloc_message_delivery_state(
        struct message *message, struct session *session)
{
    struct message_delivery_state *ret;

    if ((ret = calloc(1, sizeof(struct message_delivery_state))) == NULL)
        goto fail;

    ret->session = session;
    ret->message = message;
    ret->id = mds_id++;

    atomic_fetch_add_explicit(&session->refcnt, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&message->refcnt, 1, memory_order_relaxed);

    dbg_printf("     alloc_message_delivery_state: session=%u[%u] message=%u[%u]\n",
            session->id, session->refcnt,
            message->id, message->refcnt);

    pthread_rwlock_wrlock(&global_mds_lock);
    ret->next = global_mds_list;
    global_mds_list = ret;
    num_mds++;
    pthread_rwlock_unlock(&global_mds_lock);
    return ret;

fail:
    if (ret)
        free(ret);
    return NULL;
}

[[gnu::malloc]]
static struct subscription *alloc_subscription(struct session *session,
        struct topic *topic)
{
    struct subscription *ret = NULL;

    if ((ret = calloc(1, sizeof(struct subscription))) == NULL)
        return NULL;

    ret->id = subscription_id++;
    ret->topic = topic;
    ret->session = session;

    dbg_printf("     alloc_subscription: id=%u session=%u <%s> topic=%u <%s>\n",
            ret->id, session->id, (char *)session->client_id,
            topic->id, (char *)topic->name);
    return ret;
}

[[gnu::malloc]]
static struct session *alloc_session(struct client *client)
{
    struct session *ret;

    if (num_sessions >= MAX_SESSIONS) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((ret = calloc(1, sizeof(struct session))) == NULL)
        return NULL;

    if (pthread_rwlock_init(&ret->subscriptions_lock, NULL) == -1)
        goto fail;
    if (pthread_rwlock_init(&ret->delivery_states_lock, NULL) == -1)
        goto fail;

    if (client) {
        ret->client = client;
        client->session = ret;
        ret->client_id = (void *)strdup((const char *)client->client_id);
    }

    pthread_rwlock_wrlock(&global_sessions_lock);
    ret->next = global_session_list;
    global_session_list = ret;
    num_sessions++;
    pthread_rwlock_unlock(&global_sessions_lock);

    ret->id = session_id++;

    dbg_printf("     alloc_session: id=%u client=%u <%s>\n",
            ret->id, client ? client->id : 0, client ? (char *)client->client_id : "");
    return ret;

fail:
    if (ret)
        free(ret);

    return NULL;
}

[[gnu::malloc, gnu::nonnull]]
static struct topic *alloc_topic(const uint8_t *name)
{
    struct topic *ret;

    if (num_topics >= MAX_TOPICS) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((ret = calloc(1, sizeof(struct topic))) == NULL)
        return NULL;

    pthread_rwlock_init(&ret->subscribers_lock, NULL);
    pthread_rwlock_init(&ret->pending_queue_lock, NULL);

    ret->name = name;
    ret->id = topic_id++;
    num_topics++;

    dbg_printf("     alloc_topic: id=%u <%s>\n", ret->id, (char *)name);

    return ret;
}

[[gnu::malloc,gnu::warn_unused_result]]
static struct packet *alloc_packet(struct client *owner)
{
    struct packet *ret;

    errno = 0;

    if (num_packets >= MAX_PACKETS) {
        errno = ENOSPC;
        return NULL;
    }

    if ((ret = calloc(1, sizeof(struct packet))) == NULL)
        return NULL;

    if (owner) {
        ret->owner = owner;
        ret->refcnt++;
        pthread_rwlock_wrlock(&owner->active_packets_lock);
        ret->next_client = owner->active_packets;
        owner->active_packets = ret;
        pthread_rwlock_unlock(&owner->active_packets_lock);
    }

    pthread_rwlock_wrlock(&global_packets_lock);
    ret->next = global_packet_list;
    global_packet_list = ret;
    num_packets++;
    pthread_rwlock_unlock(&global_packets_lock);

    ret->id = packet_id++;

    dbg_printf("     alloc_packet: id=%u owner=%u <%s>\n",
            ret->id, owner ? owner->id : 0, owner ? ((char *)owner->client_id) : ""
          );

    return ret;
}

[[gnu::malloc]]
static struct message *alloc_message(void)
{
    struct message *msg;

    if (num_messages >= MAX_MESSAGES) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((msg = calloc(1, sizeof(struct message))) == NULL)
        return NULL;

    msg->state = MSG_NEW;
    pthread_rwlock_wrlock(&global_messages_lock);
    msg->next = global_message_list;
    global_message_list = msg;
    num_messages++;
    pthread_rwlock_unlock(&global_messages_lock);

    msg->id = message_id++;

    return msg;
}

[[gnu::malloc]]
static struct client *alloc_client(void)
{
    struct client *client;

    if (num_clients >= MAX_CLIETNS) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((client = calloc(1, sizeof(struct client))) == NULL)
        return NULL;

    client->state = CS_NEW;
    client->fd = -1;
    client->parse_state = READ_STATE_NEW;

    if (pthread_rwlock_init(&client->active_packets_lock, NULL) == -1)
        goto fail;

    pthread_rwlock_wrlock(&global_clients_lock);
    client->next = global_client_list;
    global_client_list = client;
    num_clients++;
    pthread_rwlock_unlock(&global_clients_lock);

    client->id = client_id++;

    dbg_printf("     alloc_client: id=%u\n", client->id);

    return client;

fail:
    if (client)
        free(client);

    return NULL;
}

static struct session *find_session(struct client *client)
{
    pthread_rwlock_rdlock(&global_sessions_lock);
    for (struct session *tmp = global_session_list; tmp; tmp = tmp->next)
    {
        if (strcmp((const char *)tmp->client_id,
                    (const char *)client->client_id))
            continue;

        if (tmp->client) {
            /* TODO */
            continue;
        }

        pthread_rwlock_unlock(&global_sessions_lock);
        return tmp;
    }
    pthread_rwlock_unlock(&global_sessions_lock);

    return NULL;
}


/*
 * packet parsing helpers
 */

[[gnu::nonnull]]
static int is_valid_topic_name(const uint8_t *name)
{
    const uint8_t *ptr;

    errno = EINVAL;
    ptr = name;

    if (!*ptr)
        return -1;

    while (*ptr)
    {
        if (*ptr == '#' || *ptr == '+')
            return -1;

        ptr++;
    }

    errno = 0;
    return 0;
}

[[gnu::nonnull]]
static int is_valid_topic_filter(const uint8_t *name)
{
    const uint8_t *ptr;

    errno = EINVAL;
    ptr = name;

    if (!*ptr)
        return -1;

    if (!strcmp((const char *)ptr, "/"))
        return -1;

    while (*ptr)
    {
        /* The multi-level wildcard character MUST be specified either on its
         * own or following a topic level separator.
         *
         * In either case it MUST be the last character specified in the
         * Topic Filter
         */
        if (*ptr == '#') {
            if (*(ptr+1))
                return -1;
            if (ptr > name && *(ptr-1) != '/')
                return -1;
        }

        if (*ptr == '+') {
            if (*(ptr+1) && *(ptr+1) != '/')
                return -1;
            if (ptr > name && *(ptr-1) != '/')
                return -1;
        }

        ptr++;
    }

    errno = 0;
    return 0;
}

[[gnu::nonnull]]
static int encode_var_byte(uint32_t value, uint8_t out[4])
{
    uint8_t byte;
    int out_len = 0;

    do {
        byte = value % 128;
        value /= 128;
        if (value > 0)
            byte |= 128;
        out[out_len++] = byte;
    } while (value > 0);

    return out_len;
}

[[gnu::nonnull]]
static uint32_t read_var_byte(const uint8_t **const ptr, size_t *bytes_left)
{
    uint32_t value = 0;
    uint32_t multi = 1;
    uint8_t tmp;

    errno = 0;

    do {
        tmp = **ptr;
        *ptr = *ptr + 1;
        *bytes_left = *bytes_left - 1;

        if (multi > 128*128*128) {
            warn("invalid variable byte int");
            errno = EINVAL;
            return 0;
        }

        value += ((tmp & 127) * multi);
        multi *= 128;
    } while((tmp & 128) != 0);

    return value;
}

[[gnu::nonnull]]
static void *read_binary(const uint8_t **const ptr, size_t *bytes_left,
        uint16_t *length)
{
    void *blob = NULL;
    uint16_t tmp;

    errno = 0;

    if (*bytes_left < 2) {
        errno = ENOSPC;
        return NULL;
    }

    memcpy(&tmp, *ptr, 2);

    *length = ntohs(tmp);
    *ptr += 2;
    *bytes_left -= 2;

    if (*bytes_left < *length) {
        errno = ENOSPC;
        return NULL;
    }

    if (*length > 0) {
        if ((blob = malloc(*length)) == NULL) {
            errno = ENOMEM;
            return NULL;
        }

        memcpy(blob, *ptr, *length);

        *ptr += *length;
        *bytes_left -= *length;
    }

    return blob;
}

[[gnu::nonnull]]
static uint8_t *read_utf8(const uint8_t **const ptr, size_t *bytes_left)
{
    uint16_t str_len;
    uint8_t *string;

    errno = 0;

    if (*bytes_left < 2) {
        errno = ENOSPC;
        return NULL;
    }

    memcpy(&str_len, *ptr, 2);
    str_len = ntohs(str_len);

    *ptr += 2;
    *bytes_left -= 2;

    if (*bytes_left < str_len) {
        errno = ENOSPC;
        return NULL;
    }

    if ((string = malloc(str_len + 1)) == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    string[str_len] = '\0';

    if (str_len > 0) {
        memcpy(string, *ptr, str_len);
        *ptr += str_len;
        *bytes_left -= str_len;

        for (unsigned idx = 0; idx < str_len; idx++) {
            if (string[idx] == '\0') {
                errno = EINVAL;
                goto fail;
            }
        }
    }

    return string;
fail:
    if (string)
        free(string);

    return NULL;
}

[[gnu::nonnull]]
static ssize_t get_properties_size(const struct property (*props)[],
        unsigned num_props)
{
    ssize_t ret;
    const struct property *prop;
    uint8_t tmp_out[4];
    type_t type;

    if (num_props == 0)
        return 0;

    ret = 0;

    for (unsigned idx = 0; idx < num_props; idx++)
    {
        prop = &(*props)[idx];
        ret++; /* Property Type */

        if (prop->ident > MQTT_MAX_PROPERTY_IDENT) {
            errno = ERANGE;
            return -1;
        }

        type = property_to_type[prop->ident];

        switch (type)
        {
            case MQTT_TYPE_BYTE:
                ret++;
                break;
            case MQTT_TYPE_2BYTE:
                ret += 2;
                break;
            case MQTT_TYPE_4BYTE:
                ret += 4;
                break;
            case MQTT_TYPE_BINARY:
                ret += prop->binary.len;
                ret += 2;
                break;
            case MQTT_TYPE_UTF8_STRING:
                ret += strlen((const char *)prop->utf8_string);
                ret += 2;
                break;
            case MQTT_TYPE_VARBYTE:
                ret += encode_var_byte(prop->varbyte, tmp_out);
                break;
            case MQTT_TYPE_UTF8_STRING_PAIR:
                ret += strlen((const char *)prop->utf8_pair[0]);
                ret += strlen((const char *)prop->utf8_pair[1]);
                ret += 4; /* 2x2 */
                break;
            case MQTT_TYPE_UNDEFINED:
            case MQTT_TYPE_MAX:
                errno = EINVAL;
                warnx("get_propertes_size: attempt to size undefined MQTT_TYPE");
                return -1;
        }
    }

    return ret;
}

[[gnu::nonnull(2)]]
static void do_one_string(const uint8_t *str, uint8_t **ptr)
{
    unsigned len;
    uint16_t enclen;

    if (str) {
        len = strlen((const char *)str);
        enclen = htons(len);
    } else {
        len = 0;
        enclen = 0;
    }
    memcpy(*ptr, &enclen, 2);
    *ptr += 2;
    if (len != 0) {
        memcpy(*ptr, str, len);
        *ptr += len;
    }
}

[[gnu::nonnull]]
static int build_properties(const struct property (*props)[],
        unsigned num_props, uint8_t **out)
{
    uint8_t *ptr = *out;
    uint16_t tmp2byte;
    uint32_t tmp4byte;
    const struct property *prop;
    type_t type;
    int rc;

    if (num_props == 0)
        return 0;

    for (unsigned idx = 0; idx < num_props; idx++)
    {
        prop = &(*props)[idx];

        if (prop->ident > MQTT_MAX_PROPERTY_IDENT) {
            errno = ERANGE;
            goto fail;
        }

        type = property_to_type[prop->ident];

        *ptr = prop->ident;
        ptr++;

        switch (type)
        {
            case MQTT_TYPE_BYTE:
                *ptr = prop->byte;
                ptr++;
                break;

            case MQTT_TYPE_2BYTE:
                tmp2byte = htons(prop->byte2);
                memcpy(ptr, &tmp2byte, 2);
                ptr += 2;
                break;

            case MQTT_TYPE_4BYTE:
                tmp4byte = htonl(prop->byte4);
                memcpy(ptr, &tmp4byte, 4);
                ptr += 4;
                break;

            case MQTT_TYPE_VARBYTE:
                if ((rc = encode_var_byte(prop->varbyte, ptr)) == -1)
                    goto fail;
                ptr += rc;
                break;

            case MQTT_TYPE_BINARY:
                tmp2byte = htons(prop->binary.len);
                memcpy(ptr, &tmp2byte, 2);
                ptr += 2;
                if (prop->binary.len) {
                    memcpy(ptr, prop->binary.data, prop->binary.len);
                    ptr += prop->binary.len;
                }
                break;

            case MQTT_TYPE_UTF8_STRING:
                do_one_string(prop->utf8_string, &ptr);
                break;

            case MQTT_TYPE_UTF8_STRING_PAIR:
                do_one_string(prop->utf8_pair[0], &ptr);
                do_one_string(prop->utf8_pair[1], &ptr);
                break;

            case MQTT_TYPE_UNDEFINED:
            case MQTT_TYPE_MAX:
                errno = EINVAL;
                warnx("build_properties: invalid MQTT_TYPE");
                goto fail;
        }
    }

    *out = ptr;
    return 0;

fail:
    return -1;
}

/* a type of -1U is used for situations where the properties are NOT the standard ones
 * in a packet, e.g. "will_properties" */
[[gnu::nonnull]]
static int parse_properties(
        const uint8_t **ptr, size_t *bytes_left,
        struct property (**store_props)[], unsigned *store_num_props,
        control_packet_t cp_type)
{
    uint32_t properties_length;
    size_t rd = 0;
    uint8_t ident;
    struct property (*props)[] = NULL;
    struct property *prop;
    unsigned num_props = 0, skip;
    void *tmp;
    type_t type;

    errno = 0;

    properties_length = read_var_byte(ptr, bytes_left);

    if (properties_length == 0 && errno)
        return -1;

    //dbg_printf("parse_properties: properties_length=%u\n", properties_length);

    if (properties_length == 0)
        return 0;

    if (*bytes_left < properties_length) {
        errno = ENOSPC;
        return -1;
    }

    rd = *bytes_left - properties_length;
    while (*bytes_left > rd)
    {
        if (num_props == MAX_PROPERTIES) {
            errno = ENOSPC;
            goto fail;
        }

        if ((tmp = realloc(props, sizeof(struct property) * (num_props + 1))) == NULL)
            goto fail;
        props = tmp;

        memset(&(*props)[num_props], 0, sizeof(struct property));

        ident = **ptr;
        *ptr = *ptr + 1;
        *bytes_left = *bytes_left - 1;

        prop = &(*props)[num_props];

        if (ident > MQTT_MAX_PROPERTY_IDENT) {
            errno = EINVAL;
            goto fail;
        }
        prop->ident = ident;

        type = property_to_type[prop->ident];

        /* TODO perform "is this valid for this control type?" */

        if (cp_type != -1U) /* for will_properties, there is cp_type */
            switch (prop->ident)
            {
                default:
                    /* TODO MQTT requires skipping not failing */
                    warn("parse_properties: unsupported property identifier %u\n",
                            prop->ident);
            }

        skip = 0;

        switch (type)
        {
            case MQTT_TYPE_BYTE:
                if (*bytes_left < 1)
                    goto fail;
                prop->byte = **ptr;
                skip = 1;
                break;

            case MQTT_TYPE_2BYTE:
                if (*bytes_left < 2)
                    goto fail;
                memcpy(&prop->byte2, *ptr, 2);
                prop->byte2 = ntohs(prop->byte2);
                skip = 2;
                break;

            case MQTT_TYPE_4BYTE:
                if (*bytes_left < 4)
                    goto fail;
                memcpy(&prop->byte4, *ptr, 4);
                prop->byte4 = ntohl(prop->byte4);
                skip = 4;
                break;

            case MQTT_TYPE_VARBYTE:
                prop->varbyte = read_var_byte(ptr, bytes_left);
                if (prop->varbyte == 0 && errno)
                    goto fail;
                break;

            case MQTT_TYPE_UTF8_STRING:
                prop->utf8_string = read_utf8(ptr, bytes_left);
                if (prop->utf8_string == NULL)
                    goto fail;
                break;

            case MQTT_TYPE_BINARY:
                if (*bytes_left < 2)
                    goto fail;

                memcpy(&prop->binary.len, *ptr, 2);
                *ptr += 2;
                *bytes_left -= 2;
                prop->binary.len = ntohs(prop->binary.len);

                if (prop->binary.len) {
                    if (prop->binary.len > *bytes_left)
                        goto fail;

                    if ((prop->binary.data = malloc(prop->binary.len)) == NULL)
                        goto fail;

                    memcpy(prop->binary.data, *ptr, prop->binary.len);
                    *ptr += prop->binary.len;
                    *bytes_left -= prop->binary.len;

                }
                break;

            case MQTT_TYPE_UTF8_STRING_PAIR:
                prop->utf8_pair[0] = read_utf8(ptr, bytes_left);
                if (prop->utf8_pair[0] == NULL)
                    goto fail;

                prop->utf8_pair[1] = read_utf8(ptr, bytes_left);
                if (prop->utf8_pair[1] == NULL)
                    goto fail;

                break;

            case MQTT_TYPE_MAX: /* Avoid GCC warnings */
            case MQTT_TYPE_UNDEFINED:
                errno = EINVAL;
                warn("parse_properties: illegal use of property type 0");
                goto fail;
        }

        if (skip) {
            *ptr = *ptr + skip;
            *bytes_left = *bytes_left - skip;
        }

        num_props++;
    }

    *store_props = props;
    *store_num_props = num_props;

    return 0;

fail:
    if (props)
        free_properties(props, num_props);

    return -1;
}

/*
 * signal handlers
 */

static void sh_sigint(int signum, siginfo_t * /*info*/, void * /*stuff*/)
{
    dbg_printf("     sh_sigint: received signal %u\n", signum);
    if (signum == SIGHUP) {
        dump_all();
        return;
    }
    if (running == false)
        _exit(EXIT_FAILURE);
    running = false;
}

/*
 * atexit() functions
 */

static void close_all_sockets(void)
{
    dbg_printf("     close_socket: closing mother_fd %u\n", mother_fd);
    if (mother_fd != -1)
        close_socket(&mother_fd);
}

static void free_all_message_delivery_states(void)
{
    dbg_printf("     free_all_message_delivery_states\n");
    while (global_mds_list)
        free_message_delivery_state(global_mds_list);
}

static void free_all_sessions(void)
{
    dbg_printf("     free_all_sessions\n");
    while (global_session_list)
        free_session(global_session_list, true);
}

static void free_all_messages(void)
{
    dbg_printf("     free_all_messages\n");
    while (global_message_list)
        free_message(global_message_list, true);
}

static void free_all_clients(void)
{
    dbg_printf("     free_all_clients\n");
    while (global_client_list)
        free_client(global_client_list, true);
}

static void free_all_packets(void)
{
    dbg_printf("     free_all_packets\n");
    while (global_packet_list)
        free_packet(global_packet_list, false);
}

static void free_all_topics(void)
{
    dbg_printf("     free_all_topics\n");
    while (global_topic_list)
        free_topic(global_topic_list);
}

/*
 * message distribution
 */

[[gnu::nonnull, gnu::warn_unused_result]]
static struct topic *find_topic(const uint8_t *name)
{
    errno = 0;

    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
    {
        if (!strcmp((const void *)name, (const void *)tmp->name)) {
            pthread_rwlock_unlock(&global_topics_lock);
            return tmp;
        }
    }
    pthread_rwlock_unlock(&global_topics_lock);

    return NULL;
}


[[gnu::nonnull]]
static int find_subscription(struct session *session, struct topic *topic)
{
    pthread_rwlock_rdlock(&topic->subscribers_lock);
    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if ((*topic->subscribers)[idx] == NULL)
            continue;

        if ((*topic->subscribers)[idx]->session == session) {
            pthread_rwlock_unlock(&topic->subscribers_lock);
            return idx;
        }
    }
    pthread_rwlock_unlock(&topic->subscribers_lock);
    errno = ENOENT;
    return -1;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static struct topic *register_topic(
        const uint8_t *name)
{
    struct topic *ret;

    errno = 0;

    if ((ret = alloc_topic(name)) == NULL)
        return NULL;

    pthread_rwlock_wrlock(&global_topics_lock);
    ret->next = global_topic_list;
    global_topic_list = ret;
    pthread_rwlock_unlock(&global_topics_lock);

    return ret;
}

[[gnu::nonnull]]
static int remove_delivery_state(
        struct message_delivery_state ***state_array, unsigned *array_length,
        struct message_delivery_state *rem)
{
    unsigned new_length = *array_length - 1;
    struct message_delivery_state **tmp = NULL;

    dbg_printf("     remove_delivery_state: array_length=%u new_length=%u rem=%u\n",
            *array_length, new_length, rem->id);

    errno = 0;

    if (*array_length == 0) {
        warnx("remove_delivery_state: is empty");
        errno = EINVAL;
        return -1;
    }

    if (new_length == 0) {
        if (*state_array)
            free(*state_array);
        *state_array = NULL;
        *array_length = 0;
        return 0;
    }

    assert(*state_array != NULL);

    if ((tmp = malloc(sizeof(struct message_delivery_state *) * new_length)) == NULL)
        goto fail;

    bool found = false;
    unsigned new_idx, old_idx;

    for (new_idx = 0, old_idx = 0; new_idx < new_length; old_idx++)
    {
        dbg_printf("     remove_delivery_state: new_idx=%u old_idx=%u\n",
                new_idx, old_idx);

        if (old_idx >= *array_length)
            break;

        if ((*state_array)[old_idx] == rem) {
            found = true;
            continue;
        }

        tmp[new_idx++] = (*state_array)[old_idx];
    }

    if (found == true) {
        if (*state_array)
            free(*state_array);
        *state_array = tmp;
        *array_length = new_length;
        return 0;
    }

    errno = ENOENT;

fail:
    if (tmp)
        free(tmp);

    return -1;
}

[[gnu::nonnull]]
static int add_to_delivery_state(
        struct message_delivery_state ***state_array, unsigned *array_length,
        pthread_rwlock_t *lock, struct message_delivery_state *add)
{
    pthread_rwlock_wrlock(lock);

    const unsigned new_length = *array_length + 1;
    const size_t new_size = sizeof(struct message_delivery_state *) * new_length;
    struct message_delivery_state **tmp;

    if ((tmp = realloc(*state_array, new_size)) == NULL)
        goto fail;

    tmp[*array_length] = add;

    *state_array = tmp;
    *array_length = new_length;

    pthread_rwlock_unlock(lock);
    return 0;

fail:
    if (tmp)
        free(tmp);

    pthread_rwlock_unlock(lock);
    return -1;
}

[[gnu::nonnull]]
static int enqueue_message(struct topic *topic, struct message *msg)
{
    struct message_delivery_state *mds;

    assert(topic->id);
    assert(msg->id);
    assert(msg->state == MSG_NEW);

    errno = 0;

    pthread_rwlock_rdlock(&topic->subscribers_lock);
    bool found = false;
    for (unsigned src_idx = 0; src_idx < topic->num_subscribers; src_idx++)
    {
        if ((*topic->subscribers)[src_idx] == NULL)
            continue;
        found = true;

        struct session *session = (*topic->subscribers)[src_idx]->session;

        assert(session);
        assert(session->id);

        /* TODO lock the subscriber? */

        if ((mds = alloc_message_delivery_state(msg, session)) == NULL) {
            warn("enqueue_message: alloc_message_delivery_state");
            /* TODO ???? */
            continue;
        }

        if (add_to_delivery_state(
                    &msg->delivery_states,
                    &msg->num_message_delivery_states,
                    &msg->delivery_states_lock,
                    mds) == -1) {
            warn("enqueue_message: add_to_delivery_state(msg)");
            /* TODO ??? */
            free_message_delivery_state(mds);
            continue;
        }

        if (add_to_delivery_state(
                    &session->delivery_states,
                    &session->num_message_delivery_states,
                    &session->delivery_states_lock,
                    mds) == -1) {
            warn("enqueue_message: add_to_delivery_state(session)");
            /* TODO ??? */
            /* We can't free mds as msg->delivery_states still points to it */
            continue;
        }
    }
    pthread_rwlock_unlock(&topic->subscribers_lock);

    if (found == false)
        warnx("enqueue_message: failed to add to subscribers!");

    msg->topic = topic;

    pthread_rwlock_wrlock(&topic->pending_queue_lock);
    msg->next_queue = topic->pending_queue;
    topic->pending_queue = msg;
    pthread_rwlock_unlock(&topic->pending_queue_lock);

    return 0;

    /* TODO fail: */
}

[[gnu::nonnull]]
static int dequeue_message(struct message *msg)
{
    errno = 0;

    assert(msg->topic != NULL);

    if (msg->topic == NULL) {
        warnx("dequeue_message: attempt to dequeue_message with topic NULL\n");
        errno = EINVAL;
        return -1; /* or 0? TODO */
    }

    if (pthread_rwlock_trywrlock(&msg->topic->pending_queue_lock) == 0) {
        warnx("dequeue_message: pending_queue_lock was unlocked");
        pthread_rwlock_unlock(&msg->topic->pending_queue_lock);
        errno = ENOLCK;
        return -1;
    }

    if (msg == msg->topic->pending_queue) {
        msg->topic->pending_queue = msg->next_queue;
        goto done;
    } else for (struct message *tmp = msg->topic->pending_queue; tmp; tmp = tmp->next_queue) {
        if (tmp->next_queue == msg) {
            tmp->next_queue = msg->next_queue;
            goto done;
        }
    }

    errno = ESRCH;
    return -1;

done:
    msg->next_queue = NULL;
    msg->topic = NULL;
    return 0;
}

[[gnu::nonnull,gnu::access(read_only,4,3)]]
static struct message *register_message(const uint8_t *topic_name, int format,
        uint16_t len, const void *payload, unsigned qos, struct session *sender)
{
    struct topic *topic;
    const uint8_t *tmp_name;

    tmp_name = NULL;
    topic = NULL;
    errno = 0;

    dbg_printf("[%2d] register_message: topic=<%s> format=%u len=%u qos=%u payload=%p\n",
            sender->id, topic_name, format, len, qos, payload);

    if ((topic = find_topic(topic_name)) == NULL) {

        if ((tmp_name = (void *)strdup((const char *)topic_name)) == NULL)
            goto fail;

        if ((topic = register_topic(tmp_name)) == NULL) {
            warn("register_message: register_topic <%s>", tmp_name);
            goto fail;
        }

        tmp_name = NULL;
    }

    struct message *msg;

    if ((msg = alloc_message()) == NULL)
        goto fail;

    msg->format = format;
    msg->payload = payload;
    msg->payload_len = len;
    msg->qos = qos;
    msg->sender = sender;
    msg->state = MSG_NEW;

    if (enqueue_message(topic, msg) == -1) {
        warn("register_message: enqueue_message");
        free_message(msg, true);
        goto fail;
    }

    msg->state = MSG_ACTIVE;

    /* TODO register the message for delivery and commit */

    return msg;

fail:
    if (tmp_name)
        free((void *)tmp_name);
    return NULL;
}

[[gnu::nonnull]]
static int add_subscription_to_topic(struct subscription *new_sub)
{
    struct subscription *(*tmp_subs)[] = NULL;
    struct topic *topic = new_sub->topic;

    errno = 0;

    size_t sub_size = sizeof(struct subscription *) * (topic->num_subscribers + 1);
    if ((tmp_subs = (void *)realloc(topic->subscribers, sub_size)) == NULL)
        goto fail;

    topic->subscribers = tmp_subs;
    (*topic->subscribers)[topic->num_subscribers] = new_sub;
    topic->num_subscribers++;
    return 0;

fail:
    if (tmp_subs)
        free(tmp_subs);
    return -1;
}

/* TODO locking */
[[gnu::nonnull]]
static int unsubscribe(struct subscription *sub)
{
    struct subscription *(*tmp_topic)[] = NULL;
    struct subscription *(*tmp_client)[] = NULL;
    size_t topic_sub_size, topic_sub_cnt = 0, client_sub_size, client_sub_cnt = 0;
    unsigned old_idx, new_idx;

    struct topic *topic;
    struct session *session;

    topic = sub->topic;
    session = sub->session;

    errno = 0;

    /*
     * remove the back references for this subscription
     */

    pthread_rwlock_wrlock(&topic->subscribers_lock);
    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if ( (*topic->subscribers)[idx] == sub) {
            (*topic->subscribers)[idx] = NULL;
            break;
        }
    }
    pthread_rwlock_unlock(&topic->subscribers_lock);

    pthread_rwlock_wrlock(&session->subscriptions_lock);
    for (unsigned idx = 0; idx < session->num_subscriptions; idx++)
    {
        if ( (*session->subscriptions)[idx] == sub) {
            (*session->subscriptions)[idx] = NULL;
            break;
        }
    }
    pthread_rwlock_unlock(&session->subscriptions_lock); /* TODO hold lock until the end */

    /*
     * compact the topic list of subscribers
     */

    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if ((*topic->subscribers)[idx] == NULL)
            continue;
        topic_sub_cnt++;
    }

    topic_sub_size = topic_sub_cnt * sizeof(struct subscription *);

    if (topic_sub_cnt == 0) {
        tmp_topic = NULL;
        goto skip_topic;
    }

    if ((tmp_topic = calloc(1, topic_sub_size)) == NULL)
        goto fail;

    for (old_idx = 0, new_idx = 0; old_idx < topic->num_subscribers; old_idx++)
    {
        if ((*topic->subscribers)[old_idx] == NULL)
            continue;

        (*tmp_topic)[new_idx] = (*topic->subscribers)[old_idx];
        new_idx++;
    }

skip_topic:

    /*
     * compact the client list of subscriptions
     */

    pthread_rwlock_wrlock(&session->subscriptions_lock); /* HOLD lock from start to finish */
    for (unsigned idx = 0; idx < session->num_subscriptions; idx++)
    {
        if ((*session->subscriptions)[idx] == NULL)
            continue;
        client_sub_cnt++;
    }



    client_sub_size = client_sub_cnt * sizeof(struct subscription *);

    if (client_sub_cnt == 0) {
        tmp_client = NULL;
        goto skip_client;
    }

    if ((tmp_client = calloc(1, client_sub_size)) == NULL) {
        pthread_rwlock_unlock(&session->subscriptions_lock);
        goto fail;
    }

    for (old_idx = 0, new_idx = 0; old_idx < session->num_subscriptions; old_idx++)
    {
        if ((*session->subscriptions)[old_idx] == NULL)
            continue;
        (*tmp_client)[new_idx] = (*session->subscriptions)[old_idx];
        new_idx++;
    }

    /*
     * free the old ones and replace
     */

skip_client:

    if (session->subscriptions)
        free(session->subscriptions);
    if (topic->subscribers)
        free(topic->subscribers);

    topic->subscribers = tmp_topic;
    session->subscriptions = tmp_client;

    topic->num_subscribers = topic_sub_cnt;
    session->num_subscriptions = client_sub_cnt;

    dbg_printf("     unsubscribe_from_topic: client_sub_cnt now %lu\n", client_sub_cnt);

    sub->topic = NULL;
    sub->session = NULL;
    free_subscription(sub);

    pthread_rwlock_unlock(&session->subscriptions_lock);

    return 0;

fail:
    if (tmp_client)
        free(tmp_client);
    if (tmp_topic)
        free(tmp_topic);

    return -1;
}

[[gnu::nonnull]]
static int subscribe_to_topics(struct session *session,
        struct topic_sub_request *request)
{
    struct subscription *(*tmp_subs)[] = NULL;
    struct topic *tmp_topic = NULL;

    errno = 0;

    pthread_rwlock_wrlock(&session->subscriptions_lock);
    size_t sub_size = sizeof(struct subscription *) * (session->num_subscriptions + request->num_topics);

    if ((tmp_subs = (void *)realloc(session->subscriptions, sub_size)) == NULL) {
        for (unsigned idx = 0; idx < request->num_topics; idx++)
            request->response_codes[idx] = MQTT_UNSPECIFIED_ERROR;

        pthread_rwlock_unlock(&session->subscriptions_lock);
        goto fail;
    }

    session->subscriptions = tmp_subs;

    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        (*session->subscriptions)[session->num_subscriptions + idx] = NULL;

        if (request->response_codes[idx] > MQTT_GRANTED_QOS_2) {
            dbg_printf("[%d] subscribe_to_topics: response code is %u\n",
                    session->id,
                    request->response_codes[idx]);
            continue;
        }

        dbg_printf("[%2d] subscribe_to_topics: subscribing to <%s>\n",
                session->id,
                (char *)request->topics[idx]);

        if ((tmp_topic = find_topic(request->topics[idx])) == NULL) {
            /* TODO somehow ensure reply does a fail for this one? */
            dbg_printf("[%2d] subscribe_to_topics: failed to find_topic(<%s>)\n",
                    session->id,
                    (char *)request->topics[idx]);
            request->response_codes[idx] = MQTT_NOT_AUTHORIZED;
            /* TODO what's the correct approach? */
            continue;
        }

        int existing_idx;
        struct subscription *new_sub;

        if ((existing_idx = find_subscription(session, tmp_topic)) == -1 && errno == ENOENT) {

            if ((new_sub = alloc_subscription(session, tmp_topic)) == NULL)
                goto fail;
            new_sub->option = request->options[idx];

            if (add_subscription_to_topic(new_sub) == -1) {
                warn("subscribe_to_topics: add_subscription_to_topic <%s>",
                        tmp_topic->name);
                request->response_codes[idx] = MQTT_UNSPECIFIED_ERROR;
                free_subscription(new_sub);
                continue;
            }

            /* TODO refactor to add_subscription_to_session() */
            (*session->subscriptions)[session->num_subscriptions + idx] = new_sub;
        } else if (existing_idx == -1) {
            warn("subscribe_to_topics: find_subscription");
            request->response_codes[idx] = MQTT_UNSPECIFIED_ERROR;
            continue;
        } else {
            /* Update the existing subscription's options (e.g. QoS) */
            dbg_printf("[%2d] subscribe_to_topics: updating existing subscription\n",
                    session->id);
            (*tmp_topic->subscribers)[existing_idx]->option = request->options[idx];
        }

        free((void *)request->topics[idx]);
        request->options[idx] = 0;
        request->topics[idx] = NULL;
        if (existing_idx == -1)
            session->num_subscriptions++;
    }

    dbg_printf("[%2d] subscribe_to_topics: num_subscriptions now %u [+%u]\n",
            session->id,
            session->num_subscriptions, request->num_topics);
    pthread_rwlock_unlock(&session->subscriptions_lock);
    return 0;

fail:

    return -1;
}

/*
 * control packet response functions
 */

/* Fixed Header:
 *  MQTT Control Packet Type [4:7]
 *  DUP flag [3]
 *  QoS [1:2]
 *  RETAIN [0]
 *  Remaining Length (VAR)
 * Variable Header:
 *  Topic Name
 *  Packet Identifier {if QoS > 0}
 *  Properties[]
 * Payload
 */

[[gnu::nonnull]]
static int send_cp_publish(struct packet *pkt)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    uint8_t proplen[4], remlen[4];
    unsigned proplen_len, remlen_len, prop_len;

    uint16_t tmp, topic_len;
    const struct message *msg;

    assert(pkt->message != NULL);
    assert(pkt->message->topic != NULL);
    assert(pkt->owner != NULL);

    dbg_printf("[%2d] send_cp_publish: owner=%s\n",
            pkt->owner->session->id,
            (char *)pkt->owner->client_id);

    errno = 0;
    msg = pkt->message;

    /* Populate Properties */

    const struct property props[] = {
        { },
    };
    const unsigned num_props = 0; /* sizeof(props) / sizeof(struct property) */

    /* Calculate the Remaining Length */

    prop_len = get_properties_size(&props, num_props);
    proplen_len = encode_var_byte(prop_len, proplen);

    length = 0;

    length += 2; /* UTF-8 length */
    length += (topic_len = strlen((char *)pkt->message->topic->name)); /* Actual String */

    if ((pkt->flags & MQTT_FLAG_PUBLISH_QOS_MASK))
        length += 2; /* packet identifier */

    length += proplen_len;
    length += prop_len;

    length += msg->payload_len;

    remlen_len = encode_var_byte(length, remlen);

    /* Calculate the total length including header */

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len;

    /* Now build the packet */

    if ((ptr = packet = calloc(1, length)) == NULL)
        goto fail;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBLISH;
    ((struct mqtt_fixed_header *)ptr)->flags = pkt->flags;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(topic_len);
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    memcpy(ptr, msg->topic->name, topic_len);
    ptr += topic_len;

    if (pkt->flags & MQTT_FLAG_PUBLISH_QOS_MASK) {
        tmp = htons(pkt->packet_identifier); /* TODO proper packet identifier */
        memcpy(ptr, &tmp, 2);
        ptr += 2;
    }

    memcpy(ptr, proplen, proplen_len);
    ptr += proplen_len;

    if (build_properties(&props, num_props, &ptr) == -1)
        goto fail;

    memcpy(ptr, msg->payload, msg->payload_len);

    /* Now send the packet */

    if ((wr_len = write(pkt->owner->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    return 0;

fail:
    if (packet)
        free(packet);
    return -1;
}

[[gnu::nonnull]]
static int send_cp_disconnect(struct client *client, reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    errno = 0;

    length = sizeof(struct mqtt_fixed_header);
    length += 1;

    length += 1; /* Disconnect Reason Code */
    length += 1; /* Properties Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_DISCONNECT;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1;
    ptr++;

    *ptr = reason_code;
    ptr++;

    *ptr = 0;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    dbg_printf("[%2d] send_cp_disconnect: sent code was %u\n",
            client->session->id, reason_code);

    free(packet);
    client->state = CS_CLOSING;

    return 0;
}

[[gnu::nonnull]]
static int send_cp_pingresp(struct client *client)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    errno = 0;

    length = sizeof(struct mqtt_fixed_header);
    length += 1; /* remaining length 1 byte */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PINGRESP;
    ptr++;

    *ptr = 0;
    ptr++;

    dbg_printf("[%2d] send_cp_pingresp: sending\n", client->session->id);

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    client->last_keep_alive = time(0);
    free(packet);

    return 0;
}

[[gnu::nonnull]]
static int send_cp_connack(struct client *client, reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    uint8_t proplen[4], remlen[4];
    unsigned proplen_len, remlen_len, prop_len;

    errno = 0;

    /* Populate Properties */

    const struct property props[] = {
        { .ident = MQTT_PROP_MAXIMUM_PACKET_SIZE               , .byte4 = MAX_PACKET_LENGTH } ,
        { .ident = MQTT_PROP_RECEIVE_MAXIMUM                   , .byte2 = MAX_RECEIVE_PUBS  } ,
        { .ident = MQTT_PROP_RETAIN_AVAILABLE                  , .byte  = 0                 } ,
        { .ident = MQTT_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE   , .byte  = 0                 } ,
        { .ident = MQTT_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE , .byte  = 0                 } ,
        { .ident = MQTT_PROP_SHARED_SUBSCRIPTION_AVAILABLE     , .byte  = 0                 } ,
    };
    const unsigned num_props = sizeof(props) / sizeof(struct property);

    /* Calculate the Remaining Length */

    prop_len = get_properties_size(&props, num_props);
    proplen_len = encode_var_byte(prop_len, proplen);

    length = 0;

    length += 2;           /* connack var header (1byte for flags, 1byte for code) */
    length += proplen_len; /* properties length (0) */
    length += prop_len;    /* property[] */

    remlen_len = encode_var_byte(length, remlen);

    /* Calculate the total length including header */

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len;  /* remaining length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    /* Now build the packet */

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_CONNACK;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    *ptr = 0;           /* Connect Ack Flags */
    ptr++;

    *ptr = reason_code; /* Connect Reason Code */
    ptr++;

    memcpy(ptr, proplen, proplen_len);
    ptr += proplen_len;

    if (build_properties(&props, num_props, &ptr) == -1)
        goto fail;

    /* Now send the packet */

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    if (reason_code >= MQTT_UNSPECIFIED_ERROR)
        client->state = CS_CLOSING;

    return 0;

fail:
    if (packet)
        free(packet);

    if (reason_code >= MQTT_UNSPECIFIED_ERROR)
        client->state = CS_CLOSING;

    return -1;
}

static int send_cp_pubrec(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    if (reason_code == MQTT_MALFORMED_PACKET ||
            reason_code == MQTT_PROTOCOL_ERROR) {
        /* Is this an illegal state? Should it always be DISCONNECT */
        client->state = CS_CLOSING;
        return 0;
    }

    length = sizeof(struct mqtt_fixed_header);
    length += 1; /* Remaining Length */

    length += 3; /* Packet Identifier + Reason Code */

    length += 1; /* Properties Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)packet)->type = MQTT_CP_PUBREC;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1;
    ptr++;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    *ptr = reason_code;
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    return 0;
}

static int send_cp_pubcomp(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    dbg_printf("[%2d] send_cp_pubcomp: packet_id=%u reason_code=%u\n",
            client->session->id,
            packet_id, reason_code);

    length = sizeof(struct mqtt_fixed_header);
    length +=1; /* Remaining Length */

    length +=2; /* Packet Identifier */
    length +=1; /* Reason Code */
    length +=1; /* Properties Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBCOMP;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1;
    ptr++;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* No properties */
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    pthread_rwlock_wrlock(&client->session->delivery_states_lock);
    for (unsigned idx = 0; idx < client->session->num_message_delivery_states; idx++)
    {
        struct message_delivery_state *mds = client->session->delivery_states[idx];

        if (mds == NULL)
            continue;

        if (mds->session == NULL && mds->message == NULL)
            continue;

        if (mds->packet_identifier == packet_id) {
            mds->completed_at = time(0);
            break;
        }
    }
    pthread_rwlock_unlock(&client->session->delivery_states_lock);

    free(packet);

    return 0;
}

static int send_cp_pubrel(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    if (reason_code == MQTT_MALFORMED_PACKET ||
            reason_code == MQTT_PROTOCOL_ERROR) {
        client->state = CS_CLOSING;
        return 0;
    }

    length = sizeof(struct mqtt_fixed_header);
    length += 1; /* Remaining Length */

    length += 3; /* Packet Identifier + Reason Code */
    length += 1; /* Properties Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBREL;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1;
    ptr++;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, 2);

    ptr += 2;

    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* Properties Length */
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    /* TODO update status thing */

    free(packet);
    return 0;
}

static int send_cp_puback(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    if (reason_code == MQTT_MALFORMED_PACKET ||
            reason_code == MQTT_PROTOCOL_ERROR) {
        client->state = CS_CLOSING;
        return 0;
    }

    length = sizeof(struct mqtt_fixed_header);
    length += 1; /* Remaining Length */

    length += 3; /* Packet Identifier + Reason Code */

    length += 1; /* Properties Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBACK;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1;
    ptr++;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    *ptr = reason_code;
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    return 0;
}


/* Fixed Header:
 *  MQTT Control Packet type [4:7]
 *  Remaining Length (VAR)
 * Variable Header:
 *  Packet Identifier
 *  Properties[]
 * Payload:
 *  Reason Codes[]
 */

[[gnu::nonnull]]
static int send_cp_suback(struct client *client, uint16_t packet_id,
        struct topic_sub_request *request)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    length = sizeof(struct mqtt_fixed_header); /* [0] MQTT Control Packet type */
    length += 1; /* [1]   Remaining Length 1byte */

    length += sizeof(packet_id); /* [2-3] Packet Identifier */
    length += 1; /* [4]   properties length (0) */
    length += 0; /*       properties TODO */
    length += 1 * request->num_topics; /* [5+] */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_SUBACK;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1; /* Remaining Length */
    ptr++;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    *ptr = 0; /* properties length */
    ptr++;

    for (unsigned i = 0; i < request->num_topics; i++) {
        *ptr = request->response_codes[i]; /* TODO which QoS? */
        if (*ptr == MQTT_MALFORMED_PACKET || *ptr == MQTT_PROTOCOL_ERROR)
            client->state = CS_CLOSING;
        ptr++;
    }

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    pthread_rwlock_wrlock(&client->active_packets_lock);
    for (struct packet *tmp = client->active_packets; tmp; tmp = tmp->next_client)
    {
        if (tmp->packet_identifier == packet_id &&
                atomic_load_explicit(&tmp->refcnt, memory_order_relaxed) > 0) {
            atomic_fetch_sub_explicit(&tmp->refcnt, 1, memory_order_acq_rel);
            break;
        }

    }
    pthread_rwlock_unlock(&client->active_packets_lock);

    return 0;
}

static int mark_one_mds(struct message_delivery_state *mds,
        control_packet_t type, reason_code_t client_reason)
{
    assert(mds->packet_identifier != 0);

    time_t now = time(0);

    switch (type)
    {
        case MQTT_CP_PUBACK: /* QoS=1 */
            if (mds->acknowledged_at)
                warnx("mark_message: duplicate acknowledgment");
            mds->acknowledged_at = now;
            mds->released_at = now;
            mds->completed_at = now;
            mds->client_reason = client_reason;
            break;

        case MQTT_CP_PUBREC: /* QoS=2 */
            if (mds->acknowledged_at)
                warnx("mark_message: duplicate acknowledgment");
            mds->acknowledged_at = now;
            mds->client_reason = client_reason;
            break;

        case MQTT_CP_PUBREL: /* QoS=2 */
            if (mds->released_at)
                warnx("mark_message: duplicate release");
            mds->released_at = now;
            break;

        case MQTT_CP_PUBCOMP: /* QoS=2 */
            if (mds->completed_at)
                warnx("mark_message: duplicate completed");
            mds->completed_at = now;
            break;

        default:
            warnx("mark_message: called with illegal type %s",
                    control_packet_str[type]);
            errno = EINVAL;
            goto fail;
    }

    return 1;

fail:
    return -1;
}

    [[gnu::nonnull]]
static int mark_message(control_packet_t type, uint16_t packet_identifier,
        reason_code_t client_reason, struct session *session, role_t role)
{
    int rc;
    struct message_delivery_state *mds;

    assert(packet_identifier != 0);

    if (role == ROLE_RECV)
        goto do_recv;

    pthread_rwlock_wrlock(&global_messages_lock);
    for (struct message *message = global_message_list; message; message = message->next)
    {
        if (message->sender != session)
            continue;

        if (message->sender_status.packet_identifier == packet_identifier) {
            rc = mark_one_mds(&message->sender_status, type, client_reason);

            if (rc == -1)
                goto fail;

            pthread_rwlock_unlock(&global_messages_lock);
            return 0;

        }
    }
    pthread_rwlock_unlock(&global_messages_lock);

    errno = ENOENT;
    goto fail;

    /* else if (role == ROLE_RECV) ... */

do_recv:
    pthread_rwlock_wrlock(&session->delivery_states_lock);
    for (unsigned idx = 0; idx < session->num_message_delivery_states; idx++)
    {
        mds = session->delivery_states[idx];

        if (mds == NULL)
            continue;

        if (mds->packet_identifier != packet_identifier)
            continue;

        rc = mark_one_mds(mds, type, client_reason);

        if (rc == -1)
            goto fail;
        else if (rc == 0)
            continue;

        pthread_rwlock_unlock(&session->delivery_states_lock);
        return 0;
    }

    errno = ENOENT;
fail:
    pthread_rwlock_unlock(&session->delivery_states_lock);
    return -1;
}


/*
 * control packet processing functions
 */

[[gnu::nonnull]]
static int handle_cp_pubrel(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_UNSPECIFIED_ERROR;
    [[maybe_unused]] reason_code_t pubrel_reason_code = 0;
    uint16_t tmp;

    errno = 0;

    if (packet->flags != MQTT_FLAG_PUBREL) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if (bytes_left < 2) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    memcpy(&tmp, ptr, 2);
    packet->packet_identifier = ntohs(tmp);
    ptr += 2;
    bytes_left -= 2;

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (bytes_left == 0) {
        pubrel_reason_code = MQTT_SUCCESS;
        goto skip_props;
    }

    pubrel_reason_code = *ptr;
    ptr++;
    bytes_left--;

    if (bytes_left == 0)
        goto skip_props;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBREL) == -1) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }
skip_props:
    if (bytes_left) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    dbg_printf("[%2d] handle_cp_pubrel: packet_identifier=%u reason_code=%u\n",
            client->session->id,
            packet->packet_identifier, pubrel_reason_code);

    if (mark_message(MQTT_CP_PUBREL, packet->packet_identifier,
                pubrel_reason_code, client->session, ROLE_SEND) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    } else
        reason_code = MQTT_SUCCESS;

    if (send_cp_pubcomp(client, packet->packet_identifier, MQTT_SUCCESS) == -1)
        goto fail;

    return 0;

fail:
    if (reason_code == MQTT_MALFORMED_PACKET ||
            reason_code == MQTT_PROTOCOL_ERROR) {
        errno = EINVAL;
        client->state = CS_CLOSING;
    }

    return -1;
}

    [[gnu::nonnull]]
static int handle_cp_puback(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code, puback_reason_code;

    errno = 0;

    if (bytes_left < 2) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, 2);
    bytes_left -= 2;
    ptr += 2;
    packet->packet_identifier = ntohs(packet->packet_identifier);

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (bytes_left == 0) {
        puback_reason_code = MQTT_SUCCESS;
        goto skip_reason;
    }

    puback_reason_code = *ptr;
    ptr++;
    bytes_left--;

skip_reason:

    if (bytes_left == 0)
        goto skip_property_length;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBACK)) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

skip_property_length:
    /* TODO record acknowledgment */

    dbg_printf("[%2d] handle_cp_puback: client=%u <%s> reason_code=%u packet_identifier=%u\n",
            client->session->id,
            client->id, (char *)client->client_id,
            puback_reason_code, packet->packet_identifier);

    if (mark_message(MQTT_CP_PUBACK, packet->packet_identifier,
                puback_reason_code, client->session, ROLE_RECV) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    return 0;

fail:
    if (reason_code == MQTT_PROTOCOL_ERROR ||
            reason_code == MQTT_MALFORMED_PACKET) {
        errno = EINVAL;
        client->state = CS_CLOSING;
        client->disconnect_reason = reason_code;
    }

    return -1;
}

[[gnu::nonnull]]
static int handle_cp_pubcomp(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code;
    reason_code_t pubcomp_reason_code;

    errno = 0;

    if (bytes_left < 2) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, 2);
    bytes_left -= 2;
    ptr += 2;
    packet->packet_identifier = ntohs(packet->packet_identifier);

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (bytes_left == 0) {
        pubcomp_reason_code = MQTT_SUCCESS;
        goto skip_props;
    }

    pubcomp_reason_code = *ptr;
    ptr++;
    bytes_left--;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBCOMP) == -1) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if (bytes_left) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

skip_props:
    dbg_printf("[%2d] handle_cp_pubcomp: packet_identifier=%u reason_code=%u\n",
            client->session->id,
            packet->packet_identifier,
            pubcomp_reason_code);

    if (mark_message(MQTT_CP_PUBCOMP, packet->packet_identifier,
                pubcomp_reason_code, client->session, ROLE_RECV) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    return 0;

fail:
    if (reason_code == MQTT_MALFORMED_PACKET ||
            reason_code == MQTT_PROTOCOL_ERROR) {
        errno = EINVAL;
        client->state = CS_CLOSING;
        client->disconnect_reason = reason_code;
    }

    return -1;
}

[[gnu::nonnull]]
static int handle_cp_pubrec(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_UNSPECIFIED_ERROR;
    reason_code_t pubrec_reason_code;
    uint16_t packet_identifier;

    errno = 0;

    if (bytes_left < 2) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    memcpy(&packet_identifier, ptr, 2);
    bytes_left -= 2;
    ptr += 2;
    packet_identifier = ntohs(packet_identifier);

    if (packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    /* The Reason Code and Property Length can be omitted if the Reason Code
     * is 0x00 (Success) and there are no Properties. */
    if (bytes_left == 0) {
        pubrec_reason_code = MQTT_SUCCESS;
        goto skip_props;
    }

    pubrec_reason_code = *ptr;
    ptr++;
    bytes_left--;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBREC) == -1) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }
skip_props:
    if (bytes_left) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    dbg_printf("[%2d] handle_cp_pubrec: packet_identifier=%u\n",
            client->session->id, packet_identifier);

    if (mark_message(MQTT_CP_PUBREC, packet_identifier, pubrec_reason_code,
                client->session, ROLE_RECV) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    if (send_cp_pubrel(client, packet_identifier, MQTT_SUCCESS) == -1)
        goto fail;

    if (mark_message(MQTT_CP_PUBREL, packet_identifier, MQTT_SUCCESS,
                client->session, ROLE_RECV) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    return 0;

fail:
    if (reason_code == MQTT_MALFORMED_PACKET ||
            reason_code == MQTT_PROTOCOL_ERROR) {
        errno = EINVAL;
        client->state = CS_CLOSING;
        client->disconnect_reason = reason_code;
    }

    return -1;
}

[[gnu::nonnull]]
static int handle_cp_publish(struct client *client, struct packet *packet, 
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    uint8_t *topic_name = NULL;
    uint16_t packet_identifier = 0;
    reason_code_t reason_code = MQTT_UNSPECIFIED_ERROR;
    unsigned qos = 0;
    [[maybe_unused]] bool flag_retain;
    [[maybe_unused]] bool flag_dup;

    errno = 0;

    if ((topic_name = read_utf8(&ptr, &bytes_left)) == NULL) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if (is_valid_topic_name(topic_name) == -1) {
        reason_code = MQTT_TOPIC_NAME_INVALID;
        goto fail;
    }

    dbg_printf("[%2d] handle_cp_publish: topic=<%s> ",
            client->session->id, topic_name);

    qos = GET_QOS(packet->flags); // & (1<<1|1<<2)) >> 1;
    flag_retain = (packet->flags & MQTT_FLAG_PUBLISH_RETAIN) != 0;
    flag_dup = (packet->flags & MQTT_FLAG_PUBLISH_DUP) != 0;

    dbg_printf("qos=%u ", qos);

    if (qos > 2) {
        reason_code = MQTT_PROTOCOL_ERROR;
        warn("handle_cp_publish: invalid QoS value");
        goto fail;
    }

    if (qos) {
        memcpy(&packet_identifier, ptr, 2);
        packet_identifier = ntohs(packet_identifier);
        ptr += 2;
        bytes_left -= 2;
        if (packet_identifier == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            goto fail;
        }
        dbg_printf("packet_ident=%u ", packet_identifier);
    }

    uint8_t payload_format = 0; /* TODO extract from properties */

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBLISH) == -1) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }
    dbg_printf("payload_format=%u [%lub]", payload_format, bytes_left);

    packet->payload_len = bytes_left;
    if ((packet->payload = malloc(bytes_left)) == NULL)
        goto fail;
    memcpy(packet->payload, ptr, bytes_left);

    dbg_printf("\n");

    struct message *msg;
    if ((msg = register_message(topic_name, payload_format, packet->payload_len,
                    packet->payload, qos, client->session)) == NULL) {
        warn("handle_cp_publish: register_message");
        goto fail;
    }
    msg->sender_status.packet_identifier = packet_identifier;

    free(topic_name);
    topic_name = NULL;

    packet->payload = NULL;
    packet->payload_len = 0;

    time_t now = time(0);

    msg->sender_status.accepted_at = now;

    if (qos == 0) {
        msg->sender_status.acknowledged_at = now;
        msg->sender_status.released_at = now;
        msg->sender_status.completed_at = now;
    } if (qos == 1) {
        if (send_cp_puback(client, packet_identifier, MQTT_SUCCESS) == -1)
            goto fail;
        msg->sender_status.acknowledged_at = now;
        msg->sender_status.released_at = now;
        msg->sender_status.completed_at = now;
    } else if (qos == 2) {
        if (send_cp_pubrec(client, packet_identifier, MQTT_SUCCESS) == -1)
            goto fail;
        msg->sender_status.acknowledged_at = now;
    }

    return 0;

fail:
    dbg_printf("\n");

    if (topic_name)
        free(topic_name);

    if (packet->payload) {
        free(packet->payload);
        packet->payload_len = 0;
    }

    if (qos == 0 || reason_code == MQTT_MALFORMED_PACKET ||
            reason_code == MQTT_PROTOCOL_ERROR) {
        client->state = CS_CLOSING;
        client->disconnect_reason = reason_code;
    } else if (qos == 1)
        send_cp_puback(client, packet_identifier, reason_code);
    else if (qos == 2)
        send_cp_pubrec(client, packet_identifier, reason_code);

    return -1;
}

[[gnu::nonnull]]
static int handle_cp_subscribe(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    struct topic_sub_request *request = NULL;
    void *tmp;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;

    errno = EINVAL;

    if (packet->flags != MQTT_FLAG_SUBSCRIBE)
        goto fail;

    if (bytes_left < 3) {
        errno = ENOSPC;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, 2);
    packet->packet_identifier = ntohs(packet->packet_identifier);
    ptr += 2;
    bytes_left -= 2;

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_SUBSCRIBE) == -1) {
        goto fail;
    }

    /* Check for 0 topic filters */
    if (bytes_left < 3)
        goto fail;

    if ((request = calloc(1, sizeof(struct topic_sub_request))) == NULL)
        goto fail;

    dbg_printf("[%2d] handle_cp_subscribe: packet_identifier=%u\n",
            client->session->id, packet->packet_identifier);

    while (bytes_left)
    {
        if (bytes_left < 3)
            goto fail;

        if ((tmp = realloc(request->topics,
                        sizeof(uint8_t *) * (request->num_topics + 1))) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->topics = tmp;

        const size_t u8_size = sizeof(uint8_t) * (request->num_topics + 1);

        if ((tmp = realloc(request->options, u8_size)) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->options = tmp;

        if ((tmp = realloc(request->response_codes, u8_size)) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->response_codes = tmp;

        if ((request->topics[request->num_topics] = read_utf8(&ptr, &bytes_left)) == NULL)
            goto fail;

        if (bytes_left < 1)
            goto fail;

        /* Validate subscribe options byte */

        /* bits 7 & 6 are reserved */
        if ((*ptr & MQTT_SUBOPT_RESERVED_MASK))
            goto fail;

        /* QoS can't be 3 */
        if ((*ptr & MQTT_SUBOPT_QOS_MASK) == MQTT_SUBOPT_QOS_MASK)
            goto fail;

        /* retain handling can't be 3 */
        if ((*ptr & MQTT_SUBOPT_RETAIN_HANDLING_MASK) == MQTT_SUBOPT_RETAIN_HANDLING_MASK)
            goto fail;

        if (!strncmp("$share/", (char *)request->topics[request->num_topics], 7)) {
            if ((*ptr & MQTT_SUBOPT_NO_LOCAL))
                goto fail;
            request->response_codes[request->num_topics] = MQTT_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED;
        }

        if (is_valid_topic_filter(request->topics[request->num_topics]) == -1) {
            request->response_codes[request->num_topics] = MQTT_TOPIC_FILTER_INVALID;
        } else {
            /* TODO why would response QoS be < request QoS ? */
            request->response_codes[request->num_topics] = (*ptr & MQTT_SUBOPT_QOS_MASK);
        }

        /* TODO do something with the RETAIN flag */

        request->options[request->num_topics] = *ptr++;
        bytes_left--;

        request->num_topics++;
    }

    atomic_fetch_add_explicit(&packet->refcnt, 1, memory_order_relaxed);

    if (subscribe_to_topics(client->session, request) == -1) {
        warn("handle_cp_subscribe: subscribe_to_topics");
        goto fail;
    }

    errno = 0;
    int rc = send_cp_suback(client, packet->packet_identifier, request);
    /* TODO senda an error back? */
    free_topic_subs(request);
    return rc;

fail:
    if (request)
        free_topic_subs(request);
    /* TODO we should send suback ? */
    send_cp_disconnect(client, reason_code);
    return -1;
}

[[gnu::nonnull]]
static int handle_cp_disconnect(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    uint8_t disconnect_reason = 0;

    errno = 0;

    if (bytes_left == 0)
        goto skip;

    if (bytes_left > 0) {
        disconnect_reason = *ptr++;
        bytes_left--;
        dbg_printf("[%2d] handle_cp_disconnect: disconnect reason was %u\n",
                client->session->id, disconnect_reason);
    }

    if (bytes_left > 0) {
        if (parse_properties(&ptr, &bytes_left, &packet->properties,
                    &packet->property_count, MQTT_CP_DISCONNECT) == -1)
            goto fail;

    } else {
skip:
        dbg_printf("[%2d] handle_cp_disconnect: no reason\n", client->session->id);
    }

    if (bytes_left)
        goto fail;

    client->state = CS_CLOSING;
    return 0;

fail:
    warnx("handle_cp_disconnect: packet malformed");
    client->state = CS_CLOSING;
    return -1;
}

[[gnu::nonnull]]
static int handle_cp_pingreq(struct client *client,
        struct packet *packet, const void * /*remain*/)
{
    dbg_printf("[%2d] handle_cp_pingreq\n", client->session->id);

    if (packet->remaining_length > 0) {
        errno = EINVAL;
        send_cp_disconnect(client, MQTT_MALFORMED_PACKET);
        return -1;
    }

    return send_cp_pingresp(client);
}

[[gnu::nonnull]]
static int handle_cp_connect(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t response_code = MQTT_UNSPECIFIED_ERROR;
    uint8_t *will_topic = NULL;
    uint8_t will_qos = 0;
    void *will_payload = NULL;
    uint16_t will_payload_len;
    uint8_t payload_format = 0;
    struct property (*will_props)[] = NULL;
    unsigned num_will_props = 0;
    uint16_t connect_header_length, keep_alive;
    uint8_t protocol_version, connect_flags;
    uint8_t protocol_name[4];

    if (bytes_left < 6+1+1+2) {
        response_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    memcpy(&connect_header_length, ptr, 2);
    connect_header_length = ntohs(connect_header_length);
    ptr += 2;
    bytes_left -= 2;

    memcpy(protocol_name, ptr, 4);
    ptr += 4;
    bytes_left -= 4;

    protocol_version = *ptr++;
    bytes_left--;

    connect_flags = *ptr++;
    bytes_left--;

    memcpy(&keep_alive, ptr, 2);
    keep_alive = ntohs(keep_alive);
    ptr += 2;
    bytes_left -= 2;

    if (memcmp(protocol_name, "MQTT", 4)) {
        response_code = MQTT_UNSUPPORTED_PROTOCOL_VERSION;
        goto fail;
    }

    if (connect_flags & MQTT_CONNECT_FLAG_RESERVED) {
        response_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if (protocol_version != 5) {
        response_code = MQTT_UNSUPPORTED_PROTOCOL_VERSION;
        goto fail;
    }

    /* Properties Length (0) + ClientID (1+1) */
    if (bytes_left < (1 + 2)) {
        response_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_CONNECT) == -1) {
        response_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if (client->client_id != NULL) {
        errno = EEXIST;
        response_code = MQTT_CLIENT_IDENTIFIER_NOT_VALID;
        warnx("client_id already set");
        goto fail;
    }

    if ((client->client_id = read_utf8(&ptr, &bytes_left)) == NULL) {
        response_code = MQTT_MALFORMED_PACKET;
        warn("read_utf8");
        goto fail;
    }
    dbg_printf("[  ] handle_cp_connect: client_id=<%s> ",
            (char *)client->client_id);

    if (connect_flags & MQTT_CONNECT_FLAG_CLEAN_START) {
        dbg_printf("clean_start ");
    }

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        dbg_printf("will_properties ");
        if (parse_properties(&ptr, &bytes_left, &will_props,
                    &num_will_props, -1) == -1) {
            response_code = MQTT_MALFORMED_PACKET;
            warn("handle_cp_connect: parse_properties(will_props)");
            goto fail;
        }
        dbg_printf("[%d props] ", num_will_props);

        dbg_printf("will_topic ");

        will_topic = read_utf8(&ptr, &bytes_left);
        if (will_topic == NULL) {
            response_code = MQTT_MALFORMED_PACKET;
            warn("handle_cp_connect: will_topic");
            goto fail;
        }
        dbg_printf("<%s> ", (char *)will_topic);

        dbg_printf("will_payload ");

        if ((will_payload = read_binary(&ptr, &bytes_left,
                        &will_payload_len)) == NULL) {
            response_code = MQTT_MALFORMED_PACKET;
            warn("handle_cp_connect: read_binary(will_payload)");
            goto fail;
        }
        dbg_printf("[%ub] ", will_payload_len);
        packet->payload = will_payload;
        packet->payload_len = will_payload_len;
        packet->will_props = will_props;
        packet->num_will_props = num_will_props;
    }

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_RETAIN) {
        dbg_printf("will_retain ");
        if ((connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) == 0) {
            response_code = MQTT_PROTOCOL_ERROR;
            warn("handle_cp_connect: Will Retain set without Will Flag");
            goto fail;
        }
    }

    if (connect_flags & MQTT_CONNECT_FLAG_USERNAME) {
        dbg_printf("username ");
        if ((client->username = read_utf8(&ptr, &bytes_left)) == NULL) {
            response_code = MQTT_MALFORMED_PACKET;
            warn("read_utf8(username)");
            goto fail;
        }
        dbg_printf("<%s> ", (char *)client->username);
    }

    if (connect_flags & MQTT_CONNECT_FLAG_PASSWORD) {
        dbg_printf("password ");
        if ((client->password = read_binary(&ptr, &bytes_left,
                        &client->password_len)) == NULL) {
            response_code = MQTT_MALFORMED_PACKET;
            warn("read_utf8(password)");
            goto fail;
        }
        dbg_printf("[%ub] ", client->password_len);
    }

    if (bytes_left) {
        response_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    will_qos = GET_WILL_QOS(connect_flags);

    if ((connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) == 0 && will_qos != 0) {
        response_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (will_qos > 2) {
        response_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    dbg_printf("QoS [%u]\n", will_qos);

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        //dbg_printf("handle_cp_connect: creating a message\n");
        struct message *msg;
        if ((msg = register_message(will_topic, payload_format, will_payload_len,
                        will_payload, will_qos, client->session)) == NULL) {
            warn("handle_cp_connect: register_message");
            free(will_topic);
            will_topic = NULL;
            will_payload = NULL; /* register_message frees in error */
            goto fail;
        }
        free(will_topic);
        will_topic = NULL;
        packet->payload = NULL;
        packet->payload_len = 0;
    }

    client->connect_flags = connect_flags;
    client->protocol_version = protocol_version;
    client->keep_alive = keep_alive;

    if ((client->session = find_session(client)) == NULL) {
create_new_session:
        if ((client->session = alloc_session(client)) == NULL) {
            goto fail;
        }
        dbg_printf("[%2d] handle_cp_connect: new_session\n", client->session->id);
    } else {
        /* Existing Session */
        if (connect_flags & MQTT_CONNECT_FLAG_CLEAN_START) {
            /* ... we don't want to re-use it */
            dbg_printf("[  ] handle_cp_connect: clean existing session [%d]\n",
                    client->session->id);
            client->session->state = SESSION_DELETE;
            client->session = NULL;
            goto create_new_session;
        }
        client->connect_response_flags |= MQTT_CONNACK_FLAG_SESSION_PRESENT;
        dbg_printf("[%2d] handle_cp_connect: connection re-established\n",
                client->session->id);
    }

    send_cp_connack(client, MQTT_SUCCESS); /* TODO handle failure */
    return 0;

fail:
    if (response_code == MQTT_MALFORMED_PACKET ||
            response_code == MQTT_PROTOCOL_ERROR) {
        client->state = CS_CLOSING;
        client->disconnect_reason = response_code;
    } else
        send_cp_connack(client, response_code);

    if (will_topic)
        free(will_topic);
    if (will_props)
        free_properties(will_props, num_will_props);
    if (will_payload)
        free(will_payload);
    return -1;
}

/*
 * control packet function lookup table
 */

static const control_func_t control_functions[MQTT_CP_MAX] = {
    [MQTT_CP_PUBLISH]    = handle_cp_publish,
    [MQTT_CP_PUBACK]     = handle_cp_puback,
    [MQTT_CP_PUBREC]     = handle_cp_pubrec,
    [MQTT_CP_PUBREL]     = handle_cp_pubrel,
    [MQTT_CP_PUBCOMP]    = handle_cp_pubcomp,
    [MQTT_CP_SUBSCRIBE]  = handle_cp_subscribe,
    [MQTT_CP_CONNECT]    = handle_cp_connect,
    [MQTT_CP_PINGREQ]    = handle_cp_pingreq,
    [MQTT_CP_DISCONNECT] = handle_cp_disconnect,
};

/*
 * other functions
 */


[[gnu::nonnull]]
static int parse_incoming(struct client *client)
{
    ssize_t rd_len;
    reason_code_t reason_code;
    struct mqtt_fixed_header *hdr;

    hdr = NULL;
    reason_code = MQTT_MALFORMED_PACKET;

    switch (client->parse_state)
    {
        case READ_STATE_NEW:
            dbg_printf("\n[%2d] parse_incoming: READ_STATE_NEW client=%u <%s>\n",
                    client->session ? client->session->id : (id_t)-1,
                    client->id, (char *)client->client_id);
            client->read_offset = 0;
            client->rl_offset = 0;
            client->rl_multi = 1;
            client->rl_value = 0;
            client->read_need = sizeof(struct mqtt_fixed_header) + 1;
            client->parse_state = READ_STATE_HEADER;
            if (client->packet_buf) {
                free(client->packet_buf);
                client->packet_buf = NULL;
            }
            if (client->new_packet &&
                    client->new_packet->packet_identifier == 0) {
                if (atomic_load_explicit(&client->new_packet->refcnt, memory_order_relaxed) == 0)
                    free_packet(client->new_packet, true);
            }
            client->new_packet = NULL;

            /* fall through */

        case READ_STATE_HEADER:
            goto more;
            /* fall through */
        case READ_STATE_MORE_HEADER:
more:
            rd_len = read(client->fd,
                    &client->header_buffer[client->read_offset],
                    client->read_need);
            if (rd_len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) ) {
                return 0;
            } else if (rd_len == -1) {
                log_io_error(NULL, rd_len, client->read_need, false);
                goto fail;
            } else if (rd_len == 0) {
eof:
                /* EOF - shared between states */
                client->state = CS_DISCONNECTED;
                close_socket(&client->fd);
                if (client->session)
                    client->session->last_connected = time(0);
                client->parse_state = READ_STATE_NEW;
                return 0;
            } else if (rd_len < client->read_need) {
                client->read_offset += rd_len;
                client->read_need -= rd_len;
                return 0;
            }

            client->read_offset += rd_len;
            client->read_need -= rd_len;

            if (client->parse_state == READ_STATE_HEADER) {
                hdr = (void *)client->header_buffer;

                if (hdr->type >= MQTT_CP_MAX || hdr->type == 0) {
                    warnx("hdr->type");
                    goto fail;
                }

                if (control_functions[hdr->type] == NULL) {
                    warnx("no func for %u", hdr->type);
                    goto fail;
                }

                if (hdr->flags & packet_permitted_flags[hdr->type]) {
                    warnx("illegal flags");
                    goto fail;
                }

                client->parse_state = READ_STATE_MORE_HEADER;

                if (client->read_need == 0)
                    goto lenread;
            }

            if (client->parse_state == READ_STATE_MORE_HEADER) {
lenread:
                uint8_t tmp;
                hdr = (void *)client->header_buffer;

                if (client->rl_multi > 128*128*128) {
                    warn("var len overflow");
                    goto fail;
                }

                tmp = client->header_buffer[1 + client->rl_offset];

                client->rl_value += (tmp & 127) * client->rl_multi;
                client->rl_multi *= 128;

                client->rl_offset++;
                client->read_need++;

                if ( (tmp & 128) != 0 )
                    return 0;

                if (client->rl_value > MAX_PACKET_LENGTH) {
                    client->disconnect_reason = MQTT_PACKET_TOO_LARGE;
                    goto fail;
                }

                dbg_printf("[%2d] parse_incoming: client=%u type=%u <%s> flags=%u remaining_length=%u\n",
                        client->session ? client->session->id : (id_t)-1,
                        client->id,
                        hdr->type, control_packet_str[hdr->type],
                        hdr->flags, client->rl_value);

                if ((client->new_packet = alloc_packet(client)) == NULL) {
                    warn("alloc_packet");
                    goto fail;
                }

                client->new_packet->remaining_length = client->rl_value;
                client->new_packet->type = hdr->type;
                client->new_packet->flags = hdr->flags;

                if ((client->packet_buf = malloc(client->new_packet->remaining_length)) == NULL) {
                    warn("malloc(%lu)", client->new_packet->remaining_length);
                    goto fail;
                }

                client->parse_state = READ_STATE_BODY;
                client->packet_offset = 0;
                client->read_need = client->new_packet->remaining_length;
                if (client->read_need == 0)
                    goto readbody;
            }
            break;

        case READ_STATE_BODY:
readbody:
            if (client->read_need == 0)
                goto exec_control;

            rd_len = read(client->fd,
                    &client->packet_buf[client->packet_offset],
                    client->read_need);

            if (rd_len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return 0;
            } else if (rd_len == -1) {
                log_io_error(NULL, rd_len, client->read_need, false);
                goto fail;
            } else if (rd_len == 0) {
                goto eof;
            } else if (rd_len < client->read_need) {
                client->packet_offset += rd_len;
                client->read_need -= rd_len;
                return 0;
            }
exec_control:
            client->parse_state = READ_STATE_NEW;

            dbg_printf("[%2d] parse_incoming: client=%u control_function\n",
                    client->session ? client->session->id : (id_t)-1,
                    client->id);
            if (control_functions[client->new_packet->type](client,
                        client->new_packet, client->packet_buf) == -1) {
                warn("control_function");
                goto fail;
            }
            atomic_fetch_sub_explicit(&client->new_packet->refcnt, 1, memory_order_acq_rel);
            client->new_packet = NULL;

            break;
    }
    return 0;

fail:
    if (client->packet_buf) {
        free(client->packet_buf);
        client->packet_buf = NULL;
    }
    if (client->new_packet) {
        free_packet(client->new_packet, true);
        client->new_packet = NULL;
    }

    client->parse_state = READ_STATE_NEW;
    client->state = CS_CLOSING;
    client->disconnect_reason = reason_code;

    return -1;
}

/* Clients */

static void client_tick(void)
{
    pthread_rwlock_wrlock(&global_clients_lock);
    for (struct client *clnt = global_client_list, *next; clnt; clnt = next)
    {
        next = clnt->next;

        switch (clnt->state)
        {
            case CS_ACTIVE:
            case CS_NEW:
                continue;

            case CS_DISCONNECTED:
                if (clnt->session) {
                    clnt->session->client = NULL;
                    clnt->session = NULL;
                }
                clnt->state = CS_CLOSED;
                continue;

            case CS_CLOSING:
                if (clnt->disconnect_reason)
                    send_cp_disconnect(clnt, clnt->disconnect_reason);
                clnt->state = CS_CLOSED;
                break;

            case CS_CLOSED:
                free_client(clnt, false);
                break;
        }
    }
    pthread_rwlock_unlock(&global_clients_lock);
}

static void tick_msg(struct message *msg)
{
    unsigned num_sent = 0, num_to_send = 0;
    struct packet *packet;
    struct message_delivery_state *mds;

    if (msg->delivery_states == NULL)
        return;

    time_t now = time(0);

    dbg_printf(NGRN "tick_msg: id=%u sender.id=%u #mds=%u"CRESET"\n",
            msg->id,
            msg->sender->id,
            msg->num_message_delivery_states
            );

    pthread_rwlock_wrlock(&msg->delivery_states_lock);
    for (unsigned idx = 0; idx < msg->num_message_delivery_states; idx++) {
        mds = msg->delivery_states[idx];

        if (mds == NULL)
            continue;

        num_to_send++;

        if (mds->acknowledged_at) {
            num_sent++;
            continue;
        }

        if (mds->last_sent && (now - mds->last_sent < 5))
            continue;

        if (mds->session == NULL || mds->message != msg) {
            warnx("tick_msg: message_delivery_state is corrupt");
            continue;
        }

        /* disconnected session */
        if (mds->session->client == NULL)
            continue;

        dbg_printf(NGRN"tick_msg: sending message: id=%u acknowledged_at=%lu last_sent=%lu"CRESET"\n",
                mds->id, mds->acknowledged_at, mds->last_sent);

        if ((packet = alloc_packet(mds->session->client)) == NULL) {
            warn("tick_msg: unable to alloc_packet for msg on topic <%s>",
                    msg->topic->name);
            continue;
        }

        /* this code might execute more than once, so avoid a double refcnt */
        if (mds->last_sent == 0)
            atomic_fetch_add_explicit(&msg->refcnt, 1, memory_order_relaxed);

        packet->message = msg;
        packet->type = MQTT_CP_PUBLISH;
        packet->flags |= MQTT_FLAG_PUBLISH_QOS(msg->qos);

        if (msg->qos) {
            if (mds->packet_identifier == 0) {
                mds->packet_identifier = ++packet->owner->last_packet_id;
            }
            packet->packet_identifier = mds->packet_identifier;

            /* acknowledged_at is 0, so this must be a retry? */
            if (mds->last_sent) {
                packet->flags |= MQTT_FLAG_PUBLISH_DUP;
            }
        }

        packet->reason_code = MQTT_SUCCESS;

        /* TODO make this async START ?? */
        mds->last_sent = time(0);
        if (send_cp_publish(packet) == -1) {
            mds->last_sent = 0;
            warn("tick_msg: unable to send_cp_publish");
            atomic_fetch_sub_explicit(&packet->refcnt, 1, memory_order_acq_rel);
            free_packet(packet, true); /* Anything else? */
            continue;
        }
        
        /* Unless we get a network error, just assume it works */
        if (msg->qos == 0) {
            mds->acknowledged_at = time(0);
            mds->completed_at = mds->acknowledged_at;
            mds->released_at = mds->acknowledged_at;
        }

        atomic_fetch_sub_explicit(&packet->refcnt, 1, memory_order_acq_rel);
        free_packet(packet, true);
        /* TODO async END */
    }

    dbg_printf(NGRN"tick_msg: num_sent=%u num_message_delivery_states=%u"CRESET"\n",
            num_sent, msg->num_message_delivery_states);

    /* We have now sent everything */
    if (num_sent == num_to_send /*msg->num_message_delivery_states*/) { /* TODO this doesn't handle holes? */
        /* TODO replace with list of subscribers to message, removal thereof,
         * then dequeue when none left */
        dbg_printf(BGRN"tick_msg: tidying up"CRESET"\n");

        while (msg->num_message_delivery_states && msg->delivery_states)
        {
            unsigned idx = 0;
            struct message_delivery_state *mds;
again:
            dbg_printf(NCYN"tick_msg: idx=%u"CRESET"\n",idx);
            if (idx >= msg->num_message_delivery_states)
                break;

            mds = msg->delivery_states[idx++];

            if (mds == NULL)
                break;

            if (mds->completed_at == 0) {
                dbg_printf(NCYN"tick_msg: not completed"CRESET"\n");
                goto again;
            }

            dbg_printf(NCYN"tick_msg: unlink %u from session %u[%u] and message %u[%u]"CRESET"\n",
                    mds->id,
                    mds->session ? mds->session->id : 0,
                    mds->session ? mds->session->refcnt : 0,
                    mds->message ? mds->message->id : 0,
                    mds->message ? mds->message->refcnt : 0);

            remove_delivery_state(&msg->delivery_states,
                    &msg->num_message_delivery_states, mds);
            atomic_fetch_sub_explicit(&mds->message->refcnt, 1, memory_order_acq_rel);
            mds->message = NULL;

            if (mds->session) {
                pthread_rwlock_wrlock(&mds->session->delivery_states_lock);
                remove_delivery_state(&mds->session->delivery_states,
                        &mds->session->num_message_delivery_states, mds);
                pthread_rwlock_unlock(&mds->session->delivery_states_lock);
                atomic_fetch_sub_explicit(&mds->session->refcnt, 1, memory_order_acq_rel);
                mds->session = NULL;
            }

            free_message_delivery_state(mds);
        }

        /* We can't just dequeue() and MSG_DEAD if any mds are not complated_at */
        if (msg->topic && msg->num_message_delivery_states == 0) {
            dbg_printf(BGRN"tick_msg: dequeue"CRESET"\n");
            if (dequeue_message(msg) == -1) {
                warn("tick_msg: dequeue_message failed");
            }
            msg->state = MSG_DEAD;
        }

    }
    pthread_rwlock_unlock(&msg->delivery_states_lock);
}

/* Topics */

static void topic_tick(void)
{
    unsigned max_messages = MAX_MESSAGES_PER_TICK;

    pthread_rwlock_rdlock(&global_messages_lock);
    pthread_rwlock_wrlock(&global_topics_lock);
    for (struct topic *topic = global_topic_list; topic; topic = topic->next)
    {
        if (max_messages == 0 || topic->pending_queue == NULL ||
                topic->num_subscribers == 0)
            continue;

        /* Iterate over the queued messages on this topic */
        pthread_rwlock_wrlock(&topic->pending_queue_lock);
        for (struct message *msg = topic->pending_queue, *next; msg; msg = next)
        {
            if (max_messages-- == 0)
                break;

            next = msg->next_queue;
            tick_msg(msg);
        }
        pthread_rwlock_unlock(&topic->pending_queue_lock);
    }
    pthread_rwlock_unlock(&global_topics_lock);
    pthread_rwlock_unlock(&global_messages_lock);
}

/* Messages */

static void message_tick(void)
{
    pthread_rwlock_wrlock(&global_messages_lock);
    for (struct message *msg = global_message_list, *next; msg; msg = next)
    {
        next = msg->next;

        if (msg->state != MSG_DEAD)
            continue;

        free_message(msg, false);
    }
    pthread_rwlock_unlock(&global_messages_lock);
}

/* Sessions */

static void session_tick(void)
{
    pthread_rwlock_wrlock(&global_sessions_lock);
    for (struct session *session = global_session_list, *next; session; session = next)
    {
        next = session->next;

        if (session->state == SESSION_DELETE)
            free_session(session, false);
    }
    pthread_rwlock_unlock(&global_sessions_lock);
}

static void packet_tick(void)
{
    pthread_rwlock_wrlock(&global_packets_lock);

    for (struct packet *next, *pkt = global_packet_list; pkt; pkt = next)
    {
        next = pkt->next;

        if (atomic_load_explicit(&pkt->refcnt, memory_order_relaxed) == 0)
            free_packet(pkt, false);
    }

    pthread_rwlock_unlock(&global_packets_lock);
}

static void tick(void)
{
    session_tick();
    client_tick();
    topic_tick();
    message_tick();
    packet_tick();
}

static int main_loop(int mother_fd)
{
    bool has_clients;
    fd_set fds_in, fds_out, fds_exc;

    running = true;

    while (running)
    {
        int max_fd = mother_fd;
        int rc = 0;
        struct timeval tv;

        FD_ZERO(&fds_in);
        FD_ZERO(&fds_out);
        FD_ZERO(&fds_exc);

        FD_SET(mother_fd, &fds_in);
        FD_SET(mother_fd, &fds_exc);

        has_clients = false;

        pthread_rwlock_rdlock(&global_clients_lock);
        for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
        {
            if (clnt->state == CS_NEW || clnt->state == CS_DISCONNECTED ||
                    clnt->fd == -1)
                continue;

            if (clnt->fd > max_fd)
                max_fd = clnt->fd;

            FD_SET(clnt->fd, &fds_in);
            //FD_SET(clnt->fd, &fds_out);
            FD_SET(clnt->fd, &fds_exc);

            has_clients = true;
        }
        pthread_rwlock_unlock(&global_clients_lock);

        tv.tv_sec = 0;
        tv.tv_usec = 10000;

        if (has_clients == false) {
            dbg_printf("\n     main_loop: no connections, going to sleep\n");
            rc = select(max_fd + 1, &fds_in, &fds_out, &fds_exc, NULL);
        } else {
            rc = select(max_fd + 1, &fds_in, &fds_out, &fds_exc, &tv);
        }

        if (rc == 0) {
            tick();
            continue;
        } else if (rc == -1 && (errno == EAGAIN || errno == EINTR)) {
            continue;
        } else if (rc == -1) {
            warn("main_loop: select");
            return -1;
        }

        if (FD_ISSET(mother_fd, &fds_in)) {
            struct sockaddr_in sin_client;
            socklen_t sin_client_len = sizeof(sin_client);
            int child_fd;
            struct client *new_client;

            dbg_printf("     main_loop: new connection\n");
            if ((child_fd = accept(mother_fd,
                            (struct sockaddr *)&sin_client,
                            &sin_client_len)) == -1) {
                warn("main_loop: accept failed");
                continue;
            }

            int flags;

            if ((flags = fcntl(child_fd, F_GETFL)) == -1) {
                warn("main_loop: fcntl: F_GETFL");
                goto shit_fd;
            }

            flags |= O_NONBLOCK;

            if (fcntl(child_fd, F_SETFL, flags) == -1) {
                warn("main_loop: fcntl: F_SETFL");
                goto shit_fd;
            }

            if ((new_client = alloc_client()) == NULL) {
shit_fd:
                close_socket(&child_fd);
                warn("main_loop: alloc_client");
                continue;
            }

            if (inet_ntop(AF_INET, &sin_client.sin_addr.s_addr,
                        new_client->hostname, sin_client_len) == NULL)
                warn("inet_ntop");

            new_client->fd = child_fd;
            new_client->state = CS_ACTIVE;
            new_client->remote_port = ntohs(sin_client.sin_port);
            new_client->remote_addr = ntohl(sin_client.sin_addr.s_addr);


            dbg_printf("     main_loop: new client from [%s:%u]\n",
                    new_client->hostname, new_client->remote_port);
        }

        if (FD_ISSET(mother_fd, &fds_exc))
            warnx("main_loop: mother_fd is in fds_exc??");

        pthread_rwlock_rdlock(&global_clients_lock); {
            for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
            {
                if (clnt->state != CS_ACTIVE)
                    continue;

                if (clnt->fd != -1 && FD_ISSET(clnt->fd, &fds_in)) {
                    if (parse_incoming(clnt) == -1) {
                        /* TODO do something? */ ;
                    }
                }

                if (clnt->fd != -1 && FD_ISSET(clnt->fd, &fds_out)) {
                    /* socket is writable without blocking [ish] */
                }

                if (clnt->fd != -1 && FD_ISSET(clnt->fd, &fds_exc)) {
                    dbg_printf("     main_loop exception event on %p[%d]\n",
                            (void *)clnt, clnt->fd);
                    /* TODO close? */
                }
            }
        } pthread_rwlock_unlock(&global_clients_lock);

        tick();
    }

    return 0;
}

/*
 * external functions
 */

int main(int argc, char *argv[])
{
    {
        int opt;
        while ((opt = getopt(argc, argv, "hVp:H:")) != -1)
        {
            switch (opt)
            {
                case 'H':
                    if (inet_pton(AF_INET, optarg, &opt_listen) == -1) {
                        warn("main: inet_pton");
                        exit(EXIT_FAILURE);
                    }
                    break;
                case 'p':
                    int tmp = atoi(optarg);
                    if (tmp == 0 || tmp > USHRT_MAX) {
                        show_usage(stderr, argv[0]);
                        exit(EXIT_FAILURE);
                    }
                    opt_port = tmp;
                    break;
                case 'h':
                    show_usage(stdout, argv[0]);
                    exit(EXIT_SUCCESS);
                case 'V':
                    show_version(stdout);
                    exit(EXIT_SUCCESS);
                default:
                    show_usage(stderr, argv[0]);
                    exit(EXIT_FAILURE);
            }
        }
    }

    setvbuf(stdin, NULL, _IONBF, 0);
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    struct sigaction sa = {
        .sa_sigaction = sh_sigint,
        .sa_flags = SA_SIGINFO,
    };

    if (sigaction(SIGINT, &sa, NULL) == -1)
        err(EXIT_FAILURE, "sigaction(SIGINT)");
    if (sigaction(SIGQUIT, &sa, NULL) == -1)
        err(EXIT_FAILURE, "sigaction(SIGQUIT)");
    if (sigaction(SIGTERM, &sa, NULL) == -1)
        err(EXIT_FAILURE, "sigaction(SIGTERM)");
    if (sigaction(SIGHUP, &sa, NULL) == -1)
        err(EXIT_FAILURE, "sigaction(SIGHUP)");

    atexit(close_all_sockets);
    atexit(free_all_message_delivery_states);
    atexit(free_all_messages);
    atexit(free_all_packets);
    atexit(free_all_sessions);
    atexit(free_all_clients);
    atexit(free_all_topics);

    const char *topic_name;
    while (optind < argc)
    {
        if ((topic_name = strdup(argv[optind])) == NULL)
            err(EXIT_FAILURE, "main: strdup(argv[])");
        if (is_valid_topic_filter((const uint8_t *)topic_name) == -1) {
            free((void *)topic_name);
            warn("main: <%s> is not a valid topic filter, skipping",
                    (char *)argv[optind]);
        } else if (register_topic((const uint8_t *)topic_name) == NULL) {
            warn("main: register_topic(<%s>)", topic_name);
            free((void *)topic_name);
        }
        optind++;
    }

    if ((mother_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
        err(EXIT_FAILURE, "socket");

    struct linger linger = {
        .l_onoff = 0,
        .l_linger = 0,
    };

    if (setsockopt(mother_fd, SOL_SOCKET, SO_LINGER, &linger,
                sizeof(linger)) == -1)
        warn("setsockopt(SO_LINGER)");

    int reuse = 1;

    if (setsockopt(mother_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                sizeof(reuse)) == -1)
        warn("setsockopt(SO_REUSEADDR)");

    struct sockaddr_in sin = {0};

    sin.sin_family = AF_INET;
    sin.sin_port = htons(opt_port);
    sin.sin_addr.s_addr = htonl(opt_listen.s_addr);

    char bind_addr[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &sin.sin_addr, bind_addr, sizeof(bind_addr));

    dbg_printf("     main: binding to %s:%u\n", bind_addr, opt_port);

    if (bind(mother_fd, (struct sockaddr *)&sin, sizeof(sin)) == -1)
        err(EXIT_FAILURE, "bind");

    if (listen(mother_fd, 5) == -1)
        err(EXIT_FAILURE, "listen");

    if (main_loop(mother_fd) == -1)
        return EXIT_FAILURE;

    return EXIT_SUCCESS;
}
