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

#include "mqtt.h"

#ifdef NDEBUG
# define dbg_printf(...)
#else
# define dbg_printf(...) printf(__VA_ARGS__)
#endif

typedef int (*control_func_t)(struct client *, struct packet *, const void *);

static int mother_fd = -1;
static bool running;

#define MAX_PACKETS 256
#define MAX_CLIETNS 64
#define MAX_TOPICS 1024
#define MAX_MESSAGES 16384

static pthread_rwlock_t global_packets_lock = PTHREAD_RWLOCK_INITIALIZER;
static struct packet *global_packet_list = NULL;
static unsigned num_packets = 0;

static pthread_rwlock_t global_clients_lock = PTHREAD_RWLOCK_INITIALIZER;
static struct client *global_client_list = NULL;
static unsigned num_clients = 0;

static pthread_rwlock_t global_topics_lock = PTHREAD_RWLOCK_INITIALIZER;
static struct topic *global_topic_list = NULL;
static unsigned num_topics = 0;

static pthread_rwlock_t global_messages_lock = PTHREAD_RWLOCK_INITIALIZER;
static struct message *global_message_list = NULL;
static unsigned num_messages = 0;


/*
 * forward declarations
 */

static int unsubscribe_from_topic(struct client *client, struct topic *topic);
static int dequeue_message(struct message *msg);

/*
 * command line stuff
 */

[[gnu::nonnull]] static void show_version(FILE *fp)
{
    fprintf(fp, "fail-mqttd " VERSION "\n" "\n" "Written by http://github.com/juur");
}

[[gnu::nonnull]] static void show_usage(FILE *fp, const char *name)
{
    fprintf(fp, "fail-mqttd -- a terrible implementation of MQTT\n" "\n"
            "Usage: %s [hV] [TOPIC..]\n"
            "Provides a MQTT broker on the default mqtt port, "
            "optionally pre-creates topics per additional command line arguments, "
            "if provided.\n"
            "\n"
            "Options:\n"
            "  -h     show help\n"
            "  -V     show version\n" "\n",
            name);
}

/*
 * debugging helpers
 */

#define log_io_error(m,r,e,d) _log_io_error(m,r,e,d,__FILE__,__func__,__LINE__);
static int _log_io_error(const char *msg, ssize_t rc, ssize_t expected, bool die, const char *file,
        const char *func, int line)
{
    if (rc == -1) {
        if (die)
            err(EXIT_FAILURE, "%s: read error at %s:%u: %s", func, file, line, msg ? msg : "");
        else
            warn("%s: read error at %s:%u: %s", func, file, line, msg ? msg : "");
        return -1;
    }

    if (die)
        errx(EXIT_FAILURE, "%s: short read (%lu < %lu) at %s:%u: %s", func, rc, expected, file, line, msg ? msg : "");
    else
        warnx("%s: short read (%lu < %lu) at %s:%u: %s", func, rc, expected, file, line, msg ? msg : "");
    errno = ERANGE;

    return -1;
}

[[maybe_unused]] static void dump_topics(void)
{
    dbg_printf("global_topics:\n");
    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
    {
        dbg_printf("  topic: <%s>\n", (char *)tmp->name);
    }
    pthread_rwlock_unlock(&global_topics_lock);
}

/*
 * allocators / deallocators
 */

[[gnu::nonnull]] static void close_socket(int *fd)
{
    if (*fd != -1) {
        shutdown(*fd, SHUT_RDWR);
        close(*fd);
        *fd = -1;
    }
}

[[gnu::nonnull]] static void free_topic_subs(struct topic_sub_request *request)
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

[[gnu::nonnull]] static void free_topic(struct topic *topic)
{
    dbg_printf("free_topic: <%s>\n",
            (topic->name == NULL) ? "" : (char *)topic->name
            );

    pthread_rwlock_wrlock(&global_topics_lock);
    if (global_topic_list == topic) {
        global_topic_list = topic->next;
    } else for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
    {
        if (tmp->next == topic) {
            tmp->next = topic->next;
            break;
        }
    }
    pthread_rwlock_unlock(&global_topics_lock);
    topic->next = NULL;


    /* TODO check if we should have a wrlock here,
     * not inside unsubscribe_from_topic */
    if (topic->subscribers) {
        dbg_printf("free_topic: subscribers=%p num_subscribers=%u\n",
                (void *)topic->subscribers, topic->num_subscribers);

        /* keep going, but restart if we unsubscribe as the array
         * will be modified */
        while (topic->num_subscribers)
        {
            for(unsigned idx = 0; idx < topic->num_subscribers; idx++)
            {
                dbg_printf("free_topic: subscriber[%u]\n", idx);
                if ((*topic->subscribers)[idx].client == NULL &&
                        (*topic->subscribers)[idx].topic == NULL)
                    continue;

                /* TODO should we handle the return code ? */
                (void)unsubscribe_from_topic((*topic->subscribers)[idx].client,
                        (*topic->subscribers)[idx].topic);
            }
        }

        /* Not sure this locking is useful */
        pthread_rwlock_wrlock(&topic->subscribers_lock);
        free(topic->subscribers);
        topic->subscribers = NULL;
        pthread_rwlock_unlock(&topic->subscribers_lock);
    }

    pthread_rwlock_wrlock(&topic->pending_queue_lock);
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
    pthread_rwlock_unlock(&topic->pending_queue_lock);

    if (topic->name) {
        free((void *)topic->name);
        topic->name = NULL;
    }

    pthread_rwlock_destroy(&topic->pending_queue_lock);
    pthread_rwlock_destroy(&topic->subscribers_lock);
    num_topics--;
    free(topic);
}

[[gnu::nonnull, gnu::access(read_write,1,2)]] static void free_properties(struct property (*props)[],
        unsigned count)
{
    for (unsigned i = 0; i < count; i++)
    {
        switch ((*props)[i].type)
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

[[gnu::nonnull]] static void free_packet(struct packet *pck)
{
    struct packet *tmp;

    if (atomic_load_explicit(&pck->refcnt, memory_order_relaxed) > 0) {
        warn("free_packet: attempt to free packet with refcnt");
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

[[gnu::nonnull]] static void free_message(struct message *msg, bool need_lock)
{
    if (msg->topic) {
        warn("free_message: attempt to free with topic <%s> set", msg->topic->name);
        return;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_messages_lock);
    if (global_message_list == msg) {
        global_message_list = msg->next;
    } else for (struct message *tmp = global_message_list; tmp; tmp = tmp->next) {
        if (tmp->next == msg) {
            tmp->next = msg->next;
            break;
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
    if (msg->client_states)
        free(msg->client_states);

    pthread_rwlock_destroy(&msg->client_states_lock);

    num_messages--;
    free(msg);
}

[[gnu::nonnull]] static void free_client(struct client *client, bool needs_lock)
{
    struct client *tmp;

    client->state = CS_CLOSED;

    if (needs_lock)
        pthread_rwlock_wrlock(&global_clients_lock);

    if (global_client_list == client) {
        global_client_list = client->next;
    } else for (tmp = global_client_list; tmp; tmp = tmp->next) {
        if (tmp->next == client) {
            tmp->next = client->next;
            break;
        }
    }
    if (needs_lock)
        pthread_rwlock_unlock(&global_clients_lock);

    client->next = NULL;

    pthread_rwlock_wrlock(&client->active_packets_lock);
    for (struct packet *p = client->active_packets, *next; p; p = next)
    {
        next = p->next_client;
        free_packet(p);
    }
    client->active_packets = NULL;
    pthread_rwlock_unlock(&client->active_packets_lock);

    pthread_rwlock_wrlock(&global_packets_lock);
    for (struct packet *p = global_packet_list; p; p = p->next)
    {
        if (p->owner == client)
            p->owner = NULL; /* TODO locking? */
    }
    pthread_rwlock_unlock(&global_packets_lock);


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

    pthread_rwlock_wrlock(&client->subscriptions_lock);
    if (client->subscriptions) {
        while (client->num_subscriptions)
            for (unsigned idx = 0; idx < client->num_subscriptions; idx++) {
                if ((*client->subscriptions)[idx].client == NULL && (*client->subscriptions)[idx].topic == NULL)
                    continue;
                unsubscribe_from_topic((*client->subscriptions)[idx].client, (*client->subscriptions)[idx].topic);
            }
        free(client->subscriptions);
        client->subscriptions = NULL;
        client->num_subscriptions = 0;
    }
    pthread_rwlock_unlock(&client->subscriptions_lock);

    /* TODO do this properly */
    pthread_rwlock_wrlock(&client->packet_ids_to_states_lock);
    if (client->packet_ids_to_states)
        free (client->packet_ids_to_states);
    pthread_rwlock_unlock(&client->packet_ids_to_states_lock);

    if (client->packet_buf)
        free(client->packet_buf);

    pthread_rwlock_destroy(&client->subscriptions_lock);
    pthread_rwlock_destroy(&client->active_packets_lock);
    pthread_rwlock_destroy(&client->packet_ids_to_states_lock);

    num_clients--;
    free(client);
}

[[gnu::malloc]] static struct topic *alloc_topic(const uint8_t *name)
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
    num_topics++;

    return ret;
}

[[gnu::malloc,gnu::warn_unused_result]] static struct packet *alloc_packet(struct client *owner)
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

    return ret;
}

[[gnu::malloc]] static struct message *alloc_message(void)
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

    return msg;
}

[[gnu::malloc]] static struct client *alloc_client(void)
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

    if (pthread_rwlock_init(&client->subscriptions_lock, NULL) == -1)
        goto fail;
    if (pthread_rwlock_init(&client->active_packets_lock, NULL) == -1)
        goto fail;
    if (pthread_rwlock_init(&client->packet_ids_to_states_lock, NULL) == -1)
        goto fail;

    pthread_rwlock_wrlock(&global_clients_lock);
    client->next = global_client_list;
    global_client_list = client;
    num_clients++;
    pthread_rwlock_unlock(&global_clients_lock);

    return client;

fail:
    if (client)
        free(client);

    return NULL;
}


/*
 * packet parsing helpers
 */

[[gnu::nonnull]] static int is_valid_topic_name(const uint8_t *name)
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

[[gnu::nonnull]] static int is_valid_topic_filter(const uint8_t *name)
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
        /* The multi-level wildcard character MUST be specified either on its own or
         * following a topic level separator.
         *
         * In either case it MUST be the last character specified in the Topic Filter
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

[[gnu::nonnull, maybe_unused]] static int encode_var_byte(uint32_t value, uint8_t out[4])
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

[[gnu::nonnull]] static uint32_t read_var_byte(const uint8_t **const ptr, size_t *bytes_left)
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

[[gnu::nonnull]] static void *read_binary(const uint8_t **const ptr, size_t *bytes_left, uint16_t *length)
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

[[gnu::nonnull]] static uint8_t *read_utf8(const uint8_t **const ptr, size_t *bytes_left)
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

#define MAX_PROPERTIES 32

/* a type of -1U is used for situations where the properties are NOT the standard ones
 * in a packet, e.g. "will_properties" */
[[gnu::nonnull]] static int parse_properties(
        const uint8_t **ptr, size_t *bytes_left,
        struct property (**store_props)[], unsigned *store_num_props,
        mqtt_control_packet_type cp_type)
{
    uint32_t properties_length;
    size_t rd = 0;
    uint8_t ident;
    struct property (*props)[] = NULL;
    struct property *prop;
    unsigned num_props = 0, skip;
    void *tmp;

    errno = 0;

    properties_length = read_var_byte(ptr, bytes_left);

    if (properties_length == 0 && errno)
        return -1;

    dbg_printf("parse_properties: properties_length=%u\n", properties_length);

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
        prop->identifier = ident;
        prop->type = mqtt_property_to_type[prop->identifier];

        /* TODO perform "is this valid for this control type?" */

        if (cp_type != -1U) /* for will_properties, there is cp_type */
            switch (prop->identifier)
            {
                default:
                    /* TODO MQTT requires skipping not failing */
                    warn("parse_properties: unsupported property identifier %u\n", prop->identifier);
            }

        skip = 0;

        switch (prop->type)
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

static int add_to_packet_ids(struct client_message_state *cs, struct message *msg)
{
    struct packet_id_to_state (*tmp)[];
    pthread_rwlock_wrlock(&cs->client->packet_ids_to_states_lock);
    unsigned tmp_size = cs->client->num_packet_id_to_state + 1;

    if ((tmp = realloc(cs->client->packet_ids_to_states,
                    sizeof(struct packet_id_to_state) * tmp_size)) == NULL) {
        pthread_rwlock_unlock(&cs->client->packet_ids_to_states_lock);
        goto fail;
    }

    (*tmp)[tmp_size - 1].packet_identifier = cs->packet_identifier;
    (*tmp)[tmp_size - 1].message = msg;
    (*tmp)[tmp_size - 1].state = cs;

    cs->client->packet_ids_to_states = tmp;
    cs->client->num_packet_id_to_state = tmp_size;
    pthread_rwlock_unlock(&cs->client->packet_ids_to_states_lock);

    return 0;

fail:
    if (tmp)
        free(tmp);

    return -1;
}

/*
 * signal handlers
 */

static void sh_sigint(int signum, siginfo_t * /*info*/, void * /*stuff*/)
{
    dbg_printf("sh_sigint: received signal %u\n", signum);
    if (running == false)
        exit(EXIT_FAILURE);
    running = false;
}

/*
 * atexit() functions
 */

static void close_all_sockets(void)
{
    dbg_printf("close_socket: closing mother_fd %u\n", mother_fd);
    if (mother_fd != -1)
        close_socket(&mother_fd);
}

static void free_all_messages(void)
{
    dbg_printf("free_messages\n");
    while (global_message_list)
        free_message(global_message_list, true);
}

static void free_all_clients(void)
{
    dbg_printf("free_clients\n");
    while (global_client_list)
        free_client(global_client_list, true);
}

static void free_all_packets(void)
{
    dbg_printf("free_packets\n");
    while (global_packet_list)
        free_packet(global_packet_list);
}

static void free_all_topics(void)
{
    dbg_printf("free_topics\n");
    while (global_topic_list)
        free_topic(global_topic_list);
}

/*
 * message distribution
 */

[[gnu::nonnull, gnu::warn_unused_result]] static struct topic *find_topic(const uint8_t *name)
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

[[gnu::nonnull, gnu::warn_unused_result]] static struct topic *register_topic(const uint8_t *name)
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

[[gnu::nonnull]] static int enqueue_message(struct topic *topic, struct message *msg)
{
    struct client_message_state (*tmp)[];
    unsigned num;

    errno = 0;

    pthread_rwlock_rdlock(&topic->subscribers_lock);
    num = topic->num_subscribers;
    if ((tmp = calloc(1, sizeof(struct client_message_state) * num)) == NULL) {
        pthread_rwlock_unlock(&topic->subscribers_lock);
        return -1;
    }

    for (unsigned idx = 0; idx < num; idx++)
    {
        (*tmp)[idx].client = (*topic->subscribers)[idx].client; /* refcnt TODO */
    }

    pthread_rwlock_unlock(&topic->subscribers_lock);

    msg->num_client_states = num;
    msg->client_states = tmp;
    msg->topic = topic;

    pthread_rwlock_wrlock(&topic->pending_queue_lock);
    msg->next_queue = topic->pending_queue;
    topic->pending_queue = msg;
    pthread_rwlock_unlock(&topic->pending_queue_lock);

    return 0;
}

[[gnu::nonnull]] static int dequeue_message(struct message *msg)
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

[[gnu::nonnull,gnu::access(read_only,4,3)]] static struct message *register_message(const uint8_t *topic_name,
        int format, uint16_t len, const void *payload, unsigned qos, struct client *sender)
{
    struct topic *topic;
    const uint8_t *tmp_name;

    tmp_name = NULL;
    topic = NULL;
    errno = 0;

    dbg_printf("register_message: topic=<%s> format=%u len=%u qos=%u payload=%p\n",
            topic_name, format, len, qos, payload);

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

[[gnu::nonnull]] static int add_subscription_to_topic(struct topic *topic,
        struct client *client, uint8_t option)
{
    struct subscription (*tmp_subs)[];

    errno = 0;

    size_t sub_size = sizeof(struct subscription) * (topic->num_subscribers + 1);
    if ((tmp_subs = (void *)realloc(topic->subscribers, sub_size)) == NULL)
        return -1;

    topic->subscribers = tmp_subs;

    (*topic->subscribers)[topic->num_subscribers].option = option;
    (*topic->subscribers)[topic->num_subscribers].client = client;
    (*topic->subscribers)[topic->num_subscribers].topic = topic;

    topic->num_subscribers++;
    return 0;
}

/* TODO locking */
[[gnu::nonnull]] static int unsubscribe_from_topic(struct client *client, struct topic *topic)
{
    struct subscription (*tmp_topic)[] = NULL;
    struct subscription (*tmp_client)[] = NULL;
    size_t topic_sub_size, topic_sub_cnt = 0, client_sub_size, client_sub_cnt = 0;

    errno = 0;

    /* remove the back references for this subscription */

    pthread_rwlock_wrlock(&topic->subscribers_lock);
    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if ( (*topic->subscribers)[idx].topic == topic &&
                (*topic->subscribers)[idx].client == client ) {
            memset(&(*topic->subscribers)[idx], 0, sizeof(struct subscription));
            break;
        }
    }
    pthread_rwlock_unlock(&topic->subscribers_lock);

    pthread_rwlock_wrlock(&client->subscriptions_lock);
    for (unsigned idx = 0; idx < client->num_subscriptions; idx++)
    {
        if ( (*client->subscriptions)[idx].topic == topic &&
                (*client->subscriptions)[idx].client == client ) {
            memset(&(*client->subscriptions)[idx], 0, sizeof(struct subscription));
            break;
        }
    }
    pthread_rwlock_unlock(&client->subscriptions_lock);

    /* compact the topic list of subscribers */

    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if ((*topic->subscribers)[idx].topic == NULL && (*topic->subscribers)[idx].client == NULL)
            continue;
        topic_sub_cnt++;
    }

    topic_sub_size = topic_sub_cnt * sizeof(struct subscription);

    if ((tmp_topic = malloc(topic_sub_size)) == NULL)
        goto fail;

    for (unsigned old_idx = 0, new_idx = 0; old_idx < topic->num_subscribers; old_idx++)
    {
        if ((*topic->subscribers)[old_idx].topic == NULL && (*topic->subscribers)[old_idx].client == NULL)
            continue;
        memcpy(&(*tmp_topic)[new_idx], &(*topic->subscribers)[old_idx], sizeof(struct subscription));
        new_idx++;
    }

    /* compact the client list of subscriptions */

    pthread_rwlock_wrlock(&client->subscriptions_lock);
    for (unsigned idx = 0; idx < client->num_subscriptions; idx++)
    {
        if ((*client->subscriptions)[idx].topic == NULL && (*client->subscriptions)[idx].client == NULL)
            continue;
        client_sub_cnt++;
    }

    client_sub_size = client_sub_cnt * sizeof(struct subscription);

    if ((tmp_client = malloc(client_sub_size)) == NULL) {
        pthread_rwlock_unlock(&client->subscriptions_lock);
        goto fail;
    }

    for (unsigned old_idx = 0, new_idx = 0; old_idx < client->num_subscriptions; old_idx++)
    {
        if ((*client->subscriptions)[old_idx].topic == NULL && (*client->subscriptions)[old_idx].client == NULL)
            continue;
        memcpy(&(*tmp_client)[new_idx], &(*client->subscriptions)[old_idx], sizeof(struct subscription));
        new_idx++;
    }

    /* free the old ones and replace */

    free(client->subscriptions);
    free(topic->subscribers);

    topic->subscribers = tmp_topic;
    client->subscriptions = tmp_client;

    topic->num_subscribers = topic_sub_cnt;
    client->num_subscriptions = client_sub_cnt;

    pthread_rwlock_unlock(&client->subscriptions_lock);
    return 0;

fail:

    if (tmp_client)
        free(tmp_client);
    if (tmp_topic)
        free(tmp_topic);

    return -1;
}

[[gnu::nonnull]] static int subscribe_to_topics(struct client *client, struct topic_sub_request *request)
{
    struct subscription (*tmp_subs)[] = NULL;
    struct topic *tmp_topic = NULL;

    errno = 0;

    pthread_rwlock_wrlock(&client->subscriptions_lock);
    size_t sub_size = sizeof(struct subscription) * (client->num_subscriptions + request->num_topics);

    if ((tmp_subs = (void *)realloc(client->subscriptions, sub_size)) == NULL) {
        for (unsigned idx = 0; idx < request->num_topics; idx++)
            request->response_codes[idx] = MQTT_UNSPECIFIED_ERROR;

        pthread_rwlock_unlock(&client->subscriptions_lock);
        goto fail;
    }

    client->subscriptions = tmp_subs;

    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        if (request->response_codes[idx] != MQTT_SUCCESS)
            continue;

        dbg_printf("subscribe_to_topics: subscribing to <%s>\n", (char *)request->topics[idx]);

        memset(&(*client->subscriptions)[client->num_subscriptions + idx], 0, sizeof(struct subscription));

        if ((tmp_topic = find_topic(request->topics[idx])) == NULL) {
            /* TODO somehow ensure reply does a fail for this one? */
            dbg_printf("subscribe_to_topics: failed to find_topic(<%s>)\n", (char *)request->topics[idx]);
            request->response_codes[idx] = MQTT_NOT_AUTHORIZED; /* TODO what's the correct approach? */
            continue;
        }

        if (add_subscription_to_topic(tmp_topic, client, request->options[idx]) == -1) {
            warn("subscribe_to_topics: add_subscription_to_topic <%s>", tmp_topic->name);
            request->response_codes[idx] = MQTT_UNSPECIFIED_ERROR;
            continue;
        }

        /* TODO refactor to add_subscription_to_client() */
        (*client->subscriptions)[client->num_subscriptions + idx].client = client;
        (*client->subscriptions)[client->num_subscriptions + idx].topic = tmp_topic;
        (*client->subscriptions)[client->num_subscriptions + idx].option = request->options[idx];

        free((void *)request->topics[idx]);
        request->options[idx] = 0;
        request->topics[idx] = NULL;
    }

    client->num_subscriptions = client->num_subscriptions + request->num_topics;
    pthread_rwlock_unlock(&client->subscriptions_lock);
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

[[gnu::nonnull]] static int send_cp_publish(struct packet *pkt)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp, topic_len;
    const struct message *msg;

    assert(pkt->message != NULL);
    assert(pkt->message->topic != NULL);
    assert(pkt->owner != NULL);

    errno = 0;

    msg = pkt->message;

    length = sizeof(struct mqtt_fixed_header); /* [0] */
    length += 1; /* [1] Remaining Length TODO calc properly */

    length += 2; /* [2-3] UTF-8 length */
    length += (topic_len = strlen((char *)pkt->message->topic->name)); /* [4..] */

    if ((pkt->flags & MQTT_FLAG_PUBLISH_QOS_MASK))
        length += 2; /* packet identifier */

    length += 1; /* properties length */

    length += msg->payload_len;

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBLISH;
    ((struct mqtt_fixed_header *)ptr)->flags = pkt->flags;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1; /* Remaining Length */
    ptr++;

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

    /* properties length */
    ptr += 1;

    memcpy(ptr, msg->payload, msg->payload_len);

    if ((wr_len = write(pkt->owner->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    return 0;
}

[[gnu::nonnull]] static int send_cp_disconnect(struct client *client, mqtt_reason_codes reason_code)
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

    free(packet);
    client->state = CS_CLOSING;

    return 0;
}

[[gnu::nonnull]] static int send_cp_pingresp(struct client *client)
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

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    client->last_keep_alive = time(0);
    free(packet);

    return 0;
}

[[gnu::nonnull]] static int send_cp_connack(struct client *client, mqtt_reason_codes reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    errno = 0;

    length = sizeof(struct mqtt_fixed_header);
    length += 1; /* remaining length 1byte */

    length += 2; /* connack var header (1byte for flags, 1byte for code) */
    length += 1; /* properties length (0) */
    length += 0; /* properties TODO */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_CONNACK;
    ptr++;

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1; /* Remaining Length */
    ptr++;

    *ptr = 0;           /* Connect Ack Flags */
    ptr++;

    *ptr = reason_code; /* Connect Reason Code */
    ptr++;

    /* properties length is set to 0 in calloc() */

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    if (reason_code >= 0x80)
        client->state = CS_CLOSING;

    return 0;
}

static int send_cp_pubrec(struct client *client, uint16_t packet_id, mqtt_reason_codes reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    if (reason_code == MQTT_MALFORMED_PACKET || reason_code == MQTT_PROTOCOL_ERROR) {
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

static int send_cp_pubcomp(struct client *client, uint16_t packet_id, mqtt_reason_codes reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    length = sizeof(struct mqtt_fixed_header);
    length +=1; /* Remaining Length */

    length +=3;

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

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    return 0;
}

static int send_cp_puback(struct client *client, uint16_t packet_id, mqtt_reason_codes reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;

    errno = 0;

    if (reason_code == MQTT_MALFORMED_PACKET || reason_code == MQTT_PROTOCOL_ERROR) {
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

[[gnu::nonnull]] static int send_cp_suback(struct client *client,
        uint16_t packet_id, struct topic_sub_request *request)
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
        if (tmp->packet_identifier == packet_id && atomic_load_explicit(&tmp->refcnt, memory_order_relaxed) > 0) {
            atomic_fetch_sub_explicit(&tmp->refcnt, 1, memory_order_acq_rel);
            break;
        }

    }
    pthread_rwlock_unlock(&client->active_packets_lock);

    return 0;
}

/*
 * control packet processing functions
 */

[[gnu::nonnull]] static int handle_cp_pubrel(struct client *client,
        struct packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    mqtt_reason_codes reason_code = MQTT_UNSPECIFIED_ERROR;
    [[maybe_unused]] mqtt_reason_codes pubrel_reason_code = 0;
    uint16_t tmp;

    dbg_printf("handle_cp_pubrel: bytes_left=%lu\n", bytes_left);

    errno = 0;

    if (packet->flags != 0x2) {
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

    if (parse_properties(&ptr, &bytes_left, &packet->properties, &packet->property_count, MQTT_CP_PUBREL) == -1) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }
skip_props:

    return send_cp_pubcomp(client, packet->packet_identifier, MQTT_SUCCESS);

fail:
    if (reason_code == MQTT_MALFORMED_PACKET || reason_code == MQTT_PROTOCOL_ERROR) {
        errno = EINVAL;
        client->state = CS_CLOSING;
    }

    return -1;
}

[[gnu::nonnull]] static int handle_cp_pubrec(struct client *client,
        struct packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    mqtt_reason_codes reason_code = MQTT_UNSPECIFIED_ERROR;
    [[maybe_unused]] mqtt_reason_codes pubrec_reason_code = 0;
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
        goto skip_props;
    }

    pubrec_reason_code = *ptr;
    ptr++;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBREC) == -1) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }
skip_props:
    pubrec_reason_code = MQTT_SUCCESS;

    bool found = false;
    pthread_rwlock_wrlock(&client->packet_ids_to_states_lock);
    for (unsigned idx = 0; idx < client->num_packet_id_to_state; idx++)
    {
        if ((*client->packet_ids_to_states)[idx].packet_identifier == packet_identifier) {
            struct packet_id_to_state *ptos = &(*client->packet_ids_to_states)[idx];
            ptos->state->acknowledged_at = time(0);
            break;
        }
    }
    pthread_rwlock_unlock(&client->packet_ids_to_states_lock);

    if (found == false) {
        reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        warn("handle_cp_pubrec: cannot find packet_identifier %u for client %s\n",
                packet_identifier, (char *)client->client_id);
        goto fail;
    }

    return 0;

fail:
    if (reason_code == MQTT_MALFORMED_PACKET || reason_code == MQTT_PROTOCOL_ERROR) {
        errno = EINVAL;
        client->state = CS_CLOSING;
        client->disconnect_reason = reason_code;
    }

    return -1;
}

[[gnu::nonnull]] static int handle_cp_publish(struct client *client,
        struct packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    uint8_t *topic_name = NULL;
    uint16_t packet_identifier = 0;
    mqtt_reason_codes reason_code = MQTT_UNSPECIFIED_ERROR;
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

    dbg_printf("handle_cp_publish: topic=<%s> ", topic_name);

    qos = GET_QOS(packet->flags); // & (1<<1|1<<2)) >> 1;
    flag_retain = (packet->flags & MQTT_FLAG_PUBLISH_RETAIN) == 1;
    flag_dup = (packet->flags & MQTT_FLAG_PUBLISH_DUP) == MQTT_FLAG_PUBLISH_DUP;

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
    dbg_printf("property_count=%u ", packet->property_count);
    dbg_printf("payload_format=%u ", payload_format);
    dbg_printf("payload_length=%lu ", bytes_left);

    packet->payload_len = bytes_left;
    if ((packet->payload = malloc(bytes_left)) == NULL)
        goto fail;
    memcpy(packet->payload, ptr, bytes_left);

    dbg_printf("\n");

    struct message *msg;
    if ((msg = register_message(topic_name, payload_format, packet->payload_len,
                packet->payload, qos, client)) == NULL) {
        warn("handle_cp_publish: register_message");
        goto fail;
    }
    msg->sender_packet_identifier = packet_identifier;

    free(topic_name);

    packet->payload = NULL;
    packet->payload_len = 0;

    if (qos == 1) {
        send_cp_puback(client, packet_identifier, MQTT_SUCCESS);
    } else if (qos == 2) {
        send_cp_pubrec(client, packet_identifier, MQTT_SUCCESS);
    }

    return 0;

fail:
    dbg_printf("\n");

    if (topic_name) {
        free(topic_name);
    }

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

[[gnu::nonnull]] static int handle_cp_subscribe(struct client *client,
        struct packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    struct topic_sub_request *request = NULL;
    void *tmp;
    mqtt_reason_codes reason_code = MQTT_UNSPECIFIED_ERROR;

    errno = 0;

    if (packet->flags != 0x2) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if (bytes_left < 3) {
        reason_code = MQTT_MALFORMED_PACKET;
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
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    /* Check for 0 topic filters */
    if (bytes_left < 3) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    if ((request = calloc(1, sizeof(struct topic_sub_request))) == NULL)
        goto fail;

    while (bytes_left)
    {
        if (bytes_left < 3) {
            reason_code = MQTT_MALFORMED_PACKET;
            goto fail;
        }

        if ((tmp = realloc(request->topics,
                        sizeof(uint8_t *) * (request->num_topics + 1))) == NULL)
            goto fail;
        request->topics = tmp;

        const size_t u8_size = sizeof(uint8_t) * (request->num_topics + 1);

        if ((tmp = realloc(request->options, u8_size)) == NULL)
            goto fail;
        request->options = tmp;

        if ((tmp = realloc(request->response_codes, u8_size)) == NULL)
            goto fail;
        request->response_codes = tmp;

        if ((request->topics[request->num_topics] = read_utf8(&ptr,
                        &bytes_left)) == NULL) {
            reason_code = MQTT_MALFORMED_PACKET;
            goto fail;
        }

        if (bytes_left < 1) {
            reason_code = MQTT_MALFORMED_PACKET;
            goto fail;
        }

        if (is_valid_topic_filter(request->topics[request->num_topics]) == -1)
            request->response_codes[request->num_topics] = MQTT_TOPIC_FILTER_INVALID;
        else
            request->response_codes[request->num_topics] = MQTT_SUCCESS;

        /* Validate subscribe options byte */

        /* bits 7 & 6 are reserved */
        if ((*ptr & 0xc0)) {
            reason_code = MQTT_MALFORMED_PACKET;
            goto fail;
        }

        /* QoS can't be 3 */
        if ((*ptr & 0x3) == 0x3) {
            reason_code = MQTT_MALFORMED_PACKET;
            goto fail;
        }

        /* retain handling can't be 3 */
        if ((*ptr & 0x30) == 0x30) {
            reason_code = MQTT_MALFORMED_PACKET;
            goto fail;
        }

        if (!strncmp("$share/", (char *)request->topics[request->num_topics], 7)) {
            if ((*ptr & 0x2)) {
                reason_code = MQTT_MALFORMED_PACKET;
                goto fail;
            }
        }

        /* TODO do something with the RETAIN flag */

        request->options[request->num_topics] = *ptr++;
        bytes_left--;

        request->num_topics++;
    }

    atomic_fetch_add_explicit(&packet->refcnt, 1, memory_order_relaxed);

    if (subscribe_to_topics(client, request) == -1) {
        warn("handle_cp_subscribe: subscribe_to_topics");
        goto fail;
    }

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

[[gnu::nonnull]] static int handle_cp_disconnect(struct client *client,
        struct packet *packet, const void *remain)
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
        dbg_printf("handle_cp_disconnect: disconnect reason was %u\n", disconnect_reason);
    }

    if (bytes_left > 0) {
        if (parse_properties(&ptr, &bytes_left, &packet->properties,
                    &packet->property_count, MQTT_CP_DISCONNECT) == -1)
            goto fail;

    } else {
skip:
        dbg_printf("handle_cp_disconnect: no reason\n");
    }

    client->state = CS_CLOSING;
    return 0;

fail:
    warnx("handle_cp_disconnect: packet malformed");
    client->state = CS_CLOSING;
    return -1;
}

[[gnu::nonnull]] static int handle_cp_pingreq(struct client *client,
        struct packet *packet, const void * /*remain*/)
{
    if (packet->remaining_length > 0) {
        errno = EINVAL;
        send_cp_disconnect(client, MQTT_MALFORMED_PACKET);
        return -1;
    }

    return send_cp_pingresp(client);
}

[[gnu::nonnull]] static int handle_cp_connect(struct client *client,
        struct packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    mqtt_reason_codes response_code = MQTT_UNSPECIFIED_ERROR;
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
    dbg_printf("client_id=<%s>\n", (char *)client->client_id);

    if (connect_flags & MQTT_CONNECT_FLAG_CLEAN_START) {
        dbg_printf("clean_start ");
    }

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        dbg_printf("will_properties ");
        if (parse_properties(&ptr, &bytes_left, &will_props, &num_will_props, -1) == -1) {
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

        if ((will_payload = read_binary(&ptr, &bytes_left, &will_payload_len)) == NULL) {
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
        if ((client->password = read_binary(&ptr, &bytes_left, &client->password_len)) == NULL) {
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
                    will_payload, will_qos, client)) == NULL) {
            warn("handle_cp_connect: register_message");
            free(will_topic);
            will_topic = NULL;
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

    send_cp_connack(client, MQTT_SUCCESS);
    return 0;

fail:
    if (response_code == MQTT_MALFORMED_PACKET || response_code == MQTT_PROTOCOL_ERROR) {
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
    [MQTT_CP_CONNECT]    = handle_cp_connect,
    [MQTT_CP_DISCONNECT] = handle_cp_disconnect,
    [MQTT_CP_PINGREQ]    = handle_cp_pingreq,
    [MQTT_CP_PUBLISH]    = handle_cp_publish,
    [MQTT_CP_SUBSCRIBE]  = handle_cp_subscribe,
    [MQTT_CP_PUBREC]     = handle_cp_pubrec,
    [MQTT_CP_PUBREL]     = handle_cp_pubrel,
};

/*
 * other functions
 */


[[gnu::nonnull]] static int parse_incoming(struct client *client)
{
    ssize_t rd_len;

    switch (client->parse_state)
    {
        case READ_STATE_NEW:
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
            if (client->new_packet && client->new_packet->packet_identifier == 0)
                free_packet(client->new_packet);
            client->new_packet = NULL;
            /* fall through */
        case READ_STATE_HEADER:
        case READ_STATE_MORE_HEADER:
            rd_len = read(client->fd, &client->header_buffer[client->read_offset], client->read_need);
            if (rd_len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) ) {
                return 0;
            } else if (rd_len == -1) {
                log_io_error(NULL, rd_len, client->read_need, false);
                goto fail;
            } else if (rd_len == 0) {
eof:
                /* EOF */
                client->state = CS_DISCONNECTED;
                close_socket(&client->fd);
                client->last_connected = time(0);
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
                struct mqtt_fixed_header *hdr = (void *)client->header_buffer;
                
                if (hdr->type >= MQTT_CP_MAX || hdr->type == 0)
                    goto fail;
                
                if (control_functions[hdr->type] == NULL)
                    goto fail;

                if (hdr->flags & mqtt_packet_permitted_flags[hdr->type])
                    goto fail;

                client->parse_state = READ_STATE_MORE_HEADER;
            } 

            if (client->parse_state == READ_STATE_MORE_HEADER) {
                uint8_t tmp;
                struct mqtt_fixed_header *hdr = (void *)client->header_buffer;

                if (client->rl_multi > 128*128*128)
                    goto fail;

                tmp = client->header_buffer[1 + client->rl_offset];

                client->rl_value += (tmp & 127) * client->rl_multi;
                client->rl_multi *= 128;

                client->rl_offset++;
                client->read_need++;
                
                if ( (tmp & 128) != 0 )
                    return 0;

                if (client->rl_value > MAX_PACKET_LENGTH)
                    goto fail;

                dbg_printf("parse_incoming: type=%u flags=%u remaining_length=%u\n",
                        hdr->type, hdr->flags, client->rl_value);


                if ((client->new_packet = alloc_packet(client)) == NULL)
                    goto fail;

                client->new_packet->remaining_length = client->rl_value;
                client->new_packet->type = hdr->type;
                client->new_packet->flags = hdr->flags;


                if ((client->packet_buf = malloc(client->new_packet->remaining_length)) == NULL)
                    goto fail;
                
                client->parse_state = READ_STATE_BODY;
                client->packet_offset = 0;
                client->read_need = client->new_packet->remaining_length;
            }
            break;
        case READ_STATE_BODY:
            rd_len = read(client->fd, &client->packet_buf[client->packet_offset], client->read_need);
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
            client->parse_state = READ_STATE_NEW;
            
            if (control_functions[client->new_packet->type](client, client->new_packet, client->packet_buf) == -1)
                goto fail;

            break;
    }
    return 0;
fail:
    if (client->packet_buf) {
        free(client->packet_buf);
        client->packet_buf = NULL;
    }
    if (client->new_packet) {
        free_packet(client->new_packet);
        client->new_packet = NULL;
    }
    client->parse_state = READ_STATE_NEW;
    return -1;
}

#if 0
[[gnu::nonnull]] static int parse_incoming(struct client *client)
{
    ssize_t rd_len;
    struct mqtt_fixed_header hdr;
    struct mqtt_packet *new_packet;
    void *packet;
    uint8_t tmp = 0;
    uint32_t value = 0;
    uint32_t multi = 1;

    new_packet = NULL;
    packet = NULL;

    if ((rd_len = read(client->fd, &hdr, sizeof(hdr))) != sizeof(hdr)) {
        if (rd_len == 0) {
            /* Allow reconnection TODO */
            client->state = CS_DISCONNECTED;
            close_socket(&client->fd);
            client->last_connected = time(0);
            return 0;
        }
        log_io_error(NULL, rd_len, sizeof(hdr), false);
        goto fail;
    }

    dbg_printf("parse_incoming: type=%u flags=%u\n", hdr.type, hdr.flags);

    if (hdr.type >= MQTT_CP_MAX || hdr.type == 0) {
        warnx("parse_incoming: invalid header type");
        goto fail;
    }

    if (control_functions[hdr.type] == NULL) {
        warnx("parse_incoming: unsupported packet %d", hdr.type);
        goto fail;
    }

    if ((new_packet = alloc_packet(client)) == NULL) {
        warn("parse_incoming: alloc_packet");
        goto fail;
    }

    new_packet->type = hdr.type;
    new_packet->flags = hdr.flags;

    do {
        if ((rd_len = read(client->fd, &tmp, 1)) != 1) {
            log_io_error(NULL, rd_len, 1, false);
            goto fail;
        }
        if (multi > 128*128*128) {
            warnx("parse_incoming: invalid variable byte int");
            goto fail;
        }
        value += (tmp & 127) * multi;
        multi *= 128;
    } while ((tmp & 128) != 0);
    new_packet->remaining_length = value;

    if (value > MAX_PACKET_LENGTH) {
        warn("parse_incoming: packet too big");
        goto fail;
    }

    if ((packet = malloc(new_packet->remaining_length)) == NULL) {
        warn("malloc(packet_len)");
        goto fail;
    }

    if ((rd_len = read(client->fd, packet, new_packet->remaining_length)) !=
            new_packet->remaining_length) {
        log_io_error(NULL, rd_len, new_packet->remaining_length, false);
        goto fail;
    }

    /* per table 2-2 */
    if (new_packet->flags & mqtt_packet_permitted_flags[new_packet->type]) {
        goto fail;
    }

    if (control_functions[hdr.type](client, new_packet, packet) == -1) {
        warn("parse_incoming: control_function returned error");
        goto fail;
    }

    free(packet);

    if (new_packet->packet_identifier == 0)
        free_packet(new_packet);

    return 0;

fail:
    if (packet)
        free(packet);
    if (new_packet)
        free_packet(new_packet);
    return -1;
}
#endif

#define MAX_MESSAGES_PER_TICK 100

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
    struct client_message_state *client_state;
    unsigned num_sent = 0;
    struct packet *packet;

    if (msg->client_states == NULL)
        return;

    pthread_rwlock_wrlock(&msg->client_states_lock);
    for (unsigned idx = 0; idx < msg->num_client_states; idx++) {
        client_state = &(*msg->client_states)[idx];

        if (client_state->acknowledged_at) {
            num_sent++;
            continue;
        }

        if (client_state->client == NULL) {
            warnx("tick_msg: client_state->client is NULL");
            continue;
        }

        dbg_printf("tick: sending message\n");

        if ((packet = alloc_packet(client_state->client)) == NULL) {
            warn("tick: unable to alloc_packet for msg on topic <%s>",
                    msg->topic->name);
            continue; /* FIXME */
        }

        packet->message = msg;
        packet->type = MQTT_CP_PUBLISH;
        packet->flags |= MQTT_FLAG_PUBLISH_QOS(msg->qos);

        if (msg->qos) {
            if (client_state->packet_identifier == 0) {
                client_state->packet_identifier = ++packet->owner->last_packet_id;
            }
            packet->packet_identifier = client_state->packet_identifier;

            /* acknowledged_at is 0, so this must be a retry? */
            if (client_state->last_sent) {
                packet->flags |= MQTT_FLAG_PUBLISH_DUP;
            }
            if (add_to_packet_ids(client_state, msg) == -1) {
                warn("tick: unable to add_to_packet_ids");
                /* FIXME alloc_packet leak */
            }
        }

        packet->reason_code = MQTT_SUCCESS;

        /* TODO make this async START ?? */
        client_state->last_sent = time(0);
        if (send_cp_publish(packet) == -1) {
            client_state->last_sent = 0;
            warn("tick: unable to send_cp_publish");
            free_packet(packet); /* Anything else? */
            continue;
        }

        if (msg->qos == 0) /* Fake? */
            client_state->acknowledged_at = time(0);

        free_packet(packet);
        /* TODO async END */
    }

    /* We have now sent everything */
    if (num_sent == msg->num_client_states) {
        /* TODO replace with list of subscribers to message, removal thereof,
         * then dequeue when none left */
        if (dequeue_message(msg) == -1) {
            warn("tick: dequeue_message failed");
        } else {
            msg->state = MSG_DEAD;
        }

        msg->num_client_states = 0;
        free(msg->client_states);
        msg->client_states = NULL;
    }
    pthread_rwlock_unlock(&msg->client_states_lock);
}

/* Topics */

static void topic_tick(void)
{
    unsigned max_messages = MAX_MESSAGES_PER_TICK;

    pthread_rwlock_wrlock(&global_topics_lock);
    for (struct topic *topic = global_topic_list; topic; topic = topic->next)
    {
        if (max_messages == 0 || topic->pending_queue == NULL || topic->num_subscribers == 0)
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

static void tick(void)
{
    client_tick();
    topic_tick();
    message_tick();
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
            dbg_printf("main_loop: no connections, going to sleep\n");
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

            dbg_printf("main_loop: new connection\n");
            if ((child_fd = accept(mother_fd, (struct sockaddr *)&sin_client, &sin_client_len)) == -1) {
                warn("main_loop: accept failed");
                continue;
            }

            if ((new_client = alloc_client()) == NULL) {
                close_socket(&child_fd);
                warn("main_loop: alloc_client");
                continue;
            }

            if (inet_ntop(AF_INET, &sin_client.sin_addr.s_addr, new_client->hostname, sin_client_len) == NULL)
                warn("inet_ntop");

            new_client->fd = child_fd;
            new_client->state = CS_ACTIVE;
            new_client->remote_port = ntohs(sin_client.sin_port);
            new_client->remote_addr = ntohl(sin_client.sin_addr.s_addr);

            dbg_printf("main_loop: new client from [%s:%u]\n", new_client->hostname, new_client->remote_port);
        }

        if (FD_ISSET(mother_fd, &fds_exc))
            warnx("main_loop: mother_fd is in fds_exc??");

        pthread_rwlock_rdlock(&global_clients_lock);
        for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
        {
            if (clnt->state != CS_ACTIVE)
                continue;

            if (FD_ISSET(clnt->fd, &fds_in)) {
                //dbg_printf("main_loop: read event on %p[%d]\n", (void *)clnt, clnt->fd);
                if (parse_incoming(clnt) == -1) {
                    /* TODO do something? */ ;
                }
                dbg_printf("\n");
            }

            if (FD_ISSET(clnt->fd, &fds_out)) {
                /* socket is writable without blocking [ish] */
            }

            if (FD_ISSET(clnt->fd, &fds_exc)) {
                dbg_printf("main_loop exception event on %p[%d]\n", (void *)clnt, clnt->fd);
                /* TODO close? */
            }
        }
        pthread_rwlock_unlock(&global_clients_lock);

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
        while ((opt = getopt(argc, argv, "hV")) != -1)
        {
            switch (opt)
            {
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
        err(EXIT_FAILURE, "sigaction(SIGINT)");

    atexit(close_all_sockets);
    atexit(free_all_messages);
    atexit(free_all_packets);
    atexit(free_all_clients);
    atexit(free_all_topics);

    const char *topic_name;
    while (optind < argc)
    {
        if ((topic_name = strdup(argv[optind])) == NULL)
            err(EXIT_FAILURE, "main: strdup(argv[])");
        if (is_valid_topic_filter((const uint8_t *)topic_name) == -1) {
            free((void *)topic_name);
            warn("main: <%s> is not a valid topic filter, skipping", (char *)argv[optind]);
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

    if (setsockopt(mother_fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger)) == -1)
        warn("setsockopt(SO_LINGER)");

    int reuse = 1;

    if (setsockopt(mother_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1)
        warn("setsockopt(SO_REUSEADDR)");

    struct sockaddr_in sin = {0};

    sin.sin_family = AF_INET;
    sin.sin_port = htons(1883);
    sin.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(mother_fd, (struct sockaddr *)&sin, sizeof(sin)) == -1)
        err(EXIT_FAILURE, "bind");

    if (listen(mother_fd, 5) == -1)
        err(EXIT_FAILURE, "listen");

    if (main_loop(mother_fd) == -1)
        return EXIT_FAILURE;

    return EXIT_SUCCESS;
}
