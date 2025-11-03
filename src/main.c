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

#include "mqtt.h"
#include "config.h"

static int mother_fd = -1;
static struct mqtt_packet *global_packet_list = NULL;
static struct client *global_clients = NULL;
static struct topic *global_topics = NULL;
static struct message *global_messages = NULL;
static bool running = true;

typedef int (*control_func_t)(struct client *, struct mqtt_packet *, const void *);

/*
 * forward declarations
 */

static int unsubscribe_from_topic(struct subscription *sub);
static int dequeue_message(struct message *msg);

/*
 * command line stuff
 */

static void show_version(FILE *fp)
{
    fprintf(fp, "fail-mqttd " VERSION "\n" "\n" "Written by http://github.com/juur");
}

static void show_usage(FILE *fp)
{
    fprintf(fp, "fail-mqttd -- a terrible implementation of MQTT\n" "\n"
            "Usage: fail-mqtt [hV]\n" "\n"
            "Options:\n"
            "  -h     show help\n"
            "  -V     show version\n" "\n");
}

/*
 * debugging helpers
 */

#define read_error(m,r,e,d) _read_error(m,r,e,d,__FILE__,__func__,__LINE__);
static int _read_error(const char *msg, ssize_t rc, ssize_t expected, bool die, const char *file, const char *func, int line)
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
    printf("global_topics:\n");
    for (struct topic *tmp = global_topics; tmp; tmp = tmp->next)
    {
        printf("  topic: <%s>\n", (char *)tmp->name);
    }
}

/*
 * allocators / deallocators
 */

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
    free(request);
}

[[gnu::nonnull]] static void free_topic(struct topic *topic)
{
    printf("free_topic: <%s>\n",
            (topic->name == NULL) ? "" : (char *)topic->name
            );
    if (global_topics == topic) {
        global_topics = topic->next;
    } else for (struct topic *tmp = global_topics; tmp; tmp = tmp->next)
    {
        if (tmp->next == topic) {
            tmp->next = topic->next;
            break;
        }
    }
    topic->next = NULL;

    if (topic->name)
        free((void *)topic->name);

    if (topic->subscribers) {
        /* TODO ??? */
        free(topic->subscribers);
    }

    if (topic->pending_queue) {
        /* TODO persist */
        struct message *msg;
        while ((msg = topic->pending_queue)) {
            if (dequeue_message(msg) == -1) {
                err(EXIT_FAILURE, "free_topic: dequeue_message");
                topic->pending_queue = msg->next_queue;
            }
            msg->state = MSG_DEAD;
        }
    }

    free(topic);
}

[[gnu::nonnull]] static void free_properties(struct property (*props)[], unsigned count)
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


[[gnu::nonnull]] static void free_packet(struct mqtt_packet *pck)
{
    struct mqtt_packet *tmp;

    if (pck->lock) {
        warn("free_packet: attempt to free packet with lock");
        return;
    }

    if (pck->refcnt) {
        warn("free_packet: attempt to free packet with refcnt");
        return;
    }

    if (pck->owner) {
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
        pck->owner = NULL;
    }

    if (pck == global_packet_list) {
        global_packet_list = pck->next;
    } else for (tmp = global_packet_list; tmp; tmp = tmp->next)
    {
        if (tmp->next == pck) {
            tmp->next = pck->next;
            break;
        }
    }
    pck->next = NULL;

    if (pck->payload) {
        free(pck->payload);
        pck->payload = NULL;
    }

    if (pck->properties) {
        free_properties(pck->properties, pck->property_count);

        pck->property_count = 0;
        pck->properties = NULL;
    }

    free(pck);
}

[[gnu::nonnull]] static void free_message(struct message *msg)
{
    if (msg->topic) {
        warn("free_message: attempt to free with topic <%s> set", msg->topic->name);
        return;
    }

    if (global_messages == msg) {
        global_messages = msg->next;
    } else for (struct message *tmp = global_messages; tmp; tmp = tmp->next) {
        if (tmp->next == msg) {
            tmp->next = msg->next;
            break;
        }
    }
    msg->next = NULL;

    if (msg->payload) {
        free((void *)msg->payload);
        msg->payload = NULL;
    }

    free(msg);
}

[[gnu::nonnull]] static void free_client(struct client *client)
{
    struct client *tmp;

    if (client->state == CS_CLOSED)
        err(EXIT_FAILURE, "free_client: double free");

    client->state = CS_CLOSED;

    if (global_clients == client) {
        global_clients = client->next;
    } else for (tmp = global_clients; tmp; tmp = tmp->next) {
        if (tmp->next == client) {
            tmp->next = client->next;
            break;
        }
    }
    client->next = NULL;

    for (struct mqtt_packet *p = client->active_packets, *next; p; p = next)
    {
        next = p->next_client;
        free_packet(p);
    }

    client->active_packets = NULL;

    if (client->fd != -1) {
        shutdown(client->fd, SHUT_RDWR);
        close(client->fd);
        client->fd = -1;
    }

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

    if (client->subscriptions) {
        for (unsigned idx = 0; idx < client->num_subscriptions; idx++)
            unsubscribe_from_topic(&(*client->subscriptions)[idx]);
        free(client->subscriptions);
        client->subscriptions = NULL;
        client->num_subscriptions = 0;
    }

    free(client);
}

[[gnu::malloc]] static struct topic *alloc_topic(const uint8_t *name)
{
    struct topic *ret;

    errno = 0;

    if ((ret = calloc(1, sizeof(struct topic))) == NULL)
        return NULL;

    ret->name = name;

    return ret;
}

[[gnu::malloc]] static struct mqtt_packet *alloc_packet(struct client *owner)
{
    struct mqtt_packet *ret;

    errno = 0;

    if ((ret = calloc(1, sizeof(struct mqtt_packet))) == NULL)
        return NULL;

    if (owner) {
        ret->owner = owner;
        ret->next_client = owner->active_packets;
        owner->active_packets = ret;
    }

    ret->next = global_packet_list;
    global_packet_list = ret;

    return ret;
}

[[gnu::malloc]] static struct message *alloc_message(void)
{
    struct message *msg;

    errno = 0;

    if ((msg = calloc(1, sizeof(struct message))) == NULL)
        return NULL;

    msg->state = MSG_NEW;
    msg->next = global_messages;
    global_messages = msg;

    return msg;
}

[[gnu::malloc]] static struct client *alloc_client(void)
{
    struct client *client;

    errno = 0;

    if ((client = calloc(1, sizeof(struct client))) == NULL)
        return NULL;

    client->state = CS_NEW;
    client->fd = -1;

    client->next = global_clients;
    global_clients = client;

    return client;
}


/*
 * packet parsing helpers
 */

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
        value += ((tmp & 127) * multi);

        if (multi > 128*128*128) {
            warn("invalid variable byte int");
            errno = EINVAL;
            return 0;
        }

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

/* a type of -1U is used for situations where the properties are NOT the standard ones
 * in a packet, e.g. "will_properties" */
[[gnu::nonnull]] static int parse_properties(
        const uint8_t **ptr, size_t *bytes_left,
        struct property (**store_props)[], unsigned *store_num_props,
        mqtt_control_packet_type type)
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

    //printf("parse_properties: properties_length=%u\n", properties_length);

    if (properties_length == 0)
        return 0;

    if (*bytes_left < properties_length) {
        errno = ENOSPC;
        return -1;
    }

    rd = *bytes_left - properties_length;
    while (*bytes_left > rd)
    {
        if ((tmp = realloc(props, sizeof(struct property) * (num_props + 1))) == NULL)
            goto fail;
        props = tmp;

        memset(&(*props)[num_props], 0, sizeof(struct property));

        ident = **ptr;
        *ptr = *ptr + 1;
        *bytes_left = *bytes_left - 1;

        prop = &(*props)[num_props];

        prop->identifier = ident;
        prop->type = mqtt_property_to_type[prop->identifier];

        if (type != -1U)
        switch (prop->identifier)
        {
            default:
                warn("parse_properties: unsupported property identifier %u\n", prop->identifier);
                goto fail;
        }

        switch (prop->type)
        {
            case MQTT_TYPE_BYTE:
                prop->byte = **ptr;
                skip = 1;
                break;
            case MQTT_TYPE_2BYTE:
                memcpy(&prop->byte2, ptr, 2);
                prop->byte2 = ntohs(prop->byte2);
                skip = 2;
                break;
            case MQTT_TYPE_4BYTE:
                memcpy(&prop->byte4, ptr, 4);
                prop->byte4 = ntohl(prop->byte4);
                skip = 4;
                break;
            case MQTT_TYPE_VARBYTE:
                prop->varbyte = read_var_byte(ptr, bytes_left);
                if (prop->varbyte == 0 && errno)
                    goto fail;
                skip = 0;
                break;
            case MQTT_TYPE_UTF8_STRING:
                prop->utf8_string = read_utf8(ptr, bytes_left);
                if (prop->utf8_string == NULL)
                    goto fail;
                skip = 0;
                break;
            default:
                warn("parse_properties: unsupported type %u\n", mqtt_property_to_type[prop->type]);
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
    printf("sh_sigint: received signal %u\n", signum);
    if (running == false)
        exit(EXIT_FAILURE);
    running = false;
}

/*
 * atexit() functions
 */

static void close_socket(void)
{
    printf("close_socket: closing mother_fd %u\n", mother_fd);
    if (mother_fd != -1) {
        shutdown(mother_fd, SHUT_RDWR);
        close(mother_fd);
    }
}

static void free_messages(void)
{
    printf("free_messages\n");
    while (global_messages)
        free_message(global_messages);
}

static void free_clients(void)
{
    printf("free_clients\n");
    while (global_clients)
        free_client(global_clients);
}

static void free_packets(void)
{
    printf("free_packets\n");
    while (global_packet_list)
        free_packet(global_packet_list);
}

static void free_topics(void)
{
    printf("free_topics\n");
    while (global_topics)
        free_topic(global_topics);
}

/*
 * message distribution
 */

static struct topic *find_topic(const uint8_t *name)
{
    errno = 0;

    for (struct topic *tmp = global_topics; tmp; tmp = tmp->next)
    {
        if (!strcmp((const void *)name, (const void *)tmp->name))
            return tmp;
    }

    return NULL;
}

static struct topic *register_topic(const uint8_t *name)
{
    struct topic *ret;

    errno = 0;

    if ((ret = alloc_topic(name)) == NULL)
        return NULL;

    ret->next = global_topics;
    global_topics = ret;

    return ret;
}

static int enqueue_message(struct topic *topic, struct message *msg)
{
    errno = 0;

    msg->topic = topic;
    msg->next_queue = topic->pending_queue;
    topic->pending_queue = msg;

    return 0;
}

static int dequeue_message(struct message *msg)
{
    errno = 0;

    if (msg->topic == NULL) {
        warnx("dequeue_message: attempt to dequeue_message with topic <%s> set\n", msg->topic->name);
        errno = EINVAL;
        return -1; /* or 0? TODO */
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

static int register_message(const uint8_t *topic_name, int format, uint16_t len, const void *payload, unsigned qos)
{
    struct topic *topic;
    errno = 0;

    printf("register_message: topic=<%s> format=%u len=%u qos=%u payload=%p\n",
            topic_name, format, len, qos, payload);

    if ((topic = find_topic(topic_name)) == NULL) {
        const uint8_t *tmp_name;

        if ((tmp_name = (void *)strdup((const char *)topic_name)) == NULL)
            goto fail;

        if ((topic = register_topic(tmp_name)) == NULL) {
            warn("register_message: register_topic <%s>", tmp_name);
            goto fail;
        }
    }

    struct message *msg;

    if ((msg = alloc_message()) == NULL)
        goto fail;

    msg->format = format;
    msg->payload = payload;

    if (enqueue_message(topic, msg) == -1) {
        warn("register_message: enqueue_message");
        free_message(msg);
        goto fail;
    }

    msg->state = MSG_ACTIVE;

    /* TODO register the message for delivery and commit */

    return 0;

fail:
    return -1;
}

static int add_subscription_to_topic(struct topic *topic, struct client *client, uint8_t option)
{
    struct subscription (*tmp_subs)[];

    errno = 0;

    if ((tmp_subs = (void *)realloc(topic->subscribers,
                    sizeof(struct subscription) * (topic->num_subscribers + 1))) == NULL)
        return -1;

    topic->subscribers = tmp_subs;

    (*topic->subscribers)[topic->num_subscribers].option = option;
    (*topic->subscribers)[topic->num_subscribers].client = client;
    (*topic->subscribers)[topic->num_subscribers].topic = topic;

    topic->num_subscribers++;
    return 0;
}

static int unsubscribe_from_topic(struct subscription *sub)
{
    errno = 0;

    /* remove from topic TODO */
    /* remove from client TODO */

    sub->topic = NULL;
    sub->client = NULL;
    sub->option = 0;

    /* squash topic sub list? */
    /* squash client sub list? */

    return 0;
}

static int subscribe_to_topics(struct client *client, struct topic_sub_request *request)
{
    struct subscription (*tmp_subs)[] = NULL;
    struct topic *tmp_topic = NULL;

    errno = 0;

    if ((tmp_subs = (void *)realloc(client->subscriptions,
                    sizeof(struct subscription) * (client->num_subscriptions + request->num_topics))) == NULL)
        goto fail;

    client->subscriptions = tmp_subs;

    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        memset(&(*client->subscriptions)[client->num_subscriptions + idx], 0, sizeof(struct subscription));

        if ((tmp_topic = find_topic(request->topics[idx])) == NULL) {
            /* TODO somehow ensure reply does a fail for this one? */
            continue;
        }

        if (add_subscription_to_topic(tmp_topic, client, request->options[idx]) == -1) {
            warn("subscribe_to_topics: add_subscription_to_topic <%s>", tmp_topic->name);
            continue;
        }

        (*client->subscriptions)[client->num_subscriptions + idx].client = client;
        (*client->subscriptions)[client->num_subscriptions + idx].topic = tmp_topic;
        (*client->subscriptions)[client->num_subscriptions + idx].option = request->options[idx];

        free((void *)request->topics[idx]);
        request->options[idx] = 0;
        request->topics[idx] = NULL;
    }

    client->num_subscriptions = client->num_subscriptions + request->num_topics;
    return 0;

fail:

    return -1;
}

/*
 * control packet response functions
 */

[[gnu::nonnull]] static int send_cp_pingresp(const struct client *client)
{
    ssize_t length = 0;

    errno = 0;

    length += sizeof(struct mqtt_fixed_header);
    length += 1; /* remaining length 1 byte */

    uint8_t *packet;

    if ((packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)packet)->type = MQTT_CP_PINGRESP;
    packet[1] = 0;

    ssize_t wr_len;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return read_error(NULL, wr_len, length, false);
    }

    free(packet);
    return 0;
}

[[gnu::nonnull]] static int send_cp_connack(struct client *client)
{
    ssize_t length = 0;

    errno = 0;

    length += sizeof(struct mqtt_fixed_header);
    length += 1; /* remaining length 1byte */

    length += 2; /* connack var header (1byte for flags, 1byte for code) */
    length += 1; /* properties length (0) */
    length += 0; /* properties TODO */

    uint8_t *packet;

    if ((packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)packet)->type = MQTT_CP_CONNACK;
    packet[1] = length - sizeof(struct mqtt_fixed_header) - 1; /* Remaining Length */

    packet[2] = 0;            /* Connect Ack Flags */
    packet[3] = MQTT_SUCCESS; /* Connect Reason Code */

    ssize_t wr_len;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return read_error(NULL, wr_len, length, false);
    }

    free(packet);
    return 0;
}

/* Fixed Header:
 *  MQTT Control Packet type [4:7]
 *  Remaining Length
 * Variable Header:
 *  Packet Identifier
 *  Properties[]
 * Payload:
 *  Reason Codes[]
 */

[[gnu::nonnull]] static int send_cp_suback(struct client *client, uint16_t packet_id, struct topic_sub_request *request)
{
    uint16_t tmp;
    ssize_t length = 0;

    errno = 0;

    length += sizeof(struct mqtt_fixed_header); /* [0] MQTT Control Packet type */
    length += 1; /* [1]   Remaining Length 1byte */

    length += sizeof(packet_id); /* [2-3] Packet Identifier */
    length += 1; /* [4]   properties length (0) */
    length += 0; /*       properties TODO */
    length += 1 * request->num_topics; /* [5+] */

    uint8_t *packet;

    if ((packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)packet)->type = MQTT_CP_SUBACK;
    packet[1] = length - sizeof(struct mqtt_fixed_header) - 1; /* Remaining Length */

    tmp = htons(packet_id);
    memcpy(&packet[2], &tmp, 2);

    packet[4] = 0; /* properties length */

    for (unsigned i = 0; i < request->num_topics; i++)
        packet[5 + i] = 0; /* TODO which QoS? what if sub failed? */

    ssize_t wr_len;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return read_error(NULL, wr_len, length, false);
    }

    free(packet);

    for (struct mqtt_packet *tmp = client->active_packets; tmp; tmp = tmp->next_client)
    {
        if (tmp->packet_identifier == packet_id && tmp->refcnt) {
            atomic_fetch_sub_explicit(&tmp->refcnt, 1, memory_order_acq_rel);
            break;
        }

    }

    return 0;
}

/*
 * control packet processing functions
 */

[[gnu::nonnull]] static int handle_cp_publish(struct client * /*client*/, struct mqtt_packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    uint8_t *topic_name;
    uint16_t packet_identifier;

    errno = 0;

    if ((topic_name = read_utf8(&ptr, &bytes_left)) == NULL)
        goto fail;

    printf("handle_cp_publish: topic=<%s> ", topic_name);

    unsigned qos;
    [[maybe_unused]] bool flag_retain;
    [[maybe_unused]] bool flag_dup;

    qos = (packet->flags & (1<<1|1<<2)) >> 1;
    flag_retain = (packet->flags & 1) == 1;
    flag_dup = (packet->flags & (1<<3)) == (1<<3);
    printf("qos=%u ", qos);

    if (qos) {
        memcpy(&packet_identifier, ptr, 2);
        packet_identifier = ntohs(packet_identifier);
        ptr += 2;
        bytes_left -= 2;
        printf("packet_ident=%u ", packet_identifier);
    }

    uint8_t payload_format = 0; /* TODO extract from properties */

    if (parse_properties(&ptr, &bytes_left, &packet->properties, &packet->property_count, MQTT_CP_PUBLISH) == -1)
        goto fail;
    printf("property_count=%u ", packet->property_count);
    printf("payload_format=%u ", payload_format);
    printf("payload_length=%lu ", bytes_left);

    packet->payload_len = bytes_left;
    if ((packet->payload = malloc(bytes_left)) == NULL)
        goto fail;
    memcpy(packet->payload, ptr, bytes_left);

    printf("\n");
    
    if (register_message(topic_name, payload_format, packet->payload_len, packet->payload, qos) == -1) {
        warn("handle_cp_publish: register_message");
        free(topic_name);
        return -1;
    }

    free(topic_name);

    packet->payload = NULL;
    packet->payload_len = 0;

    return 0;

fail:
    if (topic_name) {
        free(topic_name);
    }
    printf("\n");
    return -1;
}

[[gnu::nonnull]] static int handle_cp_subscribe(struct client *client, struct mqtt_packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    struct topic_sub_request *request = NULL;
    void *tmp;

    errno = 0;

    if (bytes_left < 3) {
        errno = ENOSPC;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, 2);
    packet->packet_identifier = ntohs(packet->packet_identifier);
    ptr += 2;
    bytes_left -= 2;

    if (parse_properties(&ptr, &bytes_left, &packet->properties, &packet->property_count, MQTT_CP_SUBSCRIBE) == -1)
        goto fail;

    /* check if bytes_left == 0 means malformed i.e. >= 1 topic required TODO */

    if ((request = malloc(sizeof(struct topic_sub_request))) == NULL)
        goto fail;

    request->topics = NULL;
    request->num_topics = 0;
    request->options = 0;

    while (bytes_left)
    {
        if (bytes_left < 3)
            goto fail;

        if ((tmp = realloc(request->topics, sizeof(uint8_t *) * (request->num_topics + 1))) == NULL)
            goto fail;
        request->topics = tmp;

        if ((tmp = realloc(request->options, sizeof(uint8_t) * (request->num_topics + 1))) == NULL)
            goto fail;
        request->options = tmp;

        if ((request->topics[request->num_topics] = read_utf8(&ptr, &bytes_left)) == NULL)
            goto fail;

        if (bytes_left < 1)
            goto fail;

        request->options[request->num_topics] = *ptr++;
        bytes_left--;

        //printf("handle_cp_subscribe: topic[%u] <%s> subscription_options=%u\n", num_topics - 1, topics[num_topics - 1].topic, topics[num_topics - 1].options);
        request->num_topics++;
    }

    atomic_fetch_add_explicit(&packet->refcnt, 1, memory_order_relaxed);

    if (subscribe_to_topics(client, request) == -1) {
        warn("handle_cp_subscribe: subscribe_to_topics");
        goto fail;
    }

    int rc = send_cp_suback(client, packet->packet_identifier, request);
    free_topic_subs(request);
    return rc;

fail:
    if (request)
        free_topic_subs(request);
    return -1;
}

[[gnu::nonnull]] static int handle_cp_disconnect(struct client *client, struct mqtt_packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;

    errno = 0;

    if (bytes_left < 2) {
        errno = ENOSPC;
        return -1;
    }

    uint8_t disconnect_reason = *ptr++;
    bytes_left--;

    if (parse_properties(&ptr, &bytes_left, &packet->properties, &packet->property_count, MQTT_CP_DISCONNECT) == -1)
        goto fail;

    printf("handle_cp_disconnect: disconnect reason was %u\n", disconnect_reason);
    client->state = CS_CLOSING;
    return 0;

fail:
    warnx("handle_cp_disconnect: packet malformed");
    client->state = CS_CLOSING;
    return -1;
}

[[gnu::nonnull]] static int handle_cp_pingreq(struct client *client, struct mqtt_packet *packet, const void * /*remain*/)
{
    if (packet->remaining_length > 0) {
        errno = EINVAL;
        return -1;
    }

    return send_cp_pingresp(client);
}

[[gnu::nonnull]] static int handle_cp_connect(struct client *client, struct mqtt_packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    struct mqtt_connect_header connect_header;

    memcpy(&connect_header, ptr, sizeof(connect_header));
    ptr += sizeof(connect_header);
    bytes_left -= sizeof(connect_header);

    packet->connect_hdr.length = ntohs(connect_header.length);
    packet->connect_hdr.flags = connect_header.flags;
    packet->connect_hdr.version = connect_header.version;
    packet->connect_hdr.keep_alive = ntohs(connect_header.keep_alive);

    printf("connect_header: length=%u ver=%u keep_alive=%u flags=%u name=%c%c%c%c\n",
            packet->connect_hdr.length,
            packet->connect_hdr.version,
            packet->connect_hdr.keep_alive,
            packet->connect_hdr.flags,
            connect_header.name[0],
            connect_header.name[1],
            connect_header.name[2],
            connect_header.name[3]
          );

    if (memcmp(&connect_header.name, "MQTT", 4)) {
        warnx("protocol name incorrect");
        return -1;
    }

    if (packet->connect_hdr.version != 5) {
        warnx("unsupported version");
        return -1;
    }

    if (parse_properties(&ptr, &bytes_left, &packet->properties, &packet->property_count, MQTT_CP_CONNECT) == -1)
        return -1;

    if (client->client_id != NULL) {
        errno = EEXIST;
        warnx("client_id already set");
        return -1;
    }

    if ((client->client_id = read_utf8(&ptr, &bytes_left)) == NULL) {
        warn("read_utf8");
        return -1;
    }
    printf("client_id=<%s>\n", (char *)client->client_id);

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_CLEAN_START) {
        printf("clean_start ");
    }

    uint8_t *will_topic = NULL;
    void *will_payload = NULL;
    uint16_t will_payload_len;
    uint8_t payload_format = 0;

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        printf("will_properties ");
        struct property (*will_props)[] = NULL;
        unsigned num_will_props = 0;

        if (parse_properties(&ptr, &bytes_left, &will_props, &num_will_props, -1) == -1) {
            warn("handle_cp_connect: parse_properties(will_props)");
            return -1;
        }
        printf("[%d props] ", num_will_props);

        printf("will_topic ");

        will_topic = read_utf8(&ptr, &bytes_left);
        if (will_topic == NULL) {
            warn("handle_cp_connect: will_topic");
            return -1;
        }
        printf("<%s> ", (char *)will_topic);

        printf("will_payload ");

        if ((will_payload = read_binary(&ptr, &bytes_left, &will_payload_len)) == NULL) {
            warn("handle_cp_connect: read_binary(will_payload)");
            return -1;
        }
        printf("[%ub] ", will_payload_len);
        packet->payload = will_payload;
        packet->payload_len = will_payload_len;
    }

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_WILL_RETAIN) {
        printf("will_retain ");
        if ((packet->connect_hdr.flags & MQTT_CONNECT_FLAG_WILL_FLAG) == 0) {
            warn("handle_cp_connect: Will Retain set without Will Flag");
            return -1;
        }
    }

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_USERNAME) {
        printf("username ");
        if ((client->username = read_utf8(&ptr, &bytes_left)) == NULL) {
            warn("read_utf8(username)");
            return -1;
        }
        printf("<%s> ", (char *)client->username);
    }

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_PASSWORD) {
        printf("password ");
        if ((client->password = read_binary(&ptr, &bytes_left, &client->password_len)) == NULL) {
            warn("read_utf8(password)");
            return -1;
        }
        printf("[%ub] ", client->password_len);
    }

    client->qos = GET_QOS(packet->connect_hdr.flags);

    printf("QoS [%u]\n", client->qos);
    
    send_cp_connack(client);

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        //printf("handle_cp_connect: creating a message\n");
        if (register_message(will_topic, payload_format, will_payload_len, will_payload, client->qos) == -1) {
            warn("handle_cp_connect: register_message");
            free(will_topic);
            return -1;
        }
        free(will_topic);
        packet->payload = NULL;
        packet->payload_len = 0;
    }

    return 0;
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
};

/*
 * other functions
 */

[[gnu::nonnull]] static int parse_incoming(struct client *client)
{
    ssize_t rd_len;
    struct mqtt_fixed_header hdr;
    struct mqtt_packet *new_packet;
    void *packet;

    new_packet = NULL;
    packet = NULL;

    if ((rd_len = read(client->fd, &hdr, sizeof(hdr))) != sizeof(hdr)) {
        if (rd_len == 0) {
            client->state = CS_CLOSING;
            return 0;
        }
        read_error(NULL, rd_len, sizeof(hdr), false);
        goto fail;
    }

    printf("parse_incoming: type=%u flags=%u\n", hdr.type, hdr.flags);

    if (hdr.type >= MQTT_CP_MAX) {
        warnx("invalid hdr.type");
        goto fail;
    }

    if (control_functions[hdr.type] == NULL) {
        warnx("unsupported packet %d", hdr.type);
        goto fail;
    }

    if ((new_packet = alloc_packet(client)) == NULL) {
        warn("alloc_packet");
        goto fail;
    }

    new_packet->type = hdr.type;
    new_packet->flags = hdr.flags;

    uint8_t tmp = 0;
    uint32_t value = 0;
    uint32_t multi = 1;

    do {
        if ((rd_len = read(client->fd, &tmp, 1)) != 1) {
            read_error(NULL, rd_len, 1, false);
            goto fail;
        }
        value += (tmp & 127) * multi;
        if (multi > 128*128*128) {
            warn("invalid variable byte int");
            goto fail;
        }
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

    if ((rd_len = read(client->fd, packet, new_packet->remaining_length)) != new_packet->remaining_length) {
        read_error(NULL, rd_len, new_packet->remaining_length, false);
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

static void tick(void)
{
    for (struct client *clnt = global_clients, *next; clnt; clnt = next)
    {
        next = clnt->next;

        if (clnt->state == CS_CLOSED || clnt->state == CS_NEW)
            continue;

        if (clnt->state == CS_CLOSING)
            free_client(clnt);
    }

    for (struct topic *topic = global_topics; topic; topic = topic->next)
    {
        if (topic->pending_queue == NULL)
            continue;

        if (topic->num_subscribers == 0)
            continue;

        struct message *msg;

        msg = topic->pending_queue;
        if (dequeue_message(msg) == -1) {
            warn("tick: dequeue_message failed");
        } else
            msg->state = MSG_DEAD;
    }

    for (struct message *msg = global_messages, *next; msg; msg = next)
    {
        next = msg->next;

        if (msg->state != MSG_DEAD)
            continue;

        free_message(msg);
    }
}

static int main_loop(int mother_fd)
{
    bool has_clients;

    fd_set fds_in, fds_out, fds_exc;

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

        for (struct client *clnt = global_clients; clnt; clnt = clnt->next)
        {
            if (clnt->fd > max_fd)
                max_fd = clnt->fd;

            FD_SET(clnt->fd, &fds_in);
            //FD_SET(clnt->fd, &fds_out);
            FD_SET(clnt->fd, &fds_exc);

            has_clients = true;
        }

        tv.tv_sec = 0;
        tv.tv_usec = 10000;

        if (has_clients == false) {
            printf("main_loop: no connections, going to sleep\n");
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

            printf("main_loop: new connection\n");
            if ((child_fd = accept(mother_fd, (struct sockaddr *)&sin_client, &sin_client_len)) == -1) {
                warn("main_loop: accept failed");
                continue;
            }

            if ((new_client = alloc_client()) == NULL) {
                warn("main_loop: alloc_client");
                continue;
            }

            new_client->fd = child_fd;
            new_client->state = CS_ACTIVE;
        }

        if (FD_ISSET(mother_fd, &fds_exc))
            warnx("main_loop: mother_fd is in fds_exc??");

        for (struct client *clnt = global_clients; clnt; clnt = clnt->next)
        {
            if (clnt->state != CS_ACTIVE)
                continue;

            if (FD_ISSET(clnt->fd, &fds_in)) {
                //printf("main_loop: read event on %p[%d]\n", (void *)clnt, clnt->fd);
                if (parse_incoming(clnt) == -1) {
                    /* TODO do something? */ ;
                }
                printf("\n");
            }

            if (FD_ISSET(clnt->fd, &fds_out)) {
                /* socket is writable without blocking [ish] */
            }

            if (FD_ISSET(clnt->fd, &fds_exc)) {
                printf("main_loop exception event on %p[%d]\n", (void *)clnt, clnt->fd);
                /* TODO close? */
            }
        }

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
                    show_usage(stdout);
                    exit(EXIT_SUCCESS);
                case 'V':
                    show_version(stdout);
                    exit(EXIT_SUCCESS);
                default:
                    show_usage(stderr);
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

    if ((mother_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
        err(EXIT_FAILURE, "socket");

    atexit(close_socket);
    atexit(free_messages);
    atexit(free_packets);
    atexit(free_clients);
    atexit(free_topics);

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
