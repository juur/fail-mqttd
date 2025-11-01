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

#include "mqtt.h"
#include "config.h"

static int mother_fd = -1;
static struct mqtt_packet *global_packet_list = NULL;
static struct client *global_clients = NULL;

typedef int (*control_func_t)(struct client *, struct mqtt_packet *, const void *);

/*
 * command line stuff
 */

static void show_version(FILE *fp)
{
    fprintf(fp,
            "fail-mqttd " VERSION "\n"
            "\n"
            "Written by http://github.com/juur"
           );
}

static void show_usage(FILE *fp)
{
    fprintf(fp,
            "fail-mqttd -- a terrible implementation of MQTT\n"
            "\n"
            "Usage: fail-mqtt [hV]\n"
            "\n"
            "Options:\n"
            "  -h     show help\n"
            "  -V     show version\n"
            "\n"
           );
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

/*
 * packet parsing helpers
 */

[[gnu::nonnull]] static uint32_t read_var_byte(const uint8_t **const ptr, size_t *bytes_left)
{
    uint32_t value = 0;
    uint32_t multi = 1;
    uint8_t tmp;

    do {
        tmp = **ptr;
        *ptr = *ptr + 1;
        *bytes_left = *bytes_left - 1;
        value += (tmp & 127) * multi;
        if (multi > 128*128*128) {
            warn("invalid variable byte int");
            return -1;
        }
    } while((tmp & 128) != 0);
    
    return value;
}

[[gnu::nonnull]] static void *read_utf8(const uint8_t **const ptr, size_t *bytes_left)
{
    size_t str_len;
    void *string;
    
    str_len = htons(**((uint16_t **)ptr));
    *ptr = *ptr + 2;
    *bytes_left = *bytes_left - 2;

    if ((string = calloc(1, str_len + 1)) == NULL) {
        warn("calloc(str_len)");
        return NULL;
    }

    memcpy(string, *ptr, str_len);
    *ptr = *ptr + str_len;
    *bytes_left = *bytes_left - str_len;

    return string;
}

/*
 * allocators / deallocators
 */

[[gnu::nonnull]] static void free_packet(struct mqtt_packet *pck)
{
    struct mqtt_packet *tmp;

    if (pck->owner) {
        if (pck->owner->active_packets == pck) {
            pck->owner->active_packets = pck->next;
        } else for (tmp = pck->owner->active_packets; tmp; tmp = tmp->next_client)
        {
            if (tmp->next_client == pck) {
                tmp->next_client = pck->next_client;
                break;
            }
        }
        pck->next_client = NULL;
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

    if (pck->payload)
        free(pck->payload);

    if (pck->properties) {
        for (unsigned i = 0; i < pck->property_count; i++)
            /* TODO */ ;
        free(pck->properties);
    }

    free(pck);
}

[[gnu::nonnull]] static void free_client(struct client *client)
{
    struct client *tmp;

    if (global_clients == client) {
        global_clients = client->next;
    } else for (tmp = global_clients; tmp; tmp = tmp->next) {
        if (tmp->next == client) {
            tmp->next = client->next;
            break;
        }
    }
    client->next = NULL;

    if (client->client_id)
        free ((void *)client->client_id);

    if (client->fd != -1) {
        shutdown(client->fd, SHUT_RDWR);
        close(client->fd);
    }

    if (client->topics) {
        for (unsigned cnt = 0; cnt < client->num_topics; cnt--)
        {
            if (client->topics[cnt].topic)
                free(client->topics[cnt].topic);
        }
        free(client->topics);
    }

    free(client);
}

[[gnu::malloc]] static struct mqtt_packet *alloc_packet(struct client *owner)
{
    struct mqtt_packet *ret;

    if ((ret = calloc(1, sizeof(struct mqtt_packet))) == NULL)
        return NULL;

    if (owner) {
        ret->owner = owner;
        ret->next_client = owner->active_packets;
        owner->active_packets = ret->next_client;
    }
    
    ret->next = global_packet_list;
    global_packet_list = ret;

    return ret;
}

[[gnu::malloc]] static struct client *alloc_client(void)
{
    struct client *client;

    if ((client = calloc(1, sizeof(struct client))) == NULL)
        return NULL;

    client->fd = -1;

    client->next = global_clients;
    global_clients = client;
    
    return client;
}

/*
 * atexit() functions
 */

static void close_socket(void)
{
    if (mother_fd != -1) {
        shutdown(mother_fd, SHUT_RDWR);
        close(mother_fd);
    }
}

static void free_clients(void)
{
    while (global_clients)
        free_client(global_clients);
}

/*
 * control packet processing functions
 */

[[gnu::nonnull]] static int send_cp_connack(struct client *client)
{
    ssize_t length = 0;

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

[[gnu::nonnull]] static int send_cp_suback(struct client *client, uint16_t packet_id)
{
    ssize_t length = 0;
    length += sizeof(struct mqtt_fixed_header);
    length += 1; /* remaining length 1byte */

    length += 2; /* packet identifier */
    length += 1; /* properties length (0) */
    length += 0; /* properties TODO */
    length += 1 * client->num_topics;

    uint8_t *packet;

    if ((packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)packet)->type = MQTT_CP_SUBACK;
    packet[1] = length - sizeof(struct mqtt_fixed_header) - 1; /* Remaining Length */

    *(uint16_t *)&packet[2] = htons(packet_id);

    for (unsigned i = 0; i < client->num_topics; i++)
        packet[4 + i] = 0; /* TODO which QoS? */

    ssize_t wr_len;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return read_error(NULL, wr_len, length, false);
    }

    free(packet);

    for (struct mqtt_packet *tmp = client->active_packets; tmp; tmp = tmp->next)
    {
        if (tmp->packet_identifier == packet_id) {
            free_packet(tmp);
            break;
        }
    }

    return 0;
}

[[gnu::nonnull]] static int handle_cp_subscribe(struct client *client, struct mqtt_packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;

    packet->packet_identifier = ntohs(*(uint16_t *)ptr);
    ptr += 2;
    bytes_left -= 2;

    uint32_t properties_length = read_var_byte(&ptr, &bytes_left);

    if (properties_length) {
        /* TODO */
    }

    ptr += properties_length;
    bytes_left -= properties_length;

    struct topic *topics = NULL;
    uint8_t *topic = NULL;
    unsigned num_topics = 0;
    void *tmp;

    while (bytes_left)
    {
        if ((tmp = realloc(topics, sizeof(struct topic) * (num_topics + 2))) == NULL)
            goto fail;
        num_topics++;
        
        topics = tmp;
        topics[num_topics].topic = NULL;
        topics[num_topics - 1].topic = NULL;

        if ((topic = read_utf8(&ptr, &bytes_left)) == NULL)
            goto fail;

        topics[num_topics - 1].topic = topic;
        topics[num_topics - 1].options = *ptr++;
        bytes_left--;
        
        printf("handle_cp_subscribe: topic[%u] <%s> subscription_options=%u\n",
                num_topics - 1,
                topics[num_topics - 1].topic,
                topics[num_topics - 1].options
                );
    }

    client->topics = topics;
    client->num_topics = num_topics;

    return send_cp_suback(client, packet->packet_identifier);

fail:
    if (topics) {
        for (unsigned i = 0; i < num_topics; i++)
            if (topics[i].topic)
                free(topics[i].topic);
        free(topics);
    }
    return -1;
}

[[gnu::nonnull]] static int handle_cp_disconnect(struct client * /*client*/, struct mqtt_packet *packet, const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;

    uint8_t disconnect_reason = *ptr++;
    bytes_left--;

    if (bytes_left <= 1)
        goto done;

    uint32_t properties_length = read_var_byte(&ptr, &bytes_left);

    if (properties_length) {
        /* TODO */
    }
    
done:
    printf("handle_cp_disconnect: disconnect reason was %u\n", disconnect_reason);


    /* TODO client->xx state to close */
    return 0;
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

    if (packet->connect_hdr.version != 5) {
        warnx("unsupported version");
        return -1;
    }

    uint32_t properties_length = read_var_byte(&ptr, &bytes_left);

    if (properties_length) {
    }

    if (client->client_id != NULL) {
        errno = EEXIST;
        warnx("client_id already set");
        return -1;
    }

    if ((client->client_id = read_utf8(&ptr, &bytes_left)) == NULL) {
        warn("read_utf8");
        return -1;
    }

    printf("client_id=<%s>\n", client->client_id);

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_CLEAN_START) {
        printf("clean_start ");
    }

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        printf("will_flag ");
    }

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        printf("will_topic ");
    }

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_USERNAME) {
        printf("username ");
    }

    if (packet->connect_hdr.flags & MQTT_CONNECT_FLAG_PASSWORD) {
        printf("password ");
    }
    printf("\n");

    send_cp_connack(client);
    return 0;
}

static control_func_t control_functions[MQTT_CP_MAX] = {
    [MQTT_CP_CONNECT] = handle_cp_connect,
    [MQTT_CP_SUBSCRIBE] = handle_cp_subscribe,
    [MQTT_CP_DISCONNECT] = handle_cp_disconnect,
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
        if (rd_len == 0)
            return -1; /* TODO tag client->xxx for close */
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
    } while ((tmp & 128) != 0);
    new_packet->remaining_length = value;

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
        /* TODO force close? */
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

    atexit(free_clients);

    if ((mother_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
        err(EXIT_FAILURE, "socket");
    atexit(close_socket);

    struct linger linger = {
        .l_onoff = 0,
        .l_linger = 0,
    };

    if (setsockopt(mother_fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger)) == -1)
        warn("setsockopt(SO_LINGER");

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

    int child_fd;

    struct sockaddr_in sin_client;
    socklen_t sin_client_len = sizeof(sin_client_len);

    printf("main: listening\n");

    if ((child_fd = accept(mother_fd, (struct sockaddr *)&sin_client, &sin_client_len)) == -1)
        err(EXIT_FAILURE, "accept");
    
    printf("main: connected\n");

    struct client *client;
    if ((client = alloc_client()) == NULL)
        err(EXIT_FAILURE, "calloc(client)");
    client->fd = child_fd;

    fd_set readfds;

    while (true)
    {
        int rc;
        FD_ZERO(&readfds);
        FD_SET(child_fd, &readfds);
        printf("main: sleeping\n");
        if ((rc = select(child_fd + 1, &readfds, NULL, NULL, NULL)) == -1) {
            err(EXIT_FAILURE, "select");
        }
        if (parse_incoming(client) != 0)
            break;
    } 
    return EXIT_SUCCESS;
}


