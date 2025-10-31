#include <stdlib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <err.h>
#include <stdio.h>
#include <string.h>

#include "mqtt.h"
#include "config.h"

static int mother_fd = -1;

static void close_socket(void)
{
    if (mother_fd != -1) {
        shutdown(mother_fd, SHUT_RDWR);
        close(mother_fd);
    }
}

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

[[gnu::nonnull]] static void *read_utf8(uint8_t **const ptr, size_t *bytes_left)
{
    size_t str_len;
    void *client_id;
    
    str_len = htons(**((uint16_t **)ptr));
    *ptr = *ptr + 2;
    *bytes_left = *bytes_left - 2;

    if ((client_id = calloc(1, str_len + 1)) == NULL) {
        warn("calloc(str_len)");
        return NULL;
    }

    memcpy(client_id, *ptr, str_len);
    *ptr = *ptr + str_len;
    *bytes_left = *bytes_left - str_len;

    return client_id;
}

[[gnu::nonnull]] static int handle_cp_connect(struct mqtt_packet *packet, const void *remain)
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

    uint32_t value = read_var_byte(&ptr, &bytes_left);
    printf("properties_length=%u\n", value);

    if (value) {
    }

    printf("payload_length=%lu\n", bytes_left);

    if ((packet->client_id = read_utf8(&ptr, &bytes_left)) == NULL) {
        warn("read_utf8");
        return -1;
    }

    printf("client_id=<%s>\n", packet->client_id);

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

    return 0;
}

int (*control_functions[MQTT_CP_MAX])(struct mqtt_packet *packet, void *remaining) = {
    [MQTT_CP_CONNECT] = handle_cp_connect,
};

static void parse_incoming(int fd)
{
    struct mqtt_fixed_header hdr;
    ssize_t rd_len;

    if ((rd_len = read(fd, &hdr, sizeof(hdr))) != sizeof(hdr)) {
        if (rd_len == -1) {
            warn("read");
            goto done;
        } else if (rd_len == 0) {
            warnx("no data left");
            goto done;
        } else {
            warnx("short read");
            goto done;
        }
    }

    printf("type=%u flags=%u\n", hdr.type, hdr.flags);

    if (hdr.type >= MQTT_CP_MAX) {
        warnx("invalid hdr.type");
        goto done;
    }

    if (control_functions[hdr.type] == NULL) {
        warnx("unsupported packet %d", hdr.type);
        goto done;
    }

    struct mqtt_packet *new_packet;

    if ((new_packet = calloc(1, sizeof(struct mqtt_packet))) == NULL) {
        warn("calloc(mqtt_packet)");
        goto done;
    }

    void *packet;

    new_packet->type = hdr.type;
    new_packet->flags = hdr.flags;

    uint8_t tmp = 0;
    uint32_t value = 0;
    uint32_t multi = 1;

    do {
        if (read(fd, &tmp, 1) != 1) {
            warn("read");
            goto done;
        }
        value += (tmp & 127) * multi;
        if (multi > 128*128*128) {
            warn("invalid variable byte int");
            goto done;
        }
    } while ((tmp & 128) != 0);
    new_packet->remaining_length = value;
    printf("length=%lu\n", new_packet->remaining_length);

    if ((packet = malloc(new_packet->remaining_length)) == NULL) {
        warn("malloc(packet_len)");
        goto done;
    }

    if ((rd_len = read(fd, packet, new_packet->remaining_length)) != new_packet->remaining_length) {
        warn("read");
        goto done;
    }

    control_functions[hdr.type](new_packet, packet);

done:
    shutdown(fd, SHUT_RDWR);
    close(fd);
}

int main(int /*argc*/, char ** /*argv[]*/)
{
    if ((mother_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
        err(EXIT_FAILURE, "socket");

    struct linger linger = {
        .l_onoff = 0,
        .l_linger = 0,
    };

    setsockopt(mother_fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));

    int reuse = 1;

    setsockopt(mother_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in sin;

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

    parse_incoming(child_fd);

    atexit(close_socket);

    return EXIT_SUCCESS;
}
