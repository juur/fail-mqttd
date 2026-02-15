#ifndef MQTT_TEST_SUPPORT_H
#define MQTT_TEST_SUPPORT_H

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "mqtt_test_api.h"
#include "test_io.h"

static inline void mqtt_test_init_client(struct client *client, int fd,
		struct session *session)
{
	memset(client, 0, sizeof(*client));
	client->fd = fd;
	client->session = session;
	client->parse_state = READ_STATE_NEW;
	client->state = CS_ACTIVE;
	pthread_rwlock_init(&client->active_packets_lock, NULL);
	pthread_rwlock_init(&client->po_lock, NULL);
}

static inline void mqtt_test_cleanup_client(struct client *client)
{
	if (client->packet_buf) {
		free(client->packet_buf);
		client->packet_buf = NULL;
	}
	if (client->client_id) {
		free((void *)client->client_id);
		client->client_id = NULL;
	}
	if (client->username) {
		free((void *)client->username);
		client->username = NULL;
	}
	if (client->password) {
		free((void *)client->password);
		client->password = NULL;
		client->password_len = 0;
	}

	pthread_rwlock_destroy(&client->active_packets_lock);
	pthread_rwlock_destroy(&client->po_lock);
}

static inline ssize_t mqtt_test_write_packet(int fd, control_packet_t type,
		uint8_t flags, const uint8_t *payload, size_t payload_len)
{
	struct mqtt_fixed_header hdr = {0};
	uint8_t len_buf[4];
	int len_len;
	uint8_t header_buf[1 + 4];
	size_t offset = 0;

	hdr.type = type;
	hdr.flags = flags;
	memcpy(header_buf, &hdr, sizeof(hdr));
	offset += sizeof(hdr);

	len_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, len_buf);
	memcpy(header_buf + offset, len_buf, (size_t)len_len);
	offset += (size_t)len_len;

	if (write(fd, header_buf, offset) != (ssize_t)offset)
		return -1;
	if (payload_len > 0 && payload) {
		if (write(fd, payload, payload_len) != (ssize_t)payload_len)
			return -1;
	}
	return (ssize_t)(offset + payload_len);
}

static inline ssize_t mqtt_test_write_packet_header(int fd, control_packet_t type,
		uint8_t flags, uint32_t remaining_length)
{
	struct mqtt_fixed_header hdr = {0};
	uint8_t len_buf[4];
	int len_len;
	uint8_t header_buf[1 + 4];
	size_t offset = 0;

	hdr.type = type;
	hdr.flags = flags;
	memcpy(header_buf, &hdr, sizeof(hdr));
	offset += sizeof(hdr);

	len_len = mqtt_test_api.encode_var_byte(remaining_length, len_buf);
	memcpy(header_buf + offset, len_buf, (size_t)len_len);
	offset += (size_t)len_len;

	if (write(fd, header_buf, offset) != (ssize_t)offset)
		return -1;

	return (ssize_t)offset;
}

static inline ssize_t mqtt_test_read_packet(int fd, uint8_t *buf, size_t buf_len)
{
	ssize_t rd;

	rd = read(fd, buf, buf_len);
	if (rd < 0)
		return -1;
	return rd;
}

static inline int mqtt_test_decode_remaining_length(const uint8_t *buf,
		size_t len, size_t *out_len, size_t *out_consumed)
{
	const uint8_t *ptr = buf;
	size_t bytes_left = len;
	uint32_t value;

	errno = 0;
	value = mqtt_test_api.read_var_byte(&ptr, &bytes_left);
	if (value == 0 && errno)
		return -1;
	*out_len = value;
	*out_consumed = (size_t)(ptr - buf);
	return 0;
}

static inline int mqtt_test_read_packet_id(const uint8_t *buf, size_t len,
		uint16_t *out_id)
{
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;
	uint16_t tmp;

	if (len < 2)
		return -1;

	if (mqtt_test_decode_remaining_length(buf + 1, len - 1, &rl_len,
				&rl_consumed) == -1)
		return -1;

	offset = 1 + rl_consumed;
	if (offset + sizeof(uint16_t) > len)
		return -1;

	memcpy(&tmp, buf + offset, sizeof(uint16_t));
	*out_id = ntohs(tmp);
	return 0;
}

static inline int mqtt_test_drive_parse(struct client *client)
{
	int rc = 0;

	for (int i = 0; i < 8; i++) {
		rc = mqtt_test_api.parse_incoming(client);
		if (rc != 0)
			return rc;
		if (client->parse_state == READ_STATE_NEW)
			return 0;
	}

	return rc;
}

static inline int mqtt_test_set_nonblock(int fd)
{
	return test_set_nonblock(fd);
}

static inline void mqtt_test_sleep_ms(unsigned ms)
{
	test_sleep_ms(ms);
}

#endif
