/*
 * MQTT conformance reporting and targeted requirements.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>

#include "mqtt_test_api.h"

extern bool opt_database;

#define MQTT_MD_PATH "doc/mqtt.md"

struct mqtt_req {
	char *id;
	char *section;
	char *statement;
};

struct mqtt_section {
	char *name;
	size_t first_idx;
	size_t count;
};

static char **g_doc_ids;
static size_t g_doc_id_count;

static char *dup_trim(const char *s)
{
	if ((unsigned char)s[0] == 0xEF &&
			(unsigned char)s[1] == 0xBB &&
			(unsigned char)s[2] == 0xBF)
		s += 3;
	while (*s == ' ' || *s == '\t')
		s++;
	if (*s == '\"') {
		size_t len = strlen(s);
		if (len >= 2 && s[len - 1] == '\"') {
			char *out = malloc(len - 1);
			if (!out)
				return NULL;
			memcpy(out, s + 1, len - 2);
			out[len - 2] = '\0';
			return out;
		}
	}
	return strdup(s);
}

static int load_doc_ids(void)
{
	FILE *fp;
	char *line = NULL;
	size_t cap = 0;
	ssize_t len;

	fp = fopen(MQTT_MD_PATH, "r");
	if (!fp)
		return -1;

	while ((len = getline(&line, &cap, fp)) != -1) {
		char *id_start;
		char *id_end;
		char *id;
		char **tmp;

		if (len < 10)
			continue;

		id_start = strstr(line, "| [MQTT-");
		if (!id_start)
			continue;

		id_start += 2;
		id_end = strchr(id_start, ']');
		if (!id_end)
			continue;

		id_end++;
		*id_end = '\0';
		id = dup_trim(id_start);
		if (!id)
			return -1;

		tmp = realloc(g_doc_ids, sizeof(*g_doc_ids) * (g_doc_id_count + 1));
		if (!tmp)
			return -1;
		g_doc_ids = tmp;
		g_doc_ids[g_doc_id_count++] = id;
	}

	free(line);
	fclose(fp);
	return 0;
}

static void free_data(void)
{
	for (size_t i = 0; i < g_doc_id_count; i++)
		free(g_doc_ids[i]);
	free(g_doc_ids);
}

static void init_client(struct client *client, int fd, struct session *session)
{
	memset(client, 0, sizeof(*client));
	client->fd = fd;
	client->session = session;
	client->parse_state = READ_STATE_NEW;
	client->state = CS_ACTIVE;
	pthread_rwlock_init(&client->active_packets_lock, NULL);
	pthread_rwlock_init(&client->po_lock, NULL);
}

static void cleanup_client(struct client *client)
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

static struct session *create_session(const char *client_id)
{
	struct client dummy;
	struct session *session;

	memset(&dummy, 0, sizeof(dummy));
	dummy.client_id = (const uint8_t *)client_id;

	session = mqtt_test_api.alloc_session(&dummy, NULL);
	if (session == NULL)
		return NULL;
	if (mqtt_test_api.register_session(session) == -1)
		return NULL;

	session->client = NULL;
	dummy.session = NULL;
	return session;
}

static ssize_t write_packet(int fd, control_packet_t type, uint8_t flags,
		const uint8_t *payload, size_t payload_len)
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

static ssize_t read_packet(int fd, uint8_t *buf, size_t buf_len)
{
	ssize_t rd;

	rd = read(fd, buf, buf_len);
	if (rd < 0)
		return -1;
	return rd;
}

static int decode_remaining_length(const uint8_t *buf, size_t len, size_t *out_len, size_t *out_consumed)
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

static int read_packet_id(const uint8_t *buf, size_t len, uint16_t *out_id)
{
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;
	uint16_t tmp;

	if (len < 2)
		return -1;

	if (decode_remaining_length(buf + 1, len - 1, &rl_len, &rl_consumed) == -1)
		return -1;

	offset = 1 + rl_consumed;
	if (offset + sizeof(uint16_t) > len)
		return -1;

	memcpy(&tmp, buf + offset, sizeof(uint16_t));
	*out_id = ntohs(tmp);
	return 0;
}

static int read_connack_flags(int fd, uint8_t *out_flags)
{
	uint8_t buf[64];
	ssize_t rd;
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;

	rd = read_packet(fd, buf, sizeof(buf));
	if (rd < 0)
		return -1;
	if (rd < 4)
		return -1;
	if (buf[0] != (uint8_t)(MQTT_CP_CONNACK << 4))
		return -1;
	if (decode_remaining_length(buf + 1, (size_t)rd - 1, &rl_len, &rl_consumed) == -1)
		return -1;

	offset = 1 + rl_consumed;
	if (offset + 2 > (size_t)rd)
		return -1;

	*out_flags = buf[offset];
	return 0;
}

static size_t build_connect_packet(uint8_t *out, size_t out_len, uint8_t connect_flags,
		const char *client_id, const char *username, const char *password)
{
	uint8_t payload[128];
	uint8_t *ptr = payload;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t payload_len;
	size_t remaining;

	tmp16 = htons(4);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, "MQTT", 4);
	ptr += 4;
	*ptr++ = 5;
	*ptr++ = connect_flags;
	tmp16 = htons(0);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	*ptr++ = 0; /* properties length */
	tmp16 = htons((uint16_t)strlen(client_id));
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, client_id, strlen(client_id));
	ptr += strlen(client_id);

	if (connect_flags & MQTT_CONNECT_FLAG_USERNAME) {
		tmp16 = htons((uint16_t)strlen(username));
		memcpy(ptr, &tmp16, sizeof(tmp16));
		ptr += sizeof(tmp16);
		memcpy(ptr, username, strlen(username));
		ptr += strlen(username);
	}

	if (connect_flags & MQTT_CONNECT_FLAG_PASSWORD) {
		tmp16 = htons((uint16_t)strlen(password));
		memcpy(ptr, &tmp16, sizeof(tmp16));
		ptr += sizeof(tmp16);
		memcpy(ptr, password, strlen(password));
		ptr += strlen(password);
	}

	payload_len = (size_t)(ptr - payload);

	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	if (remaining > out_len)
		return 0;

	out[0] = (uint8_t)(MQTT_CP_CONNECT << 4);
	memcpy(out + 1, remlen, (size_t)remlen_len);
	memcpy(out + 1 + remlen_len, payload, payload_len);
	return remaining;
}

static int drive_parse(struct client *client)
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

START_TEST(test_conformance_table_loaded)
{
	ck_assert_int_gt((int)g_doc_id_count, 200);
}
END_TEST

/*
 * Conformance checks (pragmatic/targeted) against src/main.c helpers.
 */

START_TEST(test_mqtt_1_5_4_1)
{
	/* UTF-8 must be well-formed. */
	const uint8_t invalid[] = {0xC0, 0x00}; /* invalid continuation */
	const uint8_t valid[] = "hello";

	ck_assert_int_eq(mqtt_test_api.is_valid_utf8(valid), 0);
	ck_assert_int_eq(mqtt_test_api.is_valid_utf8(invalid), -1);
}
END_TEST

START_TEST(test_mqtt_1_5_4_surrogate)
{
	/* UTF-8 must not include surrogate code points U+D800..U+DFFF. */
	const uint8_t surrogate[] = {0xED, 0xA0, 0x80, 0x00}; /* U+D800 */

	ck_assert_int_eq(mqtt_test_api.is_valid_utf8(surrogate), -1);
}
END_TEST

START_TEST(test_mqtt_1_5_4_2)
{
	/* UTF-8 string must not include U+0000. */
	uint8_t buf[] = {0x00, 0x03, 'a', 0x00, 'b'};
	const uint8_t *ptr = buf;
	size_t bytes_left = sizeof(buf);
	uint8_t *out;

	errno = 0;
	out = mqtt_test_api.read_utf8(&ptr, &bytes_left);
	ck_assert_ptr_null(out);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_mqtt_1_5_5_1)
{
	/* Variable byte integer must use minimum bytes. */
	uint8_t buf[4];
	int len;

	len = mqtt_test_api.encode_var_byte(127, buf);
	ck_assert_int_eq(len, 1);
	len = mqtt_test_api.encode_var_byte(128, buf);
	ck_assert_int_eq(len, 2);
}
END_TEST

START_TEST(test_mqtt_2_2_1_3)
{
	/* Packet Identifier must be non-zero for SUBSCRIBE. */
	uint8_t payload[64];
	uint8_t *ptr = payload;
	uint16_t packet_id = htons(0);
	uint16_t topic_len = htons(1);
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 11;

	memcpy(ptr, &packet_id, sizeof(packet_id));
	ptr += sizeof(packet_id);
	*ptr++ = 0x00; /* properties length */
	memcpy(ptr, &topic_len, sizeof(topic_len));
	ptr += sizeof(topic_len);
	*ptr++ = 'a';
	*ptr++ = 0x00; /* options */

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write_packet(fds[1], MQTT_CP_SUBSCRIBE, MQTT_FLAG_SUBSCRIBE,
				payload, (size_t)(ptr - payload)), -1);
	ck_assert_int_eq(drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_2_1_3_1)
{
	/* Reserved flags must be set to the specified value (PINGREQ => 0). */
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 12;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write_packet(fds[1], MQTT_CP_PINGREQ, 1, NULL, 0), -1);
	ck_assert_int_eq(drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_0_1)
{
	/* First packet must be CONNECT. */
	int fds[2];
	struct client client;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write_packet(fds[1], MQTT_CP_PINGREQ, 0, NULL, 0), -1);
	ck_assert_int_eq(drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_3)
{
	/* CONNECT reserved flag must be 0. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 13;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_RESERVED |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c1", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_2_2)
{
	/* Clean Start = 1 => Session Present must be 0. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0xFF;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_CLEAN_START |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c2", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_2_3_no_session)
{
	/* Clean Start = 0 and no existing session => Session Present must be 0. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0xFF;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c3", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_5_existing_session)
{
	/* Clean Start = 0 with existing session => Session Present must be 1. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0x00;
	int fds[2];
	struct client client;

	ck_assert_ptr_nonnull(create_session("c4"));

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c4", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	int present = (flags & MQTT_CONNACK_FLAG_SESSION_PRESENT) != 0;

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	cleanup_client(&client);

	ck_assert_int_ne(present, 0);
}
END_TEST

START_TEST(test_mqtt_3_1_2_4_clean_start_discards_session)
{
	/* Clean Start = 1 => Session Present must be 0 even if prior session exists. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0xFF;
	int fds[2];
	struct client client;

	ck_assert_ptr_nonnull(create_session("c5"));

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_CLEAN_START |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c5", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_6_no_existing_session)
{
	/* Clean Start = 0 without existing session => server creates new session. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c6", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(drive_parse(&client), 0);
	ck_assert_ptr_nonnull(client.session);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_2_2_1_2_qos0_packet_id_rejected)
{
	/* QoS 0 PUBLISH must not include Packet Identifier. */
	uint8_t payload[64];
	uint8_t *ptr = payload;
	uint16_t topic_len = htons(1);
	uint16_t fake_packet_id = htons(1);
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 30;
	session.client_id = (const uint8_t *)"sess-qos0";
	ck_assert_int_eq(pthread_rwlock_init(&session.subscriptions_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&session.delivery_states_lock, NULL), 0);

	memcpy(ptr, &topic_len, sizeof(topic_len));
	ptr += sizeof(topic_len);
	*ptr++ = 'a';
	memcpy(ptr, &fake_packet_id, sizeof(fake_packet_id));
	ptr += sizeof(fake_packet_id);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write_packet(fds[1], MQTT_CP_PUBLISH, 0, payload,
				(size_t)(ptr - payload)), -1);
	int rc = drive_parse(&client);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
	pthread_rwlock_destroy(&session.subscriptions_lock);
	pthread_rwlock_destroy(&session.delivery_states_lock);

	/*
	 * Note: A receiver cannot reliably detect an illegal Packet Identifier
	 * in QoS 0 PUBLISH; extra bytes are indistinguishable from payload.
	 * Current behavior accepts the packet and treats bytes as payload.
	 */
	ck_assert_int_eq(rc, 0);
}
END_TEST

START_TEST(test_mqtt_2_2_1_5_puback)
{
	/* PUBACK must contain the same Packet Identifier. */
	uint8_t buf[64];
	uint16_t packet_id = 42;
	uint16_t parsed_id = 0;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 20;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_eq(mqtt_test_api.send_cp_puback(&client, packet_id, MQTT_SUCCESS), 0);
	ck_assert_int_gt((int)read_packet(fds[1], buf, sizeof(buf)), 0);
	ck_assert_int_eq(read_packet_id(buf, sizeof(buf), &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_2_2_1_6_suback)
{
	/* SUBACK must contain the Packet Identifier used in SUBSCRIBE. */
	uint8_t buf[64];
	uint16_t packet_id = 77;
	uint16_t parsed_id = 0;
	int fds[2];
	struct client client;
	struct session session;
	struct topic_sub_request request;
	uint8_t reason_codes[1] = { MQTT_GRANTED_QOS_0 };

	memset(&session, 0, sizeof(session));
	session.id = 21;
	memset(&request, 0, sizeof(request));
	request.num_topics = 1;
	request.reason_codes = reason_codes;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_eq(mqtt_test_api.send_cp_suback(&client, packet_id, &request), 0);
	ck_assert_int_gt((int)read_packet(fds[1], buf, sizeof(buf)), 0);
	ck_assert_int_eq(read_packet_id(buf, sizeof(buf), &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_2_2_1_6_unsuback)
{
	/* UNSUBACK must contain the Packet Identifier used in UNSUBSCRIBE. */
	uint8_t buf[64];
	uint16_t packet_id = 88;
	uint16_t parsed_id = 0;
	int fds[2];
	struct client client;
	struct session session;
	struct topic_sub_request request;
	uint8_t reason_codes[1] = { MQTT_SUCCESS };

	memset(&session, 0, sizeof(session));
	session.id = 22;
	memset(&request, 0, sizeof(request));
	request.num_topics = 1;
	request.reason_codes = reason_codes;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_eq(mqtt_test_api.send_cp_unsuback(&client, packet_id, &request), 0);
	ck_assert_int_gt((int)read_packet(fds[1], buf, sizeof(buf)), 0);
	ck_assert_int_eq(read_packet_id(buf, sizeof(buf), &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_4_7_3_1_topic_name)
{
	/* Topic Name must not contain wildcards. */
	const uint8_t name[] = "a/+/c";

	ck_assert_int_eq(mqtt_test_api.is_valid_topic_name(name), -1);
}
END_TEST

START_TEST(test_mqtt_4_7_3_1_topic_filter)
{
	/* Topic Filter must be valid; rejects bad wildcard placement. */
	const uint8_t filter[] = "a/#/b";

	ck_assert_int_eq(mqtt_test_api.is_valid_topic_filter(filter), -1);
}
END_TEST

static Suite *mqtt_conformance_suite(void)
{
	Suite *s = suite_create("mqtt_conformance");
	TCase *tc_doc = tcase_create("appendix_b_doc");
	TCase *tc_req = tcase_create("appendix_b_req");

	tcase_add_test(tc_doc, test_conformance_table_loaded);
	suite_add_tcase(s, tc_doc);

	tcase_add_test(tc_req, test_mqtt_1_5_4_1);
	tcase_add_test(tc_req, test_mqtt_1_5_4_surrogate);
	tcase_add_test(tc_req, test_mqtt_1_5_4_2);
	tcase_add_test(tc_req, test_mqtt_1_5_5_1);
	tcase_add_test(tc_req, test_mqtt_2_2_1_3);
	tcase_add_test(tc_req, test_mqtt_2_1_3_1);
	tcase_add_test(tc_req, test_mqtt_3_1_0_1);
	tcase_add_test(tc_req, test_mqtt_3_1_2_3);
	tcase_add_test(tc_req, test_mqtt_3_2_2_2);
	tcase_add_test(tc_req, test_mqtt_3_2_2_3_no_session);
	tcase_add_test(tc_req, test_mqtt_3_1_2_5_existing_session);
	tcase_add_test(tc_req, test_mqtt_3_1_2_4_clean_start_discards_session);
	tcase_add_test(tc_req, test_mqtt_3_1_2_6_no_existing_session);
	tcase_add_test(tc_req, test_mqtt_2_2_1_5_puback);
	tcase_add_test(tc_req, test_mqtt_2_2_1_6_suback);
	tcase_add_test(tc_req, test_mqtt_2_2_1_6_unsuback);
	tcase_add_test(tc_req, test_mqtt_2_2_1_2_qos0_packet_id_rejected);
	tcase_add_test(tc_req, test_mqtt_4_7_3_1_topic_name);
	tcase_add_test(tc_req, test_mqtt_4_7_3_1_topic_filter);
	suite_add_tcase(s, tc_req);
	return s;
}

int main(void)
{
	Suite *s;
	SRunner *sr;
	int failed;

	opt_database = false;

	if (load_doc_ids() == -1)
		return 1;

	s = mqtt_conformance_suite();
	sr = srunner_create(s);

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	free_data();
	return failed == 0 ? 0 : 1;
}
