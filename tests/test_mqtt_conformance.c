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

#include "mqtt_test_support.h"

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

static int read_connack_flags(int fd, uint8_t *out_flags)
{
	uint8_t buf[64];
	ssize_t rd;
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;

	rd = mqtt_test_read_packet(fd, buf, sizeof(buf));
	if (rd < 0)
		return -1;
	if (rd < 4)
		return -1;
	if (buf[0] != (uint8_t)(MQTT_CP_CONNACK << 4))
		return -1;
	if (mqtt_test_decode_remaining_length(buf + 1, (size_t)rd - 1, &rl_len,
				&rl_consumed) == -1)
		return -1;

	offset = 1 + rl_consumed;
	if (offset + 2 > (size_t)rd)
		return -1;

	*out_flags = buf[offset];
	return 0;
}

static int read_connack_reason(int fd, uint8_t *out_reason)
{
	uint8_t buf[64];
	ssize_t rd;
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;

	rd = mqtt_test_read_packet(fd, buf, sizeof(buf));
	if (rd < 0)
		return -1;
	if (rd < 5)
		return -1;
	if (buf[0] != (uint8_t)(MQTT_CP_CONNACK << 4))
		return -1;
	if (mqtt_test_decode_remaining_length(buf + 1, (size_t)rd - 1, &rl_len,
				&rl_consumed) == -1)
		return -1;

	offset = 1 + rl_consumed;
	if (offset + 2 > (size_t)rd)
		return -1;

	*out_reason = buf[offset + 1];
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

static size_t build_connect_packet_props(uint8_t *out, size_t out_len, uint8_t connect_flags,
		const struct property *props, unsigned num_props,
		const char *client_id, const char *username, const char *password)
{
	uint8_t payload[256];
	uint8_t prop_buf[128];
	uint8_t *ptr = payload;
	uint8_t *prop_ptr = prop_buf;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t payload_len;
	size_t remaining;
	size_t prop_len = 0;
	int prop_len_len;

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

	if (num_props) {
		ck_assert_int_eq(mqtt_test_api.build_properties(
				(const struct property (*)[])props, num_props, &prop_ptr), 0);
		prop_len = (size_t)(prop_ptr - prop_buf);
	}

	prop_len_len = mqtt_test_api.encode_var_byte((uint32_t)prop_len, remlen);
	memcpy(ptr, remlen, (size_t)prop_len_len);
	ptr += (size_t)prop_len_len;
	if (prop_len) {
		memcpy(ptr, prop_buf, prop_len);
		ptr += prop_len;
	}

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

static size_t build_publish_packet(uint8_t *out, size_t out_len, uint8_t flags,
		const uint8_t *topic, const uint8_t *payload, size_t payload_len,
		uint16_t packet_id)
{
	uint8_t var_buf[256];
	uint8_t *ptr = var_buf;
	uint16_t topic_len = 0;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t var_len;

	if (topic)
		topic_len = (uint16_t)strlen((const char *)topic);

	tmp16 = htons(topic_len);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	if (topic_len) {
		memcpy(ptr, topic, topic_len);
		ptr += topic_len;
	}

	if (GET_QOS(flags)) {
		tmp16 = htons(packet_id);
		memcpy(ptr, &tmp16, sizeof(tmp16));
		ptr += sizeof(tmp16);
	}

	*ptr++ = 0; /* properties length */

	if (payload_len) {
		memcpy(ptr, payload, payload_len);
		ptr += payload_len;
	}

	var_len = (size_t)(ptr - var_buf);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)var_len, remlen);

	if (1 + (size_t)remlen_len + var_len > out_len)
		return 0;

	out[0] = (uint8_t)(MQTT_CP_PUBLISH << 4) | flags;
	memcpy(out + 1, remlen, (size_t)remlen_len);
	memcpy(out + 1 + remlen_len, var_buf, var_len);
	return 1 + (size_t)remlen_len + var_len;
}

static size_t build_subscribe_packet(uint8_t *out, size_t out_len,
		uint16_t packet_id, const uint8_t *filter, uint8_t options)
{
	uint8_t payload[128];
	uint8_t *ptr = payload;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t payload_len;
	size_t remaining;

	tmp16 = htons(packet_id);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	*ptr++ = 0; /* properties length */

	tmp16 = htons((uint16_t)strlen((const char *)filter));
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, filter, strlen((const char *)filter));
	ptr += strlen((const char *)filter);
	*ptr++ = options;

	payload_len = (size_t)(ptr - payload);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	if (remaining > out_len)
		return 0;

	out[0] = (uint8_t)(MQTT_CP_SUBSCRIBE << 4) | MQTT_FLAG_SUBSCRIBE;
	memcpy(out + 1, remlen, (size_t)remlen_len);
	memcpy(out + 1 + remlen_len, payload, payload_len);
	return remaining;
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
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_SUBSCRIBE, MQTT_FLAG_SUBSCRIBE,
				payload, (size_t)(ptr - payload)), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 1, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_0_1)
{
	/* First packet must be CONNECT. */
	int fds[2];
	struct client client;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 0, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_connect_auth_method_rejected)
{
	uint8_t packet[256];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	struct property props[1];

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_AUTHENTICATION_METHOD;
	props[0].utf8_string = (unsigned char *)"x";

	packet_len = build_connect_packet_props(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			props, 1, "am1", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_BAD_AUTHENTICATION_METHOD);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_connect_auth_data_rejected)
{
	uint8_t packet[256];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	struct property props[1];
	uint8_t blob[1] = {0x01};

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_AUTHENTICATION_DATA;
	props[0].binary.data = blob;
	props[0].binary.len = sizeof(blob);

	packet_len = build_connect_packet_props(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			props, 1, "ad1", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_connect_topic_alias_max_zero)
{
	uint8_t packet[256];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	struct property props[1];

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_TOPIC_ALIAS_MAXIMUM;
	props[0].byte2 = 0;

	packet_len = build_connect_packet_props(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			props, 1, "ta0", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
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
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	int present = (flags & MQTT_CONNACK_FLAG_SESSION_PRESENT) != 0;

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);

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
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
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
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);
	ck_assert_ptr_nonnull(client.session);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_2_2_1_2_qos0_packet_id_treated_as_payload)
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
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_PUBLISH, 0, payload,
				(size_t)(ptr - payload)), -1);
	int rc = mqtt_test_drive_parse(&client);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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

START_TEST(test_mqtt_publish_qos_invalid)
{
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 31;
	ck_assert_int_eq(pthread_rwlock_init(&session.subscriptions_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&session.delivery_states_lock, NULL), 0);

	packet_len = build_publish_packet(packet, sizeof(packet),
			MQTT_FLAG_PUBLISH_QOS_MASK, (const uint8_t *)"a", NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	if (client.fd != -1)
		close(fds[0]);
	mqtt_test_cleanup_client(&client);
	pthread_rwlock_destroy(&session.subscriptions_lock);
	pthread_rwlock_destroy(&session.delivery_states_lock);
}
END_TEST

START_TEST(test_mqtt_publish_qos0_dup_rejected)
{
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 32;
	ck_assert_int_eq(pthread_rwlock_init(&session.subscriptions_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&session.delivery_states_lock, NULL), 0);

	packet_len = build_publish_packet(packet, sizeof(packet),
			MQTT_FLAG_PUBLISH_DUP, (const uint8_t *)"a", NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	if (client.fd != -1)
		close(fds[0]);
	mqtt_test_cleanup_client(&client);
	pthread_rwlock_destroy(&session.subscriptions_lock);
	pthread_rwlock_destroy(&session.delivery_states_lock);
}
END_TEST

START_TEST(test_mqtt_publish_qos1_packet_id_zero)
{
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 33;
	ck_assert_int_eq(pthread_rwlock_init(&session.subscriptions_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&session.delivery_states_lock, NULL), 0);

	packet_len = build_publish_packet(packet, sizeof(packet),
			MQTT_FLAG_PUBLISH_QOS1, (const uint8_t *)"a", NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	if (client.fd != -1)
		close(fds[0]);
	mqtt_test_cleanup_client(&client);
	pthread_rwlock_destroy(&session.subscriptions_lock);
	pthread_rwlock_destroy(&session.delivery_states_lock);
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
	ssize_t rd;

	memset(&session, 0, sizeof(session));
	session.id = 20;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_eq(mqtt_test_api.send_cp_puback(&client, packet_id, MQTT_SUCCESS), 0);
	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(mqtt_test_read_packet_id(buf, (size_t)rd, &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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
	ssize_t rd;

	memset(&session, 0, sizeof(session));
	session.id = 21;
	memset(&request, 0, sizeof(request));
	request.num_topics = 1;
	request.reason_codes = reason_codes;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_eq(mqtt_test_api.send_cp_suback(&client, packet_id, &request), 0);
	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(mqtt_test_read_packet_id(buf, (size_t)rd, &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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
	ssize_t rd;

	memset(&session, 0, sizeof(session));
	session.id = 22;
	memset(&request, 0, sizeof(request));
	request.num_topics = 1;
	request.reason_codes = reason_codes;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_eq(mqtt_test_api.send_cp_unsuback(&client, packet_id, &request), 0);
	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(mqtt_test_read_packet_id(buf, (size_t)rd, &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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

START_TEST(test_mqtt_subscribe_reserved_bits_rejected)
{
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 40;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 1,
			(const uint8_t *)"a/b", MQTT_SUBOPT_RESERVED_MASK);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_subscribe_qos3_rejected)
{
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 41;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 1,
			(const uint8_t *)"a/b", MQTT_SUBOPT_QOS_MASK);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_subscribe_retain_handling3_rejected)
{
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 42;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 1,
			(const uint8_t *)"a/b", MQTT_SUBOPT_RETAIN_HANDLING_MASK);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_subscribe_shared_no_local_rejected)
{
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 43;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 1,
			(const uint8_t *)"$shared/g/a", MQTT_SUBOPT_NO_LOCAL);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
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
	tcase_add_test(tc_req, test_mqtt_connect_auth_method_rejected);
	tcase_add_test(tc_req, test_mqtt_connect_auth_data_rejected);
	tcase_add_test(tc_req, test_mqtt_connect_topic_alias_max_zero);
	tcase_add_test(tc_req, test_mqtt_2_2_1_5_puback);
	tcase_add_test(tc_req, test_mqtt_2_2_1_6_suback);
	tcase_add_test(tc_req, test_mqtt_2_2_1_6_unsuback);
	tcase_add_test(tc_req, test_mqtt_2_2_1_2_qos0_packet_id_treated_as_payload);
	tcase_add_test(tc_req, test_mqtt_publish_qos_invalid);
	tcase_add_test(tc_req, test_mqtt_publish_qos0_dup_rejected);
	tcase_add_test(tc_req, test_mqtt_publish_qos1_packet_id_zero);
	tcase_add_test(tc_req, test_mqtt_4_7_3_1_topic_name);
	tcase_add_test(tc_req, test_mqtt_4_7_3_1_topic_filter);
	tcase_add_test(tc_req, test_mqtt_subscribe_reserved_bits_rejected);
	tcase_add_test(tc_req, test_mqtt_subscribe_qos3_rejected);
	tcase_add_test(tc_req, test_mqtt_subscribe_retain_handling3_rejected);
	tcase_add_test(tc_req, test_mqtt_subscribe_shared_no_local_rejected);
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
