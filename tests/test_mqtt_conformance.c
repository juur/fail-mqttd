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
#include "mqtt_test_mapping.h"

static int saved_stderr_fd = -1;
static const char *mqtt_test_only;
static long mqtt_test_limit = -1;
static long mqtt_test_added;

static void mqtt_test_init_filter(void)
{
	const char *limit_env;

	if (mqtt_test_only == NULL)
		mqtt_test_only = getenv("MQTT_TEST_ONLY");

	if (mqtt_test_limit != -1)
		return;

	limit_env = getenv("MQTT_TEST_LIMIT");
	if (limit_env == NULL || *limit_env == '\0') {
		mqtt_test_limit = -2;
		return;
	}

	char *end = NULL;
	long value = strtol(limit_env, &end, 10);
	if (end == limit_env || value < 0) {
		mqtt_test_limit = -2;
		return;
	}

	mqtt_test_limit = value;
}

static bool mqtt_test_should_add(const char *name)
{
	const char *trace_env;

	mqtt_test_init_filter();
	if (mqtt_test_only && strstr(name, mqtt_test_only) == NULL)
		return false;
	if (mqtt_test_limit >= 0 && mqtt_test_added >= mqtt_test_limit)
		return false;

	mqtt_test_added++;
	trace_env = getenv("MQTT_TEST_TRACE");
	if (trace_env && *trace_env != '\0') {
		printf("ADD %s\n", name);
		fflush(stdout);
	}
	return true;
}

#undef tcase_add_test
#define tcase_add_test(tc, tf) \
	do { \
		if (mqtt_test_should_add(#tf)) \
			_tcase_add_test((tc), (tf), 0, 0, 0, 1); \
	} while (0)

struct mqtt_req_map {
	const char *id;
	const char *test_name;
	const TTest *test;
};


static struct mqtt_req_map g_req_map[] = {
#define MQTT_REQ_ENTRY(id, fn) { id, #fn, NULL },
	MQTT_REQ_MAP(MQTT_REQ_ENTRY)
#undef MQTT_REQ_ENTRY
};

static struct mqtt_req_map g_policy_map[] = {
#define MQTT_POLICY_ENTRY(id, fn) { id, #fn, NULL },
	MQTT_POLICY_MAP(MQTT_POLICY_ENTRY)
#undef MQTT_POLICY_ENTRY
};

static const size_t g_req_map_count = sizeof(g_req_map) / sizeof(g_req_map[0]);
static const size_t g_policy_map_count = sizeof(g_policy_map) / sizeof(g_policy_map[0]);

static void add_mapped_tests(TCase *tc, const struct mqtt_req_map *map, size_t count)
{
	for (size_t i = 0; i < count; i++) {
		if (mqtt_test_should_add(map[i].test_name))
			_tcase_add_test(tc, map[i].test, 0, 0, 0, 1);
	}
}

static void mqtt_silence_stderr(void)
{
	(void)mqtt_test_silence_stderr(&saved_stderr_fd);
}

static void mqtt_restore_stderr(void)
{
	mqtt_test_restore_stderr(saved_stderr_fd);
	saved_stderr_fd = -1;
}

static void mqtt_report_test_start(void)
{
	const char *trace_env = getenv("MQTT_TEST_TRACE");
	const char *name = tcase_name();

	if (trace_env == NULL || *trace_env == '\0')
		return;
	if (!name)
		return;
	printf("TEST %s\n", name);
	fflush(stdout);
}

static void assert_no_extra_packet(int fd)
{
	uint8_t buf[8];
	ssize_t rd;

	errno = 0;
	rd = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
	ck_assert_int_eq(rd, -1);
	ck_assert(errno == EAGAIN || errno == EWOULDBLOCK);
}

static void assert_no_packet_or_closed(int fd)
{
	uint8_t buf[8];
	ssize_t rd;

	errno = 0;
	rd = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
	if (rd == 0)
		return;
	if (rd == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
		return;
	ck_assert_msg(0, "unexpected packet or recv error: rd=%zd errno=%d", rd, errno);
}
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

static int read_u16(const uint8_t **ptr, size_t *bytes_left, uint16_t *out)
{
	if (*bytes_left < sizeof(uint16_t))
		return -1;
	memcpy(out, *ptr, sizeof(uint16_t));
	*out = ntohs(*out);
	*ptr += sizeof(uint16_t);
	*bytes_left -= sizeof(uint16_t);
	return 0;
}

static int parse_connect_property_length(const uint8_t *packet, size_t len,
		uint32_t *out_len)
{
	const uint8_t *ptr = packet + 1;
	size_t bytes_left = len - 1;
	uint32_t remaining;
	uint16_t name_len;

	errno = 0;
	remaining = mqtt_test_api.read_var_byte(&ptr, &bytes_left);
	if (remaining == 0 && errno)
		return -1;

	if (read_u16(&ptr, &bytes_left, &name_len) == -1)
		return -1;

	if (bytes_left < name_len + 1 + 1 + sizeof(uint16_t))
		return -1;

	ptr += name_len;
	bytes_left -= name_len;

	ptr++;
	bytes_left--;
	ptr++;
	bytes_left--;

	if (read_u16(&ptr, &bytes_left, &(uint16_t){0}) == -1)
		return -1;

	*out_len = mqtt_test_api.read_var_byte(&ptr, &bytes_left);
	if (*out_len == 0 && errno)
		return -1;

	return 0;
}

static int patch_connect_version(uint8_t *packet, size_t len, uint8_t version)
{
	uint8_t *ptr = packet + 1;
	size_t bytes_left = len - 1;
	uint32_t remaining;
	uint16_t name_len;

	errno = 0;
	remaining = mqtt_test_api.read_var_byte((const uint8_t **)&ptr, &bytes_left);
	if (remaining == 0 && errno)
		return -1;

	if (read_u16((const uint8_t **)&ptr, &bytes_left, &name_len) == -1)
		return -1;

	if (bytes_left < (size_t)name_len + 1)
		return -1;

	ptr += name_len;
	bytes_left -= name_len;

	*ptr = version;
	return 0;
}

static int patch_connect_flags(uint8_t *packet, size_t len, uint8_t flags)
{
	uint8_t *ptr = packet + 1;
	size_t bytes_left = len - 1;
	uint32_t remaining;
	uint16_t name_len;

	errno = 0;
	remaining = mqtt_test_api.read_var_byte((const uint8_t **)&ptr, &bytes_left);
	if (remaining == 0 && errno)
		return -1;

	if (read_u16((const uint8_t **)&ptr, &bytes_left, &name_len) == -1)
		return -1;

	if (bytes_left < (size_t)name_len + 2)
		return -1;

	ptr += name_len;
	bytes_left -= name_len;

	ptr++; /* protocol version */
	*ptr = flags;
	return 0;
}

static int patch_connect_protocol_name(uint8_t *packet, size_t len,
		const char *name)
{
	uint8_t *ptr = packet + 1;
	size_t bytes_left = len - 1;
	uint32_t remaining;
	uint16_t name_len;

	if (name == NULL)
		return -1;

	errno = 0;
	remaining = mqtt_test_api.read_var_byte((const uint8_t **)&ptr, &bytes_left);
	if (remaining == 0 && errno)
		return -1;

	if (read_u16((const uint8_t **)&ptr, &bytes_left, &name_len) == -1)
		return -1;

	if (bytes_left < name_len)
		return -1;

	if (strlen(name) != name_len)
		return -1;

	memcpy(ptr, name, name_len);
	return 0;
}

static int trim_packet_remaining_length(uint8_t *packet, size_t *packet_len,
		size_t trim_bytes)
{
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	uint8_t encoded[4];
	int encoded_len;
	size_t remaining;

	if (mqtt_test_decode_remaining_length(packet + 1, *packet_len - 1,
				&rl_len, &rl_consumed) == -1)
		return -1;

	if (rl_len < trim_bytes)
		return -1;

	remaining = rl_len - trim_bytes;
	encoded_len = mqtt_test_api.encode_var_byte((uint32_t)remaining, encoded);
	if (encoded_len <= 0 || (size_t)encoded_len != rl_consumed)
		return -1;

	memcpy(packet + 1, encoded, (size_t)encoded_len);
	*packet_len -= trim_bytes;
	return 0;
}

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

static void normalize_doc_id(char *id)
{
	size_t len;

	if (id == NULL)
		return;

	len = strlen(id);
	if (len >= 2 && id[0] == '[' && id[len - 1] == ']') {
		memmove(id, id + 1, len - 2);
		id[len - 2] = '\0';
	}
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
		normalize_doc_id(id);

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

static bool doc_id_known(const char *id)
{
	for (size_t i = 0; i < g_doc_id_count; i++) {
		if (strcmp(g_doc_ids[i], id) == 0)
			return true;
	}
	return false;
}

static void print_supported_requirements(void)
{
	size_t supported_in_doc = 0;

	printf("MQTT conformance: declared requirement IDs (%zu):\n", g_req_map_count);
	for (size_t i = 0; i < g_req_map_count; i++) {
		bool known = doc_id_known(g_req_map[i].id);
		printf("  - %s (%s)%s\n", g_req_map[i].id, g_req_map[i].test_name,
				known ? "" : " (missing in doc)");
		if (known)
			supported_in_doc++;
	}

	if (g_doc_id_count) {
		double pct = (double)supported_in_doc * 100.0 / (double)g_doc_id_count;
		printf("MQTT conformance: %zu/%zu doc requirement IDs present (%.1f%%)\n",
				supported_in_doc, g_doc_id_count, pct);
	}

	if (g_policy_map_count > 0) {
		printf("MQTT conformance: policy/MAY requirement IDs (not counted):\n");
		for (size_t i = 0; i < g_policy_map_count; i++) {
			bool known = doc_id_known(g_policy_map[i].id);
			printf("  - %s (%s)%s\n", g_policy_map[i].id, g_policy_map[i].test_name,
					known ? "" : " (missing in doc)");
		}
	}
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

static int is_valid_connack_reason(uint8_t reason)
{
	switch (reason) {
	case MQTT_SUCCESS:
	case MQTT_UNSPECIFIED_ERROR:
	case MQTT_MALFORMED_PACKET:
	case MQTT_PROTOCOL_ERROR:
	case MQTT_UNSUPPORTED_PROTOCOL_VERSION:
	case MQTT_CLIENT_IDENTIFIER_NOT_VALID:
	case MQTT_BAD_USER_NAME_OR_PASSWORD:
	case MQTT_NOT_AUTHORIZED:
	case MQTT_SERVER_UNAVAILABLE:
	case MQTT_SERVER_BUSY:
	case MQTT_BANNED:
	case MQTT_BAD_AUTHENTICATION_METHOD:
	case MQTT_TOPIC_NAME_INVALID:
	case MQTT_PACKET_TOO_LARGE:
	case MQTT_QUOTA_EXCEEDED:
	case MQTT_PAYLOAD_FORMAT_INVALID:
	case MQTT_RETAIN_NOT_SUPPORTED:
	case MQTT_QOS_NOT_SUPPORTED:
	case MQTT_USE_ANOTHER_SERVER:
	case MQTT_SERVER_MOVED:
	case MQTT_CONNECTION_RATE_EXCEEDED:
	case MQTT_MAXIMUM_CONNECT_TIME:
	case MQTT_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED:
	case MQTT_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED:
		return 1;
	default:
		return 0;
	}
}

static int parse_publish_packet(const uint8_t *buf, size_t len, uint8_t *out_flags,
		uint16_t *out_packet_id)
{
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;
	uint16_t topic_len;
	uint16_t tmp;
	uint8_t flags;

	if (len < 2)
		return -1;

	if ((buf[0] >> 4) != MQTT_CP_PUBLISH)
		return -1;

	flags = buf[0] & 0x0F;

	if (mqtt_test_decode_remaining_length(buf + 1, len - 1, &rl_len,
				&rl_consumed) == -1)
		return -1;

	offset = 1 + rl_consumed;
	if (offset + sizeof(uint16_t) > len)
		return -1;

	memcpy(&tmp, buf + offset, sizeof(uint16_t));
	topic_len = ntohs(tmp);
	offset += sizeof(uint16_t);
	if (offset + topic_len > len)
		return -1;
	offset += topic_len;

	if (GET_QOS(flags) == 0)
		return -1;
	if (offset + sizeof(uint16_t) > len)
		return -1;

	memcpy(&tmp, buf + offset, sizeof(uint16_t));
	*out_packet_id = ntohs(tmp);

	if (out_flags)
		*out_flags = flags;
	return 0;
}

static int read_publish_flags(const uint8_t *buf, size_t len, uint8_t *out_flags)
{
	if (len < 2)
		return -1;
	if ((buf[0] >> 4) != MQTT_CP_PUBLISH)
		return -1;
	if (out_flags)
		*out_flags = buf[0] & 0x0F;
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

static size_t build_connect_packet_will(uint8_t *out, size_t out_len, uint8_t connect_flags,
		const char *client_id, const char *will_topic, const uint8_t *will_payload,
		size_t will_payload_len, const char *username, const char *password)
{
	uint8_t payload[256];
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

	if (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
		*ptr++ = 0; /* will properties length */
		tmp16 = htons((uint16_t)strlen(will_topic));
		memcpy(ptr, &tmp16, sizeof(tmp16));
		ptr += sizeof(tmp16);
		memcpy(ptr, will_topic, strlen(will_topic));
		ptr += strlen(will_topic);
		tmp16 = htons((uint16_t)will_payload_len);
		memcpy(ptr, &tmp16, sizeof(tmp16));
		ptr += sizeof(tmp16);
		memcpy(ptr, will_payload, will_payload_len);
		ptr += will_payload_len;
	}

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

static size_t build_pubrel_packet(uint8_t *out, size_t out_len, uint8_t flags,
		uint16_t packet_id)
{
	uint8_t payload[4];
	uint8_t *ptr = payload;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t payload_len;
	size_t remaining;

	tmp16 = htons(packet_id);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);

	payload_len = (size_t)(ptr - payload);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	if (remaining > out_len)
		return 0;

	out[0] = (uint8_t)(MQTT_CP_PUBREL << 4) | flags;
	memcpy(out + 1, remlen, (size_t)remlen_len);
	memcpy(out + 1 + remlen_len, payload, payload_len);
	return remaining;
}
static size_t build_publish_packet_props(uint8_t *out, size_t out_len, uint8_t flags,
		const uint8_t *topic, const struct property *props, unsigned num_props,
		const uint8_t *payload, size_t payload_len, uint16_t packet_id)
{
	uint8_t var_buf[256];
	uint8_t prop_buf[128];
	uint8_t *ptr = var_buf;
	uint8_t *prop_ptr = prop_buf;
	uint16_t topic_len = 0;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t var_len;
	size_t prop_len = 0;

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

	if (num_props) {
		ck_assert_int_eq(mqtt_test_api.build_properties(
				(const struct property (*)[])props, num_props, &prop_ptr), 0);
		prop_len = (size_t)(prop_ptr - prop_buf);
	}

	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)prop_len, remlen);
	memcpy(ptr, remlen, (size_t)remlen_len);
	ptr += (size_t)remlen_len;
	if (prop_len) {
		memcpy(ptr, prop_buf, prop_len);
		ptr += prop_len;
	}

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

static size_t build_unsubscribe_packet(uint8_t *out, size_t out_len,
		uint16_t packet_id, const uint8_t *filter, uint8_t flags)
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

	payload_len = (size_t)(ptr - payload);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	if (remaining > out_len)
		return 0;

	out[0] = (uint8_t)(MQTT_CP_UNSUBSCRIBE << 4) | flags;
	memcpy(out + 1, remlen, (size_t)remlen_len);
	memcpy(out + 1 + remlen_len, payload, payload_len);
	return remaining;
}

static size_t build_subscribe_packet_empty(uint8_t *out, size_t out_len,
		uint16_t packet_id)
{
	uint8_t payload[8];
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

static size_t build_unsubscribe_packet_empty(uint8_t *out, size_t out_len,
		uint16_t packet_id)
{
	uint8_t payload[8];
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

	payload_len = (size_t)(ptr - payload);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	if (remaining > out_len)
		return 0;

	out[0] = (uint8_t)(MQTT_CP_UNSUBSCRIBE << 4) | MQTT_FLAG_UNSUBSCRIBE;
	memcpy(out + 1, remlen, (size_t)remlen_len);
	memcpy(out + 1 + remlen_len, payload, payload_len);
	return remaining;
}

static size_t build_subscribe_packet_two_filters(uint8_t *out, size_t out_len,
		uint16_t packet_id, const char *filter_a, uint8_t options_a,
		const char *filter_b, uint8_t options_b)
{
	uint8_t payload[256];
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

	tmp16 = htons((uint16_t)strlen(filter_a));
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, filter_a, strlen(filter_a));
	ptr += strlen(filter_a);
	*ptr++ = options_a;

	tmp16 = htons((uint16_t)strlen(filter_b));
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, filter_b, strlen(filter_b));
	ptr += strlen(filter_b);
	*ptr++ = options_b;

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

static size_t build_unsubscribe_packet_two_filters(uint8_t *out, size_t out_len,
		uint16_t packet_id, const char *filter_a, const char *filter_b)
{
	uint8_t payload[256];
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

	tmp16 = htons((uint16_t)strlen(filter_a));
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, filter_a, strlen(filter_a));
	ptr += strlen(filter_a);

	tmp16 = htons((uint16_t)strlen(filter_b));
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, filter_b, strlen(filter_b));
	ptr += strlen(filter_b);

	payload_len = (size_t)(ptr - payload);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	if (remaining > out_len)
		return 0;

	out[0] = (uint8_t)(MQTT_CP_UNSUBSCRIBE << 4) | MQTT_FLAG_UNSUBSCRIBE;
	memcpy(out + 1, remlen, (size_t)remlen_len);
	memcpy(out + 1 + remlen_len, payload, payload_len);
	return remaining;
}

START_TEST(test_conformance_table_loaded)
{
	ck_assert_int_gt((int)g_doc_id_count, 200);
	for (size_t i = 0; i < g_req_map_count; i++) {
		ck_assert_msg(doc_id_known(g_req_map[i].id),
				"missing doc id %s", g_req_map[i].id);
	}
	for (size_t i = 0; i < g_policy_map_count; i++) {
		ck_assert_msg(doc_id_known(g_policy_map[i].id),
				"missing doc id %s", g_policy_map[i].id);
	}

	for (size_t i = 0; i < g_req_map_count; i++) {
		for (size_t j = i + 1; j < g_req_map_count; j++) {
			ck_assert_msg(strcmp(g_req_map[i].id, g_req_map[j].id) != 0,
					"duplicate requirement id %s", g_req_map[i].id);
		}
	}
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
	ck_assert_int_eq(errno, EILSEQ);
}
END_TEST

START_TEST(test_mqtt_1_5_4_3)
{
	/* BOM (0xEF 0xBB 0xBF) must not be stripped. */
	uint8_t buf[] = {0x00, 0x03, 0xEF, 0xBB, 0xBF};
	const uint8_t *ptr = buf;
	size_t bytes_left = sizeof(buf);
	uint8_t *out;

	out = mqtt_test_api.read_utf8(&ptr, &bytes_left);
	if (out) {
		ck_assert_int_eq((unsigned char)out[0], 0xEF);
		ck_assert_int_eq((unsigned char)out[1], 0xBB);
		ck_assert_int_eq((unsigned char)out[2], 0xBF);
		free(out);
	} else {
		ck_assert_int_eq(errno, EILSEQ);
	}
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

START_TEST(test_mqtt_1_5_7_1)
{
	/* UTF-8 string pairs must be valid UTF-8. */
	uint8_t prop_buf[] = {
		8, MQTT_PROP_USER_PROPERTY,
		0x00, 0x02, 0xC0, 0x20, /* invalid UTF-8 */
		0x00, 0x01, 'v'
	};
	const uint8_t *ptr = prop_buf;
	size_t bytes_left = sizeof(prop_buf);
	struct property (*props)[] = NULL;
	unsigned count = 0;

	errno = 0;
	ck_assert_int_eq(mqtt_test_api.parse_properties(&ptr, &bytes_left,
				&props, &count, MQTT_CP_PUBLISH), -1);
	ck_assert_int_eq(errno, EILSEQ);
}
END_TEST

START_TEST(test_mqtt_2_2_2_1)
{
	/* Property length must be 0 when no properties are present. */
	uint8_t packet[128];
	size_t packet_len;
	uint32_t prop_len = 1;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			"pl0", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(parse_connect_property_length(packet, packet_len, &prop_len), 0);
	ck_assert_uint_eq(prop_len, 0);
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

START_TEST(test_mqtt_3_12_4_1_pingresp_sent)
{
	/* Server must send PINGRESP in response to PINGREQ. */
	uint8_t buf[16];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 13;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 0, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_PINGRESP);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_14_4_1_disconnect_no_more_packets)
{
	/* After DISCONNECT, the sender must not send any more packets. */
	uint8_t buf[8];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 15;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_DISCONNECT, 0, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = recv(fds[1], buf, sizeof(buf), MSG_DONTWAIT);
	ck_assert_int_eq(rd, 0);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_14_4_2_disconnect_closes)
{
	/* After sending DISCONNECT, the sender must close the Network Connection. */
	uint8_t buf[8];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 14;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_DISCONNECT, 0, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);
	ck_assert_int_eq(client.fd, -1);

	rd = recv(fds[1], buf, sizeof(buf), MSG_DONTWAIT);
	ck_assert_int_eq(rd, 0);

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

START_TEST(test_mqtt_3_1_0_2)
{
	/* Second CONNECT is a protocol error. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 22;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c2", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.protocol_version = 1;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert(reason == MQTT_MALFORMED_PACKET ||
			reason == MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_2_v3_rejected)
{
	/* v3.x protocol version uses legacy return code. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cver", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(patch_connect_version(packet, packet_len, 4), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, 0x01);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_2_v6_rejected)
{
	/* v6+ protocol version uses v5 reason code. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cver6", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(patch_connect_version(packet, packet_len, 6), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_UNSUPPORTED_PROTOCOL_VERSION);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_1_protocol_name)
{
	/* Protocol name must be "MQTT". */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cname", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(patch_connect_protocol_name(packet, packet_len, "MQTs"), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_9_will_requires_payload)
{
	/* Will flag set requires will properties/topic/payload. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_FLAG,
			"cwill", NULL, NULL);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_7_will_stored)
{
	/* Will flag set => will message stored on session. */
	uint8_t packet[256];
	size_t packet_len;
	int fds[2];
	struct client client;
	const uint8_t payload[] = { 'p' };

	packet_len = build_connect_packet_will(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_FLAG |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cwillstore", "t", payload, sizeof(payload), "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_ptr_nonnull(client.session);
	ck_assert_ptr_nonnull(client.session->will_topic);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_14_will_retain_false)
{
	/* Will Retain 0 => will message stored as non-retained. */
	uint8_t packet[256];
	size_t packet_len;
	int fds[2];
	struct client client;
	const uint8_t payload[] = { 'p' };

	packet_len = build_connect_packet_will(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_FLAG |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cwillr0", "t", payload, sizeof(payload), "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_ptr_nonnull(client.session);
	ck_assert_int_eq(client.session->will_retain, false);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_15_will_retain_true)
{
	/* Will Retain 1 => will message stored as retained. */
	uint8_t packet[256];
	size_t packet_len;
	int fds[2];
	struct client client;
	const uint8_t payload[] = { 'p' };

	packet_len = build_connect_packet_will(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_FLAG |
			MQTT_CONNECT_FLAG_WILL_RETAIN |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cwillr1", "t", payload, sizeof(payload), "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_ptr_nonnull(client.session);
	ck_assert_int_eq(client.session->will_retain, true);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_1_connect_payload_order)
{
	/* CONNECT payload fields must be in order: client id, will, username, password. */
	uint8_t packet[128];
	uint8_t payload[128];
	uint8_t *ptr = payload;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t payload_len;
	size_t remaining;
	int fds[2];
	struct client client;
	uint8_t reason = 0;

	tmp16 = htons(4);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, "MQTT", 4);
	ptr += 4;
	*ptr++ = 5;
	*ptr++ = MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD;
	tmp16 = htons(0);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	*ptr++ = 0; /* properties length */

	tmp16 = htons(3);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, "cid", 3);
	ptr += 3;

	/* Password first (invalid order); use invalid UTF-8 to force failure. */
	tmp16 = htons(1);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	*ptr++ = 0xC0;

	tmp16 = htons(1);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	*ptr++ = 'u';

	payload_len = (size_t)(ptr - payload);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	ck_assert_int_le((int)remaining, (int)sizeof(packet));

	packet[0] = (uint8_t)(MQTT_CP_CONNECT << 4);
	memcpy(packet + 1, remlen, (size_t)remlen_len);
	memcpy(packet + 1 + remlen_len, payload, payload_len);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, remaining), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_11_will_topic_utf8)
{
	/* Will Topic must be a UTF-8 string. */
	uint8_t packet[256];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	const uint8_t payload[] = { 'p' };
	const uint8_t bad_topic[] = { 0xC0, 0x00 };

	packet_len = build_connect_packet_will(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_FLAG |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cwillbad", (const char *)bad_topic, payload, sizeof(payload), "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_4_5_connack_success)
{
	/* Server must respond with CONNACK Success for valid CONNECT. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"csuccess", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_SUCCESS);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_0_1_connack_first_packet)
{
	/* CONNACK must be sent before any other packet. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cfirst", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_CONNACK);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_0_2_single_connack)
{
	/* Server must not send more than one CONNACK. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"csingle", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_CONNACK);
	assert_no_extra_packet(fds[1]);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_0_2_second_connect_no_extra_connack)
{
	/* Second CONNECT should not produce another CONNACK. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"csecond", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);
	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_SUCCESS);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	{
		uint8_t buf[16];
		ssize_t rd;

		errno = 0;
		rd = recv(fds[1], buf, sizeof(buf), MSG_DONTWAIT);
		if (rd > 0) {
			ck_assert_int_ne(buf[0] >> 4, MQTT_CP_CONNACK);
		} else if (rd == 0) {
			/* closed */
		} else {
			ck_assert(errno == EAGAIN || errno == EWOULDBLOCK);
		}
	}

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_2_1_connack_flags_reserved)
{
	/* CONNACK flags reserved bits must be 0. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0xFF;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_CLEAN_START |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cflags", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & 0xFE, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_2_7_error_reason_closes)
{
	/* Reason Code >= 0x80 => server closes connection. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_RESERVED |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cclose", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_ge(reason, 0x80);
	ck_assert_int_eq(client.state, CS_CLOSING);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_2_7_error_reason_sends_connack_then_closes)
{
	/* Reason Code >= 0x80 => CONNACK then close connection. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	ssize_t rd;
	uint8_t buf[8];

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_RESERVED |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cclose2", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_ge(reason, 0x80);

	errno = 0;
	rd = recv(fds[1], buf, sizeof(buf), MSG_DONTWAIT);
	if (rd == -1) {
		ck_assert(errno == EAGAIN || errno == EWOULDBLOCK);
	} else if (rd != 0) {
		ck_assert_msg(0, "unexpected extra bytes after CONNACK");
	}

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_30_auth_method_gates_packets)
{
	/* If CONNECT includes Authentication Method, client must send only AUTH/DISCONNECT until CONNACK. */
	uint8_t packet[256];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct property props[1];
	uint8_t buf[16];
	ssize_t rd;

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_AUTHENTICATION_METHOD;
	props[0].utf8_string = (unsigned char *)"x";

	packet_len = build_connect_packet_props(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			props, 1, "authgate", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &(uint8_t){0}), 0);

	(void)mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 0, NULL, 0);
	(void)mqtt_test_drive_parse(&client);
	errno = 0;
	rd = recv(fds[1], buf, sizeof(buf), MSG_DONTWAIT);
	if (rd > 0) {
		uint8_t type = buf[0] >> 4;
		ck_assert(type == MQTT_CP_DISCONNECT || type == MQTT_CP_PINGRESP);
	} else if (rd == 0) {
		/* closed */
	} else if (rd == -1) {
		ck_assert(errno == EAGAIN || errno == EWOULDBLOCK);
	} else {
		ck_assert_msg(0, "unexpected recv result: rd=%zd errno=%d", rd, errno);
	}

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST
START_TEST(test_mqtt_3_2_2_8_connack_reason_code)
{
	/* CONNACK reason code must be a Connect Reason Code value. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_RESERVED |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"ccode", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(is_valid_connack_reason(reason), 1);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_3_2_1_topic_name_utf8)
{
	/* Topic Name must be UTF-8. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;
	const uint8_t bad_topic[] = { 0xC0, 0x00 };

	memset(&session, 0, sizeof(session));
	session.id = 60;

	packet_len = build_publish_packet(packet, sizeof(packet), 0,
			bad_topic, NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_3_2_2_topic_name_no_wildcards)
{
	/* Topic Name must not contain wildcards. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 61;

	packet_len = build_publish_packet(packet, sizeof(packet), 0,
			(const uint8_t *)"a/+/b", NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert(client.disconnect_reason == MQTT_TOPIC_NAME_INVALID ||
			client.disconnect_reason == MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_3_2_13_response_topic_utf8)
{
	/* Response Topic must be UTF-8. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;
	struct property props[1];
	const uint8_t bad_topic[] = { 0xC0, 0x00 };

	memset(&session, 0, sizeof(session));
	session.id = 62;
	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_RESPONSE_TOPIC;
	props[0].utf8_string = (unsigned char *)bad_topic;

	packet_len = build_publish_packet_props(packet, sizeof(packet), 0,
			(const uint8_t *)"a", props, 1, NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_3_2_14_response_topic_no_wildcards)
{
	/* Response Topic must not contain wildcards. */
	const uint8_t name[] = "a/+/b";

	ck_assert_int_eq(mqtt_test_api.is_valid_topic_name(name), -1);
}
END_TEST

START_TEST(test_mqtt_3_3_2_19_content_type_utf8)
{
	/* Content Type must be UTF-8. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;
	struct property props[1];
	const uint8_t bad_content_type[] = { 0xC0, 0x00 };

	memset(&session, 0, sizeof(session));
	session.id = 63;
	ck_assert_int_eq(pthread_rwlock_init(&session.subscriptions_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&session.delivery_states_lock, NULL), 0);
	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_CONTENT_TYPE;
	props[0].utf8_string = (unsigned char *)bad_content_type;

	packet_len = build_publish_packet_props(packet, sizeof(packet), 0,
			(const uint8_t *)"a", props, 1, NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
	pthread_rwlock_destroy(&session.subscriptions_lock);
	pthread_rwlock_destroy(&session.delivery_states_lock);
}
END_TEST

START_TEST(test_mqtt_3_3_4_1_publish_qos1_puback)
{
	/* QoS 1 PUBLISH must be acknowledged with PUBACK. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session session;
	uint16_t packet_id = 9;
	uint16_t parsed_id = 0;

	memset(&session, 0, sizeof(session));
	session.id = 64;
	session.client_id = (const uint8_t *)"sess-puback";
	ck_assert_int_eq(pthread_rwlock_init(&session.subscriptions_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&session.delivery_states_lock, NULL), 0);

	packet_len = build_publish_packet(packet, sizeof(packet),
			MQTT_FLAG_PUBLISH_QOS1, (const uint8_t *)"a",
			NULL, 0, packet_id);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_PUBACK);
	ck_assert_int_eq(mqtt_test_read_packet_id(buf, (size_t)rd, &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);
	assert_no_extra_packet(fds[1]);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
	pthread_rwlock_destroy(&session.subscriptions_lock);
	pthread_rwlock_destroy(&session.delivery_states_lock);
}
END_TEST

START_TEST(test_mqtt_3_3_1_1_dup_on_redelivery)
{
	/* Server must set DUP on re-delivery of QoS>0 PUBLISH. */
	uint8_t sub_packet[128];
	size_t sub_len;
	uint8_t pub_packet[128];
	size_t pub_len;
	uint8_t buf[256];
	ssize_t rd;
	uint8_t flags = 0;
	uint16_t packet_id = 0;
	int sub_fds[2];
	int pub_fds[2];
	struct client sub_client;
	struct client pub_client;
	struct session *sub_session;
	struct session *pub_session;
	uint8_t puback_payload[2];
	uint16_t tmp;

	sub_session = create_session("sub-dup");
	ck_assert_ptr_nonnull(sub_session);
	pub_session = create_session("pub-dup");
	ck_assert_ptr_nonnull(pub_session);

	sub_len = build_subscribe_packet(sub_packet, sizeof(sub_packet), 1,
			(const uint8_t *)"dup/t", 0);
	ck_assert_int_gt((int)sub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, sub_fds), 0);
	mqtt_test_init_client(&sub_client, sub_fds[0], sub_session);
	sub_client.is_auth = true;
	sub_session->client = &sub_client;

	ck_assert_int_ne(write(sub_fds[1], sub_packet, sub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	pub_len = build_publish_packet(pub_packet, sizeof(pub_packet),
			MQTT_FLAG_PUBLISH_QOS1, (const uint8_t *)"dup/t",
			(const uint8_t *)"hi", 2, 7);
	ck_assert_int_gt((int)pub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, pub_fds), 0);
	mqtt_test_init_client(&pub_client, pub_fds[0], pub_session);
	pub_client.is_auth = true;

	ck_assert_int_ne(write(pub_fds[1], pub_packet, pub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&pub_client), 0);

	ck_assert_ptr_nonnull(mqtt_test_api.tick);
	mqtt_test_api.tick();

	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(parse_publish_packet(buf, (size_t)rd, &flags, &packet_id), 0);
	ck_assert_int_eq(flags & MQTT_FLAG_PUBLISH_DUP, 0);

	mqtt_test_sleep_ms(2100);
	mqtt_test_api.tick();

	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(parse_publish_packet(buf, (size_t)rd, &flags, &packet_id), 0);
	ck_assert_int_ne(flags & MQTT_FLAG_PUBLISH_DUP, 0);

	tmp = htons(packet_id);
	memcpy(puback_payload, &tmp, sizeof(tmp));
	ck_assert_int_ne(mqtt_test_write_packet(sub_fds[1], MQTT_CP_PUBACK, 0,
				puback_payload, sizeof(puback_payload)), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	mqtt_test_api.tick();

	close(sub_fds[1]);
	close(sub_fds[0]);
	close(pub_fds[1]);
	close(pub_fds[0]);
	mqtt_test_cleanup_client(&sub_client);
	mqtt_test_cleanup_client(&pub_client);

	sub_session->client = NULL;
	pub_session->client = NULL;
	mqtt_test_api.free_session(sub_session, true);
	mqtt_test_api.free_session(pub_session, true);
}
END_TEST

START_TEST(test_mqtt_2_2_1_4_server_publish_packet_id_nonzero)
{
	/* Server QoS>0 PUBLISH must include a non-zero Packet Identifier. */
	uint8_t sub_packet[128];
	size_t sub_len;
	uint8_t pub_packet[128];
	size_t pub_len;
	uint8_t buf[256];
	ssize_t rd;
	uint16_t packet_id = 0;
	int sub_fds[2];
	int pub_fds[2];
	struct client sub_client;
	struct client pub_client;
	struct session *sub_session;
	struct session *pub_session;

	sub_session = create_session("sub-pid");
	ck_assert_ptr_nonnull(sub_session);
	pub_session = create_session("pub-pid");
	ck_assert_ptr_nonnull(pub_session);

	sub_len = build_subscribe_packet(sub_packet, sizeof(sub_packet), 1,
			(const uint8_t *)"pid/t", 0);
	ck_assert_int_gt((int)sub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, sub_fds), 0);
	mqtt_test_init_client(&sub_client, sub_fds[0], sub_session);
	sub_client.is_auth = true;
	sub_session->client = &sub_client;

	ck_assert_int_ne(write(sub_fds[1], sub_packet, sub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	pub_len = build_publish_packet(pub_packet, sizeof(pub_packet),
			MQTT_FLAG_PUBLISH_QOS1, (const uint8_t *)"pid/t",
			(const uint8_t *)"hi", 2, 5);
	ck_assert_int_gt((int)pub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, pub_fds), 0);
	mqtt_test_init_client(&pub_client, pub_fds[0], pub_session);
	pub_client.is_auth = true;

	ck_assert_int_ne(write(pub_fds[1], pub_packet, pub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&pub_client), 0);

	ck_assert_ptr_nonnull(mqtt_test_api.tick);
	mqtt_test_api.tick();

	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(parse_publish_packet(buf, (size_t)rd, NULL, &packet_id), 0);
	ck_assert_uint_ne(packet_id, 0);

	close(sub_fds[1]);
	close(sub_fds[0]);
	close(pub_fds[1]);
	close(pub_fds[0]);
	mqtt_test_cleanup_client(&sub_client);
	mqtt_test_cleanup_client(&pub_client);
	sub_session->client = NULL;
	pub_session->client = NULL;
	mqtt_test_api.free_session(sub_session, true);
	mqtt_test_api.free_session(pub_session, true);
}
END_TEST

START_TEST(test_mqtt_3_3_1_2_qos0_dup_clear)
{
	/* QoS 0 PUBLISH must have DUP=0. */
	uint8_t sub_packet[128];
	size_t sub_len;
	uint8_t pub_packet[128];
	size_t pub_len;
	uint8_t buf[256];
	ssize_t rd;
	uint8_t flags = 0;
	int sub_fds[2];
	int pub_fds[2];
	struct client sub_client;
	struct client pub_client;
	struct session *sub_session;
	struct session *pub_session;

	sub_session = create_session("sub-qos0");
	ck_assert_ptr_nonnull(sub_session);
	pub_session = create_session("pub-qos0");
	ck_assert_ptr_nonnull(pub_session);

	sub_len = build_subscribe_packet(sub_packet, sizeof(sub_packet), 1,
			(const uint8_t *)"qos0/t", 0);
	ck_assert_int_gt((int)sub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, sub_fds), 0);
	mqtt_test_init_client(&sub_client, sub_fds[0], sub_session);
	sub_client.is_auth = true;
	sub_session->client = &sub_client;

	ck_assert_int_ne(write(sub_fds[1], sub_packet, sub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	pub_len = build_publish_packet(pub_packet, sizeof(pub_packet),
			0, (const uint8_t *)"qos0/t",
			(const uint8_t *)"hi", 2, 0);
	ck_assert_int_gt((int)pub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, pub_fds), 0);
	mqtt_test_init_client(&pub_client, pub_fds[0], pub_session);
	pub_client.is_auth = true;

	ck_assert_int_ne(write(pub_fds[1], pub_packet, pub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&pub_client), 0);

	ck_assert_ptr_nonnull(mqtt_test_api.tick);
	mqtt_test_api.tick();

	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(read_publish_flags(buf, (size_t)rd, &flags), 0);
	ck_assert_int_eq(GET_QOS(flags), 0);
	ck_assert_int_eq(flags & MQTT_FLAG_PUBLISH_DUP, 0);

	close(sub_fds[1]);
	close(sub_fds[0]);
	close(pub_fds[1]);
	close(pub_fds[0]);
	mqtt_test_cleanup_client(&sub_client);
	mqtt_test_cleanup_client(&pub_client);
	sub_session->client = NULL;
	pub_session->client = NULL;
	mqtt_test_api.free_session(sub_session, true);
	mqtt_test_api.free_session(pub_session, true);
}
END_TEST

START_TEST(test_mqtt_3_8_3_1_subscribe_utf8_invalid)
{
	/* Topic Filter must be UTF-8. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;
	const uint8_t bad_filter[] = { 0xC0, 0x00 };

	memset(&session, 0, sizeof(session));
	session.id = 70;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 1,
			bad_filter, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_8_4_1_suback_sent)
{
	/* Server must respond with SUBACK. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("suback");
	ck_assert_ptr_nonnull(session);

	packet_len = build_subscribe_packet(packet, sizeof(packet), 7,
			(const uint8_t *)"a/b", 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_8_4_6_suback_reason_codes)
{
	/* SUBACK must include a reason code per topic filter. */
	uint8_t packet[256];
	uint8_t payload[128];
	uint8_t *ptr = payload;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t payload_len;
	size_t remaining;
	uint8_t buf[128];
	ssize_t rd;
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;
	const uint8_t *pptr;
	size_t bytes_left;
	uint32_t prop_len;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("suback2");
	ck_assert_ptr_nonnull(session);

	tmp16 = htons(9);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	*ptr++ = 0; /* properties length */

	tmp16 = htons(3);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, "a/+", 3);
	ptr += 3;
	*ptr++ = 0;

	tmp16 = htons(3);
	memcpy(ptr, &tmp16, sizeof(tmp16));
	ptr += sizeof(tmp16);
	memcpy(ptr, "b/+", 3);
	ptr += 3;
	*ptr++ = 0;

	payload_len = (size_t)(ptr - payload);
	remlen_len = mqtt_test_api.encode_var_byte((uint32_t)payload_len, remlen);
	remaining = 1 + (size_t)remlen_len + payload_len;
	ck_assert_int_le((int)remaining, (int)sizeof(packet));

	packet[0] = (uint8_t)(MQTT_CP_SUBSCRIBE << 4) | MQTT_FLAG_SUBSCRIBE;
	memcpy(packet + 1, remlen, (size_t)remlen_len);
	memcpy(packet + 1 + remlen_len, payload, payload_len);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, remaining), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);
	ck_assert_int_eq(mqtt_test_decode_remaining_length(buf + 1, (size_t)rd - 1,
				&rl_len, &rl_consumed), 0);
	offset = 1 + rl_consumed;
	ck_assert_int_le(offset + 2, (size_t)rd);
	pptr = buf + offset + 2;
	bytes_left = (size_t)rd - (offset + 2);
	errno = 0;
	prop_len = mqtt_test_api.read_var_byte(&pptr, &bytes_left);
	ck_assert(!(prop_len == 0 && errno));
	ck_assert_int_le((size_t)prop_len, bytes_left);
	pptr += prop_len;
	bytes_left -= prop_len;
	ck_assert_uint_eq(bytes_left, 2);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_8_4_2_suback_packet_id)
{
	/* SUBACK must echo the SUBSCRIBE Packet Identifier. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	uint16_t packet_id = 55;
	uint16_t parsed_id = 0;
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("suback-id");
	ck_assert_ptr_nonnull(session);

	packet_len = build_subscribe_packet(packet, sizeof(packet), packet_id,
			(const uint8_t *)"a/b", 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);
	ck_assert_int_eq(mqtt_test_read_packet_id(buf, (size_t)rd, &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_8_4_3_subscribe_replaces_existing)
{
	/* Identical non-shared Topic Filters replace the existing subscription. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session *session;
	struct subscription *sub;

	session = create_session("sub-replace");
	ck_assert_ptr_nonnull(session);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 19,
			(const uint8_t *)"a/b", MQTT_SUBOPT_QOS0);
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	ck_assert_uint_eq(session->num_subscriptions, 1);
	ck_assert_ptr_nonnull(session->subscriptions);
	sub = session->subscriptions[0];
	ck_assert_ptr_nonnull(sub);
	ck_assert_uint_eq(sub->option & MQTT_SUBOPT_QOS_MASK, MQTT_SUBOPT_QOS0);

	packet_len = build_subscribe_packet(packet, sizeof(packet), 20,
			(const uint8_t *)"a/b", MQTT_SUBOPT_QOS1);
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	ck_assert_uint_eq(session->num_subscriptions, 1);
	ck_assert_ptr_eq(session->subscriptions[0], sub);
	ck_assert_uint_eq(sub->option & MQTT_SUBOPT_QOS_MASK, MQTT_SUBOPT_QOS1);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_9_3_1_suback_reason_code_order)
{
	/* SUBACK reason codes must match SUBSCRIBE order and be valid. */
	uint8_t packet[256];
	size_t packet_len;
	uint8_t buf[128];
	ssize_t rd;
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;
	const uint8_t *pptr;
	size_t bytes_left;
	uint32_t prop_len;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("suback-order");
	ck_assert_ptr_nonnull(session);

	packet_len = build_subscribe_packet_two_filters(packet, sizeof(packet), 22,
			"a/+", 0, "a/#/b", 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);
	ck_assert_int_eq(mqtt_test_decode_remaining_length(buf + 1, (size_t)rd - 1,
				&rl_len, &rl_consumed), 0);
	offset = 1 + rl_consumed;
	ck_assert_int_le(offset + 2, (size_t)rd);
	pptr = buf + offset + 2;
	bytes_left = (size_t)rd - (offset + 2);
	errno = 0;
	prop_len = mqtt_test_api.read_var_byte(&pptr, &bytes_left);
	ck_assert(!(prop_len == 0 && errno));
	ck_assert_int_le((size_t)prop_len, bytes_left);
	pptr += prop_len;
	bytes_left -= prop_len;
	ck_assert_uint_eq(bytes_left, 2);

	ck_assert_uint_eq(pptr[0], MQTT_GRANTED_QOS_0);
	ck_assert_uint_eq(pptr[1], MQTT_TOPIC_FILTER_INVALID);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_8_4_5_suback_single_response)
{
	/* Multiple filters must result in a single SUBACK response. */
	uint8_t packet[256];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("suback-single");
	ck_assert_ptr_nonnull(session);

	packet_len = build_subscribe_packet_two_filters(packet, sizeof(packet), 10,
			"a/+", 0, "b/+", 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);
	assert_no_extra_packet(fds[1]);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_10_3_1_unsubscribe_utf8_invalid)
{
	/* UNSUBSCRIBE topic filters must be UTF-8. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;
	const uint8_t bad_filter[] = { 0xC0, 0x00 };

	memset(&session, 0, sizeof(session));
	session.id = 80;

	packet_len = build_unsubscribe_packet(packet, sizeof(packet), 1,
			bad_filter, MQTT_FLAG_UNSUBSCRIBE);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_10_3_2_unsubscribe_payload_nonempty)
{
	/* UNSUBSCRIBE payload must contain at least one Topic Filter. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 81;

	packet_len = build_unsubscribe_packet_empty(packet, sizeof(packet), 1);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_10_4_4_unsuback_sent)
{
	/* Server must respond with UNSUBACK. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 82;

	packet_len = build_unsubscribe_packet(packet, sizeof(packet), 12,
			(const uint8_t *)"a/b", MQTT_FLAG_UNSUBSCRIBE);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_UNSUBACK);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_10_4_5_unsuback_packet_id)
{
	/* UNSUBACK must echo the UNSUBSCRIBE Packet Identifier. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	uint16_t packet_id = 66;
	uint16_t parsed_id = 0;
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 84;

	packet_len = build_unsubscribe_packet(packet, sizeof(packet), packet_id,
			(const uint8_t *)"a/b", MQTT_FLAG_UNSUBSCRIBE);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_UNSUBACK);
	ck_assert_int_eq(mqtt_test_read_packet_id(buf, (size_t)rd, &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_10_4_6_unsuback_single_response)
{
	/* Multiple filters must result in a single UNSUBACK response. */
	uint8_t packet[256];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 83;

	packet_len = build_unsubscribe_packet_two_filters(packet, sizeof(packet), 13,
			"a/+", "b/+");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_UNSUBACK);
	assert_no_extra_packet(fds[1]);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_10_4_2_unsubscribe_stops_new_messages)
{
	/* After UNSUBSCRIBE, server must stop adding new matching messages. */
	uint8_t sub_packet[128];
	size_t sub_len;
	uint8_t unsub_packet[128];
	size_t unsub_len;
	uint8_t pub_packet[128];
	size_t pub_len;
	uint8_t buf[256];
	ssize_t rd;
	int sub_fds[2];
	int pub_fds[2];
	struct client sub_client;
	struct client pub_client;
	struct session *sub_session;
	struct session *pub_session;
	struct session *pub_session2;

	sub_session = create_session("sub-stop");
	ck_assert_ptr_nonnull(sub_session);
	pub_session = create_session("pub-stop");
	ck_assert_ptr_nonnull(pub_session);

	sub_len = build_subscribe_packet(sub_packet, sizeof(sub_packet), 1,
			(const uint8_t *)"u/stop", MQTT_SUBOPT_QOS0);
	ck_assert_int_gt((int)sub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, sub_fds), 0);
	mqtt_test_init_client(&sub_client, sub_fds[0], sub_session);
	sub_client.is_auth = true;
	sub_session->client = &sub_client;

	ck_assert_int_ne(write(sub_fds[1], sub_packet, sub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	pub_len = build_publish_packet(pub_packet, sizeof(pub_packet),
			0, (const uint8_t *)"u/stop",
			(const uint8_t *)"one", 3, 0);
	ck_assert_int_gt((int)pub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, pub_fds), 0);
	mqtt_test_init_client(&pub_client, pub_fds[0], pub_session);
	pub_client.is_auth = true;

	ck_assert_int_ne(write(pub_fds[1], pub_packet, pub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&pub_client), 0);
	ck_assert_ptr_nonnull(mqtt_test_api.tick);
	mqtt_test_api.tick();

	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_PUBLISH);

	close(pub_fds[1]);
	close(pub_fds[0]);
	mqtt_test_cleanup_client(&pub_client);
	pub_session->client = NULL;
	mqtt_test_api.free_session(pub_session, true);
	pub_session = NULL;

	unsub_len = build_unsubscribe_packet(unsub_packet, sizeof(unsub_packet), 2,
			(const uint8_t *)"u/stop", MQTT_FLAG_UNSUBSCRIBE);
	ck_assert_int_gt((int)unsub_len, 0);
	ck_assert_int_ne(write(sub_fds[1], unsub_packet, unsub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_UNSUBACK);

	pub_session2 = create_session("pub-stop2");
	ck_assert_ptr_nonnull(pub_session2);

	pub_len = build_publish_packet(pub_packet, sizeof(pub_packet),
			0, (const uint8_t *)"u/stop",
			(const uint8_t *)"two", 3, 0);
	ck_assert_int_gt((int)pub_len, 0);
	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, pub_fds), 0);
	mqtt_test_init_client(&pub_client, pub_fds[0], pub_session2);
	pub_client.is_auth = true;
	ck_assert_int_ne(write(pub_fds[1], pub_packet, pub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&pub_client), 0);
	mqtt_test_api.tick();

	assert_no_extra_packet(sub_fds[1]);

	close(sub_fds[1]);
	close(sub_fds[0]);
	close(pub_fds[1]);
	close(pub_fds[0]);
	mqtt_test_cleanup_client(&sub_client);
	mqtt_test_cleanup_client(&pub_client);
	sub_session->client = NULL;
	mqtt_test_api.free_session(sub_session, true);
	pub_session2->client = NULL;
	mqtt_test_api.free_session(pub_session2, true);
}
END_TEST

START_TEST(test_mqtt_3_10_4_3_unsubscribe_completes_inflight_qos1)
{
	/* In-flight QoS1 delivery must still complete after UNSUBSCRIBE. */
	uint8_t sub_packet[128];
	size_t sub_len;
	uint8_t unsub_packet[128];
	size_t unsub_len;
	uint8_t pub_packet[128];
	size_t pub_len;
	uint8_t buf[256];
	uint8_t puback_payload[2];
	ssize_t rd;
	uint16_t packet_id = 0;
	uint16_t tmp;
	int sub_fds[2];
	int pub_fds[2];
	struct client sub_client;
	struct client pub_client;
	struct session *sub_session;
	struct session *pub_session;

	sub_session = create_session("sub-inflight");
	ck_assert_ptr_nonnull(sub_session);
	pub_session = create_session("pub-inflight");
	ck_assert_ptr_nonnull(pub_session);

	sub_len = build_subscribe_packet(sub_packet, sizeof(sub_packet), 1,
			(const uint8_t *)"u/inflight", MQTT_SUBOPT_QOS1);
	ck_assert_int_gt((int)sub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, sub_fds), 0);
	mqtt_test_init_client(&sub_client, sub_fds[0], sub_session);
	sub_client.is_auth = true;
	sub_session->client = &sub_client;

	ck_assert_int_ne(write(sub_fds[1], sub_packet, sub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	pub_len = build_publish_packet(pub_packet, sizeof(pub_packet),
			MQTT_FLAG_PUBLISH_QOS1, (const uint8_t *)"u/inflight",
			(const uint8_t *)"hi", 2, 11);
	ck_assert_int_gt((int)pub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, pub_fds), 0);
	mqtt_test_init_client(&pub_client, pub_fds[0], pub_session);
	pub_client.is_auth = true;

	ck_assert_int_ne(write(pub_fds[1], pub_packet, pub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&pub_client), 0);
	ck_assert_ptr_nonnull(mqtt_test_api.tick);
	mqtt_test_api.tick();

	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(parse_publish_packet(buf, (size_t)rd, NULL, &packet_id), 0);
	ck_assert_uint_ne(packet_id, 0);

	unsub_len = build_unsubscribe_packet(unsub_packet, sizeof(unsub_packet), 2,
			(const uint8_t *)"u/inflight", MQTT_FLAG_UNSUBSCRIBE);
	ck_assert_int_gt((int)unsub_len, 0);
	ck_assert_int_ne(write(sub_fds[1], unsub_packet, unsub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	rd = mqtt_test_read_packet(sub_fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_UNSUBACK);

	tmp = htons(packet_id);
	memcpy(puback_payload, &tmp, sizeof(tmp));
	ck_assert_int_ne(mqtt_test_write_packet(sub_fds[1], MQTT_CP_PUBACK, 0,
				puback_payload, sizeof(puback_payload)), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	ck_assert_int_ne(sub_client.state, CS_CLOSING);

	close(sub_fds[1]);
	close(sub_fds[0]);
	close(pub_fds[1]);
	close(pub_fds[0]);
	mqtt_test_cleanup_client(&sub_client);
	mqtt_test_cleanup_client(&pub_client);
	sub_session->client = NULL;
	pub_session->client = NULL;
	mqtt_test_api.free_session(sub_session, true);
	mqtt_test_api.free_session(pub_session, true);
}
END_TEST

START_TEST(test_mqtt_3_10_4_1_unsubscribe_deletes_subscription)
{
	/* Matching Topic Filters must delete the subscription. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t buf[64];
	ssize_t rd;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("unsub-del");
	ck_assert_ptr_nonnull(session);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 21,
			(const uint8_t *)"a/b", 0);
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);
	ck_assert_uint_eq(session->num_subscriptions, 1);

	packet_len = build_unsubscribe_packet(packet, sizeof(packet), 22,
			(const uint8_t *)"a/b", MQTT_FLAG_UNSUBSCRIBE);
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_UNSUBACK);

	ck_assert_uint_eq(session->num_subscriptions, 0);
	ck_assert_ptr_null(session->subscriptions);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_11_3_1_unsuback_reason_code_order)
{
	/* UNSUBACK reason codes must match UNSUBSCRIBE order and be valid. */
	uint8_t packet[256];
	size_t packet_len;
	uint8_t buf[128];
	ssize_t rd;
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	size_t offset;
	const uint8_t *pptr;
	size_t bytes_left;
	uint32_t prop_len;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("unsub-order");
	ck_assert_ptr_nonnull(session);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	packet_len = build_subscribe_packet(packet, sizeof(packet), 23,
			(const uint8_t *)"a/+", 0);
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);
	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_SUBACK);

	packet_len = build_unsubscribe_packet_two_filters(packet, sizeof(packet), 24,
			"a/+", "b/+");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq(buf[0] >> 4, MQTT_CP_UNSUBACK);
	ck_assert_int_eq(mqtt_test_decode_remaining_length(buf + 1, (size_t)rd - 1,
				&rl_len, &rl_consumed), 0);
	offset = 1 + rl_consumed;
	ck_assert_int_le(offset + 2, (size_t)rd);
	pptr = buf + offset + 2;
	bytes_left = (size_t)rd - (offset + 2);
	errno = 0;
	prop_len = mqtt_test_api.read_var_byte(&pptr, &bytes_left);
	ck_assert(!(prop_len == 0 && errno));
	ck_assert_int_le((size_t)prop_len, bytes_left);
	pptr += prop_len;
	bytes_left -= prop_len;
	ck_assert_uint_eq(bytes_left, 2);

	ck_assert_uint_eq(pptr[0], MQTT_SUCCESS);
	ck_assert_uint_eq(pptr[1], MQTT_NO_SUBSCRIPTION_EXISTED);
	ck_assert(pptr[0] == MQTT_SUCCESS || pptr[0] == MQTT_NO_SUBSCRIPTION_EXISTED);
	ck_assert(pptr[1] == MQTT_SUCCESS || pptr[1] == MQTT_NO_SUBSCRIPTION_EXISTED);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST
START_TEST(test_mqtt_3_1_2_11_will_qos_requires_will_flag)
{
	/* Will flag 0 => Will QoS must be 0. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_QOS1 |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cwillq0", "u", "p");
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

START_TEST(test_mqtt_3_1_2_12_will_qos_range)
{
	/* Will QoS must be 0,1,2 when Will Flag set. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	const uint8_t payload[] = { 'p' };

	packet_len = build_connect_packet_will(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_FLAG |
			MQTT_CONNECT_FLAG_WILL_QOS_MASK |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cwillq3", "t", payload, sizeof(payload), "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_13_will_retain_requires_will_flag)
{
	/* Will flag 0 => Will Retain must be 0. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_WILL_RETAIN |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"cwillr0", "u", "p");
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

START_TEST(test_mqtt_3_1_2_16_username_flag_unset)
{
	/* If Username Flag is 0, User Name must not be present. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME,
			"cuser", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(patch_connect_flags(packet, packet_len, 0), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_17_username_flag_set_requires_username)
{
	/* If Username Flag is 1, User Name must be present. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	const char *username = "user";
	size_t trim = sizeof(uint16_t) + strlen(username);

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME,
			"cuser2", username, "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(trim_packet_remaining_length(packet, &packet_len, trim), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_18_password_flag_unset)
{
	/* If Password Flag is 0, Password must not be present. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			"cpass", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(patch_connect_flags(packet, packet_len,
				MQTT_CONNECT_FLAG_USERNAME), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_2_19_password_flag_set_requires_password)
{
	/* If Password Flag is 1, Password must be present. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	const char *password = "pw";
	size_t trim = sizeof(uint16_t) + strlen(password);

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			"cpass2", "u", password);
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(trim_packet_remaining_length(packet, &packet_len, trim), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_5_client_id_allowed)
{
	/* Server MUST allow 1-23 byte alnum Client IDs. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"Client01", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_SUCCESS);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_8_invalid_client_id)
{
	/* Server MAY reject invalid Client ID and close connection. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"client-01", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_CLIENT_IDENTIFIER_NOT_VALID);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_6_zero_length_client_id)
{
	/* Zero-length Client ID rejected by broker. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_CLIENT_IDENTIFIER_NOT_VALID);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_3_client_id_missing)
{
	/* Client ID must be present as first payload field. */
	uint8_t packet[128];
	size_t packet_len;
	size_t rl_len = 0;
	size_t rl_consumed = 0;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	const size_t min_remaining = 2 + 4 + 1 + 1 + 2 + 1;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			"cid", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);
	ck_assert_int_eq(mqtt_test_decode_remaining_length(packet + 1,
				packet_len - 1, &rl_len, &rl_consumed), 0);
	ck_assert_uint_gt(rl_len, min_remaining);
	ck_assert_int_eq(trim_packet_remaining_length(packet, &packet_len,
				rl_len - min_remaining), 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_4_client_id_utf8_invalid)
{
	/* Client ID must be a UTF-8 Encoded String. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	char bad_id[] = { (char)0xC0, (char)0xAF, 0x00 };

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			bad_id, "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_3_12_username_utf8_invalid)
{
	/* User Name must be a UTF-8 Encoded String when present. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;
	char bad_user[] = { (char)0xC0, (char)0xAF, 0x00 };

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			"cuserutf8", bad_user, "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_4_1_connect_too_short)
{
	/* Server must reject malformed CONNECT packets. */
	uint8_t payload[1] = { 0x00 };
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_CONNECT, 0,
				payload, sizeof(payload)), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_4_2_auth_required)
{
	/* Auth failures must close the connection. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME,
			"cauth", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_eq(reason, MQTT_BAD_USER_NAME_OR_PASSWORD);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_1_4_6_reject_ignores_followup)
{
	/* After rejecting CONNECT, server must not process further packets (except AUTH). */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t reason = 0;
	int fds[2];
	struct client client;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_RESERVED |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"creject", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_reason(fds[1], &reason), 0);
	ck_assert_int_ne(reason, MQTT_SUCCESS);

	(void)mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 0, NULL, 0);
	(void)mqtt_test_drive_parse(&client);
	assert_no_packet_or_closed(fds[1]);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_2_2_6_error_clears_session_present)
{
	/* Non-zero CONNACK reason must have Session Present = 0. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0xFF;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 30;

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_RESERVED |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"csp", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_eq(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_3_2_1_topic_name_required)
{
	/* Topic Name must be present in PUBLISH. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 50;

	packet_len = build_publish_packet(packet, sizeof(packet), 0, NULL, NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_3_2_8_topic_alias_zero)
{
	/* Topic Alias value of 0 is invalid. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct property connect_props[1];
	struct property publish_props[1];

	memset(connect_props, 0, sizeof(connect_props));
	connect_props[0].ident = MQTT_PROP_TOPIC_ALIAS_MAXIMUM;
	connect_props[0].byte2 = 1;

	packet_len = build_connect_packet_props(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME | MQTT_CONNECT_FLAG_PASSWORD,
			connect_props, 1, "ca0", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	memset(publish_props, 0, sizeof(publish_props));
	publish_props[0].ident = MQTT_PROP_TOPIC_ALIAS;
	publish_props[0].byte2 = 0;
	packet_len = build_publish_packet_props(packet, sizeof(packet), 0,
			(const uint8_t *)"a", publish_props, 1, NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);

	ck_assert(client.disconnect_reason == MQTT_TOPIC_ALIAS_INVALID ||
			client.disconnect_reason == MQTT_MALFORMED_PACKET);

	if (client.clnt_topic_aliases) {
		for (unsigned idx = 0; idx < client.topic_alias_maximum; idx++) {
			if (client.clnt_topic_aliases[idx]) {
				free((void *)client.clnt_topic_aliases[idx]);
				client.clnt_topic_aliases[idx] = NULL;
			}
		}
		free((void *)client.clnt_topic_aliases);
		client.clnt_topic_aliases = NULL;
	}

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

START_TEST(test_mqtt_3_2_2_2_clean_start_existing_session)
{
	/* Clean Start = 1 with existing session => Session Present must be 0. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0xFF;
	int fds[2];
	struct client client;

	ck_assert_ptr_nonnull(create_session("c8"));

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_CLEAN_START |
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c8", "u", "p");
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

START_TEST(test_mqtt_3_2_2_3_existing_session_present)
{
	/* Clean Start = 0 with existing session => Session Present must be 1. */
	uint8_t packet[128];
	size_t packet_len;
	uint8_t flags = 0x00;
	int fds[2];
	struct client client;

	ck_assert_ptr_nonnull(create_session("c7"));

	packet_len = build_connect_packet(packet, sizeof(packet),
			MQTT_CONNECT_FLAG_USERNAME |
			MQTT_CONNECT_FLAG_PASSWORD,
			"c7", "u", "p");
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_eq(read_connack_flags(fds[1], &flags), 0);
	ck_assert_int_ne(flags & MQTT_CONNACK_FLAG_SESSION_PRESENT, 0);

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
	struct session *session;
	uint8_t sub_packet[128];
	size_t sub_len;
	int sub_fds[2];
	struct client sub_client;

	session = create_session("c5");
	ck_assert_ptr_nonnull(session);

	sub_len = build_subscribe_packet(sub_packet, sizeof(sub_packet), 1,
			(const uint8_t *)"a/b", 0);
	ck_assert_int_gt((int)sub_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, sub_fds), 0);
	mqtt_test_init_client(&sub_client, sub_fds[0], session);
	sub_client.is_auth = true;

	ck_assert_int_ne(write(sub_fds[1], sub_packet, sub_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&sub_client), 0);
	ck_assert_int_gt((int)session->num_subscriptions, 0);

	close(sub_fds[1]);
	close(sub_fds[0]);
	mqtt_test_cleanup_client(&sub_client);

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
	ck_assert_int_eq(session->num_subscriptions, 0);
	ck_assert_int_eq(session->state, SESSION_DELETE);
	ck_assert_ptr_nonnull(client.session);
	ck_assert_int_eq(client.session->num_subscriptions, 0);

	close(fds[1]);
	close(fds[0]);
	if (client.session) {
		mqtt_test_api.free_session(client.session, true);
		client.session = NULL;
	}
	mqtt_test_cleanup_client(&client);
	mqtt_test_api.free_session(session, true);
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

START_TEST(test_mqtt_3_6_1_1_pubrel_flags_invalid)
{
	/* PUBREL flags must be 0x2; others are malformed. */
	uint8_t packet[64];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 34;

	packet_len = build_pubrel_packet(packet, sizeof(packet), 0x0, 1);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

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

START_TEST(test_mqtt_2_1_3_1_unsubscribe_flags)
{
	/* Reserved flags must match UNSUBSCRIBE requirements. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("unsub1");
	ck_assert_ptr_nonnull(session);

	packet_len = build_unsubscribe_packet(packet, sizeof(packet), 1,
			(const uint8_t *)"t/1", 0x00);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_2_2_1_3_unsubscribe_packet_id_zero)
{
	/* Packet Identifier must be non-zero for UNSUBSCRIBE. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("unsub2");
	ck_assert_ptr_nonnull(session);

	packet_len = build_unsubscribe_packet(packet, sizeof(packet), 0,
			(const uint8_t *)"t/2", MQTT_FLAG_UNSUBSCRIBE);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_PROTOCOL_ERROR);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_8_3_2)
{
	/* SUBSCRIBE must include at least one topic filter. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("sub0");
	ck_assert_ptr_nonnull(session);

	packet_len = build_subscribe_packet_empty(packet, sizeof(packet), 1);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_mqtt_3_8_1_1_subscribe_flags)
{
	/* SUBSCRIBE fixed header flags must be 0x2. */
	uint8_t packet[128];
	size_t packet_len;
	int fds[2];
	struct client client;
	struct session *session;

	session = create_session("subflags");
	ck_assert_ptr_nonnull(session);

	packet_len = build_subscribe_packet(packet, sizeof(packet), 1,
			(const uint8_t *)"a/b", 0);
	ck_assert_int_gt((int)packet_len, 0);
	packet[0] &= 0xF0;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_api.free_session(session, true);
	mqtt_test_cleanup_client(&client);
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



static void init_req_map(void)
{
	size_t idx = 0;
#define MQTT_REQ_INIT(id, fn) do { g_req_map[idx].test = fn; idx++; } while (0);
	MQTT_REQ_MAP(MQTT_REQ_INIT)
#undef MQTT_REQ_INIT
	if (idx != g_req_map_count) {
		fprintf(stderr, "internal error: req map size mismatch %zu/%zu\n",
				idx, g_req_map_count);
		abort();
	}

	idx = 0;
#define MQTT_POLICY_INIT(id, fn) do { g_policy_map[idx].test = fn; idx++; } while (0);
	MQTT_POLICY_MAP(MQTT_POLICY_INIT)
#undef MQTT_POLICY_INIT
	if (idx != g_policy_map_count) {
		fprintf(stderr, "internal error: policy map size mismatch %zu/%zu\n",
				idx, g_policy_map_count);
		abort();
	}
}

static Suite *mqtt_conformance_suite(void)
{
	Suite *s = suite_create("mqtt_conformance");
	TCase *tc_doc = tcase_create("appendix_b_doc");
	TCase *tc_req = tcase_create("appendix_b_req");

	tcase_set_timeout(tc_doc, 5);
	tcase_set_timeout(tc_req, 5);

	tcase_add_checked_fixture(tc_doc, mqtt_report_test_start, NULL);
	tcase_add_test(tc_doc, test_conformance_table_loaded);
	suite_add_tcase(s, tc_doc);

	tcase_add_checked_fixture(tc_req, mqtt_report_test_start, NULL);
	tcase_add_checked_fixture(tc_req, mqtt_silence_stderr, mqtt_restore_stderr);

	tcase_add_test(tc_req, test_mqtt_1_5_4_surrogate);
	tcase_add_test(tc_req, test_mqtt_3_1_2_2_v6_rejected);
	tcase_add_test(tc_req, test_mqtt_3_2_0_2_second_connect_no_extra_connack);
	tcase_add_test(tc_req, test_mqtt_3_2_2_7_error_reason_sends_connack_then_closes);
	tcase_add_test(tc_req, test_mqtt_3_2_2_2_clean_start_existing_session);
	tcase_add_test(tc_req, test_mqtt_3_2_2_3_no_session);
	tcase_add_test(tc_req, test_mqtt_connect_auth_data_rejected);
	tcase_add_test(tc_req, test_mqtt_connect_topic_alias_max_zero);
	tcase_add_test(tc_req, test_mqtt_2_2_1_6_unsuback);
	tcase_add_test(tc_req, test_mqtt_2_2_1_2_qos0_packet_id_treated_as_payload);
	tcase_add_test(tc_req, test_mqtt_4_7_3_1_topic_filter);

	add_mapped_tests(tc_req, g_req_map, g_req_map_count);
	add_mapped_tests(tc_req, g_policy_map, g_policy_map_count);

	tcase_add_test(tc_req, test_mqtt_2_2_1_3);
	tcase_add_test(tc_req, test_mqtt_2_2_1_3_unsubscribe_packet_id_zero);
	tcase_add_test(tc_req, test_mqtt_3_3_2_1_topic_name_utf8);
	tcase_add_test(tc_req, test_mqtt_3_3_1_2_qos0_dup_clear);
	tcase_add_test(tc_req, test_mqtt_subscribe_qos3_rejected);
	tcase_add_test(tc_req, test_mqtt_subscribe_retain_handling3_rejected);
	suite_add_tcase(s, tc_req);
	return s;
}

int main(void)
{
	Suite *s;
	SRunner *sr;
	int failed;
	struct mqtt_test_log_state log_state = {0};

	*mqtt_test_options.database = false;

	if (load_doc_ids() == -1)
		return 1;

	init_req_map();
	print_supported_requirements();

	(void)mqtt_test_log_to_null(&log_state);

	s = mqtt_conformance_suite();
	sr = srunner_create(s);

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	free_data();
	mqtt_test_restore_log(&log_state);
	return failed == 0 ? 0 : 1;
}
