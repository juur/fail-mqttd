/*
 * MQTT PUBLISH/QoS skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <errno.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>

#include "mqtt_test_support.h"

extern bool opt_database;

static size_t build_publish_packet(uint8_t *out, size_t out_len, uint8_t flags,
		const uint8_t *topic, const struct property *props,
		unsigned num_props, const uint8_t *payload, size_t payload_len,
		uint16_t packet_id)
{
	uint8_t var_buf[256];
	uint8_t prop_buf[128];
	uint8_t *ptr = var_buf;
	uint16_t topic_len = 0;
	uint16_t tmp16;
	uint8_t remlen[4];
	int remlen_len;
	size_t var_len;
	size_t prop_len = 0;
	uint8_t *prop_ptr = prop_buf;

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

START_TEST(test_topic_name_valid)
{
	const uint8_t name[] = "a/b/c";

	ck_assert_int_eq(mqtt_test_api.is_valid_topic_name(name), 0);
}
END_TEST

START_TEST(test_topic_name_invalid_wildcards)
{
	const uint8_t name[] = "a/+/c";

	errno = 0;
	ck_assert_int_eq(mqtt_test_api.is_valid_topic_name(name), -1);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_publish_qos1_sends_puback)
{
	uint8_t packet[256];
	size_t packet_len;
	uint8_t buf[64];
	uint16_t packet_id = 7;
	uint16_t parsed_id = 0;
	int fds[2];
	struct client client;
	struct session session;
	ssize_t rd;

	memset(&session, 0, sizeof(session));
	session.id = 44;
	ck_assert_int_eq(pthread_rwlock_init(&session.subscriptions_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&session.delivery_states_lock, NULL), 0);

	packet_len = build_publish_packet(packet, sizeof(packet), MQTT_FLAG_PUBLISH_QOS1,
			(const uint8_t *)"a", NULL, 0,
			(const uint8_t *)"hi", 2, packet_id);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), 0);

	ck_assert_int_ne(mqtt_test_set_nonblock(fds[1]), -1);

	rd = -1;
	for (int i = 0; i < 10; i++) {
	rd = mqtt_test_read_packet(fds[1], buf, sizeof(buf));
		if (rd > 0)
			break;
		if (rd == -1 && errno != EAGAIN && errno != EWOULDBLOCK)
			break;
		mqtt_test_sleep_ms(1);
	}
	ck_assert_int_gt((int)rd, 0);
	ck_assert_int_eq((buf[0] >> 4), MQTT_CP_PUBACK);
	ck_assert_int_eq(mqtt_test_read_packet_id(buf, (size_t)rd, &parsed_id), 0);
	ck_assert_uint_eq(parsed_id, packet_id);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
	pthread_rwlock_destroy(&session.subscriptions_lock);
	pthread_rwlock_destroy(&session.delivery_states_lock);
}
END_TEST

START_TEST(test_publish_topic_alias_zero_rejected)
{
	uint8_t packet[128];
	size_t packet_len;
	struct property props[1];
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 45;

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_TOPIC_ALIAS;
	props[0].byte2 = 0;

	packet_len = build_publish_packet(packet, sizeof(packet), 0,
			(const uint8_t *)"a", props, 1, NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	if (client.fd != -1)
		close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_publish_topic_alias_missing_mapping)
{
	uint8_t packet[128];
	size_t packet_len;
	struct property props[1];
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 46;

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_TOPIC_ALIAS;
	props[0].byte2 = 1;

	packet_len = build_publish_packet(packet, sizeof(packet), 0,
			(const uint8_t *)"", props, 1, NULL, 0, 0);
	ck_assert_int_gt((int)packet_len, 0);

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);
	client.is_auth = true;
	client.clnt_topic_aliases = calloc(2, sizeof(uint8_t *));
	ck_assert_ptr_nonnull(client.clnt_topic_aliases);

	ck_assert_int_ne(write(fds[1], packet, packet_len), -1);
	ck_assert_int_eq(mqtt_test_drive_parse(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	free((void *)client.clnt_topic_aliases);
	client.clnt_topic_aliases = NULL;
	close(fds[1]);
	if (client.fd != -1)
		close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

static Suite *mqtt_publish_suite(void)
{
	Suite *s = suite_create("mqtt_publish");
	TCase *tc = tcase_create("publish");

	tcase_add_test(tc, test_topic_name_valid);
	tcase_add_test(tc, test_topic_name_invalid_wildcards);
	tcase_add_test(tc, test_publish_qos1_sends_puback);
	tcase_add_test(tc, test_publish_topic_alias_zero_rejected);
	tcase_add_test(tc, test_publish_topic_alias_missing_mapping);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_publish_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	opt_database = false;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
