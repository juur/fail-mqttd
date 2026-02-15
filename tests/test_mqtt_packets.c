/*
 * MQTT packet framing/parsing skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <errno.h>

#include "mqtt_test_support.h"

static const uint32_t mqtt_max_packet_length = 0x1000000U;

static void encode_var(uint32_t value, uint8_t out[static 4], int *out_len)
{
	*out_len = mqtt_test_api.encode_var_byte(value, out);
}

static uint32_t decode_var(const uint8_t *buf, size_t len, int *err)
{
	const uint8_t *ptr = buf;
	size_t bytes_left = len;
	uint32_t value;

	errno = 0;
	value = mqtt_test_api.read_var_byte(&ptr, &bytes_left);
	*err = errno;
	return value;
}

START_TEST(test_var_byte_round_trip)
{
	uint32_t values[] = {0, 127, 128, 16383, 16384, 2097151, 268435455};
	uint8_t buf[4];

	for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
		const uint8_t *ptr = buf;
		size_t bytes_left;
		uint32_t decoded;
		int len;

		len = mqtt_test_api.encode_var_byte(values[i], buf);
		ck_assert_int_ge(len, 1);
		ck_assert_int_le(len, 4);

		bytes_left = (size_t)len;
		decoded = mqtt_test_api.read_var_byte(&ptr, &bytes_left);
		ck_assert_uint_eq(decoded, values[i]);
		ck_assert_uint_eq(bytes_left, 0);
	}
}
END_TEST

START_TEST(test_parse_properties_invalid_for_connect)
{
	struct property props[1];
	uint8_t prop_buf[16];
	uint8_t pkt_buf[32];
	uint8_t *ptr = prop_buf;
	const uint8_t *parse_ptr = pkt_buf;
	size_t bytes_left;
	ssize_t size;
	int len;
	struct property (*out_props)[];
	unsigned out_count = 0;

	out_props = NULL;

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_TOPIC_ALIAS;
	props[0].byte2 = 1;

	size = mqtt_test_api.get_properties_size(&props, 1);
	ck_assert_int_gt((int)size, 0);

	ck_assert_int_eq(mqtt_test_api.build_properties(&props, 1, &ptr), 0);
	ck_assert_int_eq((int)(ptr - prop_buf), (int)size);

	len = mqtt_test_api.encode_var_byte((uint32_t)size, pkt_buf);
	memcpy(pkt_buf + len, prop_buf, (size_t)size);
	bytes_left = (size_t)len + (size_t)size;

	errno = 0;
	ck_assert_int_eq(mqtt_test_api.parse_properties(&parse_ptr, &bytes_left,
				&out_props, &out_count, MQTT_CP_CONNECT), -1);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_parse_packet_too_large)
{
	int fds[2];
	struct client client;
	struct session session;
	int rc = 0;

	memset(&session, 0, sizeof(session));
	session.id = 9;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);

	ck_assert_int_ne(mqtt_test_write_packet_header(fds[1], MQTT_CP_CONNECT, 0,
				mqtt_max_packet_length + 1U), -1);
	for (int i = 0; i < 4; i++) {
		rc = mqtt_test_api.parse_incoming(&client);
		if (rc != 0)
			break;
	}
	ck_assert_int_eq(rc, -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_PACKET_TOO_LARGE);

	close(fds[1]);
	if (client.fd != -1)
		close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_var_byte_min_bytes)
{
	uint8_t buf[4];
	int len;

	encode_var(127, buf, &len);
	ck_assert_int_eq(len, 1);
	encode_var(128, buf, &len);
	ck_assert_int_eq(len, 2);
	encode_var(16383, buf, &len);
	ck_assert_int_eq(len, 2);
	encode_var(16384, buf, &len);
	ck_assert_int_eq(len, 3);
	encode_var(2097151, buf, &len);
	ck_assert_int_eq(len, 3);
	encode_var(2097152, buf, &len);
	ck_assert_int_eq(len, 4);
}
END_TEST

START_TEST(test_var_byte_decode_empty)
{
	uint8_t buf[1] = {0};
	int err = 0;
	uint32_t value;

	value = decode_var(buf, 0, &err);
	ck_assert_uint_eq(value, 0);
	ck_assert_int_eq(err, ERANGE);
}
END_TEST

START_TEST(test_read_utf8_valid)
{
	uint8_t buf[] = {0x00, 0x03, 'a', 'b', 'c'};
	const uint8_t *ptr = buf;
	size_t bytes_left = sizeof(buf);
	uint8_t *out;

	out = mqtt_test_api.read_utf8(&ptr, &bytes_left);
	ck_assert_ptr_nonnull(out);
	ck_assert_str_eq((char *)out, "abc");
	free(out);
}
END_TEST

START_TEST(test_read_utf8_null_char)
{
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

START_TEST(test_read_utf8_invalid)
{
	uint8_t buf[] = {0x00, 0x02, 0xC0, 0x00};
	const uint8_t *ptr = buf;
	size_t bytes_left = sizeof(buf);
	uint8_t *out;

	errno = 0;
	out = mqtt_test_api.read_utf8(&ptr, &bytes_left);
	ck_assert_ptr_null(out);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_read_binary)
{
	uint8_t buf[] = {0x00, 0x02, 0xAA, 0xBB};
	const uint8_t *ptr = buf;
	size_t bytes_left = sizeof(buf);
	uint16_t len = 0;
	uint8_t *out;

	out = mqtt_test_api.read_binary(&ptr, &bytes_left, &len);
	ck_assert_ptr_nonnull(out);
	ck_assert_uint_eq(len, 2);
	ck_assert_uint_eq(out[0], 0xAA);
	ck_assert_uint_eq(out[1], 0xBB);
	free(out);
}
END_TEST

START_TEST(test_parse_pingreq_ok_no_handler)
{
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 1;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 0, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_api.parse_incoming(&client), 0);
	ck_assert_int_eq(client.parse_state, READ_STATE_NEW);
	ck_assert_int_eq(client.state, CS_ACTIVE);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_parse_pingreq_invalid_flags)
{
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 2;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 1, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_api.parse_incoming(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_parse_first_packet_not_connect)
{
	int fds[2];
	struct client client;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], NULL);

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], MQTT_CP_PINGREQ, 0, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_api.parse_incoming(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

START_TEST(test_parse_invalid_type_zero)
{
	int fds[2];
	struct client client;
	struct session session;

	memset(&session, 0, sizeof(session));
	session.id = 3;

	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
	mqtt_test_init_client(&client, fds[0], &session);

	ck_assert_int_ne(mqtt_test_write_packet(fds[1], 0, 0, NULL, 0), -1);
	ck_assert_int_eq(mqtt_test_api.parse_incoming(&client), -1);
	ck_assert_int_eq(client.state, CS_CLOSING);
	ck_assert_int_eq(client.disconnect_reason, MQTT_MALFORMED_PACKET);

	close(fds[1]);
	close(fds[0]);
	mqtt_test_cleanup_client(&client);
}
END_TEST

static Suite *mqtt_packets_suite(void)
{
	Suite *s = suite_create("mqtt_packets");
	TCase *tc = tcase_create("packets");

	tcase_add_test(tc, test_var_byte_round_trip);
	tcase_add_test(tc, test_var_byte_min_bytes);
	tcase_add_test(tc, test_var_byte_decode_empty);
	tcase_add_test(tc, test_read_utf8_valid);
	tcase_add_test(tc, test_read_utf8_null_char);
	tcase_add_test(tc, test_read_utf8_invalid);
	tcase_add_test(tc, test_read_binary);
	tcase_add_test(tc, test_parse_pingreq_ok_no_handler);
	tcase_add_test(tc, test_parse_pingreq_invalid_flags);
	tcase_add_test(tc, test_parse_first_packet_not_connect);
	tcase_add_test(tc, test_parse_invalid_type_zero);
	tcase_add_test(tc, test_parse_properties_invalid_for_connect);
	tcase_add_test(tc, test_parse_packet_too_large);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_packets_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
