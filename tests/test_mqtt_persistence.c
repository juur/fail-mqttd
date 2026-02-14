/*
 * MQTT persistence skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <string.h>

#include "mqtt_test_api.h"

START_TEST(test_properties_round_trip)
{
	struct property props[2];
	uint8_t prop_buf[64];
	uint8_t pkt_buf[64];
	uint8_t *ptr = prop_buf;
	const uint8_t *parse_ptr = pkt_buf;
	size_t bytes_left;
	ssize_t size;
	int len;
	struct property (*out_props)[];
	unsigned out_count = 0;

	memset(props, 0, sizeof(props));
	props[0].ident = MQTT_PROP_PAYLOAD_FORMAT_INDICATOR;
	props[0].byte = 1;
	props[1].ident = MQTT_PROP_USER_PROPERTY;
	props[1].utf8_pair[0] = (unsigned char *)"k";
	props[1].utf8_pair[1] = (unsigned char *)"v";

	size = mqtt_test_api.get_properties_size(&props, 2);
	ck_assert_int_gt((int)size, 0);

	ck_assert_int_eq(mqtt_test_api.build_properties(&props, 2, &ptr), 0);
	ck_assert_int_eq((int)(ptr - prop_buf), (int)size);

	len = mqtt_test_api.encode_var_byte((uint32_t)size, pkt_buf);
	memcpy(pkt_buf + len, prop_buf, (size_t)size);
	bytes_left = (size_t)len + (size_t)size;

	ck_assert_int_eq(mqtt_test_api.parse_properties(&parse_ptr, &bytes_left,
				&out_props, &out_count, MQTT_CP_PUBLISH), 0);
	ck_assert_int_eq(out_count, 2);
	ck_assert_int_eq((*out_props)[0].ident, MQTT_PROP_PAYLOAD_FORMAT_INDICATOR);
	ck_assert_int_eq((*out_props)[0].byte, 1);
	ck_assert_int_eq((*out_props)[1].ident, MQTT_PROP_USER_PROPERTY);
	ck_assert_str_eq((char *)(*out_props)[1].utf8_pair[0], "k");
	ck_assert_str_eq((char *)(*out_props)[1].utf8_pair[1], "v");

	mqtt_test_api.free_properties(out_props, out_count);
}
END_TEST

static Suite *mqtt_persistence_suite(void)
{
	Suite *s = suite_create("mqtt_persistence");
	TCase *tc = tcase_create("persistence");

	tcase_add_test(tc, test_properties_round_trip);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_persistence_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
