/*
 * MQTT SUBSCRIBE/UNSUBSCRIBE skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>

#include "mqtt_test_support.h"

START_TEST(test_topic_filter_valid)
{
	const uint8_t filter[] = "a/+/c";

	ck_assert_int_eq(mqtt_test_api.is_valid_topic_filter(filter), 0);
}
END_TEST

START_TEST(test_topic_filter_invalid)
{
	const uint8_t filter[] = "a/#/c";

	ck_assert_int_eq(mqtt_test_api.is_valid_topic_filter(filter), -1);
}
END_TEST

START_TEST(test_topic_filter_root_invalid)
{
	const uint8_t filter[] = "/";

	ck_assert_int_eq(mqtt_test_api.is_valid_topic_filter(filter), -1);
}
END_TEST

START_TEST(test_topic_match)
{
	const uint8_t name[] = "a/b/c";
	const uint8_t filter1[] = "a/+/c";
	const uint8_t filter2[] = "a/#";
	const uint8_t filter3[] = "a/+/d";

	ck_assert(mqtt_test_api.topic_match(name, filter1));
	ck_assert(mqtt_test_api.topic_match(name, filter2));
	ck_assert(!mqtt_test_api.topic_match(name, filter3));
}
END_TEST

static Suite *mqtt_subscribe_suite(void)
{
	Suite *s = suite_create("mqtt_subscribe");
	TCase *tc = tcase_create("subscribe");

	tcase_add_test(tc, test_topic_filter_valid);
	tcase_add_test(tc, test_topic_filter_invalid);
	tcase_add_test(tc, test_topic_filter_root_invalid);
	tcase_add_test(tc, test_topic_match);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_subscribe_suite();
	SRunner *sr = srunner_create(s);
	int failed;
	struct mqtt_test_log_state log_state = {0};

	(void)mqtt_test_log_to_null(&log_state);
	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	mqtt_test_restore_log(&log_state);
	return failed == 0 ? 0 : 1;
}
