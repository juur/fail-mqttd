/*
 * MQTT PUBLISH/QoS skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <errno.h>

#include "mqtt_test_api.h"

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

static Suite *mqtt_publish_suite(void)
{
	Suite *s = suite_create("mqtt_publish");
	TCase *tc = tcase_create("publish");

	tcase_add_test(tc, test_topic_name_valid);
	tcase_add_test(tc, test_topic_name_invalid_wildcards);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_publish_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
