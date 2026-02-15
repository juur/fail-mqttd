/*
 * MQTT session lifecycle skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>

#include "mqtt_test_support.h"

START_TEST(test_utf8_valid_ascii)
{
	const uint8_t text[] = "hello";

	ck_assert_int_eq(mqtt_test_api.is_valid_utf8(text), 0);
}
END_TEST

START_TEST(test_utf8_invalid_continuation)
{
	const uint8_t text[] = {0x80, 0x00};

	ck_assert_int_eq(mqtt_test_api.is_valid_utf8(text), -1);
}
END_TEST

static Suite *mqtt_session_suite(void)
{
	Suite *s = suite_create("mqtt_session");
	TCase *tc = tcase_create("session");

	tcase_add_test(tc, test_utf8_valid_ascii);
	tcase_add_test(tc, test_utf8_invalid_continuation);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_session_suite();
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
