/*
 * MQTT CONNECT/CONNACK skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <errno.h>

#include "mqtt_test_support.h"

START_TEST(test_connect_client_id_valid)
{
	const uint8_t id[] = "Client01";

	ck_assert_int_eq(mqtt_test_api.is_valid_connection_id(id), 0);
}
END_TEST

static Suite *mqtt_connect_suite(void)
{
	Suite *s = suite_create("mqtt_connect");
	TCase *tc = tcase_create("connect");

	tcase_add_test(tc, test_connect_client_id_valid);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_connect_suite();
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
