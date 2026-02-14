/*
 * MQTT constants/string table skeleton tests.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>

#include "mqtt.h"

START_TEST(test_property_strings_nonnull)
{
	for (int i = 1; i < MQTT_PROPERTY_IDENT_MAX; i++) {
		if (property_to_type[i] != MQTT_TYPE_UNDEFINED)
			ck_assert_ptr_nonnull(property_str[i]);
	}
}
END_TEST

START_TEST(test_control_packet_strings_nonnull)
{
	for (int i = 1; i < MQTT_CP_MAX; i++) {
		ck_assert_ptr_nonnull(control_packet_str[i]);
	}
}
END_TEST

static Suite *mqtt_constants_suite(void)
{
	Suite *s = suite_create("mqtt_constants");
	TCase *tc = tcase_create("constants");

	tcase_add_test(tc, test_property_strings_nonnull);
	tcase_add_test(tc, test_control_packet_strings_nonnull);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = mqtt_constants_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
