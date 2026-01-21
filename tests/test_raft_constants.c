/*
 * Check-based tests for raft_constants.c string tables.
 */
#define _XOPEN_SOURCE 800

#include <check.h>
#include <stddef.h>

#include "raft.h"

START_TEST(test_status_strings_present)
{
	int status;

	for (status = 0; status < RAFT_MAX_STATUS; status++) {
		ck_assert_ptr_nonnull(raft_status_str[status]);
	}
}
END_TEST

START_TEST(test_rpc_strings_present)
{
	int rpc;

	for (rpc = 0; rpc < RAFT_MAX_RPC; rpc++) {
		if (rpc == RAFT_INSTALL_SNAPSHOT ||
				rpc == RAFT_INSTALL_SNAPSHOT_REPLY) {
			continue;
		}
		ck_assert_ptr_nonnull(raft_rpc_str[rpc]);
	}
}
END_TEST

START_TEST(test_mode_strings_present)
{
	int mode;

	for (mode = 0; mode < RAFT_MAX_STATES; mode++) {
		ck_assert_ptr_nonnull(raft_mode_str[mode]);
	}
}
END_TEST

START_TEST(test_conn_strings_present)
{
	int conn;

	for (conn = 0; conn < RAFT_MAX_CONN; conn++) {
		ck_assert_ptr_nonnull(raft_conn_str[conn]);
	}
}
END_TEST

START_TEST(test_log_strings_present)
{
	int log;

	for (log = 0; log < RAFT_MAX_LOG; log++) {
		ck_assert_ptr_nonnull(raft_log_str[log]);
	}
}
END_TEST

START_TEST(test_sample_string_values)
{
	ck_assert_str_eq(raft_status_str[RAFT_OK], "OK");
	ck_assert_str_eq(raft_rpc_str[RAFT_HELLO], "HELLO");
	ck_assert_str_eq(raft_mode_str[RAFT_STATE_LEADER], "LEADER");
	ck_assert_str_eq(raft_conn_str[RAFT_PEER], "PEER");
	ck_assert_str_eq(raft_log_str[RAFT_LOG_REGISTER_TOPIC], "REGISTER_TOPIC");
}
END_TEST

static Suite *raft_constants_suite(void)
{
	Suite *s;
	TCase *tc;

	s = suite_create("raft_constants");
	tc = tcase_create("strings");

	tcase_add_test(tc, test_status_strings_present);
	tcase_add_test(tc, test_rpc_strings_present);
	tcase_add_test(tc, test_mode_strings_present);
	tcase_add_test(tc, test_conn_strings_present);
	tcase_add_test(tc, test_log_strings_present);
	tcase_add_test(tc, test_sample_string_values);

	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s;
	SRunner *sr;
	int failed;

	s = raft_constants_suite();
	sr = srunner_create(s);
	srunner_run_all(sr, CK_VERBOSE);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
