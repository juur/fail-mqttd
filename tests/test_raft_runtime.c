/*
 * Runtime-focused smoke tests for raft_loop/raft_init/raft_clean.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdatomic.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>

#include "raft.h"
#include "test_io.h"

extern _Atomic bool running;
extern uint8_t opt_raft_id;
extern in_port_t opt_raft_port;
extern struct in_addr opt_raft_listen;

static const struct raft_impl_limits runtime_limits[RAFT_MAX_LOG] = {
	[RAFT_LOG_NOOP] = { 0, 0 },
	[RAFT_LOG_REGISTER_TOPIC] = { 0, 0 },
	[RAFT_LOG_UNREGISTER_TOPIC] = { 0, 0 },
	[RAFT_LOG_REGISTER_SESSION] = { 0, 0 },
	[RAFT_LOG_UNREGISTER_SESSION] = { 0, 0 },
};

static const struct raft_impl runtime_impl = {
	.name = "runtime",
	.num_log_types = RAFT_MAX_LOG,
	.limits = runtime_limits,
	.handlers = {
		[0] = {0},
	},
};

START_TEST(test_raft_loop_single_pass)
{
	opt_raft_id = 1;
	opt_raft_port = 0;
	opt_raft_listen.s_addr = htonl(INADDR_LOOPBACK);
	atomic_store(&running, false);

	ck_assert_ptr_eq(raft_loop((void *)&runtime_impl), NULL);
}
END_TEST

static void handle_sigusr1(int signo)
{
	(void)signo;
}

static void *raft_loop_thread(void *arg)
{
	return raft_loop(arg);
}

START_TEST(test_raft_loop_eintr)
{
	pthread_t tid;
	struct sigaction action;
	struct sigaction old_action;

	opt_raft_id = 1;
	opt_raft_port = 0;
	opt_raft_listen.s_addr = htonl(INADDR_LOOPBACK);
	atomic_store(&running, true);

	memset(&action, 0, sizeof(action));
	action.sa_handler = handle_sigusr1;
	ck_assert_int_eq(sigaction(SIGUSR1, &action, &old_action), 0);

	ck_assert_int_eq(pthread_create(&tid, NULL, raft_loop_thread, (void *)&runtime_impl), 0);

	test_sleep_ms(30);
	ck_assert_int_eq(pthread_kill(tid, SIGUSR1), 0);

	test_sleep_ms(30);
	atomic_store(&running, false);
	ck_assert_int_eq(pthread_kill(tid, SIGUSR1), 0);
	ck_assert_int_eq(pthread_join(tid, NULL), 0);

	ck_assert_int_eq(sigaction(SIGUSR1, &old_action, NULL), 0);
}
END_TEST

static Suite *raft_runtime_suite(void)
{
	Suite *s = suite_create("raft_runtime");
	TCase *tc = tcase_create("runtime");

	tcase_add_test(tc, test_raft_loop_single_pass);
	tcase_add_test(tc, test_raft_loop_eintr);
	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = raft_runtime_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
