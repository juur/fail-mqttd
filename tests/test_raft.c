/*
 * Check-based unit tests for core raft.c functionality.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <limits.h>

#include "raft.h"
#include "raft_test_api.h"

extern struct raft_state raft_state;
extern const struct raft_impl *raft_impl;

static int free_log_called;
static int commit_called;

static int test_free_log(struct raft_log *entry)
{
	(void)entry;
	free_log_called++;
	return 0;
}

static int test_commit_and_advance(struct raft_log *entry)
{
	(void)entry;
	commit_called++;
	return 0;
}

static int test_save_log(const struct raft_log *entry, uint8_t **buf)
{
	uint32_t value;

	if (!buf) {
		errno = EINVAL;
		return -1;
	}

	value = htonl(entry->sequence_num);
	*buf = malloc(sizeof(value));
	if (!*buf)
		return -1;

	memcpy(*buf, &value, sizeof(value));
	return (int)sizeof(value);
}

static int test_read_log(struct raft_log *entry, const uint8_t *buf, int len)
{
	uint32_t value;

	if (!entry || !buf || len != (int)sizeof(value)) {
		errno = EINVAL;
		return -1;
	}

	memcpy(&value, buf, sizeof(value));
	entry->sequence_num = ntohl(value);
	return 0;
}

static raft_status_t test_process_packet_fail(size_t *bytes_remaining,
		const uint8_t **ptr, raft_rpc_t rpc, raft_log_t type,
		struct raft_log *log_entry)
{
	(void)bytes_remaining;
	(void)ptr;
	(void)rpc;
	(void)type;
	(void)log_entry;
	errno = EIO;
	return -1;
}

static int test_pre_send_fail_overflow(struct raft_log *entry, struct send_state *state)
{
	(void)entry;
	(void)state;
	errno = EOVERFLOW;
	return -1;
}

static const struct raft_impl test_impl = {
	.name = "test",
	.num_log_types = RAFT_MAX_LOG,
	.handlers = {
		[RAFT_LOG_REGISTER_TOPIC] = {
			.free_log = test_free_log,
			.commit_and_advance = test_commit_and_advance,
			.save_log = test_save_log,
			.read_log = test_read_log,
		},
	},
};

static const struct raft_impl fail_impl = {
	.name = "fail",
	.num_log_types = RAFT_MAX_LOG,
	.handlers = {
		[RAFT_LOG_REGISTER_TOPIC] = {
			.process_packet = test_process_packet_fail,
		},
	},
};

static const struct raft_impl pre_send_overflow_impl = {
	.name = "pre-send-overflow",
	.num_log_types = RAFT_MAX_LOG,
	.handlers = {
		[RAFT_LOG_REGISTER_TOPIC] = {
			.pre_send = test_pre_send_fail_overflow,
		},
	},
};

static void reset_peers(void)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();

	if (*peers_ptr) {
		free(*peers_ptr);
		*peers_ptr = NULL;
	}
	*num_ptr = 1;
}

static void reset_raft_state(void)
{
	struct raft_log *tmp;

	for (tmp = raft_state.log_head; tmp;) {
		struct raft_log *next = tmp->next;
		raft_test_api.raft_free_log(tmp);
		tmp = next;
	}

	memset(&raft_state, 0, sizeof(raft_state));
	atomic_store(&raft_state.log_length, 0);
	raft_state.voted_for = NULL_ID;
}

static void init_client_state(void)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	memset(client_state, 0, sizeof(*client_state));
	ck_assert_int_eq(pthread_rwlock_init(&client_state->lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&client_state->log_pending_lock, NULL), 0);
	client_state->current_leader_id = NULL_ID;
}

static void destroy_client_state(void)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	pthread_rwlock_destroy(&client_state->log_pending_lock);
	pthread_rwlock_destroy(&client_state->lock);
}

static void init_entry_locks(struct raft_host_entry *entry)
{
	ck_assert_int_eq(pthread_rwlock_init(&entry->wr_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&entry->ss_lock, NULL), 0);
}

static void destroy_entry_locks(struct raft_host_entry *entry)
{
	pthread_rwlock_destroy(&entry->wr_lock);
	pthread_rwlock_destroy(&entry->ss_lock);
}

static int redirect_stderr_to_null(int *saved_fd)
{
	int devnull = open("/dev/null", O_WRONLY);

	if (devnull == -1)
		return -1;

	*saved_fd = dup(STDERR_FILENO);
	if (*saved_fd == -1) {
		close(devnull);
		return -1;
	}

	if (dup2(devnull, STDERR_FILENO) == -1) {
		close(devnull);
		close(*saved_fd);
		return -1;
	}

	close(devnull);
	return 0;
}

static void restore_stderr(int saved_fd)
{
	if (saved_fd != -1) {
		dup2(saved_fd, STDERR_FILENO);
		close(saved_fd);
	}
}

static struct raft_log *make_log(uint32_t index, uint32_t term)
{
	struct raft_log *entry;

	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->index = index;
	entry->term = term;
	return entry;
}

static int call_client_log_sendv(raft_log_t event, ...)
{
	int rc;
	va_list ap;

	va_start(ap, event);
	rc = raft_test_api.raft_client_log_sendv(event, ap);
	va_end(ap);
	return rc;
}

static int enter_temp_dir(char *path, size_t path_len, int *saved_cwd_fd)
{
	const char *tmpdir = getenv("TMPDIR");

	if (!tmpdir)
		tmpdir = "/tmp";

	if (snprintf(path, path_len, "%s/raft-test-XXXXXX", tmpdir) >= (int)path_len)
		return -1;

	if (mkdtemp(path) == NULL)
		return -1;

	*saved_cwd_fd = open(".", O_RDONLY);
	if (*saved_cwd_fd == -1)
		return -1;

	if (chdir(path) == -1)
		return -1;

	return 0;
}

static void leave_temp_dir(const char *path, int saved_cwd_fd)
{
	if (saved_cwd_fd != -1) {
		if (fchdir(saved_cwd_fd) == -1)
			(void)0;
		close(saved_cwd_fd);
	}

	if (path) {
		unlink("save_state.1");
		unlink("save_state.1.new");
		rmdir(path);
	}
}

enum {
	TEST_RSS_CURRENT_TERM = 0,
	TEST_RSS_VOTED_FOR,
	TEST_RSS_TAIL_TERM,
	TEST_RSS_TAIL_INDEX,
	TEST_RSS_SIZE
};

static int read_state_header(const char *path, uint32_t header[TEST_RSS_SIZE])
{
	int fd;
	ssize_t rc;

	fd = open(path, O_RDONLY);
	if (fd == -1)
		return -1;

	rc = read(fd, header, sizeof(uint32_t) * TEST_RSS_SIZE);
	close(fd);

	if (rc != (ssize_t)(sizeof(uint32_t) * TEST_RSS_SIZE))
		return -1;

	return 0;
}

static void setup(void)
{
	raft_impl = &test_impl;
	free_log_called = 0;
	commit_called = 0;
	reset_raft_state();
	init_client_state();
	reset_peers();
}

static void teardown(void)
{
	reset_raft_state();
	reset_peers();
	destroy_client_state();
}

START_TEST(test_parse_cmdline_host_list_valid)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct in_addr addr1;
	struct in_addr addr2;

	ck_assert_int_eq(inet_pton(AF_INET, "127.0.0.1", &addr1), 1);
	ck_assert_int_eq(inet_pton(AF_INET, "127.0.0.2", &addr2), 1);

	ck_assert_int_eq(raft_parse_cmdline_host_list(
				"1:127.0.0.1:1883/2:127.0.0.2:1884", 0), 0);
	ck_assert_ptr_nonnull(*peers_ptr);
	ck_assert_uint_eq(*num_ptr, 2);
	ck_assert_uint_eq((*peers_ptr)[0].server_id, 1);
	ck_assert_uint_eq((*peers_ptr)[1].server_id, 2);
	ck_assert_uint_eq((*peers_ptr)[0].port, htons(1883));
	ck_assert_uint_eq((*peers_ptr)[1].port, htons(1884));
	ck_assert_uint_eq((*peers_ptr)[0].address.s_addr, addr1.s_addr);
	ck_assert_uint_eq((*peers_ptr)[1].address.s_addr, addr2.s_addr);
	ck_assert_int_eq((*peers_ptr)[0].peer_fd, -1);
}
END_TEST

START_TEST(test_save_and_load_state_round_trip)
{
	char tmpdir[PATH_MAX];
	int saved_cwd_fd = -1;
	int saved_stderr = -1;
	struct raft_log *log1;
	struct raft_log *log2;

	ck_assert_int_eq(enter_temp_dir(tmpdir, sizeof(tmpdir), &saved_cwd_fd), 0);

	raft_state.self_id = 1;
	raft_state.current_term = 7;
	raft_state.voted_for = 2;

	log1 = make_log(1, 7);
	log1->sequence_num = 111;
	log2 = make_log(2, 7);
	log2->sequence_num = 222;

	ck_assert_int_eq(raft_test_api.raft_append_log(log1, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	ck_assert_int_eq(raft_test_api.raft_append_log(log2, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = raft_state.log_tail->index;

	ck_assert_int_eq(raft_test_api.raft_save_state(false), 0);

	reset_raft_state();
	raft_state.self_id = 1;
	commit_called = 0;

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	ck_assert_int_eq(raft_test_api.raft_load_state(), 0);
	restore_stderr(saved_stderr);

	ck_assert_uint_eq(raft_state.current_term, 7);
	ck_assert_uint_eq(raft_state.voted_for, 2);
	ck_assert_ptr_nonnull(raft_state.log_head);
	ck_assert_ptr_nonnull(raft_state.log_tail);
	ck_assert_uint_eq(raft_state.log_tail->index, 2);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 2);
	ck_assert_int_eq(commit_called, 2);
	ck_assert_uint_eq(raft_state.commit_index, 2);
	ck_assert_uint_eq(raft_state.log_head->sequence_num, 111);
	ck_assert_ptr_nonnull(raft_state.log_head->next);
	ck_assert_uint_eq(raft_state.log_head->next->sequence_num, 222);

	leave_temp_dir(tmpdir, saved_cwd_fd);
}
END_TEST

START_TEST(test_save_state_header_only_preserves_log)
{
	char tmpdir[PATH_MAX];
	int saved_cwd_fd = -1;
	struct raft_log *log1;
	struct raft_log *log2;
	struct stat st_before;
	struct stat st_after;
	uint32_t header[TEST_RSS_SIZE];

	ck_assert_int_eq(enter_temp_dir(tmpdir, sizeof(tmpdir), &saved_cwd_fd), 0);

	raft_state.self_id = 1;
	raft_state.current_term = 3;
	raft_state.voted_for = 1;

	log1 = make_log(1, 3);
	log1->sequence_num = 10;
	log2 = make_log(2, 3);
	log2->sequence_num = 20;

	ck_assert_int_eq(raft_test_api.raft_append_log(log1, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	ck_assert_int_eq(raft_test_api.raft_append_log(log2, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = raft_state.log_tail->index;

	ck_assert_int_eq(raft_test_api.raft_save_state(false), 0);
	ck_assert_int_eq(stat("save_state.1", &st_before), 0);

	raft_state.current_term = 9;
	raft_state.voted_for = 4;
	ck_assert_int_eq(raft_test_api.raft_save_state(true), 0);

	ck_assert_int_eq(stat("save_state.1", &st_after), 0);
	ck_assert_int_eq(st_before.st_size, st_after.st_size);
	ck_assert_int_eq(read_state_header("save_state.1", header), 0);
	ck_assert_uint_eq(header[TEST_RSS_CURRENT_TERM], 9);
	ck_assert_uint_eq(header[TEST_RSS_VOTED_FOR], 4);
	ck_assert_uint_eq(header[TEST_RSS_TAIL_TERM], 3);
	ck_assert_uint_eq(header[TEST_RSS_TAIL_INDEX], 2);

	leave_temp_dir(tmpdir, saved_cwd_fd);
}
END_TEST

START_TEST(test_load_state_missing_file)
{
	char tmpdir[PATH_MAX];
	int saved_cwd_fd = -1;
	int saved_stderr = -1;

	ck_assert_int_eq(enter_temp_dir(tmpdir, sizeof(tmpdir), &saved_cwd_fd), 0);

	raft_state.self_id = 1;
	errno = 0;

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	ck_assert_int_eq(raft_test_api.raft_load_state(), -1);
	restore_stderr(saved_stderr);

	ck_assert_int_eq(errno, ENOENT);
	ck_assert_ptr_eq(raft_state.log_head, NULL);
	ck_assert_uint_eq(raft_state.commit_index, 0);

	leave_temp_dir(tmpdir, saved_cwd_fd);
}
END_TEST

START_TEST(test_load_state_truncated_log_entry)
{
	char tmpdir[PATH_MAX];
	int saved_cwd_fd = -1;
	int saved_stderr = -1;
	int fd;
	uint32_t header[TEST_RSS_SIZE] = { 1, 0, 1, 1 };
	uint32_t log_word = 1;

	ck_assert_int_eq(enter_temp_dir(tmpdir, sizeof(tmpdir), &saved_cwd_fd), 0);

	fd = open("save_state.1", O_WRONLY | O_CREAT | O_TRUNC, 0600);
	ck_assert_int_ne(fd, -1);
	ck_assert_int_eq(write(fd, header, sizeof(header)), (int)sizeof(header));
	ck_assert_int_eq(write(fd, &log_word, sizeof(log_word)), (int)sizeof(log_word));
	close(fd);

	raft_state.self_id = 1;
	commit_called = 0;
	errno = 0;

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	ck_assert_int_eq(raft_test_api.raft_load_state(), -1);
	restore_stderr(saved_stderr);

	ck_assert_ptr_eq(raft_state.log_head, NULL);
	ck_assert_int_eq(commit_called, 0);

	leave_temp_dir(tmpdir, saved_cwd_fd);
}
END_TEST

START_TEST(test_parse_cmdline_host_list_invalid_id)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();

	errno = 0;
	ck_assert_int_eq(raft_parse_cmdline_host_list("0:127.0.0.1:1883", 0), -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_ptr_eq(*peers_ptr, NULL);
	ck_assert_uint_eq(*num_ptr, 1);
}
END_TEST

START_TEST(test_parse_cmdline_host_list_invalid_port)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();

	errno = 0;
	ck_assert_int_eq(raft_parse_cmdline_host_list("1:127.0.0.1:0", 0), -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_ptr_eq(*peers_ptr, NULL);
	ck_assert_uint_eq(*num_ptr, 1);
}
END_TEST

START_TEST(test_parse_cmdline_host_list_invalid_ip)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();

	errno = 0;
	ck_assert_int_eq(raft_parse_cmdline_host_list("1:999.999.0.1:1883", 0), -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_ptr_eq(*peers_ptr, NULL);
	ck_assert_uint_eq(*num_ptr, 1);
}
END_TEST

START_TEST(test_parse_cmdline_host_list_any_addr)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();

	errno = 0;
	ck_assert_int_eq(raft_parse_cmdline_host_list("1:0.0.0.0:1883", 0), -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_ptr_eq(*peers_ptr, NULL);
	ck_assert_uint_eq(*num_ptr, 1);
}
END_TEST

START_TEST(test_reset_election_timer_future)
{
	timems_t before = raft_test_api.timems();

	ck_assert_int_eq(raft_test_api.raft_reset_election_timer(), 0);
	ck_assert(raft_state.election_timer > before);
}
END_TEST

START_TEST(test_reset_next_ping_future)
{
	timems_t before = raft_test_api.timems();

	ck_assert_int_eq(raft_test_api.raft_reset_next_ping(), 0);
	ck_assert(raft_state.next_ping > before);
}
END_TEST

START_TEST(test_change_to_leader_updates_peers)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	timems_t before;

	peers = calloc(3, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	for (unsigned idx = 0; idx < 3; idx++) {
		init_entry_locks(&peers[idx]);
		peers[idx].peer_fd = -1;
		peers[idx].server_id = idx + 1;
		peers[idx].next_index = 99;
		peers[idx].match_index = 88;
	}
	*peers_ptr = peers;
	*num_ptr = 3;

	raft_state.self_id = 1;
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.log_head = make_log(4, 2);
	raft_state.log_tail = raft_state.log_head;
	atomic_store(&raft_state.log_length, 1);

	client_state->current_leader_id = NULL_ID;
	client_state->current_leader = NULL;

	before = raft_test_api.timems();
	ck_assert_int_eq(raft_test_api.raft_change_to(RAFT_STATE_LEADER), RAFT_STATE_LEADER);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_LEADER);
	ck_assert_uint_eq(client_state->current_leader_id, raft_state.self_id);
	ck_assert_ptr_eq(client_state->current_leader, &peers[0]);
	ck_assert_uint_eq(peers[1].match_index, 0);
	ck_assert_uint_eq(peers[1].next_index, 5);
	ck_assert_uint_eq(peers[2].match_index, 0);
	ck_assert_uint_eq(peers[2].next_index, 5);
	ck_assert(raft_state.next_ping >= before);

	raft_test_api.raft_free_log(raft_state.log_head);
	raft_state.log_head = NULL;
	raft_state.log_tail = NULL;

	for (unsigned idx = 0; idx < 3; idx++)
		destroy_entry_locks(&peers[idx]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_change_to_invalid_state)
{
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_change_to(RAFT_MAX_STATES), -1);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_stop_election_resets_votes)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	timems_t before;

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	*peers_ptr = peers;
	*num_ptr = 2;

	peers[0].vote_responded = true;
	peers[0].vote_granted = 123;
	peers[1].vote_responded = true;
	peers[1].vote_granted = 456;
	raft_state.election = true;

	before = raft_test_api.timems();
	ck_assert_int_eq(raft_test_api.raft_stop_election(), 0);

	ck_assert(!raft_state.election);
	ck_assert(!peers[0].vote_responded);
	ck_assert_uint_eq(peers[0].vote_granted, NULL_ID);
	ck_assert(!peers[1].vote_responded);
	ck_assert_uint_eq(peers[1].vote_granted, NULL_ID);
	ck_assert(raft_state.election_timer > before);

	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_start_election_single_node)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;

	peers = calloc(1, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	peers[0].server_id = 1;
	*peers_ptr = peers;
	*num_ptr = 1;

	raft_state.self_id = 1;
	raft_state.state = RAFT_STATE_CANDIDATE;
	raft_state.current_term = 2;
	raft_state.voted_for = NULL_ID;
	client_state->current_leader_id = 77;

	ck_assert_int_eq(raft_test_api.raft_start_election(), 0);
	ck_assert_int_eq(raft_state.state, RAFT_STATE_LEADER);
	ck_assert_uint_eq(raft_state.current_term, 3);
	ck_assert_uint_eq(raft_state.voted_for, raft_state.self_id);
	ck_assert_uint_eq(client_state->current_leader_id, raft_state.self_id);
	ck_assert(!raft_state.election);
	ck_assert(!peers[0].vote_responded);
	ck_assert_uint_eq(peers[0].vote_granted, NULL_ID);

	destroy_entry_locks(&peers[0]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_request_votes_updates_next_request_vote)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	timems_t before;

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	peers[1].peer_fd = -1;
	*peers_ptr = peers;
	*num_ptr = 2;

	before = raft_test_api.timems();
	ck_assert_int_eq(raft_test_api.raft_request_votes(), 0);
	ck_assert(raft_state.next_request_vote > before);

	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_tick_connection_check_skips_unready)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	peers[1].peer_fd = -1;
	peers[1].port = 0;
	*peers_ptr = peers;
	*num_ptr = 2;

	ck_assert_int_eq(raft_test_api.raft_tick_connection_check(), 0);
	ck_assert_int_eq(peers[1].peer_fd, -1);

	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_reset_read_state_frees_buffer)
{
	struct raft_host_entry entry;

	memset(&entry, 0, sizeof(entry));
	entry.rd_state = RAFT_PCK_PACKET;
	entry.rd_offset = 5;
	entry.rd_need = 10;
	entry.rd_packet_length = 100;
	entry.rd_packet_buffer = malloc(16);
	ck_assert_ptr_nonnull(entry.rd_packet_buffer);

	raft_test_api.raft_reset_read_state(&entry);

	ck_assert_int_eq(entry.rd_state, RAFT_PCK_NEW);
	ck_assert_int_eq(entry.rd_offset, 0);
	ck_assert_int_eq(entry.rd_need, 0);
	ck_assert_int_eq(entry.rd_packet_length, 0);
	ck_assert_ptr_eq(entry.rd_packet_buffer, NULL);
}
END_TEST

START_TEST(test_reset_write_state_frees_buffer)
{
	struct raft_host_entry entry;

	memset(&entry, 0, sizeof(entry));
	init_entry_locks(&entry);
	entry.wr_offset = 4;
	entry.wr_need = 9;
	entry.wr_packet_length = 55;
	{
		const uint8_t *buf = (const uint8_t *)malloc(32);
		ck_assert_ptr_nonnull(buf);
		__atomic_store_n(&entry.wr_packet_buffer, buf, __ATOMIC_SEQ_CST);
	}
	ck_assert_ptr_nonnull(__atomic_load_n(&entry.wr_packet_buffer, __ATOMIC_SEQ_CST));

	ck_assert_int_eq(raft_test_api.raft_reset_write_state(&entry, true), 0);

	ck_assert_int_eq(entry.wr_offset, 0);
	ck_assert_int_eq(entry.wr_need, 0);
	ck_assert_int_eq(entry.wr_packet_length, 0);
	ck_assert_ptr_eq(__atomic_load_n(&entry.wr_packet_buffer, __ATOMIC_SEQ_CST), NULL);
	destroy_entry_locks(&entry);
}
END_TEST

START_TEST(test_reset_ss_state_frees_buffer)
{
	struct raft_host_entry entry;

	memset(&entry, 0, sizeof(entry));
	init_entry_locks(&entry);
	entry.ss_last_index = 9;
	entry.ss_last_term = 7;
	entry.ss_need = 12;
	entry.ss_offset = 5;
	entry.ss_tried_offset = 3;
	entry.ss_tried_length = 4;
	entry.ss_tried_status = RAFT_OK;
	{
		const uint8_t *buf = (const uint8_t *)malloc(8);
		ck_assert_ptr_nonnull(buf);
		__atomic_store_n(&entry.ss_data, buf, __ATOMIC_SEQ_CST);
	}
	ck_assert_ptr_nonnull(__atomic_load_n(&entry.ss_data, __ATOMIC_SEQ_CST));

	ck_assert_int_eq(raft_test_api.raft_reset_ss_state(&entry, true), 0);

	ck_assert_ptr_eq(__atomic_load_n(&entry.ss_data, __ATOMIC_SEQ_CST), NULL);
	ck_assert_int_eq(entry.ss_last_index, 0);
	ck_assert_int_eq(entry.ss_last_term, 0);
	ck_assert_int_eq(entry.ss_need, 0);
	ck_assert_int_eq(entry.ss_offset, 0);
	ck_assert_int_eq(entry.ss_tried_offset, 0);
	ck_assert_int_eq(entry.ss_tried_length, 0);
	ck_assert_int_eq(entry.ss_tried_status, RAFT_ERR);
	destroy_entry_locks(&entry);
}
END_TEST

START_TEST(test_add_write_and_try_write_success)
{
	struct raft_host_entry entry;
	int fds[2];
	const uint8_t payload[] = "raft";
	uint8_t read_buf[8] = {0};
	ssize_t rc;

	ck_assert_int_eq(pipe(fds), 0);

	memset(&entry, 0, sizeof(entry));
	init_entry_locks(&entry);
	entry.peer_fd = fds[1];

	ck_assert_int_eq(raft_test_api.raft_add_write(&entry, (const uint8_t *)strdup((const char *)payload),
				sizeof(payload) - 1), 0);
	ck_assert_int_eq(raft_test_api.raft_try_write(&entry), 0);
	ck_assert_ptr_eq(__atomic_load_n(&entry.wr_packet_buffer, __ATOMIC_SEQ_CST), NULL);

	rc = read(fds[0], read_buf, sizeof(read_buf));
	ck_assert_int_eq(rc, (ssize_t)(sizeof(payload) - 1));
	ck_assert_int_eq(memcmp(read_buf, payload, sizeof(payload) - 1), 0);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&entry);
}
END_TEST

START_TEST(test_add_write_rejects_existing_buffer)
{
	struct raft_host_entry entry;
	const uint8_t *buf;
	uint8_t *new_buf;

	memset(&entry, 0, sizeof(entry));
	init_entry_locks(&entry);
	buf = (const uint8_t *)malloc(4);
	ck_assert_ptr_nonnull(buf);
	__atomic_store_n(&entry.wr_packet_buffer, buf, __ATOMIC_SEQ_CST);

	new_buf = malloc(4);
	ck_assert_ptr_nonnull(new_buf);
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_add_write(&entry, new_buf, 4), -1);
	ck_assert_int_eq(errno, ENOSPC);
	free(new_buf);

	raft_test_api.raft_reset_write_state(&entry, true);
	destroy_entry_locks(&entry);
}
END_TEST

START_TEST(test_try_write_invalid)
{
	struct raft_host_entry entry;
	const uint8_t *buf;

	memset(&entry, 0, sizeof(entry));
	init_entry_locks(&entry);
	entry.peer_fd = -1;
	buf = (const uint8_t *)malloc(4);
	ck_assert_ptr_nonnull(buf);
	__atomic_store_n(&entry.wr_packet_buffer, buf, __ATOMIC_SEQ_CST);

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_try_write(&entry), -1);
	ck_assert_int_eq(errno, EINVAL);

	raft_test_api.raft_reset_write_state(&entry, true);
	destroy_entry_locks(&entry);
}
END_TEST

START_TEST(test_remove_and_free_unknown_host)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *first;
	struct raft_host_entry *second;

	first = calloc(1, sizeof(*first));
	second = calloc(1, sizeof(*second));
	ck_assert_ptr_nonnull(first);
	ck_assert_ptr_nonnull(second);
	init_entry_locks(first);
	init_entry_locks(second);
	first->peer_fd = -1;
	second->peer_fd = -1;
	first->unknown_next = second;
	client_state->unknown_clients = first;

	__atomic_store_n(&second->wr_packet_buffer, (const uint8_t *)malloc(8), __ATOMIC_SEQ_CST);
	__atomic_store_n(&second->ss_data, (const uint8_t *)malloc(8), __ATOMIC_SEQ_CST);
	second->rd_packet_buffer = malloc(8);

	raft_test_api.raft_remove_and_free_unknown_host(second);
	ck_assert_ptr_eq(client_state->unknown_clients, first);
	ck_assert_ptr_eq(first->unknown_next, NULL);

	raft_test_api.raft_remove_and_free_unknown_host(first);
	ck_assert_ptr_eq(client_state->unknown_clients, NULL);
}
END_TEST

static void fill_hello_packet(uint8_t *buf, uint32_t id, raft_conn_t role,
		raft_conn_t type, uint32_t mqtt_addr, uint16_t mqtt_port)
{
	uint32_t length = htonl(RAFT_HDR_SIZE + RAFT_HELLO_SIZE);
	uint32_t id_n = htonl(id);
	uint32_t addr_n = htonl(mqtt_addr);
	uint16_t port_n = htons(mqtt_port);
	uint8_t *ptr = buf;

	*ptr++ = RAFT_HELLO;
	*ptr++ = 0;
	*ptr++ = role;
	*ptr++ = 0;
	memcpy(ptr, &length, sizeof(length));
	ptr += sizeof(length);
	memcpy(ptr, &id_n, sizeof(id_n));
	ptr += sizeof(id_n);
	*ptr++ = type;
	memcpy(ptr, &addr_n, sizeof(addr_n));
	ptr += sizeof(addr_n);
	memcpy(ptr, &port_n, sizeof(port_n));
}

static void fill_header(uint8_t *buf, raft_rpc_t rpc, uint32_t length)
{
	uint32_t length_n = htonl(length);
	uint8_t *ptr = buf;

	*ptr++ = (uint8_t)rpc;
	*ptr++ = 0;
	*ptr++ = RAFT_PEER;
	*ptr++ = 0;
	memcpy(ptr, &length_n, sizeof(length_n));
}

static void fill_client_request_reply_payload(uint8_t *buf, raft_status_t status,
		raft_log_t log_type, uint32_t client_id, uint32_t seq)
{
	uint32_t client_id_n = htonl(client_id);
	uint32_t seq_n = htonl(seq);
	uint8_t *ptr = buf;

	*ptr++ = (uint8_t)status;
	*ptr++ = (uint8_t)log_type;
	memcpy(ptr, &client_id_n, sizeof(client_id_n));
	ptr += sizeof(client_id_n);
	memcpy(ptr, &seq_n, sizeof(seq_n));
}

START_TEST(test_new_conn_success)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	int fds[2];
	uint8_t packet[RAFT_HDR_SIZE + RAFT_HELLO_SIZE];
	int idx;

	ck_assert_int_eq(pipe(fds), 0);

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	init_entry_locks(&peers[1]);
	peers[0].server_id = 1;
	peers[0].peer_fd = -1;
	peers[1].server_id = 2;
	peers[1].peer_fd = -1;
	peers[1].port = htons(9999);
	*peers_ptr = peers;
	*num_ptr = 2;

	raft_state.self_id = 1;
	client_state->unknown_clients = NULL;

	fill_hello_packet(packet, 2, RAFT_PEER, RAFT_PEER, INADDR_LOOPBACK, 1883);
	ck_assert_int_eq(write(fds[1], packet, sizeof(packet)), (ssize_t)sizeof(packet));

	idx = raft_test_api.raft_new_conn(fds[0], NULL, NULL, 0);
	if (idx == -1 && errno == EAGAIN) {
		struct raft_host_entry *unknown = client_state->unknown_clients;
		ck_assert_ptr_nonnull(unknown);
		idx = raft_test_api.raft_new_conn(-1, unknown, NULL, 0);
	}

	ck_assert_int_eq(idx, 1);
	ck_assert_int_eq(peers[1].peer_fd, fds[0]);
	ck_assert_uint_eq(peers[1].mqtt_port, htons(1883));
	ck_assert_ptr_eq(client_state->unknown_clients, NULL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&peers[0]);
	destroy_entry_locks(&peers[1]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_new_conn_invalid_role)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	int fds[2];
	int saved_stderr = -1;
	uint8_t packet[RAFT_HDR_SIZE + RAFT_HELLO_SIZE];
	int rc;

	ck_assert_int_eq(pipe(fds), 0);

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	init_entry_locks(&peers[1]);
	peers[0].server_id = 1;
	peers[0].peer_fd = -1;
	peers[1].server_id = 2;
	peers[1].peer_fd = -1;
	peers[1].port = htons(9999);
	*peers_ptr = peers;
	*num_ptr = 2;

	raft_state.self_id = 1;
	client_state->unknown_clients = NULL;

	fill_hello_packet(packet, 2, RAFT_MAX_CONN, RAFT_PEER, INADDR_LOOPBACK, 1883);
	ck_assert_int_eq(write(fds[1], packet, sizeof(packet)), (ssize_t)sizeof(packet));

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	rc = raft_test_api.raft_new_conn(fds[0], NULL, NULL, 0);
	if (rc == -1 && errno == EAGAIN) {
		struct raft_host_entry *unknown = client_state->unknown_clients;
		ck_assert_ptr_nonnull(unknown);
		rc = raft_test_api.raft_new_conn(-1, unknown, NULL, 0);
	}
	restore_stderr(saved_stderr);

	ck_assert_int_eq(rc, -1);
	ck_assert_ptr_eq(client_state->unknown_clients, NULL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&peers[0]);
	destroy_entry_locks(&peers[1]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_new_conn_self_peer)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	int fds[2];
	int saved_stderr = -1;
	uint8_t packet[RAFT_HDR_SIZE + RAFT_HELLO_SIZE];
	int rc;

	ck_assert_int_eq(pipe(fds), 0);

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	init_entry_locks(&peers[1]);
	peers[0].server_id = 1;
	peers[0].peer_fd = -1;
	peers[1].server_id = 2;
	peers[1].peer_fd = -1;
	peers[1].port = htons(9999);
	*peers_ptr = peers;
	*num_ptr = 2;

	raft_state.self_id = 1;
	client_state->unknown_clients = NULL;

	fill_hello_packet(packet, 1, RAFT_PEER, RAFT_PEER, INADDR_LOOPBACK, 1883);
	ck_assert_int_eq(write(fds[1], packet, sizeof(packet)), (ssize_t)sizeof(packet));

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	rc = raft_test_api.raft_new_conn(fds[0], NULL, NULL, 0);
	if (rc == -1 && errno == EAGAIN) {
		struct raft_host_entry *unknown = client_state->unknown_clients;
		ck_assert_ptr_nonnull(unknown);
		rc = raft_test_api.raft_new_conn(-1, unknown, NULL, 0);
	}
	restore_stderr(saved_stderr);

	ck_assert_int_eq(rc, -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_ptr_eq(client_state->unknown_clients, NULL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&peers[0]);
	destroy_entry_locks(&peers[1]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_new_conn_invalid_type)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	int fds[2];
	int saved_stderr = -1;
	uint8_t packet[RAFT_HDR_SIZE + RAFT_HELLO_SIZE];
	int rc;

	ck_assert_int_eq(pipe(fds), 0);

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	init_entry_locks(&peers[1]);
	peers[0].server_id = 1;
	peers[0].peer_fd = -1;
	peers[1].server_id = 2;
	peers[1].peer_fd = -1;
	peers[1].port = htons(9999);
	*peers_ptr = peers;
	*num_ptr = 2;

	raft_state.self_id = 1;
	client_state->unknown_clients = NULL;

	fill_hello_packet(packet, 2, RAFT_PEER, RAFT_MAX_CONN, INADDR_LOOPBACK, 1883);
	ck_assert_int_eq(write(fds[1], packet, sizeof(packet)), (ssize_t)sizeof(packet));

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	rc = raft_test_api.raft_new_conn(fds[0], NULL, NULL, 0);
	if (rc == -1 && errno == EAGAIN) {
		struct raft_host_entry *unknown = client_state->unknown_clients;
		ck_assert_ptr_nonnull(unknown);
		rc = raft_test_api.raft_new_conn(-1, unknown, NULL, 0);
	}
	restore_stderr(saved_stderr);

	ck_assert_int_eq(rc, -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_ptr_eq(client_state->unknown_clients, NULL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&peers[0]);
	destroy_entry_locks(&peers[1]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_process_packet_client_request_reply_invalid_status)
{
	struct raft_host_entry client;
	uint8_t *buf;

	memset(&client, 0, sizeof(client));
	buf = malloc(RAFT_CLIENT_REQUEST_REPLY_SIZE);
	ck_assert_ptr_nonnull(buf);
	buf[0] = RAFT_MAX_STATUS;
	buf[1] = RAFT_LOG_REGISTER_TOPIC;
	memset(buf + 2, 0, RAFT_CLIENT_REQUEST_REPLY_SIZE - 2);

	client.rd_packet_buffer = buf;
	client.rd_packet_length = RAFT_CLIENT_REQUEST_REPLY_SIZE;
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_CLIENT_REQUEST_REPLY), -1);
	ck_assert_int_eq(errno, EINVAL);
	free(buf);
}
END_TEST

START_TEST(test_process_packet_client_request_invalid_type)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_CLIENT_REQUEST_SIZE];
	uint8_t *ptr = buf;
	uint32_t client_id = htonl(1);
	uint32_t seq = htonl(1);
	uint16_t len = htons(0);

	memcpy(ptr, &client_id, sizeof(client_id)); ptr += sizeof(client_id);
	memcpy(ptr, &seq, sizeof(seq)); ptr += sizeof(seq);
	*ptr++ = RAFT_MAX_LOG;
	*ptr++ = 0;
	memcpy(ptr, &len, sizeof(len));

	memset(&client, 0, sizeof(client));
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	raft_state.state = RAFT_STATE_LEADER;
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_CLIENT_REQUEST), -1);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_process_packet_client_request_no_handler)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_CLIENT_REQUEST_SIZE];
	uint8_t *ptr = buf;
	uint32_t client_id = htonl(1);
	uint32_t seq = htonl(9);
	uint16_t len = htons(0);
	int fds[2];
	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

	memcpy(ptr, &client_id, sizeof(client_id)); ptr += sizeof(client_id);
	memcpy(ptr, &seq, sizeof(seq)); ptr += sizeof(seq);
	*ptr++ = RAFT_LOG_REGISTER_TOPIC;
	*ptr++ = 0;
	memcpy(ptr, &len, sizeof(len));

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	raft_state.state = RAFT_STATE_LEADER;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_CLIENT_REQUEST), 0);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_process_packet_client_request_not_leader_reply)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_CLIENT_REQUEST_SIZE];
	uint8_t *ptr = buf;
	uint32_t client_id = htonl(1);
	uint32_t seq = htonl(2);
	uint16_t len = htons(0);
	int fds[2];
	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

	memcpy(ptr, &client_id, sizeof(client_id)); ptr += sizeof(client_id);
	memcpy(ptr, &seq, sizeof(seq)); ptr += sizeof(seq);
	*ptr++ = RAFT_LOG_REGISTER_TOPIC;
	*ptr++ = 0;
	memcpy(ptr, &len, sizeof(len));

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	raft_state.state = RAFT_STATE_FOLLOWER;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_CLIENT_REQUEST), 0);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_process_packet_register_client_not_leader)
{
	struct raft_host_entry client;
	int fds[2];
	ck_assert_int_eq(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];
	client.rd_packet_buffer = NULL;
	client.rd_packet_length = 0;

	raft_state.state = RAFT_STATE_FOLLOWER;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_REGISTER_CLIENT), 0);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_process_packet_append_entries_short_payload)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_APPEND_ENTRIES_FIXED_SIZE];
	uint8_t *ptr = buf;
	uint32_t term = htonl(1);
	uint32_t leader_id = htonl(2);
	uint32_t prev_log_index = htonl(0);
	uint32_t prev_log_term = htonl(0);
	uint32_t leader_commit = htonl(0);
	uint32_t num_entries = htonl(1);

	memcpy(ptr, &term, sizeof(term)); ptr += sizeof(term);
	memcpy(ptr, &leader_id, sizeof(leader_id)); ptr += sizeof(leader_id);
	memcpy(ptr, &prev_log_index, sizeof(prev_log_index)); ptr += sizeof(prev_log_index);
	memcpy(ptr, &prev_log_term, sizeof(prev_log_term)); ptr += sizeof(prev_log_term);
	memcpy(ptr, &leader_commit, sizeof(leader_commit)); ptr += sizeof(leader_commit);
	memcpy(ptr, &num_entries, sizeof(num_entries));

	memset(&client, 0, sizeof(client));
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_APPEND_ENTRIES), -1);
}
END_TEST

START_TEST(test_process_packet_append_entries_num_entries_overflow)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_APPEND_ENTRIES_FIXED_SIZE];
	uint8_t *ptr = buf;
	uint32_t term = htonl(1);
	uint32_t leader_id = htonl(2);
	uint32_t prev_log_index = htonl(0);
	uint32_t prev_log_term = htonl(0);
	uint32_t leader_commit = htonl(0);
	uint32_t num_entries = htonl(0xFFFFFFFFU);

	memcpy(ptr, &term, sizeof(term)); ptr += sizeof(term);
	memcpy(ptr, &leader_id, sizeof(leader_id)); ptr += sizeof(leader_id);
	memcpy(ptr, &prev_log_index, sizeof(prev_log_index)); ptr += sizeof(prev_log_index);
	memcpy(ptr, &prev_log_term, sizeof(prev_log_term)); ptr += sizeof(prev_log_term);
	memcpy(ptr, &leader_commit, sizeof(leader_commit)); ptr += sizeof(leader_commit);
	memcpy(ptr, &num_entries, sizeof(num_entries));

	memset(&client, 0, sizeof(client));
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_APPEND_ENTRIES), -1);
	ck_assert_int_eq(errno, EOVERFLOW);
}
END_TEST

START_TEST(test_process_packet_append_entries_handler_error)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_APPEND_ENTRIES_FIXED_SIZE + RAFT_LOG_FIXED_SIZE];
	uint8_t *ptr = buf;
	uint32_t term = htonl(1);
	uint32_t leader_id = htonl(2);
	uint32_t prev_log_index = htonl(0);
	uint32_t prev_log_term = htonl(0);
	uint32_t leader_commit = htonl(0);
	uint32_t num_entries = htonl(1);
	uint32_t index = htonl(1);
	uint32_t entry_term = htonl(1);
	uint16_t entry_len = htons(0);
	const struct raft_impl *saved_impl = raft_impl;

	memcpy(ptr, &term, sizeof(term)); ptr += sizeof(term);
	memcpy(ptr, &leader_id, sizeof(leader_id)); ptr += sizeof(leader_id);
	memcpy(ptr, &prev_log_index, sizeof(prev_log_index)); ptr += sizeof(prev_log_index);
	memcpy(ptr, &prev_log_term, sizeof(prev_log_term)); ptr += sizeof(prev_log_term);
	memcpy(ptr, &leader_commit, sizeof(leader_commit)); ptr += sizeof(leader_commit);
	memcpy(ptr, &num_entries, sizeof(num_entries)); ptr += sizeof(num_entries);
	*ptr++ = RAFT_LOG_REGISTER_TOPIC;
	*ptr++ = 0;
	memcpy(ptr, &index, sizeof(index)); ptr += sizeof(index);
	memcpy(ptr, &entry_term, sizeof(entry_term)); ptr += sizeof(entry_term);
	memcpy(ptr, &entry_len, sizeof(entry_len));

	memset(&client, 0, sizeof(client));
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	raft_impl = &fail_impl;
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_APPEND_ENTRIES), -1);
	ck_assert_int_eq(errno, EIO);
	raft_impl = saved_impl;
}
END_TEST

START_TEST(test_process_packet_client_request_handler_error)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_CLIENT_REQUEST_SIZE];
	uint8_t *ptr = buf;
	uint32_t client_id = htonl(1);
	uint32_t seq = htonl(1);
	uint16_t len = htons(0);
	const struct raft_impl *saved_impl = raft_impl;

	memcpy(ptr, &client_id, sizeof(client_id)); ptr += sizeof(client_id);
	memcpy(ptr, &seq, sizeof(seq)); ptr += sizeof(seq);
	*ptr++ = RAFT_LOG_REGISTER_TOPIC;
	*ptr++ = 0;
	memcpy(ptr, &len, sizeof(len));

	memset(&client, 0, sizeof(client));
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	raft_state.state = RAFT_STATE_LEADER;
	raft_impl = &fail_impl;
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_CLIENT_REQUEST), -1);
	ck_assert_int_eq(errno, EIO);
	raft_impl = saved_impl;
}
END_TEST

START_TEST(test_process_packet_request_vote_updates_term)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	struct raft_host_entry client;
	uint8_t buf[RAFT_REQUEST_VOTE_SIZE];
	uint32_t term = htonl(5);
	uint32_t candidate_id = htonl(2);
	uint32_t last_log_index = htonl(0);
	uint32_t last_log_term = htonl(0);
	int fds[2];
	uint8_t read_buf[64];

	ck_assert_int_eq(pipe(fds), 0);

	peers = calloc(1, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	peers[0].server_id = 1;
	*peers_ptr = peers;
	*num_ptr = 1;

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	memcpy(buf, &term, sizeof(term));
	memcpy(buf + 4, &candidate_id, sizeof(candidate_id));
	memcpy(buf + 8, &last_log_index, sizeof(last_log_index));
	memcpy(buf + 12, &last_log_term, sizeof(last_log_term));

	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_term = 1;

	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_REQUEST_VOTE), 0);
	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert_uint_eq(raft_state.current_term, 5);

	{
		ssize_t rc = read(fds[0], read_buf, sizeof(read_buf));
		ck_assert_int_ne(rc, -1);
	}
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	destroy_entry_locks(&peers[0]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_recv_invalid_header_length)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];
	uint32_t length = htonl(RAFT_HDR_SIZE - 1);

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	header[0] = RAFT_HELLO;
	header[1] = 0;
	header[2] = RAFT_PEER;
	header[3] = 0;
	memcpy(header + 4, &length, sizeof(length));

	ck_assert_int_eq(write(fds[1], header, sizeof(header)), (ssize_t)sizeof(header));
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	ck_assert_int_eq(client.peer_fd, -1);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_EMPTY);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_invalid_rpc)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_MAX_RPC, RAFT_HDR_SIZE);

	ck_assert_int_eq(write(fds[1], header, sizeof(header)), (ssize_t)sizeof(header));
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_int_eq(client.peer_fd, -1);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_header_too_large)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_HELLO, 0xFFFFFFFFU);

	ck_assert_int_eq(write(fds[1], header, sizeof(header)), (ssize_t)sizeof(header));
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	ck_assert_int_eq(errno, EOVERFLOW);
	ck_assert_int_eq(client.peer_fd, -1);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_header_min_size)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_HELLO, RAFT_HDR_SIZE + RAFT_HELLO_SIZE - 1);

	ck_assert_int_eq(write(fds[1], header, sizeof(header)), (ssize_t)sizeof(header));
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	ck_assert_int_eq(errno, EBADMSG);
	ck_assert_int_eq(client.peer_fd, -1);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_header_max_size)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_CLIENT_REQUEST_REPLY,
			RAFT_HDR_SIZE + RAFT_CLIENT_REQUEST_REPLY_SIZE + 1);

	ck_assert_int_eq(write(fds[1], header, sizeof(header)), (ssize_t)sizeof(header));
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	ck_assert_int_eq(errno, EMSGSIZE);
	ck_assert_int_eq(client.peer_fd, -1);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_invalid_state)
{
	struct raft_host_entry client;
	int fds[2];
	int saved_stderr = -1;

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = 99;

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	restore_stderr(saved_stderr);

	ck_assert_int_eq(errno, EINVAL);
	ck_assert_int_eq(client.peer_fd, -1);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_bad_fd)
{
	struct raft_host_entry client;

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = -1;
	client.rd_state = RAFT_PCK_NEW;

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	ck_assert_int_eq(errno, EBADF);
	ck_assert_int_eq(client.peer_fd, -1);

	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_header_partial_then_complete)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];
	uint8_t payload[RAFT_CLIENT_REQUEST_REPLY_SIZE];
	uint8_t packet[RAFT_HDR_SIZE + RAFT_CLIENT_REQUEST_REPLY_SIZE];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_CLIENT_REQUEST_REPLY,
			RAFT_HDR_SIZE + RAFT_CLIENT_REQUEST_REPLY_SIZE);
	fill_client_request_reply_payload(payload, RAFT_OK, RAFT_LOG_REGISTER_TOPIC, 1, 1);
	memcpy(packet, header, sizeof(header));
	memcpy(packet + sizeof(header), payload, sizeof(payload));

	ck_assert_int_eq(write(fds[1], packet, 3), 3);
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_HEADER);
	ck_assert_int_eq(client.rd_need, RAFT_HDR_SIZE - 3);

	ck_assert_int_eq(write(fds[1], packet + 3, sizeof(packet) - 3),
			(ssize_t)(sizeof(packet) - 3));
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_EMPTY);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_payload_partial)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];
	uint8_t payload[RAFT_CLIENT_REQUEST_REPLY_SIZE];
	uint8_t packet[RAFT_HDR_SIZE + RAFT_CLIENT_REQUEST_REPLY_SIZE];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_CLIENT_REQUEST_REPLY,
			RAFT_HDR_SIZE + RAFT_CLIENT_REQUEST_REPLY_SIZE);
	fill_client_request_reply_payload(payload, RAFT_OK, RAFT_LOG_REGISTER_TOPIC, 1, 1);
	memcpy(packet, header, sizeof(header));
	memcpy(packet + sizeof(header), payload, sizeof(payload));

	ck_assert_int_eq(write(fds[1], packet, RAFT_HDR_SIZE + 2),
			(ssize_t)(RAFT_HDR_SIZE + 2));
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_PACKET);
	ck_assert_int_eq(client.rd_need, RAFT_CLIENT_REQUEST_REPLY_SIZE - 2);

	ck_assert_int_eq(write(fds[1], packet + RAFT_HDR_SIZE + 2,
				RAFT_CLIENT_REQUEST_REPLY_SIZE - 2),
			(ssize_t)(RAFT_CLIENT_REQUEST_REPLY_SIZE - 2));
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_EMPTY);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_header_eagain)
{
	struct raft_host_entry client;
	int fds[2];
	int flags;

	ck_assert_int_eq(pipe(fds), 0);
	flags = fcntl(fds[0], F_GETFL, 0);
	ck_assert_int_ne(flags, -1);
	ck_assert_int_ne(fcntl(fds[0], F_SETFL, flags | O_NONBLOCK), -1);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(errno, EAGAIN);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_HEADER);
	ck_assert_int_eq(client.peer_fd, fds[0]);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_payload_eagain)
{
	struct raft_host_entry client;
	int fds[2];
	int flags;
	uint8_t header[RAFT_HDR_SIZE];
	uint8_t payload[RAFT_CLIENT_REQUEST_REPLY_SIZE];

	ck_assert_int_eq(pipe(fds), 0);
	flags = fcntl(fds[0], F_GETFL, 0);
	ck_assert_int_ne(flags, -1);
	ck_assert_int_ne(fcntl(fds[0], F_SETFL, flags | O_NONBLOCK), -1);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_CLIENT_REQUEST_REPLY,
			RAFT_HDR_SIZE + RAFT_CLIENT_REQUEST_REPLY_SIZE);
	fill_client_request_reply_payload(payload, RAFT_OK, RAFT_LOG_REGISTER_TOPIC, 1, 1);

	ck_assert_int_eq(write(fds[1], header, sizeof(header)), (ssize_t)sizeof(header));
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(errno, EAGAIN);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_PACKET);
	ck_assert_int_eq(client.rd_need, RAFT_CLIENT_REQUEST_REPLY_SIZE);

	ck_assert_int_eq(write(fds[1], payload, sizeof(payload)), (ssize_t)sizeof(payload));
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_EMPTY);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_header_eof)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	close(fds[1]);
	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(client.peer_fd, -1);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_EMPTY);

	close(fds[0]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_payload_eof)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t header[RAFT_HDR_SIZE];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	fill_header(header, RAFT_CLIENT_REQUEST_REPLY,
			RAFT_HDR_SIZE + RAFT_CLIENT_REQUEST_REPLY_SIZE);
	ck_assert_int_eq(write(fds[1], header, sizeof(header)), (ssize_t)sizeof(header));
	close(fds[1]);

	ck_assert_int_eq(raft_test_api.raft_recv(&client), 0);
	ck_assert_int_eq(client.peer_fd, -1);
	ck_assert_int_eq(client.rd_state, RAFT_PCK_EMPTY);

	close(fds[0]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_header_read_error)
{
	struct raft_host_entry client;
	int fds[2];
	int saved_stderr = -1;

	ck_assert_int_eq(pipe(fds), 0);
	close(fds[0]);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_NEW;

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	restore_stderr(saved_stderr);
	ck_assert_int_eq(errno, EBADF);
	ck_assert_int_eq(client.peer_fd, -1);

	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_recv_payload_read_error)
{
	struct raft_host_entry client;
	int fds[2];
	int saved_stderr = -1;

	ck_assert_int_eq(pipe(fds), 0);
	close(fds[0]);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[0];
	client.rd_state = RAFT_PCK_PACKET;
	client.rd_packet_buffer = malloc(4);
	ck_assert_ptr_nonnull(client.rd_packet_buffer);
	client.rd_need = 4;
	client.rd_offset = 0;
	client.rd_packet_length = 4;

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_recv(&client), -1);
	restore_stderr(saved_stderr);
	ck_assert_int_eq(errno, EBADF);
	ck_assert_int_eq(client.peer_fd, -1);

	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_invalid_rpc)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_SERVER, &client, RAFT_MAX_RPC), -1);
	ck_assert_int_eq(errno, EINVAL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_client_no_leader)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	client_state->current_leader = NULL;
	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_CLIENT, NULL, RAFT_HELLO,
				1U, (raft_conn_t)RAFT_CLIENT), -1);
	ck_assert_int_eq(errno, EBADF);
}
END_TEST

START_TEST(test_send_server_no_client)
{
	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_SERVER, NULL, RAFT_HELLO,
				1U, (raft_conn_t)RAFT_CLIENT), -1);
	ck_assert_int_eq(errno, EBADF);
}
END_TEST

START_TEST(test_send_peer_bad_fd)
{
	struct raft_host_entry client;

	memset(&client, 0, sizeof(client));
	client.peer_fd = -1;
	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_PEER, &client, RAFT_HELLO,
				1U, (raft_conn_t)RAFT_PEER), -1);
	ck_assert_int_eq(errno, EBADF);
}
END_TEST

START_TEST(test_send_append_entries_null_entries)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_PEER, &client, RAFT_APPEND_ENTRIES,
				1U, 1U, 0U, 0U, 0U, 1U, NULL), -1);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_client_request_invalid_type)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_SERVER, &client, RAFT_CLIENT_REQUEST,
				1U, 1U, (raft_log_t)RAFT_MAX_LOG, (struct raft_log *)NULL), -1);
	ck_assert_int_eq(errno, EINVAL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_client_request_reply_invalid_log_type)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_SERVER, &client, RAFT_CLIENT_REQUEST_REPLY,
				RAFT_OK, RAFT_MAX_LOG, 1, 1), -1);
	ck_assert_int_eq(errno, EINVAL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_request_vote_reply_invalid_status)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_SERVER, &client, RAFT_REQUEST_VOTE_REPLY,
				RAFT_MAX_STATUS, 1, 0), -1);
	ck_assert_int_eq(errno, EINVAL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_append_entries_invalid_log_event)
{
	struct raft_host_entry client;
	struct raft_log *entry;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	entry = make_log(1, 1);
	entry->event = RAFT_MAX_LOG;

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_PEER, &client, RAFT_APPEND_ENTRIES,
				1, 1, 0, 0, 0, 1, entry), -1);
	ck_assert_int_eq(errno, EINVAL);

	entry->event = RAFT_LOG_REGISTER_TOPIC;
	raft_test_api.raft_free_log(entry);
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_client_request_pre_send_overflow)
{
	struct raft_host_entry client;
	struct raft_log *entry;
	const struct raft_impl *saved_impl = raft_impl;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	entry = make_log(1, 1);

	raft_impl = &pre_send_overflow_impl;
	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_SERVER, &client, RAFT_CLIENT_REQUEST,
				1U, 1U, RAFT_LOG_REGISTER_TOPIC, entry), -1);
	ck_assert_int_eq(errno, EOVERFLOW);
	raft_impl = saved_impl;

	raft_test_api.raft_free_log(entry);
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_append_entries_pre_send_overflow)
{
	struct raft_host_entry client;
	struct raft_log *entry;
	const struct raft_impl *saved_impl = raft_impl;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	entry = make_log(1, 1);

	raft_impl = &pre_send_overflow_impl;
	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_PEER, &client, RAFT_APPEND_ENTRIES,
				1U, 1U, 0U, 0U, 0U, 1U, entry), -1);
	ck_assert_int_eq(errno, EOVERFLOW);
	raft_impl = saved_impl;

	raft_test_api.raft_free_log(entry);
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_update_leader_id)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	peers[0].server_id = 1;
	peers[1].server_id = 2;
	*peers_ptr = peers;
	*num_ptr = 2;

	client_state->current_leader_id = 0;
	client_state->current_leader = NULL;

	ck_assert_int_eq(raft_test_api.raft_update_leader_id(2, false), 0);
	ck_assert_uint_eq(client_state->current_leader_id, 2);
	ck_assert_ptr_eq(client_state->current_leader, &peers[1]);

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_update_leader_id(99, false), -1);
	ck_assert_int_eq(errno, ENOENT);

	ck_assert_int_eq(raft_test_api.raft_update_leader_id(NULL_ID, false), 0);
	ck_assert_uint_eq(client_state->current_leader_id, NULL_ID);
	ck_assert_ptr_eq(client_state->current_leader, NULL);
}
END_TEST

START_TEST(test_close_resets_state)
{
	struct raft_host_entry entry;

	memset(&entry, 0, sizeof(entry));
	init_entry_locks(&entry);
	entry.peer_fd = 5;
	entry.server_id = 1;
	entry.rd_state = RAFT_PCK_PACKET;
	entry.rd_offset = 3;
	entry.rd_need = 7;
	entry.rd_packet_length = 22;
	entry.rd_packet_buffer = malloc(16);
	entry.wr_offset = 2;
	entry.wr_need = 6;
	entry.wr_packet_length = 20;
	{
		const uint8_t *buf = (const uint8_t *)malloc(16);
		ck_assert_ptr_nonnull(buf);
		__atomic_store_n(&entry.wr_packet_buffer, buf, __ATOMIC_SEQ_CST);
	}
	{
		const uint8_t *buf = (const uint8_t *)malloc(8);
		ck_assert_ptr_nonnull(buf);
		__atomic_store_n(&entry.ss_data, buf, __ATOMIC_SEQ_CST);
	}
	entry.ss_last_index = 4;
	entry.ss_last_term = 2;

	ck_assert_ptr_nonnull(entry.rd_packet_buffer);
	ck_assert_ptr_nonnull(__atomic_load_n(&entry.wr_packet_buffer, __ATOMIC_SEQ_CST));
	ck_assert_ptr_nonnull(__atomic_load_n(&entry.ss_data, __ATOMIC_SEQ_CST));

	raft_state.self_id = 1;
	raft_state.log_tail = make_log(10, 1);

	ck_assert_int_eq(raft_test_api.raft_close(&entry), 0);
	ck_assert_int_eq(entry.peer_fd, -1);
	ck_assert_int_eq(entry.rd_state, RAFT_PCK_NEW);
	ck_assert_int_eq(entry.rd_offset, 0);
	ck_assert_int_eq(entry.rd_need, 0);
	ck_assert_int_eq(entry.rd_packet_length, 0);
	ck_assert_ptr_eq(entry.rd_packet_buffer, NULL);
	ck_assert_int_eq(entry.wr_offset, 0);
	ck_assert_int_eq(entry.wr_need, 0);
	ck_assert_int_eq(entry.wr_packet_length, 0);
	ck_assert_ptr_eq(__atomic_load_n(&entry.wr_packet_buffer, __ATOMIC_SEQ_CST), NULL);
	ck_assert_ptr_eq(__atomic_load_n(&entry.ss_data, __ATOMIC_SEQ_CST), NULL);
	ck_assert_int_eq(entry.ss_last_index, 0);
	ck_assert_int_eq(entry.ss_last_term, 0);

	raft_test_api.raft_free_log(raft_state.log_tail);
	raft_state.log_tail = NULL;
	destroy_entry_locks(&entry);
}
END_TEST

START_TEST(test_log_at_indices)
{
	ck_assert_ptr_eq(raft_test_api.raft_log_at(0), NULL);
	ck_assert_int_eq(errno, 0);

	errno = 0;
	ck_assert_ptr_eq(raft_test_api.raft_log_at(-1U), NULL);
	ck_assert_int_eq(errno, EINVAL);

	raft_test_api.raft_append_log(make_log(2, 10),
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);

	errno = 0;
	ck_assert_ptr_eq(raft_test_api.raft_log_at(1), NULL);
	ck_assert_int_eq(errno, ENOENT);
	ck_assert_ptr_nonnull(raft_test_api.raft_log_at(2));
}
END_TEST

START_TEST(test_term_at_indices)
{
	errno = 0;
	ck_assert_uint_eq(raft_test_api.raft_term_at(0), 0);
	ck_assert_int_eq(errno, 0);

	errno = 0;
	ck_assert_uint_eq(raft_test_api.raft_term_at(-1U), (uint32_t)-1);
	ck_assert_int_eq(errno, EINVAL);

	raft_test_api.raft_append_log(make_log(1, 42),
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);
	errno = 0;
	ck_assert_uint_eq(raft_test_api.raft_term_at(2), (uint32_t)-1);
	ck_assert_int_eq(errno, ENOENT);
	ck_assert_uint_eq(raft_test_api.raft_term_at(1), 42);
}
END_TEST

START_TEST(test_free_log_unknown_event)
{
	struct raft_log *entry;

	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->event = RAFT_MAX_LOG;

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_free_log(entry), -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_int_eq(free_log_called, 0);
}
END_TEST

START_TEST(test_update_term)
{
	char tmpdir[PATH_MAX];
	int saved_cwd_fd = -1;

	ck_assert_int_eq(enter_temp_dir(tmpdir, sizeof(tmpdir), &saved_cwd_fd), 0);

	raft_state.self_id = 1;
	raft_state.current_term = 5;
	raft_state.voted_for = 123;

	ck_assert_int_eq(raft_test_api.raft_update_term(4), 0);
	ck_assert_uint_eq(raft_state.current_term, 5);
	ck_assert_uint_eq(raft_state.voted_for, 123);

	ck_assert_int_eq(raft_test_api.raft_update_term(6), 0);
	ck_assert_uint_eq(raft_state.current_term, 6);
	ck_assert_uint_eq(raft_state.voted_for, NULL_ID);

	leave_temp_dir(tmpdir, saved_cwd_fd);
}
END_TEST

START_TEST(test_append_remove_prepend_log)
{
	struct raft_log *head = NULL;
	struct raft_log *tail = NULL;
	_Atomic long log_len = 0;
	struct raft_log *a = make_log(1, 1);
	struct raft_log *b = make_log(2, 1);
	struct raft_log *c = make_log(0, 1);

	ck_assert_int_eq(raft_test_api.raft_append_log(a, &head, &tail, &log_len), 0);
	ck_assert_ptr_eq(head, a);
	ck_assert_ptr_eq(tail, a);
	ck_assert_int_eq(atomic_load(&log_len), 1);

	ck_assert_int_eq(raft_test_api.raft_append_log(b, &head, &tail, &log_len), 0);
	ck_assert_ptr_eq(head, a);
	ck_assert_ptr_eq(tail, b);
	ck_assert_ptr_eq(a->next, b);
	ck_assert_int_eq(atomic_load(&log_len), 2);

	ck_assert_int_eq(raft_test_api.raft_prepend_log(c, &head, &tail, &log_len), 0);
	ck_assert_ptr_eq(head, c);
	ck_assert_ptr_eq(tail, b);
	ck_assert_int_eq(atomic_load(&log_len), 3);

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_remove_log(b, &head, &tail, NULL, &log_len), 0);
	ck_assert_int_eq(atomic_load(&log_len), 2);
	ck_assert_ptr_eq(tail, a);

	ck_assert_int_eq(raft_test_api.raft_remove_log(b, &head, &tail, NULL, &log_len), -1);
	ck_assert_int_eq(errno, ENOENT);

	raft_test_api.raft_free_log(a);
	raft_test_api.raft_free_log(b);
	raft_test_api.raft_free_log(c);
}
END_TEST

START_TEST(test_commit_and_advance_errors)
{
	raft_state.commit_index = 0;
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_commit_and_advance(), -1);
	ck_assert_int_eq(errno, ERANGE);
}
END_TEST

START_TEST(test_commit_and_advance_unknown_event)
{
	struct raft_log *entry;

	entry = make_log(1, 1);
	entry->event = RAFT_MAX_LOG;
	raft_test_api.raft_append_log(entry,
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);

	raft_state.commit_index = 0;
	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_commit_and_advance(), -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_uint_eq(raft_state.commit_index, 0);
	ck_assert_int_eq(commit_called, 0);

	entry->event = RAFT_LOG_REGISTER_TOPIC;
}
END_TEST

START_TEST(test_commit_and_advance_success)
{
	raft_state.commit_index = 0;
	raft_test_api.raft_append_log(make_log(1, 1),
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);

	ck_assert_int_eq(raft_test_api.raft_commit_and_advance(), 0);
	ck_assert_uint_eq(raft_state.commit_index, 1);
	ck_assert_int_eq(commit_called, 1);
}
END_TEST

START_TEST(test_check_commit_index_majority)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;

	peers = calloc(3, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	*peers_ptr = peers;
	*num_ptr = 3;

	peers[1].match_index = 2;
	peers[2].match_index = 1;

	raft_state.current_term = 3;
	raft_state.commit_index = 0;

	raft_test_api.raft_append_log(make_log(1, 3),
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);
	raft_test_api.raft_append_log(make_log(2, 3),
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);

	ck_assert_int_eq(raft_test_api.raft_check_commit_index(2), 2);
	ck_assert_uint_eq(raft_state.commit_index, 2);
	ck_assert_int_eq(commit_called, 2);
}
END_TEST

START_TEST(test_check_commit_index_no_majority)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;

	peers = calloc(3, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	*peers_ptr = peers;
	*num_ptr = 3;

	peers[1].match_index = 0;
	peers[2].match_index = 0;

	raft_state.current_term = 7;
	raft_state.commit_index = 0;

	raft_test_api.raft_append_log(make_log(1, 7),
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);
	raft_test_api.raft_append_log(make_log(2, 7),
			&raft_state.log_head, &raft_state.log_tail, &raft_state.log_length);

	ck_assert_int_eq(raft_test_api.raft_check_commit_index(2), 0);
	ck_assert_uint_eq(raft_state.commit_index, 0);
	ck_assert_int_eq(commit_called, 0);
}
END_TEST

START_TEST(test_client_log_sendv_invalid_event)
{
	errno = 0;
	ck_assert_int_eq(call_client_log_sendv(RAFT_MAX_LOG), -1);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_send_register_client_reply_invalid_status)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_SERVER, &client, RAFT_REGISTER_CLIENT_REPLY,
				RAFT_MAX_STATUS, 1, 0), -1);
	ck_assert_int_eq(errno, EINVAL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_send_hello_invalid_conn_type)
{
	struct raft_host_entry client;
	int fds[2];

	ck_assert_int_eq(pipe(fds), 0);

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	errno = 0;
	ck_assert_int_eq(raft_send(RAFT_PEER, &client, RAFT_HELLO,
				1, RAFT_MAX_CONN), -1);
	ck_assert_int_eq(errno, EINVAL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
}
END_TEST

START_TEST(test_new_conn_not_hello)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	int fds[2];
	int saved_stderr = -1;
	uint8_t packet[RAFT_HDR_SIZE + RAFT_HELLO_SIZE];
	int rc;

	ck_assert_int_eq(pipe(fds), 0);

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	init_entry_locks(&peers[1]);
	peers[0].server_id = 1;
	peers[0].peer_fd = -1;
	peers[1].server_id = 2;
	peers[1].peer_fd = -1;
	peers[1].port = htons(9999);
	*peers_ptr = peers;
	*num_ptr = 2;

	raft_state.self_id = 1;
	client_state->unknown_clients = NULL;

	fill_hello_packet(packet, 2, RAFT_PEER, RAFT_PEER, INADDR_LOOPBACK, 1883);
	packet[0] = RAFT_REQUEST_VOTE;
	ck_assert_int_eq(write(fds[1], packet, sizeof(packet)), (ssize_t)sizeof(packet));

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	rc = raft_test_api.raft_new_conn(fds[0], NULL, NULL, 0);
	if (rc == -1 && errno == EAGAIN) {
		struct raft_host_entry *unknown = client_state->unknown_clients;
		ck_assert_ptr_nonnull(unknown);
		rc = raft_test_api.raft_new_conn(-1, unknown, NULL, 0);
	}
	restore_stderr(saved_stderr);

	ck_assert_int_eq(rc, -1);
	ck_assert_int_eq(errno, EINVAL);
	ck_assert_ptr_eq(client_state->unknown_clients, NULL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&peers[0]);
	destroy_entry_locks(&peers[1]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_new_conn_bad_length)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	int fds[2];
	int saved_stderr = -1;
	uint8_t packet[RAFT_HDR_SIZE + RAFT_HELLO_SIZE];
	uint32_t length = htonl(RAFT_HDR_SIZE + RAFT_HELLO_SIZE + 1);
	int rc;

	ck_assert_int_eq(pipe(fds), 0);

	peers = calloc(2, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);
	init_entry_locks(&peers[0]);
	init_entry_locks(&peers[1]);
	peers[0].server_id = 1;
	peers[0].peer_fd = -1;
	peers[1].server_id = 2;
	peers[1].peer_fd = -1;
	peers[1].port = htons(9999);
	*peers_ptr = peers;
	*num_ptr = 2;

	raft_state.self_id = 1;
	client_state->unknown_clients = NULL;

	fill_hello_packet(packet, 2, RAFT_PEER, RAFT_PEER, INADDR_LOOPBACK, 1883);
	memcpy(packet + 4, &length, sizeof(length));
	ck_assert_int_eq(write(fds[1], packet, sizeof(packet)), (ssize_t)sizeof(packet));

	ck_assert_int_eq(redirect_stderr_to_null(&saved_stderr), 0);
	rc = raft_test_api.raft_new_conn(fds[0], NULL, NULL, 0);
	if (rc == -1 && errno == EAGAIN) {
		struct raft_host_entry *unknown = client_state->unknown_clients;
		ck_assert_ptr_nonnull(unknown);
		rc = raft_test_api.raft_new_conn(-1, unknown, NULL, 0);
	}
	restore_stderr(saved_stderr);

	ck_assert_int_eq(rc, -1);
	ck_assert_int_eq(errno, EFBIG);
	ck_assert_ptr_eq(client_state->unknown_clients, NULL);

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&peers[0]);
	destroy_entry_locks(&peers[1]);
	free(peers);
	*peers_ptr = NULL;
	*num_ptr = 1;
}
END_TEST

START_TEST(test_process_packet_append_entries_invalid_log_type)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_APPEND_ENTRIES_FIXED_SIZE + RAFT_LOG_FIXED_SIZE];
	uint8_t *ptr = buf;
	uint32_t term = htonl(1);
	uint32_t leader_id = htonl(2);
	uint32_t prev_log_index = htonl(0);
	uint32_t prev_log_term = htonl(0);
	uint32_t leader_commit = htonl(0);
	uint32_t num_entries = htonl(1);
	uint32_t index = htonl(1);
	uint32_t entry_term = htonl(1);
	uint16_t entry_len = htons(0);

	memcpy(ptr, &term, sizeof(term)); ptr += sizeof(term);
	memcpy(ptr, &leader_id, sizeof(leader_id)); ptr += sizeof(leader_id);
	memcpy(ptr, &prev_log_index, sizeof(prev_log_index)); ptr += sizeof(prev_log_index);
	memcpy(ptr, &prev_log_term, sizeof(prev_log_term)); ptr += sizeof(prev_log_term);
	memcpy(ptr, &leader_commit, sizeof(leader_commit)); ptr += sizeof(leader_commit);
	memcpy(ptr, &num_entries, sizeof(num_entries)); ptr += sizeof(num_entries);
	*ptr++ = RAFT_MAX_LOG;
	*ptr++ = 0;
	memcpy(ptr, &index, sizeof(index)); ptr += sizeof(index);
	memcpy(ptr, &entry_term, sizeof(entry_term)); ptr += sizeof(entry_term);
	memcpy(ptr, &entry_len, sizeof(entry_len));

	memset(&client, 0, sizeof(client));
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	errno = 0;
	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_APPEND_ENTRIES), -1);
	ck_assert_int_eq(errno, EINVAL);
}
END_TEST

START_TEST(test_process_packet_client_request_overlong)
{
	struct raft_host_entry client;
	uint8_t buf[RAFT_CLIENT_REQUEST_SIZE];
	uint8_t *ptr = buf;
	uint32_t client_id = htonl(1);
	uint32_t seq = htonl(1);
	uint16_t len = htons(8);

	memcpy(ptr, &client_id, sizeof(client_id)); ptr += sizeof(client_id);
	memcpy(ptr, &seq, sizeof(seq)); ptr += sizeof(seq);
	*ptr++ = RAFT_LOG_REGISTER_TOPIC;
	*ptr++ = 0;
	memcpy(ptr, &len, sizeof(len));

	memset(&client, 0, sizeof(client));
	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	ck_assert_int_eq(raft_test_api.raft_process_packet(&client, RAFT_CLIENT_REQUEST), -1);
}
END_TEST

START_TEST(test_client_log_append_single_conflict)
{
	struct raft_log *existing;
	struct raft_log *incoming;

	existing = make_log(1, 1);
	raft_state.log_head = existing;
	raft_state.log_tail = existing;
	atomic_store(&raft_state.log_length, 1);

	incoming = make_log(1, 2);

	ck_assert_int_eq(raft_test_api.raft_client_log_append_single(incoming, 0), 1);
	ck_assert_ptr_nonnull(raft_state.log_head);
	ck_assert_ptr_eq(raft_state.log_head, incoming);
	ck_assert_uint_eq(raft_state.log_head->term, 2);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 1);

	raft_test_api.raft_free_log(incoming);
	raft_state.log_head = NULL;
	raft_state.log_tail = NULL;
}
END_TEST

static Suite *raft_suite(void)
{
	Suite *s = suite_create("raft");
	TCase *tc = tcase_create("core");

	tcase_add_checked_fixture(tc, setup, teardown);

	tcase_add_test(tc, test_parse_cmdline_host_list_valid);
	tcase_add_test(tc, test_parse_cmdline_host_list_invalid_id);
	tcase_add_test(tc, test_parse_cmdline_host_list_invalid_port);
	tcase_add_test(tc, test_parse_cmdline_host_list_invalid_ip);
	tcase_add_test(tc, test_parse_cmdline_host_list_any_addr);
	tcase_add_test(tc, test_reset_election_timer_future);
	tcase_add_test(tc, test_reset_next_ping_future);
	tcase_add_test(tc, test_change_to_leader_updates_peers);
	tcase_add_test(tc, test_change_to_invalid_state);
	tcase_add_test(tc, test_stop_election_resets_votes);
	tcase_add_test(tc, test_start_election_single_node);
	tcase_add_test(tc, test_request_votes_updates_next_request_vote);
	tcase_add_test(tc, test_tick_connection_check_skips_unready);
	tcase_add_test(tc, test_reset_read_state_frees_buffer);
	tcase_add_test(tc, test_reset_write_state_frees_buffer);
	tcase_add_test(tc, test_reset_ss_state_frees_buffer);
	tcase_add_test(tc, test_add_write_and_try_write_success);
	tcase_add_test(tc, test_add_write_rejects_existing_buffer);
	tcase_add_test(tc, test_try_write_invalid);
	tcase_add_test(tc, test_remove_and_free_unknown_host);
	tcase_add_test(tc, test_new_conn_success);
	tcase_add_test(tc, test_new_conn_invalid_role);
	tcase_add_test(tc, test_new_conn_self_peer);
	tcase_add_test(tc, test_new_conn_invalid_type);
	tcase_add_test(tc, test_process_packet_client_request_reply_invalid_status);
	tcase_add_test(tc, test_process_packet_client_request_invalid_type);
	tcase_add_test(tc, test_process_packet_client_request_no_handler);
	tcase_add_test(tc, test_process_packet_client_request_not_leader_reply);
	tcase_add_test(tc, test_process_packet_register_client_not_leader);
	tcase_add_test(tc, test_process_packet_append_entries_short_payload);
	tcase_add_test(tc, test_process_packet_append_entries_num_entries_overflow);
	tcase_add_test(tc, test_process_packet_append_entries_handler_error);
	tcase_add_test(tc, test_process_packet_client_request_handler_error);
	tcase_add_test(tc, test_process_packet_request_vote_updates_term);
	tcase_add_test(tc, test_recv_invalid_header_length);
	tcase_add_test(tc, test_recv_invalid_rpc);
	tcase_add_test(tc, test_recv_header_too_large);
	tcase_add_test(tc, test_recv_header_min_size);
	tcase_add_test(tc, test_recv_header_max_size);
	tcase_add_test(tc, test_recv_invalid_state);
	tcase_add_test(tc, test_recv_bad_fd);
	tcase_add_test(tc, test_recv_header_partial_then_complete);
	tcase_add_test(tc, test_recv_payload_partial);
	tcase_add_test(tc, test_recv_header_eagain);
	tcase_add_test(tc, test_recv_payload_eagain);
	tcase_add_test(tc, test_recv_header_eof);
	tcase_add_test(tc, test_recv_payload_eof);
	tcase_add_test(tc, test_recv_header_read_error);
	tcase_add_test(tc, test_recv_payload_read_error);
	tcase_add_test(tc, test_send_invalid_rpc);
	tcase_add_test(tc, test_send_client_no_leader);
	tcase_add_test(tc, test_send_server_no_client);
	tcase_add_test(tc, test_send_peer_bad_fd);
	tcase_add_test(tc, test_send_append_entries_null_entries);
	tcase_add_test(tc, test_send_client_request_invalid_type);
	tcase_add_test(tc, test_send_client_request_reply_invalid_log_type);
	tcase_add_test(tc, test_send_request_vote_reply_invalid_status);
	tcase_add_test(tc, test_send_append_entries_invalid_log_event);
	tcase_add_test(tc, test_send_client_request_pre_send_overflow);
	tcase_add_test(tc, test_send_append_entries_pre_send_overflow);
	tcase_add_test(tc, test_update_leader_id);
	tcase_add_test(tc, test_close_resets_state);
	tcase_add_test(tc, test_log_at_indices);
	tcase_add_test(tc, test_term_at_indices);
	tcase_add_test(tc, test_free_log_unknown_event);
	tcase_add_test(tc, test_update_term);
	tcase_add_test(tc, test_append_remove_prepend_log);
	tcase_add_test(tc, test_client_log_append_single_conflict);
	tcase_add_test(tc, test_commit_and_advance_errors);
	tcase_add_test(tc, test_commit_and_advance_unknown_event);
	tcase_add_test(tc, test_commit_and_advance_success);
	tcase_add_test(tc, test_check_commit_index_majority);
	tcase_add_test(tc, test_check_commit_index_no_majority);
	tcase_add_test(tc, test_client_log_sendv_invalid_event);
	tcase_add_test(tc, test_send_register_client_reply_invalid_status);
	tcase_add_test(tc, test_send_hello_invalid_conn_type);
	tcase_add_test(tc, test_new_conn_not_hello);
	tcase_add_test(tc, test_new_conn_bad_length);
	tcase_add_test(tc, test_process_packet_append_entries_invalid_log_type);
	tcase_add_test(tc, test_process_packet_client_request_overlong);
	tcase_add_test(tc, test_save_and_load_state_round_trip);
	tcase_add_test(tc, test_save_state_header_only_preserves_log);
	tcase_add_test(tc, test_load_state_missing_file);
	tcase_add_test(tc, test_load_state_truncated_log_entry);

	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = raft_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
