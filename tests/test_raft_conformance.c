/*
 * Conformance tests aligned with doc/raft.tla.
 *
 * These tests drive selected Raft transitions and check invariants derived
 * from the TLA+ spec (election safety, log matching, leader completeness,
 * monotonicity). The harness will expand as more distributed scenarios are
 * modeled.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <errno.h>

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
	(void)entry;
	if (buf)
		*buf = NULL;
	return 0;
}

static raft_status_t test_process_packet(size_t *bytes_remaining,
		const uint8_t **ptr, raft_rpc_t rpc, raft_log_t log_type,
		struct raft_log *out)
{
	(void)rpc;
	(void)log_type;
	(void)out;

	if (*bytes_remaining != 0) {
		*ptr += *bytes_remaining;
		*bytes_remaining = 0;
	}

	return RAFT_OK;
}

static const struct raft_impl test_impl = {
	.name = "conformance",
	.num_log_types = RAFT_MAX_LOG,
	.handlers = {
		[RAFT_LOG_NOOP] = {
			.save_log = test_save_log,
		},
		[RAFT_LOG_REGISTER_TOPIC] = {
			.free_log = test_free_log,
			.commit_and_advance = test_commit_and_advance,
			.process_packet = test_process_packet,
			.save_log = test_save_log,
		},
	},
};

struct conformance_ctx {
	uint32_t last_term;
	uint32_t last_commit;
};

static void init_state_filenames(void);

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
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 1;
	raft_state.self_id = 1;
	init_state_filenames();
}

static void simulate_restart(void)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.commit_index = 0;
	raft_state.last_applied = 0;
	raft_state.election = false;
	raft_state.election_timer = 0;
	raft_state.next_ping = 0;
	raft_state.next_request_vote = 0;

	if (client_state) {
		client_state->current_leader_id = NULL_ID;
		client_state->current_leader = NULL;
	}

	if (*peers_ptr) {
		for (unsigned idx = 0; idx < *num_ptr; idx++) {
			struct raft_host_entry *entry = &(*peers_ptr)[idx];

			entry->vote_responded = false;
			entry->vote_granted = NULL_ID;
			entry->next_index = 1;
			entry->match_index = 0;

			raft_test_api.raft_reset_read_state(entry);
			raft_test_api.raft_reset_write_state(entry, true);
			raft_test_api.raft_reset_ss_state(entry, true);
		}
	}
}

static void setup_cluster(unsigned count)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;

	peers = calloc(count, sizeof(*peers));
	ck_assert_ptr_nonnull(peers);

	for (unsigned idx = 0; idx < count; idx++) {
		init_entry_locks(&peers[idx]);
		peers[idx].server_id = idx + 1;
		peers[idx].peer_fd = -1;
		peers[idx].address.s_addr = htonl(INADDR_LOOPBACK);
		peers[idx].port = htons((in_port_t)(1883 + idx));
	}

	*peers_ptr = peers;
	*num_ptr = count;
}

static void teardown_cluster(void)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers = *peers_ptr;

	if (peers) {
		for (unsigned idx = 0; idx < *num_ptr; idx++)
			destroy_entry_locks(&peers[idx]);
		free(peers);
	}

	*peers_ptr = NULL;
	*num_ptr = 1;
}

static void setup(void)
{
	raft_impl = &test_impl;
	free_log_called = 0;
	commit_called = 0;
	reset_raft_state();
	raft_state.self_id = 1;
	init_state_filenames();
	init_client_state();
}

static void teardown(void)
{
	unlink(raft_state.fn_vars);
	unlink(raft_state.fn_vars_new);
	unlink(raft_state.fn_log);
	reset_raft_state();
	teardown_cluster();
	destroy_client_state();
}

static int enter_temp_dir(char *path, size_t path_len, int *saved_cwd_fd)
{
	const char *tmpdir = getenv("TMPDIR");

	if (!tmpdir)
		tmpdir = "/tmp";

	if (snprintf(path, path_len, "%s/raft-tla-XXXXXX", tmpdir) >= (int)path_len)
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

static void init_state_filenames(void)
{
	snprintf(raft_state.fn_prefix, sizeof(raft_state.fn_prefix),
			"save_state_%u_", raft_state.self_id);
	snprintf(raft_state.fn_vars, sizeof(raft_state.fn_vars),
			"%svars.bin", raft_state.fn_prefix);
	snprintf(raft_state.fn_log, sizeof(raft_state.fn_log),
			"%slog.bin", raft_state.fn_prefix);
	snprintf(raft_state.fn_vars_new, sizeof(raft_state.fn_vars_new),
			"%s.new", raft_state.fn_vars);
}

static void leave_temp_dir(const char *path, int saved_cwd_fd)
{
	if (path) {
		unlink(raft_state.fn_vars);
		unlink(raft_state.fn_vars_new);
		unlink(raft_state.fn_log);
	}

	if (saved_cwd_fd != -1) {
		if (fchdir(saved_cwd_fd) == -1)
			(void)0;
		close(saved_cwd_fd);
	}

	if (path)
		rmdir(path);
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

static void assert_election_safety(void)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	if (raft_state.state == RAFT_STATE_LEADER) {
		ck_assert_uint_eq(client_state->current_leader_id, raft_state.self_id);
		ck_assert_uint_eq(raft_state.voted_for, raft_state.self_id);
	}
}

static void assert_log_matching(void)
{
	const struct raft_log *cur = raft_state.log_head;
	uint32_t last_index = 0;

	for (; cur; cur = cur->next) {
		ck_assert(cur->index > last_index);
		last_index = cur->index;
	}
}

static void assert_leader_completeness(void)
{
	if (raft_state.log_tail)
		ck_assert_uint_le(raft_state.commit_index, raft_state.log_tail->index);
	else
		ck_assert_uint_eq(raft_state.commit_index, 0);
}

static void assert_monotonicity(struct conformance_ctx *ctx)
{
	ck_assert_uint_ge(raft_state.current_term, ctx->last_term);
	ck_assert_uint_ge(raft_state.commit_index, ctx->last_commit);
	ctx->last_term = raft_state.current_term;
	ctx->last_commit = raft_state.commit_index;
}

static int read_full(int fd, uint8_t *buf, size_t len)
{
	size_t off = 0;

	while (off < len) {
		ssize_t rc = read(fd, buf + off, len - off);
		if (rc <= 0)
			return -1;
		off += (size_t)rc;
	}
	return 0;
}

static int send_request_vote(uint32_t term, uint32_t candidate_id,
		uint32_t last_log_index, uint32_t last_log_term,
		raft_status_t *out_status, uint32_t *out_term, uint32_t *out_voted_for)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t buf[RAFT_REQUEST_VOTE_SIZE];
	uint8_t header[RAFT_HDR_SIZE];
	uint32_t length;
	uint8_t reply_status;
	uint32_t reply_term;
	uint32_t reply_voted_for;

	if (pipe(fds) == -1)
		return -1;

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	term = htonl(term);
	candidate_id = htonl(candidate_id);
	last_log_index = htonl(last_log_index);
	last_log_term = htonl(last_log_term);

	memcpy(buf, &term, sizeof(term));
	memcpy(buf + 4, &candidate_id, sizeof(candidate_id));
	memcpy(buf + 8, &last_log_index, sizeof(last_log_index));
	memcpy(buf + 12, &last_log_term, sizeof(last_log_term));

	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	if (raft_test_api.raft_process_packet(&client, RAFT_REQUEST_VOTE) == -1)
		goto fail;

	if (read_full(fds[0], header, sizeof(header)) == -1)
		goto fail;

	memcpy(&length, header + 4, sizeof(length));
	length = ntohl(length);
	ck_assert_uint_eq(length - RAFT_HDR_SIZE, RAFT_REQUEST_VOTE_REPLY_SIZE);
	ck_assert_int_eq(header[0], RAFT_REQUEST_VOTE_REPLY);

	if (read_full(fds[0], buf, RAFT_REQUEST_VOTE_REPLY_SIZE) == -1)
		goto fail;

	reply_status = buf[0];
	memcpy(&reply_term, buf + 1, sizeof(reply_term));
	memcpy(&reply_voted_for, buf + 5, sizeof(reply_voted_for));
	reply_term = ntohl(reply_term);
	reply_voted_for = ntohl(reply_voted_for);

	if (out_status)
		*out_status = reply_status;
	if (out_term)
		*out_term = reply_term;
	if (out_voted_for)
		*out_voted_for = reply_voted_for;

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return 0;

fail:
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return -1;
}

static int send_client_request(uint32_t client_id, uint32_t sequence_num,
		raft_log_t log_type, raft_status_t *out_status,
		uint8_t *out_log_type, uint32_t *out_client_id,
		uint32_t *out_sequence_num)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t buf[RAFT_CLIENT_REQUEST_SIZE];
	uint8_t header[RAFT_HDR_SIZE];
	uint32_t length;
	uint8_t reply_status;
	uint8_t reply_log_type;
	uint32_t reply_client_id;
	uint32_t reply_sequence_num;
	uint16_t payload_len = htons(0);

	if (pipe(fds) == -1)
		return -1;

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	client_id = htonl(client_id);
	sequence_num = htonl(sequence_num);

	memcpy(buf, &client_id, sizeof(client_id));
	memcpy(buf + 4, &sequence_num, sizeof(sequence_num));
	buf[8] = (uint8_t)log_type;
	buf[9] = 0;
	memcpy(buf + 10, &payload_len, sizeof(payload_len));

	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	if (raft_test_api.raft_process_packet(&client, RAFT_CLIENT_REQUEST) == -1)
		goto fail;

	if (read_full(fds[0], header, sizeof(header)) == -1)
		goto fail;

	memcpy(&length, header + 4, sizeof(length));
	length = ntohl(length);
	ck_assert_uint_eq(length - RAFT_HDR_SIZE, RAFT_CLIENT_REQUEST_REPLY_SIZE);
	ck_assert_int_eq(header[0], RAFT_CLIENT_REQUEST_REPLY);

	if (read_full(fds[0], buf, RAFT_CLIENT_REQUEST_REPLY_SIZE) == -1)
		goto fail;

	reply_status = buf[0];
	reply_log_type = buf[1];
	memcpy(&reply_client_id, buf + 2, sizeof(reply_client_id));
	memcpy(&reply_sequence_num, buf + 6, sizeof(reply_sequence_num));
	reply_client_id = ntohl(reply_client_id);
	reply_sequence_num = ntohl(reply_sequence_num);

	if (out_status)
		*out_status = reply_status;
	if (out_log_type)
		*out_log_type = reply_log_type;
	if (out_client_id)
		*out_client_id = reply_client_id;
	if (out_sequence_num)
		*out_sequence_num = reply_sequence_num;

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return 0;

fail:
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return -1;
}

static int send_append_entries_heartbeat(uint32_t term, uint32_t leader_id,
		uint32_t prev_log_index, uint32_t prev_log_term, uint32_t leader_commit,
		raft_status_t *out_status)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t buf[RAFT_APPEND_ENTRIES_FIXED_SIZE];
	uint8_t header[RAFT_HDR_SIZE];
	uint32_t length;
	uint8_t reply_status;

	if (pipe(fds) == -1)
		return -1;

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	term = htonl(term);
	leader_id = htonl(leader_id);
	prev_log_index = htonl(prev_log_index);
	prev_log_term = htonl(prev_log_term);
	leader_commit = htonl(leader_commit);

	memcpy(buf, &term, sizeof(term));
	memcpy(buf + 4, &leader_id, sizeof(leader_id));
	memcpy(buf + 8, &prev_log_index, sizeof(prev_log_index));
	memcpy(buf + 12, &prev_log_term, sizeof(prev_log_term));
	memcpy(buf + 16, &leader_commit, sizeof(leader_commit));
	memset(buf + 20, 0, sizeof(uint32_t));

	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	if (raft_test_api.raft_process_packet(&client, RAFT_APPEND_ENTRIES) == -1)
		goto fail;

	if (read_full(fds[0], header, sizeof(header)) == -1)
		goto fail;

	memcpy(&length, header + 4, sizeof(length));
	length = ntohl(length);
	ck_assert_uint_eq(length - RAFT_HDR_SIZE, RAFT_APPEND_ENTRIES_REPLY_SIZE);
	ck_assert_int_eq(header[0], RAFT_APPEND_ENTRIES_REPLY);

	if (read_full(fds[0], buf, RAFT_APPEND_ENTRIES_REPLY_SIZE) == -1)
		goto fail;

	reply_status = buf[0];
	if (out_status)
		*out_status = reply_status;

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return 0;

fail:
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return -1;
}

static int send_append_entries_single(uint32_t term, uint32_t leader_id,
		uint32_t prev_log_index, uint32_t prev_log_term, uint32_t leader_commit,
		uint32_t entry_index, uint32_t entry_term, raft_status_t *out_status)
{
	struct raft_host_entry client;
	int fds[2];
	uint8_t buf[RAFT_APPEND_ENTRIES_FIXED_SIZE + RAFT_LOG_FIXED_SIZE];
	uint8_t header[RAFT_HDR_SIZE];
	uint32_t length;
	uint8_t reply_status;
	uint32_t num_entries = htonl(1);
	uint8_t *ptr = buf;

	if (pipe(fds) == -1)
		return -1;

	memset(&client, 0, sizeof(client));
	init_entry_locks(&client);
	client.peer_fd = fds[1];

	term = htonl(term);
	leader_id = htonl(leader_id);
	prev_log_index = htonl(prev_log_index);
	prev_log_term = htonl(prev_log_term);
	leader_commit = htonl(leader_commit);
	entry_index = htonl(entry_index);
	entry_term = htonl(entry_term);

	memcpy(ptr, &term, sizeof(term));
	ptr += sizeof(term);
	memcpy(ptr, &leader_id, sizeof(leader_id));
	ptr += sizeof(leader_id);
	memcpy(ptr, &prev_log_index, sizeof(prev_log_index));
	ptr += sizeof(prev_log_index);
	memcpy(ptr, &prev_log_term, sizeof(prev_log_term));
	ptr += sizeof(prev_log_term);
	memcpy(ptr, &leader_commit, sizeof(leader_commit));
	ptr += sizeof(leader_commit);
	memcpy(ptr, &num_entries, sizeof(num_entries));
	ptr += sizeof(num_entries);

	*ptr++ = RAFT_LOG_REGISTER_TOPIC;
	*ptr++ = 0;
	memcpy(ptr, &entry_index, sizeof(entry_index));
	ptr += sizeof(entry_index);
	memcpy(ptr, &entry_term, sizeof(entry_term));
	ptr += sizeof(entry_term);
	memset(ptr, 0, sizeof(uint16_t));

	client.rd_packet_buffer = buf;
	client.rd_packet_length = sizeof(buf);

	if (raft_test_api.raft_process_packet(&client, RAFT_APPEND_ENTRIES) == -1)
		goto fail;

	if (read_full(fds[0], header, sizeof(header)) == -1)
		goto fail;

	memcpy(&length, header + 4, sizeof(length));
	length = ntohl(length);
	ck_assert_uint_eq(length - RAFT_HDR_SIZE, RAFT_APPEND_ENTRIES_REPLY_SIZE);
	ck_assert_int_eq(header[0], RAFT_APPEND_ENTRIES_REPLY);

	if (read_full(fds[0], buf, RAFT_APPEND_ENTRIES_REPLY_SIZE) == -1)
		goto fail;

	reply_status = buf[0];
	if (out_status)
		*out_status = reply_status;

	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return 0;

fail:
	close(fds[0]);
	close(fds[1]);
	destroy_entry_locks(&client);
	return -1;
}

static void append_local_log(uint32_t index, uint32_t term)
{
	struct raft_log *entry = make_log(index, term);

	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = raft_state.log_tail->index;
}

static void catch_up_follower(uint32_t leader_term, uint32_t leader_id,
		uint32_t start_index, uint32_t target_index)
{
	for (uint32_t idx = start_index + 1; idx <= target_index; idx++) {
		raft_status_t status = RAFT_ERR;
		uint32_t prev_index = idx - 1;
		uint32_t prev_term = prev_index == 0 ? 0 : leader_term;

		ck_assert_int_eq(send_append_entries_single(leader_term, leader_id,
					prev_index, prev_term, idx, idx, leader_term, &status), 0);
		ck_assert_int_eq(status, RAFT_TRUE);
	}
}

static int process_append_entries_reply(struct raft_host_entry *client,
		raft_status_t status, uint32_t term, uint32_t match_index)
{
	uint8_t buf[RAFT_APPEND_ENTRIES_REPLY_SIZE];
	uint32_t term_n = htonl(term);
	uint32_t match_n = htonl(match_index);

	buf[0] = (uint8_t)status;
	memcpy(buf + 1, &term_n, sizeof(term_n));
	memcpy(buf + 5, &match_n, sizeof(match_n));

	client->rd_packet_buffer = buf;
	client->rd_packet_length = sizeof(buf);
	return raft_test_api.raft_process_packet(client, RAFT_APPEND_ENTRIES_REPLY);
}

static int process_request_vote_reply(struct raft_host_entry *client,
		raft_status_t status, uint32_t term, uint32_t voted_for)
{
	uint8_t buf[RAFT_REQUEST_VOTE_REPLY_SIZE];
	uint32_t term_n = htonl(term);
	uint32_t voted_n = htonl(voted_for);

	buf[0] = (uint8_t)status;
	memcpy(buf + 1, &term_n, sizeof(term_n));
	memcpy(buf + 5, &voted_n, sizeof(voted_n));

	client->rd_packet_buffer = buf;
	client->rd_packet_length = sizeof(buf);
	return raft_test_api.raft_process_packet(client, RAFT_REQUEST_VOTE_REPLY);
}

struct model_entry {
	uint32_t term;
};

struct model_node {
	uint32_t id;
	raft_state_t state;
	uint32_t term;
	uint32_t commit_index;
	uint32_t log_len;
	struct model_entry log[8];
};

struct model_cluster {
	struct model_node nodes[3];
};

struct impl_client_state {
	uint32_t current_leader_id;
	_Atomic uint32_t sequence_num;
	struct raft_log *log_pending_head;
	struct raft_log *log_pending_tail;
	struct raft_host_entry *unknown_clients;
};

struct impl_node {
	uint32_t id;
	struct raft_state state;
	struct impl_client_state client;
	struct raft_host_entry *peers;
	unsigned num_peers;
};

struct impl_cluster {
	struct impl_node nodes[3];
};

static bool impl_logs_match(const struct raft_state *lhs, const struct raft_state *rhs)
{
	const struct raft_log *a = lhs->log_head;
	const struct raft_log *b = rhs->log_head;

	for (; a && b; a = a->next, b = b->next) {
		if (a->index != b->index)
			return false;
		if (a->term != b->term)
			return false;
		if (a->event != b->event)
			return false;
	}

	return a == NULL && b == NULL;
}

static void model_set_entry(struct model_node *node, uint32_t index, uint32_t term)
{
	ck_assert_uint_ge(index, 1);
	ck_assert_uint_le(index, (uint32_t)(sizeof(node->log) / sizeof(node->log[0])));
	node->log[index - 1].term = term;
	if (index > node->log_len)
		node->log_len = index;
}

static bool model_log_matching(const struct model_node *nodes, size_t count)
{
	for (size_t a = 0; a < count; a++) {
		for (size_t b = a + 1; b < count; b++) {
			const struct model_node *lhs = &nodes[a];
			const struct model_node *rhs = &nodes[b];
			uint32_t max_idx = lhs->log_len < rhs->log_len ? lhs->log_len : rhs->log_len;

			for (uint32_t idx = 1; idx <= max_idx; idx++) {
				if (lhs->log[idx - 1].term != rhs->log[idx - 1].term)
					continue;
				for (uint32_t prev = 1; prev < idx; prev++) {
					if (lhs->log[prev - 1].term != rhs->log[prev - 1].term)
						return false;
				}
			}
		}
	}
	return true;
}

static bool model_leader_completeness(const struct model_node *committed_leader,
		const struct model_node *future_leader, uint32_t commit_index)
{
	if (commit_index == 0)
		return true;
	if (committed_leader->log_len < commit_index)
		return false;
	if (future_leader->log_len < commit_index)
		return false;

	for (uint32_t idx = 1; idx <= commit_index; idx++) {
		if (committed_leader->log[idx - 1].term != future_leader->log[idx - 1].term)
			return false;
	}
	return true;
}

static void model_init_cluster(struct model_cluster *cluster)
{
	for (size_t idx = 0; idx < 3; idx++) {
		cluster->nodes[idx].id = (uint32_t)(idx + 1);
		cluster->nodes[idx].state = RAFT_STATE_FOLLOWER;
		cluster->nodes[idx].term = 1;
		cluster->nodes[idx].commit_index = 0;
		cluster->nodes[idx].log_len = 0;
		memset(cluster->nodes[idx].log, 0, sizeof(cluster->nodes[idx].log));
	}
}

static void model_timeout(struct model_node *node)
{
	if (node->state == RAFT_STATE_FOLLOWER || node->state == RAFT_STATE_CANDIDATE) {
		node->state = RAFT_STATE_CANDIDATE;
		node->term += 1;
	}
}

static void model_grant_votes(struct model_cluster *cluster, struct model_node *candidate)
{
	unsigned votes = 1;
	unsigned need = 2;

	for (size_t idx = 0; idx < 3; idx++) {
		struct model_node *voter = &cluster->nodes[idx];
		if (voter->id == candidate->id)
			continue;
		if (voter->term > candidate->term)
			continue;
		voter->term = candidate->term;
		votes++;
	}

	if (votes >= need)
		candidate->state = RAFT_STATE_LEADER;
}

static void model_grant_votes_partial(struct model_cluster *cluster,
		struct model_node *candidate, unsigned grants)
{
	unsigned votes = 1;
	unsigned need = 2;

	for (size_t idx = 0; idx < 3 && grants > 0; idx++) {
		struct model_node *voter = &cluster->nodes[idx];
		if (voter->id == candidate->id)
			continue;
		if (voter->term > candidate->term)
			continue;
		voter->term = candidate->term;
		grants--;
		votes++;
	}

	if (votes >= need)
		candidate->state = RAFT_STATE_LEADER;
}

static void model_append_entry(struct model_node *leader, uint32_t term)
{
	leader->log_len++;
	leader->log[leader->log_len - 1].term = term;
}

static void model_replicate_to_all(struct model_cluster *cluster, const struct model_node *leader)
{
	for (size_t idx = 0; idx < 3; idx++) {
		struct model_node *follower = &cluster->nodes[idx];
		if (follower->id == leader->id)
			continue;
		follower->term = leader->term;
		follower->state = RAFT_STATE_FOLLOWER;
		follower->log_len = leader->log_len;
		memcpy(follower->log, leader->log, sizeof(leader->log));
	}
}

static void model_commit_all(struct model_cluster *cluster, uint32_t commit_index)
{
	for (size_t idx = 0; idx < 3; idx++)
		cluster->nodes[idx].commit_index = commit_index;
}

static bool model_election_safety(const struct model_cluster *cluster)
{
	uint32_t leader_term = 0;

	for (size_t idx = 0; idx < 3; idx++) {
		const struct model_node *node = &cluster->nodes[idx];

		if (node->state != RAFT_STATE_LEADER)
			continue;

		if (leader_term == 0)
			leader_term = node->term;
		else if (leader_term == node->term)
			return false;
	}

	return true;
}

static void model_step_down_if_newer_term(struct model_node *node, uint32_t term)
{
	if (term > node->term) {
		node->term = term;
		node->state = RAFT_STATE_FOLLOWER;
	}
}

static bool model_accept_append_entries(struct model_node *follower,
		const struct model_node *leader, uint32_t prev_index, uint32_t prev_term)
{
	if (prev_index == 0)
		goto accept;
	if (follower->log_len < prev_index)
		return false;
	if (follower->log[prev_index - 1].term != prev_term)
		return false;

accept:
	follower->term = leader->term;
	follower->state = RAFT_STATE_FOLLOWER;
	return true;
}

static void model_append_from_leader(struct model_node *follower,
		const struct model_node *leader, uint32_t index)
{
	ck_assert_uint_le(index, leader->log_len);
	model_set_entry(follower, index, leader->log[index - 1].term);
}

static bool model_deliver_append(struct model_node *follower,
		const struct model_node *leader, uint32_t prev_index, uint32_t prev_term,
		uint32_t entry_index)
{
	if (!model_accept_append_entries(follower, leader, prev_index, prev_term))
		return false;

	if (entry_index != 0)
		model_append_from_leader(follower, leader, entry_index);

	return true;
}

static bool model_quorum_intersection(void)
{
	static const uint32_t q1[] = {1, 2};
	static const uint32_t q2[] = {2, 3};
	static const uint32_t q3[] = {1, 3};
	const uint32_t *quorums[] = {q1, q2, q3};

	for (size_t a = 0; a < 3; a++) {
		for (size_t b = a + 1; b < 3; b++) {
			bool found = false;
			for (size_t i = 0; i < 2; i++) {
				for (size_t j = 0; j < 2; j++) {
					if (quorums[a][i] == quorums[b][j])
						found = true;
				}
			}
			if (!found)
				return false;
		}
	}
	return true;
}

static void model_merge_to_leader(struct model_cluster *cluster, const struct model_node *leader)
{
	model_replicate_to_all(cluster, leader);
	model_commit_all(cluster, leader->commit_index);
}

static struct raft_host_entry *impl_find_peer(struct impl_node *node, uint32_t server_id)
{
	for (unsigned idx = 0; idx < node->num_peers; idx++) {
		if (node->peers[idx].server_id == server_id)
			return &node->peers[idx];
	}
	return NULL;
}

static void impl_load_node(struct impl_node *node)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();

	raft_state = node->state;
	init_state_filenames();
	*peers_ptr = node->peers;
	*num_ptr = node->num_peers;

	client_state->current_leader_id = node->client.current_leader_id;
	client_state->sequence_num = node->client.sequence_num;
	client_state->log_pending_head = node->client.log_pending_head;
	client_state->log_pending_tail = node->client.log_pending_tail;
	client_state->unknown_clients = node->client.unknown_clients;
	client_state->current_leader = NULL;

	if (client_state->current_leader_id != NULL_ID) {
		client_state->current_leader = impl_find_peer(node,
				client_state->current_leader_id);
	}
}

static void impl_save_node(struct impl_node *node)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	node->state = raft_state;
	node->client.current_leader_id = client_state->current_leader_id;
	node->client.sequence_num = client_state->sequence_num;
	node->client.log_pending_head = client_state->log_pending_head;
	node->client.log_pending_tail = client_state->log_pending_tail;
	node->client.unknown_clients = client_state->unknown_clients;
}

static void impl_cluster_init(struct impl_cluster *cluster)
{
	for (size_t node_idx = 0; node_idx < 3; node_idx++) {
		struct impl_node *node = &cluster->nodes[node_idx];
		uint32_t self_id = (uint32_t)(node_idx + 1);
		unsigned peer_idx = 0;

		node->id = self_id;
		node->num_peers = 3;
		node->peers = calloc(node->num_peers, sizeof(*node->peers));
		ck_assert_ptr_nonnull(node->peers);

		memset(&node->state, 0, sizeof(node->state));
		node->state.self_id = self_id;
		node->state.current_term = 1;
		node->state.state = RAFT_STATE_FOLLOWER;
		node->state.voted_for = NULL_ID;
		atomic_store(&node->state.log_length, 0);

		memset(&node->client, 0, sizeof(node->client));
		node->client.current_leader_id = NULL_ID;

		node->peers[peer_idx].server_id = self_id;
		node->peers[peer_idx].peer_fd = -1;
		init_entry_locks(&node->peers[peer_idx]);
		peer_idx++;

		for (uint32_t id = 1; id <= 3; id++) {
			if (id == self_id)
				continue;
			node->peers[peer_idx].server_id = id;
			node->peers[peer_idx].peer_fd = -1;
			node->peers[peer_idx].next_index = 1;
			node->peers[peer_idx].match_index = 0;
			node->peers[peer_idx].vote_responded = false;
			node->peers[peer_idx].vote_granted = NULL_ID;
			init_entry_locks(&node->peers[peer_idx]);
			peer_idx++;
		}
	}
}

static void impl_cluster_free(struct impl_cluster *cluster)
{
	for (size_t node_idx = 0; node_idx < 3; node_idx++) {
		struct impl_node *node = &cluster->nodes[node_idx];
		for (unsigned idx = 0; idx < node->num_peers; idx++)
			destroy_entry_locks(&node->peers[idx]);
		free(node->peers);
		node->peers = NULL;
	}
}

static int impl_request_vote(struct impl_node *candidate, struct impl_node *follower)
{
	raft_status_t reply;
	uint32_t reply_term = 0;
	uint32_t reply_voted_for = 0;
	struct raft_host_entry *peer_entry;
	uint32_t last_index;
	uint32_t last_term;

	last_index = candidate->state.log_tail ? candidate->state.log_tail->index : 0;
	last_term = candidate->state.log_tail ? candidate->state.log_tail->term : 0;

	impl_load_node(follower);
	if (send_request_vote(candidate->state.current_term, candidate->id,
				last_index, last_term, &reply, &reply_term, &reply_voted_for) == -1)
		return -1;
	impl_save_node(follower);

	impl_load_node(candidate);
	peer_entry = impl_find_peer(candidate, follower->id);
	ck_assert_ptr_nonnull(peer_entry);
	ck_assert_int_eq(process_request_vote_reply(peer_entry, reply, reply_term, reply_voted_for), 0);
	impl_save_node(candidate);
	return 0;
}

static int impl_append_entries(struct impl_node *leader, struct impl_node *follower,
		uint32_t entry_index, uint32_t entry_term, uint32_t leader_commit)
{
	raft_status_t reply;
	struct raft_host_entry *peer_entry;

	impl_load_node(follower);
	if (send_append_entries_single(leader->state.current_term, leader->id,
				entry_index == 0 ? 0 : entry_index - 1,
				entry_index == 0 ? 0 : entry_term,
				leader_commit, entry_index, entry_term, &reply) == -1)
		return -1;
	impl_save_node(follower);

	impl_load_node(leader);
	peer_entry = impl_find_peer(leader, follower->id);
	ck_assert_ptr_nonnull(peer_entry);
	ck_assert_int_eq(process_append_entries_reply(peer_entry, reply,
				leader->state.current_term,
				reply == RAFT_TRUE ? entry_index : 0), 0);
	impl_save_node(leader);
	return reply == RAFT_TRUE ? 0 : -1;
}

START_TEST(test_conformance_timeout_single_node)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};

	setup_cluster(1);
	raft_state.state = RAFT_STATE_FOLLOWER;
	ck_assert_int_eq(raft_test_api.raft_change_to(RAFT_STATE_CANDIDATE), RAFT_STATE_CANDIDATE);
	ck_assert_int_eq(raft_test_api.raft_start_election(), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_LEADER);
	ck_assert_uint_eq(raft_state.current_term, 2);
	ck_assert_uint_eq(raft_state.voted_for, raft_state.self_id);
}
END_TEST

START_TEST(test_conformance_request_vote_grant)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;
	uint32_t reply_term = 0;
	uint32_t reply_voted_for = 0;

	setup_cluster(1);
	ck_assert_int_eq(send_request_vote(2, 2, 0, 0, &reply, &reply_term, &reply_voted_for), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_uint_eq(reply_term, 2);
	ck_assert_uint_eq(reply_voted_for, 2);
	ck_assert_uint_eq(raft_state.current_term, 2);
	ck_assert_uint_eq(raft_state.voted_for, 2);
}
END_TEST

START_TEST(test_conformance_request_vote_rejects_stale_term)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(1);
	raft_state.current_term = 3;
	raft_state.voted_for = NULL_ID;

	ck_assert_int_eq(send_request_vote(2, 2, 0, 0, &reply, NULL, NULL), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_uint_eq(raft_state.current_term, 3);
	ck_assert_uint_eq(raft_state.voted_for, NULL_ID);
}
END_TEST

START_TEST(test_conformance_request_vote_rejects_outdated_log)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;
	struct raft_log *entry;
	struct raft_log *entry2;

	setup_cluster(1);
	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->index = 1;
	entry->term = 5;
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
			&raft_state.log_tail, &raft_state.log_length), 0);

	entry2 = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry2);
	entry2->index = 2;
	entry2->term = 5;
	ck_assert_int_eq(raft_test_api.raft_append_log(entry2, &raft_state.log_head,
			&raft_state.log_tail, &raft_state.log_length), 0);

	ck_assert_int_eq(send_request_vote(5, 2, 1, 4, &reply, NULL, NULL), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_uint_eq(raft_state.voted_for, NULL_ID);
}
END_TEST

START_TEST(test_conformance_request_vote_rejects_already_voted)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(1);
	raft_state.current_term = 3;
	raft_state.voted_for = 5;

	ck_assert_int_eq(send_request_vote(3, 2, 0, 0, &reply, NULL, NULL), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_uint_eq(raft_state.voted_for, 5);
}
END_TEST

START_TEST(test_conformance_request_vote_rejects_null_candidate)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(1);
	raft_state.current_term = 2;
	raft_state.voted_for = NULL_ID;

	ck_assert_int_eq(send_request_vote(2, NULL_ID, 0, 0, &reply, NULL, NULL), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_uint_eq(raft_state.voted_for, NULL_ID);
}
END_TEST

START_TEST(test_conformance_request_vote_same_candidate)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(1);
	raft_state.current_term = 3;
	raft_state.voted_for = 2;

	ck_assert_int_eq(send_request_vote(3, 2, 0, 0, &reply, NULL, NULL), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_uint_eq(raft_state.voted_for, 2);
}
END_TEST

START_TEST(test_conformance_append_entries_heartbeat)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	raft_status_t reply;

	setup_cluster(2);
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 1;

	ck_assert_int_eq(send_append_entries_heartbeat(2, 2, 0, 0, 0, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_uint_eq(raft_state.current_term, 2);
	ck_assert_uint_eq(client_state->current_leader_id, 2);
}
END_TEST

START_TEST(test_conformance_append_entries_steps_down_candidate)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	raft_status_t reply;

	setup_cluster(2);
	raft_state.state = RAFT_STATE_CANDIDATE;
	raft_state.current_term = 3;

	ck_assert_int_eq(send_append_entries_heartbeat(3, 2, 0, 0, 0, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert_uint_eq(client_state->current_leader_id, 2);
}
END_TEST

START_TEST(test_conformance_append_entries_rejects_stale_term)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	raft_status_t reply;

	setup_cluster(2);
	raft_state.current_term = 5;
	client_state->current_leader_id = 9;

	ck_assert_int_eq(send_append_entries_heartbeat(4, 2, 0, 0, 0, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_uint_eq(raft_state.current_term, 5);
	ck_assert_uint_eq(client_state->current_leader_id, 9);
}
END_TEST

START_TEST(test_conformance_append_entries_rejects_prev_log_mismatch)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;
	struct raft_log *entry;

	setup_cluster(2);
	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->index = 1;
	entry->term = 1;
	raft_test_api.raft_append_log(entry, &raft_state.log_head,
			&raft_state.log_tail, &raft_state.log_length);

	ck_assert_int_eq(send_append_entries_heartbeat(2, 2, 1, 2, 0, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_uint_eq(raft_state.log_tail->index, 1);
	ck_assert_uint_eq(raft_state.log_tail->term, 1);
}
END_TEST

START_TEST(test_conformance_append_entries_rejects_prev_log_when_empty)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(2);
	ck_assert_ptr_eq(raft_state.log_head, NULL);

	ck_assert_int_eq(send_append_entries_heartbeat(2, 2, 1, 1, 0, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
}
END_TEST

START_TEST(test_conformance_append_entries_conflict_replaces_entry)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(2);
	raft_state.log_head = make_log(1, 1);
	raft_state.log_tail = raft_state.log_head;
	atomic_store(&raft_state.log_length, 1);

	ck_assert_int_eq(send_append_entries_single(2, 2, 0, 0, 0, 1, 2, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_uint_eq(raft_state.log_tail->index, 1);
	ck_assert_uint_eq(raft_state.log_tail->term, 2);
}
END_TEST

START_TEST(test_conformance_append_entries_duplicate_no_growth)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(2);
	raft_state.log_head = make_log(1, 2);
	raft_state.log_tail = raft_state.log_head;
	atomic_store(&raft_state.log_length, 1);

	ck_assert_int_eq(send_append_entries_single(2, 2, 0, 0, 0, 1, 2, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 1);
	ck_assert_uint_eq(raft_state.log_tail->index, 1);
	ck_assert_uint_eq(raft_state.log_tail->term, 2);
}
END_TEST

START_TEST(test_conformance_append_entries_appends_and_commits)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(2);
	raft_state.commit_index = 0;

	ck_assert_int_eq(send_append_entries_single(2, 2, 0, 0, 1, 1, 2, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_ptr_nonnull(raft_state.log_tail);
	ck_assert_uint_eq(raft_state.log_tail->index, 1);
	ck_assert_uint_eq(raft_state.log_tail->term, 2);
	ck_assert_uint_eq(raft_state.commit_index, 1);
}
END_TEST

START_TEST(test_conformance_append_entries_out_of_order_recovery)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(2);
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 2;

	ck_assert_int_eq(send_append_entries_single(2, 2, 1, 2, 0, 2, 2, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_ptr_eq(raft_state.log_head, NULL);

	ck_assert_int_eq(send_append_entries_single(2, 2, 0, 0, 0, 1, 2, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_uint_eq(raft_state.log_tail->index, 1);

	ck_assert_int_eq(send_append_entries_single(2, 2, 1, 2, 0, 2, 2, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_uint_eq(raft_state.log_tail->index, 2);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 2);
}
END_TEST

START_TEST(test_conformance_append_entries_commit_cap)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	raft_status_t reply;

	setup_cluster(2);
	raft_state.log_head = make_log(1, 1);
	raft_state.log_tail = raft_state.log_head;
	atomic_store(&raft_state.log_length, 1);
	raft_state.commit_index = 0;

	ck_assert_int_eq(send_append_entries_heartbeat(2, 2, 1, 1, 10, &reply), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_TRUE);
	ck_assert_uint_eq(raft_state.commit_index, 1);
}
END_TEST

START_TEST(test_conformance_append_entries_reply_updates_match_index)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	setup_cluster(3);
	peers = *peers_ptr;
	*num_ptr = 3;

	raft_state.current_term = 2;
	raft_state.commit_index = 0;
	raft_state.state = RAFT_STATE_LEADER;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;
	raft_state.log_head = make_log(1, 2);
	raft_state.log_tail = raft_state.log_head;
	atomic_store(&raft_state.log_length, 1);

	peers[1].match_index = 0;
	peers[1].next_index = 1;

	ck_assert_int_eq(process_append_entries_reply(&peers[1], RAFT_TRUE, 2, 1), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(peers[1].match_index, 1);
	ck_assert_uint_eq(peers[1].next_index, 2);
}
END_TEST

START_TEST(test_conformance_append_entries_reply_higher_term_steps_down)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_host_entry *peers;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_term = 2;
	client_state->current_leader_id = raft_state.self_id;

	ck_assert_int_eq(process_append_entries_reply(&peers[1], RAFT_TRUE, 3, 1), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert_uint_eq(raft_state.current_term, 3);
	ck_assert_uint_eq(client_state->current_leader_id, NULL_ID);
}
END_TEST

START_TEST(test_conformance_append_entries_reply_decrements_next_index)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	setup_cluster(2);
	peers = *peers_ptr;
	*num_ptr = 2;

	raft_state.current_term = 2;
	raft_state.state = RAFT_STATE_LEADER;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;
	raft_state.log_head = make_log(1, 2);
	raft_state.log_tail = raft_state.log_head;
	atomic_store(&raft_state.log_length, 1);

	peers[1].next_index = 2;
	peers[1].match_index = 0;

	ck_assert_int_eq(process_append_entries_reply(&peers[1], RAFT_FALSE, 2, 0), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(peers[1].next_index, 1);
	ck_assert_uint_eq(peers[1].match_index, 0);
}
END_TEST

START_TEST(test_conformance_follower_catch_up_fresh_short)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};

	setup_cluster(1);

	raft_state.self_id = 2;
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 1;
	raft_state.voted_for = NULL_ID;
	raft_state.commit_index = 0;
	commit_called = 0;

	catch_up_follower(2, 1, 0, 1);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(raft_state.log_tail->index, 1);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 1);
	ck_assert_uint_eq(raft_state.commit_index, 1);
	ck_assert_int_eq(commit_called, 1);
}
END_TEST

START_TEST(test_conformance_follower_catch_up_fresh_long)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};

	setup_cluster(1);

	raft_state.self_id = 2;
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 1;
	raft_state.voted_for = NULL_ID;
	raft_state.commit_index = 0;
	commit_called = 0;

	catch_up_follower(2, 1, 0, 5);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_ptr_nonnull(raft_state.log_tail);
	ck_assert_uint_eq(raft_state.log_tail->index, 5);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 5);
	ck_assert_uint_eq(raft_state.commit_index, 5);
	ck_assert_int_eq(commit_called, 5);
}
END_TEST

START_TEST(test_conformance_follower_catch_up_after_restart)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};

	setup_cluster(1);

	raft_state.self_id = 2;
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 2;
	raft_state.voted_for = NULL_ID;
	raft_state.commit_index = 2;

	append_local_log(1, 2);
	append_local_log(2, 2);

	simulate_restart();
	commit_called = 0;

	catch_up_follower(2, 1, 2, 6);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_ptr_nonnull(raft_state.log_tail);
	ck_assert_uint_eq(raft_state.log_tail->index, 6);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 6);
	ck_assert_uint_eq(raft_state.commit_index, 6);
	ck_assert_int_eq(commit_called, 6);
}
END_TEST

START_TEST(test_model_log_matching_prefix_holds)
{
	struct model_node nodes[2] = {
		{ .id = 1 },
		{ .id = 2 },
	};

	model_set_entry(&nodes[0], 1, 1);
	model_set_entry(&nodes[0], 2, 2);
	model_set_entry(&nodes[0], 3, 2);

	model_set_entry(&nodes[1], 1, 1);
	model_set_entry(&nodes[1], 2, 2);
	model_set_entry(&nodes[1], 3, 2);

	ck_assert(model_log_matching(nodes, 2));
}
END_TEST

START_TEST(test_model_log_matching_prefix_detects_violation)
{
	struct model_node nodes[2] = {
		{ .id = 1 },
		{ .id = 2 },
	};

	model_set_entry(&nodes[0], 1, 1);
	model_set_entry(&nodes[0], 2, 2);

	model_set_entry(&nodes[1], 1, 9);
	model_set_entry(&nodes[1], 2, 2);

	ck_assert(!model_log_matching(nodes, 2));
}
END_TEST

START_TEST(test_model_leader_completeness_holds)
{
	struct model_node leader = { .id = 1, .state = RAFT_STATE_LEADER };
	struct model_node future = { .id = 2, .state = RAFT_STATE_LEADER };

	model_set_entry(&leader, 1, 1);
	model_set_entry(&leader, 2, 2);
	model_set_entry(&future, 1, 1);
	model_set_entry(&future, 2, 2);
	model_set_entry(&future, 3, 3);

	ck_assert(model_leader_completeness(&leader, &future, 2));
}
END_TEST

START_TEST(test_model_leader_completeness_detects_violation)
{
	struct model_node leader = { .id = 1, .state = RAFT_STATE_LEADER };
	struct model_node future = { .id = 2, .state = RAFT_STATE_LEADER };

	model_set_entry(&leader, 1, 1);
	model_set_entry(&leader, 2, 2);
	model_set_entry(&future, 1, 1);
	model_set_entry(&future, 2, 9);

	ck_assert(!model_leader_completeness(&leader, &future, 2));
}
END_TEST

START_TEST(test_model_three_node_election_and_replication)
{
	struct model_cluster cluster;
	struct model_node *candidate;
	struct model_node *leader;

	model_init_cluster(&cluster);

	candidate = &cluster.nodes[0];
	model_timeout(candidate);
	model_grant_votes(&cluster, candidate);

	ck_assert_int_eq(candidate->state, RAFT_STATE_LEADER);

	leader = candidate;
	model_append_entry(leader, leader->term);
	model_replicate_to_all(&cluster, leader);
	model_commit_all(&cluster, leader->log_len);

	ck_assert(model_log_matching(cluster.nodes, 3));
	ck_assert(model_leader_completeness(leader, &cluster.nodes[1], leader->commit_index));
	ck_assert_uint_eq(cluster.nodes[1].commit_index, leader->commit_index);
	ck_assert_uint_eq(cluster.nodes[2].commit_index, leader->commit_index);
}
END_TEST

START_TEST(test_model_three_node_election_no_majority)
{
	struct model_cluster cluster;
	struct model_node *candidate;

	model_init_cluster(&cluster);

	candidate = &cluster.nodes[0];
	model_timeout(candidate);
	model_grant_votes_partial(&cluster, candidate, 0);

	ck_assert_int_eq(candidate->state, RAFT_STATE_CANDIDATE);
}
END_TEST

START_TEST(test_model_split_brain_single_leader)
{
	struct model_cluster cluster;
	struct model_node *cand_a;
	struct model_node *cand_b;

	model_init_cluster(&cluster);

	cand_a = &cluster.nodes[0];
	cand_b = &cluster.nodes[1];

	model_timeout(cand_a);
	model_timeout(cand_b);

	cand_a->term = 2;
	cand_b->term = 2;

	cand_a->state = RAFT_STATE_LEADER;
	cand_b->state = RAFT_STATE_CANDIDATE;

	ck_assert(model_election_safety(&cluster));

	cand_b->state = RAFT_STATE_LEADER;
	ck_assert(!model_election_safety(&cluster));
}
END_TEST

START_TEST(test_model_leadership_change_preserves_log_matching)
{
	struct model_cluster cluster;
	struct model_node *old_leader;
	struct model_node *new_leader;

	model_init_cluster(&cluster);

	old_leader = &cluster.nodes[0];
	old_leader->state = RAFT_STATE_LEADER;
	old_leader->term = 2;
	model_append_entry(old_leader, 2);
	model_replicate_to_all(&cluster, old_leader);
	model_commit_all(&cluster, old_leader->log_len);

	new_leader = &cluster.nodes[1];
	new_leader->term = 3;
	model_step_down_if_newer_term(old_leader, new_leader->term);
	new_leader->state = RAFT_STATE_LEADER;

	model_append_entry(new_leader, new_leader->term);
	model_replicate_to_all(&cluster, new_leader);
	model_commit_all(&cluster, new_leader->log_len);

	ck_assert(model_log_matching(cluster.nodes, 3));
	ck_assert(model_leader_completeness(new_leader, &cluster.nodes[2], new_leader->commit_index));
	ck_assert(model_election_safety(&cluster));
}
END_TEST

START_TEST(test_model_append_entries_backtrack_converges)
{
	struct model_cluster cluster;
	struct model_node *leader;
	struct model_node *follower;

	model_init_cluster(&cluster);
	leader = &cluster.nodes[0];
	follower = &cluster.nodes[1];

	leader->state = RAFT_STATE_LEADER;
	leader->term = 2;
	model_append_entry(leader, 1);
	model_append_entry(leader, 2);

	model_set_entry(follower, 1, 1);
	model_set_entry(follower, 2, 9);

	ck_assert(!model_accept_append_entries(follower, leader, 2, 2));
	ck_assert(model_accept_append_entries(follower, leader, 1, 1));
	model_append_from_leader(follower, leader, 2);

	ck_assert(model_log_matching(cluster.nodes, 2));
}
END_TEST

START_TEST(test_model_partition_merge_higher_term_wins)
{
	struct model_cluster cluster;
	struct model_node *leader_a;
	struct model_node *leader_b;

	model_init_cluster(&cluster);
	leader_a = &cluster.nodes[0];
	leader_b = &cluster.nodes[1];

	leader_a->state = RAFT_STATE_LEADER;
	leader_a->term = 2;
	model_append_entry(leader_a, 2);
	model_commit_all(&cluster, leader_a->log_len);

	leader_b->state = RAFT_STATE_LEADER;
	leader_b->term = 3;
	model_append_entry(leader_b, 3);

	model_step_down_if_newer_term(leader_a, leader_b->term);
	model_merge_to_leader(&cluster, leader_b);

	ck_assert(model_log_matching(cluster.nodes, 3));
	ck_assert(model_leader_completeness(leader_b, &cluster.nodes[2], leader_b->commit_index));
	ck_assert(model_election_safety(&cluster));
}
END_TEST

START_TEST(test_model_liveness_eventual_leader_and_commit)
{
	struct model_cluster cluster;
	struct model_node *candidate;
	struct model_node *leader;

	model_init_cluster(&cluster);

	candidate = &cluster.nodes[0];
	model_timeout(candidate);
	model_grant_votes(&cluster, candidate);

	ck_assert_int_eq(candidate->state, RAFT_STATE_LEADER);

	leader = candidate;
	model_append_entry(leader, leader->term);
	model_replicate_to_all(&cluster, leader);
	model_commit_all(&cluster, leader->log_len);

	ck_assert_uint_eq(cluster.nodes[0].commit_index, leader->log_len);
	ck_assert_uint_eq(cluster.nodes[1].commit_index, leader->log_len);
	ck_assert_uint_eq(cluster.nodes[2].commit_index, leader->log_len);
}
END_TEST

START_TEST(test_model_message_reorder_recovery)
{
	struct model_cluster cluster;
	struct model_node *leader;
	struct model_node *follower;

	model_init_cluster(&cluster);
	leader = &cluster.nodes[0];
	follower = &cluster.nodes[1];

	leader->state = RAFT_STATE_LEADER;
	leader->term = 2;
	model_append_entry(leader, 2);
	model_append_entry(leader, 2);

	ck_assert(!model_deliver_append(follower, leader, 1, 2, 2));
	ck_assert_uint_eq(follower->log_len, 0);

	ck_assert(model_deliver_append(follower, leader, 0, 0, 1));
	ck_assert_uint_eq(follower->log_len, 1);

	ck_assert(model_deliver_append(follower, leader, 1, 2, 2));
	ck_assert_uint_eq(follower->log_len, 2);
	ck_assert(model_log_matching(cluster.nodes, 2));
}
END_TEST

START_TEST(test_model_duplicate_append_entries_idempotent)
{
	struct model_cluster cluster;
	struct model_node *leader;
	struct model_node *follower;

	model_init_cluster(&cluster);
	leader = &cluster.nodes[0];
	follower = &cluster.nodes[1];

	leader->state = RAFT_STATE_LEADER;
	leader->term = 2;
	model_append_entry(leader, 2);

	ck_assert(model_accept_append_entries(follower, leader, 0, 0));
	model_append_from_leader(follower, leader, 1);

	ck_assert_uint_eq(follower->log_len, 1);
	ck_assert_uint_eq(follower->log[0].term, 2);

	ck_assert(model_accept_append_entries(follower, leader, 0, 0));
	model_append_from_leader(follower, leader, 1);

	ck_assert_uint_eq(follower->log_len, 1);
	ck_assert_uint_eq(follower->log[0].term, 2);
	ck_assert(model_log_matching(cluster.nodes, 2));
}
END_TEST

START_TEST(test_model_drop_append_entries_no_effect)
{
	struct model_cluster cluster;
	struct model_node *leader;
	struct model_node *follower;

	model_init_cluster(&cluster);
	leader = &cluster.nodes[0];
	follower = &cluster.nodes[1];

	leader->state = RAFT_STATE_LEADER;
	leader->term = 2;
	model_append_entry(leader, 2);

	ck_assert_uint_eq(follower->log_len, 0);
	ck_assert_uint_eq(leader->log_len, 1);
	ck_assert(model_log_matching(cluster.nodes, 2));
}
END_TEST

START_TEST(test_impl_three_node_leader_replication)
{
	struct impl_cluster cluster;
	struct impl_node *leader;
	struct impl_node *follower_a;
	struct impl_node *follower_b;
	struct raft_log *entry;
	uint32_t noop_index;
	uint32_t noop_term;

	impl_cluster_init(&cluster);

	leader = &cluster.nodes[0];
	follower_a = &cluster.nodes[1];
	follower_b = &cluster.nodes[2];

	leader->state.state = RAFT_STATE_CANDIDATE;
	leader->state.current_term = 2;
	leader->state.voted_for = leader->id;
	leader->peers[0].vote_responded = true;
	leader->peers[0].vote_granted = leader->id;

	ck_assert_int_eq(impl_request_vote(leader, follower_a), 0);
	ck_assert_int_eq(impl_request_vote(leader, follower_b), 0);

	ck_assert_int_eq(leader->state.state, RAFT_STATE_LEADER);

	impl_load_node(leader);
	noop_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
	noop_term = raft_state.log_tail ? raft_state.log_tail->term : 0;
	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->index = noop_index + 1;
	entry->term = leader->state.current_term;
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = entry->index;
	impl_save_node(leader);

	if (noop_index > 0) {
		ck_assert_int_eq(impl_append_entries(leader, follower_a, noop_index, noop_term, 0), 0);
		ck_assert_int_eq(impl_append_entries(leader, follower_b, noop_index, noop_term, 0), 0);
	}
	ck_assert_int_eq(impl_append_entries(leader, follower_a, entry->index, entry->term, 0), 0);
	ck_assert_int_eq(impl_append_entries(leader, follower_b, entry->index, entry->term, 0), 0);

	impl_load_node(leader);
	ck_assert_uint_eq(impl_find_peer(leader, follower_a->id)->match_index, entry->index);
	ck_assert_uint_eq(impl_find_peer(leader, follower_b->id)->match_index, entry->index);
	ck_assert_ptr_nonnull(raft_state.log_tail);
	ck_assert_uint_eq(raft_state.log_tail->index, entry->index);
	ck_assert_uint_eq(raft_state.log_tail->term, raft_state.current_term);
	ck_assert_uint_eq(raft_test_api.raft_term_at(entry->index), raft_state.current_term);
	(void)raft_test_api.raft_check_commit_index(entry->index);
	ck_assert_uint_eq(raft_state.commit_index, entry->index);
	impl_save_node(leader);

	impl_load_node(follower_a);
	ck_assert_int_eq(send_append_entries_heartbeat(leader->state.current_term, leader->id,
				entry->index, entry->term, entry->index, NULL), 0);
	ck_assert_uint_eq(raft_state.commit_index, entry->index);
	impl_save_node(follower_a);

	impl_load_node(follower_b);
	ck_assert_int_eq(send_append_entries_heartbeat(leader->state.current_term, leader->id,
				entry->index, entry->term, entry->index, NULL), 0);
	ck_assert_uint_eq(raft_state.commit_index, entry->index);
	impl_save_node(follower_b);

	impl_cluster_free(&cluster);
	{
		struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
		unsigned *num_ptr = raft_test_api.num_peers_ptr();

		*peers_ptr = NULL;
		*num_ptr = 1;
	}
}
END_TEST

START_TEST(test_impl_log_matching_after_replication)
{
	struct impl_cluster cluster;
	struct impl_node *leader;
	struct impl_node *follower_a;
	struct impl_node *follower_b;
	struct raft_log *entry;
	uint32_t noop_index;
	uint32_t noop_term;
	uint32_t entry_index;

	impl_cluster_init(&cluster);

	leader = &cluster.nodes[0];
	follower_a = &cluster.nodes[1];
	follower_b = &cluster.nodes[2];

	leader->state.state = RAFT_STATE_LEADER;
	leader->state.current_term = 2;
	leader->state.voted_for = leader->id;

	impl_load_node(leader);
	noop_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
	noop_term = raft_state.log_tail ? raft_state.log_tail->term : 0;
	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry_index = noop_index + 1;
	entry->index = entry_index;
	entry->term = leader->state.current_term;
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->index = entry_index + 1;
	entry->term = leader->state.current_term;
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = entry->index;
	impl_save_node(leader);

	if (noop_index > 0) {
		ck_assert_int_eq(impl_append_entries(leader, follower_a, noop_index, noop_term, 0), 0);
		ck_assert_int_eq(impl_append_entries(leader, follower_b, noop_index, noop_term, 0), 0);
	}
	ck_assert_int_eq(impl_append_entries(leader, follower_a, entry_index,
				leader->state.current_term, 0), 0);
	ck_assert_int_eq(impl_append_entries(leader, follower_a, entry_index + 1,
				leader->state.current_term, 0), 0);
	ck_assert_int_eq(impl_append_entries(leader, follower_b, entry_index,
				leader->state.current_term, 0), 0);
	ck_assert_int_eq(impl_append_entries(leader, follower_b, entry_index + 1,
				leader->state.current_term, 0), 0);

	ck_assert(impl_logs_match(&leader->state, &follower_a->state));
	ck_assert(impl_logs_match(&leader->state, &follower_b->state));

	impl_cluster_free(&cluster);
	{
		struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
		unsigned *num_ptr = raft_test_api.num_peers_ptr();

		*peers_ptr = NULL;
		*num_ptr = 1;
	}
}
END_TEST

START_TEST(test_impl_leader_completeness_after_leader_change)
{
	struct impl_cluster cluster;
	struct impl_node *leader;
	struct impl_node *follower;
	struct raft_log *entry;
	uint32_t noop_index;
	uint32_t noop_term;

	impl_cluster_init(&cluster);

	leader = &cluster.nodes[0];
	follower = &cluster.nodes[1];

	leader->state.state = RAFT_STATE_LEADER;
	leader->state.current_term = 2;
	leader->state.voted_for = leader->id;

	impl_load_node(leader);
	noop_index = raft_state.log_tail ? raft_state.log_tail->index : 0;
	noop_term = raft_state.log_tail ? raft_state.log_tail->term : 0;
	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->index = noop_index + 1;
	entry->term = leader->state.current_term;
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = entry->index;
	impl_save_node(leader);

	if (noop_index > 0)
		ck_assert_int_eq(impl_append_entries(leader, follower, noop_index, noop_term, 0), 0);
	ck_assert_int_eq(impl_append_entries(leader, follower, entry->index,
				leader->state.current_term, 0), 0);

	impl_load_node(leader);
	ck_assert_int_ge(raft_test_api.raft_check_commit_index(entry->index), 0);
	ck_assert_uint_eq(raft_state.commit_index, entry->index);
	impl_save_node(leader);

	impl_load_node(follower);
	ck_assert_int_eq(send_append_entries_heartbeat(leader->state.current_term, leader->id,
				entry->index, leader->state.current_term, entry->index, NULL), 0);
	impl_save_node(follower);

	follower->state.state = RAFT_STATE_LEADER;
	follower->state.current_term = 3;
	follower->state.voted_for = follower->id;

	ck_assert_ptr_nonnull(follower->state.log_head);
	ck_assert_uint_eq(follower->state.log_head->index, 1);
	ck_assert_uint_eq(follower->state.log_head->term, 2);

	impl_cluster_free(&cluster);
	{
		struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
		unsigned *num_ptr = raft_test_api.num_peers_ptr();

		*peers_ptr = NULL;
		*num_ptr = 1;
	}
}
END_TEST

START_TEST(test_model_quorum_intersection_majority)
{
	ck_assert(model_quorum_intersection());
}
END_TEST
START_TEST(test_conformance_request_vote_reply_higher_term_steps_down)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_host_entry *peers;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.state = RAFT_STATE_CANDIDATE;
	raft_state.current_term = 2;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;

	ck_assert_int_eq(process_request_vote_reply(&peers[1], RAFT_TRUE, 3, peers[1].server_id), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert_uint_eq(raft_state.current_term, 3);
	ck_assert_uint_eq(client_state->current_leader_id, NULL_ID);
}
END_TEST

START_TEST(test_conformance_request_vote_reply_higher_term_steps_down_leader)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_host_entry *peers;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_term = 2;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;

	ck_assert_int_eq(process_request_vote_reply(&peers[1], RAFT_TRUE, 3, peers[1].server_id), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert_uint_eq(raft_state.current_term, 3);
	ck_assert_uint_eq(client_state->current_leader_id, NULL_ID);
}
END_TEST

START_TEST(test_conformance_request_vote_reply_majority)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;

	setup_cluster(3);
	peers = *peers_ptr;
	*num_ptr = 3;

	raft_state.state = RAFT_STATE_CANDIDATE;
	raft_state.current_term = 4;
	raft_state.voted_for = raft_state.self_id;
	peers[0].vote_responded = true;
	peers[0].vote_granted = raft_state.self_id;

	ck_assert_int_eq(process_request_vote_reply(&peers[1], RAFT_TRUE, 4, raft_state.self_id), 0);
	ck_assert_int_eq(process_request_vote_reply(&peers[2], RAFT_TRUE, 4, raft_state.self_id), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_LEADER);
	ck_assert_uint_eq(client_state->current_leader_id, raft_state.self_id);
}
END_TEST

START_TEST(test_conformance_request_vote_reply_stale_term_ignored)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_host_entry *peers;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.state = RAFT_STATE_CANDIDATE;
	raft_state.current_term = 3;

	ck_assert_int_eq(process_request_vote_reply(&peers[1], RAFT_TRUE, 2, peers[1].server_id), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_CANDIDATE);
	ck_assert(!peers[1].vote_responded);
	ck_assert_uint_eq(peers[1].vote_granted, 0);
}
END_TEST

START_TEST(test_conformance_request_vote_reply_ignored_if_not_candidate)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_host_entry *peers;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 2;

	ck_assert_int_eq(process_request_vote_reply(&peers[1], RAFT_TRUE, 2, peers[1].server_id), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert(!peers[1].vote_responded);
	ck_assert_uint_eq(peers[1].vote_granted, 0);
}
END_TEST

START_TEST(test_conformance_append_entries_reply_next_index_floor)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_host_entry *peers;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.current_term = 2;
	raft_state.log_head = NULL;
	raft_state.log_tail = NULL;
	atomic_store(&raft_state.log_length, 0);

	peers[1].next_index = 1;
	peers[1].match_index = 0;

	ck_assert_int_eq(process_append_entries_reply(&peers[1], RAFT_FALSE, 2, 0), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(peers[1].next_index, 1);
}
END_TEST

START_TEST(test_conformance_append_entries_reply_needs_snapshot)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_host_entry *peers;
	struct raft_log *entry;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.current_term = 2;
	entry = make_log(5, 2);
	raft_state.log_head = entry;
	raft_state.log_tail = entry;
	atomic_store(&raft_state.log_length, 1);

	peers[1].next_index = 3;
	peers[1].match_index = 0;

	ck_assert_int_eq(process_append_entries_reply(&peers[1], RAFT_FALSE, 2, 0), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(peers[1].next_index, 3);
}
END_TEST

START_TEST(test_conformance_client_request_appends_log)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	raft_status_t reply;
	uint8_t reply_type;
	uint32_t reply_client_id;
	uint32_t reply_sequence_num;

	setup_cluster(1);
	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_term = 2;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;

	ck_assert_int_eq(send_client_request(42, 7, RAFT_LOG_REGISTER_TOPIC, &reply,
				&reply_type, &reply_client_id, &reply_sequence_num), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_OK);
	ck_assert_uint_eq(reply_type, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_uint_eq(reply_client_id, 42);
	ck_assert_uint_eq(reply_sequence_num, 7);
	ck_assert_ptr_nonnull(raft_state.log_tail);
	ck_assert_uint_eq(raft_state.log_tail->index, 1);
	ck_assert_uint_eq(raft_state.log_tail->term, 2);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 1);
}
END_TEST

START_TEST(test_conformance_client_request_rejected_when_not_leader)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	raft_status_t reply;
	uint8_t reply_type;

	setup_cluster(1);
	raft_state.state = RAFT_STATE_FOLLOWER;
	raft_state.current_term = 2;
	client_state->current_leader_id = 9;

	ck_assert_int_eq(send_client_request(99, 3, RAFT_LOG_REGISTER_TOPIC, &reply,
				&reply_type, NULL, NULL), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_int_eq(reply, RAFT_NOT_LEADER);
	ck_assert_uint_eq(reply_type, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_eq(raft_state.log_head, NULL);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 0);
}
END_TEST

START_TEST(test_conformance_client_request_commit_applies_with_quorum)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	raft_status_t reply;

	setup_cluster(3);
	peers = *peers_ptr;
	*num_ptr = 3;

	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_term = 2;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;

	ck_assert_int_eq(send_client_request(1, 1, RAFT_LOG_REGISTER_TOPIC, &reply,
				NULL, NULL, NULL), 0);
	ck_assert_int_eq(reply, RAFT_OK);

	peers[1].match_index = 1;
	peers[2].match_index = 1;

	ck_assert_int_eq(raft_test_api.raft_check_commit_index(1), 1);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(raft_state.commit_index, 1);
	ck_assert_int_eq(commit_called, 1);
}
END_TEST

START_TEST(test_conformance_advance_commit_index_requires_current_term)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	struct raft_log *entry;

	setup_cluster(3);
	peers = *peers_ptr;
	*num_ptr = 3;

	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_term = 3;
	raft_state.commit_index = 0;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;

	entry = make_log(1, 2);
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);

	peers[1].match_index = 1;
	peers[2].match_index = 1;

	ck_assert_int_eq(raft_test_api.raft_check_commit_index(1), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(raft_state.commit_index, 0);
}
END_TEST

START_TEST(test_conformance_advance_commit_index_current_term_quorum)
{
	struct conformance_ctx ctx = {
		.last_term = raft_state.current_term,
		.last_commit = raft_state.commit_index,
	};
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	struct raft_log *entry;

	setup_cluster(3);
	peers = *peers_ptr;
	*num_ptr = 3;

	raft_state.state = RAFT_STATE_LEADER;
	raft_state.current_term = 3;
	raft_state.commit_index = 0;
	raft_state.voted_for = raft_state.self_id;
	client_state->current_leader_id = raft_state.self_id;

	entry = make_log(1, 3);
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);

	peers[1].match_index = 1;
	peers[2].match_index = 1;

	ck_assert_int_eq(raft_test_api.raft_check_commit_index(1), 1);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();
	assert_monotonicity(&ctx);

	ck_assert_uint_eq(raft_state.commit_index, 1);
}
END_TEST

START_TEST(test_conformance_restart_preserves_stable_state)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	unsigned *num_ptr = raft_test_api.num_peers_ptr();
	struct raft_host_entry *peers;
	struct raft_log *entry;

	setup_cluster(3);
	peers = *peers_ptr;
	*num_ptr = 3;

	raft_state.current_term = 5;
	raft_state.voted_for = 2;
	raft_state.state = RAFT_STATE_LEADER;
	raft_state.commit_index = 2;
	raft_state.last_applied = 2;

	entry = make_log(1, 4);
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	entry = make_log(2, 5);
	ck_assert_int_eq(raft_test_api.raft_append_log(entry, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = 2;

	peers[1].next_index = 3;
	peers[1].match_index = 2;
	peers[1].vote_responded = true;
	peers[1].vote_granted = raft_state.self_id;

	simulate_restart();

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();

	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert_uint_eq(raft_state.current_term, 5);
	ck_assert_uint_eq(raft_state.voted_for, 2);
	ck_assert_uint_eq(raft_state.commit_index, 0);
	ck_assert_uint_eq(raft_state.last_applied, 0);
	ck_assert_uint_eq(raft_state.log_tail->index, 2);
	ck_assert_uint_eq(raft_state.log_tail->term, 5);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 2);
	ck_assert_uint_eq(peers[1].next_index, 1);
	ck_assert_uint_eq(peers[1].match_index, 0);
	ck_assert(!peers[1].vote_responded);
	ck_assert_uint_eq(peers[1].vote_granted, NULL_ID);
}
END_TEST

START_TEST(test_conformance_restart_keeps_voted_for)
{
	raft_status_t reply;

	setup_cluster(1);
	raft_state.current_term = 4;
	raft_state.voted_for = 2;
	raft_state.state = RAFT_STATE_LEADER;
	raft_state.commit_index = 1;

	simulate_restart();

	ck_assert_int_eq(send_request_vote(4, 3, 0, 0, &reply, NULL, NULL), 0);

	assert_election_safety();
	assert_log_matching();
	assert_leader_completeness();

	ck_assert_int_eq(reply, RAFT_FALSE);
	ck_assert_uint_eq(raft_state.voted_for, 2);
}
END_TEST

START_TEST(test_conformance_durable_state_round_trip)
{
	char tmpdir[PATH_MAX];
	int saved_cwd_fd = -1;
	struct raft_log *log1;
	struct raft_log *log2;

	ck_assert_int_eq(enter_temp_dir(tmpdir, sizeof(tmpdir), &saved_cwd_fd), 0);

	raft_state.self_id = 1;
	raft_state.current_term = 7;
	raft_state.voted_for = 2;
	init_state_filenames();

	log1 = make_log(1, 7);
	log2 = make_log(2, 7);

	ck_assert_int_eq(raft_test_api.raft_append_log(log1, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	ck_assert_int_eq(raft_test_api.raft_append_log(log2, &raft_state.log_head,
				&raft_state.log_tail, &raft_state.log_length), 0);
	raft_state.log_index = raft_state.log_tail->index;

	ck_assert_int_eq(raft_test_api.raft_save_state_vars(), 0);
	ck_assert_int_eq(raft_test_api.raft_save_state_log(), 0);

	reset_raft_state();
	raft_state.self_id = 1;
	commit_called = 0;
	init_state_filenames();

	ck_assert_int_eq(raft_test_api.raft_load_state_vars(), 0);
	ck_assert_int_eq(raft_test_api.raft_load_state_logs(), 0);

	ck_assert_uint_eq(raft_state.current_term, 7);
	ck_assert_uint_eq(raft_state.voted_for, 2);
	ck_assert_ptr_nonnull(raft_state.log_head);
	ck_assert_ptr_nonnull(raft_state.log_tail);
	ck_assert_uint_eq(raft_state.log_tail->index, 2);
	ck_assert_int_eq(atomic_load(&raft_state.log_length), 2);
	ck_assert_int_eq(commit_called, 0);
	ck_assert_uint_eq(raft_state.commit_index, 0);

	leave_temp_dir(tmpdir, saved_cwd_fd);
}
END_TEST

START_TEST(test_conformance_durable_state_truncated_log)
{
	char tmpdir[PATH_MAX];
	int saved_cwd_fd = -1;
	int fd;
	uint32_t header[4] = { 1, 1, RAFT_LOG_REGISTER_TOPIC, sizeof(uint32_t) };
	uint8_t log_byte = 0x5a;

	ck_assert_int_eq(enter_temp_dir(tmpdir, sizeof(tmpdir), &saved_cwd_fd), 0);

	raft_state.self_id = 1;
	init_state_filenames();

	fd = open(raft_state.fn_log, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	ck_assert_int_ne(fd, -1);
	ck_assert_int_eq(write(fd, header, sizeof(header)), (int)sizeof(header));
	ck_assert_int_eq(write(fd, &log_byte, sizeof(log_byte)), (int)sizeof(log_byte));
	close(fd);

	commit_called = 0;
	errno = 0;

	ck_assert_int_eq(raft_test_api.raft_load_state_logs(), -1);
	ck_assert_ptr_eq(raft_state.log_head, NULL);
	ck_assert_int_eq(commit_called, 0);

	leave_temp_dir(tmpdir, saved_cwd_fd);
}
END_TEST

START_TEST(test_conformance_restart_clears_volatile_state)
{
	struct raft_host_entry **peers_ptr = raft_test_api.peers_ptr();
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();
	struct raft_host_entry *peers;
	struct io_buf *head;
	struct io_buf *active;

	setup_cluster(2);
	peers = *peers_ptr;

	raft_state.state = RAFT_STATE_LEADER;
	raft_state.commit_index = 3;
	raft_state.last_applied = 2;
	raft_state.election = true;
	raft_state.election_timer = 123;
	raft_state.next_ping = 456;
	raft_state.next_request_vote = 789;

	client_state->current_leader_id = raft_state.self_id;
	client_state->current_leader = &peers[0];

	peers[0].vote_responded = true;
	peers[0].vote_granted = peers[0].server_id;
	peers[0].next_index = 5;
	peers[0].match_index = 4;

	peers[0].rd_state = RAFT_PCK_PACKET;
	peers[0].rd_offset = 7;
	peers[0].rd_need = 9;
	peers[0].rd_packet_length = 22;
	peers[0].rd_packet_buffer = malloc(16);
	ck_assert_ptr_nonnull(peers[0].rd_packet_buffer);

	head = calloc(1, sizeof(*head));
	active = calloc(1, sizeof(*active));
	ck_assert_ptr_nonnull(head);
	ck_assert_ptr_nonnull(active);
	head->buf = (uint8_t *)malloc(8);
	active->buf = (uint8_t *)malloc(4);
	ck_assert_ptr_nonnull(head->buf);
	ck_assert_ptr_nonnull(active->buf);
	head->size = 8;
	active->size = 4;
	peers[0].wr_head = head;
	peers[0].wr_tail = head;
	peers[0].wr_active = active;
	peers[0].wr_queue = 1;
	peers[0].wr_need = 4;
	peers[0].wr_packet_length = 4;
	peers[0].wr_offset = 1;
	atomic_store_explicit(&peers[0].wr_packet_buffer,
			(_Atomic const uint8_t *)active->buf, memory_order_seq_cst);

	peers[0].ss_need = 12;
	peers[0].ss_offset = 5;
	peers[0].ss_tried_offset = 3;
	peers[0].ss_tried_length = 7;
	peers[0].ss_tried_status = RAFT_OK;
	peers[0].ss_last_index = 9;
	peers[0].ss_last_term = 3;
	atomic_store_explicit(&peers[0].ss_data,
			(_Atomic const uint8_t *)malloc(10), memory_order_seq_cst);

	simulate_restart();

	ck_assert_int_eq(raft_state.state, RAFT_STATE_FOLLOWER);
	ck_assert_uint_eq(raft_state.commit_index, 0);
	ck_assert_uint_eq(raft_state.last_applied, 0);
	ck_assert(!raft_state.election);
	ck_assert_int_eq(raft_state.election_timer, 0);
	ck_assert_int_eq(raft_state.next_ping, 0);
	ck_assert_int_eq(raft_state.next_request_vote, 0);

	ck_assert_uint_eq(client_state->current_leader_id, NULL_ID);
	ck_assert_ptr_eq(client_state->current_leader, NULL);

	ck_assert_uint_eq(peers[0].next_index, 1);
	ck_assert_uint_eq(peers[0].match_index, 0);
	ck_assert(!peers[0].vote_responded);
	ck_assert_uint_eq(peers[0].vote_granted, NULL_ID);

	ck_assert_int_eq(peers[0].rd_state, RAFT_PCK_NEW);
	ck_assert_int_eq(peers[0].rd_offset, 0);
	ck_assert_int_eq(peers[0].rd_need, 0);
	ck_assert_int_eq(peers[0].rd_packet_length, 0);
	ck_assert_ptr_eq(peers[0].rd_packet_buffer, NULL);

	ck_assert_int_eq(peers[0].wr_offset, 0);
	ck_assert_int_eq(peers[0].wr_need, 0);
	ck_assert_int_eq(peers[0].wr_packet_length, 0);
	ck_assert_ptr_eq(atomic_load_explicit(&peers[0].wr_packet_buffer,
				memory_order_seq_cst), NULL);
	ck_assert_ptr_eq(peers[0].wr_head, NULL);
	ck_assert_ptr_eq(peers[0].wr_tail, NULL);
	ck_assert_ptr_eq(peers[0].wr_active, NULL);
	ck_assert_int_eq(peers[0].wr_queue, 0);

	ck_assert_int_eq(peers[0].ss_need, 0);
	ck_assert_int_eq(peers[0].ss_offset, 0);
	ck_assert_int_eq(peers[0].ss_tried_offset, 0);
	ck_assert_int_eq(peers[0].ss_tried_length, 0);
	ck_assert_int_eq(peers[0].ss_tried_status, RAFT_ERR);
	ck_assert_int_eq(peers[0].ss_last_index, 0);
	ck_assert_int_eq(peers[0].ss_last_term, 0);
	ck_assert_ptr_eq(atomic_load_explicit(&peers[0].ss_data,
				memory_order_seq_cst), NULL);
}
END_TEST

static Suite *raft_conformance_suite(void)
{
	Suite *s = suite_create("raft_conformance");
	TCase *tc = tcase_create("tla");

	tcase_add_checked_fixture(tc, setup, teardown);
	tcase_add_test(tc, test_conformance_timeout_single_node);
	tcase_add_test(tc, test_conformance_request_vote_grant);
	tcase_add_test(tc, test_conformance_request_vote_rejects_stale_term);
	tcase_add_test(tc, test_conformance_request_vote_rejects_outdated_log);
	tcase_add_test(tc, test_conformance_request_vote_rejects_already_voted);
	tcase_add_test(tc, test_conformance_request_vote_rejects_null_candidate);
	tcase_add_test(tc, test_conformance_request_vote_same_candidate);
	tcase_add_test(tc, test_conformance_append_entries_heartbeat);
	tcase_add_test(tc, test_conformance_append_entries_steps_down_candidate);
	tcase_add_test(tc, test_conformance_append_entries_rejects_stale_term);
	tcase_add_test(tc, test_conformance_append_entries_rejects_prev_log_mismatch);
	tcase_add_test(tc, test_conformance_append_entries_rejects_prev_log_when_empty);
	tcase_add_test(tc, test_conformance_append_entries_conflict_replaces_entry);
	tcase_add_test(tc, test_conformance_append_entries_duplicate_no_growth);
	tcase_add_test(tc, test_conformance_append_entries_appends_and_commits);
	tcase_add_test(tc, test_conformance_append_entries_out_of_order_recovery);
	tcase_add_test(tc, test_conformance_append_entries_commit_cap);
	tcase_add_test(tc, test_conformance_append_entries_reply_updates_match_index);
	tcase_add_test(tc, test_conformance_append_entries_reply_higher_term_steps_down);
	tcase_add_test(tc, test_conformance_append_entries_reply_decrements_next_index);
	tcase_add_test(tc, test_conformance_follower_catch_up_fresh_short);
	tcase_add_test(tc, test_conformance_follower_catch_up_fresh_long);
	tcase_add_test(tc, test_conformance_follower_catch_up_after_restart);
	tcase_add_test(tc, test_conformance_restart_preserves_stable_state);
	tcase_add_test(tc, test_conformance_restart_keeps_voted_for);
	tcase_add_test(tc, test_conformance_durable_state_round_trip);
	tcase_add_test(tc, test_conformance_durable_state_truncated_log);
	tcase_add_test(tc, test_conformance_restart_clears_volatile_state);
	tcase_add_test(tc, test_conformance_client_request_appends_log);
	tcase_add_test(tc, test_conformance_client_request_rejected_when_not_leader);
	tcase_add_test(tc, test_conformance_client_request_commit_applies_with_quorum);
	tcase_add_test(tc, test_conformance_advance_commit_index_requires_current_term);
	tcase_add_test(tc, test_conformance_advance_commit_index_current_term_quorum);
	tcase_add_test(tc, test_model_log_matching_prefix_holds);
	tcase_add_test(tc, test_model_log_matching_prefix_detects_violation);
	tcase_add_test(tc, test_model_leader_completeness_holds);
	tcase_add_test(tc, test_model_leader_completeness_detects_violation);
	tcase_add_test(tc, test_model_three_node_election_and_replication);
	tcase_add_test(tc, test_model_three_node_election_no_majority);
	tcase_add_test(tc, test_model_split_brain_single_leader);
	tcase_add_test(tc, test_model_leadership_change_preserves_log_matching);
	tcase_add_test(tc, test_model_append_entries_backtrack_converges);
	tcase_add_test(tc, test_model_partition_merge_higher_term_wins);
	tcase_add_test(tc, test_model_liveness_eventual_leader_and_commit);
	tcase_add_test(tc, test_model_message_reorder_recovery);
	tcase_add_test(tc, test_model_duplicate_append_entries_idempotent);
	tcase_add_test(tc, test_model_drop_append_entries_no_effect);
	tcase_add_test(tc, test_impl_three_node_leader_replication);
	tcase_add_test(tc, test_impl_log_matching_after_replication);
	tcase_add_test(tc, test_impl_leader_completeness_after_leader_change);
	tcase_add_test(tc, test_model_quorum_intersection_majority);
	tcase_add_test(tc, test_conformance_request_vote_reply_higher_term_steps_down);
	tcase_add_test(tc, test_conformance_request_vote_reply_higher_term_steps_down_leader);
	tcase_add_test(tc, test_conformance_request_vote_reply_majority);
	tcase_add_test(tc, test_conformance_request_vote_reply_stale_term_ignored);
	tcase_add_test(tc, test_conformance_request_vote_reply_ignored_if_not_candidate);
	tcase_add_test(tc, test_conformance_append_entries_reply_next_index_floor);
	tcase_add_test(tc, test_conformance_append_entries_reply_needs_snapshot);

	suite_add_tcase(s, tc);
	return s;
}

int main(void)
{
	Suite *s = raft_conformance_suite();
	SRunner *sr = srunner_create(s);
	int failed;

	srunner_run_all(sr, CK_ENV);
	failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return failed == 0 ? 0 : 1;
}
