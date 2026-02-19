#ifndef RAFT_TEST_SUPPORT_H
#define RAFT_TEST_SUPPORT_H

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <check.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>

#include "raft.h"
#include "raft_test_api.h"

static inline void raft_test_init_entry_locks(struct raft_host_entry *entry)
{
	ck_assert_int_eq(pthread_rwlock_init(&entry->wr_lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&entry->ss_lock, NULL), 0);
}

static inline void raft_test_destroy_entry_locks(struct raft_host_entry *entry)
{
	pthread_rwlock_destroy(&entry->wr_lock);
	pthread_rwlock_destroy(&entry->ss_lock);
}

static inline void raft_test_init_client_state(void)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	memset(client_state, 0, sizeof(*client_state));
	ck_assert_int_eq(pthread_rwlock_init(&client_state->lock, NULL), 0);
	ck_assert_int_eq(pthread_rwlock_init(&client_state->log_pending_lock, NULL), 0);
	client_state->current_leader_id = NULL_ID;
}

static inline void raft_test_destroy_client_state(void)
{
	struct raft_client_state *client_state = raft_test_api.client_state_ptr();

	pthread_rwlock_destroy(&client_state->log_pending_lock);
	pthread_rwlock_destroy(&client_state->lock);
}

static inline void raft_test_reset_state_basic(struct raft_state *state)
{
	struct raft_log *tmp;

	for (tmp = state->log_head; tmp;) {
		struct raft_log *next = tmp->next;
		raft_test_api.raft_free_log(tmp);
		tmp = next;
	}

	memset(state, 0, sizeof(*state));
	atomic_store(&state->log_length, 0);
	state->voted_for = NULL_ID;
}

static inline void raft_test_reset_state_full(struct raft_state *state, uint32_t self_id)
{
	raft_test_reset_state_basic(state);
	state->state = RAFT_STATE_FOLLOWER;
	state->current_term = 1;
	state->self_id = self_id;
}

static inline int raft_test_enter_temp_dir(char *path, size_t path_len,
		int *saved_cwd_fd, const char *template_name)
{
	const char *tmpdir = getenv("TMPDIR");

	if (!tmpdir)
		tmpdir = "/tmp";

	if (snprintf(path, path_len, "%s/%s", tmpdir, template_name) >= (int)path_len)
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

static inline void raft_test_init_state_filenames(struct raft_state *state)
{
	snprintf(state->fn_prefix, sizeof(state->fn_prefix),
			"save_state_%u_", state->self_id);
	snprintf(state->fn_vars, sizeof(state->fn_vars),
			"%svars.bin", state->fn_prefix);
	snprintf(state->fn_log, sizeof(state->fn_log),
			"%slog.bin", state->fn_prefix);
	snprintf(state->fn_vars_new, sizeof(state->fn_vars_new),
			"%s.new", state->fn_vars);
}

static inline void raft_test_leave_temp_dir(const struct raft_state *state,
		const char *path, int saved_cwd_fd)
{
	if (path) {
		unlink(state->fn_vars);
		unlink(state->fn_vars_new);
		unlink(state->fn_log);
	}

	if (saved_cwd_fd != -1) {
		if (fchdir(saved_cwd_fd) == -1)
			(void)0;
		close(saved_cwd_fd);
	}

	if (path)
		rmdir(path);
}

static inline struct raft_log *raft_test_make_log(uint32_t index, uint32_t term)
{
	struct raft_log *entry;

	entry = raft_test_api.raft_alloc_log(RAFT_PEER, RAFT_LOG_REGISTER_TOPIC);
	ck_assert_ptr_nonnull(entry);
	entry->index = index;
	entry->term = term;
	return entry;
}

#endif
