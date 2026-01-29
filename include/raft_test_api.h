#ifndef _FAIL_RAFT_TEST_API_H
#define _FAIL_RAFT_TEST_API_H

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "raft.h"

struct raft_test_api {
    long (*rnd)(int from, int to);
    int64_t (*timems)(void);

    int (*raft_save_state_vars)(void);
    int (*raft_save_state_log)(bool);
    int (*raft_load_state_vars)(void);
    int (*raft_load_state_logs)(void);
    void (*raft_reset_read_state)(struct raft_host_entry *client);
    bool (*raft_has_pending_write)(struct raft_host_entry *client);
    int (*raft_clear_active_write)(struct raft_host_entry *client);
    int (*raft_reset_write_state)(struct raft_host_entry *client, bool need_lock);
    int (*raft_reset_ss_state)(struct raft_host_entry *client, bool need_lock);
    int (*raft_update_leader_id)(uint32_t leader_id, bool need_lock);
    int (*raft_update_term)(uint32_t new_term);
    struct raft_log *(*raft_log_at)(uint32_t index);
    uint32_t (*raft_term_at)(uint32_t index);
    int (*raft_close)(struct raft_host_entry *client);
    struct raft_log *(*raft_alloc_log)(raft_conn_t conn_type, raft_log_t event);
    int (*raft_free_log)(struct raft_log *entry);
    int (*raft_remove_log)(struct raft_log *entry, struct raft_log **head,
            struct raft_log **tail, pthread_rwlock_t *lock, _Atomic long *log_len);
    int (*raft_append_log)(struct raft_log *entry, struct raft_log **head,
            struct raft_log **tail, _Atomic long *log_len);
    int (*raft_prepend_log)(struct raft_log *entry, struct raft_log **head,
            struct raft_log **tail, _Atomic long *log_len);
    int (*raft_commit_and_advance)(void);
    int (*raft_check_commit_index)(uint32_t best);
    int (*raft_await_client_response)(struct raft_log *pending_event);
    int (*raft_client_log_sendv)(raft_log_t event, va_list ap);
    int (*raft_client_log_append_single)(struct raft_log *raft_log, uint32_t leader_commit);
    void (*raft_remove_and_free_unknown_host)(struct raft_host_entry *entry);
    int (*raft_new_conn)(int new_fd, struct raft_host_entry *unknown_host,
            const struct sockaddr_in *sin, socklen_t sin_len);
    int (*raft_add_write)(struct raft_host_entry *client, uint8_t *buffer, ssize_t size);
    int (*raft_try_write)(struct raft_host_entry *client);
    int (*raft_append_iobuf)(struct io_buf *entry, struct io_buf **head,
            struct io_buf **tail, unsigned *iobuf_len);
    int (*raft_remove_iobuf)(struct io_buf *entry, struct io_buf **head,
            struct io_buf **tail, unsigned *iobuf_len);
    int (*raft_reset_election_timer)(void);
    int (*raft_reset_next_ping)(void);
    int (*raft_change_to)(raft_state_t mode);
    int (*raft_tick_connection_check)(void);
    int (*raft_stop_election)(void);
    int (*raft_request_votes)(void);
    int (*raft_start_election)(void);
    int (*raft_tick)(void);
    raft_status_t (*process_append_entries)(struct raft_host_entry *client,
            uint32_t term, uint32_t leader_id, uint32_t prev_log_index,
            uint32_t prev_log_term, struct raft_log *log_entry_head,
            uint32_t leader_commit, uint32_t new_match_index);
    int (*raft_process_packet)(struct raft_host_entry *client, raft_rpc_t rpc);
    int (*raft_recv)(struct raft_host_entry *client);
    void (*raft_clean)(void);
    int (*raft_init)(void);
    struct raft_host_entry **(*peers_ptr)(void);
    unsigned *(*num_peers_ptr)(void);
    struct raft_client_state *(*client_state_ptr)(void);
};

extern const struct raft_test_api raft_test_api;
#endif
