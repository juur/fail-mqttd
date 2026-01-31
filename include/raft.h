#ifndef _FAIL_RAFT_H
#define _FAIL_RAFT_H

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 800
#endif

#if defined __has_attribute
# if __has_attribute (counted_by)
# else
#  define counted_by(x)
# endif
#else
# define counted_by(x)
#endif

#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <limits.h>

#ifndef UUID_SIZE
# define UUID_SIZE 16
#endif

#ifndef TIMEMS_T_DEFINED
#define TIMEMS_T_DEFINED
typedef long timems_t;
#endif

typedef enum {
    RAFT_HELLO = 0,
    RAFT_APPEND_ENTRIES,
    RAFT_REQUEST_VOTE,
    RAFT_ADD_SERVER,
    RAFT_REMOVE_SERVER,
    RAFT_CLIENT_REQUEST,
    RAFT_REGISTER_CLIENT,
    RAFT_CLIENT_QUERY,
    RAFT_INSTALL_SNAPSHOT,

    RAFT_APPEND_ENTRIES_REPLY,
    RAFT_REQUEST_VOTE_REPLY,
    RAFT_ADD_SERVER_REPLY,
    RAFT_REMOVE_SERVER_REPLY,
    RAFT_CLIENT_REQUEST_REPLY,
    RAFT_REGISTER_CLIENT_REPLY,
    RAFT_CLIENT_QUERY_REPLY,
    RAFT_INSTALL_SNAPSHOT_REPLY,

    RAFT_MAX_RPC
} raft_rpc_t;

typedef enum {
    RAFT_ERR = -1,
    RAFT_OK = 0,
    RAFT_TRUE,
    RAFT_FALSE,
    RAFT_NOT_LEADER,
    RAFT_SESSION_EXPIRED,
    RAFT_TIMEOUT,
    RAFT_MAX_STATUS
} raft_status_t;

typedef enum {
    RAFT_STATE_NONE = 0,
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_LEADER,
    RAFT_MAX_STATES,
} raft_state_t;

typedef enum {
    RAFT_PEER = 0,
    RAFT_CLIENT,
    RAFT_SERVER,
    RAFT_MAX_CONN,
} raft_conn_t;

typedef enum {
    RAFT_PCK_NEW = 0,
    RAFT_PCK_HELLO,
    RAFT_PCK_HEADER,
    RAFT_PCK_PACKET,
    RAFT_PCK_PROCESS,
    RAFT_PCK_EMPTY,
    RAFT_MAX_PCK,
} raft_rd_state_t;

enum {
    NULL_ID = -1U,
    BROADCAST_ID = -1,
};

/* this must define a typedef enum {} raft_log_t
 * this must include RAFT_LOG_NOOP = 0 as the fast entry
 * this must include RAFT_LOG_MAX as the final entry
 *
 * this must define a union raft_log_options {}
 * this may include multiple named struct {} for each RAFT_LOG_* that has data
 */
#include "raft_impl.h"

struct raft_log {
    struct raft_log *next;
    raft_conn_t role;
    raft_log_t event;
    uint8_t flags;
    uint32_t index;
    uint32_t term;
    uint32_t sequence_num;
    pthread_mutex_t mutex;
    pthread_cond_t cv;
    bool done;
    union raft_log_options opt;
};

struct raft_packet {
    raft_rpc_t rpc; /* uint8_t */
    uint8_t flags;
    raft_conn_t role; /* uint8_t */
    uint8_t res0;
    uint32_t length;
};

#define RAFT_HDR_SIZE   (1+1+1+1+4)

/**
 * RAFT_HELLO
 *
 * Header
 * u32 id
 * u8  type (raft_conn_t)
 * u32 mqtt-addr in_addr_t
 * u16 mqtt-port in_port_t
 */

#define RAFT_HELLO_SIZE (4 + 1 + 4 + 2)

struct raft_host_entry {
    /* candidateVar = */
    bool vote_responded;   /* VARIABLE votesResponded (candidate) */
    uint32_t vote_granted; /* VARIABLE votesGranted (candidate)   */
    /* voterLog - used in proof only */

    /* leaderVars = */
    uint32_t next_index;        /* VARIABLE nextIndex (leader)  */
    uint32_t match_index;       /* VARIABLE matchIndex (leader) */
    /* elections - used in proof only */

    int peer_fd;
    uint32_t server_id;
    timems_t next_conn_attempt;
    timems_t last_leader_sync; /* rate limit RAFT_APPEND_ENTRIES (non-PING) */

    struct in_addr address;
    in_port_t port;
    struct in_addr mqtt_addr;
    in_port_t mqtt_port;

    /* for connection handling */
    raft_rd_state_t rd_state;

    /* read buffering */
    off_t rd_offset;
    raft_rpc_t rd_rpc;
    ssize_t rd_need;
    ssize_t rd_packet_length;
    struct raft_host_entry *unknown_next;
    uint8_t *rd_packet_buffer;
    uint8_t rd_packet[RAFT_HDR_SIZE + RAFT_HELLO_SIZE];

    /* write buffering */
    _Atomic const uint8_t *wr_packet_buffer;
    off_t wr_offset;
    ssize_t wr_need;
    ssize_t wr_packet_length;
    pthread_rwlock_t wr_lock;
    struct io_buf *wr_head;
    struct io_buf *wr_tail;
    struct io_buf *wr_active;
    unsigned wr_queue;

    /* snapshot sending */
    _Atomic const uint8_t *ss_data;
    off_t ss_offset;
    ssize_t ss_need;
    uint32_t ss_length;
    pthread_rwlock_t ss_lock;

    uint32_t ss_tried_offset;      /* may have failed */
    uint32_t ss_tried_length;      /* may have failed */
    raft_status_t ss_tried_status;  /* may have failed */

    uint32_t ss_last_index;        /* per RPC */
    uint32_t ss_last_term;         /* per RPC */
};

struct raft_state {
    /* logVars = */
    struct raft_log *log_head;
    struct raft_log *log_tail;
    uint32_t commit_index;      /* VARIABLE commitIndex */
    _Atomic long log_length;

    /* serverVars = */
    uint32_t current_term;      /* VARIABLE currentTerm */
    uint32_t voted_for;         /* VARIABLE votedFor    */
    raft_state_t state;         /* VARIABLE state       */

    uint32_t last_applied;

    uint32_t self_id;
    //uint32_t log_index;
    timems_t election_timer;
    bool     election;
    timems_t next_ping;
    timems_t next_request_vote;

    char fn_prefix[NAME_MAX/2];
    char fn_vars[NAME_MAX-8];
    char fn_log[NAME_MAX-8];
    char fn_vars_new[NAME_MAX];
    uint32_t last_saved_index;

    /* this is a 'global mutex' for which thread owns raft operations
     * typically this will be the raft thread, except for when the
     * leader is acting as a client to itself, where the mqtt thread
     * invokes the log append directly. */
    pthread_mutex_t lock;
};

#include <stdarg.h>

struct raft_local_append {
    struct raft_local_append *next;
    raft_log_t event;
    va_list ap;
};

struct raft_client_state {
    /* for client */
    uint32_t current_leader_id;
    struct raft_host_entry *current_leader;
    _Atomic uint32_t sequence_num;
    struct raft_log *log_pending_head;
    struct raft_log *log_pending_tail;
    struct raft_host_entry *unknown_clients;
    pthread_rwlock_t log_pending_lock;
    /* lock for the entire raft_client_state */
    pthread_rwlock_t lock;
};

struct io_buf {
    struct io_buf *next;
    uint8_t *buf;
    ssize_t size;
};

struct send_state {
    uint8_t *ptr;
    uint16_t arg_req_len;

    uint8_t *arg_str;
    uint8_t *arg_uuid;
    uint8_t *arg_msg_uuid;
    uint32_t arg_flags;
};

struct raft_impl_entry {
    int (*free_log)(struct raft_log *);
    int (*commit_and_advance)(struct raft_log *);
    int (*leader_append)(struct raft_log *, va_list);
    int (*client_append)(struct raft_log *, raft_log_t, va_list);
    int (*pre_send)(struct raft_log *, struct send_state *);
    int (*fill_send)(struct send_state *, const struct raft_log *);
    raft_status_t (*process_packet)(size_t *, const uint8_t **, raft_rpc_t rpc, raft_log_t, struct raft_log *);
    int (*save_log)(const struct raft_log *, uint8_t **);
    int (*read_log)(struct raft_log *, const uint8_t *, int);
};

struct raft_impl {
    const char *const name;
    const unsigned num_log_types;
    const struct raft_impl_entry handlers[] __attribute__((counted_by(num_log_types)));
};

extern const char *const raft_rpc_str[RAFT_MAX_RPC];
extern const char *const raft_status_str[RAFT_MAX_STATUS];
extern const char *const raft_mode_str[RAFT_MAX_STATES];
extern const char *const raft_conn_str[RAFT_MAX_CONN];
extern const char *const raft_log_str[RAFT_MAX_LOG];

/**
 * RAFT_CLIENT_REQUEST
 *
 * Header
 * u32   client_id
 * u32   sequence_num
 * u8    type
 * u8    flags
 * u16   len
 * 0..n  log_entries[1]
 */

#define RAFT_CLIENT_REQUEST_SIZE (4 + 4 + 1 + 1 + 2)

/**
 * RAFT_APPEND_ENTRIES
 *
 * Header
 * u32   term
 * u32   leader_id
 * u32   prev_log_index
 * u32   prev_log_term
 * u32   leader_commit
 * u32   num_entries
 * 0..n  log_entries[num_entries] (see: RAFT_LOG_*)
 */
#define RAFT_APPEND_ENTRIES_FIXED_SIZE  (6 * 4)


/**
 * RAFT_APPEND_ENTRIES_REPLY
 *
 * Header
 * u8    reply (RAFT_TRUE/RAFT_FALSE)
 * u32   current_term
 * u32   new_match_index
 */
#define RAFT_APPEND_ENTRIES_REPLY_SIZE (1+4+4)

/**
 * RAFT_REQUEST_VOTE_REPLY
 *
 * Header
 * u8    status
 * u32   term
 * u32   voted_for
 */
#define RAFT_REQUEST_VOTE_REPLY_SIZE (1+4+4)

/**
 * RAFT_CLIENT_REQUEST_REPLY
 *
 * Header
 * u8    status
 * u8    log_type?
 * u32   client_id
 * u32   sequence_num
 */

#define RAFT_CLIENT_REQUEST_REPLY_SIZE (1+1+4+4)

/** RAFT_REQUEST_VOTE
 *
 * Header
 * u32   term
 * u32   candidate_id
 * u32   last_log_index
 * u32   last_log_term
 */
#define RAFT_REQUEST_VOTE_SIZE (4+4+4+4)

/** RAFT_REGISTER_CLIENT_REPLY
 *
 * Header
 * u8    status
 * u32   client_id
 * u32   leader_hint
 */
#define RAFT_REGISTER_CLIENT_REPLY_SIZE (1+4+4)

/** RAFT_REGISTER_CLIENT_SIZE
 *
 * Header
 * TBC
 */
#define RAFT_REGISTER_CLIENT_SIZE (0)

/** RAFT_INSTALL_SNAPSHOT
 *
 * Header
 * u32   leader_term
 * u32   leader_id
 * u32   last_index
 * u32   last_term
 * u32   offset
 * u8    flags: 1 = done, 2 = has_last_config
 * u16   last_config_length (optional)
 * u8[]  last_config
 * u32   data_length
 * u8[]  data
 */
#define RAFT_INSTALL_SNAPSHOT_FIXED_SIZE (4+4+4+4+4+8)

/**
 * RAFT_LOG_*
 *
 * u8    type
 * u8    flags
 * u32   index
 * u32   term
 * 0..n  (depends on type)
 * u16   entry length
 *
*/

# define RAFT_LOG_FIXED_SIZE (1+1+4+4+2)

extern int   raft_client_log_send(raft_log_t event, ...);
extern int   raft_leader_log_append(raft_log_t event, ...);
extern int   raft_send(raft_conn_t mode, struct raft_host_entry *client, raft_rpc_t rpc, ...);
extern void *raft_loop(void *start_args);
extern bool  raft_is_leader(void);
extern int   raft_parse_cmdline_host_list(const char *tmp, int extra);
extern int   raft_get_leader_address(char tmpbuf[static INET_ADDRSTRLEN+1+5+1], size_t len);

#endif
