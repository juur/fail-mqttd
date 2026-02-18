#ifndef _RAFT_IMPL_H
# define _RAFT_IMPL_H

# ifndef _XOPEN_SOURCE
#  define _XOPEN_SOURCE 800
# endif

# ifndef UUID_SIZE
#  define UUID_SIZE 16
# endif

# include <stdint.h>

/* wire-format used for client to send & log[] for server
 *
 * u8   type   (raft_log_t)
 * u8   flags
 * u16  length
 * 0..n payload[length]
 */

typedef enum {
    RAFT_LOG_NOOP = 0,          /* Mandatory for all implementations */
    RAFT_LOG_REGISTER_TOPIC,
    RAFT_LOG_UNREGISTER_TOPIC,
    RAFT_LOG_REGISTER_SESSION,
    RAFT_LOG_UNREGISTER_SESSION,
    RAFT_MAX_LOG,
} raft_log_t;

union raft_log_options {
    struct {
        uint16_t length;
        uint8_t *name;
        uint8_t uuid[UUID_SIZE];
        bool retained;
        uint8_t msg_uuid[UUID_SIZE];
        uint32_t flags;
    } register_topic;
    struct {
        uint8_t uuid[UUID_SIZE];
    } unregister_topic;
    struct {
        uint16_t flags;
        uint32_t expiry_interval;
        uint8_t uuid[UUID_SIZE];
        uint32_t last_connected;
        uint16_t client_id_length;
        uint8_t *client_id;
    } register_session;
    struct {
        uint8_t uuid[UUID_SIZE];
    } unregister_session;
};

union raft_reply_options {
};

/*
 * RAFT_LOG_UNREGISTER_SESSION
 * u8[16] uuid
 */

enum { RAFT_LOG_UNREGISTER_SESSION_SIZE = 16 };

/*
 * RAFT_LOG_REGISTER_SESSION
 * u16    flags
 * u32    expiry_interval
 * u8[16] uuid
 * u32    last_connected (time_t)
 * u16    client_id_length
 * 1..n   u8[client_id_length] (unterminated uint8_t string)
 */

#define RAFT_LOG_REGISTER_SESSION_REQ_RESP_INFO (1<<0)
#define RAFT_LOG_REGISTER_SESSION_REQ_PROB_INFO (1<<1)
enum { RAFT_LOG_REGISTER_SESSION_SIZE = (2 + 4 + 16 + 4 + 2) };

/*
 * RAFT_LOG_REGISTER_TOPIC
 *
 * u32    flags (1 = retained)
 * u16    string_length
 * 1..n   u8[n] (unterminated uint8_t string)
 * u8[16] uuid
 * u8[16] uuid retained message (OPTIONAL)
 *
*/

#define RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED    (1<<0)
enum { RAFT_LOG_REGISTER_TOPIC_SIZE = (4 + 2 + 16) };

/* RAFT_LOG_UNREGISTER_TOPIC
 *
 * u8[16] uuid
 */

enum { RAFT_LOG_UNREGISTER_TOPIC_SIZE = 16 };

#endif
