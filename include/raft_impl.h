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
};

/*
 * RAFT_LOG_REGISTER_TOPIC
 *
 * u32    flags (1 = retained)
 * u16    string_length
 * 1..n   u8[n] (0 terminated uint8_t string)
 * u8[16] uuid
 * u8[16] uuid retained message (OPTIONAL)
 *
*/

# define RAFT_LOG_REGISTER_TOPIC_HAS_RETAINED    (1<<0)

/* RAFT_LOG_UNREGISTER_TOPIC
 *
 * u8[16] uuid
 */

#endif
