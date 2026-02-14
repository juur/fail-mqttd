#ifndef _FAIL_DEBUG_H
#define _FAIL_DEBUG_H

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 800
#endif

#if defined(FEATURE_DEBUG) || defined(FEATURE_RAFT_DEBUG) || defined(FEATURE_RAFT_IMPL_DEBUG)
# define CRESET "\x1b[0m"

# define BBLU "\x1b[1;34m"
# define BCYN "\x1b[1;36m"
# define BGRN "\x1b[1;32m"
# define BMAG "\x1b[1;35m"
# define BRED "\x1b[1;31m"
# define BWHT "\x1b[1;37m"
# define BYEL "\x1b[1;33m"

# define NBLU "\x1b[0;34m"
# define NCYN "\x1b[0;36m"
# define NGRN "\x1b[0;32m"
# define NMAG "\x1b[0;35m"
# define NRED "\x1b[0;31m"
# define NWHT "\x1b[0;37m"
# define NYEL "\x1b[0;33m"
#endif

#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdarg.h>

#ifdef FEATURE_DEBUG
# define dbg_printf(...) { int64_t dbg_now = timems(); printf("%lu.%03lu: ", dbg_now / 1000, dbg_now % 1000); printf(__VA_ARGS__); }
# define dbg_cprintf(...) { printf(__VA_ARGS__); }
#else
# define dbg_printf(...) { }
# define dbg_cprintf(...) { }
#endif

#ifdef FEATURE_RAFT_DEBUG
# define rdbg_printf(...) { int64_t dbg_now = timems(); printf("%lu.%03lu: ", dbg_now / 1000, dbg_now % 1000); printf(__VA_ARGS__); }
# define rdbg_cprintf(...) { printf(__VA_ARGS__); }
#else
# define rdbg_printf(...) { }
# define rdbg_cprintf(...) { }
#endif

#ifdef FEATURE_RAFT_DEBUG
# define ridbg_printf(...) { int64_t dbg_now = timems(); printf("%lu.%03lu: ", dbg_now / 1000, dbg_now % 1000); printf(__VA_ARGS__); }
# define ridbg_cprintf(...) { printf(__VA_ARGS__); }
#else
# define ridbg_printf(...) { }
# define ridbg_cprintf(...) { }
#endif

#include "mqtt.h"

extern const char *uuid_to_string(const uint8_t uuid[const static UUID_SIZE]);

void dump_client(const struct client *client);
void dump_message(const struct message *message);
void dump_mds(const struct message_delivery_state *mds);
void dump_packet(const struct packet *packet);
void dump_subscription(const struct subscription *subscription);
void dump_session(const struct session *session);
void dump_topic(const struct topic *topic);
void dump_any(mqtt_type type, const void *ptr);

void dump_all_clients(void);
void dump_all_sessions(void);
void dump_all_messages(void);
void dump_all_packets(void);
void dump_all_topics(void);
void dump_all_mds(void);
void dump_all_subscriptions(void);
void dump_all(void);
#endif
