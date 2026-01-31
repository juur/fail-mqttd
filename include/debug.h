#ifndef _DEBUG_H
#define _DEBUG_H

#define _XOPEN_SOURCE 800

#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>

#ifndef FEATURE_DEBUG
# define dbg_printf(...) { }
# define dbg_cprintf(...) { }
#else
# define dbg_printf(...) { int64_t dbg_now = timems(); printf("%lu.%03lu: ", dbg_now / 1000, dbg_now % 1000); printf(__VA_ARGS__); }
# define dbg_cprintf(...) { printf(__VA_ARGS__); }
#endif
#ifndef FEATURE_RAFT_DEBUG
# define rdbg_printf(...) { }
# define rdbg_cprintf(...) { }
#else
# define rdbg_printf(...) { int64_t dbg_now = timems(); printf("%lu.%03lu: ", dbg_now / 1000, dbg_now % 1000); printf(__VA_ARGS__); }
# define rdbg_cprintf(...) { printf(__VA_ARGS__); }
#endif

#if defined(FEATURE_DEBUG) || defined(FEATURE_RAFT_DEBUG)
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

#include "mqtt.h"

extern const char *uuid_to_string(const uint8_t uuid[const static UUID_SIZE]);
#endif
