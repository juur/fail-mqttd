#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 800
#endif

#include "config.h"

#include <stdlib.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <err.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <errno.h>
#include <sys/select.h>
#ifdef HAVE_STDATOMIC_H
# include <stdatomic.h>
#else
# warning "stdatomic.h is missing"
# define _Atomic
#endif
#include <signal.h>
#include <assert.h>
#include <arpa/inet.h>
#include <limits.h>
/* Far too much code to #ifdef guard for FEATURE_THREADS */
#ifdef HAVE_PTHREAD_H
# include <pthread.h>
#else
# error "pthread.h is required"
#endif
#include <stdarg.h>
#include <syslog.h>
#include <time.h>
#include <ifaddrs.h>
#include <net/if_arp.h>
#include <linux/if_packet.h>
#include <ndbm.h>
#include <sys/stat.h>
#include <endian.h>
#include <locale.h>

static int64_t timems(void);

#define RAFT_API_SOURCE_SELF

#include "mqtt.h"
#ifdef FEATURE_RAFT
#include "raft.h"
#endif
#include "debug.h"

#define MAX(a,b) (((a)>(b)) ? (a) : (b))
#define MIN(a,b) (((a)<(b)) ? (a) : (b))

#ifdef HAVE_STDATOMIC_H
# define GET_REFCNT(x) atomic_load_explicit(x, memory_order_relaxed)
# define IF_DEC_REFCNT(x) atomic_fetch_sub_explicit(x, 1, memory_order_acq_rel)
# ifdef DEBUG_REFCNT
#  define DEC_REFCNT(x) { \
    atomic_fetch_sub_explicit(x, 1, memory_order_acq_rel); \
    dbg_printf(NRED "     %s:%d DEC_REFCNT(%s)" CRESET "\n", \
            __func__, __LINE__, #x); }
#  define INC_REFCNT(x) { \
    atomic_fetch_add_explicit(x, 1, memory_order_relaxed); \
    dbg_printf(NYEL "     %s:%d INC_REFCNT(%s)" CRESET "\n", \
            __func__, __LINE__, #x); }
# else /* !DEBUG_REFCNT */
#  define DEC_REFCNT(x) atomic_fetch_sub_explicit(x, 1, memory_order_acq_rel)
#  define INC_REFCNT(x) atomic_fetch_add_explicit(x, 1, memory_order_relaxed)
# endif /* DEBUG_REFCNT */
#else /* !HAVE_STDATOMIC_H */
# define GET_REFCNT(x) x
# define DEC_REFCNT(x) x--
# define INC_REFCNT(x) x++
#endif /* HAVE_STDATOMIC_H */

#ifdef FEATURE_THREADS
# define RETURN_TYPE void *
#else
# define RETURN_TYPE int
#endif

/*
 * misc. globals
 */

static constexpr unsigned MAC_ADDR_LEN = 6;

_Atomic bool running;
static uint8_t global_hwaddr[MAC_ADDR_LEN];
static int global_mother_fd = -1;
#ifdef FEATURE_OM
static int global_om_fd = -1;
#endif

/*
 * unique ids
 */

static _Atomic id_t subscription_id = 1;
static _Atomic id_t session_id      = 1;
static _Atomic id_t topic_id        = 1;
static _Atomic id_t packet_id       = 1;
static _Atomic id_t message_id      = 1;
static _Atomic id_t client_id       = 1;
static _Atomic id_t mds_id          = 1;

/*
 * magic numbers
 */

static constexpr unsigned  MAX_PACKETS           = 0x100;
static constexpr unsigned  MAX_CLIENTS           = 0x100;
static constexpr unsigned  MAX_TOPICS            = 0x1000;
static constexpr unsigned  MAX_MESSAGES          = 0x100000;
static constexpr unsigned  MAX_PACKET_LENGTH     = 0x1000000U;
static constexpr unsigned  MAX_MESSAGES_PER_TICK = 0x100;
static constexpr unsigned  MAX_PROPERTIES        = 0x10;
static constexpr unsigned  MAX_RECEIVE_PUBS      = 0x10;
static constexpr unsigned  MAX_SESSIONS          = 0x100;
static constexpr unsigned  MAX_TOPIC_ALIAS       = 0x20;
static constexpr unsigned  MIN_KEEP_ALIVE        = 60;
static constexpr unsigned  MAX_CLIENTID_LEN      = 0x100;

static constexpr uint32_t  MAX_SUB_IDENTIFIER    = 0xfffffff;
static constexpr uint64_t  UUID_EPOCH_OFFSET     = 0x01B21DD213814000ULL;

static const char *const PID_FILE                = RUNSTATEDIR "/fail-mqttd.pid";
static const char *const DEF_DB_PATH             = LOCALSTATEDIR "/fail-mqttd/";

static constexpr char     SHARED_PREFIX[]        = {'$','s','h','a','r','e','d','/'};
static constexpr size_t   SHARED_PREFIX_LENGTH   = sizeof(SHARED_PREFIX);
static constexpr char     PROTOCOL_NAME[]        = {'M','Q','T','T'};
static constexpr size_t   PROTOCOL_NAME_LEN      = sizeof(PROTOCOL_NAME);
static constexpr uint8_t  PROTOCOL_VERSION       = 5;

/*
 * global lists and associated locks & counts
 */

static pthread_rwlock_t global_clients_lock       = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_sessions_lock      = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_messages_lock      = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_packets_lock       = PTHREAD_RWLOCK_INITIALIZER;
pthread_rwlock_t global_topics_lock        = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_mds_lock           = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_subscriptions_lock = PTHREAD_RWLOCK_INITIALIZER;

static struct client *global_client_list              = NULL;
static struct message *global_message_list            = NULL;
static struct packet *global_packet_list              = NULL;
static struct topic *global_topic_list                = NULL;
static struct session *global_session_list            = NULL;
static struct message_delivery_state *global_mds_list = NULL;
static struct subscription *global_subscription_list  = NULL;

static _Atomic unsigned num_clients       = 0;
static _Atomic unsigned num_messages      = 0;
static _Atomic unsigned num_packets       = 0;
static _Atomic unsigned num_topics        = 0;
static _Atomic unsigned num_sessions      = 0;
static _Atomic unsigned num_mds           = 0;
static _Atomic unsigned num_subscriptions = 0;
static _Atomic unsigned num_shared_subscriptions = 0;

static _Atomic unsigned long total_control_packets_recv            = 0;
static _Atomic unsigned long total_control_packets_processed       = 0;
static _Atomic unsigned long total_control_packets_processed_ok    = 0;

static _Atomic unsigned long total_messages_accepted_at            = 0;
static _Atomic unsigned long total_messages_acknowledged_at        = 0;
static _Atomic unsigned long total_messages_released_at            = 0;
static _Atomic unsigned long total_messages_completed_at           = 0;
static _Atomic unsigned long total_messages_sender_accepted_at     = 0;
static _Atomic unsigned long total_messages_sender_acknowledged_at = 0;
static _Atomic unsigned long total_messages_sender_released_at     = 0;
static _Atomic unsigned long total_messages_sender_completed_at    = 0;

static _Atomic bool has_clients = false;

/*
 * databases
 */

static DBM *topic_dbm = NULL;
static DBM *message_dbm = NULL;

/*
 * command line options
 */

static FILE       *opt_logfile       = NULL;
static bool        opt_logstdout     = true;
static int         opt_backlog       = 50;
static int         opt_loglevel      = LOG_INFO;
static bool        opt_logsyslog     = false;
static bool        opt_logfileappend = false;
static bool        opt_logfilesync   = false;
static bool        opt_background    = false;
static const char *opt_statepath     = NULL;

in_port_t      opt_port     = 1883;
bool           opt_database = true;
struct in_addr opt_listen;

static char *logfile_name = NULL;

#ifdef FEATURE_RAFT
uint8_t        opt_raft_id       = 0;
bool           opt_raft          = false;
in_port_t      opt_raft_port     = 0;
struct in_addr opt_raft_listen;
#endif

#ifdef FEATURE_OM
static bool           opt_openmetrics   = false;
static in_port_t      opt_om_port       = 1773;
static struct in_addr opt_om_listen;
#endif

static int load_message(datum /* key */, datum content);
static int load_topic(datum /* key */, datum content);

static const struct {
    DBM **global;
    char *const filename;
    int (*const func)(datum key, datum content);
    const size_t size;
} database_init[] = {
    { &message_dbm, "messages", load_message, sizeof(struct message_save) },
    { &topic_dbm, "topics", load_topic, sizeof(struct topic_save) },
    { NULL, NULL, NULL, -1 },
};

/*
 * forward declarations
 */

[[gnu::nonnull]] static bool is_null_uuid(const uint8_t uuid[static UUID_SIZE]);
[[gnu::nonnull, gnu::warn_unused_result]] struct session *find_session_by_uuid(const uint8_t uuid[static UUID_SIZE]);
[[gnu::nonnull, gnu::warn_unused_result]] struct topic *find_topic_by_uuid(const uint8_t uuid[static UUID_SIZE]);
[[gnu::nonnull]] static void handle_outbound(struct client *client);
[[gnu::nonnull]] static void set_outbound(struct client *client, const uint8_t *buf, unsigned len);
[[gnu::nonnull]] static void close_session(struct session *session
#ifdef FEATURE_RAFT
        , bool source_self
#endif
        );
[[gnu::nonnull]] static void close_client(struct client *client, reason_code_t reason, bool disconnect);
[[gnu::nonnull, gnu::warn_unused_result]] static int unsubscribe(struct subscription *sub, struct session *session);
[[gnu::nonnull, gnu::warn_unused_result]] static int dequeue_message(struct message *msg);
[[gnu::nonnull]] static void free_message(struct message *msg, bool need_lock);
[[gnu::nonnull, gnu::warn_unused_result]] static int remove_delivery_state(
        struct message_delivery_state ***state_array, unsigned *array_length,
        struct message_delivery_state *rem);
[[gnu::nonnull, gnu::warn_unused_result]] static int free_message_delivery_state(struct message_delivery_state *mds);
[[gnu::nonnull(3),gnu::format(printf,3,4)]] void logger(int priority,
        const struct client *client, const char *format, ...);
[[gnu::nonnull(1),gnu::warn_unused_result]] struct topic *register_topic(const uint8_t *name,
        const uint8_t uuid[UUID_SIZE] RAFT_API_SOURCE_SELF);
[[gnu::nonnull, gnu::warn_unused_result]] static int unsubscribe_from_topics(struct session *session,
        const struct topic_sub_request *request);
[[gnu::nonnull]] static int unsubscribe_session_from_all(struct session *session);
[[gnu::nonnull]] static int find_str(const char *const *lookup, const char *value, int max);

/*
 * command line stuff
 */

/**
 * show version to the specified FILE
 * @param fp FILE to output to.
 */
[[gnu::nonnull]]
static void show_version(FILE *fp)
{
    fprintf(fp, "fail-mqttd " VERSION "\n" "\n" "Written by http://github.com/juur" "\n");
}

/**
 * show usage information to the specified file
 * @param fp FILE to output to.
 * @param name typically argv[0] from main() to display
 */
[[gnu::nonnull]]
static void show_usage(FILE *fp, const char *name)
{
    char buf[BUFSIZ];
    char *ptr = buf;
    int rc;

    setlocale(LC_ALL, "C");

    for (unsigned idx = 0; database_init[idx].filename; idx++)
    {
        const size_t expected = sizeof(buf) - (ptr - buf);
        rc = snprintf(ptr, expected, "%s%s",
                database_init[idx].filename,
                database_init[idx+1].filename ? ", " : "");

        if (rc < 0)
            break;
        if ((size_t)rc > expected)
            break;

        ptr += rc;
    }

    fprintf(fp, "fail-mqttd -- a terrible implementation of MQTT\n"
            "\n"
            "Usage: %s [-hdVn] [-H ADDR] [-p PORT] "
            " [-l LOGOPTION,[LOGOPTION..]]"
            " [-D DB_PATH]"
#ifdef FEATURE_OM
            " [-o OMOPTION,[OMOPTION..]]"
#endif
#ifdef FEATURE_RAFT
            " [-r RAFTOPTION,[RAFTOPTION..]]"
#endif
            " [TOPIC..]\n"
            "Provides a MQTT 5.0 broker, optionally registering topics on boot "
            "per additional command line arguments [TOPIC..]\n"
            "\n"
            "Options:\n"
            "  -h             show help\n"
            "  -H ADDR        bind to IP address ADDR (default 127.0.0.1)\n"
            "  -p PORT        bind to TCP port PORT (default 1883)\n"
            "  -l LOGOPTION   comma separated suboptions, described below, for logging\n"
            "  -D DB_PATH     save database(s) to DB_PATH\n"

#ifdef FEATURE_OM
            "  -o OMOPTION    comma separated suboptions, described below, for openmetrics\n"
#endif

#ifdef FEATURE_RAFT
            "  -r RAFTOPTION  comma separated suboptions, described below, for raft clustering\n"
#endif

            "  -d             daemonize and create a PID file\n"
            "  -V             show version\n"
            "  -n             disable persistent database (saving/loading)\n"
            "\n"
            "The PID file will be created at %s.\n"
            "The database files will be named: %s; and by default will be located in %s.\n"
            "\n"
            "Each LOGOPTION may be:\n"
            "  syslog         log to syslog as LOG_DAEMON\n"
            "  [no]stdout     [don't] log to stdout\n"
            "  file=PATH      log to the given PATH\n"
            "  append         open PATH log file in append mode\n"
            "  sync           open PATH with no buffering\n"
            "  level=LEVEL    set the log min. priority to LEVEL, pass ? to see options\n"
            "\n"
            "The default is stdout, with priority filter set to INFO.\n"
            "\n"

#ifdef FEATURE_OM
            "Each OMOPTION may be:\n"
            "  enable         enable openmetrics\n"
            "  disable        disable openmetrics\n"
            "  port=PORT      bind to TCP port PORT (default 1337)\n"
            "  bind=ADDR      bind to IP address ADDR (default 127.0.0.1)\n"
            "\n"
            "The default is that openmetrics is disabled.\n"
            "\n"
#endif

#ifdef FEATURE_RAFT
            "Each RAFTOPTION may be:\n"
            "  addrs=HOST[/HOST...]  forward-slash separated list of id:host:port peers\n"
            "  enable                enable Raft clustering\n"
            "  disable               disable Raft clustering\n"
            "  server_id=ID          set this server's id\n"
            "  port=PORT             bind to TCP port (default 1800 + id)\n"
            "  bind=ADDR             bind to IP address ADDR (default 127.0.0.1)\n"
            "\n"
            "The default is that Raft clustering is disabled.\n"
            "\n"
            "*Additional Information*\n"
            "The latest version is at: <https://github.com/juur/fail-mqttd>\n"
            "\n"
#endif
            ,
        name,
        PID_FILE,
        buf,
        DEF_DB_PATH
            );
}

[[gnu::nonnull]]
static void parse_cmdline(int argc, char *argv[])
{
    int opt;
    char *subopts;
    char *value;

    enum {
        LOGGER_SYSLOG = 0,
        LOGGER_FILE,
        LOGGER_STDOUT,
        LOGGER_NOSTDOUT,
        LOGGER_LEVEL,
        LOGGER_APPEND,
        LOGGER_SYNC,
    };

    char *const logger_token[] = {
        [LOGGER_SYSLOG] = "syslog",
        [LOGGER_FILE] = "file",
        [LOGGER_STDOUT] = "stdout",
        [LOGGER_NOSTDOUT] = "nostdout",
        [LOGGER_LEVEL] = "level",
        [LOGGER_APPEND] = "append",
        [LOGGER_SYNC] = "sync",
        NULL
    };
#ifdef FEATURE_OM
    enum {
        OM_ENABLE = 0,
        OM_DISABLE,
        OM_PORT,
        OM_BIND,
    };

    char *const om_token[] = {
        [OM_ENABLE] = "enable",
        [OM_DISABLE] = "disable",
        [OM_PORT] = "port",
        [OM_BIND] = "bind",
        NULL,
    };

#endif
#ifdef FEATURE_RAFT
    enum {
        RAFT_CLUSTER_ADDRS = 0,
        RAFT_ENABLE,
        RAFT_DISABLE,
        RAFT_PORT,
        RAFT_BIND,
        RAFT_ID,
    };

    char *const raft_token[] = {
        [RAFT_CLUSTER_ADDRS] = "addrs",
        [RAFT_ENABLE] = "enable",
        [RAFT_DISABLE] = "disable",
        [RAFT_PORT] = "port",
        [RAFT_BIND] = "bind",
        [RAFT_ID] = "server_id",
        NULL
    };

#endif

    while ((opt = getopt(argc, argv, "hVp:H:l:do:nD:r:")) != -1)
    {
        [[maybe_unused]] int tmp;
        switch (opt)
        {
            case 'D':
                if ((opt_statepath = strdup(optarg)) == NULL)
                    err(EXIT_FAILURE, "main: strdup(optarg)");
                break;
            case 'n':
                opt_database = false;
                break;
            case 'd':
                opt_background = true;
                break;
#ifdef FEATURE_RAFT
            case 'r':
                subopts = optarg;
                while (*subopts != '\0') {
                    switch(getsubopt(&subopts, raft_token, &value))
                    {
                        case RAFT_ID:
                            opt_raft_id = atoi(value);
                            if (opt_raft_id == 0 || opt_raft_id >= 0xff)
                                goto shit_usage;
                            if (opt_raft_port == 0)
                                opt_raft_port = 1800 + opt_raft_id;
                            break;
                        case RAFT_ENABLE:
                            opt_raft = true;
                            break;
                        case RAFT_DISABLE:
                            opt_raft = false;
                            break;
                        case RAFT_PORT:
                            if (value == NULL)
                                goto shit_usage;
                            tmp = atoi(value);
                            if (tmp == 0 || tmp > USHRT_MAX)
                                goto shit_usage;
                            opt_raft_port = tmp;
                            break;
                        case RAFT_BIND:
                            if (value == NULL)
                                goto shit_usage;
                            if (inet_pton(AF_INET, value, &opt_raft_listen) != 1)
                                goto shit_usage;
                            break;
                        case RAFT_CLUSTER_ADDRS:
                            if ((raft_parse_cmdline_host_list(value, 1)) == -1)
                                goto shit_usage;
                            break;
                        default:
                            goto shit_usage;
                    }
                }
                break;
#endif
#ifdef FEATURE_OM
            case 'o':
                subopts = optarg;
                while (*subopts != '\0') {
                    switch(getsubopt(&subopts, om_token, &value))
                    {
                        case OM_ENABLE:
                            opt_openmetrics = true;
                            break;
                        case OM_DISABLE:
                            opt_openmetrics = false;
                            break;
                        case OM_PORT:
                            if (value == NULL)
                                goto shit_usage;
                            int tmp = atoi(value);
                            if (tmp == 0 || tmp > USHRT_MAX)
                                goto shit_usage;
                            opt_om_port = tmp;
                            break;
                        case OM_BIND:
                            if (value == NULL)
                                goto shit_usage;
                            if (inet_pton(AF_INET, value, &opt_om_listen) != 1)
                                goto shit_usage;
                            break;
                        default:
                            goto shit_usage;
                    }
                }
                break;
#endif
            case 'l':
                subopts = optarg;
                while (*subopts != '\0') {
                    switch(getsubopt(&subopts, logger_token, &value))
                    {
                        case LOGGER_SYSLOG:
                            opt_logsyslog = true;
                            break;
                        case LOGGER_FILE:
                            if (value == NULL)
                                goto shit_usage;
                            logfile_name = value;
                            break;
                        case LOGGER_STDOUT:
                            opt_logstdout = true;
                            break;
                        case LOGGER_NOSTDOUT:
                            opt_logstdout = false;
                            break;
                        case LOGGER_LEVEL:
                            if (value == NULL)
                                goto shit_usage;
                            if (!strcmp(value, "?")) {
                                for (unsigned idx = 0; priority_str[idx]; idx++)
                                    printf("%s\n", priority_str[idx]);
                                exit(EXIT_SUCCESS);
                            }
                            if ((opt_loglevel = find_str(priority_str, value, 0)) == -1)
                                goto shit_usage;
                            break;
                        case LOGGER_SYNC:
                            opt_logfilesync = true;
                            break;
                        case LOGGER_APPEND:
                            opt_logfileappend = true;
                            break;
                        default:
                            goto shit_usage;
                    }
                }
                break;
            case 'H':
                if (inet_pton(AF_INET, optarg, &opt_listen) != 1) {
                    warn("main: inet_pton");
                    exit(EXIT_FAILURE);
                }
                break;
            case 'p':
                int tmp = atoi(optarg);
                if (tmp == 0 || tmp > USHRT_MAX) {
                    show_usage(stderr, argv[0]);
                    exit(EXIT_FAILURE);
                }
                opt_port = tmp;
                break;
            case 'h':
                show_usage(stdout, argv[0]);
                exit(EXIT_SUCCESS);
            case 'V':
                show_version(stdout);
                exit(EXIT_SUCCESS);
            default:
shit_usage:
                show_usage(stderr, argv[0]);
                exit(EXIT_FAILURE);
        }
    }
}

/*
 * debugging helpers
 */

[[maybe_unused]] static inline int64_t timems(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
        return -1;

    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}


[[gnu::nonnull(6,7)]]
static int _log_io_error(const char *msg, ssize_t rc, ssize_t expected,
        bool die, struct client *client,
        const char *file, const char *func, int line)
{
    static const char *const read_error_fmt = "%s: read error at %s:%u: %s";
    static const char *const short_read_fmt = "%s: short read (%lu < %lu) at %s:%u: %s";

    if (rc == -1) {
        if (die)
            err(EXIT_FAILURE, read_error_fmt, func, file, line, msg ? msg : "");

        warn(read_error_fmt, func, file, line, msg ? msg : "");

        if (client)
            client->state = CS_CLOSED;

        return -1;
    }

    if (die)
        errx(EXIT_FAILURE, short_read_fmt, func, rc, expected, file, line, msg ? msg : "");

    warnx(short_read_fmt, func, rc, expected, file, line, msg ? msg : "");

    errno = ERANGE;
    return -1;
}
#define log_io_error(m,r,e,d,c) _log_io_error(m,r,e,d,c,__FILE__,__func__,__LINE__)

#if defined(FEATURE_DEBUG) || defined(FEATURE_RAFT_DEBUG)
const char *uuid_to_string(const uint8_t uuid[static UUID_SIZE])
{
    static char buf[37];

     snprintf(buf, sizeof(buf), "%02x%02x%02x%02x-"
             "%02x%02x-"
             "%02x%02x-"
             "%02x%02x-"
             "%02x%02x%02x%02x%02x%02x",
             uuid[0], uuid[1], uuid[2], uuid[3],
             uuid[4], uuid[5],
             uuid[6], uuid[7],
             uuid[8], uuid[9],
             uuid[10], uuid[11], uuid[12],
             uuid[13], uuid[14], uuid[15]);

     return buf;
}
#endif

#ifdef FEATURE_DEBUG
void dump_all_clients(void)
{
    pthread_rwlock_rdlock(&global_clients_lock);
    for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
        dump_client(clnt);
    pthread_rwlock_unlock(&global_clients_lock);
}

void dump_all_sessions(void)
{
    pthread_rwlock_rdlock(&global_sessions_lock);
    for (struct session *session = global_session_list; session; session = session->next)
        dump_session(session);
    pthread_rwlock_unlock(&global_sessions_lock);
}

void dump_all_messages(void)
{
    pthread_rwlock_rdlock(&global_messages_lock);
    for (struct message *msg = global_message_list; msg; msg = msg->next)
        dump_message(msg);
    pthread_rwlock_unlock(&global_messages_lock);
}

void dump_all_packets(void)
{
    pthread_rwlock_rdlock(&global_packets_lock);
    for (struct packet *pkt = global_packet_list; pkt; pkt = pkt->next)
        dump_packet(pkt);
    pthread_rwlock_unlock(&global_packets_lock);
}

void dump_all_topics(void)
{
    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *topic = global_topic_list; topic; topic = topic->next)
        dump_topic(topic);
    pthread_rwlock_unlock(&global_topics_lock);
}

void dump_all_mds(void)
{
    pthread_rwlock_rdlock(&global_mds_lock);
    for (struct message_delivery_state *mds = global_mds_list; mds; mds = mds->next)
        dump_mds(mds);
    pthread_rwlock_unlock(&global_mds_lock);
}

void dump_all_subscriptions(void)
{
    pthread_rwlock_rdlock(&global_subscriptions_lock);
    for (struct subscription *sub = global_subscription_list; sub; sub = sub->next)
        dump_subscription(sub);
    pthread_rwlock_unlock(&global_subscriptions_lock);
}

void dump_all(void)
{
    dump_all_clients();
    dump_all_sessions();
    dump_all_messages();
    dump_all_packets();
    dump_all_topics();
    dump_all_mds();
    dump_all_subscriptions();
}
#endif

/*
 * socket related helpers
 */

void sock_linger(int fd)
{
    struct linger linger = {
        .l_onoff = 0,
        .l_linger = 0,
    };

    if (setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger,
                sizeof(linger)) == -1)
        warn("setsockopt(SO_LINGER, mother)");
}

[[maybe_unused]] void sock_keepalive(int fd)
{
    int keepalive = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive,
                sizeof(keepalive)) == -1)
        warn("setsockopt(SO_KEEPALIVE)");
}

[[maybe_unused]] void sock_nodelay(int fd)
{
    int nodelay = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay,
                sizeof(nodelay)) == -1)
        warn("setsockopt(IPPROTO_TCP, TCP_NODELAY");
}


void sock_reuse(int fd, int reuse)
{
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                sizeof(reuse)) == -1)
        warn("setsockopt(SO_REUSEADDR, mother)");
}

void sock_nonblock(int fd)
{
    int flags;

    if ((flags = fcntl(fd, F_GETFL)) == -1) {
        warn("main: fcntl(mother_fd, get)");
    } else {
        flags |= O_NONBLOCK;
        if (fcntl(fd, F_SETFL, flags) == -1)
            warn("main: fcntl(mother_fd, set)");
    }
}

/*
 * UUID helpers
 */

static int get_first_hwaddr(uint8_t out[static MAC_ADDR_LEN], size_t out_length)
{
    struct ifaddrs *ifaddr;
    unsigned copy_len;
    const struct sockaddr_ll *sock;

    if (getifaddrs(&ifaddr) == -1) {
        logger(LOG_WARNING, NULL, "get_first_hwaddr: getifaddrs: %s",
                strerror(errno));
        return -1;
    }

    for (struct ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next)
    {
        if (ifa->ifa_addr == NULL)
            continue;

        if (ifa->ifa_addr->sa_family != AF_PACKET)
            continue;

        sock = (void *)ifa->ifa_addr;

        if (sock->sll_hatype != ARPHRD_ETHER)
            continue;

        copy_len = sock->sll_halen < out_length ? sock->sll_halen : out_length;
        assert(copy_len <= MAC_ADDR_LEN);
        memcpy(out, sock->sll_addr, copy_len);

        freeifaddrs(ifaddr);
        return copy_len;
    }

    freeifaddrs(ifaddr);
    errno = ENOENT;
    return -1;
}

static int generate_uuid(uint8_t hwaddr[static MAC_ADDR_LEN], uint8_t out[static UUID_SIZE])
{
    struct uuid_build uuid;
    struct timespec tp;
    uint64_t t = UUID_EPOCH_OFFSET;

    if (clock_gettime(CLOCK_REALTIME, &tp) == -1)
        return -1;

    t += (uint64_t)tp.tv_sec * 10000000ULL;
    t += (uint64_t)tp.tv_nsec / 100;

    t &= 0x0fffffffffffffffULL;

    uuid.time_low = (t & 0xffffffffULL);
    uuid.time_mid = ((t >> 32) & 0xffffULL);
    uuid.time_hi_and_version = ((t >> 48) & 0x0fffULL);
    uuid.time_hi_and_version |= (1U << 12);

    //srand(time(NULL) ^ getpid());
    uint16_t rnd = (uint16_t)(random() & 0x3fff);

    uuid.clk_seq_low = (uint8_t)(rnd & 0xff);
    uuid.clk_seq_hi_res = (uint8_t)((rnd >> 8) & 0x3f);
    uuid.clk_seq_hi_res |= 0x80; // (variant 10xxxxx)

    out[0] = (uuid.time_low >> 24) & 0xff;
    out[1] = (uuid.time_low >> 16) & 0xff;
    out[2] = (uuid.time_low >>  8) & 0xff;
    out[3] = (uuid.time_low      ) & 0xff;

    out[4] = (uuid.time_mid >>  8) & 0xff;
    out[5] = (uuid.time_mid      ) & 0xff;

    out[6] = (uuid.time_hi_and_version >> 8) & 0xff;
    out[7] = (uuid.time_hi_and_version     ) & 0xff;

    out[8] = (uuid.clk_seq_hi_res);
    out[9] = (uuid.clk_seq_low);

    memcpy(&out[10], hwaddr, MAC_ADDR_LEN);

    return 0;
}

#ifdef FEATURE_OM

/*
 * Open Metrics
 */

static const char *get_http_error(int error)
{
    switch (error)
    {
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 414: return "URI Too Long";
        case 431: return "Request Header Fields Too Large";
        case 501: return "Not Implemented";
        case 505: return "HTTP Version Not Supported";
    }

    return "unknown";
}

static const char *const om_datefmt = "%a, %d %b %Y %H:%M:%S GMT";

static void http_error(FILE *out, int error)
{
    logger(LOG_WARNING, NULL, "http_error: sending error %u", error);

    char date[BUFSIZ];
    struct tm *tm = NULL;
    time_t now = time(NULL);
    tm = gmtime(&now);

    strftime(date, sizeof(date), om_datefmt, tm);

    fprintf(out,
            "HTTP/1.1 %u %s\r\n"
            "connection: close\r\n"
            "content-length: 0\r\n"
            "date: %s\r\n"
            ,
            error,
            get_http_error(error),
            date
           );

    switch (error)
    {
        case 405:
            fprintf(out,
                    "allow: GET, HEAD\r\n"
                   );
            break;
    }

    fprintf(out, "\r\n");
    fflush(out);
}

[[gnu::warn_unused_result]]
static int openmetrics_export(int *fd)
{
    const size_t OM_MAX_LINE_SIZE = 8192;
    const size_t OM_MAX_HEADER_SIZE = 32768;

    FILE *mem = NULL, *in = NULL;
    bool head_request = false;
    char *buffer = NULL;
    char *http_method = NULL, *http_uri = NULL, *http_version = NULL;
    char *line = NULL;
    char date[BUFSIZ];
    char hdrbuf[BUFSIZ];
    char remote_addr[INET_ADDRSTRLEN];
    int rc;
    size_t line_len = 0;
    size_t size, len;
    socklen_t sin_len;
    ssize_t nread;
    struct sockaddr_in sin;
    struct tm *tm = NULL;
    time_t now;
    unsigned num_lines = 0;
    bool host_header = false;
    size_t header_size = 0;

    sin_len = sizeof(sin);

    struct timeval tv = {
        .tv_sec = 5,
        .tv_usec = 0,
    };

    if (setsockopt(*fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval)) == -1) {
        logger(LOG_ERR, NULL, "openmetrics_export: setsockopt(SO_RCVTIMEO): %s",
                strerror(errno));
        return -1;
    }

    if (setsockopt(*fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(struct timeval)) == -1) {
        logger(LOG_ERR, NULL, "openmetrics_export: setsockopt(SO_SNDTIMEO): %s",
                strerror(errno));
        return -1;
    }

    if (getpeername(*fd, (struct sockaddr *)&sin, &sin_len) == -1) {
        logger(LOG_WARNING, NULL, "openmetrics_export: getpeername: %s",
                strerror(errno));
        return -1;
    }

    inet_ntop(AF_INET, &sin.sin_addr, remote_addr, sizeof(remote_addr));
    logger(LOG_INFO, NULL, "openmetrics_export connection from %s:%u",
            remote_addr, ntohs(sin.sin_port));

    if ((in = fdopen(*fd, "r+b")) == NULL)
        goto fail;

    setvbuf(in, NULL, _IONBF, 0);

    while ((nread = getline(&line, &line_len, in)) != -1)
    {
        if (nread == 0)
            break;

        if ((size_t)nread > OM_MAX_LINE_SIZE || num_lines > 50) {
            http_error(in, 431);
            goto fail;
        }

        header_size += (size_t)nread;
        if (header_size > OM_MAX_HEADER_SIZE) {
            http_error(in, 431);
            goto fail;
        }

        if (nread == 2 && !memcmp(line, "\r\n", 2))
            break;

        if (num_lines == 0) {
            rc = sscanf(line, "%m[A-Z] %ms HTTP/%m[0-9.]", &http_method,
                    &http_uri, &http_version);

            if (rc != 3) {
                http_error(in, 400);
                goto fail;
            }

            if (strlen(http_uri) >= OM_MAX_LINE_SIZE) {
                http_error(in, 414);
                goto fail;
            }

            if (strcmp("1.0", http_version) && strcmp("1.1", http_version)) {
                http_error(in, 505);
                goto fail;
            }

            if (strcmp("/metrics", http_uri)) {
                http_error(in, 404);
                goto fail;
            }

            if (strcmp("GET", http_method) && strcmp("HEAD", http_method)) {
                http_error(in, 405);
                goto fail;
            }

            if (!strcmp("HEAD", http_method))
                head_request = true;

            free(http_method);
            free(http_version);
            free(http_uri);

            http_method = NULL;
            http_version = NULL;
            http_uri = NULL;
        } else {
            if (!strncasecmp(line, "host:", 5)) {
                const char *p = line + 5;
                while (*p == ' ' || *p == '\t')
                    p++;

                if (*p != '\0' && *p != '\r' && *p != '\n')
                    host_header = true;
            }
        }

        num_lines++;
    }

    if (line) {
        free(line);
        line = NULL;
    }

    if (nread == -1 || num_lines == 0) {
        if (nread == -1)
            warn("openmetrics_export: read");
        goto fail;
    }

    if (host_header == false) {
        http_error(in, 400);
        goto fail;
    }

    now = time(NULL);
    tm = gmtime(&now);
    strftime(date, sizeof(date), om_datefmt, tm);

    const char *const http_response =
        "HTTP/1.1 200 OK\r\n"
        "content-type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n"
        "connection: close\r\n"
        "cache-control: max-age=5, must-revalidate\r\n"
        "last-modified: %s\r\n"
        "content-language: en-GB\r\n"
        "allow: GET, HEAD\r\n"
        "retry-after: 5\r\n"
        "date: %s\r\n"
        "content-length: %zu\r\n"
        "\r\n";

    if ((mem = open_memstream(&buffer, &size)) == NULL) {
        logger(LOG_WARNING, NULL,
                "openmetrics_export: unable to open_memstream: %s",
                strerror(errno));
        goto fail;
    }

    fprintf(mem,
            "# TYPE num_sessions gauge\n"
            "num_sessions %u\n"
            "# TYPE num_clients gauge\n"
            "num_clients %u\n"
            "# TYPE num_messages gauge\n"
            "num_messages %u\n"
            "# TYPE num_topics gauge\n"
            "num_topics %u\n"
            "# TYPE num_mds gauge\n"
            "num_mds %u\n"
            "# TYPE num_packets gauge\n"
            "num_packets %u\n"
            "# TYPE num_subscriptions gauge\n"
            "num_subscriptions %u\n"
            "# TYPE num_shared_subscriptions gauge\n"
            "num_shared_subscriptions %u\n"
            "# TYPE num_non_shared_subscriptions gauge\n"
            "num_non_shared_subscriptions %u\n"
            "# TYPE total_messages_accepted_at counter\n"
            "total_messages_accepted_at %lu\n"
            "# TYPE total_messages_completed_at counter\n"
            "total_messages_completed_at %lu\n"
            "# TYPE total_messages_released_at counter\n"
            "total_messages_released_at %lu\n"
            "# TYPE total_messages_acknowledged_at counter\n"
            "total_messages_acknowledged_at %lu\n"
            "# TYPE total_messages_sender_accepted_at counter\n"
            "total_messages_sender_accepted_at %lu\n"
            "# TYPE total_messages_sender_completed_at counter\n"
            "total_messages_sender_completed_at %lu\n"
            "# TYPE total_messages_sender_released_at counter\n"
            "total_messages_sender_released_at %lu\n"
            "# TYPE total_messages_sender_acknowledged_at counter\n"
            "total_messages_sender_acknowledged_at %lu\n"
            "# TYPE total_control_packets_recv counter\n"
            "total_control_packets_recv %lu\n"
            "# TYPE total_control_packets_processed counter\n"
            "total_control_packets_processed %lu\n"
            "# TYPE total_control_packets_processed_ok counter\n"
            "total_control_packets_processed_ok %lu\n"
            "# TYPE total_control_packets_processed_not_ok counter\n"
            "total_control_packets_processed_not_ok %lu\n"
            ,
            num_sessions,
            num_clients,
            num_messages,
            num_topics,
            num_mds,
            num_packets,
            num_subscriptions,
            num_shared_subscriptions,
            num_subscriptions - num_shared_subscriptions,
            total_messages_accepted_at,
            total_messages_completed_at,
            total_messages_released_at,
            total_messages_acknowledged_at,
            total_messages_sender_accepted_at,
            total_messages_sender_completed_at,
            total_messages_sender_released_at,
            total_messages_sender_acknowledged_at,
            total_control_packets_recv,
            total_control_packets_processed,
            total_control_packets_processed_ok,
            total_control_packets_processed - total_control_packets_processed_ok
           );

    fprintf(mem, "# EOF\n");

    fclose(mem);
    mem = NULL;

    len = snprintf(hdrbuf, sizeof(hdrbuf), http_response, date, date, size);

    alarm(5);
    if (fwrite(hdrbuf, 1, len, in) != len) {
        goto fail;
    }

    if (!head_request) {
        if (fwrite(buffer, 1, size, in) != size) {
            goto fail;
        }
    } else
        size = 0;

    if (buffer)
        free(buffer);
    fclose(in);
    *fd = -1;
    alarm(0);

    logger(LOG_INFO, NULL, "openmetrics_export: sent %lu bytes of metrics", size);

    return 0;

fail:
    alarm(0);

    logger(LOG_WARNING, NULL, "openmetrics_export: connection closed in error");

    if (line)
        free(line);
    if (mem)
        fclose(mem);
    if (in)
        fclose(in);
    if (buffer)
        free(buffer);
    if (http_uri)
        free(http_uri);
    if (http_method)
        free(http_method);
    if (http_version)
        free(http_version);

    return -1;
}

#endif

[[gnu::nonnull(3)]]
void logger(int priority, const struct client *client,
        const char *format, ...)
{
    static const char *const fmt = "%s %s [%d]: <%s> %s\n";

    va_list varargs;
    char timebuf[128];
    char linebuf[BUFSIZ];
    char clientbuf[BUFSIZ];
    struct tm *tm;
    time_t now;
    pid_t pid;

    if (priority > opt_loglevel)
        return;

    now = time(NULL);
    tm = gmtime(&now);
    strftime(timebuf, sizeof(timebuf), "%FT%TZ", tm);

    if (client) {
        snprintf(clientbuf, sizeof(clientbuf), "%s [%s:%u]",
                client->client_id ? (const char *)client->client_id : "",
                client->hostname,
                client->remote_port);
    } else {
        strcpy(clientbuf, "");
    }

    va_start(varargs, format);
    vsnprintf(linebuf, sizeof(linebuf), format, varargs);
    va_end(varargs);

    pid = getpid();

    if (opt_logstdout)
        fprintf(stdout, fmt, timebuf, priority_str[priority], pid, clientbuf,
                linebuf);

    if (opt_logfile != NULL)
        fprintf(opt_logfile, fmt, timebuf, priority_str[priority], pid,
                clientbuf, linebuf);

    if (opt_logsyslog) {
        linebuf[strlen(linebuf)] = '\0';
        syslog(priority, "%s", linebuf);
    }

    if (priority == LOG_EMERG)
        exit(EXIT_FAILURE);
}

/*
 * Misc functions
 */

[[gnu::nonnull]]
static int find_str(const char *const *lookup, const char *value, int max)
{
    int ret;

    for (ret = 0; lookup[ret] && (max == 0 || ret < max); ret++)
        if (!strcmp(lookup[ret], value))
            return ret;

    return -1;
}

/*
 * allocators / deallocators
 */

[[gnu::nonnull, gnu::warn_unused_result]]
static int mds_detach_and_free(struct message_delivery_state *mds,
        bool session_lock, bool message_lock)
{
    errno = 0;
    int rc = 0;

    if (mds->read_only) {
        warn("mds_detach_and_free: attempt to free read_only MDS");
        errno = EPERM;
        return -1;
    }

    dbg_printf(BGRN "     mds_detach_and_free: called on mds %d (message %d, session %d)" CRESET "\n",
            mds->id,
            mds->message ? mds->message->id : -1U,
            mds->session ? mds->session->id : -1U
            );

    if (mds->message) {
        dbg_printf(BGRN "     mds_detach_and_free: processing mds->message" CRESET "\n");

        if (message_lock)
            pthread_rwlock_wrlock(&mds->message->delivery_states_lock);

        if (mds->message->num_message_delivery_states) {
            dbg_printf(BGRN "     mds_detach_and_free: %u delivery_states" CRESET "\n",
                    mds->message->num_message_delivery_states);
            if (remove_delivery_state(&mds->message->delivery_states,
                        &mds->message->num_message_delivery_states, mds) == -1) {
                warn("mds_detach_and_free: remove_delivery_state(message)");
                rc = -1;
            }
        }

        if (GET_REFCNT(&mds->message->refcnt) > 0) {
            DEC_REFCNT(&mds->message->refcnt); /* alloc_message_delivery_state */
            dbg_printf(BGRN "     mds_detach_and_free: DEC_REFCNT on message.id=%d refcnt=%u" CRESET "\n",
                    mds->message->id, mds->message->refcnt);
        }

        if (message_lock)
            pthread_rwlock_unlock(&mds->message->delivery_states_lock);
        mds->message = NULL;
    }

    if (mds->session) {
        dbg_printf(BGRN "     mds_detach_and_free: processing mds->session" CRESET "\n");

        if (session_lock)
            pthread_rwlock_wrlock(&mds->session->delivery_states_lock);

        if (mds->session->num_message_delivery_states) {
            dbg_printf(BGRN "     mds_detach_and_free: %u delivery_states" CRESET "\n",
                    mds->session->num_message_delivery_states);
            if (remove_delivery_state(&mds->session->delivery_states,
                        &mds->session->num_message_delivery_states, mds) == -1) {
                warn("mds_detach_and_free: remove_delivery_state(session)");
                rc = -1;
            }
        }

        if (GET_REFCNT(&mds->session->refcnt) > 0)
            DEC_REFCNT(&mds->session->refcnt); /* alloc_message_delivery_state */

        if (session_lock)
            pthread_rwlock_unlock(&mds->session->delivery_states_lock);

        mds->session = NULL;
    }

    if (rc == 0)
        if (free_message_delivery_state(mds) == -1)
            rc = -1;

    mds = NULL;
    dbg_printf("%s     mds_detach_and_free: returning %d%s%s" CRESET "\n",
            rc == -1 ? BRED : BGRN,
            rc,
            rc == -1 ? ": " : "",
            rc == -1 ? strerror(errno) : "")
    return rc;
}

[[gnu::nonnull]]
void close_socket(int *fd)
{
    assert(*fd != -1);
    //dbg_printf("     close_socket on fd %d\n", *fd);
    if (*fd != -1) {
        shutdown(*fd, SHUT_RDWR);
        close(*fd);
        *fd = -1;
    }
}

[[gnu::nonnull]]
static void free_subscription(struct subscription *sub)
{
    struct subscription *tmp;

    dbg_printf("     free_subscription:  id=%d type=%s filter=<%s>"CRESET"\n",
            sub->id,
            subscription_type_str[sub->type],
            (const char *)sub->topic_filter);

    pthread_rwlock_wrlock(&global_subscriptions_lock);
    if (global_subscription_list == sub) {
        global_subscription_list = sub->next;
    } else for (tmp = global_subscription_list; tmp; tmp = tmp->next) {
        if (tmp->next == sub) {
            tmp->next = sub->next;
            break;
        }
    }
    sub->next = NULL;
    pthread_rwlock_unlock(&global_subscriptions_lock);

    switch (sub->type)
    {
        case SUB_SHARED:
            if (sub->shared.sessions) {
                for (unsigned idx = 0; idx < sub->shared.num_sessions; idx++)
                    if (sub->shared.sessions[idx])
                        DEC_REFCNT(&sub->shared.sessions[idx]->refcnt);
                free(sub->shared.sessions);
            }
            if (sub->shared.qos_levels)
                free(sub->shared.qos_levels);
            if (sub->shared.share_name)
                free((void *)sub->shared.share_name);

            sub->shared.num_sessions = 0;
            sub->shared.sessions = NULL;
            sub->shared.share_name = NULL;
            sub->shared.qos_levels = NULL;
            break;

        case SUB_NON_SHARED:
            if (sub->non_shared.session) {
                DEC_REFCNT(&sub->non_shared.session->refcnt);
                sub->non_shared.session = NULL;
            }
            break;

        default:
            logger(LOG_WARNING, NULL,
                    "free_subscription: invalid subscription type");
            break;
    }

    if (sub->topic_filter) {
        free((void *)sub->topic_filter);
        sub->topic_filter = NULL;
    }

    if (sub->type == SUB_SHARED)
        num_shared_subscriptions--;

    num_subscriptions--;
    free(sub);
}

[[gnu::nonnull]]
static void free_topic_subs(struct topic_sub_request *request)
{
    if (request->topics) {
        for (unsigned cnt = 0; cnt < request->num_topics; cnt++)
        {
            if (request->topics[cnt]) {
                free((void *)request->topics[cnt]);
                request->topics[cnt] = NULL;
            }
        }
        free(request->topics);
        request->topics = NULL;
    }

    if (request->options) {
        free(request->options);
        request->options = NULL;
    }

    if (request->reason_codes) {
        free(request->reason_codes);
        request->reason_codes = NULL;
    }

    free(request);
}

[[gnu::nonnull]]
static void free_topic(struct topic *topic)
{
    dbg_printf("     free_topic: id=%d <%s> refcnt=%u [%p]\n",
            topic->id,
            (topic->name == NULL) ? "" : (char *)topic->name,
            GET_REFCNT(&topic->refcnt),
            topic
            );

    /* used in free_all_topics() to allow almost-dead topics to persist due
     * to dangling references in session->will_topic */
    topic->state = TOPIC_DEAD;

    pthread_rwlock_wrlock(&topic->pending_queue_lock);
    if (topic->pending_queue) {
        /* TODO persist */
        struct message *msg;
        while ((msg = topic->pending_queue)) {
            if (dequeue_message(msg) == -1) {
                topic->pending_queue = msg->next_queue;
                pthread_rwlock_unlock(&topic->pending_queue_lock);
                err(EXIT_FAILURE, "free_topic: dequeue_message"); /* TODO y/n ? */
            }
            msg->state = MSG_DEAD;
        }
        topic->pending_queue = NULL;
    }
    pthread_rwlock_unlock(&topic->pending_queue_lock);

    /* handle any retained message */
    if (topic->retained_message) {
        struct message *msg = topic->retained_message;
        dbg_printf("     free_topic: freeing retained_message id=%d\n", msg->id);

        msg->state = MSG_DEAD;

        pthread_rwlock_wrlock(&msg->delivery_states_lock);
        for (unsigned idx = 0; idx < msg->num_message_delivery_states; idx++)
        {
            if (msg->delivery_states[idx] == NULL)
                continue;

            if (mds_detach_and_free(msg->delivery_states[idx], true, false) == -1)
                warn("free_topic: mds_detach_and_free");
            msg->delivery_states[idx] = NULL;
        }
        pthread_rwlock_unlock(&msg->delivery_states_lock);

        DEC_REFCNT(&msg->refcnt);
        DEC_REFCNT(&msg->topic->refcnt);
        msg->topic = NULL;

        if (GET_REFCNT(&msg->refcnt) == 0) {
            free_message(msg, true);
            msg = NULL;
        } else
            warn("free_topic: can't free retained_message, refcnt is %u\n",
                    GET_REFCNT(&msg->refcnt));
        topic->retained_message = NULL;
    }

    /* Do this hear, as free_message() etc. above may have changed refcnt */
    pthread_rwlock_wrlock(&global_topics_lock);
    if (GET_REFCNT(&topic->refcnt) == 0) {
        if (global_topic_list == topic) {
            global_topic_list = topic->next;
        } else for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
        {
            if (tmp->next == topic) {
                tmp->next = topic->next;
                break;
            }
        }
        topic->next = NULL;
    }
    pthread_rwlock_unlock(&global_topics_lock);

    if (GET_REFCNT(&topic->refcnt) > 0)
        return;

    if (topic->name) {
        free((void *)topic->name);
        topic->name = NULL;
    }

    pthread_rwlock_destroy(&topic->pending_queue_lock);

    num_topics--;
    free(topic);
}

[[gnu::nonnull]]
static void free_properties(struct property (*props)[], unsigned count)
{
    type_t type;

    for (unsigned i = 0; i < count; i++)
    {
        if ((*props)[i].ident >= MQTT_PROPERTY_IDENT_MAX) /* TODO handle error */
            continue;

        type = property_to_type[(*props)[i].ident];

        switch (type)
        {
            case MQTT_TYPE_UTF8_STRING_PAIR:
                if ((*props)[i].utf8_pair[0]) {
                    free((*props)[i].utf8_pair[0]);
                    (*props)[i].utf8_pair[0] = NULL;
                }
                if ((*props)[i].utf8_pair[1]) {
                    free((*props)[i].utf8_pair[1]);
                    (*props)[i].utf8_pair[1] = NULL;
                }
                break;

            case MQTT_TYPE_UTF8_STRING:
                if ((*props)[i].utf8_string) {
                    free((*props)[i].utf8_string);
                    (*props)[i].utf8_string = NULL;
                }
                break;

            case MQTT_TYPE_BINARY:
                if ((*props)[i].binary.data) {
                    free((*props)[i].binary.data);
                    (*props)[i].binary.data = NULL;
                }
                break;

            default:
                break;
        }
    }
    free(props);
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int free_message_delivery_state(struct message_delivery_state *mds)
{
    assert(mds != NULL);

    if (mds->state == MDS_DELETED) {
        warnx("free_message_delivery_state: is deleted: mds=%p id=%u", mds, mds->id);
        errno = EFAULT;
        abort();
        return -1;
    }

    if ((mds)->read_only)
        errx(EXIT_FAILURE, "attempt to free read-only mds");

    dbg_printf("     free_message_delivery_state: id=%d\n",
            (mds)->id);

    pthread_rwlock_wrlock(&global_mds_lock);
    if (global_mds_list == mds) {
        global_mds_list = (mds)->next;
    } else for (struct message_delivery_state *tmp = global_mds_list;
            tmp; tmp = tmp->next)
    {
        if (tmp->next == mds) {
            tmp->next = (mds)->next;
            break;
        }
    }
    (mds)->next = NULL;
    pthread_rwlock_unlock(&global_mds_lock);

    if((mds)->session)
        abort();
    if((mds)->message)
        abort();

    mds->state = MDS_DELETED;

    free(mds);
    mds = NULL;
    num_mds--;

    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int free_delivery_states(pthread_rwlock_t *lock, unsigned *num,
        struct message_delivery_state ***msgs)
{
    int rc = 0;

    if (*msgs == NULL) {
        errno = ENOENT;
        return -1;
    }

    pthread_rwlock_wrlock(lock);
    while (*msgs && *num && **msgs)
        if (mds_detach_and_free(**msgs, false, false) == -1) {
            warn("free_delivery_states: mds_detach_and_free");
            rc = -1;
            break;
        }

    if (*msgs)
        free(*msgs);
    *msgs = NULL;
    *num = 0;
    pthread_rwlock_unlock(lock);

    return rc;
}

[[gnu::nonnull]]
static void free_packet(struct packet *pck, bool need_lock, bool need_owner_lock)
{
    struct packet *tmp;
    unsigned lck;

    if (pck->deleted)
        errx(EXIT_FAILURE, "free_packet: is deleted");

    dbg_printf("     free_packet: id=%d owner=%u <%s> owner.session=%d <%s> refcnt=%u message=%d\n",
            pck->id,
            pck->owner ? pck->owner->id : 0,
            pck->owner ? (char *)pck->owner->client_id : "",
            (pck->owner && pck->owner->session) ? pck->owner->session->id : 0,
            (pck->owner && pck->owner->session) ? (char *)pck->owner->session->client_id : "",
            GET_REFCNT(&pck->refcnt),
            pck->message ? pck->message->id : (id_t)-1
            );

    if ((lck = GET_REFCNT(&pck->refcnt)) > 0) {
        warnx("free_packet: attempt to free packet with refcnt=%u", lck);
        abort();
        return;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_packets_lock);

    if (pck->owner) {
        if (need_owner_lock)
            pthread_rwlock_wrlock(&pck->owner->active_packets_lock);
        if (pck->owner->active_packets == pck) {
            pck->owner->active_packets = pck->next_client;
        } else for (tmp = pck->owner->active_packets; tmp; tmp = tmp->next_client)
        {
            if (tmp->next_client == pck) {
                tmp->next_client = pck->next_client;
                break;
            }
        }
        pck->next_client = NULL;
        if (need_owner_lock)
            pthread_rwlock_unlock(&pck->owner->active_packets_lock);
        DEC_REFCNT(&pck->owner->refcnt);
        pck->owner = NULL;
    }

    if (pck == global_packet_list) {
        global_packet_list = pck->next;
    } else for (tmp = global_packet_list; tmp; tmp = tmp->next)
    {
        if (tmp->next == pck) {
            tmp->next = pck->next;
            break;
        }
    }
    pck->next = NULL;

    if (need_lock)
        pthread_rwlock_unlock(&global_packets_lock);

    if (pck->payload) {
        free(pck->payload);
        pck->payload = NULL;
    }

    if (pck->properties) {
        free_properties(pck->properties, pck->property_count);
        pck->property_count = 0;
        pck->properties = NULL;
    }

    if (pck->message) {
        DEC_REFCNT(&pck->message->refcnt);
        dbg_printf("     free_packet: DEC_REFCNT on message.id=%d refcnt=%u\n",
                pck->message->id, pck->message->refcnt);
        pck->message = NULL;
    }

    num_packets--;
    pck->deleted = true;
    free(pck);
}

[[gnu::nonnull]]
static void free_message(struct message *msg, bool need_lock)
{
    struct message *tmp;
    unsigned lck;

    if (msg->deleted)
        errx(EXIT_FAILURE, "free_message: is deleted");

    assert(msg != NULL);

    dbg_printf("     free_message: id=%d [%s] lock=%s type=%s refcnt=%u",
            msg->id, message_state_str[msg->state],
            need_lock ? "yes" : "no",
            message_type_str[msg->type],
            GET_REFCNT(&msg->refcnt)
            );

    dbg_cprintf(" topic=%d <%s>\n",
            msg->topic ? msg->topic->id : (id_t)-1,
            msg->topic ? (char *)msg->topic->name : ""
            );

    if ((lck = GET_REFCNT(&msg->refcnt)) > 0) {
        return;
    }

    if (msg->topic) {
        warnx("free_message: attempt to free message with topic <%s> set",
                msg->topic->name);
        abort();
        return;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_messages_lock);
    {
        if (global_message_list == msg) {
            global_message_list = msg->next;
        } else for (tmp = global_message_list; tmp; tmp = tmp->next) {
            if (tmp->next == msg) {
                tmp->next = msg->next;
                break;
            }
        }
    }
    if (need_lock)
        pthread_rwlock_unlock(&global_messages_lock);

    msg->next = NULL;

    if (msg->payload) {
        free((void *)msg->payload);
        msg->payload = NULL;
    }

    if (running)
        if (msg->delivery_states) {
            if (free_delivery_states(&msg->delivery_states_lock,
                    &msg->num_message_delivery_states, &msg->delivery_states) == -1)
                warn("free_message: free_delivery_states");
            msg->delivery_states = NULL;
        }

    if (msg->delivery_states) {
        free(msg->delivery_states);
        msg->delivery_states = NULL;
    }

    if (pthread_rwlock_destroy(&msg->delivery_states_lock) == -1)
        warn("free_message: pthread_rwlock_destroy");

    if (msg->type == MSG_WILL && msg->sender) {
        msg->sender->will_topic = NULL;
    }

    /* INC in register_message(), doesn't happen to RETAIN */
    if (msg->sender && GET_REFCNT(&msg->sender->refcnt))
        DEC_REFCNT(&msg->sender->refcnt);

    num_messages--;
    msg->deleted = true;
    free(msg);
}

[[gnu::nonnull]]
static void free_client(struct client *client, bool needs_lock)
{
    struct client *tmp;

    dbg_printf("     free_client: id=%d [%s] lock=%s client_id=%s session=%d %s refcnt=%u\n",
            client->id, client_state_str[client->state],
            needs_lock ? "yes" : "no",
            (char *)client->client_id,
            client->session ? client->session->id : 0,
            client->session ? (char *)client->session->client_id : "",
            GET_REFCNT(&client->refcnt));

    if (needs_lock)
        pthread_rwlock_wrlock(&global_clients_lock);

    if (client->state == CS_ACTIVE)
        client->state = CS_CLOSING;

    pthread_rwlock_wrlock(&client->active_packets_lock);
    for (struct packet *p = client->active_packets, *next; p; p = next)
    {
        next = p->next_client;
        DEC_REFCNT(&p->refcnt);
        free_packet(p, true, false);
    }
    client->active_packets = NULL;
    pthread_rwlock_unlock(&client->active_packets_lock);

    if (GET_REFCNT(&client->refcnt) > 0) {
        warnx("free_client: attempt to free client with refcnt %d", client->refcnt);
        warnx("free_client:   active_packets=%s", client->active_packets ? "yes" : "no");
        if (needs_lock)
            pthread_rwlock_unlock(&global_clients_lock);
        abort();
        return;
    }

    client->state = CS_CLOSED;

    if (global_client_list == client) {
        global_client_list = client->next;
    } else for (tmp = global_client_list; tmp; tmp = tmp->next) {
        if (tmp->next == client) {
            tmp->next = client->next;
            break;
        }
    }
    if (needs_lock)
        pthread_rwlock_unlock(&global_clients_lock);

    client->next = NULL;

    if (client->fd != -1)
        close_socket(&client->fd);

    pthread_rwlock_wrlock(&client->active_packets_lock);
    for (struct packet *p = client->active_packets, *next; p; p = next)
    {
        next = p->next_client;
        DEC_REFCNT(&p->refcnt);
        free_packet(p, true, false);
    }
    client->active_packets = NULL;
    pthread_rwlock_unlock(&client->active_packets_lock);

    pthread_rwlock_wrlock(&global_packets_lock);
    for (struct packet *p = global_packet_list; p; p = p->next)
    {
        if (p->owner == client)
            p->owner = NULL; /* TODO locking? */
    }
    pthread_rwlock_unlock(&global_packets_lock);

    if (client->clnt_topic_aliases) {
        for (unsigned idx = 0; idx < client->topic_alias_maximum; idx++)
            if (client->clnt_topic_aliases[idx]) {
                free((void *)client->clnt_topic_aliases[idx]);
                client->clnt_topic_aliases[idx] = NULL;
            }
        free((void *)client->clnt_topic_aliases);
        client->clnt_topic_aliases = NULL;
    }


    if (client->session) {
        client->session->client = NULL;
        DEC_REFCNT(&client->session->refcnt); /* handle_cp_connect */
        client->session = NULL;
    }

    if (client->client_id) {
        free ((void *)client->client_id);
        client->client_id = NULL;
    }

    if (client->username) {
        free ((void *)client->username);
        client->username = NULL;
    }

    if (client->password) {
        free ((void *)client->password);
        client->password = NULL;
    }

    if (client->packet_buf) {
        free(client->packet_buf);
        client->packet_buf = NULL;
    }

    if (client->po_buf) {
        free((void *)client->po_buf);
        client->po_buf = NULL;
    }

    if (client->svr_topic_aliases) {
        free((void *)client->svr_topic_aliases);
        client->svr_topic_aliases = NULL;
    }

    pthread_rwlock_destroy(&client->active_packets_lock);
    pthread_rwlock_destroy(&client->po_lock);

    num_clients--;
    free(client);
}

[[gnu::nonnull]]
static void free_session(struct session *session, bool need_lock)
{
    struct session *tmp;

    dbg_printf("     free_session: session=%d <%s> [%s] client=%u <%s> exp_at=%lu refcnt=%u\n",
            session->id,
            session->client_id, session_state_str[session->state],
            session->client ? session->client->id : 0,
            session->client ? (char *)session->client->client_id : "",
            session->expires_at,
            GET_REFCNT(&session->refcnt));

    pthread_rwlock_wrlock(&session->subscriptions_lock);
    if (session->subscriptions) {
        unsubscribe_session_from_all(session);
        free(session->subscriptions);
        session->subscriptions = NULL;
        session->num_subscriptions = 0;
    }
    pthread_rwlock_unlock(&session->subscriptions_lock);

    /* TODO do this properly */
    if (running) {
        if (session->delivery_states && session->num_message_delivery_states) {
            dbg_printf("     free_session: num_mds=%u\n", session->num_message_delivery_states);
            if (free_delivery_states(&session->delivery_states_lock,
                    &session->num_message_delivery_states, &session->delivery_states) == -1)
                warn("free_session: free_delivery_states");
            session->delivery_states = NULL;
        }
    }

    if (GET_REFCNT(&session->refcnt) > 0) {
        dbg_printf("     free_session: refcnt != 0\n");
        return;
    }

    /* Will handling must be after retcnt is 0 */
    if (session->will_payload) {
        free(session->will_payload);
        session->will_payload = NULL;
    }

    session->will_payload_len = 0;

    if (session->will_topic) {
        DEC_REFCNT(&session->will_topic->refcnt);
        session->will_topic = NULL;
    }

    if (session->will_props) {
        free_properties(session->will_props, session->num_will_props);
        session->will_props = NULL;
        session->num_will_props = 0;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_sessions_lock);
    if (global_session_list == session) {
        global_session_list = session->next;
    } else for (tmp = global_session_list; tmp; tmp = tmp->next) {
        if (tmp->next == session) {
            tmp->next = session->next;
            break;
        }
    }
    session->next = NULL;

    if (session->client) {
        warn("free_session: freeing session with connected client!");
        session->client->state = CS_CLOSED;
        close_socket(&session->client->fd);
        session->client->session = NULL;
        //DEC_REFCNT(&session->client->refcnt); // TODO add INC_REFCNTs everywhere for client->session
        session->client = NULL;
    }

    if (need_lock)
        pthread_rwlock_unlock(&global_sessions_lock);

    if (session->client_id) {
        free((void *)session->client_id);
        session->client_id = NULL;
    }

    if (session->delivery_states) {
        free(session->delivery_states);
        session->delivery_states = NULL;
    }

    pthread_rwlock_destroy(&session->subscriptions_lock);
    pthread_rwlock_destroy(&session->delivery_states_lock);

    num_sessions--;
    free(session);
}

[[gnu::nonnull]]
static void attach_mds(struct message_delivery_state *mds,
        struct message *message, struct session *session)
{
    INC_REFCNT(&session->refcnt);
    mds->session = session;
    INC_REFCNT(&message->refcnt);
    mds->message = message;

    pthread_rwlock_wrlock(&global_mds_lock);
    mds->state = MDS_ACTIVE;
    mds->next = global_mds_list;
    global_mds_list = mds;
    num_mds++;
    pthread_rwlock_unlock(&global_mds_lock);
}

[[gnu::malloc, gnu::warn_unused_result]]
static struct message_delivery_state *alloc_message_delivery_state(
        struct message *message, struct session *session, bool detached)
{
    struct message_delivery_state *ret;

    if ((ret = calloc(1, sizeof(struct message_delivery_state))) == NULL)
        goto fail;

    ret->state = MDS_NEW;

    if (detached == false && (message == NULL || session == NULL)) {
        errno = EINVAL;
        return NULL;
    }

    ret->id = mds_id++;

    if (detached) {
        dbg_printf("     alloc_message_delivery_state: detached\n");
    } else {
        dbg_printf("     alloc_message_delivery_state: session=%d[%u] message=%u[%u]\n",
                session->id, session->refcnt,
                message->id, message->refcnt);
    }

    if (!detached)
        attach_mds(ret, message, session);

    return ret;

fail:
    if (ret)
        free(ret);
    return NULL;
}

static int remove_session_from_shared_sub(struct subscription *sub,
        struct session *session)
{
    struct session **tmp = NULL;
    uint8_t *tmp_qos = NULL;
    bool found = false;
    size_t new_size;

    assert(session != NULL);
    assert(sub->type == SUB_SHARED);
    assert(sub->shared.sessions != NULL);
    assert(sub->shared.qos_levels != NULL);

    dbg_printf("     remove_session_from_shared_sub: subscription.id=%d session.id=%d\n",
            sub->id, session->id);

    new_size = sizeof(struct subscription *) * (sub->shared.num_sessions - 1);
    if ((tmp = malloc(new_size)) == NULL)
        goto fail;

    new_size = sizeof(uint8_t) * (sub->shared.num_sessions - 1);
    if ((tmp_qos = malloc(new_size)) == NULL)
        goto fail;

    for (unsigned old_idx = 0, new_idx = 0; old_idx < sub->shared.num_sessions; old_idx++)
    {
        if (sub->shared.sessions[old_idx] == session) {
            found = true;
            continue;
        }

        if (new_idx == sub->shared.num_sessions - 1)
            break;

        tmp[new_idx] = sub->shared.sessions[old_idx];
        tmp_qos[new_idx] = sub->shared.qos_levels[old_idx];
        new_idx++;
    }

    if (found == true) {
        sub->shared.num_sessions--;
        free(sub->shared.sessions);
        free(sub->shared.qos_levels);
        sub->shared.sessions = tmp;
        sub->shared.qos_levels = tmp_qos;
        return 0;
    }

    errno = ENOENT;
fail:
    if (tmp)
        free(tmp);
    if (tmp_qos)
        free(tmp_qos);

    return -1;
}

    [[gnu::nonnull]]
static int add_session_to_shared_sub(struct subscription *sub,
        struct session *session, uint8_t qos)
{
    void *tmp = NULL;

    errno = 0;

    /* first, check if the sub is already present, if so, updated qos and return */
    for (unsigned idx = 0; idx < sub->shared.num_sessions; idx++)
        if (sub->shared.sessions[idx] == session) {
            sub->shared.qos_levels[idx] = qos;
            errno = EEXIST;
            return -1;
        }

    dbg_printf("[%2d] add_session_to_shared_sub: subscription.id=%d session.id=%d\n",
            session->id, sub->id, session->id);

    assert(sub->type == SUB_SHARED);
    size_t new_size = sizeof(struct subscription *) * (sub->shared.num_sessions + 1);

    if ((tmp = realloc(sub->shared.sessions, new_size)) == NULL)
        goto fail;
    sub->shared.sessions = tmp;

    /* [MQTT-4.8.2-3] */
    new_size = sizeof(uint8_t) * (sub->shared.num_sessions + 1);
    if ((tmp = realloc(sub->shared.qos_levels, new_size)) == NULL)
        goto fail;
    sub->shared.qos_levels = tmp;

    INC_REFCNT(&session->refcnt); /* remove_session_from_shared_sub() || free_subscription() */
    sub->shared.sessions[sub->shared.num_sessions] = session;
    sub->shared.qos_levels[sub->shared.num_sessions] = qos;
    sub->shared.num_sessions++;

    return 0;

fail:
    return -1;
}

[[gnu::malloc, gnu::warn_unused_result, gnu::nonnull]]
static struct subscription *alloc_subscription(struct session *session,
        subscription_type_t type, const uint8_t *topic_filter,
        uint32_t subscription_identifier)
{
    struct subscription *ret = NULL;

    if ((ret = calloc(1, sizeof(struct subscription))) == NULL)
        return NULL;

    ret->id = subscription_id++;
    ret->type = type;
    ret->subscription_identifier = subscription_identifier;

    if ((ret->topic_filter = (void *)strdup((const void *)topic_filter)) == NULL)
        goto fail;

    switch (type)
    {
        case SUB_NON_SHARED:
            INC_REFCNT(&session->refcnt); /* free_subscription */
            ret->non_shared.session = session;
            break;

        case SUB_SHARED:
            /* INC_REFCNT is in add_session_to_shared_sub */
            break;

        default:
            errno = EINVAL;
            warn("alloc_subscription: unknown type");
            goto fail;
    }

    dbg_printf("     alloc_subscription: %s id=%d session=%d <%s> filter=%s"CRESET"\n",
            subscription_type_str[type],
            ret->id, session->id, (char *)session->client_id, topic_filter);

    pthread_rwlock_wrlock(&global_subscriptions_lock);
    ret->next = global_subscription_list;
    global_subscription_list = ret;
    num_subscriptions++;
    if (type == SUB_SHARED)
        num_shared_subscriptions++;
    pthread_rwlock_unlock(&global_subscriptions_lock);

    return ret;

fail:
    if (ret->topic_filter) {
        free((void *)ret->topic_filter);
        ret->topic_filter = NULL;
    }

    if (ret)
        free(ret);

    return NULL;
}

[[gnu::malloc, gnu::warn_unused_result]]
static struct session *alloc_session(struct client *client, const uint8_t uuid[UUID_SIZE])
{
    struct session *ret;

    if (num_sessions >= MAX_SESSIONS) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((ret = calloc(1, sizeof(struct session))) == NULL)
        return NULL;

    if (uuid == NULL && generate_uuid(global_hwaddr, ret->uuid) == -1)
        goto fail;
    else if (uuid != NULL)
        memcpy(ret->uuid, uuid, UUID_SIZE);

    if (pthread_rwlock_init(&ret->subscriptions_lock, NULL) != 0)
        goto fail;
    if (pthread_rwlock_init(&ret->delivery_states_lock, NULL) != 0)
        goto fail;

    if (client) {
        ret->client = client;
        client->session = ret;
        ret->client_id = (void *)strdup((const char *)client->client_id);
    }

    if (pthread_rwlock_wrlock(&global_sessions_lock) == -1) {
        warn("alloc_session: pthread_rwlock_wrlock");
        goto fail;
    }
    ret->next = global_session_list;
    global_session_list = ret;
    num_sessions++;
    pthread_rwlock_unlock(&global_sessions_lock);

    ret->id = session_id++;

    dbg_printf("     alloc_session: id=%d client=%d <%s>\n",
            ret->id, client ? client->id : 0, client ? (char *)client->client_id : "");
    return ret;

fail:
    if (ret->client_id)
        free((void *)ret->client_id);
    if (ret)
        free(ret);

    return NULL;
}

[[gnu::nonnull(1), gnu::malloc, gnu::warn_unused_result]]
static struct topic *alloc_topic(const uint8_t *name, const uint8_t uuid[UUID_SIZE])
{
    struct topic *ret = NULL;

    if (num_topics >= MAX_TOPICS) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((ret = calloc(1, sizeof(struct topic))) == NULL)
        return NULL;

    if (uuid == NULL && generate_uuid(global_hwaddr, ret->uuid) == -1)
        goto fail;
    else if (uuid != NULL)
        memcpy(ret->uuid, uuid, UUID_SIZE);

    if ((ret->name = (void *)strdup((char *)name)) == NULL)
        goto fail;

    pthread_rwlock_init(&ret->pending_queue_lock, NULL);

    ret->id = topic_id++;
    num_topics++;

    dbg_printf("     alloc_topic: id=%d <%s>\n", ret->id, (char *)name);

    return ret;

fail:
    if (ret)
        free(ret);

    return NULL;
}

[[gnu::malloc,gnu::warn_unused_result]]
static struct packet *alloc_packet(struct client *owner, packet_type_t direction)
{
    struct packet *ret;

    errno = 0;

    if (num_packets >= MAX_PACKETS) {
        errno = ENOSPC;
        return NULL;
    }

    if ((ret = calloc(1, sizeof(struct packet))) == NULL)
        return NULL;

    if (owner) {
        INC_REFCNT(&owner->refcnt);
        INC_REFCNT(&ret->refcnt);

        ret->owner = owner;

        pthread_rwlock_wrlock(&owner->active_packets_lock);
        ret->next_client = owner->active_packets;
        owner->active_packets = ret;
        pthread_rwlock_unlock(&owner->active_packets_lock);
    }

    pthread_rwlock_wrlock(&global_packets_lock);
    ret->next = global_packet_list;
    global_packet_list = ret;
    num_packets++;
    pthread_rwlock_unlock(&global_packets_lock);

    ret->direction = direction;
    ret->id = packet_id++;

    dbg_printf("     alloc_packet: id=%d owner=%d <%s> type=%s\n",
            ret->id, owner ? owner->id : (id_t)-1,
            (owner && owner->client_id) ? ((const char *)owner->client_id) : "",
            packet_dir_str[direction]
          );

    return ret;
}

[[gnu::malloc, gnu::warn_unused_result]]
static struct message *alloc_message(const uint8_t uuid[UUID_SIZE])
{
    struct message *ret = NULL;

    if (num_messages >= MAX_MESSAGES) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((ret = calloc(1, sizeof(struct message))) == NULL)
        return NULL;

    ret->state = MSG_NEW;
    ret->sender_status.read_only = true;
    ret->sender_status.state = MDS_ACTIVE;

    if (uuid == NULL && generate_uuid(global_hwaddr, ret->uuid) == -1)
        goto fail;
    else if (uuid != NULL)
        memcpy(ret->uuid, uuid, UUID_SIZE);

    if (pthread_rwlock_init(&ret->delivery_states_lock, NULL) != 0)
        goto fail;

    pthread_rwlock_wrlock(&global_messages_lock);
    ret->next = global_message_list;
    global_message_list = ret;
    num_messages++;
    pthread_rwlock_unlock(&global_messages_lock);

    ret->id = message_id++;

    return ret;

fail:
    if (ret)
        free(ret);

    return NULL;
}

[[gnu::malloc, gnu::warn_unused_result]]
static struct client *alloc_client(void)
{
    struct client *client;

    if (num_clients >= MAX_CLIENTS) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((client = calloc(1, sizeof(struct client))) == NULL)
        return NULL;

    if ((client->svr_topic_aliases = calloc(1,
                    sizeof(uint8_t *) * MAX_TOPIC_ALIAS)) == NULL)
        goto fail;

    client->state = CS_NEW;
    client->fd = -1;
    client->parse_state = READ_STATE_NEW;
    client->is_auth = false;
    client->send_quota = MAX_RECEIVE_PUBS; /* [MQTT-4.9.0-1] */
    client->last_keep_alive = time(NULL);

    if (pthread_rwlock_init(&client->active_packets_lock, NULL) == -1)
        goto fail;

    if (pthread_rwlock_init(&client->po_lock, NULL) == -1)
        goto fail;

    pthread_rwlock_wrlock(&global_clients_lock);
    client->next = global_client_list;
    global_client_list = client;
    num_clients++;
    pthread_rwlock_unlock(&global_clients_lock);

    client->id = client_id++;

    dbg_printf("     alloc_client: id=%d\n", client->id);

    return client;

fail:
    if (client->svr_topic_aliases)
        free(client->svr_topic_aliases);
    if (client)
        free(client);

    return NULL;
}

/*
 * persistence functions
 */

static inline void write_u32(char **out, uint32_t val)
{
    val = htonl(val);
    memcpy(*out, &val, sizeof(val));
    *out += sizeof(val);
}

static inline void write_u16(char **out, uint16_t val)
{
    val = htons(val);
    memcpy(*out, &val, sizeof(val));
    *out += sizeof(val);
}

static inline void write_u8(char **out, uint8_t val)
{
    memcpy(*out, &val, sizeof(val));
    *out += sizeof(val);
}

static inline void write_bytes(char **out, const void *src, size_t len)
{
    memcpy(*out, src, len);
    *out += len;
}

static inline void write_uuid(char **out, const uint8_t *src)
{
    memcpy(*out, src, UUID_SIZE);
    *out += UUID_SIZE;
}

static inline int read_u32(uint32_t *out, const char **in, size_t *bytes_remaining)
{
    if (*bytes_remaining < sizeof(uint32_t)) {
        errno = ENOSPC;
        return -1;
    }

    memcpy(out, *in, sizeof(uint32_t));
    *out = ntohl(*out);

    *in += sizeof(uint32_t);
    *bytes_remaining -= sizeof(uint32_t);

    return 0;
}

static inline int read_u16(uint16_t *out, const char **in, size_t *bytes_remaining)
{
    if (*bytes_remaining < sizeof(uint16_t)) {
        errno = ENOSPC;
        return -1;
    }

    memcpy(out, *in, sizeof(uint16_t));
    *out = ntohs(*out);

    *in += sizeof(uint16_t);
    *bytes_remaining -= sizeof(uint16_t);

    return 0;
}

static inline int read_u8(uint8_t *out, const char **in, size_t *bytes_remaining)
{
    if (*bytes_remaining < sizeof(uint8_t)) {
        errno = ENOSPC;
        return -1;
    }

    *out = **in;

    *in += sizeof(uint8_t);
    *bytes_remaining -= sizeof(uint8_t);

    return 0;
}

static inline int read_uuid(uint8_t out[static UUID_SIZE], const char **in, size_t *bytes_remaining)
{
    if (*bytes_remaining < UUID_SIZE) {
        errno = ENOSPC;
        return -1;
    }

    memcpy(out, *in, UUID_SIZE);

    *in += UUID_SIZE;
    *bytes_remaining -= UUID_SIZE;

    return 0;
}

static inline int read_bytes(uint8_t *out, const char **in, size_t *bytes_remaining, size_t len)
{
    if (*bytes_remaining < len) {
        errno = ENOSPC;
        return -1;
    }

    memcpy(out, *in, len);

    *in += len;
    *bytes_remaining -= len;

    return 0;
}

[[maybe_unused]] static int size_topic(const struct topic *topic)
{
    int ret = 0;

    if (topic->state != TOPIC_ACTIVE) {
        errno = EINVAL;
        return -1;
    }

    ret += sizeof(topic->uuid);
    ret += sizeof(topic->retained_message->uuid);
    ret += sizeof(uint16_t); /* name_length */
    const size_t name_len = strlen((void *)topic->name);
    if (name_len > UINT16_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    ret += name_len;

    return ret;
}

[[maybe_unused]] static int size_message(const struct message *msg)
{
    int ret = 0;

    if (msg->deleted || msg->state != MSG_ACTIVE) {
        errno = EINVAL;
        return -1;
    }

    ret += sizeof(msg->uuid);

    ret += sizeof((uint32_t)msg->payload_len);
    ret += sizeof((uint8_t)msg->format);
    ret += sizeof((uint8_t)msg->qos);
    ret += sizeof((uint8_t)msg->retain);
    ret += sizeof((uint8_t)msg->type);

    /* the abscence of a sender is implied by NULL_UUID */
    ret += sizeof(msg->sender->uuid);
    /* the abscence of a topic is implied by NULL_UUID */
    ret += sizeof(msg->topic->uuid);

    if (msg->payload_len > UINT32_MAX) {
        errno = EOVERFLOW;
        return -1;
    }

    if (msg->qos > 0) {
        static constexpr int mds_size =
          sizeof((uint16_t)msg->sender_status.packet_identifier)
        + sizeof(msg->sender_status.session->uuid)
        + sizeof((uint32_t)msg->sender_status.last_sent)
        + sizeof((uint32_t)msg->sender_status.acknowledged_at)
        + sizeof((uint32_t)msg->sender_status.released_at)
        + sizeof((uint32_t)msg->sender_status.completed_at)
        + sizeof((uint8_t)msg->sender_status.client_reason);

        if (msg->num_message_delivery_states > UINT16_MAX) {
            errno = EOVERFLOW;
            return -1;
        }
        ret += mds_size; /* sender_status */
        ret += sizeof((uint16_t)msg->num_message_delivery_states);
        ret += mds_size * msg->num_message_delivery_states;
    }

    ret += msg->payload_len;

    return ret;
}

[[maybe_unused]] static int size_session(const struct session *s)
{
    int ret = 0;

    if (s->state != SESSION_ACTIVE) {
        errno = EINVAL;
        return -1;
    }

    ret += sizeof(s->uuid);
    ret += sizeof((uint32_t)s->last_connected);
    ret += sizeof((uint32_t)s->expiry_interval);
    ret += sizeof((uint8_t)s->request_response_information);
    ret += sizeof((uint8_t)s->request_problem_information);
    ret += sizeof(uint16_t); /* client_id_length */

    /* Cannot be NULL */
    const size_t client_id_length = strlen((void *)s->client_id);
    if (client_id_length > UINT16_MAX) {
        errno = EOVERFLOW;
        return -1;
    }
    ret += client_id_length;

    if (s->num_subscriptions > UINT16_MAX) {
        errno = EOVERFLOW;
        return -1;
    }

    ret += sizeof((uint16_t)s->num_subscriptions);

    static constexpr int sub_size_fixed =
      sizeof((uint8_t)s->subscriptions[0]->type)
    + sizeof(uint16_t) /* topic_filter_length */
    + sizeof((uint8_t)s->subscriptions[0]->option)
    + sizeof((uint32_t)s->subscriptions[0]->subscription_identifier);

    for (unsigned idx = 0; idx < s->num_subscriptions; idx++)
    {
        if (s->subscriptions[idx] == NULL || s->subscriptions[idx]->topic_filter == NULL) {
            errno = EINVAL;
            return -1;
        }

        ret += sub_size_fixed;

        const size_t topic_filter_len = strlen((void *)s->subscriptions[idx]->topic_filter);
        if (topic_filter_len > UINT16_MAX) {
            errno = EOVERFLOW;
            return -1;
        }

        ret += topic_filter_len;
    }

    return ret;
}

static constexpr uint8_t NULL_UUID[UUID_SIZE] = { 0 };

/**
 * only to be called from a serialise_ function
 */
static int write_mds(char **dst, const struct message_delivery_state *mds)
{
    write_u16(dst, mds->packet_identifier);
    if (mds->session)
        write_uuid(dst, mds->session->uuid);
    else {
        errno = EINVAL;
        return -1;
    }
    write_u32(dst, mds->last_sent);
    write_u32(dst, mds->acknowledged_at);
    write_u32(dst, mds->released_at);
    write_u32(dst, mds->completed_at);
    write_u8(dst, mds->client_reason);

    return 0;
}

[[gnu::nonnull]]
static int read_mds(struct message_delivery_state *mds, const char **src, size_t *bytes_remaining, struct message *msg, bool attach)
{
    uint8_t tmp_uuid[UUID_SIZE];
    uint32_t tmp_u32;
    uint8_t tmp_u8;
    int rc;

    rc = read_u16(&mds->packet_identifier, src, bytes_remaining);
    if (rc == -1) goto fail;

    rc = read_uuid(tmp_uuid, src, bytes_remaining);
    if (rc == -1) goto fail;

    rc = read_u32(&tmp_u32, src, bytes_remaining); mds->last_sent = tmp_u32;
    if (rc == -1) goto fail;

    rc = read_u32(&tmp_u32, src, bytes_remaining); mds->acknowledged_at = tmp_u32;
    if (rc == -1) goto fail;

    rc = read_u32(&tmp_u32, src, bytes_remaining); mds->released_at = tmp_u32;
    if (rc == -1) goto fail;

    rc = read_u32(&tmp_u32, src, bytes_remaining); mds->completed_at = tmp_u32;
    if (rc == -1) goto fail;

    rc = read_u8(&tmp_u8, src, bytes_remaining); mds->client_reason = (reason_code_t)tmp_u8;
    if (rc == -1) goto fail;

    struct session *session;

    if (memcmp(tmp_uuid, NULL_UUID, UUID_SIZE) == 0) {
        goto no_session;
    } else {
        if ((session = find_session_by_uuid(tmp_uuid)) == NULL) {
no_session:
            errno = ENOENT;
            return -1;
        }
    }


    if (attach)
        attach_mds(mds, msg, session);
    else {
        mds->session = session;
    }

    return 0;
fail:
    return -1;
}

[[maybe_unused]] static int deserialise_message(const struct message **ret, void *buffer, size_t *bytes_remaining)
{
    const char **src = buffer;
    uint32_t tmp_u32;
    uint8_t tmp_u8;
    uint16_t tmp_u16;
    uint8_t tmp_uuid[UUID_SIZE];
    int rc;
    struct message *out = NULL;

    *ret = NULL;

    rc = read_uuid(tmp_uuid, src, bytes_remaining);
    if (rc == -1 || is_null_uuid(tmp_uuid)) goto fail;

    if ((out = alloc_message(tmp_uuid)) == NULL)
        goto fail;

    rc = read_u32(&tmp_u32, src, bytes_remaining); out->payload_len = tmp_u32;
    if (rc == -1) goto fail;
    rc = read_u8(&tmp_u8, src, bytes_remaining); out->format = tmp_u8;
    if (rc == -1) goto fail;
    rc = read_u8(&tmp_u8, src, bytes_remaining); out->qos = tmp_u8;
    if (rc == -1) goto fail;
    rc = read_u8(&tmp_u8, src, bytes_remaining); out->retain = tmp_u8;
    if (rc == -1) goto fail;
    rc = read_u8(&tmp_u8, src, bytes_remaining); out->type = (message_type_t)tmp_u8;
    if (rc == -1) goto fail;

    if ( 
            (out->payload_len > MAX_PACKET_LENGTH) ||
            (out->qos > 2) ||
            (out->type >= MSG_TYPE_MAX) 
       ) {
        errno = EINVAL;
        goto fail;
    }

        errno = 

    rc = read_uuid(tmp_uuid, src, bytes_remaining);
    if (rc == -1) goto fail;
    if (is_null_uuid(tmp_uuid))
        out->sender = NULL;
    else if ((out->sender = find_session_by_uuid(tmp_uuid)) == NULL) {
        errno = ENOENT;
        goto fail;
    }

    rc = read_uuid(tmp_uuid, src, bytes_remaining);
    if (rc == -1) goto fail;
    if (is_null_uuid(tmp_uuid))
        out->topic = NULL;
    else if ((out->topic = find_topic_by_uuid(tmp_uuid)) == NULL) {
        errno = ENOENT;
        goto fail;
    }

    if (out->qos) {
        rc = read_mds(&out->sender_status, src, bytes_remaining, out, false);
        if (rc == -1) goto fail;
        rc = read_u16(&tmp_u16, src, bytes_remaining); out->num_message_delivery_states = tmp_u16;
        if (rc == -1) goto fail;

        out->delivery_states = calloc(out->num_message_delivery_states,
                sizeof(struct message_delivery_state *));

        if (out->delivery_states == NULL)
            goto fail;

        for (unsigned idx = 0; idx < out->num_message_delivery_states; idx++)
        {
            if ((out->delivery_states[idx] = alloc_message_delivery_state(NULL, NULL, true)) == NULL)
                goto fail;

            rc = read_mds(out->delivery_states[idx], src, bytes_remaining, out, true);
            if (rc == -1) goto fail;
        }
    }

    if (out->payload_len) {
        if (out->payload_len > *bytes_remaining) {
            errno = EOVERFLOW;
            goto fail;
        }

        if ((out->payload = malloc(out->payload_len)) == NULL)
            goto fail;
        rc = read_bytes((void *)out->payload, src, bytes_remaining, out->payload_len);
        if (rc == -1) goto fail;
    } else
        out->payload = NULL;

    out->state = MSG_ACTIVE;
    *ret = out;
    return 0;

fail:
    if (out) {
        out->state = MSG_DEAD;
        free_message(out, true);
    }
    *ret = NULL;
    return -1;
}

/**
 * buffer must be sized using size_message() first
 */
[[maybe_unused]] static int serialise_message(const struct message *msg, void *buffer)
{
    char *dst = buffer;

    write_uuid(&dst, msg->uuid);

    write_u32(&dst, msg->payload_len);
    write_u8(&dst, msg->format);
    write_u8(&dst, msg->qos);
    write_u8(&dst, msg->retain);
    write_u8(&dst, msg->type);

    write_uuid(&dst, msg->sender ? msg->sender->uuid : NULL_UUID);

    /* we need to store even for no retain, so QoS>0 works */
    write_uuid(&dst, msg->topic ? msg->topic->uuid : NULL_UUID);

    if (msg->qos) {
        if (write_mds(&dst, &msg->sender_status) == -1)
            return -1;
        write_u16(&dst, msg->num_message_delivery_states);

        for (unsigned idx = 0; idx < msg->num_message_delivery_states; idx++)
            if (write_mds(&dst, msg->delivery_states[idx]) == -1)
                return -1;
    }

    write_bytes(&dst, msg->payload, msg->payload_len);

    return 0;
}

/**
 * buffer must be sized using size_session() first
 */
[[maybe_unused]] static int serialise_session(const struct session *s, void *buffer)
{
    char *dst = buffer;

    write_uuid(&dst, s->uuid);
    write_u32(&dst, s->last_connected);
    write_u32(&dst, s->expiry_interval);
    write_u8(&dst, s->request_response_information);
    write_u8(&dst, s->request_problem_information);

    const size_t client_id_len = strlen((void *)s->client_id);
    write_u16(&dst, client_id_len);
    write_bytes(&dst, s->client_id, client_id_len);

    write_u16(&dst, s->num_subscriptions);

    for (unsigned idx = 0; idx < s->num_subscriptions; idx++)
    {
        write_u8(&dst, s->subscriptions[idx]->type);
        const size_t topic_filter_len = strlen((void *)s->subscriptions[idx]->topic_filter);
        write_u16(&dst, topic_filter_len);
        write_bytes(&dst, s->subscriptions[idx]->topic_filter, topic_filter_len);
        write_u8(&dst, s->subscriptions[idx]->option);
        write_u32(&dst, s->subscriptions[idx]->subscription_identifier);
    }

    return 0;
}

/**
 * buffer must be sized using size_message() first
 */
[[maybe_unused]] static int serialise_topic(const struct topic *topic, void *buffer)
{
    char *dst = buffer;

    write_uuid(&dst, topic->uuid);
    if (topic->retained_message)
        write_uuid(&dst, topic->retained_message->uuid);
    else
        write_uuid(&dst, NULL_UUID);

    const size_t name_len = strlen((void *)topic->name);
    write_u16(&dst, name_len);
    write_bytes(&dst, topic->name, name_len);

    return 0;
}

[[gnu::nonnull]]
static int save_message(const struct message *msg)
{
    struct message_save *save = NULL;

    assert(msg->uuid);

    errno = EINVAL;

    pthread_rwlock_rdlock(&global_messages_lock);

    if (msg->state != MSG_ACTIVE) {
        warnx("save_message: attempt to save message in invalid state");
        goto fail;
    }

    dbg_printf("     save_message: saving message id=%d uuid=%s\n",
            msg->id, uuid_to_string(msg->uuid));

    size_t size = sizeof(struct message_save) + msg->payload_len;

    if ((save = malloc(size)) == NULL)
        goto fail;

    save->id = msg->id;
    save->format = msg->format;
    save->payload_len = msg->payload_len;
    save->qos = msg->qos;
    save->retain = msg->retain;
    save->type = msg->type;

    memcpy(save->uuid, msg->uuid, UUID_SIZE);
    if (msg->topic)
        memcpy(save->topic_uuid, msg->topic->uuid, UUID_SIZE);
    if (msg->payload)
        memcpy(&save->payload, msg->payload, msg->payload_len);

    datum key = {
        .dptr = (char *)msg->uuid,
        .dsize = sizeof(msg->uuid),
    };

    datum content = {
        .dptr = (void *)save,
        .dsize = size
    };

    if (dbm_store(message_dbm, key, content, DBM_REPLACE) < 0) {
        int err = dbm_error(message_dbm);
        logger(LOG_WARNING, NULL, "save_message: dbm_store: %u:%s", err, strerror(err));
        errno = err;
        dbm_clearerr(message_dbm);
        goto fail;
    }

    free(save);

    pthread_rwlock_unlock(&global_messages_lock);
    return 0;

fail:
    if (save)
        free(save);
    pthread_rwlock_unlock(&global_messages_lock);
    return -1;
}

[[gnu::nonnull]]
int save_topic(const struct topic *topic)
{
    errno = EINVAL;

    pthread_rwlock_rdlock(&global_topics_lock);

    if (topic->state != TOPIC_ACTIVE) {
        warn("save_topic: attempt to save topic <%s> in illegal state %u",
                topic->name, topic->state);
        goto fail;
    }

    dbg_printf("     save_topic: saving topic id=%d name=<%s> %s%s\n",
            topic->id, topic->name,
            topic->retained_message ? "retained=" : "",
            topic->retained_message ? uuid_to_string(topic->retained_message->uuid) : ""
            );

    struct topic_save save;
    memset(&save, 0, sizeof(save));

    save.id = topic->id;
    memcpy(save.uuid, topic->uuid, UUID_SIZE);
    strncpy(save.name, (char *)topic->name, sizeof(save.name) - 1);
    if (topic->retained_message) {
        memcpy(save.retained_message_uuid, topic->retained_message->uuid, UUID_SIZE);
        dbg_printf("     save_topic: set retained_message_uuid to %s\n",
                uuid_to_string(save.retained_message_uuid));
    }

    datum key = {
        .dptr = (char *)topic->uuid,
        .dsize = sizeof(topic->uuid),
    };

    datum content = {
        .dptr = (char *)&save,
        .dsize = sizeof(save),
    };

    if (topic->retained_message)
        if (save_message(topic->retained_message) == -1) {
            logger(LOG_WARNING, NULL, "save_topic: not saving topic due to save_message: %s", strerror(errno));
            goto fail;
        }

    if (dbm_store(topic_dbm, key, content, DBM_REPLACE) < 0) {
        int err = dbm_error(topic_dbm);
        logger(LOG_WARNING, NULL, "save_topic: dbm_store: %u:%s", err, strerror(err));
        dbm_clearerr(topic_dbm);
    }

    pthread_rwlock_unlock(&global_topics_lock);
    return 0;

fail:
    pthread_rwlock_unlock(&global_topics_lock);
    return -1;
}

static int register_session(struct session *session
#ifdef FEATURE_RAFT
        , bool source_self
#endif
        )
{
    pthread_rwlock_wrlock(&global_sessions_lock);
    if (session->state != SESSION_NEW) {
        errno = EINVAL;
        goto fail;
    }

    dbg_printf("[%2d] register_session: session %d registered\n",
            session->id, session->id);
#ifdef FEATURE_RAFT
    if (source_self) {
        /* TODO raft_client_log_send(RAFT_LOG_REGISTER_SESSION, ...); */
        session->state = SESSION_ACTIVE;
    } else {
        session->state = SESSION_ACTIVE;
    }
#else
    session->state = SESSION_ACTIVE;
#endif
    pthread_rwlock_unlock(&global_sessions_lock);
    return 0;

fail:
    pthread_rwlock_unlock(&global_sessions_lock);
    return -1;
}

/* [MQTT-3.1.3-2] */
[[gnu::nonnull, gnu::warn_unused_result]]
static struct session *find_session(const struct client *client)
{
    pthread_rwlock_rdlock(&global_sessions_lock);
    for (struct session *tmp = global_session_list; tmp; tmp = tmp->next)
    {
        if (tmp->state != SESSION_ACTIVE)
            continue;

        if (strcmp((const char *)tmp->client_id,
                    (const char *)client->client_id))
            continue;

        pthread_rwlock_unlock(&global_sessions_lock);
        return tmp;
    }
    pthread_rwlock_unlock(&global_sessions_lock);

    return NULL;
}

/*
 * packet parsing helpers
 */

[[gnu::warn_unused_result]]
static bool is_malformed(reason_code_t code)
{
    if (code < MQTT_MALFORMED_PACKET)
        return false;

    switch (code)
    {
        case MQTT_MALFORMED_PACKET:
        case MQTT_PROTOCOL_ERROR:
        case MQTT_RECEIVE_MAXIMUM_EXCEEDED:
        case MQTT_PACKET_TOO_LARGE:
        case MQTT_RETAIN_NOT_SUPPORTED:
        case MQTT_QOS_NOT_SUPPORTED:
        case MQTT_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED:
        case MQTT_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED:
        case MQTT_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED:
            return true;
        default:
            return false;
    }
}

static int disconnect_if_malformed(struct client *client, reason_code_t code)
{
    if (!is_malformed(code))
        return 0;

    client->send_disconnect = false;
    client->disconnect_reason = code;
    client->state = CS_CLOSED;
    if (client->fd != -1)
        close_socket(&client->fd);

    if (errno == 0)
        errno = EINVAL;
    return -1;
}

[[gnu::nonnull]]
static bool is_shared_subscription(const uint8_t *name)
{
    if (!strncmp(SHARED_PREFIX, (const char *)name, SHARED_PREFIX_LENGTH))
        return true;

    return false;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int is_valid_topic_name(const uint8_t *name)
{
    const uint8_t *ptr;
    ptr = name;

    /* [MQTT-4.7.3-1] */
    if (!*ptr) {
        errno = EINVAL;
        return -1;
    }

    while (*ptr)
    {
        if (*ptr == '#' || *ptr == '+') {
            errno = EINVAL;
            return -1;
        }

        ptr++;
    }

    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int is_valid_topic_filter(const uint8_t *name)
{
    const uint8_t *ptr;

    errno = EINVAL;
    ptr = name;

    /* [MQTT-4.7.3-1] */
    if (!*ptr)
        return -1;

    if (!strcmp((const char *)ptr, "/"))
        return -1;

    while (*ptr)
    {
        /* The multi-level wildcard character MUST be specified either on its
         * own or following a topic level separator.
         *
         * In either case it MUST be the last character specified in the
         * Topic Filter
         */
        if (*ptr == '#') {
            if (*(ptr+1))
                return -1;
            if (ptr > name && *(ptr-1) != '/')
                return -1;
        }

        if (*ptr == '+') {
            if (*(ptr+1) && *(ptr+1) != '/')
                return -1;
            if (ptr > name && *(ptr-1) != '/')
                return -1;
        }

        ptr++;
    }

    errno = 0;
    return 0;
}

/* assumes name and filter are is_valid_topic_name() and is_valid_topic_filter() */
[[gnu::nonnull]]
static bool topic_match(const uint8_t *const name, const uint8_t *const filter)
{
    const uint8_t *name_ptr, *tmp_name_ptr;
    const uint8_t *filter_ptr, *tmp_filter_ptr;
    uint8_t tmpnamebuf[BUFSIZ];
    uint8_t tmpfilterbuf[BUFSIZ];
    bool multi_match = false;
    size_t len = 0;

    errno = 0;

    name_ptr = name;
    filter_ptr = filter;

    while (*name_ptr && *filter_ptr)
    {
        tmp_name_ptr = name_ptr;

        while (*tmp_name_ptr && *tmp_name_ptr != '/')
            tmp_name_ptr++;

        memcpy(tmpnamebuf, name_ptr, tmp_name_ptr - name_ptr);
        tmpnamebuf[tmp_name_ptr - name_ptr] = '\0';

        if (*tmp_name_ptr == '/')
            tmp_name_ptr++;

        name_ptr = tmp_name_ptr;

        if (multi_match) {
            multi_match = false;
            goto skip_multi;
        }

        tmp_filter_ptr = filter_ptr;

        while (*tmp_filter_ptr && *tmp_filter_ptr != '/')
            tmp_filter_ptr++;

        memcpy(tmpfilterbuf, filter_ptr, tmp_filter_ptr - filter_ptr);
        tmpfilterbuf[tmp_filter_ptr - filter_ptr] = '\0';

        if (*tmp_filter_ptr == '/')
            tmp_filter_ptr++;

        filter_ptr = tmp_filter_ptr;
        len = strlen((const void *)tmpfilterbuf);

        if (len == 1 && tmpfilterbuf[0] == '#')
            filter_ptr--;

skip_multi:
        if (!strcmp((const void *)tmpfilterbuf, (const void *)tmpnamebuf))
            goto next;

        if (len == 1 && tmpfilterbuf[0] == '+')
            goto next;

        if (len == 1 && tmpfilterbuf[0] == '#') {
            multi_match = true;
            goto next;
        }

        return false;
next:
    }

    /* handle the corner-case where the last filter is a '#' but only if
     * it's not after a full path match.
     * e.g. a/b/c a/+/+/# should fail, a/# and a/+/# and # should pass
     */
    if (*name_ptr ||                        /* we have name left, fail       */
            /* OR                            */
            (*filter_ptr &&                 /* we have filter left, fail     */
             !(multi_match &&               /* .. UNLESS we have: 0 #match   */
                 *filter_ptr == '#' &&      /* ... AND this is a # match     */
                 *(filter_ptr+1) == '\0')   /* ... AND the # is last (not needed as this would be invalid filter?) */
            )
       )
        return false;

    return true;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int encode_var_byte(uint32_t value, uint8_t out[static 4])
{
    uint8_t byte;
    int out_len = 0;

    do {
        byte = value % 128;
        value /= 128;
        if (value > 0)
            byte |= 128;
        out[out_len++] = byte;
    } while (value > 0);

    return out_len;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static uint32_t read_var_byte(const uint8_t **const ptr, size_t *bytes_left)
{
    uint32_t value = 0;
    uint32_t multi = 1;
    uint8_t tmp;

    errno = 0;

    do {
        if (*bytes_left == 0) {
            errno = ERANGE;
            warn("read_var_byte: bytes_left is 0");
            return 0;
        }

        tmp = **ptr;
        *ptr = *ptr + 1;
        *bytes_left = *bytes_left - 1;

        if (multi > 128*128*128) {
            warn("invalid variable byte int");
            errno = EINVAL;
            return 0;
        }

        value += ((tmp & 127) * multi);
        multi *= 128;
    } while((tmp & 128) != 0);

    return value;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static void *read_binary(const uint8_t **const ptr, size_t *bytes_left,
        uint16_t *length)
{
    void *blob = NULL;
    uint16_t tmp;

    errno = 0;

    if (*bytes_left < sizeof(uint16_t)) {
        errno = ENOSPC;
        return NULL;
    }

    memcpy(&tmp, *ptr, sizeof(uint16_t));

    *length = ntohs(tmp);
    *ptr += sizeof(uint16_t);
    *bytes_left -= sizeof(uint16_t);

    if (*bytes_left < *length) {
        errno = ENOSPC;
        return NULL;
    }

    if (*length > 0) {
        if ((blob = malloc(*length)) == NULL) {
            errno = ENOMEM;
            return NULL;
        }

        memcpy(blob, *ptr, *length);

        *ptr += *length;
        *bytes_left -= *length;
    }

    return blob;
}

/* [MQTT-3.1.3-5]. */
static int is_valid_connection_id(const uint8_t *str)
{
    const uint8_t *ptr = str;

    errno = 0;

    while (*ptr)
    {
        if (*ptr >= '0' && *ptr <= '9')
            goto next;
        if (*ptr >= 'A' && *ptr <= 'Z')
            goto next;
        if (*ptr >= 'a' && *ptr <= 'z')
            goto next;

        errno = EINVAL;
        return -1;

next:
        ptr++;
    }

    return 0;
}

static int is_valid_utf8(const uint8_t *str)
{
    const uint8_t *ptr = str;

    unsigned bytes;

    while (*ptr)
    {
        if (*ptr < 0x80) {
            ptr++;
            continue;
        }

        if ((*ptr & 0xc0) == 0x80)
            bytes = 1;
        else if ((*ptr & 0xe0) == 0xc0)
            bytes = 2;
        else if ((*ptr & 0xf0) == 0xe0)
            bytes = 3;
        else
            return -1;

        ptr++;

        while(bytes--)
        {
            if (*ptr == '\0')
                return -1;
            if ((*ptr & 0xc0) != 0x80)
                return -1;
            ptr++;
        }
    }

    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static uint8_t *read_utf8(const uint8_t **const ptr, size_t *bytes_left)
{
    uint16_t str_len;
    uint8_t *string;

    errno = 0;

    if (*bytes_left < sizeof(uint16_t)) {
        errno = ENOSPC;
        return NULL;
    }

    memcpy(&str_len, *ptr, sizeof(uint16_t));
    str_len = ntohs(str_len);

    *ptr += sizeof(uint16_t);
    *bytes_left -= sizeof(uint16_t);

    if (*bytes_left < str_len) {
        errno = ENOSPC;
        return NULL;
    }

    if ((string = malloc(str_len + 1)) == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    string[str_len] = '\0';

    if (str_len > 0) {
        memcpy(string, *ptr, str_len);
        *ptr += str_len;
        *bytes_left -= str_len;

        for (unsigned idx = 0; idx < str_len; idx++) {
            if (string[idx] == '\0') {
                errno = EINVAL;
                goto fail;
            }
        }
    }

    if (is_valid_utf8(string) == -1)
        goto fail;

    return string;
fail:
    if (string)
        free(string);

    return NULL;
}

[[gnu::nonnull]]
static int get_property_value(const struct property (*props)[],
        unsigned num_props, property_ident_t id, const struct property **out)
{
    errno = 0;

    for (unsigned idx = 0; idx < num_props; idx++)
    {
        if ((*props)[idx].ident != id)
            continue;

        *out = &((*props)[idx]);
        return 0;
    }

    errno = ESRCH;
    return -1;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static ssize_t get_properties_size(const struct property (*props)[],
        unsigned num_props)
{
    ssize_t ret;
    const struct property *prop;
    uint8_t tmp_out[4];
    type_t type;

    if (num_props == 0)
        return 0;

    ret = 0;

    for (unsigned idx = 0; idx < num_props; idx++)
    {
        prop = &(*props)[idx];
        ret++; /* Property Type */

        if (prop->ident >= MQTT_PROPERTY_IDENT_MAX) {
            errno = ERANGE;
            return -1;
        }

        type = property_to_type[prop->ident];

        switch (type)
        {
            case MQTT_TYPE_BYTE:
                ret++;
                break;
            case MQTT_TYPE_2BYTE:
                ret += sizeof(uint16_t);
                break;
            case MQTT_TYPE_4BYTE:
                ret += sizeof(uint32_t);
                break;
            case MQTT_TYPE_BINARY:
                ret += prop->binary.len;
                ret += sizeof(uint16_t);
                break;
            case MQTT_TYPE_UTF8_STRING:
                ret += strlen((const char *)prop->utf8_string);
                ret += sizeof(uint16_t);
                break;
            case MQTT_TYPE_VARBYTE:
                ret += encode_var_byte(prop->varbyte, tmp_out);
                break;
            case MQTT_TYPE_UTF8_STRING_PAIR:
                ret += strlen((const char *)prop->utf8_pair[0]);
                ret += strlen((const char *)prop->utf8_pair[1]);
                ret += (2 * sizeof(uint16_t)); /* 2x2 */
                break;
            case MQTT_TYPE_UNDEFINED:
            case MQTT_TYPE_MAX:
                errno = EINVAL;
                warnx("get_propertes_size: attempt to size undefined MQTT_TYPE");
                return -1;
        }
    }

    return ret;
}

[[gnu::nonnull(2)]]
static void do_one_string(const uint8_t *str, uint8_t **ptr)
{
    unsigned len;
    uint16_t enclen;

    if (str) {
        len = strlen((const char *)str);
        enclen = htons(len);
    } else {
        len = 0;
        enclen = 0;
    }
    memcpy(*ptr, &enclen, sizeof(uint16_t));
    *ptr += sizeof(uint16_t);
    if (len != 0) {
        memcpy(*ptr, str, len);
        *ptr += len;
    }
}

[[gnu::nonnull]]
static int build_properties(const struct property (*props)[],
        unsigned num_props, uint8_t **out)
{
    uint8_t *ptr = *out;
    uint16_t tmp2byte;
    uint32_t tmp4byte;
    const struct property *prop;
    type_t type;
    int rc;

    if (num_props == 0)
        return 0;

    for (unsigned idx = 0; idx < num_props; idx++)
    {
        prop = &(*props)[idx];

        if (prop->ident >= MQTT_PROPERTY_IDENT_MAX) {
            errno = ERANGE;
            goto fail;
        }

        type = property_to_type[prop->ident];

        *ptr = prop->ident;
        ptr++;

        switch (type)
        {
            case MQTT_TYPE_BYTE:
                *ptr = prop->byte;
                ptr++;
                break;

            case MQTT_TYPE_2BYTE:
                tmp2byte = htons(prop->byte2);
                memcpy(ptr, &tmp2byte, sizeof(uint16_t));
                ptr += sizeof(uint16_t);
                break;

            case MQTT_TYPE_4BYTE:
                tmp4byte = htonl(prop->byte4);
                memcpy(ptr, &tmp4byte, sizeof(uint32_t));
                ptr += sizeof(uint32_t);
                break;

            case MQTT_TYPE_VARBYTE:
                if ((rc = encode_var_byte(prop->varbyte, ptr)) == -1)
                    goto fail;
                ptr += rc;
                break;

            case MQTT_TYPE_BINARY:
                tmp2byte = htons(prop->binary.len);
                memcpy(ptr, &tmp2byte, sizeof(uint16_t));
                ptr += sizeof(uint16_t);
                if (prop->binary.len) {
                    memcpy(ptr, prop->binary.data, prop->binary.len);
                    ptr += prop->binary.len;
                }
                break;

            case MQTT_TYPE_UTF8_STRING:
                do_one_string(prop->utf8_string, &ptr);
                break;

            case MQTT_TYPE_UTF8_STRING_PAIR:
                do_one_string(prop->utf8_pair[0], &ptr);
                do_one_string(prop->utf8_pair[1], &ptr);
                break;

            case MQTT_TYPE_UNDEFINED:
            case MQTT_TYPE_MAX:
                errno = EINVAL;
                warnx("build_properties: invalid MQTT_TYPE");
                goto fail;
        }
    }

    *out = ptr;
    return 0;

fail:
    return -1;
}

/* a type of MQTT_CP_INVALID is used for situations where the
 * properties are NOT the standard ones in a packet,
 * e.g. "will_properties" */
[[gnu::nonnull]]
static int parse_properties(
        const uint8_t **ptr, size_t *bytes_left,
        struct property (**store_props)[], unsigned *store_num_props,
        control_packet_t cp_type)
{
    uint32_t properties_length;
    size_t rd = 0;
    uint8_t ident;
    struct property (*props)[] = NULL;
    struct property *prop;
    unsigned num_props = 0, skip;
    void *tmp;
    type_t type;

    errno = 0;

    if (*bytes_left == 0)
        return 0;

    properties_length = read_var_byte(ptr, bytes_left);

    if (properties_length == 0 && errno)
        return -1;

    if (properties_length == 0)
        return 0;

    if (*bytes_left < properties_length) {
        errno = ENOSPC;
        return -1;
    }

    rd = *bytes_left - properties_length;
    while (*bytes_left > rd)
    {
        if (num_props == MAX_PROPERTIES) {
            errno = ENOSPC;
            goto fail;
        }

        if ((tmp = realloc(props, sizeof(struct property) * (num_props + 1))) == NULL)
            goto fail;
        props = tmp;

        memset(&(*props)[num_props], 0, sizeof(struct property));

        ident = **ptr;
        *ptr = *ptr + 1;
        *bytes_left = *bytes_left - 1;

        prop = &(*props)[num_props];

        if (ident >= MQTT_PROPERTY_IDENT_MAX) {
            errno = EINVAL;
            goto fail;
        }
        prop->ident = ident;

        type = property_to_type[prop->ident];

        /* TODO perform "is this valid for this control type?" */

        /* for will_properties, there is no cp_type */
        if (cp_type != MQTT_CP_INVALID)
            if (property_per_control[prop->ident][cp_type] == false) {
                warnx("parse_properties: %s:%s is invalid",
                        control_packet_str[cp_type],
                        property_str[prop->ident]);
                errno = EINVAL;
                goto fail;
            }

        skip = 0;

        switch (type)
        {
            case MQTT_TYPE_BYTE:
                if (*bytes_left < 1)
                    goto fail;
                prop->byte = **ptr;
                skip = 1;
                break;

            case MQTT_TYPE_2BYTE:
                if (*bytes_left < sizeof(uint16_t))
                    goto fail;
                memcpy(&prop->byte2, *ptr, sizeof(uint16_t));
                prop->byte2 = ntohs(prop->byte2);
                skip = sizeof(uint16_t);
                break;

            case MQTT_TYPE_4BYTE:
                if (*bytes_left < sizeof(uint32_t))
                    goto fail;
                memcpy(&prop->byte4, *ptr, sizeof(uint32_t));
                prop->byte4 = ntohl(prop->byte4);
                skip = sizeof(uint32_t);
                break;

            case MQTT_TYPE_VARBYTE:
                prop->varbyte = read_var_byte(ptr, bytes_left);
                if (prop->varbyte == 0 && errno)
                    goto fail;
                break;

            case MQTT_TYPE_UTF8_STRING:
                prop->utf8_string = read_utf8(ptr, bytes_left);
                if (prop->utf8_string == NULL)
                    goto fail;
                break;

            case MQTT_TYPE_BINARY:
                if (*bytes_left < sizeof(uint16_t))
                    goto fail;

                memcpy(&prop->binary.len, *ptr, sizeof(uint16_t));
                *ptr += sizeof(uint16_t);
                *bytes_left -= sizeof(uint16_t);
                prop->binary.len = ntohs(prop->binary.len);

                if (prop->binary.len) {
                    if (prop->binary.len > *bytes_left)
                        goto fail;

                    if ((prop->binary.data = malloc(prop->binary.len)) == NULL)
                        goto fail;

                    memcpy(prop->binary.data, *ptr, prop->binary.len);
                    *ptr += prop->binary.len;
                    *bytes_left -= prop->binary.len;

                }
                break;

            case MQTT_TYPE_UTF8_STRING_PAIR:
                prop->utf8_pair[0] = read_utf8(ptr, bytes_left);
                if (prop->utf8_pair[0] == NULL)
                    goto fail;

                prop->utf8_pair[1] = read_utf8(ptr, bytes_left);
                if (prop->utf8_pair[1] == NULL)
                    goto fail;

                break;

            case MQTT_TYPE_MAX: /* Avoid GCC warnings */
            case MQTT_TYPE_UNDEFINED:
                errno = EINVAL;
                warn("parse_properties: illegal use of property type 0");
                goto fail;
        }

        if (skip) {
            *ptr = *ptr + skip;
            *bytes_left = *bytes_left - skip;
        }

        num_props++;
    }

    *store_props = props;
    *store_num_props = num_props;

    return 0;

fail:
    if (props)
        free_properties(props, num_props);

    return -1;
}

/*
 * signal handlers
 */

static void sh_sigint(int signum, siginfo_t * /*info*/, void * /*stuff*/)
{
    if (signum == SIGALRM)
        return;

    logger(LOG_WARNING, NULL, "sh_sigint: received signal %u", signum);

    if (signum == SIGHUP) {
        return;
    }

    if (running == false)
        _exit(EXIT_FAILURE);

    running = false;
}

/*
 * atexit() functions
 */

static void clean_pid(void)
{
    if (unlink(PID_FILE) == -1)
        logger(LOG_WARNING, NULL, "unable to unlink PID file: %s",
                strerror(errno));
}

static void close_logfile(void)
{
    if (opt_logfile) {
        fclose(opt_logfile);
        opt_logfile = NULL;
    }
}

static void close_databases(void)
{
    if (topic_dbm) {
        dbm_close(topic_dbm);
        topic_dbm = NULL;
    }

    if (message_dbm) {
        dbm_close(message_dbm);
        message_dbm = NULL;
    }
}

static void close_all_sockets(void)
{
    dbg_printf("     close_socket: closing mother_fd %u\n", global_mother_fd);
    if (global_mother_fd != -1)
        close_socket(&global_mother_fd);

#ifdef FEATURE_OM
    if (opt_openmetrics) {
        dbg_printf("     close_socket: closing openmetrics_fd %u\n", global_om_fd);
        if (global_om_fd != -1)
            close_socket(&global_om_fd);
    }
#endif

}

int attempt_save_all_topics(void)
{
    int rc = 0;
    const struct topic *topic;

    if (opt_database == false)
        return 0;

    dbg_printf("     "BYEL"save_all_topics"CRESET"\n");
    for (topic = global_topic_list; topic; topic = topic->next)
        if (save_topic(topic) == -1)
            rc = -1;

    return rc;
}

static void save_all_topics(void)
{
    if (attempt_save_all_topics() == -1)
        warn("save_all_topics");
}

static void free_all_message_delivery_states(void)
{
    struct message_delivery_state *mds, *next;

    dbg_printf("     "BYEL"free_all_message_delivery_states"CRESET"\n");
    /* don't bother locking this late in tear down */
    for (mds = global_mds_list; mds; mds = next)
    {
        next = mds->next;
        if (mds->state != MDS_ACTIVE) {
            if (free_message_delivery_state(mds) == -1)
                warn("free_all_message_delivery_states: free_message_delivery_state");
        } else if (mds_detach_and_free(mds, false, false) == -1)
            warn("free_all_message_delivery_states: mds_detach_and_free");
    }
}

static void free_all_sessions(void)
{
    dbg_printf("     "BYEL"free_all_sessions"CRESET"\n");
    while (global_session_list)
        free_session(global_session_list, true);
}

static void free_all_messages_two(void)
{
    dbg_printf("     "BYEL"free_all_messages_two"CRESET"\n");
    while (global_message_list)
        free_message(global_message_list, true);
}

static void free_all_messages(void)
{
    dbg_printf("     "BYEL"free_all_messages"CRESET"\n");
    while (global_message_list)
        free_message(global_message_list, true);
}

static void free_all_clients(void)
{
    dbg_printf("     "BYEL"free_all_clients"CRESET"\n");
    while (global_client_list)
        free_client(global_client_list, true);
}

static void free_all_packets(void)
{
    dbg_printf("     "BYEL"free_all_packets"CRESET"\n");
    while (global_packet_list)
        free_packet(global_packet_list, false, false);
}

static void free_all_topics_two(void)
{
    dbg_printf("     "BYEL"free_all_topics_two"CRESET"\n");
    while (global_topic_list)
        free_topic(global_topic_list);
    assert(global_subscription_list == NULL);
}

static void free_all_topics(void)
{
    struct topic *tmp;
    bool to_parse;

    dbg_printf("     "BYEL"free_all_topics"CRESET"\n");

    to_parse = (global_topic_list != NULL);

    while (to_parse)
    {
        to_parse = false;

        for (tmp = global_topic_list; tmp; tmp = tmp->next)
        {
            if (tmp->state == TOPIC_DEAD)
                continue;

            to_parse = true;

            free_topic(tmp);
            break;
        }
    }
}

/*
 * message distribution
 */

[[gnu::nonnull, gnu::warn_unused_result]]
struct message *find_message_by_uuid(const uint8_t uuid[static const UUID_SIZE])
{
    errno = 0;

    pthread_rwlock_rdlock(&global_messages_lock);
    for (struct message *msg = global_message_list; msg; msg = msg->next)
    {
        if (!memcmp(msg->uuid, uuid, UUID_SIZE)) {
            pthread_rwlock_unlock(&global_messages_lock);
            return msg;
        }
    }
    pthread_rwlock_unlock(&global_messages_lock);

    errno = ENOENT;
    return NULL;
}

struct topic *find_topic_by_uuid(const uint8_t uuid[static const UUID_SIZE])
{
    errno = 0;

    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *topic = global_topic_list; topic; topic = topic->next)
    {
        if (!memcmp(topic->uuid, uuid, UUID_SIZE)) {
            pthread_rwlock_unlock(&global_topics_lock);
            return topic;
        }
    }
    pthread_rwlock_unlock(&global_topics_lock);
    return NULL;
}

[[gnu::nonnull, gnu::warn_unused_result]]
struct topic *find_topic(const uint8_t *name, bool active_only, bool need_lock)
{
    errno = 0;

    if (need_lock) pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
    {
        if (!strcmp((const void *)name, (const void *)tmp->name)) {
            if (active_only && tmp->state != TOPIC_ACTIVE)
                continue;
            if (need_lock) pthread_rwlock_unlock(&global_topics_lock);
            return tmp;
        }
    }
    if (need_lock) pthread_rwlock_unlock(&global_topics_lock);

    return NULL;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static struct subscription *find_matching_subscription(const uint8_t *name,
        struct subscription **start)
{
    struct subscription *tmp;

    for (tmp = *start; tmp; tmp = tmp->next)
    {
        dbg_printf("     find_matching_subscription: comparing <%s> to <%s>\n",
                name, tmp->topic_filter);

        if (topic_match(name, tmp->topic_filter)) {
            *start = tmp->next;
            return tmp;
        }
    }

    *start = NULL;

    dbg_printf("     find_matching_subscription: no match\n");
    return NULL;
}

[[gnu::nonnull(1), gnu::warn_unused_result]]
static struct topic *find_or_register_topic(const uint8_t *name)
{
    struct topic *topic = NULL;
    const uint8_t *tmp_name = NULL;

    if ((topic = find_topic(name, false, true)) == NULL) {
        if ((tmp_name = (void *)strdup((const char *)name)) == NULL)
            goto fail;

#ifdef FEATURE_RAFT
        if ((topic = register_topic(tmp_name, NULL, true)) == NULL) {
#else
        if ((topic = register_topic(tmp_name, NULL)) == NULL) {
#endif
            warn("find_or_register_topic: register_topic <%s>", tmp_name);
            goto fail;
        }

        free((void *)tmp_name);
        if (opt_database)
            save_topic(topic);
    }

    return topic;

fail:

    if (tmp_name)
        free((void *)tmp_name);

    return NULL;
}

[[gnu::nonnull]]
static struct subscription *find_subscription(const struct session *session,
        const uint8_t *topic_filter)
{
    struct subscription *tmp;

    pthread_rwlock_rdlock(&global_subscriptions_lock);
    for (tmp = global_subscription_list; tmp; tmp = tmp->next)
    {
        if (tmp->topic_filter == NULL)
            continue;

        if (strcmp((const void *)tmp->topic_filter, (const void *)topic_filter))
            continue;

        switch(tmp->type)
        {
            case SUB_NON_SHARED:
                if (session != tmp->non_shared.session)
                    continue;
                break;

            case SUB_SHARED:
                break;

            default:
                logger(LOG_WARNING, NULL,
                        "find_subscription: unsupported subscription type");
                errno = EINVAL;
                return NULL;
        }

        errno = 0;
        pthread_rwlock_unlock(&global_subscriptions_lock);

        return tmp;
    }
    pthread_rwlock_unlock(&global_subscriptions_lock);
    errno = ENOENT;
    return NULL;
}

[[gnu::nonnull(1), gnu::warn_unused_result]]
struct topic *register_topic(const uint8_t *name, const uint8_t uuid[UUID_SIZE] RAFT_API_SOURCE_SELF)
{
    struct topic *ret;

    errno = 0;

    assert(name != NULL);

    if (is_valid_topic_name(name) == -1)
        return NULL;

    if ((ret = alloc_topic(name, uuid)) == NULL)
        return NULL;

    /* We do not save here, caller must save, find_or_register_topic() does this */

    dbg_printf(BYEL "     register_topic: name=%s" CRESET "\n", (char *)name);

#ifdef FEATURE_RAFT
    if (source_self) {
        uint32_t flags = 0;
        if (raft_client_log_send(RAFT_LOG_REGISTER_TOPIC, ret->name, &ret->uuid, flags,
                    ret->retained_message ? &ret->retained_message->uuid : NULL) == -1)
            goto fail;
        ret->state = TOPIC_PREACTIVE;
    } else{
        ret->state = TOPIC_ACTIVE;
    }
#else
    ret->state = TOPIC_ACTIVE;
#endif

    pthread_rwlock_wrlock(&global_topics_lock);
    ret->next = global_topic_list;
    global_topic_list = ret;
    pthread_rwlock_unlock(&global_topics_lock);

    return ret;

#ifdef FEATURE_RAFT
fail:
    if (ret)
        free_topic(ret);
    return NULL;
#endif
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int remove_delivery_state(
        struct message_delivery_state ***state_array,
        unsigned *array_length,
        struct message_delivery_state *rem)
{
    const unsigned old_length = *array_length;
    const unsigned new_length = *array_length - 1;
    struct message_delivery_state **tmp = NULL;

    dbg_printf("     remove_delivery_state: array_length=%u new_length=%u rem=%u session=%d message=%d\n",
            *array_length, new_length, rem->id,
            rem->session ? rem->session->id : (id_t)-1,
            rem->message ? rem->message->id : (id_t)-1
            );

    errno = 0;

    if (old_length == 0) {
        warnx("remove_delivery_state: is empty");
        errno = EINVAL;
        return -1;
    }

    if (new_length == 0) {
        if (*state_array)
            free(*state_array);
        *state_array = NULL;
        *array_length = 0;
        return 0;
    }

    assert(*state_array != NULL);
    unsigned new_idx = 0, old_idx = 0;

    if ((tmp = malloc(sizeof(struct message_delivery_state *) * new_length)) == NULL)
        goto fail;

    bool found = false;

    for (; new_idx <= new_length && old_idx < old_length; old_idx++)
    {
        if ((*state_array)[old_idx] == rem) {
            found = true;
            continue;
        }

        if (new_idx == new_length)
            break;

        tmp[new_idx] = (*state_array)[old_idx];
        new_idx++;
    }

    if (found == true) {
        if (*state_array)
            free(*state_array);
        *state_array = tmp;
        *array_length = new_length;
        dbg_printf("     remove_delivery_state: done: old_length=%u new_length=%u\n",
                old_length, new_length);
        return 0;
    }

    errno = ENOENT;

fail:
    if (tmp)
        free(tmp);

    warn("remove_delivery_state: failed on %p id=%d old_length=%u new_length=%u old_idx=%u new_idx=%u",
            rem, rem->id, old_length, new_length, old_idx, new_idx);

    for (old_idx = 0; old_idx < old_length; old_idx++)
        warnx("remove_delivery_state: old[%2u] = %p",
                old_idx, (*state_array)[old_idx]);

    warnx("remove_delivery_state: rem: id=%u, session=%p, message=%p",
            rem->id, rem->session, rem->message);

    return -1;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int add_to_delivery_state(
        struct message_delivery_state ***state_array, unsigned *array_length,
        pthread_rwlock_t *lock, const struct message_delivery_state *add)
{
    pthread_rwlock_wrlock(lock);

    const unsigned new_length = (*array_length) + 1;
    const size_t new_size = sizeof(struct message_delivery_state *) * new_length;
    struct message_delivery_state **tmp = NULL;

    if ((tmp = realloc(*state_array, new_size)) == NULL)
        goto fail;

    tmp[*array_length] = (void *)add;

    *state_array = tmp;
    *array_length = new_length;

    pthread_rwlock_unlock(lock);
    return 0;

fail:
    if (tmp)
        free(tmp);

    pthread_rwlock_unlock(lock);
    return -1;
}

[[gnu::nonnull]]
static int enqueue_one_mds(struct message *msg, struct session *session)
{
    struct message_delivery_state *mds;

    dbg_printf("[%2d] enqueue_one_mds: msg.id=%d", session->id, msg->id);

    /* TODO lock the subscriber? */

    if ((mds = alloc_message_delivery_state(msg, session, false)) == NULL) {
        warn("enqueue_message: alloc_message_delivery_state");
        /* TODO ???? */
        return -1;
    }

    if (add_to_delivery_state(
                &msg->delivery_states,
                &msg->num_message_delivery_states,
                &msg->delivery_states_lock,
                mds) == -1) {
        warn("enqueue_message: add_to_delivery_state(msg)");
        if (mds_detach_and_free(mds, true, true) == -1)
            warn("enqueue_one_mds: mds_detach_and_free");
        mds = NULL;
        return -1;
    }

    if (add_to_delivery_state(
                &session->delivery_states,
                &session->num_message_delivery_states,
                &session->delivery_states_lock,
                mds) == -1) {
        warn("enqueue_message: add_to_delivery_state(session)");
        if (mds_detach_and_free(mds, true, true) == -1)
            warn("enqueue_one_mds: mds_detach_and_free");
        mds = NULL;
        return -1;
    }

    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int enqueue_message(struct topic *topic, struct message *msg)
{
    assert(topic->id);
    assert(msg->id);
    assert(msg->state == MSG_NEW);

    dbg_printf("[%2d] enqueue_message: topic=%d <%s>\n",
            msg->sender ? msg->sender->id : (id_t)-1,
            topic->id, topic->name);

    errno = 0;

    struct subscription *save_sub = global_subscription_list;
    struct subscription *matched_sub;
    bool found = false;

    pthread_rwlock_rdlock(&global_subscriptions_lock);

    while ((matched_sub = find_matching_subscription(topic->name, &save_sub)) != NULL)
    {
        dbg_printf("     enqueue_message: matched sub %s with filter <%s> [save_sub=%p, sub=%p]\n",
                subscription_type_str[matched_sub->type],
                matched_sub->topic_filter, save_sub, matched_sub);

        switch(matched_sub->type)
        {
            case SUB_SHARED:
                for (unsigned idx = 0; idx < matched_sub->shared.num_sessions; idx++) {
                    if (matched_sub->shared.sessions[idx] == NULL)
                        continue;
                    if (matched_sub->shared.sessions[idx]->state != SESSION_ACTIVE)
                        continue;
                    if (matched_sub->shared.sessions[idx]->client == NULL)
                        continue;
                    if (matched_sub->shared.sessions[idx]->client->state != CS_ACTIVE)
                        continue;
                    dbg_printf("     enqueue_message: selected session_id=%d\n",
                            matched_sub->shared.sessions[idx]->id);
                    enqueue_one_mds(msg, matched_sub->shared.sessions[idx]);

                    found = true;
                    break;
                }
                break;

            case SUB_NON_SHARED:
                if (matched_sub->non_shared.session == msg->sender) /* TODO echo y/n ? */
                    continue;

                dbg_printf("     enqueue_message: session=%p\n",
                        matched_sub->non_shared.session);
                enqueue_one_mds(msg, matched_sub->non_shared.session);

                found = true;
                break;

            default:
                logger(LOG_WARNING, NULL,
                        "enqueue_message: unsupported subscription type");
                continue;
        }
    }

    pthread_rwlock_unlock(&global_subscriptions_lock);

    if (found == false) {
        //warnx("enqueue_message: failed to add to subscribers!");
    }

    INC_REFCNT(&topic->refcnt);
    msg->topic = topic;

    pthread_rwlock_wrlock(&topic->pending_queue_lock);
    msg->next_queue = topic->pending_queue;
    topic->pending_queue = msg;
    pthread_rwlock_unlock(&topic->pending_queue_lock);

    return 0;

    /* TODO fail: */
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int dequeue_message(struct message *msg)
{
    errno = 0;

    assert(msg->topic != NULL);

    dbg_printf("     dequeue_message: id=%d\n", msg->id);

    if (msg->topic == NULL) {
        warnx("dequeue_message: attempt to dequeue_message with topic NULL\n");
        errno = EINVAL;
        return -1; /* or 0? TODO */
    }

    if (pthread_rwlock_trywrlock(&msg->topic->pending_queue_lock) == 0) {
        warnx("dequeue_message: pending_queue_lock was unlocked");
        pthread_rwlock_unlock(&msg->topic->pending_queue_lock);
        errno = ENOLCK;
        return -1;
    }

    if (msg == msg->topic->pending_queue) {
        msg->topic->pending_queue = msg->next_queue;
        goto done;
    } else for (struct message *tmp = msg->topic->pending_queue; tmp; tmp = tmp->next_queue) {
        if (tmp->next_queue == msg) {
            tmp->next_queue = msg->next_queue;
            goto done;
        }
    }

    errno = ESRCH;
    return -1;

done:
    msg->next_queue = NULL;
    if (!msg->retain || msg->topic->retained_message != msg) {
        DEC_REFCNT(&msg->topic->refcnt);
        msg->topic = NULL;
    } else {
        dbg_printf("     dequeue_message: id=%d retained message (topic=%d)\n",
                msg->id, msg->topic->id);
    }
    return 0;
}

/**
 * refcnt for non-retained messages should only be touched in enqueue_message
 * or dequeue_message
 */
[[gnu::nonnull]]
static struct message *register_message(const uint8_t *topic_name, int format,
        uint32_t len, const void *payload, unsigned qos, struct session *sender,
        bool retain, message_type_t type)
{
    struct topic *topic;

    topic = NULL;
    errno = 0;

    dbg_printf("[%2d] register_message: topic=<%s> format=%u len=%u qos=%u %spayload=%p\n",
            sender->id, topic_name, format, len, qos,
            retain ? BWHT "retain" CRESET " " : "",
            payload);

    if ((topic = find_or_register_topic(topic_name)) == NULL) {
        warn("register_message: find_or_register_topic");
        goto fail;
    }

    struct message *msg;

    if ((msg = alloc_message(NULL)) == NULL) {
        warn("register_message: alloc_message");
        goto fail;
    }

    msg->type = type;
    msg->format = format;
    msg->payload = payload;
    msg->payload_len = len;
    msg->qos = qos;
    msg->sender = sender;
    msg->state = MSG_NEW;
    msg->retain = retain;

    if (retain) {
        /* TODO retained_message locking ? */
        if (topic->retained_message) {
            DEC_REFCNT(&topic->retained_message->refcnt);
            DEC_REFCNT(&topic->refcnt);
            topic->retained_message->retain = false;
            topic->retained_message->topic = NULL;
            topic->retained_message->state = MSG_DEAD;
            topic->retained_message = NULL;
        }

        /* [MQTT-3.3.1-6] and [MQTT-3.3.1-7] */
        if (msg->payload_len == 0) {
            msg->state = MSG_DEAD;
            return msg;
        }

        INC_REFCNT(&topic->refcnt);
        msg->topic = topic;
        /* if the sender disconnects, boom TODO check this is correct */

        if (qos == 0) {
            msg->sender = NULL;
        } else {
            /* We need to keep sender else we can't ACK/COMP/REC */
            INC_REFCNT(&msg->sender->refcnt); /* send_cp_pubcomp || handle_cp_publish */
        }
        topic->retained_message = msg;
        dbg_printf("     register_message: set retained_message on topic <%s>\n",
                (char *)topic->name);
        INC_REFCNT(&msg->refcnt);
        goto skip_enqueue;
    } else
        INC_REFCNT(&sender->refcnt); /* DEC in free_message() */

    if (enqueue_message(topic, msg) == -1) {
        warn("register_message: enqueue_message");
        free_message(msg, true);
        msg = NULL;
        goto fail;
    }

skip_enqueue:
    msg->state = MSG_ACTIVE;

    /* TODO register the message for delivery and commit */

    return msg;

fail:
    return NULL;
}

/* TODO locking */
[[gnu::nonnull, gnu::warn_unused_result]]
static int unsubscribe(struct subscription *sub, struct session *session)
{
    struct subscription **tmp_topic = NULL;
    struct subscription **tmp_client = NULL;
    size_t client_sub_size, client_sub_cnt = 0;
    unsigned old_idx, new_idx;

    dbg_printf(BWHT "     unsubscribe: sub.id=%d session.id=%d [%s] <%s>" CRESET "\n",
            sub->id, session->id, subscription_type_str[sub->type], sub->topic_filter);

    errno = 0;

    /* remove the back references for this subscription */
    pthread_rwlock_wrlock(&session->subscriptions_lock);
    for (unsigned idx = 0; idx < session->num_subscriptions; idx++)
    {
        if (session->subscriptions[idx] == sub) {
            session->subscriptions[idx] = NULL;
            break;
        }
    }

    /* compact the client list of subscriptions */
    for (unsigned idx = 0; idx < session->num_subscriptions; idx++)
    {
        if (session->subscriptions[idx] == NULL)
            continue;
        client_sub_cnt++;
    }

    client_sub_size = client_sub_cnt * sizeof(struct subscription *);

    if (client_sub_cnt == 0) {
        tmp_client = NULL;
        goto skip_client;
    }

    if ((tmp_client = calloc(1, client_sub_size)) == NULL) {
        pthread_rwlock_unlock(&session->subscriptions_lock);
        goto fail;
    }

    for (old_idx = 0, new_idx = 0; old_idx < session->num_subscriptions; old_idx++)
    {
        if (session->subscriptions[old_idx] == NULL)
            continue;
        tmp_client[new_idx] = session->subscriptions[old_idx];
        new_idx++;
    }

skip_client:

    /* free the old ones and replace */
    if (session->subscriptions) {
        free(session->subscriptions);
        session->subscriptions = NULL;
    }

    session->subscriptions = tmp_client;
    session->num_subscriptions = client_sub_cnt;
    DEC_REFCNT(&session->refcnt); /* alloc_subscription || add_session_to_shared_sub */

    switch (sub->type)
    {
        case SUB_NON_SHARED:
            sub->non_shared.session = NULL;
            free_subscription(sub);
            break;
        case SUB_SHARED:
            if (remove_session_from_shared_sub(sub, session) == -1)
                warn("unsubscribe: remove_session_from_shared_sub");
            if (sub->shared.num_sessions == 0)
                free_subscription(sub);
            break;
        default:
            errno = EINVAL;
            warn("unsubscribe: invalid sub.type");
            break;
    }
    sub = NULL;

    pthread_rwlock_unlock(&session->subscriptions_lock);
    return 0;

fail:
    if (tmp_client)
        free(tmp_client);
    if (tmp_topic)
        free(tmp_topic);

    return -1;
}

[[gnu::nonnull]]
static int unsubscribe_session_from_all(struct session *session)
{
    int rc = 0;

    dbg_printf("[%2d] unsubscribe_session_from_all: num_subs=%u\n",
            session->id, session->num_subscriptions);

    if (session->subscriptions == NULL || session->num_subscriptions == 0)
        return 0;

    /* construct the topic_sub_request */
    const struct topic_sub_request req = {
        .topics = malloc(sizeof(uint8_t *) * session->num_subscriptions),
        .num_topics = session->num_subscriptions,
        .id = (id_t)-1,
    };

    if (req.topics == NULL)
        return -1;

    for (unsigned idx = 0; idx < session->num_subscriptions; idx++) {
        if (session->subscriptions[idx])
            req.topics[idx] = session->subscriptions[idx]->topic_filter;
        else
            req.topics[idx] = NULL;
    }

    if ((rc = unsubscribe_from_topics(session, &req)) == -1)
        warn("free_session: unsubscribe_from_topics");

    free(req.topics);

    return rc;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int unsubscribe_from_topics(struct session *session,
        const struct topic_sub_request *request)
{
    struct subscription *sub;
    errno = 0;

    dbg_printf("[%2d] unsubscribe_from_topics: num_topics=%d\n",
            session->id, request->num_topics);

    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        if (request->topics[idx] == NULL)
            continue;

        if ((sub = find_subscription(session, request->topics[idx])) == NULL) {
            if (request->reason_codes) {
                if (errno == ENOENT)
                    request->reason_codes[idx] = MQTT_NO_SUBSCRIPTION_EXISTED;
                else
                    request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
            }
            continue;
        }

        switch (sub->type)
        {
            case SUB_SHARED:
                if (unsubscribe(sub, session) == -1) {
                    if (request->reason_codes)
                        request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
                    continue;
                }
#if 0
                if (request->reason_codes)
                    request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
                warn("unsubscribe_from_topics: SUB_SHARED not implemented");
#endif
                continue;

            case SUB_NON_SHARED:
                if (unsubscribe(sub, session) == -1) {
                    if (request->reason_codes)
                        request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
                    continue;
                }
                break;

            default:
                warn("unsubscribe_from_topics: unknown subscription type");
                break;
        }

        if (request->reason_codes)
            request->reason_codes[idx] = MQTT_SUCCESS;
    }
    return 0;
}

/**
 * caller must hold session->subscriptions_lock
 */
[[gnu::nonnull]]
static int subscribe_to_one_topic(struct session *session,
        uint8_t *reason_code,
        bool *added,
        const uint8_t *topic_filter,
        uint32_t subscription_identifier,
        uint8_t options,
        unsigned sub_idx)
{
    struct subscription *existing_sub = NULL;
    struct subscription *new_sub = NULL;
    const uint8_t *ptr = NULL;
    const uint8_t *share_name = NULL;
    subscription_type_t type = SUB_NON_SHARED;

    errno = 0;
    *added = false;

    if (*reason_code > MQTT_GRANTED_QOS_2) {
        dbg_printf("[%d] subscribe_to_topics: reason code is %u\n",
                session->id, *reason_code);
        goto done;
    }

    dbg_printf("[%2d] subscribe_to_topics: subscribing to <%s>\n",
            session->id, (const char *)topic_filter);

    if (is_shared_subscription(topic_filter)) {
        ptr = topic_filter + SHARED_PREFIX_LENGTH;

        while (*ptr && *ptr != '/')
            ptr++;

        /* need to check the spec if "$shared/share" without the
         * trailing /, should be considered a normal filter or not
         */
        if (*ptr == '\0')
            goto not_shared;

        ptr++;

        if (is_valid_topic_filter(ptr) == -1) {
            *reason_code = MQTT_TOPIC_FILTER_INVALID;
            goto done;
        }

        if ((share_name = (void *)strdup((const char *)ptr)) == NULL)
            goto fail;

        type = SUB_SHARED;
not_shared:
    }

    existing_sub = find_subscription(session, topic_filter);

    /* no existing subscription, create one */
    if ((existing_sub == NULL) && errno == ENOENT) {

        if ((new_sub = alloc_subscription(session, type, topic_filter,
                        subscription_identifier)) == NULL)
            goto fail;

        switch (type)
        {
            case SUB_SHARED:
                new_sub->shared.share_name = share_name;
                share_name = NULL;
                break;

            default:
                break;
        }

        session->subscriptions[sub_idx] = new_sub;
        //session->num_subscriptions++;
        *added = true;
        existing_sub = new_sub;

        goto force_existing;

    } else if (existing_sub == NULL) {
        /* error inside find_subscription, fail */

        warn("subscribe_to_topics: find_subscription");
        *reason_code = MQTT_UNSPECIFIED_ERROR;
        goto done;
    }
    /* else: Update the existing subscription's options (e.g. QoS) */

    dbg_printf("[%2d] subscribe_to_topics: updating existing subscription\n",
            session->id);

force_existing:
    /* At this point existing_sub = new_sub|existing_sub */

    /* TODO what if non-QoS options have changed ? */
    switch(type)
    {
        case SUB_NON_SHARED:
            existing_sub->option = options; /* TODO move this to sub->non_shared? */
            break;

        case SUB_SHARED:
            if (add_session_to_shared_sub(existing_sub, session,
                    (options & MQTT_SUBOPT_QOS_MASK)) == -1) {

                /* We were just doing an update. */
                if (errno == EEXIST)
                    break;

                *reason_code = MQTT_UNSPECIFIED_ERROR;
                goto done;
            }

            session->subscriptions[sub_idx] = existing_sub;
            session->num_subscriptions++;
            break;

        default:
            logger(LOG_WARNING, NULL, "subscribe_to_topics: invalid type");
            *reason_code = MQTT_UNSPECIFIED_ERROR;
            goto done;
    }

done:
    if (share_name)
        free((void *)share_name);

    return 0;

fail:
    if (share_name)
        free((void *)share_name);

    return -1;
}

/**
 * request should be freed by the caller using free_topic_subs()
 */
[[gnu::nonnull, gnu::warn_unused_result]]
static int subscribe_to_topics(struct session *session,
        struct topic_sub_request *request)
{
    struct subscription **tmp_subs = NULL;
    size_t sub_size;
    unsigned base, added = 0;

    errno = 0;

    pthread_rwlock_wrlock(&session->subscriptions_lock);

    /* Grow the session subscription array by the number of new subscriptions */
    /* TODO if any of the requests are _updates_ this leaves holes in the
     * subscriptions[] array of the session */
    //sub_size = sizeof(struct subscription *) * (session->num_subscriptions +
    //        request->num_topics);

    base = session->num_subscriptions;
    sub_size = sizeof(struct subscription *) * (base + request->num_topics);

    if ((tmp_subs = realloc(session->subscriptions, sub_size)) == NULL) {
        /* Ensure we acknowledged our failures */
        for (unsigned idx = 0; idx < request->num_topics; idx++)
            request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;

        goto fail;
    }
    session->subscriptions = tmp_subs;

    /* Iterate over each requested subscription */
    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        session->subscriptions[session->num_subscriptions + idx] = NULL;

        /* TODO: this will result in 'gaps' in session->subscriptions[] */
        if (request->topics[idx] == NULL) {
            //session->subscriptions[session->num_subscriptions + idx] = NULL;
            session->subscriptions[base + idx] = NULL;
            continue;
        }

        bool did_add = false;
        if (subscribe_to_one_topic(session, &request->reason_codes[idx],
                    &did_add,
                    request->topics[idx],
                    request->subscription_identifier,
                    request->options[idx],
                    session->num_subscriptions + idx) == -1)
            goto fail;

        if (did_add)
            added++;
    }

    session->num_subscriptions = base + added;
    dbg_printf("[%2d] subscribe_to_topics: num_subscriptions now %u [+%u/%u]\n",
            session->id, session->num_subscriptions, added, request->num_topics);

    pthread_rwlock_unlock(&session->subscriptions_lock);
    return 0;

fail:
    /* TODO set all reason_codes[] to something ? */
    pthread_rwlock_unlock(&session->subscriptions_lock);
    return -1;
}

[[gnu::nonnull]]
/* TODO the send_quota logic needs to adjust based on direction
 * of message */
static int mark_one_mds(struct message_delivery_state *mds,
        control_packet_t type, reason_code_t client_reason,
        struct client *client, bool is_sender)
{
    assert(mds->packet_identifier != 0);

    const time_t now = time(NULL);

    switch (type)
    {
        case MQTT_CP_PUBACK: /* QoS=1 */
            if (mds->acknowledged_at)
                warnx("mark_message: duplicate acknowledgment");
            mds->acknowledged_at = now;
            mds->released_at = now;
            mds->completed_at = now;
            mds->client_reason = client_reason;
            if (client->send_quota < MAX_RECEIVE_PUBS)
                client->send_quota++;
            if (is_sender)
                total_messages_sender_acknowledged_at++;
            else
                total_messages_acknowledged_at++;
            break;

        case MQTT_CP_PUBREC: /* QoS=2 */
            if (mds->acknowledged_at)
                warnx("mark_message: duplicate acknowledgment");
            mds->acknowledged_at = now;

            if (is_sender)
                total_messages_sender_acknowledged_at++;
            else
                total_messages_acknowledged_at++;

            mds->client_reason = client_reason;
            if (client->send_quota < MAX_RECEIVE_PUBS && client_reason >= 0x80)
                client->send_quota++;
            break;

        case MQTT_CP_PUBREL: /* QoS=2 */
            if (mds->released_at)
                warnx("mark_message: duplicate release");
            mds->released_at = now;
            if (is_sender)
                total_messages_sender_released_at++;
            else
                total_messages_released_at++;
            break;

        case MQTT_CP_PUBCOMP: /* QoS=2 */
            if (mds->completed_at)
                warnx("mark_message: duplicate completed");
            mds->completed_at = now;
            if (client->send_quota < MAX_RECEIVE_PUBS)
                client->send_quota++;
            if (is_sender)
                total_messages_sender_completed_at++;
            else
                total_messages_completed_at++;
            break;

        default:
            warnx("mark_message: called with illegal type %s",
                    control_packet_str[type]);
            errno = EINVAL;
            goto fail;
    }

    return 1;

fail:
    return -1;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int mark_message(control_packet_t type, uint16_t packet_identifier,
        reason_code_t client_reason, struct client *client, role_t role)
{
    int rc;
    struct message_delivery_state *mds;
    struct session *session;

    assert(packet_identifier != 0);

    session = client->session;

    if (role == ROLE_RECV)
        goto do_recv;

    pthread_rwlock_wrlock(&global_messages_lock);
    for (struct message *msg = global_message_list; msg; msg = msg->next)
    {
        if (msg->sender != session)
            continue;

        if (msg->sender_status.packet_identifier == packet_identifier) {
            rc = mark_one_mds(&msg->sender_status, type, client_reason, client, true);

            if (rc == -1)
                goto fail;

            pthread_rwlock_unlock(&global_messages_lock);
            return 0;
        }
    }
    pthread_rwlock_unlock(&global_messages_lock);

    errno = ENOENT;
    goto fail;

    /* else if (role == ROLE_RECV) ... */

do_recv:
    pthread_rwlock_wrlock(&session->delivery_states_lock);
    for (unsigned idx = 0; idx < session->num_message_delivery_states; idx++)
    {
        mds = session->delivery_states[idx];

        if (mds == NULL)
            continue;

        if (mds->packet_identifier != packet_identifier)
            continue;

        rc = mark_one_mds(mds, type, client_reason, client, false);

        if (rc == -1)
            goto fail;
        else if (rc == 0)
            continue;

        pthread_rwlock_unlock(&session->delivery_states_lock);
        return 0;
    }

    errno = ENOENT;
fail:
    pthread_rwlock_unlock(&session->delivery_states_lock);
    return -1;
}

/*
 * control packet response functions
 */

/* Fixed Header:
 *  MQTT Control Packet Type [4:7]
 *  DUP flag [3]
 *  QoS [1:2]
 *  RETAIN [0]
 *  Remaining Length (VAR)
 * Variable Header:
 *  Topic Name
 *  Packet Identifier {if QoS > 0}
 *  Properties[]
 * Payload
 */

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_publish(struct packet *pkt)
{
    ssize_t length;//, wr_len;
    uint8_t *packet, *ptr;
    uint8_t proplen[4], remlen[4];
    int proplen_len, remlen_len, prop_len;
    uint16_t tmp, topic_len;
    const struct message *msg;
    reason_code_t reason_code = MQTT_SUCCESS;

    errno = 0;

    assert(pkt->message != NULL);
    assert(pkt->message->topic != NULL);
    assert(pkt->owner != NULL);

    if (pkt->owner->state != CS_ACTIVE) {
        errno = EBADF;
        return -1;
    }

    dbg_printf("[%2d] send_cp_publish: owner=<%s>\n",
            pkt->owner->session->id, (char *)pkt->owner->client_id);

    packet = NULL;
    msg = pkt->message;

    /* Populate Properties */
    const struct property props[] = {
        { 0 },
    };
    const unsigned num_props = 0; /* sizeof(props) / sizeof(struct property) */

    /* Calculate Property[] Length */
    if ((prop_len = get_properties_size(&props, num_props)) == -1)
        goto fail;

    /* Calculate the length of Property Length */
    if ((proplen_len = encode_var_byte(prop_len, proplen)) == -1)
        goto fail;

    length = 0;
    length += sizeof(uint16_t); /* UTF-8 length */
    length += (topic_len = strlen((char *)pkt->message->topic->name)); /* Actual String */

    if ((pkt->flags & MQTT_FLAG_PUBLISH_QOS_MASK))
        length += sizeof(uint16_t); /* packet identifier */

    length += proplen_len;
    length += prop_len;
    length += msg->payload_len;

    /* Remaining Length excludes the fixed header */
    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        goto fail;

    /* Calculate the total length including header */
    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len;

    /* Now build the packet */
    if ((ptr = packet = calloc(1, length)) == NULL)
        goto fail;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBLISH;
    ((struct mqtt_fixed_header *)ptr)->flags = pkt->flags;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(topic_len);
    memcpy(ptr, &tmp, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    memcpy(ptr, msg->topic->name, topic_len);
    ptr += topic_len;

    if (pkt->flags & MQTT_FLAG_PUBLISH_QOS_MASK) {
        tmp = htons(pkt->packet_identifier); /* TODO proper packet identifier */
        memcpy(ptr, &tmp, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
    }

    memcpy(ptr, proplen, proplen_len);
    ptr += proplen_len;

    if (build_properties(&props, num_props, &ptr) == -1)
        goto fail;

    memcpy(ptr, msg->payload, msg->payload_len);

    /* [MQTT-3.1.2-25] */
    if (pkt->owner->maximum_packet_size && length > pkt->owner->maximum_packet_size)
        goto skip_write;

    /* [MQTT-4.9.0-2] */
    if (msg->qos && --pkt->owner->send_quota == 0) {
        errno = EDQUOT;
        reason_code = MQTT_RECEIVE_MAXIMUM_EXCEEDED;
        goto fail;
    }

    /* We lock here to avoid another thread picking up half way inside
     * set_outbound() and clashing with handle_outbound() */
    pthread_rwlock_wrlock(&pkt->owner->po_lock);
    set_outbound(pkt->owner, packet, length);
    handle_outbound(pkt->owner);
    pthread_rwlock_unlock(&pkt->owner->po_lock);
    dbg_printf("     send_cp_publish: <" NRED "PUBLISH" CRESET "> done\n");
    return 0;

skip_write:
    dbg_printf("      send_cp_publish: <" NRED "PUBLISH" CRESET "> done\n");
    free(packet);
    return 0;

fail:
    if (packet)
        free(packet);

    if (disconnect_if_malformed(pkt->owner, reason_code))
        return -1;

    return -1;
}

[[gnu::nonnull]]
static int send_cp_disconnect(struct client *client, reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    uint8_t remlen[4]; int remlen_len;

    errno = 0;

    length = 0;
    length += 1; /* Disconnect Reason Code */
    length += 1; /* Property Length */

    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        return -1;

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len; /* Remaining Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_DISCONNECT;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* Property Length */

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    dbg_printf("[%2d] send_cp_disconnect: <"NRED"DISCONNECT"CRESET"> sent code was %u\n",
            client->session ? client->session->id : (id_t)-1, reason_code);

    free(packet);
    client->send_disconnect = false;
    close_socket(&client->fd);
#if 0
    client->state = CS_CLOSING;
    client->disconnect_reason = 0;
#endif

    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_pingresp(struct client *client)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    errno = 0;

    length = sizeof(struct mqtt_fixed_header);
    length += 1; /* remaining length 1 byte */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PINGRESP;
    ptr++;

    *ptr = 0; /* Remaining Length */
    ptr++;

    dbg_printf("[%2d] send_cp_pingresp: sending\n", client->session->id);

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    /* last_keep_alive is updated in parse_incoming after
     * any successful control packets */
    free(packet);

    dbg_printf("[%2d] send_cp_pingack: <" NRED "PINGACK" CRESET "> done\n",
            client->session->id);
    return 0;
}

static const struct property connack_props[] = {
        { .ident = MQTT_PROP_MAXIMUM_PACKET_SIZE               , .byte4 = MAX_PACKET_LENGTH } ,
        { .ident = MQTT_PROP_RECEIVE_MAXIMUM                   , .byte2 = MAX_RECEIVE_PUBS  } ,
        { .ident = MQTT_PROP_RETAIN_AVAILABLE                  , .byte  = 1                 } ,
        { .ident = MQTT_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE   , .byte  = 1                 } ,
        { .ident = MQTT_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE , .byte  = 1                 } ,
        { .ident = MQTT_PROP_SHARED_SUBSCRIPTION_AVAILABLE     , .byte  = 1                 } ,
        { .ident = MQTT_PROP_TOPIC_ALIAS_MAXIMUM               , .byte  = MAX_TOPIC_ALIAS   } ,
    };
static const unsigned num_connack_props = sizeof(connack_props) / sizeof(struct property);

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_connack(struct client *client, reason_code_t reason_code)
{
    /* Populate Properties */

    ssize_t length, wr_len;
    uint8_t *packet = NULL, *ptr = NULL;

    uint8_t proplen[4], remlen[4];
    int proplen_len, remlen_len, prop_len;

    struct property (*tmp_connack_props)[] = NULL;
    unsigned num_tmp_connack_props = 0;

    errno = 0;

    dbg_printf("     send_cp_connack: %s to client <%s>\n",
            reason_codes_str[reason_code], (const char *)client->client_id);

    if (reason_code < MQTT_MALFORMED_PACKET) {
        num_tmp_connack_props = num_connack_props;
        if (client->keep_alive_override)
            num_tmp_connack_props++;

        if ((tmp_connack_props = calloc(num_tmp_connack_props, sizeof(struct property))) == NULL)
            return -1;

        for (unsigned idx = 0; idx < num_connack_props; idx++)
            memcpy(&(*tmp_connack_props)[idx], &connack_props[idx], sizeof(struct property));

        if (client->keep_alive_override) {
            (*tmp_connack_props)[num_connack_props].byte2 = client->keep_alive;
            (*tmp_connack_props)[num_connack_props].ident = MQTT_PROP_SERVER_KEEP_ALIVE;
        }
#ifdef FEATURE_RAFT
    } else if (reason_code == MQTT_USE_ANOTHER_SERVER) {

        char tmpbuf[INET_ADDRSTRLEN + 1 + 5 + 1];

        if (raft_get_leader_address(tmpbuf, sizeof(tmpbuf)) != -1) {
            num_tmp_connack_props = 1;

            if ((tmp_connack_props = calloc(num_tmp_connack_props, sizeof(struct property))) == NULL)
                return -1;

# ifdef RAFT_DEBUG
            dbg_printf("     send_cp_connack: setting MQTT_PROP_SERVER_REFERENCE to <%s>\n", tmpbuf);
# endif

            (*tmp_connack_props)[0].utf8_string = (void *)strdup(tmpbuf);
            (*tmp_connack_props)[0].ident = MQTT_PROP_SERVER_REFERENCE;
        }
#endif
    }

    if (num_tmp_connack_props) {
        /* Calculate the Property[] Length */
        if ((prop_len = get_properties_size(tmp_connack_props, num_tmp_connack_props)) == -1)
            return -1;
    } else {
        prop_len = 0;
    }

    /* Calculate the length of Property Length */
    if ((proplen_len = encode_var_byte(prop_len, proplen)) == -1)
        return -1;

    length = 0;
    length += 1+1;           /* connack var header (1byte for flags, 1byte for code) */
    length += proplen_len; /* properties length (0) */
    length += prop_len;    /* property[] */

    /* Calculate the length of Remaining Length */
    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        return -1;

    /* Calculate the total length including header */
    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len;  /* Remaining Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    dbg_printf("     send_cp_connack: allocated %lub\n", length);

    /* Now build the packet */
    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_CONNACK;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    *ptr = 0;           /* Connect Ack Flags */
    ptr++;

    *ptr = reason_code; /* Connect Reason Code */
    ptr++;

    memcpy(ptr, proplen, proplen_len);
    ptr += proplen_len;

    if (num_tmp_connack_props)
        if (build_properties(tmp_connack_props, num_tmp_connack_props, &ptr) == -1)
            goto fail;

    /* Now send the packet */
    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    free(packet);
    free_properties(tmp_connack_props, num_tmp_connack_props);

#if 0
    /* TODO check caller has called close_socket() correctly */
    if (is_malformed(reason_code)) {
        client->disconnect_reason = reason_code;
        client->state = CS_CLOSING;
    }
#endif

    dbg_printf("[%2d] send_cp_connack: <" NRED "CONNACK" CRESET "> done\n",
            client->session->id);
    return 0;

fail:
    if (packet)
        free(packet);

    if (tmp_connack_props)
        free_properties(tmp_connack_props, num_tmp_connack_props);

    if (is_malformed(reason_code))
        client->state = CS_CLOSING;

    return -1;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_pubrec(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;
    uint8_t remlen[4]; int remlen_len;

    errno = 0;

    dbg_printf("[%2d] send_cp_pubrec: packet_id=%u reason=%s\n",
            client->session->id, packet_id, reason_codes_str[reason_code]);

    length = 0;
    length += 3; /* Packet Identifier + Reason Code */
    length += 1; /* Properties Length (0) */

    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        return -1;

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len; /* Remaining Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)packet)->type = MQTT_CP_PUBREC;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* Property Length */
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    free(packet);

    dbg_printf("[%2d] send_cp_pubrec: <" NRED "PUBREC" CRESET "> done\n",
            client->session->id);

    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_pubcomp(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;
    uint8_t remlen[4]; int remlen_len;

    errno = 0;
    dbg_printf("[%2d] send_cp_pubcomp: packet_id=%u reason_code=%d <%s> client.id=%d\n",
            client->session ? client->session->id : (id_t)-1, packet_id, reason_code,
            reason_codes_str[reason_code], client->id);

    length = 0;
    length +=2; /* Packet Identifier */

    if (reason_code > 0) {
        length +=1; /* Reason Code */
        length +=1; /* Properties Length */
    }

    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        return -1;

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len; /* Remaining Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBCOMP;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    if (reason_code > 0) {
    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* No properties */
    ptr++;
    }

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    const time_t now = time(NULL);

    pthread_rwlock_wrlock(&client->session->delivery_states_lock);
    for (unsigned idx = 0; idx < client->session->num_message_delivery_states; idx++)
    {
        struct message_delivery_state *mds = client->session->delivery_states[idx];

        if (mds == NULL)
            continue;

        if (mds->session == NULL && mds->message == NULL)
            continue;

        if (mds->packet_identifier == packet_id) {
            mds->completed_at = now;
            break;
        }

        /* TODO check the logic here is right, that packet_id != packet_id */

        if (mds->message->retain && mds->message->sender) {
            /* Now we've COMP, we can forget the sender */
            DEC_REFCNT(&mds->message->sender->refcnt);
            mds->message->sender = NULL;
        }
    }

    pthread_rwlock_wrlock(&global_messages_lock);
    for (struct message *msg = global_message_list; msg; msg = msg->next)
    {
        if (msg->state != MSG_ACTIVE)
            continue;

        if (msg->sender_status.packet_identifier != packet_id)
            continue;

        if (msg->sender == NULL || msg->sender != client->session)
            continue;

        msg->sender_status.completed_at = now;
        break;
    }
    pthread_rwlock_unlock(&global_messages_lock);
    pthread_rwlock_unlock(&client->session->delivery_states_lock);

    free(packet);

    dbg_printf("[%2d] send_cp_pubcomp: <" NRED "PUBCOMP" CRESET "> done\n",
            client->session->id);
    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_pubrel(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;
    uint8_t remlen[4]; int remlen_len = 0;

    dbg_printf("[%2d] send_cp_pubrel: packet_id=%u reason_code=%d <%s> client.id=%d\n",
            client->session ? client->session->id : (id_t)-1, packet_id,
            reason_code, reason_codes_str[reason_code], client->id);

    errno = 0;

    length = 0;
    length += sizeof(uint16_t); /* Packet Identifier */

    if (reason_code > 0) {
        length += 1; /* Reason Code */
        length += 1; /* Properties Length */
    }
    remlen_len = encode_var_byte(length, remlen);

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len; /* Remaining Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBREL;
    ((struct mqtt_fixed_header *)ptr)->flags = MQTT_FLAG_PUBREL;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    if (reason_code > 0) {
        *ptr = reason_code;
        ptr++;

        *ptr = 0; /* Properties Length */
        ptr++;
    }

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    /* TODO update status thing */

    free(packet);
    dbg_printf("[%2d] send_cp_pubrel: <" NRED "PUBREL" CRESET "> done\n",
            client->session->id);
    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_puback(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;
    uint8_t remlen[4]; int remlen_len;

    dbg_printf("[%2d] send_cp_puback: packet_id=%u reason_code=%d <%s> client.id=%d\n",
            client->session ? client->session->id : (id_t)-1, packet_id,
            reason_code, reason_codes_str[reason_code], client->id);
    errno = 0;

    length = 0;
    length += 3; /* Packet Identifier + Reason Code */
    length += 1; /* Properties Length */

    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        return -1;

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len; /* Remaining Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBACK;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    *ptr = reason_code;
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    free(packet);

    dbg_printf("[%2d] send_cp_puback: <" NRED "PUBACK" CRESET "> done\n",
            client->session->id);
    return 0;
}

/* Fixed Header:
 *  MQTT Control Packet Type [4:7]
 *  Reserved [0-3]
 *  Remaining Length (VAR)
 * Variable Header:
 *  Packet Identifier
 *  Properties[]
 * Payload:
 *  Reason Code[]
 */

[[gnu::nonnull]]
static int send_cp_unsuback(struct client *client, uint16_t packet_id,
        struct topic_sub_request *request)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;
    uint8_t remlen[4];
    int remlen_len;

    packet = NULL;

    if (packet_id == 0) {
        errno = EINVAL;
        goto fail;
    }

    errno = 0;

    length = sizeof(packet_id);
    length += 1; /* Property Length */

    length += request->num_topics;

    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        goto fail;

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len;

    if ((ptr = packet = calloc(1, length)) == NULL)
        goto fail;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_UNSUBACK;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    *ptr = 0;
    ptr++;

    for (unsigned idx = 0; idx < request->num_topics; idx++) {
        *ptr = request->reason_codes[idx];
        if (disconnect_if_malformed(client, (reason_code_t)*ptr))
            goto fail;
        ptr++;
    }

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    free(packet);
    pthread_rwlock_wrlock(&client->active_packets_lock);
    for (struct packet *tmp = client->active_packets; tmp; tmp = tmp->next_client)
    {
        if (tmp->packet_identifier == packet_id &&
                GET_REFCNT(&tmp->refcnt) > 0) {
            DEC_REFCNT(&tmp->refcnt);
            break;
        }

    }
    pthread_rwlock_unlock(&client->active_packets_lock);

    return 0;

fail:
    if (packet)
        free(packet);
    return -1;
}

/* Fixed Header:
 *  MQTT Control Packet type [4:7]
 *  Remaining Length (VAR)
 * Variable Header:
 *  Packet Identifier
 *  Properties[]
 * Payload:
 *  Reason Code[]
 */

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_suback(struct client *client, uint16_t packet_id,
        struct topic_sub_request *request)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;
    uint8_t remlen[4];
    int remlen_len;

    packet = NULL;

    if (packet_id == 0) {
        errno = EINVAL;
        goto fail;
    }

    errno = 0;

    length = sizeof(packet_id); /* [2-3] Packet Identifier */
    length += 1; /* [4]   properties length (0) */
    length += request->num_topics; /* [5+] */

    if ((remlen_len = encode_var_byte(length, remlen)) == -1)
        goto fail;

    length += sizeof(struct mqtt_fixed_header); /* [0] MQTT Control Packet type */
    length += remlen_len; /* [1]   Remaining Length 1byte */

    if ((ptr = packet = calloc(1, length)) == NULL)
        goto fail;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_SUBACK;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    *ptr = 0; /* properties length */
    ptr++;

    for (unsigned i = 0; i < request->num_topics; i++) {
        *ptr = request->reason_codes[i]; /* TODO which QoS? */
        if (disconnect_if_malformed(client, (reason_code_t)*ptr))
            goto fail;
        ptr++;
    }

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false, client);
    }

    free(packet);

    pthread_rwlock_wrlock(&client->active_packets_lock);
    for (struct packet *tmp = client->active_packets; tmp; tmp = tmp->next_client)
    {
        if (tmp->packet_identifier == packet_id &&
                GET_REFCNT(&tmp->refcnt) > 0) {
            DEC_REFCNT(&tmp->refcnt);
            break;
        }

    }
    pthread_rwlock_unlock(&client->active_packets_lock);

    return 0;

fail:
    if (packet)
        free(packet);
    return -1;
}

/*
 * control packet processing functions
 */

[[gnu::nonnull]]
static int handle_cp_pubrel(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    reason_code_t pubrel_reason_code = 0;
    uint16_t tmp;

    errno = 0;

    if (packet->flags != MQTT_FLAG_PUBREL)
        goto fail;

    if (bytes_left < sizeof(uint16_t))
        goto fail;

    memcpy(&tmp, ptr, sizeof(uint16_t));
    packet->packet_identifier = ntohs(tmp);
    ptr += sizeof(uint16_t);
    bytes_left -= sizeof(uint16_t);

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (bytes_left == 0) {
        pubrel_reason_code = MQTT_SUCCESS;
        goto skip_props;
    }

    pubrel_reason_code = *ptr;
    ptr++;
    bytes_left--;

    if (bytes_left == 0)
        goto skip_props;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBREL) == -1) {
        goto fail;
    }
skip_props:
    if (bytes_left)
        goto fail;

    dbg_printf("[%2d] handle_cp_pubrel: packet_identifier=%u reason_code=%u\n",
            client->session->id,
            packet->packet_identifier, pubrel_reason_code);

    if (mark_message(MQTT_CP_PUBREL, packet->packet_identifier,
                pubrel_reason_code, client, ROLE_SEND) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    } else
        reason_code = MQTT_SUCCESS;

    if (send_cp_pubcomp(client, packet->packet_identifier, reason_code) == -1) {
        warn("handle_cp_pubrel: send_cp_pubcomp");
        goto fail;
    }

    dbg_printf("[%2d] handle_cp_pubrel: done: %s\n", client->session->id, reason_codes_str[reason_code]);
    return 0;

fail:
    dbg_printf("[%2d] handle_cp_pubrel: fail: %s\n", client->session->id, reason_codes_str[reason_code]);
    return disconnect_if_malformed(client, reason_code);
}

[[gnu::nonnull]]
static int handle_cp_puback(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET, puback_reason_code;

    errno = 0;

    if (bytes_left < sizeof(uint16_t))
        goto fail;

    memcpy(&packet->packet_identifier, ptr, sizeof(uint16_t));
    bytes_left -= sizeof(uint16_t);
    ptr += sizeof(uint16_t);
    packet->packet_identifier = ntohs(packet->packet_identifier);

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (bytes_left == 0) {
        puback_reason_code = MQTT_SUCCESS;
        goto skip_reason;
    }

    puback_reason_code = *ptr;
    ptr++;
    bytes_left--;

skip_reason:

    if (bytes_left == 0)
        goto skip_property_length;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBACK))
        goto fail;

skip_property_length:
    /* TODO record acknowledgment */

    dbg_printf("[%2d] handle_cp_puback: client=%d <%s> reason_code=%u packet_identifier=%u\n",
            client->session->id,
            client->id, (char *)client->client_id,
            puback_reason_code, packet->packet_identifier);

    if (mark_message(MQTT_CP_PUBACK, packet->packet_identifier,
                puback_reason_code, client, ROLE_RECV) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    return 0;

fail:
    disconnect_if_malformed(client, reason_code);
    /* TODO what if the reason_code is >0x80 but not malformed? */
    return -1;
}

[[gnu::nonnull]]
static int handle_cp_pubcomp(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    reason_code_t pubcomp_reason_code;

    errno = 0;

    if (bytes_left < sizeof(uint16_t))
        goto fail;

    memcpy(&packet->packet_identifier, ptr, sizeof(uint16_t));
    bytes_left -= sizeof(uint16_t);
    ptr += sizeof(uint16_t);
    packet->packet_identifier = ntohs(packet->packet_identifier);

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (bytes_left == 0) {
        pubcomp_reason_code = MQTT_SUCCESS;
        goto skip_props;
    }

    pubcomp_reason_code = *ptr;
    ptr++;
    bytes_left--;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBCOMP) == -1)
        goto fail;

    if (bytes_left)
        goto fail;

skip_props:
    dbg_printf("[%2d] handle_cp_pubcomp: packet_identifier=%u reason_code=%u\n",
            client->session->id,
            packet->packet_identifier,
            pubcomp_reason_code);

    if (mark_message(MQTT_CP_PUBCOMP, packet->packet_identifier,
                pubcomp_reason_code, client, ROLE_RECV) == -1) {
        if (errno == ENOENT)
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
        else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    return 0;

fail:
    disconnect_if_malformed(client, reason_code);
    /* TODO what if reason_code is not malformed ? */
    return -1;
}

[[gnu::nonnull]]
static int handle_cp_pubrec(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    reason_code_t pubrec_reason_code;
    uint16_t packet_identifier;

    errno = 0;

    if (bytes_left < sizeof(uint16_t))
        goto fail;

    memcpy(&packet_identifier, ptr, sizeof(uint16_t));
    bytes_left -= sizeof(uint16_t);
    ptr += sizeof(uint16_t);
    packet_identifier = ntohs(packet_identifier);

    if (packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    /* The Reason Code and Property Length can be omitted if the Reason Code
     * is 0x00 (Success) and there are no Properties. */
    if (bytes_left == 0) {
        pubrec_reason_code = MQTT_SUCCESS;
        goto skip_props;
    }

    pubrec_reason_code = *ptr;
    ptr++;
    bytes_left--;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBREC) == -1)
        goto fail;

skip_props:
    if (bytes_left)
        goto fail;

    dbg_printf("[%2d] handle_cp_pubrec: packet_identifier=%u\n",
            client->session->id, packet_identifier);

    if (mark_message(MQTT_CP_PUBREC, packet_identifier, pubrec_reason_code,
                client, ROLE_RECV) == -1) {
        if (errno == ENOENT) {
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
            goto normal; /* PUBREL only supports this error */
        } else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    /* TODO what if the above succeeds, but the below fails? */

    if (mark_message(MQTT_CP_PUBREL, packet_identifier, MQTT_SUCCESS,
                client, ROLE_RECV) == -1) {
        if (errno == ENOENT) {
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
            goto normal; /* PUBREL only supports this error */
        } else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    reason_code = MQTT_SUCCESS;

normal:
    if (send_cp_pubrel(client, packet_identifier, reason_code) == -1)
        goto fail;

    return 0;

fail:
    return disconnect_if_malformed(client, reason_code);
}

[[gnu::nonnull]]
static int handle_cp_publish(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    uint8_t *topic_name = NULL;
    uint16_t packet_identifier = 0;
    uint32_t subscription_identifier = 0;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    unsigned qos = 0;
    const struct property *prop = NULL;
    bool flag_retain = false;
    size_t topic_name_length = 0;
    uint16_t topic_alias = 0;
    [[maybe_unused]] bool flag_dup; /* TODO use this somehow! */

    errno = 0;

    if ((topic_name = read_utf8(&ptr, &bytes_left)) == NULL)
        goto fail;

    topic_name_length = strlen((const char *)topic_name);

    if (topic_name_length && is_valid_topic_name(topic_name) == -1) {
        reason_code = MQTT_TOPIC_NAME_INVALID;
        goto fail;
    } else if (topic_name_length == 0) {
        free((void *)topic_name);
        topic_name = NULL;
    }

    dbg_printf("[%2d] handle_cp_publish: topic=<%s> ",
            client->session->id, topic_name);

    qos = GET_QOS(packet->flags); // & (1<<1|1<<2)) >> 1;
    flag_retain = (packet->flags & MQTT_FLAG_PUBLISH_RETAIN) != 0;
    flag_dup = (packet->flags & MQTT_FLAG_PUBLISH_DUP) != 0;

    dbg_cprintf("qos=%u ", qos);

    if (qos > 2) {
        reason_code = MQTT_PROTOCOL_ERROR;
        warn("handle_cp_publish: invalid QoS value");
        goto fail;
    }

    if (qos) {
        if (bytes_left < sizeof(uint16_t)) {
            reason_code = MQTT_MALFORMED_PACKET;
            goto fail;
        }

        memcpy(&packet_identifier, ptr, sizeof(uint16_t));
        packet_identifier = ntohs(packet_identifier);
        ptr += sizeof(uint16_t);
        bytes_left -= sizeof(uint16_t);

        if (packet_identifier == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            goto fail;
        }
        dbg_cprintf("packet_ident=%u ", packet_identifier);
    }

    uint8_t payload_format = 0; /* TODO extract from properties */

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBLISH) == -1)
        goto fail;
    dbg_cprintf("payload_format=%u [%lub] ", payload_format, bytes_left);

    packet->payload_len = bytes_left;
    if ((packet->payload = malloc(bytes_left)) == NULL)
        goto fail;
    memcpy(packet->payload, ptr, bytes_left);
    dbg_cprintf("payload_len=%u ", packet->payload_len);

    dbg_cprintf("\n");

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_SUBSCRIPTION_IDENTIFIER, &prop) == 0) {
        subscription_identifier = prop->byte4;

        if (subscription_identifier > MAX_SUB_IDENTIFIER ||
                subscription_identifier == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            goto fail;
        }
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_TOPIC_ALIAS, &prop) == 0) {

        /* Server -> Client so clnt_topic_aliases */

        if (prop->byte2 == 0) {
            dbg_printf("     handle_cp_publish: topic_alias is 0\n");
            reason_code = MQTT_TOPIC_ALIAS_INVALID;
            goto fail;
        }

        if (prop->byte2 >= MAX_TOPIC_ALIAS ||
                ((!topic_name_length) && client->clnt_topic_aliases[prop->byte2] == NULL)) {
            dbg_printf("     handle_cp_publish: topic_alias is too big\n");
            reason_code = MQTT_TOPIC_ALIAS_INVALID;
            goto fail;
        }

        if (topic_name_length) {
            if (client->clnt_topic_aliases[prop->byte2])
                free((void *)client->clnt_topic_aliases[prop->byte2]);

            if ((client->clnt_topic_aliases[prop->byte2] = (void *)
                        strdup((const char *)topic_name)) == NULL) {
                reason_code = MQTT_UNSPECIFIED_ERROR;
                goto fail;
            }

            topic_alias = prop->byte2;
        } else {
            topic_name = (void *)strdup((const char *)client->clnt_topic_aliases[prop->byte2]);
            if (topic_name == NULL)
                goto fail;
        }

    } else if (!topic_name_length) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    struct message *msg;
    if ((msg = register_message(topic_name, payload_format, packet->payload_len,
                    packet->payload, qos, client->session, flag_retain,
                    MSG_NORMAL)) == NULL) {
        warn("handle_cp_publish: register_message");
        goto fail;
    }
    msg->sender_status.packet_identifier = packet_identifier;

    free(topic_name);
    topic_name = NULL;

    packet->payload = NULL;
    packet->payload_len = 0;

    const time_t now = time(NULL);

    msg->sender_status.accepted_at = now;
    total_messages_sender_accepted_at++;

    dbg_printf("[%2d] handle_cp_publish: QoS %d specifics\n", client->session->id, qos);

    if (qos == 0) {
        msg->sender_status.acknowledged_at = now;
        msg->sender_status.released_at = now;
        msg->sender_status.completed_at = now;
    } if (qos == 1) {
        if (send_cp_puback(client, packet_identifier,
                    msg->delivery_states ? MQTT_SUCCESS : MQTT_NO_MATCHING_SUBSCRIBERS) == -1) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        if (msg->retain) {
            DEC_REFCNT(&msg->sender->refcnt);
            msg->sender = NULL;
        }
        msg->sender_status.acknowledged_at = now;
        msg->sender_status.released_at = now;
        msg->sender_status.completed_at = now;
    } else if (qos == 2) {
        if (send_cp_pubrec(client, packet_identifier,
                    msg->delivery_states ? MQTT_SUCCESS : MQTT_NO_MATCHING_SUBSCRIBERS) == -1) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        msg->sender_status.acknowledged_at = now;
    }

    reason_code = MQTT_SUCCESS;

    dbg_printf("[%2d] handle_cp_publish: ok with %s\n",
            client->session->id, reason_codes_str[reason_code]);

    return 0;

fail:
    dbg_printf("[%2d] handle_cp_publish: fail with reason code %s\n",
            client->session->id, reason_codes_str[reason_code]);

    if (topic_name) {
        free(topic_name);
        topic_name = NULL;
    }

    if (topic_alias) {
        free((void *)client->clnt_topic_aliases[topic_alias]);
        client->clnt_topic_aliases[topic_alias] = NULL;
        topic_alias = 0;
    }

    if (packet->payload) {
        free(packet->payload);
        packet->payload = NULL;
        packet->payload_len = 0;
    }

    if (disconnect_if_malformed(client, reason_code)) {
        return -1;
    } else if (qos == 1) {
        if (send_cp_puback(client, packet_identifier, reason_code) == -1)
            return -1;
    } else if (qos == 2) {
        if (send_cp_pubrec(client, packet_identifier, reason_code) == -1)
            return -1;
    }

    return -1;
}

[[gnu::nonnull]]
static int handle_cp_unsubscribe(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    struct topic_sub_request *request = NULL;
    void *tmp;

    if (packet->flags != MQTT_FLAG_UNSUBSCRIBE)
        goto fail;

    if (bytes_left < (1 + sizeof(uint16_t))) {
        errno = ENOSPC;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, sizeof(uint16_t));
    packet->packet_identifier = ntohs(packet->packet_identifier);
    ptr += sizeof(uint16_t);
    bytes_left -= sizeof(uint16_t);

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_UNSUBSCRIBE) == -1)
        goto fail;

    if (bytes_left < 3)
        goto fail;

    if ((request = calloc(1, sizeof(struct topic_sub_request))) == NULL)
        goto fail;

    /* TODO: this kinda overlaps with handle_cp_subscribe,
     * is there anyway to merge whole/part? */
    while (bytes_left)
    {
        if (bytes_left < 3)
            goto fail;

        if ((tmp = realloc(request->topics,
                        sizeof(uint8_t *) * (request->num_topics + 1))) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->topics = tmp;

        const size_t u8_size = sizeof(uint8_t) * (request->num_topics + 1);
        if ((tmp = realloc(request->reason_codes, u8_size)) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->reason_codes = tmp;

        if ((request->topics[request->num_topics] = read_utf8(&ptr,
                        &bytes_left)) == NULL)
            goto fail;

        request->reason_codes[request->num_topics] = MQTT_SUCCESS;
        request->num_topics++;
    }

    if (unsubscribe_from_topics(client->session, request) == -1) {
        warn("handle_cp_unsubscribe: unsubscribe_from_topics");
        goto fail;
    }

    errno = 0;
    int rc = send_cp_unsuback(client, packet->packet_identifier, request);
    free_topic_subs(request);
    request = NULL;

    INC_REFCNT(&packet->refcnt);
    return rc;

fail:

    if (request)
        free_topic_subs(request);

    disconnect_if_malformed(client, reason_code);

    return -1;
}

[[gnu::nonnull]]
static int handle_cp_subscribe(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    struct topic_sub_request *request = NULL;
    void *tmp;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    uint32_t subscription_identifier = 0;
    const struct property *prop;

    errno = EINVAL;

    if (packet->flags != MQTT_FLAG_SUBSCRIBE)
        goto fail;

    if (bytes_left < 3) {
        errno = ENOSPC;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, sizeof(uint16_t));
    packet->packet_identifier = ntohs(packet->packet_identifier);
    ptr += sizeof(uint16_t);
    bytes_left -= sizeof(uint16_t);

    if (packet->packet_identifier == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_SUBSCRIBE) == -1)
        goto fail;

    /* Check for 0 topic filters per [MQTT-3.8.3-2] */
    if (bytes_left < 3)
        goto fail;

    if ((request = calloc(1, sizeof(struct topic_sub_request))) == NULL)
        goto fail;

    dbg_printf("[%2d] handle_cp_subscribe: packet_identifier=%u\n",
            client->session->id, packet->packet_identifier);

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_SUBSCRIPTION_IDENTIFIER, &prop) == 0) {
        subscription_identifier = prop->byte4;

        if (subscription_identifier > MAX_SUB_IDENTIFIER ||
                subscription_identifier == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            goto fail;
        }

        request->subscription_identifier = subscription_identifier;
    }

    while (bytes_left)
    {
        if (bytes_left < 3)
            goto fail;

        if ((tmp = realloc(request->topics,
                        sizeof(uint8_t *) * (request->num_topics + 1))) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->topics = tmp;

        const size_t u8_size = sizeof(uint8_t) * (request->num_topics + 1);

        if ((tmp = realloc(request->options, u8_size)) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->options = tmp;

        if ((tmp = realloc(request->reason_codes, u8_size)) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->reason_codes = tmp;

        if ((request->topics[request->num_topics] = read_utf8(&ptr,
                        &bytes_left)) == NULL)
            goto fail;

        dbg_printf("[%2d] handle_cp_subscribe: got topic <%s>\n", client->session->id,
                (const char *)request->topics[request->num_topics]);

        if (bytes_left < 1)
            goto fail;

        /* Validate subscribe options byte */

        /* bits 7 & 6 are reserved */
        if ((*ptr & MQTT_SUBOPT_RESERVED_MASK))
            goto fail;

        /* QoS can't be 3 */
        if ((*ptr & MQTT_SUBOPT_QOS_MASK) == MQTT_SUBOPT_QOS_MASK)
            goto fail;

        /* retain handling can't be 3 */
        if ((*ptr & MQTT_SUBOPT_RETAIN_HANDLING_MASK) == MQTT_SUBOPT_RETAIN_HANDLING_MASK)
            goto fail;

        if (!strncmp(SHARED_PREFIX, (char *)request->topics[request->num_topics], SHARED_PREFIX_LENGTH)) {
            dbg_printf("[%2d] handle_cp_subscribe: SHARED request: <%s>\n", client->session->id,
                    (const char *)request->topics[request->num_topics]);
            if ((*ptr & MQTT_SUBOPT_NO_LOCAL))
                goto fail;
        }

        if (is_valid_topic_filter(request->topics[request->num_topics]) == -1) {
            dbg_printf("[%2d] handle_cp_subscribe: invalid topic <%s>: %s\n",
                    client->session->id,
                    request->topics[request->num_topics], strerror(errno));
            request->reason_codes[request->num_topics] = MQTT_TOPIC_FILTER_INVALID;
            free((void *)request->topics[request->num_topics]);
            request->topics[request->num_topics] = NULL;
        } else {
            /* TODO why would response QoS be < request QoS ? */
            request->reason_codes[request->num_topics] = (*ptr & MQTT_SUBOPT_QOS_MASK);
        }

        /* TODO do something with the RETAIN flag */

        request->options[request->num_topics] = *ptr++;
        bytes_left--;

        request->num_topics++;
    }

    if (subscribe_to_topics(client->session, request) == -1) {
        warn("handle_cp_subscribe: subscribe_to_topics");
        goto fail;
    }

    errno = 0;
    int rc = send_cp_suback(client, packet->packet_identifier, request);
    /* TODO send an error back? */

    if (rc == -1)
        goto skip_retain_check;

    /* Check for retain message sending
     * TODO handle SUB_SHARED properly */
    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        dbg_printf("[%2d] handle_cp_subscribe: retain check <%s>\n",
                client->session->id, request->topics[idx]);
        if (request->topics[idx] == NULL)
            continue;

        pthread_rwlock_rdlock(&global_topics_lock);
        for (struct topic *topic = global_topic_list; topic; topic = topic->next)
        {
            struct message *msg;
            dbg_printf("[%2d] handle_cp_subscribe: retain check: topic=<%s>\n",
                    client->session->id, topic->name);

            if ((msg = topic->retained_message) == NULL)
                continue;

            if (topic->retained_message->state != MSG_ACTIVE)
                continue;

            if (!topic_match(topic->name, request->topics[idx]))
                continue;

            dbg_printf("[%2d] handle_cp_subscribe: handling retain message\n",
                    client->session->id);

            if (enqueue_one_mds(msg, client->session) == -1)
                continue;

            dbg_printf("[%2d] handle_cp_subscribe: added retained message\n",
                    client->session->id);
            pthread_rwlock_wrlock(&msg->topic->pending_queue_lock);
            msg->next_queue = msg->topic->pending_queue;
            msg->topic->pending_queue = msg;
            pthread_rwlock_unlock(&msg->topic->pending_queue_lock);
        }
        pthread_rwlock_unlock(&global_topics_lock);
    }

skip_retain_check:

    free_topic_subs(request);
    request = NULL;
    INC_REFCNT(&packet->refcnt);
    return rc;

fail:
    if (request)
        free_topic_subs(request);
    send_cp_disconnect(client, reason_code);
    return -1;
}

[[gnu::nonnull]]
static int handle_cp_disconnect(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    uint8_t disconnect_reason = 0;

    errno = 0;

    if (bytes_left == 0)
        goto skip;

    if (bytes_left > 0) {
        disconnect_reason = *ptr++;
        bytes_left--;
        dbg_printf("[%2d] handle_cp_disconnect: disconnect reason was %u\n",
                client->session->id, disconnect_reason);
        logger(LOG_DEBUG, client, "handle_cp_disconnect: disconnect request with reason %s",
                (disconnect_reason < MQTT_REASON_CODE_MAX) ? reason_codes_str[disconnect_reason] :
                "UNKNOWN");
    }

    if (bytes_left > 0) {
        if (parse_properties(&ptr, &bytes_left, &packet->properties,
                    &packet->property_count, MQTT_CP_DISCONNECT) == -1)
            goto fail;

    } else {
skip:
        dbg_printf("[%2d] handle_cp_disconnect: no reason\n",
                client->session->id);
        logger(LOG_DEBUG, client, "handle_cp_disconnect: disconnect request with no reason");
    }

    if (bytes_left)
        goto fail;

    /* TODO MQTT_DISCONNECT_WITH_WILL_MESSAGE ? */
    if (disconnect_reason == MQTT_NORMAL_DISCONNECTION) {
        if (client->session && client->session->will_topic) {
            client->session->will_retain = false;
            if (client->session->will_payload) {
                free(client->session->will_payload);
                client->session->will_payload = NULL;
            }
            if (client->session->will_topic) {
                DEC_REFCNT(&client->session->will_topic->refcnt);
                client->session->will_topic = NULL;
            }
            if (client->session->will_props) {
                free_properties(client->session->will_props,
                        client->session->num_will_props);
                client->session->num_will_props = 0;
                client->session->will_props = NULL;
            }
        }
    }

    close_client(client, disconnect_reason, true); /* TODO check */
    close_socket(&client->fd);
    //client->state = CS_DISCONNECTED;
    return 0;

fail:
    //warnx("handle_cp_disconnect: packet malformed");
    //client->state = CS_CLOSING;
    close_client(client, MQTT_MALFORMED_PACKET, false);
    close_socket(&client->fd);
    return -1;
}

[[gnu::nonnull]]
static int handle_cp_pingreq(struct client *client,
        struct packet *packet, const void * /*remain*/)
{
    dbg_printf("[%2d] handle_cp_pingreq\n", client->session->id);

    if (packet->remaining_length > 0) {
        errno = EINVAL;
        close_client(client, MQTT_MALFORMED_PACKET, false);
        return -1;
    }

    client->last_keep_alive = time(NULL);
    dbg_printf("[%2d] handle_cp_pingreq: updating last_keep_alive to %lu\n",
            client->session->id, client->last_keep_alive);

    return send_cp_pingresp(client);
}

[[gnu::nonnull]]
static int handle_cp_connect(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    uint16_t connect_header_length, keep_alive = 0;
    uint8_t protocol_version, connect_flags;
    uint8_t protocol_name[PROTOCOL_NAME_LEN];
    const struct property *prop = NULL;
    bool reconnect = false;
    bool clean = false;
    bool unlock = false;

    uint8_t *will_topic = NULL;
    uint8_t will_qos = 0;
    void *will_payload = NULL;
    bool will_retain = false;
    bool keep_alive_override = false;
    uint16_t will_payload_len = 0;
    uint8_t payload_format = 0;
    struct property (*will_props)[] = NULL;
    unsigned num_will_props = 0;

    errno = EINVAL;

    dbg_printf("     handle_cp_connect: begin from client %u\n", client->id);

    if (client->state != CS_ACTIVE || client->protocol_version != 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (bytes_left < 2+4+1+1+2) /* Connect Header, Protocol Name, Protocol Version, Connect Flags, Keep Alive */
        goto fail;

    memcpy(&connect_header_length, ptr, sizeof(uint16_t));
    connect_header_length = ntohs(connect_header_length);
    ptr += sizeof(uint16_t);
    bytes_left -= sizeof(uint16_t);

    if (connect_header_length != 4) {
        if (connect_header_length == 6) {
            protocol_version = 3; /* TODO parse old headers properly just to moan about it */
            goto version_fail;
        }
        goto fail;
    }

    memcpy(protocol_name, ptr, PROTOCOL_NAME_LEN);
    ptr += PROTOCOL_NAME_LEN;
    bytes_left -= PROTOCOL_NAME_LEN;

    protocol_version = *ptr++;
    bytes_left--;

    connect_flags = *ptr++;
    bytes_left--;

    memcpy(&keep_alive, ptr, sizeof(uint16_t));
    keep_alive = ntohs(keep_alive);
    ptr += sizeof(uint16_t);
    bytes_left -= sizeof(uint16_t);

    if (keep_alive > 0 && keep_alive < MIN_KEEP_ALIVE) {
        keep_alive = MIN_KEEP_ALIVE;
        keep_alive_override = true;
    }

    if (memcmp(protocol_name, PROTOCOL_NAME, PROTOCOL_NAME_LEN))
        goto fail;

    if (connect_flags & MQTT_CONNECT_FLAG_RESERVED)
        goto fail;

    if (protocol_version != PROTOCOL_VERSION) {
version_fail:
        logger(LOG_WARNING, client, "handle_cp_connect: unsupported protocol version %d",
                protocol_version);
        if (protocol_version < PROTOCOL_VERSION)
            reason_code = 0x1; /* Connection Refused, unacceptable protocol version */
        else
            reason_code = MQTT_UNSUPPORTED_PROTOCOL_VERSION;
        goto fail;
    }

    /* Properties Length (1) + ClientID (2) */
    if (bytes_left < 1+2)
        goto fail;

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_CONNECT) == -1)
        goto fail;

    if (client->client_id != NULL) {
        logger(LOG_ERR, client, "clientid already set");
        errno = EEXIST;
        reason_code = MQTT_CLIENT_IDENTIFIER_NOT_VALID;
        goto fail;
    }

    if ((client->client_id = read_utf8(&ptr, &bytes_left)) == NULL)
        goto fail;

    /* [MQTT-3.1.3-5] */
    if (is_valid_connection_id(client->client_id) == -1) {
        logger(LOG_WARNING, NULL, "handle_cp_connect: invalid clientid");
        reason_code = MQTT_CLIENT_IDENTIFIER_NOT_VALID;
        goto fail;
    }

    const size_t clientid_len = strlen((const char *)client->client_id);

    if (clientid_len > MAX_CLIENTID_LEN) {
        logger(LOG_WARNING, NULL, "handle_cp_connect: clientid too long");
        reason_code = MQTT_CLIENT_IDENTIFIER_NOT_VALID;
        goto fail;
    }

    if (clientid_len == 0) {
        logger(LOG_WARNING, NULL, "handle_cp_connect: zero-length clientid not supported");
        reason_code = MQTT_CLIENT_IDENTIFIER_NOT_VALID;
        goto fail;
    }

    dbg_printf("[  ] handle_cp_connect: client_id=<%s> ",
            (char *)client->client_id);

    if (connect_flags & MQTT_CONNECT_FLAG_CLEAN_START) {
        dbg_cprintf("clean_start ");
    }

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        dbg_cprintf("will_properties ");
        if (parse_properties(&ptr, &bytes_left, &will_props,
                    &num_will_props, MQTT_CP_INVALID) == -1)
            goto fail;

        dbg_cprintf("[%d props] will_topic ", num_will_props);

        will_topic = read_utf8(&ptr, &bytes_left);
        if (will_topic == NULL)
            goto fail;

        if ((will_payload = read_binary(&ptr, &bytes_left,
                        &will_payload_len)) == NULL)
            goto fail;

        dbg_cprintf("[%ub] ", will_payload_len);
        will_retain = (connect_flags & MQTT_CONNECT_FLAG_WILL_RETAIN);
    }

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_RETAIN) {
        dbg_cprintf("will_retain ");
        if ((connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            warn("handle_cp_connect: Will Retain set without Will Flag");
            goto fail;
        }
    }

    if (connect_flags & MQTT_CONNECT_FLAG_USERNAME) {
        dbg_cprintf("username ");
        if ((client->username = read_utf8(&ptr, &bytes_left)) == NULL)
            goto fail;

        dbg_cprintf("<%s> ", (char *)client->username);
    }

    if (connect_flags & MQTT_CONNECT_FLAG_PASSWORD) {
        dbg_cprintf("password ");
        if ((client->password = read_binary(&ptr, &bytes_left,
                        &client->password_len)) == NULL)
            goto fail;

        dbg_cprintf("[%ub] ", client->password_len);
    }

    if (bytes_left)
        goto fail;

    will_qos = GET_WILL_QOS(connect_flags);

    if ((connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) == 0 && will_qos != 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (will_qos > 2)
        goto fail;

    if ((connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG)) {
        dbg_cprintf("will_qos [%u] ", will_qos);
    }

    dbg_cprintf("keep_alive=%u ", keep_alive);
    dbg_cprintf("\n");

    /* FIXME */
    if (client->username && client->password)
        client->is_auth = true;
    else {
        errno = EACCES;
        logger(LOG_WARNING, client, "handle_cp_connect: invalid/empty username and/or password received");
        reason_code = MQTT_BAD_USER_NAME_OR_PASSWORD;
        goto fail;
    }

    /* we store these here as setting protocol_version implies a level of
     * success */
    client->connect_flags = connect_flags;
    client->protocol_version = protocol_version;
    client->keep_alive = keep_alive;

#if defined(FEATURE_RAFT) && !defined(FEATURE_MULTI_MASTER)
    if (opt_raft && !raft_is_leader()) {
        reason_code = MQTT_USE_ANOTHER_SERVER;
        goto fail;
    }
#endif

    if ((client->session = find_session(client)) == NULL) {
        /* New Session */
        dbg_printf(BWHT"     handle_cp_connect: no existing session"CRESET"\n");

create_new_session:
        if ((client->session = alloc_session(client, NULL)) == NULL) {
            warn("handle_cp_connect: alloc_session failed");
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        dbg_printf("[%2d] handle_cp_connect: new session\n", client->session->id);
        pthread_rwlock_wrlock(&global_sessions_lock);
        unlock = true;
    } else {
        pthread_rwlock_wrlock(&global_sessions_lock);
        unlock = true;
        /* Existing Session */
        dbg_printf(BWHT"[%2d] handle_cp_connect: has existing session"CRESET"\n",
                client->session->id);

        /* [MQTT-3.1.4-3] */
        if (client->session->client) {
            if (send_cp_disconnect(client->session->client,
                        MQTT_SESSION_TAKEN_OVER) == -1)
                client->session->client->state = CS_CLOSING;

            DEC_REFCNT(&client->session->refcnt); /* handle_cp_connect */

            client->session->client->session = NULL;
            client->session->client = NULL;
        }

        reconnect = true;

        /* [MQTT-3.1.4-4] */
        if (connect_flags & MQTT_CONNECT_FLAG_CLEAN_START) {
            clean = true;
            /* ... we don't want to re-use it */
            dbg_printf(BWHT"[  ] handle_cp_connect: clean existing session [%d]"CRESET"\n",
                    client->session->id);
#ifdef FEATURE_RAFT
            close_session(client->session, true);
#else
            close_session(client->session);
#endif
            client->session = NULL;
            goto create_new_session;
        }
        /* else [MQTT-3.1.2-5] */

        client->connect_response_flags |= MQTT_CONNACK_FLAG_SESSION_PRESENT;
        dbg_printf(BWHT"[%2d] handle_cp_connect: connection re-established to client %d"CRESET"\n",
                client->session->id, client->id);
        client->session->client = client;
    }

    assert(client->session->state != SESSION_DELETE);

    INC_REFCNT(&client->session->refcnt); /* free_client || client_tick || handle_cp_connect */
    client->session->last_connected = time(NULL);

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        if ((client->session->will_topic = find_or_register_topic(will_topic)) == NULL) {
            errno = ENOENT;
            reason_code = MQTT_TOPIC_NAME_INVALID;
            goto fail;
        }
        free(will_topic); /* find_or_register_topic duplicates */
        will_topic = NULL;

        INC_REFCNT(&client->session->will_topic->refcnt); /* free_session | handle_cp_disconnect | session_tick  */

        client->session->will_retain         = will_retain;
        client->session->will_payload        = will_payload;
        client->session->will_payload_len    = will_payload_len;
        client->session->will_qos            = will_qos;
        client->session->will_payload_format = payload_format;
        client->session->will_props          = will_props;
        client->session->num_will_props      = num_will_props;
        dbg_printf(BBLU "[%2d] handle_cp_connect: setting session will_message"CRESET"\n",
                client->session->id);
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_SESSION_EXPIRY_INTERVAL, &prop) == 0) {
        client->session->expiry_interval = prop->byte4;
        dbg_printf("[%2d] handle_cp_connect: SESSION_EXPIRY_INTERVAL=%u\n", client->session->id, prop->byte4);
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_REQUEST_RESPONSE_INFORMATION, &prop) == 0) {
        client->session->request_response_information = prop->byte;
        dbg_printf("[%2d] handle_cp_connect: REQUEST_RESPONSE_INFORMATION=%u\n", client->session->id, prop->byte);
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_REQUEST_PROBLEM_INFORMATION, &prop) == 0) {
        client->session->request_problem_information = prop->byte;
        dbg_printf("[%2d] handle_cp_connect: REQUEST_PROBLEM_INFORMATION=%u\n", client->session->id, prop->byte);
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_AUTHENTICATION_METHOD, &prop) == 0) {
        reason_code = MQTT_BAD_AUTHENTICATION_METHOD;
        goto fail;
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_AUTHENTICATION_DATA, &prop) == 0) {
        reason_code = MQTT_PROTOCOL_ERROR;
        goto fail;
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_MAXIMUM_PACKET_SIZE, &prop) == 0) {
        client->maximum_packet_size = prop->byte4;
        dbg_printf("[%2d] handle_cp_connect: MAXIMUM_PACKET_SIZE=%u\n", client->session->id, prop->byte4);
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_TOPIC_ALIAS_MAXIMUM, &prop) == 0) {
        dbg_printf("[%2d] handle_cp_connect: TOPIC_ALIAS_MAXIMUM=%u\n", client->session->id, prop->byte2);
        if (prop->byte2 == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            goto fail;
        }

        if (prop->byte2 > MAX_TOPIC_ALIAS) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }

        if ((client->clnt_topic_aliases = calloc(1,
                        sizeof(uint8_t *) * prop->byte2)) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }

        client->topic_alias_maximum = prop->byte2;
    }

    client->keep_alive_override = keep_alive_override;

    if (send_cp_connack(client, MQTT_SUCCESS) == -1) {
        reason_code = MQTT_UNSPECIFIED_ERROR;
        warn("handle_cp_connect: send_cp_connack failed");
        goto fail;
    }
    pthread_rwlock_unlock(&global_sessions_lock);

#ifdef FEATURE_RAFT
    register_session(client->session, true);
#else
    register_session(client->session);
#endif

    logger(LOG_DEBUG, client, "handle_cp_connect: session established%s%s%s",
            reconnect ? " (reconnect)" : "",
            clean ? " (clean_start)" : "",
            (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) ? " (will)" : "");

    return 0;

fail:
    if (unlock)
        pthread_rwlock_unlock(&global_sessions_lock);

    if (reason_code == MQTT_CLIENT_IDENTIFIER_NOT_VALID) {
        if (client->client_id) {
            free((void *)client->client_id);
            client->client_id = NULL;
        }
    }

    bool connack_failed = false;

    if (send_cp_connack(client, reason_code) == -1)
        connack_failed = true;

    if (will_topic)
        free(will_topic);
    if (will_props)
        free_properties(will_props, num_will_props);
    if (will_payload)
        free(will_payload);
    if (client->session && client->session->state == SESSION_NEW)
#ifdef FEATURE_RAFT
        close_session(client->session, false);
#else
        close_session(client->session);
#endif

    if (errno == 0)
        errno = EINVAL;

    close_client(client, reason_code, false);
    if (!connack_failed) /* response already sent via CONNACK */
        client->send_disconnect = false;

#if 0
    client->state = CS_CLOSING;
    if (client->disconnect_reason == 0)
        client->disconnect_reason = reason_code;
#endif

    return -1;
}

[[gnu::nonnull]]
static int handle_cp_auth(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_SUCCESS;
    [[maybe_unused]] reason_code_t auth_reason_code = MQTT_SUCCESS;

    if (bytes_left == 0)
        goto skip_props;

    if (bytes_left < 2) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    auth_reason_code = *ptr++;
    bytes_left--;

    dbg_printf("[%2d] handle_cp_auth: auth_reason_code=%u\n",
            client->session ? client->session->id : (id_t)-1,
            auth_reason_code);

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_AUTH) == -1) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }
skip_props:

    if (bytes_left) {
        reason_code = MQTT_MALFORMED_PACKET;
        goto fail;
    }

    reason_code = MQTT_PROTOCOL_ERROR;
    goto fail;

    /* TODO */

    return 0;

fail:
    close_client(client, reason_code, false);
#if 0
    if (disconnect_if_malformed(client, reason_code))
        return -1;
#endif

    return -1;
}

/*
 * control packet function lookup table
 */

typedef int (*control_func_t)(struct client *, struct packet *, const void *);

static const struct {
    const control_func_t func;
    const bool needs_auth;
} control_functions[MQTT_CP_MAX] = {
    [MQTT_CP_PUBLISH]     = { handle_cp_publish     , true  },
    [MQTT_CP_PUBACK]      = { handle_cp_puback      , true  },
    [MQTT_CP_PUBREC]      = { handle_cp_pubrec      , true  },
    [MQTT_CP_PUBREL]      = { handle_cp_pubrel      , true  },
    [MQTT_CP_PUBCOMP]     = { handle_cp_pubcomp     , true  },
    [MQTT_CP_PINGREQ]     = { handle_cp_pingreq     , true  },
    [MQTT_CP_SUBSCRIBE]   = { handle_cp_subscribe   , true  },
    [MQTT_CP_UNSUBSCRIBE] = { handle_cp_unsubscribe , true  },
    [MQTT_CP_CONNECT]     = { handle_cp_connect     , false },
    [MQTT_CP_DISCONNECT]  = { handle_cp_disconnect  , true  },
    [MQTT_CP_AUTH]        = { handle_cp_auth        , false },
};

/*
 * other functions
 */

[[gnu::nonnull]]
static void handle_outbound(struct client *client)
{
    ssize_t rc;

    if (client->fd == -1)
        return;

    dbg_printf(BWHT "[%2d] handle_outbound: size=%u offset=%u remaining=%u" CRESET "\n",
            client->session ? client->session->id : (id_t)-1,
            client->po_size,
            client->po_offset,
            client->po_remaining);

    rc = write(client->fd, client->po_buf + client->po_offset, client->po_remaining);

    if (rc == client->po_remaining) {
free_and_return:
        free((void *)client->po_buf);
        client->po_buf = NULL;
        client->po_remaining = 0;
        client->po_offset = 0;
        client->po_size = 0;
        return;
    }

    if (rc == -1) {
        if (errno == EINTR)
            return;

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            client->write_ok = false;
            return;
        }

        client->state = CS_CLOSING;
        log_io_error(NULL, rc, client->po_remaining, false, client);
        goto free_and_return;
    }

    /* short write */
    client->po_remaining -= rc;
    client->po_offset += rc;
    log_io_error(NULL, rc, client->po_remaining, false, client);
}

[[gnu::nonnull]]
static void set_outbound(struct client *client, const uint8_t *buf, unsigned len)
{
    if (client->po_buf)
        free((void *)client->po_buf);

    client->po_buf = buf;
    client->po_size = len;
    client->po_offset = 0;
    client->po_remaining = len;
}

[[gnu::nonnull]]
static int parse_incoming(struct client *client)
{
    ssize_t rd_len;
    reason_code_t reason_code;
    struct mqtt_fixed_header *hdr;

    errno = 0;
    hdr = NULL;
    reason_code = MQTT_MALFORMED_PACKET;

    switch (client->parse_state)
    {
        case READ_STATE_NEW:
            dbg_cprintf("\n");
            dbg_printf("[%2d] parse_incoming: READ_STATE_NEW client=%d <%s>\n",
                    client->session ? client->session->id : (id_t)-1,
                    client->id,
                    client->client_id ? (char *)client->client_id : "");
            client->read_offset = 0;
            client->rl_offset = 0;
            client->rl_multi = 1;
            client->rl_value = 0;
            client->read_need = sizeof(struct mqtt_fixed_header) + 1;
            client->parse_state = READ_STATE_HEADER;
            if (client->packet_buf) {
                free(client->packet_buf);
                client->packet_buf = NULL;
            }
            if (client->new_packet &&
                    client->new_packet->packet_identifier == 0) {
                if (GET_REFCNT(&client->new_packet->refcnt) == 0)
                    free_packet(client->new_packet, true, true);
            }
            client->new_packet = NULL;

            /* fall through */

        case READ_STATE_HEADER:
            goto more;
            /* fall through */
        case READ_STATE_MORE_HEADER:
more:
            rd_len = read(client->fd,
                    &client->header_buffer[client->read_offset],
                    client->read_need);
            if (rd_len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) ) {
                return 0;
            } else if (rd_len == -1) {
                log_io_error(NULL, rd_len, client->read_need, false, NULL);
                goto eof;
            } else if (rd_len == 0) {
eof:
                dbg_printf("[%2d] parse_incoming: %s EOF on client=%u <%s>\n",
                        client->session ? client->session->id : (id_t)-1,
                        read_state_str[client->parse_state],
                        client->id, (char *)client->client_id);
                /* EOF - shared between states */
                client->state = CS_DISCONNECTED;
                close_socket(&client->fd);
                client->parse_state = READ_STATE_NEW;
                return 0;
            } else if (rd_len < client->read_need) {
                client->read_offset += rd_len;
                client->read_need -= rd_len;
                return 0;
            }

            client->read_offset += rd_len;
            client->read_need -= rd_len;

            if (client->parse_state == READ_STATE_HEADER) {
                hdr = (void *)client->header_buffer;

                if (hdr->type >= MQTT_CP_MAX || hdr->type == 0) {
                    warnx("hdr->type");
                    goto fail;
                }

                if (control_functions[hdr->type].func == NULL) {
                    warnx("no func for %u", hdr->type);
                    goto fail;
                }

                if (hdr->flags & packet_permitted_flags[hdr->type]) {
                    warnx("illegal flags");
                    goto fail;
                }

                client->parse_state = READ_STATE_MORE_HEADER;

                if (client->read_need == 0)
                    goto lenread;
            }

            if (client->parse_state == READ_STATE_MORE_HEADER) {
lenread:
                uint8_t tmp;
                hdr = (void *)client->header_buffer;

                if (client->rl_multi > 128*128*128) {
                    errno = ERANGE;
                    warn("var len overflow");
                    goto fail;
                }

                tmp = client->header_buffer[1 + client->rl_offset];

                client->rl_value += (tmp & 127) * client->rl_multi;
                client->rl_multi *= 128;

                client->rl_offset++;
                client->read_need++;

                if ( (tmp & 128) != 0 )
                    return 0;

                if (client->rl_value > MAX_PACKET_LENGTH) {
                    reason_code = MQTT_PACKET_TOO_LARGE;
                    errno = EFBIG;
                    goto fail;
                }

                dbg_printf("[%2d] parse_incoming: client=%u type=%u <"
                        BRED"%s"CRESET"> flags=%u remaining_length=%u\n",
                        client->session ? client->session->id : (id_t)-1,
                        client->id,
                        hdr->type, control_packet_str[hdr->type],
                        hdr->flags, client->rl_value);

                if ((client->new_packet = alloc_packet(client, PACKET_IN)) == NULL) {
                    warn("alloc_packet");
                    goto fail;
                }

                client->new_packet->remaining_length = client->rl_value;
                client->new_packet->type = hdr->type;
                client->new_packet->flags = hdr->flags;

                if ((client->packet_buf = malloc(client->new_packet->remaining_length)) == NULL) {
                    warn("malloc(%lu)", client->new_packet->remaining_length);
                    goto fail;
                }

                client->parse_state = READ_STATE_BODY;
                client->packet_offset = 0;
                client->read_need = client->new_packet->remaining_length;
                if (client->read_need == 0)
                    goto readbody;
            }
            break;

        case READ_STATE_BODY:
readbody:
            if (client->read_need == 0)
                goto exec_control;

            rd_len = read(client->fd,
                    &client->packet_buf[client->packet_offset],
                    client->read_need);

            if (rd_len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                return 0;
            } else if (rd_len == -1) {
                log_io_error(NULL, rd_len, client->read_need, false, NULL);
                goto fail;
            } else if (rd_len == 0) {
                goto eof;
            } else if (rd_len < client->read_need) {
                client->packet_offset += rd_len;
                client->read_need -= rd_len;
                return 0;
            }
exec_control:
            client->parse_state = READ_STATE_NEW;

            if (client->session == NULL &&
                    client->new_packet->type != MQTT_CP_CONNECT) {
                warnx("parse_incoming: first packet is not CONNECT");
                goto fail;
            }

            dbg_printf("[%2d] parse_incoming: client=%u control_function\n",
                    client->session ? client->session->id : (id_t)-1,
                    client->id);

            total_control_packets_recv++;

            /* [MQTT-3.1.4-6] - maybe */
            if (!control_functions[client->new_packet->type].needs_auth ||
                    client->is_auth) {
                bool unlock = false;

                if (client->session) {
                    pthread_rwlock_rdlock(&global_sessions_lock);
                    unlock = true;
                }

                total_control_packets_processed++;
                if (control_functions[client->new_packet->type].func(client,
                            client->new_packet, client->packet_buf) == -1) {
                    warn("control_function");
                    if (client->disconnect_reason)
                        reason_code = client->disconnect_reason;
                    if (errno == EPIPE)
                        client->state = CS_CLOSED;
                    if (unlock)
                        pthread_rwlock_unlock(&global_sessions_lock);
                    goto fail;
                }
                total_control_packets_processed_ok++;
                if (unlock)
                    pthread_rwlock_unlock(&global_sessions_lock);
            }
            client->last_keep_alive = time(NULL);

            /* If we don't lock here, there is a race between refcnt-- & free_packet() */
            pthread_rwlock_wrlock(&global_packets_lock);
            if (IF_DEC_REFCNT(&client->new_packet->refcnt) == 1) {
                free_packet(client->new_packet, false, true);
            } else {
                dbg_printf("[%2d] parse_incoming: can't free packet refcnt>0\n",
                        client->session ? client->session->id : (id_t)-1);
            }
            client->new_packet = NULL;
            pthread_rwlock_unlock(&global_packets_lock);

            break;
        case READ_STATE_MAX:
            errno = EINVAL;
            warnx("parse_incoming: illegal read_state");
            goto fail;
    }
    return 0;

fail:
    if (client->packet_buf) {
        free(client->packet_buf);
        client->packet_buf = NULL;
    }
    if (client->new_packet) {
        if (IF_DEC_REFCNT(&client->new_packet->refcnt) == 1)
            free_packet(client->new_packet, true, true);
        client->new_packet = NULL;
    }

    client->parse_state = READ_STATE_NEW;
    client->state = CS_CLOSING;
    client->disconnect_reason = reason_code;
    client->send_disconnect = true; /* TODO move to close_client() ? */

    return -1;
}

/* Clients */

/*
 * we need to handle several scenarios:
 * 1. a polite disconnection (ready for reconnection)
 * 2. a requested disconnection (no reconnection)
 * 3. a termination (with DISCONNECT)
 * 4. a termination (without DISCONNECT) e.g. a malformed packet
 */
static void close_client(struct client *client, reason_code_t reason, bool disconnect)
{
    /* Scenario 4 - no DISCONNECT */
    if (disconnect_if_malformed(client, disconnect) == -1)
        return;

    client->disconnect_reason = reason;

    if (disconnect) {
        /* Scenario 1 - can reconnect */
        client->state = CS_DISCONNECTED;
    } else {
        /* Scenario 3 */
        client->send_disconnect = true;
        client->state = CS_CLOSING;
    }
}

static void client_tick(void)
{
    const struct property *prop = NULL;
    const time_t now = time(NULL);
    uint32_t will_delay = 0;

    pthread_rwlock_wrlock(&global_clients_lock);
    for (struct client *clnt = global_client_list, *next; clnt; clnt = next)
    {
        next = clnt->next;

        switch (clnt->state)
        {
            case CS_ACTIVE:

                if (clnt->session == NULL && clnt->tcp_accepted_at != 0
                        && (now - clnt->tcp_accepted_at) > 5) {
                    warnx("client_tick: closing idle link: no CONNECTION");
                    goto force_close;
                }

                /* [MQTT-3.1.2-22] */
                if (clnt->keep_alive) {
                    time_t overdue = 1 + (clnt->keep_alive * 1.5f);
                    if (now > (clnt->last_keep_alive + overdue)) {
                        warnx("client_tick: closing idle link: no PINGREQ within Keep Alive: %lu > %lu",
                                now, (clnt->last_keep_alive + overdue));
                        goto force_close;
                    }
                }

                if (clnt->write_ok && clnt->po_buf) {
                    /* avoid two threads attempting to send random data */
#ifdef FEATURE_THREADS
                    if (pthread_rwlock_trywrlock(&clnt->po_lock) == 0)
#endif
                        handle_outbound(clnt);
                }

                break;

            case CS_NEW:
                break;

            case CS_DISCONNECTED:
                logger(LOG_DEBUG, clnt, "client_tick: client disconnected");
                if (clnt->session) {

                    /* Prepare the Will Message to be sent, optionally
                     * delaying by Will Delay. session_tick() will actually
                     * send. */
                    if (clnt->session->will_topic) {
                        dbg_printf("[%2d] client_tick: handling WILL\n",
                                clnt->session->id);

                        will_delay = 0;
                        if (clnt->session->will_props) {
                            if (get_property_value(clnt->session->will_props,
                                        clnt->session->num_will_props,
                                        MQTT_PROP_WILL_DELAY_INTERVAL,
                                        &prop) != -1)
                                will_delay = prop->byte4;
                        }
                        clnt->session->will_at = now + will_delay;
                    }

                    clnt->session->last_connected = now;
                    clnt->session->client = NULL;

                    /* TODO set a sensible maximum */
                    /* [MQTT-3.1.2-23 */
                    if (clnt->session->expiry_interval == 0) {
                        dbg_printf("[%2d] client_tick: expiring session instantly\n",
                                clnt->session->id);
#ifdef FEATURE_RAFT
                        close_session(clnt->session, true);
#else
                        close_session(clnt->session);
#endif
                    } else if (clnt->session->expiry_interval == UINT_MAX) {
                        clnt->session->expires_at = LONG_MAX; /* "does not expire" */
                    } else {
                        clnt->session->expires_at =
                            now + clnt->session->expiry_interval;
                    }

                    DEC_REFCNT(&clnt->session->refcnt); /* handle_cp_connect */
                    clnt->session = NULL;
                }
                clnt->send_disconnect = false;
                goto skip_send_disconnect;

            case CS_CLOSING:
                if (clnt->session)
                    warnx("[%2d] client_tick: session present in CS_CLOSING",
                            clnt->session->id);

                if (clnt->send_disconnect) {
                    logger(LOG_NOTICE, clnt,
                            "client_tick: disconnecting client with reason %s",
                            reason_codes_str[clnt->disconnect_reason]);
                    send_cp_disconnect(clnt, clnt->disconnect_reason);
                    clnt->send_disconnect = false;
                }

skip_send_disconnect:
                /* Common for CS_DISCONNECTED */
                if (clnt->session && clnt->session->last_connected == 0)
                    clnt->session->last_connected = time(NULL);

force_close:
                /* Common for CS_ACTIVE with zero MQTT_CP_CONNECT */
                if (clnt->fd != -1)
                    close_socket(&clnt->fd);
                clnt->state = CS_CLOSED;

                break;

            case CS_CLOSED:
                free_client(clnt, false);
                clnt = NULL;
                break;

            case CLIENT_STATE_MAX:
                warn("client_tick: illegal client_state, closing.");
                clnt->state = CS_CLOSING;
                errno = EINVAL;
                break;
        }
    }
    pthread_rwlock_unlock(&global_clients_lock);
}

/* Topics */

static void tick_msg(struct message *msg)
{
    unsigned num_sent = 0, num_to_send = 0;
    struct packet *packet;
    struct message_delivery_state *mds;
    const time_t now = time(NULL);

    /* This handles the case the message has no target */
    if (msg->delivery_states == NULL && (msg->qos != 2 || msg->sender_status.completed_at)) {
        dbg_printf("     tick_msg: dequeue_message due to no delivery states %d\n", msg->id);
        if (msg->topic) {
            if (dequeue_message(msg) == -1)
                warn("tick_msg: dequeue_message failed");
            if (!msg->retain || msg->topic == NULL
                    || msg->topic->retained_message != msg)
                msg->state = MSG_DEAD;
        }
        return;
    } else if (msg->delivery_states == NULL) {
        dbg_printf("     tick_msg: no delivery_states %d (qos=%d, completed_at=%lu)\n",
                msg->id,
                msg->qos,
                msg->sender_status.completed_at);
        return;
    }

    pthread_rwlock_wrlock(&msg->delivery_states_lock);
    for (unsigned idx = 0; idx < msg->num_message_delivery_states; idx++)
    {
        mds = msg->delivery_states[idx];

        if (mds == NULL)
            continue;

        num_to_send++;

        if (mds->acknowledged_at) {
            num_sent++;
            continue;
        }

        if (mds->last_sent && (now - mds->last_sent < 2))
            continue;

        if (mds->session == NULL || mds->message != msg) {
            warnx("     tick_msg: message_delivery_state is corrupt");
            continue;
        }

        /* disconnected session */
        if (mds->session->client == NULL)
            continue;

        if (mds->session->state != SESSION_ACTIVE)
            continue;

        if (mds->session->client->state != CS_ACTIVE)
            continue;

        dbg_printf(
                "     tick_msg: sending message: id=%d subscriber.id=%d <%s> ackat=%lu lastsent=%lu"
                CRESET"\n",
                mds->id, mds->session->id, (char *)mds->session->client_id,
                mds->acknowledged_at, mds->last_sent);

        if ((packet = alloc_packet(mds->session->client, PACKET_OUT)) == NULL) {
            warn("tick_msg: unable to alloc_packet for msg on topic <%s>",
                    msg->topic->name);
            continue;
        }

        /* this code might execute more than once, so avoid a double refcnt */
        //if (mds->last_sent == 0)
        INC_REFCNT(&msg->refcnt);
        packet->message = msg;

        packet->type = MQTT_CP_PUBLISH;
        packet->flags |= MQTT_FLAG_PUBLISH_QOS(msg->qos);

        if (msg->qos) {
            /* Allocate a packet_identifier if this is the first time */
            if (mds->packet_identifier == 0)
                while (mds->packet_identifier == 0) /* wrap around handling */
                    mds->packet_identifier = ++packet->owner->last_packet_id;
            packet->packet_identifier = mds->packet_identifier;

            /* acknowledged_at is 0, so this must be a retry? */
            if (mds->last_sent) {
                packet->flags |= MQTT_FLAG_PUBLISH_DUP;
            }
        }

        if (msg->retain)
            packet->flags |= MQTT_FLAG_PUBLISH_RETAIN;

        packet->reason_code = MQTT_SUCCESS;

        /* TODO make this async START ?? */
        mds->last_sent = now;
        if (send_cp_publish(packet) == -1) {
            mds->last_sent = 0;
            warn("tick_msg: unable to send_cp_publish refcnt=%u", GET_REFCNT(&packet->refcnt));
            DEC_REFCNT(&packet->refcnt);
            free_packet(packet, true, true); /* Anything else? */
            packet = NULL;
            continue;
        }

        total_messages_accepted_at++;

        /* Unless we get a network error, just assume it works */
        if (msg->qos == 0) {
            mds->acknowledged_at = now;
            mds->completed_at = mds->acknowledged_at;
            mds->released_at = mds->acknowledged_at;
        }

        DEC_REFCNT(&packet->refcnt);
        free_packet(packet, true, true);
        packet = NULL;
        /* TODO async END */
    }

    /* We have now sent everything */
    if (num_sent == num_to_send /*msg->num_message_delivery_states*/) {
        /* TODO this doesn't handle holes? */

        /* TODO replace with list of subscribers to message, removal thereof,
         * then dequeue when none left */

        while (msg->num_message_delivery_states && msg->delivery_states)
        {
            unsigned idx = 0;
            struct message_delivery_state *mds;
again:
            if (msg->delivery_states == NULL)
                break;

            if (idx >= msg->num_message_delivery_states)
                break;

            mds = msg->delivery_states[idx++];

            if (mds == NULL) {
                dbg_printf(BYEL"     tick_msg: mds.idx=%u is NULL"CRESET"\n", idx-1);
                break;
            }

            if (mds->completed_at == 0)
                goto again;

            dbg_printf(
                    "     tick_msg: mds.idx=%u unlink mds.id=%d from session %u[%u] and message %u[%u]"
                    CRESET"\n",
                    idx - 1,
                    mds->id,
                    mds->session ? mds->session->id : 0,
                    mds->session ? mds->session->refcnt : 0,
                    mds->message ? mds->message->id : 0,
                    mds->message ? mds->message->refcnt : 0);

            if (mds_detach_and_free(mds, true, false) == -1)
                warn("tick_msg: mds_detach_and_free: msg.id=%d mds.idx=%u mds.id=%d session.id=%d message.id=%d",
                        msg->id,
                        idx - 1,
                        mds->id,
                        mds->session ? mds->session->id : (id_t)-1,
                        mds->message ? mds->message->id : (id_t)-1
                        );
            mds = NULL;
        }

        /* We can't just dequeue() and MSG_DEAD if any mds are not completed_at */
        if (msg->topic && msg->num_message_delivery_states == 0) {
            dbg_printf(NGRN"     tick_msg: dequeue"CRESET"\n");
            if (dequeue_message(msg) == -1) {
                warn("tick_msg: dequeue_message failed");
            }
            if (!msg->retain || msg->topic == NULL
                    || msg->topic->retained_message != msg)
                msg->state = MSG_DEAD;
        }

    }
    pthread_rwlock_unlock(&msg->delivery_states_lock);
}

static void topic_tick(void)
{
    unsigned max_messages = MAX_MESSAGES_PER_TICK;
    bool first = true;
    bool found = false;

    pthread_rwlock_rdlock(&global_messages_lock);
    pthread_rwlock_wrlock(&global_topics_lock);
    for (struct topic *topic = global_topic_list; topic; topic = topic->next)
    {
        if (topic->state != TOPIC_ACTIVE)
            continue;

        if (max_messages == 0 || topic->pending_queue == NULL)
            continue;

        found = true;

        if (first) {
            dbg_printf("\n");
            first = false;
        }

        dbg_printf("     topic_tick: <%s> %p\n",
                topic->name, (void *)topic->pending_queue);

        /* Iterate over the queued messages on this topic */
        pthread_rwlock_wrlock(&topic->pending_queue_lock);
        for (struct message *msg = topic->pending_queue, *next; msg; msg = next)
        {
            if (--max_messages == 0)
                break;

            next = msg->next_queue;
            tick_msg(msg);
        }
        pthread_rwlock_unlock(&topic->pending_queue_lock);
    }
    pthread_rwlock_unlock(&global_topics_lock);
    pthread_rwlock_unlock(&global_messages_lock);

    if (found) {
        dbg_printf("\n");
    }
}

/* Messages */

static void message_tick(void)
{
    pthread_rwlock_wrlock(&global_messages_lock);
    for (struct message *msg = global_message_list, *next; msg; msg = next)
    {
        next = msg->next;

        if (msg->state != MSG_DEAD)
            continue;

        if (msg->retain)
            continue;

        free_message(msg, false);
        msg = NULL;
    }
    pthread_rwlock_unlock(&global_messages_lock);
}

/* Sessions */

static void close_session(struct session *session
#ifdef FEATURE_RAFT
        , bool source_self
#endif
        )
{
    dbg_printf(BMAG "     close_session: id=%d state=%s client=%d client_id=<%s>" CRESET "\n",
            session->id, session_state_str[session->state],
            session->client ? session->client->id : (id_t)-1,
            (const char *)session->client_id);

    time_t now = time(NULL);

    for (struct message_delivery_state *mds = global_mds_list ; mds ; mds = mds->next)
    {
        if (mds->state != MDS_ACTIVE)
            continue;

        if (mds->session != session)
            continue;

        dbg_printf(NMAG "     close_session: updating mds.id=%d for message.id=%d\n",
                mds->id, mds->message->id);

        if (mds->acknowledged_at == 0) mds->acknowledged_at = now;
        if (mds->last_sent == 0) mds->last_sent = now;
        if (mds->completed_at == 0) mds->completed_at = now;
        if (mds->released_at == 0) mds->released_at = now;
        if (mds->accepted_at == 0) mds->accepted_at = now;
    }

    unsubscribe_session_from_all(session);

#ifdef FEATURE_RAFT
    if (source_self && session->state == SESSION_ACTIVE && session->expiry_interval) {
        /* TODO raft_client_log_send(RAFT_LOG_UNREGISTER_SESSION, ...); */
        session->state = SESSION_DELETE;
    } else {
        session->state = SESSION_DELETE;
    }
#else
    session->state = SESSION_DELETE;
#endif
}

#ifdef FEATURE_RAFT

#endif

/*
 * Tick functions
 */

static void session_tick(void)
{
    const time_t now = time(NULL);

    pthread_rwlock_wrlock(&global_sessions_lock);
    for (struct session *session = global_session_list, *next; session;
            session = next)
    {
        next = session->next;

        if (session->state == SESSION_NEW)
            continue;

        if (session->state == SESSION_DELETE) {
            if (GET_REFCNT(&session->refcnt) > 0) {
                if (++session->kill_attempts > 20)
                    dbg_printf("[%2d] session_tick: can't kill session refcnt is %u (attempts %u)\n",
                            session->id, GET_REFCNT(&session->refcnt),
                            session->kill_attempts);
            } else if (session->will_at) {
                dbg_printf(BBLU"[%2d] session_tick: force_will"CRESET"\n",
                        session->id);
                goto force_will;
            } else {
                free_session(session, false);
                session = NULL;
            }
            continue;
        }

        if (session->client == NULL) { /* SESSION_ACTIVE */
            if (session->expires_at == 0 || now > session->expires_at) {
                dbg_printf("[%2d] session_tick: setting SESSION_DELETE refcnt is %u\n",
                        session->id, GET_REFCNT(&session->refcnt));

                if (session->will_at) {
                    dbg_printf(BBLU"[%2d] session_tick: force_will"CRESET"\n",
                            session->id);
                    goto force_will;
                }

#ifdef FEATURE_RAFT
                close_session(session, true);
#else
                close_session(session);
#endif
                session = NULL;
                continue;
            }
        }

        if (session->will_at && (now > session->will_at)) {
force_will:
            if (session->will_topic == NULL) {
                warnx("client_tick: session->will_topic is NULL");
                goto will_fail;
            }

            struct message *msg;
            /* a) Will Delay Interval or b) Session end is the trigger */
            if ((msg = register_message(session->will_topic->name,
                            session->will_payload_format,
                            session->will_payload_len,
                            session->will_payload,
                            session->will_qos,
                            session, session->will_retain, MSG_WILL)) == NULL) {
                warn("client_tick: register_message(will)");
                if (session->will_payload) {
                    free(session->will_payload);
                    session->will_payload = NULL;
                }
                goto will_fail;
            }
            msg->sender_status.completed_at = now;
            msg->sender_status.last_sent = now;
            msg->sender_status.accepted_at = now;
            msg->sender_status.released_at = now;

            /* avoid subsequent free in happy-path, as now in msg->payload */
            session->will_payload = NULL;
will_fail:
            if (session->will_payload)
                free(session->will_payload);
            session->will_payload = NULL;

            if (session->will_topic)
                DEC_REFCNT(&session->will_topic->refcnt);

            session->will_topic = NULL;
            session->will_at = 0;
            session->will_retain = false;
        }
    }
    pthread_rwlock_unlock(&global_sessions_lock);
}

static void packet_tick(void)
{
    pthread_rwlock_wrlock(&global_packets_lock);

    for (struct packet *next, *pkt = global_packet_list; pkt; pkt = next)
    {
        next = pkt->next;

        if (GET_REFCNT(&pkt->refcnt) == 0)
            free_packet(pkt, false, true);
    }

    pthread_rwlock_unlock(&global_packets_lock);
}

static void tick(void)
{
    if (num_sessions)
        session_tick();
    if (num_clients)
        client_tick();
    if (num_topics)
        topic_tick();
    if (num_messages)
        message_tick();
    if (num_packets)
        packet_tick();
}

#ifdef FEATURE_THREADS
static void *tick_loop(void * /* arg */)
{
    while (running)
    {
        const struct timespec req = {
            .tv_sec = 0,
            .tv_nsec = 100000000,
        };
        nanosleep(&req, NULL);

        tick();
    }
    logger(LOG_INFO, NULL, "tick_loop: terminated normally");

    return NULL;
}
#endif

#ifdef FEATURE_THREADS
static RETURN_TYPE openmetrics_loop(void *start_args)
{
    const int om_fd = ((const struct start_args *)start_args)->om_fd;

    while (running)
    {
        int fd, rc;
        fd_set fds_in;

        FD_ZERO(&fds_in);
        FD_SET(om_fd, &fds_in);

        struct timeval timeout = {
            .tv_sec = 1,
            .tv_usec = 0,
        };

        if ((rc = select(om_fd + 1, &fds_in, NULL, NULL, &timeout)) == -1) {
            if (errno == EINTR)
                continue;
            logger(LOG_WARNING, NULL, "openmetrics_loop: select: %s", strerror(errno));
            sleep(1);
        }

        if (rc == 0)
            continue;

        if ((fd = accept(om_fd, NULL, NULL)) == -1) {
            if (errno != EWOULDBLOCK && errno != EAGAIN && errno != EINTR)
                logger(LOG_WARNING, NULL, "openmetrics_loop: accept: %s", strerror(errno));
            sleep(1);
            continue;
        }

        if (openmetrics_export(fd) == -1)
            close_socket(&fd);
        else
            close_socket(&fd);
    }

    logger(LOG_INFO, NULL, "openmetrics_loop: terminated normally");

    return 0;
}
#endif

#ifdef FEATURE_RAFT
#endif

static RETURN_TYPE main_loop(void *start_args)
{
    fd_set fds_in, fds_out, fds_exc;
    const int mother_fd = ((const struct start_args *)start_args)->fd;
#ifdef FEATURE_OM
    const int om_fd = ((const struct start_args *)start_args)->om_fd;
#endif
#ifdef FEATURE_THREADS
    const pthread_t self = pthread_self();
#endif

    while (running)
    {
        int rc = 0;
        struct timeval tv;
        int max_fd = mother_fd;

#ifdef FEATURE_OM
        max_fd = MAX(mother_fd, om_fd);
#endif

        FD_ZERO(&fds_in);
        FD_ZERO(&fds_out);
        FD_ZERO(&fds_exc);

        if (mother_fd != -1)
            FD_SET(mother_fd, &fds_in);
#ifdef FEATURE_OM
        if (om_fd != -1)
            FD_SET(om_fd, &fds_in);
#endif
        has_clients = false;

        if (num_clients) {
            pthread_rwlock_rdlock(&global_clients_lock);
            for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
            {
                if (clnt->state == CS_NEW || clnt->state == CS_DISCONNECTED ||
                        clnt->fd == -1)
                    continue;

#ifdef FEATURE_THREADS
                if (clnt->owner != self)
                    continue;
#endif
                if (clnt->fd > max_fd)
                    max_fd = clnt->fd;

                /* We don't want to start parsing a new incoming packet,
                 * if we're still sending an outbound one
                 */
                if (clnt->po_buf == NULL)
                    FD_SET(clnt->fd, &fds_in);
                FD_SET(clnt->fd, &fds_out);
                FD_SET(clnt->fd, &fds_exc);

                has_clients = true;
            }
            pthread_rwlock_unlock(&global_clients_lock);
        }

        tv.tv_sec = 0;

        if (has_clients == false) {
            tv.tv_usec = 1000000;
            rc = select(max_fd + 1, &fds_in, NULL, NULL, &tv);
            tv.tv_sec = 0;
            tv.tv_usec = 500;
            select(max_fd + 1, NULL, &fds_out, &fds_exc, &tv);
        } else {
            tv.tv_usec = 5000;
            rc = select(max_fd + 1, &fds_in, NULL, &fds_exc, &tv);

            /* this is a kludge but not sure how else a) get a hint at blocked
             * writes and b) avoid select instantly returning (as any
             * non-blocking writable fd seems to terminate the select,
             * i.e. all of them */
            tv.tv_sec = 0;
            tv.tv_usec = 500;
            select(max_fd + 1, NULL, &fds_out, NULL, &tv);
        }

        if (rc == 0) {
            /* a timeout occured, but no fds */
#ifndef FEATURE_THREADS
            /* TODO we still need to check outbound data ... need a better solution
             * here. */
            /* goto tick_me; */
#endif
        } else if (rc == -1 && (errno == EAGAIN || errno == EINTR || errno == EBADF)) {
            if (errno == EBADF) {
                /* TODO check all fd's and close any connections that are
                 * EBADF to avoid constant looping */
                warn("main_loop");
            }
            /* TODO calculate the remaining time to sleep? */
            continue;
        } else if (rc == -1) {
            warn("main_loop: select");
#ifdef FEATURE_THREADS
            pthread_exit(&errno);
#else
            running = false;
            return -1;
#endif
        }

#ifdef FEATURE_OM
        if (om_fd > 0 && FD_ISSET(om_fd, &fds_in)) {
            int tmp_fd;

            if ((tmp_fd = accept(om_fd, NULL, NULL)) != -1) {
                if (openmetrics_export(&tmp_fd) == -1)
                    close_socket(&tmp_fd);
            }
        }
#endif

        if (mother_fd > 0) {
            if (FD_ISSET(mother_fd, &fds_in)) {
                struct sockaddr_in sin_client;
                socklen_t sin_client_len = sizeof(sin_client);
                int child_fd;
                struct client *new_client;

                FD_CLR(mother_fd, &fds_in);

                dbg_printf("     main_loop: new connection\n");
                if ((child_fd = accept(mother_fd,
                                (struct sockaddr *)&sin_client,
                                &sin_client_len)) == -1) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                        warn("main_loop: accept failed");
                        continue;
                    }
                }

                int flags;

                if ((flags = fcntl(child_fd, F_GETFL)) == -1) {
                    warn("main_loop: fcntl: F_GETFL");
                    goto shit_fd;
                }

                flags |= O_NONBLOCK;

                if (fcntl(child_fd, F_SETFL, flags) == -1) {
                    warn("main_loop: fcntl: F_SETFL");
                    goto shit_fd;
                }

                if ((new_client = alloc_client()) == NULL) {
shit_fd:
                    close_socket(&child_fd);
                    warn("main_loop: alloc_client");
                    continue;
                }

#ifdef FEATURE_THREADS
                new_client->owner = self;
#endif
                if (inet_ntop(AF_INET, &sin_client.sin_addr.s_addr,
                            new_client->hostname, sin_client_len) == NULL)
                    warn("inet_ntop");

                new_client->fd = child_fd;
                new_client->tcp_accepted_at = time(NULL);
                new_client->remote_port = ntohs(sin_client.sin_port);
                new_client->remote_addr = ntohl(sin_client.sin_addr.s_addr);

                dbg_printf("     main_loop: new client from [%s:%u]\n",
                        new_client->hostname, new_client->remote_port);
                logger(LOG_INFO, new_client, "main_loop: new connection");
                new_client->state = CS_ACTIVE;
            }
        }

        if (num_clients) {
            pthread_rwlock_rdlock(&global_clients_lock);
            for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
            {
                if (clnt->state != CS_ACTIVE || clnt->fd == -1)
                    continue;

#ifdef FEATURE_THREADS
                if (clnt->owner != self)
                    continue;
#endif
                if (clnt->fd == -1)
                    continue;

                if (FD_ISSET(clnt->fd, &fds_out)) {
                    clnt->write_ok = true;
                    /* socket is writable without blocking [ish] */
                } else {
                    clnt->write_ok = false;
                }

                if (clnt->fd == -1)
                    continue;

                if (FD_ISSET(clnt->fd, &fds_in)) {
                    if (parse_incoming(clnt) == -1) {
                        /* TODO do something? */ ;
                    }
                }

                if (clnt->fd == -1)
                    continue;

                if (FD_ISSET(clnt->fd, &fds_exc)) {
                    dbg_printf("     main_loop exception event on %p[%d]\n",
                            (void *)clnt, clnt->fd);
                    close_socket(&clnt->fd);
                    /* TODO close? */
                }
            }
            pthread_rwlock_unlock(&global_clients_lock);
        }

#ifndef FEATURE_THREADS
        tick();
#endif
    }

#ifdef FEATURE_DEBUG
    dump_all();
#endif
    logger(LOG_INFO, NULL, "main_loop: terminated normally");

    errno = 0;
#ifdef FEATURE_THREADS
    pthread_exit(&errno);
#else
    return 0;
#endif
}

static int load_message(datum /* key */, datum content)
{
    const struct message_save *save = (void *)content.dptr;
    struct message *msg;

    if ((msg = alloc_message(save->uuid)) == NULL)
        return -1;

    if (save->payload_len) {
        if ((msg->payload = malloc(save->payload_len)) == NULL)
            goto fail;
        memcpy((void *)msg->payload, save->payload, save->payload_len);
    }

    msg->format = save->format;
    msg->payload_len = save->payload_len;
    msg->qos = save->qos;
    msg->retain = save->retain;
    msg->type = save->type;

    msg->state = MSG_ACTIVE;

    dbg_printf("     open_databases: loaded saved message with uuid=%s\n",
            uuid_to_string(msg->uuid));

    return 0;
fail:
    if (msg) {
        msg->state = MSG_DEAD;
        free_message(msg, true);
    }
    return -1;
}

static bool is_null_uuid(const uint8_t uuid[static 16])
{
    for (unsigned idx = 0; idx < 16; idx++)
        if (uuid[idx] != 0)
            return false;
    return true;
}

static int load_topic(datum /* key */, datum content)
{
    const struct topic_save *save = (void *)content.dptr;
    struct topic *topic = NULL;

    if (find_topic((void *)save->name, false, true) != NULL) {
        logger(LOG_WARNING, NULL, "open_databases: duplicate topic for %s",
                save->name);
        errno = EEXIST;
        return -1;
    }

#ifdef FEATURE_RAFT
    if ((topic = register_topic((void *)save->name, save->uuid, false)) == NULL)
#else
        if ((topic = register_topic((void *)save->name, save->uuid)) == NULL)
#endif
            return -1;

    if (!is_null_uuid(save->retained_message_uuid)) {
        dbg_printf("     open_databases: retained_message_uuid=%s\n",
                uuid_to_string(save->retained_message_uuid));
        if ((topic->retained_message =
                    find_message_by_uuid(save->retained_message_uuid)) == NULL)
            logger(LOG_WARNING, NULL,
                    "open_databases: unable to find retained message for topic <%s>",
                    topic->name);
        else {
            topic->retained_message->topic = topic;
            INC_REFCNT(&topic->retained_message->refcnt);
            INC_REFCNT(&topic->refcnt);
            dbg_printf("     load_topic: set retained message\n");
        }
    }

    topic->state = TOPIC_ACTIVE;

    logger(LOG_INFO, NULL,
            "open_databases: registered previously saved topic <%s>",
            topic->name);

    return 0;
}

static int open_databases(void)
{
    datum tmp_key, tmp_content;
    DBM *dbm;

    for (unsigned idx = 0; database_init[idx].filename; idx++)
    {
        if ((dbm = dbm_open(database_init[idx].filename,
                        O_RDWR|O_CREAT, S_IRUSR|S_IWUSR)) == NULL)
            goto fail;

        *database_init[idx].global = dbm;

        tmp_key = dbm_firstkey(dbm);
        while (tmp_key.dptr)
        {
            tmp_content = dbm_fetch(dbm, tmp_key);

            if (tmp_content.dptr == NULL)
                goto skip;

            /* TODO message_save.payload is variable so < not != but this then
             * makes others more error prone ... */
            if ((size_t)tmp_content.dsize < database_init[idx].size) {
                logger(LOG_ERR, NULL,
                        "open_databases: content.dsize mismatch in <%s> (got %d, expected %lu)",
                        database_init[idx].filename,
                        tmp_content.dsize,
                        database_init[idx].size);
                errno = EIO;
                goto fail;
            }

            if (database_init[idx].func(tmp_key, tmp_content) == -1) {
                logger(LOG_WARNING, NULL,
                        "open_databases: <%s>.func failed: %s",
                        database_init[idx].filename,
                        strerror(errno));
                goto skip;
            }
skip:
            tmp_key = dbm_nextkey(dbm);
        }
    }

    return 0;

fail:
    close_databases();
    return -1;
}

/*
 * external functions
 */

int main(int argc, char *argv[])
{
    errno = 0;

    srand(time(NULL) ^ getpid());
    srandom(rand());

    /* Set defaults */

    opt_listen.s_addr = htonl(INADDR_LOOPBACK);
    opt_statepath = DEF_DB_PATH;
#ifdef FEATURE_OM
    opt_om_listen.s_addr = htonl(INADDR_LOOPBACK);
#endif
#ifdef FEATURE_RAFT
    opt_raft_listen.s_addr = htonl(INADDR_LOOPBACK);
#endif

    parse_cmdline(argc, argv);

    /* cmd line logic checks */

#ifdef FEATURE_RAFT
    if (opt_raft) {
        if (opt_raft_id == 0) {
            warnx("raft id missing");
            goto shit_usage;
        }
    }
#endif

    if (logfile_name) {
        if ((opt_logfile = fopen(logfile_name,
                        opt_logfileappend ? "a" : "w")) == NULL)
            errx(EXIT_FAILURE, "fopen(%s)", logfile_name);
        if (opt_logfilesync)
            setvbuf(opt_logfile, NULL, _IONBF, 0);
        atexit(close_logfile);
    } else if (opt_logfilesync || opt_logfileappend) {
        warnx("Invalid options without -l file=FILE provided");
        goto shit_usage;
    }

    if (opt_background && opt_logstdout)
        errx(EXIT_FAILURE, "cannot log to stdout when a daemon");

    if (opt_logsyslog)
        openlog("fail-mqttd", LOG_PID, LOG_DAEMON);

    setvbuf(stdin, NULL, _IONBF, 0);
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    if (opt_background) {
        pid_t child1, child2;
        int filedes[2];
        char buf;
        int child_pipe_fd, parent_pipe_fd;

        /* Parent Process */
        if (pipe(filedes) == -1)
            logger(LOG_EMERG, NULL, "main: pipe: %s", strerror(errno));

        parent_pipe_fd = filedes[0];
        child_pipe_fd = filedes[1];

        if ((child1 = fork()) == 0) {
            /* Parent Process */
            close(child_pipe_fd);

            if ((read(parent_pipe_fd, &buf, 1)) == -1)
                logger(LOG_EMERG, NULL, "main: pipe read: %s", strerror(errno));

            close(parent_pipe_fd);
            exit(EXIT_SUCCESS);
        }

        if (child1 == -1)
            logger(LOG_EMERG, NULL, "main: fork1: %s", strerror(errno));

        /* Child 1 Process */
        setsid();

        if ((child2 = fork()) > 0) {
            /* Child 1 Process */
            exit(EXIT_SUCCESS);
        } else if (child2 == -1) {
            logger(LOG_EMERG, NULL, "main: fork2: %s", strerror(errno));
        }

        /* Child 2 Process */
        fclose(stdout);
        fclose(stdin);
        fclose(stderr);

        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        close(parent_pipe_fd);

        open("/dev/null", O_RDONLY);
        open("/dev/null", O_WRONLY);
        open("/dev/null", O_WRONLY);

        umask(0022);
        atexit(clean_pid);

        FILE *pid_file;
        if ((pid_file = fopen(PID_FILE, "w")) != NULL) {
            fprintf(pid_file, "%u", getpid());
            fclose(pid_file);
            logger(LOG_NOTICE, NULL, "main: created PID file <%s>", PID_FILE);
        } else {
            logger(LOG_WARNING, NULL, "main: cannot open PID file <%s>: %s",
                    PID_FILE,
                    strerror(errno));
        }

        buf = 'X';
        if (write(child_pipe_fd, &buf, 1) == -1)
            logger(LOG_EMERG, NULL, "main: pipe write: %s", strerror(errno));
        close(child_pipe_fd);

        logger(LOG_INFO, NULL, "main: successfully daemonised");
    }

    /* Signal handling */

    struct sigaction sa = {
        .sa_sigaction = sh_sigint,
        .sa_flags = SA_SIGINFO,
    };

    if (sigaction(SIGINT, &sa, NULL) == -1)
        logger(LOG_EMERG, NULL, "sigaction(SIGINT): %s", strerror(errno));
    if (sigaction(SIGQUIT, &sa, NULL) == -1)
        logger(LOG_EMERG, NULL, "sigaction(SIGQUIT): %s", strerror(errno));
    if (sigaction(SIGTERM, &sa, NULL) == -1)
        logger(LOG_EMERG, NULL, "sigaction(SIGTERM): %s", strerror(errno));
    if (sigaction(SIGHUP, &sa, NULL) == -1)
        logger(LOG_EMERG, NULL, "sigaction(SIGHUP): %s", strerror(errno));
    if (sigaction(SIGALRM, &sa, NULL) == -1)
        logger(LOG_EMERG, NULL, "sigaction(SIGALRM): %s", strerror(errno));

    sa.sa_flags = 0;
    sa.sa_handler = SIG_IGN;

    if (sigaction(SIGPIPE, &sa, NULL) == -1)
        logger(LOG_EMERG, NULL, "sigaction(SIGPIPE): %s", strerror(errno));

    if (opt_database) {
        if (open_databases() == -1)
            logger(LOG_EMERG, NULL, "open_databases: %s", strerror(errno));
        atexit(close_databases);
    }

    atexit(close_all_sockets);
    atexit(free_all_topics_two);
    atexit(free_all_messages_two);
    atexit(free_all_sessions);
    atexit(free_all_messages);
    atexit(free_all_topics);
    atexit(free_all_packets);
    atexit(free_all_clients);
    atexit(free_all_message_delivery_states);

    if (opt_database) {
        atexit(save_all_topics);
    }

    if (opt_database
#ifdef FEATURE_RAFT
            || opt_raft
#endif
            ) {
        if (get_first_hwaddr(global_hwaddr, sizeof(global_hwaddr)) == -1)
            logger(LOG_EMERG, NULL, "main: cannot find MAC address");

        logger(LOG_INFO, NULL,
                "main: using hardware address %02x:%02x:%02x:%02x:%02x:%02x",
                global_hwaddr[0], global_hwaddr[1], global_hwaddr[2],
                global_hwaddr[3], global_hwaddr[4], global_hwaddr[5]
              );
    }

    /* if any topic names have been passed on the cmdline, process them */

    while (optind < argc)
    {
        if (is_valid_topic_filter((const uint8_t *)argv[optind]) == -1) {
            logger(LOG_WARNING, NULL,
                    "main: command line topic creation: <%s> is not a valid topic filter, skipping",
                    argv[optind]);
        } else if (find_topic((const uint8_t *)argv[optind], false, true) != NULL) {
            logger(LOG_WARNING, NULL,
                    "main: command line topic creation: topic <%s> already exists, skipping",
                    argv[optind]);
#ifdef FEATURE_RAFT
        } else if (register_topic((const uint8_t *)argv[optind], NULL, false) == NULL) {
#else
        } else if (register_topic((const uint8_t *)argv[optind], NULL) == NULL) {
#endif
            logger(LOG_WARNING, NULL,
                    "main: command line topic creation: register_topic<%s> failed, skipping: %s",
                    argv[optind], strerror(errno));
        } else {
            logger(LOG_INFO, NULL,
                    "main: command line topic creation: topic <%s> created",
                    argv[optind]);
        }
        optind++;
    }

    /* Set-up sockets */

    if ((global_mother_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
        logger(LOG_EMERG, NULL, "socket(mother): %s", strerror(errno));

    sock_linger(global_mother_fd);
    sock_reuse(global_mother_fd, 1);
    sock_nonblock(global_mother_fd);

#ifdef FEATURE_OM
    if (opt_openmetrics) {
        if ((global_om_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
            logger(LOG_EMERG, NULL, "socket(om): %s", strerror(errno));
        sock_linger(global_om_fd);
        sock_reuse(global_om_fd, 1);
        sock_nonblock(global_om_fd);
    }
#endif

#ifdef FEATURE_RAFT
#endif

    struct sockaddr_in sin = {0};
    char bind_addr[INET_ADDRSTRLEN];

    /* Mother FD */
    sin.sin_family = AF_INET;
    sin.sin_port = htons(opt_port);
    sin.sin_addr.s_addr = opt_listen.s_addr;
    inet_ntop(AF_INET, &sin.sin_addr, bind_addr, sizeof(bind_addr));
    logger(LOG_NOTICE, NULL, "main: binding to %s:%u", bind_addr, opt_port);
    dbg_printf("     main: binding to %s:%u\n", bind_addr, opt_port);

    if (bind(global_mother_fd, (struct sockaddr *)&sin, sizeof(sin)) == -1)
        logger(LOG_EMERG, NULL, "bind(mother): %s", strerror(errno));

    if (listen(global_mother_fd, opt_backlog) == -1)
        logger(LOG_EMERG, NULL, "listen(mother): %s", strerror(errno));

#ifdef FEATURE_OM
    /* Openmetrics FD */
    if (opt_openmetrics) {
        sin.sin_family = AF_INET;
        sin.sin_port = htons(opt_om_port);
        sin.sin_addr.s_addr = opt_om_listen.s_addr;
        inet_ntop(AF_INET, &sin.sin_addr, bind_addr, sizeof(bind_addr));
        logger(LOG_NOTICE, NULL, "main: openmetrics binding to %s:%u",
                bind_addr, opt_om_port);

        if (bind(global_om_fd, (struct sockaddr *)&sin, sizeof(sin)) == -1)
            logger(LOG_EMERG, NULL, "bind(om): %s", strerror(errno));

        if (listen(global_om_fd, 5) == -1)
            logger(LOG_EMERG, NULL, "listen(om): %s", strerror(errno));
    }
#endif

#ifdef FEATURE_THREADS
    struct start_args start_args0 = {
        .fd = global_mother_fd, .om_fd = -1,
    };

    struct start_args start_argsn = {
        .fd = -1, .om_fd = -1,
    };

    struct start_args start_args_om = {
        .fd = -1,
# ifdef FEATURE_OM
        .om_fd = opt_openmetrics ? global_om_fd : -1,
# else
        .om_fd = -1,
# endif
    };
#else /* #ifdef FEATURE_THREADS */
    struct start_args start_args = {
        .fd = global_mother_fd,
# ifdef FEATURE_OM
        .om_fd = opt_openmetrics ? global_om_fd : -1,
# else
        .om_fd = -1,
# endif
    };
#endif /* #ifdef FEATURE_THREADS */
    running = true;

#ifdef FEATURE_RAFT
    pthread_t raft_thread = 0;
    extern const struct raft_impl mqtt_raft_impl;

    if (opt_raft &&
            pthread_create(&raft_thread, NULL, raft_loop, (void *)&mqtt_raft_impl) != 0)
        err(EXIT_FAILURE, "pthread_create: raft_loop");
#endif

#ifdef FEATURE_THREADS
# define NUM_THREADS 16
    pthread_t main_thread[NUM_THREADS], tick_thread[NUM_THREADS], om_thread;

    if (opt_openmetrics &&
            pthread_create(&om_thread, NULL, openmetrics_loop, &start_args_om) != 0)
        err(EXIT_FAILURE, "pthread_create: openmetrics_loop");

    for (unsigned idx = 0; idx < NUM_THREADS; idx++)
        if (pthread_create(&main_thread[idx], NULL, main_loop,
                    idx == 0 ? &start_args0 : &start_argsn) == -1)
            err(EXIT_FAILURE, "pthread_create: main_thread[%u]", idx);

    for (unsigned idx = 0; idx < NUM_THREADS; idx++)
        if (pthread_create(&tick_thread[idx], NULL, tick_loop, NULL) != 0)
            err(EXIT_FAILURE, "pthread_create: tick[%u]", idx);

# ifdef FEATURE_OM
    if (opt_openmetrics)
        pthread_join(om_thread, NULL);
# endif
# ifdef FEATURE_RAFT
    if (opt_raft)
        pthread_join(raft_thread, NULL);
# endif

    for (unsigned idx = 0; idx < NUM_THREADS; idx++)
        if (pthread_join(tick_thread[idx], NULL) == -1)
            warn("main: pthread_join(tick_thread)");
        else
            logger(LOG_INFO, NULL, "main: pthread_join(tick_thread[%u]): OK", idx);

    for (unsigned idx = 0; idx < NUM_THREADS; idx++)
        if (pthread_join(main_thread[idx], NULL) == -1)
            warn("main: pthread_join(tick_thread)");
        else
            logger(LOG_INFO, NULL, "main: pthread_join(main_thread[%u]): OK", idx);
#else /* FEATURE_THREADS */

    if (main_loop(&start_args) == -1)
        logger(LOG_EMERG, NULL, "main_loop returned an error: %s", strerror(errno));
# ifdef FEATURE_RAFT
    if (opt_raft)
        pthread_join(raft_thread, NULL);
# endif
#endif /* FEATURE_THREADS */

    logger(LOG_INFO, NULL, "main: done");
    exit(EXIT_SUCCESS);

shit_usage:
    show_usage(stderr, argv[0]);
    exit(EXIT_FAILURE);
}
