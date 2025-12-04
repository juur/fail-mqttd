#define _XOPEN_SOURCE 800
#include "config.h"

#include <stdlib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <err.h>
#include <stdio.h>
#include <string.h>
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

#include "mqtt.h"

#ifdef NDEBUG
# define dbg_printf(...)
#else
# define dbg_printf(...) printf(__VA_ARGS__)
#endif

#ifndef NDEBUG
# define CRESET "\x1b[0m"
# define BBLK "\x1b[1;30m"
# define BRED "\x1b[1;31m"
# define NRED "\x1b[0;31m"
# define BGRN "\x1b[1;32m"
# define NGRN "\x1b[0;32m"
# define BYEL "\x1b[1;33m"
# define NYEL "\x1b[0;33m"
# define BBLU "\x1b[1;34m"
# define BMAG "\x1b[1;35m"
# define BCYN "\x1b[1;36m"
# define NCYN "\x1b[1;36m"
# define BWHT "\x1b[1;37m"
#endif

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

#ifdef WITH_THREADS
# define RETURN_TYPE void *
#else
# define RETURN_TYPE int
#endif

typedef int (*control_func_t)(struct client *, struct packet *, const void *);

/*
 * misc. globals
 */

static int global_mother_fd = -1;
static int global_om_fd = -1;
static _Atomic bool running;
static uint8_t global_hwaddr[8];

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

static const unsigned  MAX_PACKETS           = 256;
static const unsigned  MAX_CLIENTS           = 64;
static const unsigned  MAX_TOPICS            = 1024;
static const unsigned  MAX_MESSAGES          = 16384;
static const unsigned  MAX_PACKET_LENGTH     = 0x1000000U;
static const unsigned  MAX_MESSAGES_PER_TICK = 100;
static const unsigned  MAX_PROPERTIES        = 32;
static const unsigned  MAX_RECEIVE_PUBS      = 8;
static const unsigned  MAX_SESSIONS          = 128;

static const uint64_t  UUID_EPOCH_OFFSET     = 0x01B21DD213814000ULL;

static const char     *const PID_FILE        = RUNSTATEDIR "/fail-mqttd.pid";


/*
 * global lists and associated locks & counts
 */

static pthread_rwlock_t global_clients_lock  = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_sessions_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_messages_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_packets_lock  = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_topics_lock   = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t global_mds_lock      = PTHREAD_RWLOCK_INITIALIZER;

static struct client *global_client_list              = NULL;
static struct message *global_message_list            = NULL;
static struct packet *global_packet_list              = NULL;
static struct topic *global_topic_list                = NULL;
static struct session *global_session_list            = NULL;
static struct message_delivery_state *global_mds_list = NULL;

static unsigned num_clients  = 0;
static unsigned num_messages = 0;
static unsigned num_packets  = 0;
static unsigned num_topics   = 0;
static unsigned num_sessions = 0;
static unsigned num_mds      = 0;

/*
 * databases
 */

static DBM *topic_dbm = NULL;
static DBM *message_dbm = NULL;

/*
 * command line options
 */

static   FILE       *opt_logfile       = NULL;
static   bool        opt_logstdout     = true;
static   in_port_t   opt_port          = 1883;
static   in_port_t   opt_om_port       = 1773;
static   int         opt_backlog       = 50;
static   int         opt_loglevel      = LOG_INFO;
static   bool        opt_logsyslog     = false;
static   bool        opt_logfileappend = false;
static   bool        opt_background    = false;
static   bool        opt_openmetrics   = false;

static struct in_addr opt_listen;
static struct in_addr opt_om_listen;

/*
 * forward declarations
 */

[[gnu::nonnull]] static int unsubscribe(struct subscription *sub);
[[gnu::nonnull]] static int dequeue_message(struct message *msg);
[[gnu::nonnull]] static void free_message(struct message *msg, bool need_lock);
[[gnu::nonnull]] static int remove_delivery_state(
        struct message_delivery_state ***state_array, unsigned *array_length,
        struct message_delivery_state *rem);
[[gnu::nonnull]] static void free_message_delivery_state(struct message_delivery_state **mds);
[[gnu::nonnull(3),gnu::format(printf,3,4)]] static void logger(int priority, const struct client *client, const char *format, ...);
[[gnu::nonnull(1),gnu::warn_unused_result]] static struct topic *register_topic(const uint8_t *name, const uint8_t uuid[const UUID_SIZE]);
[[gnu::nonnull, gnu::warn_unused_result]] static int unsubscribe_from_topics(struct session *session, const struct topic_sub_request *request);
static int unsubscribe_session_from_all(struct session *session);

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
    fprintf(fp, "fail-mqttd " VERSION "\n" "\n" "Written by http://github.com/juur");
}

/**
 * show usage information to the specified file
 * @param fp FILE to output to.
 * @param name typically argv[0] from main() to display
 */
[[gnu::nonnull]]
static void show_usage(FILE *fp, const char *name)
{
    fprintf(fp, "fail-mqttd -- a terrible implementation of MQTT\n" "\n"
            "Usage: %s [-hV] [-H ADDR] [-p PORT] [-l LOGOPTION,[LOGOPTION..]] [TOPIC..]\n"
            "Provides a MQTT broker, "
            "pre-creating topics per additional command line arguments, "
            "if provided.\n"
            "\n"
            "Options:\n"
            "  -h             show help\n"
            "  -H ADDR        bind to IP address ADDR (default 127.0.0.1)\n"
            "  -l LOGOPTION   comma separated suboptions, described below\n"
            "  -p PORT        bind to TCP port PORT (default 1883)\n"
            "  -d             daemonize and create a PID file\n"
            "  -V             show version\n"
            "\n"
            "Each LOGOPTION may be:\n"
            "  syslog         log to syslog as LOG_DAEMON\n"
            "  [no]stdout     [don't] log to stdout (default yes)\n"
            "  file=PATH      log to given PATH\n"
            "  append         open PATH log file in append mode\n"
            "\n"
            "The default is stdout.\n"
            "\n",
            name);
}

/*
 * debugging helpers
 */

static int _log_io_error(const char *msg, ssize_t rc, ssize_t expected, bool die,
        const char *file, const char *func, int line)
{
    static const char *const read_error_fmt = "%s: read error at %s:%u: %s";
    static const char *const short_read_fmt = "%s: short read (%lu < %lu) at %s:%u: %s";

    if (rc == -1) {
        if (die)
            err(EXIT_FAILURE, read_error_fmt, func, file, line, msg ? msg : "");

        warn(read_error_fmt, func, file, line, msg ? msg : "");
        return -1;
    }

    if (die)
        errx(EXIT_FAILURE, short_read_fmt, func, rc, expected, file, line, msg ? msg : "");

    warnx(short_read_fmt, func, rc, expected, file, line, msg ? msg : "");

    errno = ERANGE;
    return -1;
}
#define log_io_error(m,r,e,d) _log_io_error(m,r,e,d,__FILE__,__func__,__LINE__)

#ifndef NDEBUG
static const char *uuid_to_string(const uint8_t uuid[const static UUID_SIZE])
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

[[maybe_unused]]
static void dump_topics(void)
{
    dbg_printf("   global_topics:\n");
    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
    {
        dbg_printf("  topic: <%s>\n", (char *)tmp->name);
    }
    pthread_rwlock_unlock(&global_topics_lock);
}

static void dump_clients(void)
{
    pthread_rwlock_rdlock(&global_clients_lock);
    for (struct client *client = global_client_list; client; client = client->next)
    {
        dbg_printf("{\"id\": %u, \"session\": {\"id\": %u}, \"client-id\": \"%s\"}%s\n",
                client->id,
                client->session->id,
                client->client_id,
                client->next ? "," : ""
              );
    }
    pthread_rwlock_unlock(&global_clients_lock);
}

/*
 * UUID helpers
 */

static int get_first_hwaddr(uint8_t out[static 6], size_t out_length)
{
    struct ifaddrs *ifaddr;
    int copy_len;
    const struct sockaddr_ll *sock;

    if (getifaddrs(&ifaddr) == -1) {
        logger(LOG_WARNING, NULL, "get_first_hwaddr: getifaddrs: %s", strerror(errno));
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
        memcpy(out, sock->sll_addr, copy_len);

        freeifaddrs(ifaddr);
        return copy_len;
    }

    freeifaddrs(ifaddr);
    errno = ENOENT;
    return -1;
}

static int generate_uuid(uint8_t hwaddr[const static 6], uint8_t out[UUID_SIZE])
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

    srand(time(NULL) ^ getpid());
    uint16_t rnd = (uint16_t)(rand() & 0x3fff);

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

    memcpy(&out[10], hwaddr, 6);

    return 0;
}

/*
 * Open Metrics
 */

static const char *get_http_error(int error)
{
    switch (error)
    {
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 501: return "Not Implemented";
        case 505: return "HTTP Version Not Supported";
    }

    return "unknown";
}

static void http_error(FILE *out, int error)
{
    fprintf(out,
            "HTTP/1.1 %u %s\r\n"
            "connection: close"
            "content-length: 0"
            "\r\n"
            ,
            error,
            get_http_error(error)
           );
}

static int openmetrics_export(int fd)
{
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

    sin_len = sizeof(sin);

    if (getpeername(fd, (struct sockaddr *)&sin, &sin_len) == -1) {
        logger(LOG_WARNING, NULL, "openmetrics_export: getpeername: %s", strerror(errno));
        return -1;
    }

    inet_ntop(AF_INET, &sin.sin_addr, remote_addr, sizeof(remote_addr));
    logger(LOG_INFO, NULL, "openmetrics_export connection from %s:%u", remote_addr, htons(sin.sin_port));

    if ((in = fdopen(fd, "r+b")) == NULL)
        goto fail;

    setvbuf(in, NULL, _IONBF, 0);

    alarm(5);
    while ((nread = getline(&line, &line_len, in)) != -1)
    {
        if (nread == 0)
            break;

        if (nread > 4096 || num_lines > 50) {
            goto fail;
        }

        if (nread == 2 && !memcmp(line, "\r\n", 2))
            break;

        if (num_lines == 0) {
            rc = sscanf(line, "%m[A-Z] %ms HTTP/%m[0-9.]", &http_method, &http_uri, &http_version);

            if (rc != 3) {
                http_error(in, 400);
                goto fail;
            }

            if (strcmp("GET", http_method) && strcmp("HEAD", http_method)) {
                http_error(in, 501);
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

            if (!strcmp("HEAD", http_method))
                head_request = true;

            free(http_method);
            free(http_version);
            free(http_uri);

            http_method = NULL;
            http_version = NULL;
            http_uri = NULL;
        }

        num_lines++;
    }
    alarm(0);

    if (line) {
        free(line);
        line = NULL;
    }

    if (nread == -1 || num_lines == 0) {
        goto fail;
    }

    const char *const datefmt = "%a, %d %b %Y %T %Z";

    now = time(NULL);
    tm = gmtime(&now);
    strftime(date, sizeof(date), datefmt, tm);

    const char *const http_response =
        "HTTP/1.1 200 OK\r\n"
        "content-type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n"
        "connection: close\r\n"
        "date: %s\r\n"
        "content-length: %lu\r\n"
        "\r\n";

    if ((mem = open_memstream(&buffer, &size)) == NULL) {
        logger(LOG_WARNING, NULL, "openmetrics_export: unable to open_memstream: %s", strerror(errno));
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
            ,
            num_sessions,
            num_clients,
            num_messages,
            num_topics,
            num_mds,
            num_packets
           );

    fprintf(mem, "# TYPE topic_subscribers gauge\n");
    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *topic = global_topic_list; topic; topic = topic->next)
    {
        fprintf(mem,
                "topic_subscribers{topic_name=\"%s\",topic_id=\"%u\"} %u\n"
                ,
                topic->name,
                topic->id,
                topic->num_subscribers
               );
    }
    pthread_rwlock_unlock(&global_topics_lock);

    fprintf(mem, "# EOF\n");

    fclose(mem);
    mem = NULL;

    len = snprintf(hdrbuf, sizeof(hdrbuf), http_response, date, size);

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

static void dump_all(void)
{
    dbg_printf("{\"clients\":[\n");
    dump_clients();
    dbg_printf("]}\n");
}

static void logger(int priority, const struct client *client, const char *format, ...)
{
    static const char *fmt = "%s %s [%d]: <%s> %s\n";

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
        fprintf(stdout, fmt, timebuf, priority_str[priority], pid, clientbuf, linebuf);

    if (opt_logfile != NULL)
        fprintf(opt_logfile, fmt, timebuf, priority_str[priority], pid, clientbuf, linebuf);

    if (opt_logsyslog) {
        linebuf[strlen(linebuf)] = '\0';
        syslog(priority, "%s", linebuf);
    }

    if (priority == LOG_EMERG)
        exit(EXIT_FAILURE);
}

/*
 * allocators / deallocators
 */

[[gnu::nonnull]]
static int mds_detach_and_free(struct message_delivery_state *mds, bool session_lock, bool message_lock)
{
    int rc = 0;

    if (mds->message) {
        if (message_lock)
            pthread_rwlock_wrlock(&mds->message->delivery_states_lock);

        if (remove_delivery_state(&mds->message->delivery_states,
                    &mds->message->num_message_delivery_states, mds) == -1) {
            warn("free_topic: remove_delivery_state(message)");
            rc = -1;
        }

        if (message_lock)
            pthread_rwlock_unlock(&mds->message->delivery_states_lock);

        DEC_REFCNT(&mds->message->refcnt); /* alloc_message_delivery_state */
        mds->message = NULL;
    }

    if (mds->session) {
        if (session_lock)
            pthread_rwlock_wrlock(&mds->session->delivery_states_lock);

        if (remove_delivery_state(&mds->session->delivery_states,
                    &mds->session->num_message_delivery_states, mds) == -1) {
            warn("free_topic: remove_delivery_state(session)");
            rc = -1;
        }

        if (session_lock)
            pthread_rwlock_unlock(&mds->session->delivery_states_lock);

        DEC_REFCNT(&mds->session->refcnt); /* alloc_message_delivery_state */
        mds->session = NULL;
    }

    free_message_delivery_state(&mds);

    return rc;
}

[[gnu::nonnull]]
static void close_socket(int *fd)
{
    if (*fd != -1) {
        shutdown(*fd, SHUT_RDWR);
        close(*fd);
        *fd = -1;
    }
}

[[gnu::nonnull]]
static void free_subscription(struct subscription *sub)
{
    dbg_printf("     free_subscription:  id=%u type=%s topic=%d <%s>\n",
            sub->id,
            subscription_type_str[sub->type],
            sub->topic ? sub->topic->id : (id_t)-1,
            sub->topic ? (char *)sub->topic->name : "");

    switch (sub->type)
    {
        case SUB_SHARED:
            free(sub->sessions);
            sub->num_sessions = 0;
            sub->sessions = NULL;
            break;

        default:
            break;
    }
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

    if (request->topic_refs) {
        free(request->topic_refs);
        request->topic_refs = NULL;
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
    dbg_printf("     free_topic: id=%u <%s> refcnt=%u\n",
            topic->id,
            (topic->name == NULL) ? "" : (char *)topic->name,
            GET_REFCNT(&topic->refcnt)
            );

    /* used in free_all_topics() to allow almost-dead topics to persist due
     * to dangling references in session->will_topic */
    topic->state = TOPIC_DEAD;

    /* TODO check if we should have a wrlock here,
     * not inside unsubscribe_from_topics */
    if (topic->subscribers) {
        dbg_printf("     free_topic: subscribers=%p num_subscribers=%u\n",
                (void *)topic->subscribers, topic->num_subscribers);

        /* keep going, but restart if we unsubscribe as the array
         * will be modified */
        while (topic->num_subscribers && topic->subscribers)
        {
            for(unsigned idx = 0; idx < topic->num_subscribers; idx++)
            {
                if (topic->subscribers[idx] == NULL)
                    continue;

                dbg_printf("     free_topic: subscriber[%u] <%s> in <%s>\n",
                        idx,
                        topic->subscribers[idx]->session->client_id,
                        topic->subscribers[idx]->topic->name
                        );

                /* TODO should we handle the return code ? */
                (void)unsubscribe(topic->subscribers[idx]);
            }
        }

        /* Not sure this locking is useful */
        pthread_rwlock_wrlock(&topic->subscribers_lock);
        free(topic->subscribers);
        topic->subscribers = NULL;
        pthread_rwlock_unlock(&topic->subscribers_lock);
    }

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

    if (topic->retained_message) {
        dbg_printf("     free_topic: freeing retained_message\n");
        struct message *msg = topic->retained_message;

        pthread_rwlock_wrlock(&msg->delivery_states_lock);
        for (unsigned idx = 0; idx < msg->num_message_delivery_states; idx++)
        {
            if (msg->delivery_states[idx] == NULL)
                continue;

            mds_detach_and_free(msg->delivery_states[idx], true, false);
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
    if (GET_REFCNT(&topic->refcnt) == 0) {
        pthread_rwlock_wrlock(&global_topics_lock);
        if (global_topic_list == topic) {
            global_topic_list = topic->next;
        } else for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
        {
            if (tmp->next == topic) {
                tmp->next = topic->next;
                break;
            }
        }
        pthread_rwlock_unlock(&global_topics_lock);
        topic->next = NULL;
    }

    if (GET_REFCNT(&topic->refcnt) > 0)
        return;

    if (topic->name) {
        free((void *)topic->name);
        topic->name = NULL;
    }

    pthread_rwlock_destroy(&topic->pending_queue_lock);
    pthread_rwlock_destroy(&topic->subscribers_lock);

    num_topics--;
    free(topic);
}

[[gnu::nonnull]]
static void free_properties(
        struct property (*props)[], unsigned count)
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

[[gnu::nonnull]]
static void free_message_delivery_state(struct message_delivery_state **mds)
{
    assert(*mds != NULL);

    dbg_printf("     free_message_delivery_state: id=%u\n",
            (*mds)->id);

    pthread_rwlock_wrlock(&global_mds_lock);
    if (global_mds_list == *mds) {
        global_mds_list = (*mds)->next;
    } else for (struct message_delivery_state *tmp = global_mds_list;
            tmp; tmp = tmp->next)
    {
        if (tmp->next == *mds) {
            tmp->next = (*mds)->next;
            break;
        }
    }
    (*mds)->next = NULL;
    pthread_rwlock_unlock(&global_mds_lock);

    assert((*mds)->session == NULL);
    assert((*mds)->message == NULL);

    free(*mds);
    *mds = NULL;
    num_mds--;
}

[[gnu::nonnull]]
static void free_delivery_states(pthread_rwlock_t *lock, unsigned *num, struct message_delivery_state ***msgs)
{
    if (*msgs == NULL)
        return;

    pthread_rwlock_wrlock(lock);
    for (unsigned idx = 0; idx < *num; idx++) {
        if (*msgs[idx] == NULL)
            continue;
        mds_detach_and_free(*msgs[idx], false, false);
        *msgs[idx] = NULL;
    }
    pthread_rwlock_unlock(lock);

    free(*msgs);
    *msgs = NULL;
    *num = 0;
}

[[gnu::nonnull]]
static void free_packet(struct packet *pck, bool need_lock, bool need_owner_lock)
{
    struct packet *tmp;
    unsigned lck;

    dbg_printf("     free_packet: id=%u owner=%u <%s> owner.session=%d <%s> refcnt=%u\n",
            pck->id,
            pck->owner ? pck->owner->id : 0,
            pck->owner ? (char *)pck->owner->client_id : "",
            (pck->owner && pck->owner->session) ? pck->owner->session->id : 0,
            (pck->owner && pck->owner->session) ? (char *)pck->owner->session->client_id : "",
            GET_REFCNT(&pck->refcnt)
            );

    if ((lck = GET_REFCNT(&pck->refcnt)) > 0) {
        warnx("free_packet: attempt to free packet with refcnt=%u", lck);
        abort();
        return;
    }

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

    if (need_lock)
        pthread_rwlock_wrlock(&global_packets_lock);
    if (pck == global_packet_list) {
        global_packet_list = pck->next;
    } else for (tmp = global_packet_list; tmp; tmp = tmp->next)
    {
        if (tmp->next == pck) {
            tmp->next = pck->next;
            break;
        }
    }
    if (need_lock)
        pthread_rwlock_unlock(&global_packets_lock);
    pck->next = NULL;

    if (pck->payload) {
        free(pck->payload);
        pck->payload = NULL;
    }

    /*if (pck->will_props) {
        free_properties(pck->will_props, pck->num_will_props);

        pck->num_will_props = 0;
        pck->will_props = NULL;
    }*/

    if (pck->properties) {
        free_properties(pck->properties, pck->property_count);

        pck->property_count = 0;
        pck->properties = NULL;
    }

    if (pck->message) {
        DEC_REFCNT(&pck->message->refcnt);
        pck->message = NULL;
    }

    num_packets--;
    free(pck);
}

[[gnu::nonnull]]
static void free_message(struct message *msg, bool need_lock)
{
    struct message *tmp;
    unsigned lck;

    dbg_printf("     free_message: id=%u [%s] lock=%s topic=%u <%s> type=%s refcnt=%u\n",
            msg->id, message_state_str[msg->state],
            need_lock ? "yes" : "no",
            msg->topic ? msg->topic->id : 0,
            msg->topic ? (char *)msg->topic->name : "",
            message_type_str[msg->type],
            GET_REFCNT(&msg->refcnt)
            );

    if ((lck = GET_REFCNT(&msg->refcnt)) > 0) {
        warnx("free_message: attempt to free message with refcnt=%u", lck);
        abort();
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

    if (msg->delivery_states) {
        free_delivery_states(&msg->delivery_states_lock,
                &msg->num_message_delivery_states, &msg->delivery_states);
        msg->delivery_states = NULL;
    }

    pthread_rwlock_destroy(&msg->delivery_states_lock);

    if (msg->type == MSG_WILL && msg->sender) {
        msg->sender->will_topic = NULL;
    }

    /* INC in register_message(), doesn't happen to RETAIN */
    if (msg->sender && GET_REFCNT(&msg->sender->refcnt))
        DEC_REFCNT(&msg->sender->refcnt);

    num_messages--;
    free(msg);
}

    [[gnu::nonnull]]
static void free_client(struct client *client, bool needs_lock)
{
    struct client *tmp;

    dbg_printf("     free_client: id=%u [%s] lock=%s client_id=%s session=%d %s refcnt=%u\n",
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

    if (client->fd != -1)
        close_socket(&client->fd);

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

    if (client->packet_buf)
        free(client->packet_buf);

    if (client->session) {
        client->session->client = NULL;
        DEC_REFCNT(&client->session->refcnt); /* handle_cp_connect */
        client->session = NULL;
    }

    pthread_rwlock_destroy(&client->active_packets_lock);

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

    if (GET_REFCNT(&session->refcnt) > 0) {
        warn("free_session: attempt to free session with refcnt=%d", session->refcnt);
        abort();
        return;
    }

    if (need_lock)
        pthread_rwlock_wrlock(&global_sessions_lock);
    {
        if (global_session_list == session) {
            global_session_list = session->next;
        } else for (tmp = global_session_list; tmp; tmp = tmp->next) {
            if (tmp->next == session) {
                tmp->next = session->next;
                break;
            }
        }
        if (session->client) {
            warn("free_session: freeing session with connected client!");
            session->client->state = CS_CLOSED;
            close_socket(&session->client->fd);
            session->client->session = NULL;
            //DEC_REFCNT(&session->client->refcnt); // TODO add INC_REFCNTs everywhere for client->session
            session->client = NULL;
        }
    }
    if (need_lock)
        pthread_rwlock_unlock(&global_sessions_lock);

    pthread_rwlock_wrlock(&session->subscriptions_lock);
    if (session->subscriptions) {
        unsubscribe_session_from_all(session);
        free(session->subscriptions);
        session->subscriptions = NULL;
        session->num_subscriptions = 0;
    }
    pthread_rwlock_unlock(&session->subscriptions_lock);

    /* TODO do this properly */
    if (session->delivery_states && session->num_message_delivery_states) {
        dbg_printf("     free_session: num_mds=%u\n", session->num_message_delivery_states);
        free_delivery_states(&session->delivery_states_lock,
                &session->num_message_delivery_states, &session->delivery_states);
        session->delivery_states = NULL;
    }

    if (session->client_id) {
        free((void *)session->client_id);
        session->client_id = NULL;
    }

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

    pthread_rwlock_destroy(&session->subscriptions_lock);
    pthread_rwlock_destroy(&session->delivery_states_lock);

    num_sessions--;
    free(session);
}

[[gnu::malloc, gnu::nonnull, gnu::warn_unused_result]]
static struct message_delivery_state *alloc_message_delivery_state(
        struct message *message, struct session *session)
{
    struct message_delivery_state *ret;

    if ((ret = calloc(1, sizeof(struct message_delivery_state))) == NULL)
        goto fail;

    INC_REFCNT(&session->refcnt); /* mds_detach_and_free */
    ret->session = session;

    INC_REFCNT(&message->refcnt); /* mds_detach_and_free */
    ret->message = message;

    ret->id = mds_id++;

    dbg_printf("     alloc_message_delivery_state: session=%d[%u] message=%u[%u]\n",
            session->id, session->refcnt,
            message->id, message->refcnt);

    pthread_rwlock_wrlock(&global_mds_lock);
    ret->next = global_mds_list;
    global_mds_list = ret;
    num_mds++;
    pthread_rwlock_unlock(&global_mds_lock);
    return ret;

fail:
    if (ret)
        free(ret);
    return NULL;
}

[[gnu::nonnull]]
static int add_session_to_shared_sub(struct subscription *sub,
        struct session *session)
{
    void *tmp;

    assert(sub->type == SUB_SHARED);
    size_t new_size = sizeof(struct subscription *) * sub->num_sessions + 1;

    if ((tmp = realloc(sub->sessions, new_size)) == NULL)
        goto fail;
    sub->sessions = tmp;
    
    INC_REFCNT(&session->refcnt);
    sub->sessions[sub->num_sessions] = session;
    sub->num_sessions++;

    return 0;

fail:
    return -1;
}

[[gnu::malloc, gnu::warn_unused_result, gnu::nonnull]]
static struct subscription *alloc_subscription(struct session *session,
        struct topic *topic, subscription_type_t type)
{
    struct subscription *ret = NULL;

    if ((ret = calloc(1, sizeof(struct subscription))) == NULL)
        return NULL;

    ret->id = subscription_id++;
    ret->type = type;

    switch (type)
    {
        case SUB_NON_SHARED:
            INC_REFCNT(&session->refcnt); /* free_subscription */
            ret->session = session;
            break;

        case SUB_SHARED:
            add_session_to_shared_sub(ret, session);
            break;

        default:
            errno = EINVAL;
            warn("alloc_subscription: unknown type");
            goto fail;
    }

    INC_REFCNT(&topic->refcnt);
    ret->topic = topic;

    dbg_printf("     alloc_subscription: id=%u session=%d <%s> topic=%u <%s>\n",
            ret->id, session->id, (char *)session->client_id,
            topic->id, (char *)topic->name);
    return ret;

fail:
    if (ret)
        free(ret);

    return NULL;
}

[[gnu::malloc, gnu::warn_unused_result]]
static struct session *alloc_session(struct client *client)
{
    struct session *ret;

    if (num_sessions >= MAX_SESSIONS) {
        errno = ENOSPC;
        return NULL;
    }

    errno = 0;

    if ((ret = calloc(1, sizeof(struct session))) == NULL)
        return NULL;

    if (pthread_rwlock_init(&ret->subscriptions_lock, NULL) == -1)
        goto fail;
    if (pthread_rwlock_init(&ret->delivery_states_lock, NULL) == -1)
        goto fail;

    if (client) {
        ret->client = client;
        client->session = ret;
        ret->client_id = (void *)strdup((const char *)client->client_id);
    }

    pthread_rwlock_wrlock(&global_sessions_lock);
    ret->next = global_session_list;
    global_session_list = ret;
    num_sessions++;
    pthread_rwlock_unlock(&global_sessions_lock);

    ret->id = session_id++;

    dbg_printf("     alloc_session: id=%u client=%u <%s>\n",
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
static struct topic *alloc_topic(const uint8_t *name, const uint8_t uuid[const UUID_SIZE])
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

    pthread_rwlock_init(&ret->subscribers_lock, NULL);
    pthread_rwlock_init(&ret->pending_queue_lock, NULL);

    ret->id = topic_id++;
    num_topics++;

    dbg_printf("     alloc_topic: id=%u <%s>\n", ret->id, (char *)name);

    return ret;

fail:
    if (ret)
        free(ret);

    return NULL;
}

[[gnu::malloc,gnu::warn_unused_result]]
static struct packet *alloc_packet(struct client *owner)
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

    ret->id = packet_id++;

    dbg_printf("     alloc_packet: id=%u owner=%u <%s>\n",
            ret->id, owner ? owner->id : 0, owner ? ((char *)owner->client_id) : ""
          );

    return ret;
}

[[gnu::malloc, gnu::warn_unused_result]]
static struct message *alloc_message(const uint8_t uuid[const UUID_SIZE])
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

    if (uuid == NULL && generate_uuid(global_hwaddr, ret->uuid) == -1)
        goto fail;
    else if (uuid != NULL)
        memcpy(ret->uuid, uuid, UUID_SIZE);

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

    client->state = CS_NEW;
    client->fd = -1;
    client->parse_state = READ_STATE_NEW;
    client->is_auth = false;

    if (pthread_rwlock_init(&client->active_packets_lock, NULL) == -1)
        goto fail;

    pthread_rwlock_wrlock(&global_clients_lock);
    client->next = global_client_list;
    global_client_list = client;
    num_clients++;
    pthread_rwlock_unlock(&global_clients_lock);

    client->id = client_id++;

    dbg_printf("     alloc_client: id=%u\n", client->id);

    return client;

fail:
    if (client)
        free(client);

    return NULL;
}

/*
 * persistence functions
 */

[[gnu::nonnull]]
static int save_message(const struct message *msg)
{
    struct message_save *save = NULL;

    assert(msg->uuid);

    errno = EINVAL;

    pthread_rwlock_rdlock(&global_messages_lock);

    if (msg->state != MSG_ACTIVE)
        goto fail;

    dbg_printf("     save_message: saving message id=%u uuid=%s\n",
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
static int save_topic(const struct topic *topic)
{
    errno = EINVAL;

    pthread_rwlock_rdlock(&global_topics_lock);

    if (topic->state != TOPIC_ACTIVE)
        goto fail;

    dbg_printf("     save_topic: saving topic id=%u name=<%s> %s%s\n",
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

/* [MQTT-3.1.3-2] */
[[gnu::nonnull, gnu::warn_unused_result]]
static struct session *find_session(struct client *client)
{
    pthread_rwlock_rdlock(&global_sessions_lock);
    for (struct session *tmp = global_session_list; tmp; tmp = tmp->next)
    {
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

    client->disconnect_reason = code;
    client->state = CS_CLOSED;

    if (errno == 0)
        errno = EINVAL;
    return -1;
}

[[gnu::nonnull]]
static bool is_shared_subscription(const uint8_t *name)
{
    if (!strncmp("$shared/", (const char *)name, 8))
        return true;

    return false;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int is_valid_topic_name(const uint8_t *name)
{
    const uint8_t *ptr;

    errno = EINVAL;
    ptr = name;

    if (!*ptr)
        return -1;

    while (*ptr)
    {
        if (*ptr == '#' || *ptr == '+')
            return -1;

        ptr++;
    }

    errno = 0;
    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int is_valid_topic_filter(const uint8_t *name)
{
    const uint8_t *ptr;

    errno = EINVAL;
    ptr = name;

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

    if (*bytes_left < 2) {
        errno = ENOSPC;
        return NULL;
    }

    memcpy(&tmp, *ptr, 2);

    *length = ntohs(tmp);
    *ptr += 2;
    *bytes_left -= 2;

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

    if (*bytes_left < 2) {
        errno = ENOSPC;
        return NULL;
    }

    memcpy(&str_len, *ptr, 2);
    str_len = ntohs(str_len);

    *ptr += 2;
    *bytes_left -= 2;

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
                ret += 2;
                break;
            case MQTT_TYPE_4BYTE:
                ret += 4;
                break;
            case MQTT_TYPE_BINARY:
                ret += prop->binary.len;
                ret += 2;
                break;
            case MQTT_TYPE_UTF8_STRING:
                ret += strlen((const char *)prop->utf8_string);
                ret += 2;
                break;
            case MQTT_TYPE_VARBYTE:
                ret += encode_var_byte(prop->varbyte, tmp_out);
                break;
            case MQTT_TYPE_UTF8_STRING_PAIR:
                ret += strlen((const char *)prop->utf8_pair[0]);
                ret += strlen((const char *)prop->utf8_pair[1]);
                ret += 4; /* 2x2 */
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
    memcpy(*ptr, &enclen, 2);
    *ptr += 2;
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
                memcpy(ptr, &tmp2byte, 2);
                ptr += 2;
                break;

            case MQTT_TYPE_4BYTE:
                tmp4byte = htonl(prop->byte4);
                memcpy(ptr, &tmp4byte, 4);
                ptr += 4;
                break;

            case MQTT_TYPE_VARBYTE:
                if ((rc = encode_var_byte(prop->varbyte, ptr)) == -1)
                    goto fail;
                ptr += rc;
                break;

            case MQTT_TYPE_BINARY:
                tmp2byte = htons(prop->binary.len);
                memcpy(ptr, &tmp2byte, 2);
                ptr += 2;
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

    //dbg_printf("parse_properties: properties_length=%u\n", properties_length);

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
                if (*bytes_left < 2)
                    goto fail;
                memcpy(&prop->byte2, *ptr, 2);
                prop->byte2 = ntohs(prop->byte2);
                skip = 2;
                break;

            case MQTT_TYPE_4BYTE:
                if (*bytes_left < 4)
                    goto fail;
                memcpy(&prop->byte4, *ptr, 4);
                prop->byte4 = ntohl(prop->byte4);
                skip = 4;
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
                if (*bytes_left < 2)
                    goto fail;

                memcpy(&prop->binary.len, *ptr, 2);
                *ptr += 2;
                *bytes_left -= 2;
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
        dump_all();
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
    dbg_printf("     close_socket: closing openmetrics_fd %u\n", global_om_fd);
    if (global_om_fd != -1)
        close_socket(&global_om_fd);
}

static void save_all_topics(void)
{
    dbg_printf("     "BYEL"save_all_topics"CRESET"\n");
    for (const struct topic *topic = global_topic_list; topic; topic = topic->next)
        save_topic(topic);
}

static void free_all_message_delivery_states(void)
{
    dbg_printf("     "BYEL"free_all_message_delivery_states"CRESET"\n");
    /* don't bother locking this late in tear down */
    while (global_mds_list)
        mds_detach_and_free(global_mds_list, false, false);
}

static void free_all_sessions(void)
{
    dbg_printf("     "BYEL"free_all_sessions"CRESET"\n");
    while (global_session_list)
        free_session(global_session_list, true);
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
static struct message *find_message_by_uuid(const uint8_t uuid[static const UUID_SIZE])
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

[[gnu::nonnull, gnu::warn_unused_result]]
static struct topic *find_topic(const uint8_t *name)
{
    errno = 0;

    pthread_rwlock_rdlock(&global_topics_lock);
    for (struct topic *tmp = global_topic_list; tmp; tmp = tmp->next)
    {
        if (!strcmp((const void *)name, (const void *)tmp->name)) {
            pthread_rwlock_unlock(&global_topics_lock);
            return tmp;
        }
    }
    pthread_rwlock_unlock(&global_topics_lock);

    return NULL;
}

[[gnu::nonnull(1), gnu::warn_unused_result]]
static struct topic *find_or_register_topic(const uint8_t *name)
{
    struct topic *topic;
    const uint8_t *tmp_name = NULL;

    if ((topic = find_topic(name)) == NULL) {
        if ((tmp_name = (void *)strdup((const char *)name)) == NULL)
            goto fail;

        if ((topic = register_topic(tmp_name, NULL)) == NULL) {
            warn("find_or_register_topic: register_topic <%s>", tmp_name);
            goto fail;
        }

        free((void *)tmp_name);
        save_topic(topic);
    }

    return topic;

fail:

    if (tmp_name)
        free((void *)tmp_name);

    return NULL;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int find_subscription(struct session *session, struct topic *topic)
{
    pthread_rwlock_rdlock(&topic->subscribers_lock);
    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if (topic->subscribers[idx] == NULL)
            continue;

        if (topic->subscribers[idx]->session == session) {
            pthread_rwlock_unlock(&topic->subscribers_lock);
            return idx;
        }
    }
    pthread_rwlock_unlock(&topic->subscribers_lock);
    errno = ENOENT;
    return -1;
}

[[gnu::nonnull(1), gnu::warn_unused_result]]
static struct topic *register_topic(const uint8_t *name, const uint8_t uuid[const UUID_SIZE])
{
    struct topic *ret;

    assert(name != NULL);

    errno = 0;

    if ((ret = alloc_topic(name, uuid)) == NULL)
        return NULL;

    ret->state = TOPIC_ACTIVE;
    /* We do not save here, caller must save, find_or_register_topic() does this */

    pthread_rwlock_wrlock(&global_topics_lock);
    ret->next = global_topic_list;
    global_topic_list = ret;
    pthread_rwlock_unlock(&global_topics_lock);

    return ret;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int remove_delivery_state(
        struct message_delivery_state ***state_array, unsigned *array_length,
        struct message_delivery_state *rem)
{
    const unsigned old_length = *array_length;
    unsigned new_length = *array_length - 1;
    struct message_delivery_state **tmp = NULL;

    dbg_printf("     remove_delivery_state: array_length=%u new_length=%u rem=%u\n",
            *array_length, new_length, rem->id);

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

    if ((tmp = malloc(sizeof(struct message_delivery_state *) * new_length)) == NULL)
        goto fail;

    bool found = false;
    unsigned new_idx, old_idx;

    for (new_idx = 0, old_idx = 0; new_idx < new_length && old_idx < old_length; old_idx++)
    {
        dbg_printf("     remove_delivery_state: new_idx=%u old_idx=%u\n",
                new_idx, old_idx);

        if ((*state_array)[old_idx] == rem) {
            found = true;
            continue;
        }

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

    dbg_printf(BYEL"     remove_delivery_state: ENOENT"CRESET"\n");

    errno = ENOENT;

fail:
    if (tmp)
        free(tmp);

    return -1;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int add_to_delivery_state(
        struct message_delivery_state ***state_array, unsigned *array_length,
        pthread_rwlock_t *lock, struct message_delivery_state *add)
{
    pthread_rwlock_wrlock(lock);

    const unsigned new_length = *array_length + 1;
    const size_t new_size = sizeof(struct message_delivery_state *) * new_length;
    struct message_delivery_state **tmp = NULL;

    if ((tmp = realloc(*state_array, new_size)) == NULL)
        goto fail;

    tmp[*array_length] = add;

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

[[gnu::nonnull, gnu::warn_unused_result]]
static int enqueue_message(struct topic *topic, struct message *msg)
{
    struct message_delivery_state *mds;

    assert(topic->id);
    assert(msg->id);
    assert(msg->state == MSG_NEW);

    dbg_printf("[%2d] enqueue_message: topic=%d <%s> num_subscribers=%u\n",
            msg->sender ? msg->sender->id : (id_t)-1,
            topic->id, topic->name,
            topic->num_subscribers);

    errno = 0;

    /* TODO check the specification to see if empty topic handling is conformant */
    if (topic->num_subscribers == 0)
        goto no_subscribers;

    pthread_rwlock_rdlock(&topic->subscribers_lock);
    bool found = false;
    for (unsigned src_idx = 0; src_idx < topic->num_subscribers; src_idx++)
    {
        if (topic->subscribers[src_idx] == NULL)
            continue;
        struct session *session = topic->subscribers[src_idx]->session;

        assert(session);
        assert(session->id);

        /* TODO confirm we don't echo per standard? If we do, need to
         * somehow exclude WILL messages */
        if (session == msg->sender)
            continue;

        found = true;


        /* TODO lock the subscriber? */

        if ((mds = alloc_message_delivery_state(msg, session)) == NULL) {
            warn("enqueue_message: alloc_message_delivery_state");
            /* TODO ???? */
            continue;
        }

        if (add_to_delivery_state(
                    &msg->delivery_states,
                    &msg->num_message_delivery_states,
                    &msg->delivery_states_lock,
                    mds) == -1) {
            warn("enqueue_message: add_to_delivery_state(msg)");
            mds_detach_and_free(mds, true, true);
            mds = NULL;
            continue;
        }

        if (add_to_delivery_state(
                    &session->delivery_states,
                    &session->num_message_delivery_states,
                    &session->delivery_states_lock,
                    mds) == -1) {
            warn("enqueue_message: add_to_delivery_state(session)");
            mds_detach_and_free(mds, true, true);
            mds = NULL;
            continue;
        }
    }
    pthread_rwlock_unlock(&topic->subscribers_lock);

    if (found == false) {
        warnx("enqueue_message: failed to add to subscribers!");
    }
no_subscribers:

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
    }
    return 0;
}

/**
 * refcnt for non-retained messages should only be touched in enqueue_message or dequeue_message
 */
[[gnu::nonnull]]
static struct message *register_message(const uint8_t *topic_name, int format,
        uint16_t len, const void *payload, unsigned qos, struct session *sender,
        bool retain, message_type_t type)
{
    struct topic *topic;

    topic = NULL;
    errno = 0;

    dbg_printf("[%2d] register_message: topic=<%s> format=%u len=%u qos=%u %spayload=%p\n",
            sender->id, topic_name, format, len, qos,
            retain ? BWHT "retain" CRESET " " : "",
            payload);

    if ((topic = find_or_register_topic(topic_name)) == NULL)
            goto fail;

    struct message *msg;

    if ((msg = alloc_message(NULL)) == NULL)
        goto fail;

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

[[gnu::nonnull]]
static int add_subscription_to_topic(struct subscription *new_sub)
{
    struct subscription **tmp_subs = NULL;
    struct topic *topic = new_sub->topic;

    errno = 0;

    size_t sub_size = sizeof(struct subscription *) * (topic->num_subscribers + 1);
    if ((tmp_subs = realloc(topic->subscribers, sub_size)) == NULL)
        goto fail;

    topic->subscribers = tmp_subs;
    topic->subscribers[topic->num_subscribers] = new_sub;
    topic->num_subscribers++;
    return 0;

fail:
    if (tmp_subs)
        free(tmp_subs);
    return -1;
}

/* TODO locking */
[[gnu::nonnull, gnu::warn_unused_result]]
static int unsubscribe(struct subscription *sub)
{
    struct subscription **tmp_topic = NULL;
    struct subscription **tmp_client = NULL;
    size_t topic_sub_size, topic_sub_cnt = 0, client_sub_size, client_sub_cnt = 0;
    unsigned old_idx, new_idx;

    struct topic *topic;
    struct session *session;

    topic = sub->topic;
    session = sub->session;

    errno = 0;

    /*
     * remove the back references for this subscription
     */

    pthread_rwlock_wrlock(&topic->subscribers_lock);
    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if (topic->subscribers[idx] == sub) {
            topic->subscribers[idx] = NULL;
            break;
        }
    }
    pthread_rwlock_unlock(&topic->subscribers_lock);

    pthread_rwlock_wrlock(&session->subscriptions_lock);
    for (unsigned idx = 0; idx < session->num_subscriptions; idx++)
    {
        if ( session->subscriptions[idx] == sub) {
            session->subscriptions[idx] = NULL;
            break;
        }
    }
    pthread_rwlock_unlock(&session->subscriptions_lock); /* TODO hold lock until the end */

    /*
     * compact the topic list of subscribers
     */

    for (unsigned idx = 0; idx < topic->num_subscribers; idx++)
    {
        if (topic->subscribers[idx] == NULL)
            continue;
        topic_sub_cnt++;
    }

    topic_sub_size = topic_sub_cnt * sizeof(struct subscription *);

    if (topic_sub_cnt == 0) {
        tmp_topic = NULL;
        goto skip_topic;
    }

    if ((tmp_topic = calloc(1, topic_sub_size)) == NULL)
        goto fail;

    for (old_idx = 0, new_idx = 0; old_idx < topic->num_subscribers; old_idx++)
    {
        if (topic->subscribers[old_idx] == NULL)
            continue;

        tmp_topic[new_idx] = topic->subscribers[old_idx];
        new_idx++;
    }

skip_topic:

    /*
     * compact the client list of subscriptions
     */

    pthread_rwlock_wrlock(&session->subscriptions_lock); /* HOLD lock from start to finish */
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

    /*
     * free the old ones and replace
     */

skip_client:

    if (session->subscriptions) {
        free(session->subscriptions);
        session->subscriptions = NULL;
    }
    if (topic->subscribers) {
        free(topic->subscribers);
        topic->subscribers = NULL;
    }

    topic->subscribers = tmp_topic;
    session->subscriptions = tmp_client;

    topic->num_subscribers = topic_sub_cnt;
    session->num_subscriptions = client_sub_cnt;

    //dbg_printf("     unsubscribe_from_topic: client_sub_cnt now %lu\n", client_sub_cnt);

    DEC_REFCNT(&sub->topic->refcnt);
    sub->topic = NULL;

    DEC_REFCNT(&sub->session->refcnt); /* alloc_subscription */
    sub->session = NULL;

    free_subscription(sub);
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

static int unsubscribe_session_from_all(struct session *session)
{
    int rc = 0;

    if (session->subscriptions == NULL || session->num_subscriptions == 0)
        return 0;

    const struct topic_sub_request req = {
        .topic_refs = malloc(sizeof(struct topic *) * session->num_subscriptions),
        .num_topics = session->num_subscriptions,
        .id = (id_t)-1,
    };

    if (req.topic_refs != NULL) {
        for (unsigned idx = 0; idx < session->num_subscriptions; idx++)
            req.topic_refs[idx] = session->subscriptions[idx]->topic;
        
        if (unsubscribe_from_topics(session, &req) == -1) {
            rc = -1;
            warn("free_session: unsubscribe_from_topics");
        }
        free(req.topic_refs);
    } else
        rc = -1;

    return rc;
}

    [[gnu::nonnull, gnu::warn_unused_result]]
static int unsubscribe_from_topics(struct session *session,
        const struct topic_sub_request *request)
{
    struct subscription *sub;
    int sub_idx;
    errno = 0;

    if (request->topic_refs == NULL) {
        warnx("unsubscribe_from_topics: topic_refs is NULL");
        errno = EINVAL;
        return -1;
    }

    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        if (request->topic_refs[idx] == NULL)
            if ((request->topic_refs[idx] = find_topic(request->topics[idx])) == NULL) {
                if (request->reason_codes)
                    request->reason_codes[idx] = MQTT_NO_SUBSCRIPTION_EXISTED;
                continue;
            }

        if ((sub_idx = find_subscription(session, request->topic_refs[idx])) == -1) {
            if (request->reason_codes) {
                if (errno == ENOENT)
                    request->reason_codes[idx] = MQTT_NO_SUBSCRIPTION_EXISTED;
                else
                    request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
            }
            continue;
        }

        sub = request->topic_refs[idx]->subscribers[sub_idx];
        switch (sub->type)
        {
            case SUB_SHARED:
                if (request->reason_codes)
                    request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
                warn("unsubscribe_from_topics: SUB_SHARED not implemented");
                continue;

            case SUB_NON_SHARED:
                if (unsubscribe(sub) == -1) {
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

    [[gnu::nonnull, gnu::warn_unused_result]]
static int subscribe_to_topics(struct session *session,
        struct topic_sub_request *request)
{
    struct subscription **tmp_subs = NULL;
    struct topic *tmp_topic = NULL;

    errno = 0;

    pthread_rwlock_wrlock(&session->subscriptions_lock);
    size_t sub_size = sizeof(struct subscription *) * (session->num_subscriptions + request->num_topics);

    if ((tmp_subs = realloc(session->subscriptions, sub_size)) == NULL) {
        for (unsigned idx = 0; idx < request->num_topics; idx++)
            request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;

        pthread_rwlock_unlock(&session->subscriptions_lock);
        goto fail;
    }

    session->subscriptions = tmp_subs;

    for (unsigned idx = 0; idx < request->num_topics; idx++)
    {
        subscription_type_t type = SUB_NON_SHARED;

        session->subscriptions[session->num_subscriptions + idx] = NULL;

        if (request->reason_codes[idx] > MQTT_GRANTED_QOS_2) {
            dbg_printf("[%d] subscribe_to_topics: response code is %u\n",
                    session->id,
                    request->reason_codes[idx]);
            continue;
        }

        dbg_printf("[%2d] subscribe_to_topics: subscribing to <%s>\n",
                session->id,
                (char *)request->topics[idx]);

        if (is_shared_subscription(request->topics[idx])) {
            type = SUB_SHARED;
            /* TODO */
        }

        if ((tmp_topic = find_or_register_topic(request->topics[idx])) == NULL) {
            /* TODO somehow ensure reply does a fail for this one? */
            dbg_printf("[%2d] subscribe_to_topics: failed to find_or_register_topic(<%s>)\n",
                    session->id,
                    (char *)request->topics[idx]);
            request->reason_codes[idx] = MQTT_TOPIC_NAME_INVALID;
            continue;
        }

        int existing_idx;
        struct subscription *new_sub;

        if ((existing_idx = find_subscription(session, tmp_topic)) == -1 && errno == ENOENT) {

            if ((new_sub = alloc_subscription(session, tmp_topic, type)) == NULL)
                goto fail;
            new_sub->option = request->options[idx];

            if (add_subscription_to_topic(new_sub) == -1) {
                warn("subscribe_to_topics: add_subscription_to_topic <%s>",
                        tmp_topic->name);
                request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
                free_subscription(new_sub);
                new_sub = NULL;
                continue;
            }

            /* TODO refactor to add_subscription_to_session() */
            session->subscriptions[session->num_subscriptions + idx] = new_sub;
        } else if (existing_idx == -1) {
            warn("subscribe_to_topics: find_subscription");
            request->reason_codes[idx] = MQTT_UNSPECIFIED_ERROR;
            continue;
        } else {
            /* Update the existing subscription's options (e.g. QoS) */
            dbg_printf("[%2d] subscribe_to_topics: updating existing subscription\n",
                    session->id);
            tmp_topic->subscribers[existing_idx]->option = request->options[idx];
        }

        free((void *)request->topics[idx]);
        request->topics[idx] = NULL;

        request->options[idx] = 0;
        request->topic_refs[idx] = tmp_topic;

        if (existing_idx == -1)
            session->num_subscriptions++;
    }

    dbg_printf("[%2d] subscribe_to_topics: num_subscriptions now %u [+%u]\n",
            session->id,
            session->num_subscriptions, request->num_topics);
    pthread_rwlock_unlock(&session->subscriptions_lock);
    return 0;

fail:

    return -1;
}

[[gnu::nonnull]]
static int mark_one_mds(struct message_delivery_state *mds,
        control_packet_t type, reason_code_t client_reason)
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
            break;

        case MQTT_CP_PUBREC: /* QoS=2 */
            if (mds->acknowledged_at)
                warnx("mark_message: duplicate acknowledgment");
            mds->acknowledged_at = now;
            mds->client_reason = client_reason;
            break;

        case MQTT_CP_PUBREL: /* QoS=2 */
            if (mds->released_at)
                warnx("mark_message: duplicate release");
            mds->released_at = now;
            break;

        case MQTT_CP_PUBCOMP: /* QoS=2 */
            if (mds->completed_at)
                warnx("mark_message: duplicate completed");
            mds->completed_at = now;
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
        reason_code_t client_reason, struct session *session, role_t role)
{
    int rc;
    struct message_delivery_state *mds;

    assert(packet_identifier != 0);

    if (role == ROLE_RECV)
        goto do_recv;

    pthread_rwlock_wrlock(&global_messages_lock);
    for (struct message *message = global_message_list; message; message = message->next)
    {
        if (message->sender != session)
            continue;

        if (message->sender_status.packet_identifier == packet_identifier) {
            rc = mark_one_mds(&message->sender_status, type, client_reason);

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

        rc = mark_one_mds(mds, type, client_reason);

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
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    uint8_t proplen[4], remlen[4];
    int proplen_len, remlen_len, prop_len;

    uint16_t tmp, topic_len;
    const struct message *msg;

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

    errno = 0;
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
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    memcpy(ptr, msg->topic->name, topic_len);
    ptr += topic_len;

    if (pkt->flags & MQTT_FLAG_PUBLISH_QOS_MASK) {
        tmp = htons(pkt->packet_identifier); /* TODO proper packet identifier */
        memcpy(ptr, &tmp, 2);
        ptr += 2;
    }

    memcpy(ptr, proplen, proplen_len);
    ptr += proplen_len;

    if (build_properties(&props, num_props, &ptr) == -1)
        goto fail;

    memcpy(ptr, msg->payload, msg->payload_len);

    /* [MQTT-3.1.2-25] */
    if (pkt->owner->maximum_packet_size && length > pkt->owner->maximum_packet_size)
        goto skip_write;

    /* Now send the packet */
    if ((wr_len = write(pkt->owner->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

skip_write:
    free(packet);
    return 0;

fail:
    if (packet)
        free(packet);
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
        return log_io_error(NULL, wr_len, length, false);
    }

    dbg_printf("[%2d] send_cp_disconnect: sent code was %u\n",
            client->session ? client->session->id : (id_t)-1, reason_code);

    free(packet);
    client->state = CS_CLOSING;
    client->disconnect_reason = 0;

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
        return log_io_error(NULL, wr_len, length, false);
    }

    /* last_keep_alive is updated in parse_incoming after
     * any successful control packets */
    free(packet);

    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_connack(struct client *client, reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;

    uint8_t proplen[4], remlen[4];
    int proplen_len, remlen_len, prop_len;

    errno = 0;

    /* Populate Properties */

    const struct property props[] = {
        { .ident = MQTT_PROP_MAXIMUM_PACKET_SIZE               , .byte4 = MAX_PACKET_LENGTH } ,
        { .ident = MQTT_PROP_RECEIVE_MAXIMUM                   , .byte2 = MAX_RECEIVE_PUBS  } ,
        { .ident = MQTT_PROP_RETAIN_AVAILABLE                  , .byte  = 1                 } ,
        { .ident = MQTT_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE   , .byte  = 0                 } ,
        { .ident = MQTT_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE , .byte  = 0                 } ,
        { .ident = MQTT_PROP_SHARED_SUBSCRIPTION_AVAILABLE     , .byte  = 0                 } ,
    };
    const unsigned num_props = sizeof(props) / sizeof(struct property);

    /* Calculate the Property[] Length */
    if ((prop_len = get_properties_size(&props, num_props)) == -1)
        return -1;

    /* Calculate the length of Property Length */
    if ((proplen_len = encode_var_byte(prop_len, proplen)) == -1)
        return -1;

    length = 0;
    length += 2;           /* connack var header (1byte for flags, 1byte for code) */
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

    if (build_properties(&props, num_props, &ptr) == -1)
        goto fail;

    /* Now send the packet */
    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

    if (is_malformed(reason_code)) {
        client->disconnect_reason = reason_code;
        client->state = CS_CLOSING;
    }

    return 0;

fail:
    if (packet)
        free(packet);

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
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* Property Length */
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

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
    dbg_printf("[%2d] send_cp_pubcomp: packet_id=%u reason_code=%d <%s> client.id=%d\n", client->session ? client->session->id : (id_t)-1, packet_id, reason_code, reason_codes_str[reason_code], client->id);


    length = 0;
    length +=2; /* Packet Identifier */
    length +=1; /* Reason Code */
    length +=1; /* Properties Length */

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
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* No properties */
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    pthread_rwlock_wrlock(&client->session->delivery_states_lock);
    for (unsigned idx = 0; idx < client->session->num_message_delivery_states; idx++)
    {
        struct message_delivery_state *mds = client->session->delivery_states[idx];

        if (mds == NULL)
            continue;

        if (mds->session == NULL && mds->message == NULL)
            continue;

        if (mds->packet_identifier == packet_id) {
            mds->completed_at = time(0);
            break;
        }

        if (mds->message->retain && mds->message->sender) {
            /* Now we've COMP, we can forget the sender */
            DEC_REFCNT(&mds->message->sender->refcnt);
            mds->message->sender = NULL;
        }
    }
    pthread_rwlock_unlock(&client->session->delivery_states_lock);

    free(packet);
    return 0;
}

[[gnu::nonnull, gnu::warn_unused_result]]
static int send_cp_pubrel(struct client *client, uint16_t packet_id,
        reason_code_t reason_code)
{
    ssize_t length, wr_len;
    uint8_t *packet, *ptr;
    uint16_t tmp;
    uint8_t remlen[4]; int remlen_len;

    dbg_printf("[%2d] send_cp_pubrel: packet_id=%u reason_code=%d <%s> client.id=%d\n", client->session ? client->session->id : (id_t)-1, packet_id, reason_code, reason_codes_str[reason_code], client->id);

    errno = 0;

    length = 0;
    length += 3; /* Packet Identifier + Reason Code */
    length += 1; /* Properties Length */

    remlen_len = encode_var_byte(length, remlen);

    length += sizeof(struct mqtt_fixed_header);
    length += remlen_len; /* Remaining Length */

    if ((ptr = packet = calloc(1, length)) == NULL)
        return -1;

    ((struct mqtt_fixed_header *)ptr)->type = MQTT_CP_PUBREL;
    ptr++;

    memcpy(ptr, remlen, remlen_len);
    ptr += remlen_len;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, 2);

    ptr += 2;

    *ptr = reason_code;
    ptr++;

    *ptr = 0; /* Properties Length */
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    /* TODO update status thing */

    free(packet);
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

    dbg_printf("[%2d] send_cp_puback: packet_id=%u reason_code=%d <%s> client.id=%d\n", client->session ? client->session->id : (id_t)-1, packet_id, reason_code, reason_codes_str[reason_code], client->id);
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
    memcpy(ptr, &tmp, 2);
    ptr += 2;

    *ptr = reason_code;
    ptr++;

    if ((wr_len = write(client->fd, packet, length)) != length) {
        free(packet);
        return log_io_error(NULL, wr_len, length, false);
    }

    free(packet);

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
    memcpy(ptr, &tmp, 2);
    ptr += 2;

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
        return log_io_error(NULL, wr_len, length, false);
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

    *ptr = length - sizeof(struct mqtt_fixed_header) - 1; /* Remaining Length */
    ptr++;

    tmp = htons(packet_id);
    memcpy(ptr, &tmp, 2);
    ptr += 2;

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
        return log_io_error(NULL, wr_len, length, false);
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

    if (bytes_left < 2)
        goto fail;

    memcpy(&tmp, ptr, 2);
    packet->packet_identifier = ntohs(tmp);
    ptr += 2;
    bytes_left -= 2;

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
                pubrel_reason_code, client->session, ROLE_SEND) == -1) {
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

    return 0;

fail:
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

    if (bytes_left < 2)
        goto fail;

    memcpy(&packet->packet_identifier, ptr, 2);
    bytes_left -= 2;
    ptr += 2;
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

    dbg_printf("[%2d] handle_cp_puback: client=%u <%s> reason_code=%u packet_identifier=%u\n",
            client->session->id,
            client->id, (char *)client->client_id,
            puback_reason_code, packet->packet_identifier);

    if (mark_message(MQTT_CP_PUBACK, packet->packet_identifier,
                puback_reason_code, client->session, ROLE_RECV) == -1) {
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

    if (bytes_left < 2)
        goto fail;

    memcpy(&packet->packet_identifier, ptr, 2);
    bytes_left -= 2;
    ptr += 2;
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
                pubcomp_reason_code, client->session, ROLE_RECV) == -1) {
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

    if (bytes_left < 2)
        goto fail;

    memcpy(&packet_identifier, ptr, 2);
    bytes_left -= 2;
    ptr += 2;
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
                client->session, ROLE_RECV) == -1) {
        if (errno == ENOENT) {
            reason_code = MQTT_PACKET_IDENTIFIER_NOT_FOUND;
            goto normal;
        } else
            reason_code = MQTT_UNSPECIFIED_ERROR;
        goto fail;
    }

    /* TODO what if the above succeeds, but the below fails? */

    if (mark_message(MQTT_CP_PUBREL, packet_identifier, MQTT_SUCCESS,
                client->session, ROLE_RECV) == -1) {
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
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    unsigned qos = 0;
    bool flag_retain;
    [[maybe_unused]] bool flag_dup; /* TODO use this somehow! */

    errno = 0;

    if ((topic_name = read_utf8(&ptr, &bytes_left)) == NULL)
        goto fail;

    if (is_valid_topic_name(topic_name) == -1) {
        reason_code = MQTT_TOPIC_NAME_INVALID;
        goto fail;
    }

    dbg_printf("[%2d] handle_cp_publish: topic=<%s> ",
            client->session->id, topic_name);

    qos = GET_QOS(packet->flags); // & (1<<1|1<<2)) >> 1;
    flag_retain = (packet->flags & MQTT_FLAG_PUBLISH_RETAIN) != 0;
    flag_dup = (packet->flags & MQTT_FLAG_PUBLISH_DUP) != 0;

    dbg_printf("qos=%u ", qos);

    if (qos > 2) {
        reason_code = MQTT_PROTOCOL_ERROR;
        warn("handle_cp_publish: invalid QoS value");
        goto fail;
    }

    if (qos) {
        memcpy(&packet_identifier, ptr, 2);
        packet_identifier = ntohs(packet_identifier);
        ptr += 2;
        bytes_left -= 2;
        if (packet_identifier == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            goto fail;
        }
        dbg_printf("packet_ident=%u ", packet_identifier);
    }

    uint8_t payload_format = 0; /* TODO extract from properties */

    if (parse_properties(&ptr, &bytes_left, &packet->properties,
                &packet->property_count, MQTT_CP_PUBLISH) == -1)
        goto fail;
    dbg_printf("payload_format=%u [%lub]", payload_format, bytes_left);

    packet->payload_len = bytes_left;
    if ((packet->payload = malloc(bytes_left)) == NULL)
        goto fail;
    memcpy(packet->payload, ptr, bytes_left);

    dbg_printf("\n");

    struct message *msg;
    if ((msg = register_message(topic_name, payload_format, packet->payload_len,
                    packet->payload, qos, client->session, flag_retain, MSG_NORMAL)) == NULL) {
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

    if (qos == 0) {
        msg->sender_status.acknowledged_at = now;
        msg->sender_status.released_at = now;
        msg->sender_status.completed_at = now;
    } if (qos == 1) {
        if (send_cp_puback(client, packet_identifier, MQTT_SUCCESS) == -1) {
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
        if (send_cp_pubrec(client, packet_identifier, MQTT_SUCCESS) == -1) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        msg->sender_status.acknowledged_at = now;
    }

    return 0;

fail:
    dbg_printf("\n");

    if (topic_name)
        free(topic_name);

    if (packet->payload) {
        free(packet->payload);
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

    if (bytes_left < 3) {
        errno = ENOSPC;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, 2);
    packet->packet_identifier = ntohs(packet->packet_identifier);
    ptr += 2;
    bytes_left -= 2;

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

        if ((request->topics[request->num_topics] = read_utf8(&ptr, &bytes_left)) == NULL)
            goto fail;

        if ((tmp = realloc(request->topic_refs,
                        sizeof(struct topic *) * (request->num_topics + 1))) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->topic_refs = tmp;
        request->topic_refs[request->num_topics] = NULL;

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

    errno = EINVAL;

    if (packet->flags != MQTT_FLAG_SUBSCRIBE)
        goto fail;

    if (bytes_left < 3) {
        errno = ENOSPC;
        goto fail;
    }

    memcpy(&packet->packet_identifier, ptr, 2);
    packet->packet_identifier = ntohs(packet->packet_identifier);
    ptr += 2;
    bytes_left -= 2;

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

        if ((tmp = realloc(request->topic_refs,
                        sizeof(struct topic *) * (request->num_topics + 1))) == NULL) {
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        request->topic_refs = tmp;
        request->topic_refs[request->num_topics] = NULL;

        if ((request->topics[request->num_topics] = read_utf8(&ptr, &bytes_left)) == NULL)
            goto fail;

        if (bytes_left < 1)
            goto fail;

        /* Validate subscribe options byte */

        /* bits 7 & 6 are reserved */
        if ((*ptr & MQTT_SUBOPT_RESERVED_MASK))
            goto fail;

        /* QoS can't be 3 */
        if ((*ptr & MQTT_SUBOPT_QOS_MASK) == MQTT_SUBOPT_QOS_MASK) {
            goto fail;
        }

        /* retain handling can't be 3 */
        if ((*ptr & MQTT_SUBOPT_RETAIN_HANDLING_MASK) == MQTT_SUBOPT_RETAIN_HANDLING_MASK)
            goto fail;

        if (!strncmp("$share/", (char *)request->topics[request->num_topics], 7)) {
            if ((*ptr & MQTT_SUBOPT_NO_LOCAL))
                goto fail;
            request->reason_codes[request->num_topics] = MQTT_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED;
        }

        if (is_valid_topic_filter(request->topics[request->num_topics]) == -1) {
            request->reason_codes[request->num_topics] = MQTT_TOPIC_FILTER_INVALID;
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

    if (rc == 0) {
        for (unsigned idx = 0; idx < request->num_topics; idx++)
        {
            if (request->topic_refs[idx] == NULL)
                continue;

            struct message *msg;

            if ((msg = request->topic_refs[idx]->retained_message) == NULL)
                continue;

            if (msg->state != MSG_ACTIVE)
                continue;

            dbg_printf("[%2d] handle_cp_subscribe: handling retain message\n",
                    client->session->id);

            struct message_delivery_state *mds;

            if ((mds = alloc_message_delivery_state(msg, client->session)) == NULL) {
                /* TODO ??? */
                continue;
            }

            if (add_to_delivery_state(&msg->delivery_states,
                        &msg->num_message_delivery_states,
                        &msg->delivery_states_lock,
                        mds) == -1) {
                warn("handle_cp_subscribe: retain: add_to_delivery_state(msg)");
                mds_detach_and_free(mds, true, true);
                mds = NULL;
                continue;
            }

            if (add_to_delivery_state(&client->session->delivery_states,
                        &client->session->num_message_delivery_states,
                        &client->session->delivery_states_lock,
                        mds) == -1) {
                warn("handle_cp_subscribe: retain: add_to_delivery_state(session)");
                mds_detach_and_free(mds, true, true);
                mds = NULL;
                continue;
            }

            dbg_printf("[%2d] handle_cp_subscribe: added retained message\n", client->session->id);
            msg->next_queue = msg->topic->pending_queue;
            msg->topic->pending_queue = msg;

            /* TODO */
        }
    }

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
        logger(LOG_INFO, client, "handle_cp_disconnect: disconnect request with reason %s",
                (disconnect_reason < MQTT_REASON_CODE_MAX) ? reason_codes_str[disconnect_reason] :
                "UNKNOWN");
    }

    if (bytes_left > 0) {
        if (parse_properties(&ptr, &bytes_left, &packet->properties,
                    &packet->property_count, MQTT_CP_DISCONNECT) == -1)
            goto fail;

    } else {
skip:
        dbg_printf("[%2d] handle_cp_disconnect: no reason\n", client->session->id);
        logger(LOG_INFO, client, "handle_cp_disconnect: disconnect request with no reason");
    }

    if (bytes_left)
        goto fail;

    if (disconnect_reason == 0) {
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
                free_properties(client->session->will_props, client->session->num_will_props);
                client->session->num_will_props = 0;
                client->session->will_props = NULL;
            }
        }
    }

    client->state = CS_DISCONNECTED;
    return 0;

fail:
    warnx("handle_cp_disconnect: packet malformed");
    client->state = CS_CLOSING;
    return -1;
}

[[gnu::nonnull]]
static int handle_cp_pingreq(struct client *client,
        struct packet *packet, const void * /*remain*/)
{
    dbg_printf("[%2d] handle_cp_pingreq\n", client->session->id);

    if (packet->remaining_length > 0) {
        errno = EINVAL;
        disconnect_if_malformed(client, MQTT_MALFORMED_PACKET);
        return -1;
    }

    return send_cp_pingresp(client);
}

[[gnu::nonnull]]
static int handle_cp_connect(struct client *client, struct packet *packet,
        const void *remain)
{
    const uint8_t *ptr = remain;
    size_t bytes_left = packet->remaining_length;
    reason_code_t reason_code = MQTT_MALFORMED_PACKET;
    uint16_t connect_header_length, keep_alive;
    uint8_t protocol_version, connect_flags;
    uint8_t protocol_name[4];
    const struct property *prop;
    bool reconnect = false;
    bool clean = false;

    uint8_t *will_topic = NULL;
    uint8_t will_qos = 0;
    void *will_payload = NULL;
    bool will_retain = false;
    uint16_t will_payload_len;
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

    memcpy(&connect_header_length, ptr, 2);
    connect_header_length = ntohs(connect_header_length);
    ptr += 2;
    bytes_left -= 2;

    if (connect_header_length != 4) {
        if (connect_header_length == 6) {
            protocol_version = 3; /* TODO parse old headers properly just to moan about it */
            goto version_fail;
        }
        goto fail;
    }

    memcpy(protocol_name, ptr, 4);
    ptr += 4;
    bytes_left -= 4;

    protocol_version = *ptr++;
    bytes_left--;

    connect_flags = *ptr++;
    bytes_left--;

    memcpy(&keep_alive, ptr, 2);
    keep_alive = ntohs(keep_alive);
    ptr += 2;
    bytes_left -= 2;

    if (memcmp(protocol_name, "MQTT", 4))
        goto fail;

    if (connect_flags & MQTT_CONNECT_FLAG_RESERVED)
        goto fail;

    if (protocol_version != 5) {
version_fail:
        logger(LOG_WARNING, client, "handle_cp_connect: unsupported protocol version %d", protocol_version);
        //warnx("handle_cp_connect: version %d is not supported", protocol_version);
        if (protocol_version < 5)
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
        errno = EEXIST;
        reason_code = MQTT_CLIENT_IDENTIFIER_NOT_VALID;
        warnx("client_id already set");
        goto fail;
    }

    if ((client->client_id = read_utf8(&ptr, &bytes_left)) == NULL)
        goto fail;
    dbg_printf("[  ] handle_cp_connect: client_id=<%s> ",
            (char *)client->client_id);

    /* [MQTT-3.1.3-5] */
    if (is_valid_connection_id(client->client_id) == -1) {
        warn("handle_cp_connect: invalid connection id");
        reason_code = MQTT_CLIENT_IDENTIFIER_NOT_VALID;
        goto fail;
    }

    if (connect_flags & MQTT_CONNECT_FLAG_CLEAN_START) {
        dbg_printf("clean_start ");
    }

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) {
        dbg_printf("will_properties ");
        if (parse_properties(&ptr, &bytes_left, &will_props,
                    &num_will_props, MQTT_CP_INVALID) == -1)
            goto fail;

        dbg_printf("[%d props] will_topic ", num_will_props);

        will_topic = read_utf8(&ptr, &bytes_left);
        if (will_topic == NULL)
            goto fail;


        if ((will_payload = read_binary(&ptr, &bytes_left,
                        &will_payload_len)) == NULL)
            goto fail;

        dbg_printf("[%ub] ", will_payload_len);
        will_retain = (connect_flags & MQTT_CONNECT_FLAG_WILL_RETAIN);
    }

    if (connect_flags & MQTT_CONNECT_FLAG_WILL_RETAIN) {
        dbg_printf("will_retain ");
        if ((connect_flags & MQTT_CONNECT_FLAG_WILL_FLAG) == 0) {
            reason_code = MQTT_PROTOCOL_ERROR;
            warn("handle_cp_connect: Will Retain set without Will Flag");
            goto fail;
        }
    }

    if (connect_flags & MQTT_CONNECT_FLAG_USERNAME) {
        dbg_printf("username ");
        if ((client->username = read_utf8(&ptr, &bytes_left)) == NULL)
            goto fail;

        dbg_printf("<%s> ", (char *)client->username);
    }

    if (connect_flags & MQTT_CONNECT_FLAG_PASSWORD) {
        dbg_printf("password ");
        if ((client->password = read_binary(&ptr, &bytes_left,
                        &client->password_len)) == NULL)
            goto fail;

        dbg_printf("[%ub] ", client->password_len);
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

    dbg_printf("will_qos [%u]\n", will_qos);

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

    if ((client->session = find_session(client)) == NULL) {
        /* New Session */
        dbg_printf(BWHT"     handle_cp_connect: no existing session"CRESET"\n");

create_new_session:
        if ((client->session = alloc_session(client)) == NULL) {
            warn("handle_cp_connect: alloc_session failed");
            reason_code = MQTT_UNSPECIFIED_ERROR;
            goto fail;
        }
        dbg_printf("[%2d] handle_cp_connect: new session\n", client->session->id);
    } else {
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
            client->session->state = SESSION_DELETE;
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

        INC_REFCNT(&client->session->will_topic->refcnt);

        client->session->will_retain         = will_retain;
        client->session->will_payload        = will_payload;
        client->session->will_payload_len    = will_payload_len;
        client->session->will_qos            = will_qos;
        client->session->will_payload_format = payload_format;
        client->session->will_props          = will_props;
        client->session->num_will_props      = num_will_props;
        dbg_printf(BBLU "[%2d] handle_cp_connect: setting session will_message"CRESET"\n", client->session->id);
    }

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_SESSION_EXPIRY_INTERVAL, &prop) == 0)
        client->session->expiry_interval = prop->byte4;

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_REQUEST_RESPONSE_INFORMATION, &prop) == 0)
        client->session->request_response_information = prop->byte;

    if (get_property_value(packet->properties, packet->property_count,
                MQTT_PROP_REQUEST_PROBLEM_INFORMATION, &prop) == 0)
        client->session->request_problem_information = prop->byte;

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
    }

    if (send_cp_connack(client, MQTT_SUCCESS) == -1) {
        reason_code = MQTT_UNSPECIFIED_ERROR;
        warn("handle_cp_connect: send_cp_connack failed");
        goto fail;
    }

    client->session->state = SESSION_ACTIVE;

    logger(LOG_INFO, client, "handle_cp_connect: session established%s%s",
            reconnect ? " (reconnect)" : "",
            clean ? " (clean_start)" : "");

    return 0;

fail:
    if (send_cp_connack(client, reason_code) == -1) {
        client->state = CS_CLOSING;
        if (client->disconnect_reason == 0)
            client->disconnect_reason = reason_code;
    }

    if (will_topic)
        free(will_topic);
    if (will_props)
        free_properties(will_props, num_will_props);
    if (will_payload)
        free(will_payload);

    if (errno == 0)
        errno = EINVAL;

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
    if (disconnect_if_malformed(client, reason_code))
        return -1;

    return -1;
}

/*
 * control packet function lookup table
 */

static const struct {
    const control_func_t func;
    const bool needs_auth;
} control_functions[MQTT_CP_MAX] = {
    [MQTT_CP_PUBLISH]     = { handle_cp_publish     , true  },
    [MQTT_CP_PUBACK]      = { handle_cp_puback      , true  },
    [MQTT_CP_PUBREC]      = { handle_cp_pubrec      , true  },
    [MQTT_CP_PUBREL]      = { handle_cp_pubrel      , true  },
    [MQTT_CP_PUBCOMP]     = { handle_cp_pubcomp     , true  },
    [MQTT_CP_SUBSCRIBE]   = { handle_cp_subscribe   , true  },
    [MQTT_CP_UNSUBSCRIBE] = { handle_cp_unsubscribe , true  },
    [MQTT_CP_CONNECT]     = { handle_cp_connect     , false },
    [MQTT_CP_PINGREQ]     = { handle_cp_pingreq     , true  },
    [MQTT_CP_DISCONNECT]  = { handle_cp_disconnect  , true  },
    [MQTT_CP_AUTH]        = { handle_cp_auth        , false },
};

/*
 * other functions
 */


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
            dbg_printf("\n[%2d] parse_incoming: READ_STATE_NEW client=%u <%s>\n",
                    client->session ? client->session->id : (id_t)-1,
                    client->id, (char *)client->client_id);
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
            if (rd_len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) ) {
                return 0;
            } else if (rd_len == -1) {
                log_io_error(NULL, rd_len, client->read_need, false);
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
                    client->disconnect_reason = MQTT_PACKET_TOO_LARGE;
                    errno = EFBIG;
                    goto fail;
                }

                dbg_printf("[%2d] parse_incoming: client=%u type=%u <"
                        BRED"%s"CRESET"> flags=%u remaining_length=%u\n",
                        client->session ? client->session->id : (id_t)-1,
                        client->id,
                        hdr->type, control_packet_str[hdr->type],
                        hdr->flags, client->rl_value);

                if ((client->new_packet = alloc_packet(client)) == NULL) {
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

            if (rd_len == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return 0;
            } else if (rd_len == -1) {
                log_io_error(NULL, rd_len, client->read_need, false);
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

            /* [MQTT-3.1.4-6] - maybe */
            if (!control_functions[client->new_packet->type].needs_auth ||
                    client->is_auth)
                if (control_functions[client->new_packet->type].func(client,
                            client->new_packet, client->packet_buf) == -1) {
                    warn("control_function");
                    goto fail;
                }
            client->last_keep_alive = time(NULL);

            if (IF_DEC_REFCNT(&client->new_packet->refcnt) == 1) {
                free_packet(client->new_packet, true, true);
            } else {
                dbg_printf("[%2d] parse_incoming: can't free packet refcnt>0\n",
                        client->session ? client->session->id : (id_t)-1);
            }
            client->new_packet = NULL;

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

    return -1;
}

/* Clients */

static void client_tick(void)
{
    const struct property *prop;
    uint32_t will_delay = 0;
    const time_t now = time(NULL);

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
                if (clnt->keep_alive &&
                        now > clnt->last_keep_alive + (clnt->keep_alive * 1.5f)) {
                    warnx("client_tick: closing idle link: no PINGREQ within Keep Alive");
                    goto force_close;
                }

                break;

            case CS_NEW:
                break;

            case CS_DISCONNECTED:
                logger(LOG_INFO, clnt, "client_tick: client disconnected");
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
                        clnt->session->state = SESSION_DELETE;
                    } else if (clnt->session->expiry_interval == UINT_MAX) {
                        clnt->session->expires_at = LONG_MAX; /* "does not expire" */
                    } else {
                        clnt->session->expires_at =
                            now + clnt->session->expiry_interval;
                    }

                    DEC_REFCNT(&clnt->session->refcnt); /* handle_cp_connect */
                    clnt->session = NULL;
                }
                goto skip_send_disconnect;

            case CS_CLOSING:
                if (clnt->session)
                    warnx("[%2d] client_tick: session present in CS_CLOSING",
                            clnt->session->id);

                if (clnt->disconnect_reason) {
                    logger(LOG_NOTICE, clnt,
                            "client_tick: disconnecting client with reason %s",
                            reason_codes_str[clnt->disconnect_reason]);
                    send_cp_disconnect(clnt, clnt->disconnect_reason);
                    clnt->disconnect_reason = 0;
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

    /* This handles the case the message has no target */
    if (msg->delivery_states == NULL) {
        if (msg->topic) {
            if (dequeue_message(msg) == -1)
                warn("tick_msg: dequeue_message failed");
            if (!msg->retain || msg->topic == NULL
                    || msg->topic->retained_message != msg)
                msg->state = MSG_DEAD;
        }
        return;
    }

    const time_t now = time(NULL);

    dbg_printf(NGRN "     tick_msg: id=%u sender.id=%d #mds=%u"CRESET"\n",
            msg->id, msg->sender ? msg->sender->id : (id_t)-1,
            msg->num_message_delivery_states);

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

        if (mds->last_sent && (now - mds->last_sent < 5))
            continue;

        if (mds->session == NULL || mds->message != msg) {
            warnx("     tick_msg: message_delivery_state is corrupt");
            continue;
        }

        /* disconnected session */
        if (mds->session->client == NULL) {
            continue;
        }

        dbg_printf(BGRN
                "     tick_msg: sending message: id=%u subscriber.id=%d <%s> ackat=%lu lastsent=%lu"
                CRESET"\n",
                mds->id, mds->session->id, (char *)mds->session->client_id,
                mds->acknowledged_at, mds->last_sent);

        if ((packet = alloc_packet(mds->session->client)) == NULL) {
            warn("tick_msg: unable to alloc_packet for msg on topic <%s>",
                    msg->topic->name);
            continue;
        }

        /* this code might execute more than once, so avoid a double refcnt */
        if (mds->last_sent == 0)
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
            warn("tick_msg: unable to send_cp_publish");
            DEC_REFCNT(&packet->refcnt);
            free_packet(packet, true, true); /* Anything else? */
            packet = NULL;
            continue;
        }

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

    //dbg_printf(NGRN"     tick_msg: num_sent=%u num_message_delivery_states=%u"CRESET"\n",
    //        num_sent, msg->num_message_delivery_states);

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
            if (idx >= msg->num_message_delivery_states)
                break;

            mds = msg->delivery_states[idx++];

            if (mds == NULL) {
                dbg_printf(BYEL"     tick_msg: mds.idx=%u is NULL"CRESET"\n", idx-1);
                break;
            }

            dbg_printf(NGRN"     tick_msg: mds.idx=%u acc=%lu ack=%lu rel=%lu cmp=%lu"CRESET"\n",
                    idx - 1,
                    mds->accepted_at,
                    mds->acknowledged_at,
                    mds->released_at,
                    mds->completed_at);

            if (mds->completed_at == 0)
                goto again;

            dbg_printf(NGRN
                    "     tick_msg: mds.idx=%u unlink mds.id=%u from session %u[%u] and message %u[%u]"
                    CRESET"\n",
                    idx - 1,
                    mds->id,
                    mds->session ? mds->session->id : 0,
                    mds->session ? mds->session->refcnt : 0,
                    mds->message ? mds->message->id : 0,
                    mds->message ? mds->message->refcnt : 0);

            mds_detach_and_free(mds, true, false);
            mds = NULL;
        }

        /* We can't just dequeue() and MSG_DEAD if any mds are not complated_at */
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
            if (max_messages-- == 0)
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
                /* as the session is SESSION_DELETE, check we're unsubscribed
                 * which should resolve the refcnt to be 0 */
                /* TODO replace this with a unsubscribe_from_topics() */
                unsubscribe_session_from_all(session);
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
                session->state = SESSION_DELETE;

                if (session->will_at) {
                    dbg_printf(BBLU"[%2d] session_tick: force_will"CRESET"\n",
                            session->id);
                    goto force_will;
                }
            }
        }

        if (session->will_at && (now > session->will_at)) {
force_will:
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
will_fail:

            session->will_payload = NULL;
            session->will_retain = false;

            DEC_REFCNT(&session->will_topic->refcnt);
            session->will_topic = NULL;
            session->will_at = 0;
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
    session_tick();
    client_tick();
    topic_tick();
    message_tick();
    packet_tick();
}

struct start_args {
    int fd;
    int om_fd;
};

#ifdef WITH_THREADS
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

#ifdef WITH_THREADS
static RETURN_TYPE om_loop(void *start_args)
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
            logger(LOG_WARNING, NULL, "om_loop: select: %s", strerror(errno));
            sleep(1);
        }

        if (rc == 0)
            continue;

        if ((fd = accept(om_fd, NULL, NULL)) == -1) {
            logger(LOG_WARNING, NULL, "om_loop: accept: %s", strerror(errno));
            sleep(1);
            continue;
        }

        openmetrics_export(fd);
        close_socket(&fd);
    }

    logger(LOG_INFO, NULL, "om_loop: terminated normally");

    return 0;
}
#endif

static RETURN_TYPE main_loop(void *start_args)
{
    bool has_clients;
    fd_set fds_in, fds_out, fds_exc;
    int mother_fd = ((const struct start_args *)start_args)->fd;
    int om_fd = ((const struct start_args *)start_args)->om_fd;

    while (running)
    {
        int max_fd = mother_fd > om_fd ? mother_fd : om_fd;
        int rc = 0;
        struct timeval tv;

        FD_ZERO(&fds_in);
        FD_ZERO(&fds_out);
        FD_ZERO(&fds_exc);

        FD_SET(mother_fd, &fds_in);
        if (om_fd > 0)
            FD_SET(om_fd, &fds_in);

        has_clients = false;

        pthread_rwlock_rdlock(&global_clients_lock);
        for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
        {
            if (clnt->state == CS_NEW || clnt->state == CS_DISCONNECTED ||
                    clnt->fd == -1)
                continue;

            if (clnt->fd > max_fd)
                max_fd = clnt->fd;

            FD_SET(clnt->fd, &fds_in);
            FD_SET(clnt->fd, &fds_out);
            FD_SET(clnt->fd, &fds_exc);

            has_clients = true;
        }
        pthread_rwlock_unlock(&global_clients_lock);

        tv.tv_sec = 0;

        if (has_clients == false) {
            tv.tv_usec = 100000;
            rc = select(max_fd + 1, &fds_in, NULL, NULL, &tv);
        } else {
            tv.tv_usec = 10000;
            rc = select(max_fd + 1, &fds_in, NULL, &fds_exc, &tv);

            /* this is a kludge but not sure how else a) get a hint at blocked
             * writes and b) avoid select instantly returning (as any
             * non-blocking writable fd seems to terminate the select,
             * i.e. all of them */
            tv.tv_sec = 0;
            tv.tv_usec = 1000;
            select(max_fd + 1, NULL, &fds_out, NULL, &tv);
        }

        if (rc == 0) {
            /* a timeout occured, but no fds */
#ifndef WITH_THREADS
            goto tick_me;
#endif
        } else if (rc == -1 && (errno == EAGAIN || errno == EINTR)) {
            /* TODO calculate the remaining time to sleep? */
            continue;
        } else if (rc == -1) {
            warn("main_loop: select");
#ifdef WITH_THREADS
            pthread_exit(&errno);
#else
            running = false;
            return -1;
#endif
        }

        if (om_fd > 0 && FD_ISSET(om_fd, &fds_in)) {
            int tmp_fd;

            if ((tmp_fd = accept(om_fd, NULL, NULL)) != -1) {
                openmetrics_export(tmp_fd);
                close(tmp_fd);
            }
        }

        if (FD_ISSET(mother_fd, &fds_in)) {
            struct sockaddr_in sin_client;
            socklen_t sin_client_len = sizeof(sin_client);
            int child_fd;
            struct client *new_client;

            dbg_printf("     main_loop: new connection\n");
            if ((child_fd = accept(mother_fd,
                            (struct sockaddr *)&sin_client,
                            &sin_client_len)) == -1) {
                warn("main_loop: accept failed");
                continue;
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

            if (inet_ntop(AF_INET, &sin_client.sin_addr.s_addr,
                        new_client->hostname, sin_client_len) == NULL)
                warn("inet_ntop");

            new_client->fd = child_fd;
            new_client->state = CS_ACTIVE;
            new_client->tcp_accepted_at = time(NULL);
            new_client->remote_port = ntohs(sin_client.sin_port);
            new_client->remote_addr = ntohl(sin_client.sin_addr.s_addr);


            dbg_printf("     main_loop: new client from [%s:%u]\n",
                    new_client->hostname, new_client->remote_port);
            logger(LOG_INFO, new_client, "main_loop: new connection");
        }

        pthread_rwlock_rdlock(&global_clients_lock);
        for (struct client *clnt = global_client_list; clnt; clnt = clnt->next)
        {
            if (clnt->state != CS_ACTIVE || clnt->fd == -1)
                continue;

            if (FD_ISSET(clnt->fd, &fds_in)) {
                if (parse_incoming(clnt) == -1) {
                    /* TODO do something? */ ;
                }
            }

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

            if (FD_ISSET(clnt->fd, &fds_exc)) {
                dbg_printf("     main_loop exception event on %p[%d]\n",
                        (void *)clnt, clnt->fd);
                /* TODO close? */
            }
        }
        pthread_rwlock_unlock(&global_clients_lock);

#ifndef WITH_THREADS
tick_me:
            tick();
#endif
    }

    logger(LOG_INFO, NULL, "main_loop: terminated normally");

    errno = 0;
#ifdef WITH_THREADS
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

static bool is_null_uuid(const uint8_t uuid[const static 16])
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

    if (find_topic((void *)save->name) != NULL) {
        logger(LOG_WARNING, NULL, "open_databases: duplicate topic for %s",
                save->name);
        errno = EEXIST;
        return -1;
    }

    if ((topic = register_topic((void *)save->name, save->uuid)) == NULL)
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

static const struct {
    DBM **global;
    char *filename;
    int (*const func)(datum key, datum content);
    size_t size;
} database_init[] = {
    { &message_dbm, "messages", load_message, sizeof(struct message_save) },
    { &topic_dbm, "topics", load_topic, sizeof(struct topic_save) },
    { NULL, NULL, NULL, -1 },
};

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
    opt_listen.s_addr = htonl(INADDR_LOOPBACK);
    opt_om_listen.s_addr = htonl(INADDR_LOOPBACK);

    char *logfile_name = NULL;

    {
        int opt;
        char *subopts;
        char *value;

        enum {
            LOGGER_SYSLOG = 0,
            LOGGER_FILE,
            LOGGER_STDOUT,
            LOGGER_NOSTDOUT,
        };

        char *const logger_token[] = {
            [LOGGER_SYSLOG] = "syslog",
            [LOGGER_FILE] = "file",
            [LOGGER_STDOUT] = "stdout",
            [LOGGER_NOSTDOUT] = "nostdout",
            NULL
        };

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

        while ((opt = getopt(argc, argv, "hVp:H:l:do:")) != -1)
        {
            switch (opt)
            {
                case 'd':
                    opt_background = true;
                    break;
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
                                if (inet_pton(AF_INET, value, &opt_om_listen) == -1)
                                    goto shit_usage;
                                break;
                            default:
                                goto shit_usage;
                        }
                    }
                    break;
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
                            default:
                                goto shit_usage;
                        }
                    }
                    break;
                case 'H':
                    if (inet_pton(AF_INET, optarg, &opt_listen) == -1) {
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

    if (logfile_name) {
        if ((opt_logfile = fopen(logfile_name,
                        opt_logfileappend ? "a" : "w")) == NULL)
            errx(EXIT_FAILURE, "fopen(%s)", logfile_name);
        setvbuf(opt_logfile, NULL, _IONBF, 0);
        atexit(close_logfile);
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

    if (open_databases() == -1)
        logger(LOG_EMERG, NULL, "open_databases: %s", strerror(errno));
    atexit(close_databases);

    atexit(close_all_sockets);
    atexit(free_all_topics_two);
    atexit(free_all_sessions);
    atexit(free_all_message_delivery_states);
    atexit(free_all_messages);
    atexit(free_all_topics);
    atexit(free_all_packets);
    atexit(free_all_clients);

    atexit(save_all_topics);

    if (get_first_hwaddr(global_hwaddr, sizeof(global_hwaddr)) == -1)
        logger(LOG_EMERG, NULL, "main: cannot find MAC address");

    logger(LOG_INFO, NULL,
            "main: using hardware address %02x:%02x:%02x:%02x:%02x:%02x",
            global_hwaddr[0], global_hwaddr[1], global_hwaddr[2],
            global_hwaddr[3], global_hwaddr[4], global_hwaddr[5]
          );

    while (optind < argc)
    {
        if (is_valid_topic_filter((const uint8_t *)argv[optind]) == -1) {
            logger(LOG_WARNING, NULL, "main: command line topic creation: <%s> is not a valid topic filter, skipping", argv[optind]);
        } else if (find_topic((const uint8_t *)argv[optind]) != NULL) {
            logger(LOG_WARNING, NULL, "main: command line topic creation: topic <%s> already exists, skipping", argv[optind]);
        } else if (register_topic((const uint8_t *)argv[optind], NULL) == NULL) {
            logger(LOG_WARNING, NULL, "main: command line topic creation: register_topic<%s> failed, skipping: %s", argv[optind], strerror(errno));
        } else {
            logger(LOG_INFO, NULL, "main: command line topic creation: topic <%s> created", argv[optind]);
        }
        optind++;
    }

    if ((global_mother_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
        logger(LOG_EMERG, NULL, "socket(mother): %s", strerror(errno));

    if (opt_openmetrics)
        if ((global_om_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1)
            logger(LOG_EMERG, NULL, "socket(om): %s", strerror(errno));

    struct linger linger = {
        .l_onoff = 0,
        .l_linger = 0,
    };

    if (setsockopt(global_mother_fd, SOL_SOCKET, SO_LINGER, &linger,
                sizeof(linger)) == -1)
        warn("setsockopt(SO_LINGER, mother)");

    if (opt_openmetrics)
        if (setsockopt(global_om_fd, SOL_SOCKET, SO_LINGER, &linger,
                    sizeof(linger)) == -1)
            warn("setsockopt(SO_LINGER, openmetrics)");

    int reuse = 1;

    if (setsockopt(global_mother_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                sizeof(reuse)) == -1)
        warn("setsockopt(SO_REUSEADDR, mother)");

    if (opt_openmetrics)
        if (setsockopt(global_om_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                    sizeof(reuse)) == -1)
            warn("setsockopt(SO_REUSEADDR, openmetrics)");

    struct sockaddr_in sin = {0};
    char bind_addr[INET_ADDRSTRLEN];

    /* Mother FD */
    sin.sin_family = AF_INET;
    sin.sin_port = htons(opt_port);
    sin.sin_addr.s_addr = opt_listen.s_addr;
    inet_ntop(AF_INET, &sin.sin_addr, bind_addr, sizeof(bind_addr));
    logger(LOG_NOTICE, NULL, "main: binding to %s:%u", bind_addr, opt_port);

    if (bind(global_mother_fd, (struct sockaddr *)&sin, sizeof(sin)) == -1)
        logger(LOG_EMERG, NULL, "bind(mother): %s", strerror(errno));

    if (listen(global_mother_fd, opt_backlog) == -1)
        logger(LOG_EMERG, NULL, "listen(mother): %s", strerror(errno));

    /* Openmetrics FD */
    if (opt_openmetrics) {
        sin.sin_family = AF_INET;
        sin.sin_port = htons(opt_om_port);
        sin.sin_addr.s_addr = opt_om_listen.s_addr;
        inet_ntop(AF_INET, &sin.sin_addr, bind_addr, sizeof(bind_addr));
        logger(LOG_NOTICE, NULL, "main: openmetrics binding to %s:%u", bind_addr, opt_om_port);

        if (bind(global_om_fd, (struct sockaddr *)&sin, sizeof(sin)) == -1)
            logger(LOG_EMERG, NULL, "bind(om): %s", strerror(errno));

        if (listen(global_om_fd, 5) == -1)
            logger(LOG_EMERG, NULL, "listen(om): %s", strerror(errno));
    }

#ifdef WITH_THREADS
    struct start_args start_args = {
        .fd = global_mother_fd,
        .om_fd = -1,
    };

    struct start_args start_args_om = {
        .fd = -1,
        .om_fd = opt_openmetrics ? global_om_fd : 0,
    };
#else
    struct start_args start_args = {
        .fd = global_mother_fd,
        .om_fd = opt_openmetrics ? global_om_fd : 0,
    };
#endif

    running = true;

#ifdef WITH_THREADS
    pthread_t main_thread, tick_thread, om_thread;

    if (opt_openmetrics &&
            pthread_create(&om_thread, NULL, om_loop, &start_args_om) == -1)
        err(EXIT_FAILURE, "pthread_create: om_loop");

    if (pthread_create(&main_thread, NULL, main_loop, &start_args) == -1)
        err(EXIT_FAILURE, "pthread_create: main_thread");

    if (pthread_create(&tick_thread, NULL, tick_loop, NULL) == -1)
        err(EXIT_FAILURE, "pthread_create: tick");

    pthread_join(main_thread, NULL);
    pthread_join(tick_thread, NULL);
    if (opt_openmetrics)
        pthread_join(om_thread, NULL);
#else
    if (main_loop(&start_args) == -1)
        logger(LOG_EMERG, NULL, "main_loop returned an error: %s", strerror(errno));
#endif

    exit(EXIT_SUCCESS);
}
