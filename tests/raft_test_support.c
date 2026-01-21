/*
 * Test stubs for external dependencies used by raft.c.
 */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <pthread.h>

struct client;
struct topic;
struct message;

uint8_t opt_raft_id = 1;
bool opt_raft = true;
in_port_t opt_raft_port = 1883;
struct in_addr opt_raft_listen = { .s_addr = INADDR_ANY };
bool opt_database = false;
struct in_addr opt_listen = { .s_addr = INADDR_ANY };
in_port_t opt_port = 1883;
pthread_rwlock_t global_topics_lock = PTHREAD_RWLOCK_INITIALIZER;
_Atomic bool running = false;

void close_socket(int *fd)
{
	if (fd)
		*fd = -1;
}

void sock_linger(int fd)
{
	(void)fd;
}

void sock_keepalive(int fd)
{
	(void)fd;
}

void sock_nodelay(int fd)
{
	(void)fd;
}

void sock_reuse(int fd, int reuse)
{
	(void)fd;
	(void)reuse;
}

void sock_nonblock(int fd)
{
	(void)fd;
}

void logger(int priority, const struct client *client, const char *format, ...)
{
	(void)priority;
	(void)client;
	(void)format;
}

struct topic *find_topic(const uint8_t *name, bool active_only, bool need_lock)
{
	(void)name;
	(void)active_only;
	(void)need_lock;
	return NULL;
}

struct message *find_message_by_uuid(const uint8_t uuid[static const 16])
{
	(void)uuid;
	return NULL;
}

int attempt_save_all_topics(void)
{
	return -1;
}

int save_topic(const struct topic *topic)
{
	(void)topic;
	return -1;
}

struct topic *register_topic(const uint8_t *name, const uint8_t uuid[const 16]
#ifdef FEATURE_RAFT
		, bool source_self
#endif
		)
{
	(void)name;
	(void)uuid;
#ifdef FEATURE_RAFT
	(void)source_self;
#endif
	return NULL;
}

const char *uuid_to_string(const uint8_t uuid[const static 16])
{
	(void)uuid;
	return "00000000-0000-0000-0000-000000000000";
}
