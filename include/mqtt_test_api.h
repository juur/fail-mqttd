#ifndef _FAIL_MQTT_TEST_API_H
#define _FAIL_MQTT_TEST_API_H

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>
#include <netinet/in.h>

#include "mqtt.h"

struct mqtt_test_api {
	int (*encode_var_byte)(uint32_t value, uint8_t out[static 4]);
	uint32_t (*read_var_byte)(const uint8_t **ptr, size_t *bytes_left);
	void *(*read_binary)(const uint8_t **ptr, size_t *bytes_left, uint16_t *length);
	uint8_t *(*read_utf8)(const uint8_t **ptr, size_t *bytes_left);

	int (*is_valid_utf8)(const uint8_t *str, size_t len);
	int (*is_valid_connection_id)(const uint8_t *str);
	int (*is_valid_topic_name)(const uint8_t *name);
	int (*is_valid_topic_filter)(const uint8_t *name);
	bool (*topic_match)(const uint8_t *name, const uint8_t *filter);

	int (*parse_properties)(const uint8_t **ptr, size_t *bytes_left,
			struct property (**store_props)[], unsigned *store_num_props,
			control_packet_t cp_type);
	int (*build_properties)(const struct property (*props)[],
			unsigned num_props, uint8_t **out);
	ssize_t (*get_properties_size)(const struct property (*props)[],
			unsigned num_props);
	int (*get_property_value)(const struct property (*props)[],
			unsigned num_props, property_ident_t id,
			const struct property **out);
	void (*free_properties)(struct property (*props)[], unsigned count);

	int (*parse_incoming)(struct client *client);
	void (*tick)(void);

	int (*send_cp_puback)(struct client *client, uint16_t packet_id,
			reason_code_t reason_code);
	int (*send_cp_pubrec)(struct client *client, uint16_t packet_id,
			reason_code_t reason_code);
	int (*send_cp_pubrel)(struct client *client, uint16_t packet_id,
			reason_code_t reason_code);
	int (*send_cp_pubcomp)(struct client *client, uint16_t packet_id,
			reason_code_t reason_code);
	int (*send_cp_suback)(struct client *client, uint16_t packet_id,
			struct topic_sub_request *request);
	int (*send_cp_unsuback)(struct client *client, uint16_t packet_id,
			struct topic_sub_request *request);

	struct session *(*alloc_session)(struct client *client,
			const uint8_t uuid[UUID_SIZE]);
	int (*register_session)(struct session *session);
	void (*free_session)(struct session *session, bool need_lock);
};

struct mqtt_test_limits {
	unsigned max_packets;
	unsigned max_clients;
	unsigned max_topics;
	unsigned max_messages;
	uint32_t max_packet_length;
	unsigned max_messages_per_tick;
	unsigned max_properties;
	unsigned max_receive_pubs;
	unsigned max_sessions;
	unsigned max_topic_alias;
	unsigned max_clientid_len;
	uint32_t max_sub_identifier;
};

struct mqtt_test_options {
	FILE **logfile;
	bool *logstdout;
	int *backlog;
	int *loglevel;
	bool *logsyslog;
	bool *logfileappend;
	bool *logfilesync;
	bool *background;
	const char **statepath;
	in_port_t *port;
	bool *database;
	struct in_addr *listen;
	uint8_t *raft_id;
	bool *raft;
	in_port_t *raft_port;
	struct in_addr *raft_listen;
	bool *openmetrics;
	in_port_t *om_port;
	struct in_addr *om_listen;
};

extern const struct mqtt_test_api mqtt_test_api;
extern const struct mqtt_test_limits mqtt_test_limits;
extern const struct mqtt_test_options mqtt_test_options;

#endif
