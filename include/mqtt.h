#ifndef _FAIL_MQTT_H
#define _FAIL_MQTT_H

#include <sys/types.h>
#include <stdint.h>
#include <uchar.h>

struct mqtt_fixed_header {
    unsigned flags:4;
    unsigned type:4;
    //uint8_t length; /* variable */
} __attribute__((packed));

struct mqtt_connect_header {
    uint16_t length; /* must be 4 */
    uint8_t name[4];
    uint8_t version;
    uint8_t flags;
    uint16_t keep_alive;
    //uint8_t properties_length; /* variable */
    /* properties[] */
    /* uint8_t payload[] */
} __attribute__((packed));

struct mqtt_connack_header {
    uint8_t ack_flags;
    uint8_t reason_code;
    // uint8_t properties_length; /* variable */
    /* properties[] */
} __attribute__((packed));

typedef enum {
    // Reserved = 0,
    MQTT_CP_CONNECT = 1,
    MQTT_CP_CONNACK = 2,
    MQTT_CP_PUBLISH = 3,
    MQTT_CP_PUBACK  = 4,
    MQTT_CP_PUBREC  = 5,
    MQTT_CP_PUBREL  = 6,
    MQTT_CP_PUBCOMP = 7,
    MQTT_CP_SUBSCRIBE = 8,
    MQTT_CP_SUBACK  = 9,
    MQTT_CP_UNSUBSCRIBE = 10,
    MQTT_CP_UNSUBACK = 11,
    MQTT_CP_PINGREQ = 12,
    MQTT_CP_PINGRESP = 13,
    MQTT_CP_DISCONNECT = 14,
    MQTT_CP_AUTH = 15,

    MQTT_CP_MAX = 16,
} mqtt_control_packet_type;

#define MQTT_FLAG_PUBREL (1<<1)
#define MQTT_FLAG_SUBSCRIBE (1<<1)
#define MQTT_FLAG_UNSUBSCRIBE (1<<1)

#define MQTT_FLAG_PUBLISH_DUP (1<<3)
#define MQTT_FLAG_PUBLISH_QOS (1<<2|1<<1)
#define MQTT_FLAG_PUBLISH_RETAIN (1<<0)

#define MQTT_CONNECT_FLAG_CLEAN_START   (1<<1)
#define MQTT_CONNECT_FLAG_WILL_FLAG     (1<<2)
#define MQTT_CONNECT_FLAG_WILL_QOS      (1<<3)
#define MQTT_CONNECT_FLAG_WILL_RETAIN   (1<<4)
#define MQTT_CONNECT_FLAG_PASSWORD      (1<<5)
#define MQTT_CONNECT_FLAG_USERNAME      (1<<6)

typedef enum {
    MQTT_TYPE_BYTE = 1,
    MQTT_TYPE_4BYTE = 2,
    MQTT_TYPE_UTF8_STRING = 3,
    MQTT_TYPE_BINARY = 4,
    MQTT_TYPE_VARBYTE = 5,
    MQTT_TYPE_2BYTE = 6,
    MQTT_TYPE_UTF8_STRING_PAIR = 7,

    MQTT_TYPE_MAX = 8
} mqtt_types;

/* Properties */
typedef enum {
    MQTT_PAYLOAD_FORMAT_INDICATOR = 1,
    MQTT_MESSAGE_EXPIRY_INTERVAL = 2,
    MQTT_CONTENT_TYPE = 3,
    MQTT_RESPONSE_TYPE = 8,
    MQTT_CORRLEATION_DATA = 9,
    MQTT_SUBSCRIPTION_IDENTIFIER = 11,
    MQTT_SESSION_EXPIRY_INTERVAL = 17,
    MQTT_ASSIGNED_CLIENT_IDENTIFIER = 18,
    MQTT_SERVER_KEEP_ALIVE = 19,
    MQTT_AUTHENTICATION_METHD = 21,
    MQTT_AUTHENTICATION_DATA = 22,
    MQTT_REQUEST_PROBLEM_INFORMATION = 23,
    MQTT_WILL_DELAY_INTERVAL = 24,
    MQTT_REQUEST_RESPONSE_INFORMATION = 25,
    MQTT_RESPONSE_INFORMATION = 26,
    MQTT_SERVER_REFERENCE = 28,
    MQTT_REASON_STRING = 31,
    MQTT_RECEIVE_MAXIMUM = 33,
    MQTT_TOPIC_ALIAS_MAXIMUM = 34,
    MQTT_TOPIC_ALIAS = 35,
    MQTT_MAXIMUM_QOS = 36,
    MQTT_RETAIN_AVAILABLE = 37,
    MQTT_USER_PROPERTY = 38,
    MQTT_MAXIMUM_PACKET_SIZE = 39,
    MQTT_WILDCARD_SUBSCRIPTION_AVAILABLE = 40,
    MQTT_SUBSCRIPTION_IDENTIFIER_AVAILABLE = 41,
    MQTT_SHARED_SUBSCRIPTION_AVAILABLE = 42,

    MQTT_MAX_PROPERTY_IDENT = 43,
} mqtt_property_ident;

const mqtt_types mqtt_property_to_type[] = {
    [MQTT_PAYLOAD_FORMAT_INDICATOR]          = MQTT_TYPE_BYTE,
    [MQTT_MESSAGE_EXPIRY_INTERVAL]           = MQTT_TYPE_4BYTE,
    [MQTT_CONTENT_TYPE]                      = MQTT_TYPE_UTF8_STRING,
    [MQTT_RESPONSE_TYPE]                     = MQTT_TYPE_UTF8_STRING,
    [MQTT_CORRLEATION_DATA]                  = MQTT_TYPE_BINARY,
    [MQTT_SUBSCRIPTION_IDENTIFIER]           = MQTT_TYPE_VARBYTE,
    [MQTT_SESSION_EXPIRY_INTERVAL]           = MQTT_TYPE_4BYTE,
    [MQTT_ASSIGNED_CLIENT_IDENTIFIER]        = MQTT_TYPE_UTF8_STRING,
    [MQTT_SERVER_KEEP_ALIVE]                 = MQTT_TYPE_2BYTE,
    [MQTT_AUTHENTICATION_METHD]              = MQTT_TYPE_UTF8_STRING,
    [MQTT_AUTHENTICATION_DATA]               = MQTT_TYPE_BINARY,
    [MQTT_REQUEST_PROBLEM_INFORMATION]       = MQTT_TYPE_BYTE,
    [MQTT_WILL_DELAY_INTERVAL]               = MQTT_TYPE_4BYTE,
    [MQTT_REQUEST_RESPONSE_INFORMATION]      = MQTT_TYPE_BYTE,
    [MQTT_RESPONSE_INFORMATION]              = MQTT_TYPE_UTF8_STRING,
    [MQTT_SERVER_REFERENCE]                  = MQTT_TYPE_UTF8_STRING,
    [MQTT_REASON_STRING]                     = MQTT_TYPE_UTF8_STRING,
    [MQTT_RECEIVE_MAXIMUM]                   = MQTT_TYPE_2BYTE,
    [MQTT_TOPIC_ALIAS_MAXIMUM]               = MQTT_TYPE_2BYTE,
    [MQTT_TOPIC_ALIAS]                       = MQTT_TYPE_2BYTE,
    [MQTT_MAXIMUM_QOS]                       = MQTT_TYPE_BYTE,
    [MQTT_RETAIN_AVAILABLE]                  = MQTT_TYPE_BYTE,
    [MQTT_USER_PROPERTY]                     = MQTT_TYPE_UTF8_STRING_PAIR,
    [MQTT_MAXIMUM_PACKET_SIZE]               = MQTT_TYPE_4BYTE,
    [MQTT_WILDCARD_SUBSCRIPTION_AVAILABLE]   = MQTT_TYPE_BYTE,
    [MQTT_SUBSCRIPTION_IDENTIFIER_AVAILABLE] = MQTT_TYPE_BYTE,
    [MQTT_SHARED_SUBSCRIPTION_AVAILABLE]     = MQTT_TYPE_BYTE,
};

typedef enum {
    MQTT_PAYLOAD_NONE = 0,
    MQTT_PAYLOAD_REQUIRED = 1,
    MQTT_PAYLOAD_OPTIONAL = 2,
} mqtt_payload_required;

const mqtt_payload_required mqtt_packet_to_payload[] = {
    [MQTT_CP_CONNECT]     = MQTT_PAYLOAD_REQUIRED,
    [MQTT_CP_CONNACK]     = MQTT_PAYLOAD_NONE,
    [MQTT_CP_PUBLISH]     = MQTT_PAYLOAD_OPTIONAL,
    [MQTT_CP_PUBACK]      = MQTT_PAYLOAD_NONE,
    [MQTT_CP_PUBREC]      = MQTT_PAYLOAD_NONE,
    [MQTT_CP_PUBREL]      = MQTT_PAYLOAD_NONE,
    [MQTT_CP_PUBCOMP]     = MQTT_PAYLOAD_NONE,
    [MQTT_CP_SUBSCRIBE]   = MQTT_PAYLOAD_REQUIRED,
    [MQTT_CP_SUBACK]      = MQTT_PAYLOAD_REQUIRED,
    [MQTT_CP_UNSUBSCRIBE] = MQTT_PAYLOAD_REQUIRED,
    [MQTT_CP_UNSUBACK]    = MQTT_PAYLOAD_REQUIRED,
    [MQTT_CP_PINGREQ]     = MQTT_PAYLOAD_NONE,
    [MQTT_CP_PINGRESP]    = MQTT_PAYLOAD_NONE,
    [MQTT_CP_DISCONNECT]  = MQTT_PAYLOAD_NONE,
    [MQTT_CP_AUTH]        = MQTT_PAYLOAD_NONE,
};

enum {
    MQTT_SUCCESS = 0,
    MQTT_NORMAL_DISCONNECTION = 0,
    MQTT_GRANTED_QOS_0 = 0,
    MQTT_GRANTED_QOS_1 = 1,
    MQTT_GRANTED_QOS_2 = 2,
    MQTT_DISCONNECT_WITH_WILL_MESSAGE = 4,
    MQTT_NO_MATCHING_SUBSCRIBERS = 16,
    MQTT_NO_SUBSCRIPTION_EXISTED = 17,
    MQTT_CONTINUE_AUTHENTICATION = 24,
    MQTT_REAUTHENTICATE = 25 ,
    MQTT_UNSPECIFIED_ERROR = 128,
    MQTT_MALFORMED_PACKET = 129,
    MQTT_PROTOCOL_ERROR = 130,
    MQTT_IMPLEMENTATION_SPECIFIC_ERROR = 131,
    MQTT_UNSUPPORTED_PROTOCOL_VERSION = 132,
    MQTT_CLIENT_IDENTIFIER_NOT_VALID = 133,
    MQTT_BAD_USER_NAME_OR_PASSWORD = 134,
    MQTT_NOT_AUTHORIZED = 135,
    MQTT_SERVER_UNAVAILABLE = 136,
    MQTT_SERVER_BUSY = 137,
    MQTT_BANNED = 138,
    MQTT_SERVER_SHUTTING_DOWN = 139,
    MQTT_BAD_AUTHENTICATION_METHOD = 140,
    MQTT_KEEP_ALIVE_TIMEOUT = 141,
    MQTT_SESSION_TAKEN_OVER = 142,
    MQTT_TOPIC_FILTER_INVALID = 143,
    MQTT_TOPIC_NAME_INVALID = 144,
    MQTT_PACKET_IDENTIFIER_IN_USE = 145,
    MQTT_PACKET_IDENTIFIER_NOT_FOUND = 146,
    MQTT_RECEIVE_MAXIMUM_EXCEEDED = 147,
    MQTT_TOPIC_ALIAS_INVALID = 148,
    MQTT_PACKET_TOO_LARGE = 149,
    MQTT_MESSAGE_RATE_TOO_HIGH = 150,
    MQTT_QUOTA_EXCEEDED = 151,
    MQTT_ADMINISTRATIVE_ACTION = 152,
    MQTT_PAYLOAD_FORMAT_INVALID = 153,
    MQTT_RETAIN_NOT_SUPPORTED = 154,
    MQTT_QOS_NOT_SUPPORTED = 155,
    MQTT_USE_ANOTHER_SERVER = 156,
    MQTT_SERVER_MOVED = 157,
    MQTT_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 158,
    MQTT_CONNECTION_RATE_EXCEEDED = 159,
    MQTT_MAXIMUM_CONNECT_TIME = 160,
    MQTT_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 161,
    MQTT_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 162,
} mqtt_reason_codes;

struct property {
    mqtt_types type;
    union {
        uint8_t byte;
        uint16_t byte2;
        uint32_t byte4;
        unsigned char *utf8_string;
        struct {
            uint16_t len;
            uint8_t *data;
        } binary;
        unsigned char *utf8_pair[2];
        uint32_t varbyte;
    };
};

struct mqtt_packet {
    struct mqtt_packet *next;
    struct mqtt_packet *next_client;
    struct client *owner;

    mqtt_control_packet_type type;
    ssize_t remaining_length;
    unsigned flags;
    
    union {
        struct {
            unsigned flags;
            unsigned length;
            unsigned keep_alive;
            unsigned version;
        } connect_hdr;
    };

    unsigned property_count;
    struct property (*properties)[];

    uint16_t packet_identifier;
    uint32_t payload_len;
    void *payload;
};

struct topic {
    uint8_t *topic;
    uint8_t options;
};

struct client {
    struct client *next;
    struct mqtt_packet *active_packets;

    int fd;
    const uint8_t *client_id;

    struct topic *topics;
    unsigned num_topics;
};


#endif
