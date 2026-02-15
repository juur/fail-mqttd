#ifndef _FAIL_MQTT_H
#define _FAIL_MQTT_H

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 800
#endif

#include <sys/types.h>
#include <stdint.h>
#include <uchar.h>
#include <arpa/inet.h>
#include <stdbool.h>

#if defined __has_attribute
# if __has_attribute (counted_by)
# else
#  define counted_by(x)
# endif
#else
# define counted_by(x)
#endif

//#include "config.h"

#ifndef TIMEMS_T_DEFINED
# define TIMEMS_T_DEFINED
typedef long timems_t;
#endif

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
} __attribute__((packed));

struct mqtt_connack_header {
    uint8_t ack_flags;
    uint8_t reason_code;
} __attribute__((packed));

typedef enum {
    MQTT_CP_INVALID = 0,
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

    MQTT_CP_MAX
} control_packet_t;

#define MQTT_FLAG_PUBREL (1<<1)
#define MQTT_FLAG_SUBSCRIBE (1<<1)
#define MQTT_FLAG_UNSUBSCRIBE (1<<1)

#define MQTT_FLAG_PUBLISH_RETAIN (1<<0)
#define MQTT_FLAG_PUBLISH_QOS(x) (((x)&0x3) << 1U)
#define MQTT_FLAG_PUBLISH_QOS0 (0)
#define MQTT_FLAG_PUBLISH_QOS1 (1<<1)
#define MQTT_FLAG_PUBLISH_QOS2 (1<<2)
#define MQTT_FLAG_PUBLISH_QOS_MASK ((1<<1)|(1<<2))
#define MQTT_FLAG_PUBLISH_DUP (1<<3)

#define MQTT_CONNECT_FLAG_RESERVED      (1<<0)
#define MQTT_CONNECT_FLAG_CLEAN_START   (1<<1)
#define MQTT_CONNECT_FLAG_WILL_FLAG     (1<<2)
#define MQTT_CONNECT_FLAG_WILL_QOS(x)   (((x)&0x3) << 3U)
#define MQTT_CONNECT_FLAG_WILL_QOS0     (0)
#define MQTT_CONNECT_FLAG_WILL_QOS1     (1<<3)
#define MQTT_CONNECT_FLAG_WILL_QOS2     (1<<4)
#define MQTT_CONNECT_FLAG_WILL_QOS_MASK ((1<<3)|(1<<4))
#define MQTT_CONNECT_FLAG_WILL_RETAIN   (1<<5)
#define MQTT_CONNECT_FLAG_PASSWORD      (1<<6)
#define MQTT_CONNECT_FLAG_USERNAME      (1<<7)

#define MQTT_CONNACK_FLAG_SESSION_PRESENT (1<<0)
#define MQTT_CONNACK_FLAG_RESERVED        (~(1<<0))

#define MQTT_SUBOPT_QOS0                 (0)
#define MQTT_SUBOPT_QOS1                 (1<<0)
#define MQTT_SUBOPT_QOS2                 (1<<1)
#define MQTT_SUBOPT_QOS_MASK             ((1<<0)|(1<<1))
#define MQTT_SUBOPT_QOS(x)               ((x)&MQTT_SUBOPT_QOS_MASK)
#define MQTT_SUBOPT_NO_LOCAL             (1<<2)
#define MQTT_SUBOPT_RETAIN_AS_PUBLISHED  (1<<3)
#define MQTT_SUBOPT_RETAIN_HANDLING_MASK ((1<<5)|(1<<4))
#define MQTT_SUBOPT_RETAIN_HANDLING(x)   (((x)&0x3) << 4U)
#define MQTT_SUBOPT_RETAIN_HANDLING0     (0)
#define MQTT_SUBOPT_RETAIN_HANDLING1     (1<<4)
#define MQTT_SUBOPT_RETAIN_HANDLING2     (1<<5)
#define MQTT_SUBOPT_RESERVED_MASK        ((1<<7)|(1<<6))

#define GET_WILL_QOS(x) (((x) & MQTT_CONNECT_FLAG_WILL_QOS_MASK) >> 3U)

#define GET_QOS(x) ( ((x) & MQTT_FLAG_PUBLISH_QOS_MASK) >> 1U)
#define SET_QOS(x,y) ( (x) | (((y) & 0x3) <<1U) )

#define UUID_SIZE 16

typedef enum {
    MQTT_TYPE_UNDEFINED = 0,
    MQTT_TYPE_BYTE = 1,
    MQTT_TYPE_4BYTE = 2,
    MQTT_TYPE_UTF8_STRING = 3,
    MQTT_TYPE_BINARY = 4,
    MQTT_TYPE_VARBYTE = 5,
    MQTT_TYPE_2BYTE = 6,
    MQTT_TYPE_UTF8_STRING_PAIR = 7,

    MQTT_TYPE_MAX
} type_t;

/* Properties */
typedef enum {
    MQTT_PROP_PAYLOAD_FORMAT_INDICATOR = 1,
    MQTT_PROP_MESSAGE_EXPIRY_INTERVAL = 2,
    MQTT_PROP_CONTENT_TYPE = 3,
    MQTT_PROP_RESPONSE_TOPIC = 8,
    MQTT_PROP_CORRELATION_DATA = 9,
    MQTT_PROP_SUBSCRIPTION_IDENTIFIER = 11,
    MQTT_PROP_SESSION_EXPIRY_INTERVAL = 17,
    MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER = 18,
    MQTT_PROP_SERVER_KEEP_ALIVE = 19,
    MQTT_PROP_AUTHENTICATION_METHOD = 21,
    MQTT_PROP_AUTHENTICATION_DATA = 22,
    MQTT_PROP_REQUEST_PROBLEM_INFORMATION = 23,
    MQTT_PROP_WILL_DELAY_INTERVAL = 24,
    MQTT_PROP_REQUEST_RESPONSE_INFORMATION = 25,
    MQTT_PROP_RESPONSE_INFORMATION = 26,
    MQTT_PROP_SERVER_REFERENCE = 28,
    MQTT_PROP_REASON_STRING = 31,
    MQTT_PROP_RECEIVE_MAXIMUM = 33,
    MQTT_PROP_TOPIC_ALIAS_MAXIMUM = 34,
    MQTT_PROP_TOPIC_ALIAS = 35,
    MQTT_PROP_MAXIMUM_QOS = 36,
    MQTT_PROP_RETAIN_AVAILABLE = 37,
    MQTT_PROP_USER_PROPERTY = 38,
    MQTT_PROP_MAXIMUM_PACKET_SIZE = 39,
    MQTT_PROP_WILDCARD_SUBSCRIPTION_AVAILABLE = 40,
    MQTT_PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE = 41,
    MQTT_PROP_SHARED_SUBSCRIPTION_AVAILABLE = 42,

    MQTT_PROPERTY_IDENT_MAX
} property_ident_t;

typedef enum {
    MQTT_PAYLOAD_NONE = 0,
    MQTT_PAYLOAD_REQUIRED = 1,
    MQTT_PAYLOAD_OPTIONAL = 2,
    MQTT_PAYLOAD_REQUIRED_MAX
} payload_required_t;

typedef enum {
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
    MQTT_REASON_CODE_MAX
} reason_code_t;

typedef enum {
    CS_NEW = 0,
    CS_ACTIVE,
    CS_CLOSING,
    CS_CLOSED,
    CS_DISCONNECTED,
    CLIENT_STATE_MAX
} client_state_t;

typedef enum {
    TOPIC_NEW = 0,
    TOPIC_PREACTIVE,
    TOPIC_ACTIVE,
    TOPIC_DEAD,
    TOPIC_STATE_MAX
} topic_state_t;

typedef enum {
    MSG_NEW = 0,
    MSG_PREACTIVE,
    MSG_ACTIVE,
    MSG_DEAD,
    MSG_STATE_MAX
} message_state_t;

typedef enum {
    MDS_NEW = 0,
    MDS_ACTIVE,
    MDS_DELETED,
    MDS_STATE_MAX
} mds_state_t;

typedef enum {
    MSG_NORMAL = 0,
    MSG_WILL = 1,
    MSG_TYPE_MAX
} message_type_t;

typedef enum {
    SESSION_NEW = 0,
    SESSION_ACTIVE,
    SESSION_DELETE,
    SESSION_STATE_MAX
} session_state_t;

typedef enum {
    ROLE_SEND = 0,
    ROLE_RECV = 1,
    ROLE_MAX
} role_t;

/* client.parse_state */
typedef enum {
    READ_STATE_NEW = 0,
    READ_STATE_HEADER,
    READ_STATE_MORE_HEADER,
    READ_STATE_BODY,
    READ_STATE_MAX
} read_state_t;

typedef enum {
    PACKET_IN = 0,
    PACKET_OUT = 1,
    PACKET_DIR_MAX
} packet_type_t;

typedef enum {
    SUB_NON_SHARED = 0,
    SUB_SHARED,
    SUB_TYPE_MAX,
} subscription_type_t;

struct property {
    property_ident_t ident;
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

struct packet {
    struct packet *next;
    id_t id;
    bool deleted;

    /* HEAD for next_client list */
    struct client *owner;
    struct packet *next_client;

    control_packet_t type;
    size_t remaining_length;
    unsigned flags;

    struct property (*properties)[];
    void *payload;
    struct message *message;
    uint16_t packet_identifier;
    uint32_t payload_len;
    unsigned property_count;
    reason_code_t reason_code;
    packet_type_t direction;

    _Atomic unsigned refcnt;
};

struct client;
struct subscription;

struct message_delivery_state {
    struct message_delivery_state *next;
    mds_state_t state;
    id_t id;
    struct session *session; /* _Nonnull */
    struct message *message; /* _Nonnull */
    bool read_only;
    uint16_t packet_identifier;
    uint8_t qos;
                                /*   QoS=1     |   QoS=2     */
    union {
      time_t last_sent;         /*   PUBLISH-> |   PUBLISH-> */
      time_t accepted_at;
    };
    time_t acknowledged_at;     /* ->PUBACK    | ->PUBREC    */
    time_t released_at;         /* ->PUBACK    |   PUBREL->  */
    time_t completed_at;        /* ->PUBACK    | ->PUBCOMP   */

    reason_code_t client_reason;
};

struct message_save {
    uint64_t id;
    uint8_t uuid[UUID_SIZE];
    uint8_t topic_uuid[UUID_SIZE];
    uint8_t format;
    uint8_t retain;
    uint8_t qos;
    uint8_t type;
    uint32_t payload_len;
    uint8_t payload[] __attribute__((counted_by(payload_len)));
};

struct message {
    struct message *next;
    id_t id;
    bool deleted;
    struct message *next_queue;
    struct session *sender;
    struct topic *topic;
    const void *payload;
    uint8_t format;
    size_t payload_len;
    unsigned qos;
    _Atomic message_state_t state;
    message_type_t type;
    bool retain;
    uint8_t uuid[UUID_SIZE];

    unsigned num_message_delivery_states;
    struct message_delivery_state **delivery_states;
    pthread_rwlock_t delivery_states_lock;

    struct message_delivery_state sender_status;
#if 0
                                /*   QoS=1    |   QoS=2     */
    time_t accepted_at;         /* ->PUBLISH  | ->PUBLISH   */
    time_t acknowledged_at;     /*   PUBACK-> |   PUBREC->  */
    time_t released_at;         /*   PUBACK-> | ->PUBREL    */
    time_t completed_at;        /*   PUBACK-> |   PUBCOMP-> */
#endif

    _Atomic unsigned refcnt;
};

struct topic_sub_request {
    id_t id;
    const uint8_t **topics;
    //struct topic **topic_refs;
    uint8_t *options;
    uint8_t *reason_codes;
    unsigned num_topics;
    uint32_t subscription_identifier;
};

struct subscription {
    struct subscription *next;
    id_t id;
    union {
        struct {
            struct session *session;
        } non_shared;
        struct {
            const uint8_t *share_name;
            struct session **sessions;
            uint8_t *qos_levels;
            unsigned num_sessions;
        } shared;
    };
    const uint8_t *topic_filter;
    uint8_t option;
    subscription_type_t type;
    uint32_t subscription_identifier;
};

struct session {
    struct session *next;
    id_t id;
    uint8_t uuid[UUID_SIZE];
    struct client *client;

    pthread_rwlock_t subscriptions_lock;
    struct subscription **subscriptions;
    unsigned num_subscriptions;

    unsigned num_message_delivery_states;
    struct message_delivery_state **delivery_states;
    pthread_rwlock_t delivery_states_lock;

    const uint8_t *client_id;

    time_t last_connected;
    time_t expires_at;

    uint32_t expiry_interval;
    bool request_response_information;
    bool request_problem_information;

    _Atomic session_state_t state;

    /* Will Flag handling */
    struct topic *will_topic;
    struct property (*will_props)[];
    unsigned num_will_props;
    void *will_payload;
    size_t will_payload_len;
    unsigned will_payload_format;
    unsigned will_qos;
    bool will_retain;
    time_t will_at;

    _Atomic unsigned kill_attempts;
    _Atomic unsigned refcnt;
};

struct client {
    struct client *next;
    id_t id;
    pthread_t owner;
    struct session *session;

    pthread_rwlock_t active_packets_lock;
    struct packet *active_packets;

    const uint8_t *client_id;
    const uint8_t *username;
    const uint8_t *password;
    _Atomic client_state_t state;
    int fd;

    /* host byte order */
    in_addr_t remote_addr;
    in_port_t remote_port;

    uint16_t password_len;
    uint16_t last_packet_id;
    uint8_t connect_flags;
    uint8_t connect_response_flags;
    uint8_t protocol_version;
    uint16_t keep_alive;
    bool keep_alive_override;
    time_t tcp_accepted_at;
    time_t last_connected;
    time_t last_keep_alive;
    reason_code_t disconnect_reason;
    bool send_disconnect;
    uint32_t maximum_packet_size;
    bool is_auth;
    bool write_ok;
    unsigned send_quota;

    /* used by parse_incoming() */
    uint8_t *packet_buf;
    struct packet *new_packet;
    _Atomic read_state_t parse_state;
    unsigned packet_offset;
    unsigned read_offset;
    unsigned read_need;
    unsigned rl_value;
    unsigned rl_multi;
    unsigned rl_offset;
    uint8_t header_buffer[sizeof(struct mqtt_fixed_header) + 4];

    /* packet outbound (po) data */
    const uint8_t *po_buf;
    unsigned po_size;
    unsigned po_offset;
    unsigned po_remaining;
    pthread_rwlock_t po_lock;

    _Atomic unsigned refcnt;

    uint16_t topic_alias_maximum; /* Client, Server is MAX_TOPIC_ALIAS */

    const uint8_t **clnt_topic_aliases;
    const uint8_t **svr_topic_aliases;

    char hostname[INET_ADDRSTRLEN];
};

struct topic_save {
    id_t id;
    uint8_t uuid[UUID_SIZE];
    char name[128];
    uint8_t retained_message_uuid[UUID_SIZE];
} __attribute__((packed));

struct topic {
    struct topic *next;
    id_t id;
    uint8_t uuid[UUID_SIZE];
    const uint8_t *name;
    struct message *retained_message;
    uint8_t retained_msg_uuid[UUID_SIZE]; /* used during loading */
    bool retained_msg_link;
    struct message *pending_queue;
    pthread_rwlock_t pending_queue_lock;
    _Atomic topic_state_t state;
    _Atomic unsigned refcnt;
};

/* used to help build a uuid */
struct uuid_build {
    uint32_t time_low;
    uint16_t time_mid;
    uint16_t time_hi_and_version;
    uint8_t clk_seq_hi_res;
    uint8_t clk_seq_low;
    uint16_t node01;
    uint32_t node25;
};

struct start_args {
    int fd;
    int om_fd;
};

extern const payload_required_t packet_to_payload[MQTT_CP_MAX];
extern const uint8_t packet_permitted_flags[MQTT_CP_MAX];
extern const type_t property_to_type[MQTT_PROPERTY_IDENT_MAX];
extern const type_t property_per_control[MQTT_PROPERTY_IDENT_MAX][MQTT_CP_MAX];

extern const char *const client_state_str[CLIENT_STATE_MAX];
extern const char *const message_state_str[MSG_STATE_MAX];
extern const char *const topic_state_str[TOPIC_STATE_MAX];
extern const char *const session_state_str[SESSION_STATE_MAX];
extern const char *const property_str[MQTT_PROPERTY_IDENT_MAX];
extern const char *const control_packet_str[MQTT_CP_MAX];
extern const char *const read_state_str[READ_STATE_MAX];
extern const char *const priority_str[];
extern const char *const reason_codes_str[MQTT_REASON_CODE_MAX];
extern const char *const message_type_str[MSG_TYPE_MAX];
extern const char *const subscription_type_str[SUB_TYPE_MAX];
extern const char *const packet_dir_str[PACKET_DIR_MAX];

typedef enum {
    T_NULL = 0,
    T_CLIENT,
    T_MESSAGE,
    T_MDS,
    T_PACKET,
    T_SESSION,
    T_SUBSCRIPTION,
    T_TOPIC,
    T_MAX,
} mqtt_type;

#endif

