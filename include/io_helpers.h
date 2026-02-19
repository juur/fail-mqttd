#ifndef _IO_HELPERS_H
# define _IO_HELPERS_H

# ifndef _XOPEN_SOURCE
#  define _XOPEN_SOURCE 800
# endif

#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>

# ifndef UUID_SIZE
#  define UUID_SIZE 16
# endif

static inline void write_u32(char **restrict out, uint32_t val)
{
    val = htonl(val);
    memcpy(*out, &val, sizeof(val));
    *out += sizeof(val);
}

static inline void write_u16(char **restrict out, uint16_t val)
{
    val = htons(val);
    memcpy(*out, &val, sizeof(val));
    *out += sizeof(val);
}

static inline void write_u8(char **restrict out, uint8_t val)
{
    memcpy(*out, &val, sizeof(val));
    *out += sizeof(val);
}

static inline void write_bytes(char **restrict out, const void *restrict src, size_t len)
{
    memcpy(*out, src, len);
    *out += len;
}

static inline void write_uuid(char **restrict out, const uint8_t *restrict src)
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
#endif
