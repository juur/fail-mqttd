#ifndef RAFT_TEST_IO_H
#define RAFT_TEST_IO_H

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 800
#endif

#include <errno.h>
#include <stdint.h>
#include <unistd.h>

#include "test_io.h"

static inline int raft_test_set_nonblock(int fd)
{
	return test_set_nonblock(fd);
}

static inline ssize_t raft_test_fill_pipe_nonblocking(int fd, uint8_t *buf,
		size_t buf_len)
{
	ssize_t total = 0;
	ssize_t rc;

	for (;;) {
		rc = write(fd, buf, buf_len);
		if (rc > 0) {
			total += rc;
			continue;
		}
		if (rc == -1 && errno == EAGAIN)
			break;
		return -1;
	}

	return total;
}

static inline int raft_test_read_full(int fd, uint8_t *buf, size_t len)
{
	return test_read_full(fd, buf, len);
}

#endif
