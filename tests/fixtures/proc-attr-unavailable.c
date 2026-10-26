/* Reproduce kernels that cannot report a process security label. Linux only. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

ssize_t read(int fd, void *buffer, size_t count) {
    int previous_errno = errno;
    char descriptor[64];
    char target[PATH_MAX];
    snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd);
    ssize_t length = syscall(SYS_readlinkat, AT_FDCWD, descriptor, target,
                             sizeof(target) - 1);
    if (length >= 0) {
        target[length] = '\0';
        const char *suffix = "/attr/current";
        size_t suffix_length = strlen(suffix);
        if (strncmp(target, "/proc/", 6) == 0 &&
            (size_t)length >= suffix_length &&
            strcmp(target + length - suffix_length, suffix) == 0) {
            errno = EINVAL;
            return -1;
        }
    }
    errno = previous_errno;
    return syscall(SYS_read, fd, buffer, count);
}
