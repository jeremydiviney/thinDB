#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>

/* Diagnostic interposer for the isolated service; never preload in production. */
int accept4(int listener, struct sockaddr *address, socklen_t *length, int flags) {
    int (*real_accept4)(int, struct sockaddr *, socklen_t *, int) = dlsym(RTLD_NEXT, "accept4");
    int fd = real_accept4(listener, address, length, flags);
    if (fd < 0) return fd;
    int saved_errno = errno;
    struct sockaddr_in local;
    socklen_t size = sizeof(local);
    if (getsockname(fd, (struct sockaddr *)&local, &size) == 0 &&
        local.sin_family == AF_INET && ntohs(local.sin_port) == 13311) {
        int before = -1, after = -1;
        socklen_t option_size = sizeof(before);
        getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &before, &option_size);
        const char *setting = getenv("DIAG_TCP_NODELAY");
        if (setting && (setting[0] == '0' || setting[0] == '1') && !setting[1]) {
            int value = setting[0] == '1';
            int rc = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
            option_size = sizeof(after);
            getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &after, &option_size);
            fprintf(stderr, "[diagnostic-socket] port=13311 fd=%d nodelay_before=%d requested=%d after=%d rc=%d\n",
                    fd, before, value, after, rc);
        }
    }
    errno = saved_errno;
    return fd;
}
