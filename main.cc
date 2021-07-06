#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/uio.h>

int main() {
    struct iovec iov[3];
    ssize_t nr;
    int fd, i;

    char *buf[] = {
        "Hello world!",
        "This is a test",
        "Of the BSD syscall"
    };

    fd = open("buccaneer.txt", O_WRONLY);
    if (fd == -1) {
        perror("open failed");
        return 1;
    }

    for (i = 0; i < 3; i++) {
        iov[i].iov_base = buf[i];
        iov[i].iov_len = strlen(buf[i]);
    }

    nr = writev (fd, iov, 3);
    if (nr == -1) {
        perror ("writev failed");
        return 1;
    }

    printf ("wrote %ld bytes\n", nr);

    if (close(fd)) {
        perror ("close");
        return 1;
    }
}
