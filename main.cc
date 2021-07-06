#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <arpa/inet.h>

int main(int argc, char **argv) {
    struct sockaddr_in server_addr;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0){
        printf("error: unable to create socket\n");
        return -1;
    }
    printf("info: socket created successfully\n");
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(2000);
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    
    if(connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0){
        printf("error: unable to connect\n");
        return -1;
    }
    printf("info: connected with server successfully\n");
    
    struct iovec iov[3];
    char *buf[] = {
        "Hello",
        "This",
        "Ofc"
    };

    for (int i = 0; i < 3; i++) {
        iov[i].iov_base = buf[i];
        iov[i].iov_len = strlen(buf[i]);
    }

    ssize_t nr = writev (fd, iov, 3);
    if (nr == -1) {
        perror("error: call to writev failed");
        return 1;
    }

    printf ("info: wrote %ld bytes\n", nr);

    if (close(fd)) {
        perror("info: close");
        return 1;
    }
}
