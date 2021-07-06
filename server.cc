#include <stdio.h>
#include <iostream>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <arrow/api.h>

int main(int argc, char** argv) {
    int socket_desc, client_sock;
    socklen_t client_size;
    struct sockaddr_in server_addr, client_addr;

    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    
    if(socket_desc < 0){
        printf("error: error while creating socket\n");
        return -1;
    }
    printf("info: socket created successfully\n");
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(2000);
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if(bind(socket_desc, (struct sockaddr*)&server_addr, sizeof(server_addr))<0){
        printf("error: couldn't bind to the port\n");
        return -1;
    }
    printf("info: done with binding\n");
    
    if(listen(socket_desc, 1) < 0){
        printf("error: error while listening\n");
        return -1;
    }
    printf("info: listening for incoming connections.....\n");
    
    while (1) {
        char *msg_a = new char[80];
        char *msg_b = new char[80];
        char *msg_c = new char[80];
        struct iovec iov[3];
        ssize_t nr;

        iov[0].iov_base = msg_a;
        iov[0].iov_len = 80;
        iov[1].iov_base = msg_b;
        iov[1].iov_len = 80;
        iov[2].iov_base = msg_c;
        iov[2].iov_len = 80;

        client_size = sizeof(client_addr);
        client_sock = accept(socket_desc, (struct sockaddr*)&client_addr, &client_size);
        
        if (client_sock < 0){
            printf("error: can't accept\n");
            return -1;
        }
        printf("info: client connected at IP: %s and port: %i\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        nr = readv(client_sock, iov, 3);
        if (nr == -1) {
            perror("error: readv failed");
            return 1;
        }

        std::vector<std::shared_ptr<arrow::ChunkedArray>> cols(3);

        for (int i = 0; i < 3; i++) {
            std::vector<std::shared_ptr<arrow::Buffer>> buffers;
            buffers.push_back(nullptr);
            buffers.push_back(std::make_shared<arrow::Buffer>((uint8_t*)iov[i].iov_base, 80));
            auto data = arrow::ArrayData::Make(arrow::int64(), 10, std::move(buffers), 0);
            auto col = std::make_shared<arrow::ChunkedArray>(arrow::MakeArray(data));
            cols[i] = col;
        }

        auto table = arrow::Table::Make(
            arrow::schema({
                arrow::field("a", arrow::int64()), 
                arrow::field("b", arrow::int64()),
                arrow::field("c", arrow::int64())
            }), 
            std::move(cols), 
            10
        );

        std::cout << table->ToString();
    }
    
    close(client_sock);
    close(socket_desc);
    return 0;
}
