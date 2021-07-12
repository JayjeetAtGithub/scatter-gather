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
        // need to recieve shape metadata here
        int64_t num_cols = 3;
        int64_t num_rows = 10;
        std::vector<int> total_bytes{80, 80, 80};
        std::vector<int> null_count{0, 0, 0};
        std::vector<int> length{10, 10, 10};

        std::vector<char*> column_buffers(num_cols);
        for (int64_t i = 0; i < num_cols; i++) {
            column_buffers[i] = new char[total_bytes[i]];
        }

        client_size = sizeof(client_addr);
        client_sock = accept(socket_desc, (struct sockaddr*)&client_addr, &client_size);
        
        if (client_sock < 0){
            printf("error: can't accept\n");
            return -1;
        }
        printf("info: client connected at IP: %s and port: %i\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        for (int64_t i = 0; i < num_cols; i++) {
            int e = recv(client_sock, column_buffers[i], total_bytes[i], 0);
            if (e == -1) {
                perror("error: readv failed");
                return 1;
            }
        }

        std::vector<std::shared_ptr<arrow::ChunkedArray>> cols(num_cols);
        for (int i = 0; i < num_cols; i++) {
            std::vector<std::shared_ptr<arrow::Buffer>> buffers;
            buffers.push_back(nullptr);
            buffers.push_back(std::make_shared<arrow::Buffer>((uint8_t*)column_buffers[i], total_bytes[i]));
            auto data = arrow::ArrayData::Make(arrow::int64(), length[i], std::move(buffers), null_count[i]);
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
            num_rows
        );

        std::cout << table->ToString();
    }
    
    close(client_sock);
    close(socket_desc);
    return 0;
}
