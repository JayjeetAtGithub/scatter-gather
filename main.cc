#include <iostream>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>

#include <arrow/api.h>

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);


std::shared_ptr<arrow::Table> CreateTable() {
  auto schema =
      arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
                     arrow::field("c", arrow::int64())});
  std::shared_ptr<arrow::Array> array_a;
  std::shared_ptr<arrow::Array> array_b;
  std::shared_ptr<arrow::Array> array_c;
  arrow::NumericBuilder<arrow::Int64Type> builder;
  ABORT_ON_FAILURE(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
  ABORT_ON_FAILURE(builder.Finish(&array_a));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
  ABORT_ON_FAILURE(builder.Finish(&array_b));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
  ABORT_ON_FAILURE(builder.Finish(&array_c));
  return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

int SendBuffers(std::shared_ptr<arrow::Table> table, int fd) {
    int64_t num_cols = table->num_columns();
    for (int64_t i = 0; i < num_cols; i++) {
        /// Ensure that the table has a single record batch
        auto data = table->column(i)->chunk(0)->data();
        for (int j = 0; j < data->buffers.size(); j++) {
            if (data->buffers[j] != NULL) {
                std::cout << data->buffers[j]->size() << "\n"; 
                int e = send(fd, data->buffers[j]->mutable_data(), data->buffers[j]->size(), 0);
                if (e == -1) {
                    perror("error: failed to send buffer");
                    return 1;
                }
            }
        }
    }
    return 0;
}

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

    auto table = CreateTable();
    std::cout << table->ToString() << "\n";

    int e = SendBuffers(table, fd);
    if (e == -1) {
        perror("error: failed to send table\n");
    } else {
        printf("info: sent table\n");
    }

    if (close(fd)) {
        perror("info: close");
        return 1;
    }
}
