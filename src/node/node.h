#include "replication.pb.h"
#include "replication.grpc.pb.h"
#include <memory>
#include <string>

class Node {
private:
    int node_id = 0;
public:
    int ring_size = 0;
    std::shared_ptr<replication::ReplicationService::Stub> prev;
    std::shared_ptr<replication::ReplicationService::Stub> next;
public:
};
