#include "replication_common.h"
#include "../replication.h"

#include <grpcpp/grpcpp.h>
#include <thread>

using replication::CommitAck;
using replication::CommitAckResponse;
using replication::PutResponse;
using replication::WriteAck;
using replication::WriteAckResponse;

void Replication::add_to_pending_acks(PutRequest request) {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_acks[request.request_id()] = request;
}

void Replication::forward_put(PutRequest request,
                              const std::shared_ptr<ReplicationService::Stub>& next_stub) {
    if (!next_stub) {
        return;
    }
    add_to_pending_acks(request);

    std::shared_ptr<ReplicationService::Stub> stub = next_stub;
    std::thread([stub, request]() mutable {
        grpc::ClientContext context;
        PutResponse response;
        stub->ForwardPut(&context, request, &response);
    }).detach();
}

void Replication::send_ack(int64_t request_id,
                           const std::shared_ptr<ReplicationService::Stub>& prev_stub) {
    PutRequest request;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_acks.find(request_id);
        if (it == pending_acks.end()) {
            return;
        }
        request = it->second;
    }

    if (prev_stub) {
        WriteAck ack;
        ack.set_request_id(request.request_id());
        ack.set_key(request.key());
        ack.set_version(request.version());
        ack.set_epoch(request.epoch());

        WriteAckResponse response;
        grpc::ClientContext context;
        prev_stub->SendWriteAck(&context, ack, &response);
        return;
    }

    std::string host;
    int port = 0;
    if (!replication_common::parse_host_port(request.client_addr(), &host, &port)) {
        return;
    }
    auto channel = grpc::CreateChannel(request.client_addr(),
                                       grpc::InsecureChannelCredentials());
    auto stub = replication::ClientAckService::NewStub(channel);

    CommitAck ack;
    ack.set_key(request.key());
    ack.set_version(request.version());
    CommitAckResponse response;
    grpc::ClientContext context;
    stub->CommitAckMsg(&context, ack, &response);
}
