#include "replication.grpc.pb.h"
#include <grpcpp/grpcpp.h>

#include <iostream>
#include <string>
#include <vector>

using replication::MembershipRequest;
using replication::MembershipResponse;
using replication::MetadataService;

namespace {

void PrintMembership(const MembershipResponse& response) {
    std::cout << "Epoch: " << response.epoch() << std::endl;
    std::cout << "Mode: " << response.mode() << std::endl;
    std::cout << "Members:" << std::endl;
    for (const auto& member : response.membership()) {
        std::cout << "  " << member.node_id() << " @ " << member.address() << ":"
                  << member.port() << std::endl;
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string metadata_addr = "127.0.0.1:50050";
    bool get_membership = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--metadata" && i + 1 < argc) {
            metadata_addr = argv[++i];
        } else if (arg == "--get-membership") {
            get_membership = true;
        }
    }

    if (!get_membership) {
        std::cout << "Usage: client --get-membership [--metadata host:port]" << std::endl;
        return 0;
    }

    auto channel = grpc::CreateChannel(metadata_addr, grpc::InsecureChannelCredentials());
    auto stub = MetadataService::NewStub(channel);

    MembershipRequest request;
    MembershipResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub->GetMembership(&context, request, &response);
    if (!status.ok()) {
        std::cerr << "GetMembership failed: " << status.error_message() << std::endl;
        return 1;
    }

    PrintMembership(response);
    return 0;
}
