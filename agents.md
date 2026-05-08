For a write which goes to the head node for a given key:
1. if version in PutRequest == 0, it means the request is from a client. If not, it means the head node assigned it some version and it is a server to server request.
2. Do a check, if the request is for head node (version == 0) check if the node is a head for that key, if not reject it. If version != 0, it means the request is from a server so no need to do this check. 
3. At head node, get the latest version for that key, do version++ and apply the write locally. 
4. Pass on the request to next node (call Put rpc). 
5. Add to a pending list and wait for an ACK.
6. If ACK not received, retry (this all should be on a separate thread). 

For a write which goes to non-head node:
1. Apply the put request locally and pass on the version to kv_store.put which you got from the Put request.
2. check if not tail, forward the request (similar to head), else send ACK to the client and to the prev node.

For READS:
1. in CHAIN - If not tail, reject else send latest version.
2. for CRAQ and CROWN, if there's a dirty version, check latest version with the tail. Return based on the following:
    - send version request to TAIL. 
    - create method handle_version_request in craq and crown implementation
    - if you receive version X from tail in version query response, check if it exists in dirty store (O(1) check). If not check the latest version in clean store, if >= X return that to the client, else return an error

metadata_store does the following:
1. store membership of nodes
2. store current mode
3. store key mappings
4. send heartbeats to nodes in the ring
5. in case of failures, send reconfiguration to all nodes
6. reconfiguration protocol is defined below:

For reconfiguration (and failure handling):
1. For CROWN:
    - metadata_store multicasts FREEZE message to all nodes
    - when a node receives a multicast message, it will stop accepting any incoming client write requests
    - any inflight writes (server to server) still continue
    - reads are still served
    - each node starts its own Inflight message which must go through the entire ring
    - when a node receives an Inflight message, it sends out all pending writes to next node and then propagates the Inflight message
    - when a node receives its own Inflight message back, it know all of its pending writes have been completed so it sends an ACK back to the metadata_store
    - when the metadata_store receives ACK from all of the nodes in the ring, it knows all of the writes have stabilised so now we can safely add the new node
    - the new node fetches the data from any of the nodes and lets the metadata_store know
    - metadata_store now sends new configuration and unfreeze message (in the same request)
2. For chain and craq:
    - sends FREEZE message to head (it stops incoming write requests)
    - also send new tail to the current tail, so it can propagate any incoming writes to the new tail
    - reads are still served
    - new node fetches data from the tail
    - once it has all the data, it lets metadata_store know
    - metadata_store sends reconfiguration including the new tail
