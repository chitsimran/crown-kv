# crown-kv
CROWN: Chain Replication with Orchestrated Write Nodes

## How to run

Generate proto files with:
```
protoc \
  -I proto \
  --cpp_out=src/proto \
  --grpc_out=src/proto \
  --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) \
  proto/replication.proto
```

```
cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/homebrew -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build
```

```
ln -s build/compile_commands.json compile_commands.json
```
