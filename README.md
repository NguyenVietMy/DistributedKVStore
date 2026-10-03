# DistributedKVStore

This project is a small three-node Raft key-value store in progress. Each node
persists its Raft term, vote, and log. The Linux `dkv_node` program runs the
election and heartbeat timers and exchanges Raft messages over local TCP.

Build and test:

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Run three nodes in separate terminals, using distinct data directories:

```sh
./build/dkv_node 1 /tmp/dkv-1 9101 9102 9103
./build/dkv_node 2 /tmp/dkv-2 9101 9102 9103
./build/dkv_node 3 /tmp/dkv-3 9101 9102 9103
```

The ports are ordered by node ID. On each terminal, `status` shows the current
term, role, and log progress. On the leader, `put x 10` proposes a write and
prints its log index. An `applied index N` line means that node learned the
entry was committed and applied it. `delete x` proposes a deletion. `inspect x`
shows only that node's local applied state; it is a debugging command, not a
linearizable client read. Use `quit` to stop a node.

The current TCP transport is localhost-only and uses one short-lived connection
per message. It has no client network protocol yet. Writes entered through the
console are proposals, not client acknowledgments; a client API will need to
wait for the proposed index to commit before reporting success.
