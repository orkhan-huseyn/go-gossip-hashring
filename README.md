# Simple Gossip Based Workload Sharing Example in Go

This is a simple one file Go application that demonstrates how to create a consistent hash ring and share it using gossip protocol via `hashicorp/memberlist` library.

## Running a 5-Node Local Cluster

Open 5 terminal windows and run each command sequentially to spin up a 5-node local cluster:

```bash
# Terminal 1 (Seed Node)
go run main.go -name=node1 -port=7946

# Terminals 2-5 (Join Seed Node)
go run main.go -name=node2 -port=7947 -join=127.0.0.1:7946
go run main.go -name=node3 -port=7948 -join=127.0.0.1:7946
go run main.go -name=node4 -port=7949 -join=127.0.0.1:7946
go run main.go -name=node5 -port=7950 -join=127.0.0.1:7946
```

Watch the logs after that. Each node should start simulating a prometheus scrape job like `grafana/alloy` and they should share the workload among them.
