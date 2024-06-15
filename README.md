# RaftKeeper Benchmark

It is a RaftKeeper or Zookeeper benchmark tool.

1. Install requirements (the following shows how to install in Ubuntu)
```
sudo apt-get update && sudo apt-get install openjdk-8-jdk maven
```

2. Build the benchmark tool

```
cd raftkeeper-bench && mvn package
```

3. Run benchmark test

```
cd target && unzip raftkeeper-bench-1.0-bin.zip && cd raftkeeper-bench-1.0-bin && \ 
    bin/run.sh $nodes $parallel $payload_size $run_duration $mode

For example: 
./run.sh "localhost:2181" 10 100 60 mix

Arguments:
nodes: RaftKeeper or ZooKeeper connection string
parallel: execution parallel
payload_size: znode value size
run_duration: run duration in second
mode: execution mode only 'create' or 'mix'
```
