#!/bin/bash

# Run benchmark testing
#
# Usage:
#   ./run.sh $nodes $parallel $payload_size $run_duration $mode
#
# For example:
#   ./run.sh "localhost:2181" 10 100 60 mix
#
# Arguments:
#   nodes: RaftKeeper or ZooKeeper connection string
#   parallel: execution parallel
#   payload_size: znode value size
#   run_duration: run duration in second
#   mode: execution mode only 'create' or 'mix'

nodes=$1
parallel=$2
payload_size=$3
run_duration=$4
mode=$5

binDir=`dirname $0`
libDir=$binDir/../lib
confDir=$binDir/../conf

jars=(`ls $libDir`)

classpath='.'
for j in ${jars[*]}
do
    classpath=$classpath:$libDir/$j
done
classpath=$confDir:$classpath

java -cp $classpath raftkeeper.Benchmark -c $nodes -p $parallel -s $payload_size -t $run_duration -m $mode
