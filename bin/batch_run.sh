#!/bin/bash

# Run multiple benchmark testing and get report
#
# Usage:
#   ./batch_run.sh $nodes $mode
#
# For example:
#   ./batch_run.sh "localhost:2181" mix
#
# Get CSV report:
#  ./batch_run.sh $nodes $mode | grep -A2 "Benchmark result" | grep -v "Benchmark" | grep -v "parallel" | grep -v '-'

nodes=$1 # RaftKeeper or Zookeeper connection string
mode=$2  # Execution mode, only 'create' or 'mix'

workDir=`dirname $0`

sequence=(`seq 1 5`)
for i in ${sequence[*]}
do
        parallel=$(($i * 2))
        ./start.sh $parallel
        sleep 1
done

sequence=(`seq 1 15`)

for i in ${sequence[*]}
do
        parallel=$(($i * 20))
        ./start.sh $parallel
        sleep 1
done

sequence=(`seq 2 10`)
for i in ${sequence[*]}
do
        parallel=$(($i * 200))
        ./start.sh $parallel
        sleep 1
done

echo "done!"
