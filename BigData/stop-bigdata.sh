#!/usr/bin/env bash
set -e

HADOOP_HOME=/home/zelanard/BigData/hadoop-3.5.0
SPARK_HOME=/home/zelanard/BigData/spark-4.2.0-bin-hadoop3

"$SPARK_HOME/sbin/stop-all.sh"
"$HADOOP_HOME/sbin/stop-all.sh"

jps