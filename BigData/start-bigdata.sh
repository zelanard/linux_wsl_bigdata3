#!/usr/bin/env bash
set -e

export HADOOP_HOME=/home/zelanard/BigData/hadoop-3.5.0
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME=/home/zelanard/BigData/spark-4.2.0-bin-hadoop3

"$HADOOP_HOME/sbin/start-all.sh"
"$SPARK_HOME/sbin/start-all.sh"

jps