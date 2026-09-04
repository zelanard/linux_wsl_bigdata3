#!/usr/bin/env bash
set -euo pipefail

if [[ "${BIGDATA_START_SESSION:-}" != "detached" ]]; then
    exec setsid --wait env BIGDATA_START_SESSION=detached bash "$0" "$@"
fi

export HADOOP_HOME=/home/zelanard/BigData/hadoop-3.5.0
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME=/home/zelanard/BigData/spark-4.2.0-bin-hadoop3
export HIVE_HOME=/home/zelanard/BigData/apache-hive-4.2.1-bin
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

BIGDATA_STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/bigdata"
HIVE_PID_FILE="$BIGDATA_STATE_DIR/hiveserver2.pid"
HIVE_LOG_FILE="$BIGDATA_STATE_DIR/hiveserver2.log"
HIVE_PORT=10000
HADOOP_SERVICES=(
    org.apache.hadoop.hdfs.server.namenode.NameNode
    org.apache.hadoop.hdfs.server.datanode.DataNode
    org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode
    org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
    org.apache.hadoop.yarn.server.nodemanager.NodeManager
)
SPARK_SERVICES=(
    org.apache.spark.deploy.master.Master
    org.apache.spark.deploy.worker.Worker
)

java_service_is_running() {
    local service="$1"
    local pid=""
    local class_name=""

    while read -r pid class_name; do
        if [[ "$class_name" == "$service" ]]; then
            return 0
        fi
    done < <(jps -l)
    return 1
}

all_java_services_are_running() {
    local service=""
    for service in "$@"; do
        if ! java_service_is_running "$service"; then
            return 1
        fi
    done
}

wait_for_java_services() {
    local group="$1"
    shift
    local service=""

    for _ in {1..15}; do
        if all_java_services_are_running "$@"; then
            echo "$group services are ready"
            return 0
        fi
        sleep 1
    done

    echo "$group did not start all required services:" >&2
    for service in "$@"; do
        if ! java_service_is_running "$service"; then
            echo "  missing: $service" >&2
        fi
    done
    return 1
}

start_hadoop() {
    if all_java_services_are_running "${HADOOP_SERVICES[@]}"; then
        echo "Hadoop services are already running"
        return 0
    fi

    "$HADOOP_HOME/sbin/start-all.sh" || true
    wait_for_java_services "Hadoop" "${HADOOP_SERVICES[@]}"
}

start_spark() {
    if all_java_services_are_running "${SPARK_SERVICES[@]}"; then
        echo "Spark services are already running"
        return 0
    fi

    "$SPARK_HOME/sbin/start-all.sh" || true
    wait_for_java_services "Spark" "${SPARK_SERVICES[@]}"
}

hive_is_listening() {
    ss -ltnH "sport = :$HIVE_PORT" 2>/dev/null | grep -q .
}

hive_accepts_queries() {
    JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Dorg.jline.terminal.provider=dumb" \
        timeout 25 "$HIVE_HOME/bin/beeline" \
        -u "jdbc:hive2://localhost:$HIVE_PORT/default" \
        -n "$(id -un)" \
        --silent=true \
        --showHeader=false \
        --outputformat=tsv2 \
        -e 'SELECT 1;' >/dev/null 2>&1
}

hive_process_is_running() {
    local pid="${1:-}"
    [[ "$pid" =~ ^[0-9]+$ ]] \
        && kill -0 "$pid" 2>/dev/null \
        && ps -p "$pid" -o args= | grep -Eq 'HiveServer2|hiveserver2'
}

start_hiveserver2() {
    local hive_pid=""

    mkdir -p -- "$BIGDATA_STATE_DIR"

    if hive_is_listening && hive_accepts_queries; then
        echo "HiveServer2 is already ready on port $HIVE_PORT"
        return 0
    fi

    if [[ -r "$HIVE_PID_FILE" ]]; then
        read -r hive_pid < "$HIVE_PID_FILE" || true
    fi

    if ! hive_process_is_running "$hive_pid"; then
        hive_pid="$(pgrep -f 'org\.apache\.hive\.service\.server\.HiveServer2' | head -n 1 || true)"
    fi

    if hive_process_is_running "$hive_pid"; then
        printf '%s\n' "$hive_pid" > "$HIVE_PID_FILE"
        echo "HiveServer2 is starting (PID $hive_pid)"
    else
        rm -f -- "$HIVE_PID_FILE"
        (
            cd "$HIVE_HOME"
            nohup "$HIVE_HOME/bin/hiveserver2" \
                --hiveconf hive.server2.enable.doAs=false \
                --hiveconf hive.server2.tez.initialize.default.sessions=false \
                --hiveconf hive.execution.engine=mr \
                > "$HIVE_LOG_FILE" 2>&1 &
            printf '%s\n' "$!" > "$HIVE_PID_FILE"
        )
        read -r hive_pid < "$HIVE_PID_FILE"
        echo "Starting HiveServer2 in the background (PID $hive_pid)"
        echo "HiveServer2 log: $HIVE_LOG_FILE"
    fi

    for _ in {1..120}; do
        if hive_is_listening; then
            break
        fi
        if ! hive_process_is_running "$hive_pid"; then
            echo "HiveServer2 stopped before opening port $HIVE_PORT" >&2
            tail -n 30 "$HIVE_LOG_FILE" >&2 || true
            return 1
        fi
        sleep 1
    done

    if ! hive_is_listening; then
        echo "HiveServer2 did not open port $HIVE_PORT before the readiness timeout" >&2
        tail -n 30 "$HIVE_LOG_FILE" >&2 || true
        return 1
    fi

    for _ in {1..15}; do
        if hive_accepts_queries; then
            echo "HiveServer2 is ready on port $HIVE_PORT"
            return 0
        fi
        if ! hive_process_is_running "$hive_pid"; then
            echo "HiveServer2 stopped before it became ready" >&2
            tail -n 30 "$HIVE_LOG_FILE" >&2 || true
            return 1
        fi
        sleep 2
    done

    echo "HiveServer2 did not accept queries before the readiness timeout" >&2
    tail -n 30 "$HIVE_LOG_FILE" >&2 || true
    return 1
}

start_hadoop
start_spark
start_hiveserver2

jps
