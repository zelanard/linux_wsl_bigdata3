#!/usr/bin/env bash
set -euo pipefail

HADOOP_HOME=/home/zelanard/BigData/hadoop-3.5.0
SPARK_HOME=/home/zelanard/BigData/spark-4.2.0-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
BIGDATA_STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/bigdata"
HIVE_PID_FILE="$BIGDATA_STATE_DIR/hiveserver2.pid"

hive_process_is_running() {
    local pid="${1:-}"
    [[ "$pid" =~ ^[0-9]+$ ]] \
        && kill -0 "$pid" 2>/dev/null \
        && ps -p "$pid" -o args= | grep -Eq 'HiveServer2|hiveserver2'
}

stop_hiveserver2() {
    local pid=""
    local candidate=""
    local still_running=false
    local -a hive_pids=()

    if [[ -r "$HIVE_PID_FILE" ]]; then
        read -r candidate < "$HIVE_PID_FILE" || true
        if hive_process_is_running "$candidate"; then
            hive_pids+=("$candidate")
        fi
    fi

    while IFS= read -r candidate; do
        if hive_process_is_running "$candidate" \
            && [[ ! " ${hive_pids[*]} " =~ " $candidate " ]]; then
            hive_pids+=("$candidate")
        fi
    done < <(pgrep -f 'org\.apache\.hive\.service\.server\.HiveServer2' || true)

    if ((${#hive_pids[@]} == 0)); then
        echo "HiveServer2 is not running"
        rm -f -- "$HIVE_PID_FILE"
        return 0
    fi

    echo "Stopping HiveServer2 (PID ${hive_pids[*]})"
    kill "${hive_pids[@]}"

    for _ in {1..30}; do
        still_running=false
        for pid in "${hive_pids[@]}"; do
            if hive_process_is_running "$pid"; then
                still_running=true
                break
            fi
        done
        if [[ "$still_running" == false ]]; then
            rm -f -- "$HIVE_PID_FILE"
            echo "HiveServer2 stopped"
            return 0
        fi
        sleep 1
    done

    echo "HiveServer2 did not stop cleanly; forcing shutdown" >&2
    for pid in "${hive_pids[@]}"; do
        if hive_process_is_running "$pid"; then
            kill -KILL "$pid"
        fi
    done
    rm -f -- "$HIVE_PID_FILE"
}

stop_hiveserver2
"$SPARK_HOME/sbin/stop-all.sh"
"$HADOOP_HOME/sbin/stop-all.sh"

jps
