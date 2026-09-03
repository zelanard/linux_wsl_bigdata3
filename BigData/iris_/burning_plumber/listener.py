#!/usr/bin/env python3

"""Watch HDFS input and coordinate transform/load lifecycle commands."""

import fcntl
import os
import time
from pathlib import Path
from urllib.parse import urlsplit

from .extract import INPUT_DIR, run_hdfs
from .hive import HIVE_JDBC_URL, HIVE_WAREHOUSE_DIR, load_hive
from .load import OUTPUT_DIR, load
from .transform import transform


STATE_DIR = Path.home() / ".local" / "state" / "burning_plumber"
LOCK_FILE = STATE_DIR / "listener.lock"
PID_FILE = STATE_DIR / "listener.pid"
STOP_FILE = STATE_DIR / "listener.stop"
FINGERPRINT_FILE = STATE_DIR / "last_fingerprint"


def _active_listener_pid():
    """Return the listener PID when it still exists."""

    try:
        pid = int(PID_FILE.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        return pid
    except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
        return None


def _hdfs_fingerprint(input_path):
    """Return the HDFS checksum output, or None when input is absent."""

    result = run_hdfs(
        "-checksum",
        input_path,
        check=False,
        capture_output=True,
    )
    if result.returncode == 0:
        return result.stdout.strip()
    if "No such file or directory" in result.stderr:
        return None
    raise RuntimeError(result.stderr.strip() or "Kunne ikke læse HDFS-checksum")


def _read_last_fingerprint():
    try:
        return FINGERPRINT_FILE.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def _write_last_fingerprint(fingerprint):
    temporary_file = STATE_DIR / f"fingerprint-{os.getpid()}.tmp"
    temporary_file.write_text(fingerprint, encoding="utf-8")
    temporary_file.replace(FINGERPRINT_FILE)


def listen(
    spark,
    input_path,
    source_url,
    output_dir=OUTPUT_DIR,
    poll_interval=2.0,
    file_format=None,
    read_options=None,
    write_options=None,
    column_names=None,
    filter_column=None,
    filter_value=None,
    hive_database=None,
    hive_table=None,
    hive_warehouse_dir=HIVE_WAREHOUSE_DIR,
    hive_jdbc_url=HIVE_JDBC_URL,
):
    """Transform and load whenever the HDFS input content changes.

    ``spark`` may be either an existing SparkSession or a zero-argument
    function that creates one. A factory lets the listener release all Spark
    resources between changes.
    """

    if poll_interval <= 0:
        raise ValueError("Poll-intervallet skal være større end nul")

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with LOCK_FILE.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("En listener kører allerede") from error

        STOP_FILE.unlink(missing_ok=True)
        PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
        last_fingerprint = _read_last_fingerprint()
        print(f"Lytter efter ændringer: {input_path}", flush=True)

        try:
            while not STOP_FILE.exists():
                fingerprint = _hdfs_fingerprint(input_path)
                if fingerprint is not None and fingerprint != last_fingerprint:
                    print("Ændring fundet; transformerer og gemmer data", flush=True)
                    creates_spark = callable(spark)
                    active_spark = spark() if creates_spark else spark

                    try:
                        transformed_data = transform(
                            active_spark,
                            input_path,
                            file_format=file_format,
                            read_options=read_options,
                            column_names=column_names,
                            filter_column=filter_column,
                            filter_value=filter_value,
                        )
                        output_path = load(
                            transformed_data,
                            source_url,
                            output_dir=output_dir,
                            file_format=file_format,
                            write_options=write_options,
                        )
                        if hive_database is not None or hive_table is not None:
                            if not hive_database or not hive_table:
                                raise ValueError(
                                    "Både Hive-database og Hive-tabel skal angives"
                                )
                            load_hive(
                                transformed_data,
                                hive_database,
                                hive_table,
                                warehouse_dir=hive_warehouse_dir,
                                jdbc_url=hive_jdbc_url,
                                mode="overwrite",
                            )
                    finally:
                        if creates_spark:
                            active_spark.stop()

                    _write_last_fingerprint(fingerprint)
                    last_fingerprint = fingerprint
                    print(f"Ændringen er behandlet: {output_path}", flush=True)

                time.sleep(poll_interval)
        except KeyboardInterrupt:
            print("Listeneren blev stoppet fra terminalen", flush=True)
        finally:
            PID_FILE.unlink(missing_ok=True)
            STOP_FILE.unlink(missing_ok=True)
            fcntl.flock(lock_handle, fcntl.LOCK_UN)


def stop(wait_timeout=30.0):
    """Ask the active listener to stop and wait for a clean shutdown."""

    pid = _active_listener_pid()
    if pid is None:
        PID_FILE.unlink(missing_ok=True)
        STOP_FILE.unlink(missing_ok=True)
        print("Ingen aktiv listener")
        return False

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STOP_FILE.touch()
    deadline = time.monotonic() + wait_timeout
    while _active_listener_pid() is not None and time.monotonic() < deadline:
        time.sleep(0.2)

    if _active_listener_pid() is not None:
        raise TimeoutError(
            f"Listener-processen {pid} stoppede ikke inden for {wait_timeout} sekunder"
        )

    print("Listeneren er stoppet")
    return True


def _validate_reset_path(path):
    """Reject broad HDFS paths before recursive deletion."""

    parts = [part for part in urlsplit(path).path.split("/") if part]
    if len(parts) < 3 or parts[0] != "user":
        raise ValueError(f"Reset-stien er for bred eller ugyldig: {path}")


def reset(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR):
    """Stop listening and remove pipeline data and listener state."""

    stop()
    for path in (input_dir, output_dir):
        _validate_reset_path(path)
        run_hdfs("-rm", "-r", "-f", path)

    for state_file in (FINGERPRINT_FILE, PID_FILE, STOP_FILE, LOCK_FILE):
        state_file.unlink(missing_ok=True)
    try:
        STATE_DIR.rmdir()
    except OSError:
        pass

    print("Pipeline-data og listener-status er nulstillet")
