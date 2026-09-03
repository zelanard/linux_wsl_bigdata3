#!/usr/bin/env python3

"""Store transformed Spark data in HDFS and register it through Beeline."""

from __future__ import annotations

import getpass
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import urlsplit


HDFS_URI = "hdfs://localhost:9000"
HIVE_WAREHOUSE_DIR = f"{HDFS_URI}/user/hive/warehouse"
HIVE_JDBC_URL = "jdbc:hive2://localhost:10000/default"
HADOOP_HOME = Path(
    os.environ.get("HADOOP_HOME", "/home/zelanard/BigData/hadoop-3.5.0")
)
HIVE_HOME = Path(
    os.environ.get(
        "HIVE_HOME",
        "/home/zelanard/BigData/apache-hive-4.2.1-bin",
    )
)
BEELINE = HIVE_HOME / "bin" / "beeline"

_SIMPLE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HIVE_DATA_TYPE = re.compile(
    r"^(?:tinyint|smallint|int|bigint|float|double|boolean|string|binary|date|"
    r"timestamp|decimal\(\d+,\d+\)|char\(\d+\)|varchar\(\d+\))$",
    flags=re.IGNORECASE,
)


class HiveLoadError(RuntimeError):
    """Raised when Hive metadata or the Parquet write is invalid."""


def _simple_identifier(value, label):
    if not isinstance(value, str) or not _SIMPLE_IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"{label} skal starte med et bogstav eller underscore og kun "
            "indeholde bogstaver, tal og underscores"
        )
    return value


def _quoted_identifier(value):
    """Quote a column heading while preserving the CSV/DataFrame name."""

    if (
        not isinstance(value, str)
        or not value.strip()
        or "`" in value
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"Ugyldigt Hive-kolonnenavn: {value!r}")
    return f"`{value}`"


def _validate_unique_columns(columns):
    normalized = [column.casefold() for column in columns]
    if len(normalized) != len(set(normalized)):
        raise ValueError("Hive-kolonnenavne skal være unikke, også uden forskel på store og små bogstaver")


def _hive_type(data_type):
    type_name = data_type.simpleString().lower()
    if type_name == "timestamp_ntz":
        type_name = "timestamp"
    if not _HIVE_DATA_TYPE.fullmatch(type_name):
        raise HiveLoadError(
            f"Spark-typen {type_name!r} kan ikke oprettes automatisk i Hive"
        )
    return type_name


def _validate_hdfs_uri(value, label="Hive warehouse"):
    parsed = urlsplit(value)
    if (
        parsed.scheme != "hdfs"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.query
        or parsed.fragment
        or any(character in value for character in "'\";\r\n\x00")
    ):
        raise ValueError(f"{label} skal være en sikker, absolut HDFS-URI")
    return value.rstrip("/")


def _beeline_environment():
    environment = os.environ.copy()
    environment["HADOOP_HOME"] = str(HADOOP_HOME)
    environment.setdefault(
        "HADOOP_CONF_DIR",
        str(HADOOP_HOME / "etc" / "hadoop"),
    )
    environment["HIVE_HOME"] = str(HIVE_HOME)
    # Hive 4.2.1 uses JLine 4. Its native terminal providers require a TTY,
    # while this loader deliberately runs Beeline as a non-interactive child
    # process. The pure-Java dumb provider keeps -e queries deterministic.
    jline_option = "-Dorg.jline.terminal.provider=dumb"
    java_options = environment.get("JAVA_TOOL_OPTIONS", "")
    if jline_option not in java_options.split():
        environment["JAVA_TOOL_OPTIONS"] = (
            f"{java_options} {jline_option}".strip()
        )
    return environment


def run_beeline(
    sql,
    *,
    jdbc_url=HIVE_JDBC_URL,
    username=None,
    capture_output=False,
):
    """Execute Hive SQL with Hive's built-in Beeline client."""

    if not BEELINE.is_file():
        raise HiveLoadError(f"Beeline blev ikke fundet: {BEELINE}")
    if not jdbc_url.startswith("jdbc:hive2://"):
        raise ValueError("Hive-forbindelsen skal være en jdbc:hive2-URL")

    command = [
        str(BEELINE),
        "-u",
        jdbc_url,
        "-n",
        username or os.environ.get("HIVE_USER") or getpass.getuser(),
        "--silent=true",
        "--showHeader=false",
        "--outputformat=tsv2",
        "-e",
        sql,
    ]
    try:
        return subprocess.run(
            command,
            shell=False,
            check=True,
            capture_output=capture_output,
            text=True,
            encoding="utf-8",
            env=_beeline_environment(),
        )
    except subprocess.CalledProcessError as error:
        details = (error.stderr or error.stdout or str(error)).strip()
        raise HiveLoadError(f"Beeline fejlede: {details}") from error


def _parse_describe(output):
    fields = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        name = parts[0].strip()
        data_type = parts[1].strip().lower()
        if not name or name.startswith("#"):
            break
        if _HIVE_DATA_TYPE.fullmatch(data_type):
            fields.append((name.casefold(), data_type))
    return tuple(fields)


def _parse_location(output):
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0].strip().rstrip(":").casefold() == "location":
            return parts[1].strip().rstrip("/")
    return None


def load_hive(
    transformed_data,
    database,
    table,
    *,
    warehouse_dir=HIVE_WAREHOUSE_DIR,
    jdbc_url=HIVE_JDBC_URL,
    mode="overwrite",
    beeline_runner=None,
):
    """Store a Spark DataFrame as Parquet and register it in Hive.

    The database and table are created automatically through Beeline. The
    Parquet files live in the configured HDFS warehouse, so HiveServer2 and
    Spark do not need to share Spark's embedded Hive metastore.
    """

    database = _simple_identifier(database, "Databasenavnet")
    table = _simple_identifier(table, "Tabelnavnet")
    warehouse_dir = _validate_hdfs_uri(warehouse_dir)
    if mode not in {"append", "overwrite"}:
        raise ValueError("Hive write mode skal være append eller overwrite")

    spark = transformed_data.sparkSession
    if spark.sparkContext.master.startswith("local"):
        raise RuntimeError("Brug Spark-clusteren, ikke local mode")

    columns = list(transformed_data.schema.fields)
    if not columns:
        raise ValueError("DataFrame har ingen kolonner")
    _validate_unique_columns([field.name for field in columns])

    incoming_schema = tuple(
        (field.name.casefold(), _hive_type(field.dataType))
        for field in columns
    )
    fields_sql = ", ".join(
        f"{_quoted_identifier(field.name)} {_hive_type(field.dataType)}"
        for field in columns
    )

    database_location = f"{warehouse_dir}/{database}.db"
    table_location = f"{database_location}/{table}"
    qualified_table = f"`{database}`.`{table}`"
    create_sql = f"""
CREATE DATABASE IF NOT EXISTS `{database}`
LOCATION '{database_location}';
CREATE EXTERNAL TABLE IF NOT EXISTS {qualified_table} ({fields_sql})
STORED AS PARQUET
LOCATION '{table_location}';
""".strip()

    runner = beeline_runner or run_beeline
    runner(create_sql, jdbc_url=jdbc_url)

    describe = runner(
        f"DESCRIBE {qualified_table};",
        jdbc_url=jdbc_url,
        capture_output=True,
    )
    existing_schema = _parse_describe(describe.stdout)
    if existing_schema != incoming_schema:
        raise HiveLoadError(
            f"DataFrame-schemaet {incoming_schema} matcher ikke Hive-tabellen "
            f"{database}.{table} {existing_schema}; ingen data blev skrevet"
        )

    formatted = runner(
        f"DESCRIBE FORMATTED {qualified_table};",
        jdbc_url=jdbc_url,
        capture_output=True,
    )
    existing_location = _parse_location(formatted.stdout)
    if existing_location != table_location:
        raise HiveLoadError(
            f"Hive-tabellen bruger {existing_location!r}, men Burning Plumber "
            f"forventer {table_location!r}; ingen data blev skrevet"
        )

    (
        transformed_data.write
        .mode(mode)
        .format("parquet")
        .save(table_location)
    )
    print(f"Hive-tabel gemt: {database}.{table} ({table_location})")
    return table_location


def show_hive(
    database,
    table,
    *,
    jdbc_url=HIVE_JDBC_URL,
    limit=20,
    beeline_runner=None,
):
    """Show databases, schema, and table rows through Beeline."""

    database = _simple_identifier(database, "Databasenavnet")
    table = _simple_identifier(table, "Tabelnavnet")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ValueError("Beeline limit skal være et positivt heltal")

    sql = f"""
SHOW DATABASES;
USE `{database}`;
SHOW TABLES;
DESCRIBE `{table}`;
SELECT * FROM `{table}` LIMIT {limit};
""".strip()
    runner = beeline_runner or run_beeline
    return runner(sql, jdbc_url=jdbc_url)
