#!/usr/bin/env python3

"""Print Spark or HDFS data as a readable CLI table."""

import argparse
import csv
import io
import json
import os
import subprocess
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit


def Print(data):
    """Print every row without truncating column values."""

    row_count = data.count()
    print(f"Data ({row_count} rows):")
    data.show(n=row_count, truncate=False)


def _cell_text(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _show_table(headers, rows):
    """Render text rows without requiring a Spark application."""

    text_headers = [_cell_text(value) for value in headers]
    column_count = len(text_headers)
    text_rows = []
    for row in rows:
        text_row = [_cell_text(value) for value in row[:column_count]]
        text_row.extend("" for _ in range(column_count - len(text_row)))
        text_rows.append(text_row)
    widths = [
        max([
            len(text_headers[index]),
            *(len(row[index]) for row in text_rows),
        ])
        for index in range(len(text_headers))
    ]
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def print_row(row):
        cells = (
            value.ljust(width)
            for value, width in zip(row, widths)
        )
        print("| " + " | ".join(cells) + " |")

    print(border)
    print_row(text_headers)
    print(border)
    for row in text_rows:
        print_row(row)
    print(border)


def _read_hdfs(input_path):
    hadoop_home = Path(
        os.environ.get(
            "HADOOP_HOME",
            "/home/zelanard/BigData/hadoop-3.5.0",
        )
    )
    environment = os.environ.copy()
    environment["HADOOP_HOME"] = str(hadoop_home)
    environment.setdefault(
        "HADOOP_CONF_DIR",
        str(hadoop_home / "etc" / "hadoop"),
    )
    result = subprocess.run(
        [str(hadoop_home / "bin" / "hdfs"), "dfs", "-cat", input_path],
        shell=False,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
    )
    return result.stdout


def print_hdfs(input_path):
    """Read a completed CSV/JSON file from HDFS and print it quickly."""

    contents = _read_hdfs(input_path)
    extension = PurePosixPath(urlsplit(input_path).path).suffix.lower()

    if extension == ".csv":
        records = list(csv.reader(io.StringIO(contents, newline="")))
        if not records:
            raise ValueError("CSV-filen er tom")
        headers = records[0]
        rows = records[1:]
    elif extension == ".json":
        objects = [
            json.loads(line)
            for line in contents.splitlines()
            if line.strip()
        ]
        if not objects:
            raise ValueError("JSON-filen er tom")
        if any(not isinstance(value, dict) for value in objects):
            objects = [
                value if isinstance(value, dict) else {"value": value}
                for value in objects
            ]
        headers = list(
            dict.fromkeys(
                key
                for value in objects
                for key in value
            )
        )
        rows = [
            [value.get(header) for header in headers]
            for value in objects
        ]
    else:
        raise ValueError("Kun CSV- og JSON-filer kan vises")

    print(f"Data ({len(rows)} rows):")
    _show_table(headers, rows)


def main():
    parser = argparse.ArgumentParser(description="Print an HDFS CSV/JSON table")
    parser.add_argument("input_path")
    arguments = parser.parse_args()

    try:
        print_hdfs(arguments.input_path)
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        message = getattr(error, "stderr", None) or str(error)
        parser.exit(1, f"Kunne ikke vise HDFS-filen: {message.strip()}\n")


if __name__ == "__main__":
    main()
