#!/usr/bin/env python3

"""Download a CSV or JSON source directly to HDFS with curl."""

import csv
import io
import os
import subprocess
import uuid
from pathlib import Path

from .formats import file_format_from_location, filename_from_url


INPUT_DIR = "/user/zelanard/Input_dir"

HADOOP_HOME = Path(
    os.environ.get(
        "HADOOP_HOME",
        "/home/zelanard/BigData/hadoop-3.5.0",
    )
)
HDFS = str(HADOOP_HOME / "bin" / "hdfs")
HDFS_ENV = os.environ.copy()
HDFS_ENV["HADOOP_HOME"] = str(HADOOP_HOME)
HDFS_ENV.setdefault(
    "HADOOP_CONF_DIR",
    str(HADOOP_HOME / "etc" / "hadoop"),
)


def run_hdfs(*arguments, check=True, capture_output=False):
    # Sikkerhed mod command injection: Der bruges en argumentliste og
    # shell=False. URL og filnavn bliver derfor aldrig kørt som shell-kode.
    return subprocess.run(
        [HDFS, "dfs", *arguments],
        shell=False,
        check=check,
        capture_output=capture_output,
        text=True,
        env=HDFS_ENV,
    )


def stop_process(process):
    """Stop en underproces efter fejl eller Ctrl+C."""

    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _csv_header(column_names):
    """Build an encoded CSV header without creating a local file."""

    if not column_names or any(
        not isinstance(name, str) or not name.strip()
        for name in column_names
    ):
        raise ValueError("CSV-kolonnenavne skal være tekst og må ikke være tomme")

    header = io.StringIO(newline="")
    csv.writer(header, lineterminator="\n").writerow(column_names)
    return header.getvalue().encode("utf-8")


def extract(source_url, input_dir=INPUT_DIR, column_names=None):
    """Stream a CSV or JSON source directly into an HDFS file.

    When ``column_names`` is provided for a CSV source, its header is written
    directly to the HDFS upload stream before the downloaded data.
    """

    filename = filename_from_url(source_url)
    file_format = file_format_from_location(source_url)
    if column_names is not None and file_format != "csv":
        raise ValueError("Kolonnenavne kan kun tilføjes til CSV-filer")

    header = _csv_header(column_names) if column_names is not None else b""
    input_dir = input_dir.rstrip("/")
    destination = f"{input_dir}/{filename}"
    temporary = (
        f"{input_dir}/.{filename}.uploading-{uuid.uuid4().hex}"
    )

    run_hdfs("-mkdir", "-p", input_dir)

    upload = download = None

    try:
        # "-" betyder, at HDFS læser data fra stdin. Kildefilen bliver
        # derfor aldrig skrevet til det lokale filsystem.
        upload = subprocess.Popen(
            [HDFS, "dfs", "-put", "-", temporary],
            shell=False,
            stdin=subprocess.PIPE,
            env=HDFS_ENV,
        )

        # Kolonnenavnene skrives direkte til HDFS-pipen. Hverken headeren
        # eller den downloadede fil gemmes på det lokale filsystem.
        if header:
            upload.stdin.write(header)
            upload.stdin.flush()

        # Robusthed og integritet: curl fejler ved HTTP/netværksfejl,
        # bruger timeout og prøver igen. HTTPS validerer TLS-certifikatet.
        download = subprocess.Popen(
            [
                "/usr/bin/curl",
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                "--proto",
                "=https",
                "--proto-redir",
                "=https",
                "--retry",
                "5",
                "--retry-all-errors",
                "--connect-timeout",
                "15",
                "--max-time",
                "300",
                "--url",
                source_url,
            ],
            shell=False,
            stdout=upload.stdin,
        )

        # Luk kun forælderens kopi af pipen. curl beholder sin kopi,
        # indtil download er færdig.
        upload.stdin.close()
        curl_code = download.wait()
        hdfs_code = upload.wait()

        if curl_code != 0 or hdfs_code != 0:
            raise RuntimeError("Download eller HDFS-upload fejlede")

        # HDFS bruger egne blok-checksums. Størrelseskontrollen sikrer,
        # at datakilden ikke var tom.
        size = int(
            run_hdfs(
                "-stat",
                "%b",
                temporary,
                capture_output=True,
            ).stdout.strip()
        )
        if size <= len(header):
            raise RuntimeError("Datakilden returnerede ingen data")

        # En afbrudt download rammer kun den midlertidige HDFS-fil.
        run_hdfs("-rm", "-f", destination)
        run_hdfs("-mv", temporary, destination)
        print(f"Filen blev gemt i HDFS: {destination}")
        return destination

    except BaseException:
        if upload is not None and upload.stdin is not None:
            if not upload.stdin.closed:
                upload.stdin.close()

        stop_process(download)
        stop_process(upload)
        run_hdfs("-rm", "-f", temporary, check=False)
        run_hdfs(
            "-rm",
            "-f",
            f"{temporary}._COPYING_",
            check=False,
        )
        raise
