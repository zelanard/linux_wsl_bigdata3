#!/usr/bin/env python3

"""Save a DataFrame as one CSV or JSON file in HDFS."""

import uuid

from .formats import (
    file_format_from_location,
    filename_from_url,
    normalize_file_format,
)


OUTPUT_DIR = "hdfs://localhost:9000/user/zelanard/Output_dir"


def output_filename(source_url):
    """Prefix the source CSV or JSON filename with transformed_."""

    return f"transformed_{filename_from_url(source_url)}"


def load(
    transformed_data,
    source_url,
    output_dir=OUTPUT_DIR,
    file_format=None,
    write_options=None,
):
    """Overwrite a URL-named CSV or JSON file in HDFS Output_dir."""

    if not output_dir.startswith("hdfs://"):
        raise ValueError("Output_dir skal være en HDFS-URI")

    spark = transformed_data.sparkSession
    if spark.sparkContext.master.startswith("local"):
        raise RuntimeError("Brug Spark-clusteren, ikke local mode")

    if file_format is None:
        file_format = file_format_from_location(source_url)
    else:
        file_format = normalize_file_format(file_format)

    options = dict(write_options or {})
    if file_format == "csv":
        options.setdefault("header", True)

    output_dir = output_dir.rstrip("/")
    final_path = f"{output_dir}/{output_filename(source_url)}"
    temporary_dir = f"{output_dir}/._load-{uuid.uuid4().hex}"

    # Spark skriver output som en mappe med part-filer. Hadoop-API'et i
    # Spark-JVM'en omdøber part-filen direkte i HDFS uden brug af LFS.
    context = spark.sparkContext
    HadoopPath = context._jvm.org.apache.hadoop.fs.Path
    configuration = context._jsc.hadoopConfiguration()
    output_path = HadoopPath(output_dir)
    temporary_path = HadoopPath(temporary_dir)
    final_hadoop_path = HadoopPath(final_path)
    filesystem = output_path.getFileSystem(configuration)
    filesystem.mkdirs(output_path)

    try:
        (
            transformed_data
            .coalesce(1)
            .write
            .mode("overwrite")
            .format(file_format)
            .options(**options)
            .save(temporary_dir)
        )

        data_parts = [
            status.getPath()
            for status in filesystem.listStatus(temporary_path)
            if status.getPath().getName().startswith("part-")
            and status.getPath().getName().endswith(
                f".{file_format}"
            )
        ]
        if len(data_parts) != 1:
            raise RuntimeError(
                "Spark oprettede ikke præcis én outputfil"
            )

        # Kravet om overskrivning opfyldes ved at fjerne den gamle fil.
        filesystem.delete(final_hadoop_path, True)
        if not filesystem.rename(data_parts[0], final_hadoop_path):
            raise RuntimeError("Kunne ikke gemme filen i HDFS")

        print(f"{file_format.upper()}-fil gemt i HDFS: {final_path}")
        return final_path

    finally:
        # Fjern Sparks _SUCCESS-fil og den midlertidige HDFS-mappe.
        if filesystem.exists(temporary_path):
            filesystem.delete(temporary_path, True)
