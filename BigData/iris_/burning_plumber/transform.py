#!/usr/bin/env python3

"""Read and optionally filter CSV or JSON data with PySpark."""

from pyspark.sql import functions as F

from .formats import file_format_from_location, normalize_file_format


def transform(
    spark,
    input_path,
    file_format=None,
    read_options=None,
    column_names=None,
    filter_column=None,
    filter_value=None,
    count_duplicates=True,
):
    """Read, filter, and optionally consolidate duplicate CSV/JSON rows."""

    # Opgaven kræver Spark-clusteren og HDFS, ikke local mode eller LFS.
    if spark.sparkContext.master.startswith("local"):
        raise RuntimeError("Brug Spark-clusteren, ikke local mode")
    if not input_path.startswith("hdfs://"):
        raise ValueError("Input skal være en HDFS-URI")

    if file_format is None:
        file_format = file_format_from_location(input_path)
    else:
        file_format = normalize_file_format(file_format)

    options = dict(read_options or {})
    if file_format == "csv":
        options.setdefault("header", True)
        options.setdefault("inferSchema", True)

    data = (
        spark.read
        .format(file_format)
        .options(**options)
        .load(input_path)
    )

    if column_names is not None:
        if len(column_names) != len(data.columns):
            raise ValueError(
                "Antallet af kolonnenavne matcher ikke inputdata"
            )
        data = data.toDF(*column_names)

    if filter_column is not None:
        if filter_column not in data.columns:
            raise ValueError(
                f"Filterkolonnen findes ikke: {filter_column}"
            )
        data = data.where(
            F.col(filter_column) == F.lit(filter_value)
        )

    if count_duplicates:
        count_column = "occurrences"
        suffix = 2
        while count_column in data.columns:
            count_column = f"occurrences_{suffix}"
            suffix += 1

        data = data.groupBy(*data.columns).agg(
            F.count(F.lit(1)).alias(count_column)
        )

    return data
