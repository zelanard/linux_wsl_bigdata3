#!/usr/bin/env python3

"""Generate diagrams from Spark DataFrames and store them in HDFS."""

from __future__ import annotations

from collections import defaultdict
import math
from pathlib import Path, PurePosixPath
import re
import tempfile
import uuid
from urllib.parse import urlsplit

from .extract import run_hdfs
from .load import OUTPUT_DIR


_PNG_FILENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*[.]png$")


def _pyplot():
    """Import Matplotlib lazily and select its headless renderer."""

    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot

    return pyplot


def _required_text(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} skal være tekst og må ikke være tom")
    return value.strip()


def _column(dataframe, name):
    name = _required_text(name, "Kolonnenavnet")
    if name not in dataframe.columns:
        raise ValueError(f"DataFrame-kolonnen findes ikke: {name}")
    return name


def _row_limit(value):
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("max_rows skal være et positivt heltal")
    return value


def _rows(dataframe, columns, max_rows):
    rows = (
        dataframe.select(*columns)
        .dropna()
        .limit(_row_limit(max_rows))
        .collect()
    )
    if not rows:
        raise ValueError("DataFrame indeholder ingen data til diagrammet")
    return rows


def _number(value, column):
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Kolonnen {column!r} skal indeholde tal") from error
    if not math.isfinite(number):
        raise ValueError(f"Kolonnen {column!r} indeholder et ugyldigt tal")
    return number


def _validate_destination(output_dir, filename):
    parsed = urlsplit(output_dir)
    if (
        parsed.scheme != "hdfs"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.query
        or parsed.fragment
        or any(character in output_dir for character in "'\";\r\n\x00")
    ):
        raise ValueError("Diagrammets output-mappe skal være en sikker HDFS-URI")
    if (
        not isinstance(filename, str)
        or PurePosixPath(filename).name != filename
        or not _PNG_FILENAME.fullmatch(filename)
    ):
        raise ValueError("Diagrammets filnavn skal være et sikkert .png-filnavn")
    return output_dir.rstrip("/"), filename


def _store_figure(
    figure,
    pyplot,
    output_dir,
    filename,
    *,
    hdfs_runner=None,
):
    """Render a figure locally and atomically move it into HDFS."""

    output_dir, filename = _validate_destination(output_dir, filename)
    runner = hdfs_runner or run_hdfs
    destination = f"{output_dir}/{filename}"
    temporary_destination = (
        f"{output_dir}/.{filename}.uploading-{uuid.uuid4().hex}"
    )
    local_path = None
    temporary_upload_attempted = False

    try:
        with tempfile.NamedTemporaryFile(
            suffix=".png",
            prefix="burning-plumber-",
            delete=False,
        ) as handle:
            local_path = Path(handle.name)

        figure.savefig(
            local_path,
            format="png",
            dpi=160,
            bbox_inches="tight",
        )
        runner("-mkdir", "-p", output_dir)
        temporary_upload_attempted = True
        runner("-put", str(local_path), temporary_destination)
        runner("-rm", "-f", destination, check=False)
        runner("-mv", temporary_destination, destination)
        temporary_upload_attempted = False
    finally:
        pyplot.close(figure)
        if local_path is not None:
            local_path.unlink(missing_ok=True)
        if temporary_upload_attempted:
            runner("-rm", "-f", temporary_destination, check=False)

    print(f"Diagram gemt i HDFS: {destination}")
    return destination


def _finish_axes(axis, title, x_axis, y_axis):
    axis.set_title(_required_text(title, "Diagramtitlen"))
    axis.set_xlabel(
        _required_text(x_axis, "Navnet på x-aksen"),
        labelpad=12,
    )
    axis.set_ylabel(
        _required_text(y_axis, "Navnet på y-aksen"),
        labelpad=10,
    )
    axis.grid(True, alpha=0.25)


def generate_scatter(
    dataframe,
    x_column,
    y_column,
    *,
    title,
    x_axis,
    y_axis,
    output_dir=OUTPUT_DIR,
    filename="scatter_plot.png",
    max_rows=10_000,
    hdfs_runner=None,
):
    """Generate a scatter plot from two numeric DataFrame columns."""

    x_column = _column(dataframe, x_column)
    y_column = _column(dataframe, y_column)
    rows = _rows(dataframe, [x_column, y_column], max_rows)
    x_values = [_number(row[x_column], x_column) for row in rows]
    y_values = [_number(row[y_column], y_column) for row in rows]

    pyplot = _pyplot()
    figure, axis = pyplot.subplots(figsize=(10, 6))
    axis.scatter(x_values, y_values, alpha=0.75, color="#1976d2")
    _finish_axes(axis, title, x_axis, y_axis)
    figure.tight_layout()
    return _store_figure(
        figure,
        pyplot,
        output_dir,
        filename,
        hdfs_runner=hdfs_runner,
    )


def generate_histogram(
    dataframe,
    column,
    *,
    title,
    x_axis,
    y_axis,
    output_dir=OUTPUT_DIR,
    filename="histogram.png",
    bins=10,
    max_rows=10_000,
    hdfs_runner=None,
):
    """Generate a histogram from one numeric DataFrame column."""

    column = _column(dataframe, column)
    if not isinstance(bins, int) or isinstance(bins, bool) or not 1 <= bins <= 100:
        raise ValueError("Antallet af histogramintervaller skal være mellem 1 og 100")
    values = [
        _number(row[column], column)
        for row in _rows(dataframe, [column], max_rows)
    ]

    pyplot = _pyplot()
    figure, axis = pyplot.subplots(figsize=(10, 6))
    axis.hist(values, bins=bins, color="#00897b", edgecolor="white")
    _finish_axes(axis, title, x_axis, y_axis)
    figure.tight_layout()
    return _store_figure(
        figure,
        pyplot,
        output_dir,
        filename,
        hdfs_runner=hdfs_runner,
    )


def generate_boxplot(
    dataframe,
    value_column,
    *,
    title,
    x_axis,
    y_axis,
    category_column=None,
    output_dir=OUTPUT_DIR,
    filename="boxplot.png",
    max_rows=10_000,
    hdfs_runner=None,
):
    """Generate a boxplot, optionally grouped by a category column."""

    value_column = _column(dataframe, value_column)
    columns = [value_column]
    if category_column is not None:
        category_column = _column(dataframe, category_column)
        columns.insert(0, category_column)

    grouped = defaultdict(list)
    for row in _rows(dataframe, columns, max_rows):
        category = str(row[category_column]) if category_column else value_column
        grouped[category].append(_number(row[value_column], value_column))
    if len(grouped) > 20:
        raise ValueError("Et boxplot kan højst vise 20 kategorier")

    categories = sorted(grouped, key=str.casefold)
    pyplot = _pyplot()
    figure, axis = pyplot.subplots(figsize=(10, 6))
    artists = axis.boxplot(
        [grouped[category] for category in categories],
        tick_labels=categories,
        patch_artist=True,
    )
    for box in artists["boxes"]:
        box.set_facecolor("#7e57c2")
        box.set_alpha(0.55)
    _finish_axes(axis, title, x_axis, y_axis)
    figure.tight_layout()
    return _store_figure(
        figure,
        pyplot,
        output_dir,
        filename,
        hdfs_runner=hdfs_runner,
    )
