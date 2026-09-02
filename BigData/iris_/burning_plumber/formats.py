"""Shared CSV and JSON format validation."""

import re
from pathlib import PurePosixPath
from urllib.parse import unquote, urlsplit


SUPPORTED_FORMATS = frozenset({"csv", "json"})
FILENAME_PATTERN = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]*\.(?:csv|json)",
    flags=re.IGNORECASE,
)


def filename_from_location(location):
    """Return a safe CSV or JSON filename from a URL or HDFS URI."""

    filename = unquote(PurePosixPath(urlsplit(location).path).name)
    if not FILENAME_PATTERN.fullmatch(filename):
        raise ValueError("Placeringen skal have et gyldigt CSV- eller JSON-navn")
    return filename


def filename_from_url(source_url):
    """Return a safe filename from an HTTPS source URL."""

    if urlsplit(source_url).scheme.lower() != "https":
        raise ValueError("Datakilden skal bruge HTTPS")
    return filename_from_location(source_url)


def normalize_file_format(file_format):
    """Validate and normalize a supported Spark file format."""

    normalized = file_format.lower().lstrip(".")
    if normalized not in SUPPORTED_FORMATS:
        raise ValueError("Filformatet skal være CSV eller JSON")
    return normalized


def file_format_from_location(location):
    """Detect CSV or JSON from a URL or HDFS URI filename."""

    filename = filename_from_location(location)
    return normalize_file_format(PurePosixPath(filename).suffix)
