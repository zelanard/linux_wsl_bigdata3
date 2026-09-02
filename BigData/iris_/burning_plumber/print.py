#!/usr/bin/env python3

"""Print a Spark DataFrame as a readable CLI table."""


def Print(data):
    """Print every row without truncating column values."""

    row_count = data.count()
    print(f"Data ({row_count} rows):")
    data.show(n=row_count, truncate=False)
