import argparse
from pathlib import PurePosixPath
from urllib.parse import unquote, urlsplit

from pyspark.sql import SparkSession
import burning_plumber as mario


SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "jbrownlee/Datasets/master/iris.csv"
)
HDFS_URI = "hdfs://localhost:9000"
INPUT_DIR = "/user/zelanard/Input_dir"
OUTPUT_DIR = f"{HDFS_URI}/user/zelanard/Output_dir"
READ_OPTIONS = {
    "header": True,
    "inferSchema": True,
}
COLUMN_NAMES = [
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "species",
]
FILTER_COLUMN = "species"
FILTER_VALUE = "Iris-setosa"


def extract():
    """Download the configured source into HDFS."""

    return mario.extract(
        SOURCE_URL,
        input_dir=INPUT_DIR,
        column_names=COLUMN_NAMES,
    )


def _source_name():
    return unquote(PurePosixPath(urlsplit(SOURCE_URL).path).name)


def _input_path():
    return f"{HDFS_URI}{INPUT_DIR}/{_source_name()}"


def _output_path():
    return f"{OUTPUT_DIR}/transformed_{_source_name()}"


def flow():
    """Run extract, transform, print, and load once."""

    extracted_path = extract()
    spark = (
        SparkSession.builder
        .appName("IrisETLFlow")
        .getOrCreate()
    )

    try:
        transformed_data = mario.transform(
            spark,
            f"{HDFS_URI}{extracted_path}",
            read_options=READ_OPTIONS,
            column_names=COLUMN_NAMES,
            filter_column=FILTER_COLUMN,
            filter_value=FILTER_VALUE,
        )
        mario.Print(transformed_data)
        return mario.load(
            transformed_data,
            SOURCE_URL,
            output_dir=OUTPUT_DIR,
        )
    finally:
        spark.stop()


def listen(poll_interval=2.0):
    """Watch the HDFS input and run transform/load after changes."""

    def create_spark():
        return (
            SparkSession.builder
            .appName("IrisETLListener")
            .getOrCreate()
        )

    mario.listen(
        create_spark,
        _input_path(),
        SOURCE_URL,
        output_dir=OUTPUT_DIR,
        poll_interval=poll_interval,
        read_options=READ_OPTIONS,
        column_names=COLUMN_NAMES,
        filter_column=FILTER_COLUMN,
        filter_value=FILTER_VALUE,
    )


def print_output():
    """Print the current transformed HDFS output as a CLI table."""

    return mario.print_hdfs(_output_path())


def stop():
    """Stop the active listener without deleting data."""

    return mario.stop()


def reset():
    """Stop listening and remove the configured HDFS pipeline data."""

    return mario.reset(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR)


def main():
    parser = argparse.ArgumentParser(description="CSV/JSON HDFS ETL controller")
    parser.add_argument(
        "command",
        choices=("extract", "flow", "listen", "print", "stop", "reset"),
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Seconds between HDFS checks while listening",
    )
    arguments = parser.parse_args()

    commands = {
        "extract": extract,
        "flow": flow,
        "listen": lambda: listen(arguments.poll_interval),
        "print": print_output,
        "stop": stop,
        "reset": reset,
    }
    commands[arguments.command]()


if __name__ == "__main__":
    main()
