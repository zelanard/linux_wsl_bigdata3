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
HIVE_DATABASE = "iris_db"
HIVE_TABLE = "iris"
HIVE_WAREHOUSE_DIR = f"{HDFS_URI}/user/hive/warehouse"
HIVE_JDBC_URL = "jdbc:hive2://localhost:10000/default"
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
        output_path = mario.load(
            transformed_data,
            SOURCE_URL,
            output_dir=OUTPUT_DIR,
        )
        mario.load_hive(
            transformed_data,
            HIVE_DATABASE,
            HIVE_TABLE,
            warehouse_dir=HIVE_WAREHOUSE_DIR,
            jdbc_url=HIVE_JDBC_URL,
            mode="overwrite",
        )
        return output_path
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
        hive_database=HIVE_DATABASE,
        hive_table=HIVE_TABLE,
        hive_warehouse_dir=HIVE_WAREHOUSE_DIR,
        hive_jdbc_url=HIVE_JDBC_URL,
    )


def print_output():
    """Print the current transformed HDFS output as a CLI table."""

    return mario.print_hdfs(_output_path())


def show_hive():
    """Show the Hive table using the built-in Beeline client."""

    return mario.show_hive(
        HIVE_DATABASE,
        HIVE_TABLE,
        jdbc_url=HIVE_JDBC_URL,
    )


def _generate_diagram(generator, app_name, *arguments, **options):
    """Load the Hive table and pass its DataFrame to a diagram generator."""

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    try:
        dataframe = mario.hive_dataframe(
            spark,
            HIVE_DATABASE,
            HIVE_TABLE,
            jdbc_url=HIVE_JDBC_URL,
        )
        return generator(
            dataframe,
            *arguments,
            output_dir=OUTPUT_DIR,
            **options,
        )
    finally:
        spark.stop()


def generate_scatter():
    """Generate the Iris sepal scatter plot from Hive data."""

    return _generate_diagram(
        mario.generate_scatter,
        "IrisScatterPlot",
        "sepal_length",
        "sepal_width",
        title="Iris sepal measurements",
        x_axis="Sepal length (cm)",
        y_axis="Sepal width (cm)",
    )


def generate_histogram():
    """Generate the Iris sepal-length histogram from Hive data."""

    return _generate_diagram(
        mario.generate_histogram,
        "IrisHistogram",
        "sepal_length",
        title="Iris sepal-length distribution",
        x_axis="Sepal length (cm)",
        y_axis="Frequency",
    )


def generate_boxplot():
    """Generate the Iris petal-length boxplot from Hive data."""

    return _generate_diagram(
        mario.generate_boxplot,
        "IrisBoxplot",
        "petal_length",
        category_column="species",
        title="Iris petal length by species",
        x_axis="Species",
        y_axis="Petal length (cm)",
    )


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
        choices=(
            "extract",
            "flow",
            "listen",
            "print",
            "show-hive",
            "generate",
            "stop",
            "reset",
        ),
    )
    parser.add_argument(
        "diagram",
        nargs="?",
        choices=("scatter", "histogram", "boxplot"),
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
        "show-hive": show_hive,
        "stop": stop,
        "reset": reset,
    }
    diagram_commands = {
        "scatter": generate_scatter,
        "histogram": generate_histogram,
        "boxplot": generate_boxplot,
    }
    if arguments.command == "generate":
        if arguments.diagram is None:
            parser.error("generate kræver scatter, histogram eller boxplot")
        diagram_commands[arguments.diagram]()
    else:
        if arguments.diagram is not None:
            parser.error("diagramtypen kan kun bruges sammen med generate")
        commands[arguments.command]()


if __name__ == "__main__":
    main()
