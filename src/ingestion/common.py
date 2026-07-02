import pandas as pd
from pyspark.sql import DataFrame

def write_dataframe_to_csv(
        df: pd.DataFrame | DataFrame,
        file_path: str
) -> None:
    """Write a pandas or Spark DataFrame to a CSV file.

    Spark DataFrames are first collected to pandas via ``toPandas()``, so the
    entire frame must fit in driver memory. The index is omitted from the
    output.

    Args:
        df: The DataFrame to write, either pandas or Spark.
        file_path: Destination path for the CSV file.

    Raises:
        OSError: If the file cannot be written (for example, the destination
            directory does not exist); re-raised with the file path noted.
    """
    if isinstance(df, DataFrame):
        df = df.toPandas()

    try:
        df.to_csv(file_path, index=False)
    except OSError as e:
        e.add_note(f"Failed to write dataframe to CSV with file path: {file_path}")
        raise