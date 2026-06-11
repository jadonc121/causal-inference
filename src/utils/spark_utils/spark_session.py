from pyspark.sql import SparkSession, DataFrame

def _patch_dataframe_display(row_limit: int = 100):
    """
    Monkeypatches a display method onto the Spark DataFrame object to mirror the display behavior in Databricks.

    Parameters:
        - row_limit: int, default = 100
            This is the max row length the returned dataframe will show.

    Returns:
        - Pandas DataFrame display

    Example: 
        df = spark.createDataFrame(...)
        df.display()
    """

    try:
        from IPython.display import display as ipython_display

        def custom_display(self, limit: int = row_limit):
            ipython_display(self.limit(limit).toPandas())

        DataFrame.display = custom_display

    except ImportError:
        # Ignores if not in a Jupyter notebook.
        pass

def get_spark_session(app_name: str = "causal_inference_project") -> SparkSession:
    """
    Initializes and returns a local SparkSession. 

    For optimization on a small local machine, this sets:
        - small driver memory
        - small level for shuffle partitions
        - log level of "ERROR"

    Note: Log level of ERROR will ignore numerical errors! Convert back to WARN to view these.
    """
    _patch_dataframe_display()

    spark = (
        SparkSession.builder
        .master("local[*]")  # The [*] tells Spark to use all available CPU cores on your Mac
        .appName(app_name)
        .config("spark.driver.memory", "4g")  # Allocates 4GB of RAM to the local driver
        .config("spark.sql.shuffle.partitions", "4")  # Optimizes shuffles for a local machine
        .getOrCreate()
    )
    
    # Keep the console clean from excessive INFO logs
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark