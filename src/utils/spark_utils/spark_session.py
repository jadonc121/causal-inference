from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "causal_inference_project") -> SparkSession:
    """
    Initializes and returns a local SparkSession. 

    For optimization on a small local machine, this sets:
        - small driver memory
        - small level for shuffle partitions
        - log level of "ERROR"

    Note: Log level of ERROR will ignore numerical errors! Convert back to WARN to view these.
    """
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