from pyspark.sql import SparkSession

#Python → Py4J → JVM → SparkContext → Cluster Manager → Executors
def get_spark_session(app_name: str = "AmazonProductETL-Medallion") -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(app_name)                                          # Application name
        .master("local[2]")                                         # Limit to 2 cores to reduce concurrent memory pressure
        .config("spark.sql.parquet.compression.codec", "snappy")    # Compressed columnar storage
        .config("spark.sql.shuffle.partitions", "4")                # Keep shuffle partitions low for local mode
        .config("spark.driver.memory", "4g")                        # Increased from 2g to handle large text columns
        .config("spark.executor.memory", "4g")                      # Executor memory for task execution
        .config("spark.driver.maxResultSize", "2g")                 # Prevent OOM on collect/count
        .config("spark.local.dir", "/tmp/spark_local")              # Spill to root partition (52GB free) not Apps (813MB free)
        .config("spark.memory.fraction", "0.6")                     # Fraction of JVM heap for execution + storage
        .config("spark.memory.storageFraction", "0.3")              # Fraction of above reserved for caching
        .config("spark.sql.files.maxPartitionBytes", "67108864")    # 64MB max partition size to reduce per-task memory
        .config("spark.network.timeout", "800s")                    # Avoid task timeout during GC pressure
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")                          # Show warnings and errors only
    return spark

