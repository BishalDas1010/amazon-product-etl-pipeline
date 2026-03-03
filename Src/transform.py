from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, when, expr


def transform_to_silver(spark: SparkSession, bronze_path: str, silver_output_path: str):
    """
    Silver Layer: Clean, deduplicate, and conform the data.
    Actual columns: PRODUCT_ID, TITLE, BULLET_POINTS, DESCRIPTION,
                    PRODUCT_TYPE_ID, PRODUCT_LENGTH
    - Drop nulls on critical fields
    - Cast data types
    - Standardize text fields
    - Remove duplicates
    """
    bronze_df = spark.read.parquet(bronze_path)

    silver_df = bronze_df \
        .dropDuplicates(["PRODUCT_ID"]) \
        .dropna(subset=["PRODUCT_ID", "TITLE"]) \
        .withColumn("TITLE", trim(col("TITLE"))) \
        .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
        .withColumn("BULLET_POINTS", trim(col("BULLET_POINTS"))) \
        .withColumn("PRODUCT_TYPE_ID", expr("try_cast(PRODUCT_TYPE_ID as INT)")) \
        .withColumn("PRODUCT_LENGTH", expr("try_cast(PRODUCT_LENGTH as FLOAT)")) \
        .drop("ingestion_timestamp", "source_file", "layer")

    # Handle nulls in numeric columns
    silver_df = silver_df \
        .withColumn("PRODUCT_LENGTH",
                     when(col("PRODUCT_LENGTH").isNull(), 0.0).otherwise(col("PRODUCT_LENGTH"))) \
        .withColumn("PRODUCT_TYPE_ID",
                     when(col("PRODUCT_TYPE_ID").isNull(), 0).otherwise(col("PRODUCT_TYPE_ID")))

    silver_df.cache()
    row_count = silver_df.count()  # Trigger caching before write to avoid double execution
    silver_df.write.mode("overwrite").parquet(silver_output_path)
    silver_df.unpersist()

    print(f"[SILVER]  Cleaned {row_count} rows to {silver_output_path}")
    return silver_df