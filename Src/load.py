from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, count, min as spark_min, max as spark_max, round as spark_round


def load_to_gold(spark: SparkSession, silver_path: str, gold_output_path: str):
    """
    Gold Layer: Business-level aggregations and KPIs.
    Actual columns: PRODUCT_ID, TITLE, BULLET_POINTS, DESCRIPTION,
                    PRODUCT_TYPE_ID, PRODUCT_LENGTH

    - Product type summary: count and avg/min/max length per PRODUCT_TYPE_ID
    - Top products ranked by PRODUCT_LENGTH
    """
    silver_df = spark.read.parquet(silver_path)
    silver_df.cache()  # Cache to avoid re-reading parquet for each aggregation

    # Aggregation 1: Summary per product type
    type_summary = silver_df \
        .groupBy("PRODUCT_TYPE_ID") \
        .agg(
            count("PRODUCT_ID").alias("product_count"),
            spark_round(avg("PRODUCT_LENGTH"), 2).alias("avg_length"),
            spark_round(spark_min("PRODUCT_LENGTH"), 2).alias("min_length"),
            spark_round(spark_max("PRODUCT_LENGTH"), 2).alias("max_length")
        ) \
        .orderBy(col("product_count").desc())

    type_summary.write.mode("overwrite").parquet(f"{gold_output_path}/type_summary")

    # Aggregation 2: Top products by PRODUCT_LENGTH
    top_by_length = silver_df \
        .filter(col("PRODUCT_LENGTH") > 0) \
        .select("PRODUCT_ID", "TITLE", "PRODUCT_TYPE_ID", "PRODUCT_LENGTH") \
        .orderBy(col("PRODUCT_LENGTH").desc())

    top_by_length.write.mode("overwrite").parquet(f"{gold_output_path}/top_by_length")

    type_count = type_summary.count()
    top_count = top_by_length.count()

    silver_df.unpersist()

    print(f"[GOLD]  Written {type_count} product-type summaries "
          f"and {top_count} length-ranked products to {gold_output_path}")
    return type_summary
