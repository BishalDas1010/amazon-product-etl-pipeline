"""
Amazon Product ETL Pipeline
============================
Medallion Architecture: Bronze → Silver → Gold
Engine: Apache Spark (PySpark)
"""

from spark_session import get_spark_session
from extract import extract_to_bronze
from transform import transform_to_silver
from load import load_to_gold




# Configuration
INPUT_DATA_PATH = "/media/bishaldas/Apps/amazon-product-etl-pipeline/db/train.csv"
BRONZE_PATH = "/media/bishaldas/Apps/amazon-product-etl-pipeline/output/Bronze"
SILVER_PATH = "/media/bishaldas/Apps/amazon-product-etl-pipeline/output/silver"
GOLD_PATH = "/media/bishaldas/Apps/amazon-product-etl-pipeline/output/Gold"


def run_pipeline():
    print("=" * 60)
    print("🚀 Amazon Product ETL Pipeline - Medallion Architecture")
    print("=" * 60)

    # Initialize Spark
    spark = get_spark_session()

    try:
        # Layer 1: Bronze (Raw Ingestion)
        print("\n BRONZE LAYER: Raw Data Ingestion")
        print("-" * 40)
        extract_to_bronze(spark, INPUT_DATA_PATH, BRONZE_PATH)

        # Layer 2: Silver (Cleaning & Transformation)
        print("\nSILVER LAYER: Data Cleaning & Transformation")
        print("-" * 40)
        transform_to_silver(spark, BRONZE_PATH, SILVER_PATH)

        # Layer 3: Gold (Business Aggregations)
        print("\n GOLD LAYER: Business Aggregations")
        print("-" * 40)
        load_to_gold(spark, SILVER_PATH, GOLD_PATH)

        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()