"""
Amazon Product ETL Pipeline
Medallion Architecture: Bronze → Silver → Gold
Engine: Apache Spark (PySpark)
"""

from spark_session import get_spark_Session
from extract import extract_to_brinze 
