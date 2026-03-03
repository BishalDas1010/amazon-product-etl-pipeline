#this layer is Bronze layer 
import logging 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp,input_file_name,lit 


#This creates a logger object.
#__name__ = name of current file
# This helps identify which module produced the log...
logger = logging.getLogger(__name__)

def extract_to_bronze (spark:SparkSession,input_path:str,bronze_output_path:str):

    #raw  csv data 
    raw_df =spark.read.option("header",True) \
        .option("inferschema",True) \
        .csv(input_path)
    # add ingestion metadata 
    """
    .parquet(bronze_output_path)
    Saves data in Parquet format.
    why Parquet?
    ✔ Columnar
    ✔ Compressed
    ✔ Fast for analytics
    ✔ Best for Data Lake
    """
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp",current_timestamp())\
        .withColumn("source_file",input_file_name())\
        .withColumn("layer",lit("bronze"))
    bronze_df.write.mode("overwrite").parquet(bronze_output_path)
    print(f"Bronze ingested to  {bronze_df.count()} rows to  {bronze_output_path}")
    return bronze_df