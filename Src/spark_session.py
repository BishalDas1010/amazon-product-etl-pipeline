from pyspark.sql import SparkSession


def get_spark_Session(app_name:str = "AmazoneProductEtl") -> SparkSession:
    return(
        SparkSession.builder
        .appName(app_name)
        # Spark does NOT natively know how to talk to PostgreSQL.
        # It needs a JDBC driver
        .config("spark.jars","jars/postgresql-42.6.0.jar")
        .master("local[*]")#run the local machine and all cpu cores :(
        .getOrCreate()#if sparkSession alredy exixts resue it if not then create it 

    )