import os
from pyspark.sql import SparkSession

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
USER = os.getenv("MINIO_USER", "minioadmin")
PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin")

BRONZE = "s3a://bronze/questions"
SILVER = "s3a://silver/questions"
GOLD = "s3a://gold"

PACKAGES = ",".join([
    "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
])


def get_spark(name):
    spark = (
        SparkSession.builder.appName(name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.jars.packages", PACKAGES)
        .config("spark.jars.ivy", "/home/airflow/.ivy2")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", USER)
        .config("spark.hadoop.fs.s3a.secret.key", PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
