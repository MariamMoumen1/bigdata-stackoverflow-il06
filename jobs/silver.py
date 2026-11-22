"""SILVER : parse le JSON, applique le nettoyage de Hedda, et fait un UPSERT dans Apache Hudi.
Utilise clean_questions (jobs/transform/cleaning.py).
"""
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
from common import get_spark, BRONZE, SILVER
from transform.cleaning import clean_questions

spark = get_spark("silver_hudi")

schema = StructType([
    StructField("id", LongType()),
    StructField("owner_user_id", StringType()),
    StructField("creation_date", StringType()),
    StructField("score", IntegerType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("tags", ArrayType(StringType())),
])

bronze = spark.read.parquet(BRONZE)
parsed = bronze.select(from_json(col("json"), schema).alias("d"), "kafka_ts").select("d.*", "kafka_ts")

clean = clean_questions(parsed)

hudi_options = {
    "hoodie.table.name": "questions_silver",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "kafka_ts",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.partitionpath.field": "year_month",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.upsert.shuffle.parallelism": "4",
    "hoodie.insert.shuffle.parallelism": "4",
    "hoodie.metadata.enable": "false",
}

clean.write.format("hudi").options(**hudi_options).mode("append").save(SILVER)
print(f"SILVER OK : {clean.count()} questions upsertees dans {SILVER}")
spark.stop()
