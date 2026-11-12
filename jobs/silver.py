"""SILVER : parse le JSON, nettoie le HTML, dedoublonne et fait un UPSERT dans une table Apache Hudi."""
from pyspark.sql.functions import from_json, col, to_timestamp, regexp_replace, date_format, size
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
from common import get_spark, BRONZE, SILVER

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

clean = (
    parsed.dropna(subset=["id"])
    .withColumn("creation_ts", to_timestamp("creation_date"))
    .withColumn("body_text", regexp_replace("body", "<[^>]+>", " "))
    .withColumn("year_month", date_format("creation_ts", "yyyy-MM"))
    .withColumn("n_tags", size("tags"))
    .drop("body")
    .dropDuplicates(["id"])
)

hudi_options = {
    "hoodie.table.name": "questions_silver",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "kafka_ts",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.upsert.shuffle.parallelism": "4",
    "hoodie.insert.shuffle.parallelism": "4",
    "hoodie.metadata.enable": "false",
}

clean.write.format("hudi").options(**hudi_options).mode("append").save(SILVER)
print(f"SILVER OK : {clean.count()} questions upsertees dans {SILVER}")
spark.stop()
