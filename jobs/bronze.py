"""BRONZE : lit tous les messages du topic Kafka 'questions' et les stocke bruts (JSON) en Parquet dans MinIO."""
from pyspark.sql.functions import col
from common import get_spark, KAFKA, BRONZE

spark = get_spark("bronze_ingest")

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "questions")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
raw = df.select(col("value").cast("string").alias("json"), col("timestamp").alias("kafka_ts"))
raw.write.mode("overwrite").parquet(BRONZE)
print(f"BRONZE OK : {raw.count()} messages ecrits dans {BRONZE}")
spark.stop()
