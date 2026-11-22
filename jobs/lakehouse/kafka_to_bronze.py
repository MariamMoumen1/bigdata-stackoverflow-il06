"""D : Kafka -> Bronze. Stocke les messages BRUTS (aucune transformation) en Parquet dans MinIO.
On garde aussi les metadonnees Kafka (topic, partition, offset, horodatage) pour la tracabilite.
Streaming en mode 'availableNow' + checkpoint : on lit tout ce qui est nouveau, puis on s'arrete.
Relancer le job ne cree donc pas de doublons dans Bronze."""
import sys
sys.path.insert(0, "/opt/project/jobs")
from pyspark.sql.functions import col
from common import get_spark, KAFKA, BRONZE

CHECKPOINT = "s3a://bronze/_checkpoints/questions"

spark = get_spark("kafka_to_bronze")

stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "questions")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", "false")
    .load()
)

raw = stream.select(
    col("value").cast("string").alias("json"),        # message JSON tel que recu, non modifie
    col("timestamp").alias("kafka_ts"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
)

query = (
    raw.writeStream.format("parquet")
    .option("path", BRONZE)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

n = spark.read.parquet(BRONZE).count()
print(f"BRONZE OK : {n} messages dans {BRONZE}")
spark.stop()