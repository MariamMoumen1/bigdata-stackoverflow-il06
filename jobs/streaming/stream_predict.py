"""Tache 6 : Structured Streaming - prediction des tags en continu sur le flux Kafka.
Responsable : Mariam Moumen (aidee par Mohammed Hedda).

1. Lit en continu le topic Kafka 'questions'.
2. Construit le texte (titre + corps sans HTML).
3. Applique le pipeline TF-IDF de Hedda (PipelineModel sauvegarde par jobs/ml/features.py).
4. Applique les classifieurs One-vs-Rest (un par tag, sauvegardes par train_model.py).
5. Ecrit les predictions en Parquet dans MinIO (checkpoint = reprise sans perte ni doublon)
   et affiche un extrait de chaque micro-lot.

Variables d'environnement :
  STREAM_ONCE=1        traite ce qui est deja dans Kafka puis s'arrete (test)
  STREAM_OFFSETS       earliest ou latest (defaut : earliest si STREAM_ONCE=1, sinon latest)
  STREAM_THRESHOLD     seuil de probabilite pour retenir un tag (defaut 0.5)
  STREAM_RUN           suffixe du checkpoint : changer (v2, v3...) pour rejouer depuis le debut
  STREAM_MAX_OFFSETS   nombre maximum de messages par micro-lot (defaut 2000)
"""

import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.functions import vector_to_array
from common import get_spark, KAFKA, GOLD

TOPIC = os.getenv("KAFKA_TOPIC", "questions")
ONCE = os.getenv("STREAM_ONCE") == "1"
OFFSETS = os.getenv("STREAM_OFFSETS", "earliest" if ONCE else "latest")
THRESHOLD = float(os.getenv("STREAM_THRESHOLD", "0.5"))
RUN_ID = os.getenv("STREAM_RUN", "v1")
MAX_PER_BATCH = os.getenv("STREAM_MAX_OFFSETS", "2000")

MODELS = f"{GOLD}/models"
OUT = f"{GOLD}/streaming/predictions"
CHECKPOINT = f"{GOLD}/streaming/_checkpoint/predictions_{RUN_ID}"

spark = get_spark("streaming_tag_prediction")
spark.conf.set("spark.sql.shuffle.partitions", "4")

print("=" * 70)
print("TACHE 6 : STRUCTURED STREAMING - PREDICTION EN TEMPS REEL")
print("=" * 70)

# Meme schema que producer.py
schema = StructType([
    StructField("id", LongType()),
    StructField("owner_user_id", StringType()),
    StructField("creation_date", StringType()),
    StructField("score", IntegerType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("tags", ArrayType(StringType())),
])


# 1. Pipeline TF-IDF de Hedda (s3a://models si le bucket existe, sinon le repli gold/ml)
def load_tfidf():
    for path in ("s3a://models/tfidf_pipeline", f"{GOLD}/ml/tfidf_pipeline"):
        try:
            model = PipelineModel.load(path)
            print(f"Pipeline TF-IDF charge depuis {path}")
            return model
        except Exception as e:
            print(f"TF-IDF introuvable dans {path} : {str(e)[:120]}")
    sys.exit("ERREUR : pipeline TF-IDF introuvable. Lance d'abord train_model.py.")


tfidf_model = load_tfidf()

# 2. Tags modelises et classifieurs (lr_00, lr_01... dans l'ordre de top_tags)
top_tags = [r["tag"] for r in spark.read.parquet(f"{GOLD}/ml/top_tags").orderBy("rank").collect()]
models = []
for i, tag in enumerate(top_tags):
    try:
        models.append((tag, LogisticRegressionModel.load(f"{MODELS}/lr_{i:02d}")))
    except Exception:
        print(f"Modele du tag '{tag}' absent, ignore")
if not models:
    sys.exit(f"ERREUR : aucun modele dans {MODELS}. Lance d'abord train_model.py.")
print(f"{len(models)} classifieurs charges : {[t for t, _ in models]}")

# 3. Lecture du flux Kafka
print(f"Connexion au topic '{TOPIC}' sur {KAFKA} (offsets : {OFFSETS})...")
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_PER_BATCH)
    .load()
)

parsed = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .where(F.col("id").isNotNull())
)

texts = (
    parsed
    .withColumn("clean_body", F.regexp_replace(F.coalesce("body", F.lit("")), "<[^>]+>", " "))
    .withColumn("text", F.trim(F.concat_ws(" ", F.lower(F.coalesce("title", F.lit(""))), F.lower("clean_body"))))
    .select("id", "title", "text", "tags")
)

features_stream = tfidf_model.transform(texts)


# 4. Pour chaque micro-lot : un classifieur par tag, puis ecriture dans MinIO
def score_batch(batch, batch_id):
    if batch.isEmpty():
        return

    scored = batch
    for i, (tag, m) in enumerate(models):
        scored = (
            m.transform(scored)
            .withColumn(f"p_{i}", vector_to_array("probability")[1])
            .drop("rawPrediction", "probability", "prediction")
        )

    candidates = [F.when(F.col(f"p_{i}") >= THRESHOLD, F.lit(tag)) for i, (tag, _) in enumerate(models)]
    out = (
        scored
        .withColumn("predicted_tags", F.filter(F.array(*candidates), lambda x: x.isNotNull()))
        .withColumn(
            "n_correct",
            F.when(F.col("tags").isNotNull(), F.size(F.array_intersect("tags", "predicted_tags"))).otherwise(0),
        )
        .select("id", "title", "tags", "predicted_tags", "n_correct")
        .withColumn("batch_id", F.lit(int(batch_id)))
        .withColumn("predicted_at", F.current_timestamp())
        .persist()
    )

    out.write.mode("append").parquet(OUT)
    st = out.agg(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("n_correct") > 0, 1).otherwise(0)).alias("hit"),
    ).first()
    print(f"\n--- Micro-lot {batch_id} : {st['n']} questions, "
          f"au moins 1 tag correct pour {st['hit']} ---")
    out.select("id", "title", "tags", "predicted_tags").show(5, truncate=50)
    out.unpersist()


writer = (
    features_stream.writeStream
    .foreachBatch(score_batch)
    .option("checkpointLocation", CHECKPOINT)
)

if ONCE:
    query = writer.trigger(availableNow=True).start()
else:
    query = writer.trigger(processingTime="10 seconds").start()

print(f"\nSTREAMING actif : {TOPIC} -> {OUT} (checkpoint : {CHECKPOINT})")
query.awaitTermination()
print("STREAMING termine")
spark.stop()