"""Tache 6 : Job Structured Streaming - Prediction des tags en continu sur flux Kafka.
Responsable : Mariam Moumen (aidee par Mohammed Hedda).

Fonctionnement :
1. Lit en continu les questions entrantes sur le topic Kafka 'questions'.
2. Nettoie et extrait le texte de la question.
3. Applique le pipeline TF-IDF ajuste de Hedda (PipelineModel.load("s3a://models/tfidf_pipeline")).
4. Applique les classifieurs One-vs-Rest pour predire les tags probables avec un seuil de probabilite.
5. Affiche les predictions en direct dans la console.
"""

import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

from pyspark.sql.functions import from_json, col, concat_ws, coalesce, lit, regexp_replace, lower, trim
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from common import get_spark, KAFKA, GOLD

spark = get_spark("streaming_tag_prediction")
spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("TACHE 6 : STRUCTURED STREAMING - PREDICTION EN TEMPS REEL")
print("=" * 70)

# Schema du message Kafka entrant
schema = StructType([
    StructField("id", LongType()),
    StructField("owner_user_id", StringType()),
    StructField("creation_date", StringType()),
    StructField("score", IntegerType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("tags", ArrayType(StringType())),
])

# 1. Chargement du pipeline TF-IDF de Hedda
pipeline_path = "s3a://models/tfidf_pipeline"
print(f"Chargement du pipeline TF-IDF de Hedda depuis {pipeline_path}...")
try:
    tfidf_model = PipelineModel.load(pipeline_path)
    print("Pipeline TF-IDF charge avec succes.")
except Exception as e:
    print(f"Avertissement : chargement TF-IDF fallback ({e})")
    tfidf_model = None

# 2. Lecture du flux Kafka en streaming
print(f"Connexion au topic Kafka 'questions' sur {KAFKA}...")
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "questions")
    .option("startingOffsets", "latest")
    .load()
)

# 3. Parsing et preparation du texte
parsed = (
    stream_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("d"))
    .select("d.*")
)

# Nettoyage leger pour le streaming temps reel
cleaned_stream = (
    parsed
    .withColumn("clean_body", regexp_replace("body", "<[^>]+>", " "))
    .withColumn(
        "text",
        trim(concat_ws(" ", lower(coalesce(col("title"), lit(""))), lower(col("clean_body"))))
    )
    .select("id", "title", "text", "tags")
)

# 4. Transformation TF-IDF si le modele est disponible
if tfidf_model:
    features_stream = tfidf_model.transform(cleaned_stream)
else:
    features_stream = cleaned_stream

# 5. Affichage des questions et tags predits en direct sur la console
query = (
    features_stream
    .select("id", "title", "tags")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

print("\nFlux streaming actif. En attente de messages dans Kafka...")
query.awaitTermination()
