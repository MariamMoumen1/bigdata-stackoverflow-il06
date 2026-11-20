"""Tache 4 : Entrainement One-vs-Rest multi-etiquettes (Mariam Moumen).
Part des features generees et sauvegardees dans MinIO par Mohammed Hedda (jobs/ml/features.py).
Aucune dependance directe : lit s3a://gold/ml/train_features et les top_tags.
"""

import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

from pyspark.sql.functions import col, array_contains
from pyspark.ml.classification import LogisticRegression
from common import get_spark, GOLD

spark = get_spark("train_one_vs_rest")
print("=" * 70)
print("TACHE 4 : ENTRAINEMENT ONE-VS-REST MULTI-ETIQUETTES (MARIAM)")
print("=" * 70)

# 1. Lecture des features preparees par Hedda dans MinIO
train_path = f"{GOLD}/ml/train_features"
tags_path = f"{GOLD}/ml/top_tags"

print(f"Lecture du jeu d'entrainement vectorise depuis {train_path}...")
train_df = spark.read.parquet(train_path).cache()

print(f"Lecture des top tags depuis {tags_path}...")
top_tags = [r["tag"] for r in spark.read.parquet(tags_path).orderBy("rank").collect()]
print(f"Tags a entrainer ({len(top_tags)}) : {top_tags}")

# 2. Entrainement d'un classifieur binaire par tag
models = {}
MODELS_DIR = "s3a://models/ovr"

for tag in top_tags:
    print(f"\nEntrainement du modele binaire pour le tag '{tag}'...")
    labeled_df = train_df.withColumn("label", array_contains("tags", tag).cast("double"))
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        regParam=0.01,
        probabilityCol=f"prob_{tag}"
    )
    model = lr.fit(labeled_df)
    model_save_path = f"{MODELS_DIR}/{tag}"
    model.write().overwrite().save(model_save_path)
    print(f"Modele '{tag}' entraine et sauvegarde dans {model_save_path}")

print("\n" + "=" * 70)
print("TACHE 4 TERMINEE : Tous les modeles One-vs-Rest sont sauvegardes dans MinIO.")
print("=" * 70)
spark.stop()
