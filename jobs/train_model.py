"""MODELE : point d'entree du DAG (task train_model).
Enchaine : features TF-IDF (Hedda, ml/features.py) -> entrainement (tache 4, ml/train.py)
-> evaluation et sauvegarde des metriques (tache 5, ml/evaluate.py).
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from common import get_spark, SILVER
from ml.features import prepare_ml_data, save_features_and_pipeline
from ml.train import train_models
from ml.evaluate import evaluate_models

N_TAGS = int(os.getenv("N_TAGS", "10"))
spark = get_spark("train_model")

# 1. Lecture de Silver
try:
    q = spark.read.format("hudi").option("hoodie.metadata.enable", "false").load(SILVER)
except Exception:
    q = spark.read.parquet(SILVER)

# 2. Features de Hedda (sauvegardees dans MinIO pour le streaming et la reproductibilite)
fitted_pipeline, top_tags, train_f, test_f, sample_f = prepare_ml_data(q, n_tags=N_TAGS, train_ratio=0.8, seed=42)
train_f = train_f.cache()
test_f = test_f.cache()
save_features_and_pipeline(spark, fitted_pipeline, top_tags, train_f, test_f, sample_features=sample_f)

# 3. Tache 4 puis tache 5
train_models(train_f, top_tags)
evaluate_models(spark, test_f)

print("MODELE OK : entrainement et evaluation termines")
spark.stop()