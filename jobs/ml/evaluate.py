"""Tache 5 : Evaluation multi-metriques et rapport global (Mariam Moumen).
Evalue les modeles One-vs-Rest sur le jeu de test de Hedda (s3a://gold/ml/test_features).
Calcule Precision, Rappel, F1-Score et AUC pour chaque tag, puis sauvegarde dans s3a://gold/model_metrics.
"""

import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

from pyspark.sql.functions import col, array_contains
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from common import get_spark, GOLD

spark = get_spark("evaluate_models")
print("=" * 70)
print("TACHE 5 : EVALUATION DES MODELES (PRECISION, RAPPEL, F1, AUC) - MARIAM")
print("=" * 70)

test_path = f"{GOLD}/ml/test_features"
tags_path = f"{GOLD}/ml/top_tags"
MODELS_DIR = "s3a://models/ovr"

print(f"Lecture du jeu de test vectorise depuis {test_path}...")
test_df = spark.read.parquet(test_path).cache()

top_tags = [r["tag"] for r in spark.read.parquet(tags_path).orderBy("rank").collect()]

auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
f1_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
precision_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
recall_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")

metrics_rows = []

for tag in top_tags:
    model_path = f"{MODELS_DIR}/{tag}"
    try:
        model = LogisticRegressionModel.load(model_path)
        labeled_test = test_df.withColumn("label", array_contains("tags", tag).cast("double"))
        preds = model.transform(labeled_test)

        auc = float(auc_eval.evaluate(preds))
        f1 = float(f1_eval.evaluate(preds))
        precision = float(precision_eval.evaluate(preds))
        recall = float(recall_eval.evaluate(preds))
        nb_pos = int(labeled_test.filter("label = 1").count())

        metrics_rows.append((tag, nb_pos, auc, precision, recall, f1))
        print(f"Tag: {tag:<15} | AUC: {auc:.4f} | Prec: {precision:.4f} | Rappel: {recall:.4f} | F1: {f1:.4f}")
    except Exception as e:
        print(f"Impossible d'evaluer '{tag}' ({e})")

# Sauvegarde des metriques pour le Dashboard Streamlit
schema = "tag string, nb_test_positifs int, auc double, precision double, rappel double, f1 double"
metrics_df = spark.createDataFrame(metrics_rows, schema)

output_metrics_path = f"{GOLD}/model_metrics"
metrics_df.write.mode("overwrite").parquet(output_metrics_path)
print(f"\nTACHE 5 TERMINEE : Metriques sauvegardees dans {output_metrics_path} pour le dashboard.")
spark.stop()
