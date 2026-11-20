"""MODELE : prediction des tags a partir du texte.
One-vs-Rest multi-etiquettes (Mariam) base sur les features TF-IDF de Mohammed Hedda (jobs/ml/features.py).
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import col, array_contains
from common import get_spark, SILVER, GOLD
from ml.features import prepare_ml_data, save_features_and_pipeline

N_TAGS = int(os.getenv("N_TAGS", "10"))
spark = get_spark("train_model")

# 1. Lecture de Silver
try:
    q = spark.read.format("hudi").option("hoodie.metadata.enable", "false").load(SILVER)
except Exception:
    q = spark.read.parquet(SILVER)

# 2. Utilisation du module features de Hedda
fitted_pipeline, top_tags, train_f, test_f, sample_f = prepare_ml_data(q, n_tags=N_TAGS, train_ratio=0.8, seed=42)
train_f = train_f.cache()
test_f = test_f.cache()

# Sauvegarde des features dans MinIO pour streaming et reproductibilite (dont le sample Phase 2)
save_features_and_pipeline(spark, fitted_pipeline, top_tags, train_f, test_f, sample_features=sample_f)

# 3. Entrainement One-vs-Rest par Mariam
print(f"\nEntrainement des modeles de regression logistique pour les {len(top_tags)} tags...")
auc_eval = BinaryClassificationEvaluator(metricName="areaUnderROC")
f1_eval = MulticlassClassificationEvaluator(metricName="f1")

rows = []
for tag in top_tags:
    tr = train_f.withColumn("label", array_contains("tags", tag).cast("double"))
    te = test_f.withColumn("label", array_contains("tags", tag).cast("double"))
    try:
        model = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.01).fit(tr)
        pred = model.transform(te)
        auc, f1 = float(auc_eval.evaluate(pred)), float(f1_eval.evaluate(pred))
    except Exception as e:
        print(f"Tag {tag} ignore : {e}")
        auc, f1 = None, None
    n_pos = int(tr.filter("label = 1").count())
    rows.append((tag, n_pos, auc, f1))
    print(f"{tag}: {n_pos} exemples positifs | AUC={auc:.4f} | F1={f1:.4f}")

metrics_df = spark.createDataFrame(rows, "tag string, nb_train_positifs int, auc double, f1 double")
metrics_df.write.mode("overwrite").parquet(f"{GOLD}/model_metrics")
print(f"\nMODELE OK : metriques ecrites dans {GOLD}/model_metrics")
spark.stop()
