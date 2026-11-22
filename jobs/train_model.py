"""MODELE : prediction des tags a partir du texte.
One-vs-Rest multi-etiquettes (Mariam) base sur les features TF-IDF de Hedda (jobs/ml/features.py).
Un classifieur binaire par tag ("ce tag est present / absent"), evalue sur le jeu de test
(precision, rappel, F1 du tag, AUC), modeles et metriques sauvegardes dans MinIO.
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from common import get_spark, SILVER, GOLD
from ml.features import prepare_ml_data, save_features_and_pipeline

N_TAGS = int(os.getenv("N_TAGS", "10"))
POS_WEIGHT_MAX = float(os.getenv("POS_WEIGHT_MAX", "20"))  # plafond du poids des exemples positifs
MODELS = f"{GOLD}/models"
spark = get_spark("train_model")

# 1. Lecture de Silver
try:
    q = spark.read.format("hudi").option("hoodie.metadata.enable", "false").load(SILVER)
except Exception:
    q = spark.read.parquet(SILVER)

# 2. Features de Hedda
fitted_pipeline, top_tags, train_f, test_f, sample_f = prepare_ml_data(q, n_tags=N_TAGS, train_ratio=0.8, seed=42)
train_f = train_f.cache()
test_f = test_f.cache()
n_train = train_f.count()
save_features_and_pipeline(spark, fitted_pipeline, top_tags, train_f, test_f, sample_features=sample_f)


def fmt(x):
    return "n/a" if x is None else f"{x:.4f}"


# 3. Un modele par tag : entrainement (tache 4) + evaluation et sauvegarde (tache 5)
print(f"\nEntrainement de {len(top_tags)} modeles (un par tag), train={n_train} lignes")
auc_eval = BinaryClassificationEvaluator(metricName="areaUnderROC")

rows = []
for i, tag in enumerate(top_tags):
    # Index dans le chemin : des tags comme "c#", "c++" ou ".net" casseraient un chemin S3
    model_path = f"{MODELS}/lr_{i:02d}"
    tr = train_f.withColumn("label", F.array_contains("tags", tag).cast("double"))
    te = test_f.withColumn("label", F.array_contains("tags", tag).cast("double"))
    n_pos = int(tr.filter("label = 1").count())

    precision = recall = f1 = auc = None
    tp = fp = fn = None
    saved_path = None
    try:
        if n_pos == 0:
            raise ValueError("aucun exemple positif dans le train")
        # Tags rares : on donne plus de poids aux exemples positifs (plafonne)
        w = min((n_train - n_pos) / n_pos, POS_WEIGHT_MAX)
        tr = tr.withColumn("weight", F.when(F.col("label") == 1.0, F.lit(w)).otherwise(F.lit(1.0)))

        model = LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight",
                                   maxIter=20, regParam=0.01).fit(tr)
        pred = model.transform(te)

        c = pred.agg(
            F.sum(F.when((F.col("label") == 1.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col("label") == 0.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias("fp"),
            F.sum(F.when((F.col("label") == 1.0) & (F.col("prediction") == 0.0), 1).otherwise(0)).alias("fn"),
        ).first()
        tp, fp, fn = int(c["tp"] or 0), int(c["fp"] or 0), int(c["fn"] or 0)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        auc = float(auc_eval.evaluate(pred))

        model.write().overwrite().save(model_path)
        saved_path = model_path
    except Exception as e:
        print(f"Tag {tag} ignore : {e}")

    rows.append((tag, saved_path, n_pos, precision, recall, f1, auc, tp, fp, fn))
    print(f"{tag}: {n_pos} positifs | P={fmt(precision)} R={fmt(recall)} F1={fmt(f1)} AUC={fmt(auc)}")

# 4. Metriques dans Gold (memes colonnes qu'avant + precision, recall, chemin du modele)
schema = ("tag string, model_path string, nb_train_positifs int, precision double, recall double, "
          "f1 double, auc double, tp long, fp long, fn long")
metrics_df = spark.createDataFrame(rows, schema)
metrics_df.write.mode("overwrite").parquet(f"{GOLD}/model_metrics")

ok = [r for r in rows if r[5] is not None]
if ok:
    print(f"\nMoyenne (macro) sur {len(ok)} tags : "
          f"P={sum(r[3] for r in ok)/len(ok):.4f} R={sum(r[4] for r in ok)/len(ok):.4f} "
          f"F1={sum(r[5] for r in ok)/len(ok):.4f} AUC={sum(r[6] for r in ok)/len(ok):.4f}")
print(f"MODELE OK : {len(ok)} modeles dans {MODELS}, metriques dans {GOLD}/model_metrics")
spark.stop()