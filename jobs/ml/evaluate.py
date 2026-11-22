"""Tache 5 : evaluation (precision, rappel, F1, AUC) et sauvegarde des metriques dans MinIO.
Responsable : Mariam Moumen.
Recharge depuis MinIO les modeles sauvegardes par ml/train.py (preuve de la sauvegarde),
les evalue sur le jeu de test, et ecrit s3a://gold/model_metrics.
Precision, rappel et F1 sont calcules par tag a partir de tp, fp, fn (pas les metriques
ponderees de Spark, gonflees pour les tags rares).
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from common import get_spark, GOLD

MODELS_INDEX = f"{GOLD}/models_index"
METRICS = f"{GOLD}/model_metrics"


def _fmt(x):
    return "n/a" if x is None else f"{x:.4f}"


def evaluate_models(spark, test_df):
    """Evalue chaque modele sur le jeu de test et sauvegarde les metriques."""
    index = spark.read.parquet(MODELS_INDEX).orderBy("rank").collect()
    auc_eval = BinaryClassificationEvaluator(metricName="areaUnderROC")
    print(f"\nEvaluation de {len(index)} modeles sur le jeu de test")

    rows = []
    for r in index:
        tag, path = r["tag"], r["model_path"]
        precision = recall = f1 = auc = tp = fp = fn = None
        try:
            if path is None:
                raise ValueError("pas de modele pour ce tag")
            model = LogisticRegressionModel.load(path)
            te = test_df.withColumn("label", F.array_contains("tags", tag).cast("double"))
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
        except Exception as e:
            print(f"Tag {tag} ignore : {e}")
        rows.append((tag, path, r["nb_train_positifs"], r["threshold"], precision, recall, f1, auc, tp, fp, fn))
        print(f"{tag}: {r['nb_train_positifs']} positifs | P={_fmt(precision)} R={_fmt(recall)} F1={_fmt(f1)} AUC={_fmt(auc)}")

    schema = ("tag string, model_path string, nb_train_positifs int, threshold double, precision double, "
              "recall double, f1 double, auc double, tp long, fp long, fn long")
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(METRICS)

    ok = [x for x in rows if x[6] is not None]
    if ok:
        n = len(ok)
        print(f"\nMoyenne (macro) sur {n} tags : P={sum(x[4] for x in ok)/n:.4f} R={sum(x[5] for x in ok)/n:.4f} "
              f"F1={sum(x[6] for x in ok)/n:.4f} AUC={sum(x[7] for x in ok)/n:.4f}")
    print(f"EVALUATION OK : metriques ecrites dans {METRICS}")
    return rows


def main():
    """Lancement autonome : lit le jeu de test de Hedda deja sauvegarde dans MinIO."""
    spark = get_spark("evaluate_models")
    test_df = spark.read.parquet(f"{GOLD}/ml/test_features").cache()
    evaluate_models(spark, test_df)
    spark.stop()


if __name__ == "__main__":
    main()