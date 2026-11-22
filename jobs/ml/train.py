"""Tache 4 : entrainement One-vs-Rest multi-etiquettes (Mariam Moumen).
Un classifieur binaire (regression logistique) par tag : "ce tag est present ou absent".
Le seuil de probabilite (PREDICTION_THRESHOLD, defaut 0.5) decide si le tag est retenu.
Les exemples positifs sont ponderes car les tags sont rares.
Les modeles sont sauvegardes dans MinIO : s3a://gold/models/lr_00, lr_01...
Un index (gold/models_index) relie chaque tag a son modele.
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

from pyspark.ml.classification import LogisticRegression
from pyspark.sql import functions as F
from common import get_spark, GOLD

THRESHOLD = float(os.getenv("PREDICTION_THRESHOLD", "0.5"))
POS_WEIGHT_MAX = float(os.getenv("POS_WEIGHT_MAX", "20"))
MODELS = f"{GOLD}/models"
MODELS_INDEX = f"{GOLD}/models_index"


def train_models(train_df, top_tags):
    """Entraine un modele par tag et sauvegarde les modeles dans MinIO."""
    spark = train_df.sparkSession
    n_train = train_df.count()
    print(f"\nEntrainement de {len(top_tags)} modeles (un par tag), train={n_train} lignes, seuil={THRESHOLD}")

    index_rows = []
    for i, tag in enumerate(top_tags):
        # Index dans le chemin : des tags comme "c#", "c++" ou ".net" casseraient un chemin S3
        model_path = f"{MODELS}/lr_{i:02d}"
        tr = train_df.withColumn("label", F.array_contains("tags", tag).cast("double"))
        n_pos = int(tr.filter("label = 1").count())
        saved, weight = None, None
        try:
            if n_pos == 0:
                raise ValueError("aucun exemple positif dans le train")
            weight = float(min((n_train - n_pos) / n_pos, POS_WEIGHT_MAX))
            tr = tr.withColumn("weight", F.when(F.col("label") == 1.0, F.lit(weight)).otherwise(F.lit(1.0)))
            model = LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight",
                                       maxIter=20, regParam=0.01, threshold=THRESHOLD).fit(tr)
            model.write().overwrite().save(model_path)
            saved = model_path
            print(f"[{i:02d}] {tag}: {n_pos} positifs, poids={weight:.1f} -> {model_path}")
        except Exception as e:
            print(f"Tag {tag} ignore : {e}")
        index_rows.append((i, tag, saved, n_pos, weight, THRESHOLD))

    schema = "rank int, tag string, model_path string, nb_train_positifs int, weight double, threshold double"
    spark.createDataFrame(index_rows, schema).write.mode("overwrite").parquet(MODELS_INDEX)
    ok = sum(1 for r in index_rows if r[2] is not None)
    print(f"TRAIN OK : {ok} modeles dans {MODELS}, index dans {MODELS_INDEX}")
    return index_rows


def main():
    """Lancement autonome : lit les features de Hedda deja sauvegardees dans MinIO."""
    spark = get_spark("train_one_vs_rest")
    train_df = spark.read.parquet(f"{GOLD}/ml/train_features").cache()
    top_tags = [r["tag"] for r in spark.read.parquet(f"{GOLD}/ml/top_tags").orderBy("rank").collect()]
    train_models(train_df, top_tags)
    spark.stop()


if __name__ == "__main__":
    main()