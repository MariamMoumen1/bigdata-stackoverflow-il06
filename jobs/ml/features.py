"""Pipeline MLlib : Extraction des features textuelles TF-IDF et selection des tags.
Tache 3 - Responsable : Mohammed Hedda.

Contrat d'interface :
- Hedda -> Mariam (entrainement One-vs-Rest et streaming) :
  1. Pipeline TF-IDF ajuste (fitted PipelineModel) sauvegarde dans MinIO (s3a://models/tfidf_pipeline).
  2. Liste des tags frequents modelises sauvegardee dans MinIO (s3a://gold/ml/top_tags.parquet).
  3. Jeux train/test avec colonnes 'features' et 'tags' sauvegardes dans MinIO (s3a://gold/ml/train_features, test_features).
  Mariam peut ainsi directement entrainer ses classifieurs ou charger le pipeline pour le streaming.
"""

import sys
import os
import json
from typing import List, Tuple, Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    count,
    concat_ws,
    coalesce,
    lit,
    size,
    array_contains,
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    HashingTF,
    IDF,
)

# Configuration sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)

from common import get_spark, SILVER, GOLD

# Mots vides complementaires specifiques au developpement web / code
CUSTOM_STOP_WORDS = [
    "http", "https", "com", "org", "www", "net",
    "question", "using", "code", "want", "like", "trying",
    "example", "error", "problem", "way", "need", "know",
    "help", "issue", "thanks", "thank", "work", "works",
]


def get_top_tags(df: DataFrame, n_tags: int = 20) -> List[str]:
    """Identifie les N tags les plus frequents dans le jeu de donnees.

    Parametres :
    - df : DataFrame Silver avec colonne 'tags' (tableau de chaines).
    - n_tags : nombre de tags a selectionner (defaut 20).

    Retourne :
    - Liste ordonnee des tags les plus frequents.
    """
    top_rows = (
        df.filter(col("tags").isNotNull())
        .select(explode("tags").alias("tag"))
        .groupBy("tag")
        .agg(count("*").alias("frequency"))
        .orderBy(col("frequency").desc())
        .limit(n_tags)
        .collect()
    )
    return [r["tag"] for r in top_rows]


def build_feature_pipeline(
    input_col: str = "text",
    output_col: str = "features",
    num_features: int = 1 << 14,
    min_doc_freq: int = 2,
    custom_stopwords: List[str] = None
) -> Pipeline:
    """Construit le pipeline Spark MLlib d'extraction de caracteristiques TF-IDF.

    Etapes du pipeline :
    1. RegexTokenizer : decoupe le texte en tokens de mots (minuscules, longueur >= 2).
    2. StopWordsRemover : elimine les mots vides standard en anglais + mots specifiques.
    3. HashingTF : projette les tokens dans un espace vectoriel de dimension fixe (termes frequents).
    4. IDF : calcule la frequence inverse de document pour ponderer l'importance des termes.

    Parametres :
    - input_col : nom de la colonne texte nettoyee.
    - output_col : nom de la colonne finale de vecteurs.
    - num_features : nombre de dimensions du vecteur TF (defaut 16384).
    - min_doc_freq : nombre minimum de documents pour qu'un terme soit considere dans l'IDF.
    """
    stopwords = StopWordsRemover.loadDefaultStopWords("english")
    if custom_stopwords:
        stopwords = list(set(stopwords + custom_stopwords))

    tokenizer = RegexTokenizer(
        inputCol=input_col,
        outputCol="tokens",
        pattern="\\W+",
        minTokenLength=2
    )

    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="clean_tokens",
        stopWords=stopwords
    )

    hashing_tf = HashingTF(
        inputCol="clean_tokens",
        outputCol="tf",
        numFeatures=num_features
    )

    idf = IDF(
        inputCol="tf",
        outputCol=output_col,
        minDocFreq=min_doc_freq
    )

    return Pipeline(stages=[tokenizer, remover, hashing_tf, idf])


def prepare_ml_data(
    df: DataFrame,
    n_tags: int = 20,
    train_ratio: float = 0.8,
    seed: int = 42
) -> Tuple[PipelineModel, List[str], DataFrame, DataFrame]:
    """Prepare les donnees pour le Machine Learning.

    1. Selectionne les N tags les plus frequents.
    2. S'assure que la colonne unifiee 'text' est presente.
    3. Separe le jeu de donnees en train (80%) et test (20%).
    4. Ajuste (fit) le pipeline TF-IDF sur le jeu d'entrainement uniquement.
    5. Transforme train et test en generant les colonnes 'features'.

    Retourne :
    - (fitted_pipeline, top_tags, train_features, test_features)
    """
    top_tags = get_top_tags(df, n_tags=n_tags)
    print(f"Top {len(top_tags)} tags retenus pour la modelisation : {top_tags}")

    # S'assurer de la presence de la colonne 'text'
    if "text" not in df.columns:
        df = df.withColumn(
            "text",
            concat_ws(" ", coalesce(col("title"), lit("")), coalesce(col("body_text"), lit("")))
        )

    # Filtrer les questions qui possedent au moins un tag parmi les tags cibles
    # Cela garantit que chaque observation a un interet pour l'apprentissage multi-etiquette
    condition = array_contains("tags", top_tags[0])
    for tag in top_tags[1:]:
        condition = condition | array_contains("tags", tag)
    df_filtered = df.filter(condition).select("id", "text", "tags")

    print(f"Questions contenant au moins un des top tags : {df_filtered.count()}")

    # Partitionnement Train (80%) / Test (20%) reproductible
    train_df, test_df = df_filtered.randomSplit([train_ratio, 1.0 - train_ratio], seed=seed)

    # Construction et ajustement du pipeline TF-IDF
    pipeline = build_feature_pipeline(
        input_col="text",
        output_col="features",
        num_features=1 << 14,
        custom_stopwords=CUSTOM_STOP_WORDS
    )
    print("Ajustement (fit) du pipeline TF-IDF sur le jeu train...")
    fitted_pipeline = pipeline.fit(train_df)

    # Transformation des deux jeux de donnees
    print("Transformation de train et test...")
    train_features = fitted_pipeline.transform(train_df).select("id", "tags", "features")
    test_features = fitted_pipeline.transform(test_df).select("id", "tags", "features")

    return fitted_pipeline, top_tags, train_features, test_features


def save_features_and_pipeline(
    spark: SparkSession,
    fitted_pipeline: PipelineModel,
    top_tags: List[str],
    train_features: DataFrame,
    test_features: DataFrame,
    output_base: str = f"{GOLD}/ml",
    model_export_path: str = "s3a://models/tfidf_pipeline"
) -> Dict[str, str]:
    """Sauvegarde tous les artefacts ML dans MinIO pour Mariam (taches 4, 5, 6).

    Artefacts generes :
    1. Pipeline TF-IDF ajuste : {model_export_path} et {output_base}/tfidf_pipeline
    2. Tags retenus : {output_base}/top_tags
    3. Features train : {output_base}/train_features
    4. Features test  : {output_base}/test_features
    """
    paths = {}

    # 1. Sauvegarde du modele de pipeline TF-IDF
    print(f"Sauvegarde du pipeline TF-IDF ajuste dans {model_export_path}...")
    try:
        fitted_pipeline.write().overwrite().save(model_export_path)
        paths["pipeline_model"] = model_export_path
    except Exception as e:
        print(f"Avertissement export s3a://models ({e}), sauvegarde locale...")
        fallback_path = f"{output_base}/tfidf_pipeline"
        fitted_pipeline.write().overwrite().save(fallback_path)
        paths["pipeline_model"] = fallback_path

    # 2. Sauvegarde des tags cibles
    tags_df = spark.createDataFrame([(i + 1, tag) for i, tag in enumerate(top_tags)], ["rank", "tag"])
    tags_path = f"{output_base}/top_tags"
    tags_df.write.mode("overwrite").parquet(tags_path)
    paths["top_tags"] = tags_path

    # 3. Sauvegarde des jeux de donnees vectorises
    train_path = f"{output_base}/train_features"
    test_path = f"{output_base}/test_features"

    print(f"Sauvegarde des features Train dans {train_path}...")
    train_features.write.mode("overwrite").parquet(train_path)
    paths["train_features"] = train_path

    print(f"Sauvegarde des features Test dans {test_path}...")
    test_features.write.mode("overwrite").parquet(test_path)
    paths["test_features"] = test_path

    return paths


def load_feature_pipeline(path: str = "s3a://models/tfidf_pipeline") -> PipelineModel:
    """Charge un pipeline TF-IDF ajuste depuis MinIO (utilise par Mariam en streaming)."""
    return PipelineModel.load(path)


def run_features_job():
    """Point d'entree d'execution du job ML Features de Mohammed Hedda."""
    spark = get_spark("ml_features_hedda")
    print("=" * 70)
    print("DEMARRAGE DU PIPELINE DE FEATURES TF-IDF (MLlib) - HEDDA")
    print("=" * 70)

    n_tags = int(os.getenv("N_TAGS", "20"))
    print(f"Configuration N_TAGS = {n_tags}")

    # Lecture de Silver
    print(f"Lecture des questions depuis {SILVER}...")
    try:
        silver_df = (
            spark.read.format("hudi")
            .option("hoodie.metadata.enable", "false")
            .load(SILVER)
        )
    except Exception as e:
        print(f"Lecture Parquet ({e})...")
        silver_df = spark.read.parquet(SILVER)

    # Preparation et vectorisation
    fitted_pipeline, top_tags, train_features, test_features = prepare_ml_data(
        silver_df,
        n_tags=n_tags,
        train_ratio=0.8,
        seed=42
    )

    n_train = train_features.count()
    n_test = test_features.count()
    print(f"Jeu Train : {n_train} observations vectorisees")
    print(f"Jeu Test  : {n_test} observations vectorisees")

    # Exemple de vecteur TF-IDF
    print("\nExemple de vecteur TF-IDF extrait (premier document train) :")
    sample_row = train_features.select("id", "tags", "features").first()
    if sample_row:
        print(f"Question ID : {sample_row['id']}")
        print(f"Tags        : {sample_row['tags']}")
        print(f"Features    : Dimension {sample_row['features'].size}, non-nuls {len(sample_row['features'].indices)}")

    # Sauvegarde des artefacts pour Mariam
    paths = save_features_and_pipeline(
        spark=spark,
        fitted_pipeline=fitted_pipeline,
        top_tags=top_tags,
        train_features=train_features,
        test_features=test_features
    )

    print("\n" + "=" * 70)
    print("ARTEFACTS DEPOSES DANS MINIO POUR MARIAM (TRAIN & STREAMING) :")
    for k, v in paths.items():
        print(f"  - {k} : {v}")
    print("=" * 70)
    print("FEATURES OK : Pipeline TF-IDF et datasets prets avec succes.")
    spark.stop()


if __name__ == "__main__":
    run_features_job()
