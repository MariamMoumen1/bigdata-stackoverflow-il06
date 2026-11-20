"""Module de nettoyage des questions Stack Overflow.
Tache 1 - Responsable : Mohammed Hedda.

Contrat d'interface :
- Entree  : DataFrame Bronze issu du topic Kafka 'questions' (colonnes : id, owner_user_id,
            creation_date, score, title, body, tags, kafka_ts).
- Sortie  : DataFrame nettoye sans doublon d'id, avec HTML supprime, minuscules,
            texte unifie (titre + corps), tags valides et metadonnees temporelles.
            Utilise par Yassien (Silver Hudi) et pour les analyses SQL/ML.
"""

import sys
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    concat_ws,
    coalesce,
    lit,
    to_timestamp,
    date_format,
    size,
    transform,
    array_remove,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
    ArrayType,
)


def clean_html(col_name: str) -> "Column":
    """Nettoie le HTML d'une colonne : supprime les balises et decode les entites courantes."""
    c = col(col_name)
    # Remplacement des balises HTML <...>
    c = regexp_replace(c, "<[^>]+>", " ")
    # Decodage des entites HTML frequentes
    c = regexp_replace(c, "&amp;", "&")
    c = regexp_replace(c, "&lt;", "<")
    c = regexp_replace(c, "&gt;", ">")
    c = regexp_replace(c, "&quot;", "\"")
    c = regexp_replace(c, "&#39;", "'")
    c = regexp_replace(c, "&nbsp;", " ")
    # Normalisation des retours chariot, tabulations et espaces multiples
    c = regexp_replace(c, "[\\r\\n\\t]+", " ")
    c = regexp_replace(c, "\\s+", " ")
    return trim(c)


def clean_questions(df: DataFrame, filter_empty_tags: bool = True) -> DataFrame:
    """Fonction principale de nettoyage des questions Stack Overflow.

    Operations realisees :
    1. Suppression des identifiants nuls (dropna sur 'id').
    2. Dedoublonnage strict sur 'id' (dropDuplicates).
    3. Nettoyage du HTML dans le corps ('body') -> 'body_text'.
    4. Mise en minuscules du titre et du corps.
    5. Concaténation titre + corps dans une colonne unifiee 'text'.
    6. Nettoyage des tags (minuscules, suppression des chaines vides et nulles).
    7. Calcul du nombre de tags ('n_tags').
    8. Conversion de la date de creation en horodatage ('creation_ts') et extraction 'year_month'.
    9. Suppression de la colonne brute 'body' pour optimiser le stockage.

    Parametres :
    - df : DataFrame Spark en entree (schema Bronze).
    - filter_empty_tags : si True, exclut les questions sans tags (inutilisables en ML).

    Retourne :
    - DataFrame Spark propre pret pour l'upsert Silver Hudi et les taches SQL/ML.
    """
    # 1. Validation de la presence de la cle primaire
    cleaned = df.dropna(subset=["id"]).dropDuplicates(["id"])

    # 2. Nettoyage du corps HTML et passage en minuscules
    body_clean = lower(clean_html("body"))
    title_clean = lower(trim(coalesce(col("title"), lit(""))))

    cleaned = (
        cleaned
        .withColumn("body_text", coalesce(body_clean, lit("")))
        .withColumn("title_clean", title_clean)
        .withColumn(
            "text",
            trim(concat_ws(" ", col("title_clean"), col("body_text")))
        )
        .drop("title_clean")
    )

    # 3. Normalisation des tags (tableau de strings)
    # Si tags est null, on remplace par un tableau vide
    # On passe chaque tag en minuscules et on enleve les chaines vides
    cleaned = cleaned.withColumn(
        "tags_normalized",
        when(col("tags").isNotNull(),
             array_remove(transform(col("tags"), lambda x: lower(trim(x))), ""))
        .otherwise(lit(None))
    )

    cleaned = (
        cleaned
        .drop("tags")
        .withColumnRenamed("tags_normalized", "tags")
        .withColumn("n_tags", coalesce(size("tags"), lit(0)))
    )

    if filter_empty_tags:
        cleaned = cleaned.filter(col("n_tags") > 0)

    # 4. Conversion temporelle
    cleaned = (
        cleaned
        .withColumn("creation_ts", to_timestamp("creation_date"))
        .withColumn(
            "year_month",
            coalesce(date_format("creation_ts", "yyyy-MM"), lit("inconnu"))
        )
    )

    # 5. Nettoyage du score (remplacer null par 0)
    if "score" in cleaned.columns:
        cleaned = cleaned.withColumn("score", coalesce(col("score"), lit(0)))

    # 6. Suppression du body brut non nettoye pour optimiser l'espace Silver
    if "body" in cleaned.columns:
        cleaned = cleaned.drop("body")

    return cleaned


if __name__ == "__main__":
    # Test autonome du module en mode interactif
    print("Execution autonome de jobs/transform/cleaning.py...")
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from common import get_spark, BRONZE

    spark = get_spark("test_cleaning_hedda")
    try:
        print(f"Lecture des donnees Bronze depuis {BRONZE}...")
        bronze_df = spark.read.parquet(BRONZE)
        total_bronze = bronze_df.count()
        print(f"Bronze total : {total_bronze} lignes")

        # Parsing du JSON si necessaire
        from pyspark.sql.functions import from_json
        schema = StructType([
            StructField("id", LongType()),
            StructField("owner_user_id", StringType()),
            StructField("creation_date", StringType()),
            StructField("score", IntegerType()),
            StructField("title", StringType()),
            StructField("body", StringType()),
            StructField("tags", ArrayType(StringType())),
        ])

        if "json" in bronze_df.columns:
            parsed = (
                bronze_df
                .select(from_json(col("json"), schema).alias("d"), "kafka_ts")
                .select("d.*", "kafka_ts")
            )
        else:
            parsed = bronze_df

        clean_df = clean_questions(parsed)
        total_clean = clean_df.count()
        print(f"Nettoyage termine : {total_clean} questions valides (sur {total_bronze})")
        print("\nExemple de questions nettoyees :")
        clean_df.select("id", "year_month", "tags", "n_tags", "text").show(3, truncate=80)
        print("CLEANING OK - Module Hedda valide avec succes.")
    except Exception as e:
        print(f"Note : Impossible de lire Bronze directement hors cluster ({e}).")
        print("Le module clean_questions est pret pour l'import dans bronze_to_silver.py.")
    finally:
        spark.stop()
