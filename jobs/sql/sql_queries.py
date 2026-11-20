"""Requetes Spark SQL et fonctions de calcul pour la couche Gold.
Tache 2 - Responsable : Mohammed Hedda.

Contrats d'interface :
- Hedda -> Yassien (Gold)   : Table Gold avec cle composite 'tag_month_id' et 'updated_at'
                              ecrite au format Hudi partitionnee par mois.
- Hedda -> Hasnaoui (Dash)  : Tables 'tag_trends' et 'top_tags' compatibles Streamlit.
"""

from typing import Dict
from pyspark.sql import DataFrame, SparkSession


SQL_TAG_MONTHLY_VOLUME = """
SELECT
    tag,
    year_month,
    COUNT(*) AS nombre_questions,
    COUNT(*) AS nb_questions,
    ROUND(AVG(score), 2) AS avg_score,
    MIN(score) AS min_score,
    MAX(score) AS max_score,
    CONCAT(tag, '|', year_month) AS tag_month_id,
    CURRENT_TIMESTAMP() AS updated_at
FROM (
    SELECT id, score, year_month, EXPLODE(tags) AS tag
    FROM questions_silver
    WHERE year_month IS NOT NULL 
      AND year_month != 'inconnu'
      AND tags IS NOT NULL
)
GROUP BY tag, year_month
"""

SQL_TOP_TAGS_RANKING = """
SELECT
    tag,
    COUNT(*) AS nb_questions,
    COUNT(*) AS nombre_questions,
    ROUND(AVG(score), 2) AS avg_score,
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS global_rank
FROM (
    SELECT id, score, EXPLODE(tags) AS tag
    FROM questions_silver
    WHERE tags IS NOT NULL
)
GROUP BY tag
ORDER BY nb_questions DESC
"""

SQL_TAG_RANKINGS = SQL_TOP_TAGS_RANKING

SQL_TAG_MONTHLY_GROWTH = """
WITH monthly_base AS (
    SELECT
        tag,
        year_month,
        COUNT(*) AS nombre_questions,
        ROUND(AVG(score), 2) AS avg_score,
        CONCAT(tag, '|', year_month) AS tag_month_id,
        CURRENT_TIMESTAMP() AS updated_at,
        DENSE_RANK() OVER (PARTITION BY year_month ORDER BY COUNT(*) DESC) AS rank_in_month
    FROM (
        SELECT id, score, year_month, EXPLODE(tags) AS tag
        FROM questions_silver
        WHERE year_month IS NOT NULL 
          AND year_month != 'inconnu'
          AND tags IS NOT NULL
    )
    GROUP BY tag, year_month
),
with_lag AS (
    SELECT
        tag,
        year_month,
        nombre_questions,
        avg_score,
        rank_in_month,
        tag_month_id,
        updated_at,
        LAG(nombre_questions, 1) OVER (
            PARTITION BY tag 
            ORDER BY year_month
        ) AS prev_month_questions
    FROM monthly_base
)
SELECT
    tag,
    year_month,
    nombre_questions,
    nombre_questions AS nb_questions,
    avg_score,
    rank_in_month,
    tag_month_id,
    updated_at,
    COALESCE(prev_month_questions, 0) AS prev_month_questions,
    CASE 
        WHEN prev_month_questions IS NULL THEN 0
        ELSE (nombre_questions - prev_month_questions)
    END AS growth_abs,
    CASE 
        WHEN prev_month_questions IS NULL OR prev_month_questions = 0 THEN 0.0
        ELSE ROUND(((nombre_questions - prev_month_questions) * 100.0) / prev_month_questions, 2)
    END AS growth_rate_pct,
    CASE
        WHEN prev_month_questions IS NULL THEN 'NOUVEAU'
        WHEN nombre_questions > prev_month_questions THEN 'HAUSSE'
        WHEN nombre_questions < prev_month_questions THEN 'BAISSE'
        ELSE 'STABLE'
    END AS trend_status
FROM with_lag
"""


def compute_gold_tables(spark: SparkSession, silver_df: DataFrame) -> Dict[str, DataFrame]:
    """Execute les requetes Spark SQL sur les donnees Silver et retourne les tables Gold.

    Parametres :
    - spark : session Spark active
    - silver_df : DataFrame Silver contenant les questions nettoyees

    Retourne un dictionnaire avec :
    - 'top_tags' : Classement global des tags (avec global_rank)
    - 'tag_trends' : Volume mensuel, classement du mois et croissance (croissance %, croissance absolue)
    - 'tag_volume' : Volume brut agrege par (tag, year_month)
    """
    silver_df.createOrReplaceTempView("questions_silver")

    top_tags = spark.sql(SQL_TOP_TAGS_RANKING)
    tag_volume = spark.sql(SQL_TAG_MONTHLY_VOLUME)
    tag_trends = spark.sql(SQL_TAG_MONTHLY_GROWTH)

    return {
        "top_tags": top_tags,
        "tag_volume": tag_volume,
        "tag_trends": tag_trends,
    }
