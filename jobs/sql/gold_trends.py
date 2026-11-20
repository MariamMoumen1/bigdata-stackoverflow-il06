"""Job Gold : Analyse SQL des tendances par tag, classement et croissance mensuelle.
Tache 2 - Responsable : Mohammed Hedda.

Contrats d'interface respectes :
1. Hedda -> Yassien (Gold) : Ecriture de la table Hudi 'tag_trends_hudi' dans MinIO (s3a://gold/tag_trends_hudi)
   avec la cle composite 'tag_month_id', precombine 'updated_at' et partition 'year_month'.
2. Hedda -> Hasnaoui (Dashboard) : Ecriture des tables Parquet 'top_tags' et 'tag_trends'
   dans MinIO (s3a://gold/) lues par dashboard/app.py.
"""

import sys
import os

# Configuration sys.path pour execution autonome ou via Airflow
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if JOBS_DIR not in sys.path:
    sys.path.insert(0, JOBS_DIR)
LAKEHOUSE_DIR = os.path.join(JOBS_DIR, "lakehouse")
if LAKEHOUSE_DIR not in sys.path:
    sys.path.insert(0, LAKEHOUSE_DIR)

from common import get_spark, SILVER, GOLD
from sql.sql_queries import compute_gold_tables

# Import de hudi_utils (developpe par Yassien) pour l'ecriture Hudi
try:
    from hudi_utils import write_hudi
    HUDI_UTILS_AVAILABLE = True
except ImportError:
    HUDI_UTILS_AVAILABLE = False


def run_gold_job():
    """Execute la generation des tables Gold via Spark SQL."""
    spark = get_spark("gold_trends_hedda")
    print("=" * 70)
    print("DEMARRAGE DU JOB GOLD (SPARK SQL) - HEDDA")
    print("=" * 70)

    # 1. Lecture de la table Silver (Hudi ou Parquet)
    print(f"Lecture des donnees Silver depuis {SILVER}...")
    try:
        silver_df = (
            spark.read.format("hudi")
            .option("hoodie.metadata.enable", "false")
            .load(SILVER)
        )
        print("Silver charge avec succes au format Hudi.")
    except Exception as e:
        print(f"Fallback : lecture Parquet directe ({e})...")
        silver_df = spark.read.parquet(SILVER)

    n_silver = silver_df.count()
    print(f"Nombre de questions dans Silver : {n_silver}")

    # 2. Execution des requetes SQL de Mohammed Hedda
    print("\nCalcul des tables Gold via Spark SQL...")
    gold_tables = compute_gold_tables(spark, silver_df)
    top_tags = gold_tables["top_tags"]
    tag_trends = gold_tables["tag_trends"]
    tag_volume = gold_tables["tag_volume"]

    # 3. Ecriture de la table Hudi (Contrat Lakehouse de Yassien)
    hudi_path = f"{GOLD}/tag_trends_hudi"
    if HUDI_UTILS_AVAILABLE:
        try:
            print(f"\n[1/3] Ecriture Hudi dans {hudi_path} (cle=tag_month_id, partition=year_month)...")
            write_hudi(
                df=tag_trends,
                table_name="tag_trends_hudi",
                path=hudi_path,
                key="tag_month_id",
                precombine="updated_at",
                partition="year_month",
                mode="append"
            )
            print("Table Hudi ecrite avec succes.")
        except Exception as e:
            print(f"Avertissement lors de l'ecriture Hudi : {e}")
    else:
        print("hudi_utils non disponible dans l'environnement, ecriture Parquet uniquement.")

    # 4. Ecriture des tables Parquet pour le Dashboard Streamlit (Contrat Hasnaoui)
    top_tags_path = f"{GOLD}/top_tags"
    tag_trends_path = f"{GOLD}/tag_trends"
    growth_path = f"{GOLD}/tag_growth"

    print(f"\n[2/3] Ecriture Parquet top_tags dans {top_tags_path}...")
    top_tags.write.mode("overwrite").parquet(top_tags_path)

    print(f"[3/3] Ecriture Parquet tag_trends dans {tag_trends_path}...")
    tag_trends.write.mode("overwrite").parquet(tag_trends_path)

    print(f"Ecriture Parquet tag_growth dans {growth_path}...")
    tag_trends.select(
        "tag", "year_month", "nombre_questions", "prev_month_questions",
        "growth_abs", "growth_rate_pct", "trend_status"
    ).write.mode("overwrite").parquet(growth_path)

    # 5. Affichage des resultats pour la demonstration
    print("\n" + "=" * 70)
    print("RESULTATS DU GOLD (REQUETES SPARK SQL)")
    print("=" * 70)

    print("\n--- TOP 10 DES TAGS GLOBAUX (Classement SQL avec DENSE_RANK) ---")
    top_tags.show(10, truncate=False)

    print("\n--- EXEMPLE DE TENDANCES ET CROISSANCE MENSUELLE (Window LAG) ---")
    tag_trends.filter("prev_month_questions > 0") \
        .select("tag", "year_month", "nombre_questions", "growth_abs", "growth_rate_pct", "trend_status", "rank_in_month") \
        .show(10, truncate=False)

    print(f"\nGOLD OK : Tables Gold generees avec succes dans {GOLD}.")
    spark.stop()


if __name__ == "__main__":
    run_gold_job()
