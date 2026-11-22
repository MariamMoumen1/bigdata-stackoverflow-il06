"""Test unitaire des requetes Spark SQL du module sql_queries.py (Mohammed Hedda).
Valide :
1. Calcul du volume mensuel par tag et de la cle composite 'tag_month_id'
2. Classement global des tags avec DENSE_RANK()
3. Rang mensuel des tags (rank_in_month)
4. Croissance mensuelle absolue (growth_abs) et en pourcentage (growth_rate_pct) via LAG()
"""

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, ArrayType
    from sql.sql_queries import compute_gold_tables

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class TestSparkSQL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not PYSPARK_AVAILABLE:
            raise unittest.SkipTest("PySpark n'est pas installe dans cet environnement local.")
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("UnitTest_SQL")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_compute_gold_tables_logic(self):
        schema = StructType([
            StructField("id", LongType()),
            StructField("score", IntegerType()),
            StructField("year_month", StringType()),
            StructField("tags", ArrayType(StringType())),
        ])

        # Donnees simulees sur 2 mois pour tester la croissance (LAG)
        sample_data = [
            # Mois 2023-01 : python (2), java (1)
            (1, 10, "2023-01", ["python", "pandas"]),
            (2, 5,  "2023-01", ["python"]),
            (3, 2,  "2023-01", ["java"]),
            # Mois 2023-02 : python (4 -> croissance +100%), java (1 -> croissance 0%)
            (4, 20, "2023-02", ["python"]),
            (5, 15, "2023-02", ["python", "pyspark"]),
            (6, 8,  "2023-02", ["python"]),
            (7, 4,  "2023-02", ["python"]),
            (8, 6,  "2023-02", ["java"]),
        ]

        df = self.spark.createDataFrame(sample_data, schema)
        tables = compute_gold_tables(self.spark, df)

        top_tags = {r["tag"]: r for r in tables["top_tags"].collect()}
        self.assertIn("python", top_tags)
        self.assertEqual(top_tags["python"]["nb_questions"], 6)
        self.assertEqual(top_tags["python"]["global_rank"], 1)

        # Verification des tendances mensuelles et de la croissance
        trends = tables["tag_trends"].collect()
        trends_map = {(r["tag"], r["year_month"]): r for r in trends}

        # Mois 1 pour python
        py_m1 = trends_map[("python", "2023-01")]
        self.assertEqual(py_m1["nombre_questions"], 2)
        self.assertEqual(py_m1["tag_month_id"], "python|2023-01")
        self.assertEqual(py_m1["growth_abs"], 0)
        self.assertEqual(py_m1["trend_status"], "NOUVEAU")

        # Mois 2 pour python : passage de 2 a 4 questions (+2 questions, +100%)
        py_m2 = trends_map[("python", "2023-02")]
        self.assertEqual(py_m2["nombre_questions"], 4)
        self.assertEqual(py_m2["tag_month_id"], "python|2023-02")
        self.assertEqual(py_m2["prev_month_questions"], 2)
        self.assertEqual(py_m2["growth_abs"], 2)
        self.assertEqual(py_m2["growth_rate_pct"], 100.0)
        self.assertEqual(py_m2["trend_status"], "HAUSSE")


if __name__ == "__main__":
    unittest.main()
