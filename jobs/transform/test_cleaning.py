"""Test unitaire du module de nettoyage cleaning.py (Mohammed Hedda).
Valide :
1. Suppression des balises HTML (<p>, <code>, <a>, etc.)
2. Decodage des entites HTML (&lt;, &gt;, &amp;, &quot;, &#39;)
3. Mise en minuscules du titre et du corps
4. Concaténation titre + corps dans 'text'
5. Nettoyage des tags (suppression vides, minuscules)
6. Filtrage des tags vides
7. Dedoublonnage et suppression des ID nuls
"""

import unittest
import sys
import os

# Ajout du repertoire parent au sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, ArrayType
    from transform.cleaning import clean_questions

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class TestCleaning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not PYSPARK_AVAILABLE:
            raise unittest.SkipTest("PySpark n'est pas installe dans cet environnement local.")
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("UnitTest_Cleaning")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_clean_questions_logic(self):
        schema = StructType([
            StructField("id", LongType()),
            StructField("owner_user_id", StringType()),
            StructField("creation_date", StringType()),
            StructField("score", IntegerType()),
            StructField("title", StringType()),
            StructField("body", StringType()),
            StructField("tags", ArrayType(StringType())),
            StructField("kafka_ts", LongType()),
        ])

        sample_data = [
            # 1. Question normale avec HTML et entites
            (
                101, "u1", "2023-05-10 14:00:00", 5,
                "How to parse JSON in Python?",
                "<p>I have an <code>&lt;issue&gt;</code> with &amp; without quotes.</p>",
                ["Python", "JSON"], 1683727200000
            ),
            # 2. Doublon de l'ID 101 (doit etre elimine)
            (
                101, "u1", "2023-05-10 14:00:00", 5,
                "How to parse JSON in Python? (DUPLICATE)",
                "<p>Duplicate</p>",
                ["python"], 1683727200001
            ),
            # 3. ID nul (doit etre elimine)
            (
                None, "u2", "2023-05-10 15:00:00", 0,
                "Null ID question",
                "<p>Body with null id</p>",
                ["java"], 1683727200002
            ),
            # 4. Tags vides ou espaces (doit etre elimine si filter_empty_tags=True)
            (
                102, "u3", "2023-05-11 10:00:00", 1,
                "Question with empty tags",
                "<p>No tags here</p>",
                ["", "  "], 1683727200003
            ),
            # 5. Question valide avec tags en majuscules et espaces
            (
                103, "u4", "2023-06-01 08:30:00", 10,
                "SPARK SQL Performance",
                "<div>Optimizing <b>JOIN</b> &quot;operations&quot;</div>",
                [" SPARK ", "SPARK-SQL", "Performance "], 1685608200000
            ),
        ]

        df = self.spark.createDataFrame(sample_data, schema)
        cleaned = clean_questions(df)
        rows = {r["id"]: r for r in cleaned.collect()}

        # Verifications
        self.assertEqual(len(rows), 2, "Seules les 2 questions valides (101 et 103) doivent rester.")
        self.assertIn(101, rows)
        self.assertIn(103, rows)
        self.assertNotIn(102, rows, "La question sans tag valide doit etre rejetee.")

        # Verification question 101
        r101 = rows[101]
        self.assertEqual(r101["year_month"], "2023-05")
        self.assertNotIn("<p>", r101["body_text"])
        self.assertNotIn("<code>", r101["body_text"])
        self.assertIn("<issue>", r101["body_text"])
        self.assertIn("&", r101["body_text"])
        self.assertEqual(r101["tags"], ["python", "json"])
        self.assertEqual(r101["n_tags"], 2)
        self.assertIn("how to parse json in python?", r101["text"])

        # Verification question 103
        r103 = rows[103]
        self.assertEqual(r103["year_month"], "2023-06")
        self.assertEqual(r103["tags"], ["spark", "spark-sql", "performance"])
        self.assertEqual(r103["n_tags"], 3)
        self.assertIn("optimizing join \"operations\"", r103["text"])


if __name__ == "__main__":
    unittest.main()
