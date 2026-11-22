"""Test unitaire du module de features MLlib features.py (Mohammed Hedda).
Valide :
1. Extraction des top N tags (get_top_tags)
2. Construction du pipeline TF-IDF (RegexTokenizer, StopWordsRemover, HashingTF, IDF)
3. Ajustement (fit) et transformation du texte en vecteurs numeriques de features
"""

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType
    from ml.features import get_top_tags, build_feature_pipeline

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class TestMLFeatures(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not PYSPARK_AVAILABLE:
            raise unittest.SkipTest("PySpark n'est pas installe dans cet environnement local.")
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("UnitTest_MLFeatures")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_get_top_tags(self):
        schema = StructType([
            StructField("id", LongType()),
            StructField("tags", ArrayType(StringType())),
        ])
        data = [
            (1, ["python", "pandas"]),
            (2, ["python", "numpy"]),
            (3, ["python", "sql"]),
            (4, ["java", "spring"]),
            (5, ["java"]),
        ]
        df = self.spark.createDataFrame(data, schema)
        top2 = get_top_tags(df, n_tags=2)
        self.assertEqual(len(top2), 2)
        self.assertEqual(top2[0], "python", "Le tag le plus frequent doit etre python (3 occurrences)")
        self.assertEqual(top2[1], "java", "Le 2eme tag doit etre java (2 occurrences)")

    def test_feature_pipeline_transformation(self):
        schema = StructType([
            StructField("id", LongType()),
            StructField("text", StringType()),
        ])
        data = [
            (1, "how to filter a dataframe with python and pandas"),
            (2, "optimizing spark sql performance with broadcast joins"),
            (3, "java spring boot dependency injection error"),
            (4, "python data structures and algorithms using lists"),
        ]
        df = self.spark.createDataFrame(data, schema)

        pipeline = build_feature_pipeline(
            input_col="text",
            output_col="features",
            num_features=128,
            min_doc_freq=1
        )
        model = pipeline.fit(df)
        transformed = model.transform(df)

        self.assertIn("tokens", transformed.columns)
        self.assertIn("clean_tokens", transformed.columns)
        self.assertIn("tf", transformed.columns)
        self.assertIn("features", transformed.columns)

        first_row = transformed.first()
        self.assertEqual(first_row["features"].size, 128)
        self.assertGreater(len(first_row["features"].indices), 0, "Le vecteur doit contenir des indices non-nuls")


if __name__ == "__main__":
    unittest.main()
