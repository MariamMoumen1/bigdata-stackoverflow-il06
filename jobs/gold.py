"""GOLD : tendances par tag et par mois (Spark SQL / DataFrame), ecrites en Parquet pour le dashboard."""
from pyspark.sql.functions import explode, count, avg, round as sround, col
from common import get_spark, SILVER, GOLD

spark = get_spark("gold_trends")

q = spark.read.format("hudi").option("hoodie.metadata.enable", "false").load(SILVER)
exploded = q.filter(col("year_month").isNotNull()).select("id", "score", "year_month", explode("tags").alias("tag"))

trends = exploded.groupBy("tag", "year_month").agg(
    count("*").alias("nb_questions"), sround(avg("score"), 2).alias("avg_score"))
top = exploded.groupBy("tag").agg(
    count("*").alias("nb_questions"), sround(avg("score"), 2).alias("avg_score")
).orderBy(col("nb_questions").desc())

trends.write.mode("overwrite").parquet(f"{GOLD}/tag_trends")
top.write.mode("overwrite").parquet(f"{GOLD}/top_tags")
print("GOLD OK : tag_trends et top_tags ecrits")
spark.stop()
