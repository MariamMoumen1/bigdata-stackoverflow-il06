"""Verification (pas le vrai Gold, qui est ecrit par Hedda) : ecrit un Gold d'essai avec hudi_utils."""
import sys
sys.path.insert(0, "/opt/project/jobs")
sys.path.insert(0, "/opt/project/jobs/lakehouse")
from pyspark.sql.functions import explode, col, count, concat_ws, current_timestamp
from common import get_spark, SILVER, GOLD
from hudi_utils import write_hudi, read_hudi

spark = get_spark("gold_check")
silver = read_hudi(spark, SILVER)

gold = (
    silver.select("year_month", explode("tags").alias("tag"))
    .groupBy("tag", "year_month").agg(count("*").alias("nombre_questions"))
    .withColumn("tag_month_id", concat_ws("|", col("tag"), col("year_month")))
    .withColumn("updated_at", current_timestamp())
)

PATH = GOLD + "/_check_tag_month"
write_hudi(gold, "gold_check", PATH, key="tag_month_id", precombine="updated_at", partition="year_month")

res = read_hudi(spark, PATH)
print(f"GOLD CHECK OK : {res.count()} lignes (tag, mois)")
res.orderBy(col("nombre_questions").desc()).select("tag", "year_month", "nombre_questions").show(5)
spark.stop()