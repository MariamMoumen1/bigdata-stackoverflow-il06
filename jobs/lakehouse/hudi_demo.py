"""F : demo des 3 fonctions Hudi demandees par le guide : upsert, requete incrementale, historique (time travel)."""
import sys
sys.path.insert(0, "/opt/project/jobs")
sys.path.insert(0, "/opt/project/jobs/lakehouse")
from pyspark.sql.functions import col, current_timestamp
from common import get_spark, SILVER
from hudi_utils import write_hudi, read_hudi

spark = get_spark("hudi_demo")

df = read_hudi(spark, SILVER)
avant = df.count()
q = df.orderBy("id").first()
qid = q["id"]
commit_avant = df.agg({"_hoodie_commit_time": "max"}).collect()[0][0]
print(f"\n=== Etat initial : {avant} lignes ; question id={qid}, score={q['score']} ; dernier commit={commit_avant}")

# 1) UPSERT : on reinjecte la meme question avec un score modifie
meta = [c for c in df.columns if c.startswith("_hoodie")]
modif = (
    df.filter(col("id") == qid).drop(*meta)
    .withColumn("score", col("score") + 1000)
    .withColumn("kafka_ts", current_timestamp())
)
write_hudi(modif, "questions_silver", SILVER, key="id", precombine="kafka_ts", partition="year_month")

apres = read_hudi(spark, SILVER)
print(f"\n=== 1) UPSERT : {apres.count()} lignes (avant : {avant}) -> pas de doublon")
apres.filter(col("id") == qid).select("id", "score").show()

# 2) REQUETE INCREMENTALE : seulement ce qui a change depuis le commit precedent
inc = (
    spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", commit_avant)
    .load(SILVER)
)
print(f"=== 2) INCREMENTALE : {inc.count()} ligne(s) modifiee(s) depuis le commit {commit_avant}")
inc.select("id", "score").show()

# 3) HISTORIQUE (time travel) : la table telle qu'elle etait avant la modification
ancien = spark.read.format("hudi").option("as.of.instant", commit_avant).load(SILVER)
print("=== 3) TIME TRAVEL : la question a l'etat du commit", commit_avant)
ancien.filter(col("id") == qid).select("id", "score").show()

spark.stop()