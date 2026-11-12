"""MODELE : prediction des tags a partir du texte. TF-IDF + une regression logistique par tag (One-vs-Rest multi-etiquettes)."""
import os
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import explode, concat_ws, coalesce, lit, col, array_contains, count
from common import get_spark, SILVER, GOLD

N_TAGS = int(os.getenv("N_TAGS", "10"))
spark = get_spark("train_model")

q = spark.read.format("hudi").option("hoodie.metadata.enable", "false").load(SILVER)
top_tags = [r["tag"] for r in
            q.select(explode("tags").alias("tag")).groupBy("tag").agg(count("*").alias("n"))
             .orderBy(col("n").desc()).limit(N_TAGS).collect()]
print("Tags modelises :", top_tags)

df = q.select("id", concat_ws(" ", coalesce(col("title"), lit("")), coalesce(col("body_text"), lit(""))).alias("text"), "tags")
train, test = df.randomSplit([0.8, 0.2], seed=42)

prep = Pipeline(stages=[
    RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\W+", minTokenLength=2),
    StopWordsRemover(inputCol="tokens", outputCol="clean"),
    HashingTF(inputCol="clean", outputCol="tf", numFeatures=1 << 14),
    IDF(inputCol="tf", outputCol="features"),
]).fit(train)
train_f = prep.transform(train).cache()
test_f = prep.transform(test).cache()

auc_eval = BinaryClassificationEvaluator(metricName="areaUnderROC")
f1_eval = MulticlassClassificationEvaluator(metricName="f1")

rows = []
for tag in top_tags:
    tr = train_f.withColumn("label", array_contains("tags", tag).cast("double"))
    te = test_f.withColumn("label", array_contains("tags", tag).cast("double"))
    try:
        model = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.01).fit(tr)
        pred = model.transform(te)
        auc, f1 = float(auc_eval.evaluate(pred)), float(f1_eval.evaluate(pred))
    except Exception as e:
        print(f"Tag {tag} ignore : {e}")
        auc, f1 = None, None
    rows.append((tag, int(tr.filter("label = 1").count()), auc, f1))
    print(f"{tag}: AUC={auc} F1={f1}")

spark.createDataFrame(rows, "tag string, nb_train_positifs int, auc double, f1 double") \
    .write.mode("overwrite").parquet(f"{GOLD}/model_metrics")
print("MODELE OK : metriques ecrites dans gold/model_metrics")
spark.stop()
