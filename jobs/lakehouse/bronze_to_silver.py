"""E : Bronze -> Silver. Parse le JSON, appelle clean_questions (Hedda), upsert Hudi partitionne par mois.
Relancer le job ne cree pas de doublon : la cle est 'id', la version la plus recente (kafka_ts) gagne."""
import sys
sys.path.insert(0, "/opt/project/jobs")
sys.path.insert(0, "/opt/project/jobs/lakehouse")
from pyspark.sql.functions import from_json, col, to_timestamp, date_format, coalesce, lit, regexp_replace, lower, concat_ws
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
from common import get_spark, BRONZE, SILVER
from hudi_utils import write_hudi

# Fonction de nettoyage de Hedda. Tant qu'elle n'est pas livree, on utilise une version temporaire.
try:
    from transform.cleaning import clean_questions
    print("clean_questions : version de Hedda")
except ImportError:
    print("clean_questions : version TEMPORAIRE (en attente de Hedda)")

    def clean_questions(df):
        return (
            df.withColumn("body_text", lower(regexp_replace("body", "<[^>]+>", " ")))
            .withColumn("text", concat_ws(" ", lower(col("title")), col("body_text")))
            .drop("body")
            .dropna(subset=["id"])
            .dropDuplicates(["id"])
        )

# Contrat d'interface : docs/message_schema.md (champs en minuscules)
schema = StructType([
    StructField("id", LongType()),
    StructField("owner_user_id", StringType()),
    StructField("creation_date", StringType()),
    StructField("score", IntegerType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("tags", ArrayType(StringType())),
])

spark = get_spark("bronze_to_silver")

bronze = spark.read.parquet(BRONZE)
parsed = (
    bronze.select(from_json(col("json"), schema).alias("d"), "kafka_ts")
    .select("d.*", "kafka_ts")
)

clean = clean_questions(parsed)

# Partition par mois de creation ; 'inconnu' si la date est absente (une partition ne peut pas etre nulle)
silver_df = clean.withColumn(
    "year_month",
    coalesce(date_format(to_timestamp("creation_date"), "yyyy-MM"), lit("inconnu")),
)

write_hudi(silver_df, "questions_silver", SILVER, key="id", precombine="kafka_ts", partition="year_month")

n = spark.read.format("hudi").load(SILVER).count()
print(f"SILVER OK : {n} questions dans {SILVER}")
spark.stop()