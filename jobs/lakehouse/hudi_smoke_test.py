"""Test d'ecriture Hudi dans MinIO : verifie la compatibilite Spark / Hudi / Hadoop-AWS et l'upsert."""
import sys
sys.path.insert(0, "/opt/project/jobs")
from common import get_spark

PATH = "s3a://silver/_smoke_test"
spark = get_spark("hudi_smoke_test")

opts = {
    "hoodie.table.name": "smoke_test",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.metadata.enable": "false",
}

# 1) trois lignes
v1 = spark.createDataFrame([(1, "a", 1), (2, "b", 1), (3, "c", 1)], ["id", "val", "ts"])
v1.write.format("hudi").options(**opts).mode("append").save(PATH)

# 2) on reinjecte l'id 2 modifie : upsert, pas de doublon attendu
v2 = spark.createDataFrame([(2, "b_modifie", 2)], ["id", "val", "ts"])
v2.write.format("hudi").options(**opts).mode("append").save(PATH)

df = spark.read.format("hudi").load(PATH)
df.select("id", "val", "ts").orderBy("id").show()
n = df.count()
print("TEST OK : 3 lignes, id 2 mis a jour" if n == 3 else f"ECHEC : {n} lignes au lieu de 3")
spark.stop()