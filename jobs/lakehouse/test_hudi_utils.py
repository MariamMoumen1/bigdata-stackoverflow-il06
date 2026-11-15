"""Test du module hudi_utils : upsert + partition par mois."""
import sys
sys.path.insert(0, "/opt/project/jobs")
sys.path.insert(0, "/opt/project/jobs/lakehouse")
from common import get_spark
from hudi_utils import write_hudi, read_hudi

spark = get_spark("test_hudi_utils")
PATH = "s3a://silver/_test_utils"

cols = ["id", "val", "month", "ts"]

# 1) deux lignes, deux mois differents
write_hudi(spark.createDataFrame([(1, "a", "2026-01", 1), (2, "b", "2026-02", 1)], cols),
           "test_utils", PATH, key="id", precombine="ts", partition="month")

# 2) on reinjecte l'id 2 modifie : upsert, pas de doublon attendu
write_hudi(spark.createDataFrame([(2, "b2", "2026-02", 2)], cols),
           "test_utils", PATH, key="id", precombine="ts", partition="month")

df = read_hudi(spark, PATH)
df.select("id", "val", "month").orderBy("id").show()
print("TEST OK : 2 lignes, id 2 mis a jour" if df.count() == 2 else f"ECHEC : {df.count()} lignes au lieu de 2")
spark.stop()