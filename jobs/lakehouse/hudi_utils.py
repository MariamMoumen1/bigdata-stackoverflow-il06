"""Module commun d'ecriture Hudi (Silver et Gold). Utilise par Yassine (Silver) et Hedda (Gold)."""


def hudi_options(table_name, key, precombine, partition=None, table_type="COPY_ON_WRITE", operation="upsert"):
    """Construit les options Hudi. partition=None -> table non partitionnee."""
    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.recordkey.field": key,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.operation": operation,
        "hoodie.upsert.shuffle.parallelism": "4",
        "hoodie.insert.shuffle.parallelism": "4",
        "hoodie.metadata.enable": "false",
    }
    if partition:
        opts["hoodie.datasource.write.partitionpath.field"] = partition
        opts["hoodie.datasource.write.hive_style_partitioning"] = "true"
    else:
        opts["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
    return opts


def write_hudi(df, table_name, path, key, precombine, partition=None, mode="append", **extra):
    """Ecrit df dans une table Hudi (upsert par defaut). Retourne le chemin ecrit."""
    opts = hudi_options(table_name, key, precombine, partition)
    opts.update(extra)
    df.write.format("hudi").options(**opts).mode(mode).save(path)
    return path


def read_hudi(spark, path):
    """Lit une table Hudi."""
    return spark.read.format("hudi").load(path)