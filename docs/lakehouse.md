# Couche 2 : Data Lake MinIO et Lakehouse Hudi

## Zones
| Zone | Bucket | Contenu | Format |
|---|---|---|---|
| Bronze | `bronze/questions` | Messages Kafka bruts (colonne `json`) + metadonnees Kafka (partition, offset, horodatage) | Parquet |
| Silver | `silver/questions` | Questions nettoyees, sans doublon, partitionnees par mois de creation | Hudi (Copy-on-Write) |
| Gold | `gold/` | Volume de questions par tag et par mois (ecrit par la couche Spark) | Hudi |

## Stockage objet
MinIO n'est plus distribue sur Docker Hub ni Quay (septembre 2026). Le projet utilise `pgsty/silo`,
un fork compatible S3, et `amazon/aws-cli` pour creer les buckets.

## Table Hudi Silver
- Cle d'enregistrement : `id`
- Champ de pre-combinaison : `kafka_ts` (la version la plus recente gagne)
- Partition : `year_month` (`yyyy-MM`, `inconnu` si la date est absente)
- Type : Copy-on-Write. Choisi car le Silver est lu souvent (Spark SQL, dashboard) et mis a jour par lots mensuels.

## Jobs
| Fichier | Role |
|---|---|
| `jobs/lakehouse/hudi_utils.py` | Module commun d'ecriture Hudi (Silver et Gold) |
| `jobs/lakehouse/kafka_to_bronze.py` | Kafka vers Bronze, checkpoint pour eviter les doublons |
| `jobs/lakehouse/bronze_to_silver.py` | Parse le JSON, nettoyage, upsert Hudi |
| `jobs/lakehouse/hudi_demo.py` | Demo : upsert, requete incrementale, time travel |

## Preuves
(captures : buckets, `.hoodie`, 50 000 lignes apres deux executions, sortie de hudi_demo.py)