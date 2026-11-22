# Couche 2 : Data Lake MinIO et Lakehouse Hudi

Responsable : Meskaoui Yassien. Cette couche reçoit les messages du topic Kafka `questions`, les stocke
en trois zones (Bronze, Silver, Gold) et maintient des tables Hudi sans doublon.

## Vue d'ensemble

```
Kafka (topic questions) -> Bronze (brut) -> Silver (nettoyé, Hudi) -> Gold (agrégé, Hudi) -> Superset
```

## Zones

| Zone | Bucket | Contenu | Format | Écrit par |
|---|---|---|---|---|
| Bronze | `bronze/questions` | Messages Kafka bruts (colonne `json`) + métadonnées Kafka (topic, partition, offset, horodatage) | Parquet | `kafka_to_bronze.py` |
| Silver | `silver/questions` | Questions nettoyées, sans doublon, partitionnées par mois de création | Hudi (Copy-on-Write) | `bronze_to_silver.py` |
| Gold | `gold/` | Volume de questions par tag et par mois | Hudi | couche Spark (Hedda), avec `hudi_utils.py` |

Pourquoi garder Bronze brut : si une erreur de nettoyage est découverte plus tard, on refait Silver
depuis la donnée d'origine, qui n'a jamais été modifiée.

## Stockage objet

MinIO n'est plus distribué sur Docker Hub ni sur Quay (septembre 2026) : `docker pull minio/minio`
échoue avec `pull access denied`. Le projet utilise donc :

- `pgsty/silo` : fork de MinIO compatible S3 (mêmes variables `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`),
- `amazon/aws-cli` : service `minio-init` qui crée les buckets `bronze`, `silver` et `gold`.

Console : http://localhost:9001 (identifiants du `.env`, par défaut `minioadmin` / `minioadmin`).
API S3 : http://localhost:9000.

## Versions figées

| Composant | Version |
|---|---|
| Spark (PySpark) | 3.5.1 |
| Java | 17 |
| Hudi | `hudi-spark3.5-bundle_2.12:0.15.0` |
| Hadoop-AWS | 3.3.4 |
| AWS SDK bundle | 1.12.262 |
| Spark-Kafka | `spark-sql-kafka-0-10_2.12:3.5.1` |

Ces versions sont définies dans `jobs/common.py` (`PACKAGES`). Hadoop-AWS doit correspondre à la version
de Hadoop embarquée dans Spark 3.5.1 (3.3.4), sinon `NoSuchMethodError`. Validé par `hudi_smoke_test.py`.

## Table Hudi Silver

- Clé d'enregistrement : `id`
- Champ de pré-combinaison : `kafka_ts` (si deux versions d'une même question arrivent, la plus récente gagne)
- Partition : `year_month` (`yyyy-MM`, valeur `inconnu` si `creation_date` est absente)
- Opération : `upsert`
- Type : **Copy-on-Write**. Le Silver est lu souvent (Spark SQL, dashboard) et mis à jour par lots mensuels.
  Copy-on-Write réécrit le fichier concerné à chaque mise à jour : l'écriture est un peu plus lente, mais la
  lecture est simple et rapide. Merge-on-Read serait préférable pour des écritures très fréquentes.

## Gold

Le Gold est écrit par la couche Spark (Hedda) avec `hudi_utils.write_hudi`. Une ligne représente un couple
(tag, mois). Il n'existe pas de colonne unique naturelle : il faut créer une clé composite, par exemple
`tag_month_id = tag|mois`, puis l'utiliser comme `key`.

Exemple fonctionnel : `jobs/lakehouse/gold_check.py` (écrit dans `gold/_check_tag_month`, table d'essai).

Colonnes de l'exemple : `tag`, `year_month`, `nombre_questions`, `tag_month_id` (clé), `updated_at` (pré-combinaison).

## Jobs

| Fichier | Rôle |
|---|---|
| `jobs/lakehouse/hudi_utils.py` | Module commun d'écriture Hudi (Silver et Gold) : `hudi_options`, `write_hudi`, `read_hudi` |
| `jobs/lakehouse/kafka_to_bronze.py` | Kafka vers Bronze, en Structured Streaming `availableNow` avec checkpoint |
| `jobs/lakehouse/bronze_to_silver.py` | Parse le JSON, appelle `clean_questions`, upsert Hudi partitionné par mois |
| `jobs/lakehouse/hudi_demo.py` | Démonstration : upsert, requête incrémentale, time travel |
| `jobs/lakehouse/gold_check.py` | Vérification : écrit un Gold d'essai avec `hudi_utils` |
| `jobs/lakehouse/hudi_smoke_test.py` | Test des versions Spark / Hudi / Hadoop-AWS |
| `jobs/lakehouse/test_hudi_utils.py` | Test du module commun (upsert + partition) |

### Choix techniques

- **Pas de doublon dans Bronze** : le checkpoint Spark retient les offsets déjà lus. Relancer
  `kafka_to_bronze.py` ne relit que les nouveaux messages.
- **Pas de doublon dans Silver** : l'upsert Hudi reconnaît l'`id` et met la ligne à jour.
- **Contrat d'interface** : les champs du message JSON sont en minuscules (`docs/message_schema.md`).
- **Nettoyage** : `bronze_to_silver.py` utilise `clean_questions(df)` de Hedda (`jobs/transform/cleaning.py`).
  Tant que ce fichier n'existe pas, une version temporaire est utilisée (le job l'indique à l'écran).

## Les trois fonctions Hudi démontrées (`hudi_demo.py`)

1. **Upsert** : la question est réinjectée avec un score modifié, le nombre de lignes reste 50 000.
2. **Requête incrémentale** : seule la ligne modifiée depuis le commit précédent est lue.
3. **Historique (time travel)** : la table est relue à l'état d'un ancien commit, l'ancien score réapparaît.

Après la démo, la question modifiée garde sa nouvelle valeur. Pour revenir à l'état d'origine, supprimer
`silver/questions` dans la console MinIO et relancer `bronze_to_silver.py`.

## Lancement des jobs

Prérequis : `.env` créé à partir de `.env.example`, `Questions.csv` et `Tags.csv` dans `data/`, topic rempli
(voir `docs/guide_demarrage_equipe.md`).

```powershell
docker compose up -d kafka minio minio-init
docker compose run --rm --no-deps airflow python /opt/project/jobs/lakehouse/kafka_to_bronze.py
docker compose run --rm --no-deps airflow python /opt/project/jobs/lakehouse/bronze_to_silver.py
docker compose run --rm --no-deps airflow python /opt/project/jobs/lakehouse/hudi_demo.py
```

Le premier lancement de Spark télécharge les jars (Hudi, Hadoop-AWS, Kafka) dans le volume `ivy_cache` : il est
long, les suivants sont rapides.

## Preuves de réussite

| Preuve | Fichier de capture |
|---|---|
| Les 3 buckets dans la console MinIO | `docs/captures/01_buckets.png` |
| `.hoodie` et fichiers Parquet dans `silver/_smoke_test` | `docs/captures/02_smoke_test.png` |
| `BRONZE OK : 50000 messages` | `docs/captures/03_bronze_ok.png` |
| `bronze` : `questions/` et `_checkpoints/` | `docs/captures/04_bronze_bucket.png` |
| `SILVER OK : 50000 questions` (2e exécution : toujours 50000) | `docs/captures/05_silver_ok.png` |
| `silver/questions` : `.hoodie` et partitions `year_month=...` | `docs/captures/06_silver_bucket.png` |
| Sortie de `hudi_demo.py` (upsert, incrémentale, time travel) | `docs/captures/07_hudi_demo.png` |
| `GOLD CHECK OK` | `docs/captures/08_gold_check.png` |

## À savoir expliquer (jury)

- **Bronze, Silver, Gold** : brut, nettoyé sans doublon, agrégé prêt à l'emploi.
- **Upsert** : mise à jour si la clé existe, insertion sinon, donc jamais de doublon.
- **Clé et champ de pré-combinaison** : la clé identifie la ligne, le champ de pré-combinaison départage deux
  versions (la plus grande valeur gagne).
- **Copy-on-Write contre Merge-on-Read** : réécriture du fichier à chaque mise à jour contre fusion différée.
- **Partition** : sous-dossier par mois, une requête sur un mois ne lit que ce dossier.
- **Checkpoint** : mémoire de Spark sur ce qui a déjà été lu dans Kafka.