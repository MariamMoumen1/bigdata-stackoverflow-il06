# Guide de démarrage pour toute l'équipe

À suivre dans l'ordre sur chaque PC. Chacun a son propre Kafka et son propre MinIO : rien n'est partagé
entre les machines, tout est reconstruit localement à partir des deux CSV.

## 0. Ce qui a changé : MinIO

MinIO a supprimé ses images Docker Hub et Quay (septembre 2026). Avec l'ancien compose, `docker compose up`
échoue avec `pull access denied for minio/minio`. Le compose actuel utilise `pgsty/silo` et `amazon/aws-cli`.

Récupérez la version à jour avant tout :

```powershell
git fetch --all
git checkout feature/lakehouse-hudi
git pull
git checkout -b feature/ici mes nom de votre branche
git branch      verifie est ce que etait vraiment cree ou non et commnece votre taches
```

Ne remettez jamais `minio/minio` ni `minio/mc` dans le compose.

## 1. Fichier `.env`

Docker Compose lit uniquement un fichier nommé `.env` (pas `.env.example`).

```powershell
copy .env.example .env
type .env
```

Le contenu doit être exactement :

```
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin
MAX_ROWS=50000
N_TAGS=20
PRODUCER_DELAY=0
```

Erreurs fréquentes :

- `MINIO_USER` avec une autre valeur que `minioadmin` : les identifiants ne correspondent plus à la
  documentation. Si vous l'avez changé, `docker compose down` puis `docker compose up -d`.
- Messages `The "MINIO_USER" variable is not set` : le `.env` n'existe pas ou porte un mauvais nom
  (`.env.txt`). Vérifiez avec `dir -Force .env*`.
- Ne poussez jamais `.env` sur GitHub.

## 2. Données

Placez `Questions.csv` et `Tags.csv` dans `data\` (lien Drive de Mariam, ou Kaggle StackSample).
`Answers.csv` est inutile. Ces fichiers ne sont jamais poussés sur Git.

## 3. Démarrage minimal (sans le build Airflow)

```powershell
docker compose up -d kafka minio minio-init
docker compose ps
docker compose logs minio-init
```

`minio-init` doit afficher `make_bucket: bronze`, `silver`, `gold`.
Console MinIO : http://localhost:9001 (`minioadmin` / `minioadmin`).

## 4. Image Airflow (lourde, une seule fois)

Elle contient Spark, Java et les outils. Téléchargement de plus d'un Go.

```powershell
docker pull apache/airflow:2.9.3-python3.11
docker compose build airflow
```

Si ça échoue :

| Erreur | Cause | Solution |
|---|---|---|
| `TLS handshake timeout`, `name resolution` | Connexion instable | Relancer la même commande, essayer le partage de connexion du téléphone, mettre `"dns": ["8.8.8.8", "1.1.1.1"]` dans Docker Desktop (Settings, Docker Engine) |
| `THESE PACKAGES DO NOT MATCH THE HASHES` (pyspark) | Téléchargement corrompu, pas un piratage | Relancer `docker compose build airflow`. Si ça persiste : télécharger `pyspark-3.5.1.tar.gz` avec `curl.exe -C -` (reprise), vérifier son SHA256, l'installer depuis le Dockerfile en local sans committer ce changement |
| `pull access denied for minio/...` | Ancien compose | Voir section 0 |

## 5. Remplir Kafka

```powershell
docker compose up -d kafka
.\scripts\create_topic.ps1
docker compose run --rm --no-deps airflow python /opt/project/producer/producer.py
```

10 à 15 minutes pour 50 000 questions. Vérification :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic questions
```

La somme des trois partitions doit faire 50 000. Si PowerShell bloque le script :
`powershell -ExecutionPolicy Bypass -File .\scripts\create_topic.ps1`

## 6. Chaîne Bronze, Silver

```powershell
docker compose run --rm --no-deps airflow python /opt/project/jobs/lakehouse/kafka_to_bronze.py
docker compose run --rm --no-deps airflow python /opt/project/jobs/lakehouse/bronze_to_silver.py
```

Résultats attendus : `BRONZE OK : 50000 messages` puis `SILVER OK : 50000 questions`.
Le premier lancement de Spark télécharge ses jars (10 à 20 minutes), ensuite c'est rapide.
Relancer ces commandes ne crée pas de doublon.

## 7. Contrats entre couches

| De | Vers | Contrat |
|---|---|---|
| Mariam | Yassine | Message JSON, champs en minuscules : `id`, `owner_user_id`, `creation_date`, `score`, `title`, `body`, `tags` (`docs/message_schema.md`) |
| Hedda | Yassine | `clean_questions(df)` dans `jobs/transform/cleaning.py` : reçoit les colonnes ci-dessus + `kafka_ts`, rend un DataFrame propre qui garde `id`, `creation_date`, `tags`, `kafka_ts` |
| Yassine | Hedda | `jobs/lakehouse/hudi_utils.py` : `write_hudi(df, table, path, key, precombine, partition)` |
| Hedda | Hasnaoui | Table Gold `tag`, `year_month`, `nombre_questions` (clé composite `tag_month_id`) |

## 8. Pour Hasnaoui (Airflow)

Le DAG doit appeler les jobs de `jobs/lakehouse/` (`kafka_to_bronze.py`, `bronze_to_silver.py`) et non les
anciens `bronze.py` et `silver.py`. Les jobs sont lancés avec `python` dans le conteneur `airflow`.

## 9. Règles Git

- Une branche par personne, pull request vers `main2` relue par un autre membre.
- Un commit par petit changement, avec un message clair, sous son propre compte.
- `git add <fichier>`, jamais `git add .` (risque d'ajouter `.vscode/`, `.env` ou des CSV).
- Jamais de `.env`, de CSV ni de fichiers de plus de 100 Mo sur GitHub.
- `git pull` avant de commencer, `git push` en fin de séance.

## 10. Repartir de zéro

```powershell
docker compose down -v      # supprime aussi les volumes (Kafka, MinIO)
docker compose up -d kafka minio minio-init
```

Après `down -v`, il faut recréer le topic et relancer le producteur (section 5).