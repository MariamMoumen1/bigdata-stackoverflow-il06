# IL-06 : Analyse des questions Stack Overflow (Big Data)

Pipeline : **CSV -> Kafka -> Bronze (Parquet) -> Silver (Hudi) -> Gold (tendances) -> modèle MLlib -> dashboard**, orchestré par **Airflow** (DAG mensuel), stockage **MinIO** (S3). Toute la plateforme démarre avec Docker Compose.

> Projet non testé de bout en bout sous Docker au moment de sa génération : il peut y avoir de petits bugs au premier lancement. Copiez l'erreur exacte pour la corriger.

## Équipe et répartition

Une couche par membre, la couche Spark est partagée entre Mariam et Hedda.

| Membre | Couche | Dossiers | Tâche commune pilotée |
|---|---|---|---|
| **Mariam Moumen** | Sources et ingestion Kafka (couche 1 et 2) ; Spark : One-vs-Rest, évaluation, streaming (tâches 4, 5, 6) | `producer/`, `data/`, `jobs/ml/train.py`, `jobs/ml/evaluate.py`, `jobs/streaming/` | `docker-compose.yml`, `run_all.sh`, cahier des charges, rapport final, dépôt Git |
| **Meskaoui Yassien** | Data Lake MinIO et Lakehouse Hudi (Bronze, Silver, Gold) | `scripts/`, `jobs/lakehouse/` | aucune (couche lourde) |
| **Mohammed Hedda** | Spark : nettoyage, requêtes SQL, features TF-IDF (tâches 1, 2, 3) | `jobs/transform/`, `jobs/sql/`, `jobs/ml/features.py` | aucune (couche la plus lourde) |
| **Mohammed Hasnaoui** | Orchestration Airflow et tableau de bord | `dags/`, `airflow/`, `dashboard/` | README et support de présentation |

### Les 6 tâches Spark

| # | Tâche | Qui | Dossier |
|---|---|---|---|
| 1 | Fonction de nettoyage `clean_questions(df)` (HTML, minuscules, titre + corps, tags en liste, doublons) | Hedda | `jobs/transform/` |
| 2 | Requêtes Spark SQL : volume par tag et par mois (Gold), classement, croissance mensuelle | Hedda | `jobs/sql/` |
| 3 | Pipeline MLlib : Tokenizer, StopWordsRemover, TF-IDF, tags les plus fréquents | Hedda | `jobs/ml/features.py` |
| 4 | One-vs-Rest multi-étiquettes : un classifieur binaire (régression logistique) par tag, seuil de probabilité | Mariam | `jobs/ml/train.py` |
| 5 | Évaluation (précision, rappel, F1) et sauvegarde du modèle dans MinIO | Mariam | `jobs/ml/evaluate.py` |
| 6 | Job Structured Streaming : lit Kafka, prédit les tags en continu | Mariam | `jobs/streaming/` |

### Qui passe quoi à qui (contrats d'interface)

| De | Vers | Ce qui est convenu |
|---|---|---|
| Mariam (producteur) | Yassien (Bronze) | Message JSON du topic `questions`, schéma dans `docs/message_schema.md` |
| Hedda (nettoyage) | Yassien (Silver) | Fonction `clean_questions(df)` : DataFrame Bronze en entrée, DataFrame propre en sortie ; clé Silver : `id` |
| Hedda (SQL) | Yassien (Gold) | Table Gold `tag`, `mois`, `nombre_questions`, écrite avec `hudi_utils.py` |
| Gold | Hasnaoui (dashboard) | Table Gold exposée par un moteur SQL (Spark Thrift Server ou copie PostgreSQL, décision en phase 2) |
| Hedda (features) | Mariam (entraînement, streaming) | Features TF-IDF, liste des tags retenus, jeu train/test et pipeline TF-IDF ajusté, déposés dans MinIO |
| Mariam (modèle) | Hasnaoui (DAG) | Script de réentraînement lançable par `spark-submit`, modèle écrit au même endroit dans MinIO |

## Préparer le projet chez soi (chaque membre)

**Chaque membre doit faire cette préparation sur son propre PC.** Les données ne sont pas dans Git : le fichier `Questions.csv` pèse 1,9 Go et GitHub refuse tout fichier de plus de 100 Mo. Le dépôt contient seulement le code, le schéma, les scripts et les réglages ; chaque PC a son propre Kafka, qu'il faut remplir.

```
 CSV (Drive) ──► data\ ──► producteur (1 commande) ──► Kafka de SON PC
```

### 1. Cloner et se placer sur la bonne branche

```powershell
git clone https://github.com/MariamMoumen1/bigdata-stackoverflow-il06
cd bigdata-stackoverflow-il06
git checkout main2
```

La couche 1 (Kafka et producteur) est sur la branche `mariam/kafka-producer`, dont `main2` est une copie identique. Tant que la pull request n'est pas fusionnée dans `main`, un clone arrive sur un `main` sans ce travail : utilisez `main2`. Une fois la fusion faite, `main` suffit.

### 2. Récupérer les données

Placer `Questions.csv` et `Tags.csv` directement dans le dossier `data\` (pas dans un sous-dossier) :

- soit depuis le lien Drive partagé par Mariam : **[LIEN_DRIVE_A_REMPLACER]**,
- soit depuis Kaggle : https://www.kaggle.com/datasets/stackoverflow/stacksample (`Answers.csv` n'est pas utilisé).

Ces fichiers sont ignorés par Git (`.gitignore`) : ne les poussez jamais.

### 3. Créer son fichier `.env`

```powershell
Copy-Item .env.example .env
```

`.env.example` est déjà réglé à 50 000 questions et 20 tags. Le fichier `.env` reste local (ignoré par Git).

### 4. Remplir son Kafka local (3 commandes)

Docker Desktop doit être démarré. Depuis le dossier du projet :

```powershell
docker compose up -d kafka
.\scripts\create_topic.ps1
docker run --rm --network bigdata-stackoverflow-il06_default -v "${PWD}:/opt/project" -e KAFKA_BOOTSTRAP=kafka:9092 -e MAX_ROWS=50000 -e PRODUCER_DELAY=0 python:3.11-slim bash -c "pip install -q pandas kafka-python && python /opt/project/producer/producer.py"
```

La dernière commande est à écrire **sur une seule ligne**. Compter 10 à 15 minutes : le script lit un CSV de 1,9 Go à travers le partage de fichiers Docker. Résultat attendu : `PRODUCER OK : 50000 messages envoyes dans le topic 'questions'`. Tout le monde obtient les mêmes 50 000 questions (échantillon reproductible, `random_state=42`).

Si Windows affiche « Docker wants to access ... Do you want to share it? », cliquer sur **Yes**.

### 5. Vérifier

```powershell
.\scripts\check_offsets.ps1     # la somme des offsets doit faire 50000
.\scripts\read_sample.ps1       # affiche 3 questions en JSON
.\scripts\describe_topic.ps1    # partitions et réplication du topic
```

### 6. Créer sa branche de travail

```powershell
git checkout -b feature/<ta-couche>
git config user.name "Ton Nom"
git config user.email "ton-email-github"
```

Branches prévues : `feature/lakehouse-hudi` (Yassien), `feature/spark-features` (Hedda), `feature/spark-ml-streaming` (Mariam, Spark), `feature/airflow-dashboard` (Hasnaoui).

## Prérequis

- Docker Desktop avec **6 Go de mémoire minimum** (Settings > Resources). Avec 2 Go seule la couche Kafka tourne.
- Git. Sous Windows : PowerShell pour les scripts `.ps1`, Git Bash ou WSL pour `run_all.sh`.
- Sur certains PC (ordinateurs d'école ou de travail), une stratégie Windows bloque les fichiers natifs de pandas : lancer alors le producteur dans Docker, comme ci-dessus, plutôt qu'avec le Python de Windows.

## Lancement de la plateforme complète

```bash
./run_all.sh
```

Le script démarre les services, crée le topic Kafka `questions` (3 partitions), envoie les questions et déclenche le DAG. Sans `Questions.csv` et `Tags.csv`, il génère de fausses données (plan B).

Ou étape par étape :

```bash
docker compose up -d --build
docker compose exec airflow python /opt/project/producer/producer.py
docker compose exec airflow airflow dags unpause stackoverflow_monthly
docker compose exec airflow airflow dags trigger stackoverflow_monthly
```

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Dashboard | http://localhost:8501 | - |

Le dashboard du squelette actuel est en Streamlit ; le guide prévoit Superset, choix à confirmer en phase 2 (Hasnaoui, Yassien, Mariam).

## Configuration (fichier `.env`)

- `MAX_ROWS` : nombre de questions envoyées dans Kafka (50000 pour la version finale, 500 pour un test rapide, 20000 pour la démo).
- `N_TAGS` : nombre de tags modélisés (20 ; le guide demande 20 à 50).
- `PRODUCER_DELAY` : délai en secondes entre deux messages (0 pour charger vite, 0.01 pour simuler un flux visible).
- `MINIO_USER`, `MINIO_PASSWORD` : identifiants MinIO.

Après modification de `.env` : `docker compose up -d` puis relancer le producteur et le DAG.

## Message Kafka

Un message par question, topic `questions`. Détail dans `docs/message_schema.md`.

| Champ | Type | Remarque |
|---|---|---|
| `id` | entier | identifiant de la question |
| `owner_user_id` | texte ou null | |
| `creation_date` | texte | `YYYY-MM-DD HH:MM:SS` |
| `score` | entier ou null | |
| `title` | texte | |
| `body` | texte HTML | brut, tronqué à 5000 caractères |
| `tags` | liste de textes | liste vide possible : à filtrer au nettoyage |

## Structure

- `docker-compose.yml` : Kafka (KRaft), MinIO, Airflow (avec Spark en mode local), dashboard.
- `producer/` : `producer.py` (CSV -> topic Kafka `questions`), `Dockerfile`, `requirements.txt`.
- `jobs/` : `bronze.py`, `silver.py` (upsert Hudi), `gold.py` (tendances), `train_model.py` ; à terme découpés en `transform/`, `sql/`, `ml/`, `streaming/`, `lakehouse/` selon la répartition.
- `dags/stackoverflow_dag.py` : DAG mensuel, 4 tâches.
- `dashboard/app.py` : tableau de bord.
- `scripts/` : `create_topic.ps1`, `check_offsets.ps1`, `read_sample.ps1`, `describe_topic.ps1`, `generate_fake_data.py` (données de secours).
- `docs/` : `message_schema.md`, `couche1_kafka.md`, rapport et captures de chaque couche.

## Problèmes fréquents

- **`docker compose` répond « empty compose file »** : vous n'êtes pas dans le dossier du projet.
- **`error during connect ... EOF`** : le moteur Docker redémarre ; attendre « Engine running » et relancer.
- **Le producteur ne se connecte pas** : depuis un conteneur, utiliser `kafka:9092` ; depuis le PC, `localhost:29092`.
- **Port déjà utilisé** : fermer l'autre programme ou changer le port dans `docker-compose.yml`.
- **Services qui redémarrent / tâche Spark tuée (OOM)** : augmenter la mémoire de Docker Desktop (fichier `.wslconfig` : `memory=6GB`), ou baisser `MAX_ROWS`.
- **Première tâche très lente** : Spark télécharge ses dépendances (Hudi, Kafka, S3), une seule fois.
- **Logs** : `docker compose logs airflow`, ou les logs de chaque tâche dans l'interface Airflow.
- **Repartir de zéro** : `docker compose down -v` puis `./run_all.sh`.
- **Linux** : si erreur de droits sur le dossier monté, lancer `sudo chmod -R a+rw .` dans le projet.

## Git (travail à quatre)

- Le dépôt est celui de Mariam ; les trois autres sont collaborateurs (Settings, Collaborators) et acceptent l'invitation.
- `main` contient uniquement du code qui fonctionne et se démontre. Une branche par couche, fusion par pull request relue par un autre membre.
- Chacun règle `git config user.name` et `user.email` avec son propre compte : la contribution individuelle est notée d'après l'historique Git. Aucun commit au nom d'un autre.
- Commits petits et fréquents, avec un message clair (« Ajout du producteur Kafka avec débit réglable »). `git pull` avant de travailler, `git push` en fin de séance.
- Ne jamais pousser : `.env`, `Questions.csv`, `Tags.csv` ni aucun fichier de plus de 100 Mo (déjà dans `.gitignore`).
- En fin de projet : capture de GitHub, Insights, Contributors pour le rapport.

## Démonstration (ordre prévu)

1. Mariam lance `docker compose up` et montre que tous les services sont démarrés.
2. Mariam lance le producteur et montre les questions qui arrivent dans le topic.
3. Yassien montre MinIO et la table Silver, puis réinjecte une question modifiée : pas de doublon.
4. Hedda exécute la requête Spark SQL du Gold ; Mariam montre l'évaluation du modèle et le streaming qui prédit les tags d'une question écrite en direct.
5. Hasnaoui déclenche le DAG de démonstration, puis montre le tableau de bord mis à jour.

Plan B : échantillon réduit de données pour un démarrage rapide, et vidéo de la démo enregistrée la veille.
