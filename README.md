# IL-06 : Analyse des questions Stack Overflow (Big Data)

Pipeline : **CSV -> Kafka -> Bronze (Parquet) -> Silver (Hudi) -> Gold (tendances) -> modele MLlib -> dashboard**, orchestre par **Airflow** (DAG mensuel), stockage **MinIO** (S3).

> Projet non teste sous Docker au moment de sa generation : il peut y avoir de petits bugs au premier lancement. Copiez l'erreur exacte pour la corriger.

## Prerequis
- Docker Desktop (6 Go de memoire minimum, Settings > Resources) et Git. Sous Windows : Git Bash ou WSL.
- Donnees StackSample (https://www.kaggle.com/datasets/stackoverflow/stacksample) : placer `Questions.csv` et `Tags.csv` dans `data/`. Sans elles, `run_all.sh` genere de fausses donnees.

## Lancement
```bash
./run_all.sh
```
Ou etape par etape :
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

## Configuration (fichier `.env`)
- `MAX_ROWS` : nombre de questions envoyees dans Kafka (defaut 20000).
- `N_TAGS` : nombre de tags modelises (defaut 10).
- `PRODUCER_DELAY` : delai en secondes entre deux messages (ex. 0.01 pour simuler un flux).

Apres modification de `.env` : `docker compose up -d` puis relancer le producteur et le DAG.

## Structure
- `docker-compose.yml` : Kafka (KRaft), MinIO, Airflow (avec Spark en mode local), dashboard.
- `producer/producer.py` : CSV -> topic Kafka `questions`.
- `jobs/` : `bronze.py`, `silver.py` (upsert Hudi), `gold.py` (tendances), `train_model.py` (TF-IDF + regression logistique par tag).
- `dags/stackoverflow_dag.py` : DAG mensuel, 4 taches.
- `dashboard/app.py` : Streamlit.
- `scripts/generate_fake_data.py` : donnees de secours.
- `docs/PLAN_2_JOURS.md` : planning.

## Problemes frequents
- **Port deja utilise** : fermez l'autre programme ou changez le port dans `docker-compose.yml`.
- **Services qui redemarrent / tache Spark tuee (OOM)** : augmentez la memoire de Docker Desktop, ou baissez `MAX_ROWS`.
- **Premiere tache tres lente** : Spark telecharge ses dependances (Hudi, Kafka, S3), une seule fois.
- **Logs** : `docker compose logs airflow`, ou les logs de chaque tache dans l'interface Airflow.
- **Repartir de zero** : `docker compose down -v` puis `./run_all.sh`.
- **Linux** : si erreur de droits sur le dossier monte, lancez `sudo chmod -R a+rw .` dans le projet.

## Git (travail a deux)
Une branche par personne (`feat/...`), Pull Request relue par l'autre, merge dans `main`. Chacun commite avec son propre compte. Ne jamais committer les CSV (deja dans `.gitignore`).
