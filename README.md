# IL-06 : Analyse des questions Stack Overflow (Big Data)

Pipeline : **CSV -> Kafka -> Bronze (Parquet) -> Silver (Hudi) -> Gold (tendances) -> modele MLlib -> dashboard**, orchestre par **Airflow** (DAG mensuel), stockage **MinIO** (S3).

> Pipeline execute sous Docker de bout en bout, job par job, sur 50 000 questions et 20 tags. Ce qui reste a tester (DAG complet, dashboard) est indique dans le guide pour Hasnaoui, plus bas. En cas de bug au premier lancement, copiez l'erreur exacte pour la corriger.

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


---

## Guide pour Mohammed Hasnaoui : Airflow et tableau de bord

Ta partie : orchestration Airflow (dags/, airflow/) et tableau de bord (dashboard/).
Ce guide va du clonage du projet jusqu'a ce que tu dois faire.

### 1. Ce qui est deja fait

Le pipeline fonctionne, lance a la main, un job apres l'autre, sur **50 000 questions et 20 tags** (les valeurs du projet) :

    CSV -> Kafka -> Bronze -> Silver (Hudi) -> Gold -> modeles par tag -> predictions en continu

Les parties de Hedda (Bronze, Silver, Gold, features) et de Mariam (taches 4, 5 et 6 : entrainement, evaluation, streaming) sont terminees et testees. Resultats obtenus avec 50 000 questions et 20 tags (27 178 lignes d'entrainement) : precision 0.47, rappel 0.54, F1 0.50, AUC 0.88 en moyenne. Detail dans documentation/resultats_finaux_50000q.md.

**Ce qui n'a pas ete teste, et qui est ta partie :**
- Le DAG Airflow n'a pas ete declenche en entier : Mariam n'a lance que les jobs un par un.
- Le dashboard n'a jamais tourne chez Mariam : l'image n'a pas pu etre construite (reseau trop lent).

### 2. Cloner et demarrer

Prerequis : Docker Desktop avec 6 Go de memoire minimum (Settings > Resources), Git, environ 15 Go de disque. Clone hors d'un dossier OneDrive (OneDrive verrouille des fichiers et fait echouer Git et Docker).

    git clone https://github.com/MariamMoumen1/bigdata-stackoverflow-il06.git
    cd bigdata-stackoverflow-il06
    git checkout mariam/spark-ml-streaming
    git checkout -b hasnaoui/airflow-dashboard

Donnees : place Questions.csv et Tags.csv (Kaggle, StackSample) dans data/. Ne les commite jamais.

Fichier .env : copie .env.example (ou cree-le) avec les valeurs du projet. Enregistre-le en UTF-8 sans BOM (un BOM rend la premiere variable illisible pour Docker).

    MINIO_USER=minioadmin
    MINIO_PASSWORD=minioadmin
    MAX_ROWS=50000
    N_TAGS=20
    PRODUCER_DELAY=0

MAX_ROWS=50000 et N_TAGS=20 sont les valeurs du projet : tous les resultats de Mariam (taches 4, 5 et 6) ont ete obtenus avec elles. Le pipeline complet prend alors plusieurs dizaines de minutes (l'entrainement des 20 modeles est l'etape la plus longue) et demande 6 Go de memoire pour Docker.

Test rapide facultatif, pour verifier l'installation en quelques minutes avant le volume reel : MAX_ROWS=5000 et N_TAGS=5. Pour repasser au volume reel, remets 50000 et 20, puis recree le conteneur :

    docker compose up -d --no-build --force-recreate airflow

Demarrage :

    docker compose up -d --build
    docker compose ps
    docker compose up --no-build minio-init

minio-init cree les buckets bronze, silver et gold.

Adresses : Airflow http://localhost:8080 (admin / admin), MinIO http://localhost:9001 (minioadmin / minioadmin), dashboard http://localhost:8501. Si le port d'Airflow a ete change dans docker-compose.yml, utilise celui-la.

Au premier lancement, Spark telecharge ses paquets (Hudi, S3, Kafka) : c'est long, une seule fois. Ne lance jamais docker compose down -v : il supprime ce cache et les donnees MinIO.

### 3. Verifier que le pipeline marche chez toi

Un job a la fois, en attendant la ligne OK de chacun :

    docker compose exec airflow python /opt/project/producer/producer.py
    docker compose exec airflow bash -c "cd /opt/project/jobs && python bronze.py"
    docker compose exec airflow bash -c "cd /opt/project/jobs && python silver.py"
    docker compose exec airflow bash -c "cd /opt/project/jobs && python gold.py"
    docker compose exec airflow bash -c "cd /opt/project/jobs && python train_model.py"

Attendu avec 50 000 questions et 20 tags : le producteur envoie 50000 messages, bronze ecrit 50000 messages, et train_model affiche 20 lignes de tags puis MODELE OK. Le job d'entrainement peut rester plusieurs minutes sans rien afficher, c'est normal.

Dans MinIO (bucket gold, bouton de rafraichissement), tu dois voir : models/lr_00 a lr_19, model_metrics, models_index, ml/, top_tags, tag_trends.

Si un job est tue (Killed ou OutOfMemoryError), augmente SPARK_DRIVER_MEMORY dans docker-compose.yml, ferme d'autres programmes, ou baisse MAX_ROWS (par exemple 20000), puis repars d'un etat propre (voir plus bas).

Pour repartir de zero : supprimer le topic Kafka questions et vider les buckets bronze, silver et gold, sinon les anciennes donnees et l'ancien checkpoint du streaming se melangent aux nouveaux.

### 4. Ta partie

#### A. Declencher le DAG stackoverflow_monthly en entier dans Airflow

Mariam n'a lance que les jobs un par un, jamais le DAG. Le producteur Kafka n'est pas dans le DAG : lance-le avant (etape 3). Puis :

    docker compose exec airflow airflow dags unpause stackoverflow_monthly
    docker compose exec airflow airflow dags trigger stackoverflow_monthly

Ouvre Airflow : les 4 taches (bronze_ingest, silver_hudi, gold_trends, train_model) doivent passer au vert. Si une est rouge, ouvre ses Logs, corrige, puis relance. Note la duree de chaque tache pour le rapport.

Ameliorations possibles : retries et retry_delay sur les taches, un execution_timeout, et une tache de controle en fin de chaine (verifier que gold/model_metrics contient bien N_TAGS lignes). A decider avec l'equipe : le producteur Kafka entre-t-il dans le DAG ou reste-t-il manuel ?

Le streaming (jobs/streaming/stream_predict.py) tourne en continu, donc il n'est volontairement pas dans le DAG mensuel.

#### B. Construire le dashboard (il n'a jamais tourne chez Mariam)

    docker compose build dashboard
    docker compose up -d dashboard
    docker compose logs --tail 20 dashboard

Ouvre http://localhost:8501. Le dashboard lit gold/top_tags, tag_trends et model_metrics, et trie le tableau des metriques sur la colonne auc. Cette colonne existe, donc la lecture devrait marcher, mais c'est une lecture du code, non executee : verifie que les tendances et le tableau des 20 tags s'affichent. Si pip echoue sur le reseau pendant le build, relance la commande : Docker garde les etapes deja faites.

Ensuite, ajoute ce qui manque :
- les metriques du modele en graphique (precision, rappel, F1 et AUC par tag), a partir de gold/model_metrics ;
- les predictions du streaming (gold/streaming/predictions : tags reels, tags predits, n_correct).

Colonnes de gold/model_metrics : tag, model_path, nb_train_positifs, threshold, precision, recall, f1, auc, tp, fp, fn.
Colonnes de gold/streaming/predictions : id, title, tags, predicted_tags, n_correct, batch_id, predicted_at.

#### C. Verifier avec Mohammed et Hedda : Streamlit ou Superset ?

La repartition prevoit Superset, mais le depot contient un dashboard Streamlit (dashboard/app.py, port 8501) et aucun service Superset dans docker-compose.yml. Mets-toi d'accord avec Mohammed et Hedda avant de continuer.
- Garder Streamlit : il existe deja et lit MinIO directement. C'est l'option la plus rapide pour les soutenances.
- Passer a Superset : il faut l'ajouter a docker-compose.yml, et il ne lit pas du Parquet sur MinIO directement (il faut une couche SQL au milieu, par exemple DuckDB, Trino ou un export vers PostgreSQL). Cela n'a pas ete teste.

### 5. Quand c'est fini

- Le DAG passe entierement au vert dans Airflow, avec 50 000 questions et 20 tags.
- Le dashboard affiche les 20 tags et leurs metriques.
- Un clone neuf du depot, suivi de ce guide, redemarre le projet.

### 6. Git

Une branche par personne, Pull Request relue par un autre membre. Ne jamais commiter .env, data/*.csv, ni des reglages locaux (port, memoire) de docker-compose.yml.

### 7. Qui contacter

- Kafka, entrainement, evaluation, streaming : Mariam.
- Bronze, Silver, Gold, features : Hedda.
- Producteur et squelette du projet : Mohammed.