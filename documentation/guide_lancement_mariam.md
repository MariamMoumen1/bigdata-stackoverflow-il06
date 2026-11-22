# Guide : relancer et verifier les taches 4, 5 et 6

## Preparation (.env, sans BOM)
Test de code : MAX_ROWS=5000, N_TAGS=5. Volume reel : MAX_ROWS=50000, N_TAGS=20. Apres une modification de .env, recreer le conteneur :
    docker compose up -d --no-build --force-recreate airflow
    docker compose exec airflow printenv MAX_ROWS N_TAGS

## Repartir d'un etat propre
Supprimer le topic Kafka questions et vider les buckets bronze, silver et gold (sinon les anciennes donnees et l'ancien checkpoint se melangent aux nouveaux).

## Chaine complete (un job a la fois)
    docker compose exec airflow python /opt/project/producer/producer.py
    docker compose exec airflow bash -c "cd /opt/project/jobs && python bronze.py"
    docker compose exec airflow bash -c "cd /opt/project/jobs && python silver.py"
    docker compose exec airflow bash -c "cd /opt/project/jobs && python gold.py"
    docker compose exec airflow bash -c "cd /opt/project/jobs && python train_model.py"
    docker compose exec -e STREAM_ONCE=1 airflow bash -c "cd /opt/project/jobs && python streaming/stream_predict.py"

## Verification dans MinIO (http://localhost:9001, bucket gold, bouton de rafraichissement)
- models/lr_00 a lr_19 : les 20 modeles (tache 4)
- models_index/ et model_metrics/ : index et metriques (tache 5)
- streaming/predictions/ et streaming/_checkpoint/ : predictions et reprise (tache 6)
Verifier que model_metrics, models_index et ml/top_tags contiennent le meme nombre de tags : un lancement avec N_TAGS=3 les ecrase.

## Problemes rencontres
- Reseau lent : telecharger les images et les paquets Spark une fois, le cache (volume ivy_cache) les conserve. Ne jamais lancer docker compose down -v.
- Memoire : Docker a besoin de 5 a 6 Go. En cas de Killed ou OutOfMemoryError, augmenter SPARK_DRIVER_MEMORY ou baisser MAX_ROWS (20000).
- Fichiers .env, docker-compose.yml, Dockerfile : enregistrer en UTF-8 sans BOM.