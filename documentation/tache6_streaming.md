# Tache 6 : Structured Streaming (jobs/streaming/stream_predict.py)

Responsable : Mariam Moumen.

## Principe
Lecture du topic Kafka questions par micro-lots (2000 messages maximum). Pour chaque lot : texte (titre + corps sans HTML), pipeline TF-IDF de Hedda (s3a://models avec repli sur gold/ml/tfidf_pipeline car le bucket models n'existe pas), classifieurs par tag (gold/models/lr_XX), tags retenus au-dessus du seuil. Ecriture Parquet dans s3a://gold/streaming/predictions (id, title, tags, predicted_tags, n_correct, batch_id, predicted_at).

## Reprise
Le checkpoint s3a://gold/streaming/_checkpoint memorise les positions Kafka deja traitees : reprise apres arret sans perte ni doublon. Pour rejouer depuis le debut, changer STREAM_RUN.

## Lancer
Traiter l'existant puis s'arreter :
    docker compose exec -e STREAM_ONCE=1 airflow bash -c "cd /opt/project/jobs && python streaming/stream_predict.py"
En continu (deux terminaux) :
    docker compose exec airflow bash -c "cd /opt/project/jobs && python streaming/stream_predict.py"
    docker compose exec -e PRODUCER_DELAY=0.01 -e MAX_ROWS=500 airflow python /opt/project/producer/producer.py
Ce job n'est pas dans le DAG mensuel : il tourne en continu.

## Variables
STREAM_ONCE, STREAM_OFFSETS, STREAM_THRESHOLD (sinon PREDICTION_THRESHOLD), STREAM_RUN, STREAM_MAX_OFFSETS.