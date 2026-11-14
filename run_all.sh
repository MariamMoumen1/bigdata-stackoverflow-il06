#!/bin/bash
# Lance toute la chaine : plateforme -> donnees -> Kafka -> DAG Airflow
set -e
cd "$(dirname "$0")"

echo "==> 1/5 Demarrage des services Docker (premier lancement : 10-20 min)"
docker compose up -d --build

echo "==> Creation du topic Kafka 'questions'"
until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 3; done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic questions --partitions 3 --replication-factor 1

echo "==> 2/5 Attente d'Airflow"
until curl -sf http://localhost:8080/health > /dev/null; do sleep 5; done
until docker compose exec -T airflow airflow dags list 2>/dev/null | grep -q stackoverflow_monthly; do sleep 5; done

echo "==> 3/5 Donnees"
if [ ! -f data/Questions.csv ] || [ ! -f data/Tags.csv ]; then
  echo "Questions.csv / Tags.csv absents : generation de fausses donnees (plan B)"
  docker compose exec -T airflow python /opt/project/scripts/generate_fake_data.py 20000
fi

echo "==> 4/5 Envoi des questions dans Kafka"
docker compose exec -T airflow python /opt/project/producer/producer.py

echo "==> 5/5 Declenchement du DAG"
docker compose exec -T airflow airflow dags unpause stackoverflow_monthly
docker compose exec -T airflow airflow dags trigger stackoverflow_monthly

echo
echo "Airflow   : http://localhost:8080  (admin / admin)"
echo "MinIO     : http://localhost:9001  (minioadmin / minioadmin)"
echo "Dashboard : http://localhost:8501  (apres que les 4 taches soient vertes)"
