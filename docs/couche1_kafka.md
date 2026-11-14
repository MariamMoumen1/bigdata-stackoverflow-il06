# Couche 1 : sources et ingestion Kafka

Objectif : rejouer StackSample en flux dans le topic Kafka questions.

## Donnees

Questions.csv et Tags.csv de StackSample, places dans data/ (ignores par Git). Les tags sont regroupes par question par le producteur.

## Lancement

1. docker compose up -d kafka
2. .\scripts\create_topic.ps1
3. lancer le producteur (voir README)
4. .\scripts\check_offsets.ps1
