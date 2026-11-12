# Plan sur 2 jours (2 personnes)

## Jour 1 : faire tourner la chaine
- **Ensemble (1h)** : Docker, clone du depot, donnees, `docker compose up -d --build`.
- **Membre A** : Kafka + producteur, puis Bronze et Silver. Verifier les fichiers dans MinIO (buckets `bronze`, `silver`).
- **Membre B** : cahier des charges (PDF), puis Gold, modele MLlib, dashboard.
- **Fin de journee** : les 4 taches Airflow sont vertes et le dashboard affiche des graphiques. Sinon, ne pas commencer le rapport.

## Jour 2 : finaliser
- **Matin** : corriger les bugs, test complet `docker compose down -v` puis `./run_all.sh`, passer `MAX_ROWS` a 50000 si la machine suit.
- **Apres-midi** : rapport (chacun sa partie), slides (10 max), demo repetee 2 fois, video de secours.
- **Soir** : chacun explique le code de l'autre (la note individuelle porte sur la comprehension globale).

## Si le temps manque
Garder toute la chaine, reduire les volumes (`MAX_ROWS=10000`, `N_TAGS=5`).
