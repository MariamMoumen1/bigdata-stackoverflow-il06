# Tache 4 : One-vs-Rest multi-etiquettes (jobs/ml/train.py)

Responsable : Mariam Moumen.

## Principe
Une question Stack Overflow porte plusieurs tags. On entraine un classifieur de regression logistique binaire par tag ("present ou absent") sur les features TF-IDF de Hedda. Un seuil de probabilite (PREDICTION_THRESHOLD, 0.5 par defaut) decide si le tag est retenu.

## Choix
- OneVsRest de MLlib n'est pas utilise : il n'accepte qu'une etiquette par ligne.
- Les exemples positifs sont ponderes (poids plafonne a 20, POS_WEIGHT_MAX) : sans cela, un tag rare est presque toujours predit "absent".
- Chemin des modeles par index (gold/models/lr_00 a lr_19) : c#, c++ ou .net posent probleme dans un chemin S3. L'index gold/models_index relie chaque tag a son modele, son poids et son seuil.

## Lancer
    docker compose exec airflow bash -c "cd /opt/project/jobs && python train_model.py"
Variables : MAX_ROWS et N_TAGS (fichier .env), PREDICTION_THRESHOLD, POS_WEIGHT_MAX.