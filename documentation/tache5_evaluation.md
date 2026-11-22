# Tache 5 : evaluation et sauvegarde (jobs/ml/evaluate.py)

Responsable : Mariam Moumen.

## Principe
Separation train/test 80/20 avec graine fixe (faite par Hedda dans prepare_ml_data). Les modeles sont recharges depuis MinIO et evalues sur le jeu de test. Pour chaque tag : vrais positifs (tp), faux positifs (fp), faux negatifs (fn), puis precision = tp/(tp+fp), rappel = tp/(tp+fn), F1 du tag et AUC. Moyenne macro sur tous les tags.

## Choix
Les metriques ponderees de Spark (weightedPrecision, weightedRecall, f1 multiclasse) ne sont pas utilisees : elles sont dominees par la classe "absent" et gonflees pour les tags rares.

## Sauvegarde
s3a://gold/model_metrics : tag, model_path, nb_train_positifs, threshold, precision, recall, f1, auc, tp, fp, fn.

## Lancer
    docker compose exec airflow bash -c "cd /opt/project/jobs && python train_model.py"