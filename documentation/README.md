# Guide de la Couche Spark (Mohammed Hedda)
> **Projet Big Data IL-06 : Analyse des questions Stack Overflow**  
> Ce document explique en détail les tâches réalisées par **Mohammed Hedda**, l'architecture de la couche Spark, et comment chaque membre de l'équipe (Yassien, Mariam, Hasnaoui) peut utiliser ces modules dans sa propre couche.

---

## Sommaire
1. [Rôle et Vue d'ensemble](#1-rôle-et-vue-densemble)
2. [Ce que chaque camarade doit savoir (Contrats d'interface)](#2-ce-que-chaque-camarade-doit-savoir-contrats-dinterface)
   - [Pour Yassien (Lakehouse MinIO & Hudi)](#pour-yassien-lakehouse-minio--hudi)
   - [Pour Hasnaoui (Airflow & Dashboard)](#pour-hasnaoui-airflow--dashboard)
   - [Pour Mariam (MLlib & Structured Streaming)](#pour-mariam-mllib--structured-streaming)
3. [Détail des 3 Tâches réalisées par Hedda](#3-détail-des-3-tâches-réalisées-par-hedda)
   - [Tâche 1 : Nettoyage des questions (jobs/transform/)](#tâche-1--nettoyage-des-questions-jobstransform)
   - [Tâche 2 : Requêtes Spark SQL Gold (jobs/sql/)](#tâche-2--requêtes-spark-sql-gold-jobssql)
   - [Tâche 3 : Pipeline MLlib Features TF-IDF (jobs/ml/)](#tâche-3--pipeline-mllib-features-tf-idf-jobsml)
4. [Guide d'exécution pas à pas (Commandes Docker)](#4-guide-dexécution-pas-à-pas-commandes-docker)
5. [Tests unitaires](#5-tests-unitaires)
6. [Arborescence des fichiers livrés](#6-arborescence-des-fichiers-livrés)

---

## 1. Rôle et Vue d'ensemble

Dans l'équipe, **Mohammed Hedda** est responsable du traitement Spark analytique et de la préparation des données (Tâches 1, 2 et 3) :

```
Kafka (topic questions) 
   │
   ▼
Bronze (Parquet brut)
   │
   ▼
[TÂCHE 1 : jobs/transform/cleaning.py]  <─── Hedda : Nettoyage HTML, minuscules, tags, dédoublonnage
   │
   ▼
Silver (Table Hudi Copy-on-Write, partitionnée par mois)
   │
   ├───────────────────────────────────────────────────────┐
   ▼                                                       ▼
[TÂCHE 2 : jobs/sql/ (Gold Trends)]             [TÂCHE 3 : jobs/ml/features.py]  <─── Hedda
   │                                                       │
   ├─► Hudi Gold (tag_trends_hudi)                         ├─► Pipeline TF-IDF (s3a://models/)
   ├─► Parquet Gold (top_tags, tag_trends)                 ├─► Datasets train/test vectorisés
   │                                                       └─► Jeu d'essai Phase 2 (~2000 questions)
   ▼                                                       ▼
Dashboard Streamlit (Hasnaoui)                  Entraînement One-vs-Rest & Streaming (Mariam)
```

---

## 2. Ce que chaque camarade doit savoir (Contrats d'interface)

### Pour Yassien (Lakehouse MinIO & Hudi)
1. **Intégration Silver :** Votre job `jobs/lakehouse/bronze_to_silver.py` appelle directement `from transform.cleaning import clean_questions`. Le module est prêt : il reçoit le DataFrame Bronze et renvoie les questions nettoyées sans doublon prêtes pour votre upsert Hudi.
2. **Table Gold Hudi :** Hedda écrit la table `s3a://gold/tag_trends_hudi` en utilisant votre module `hudi_utils.write_hudi` avec :
   - Clé Hudi unique : `tag_month_id = CONCAT(tag, '|', year_month)`
   - Champ de pré-combinaison : `updated_at` (horodatage courant)
   - Partition : `year_month` (`yyyy-MM`)

### Pour Hasnaoui (Airflow & Dashboard)
1. **Dashboard Streamlit :** Vos composants `dashboard/app.py` attendent les tables `top_tags` et `tag_trends`. Hedda les génère directement au format Parquet dans MinIO :
   - `s3a://gold/top_tags` : contient `tag`, `nb_questions`, `avg_score`, `global_rank`.
   - `s3a://gold/tag_trends` : contient `year_month`, `tag`, `nb_questions`, `avg_score`, `rank_in_month`, `growth_rate_pct`, `trend_status`.
2. **Orchestration Airflow :** Les scripts racine `jobs/silver.py` et `jobs/gold.py` appellent désormais automatiquement les modules de Hedda. Vous pouvez déclencher votre DAG normalement sans modifier vos tâches Airflow.

### Pour Mariam (MLlib & Structured Streaming)
1. **Déblocage Phase 2 (Sans attente) :** Comme prévu dans le document de répartition, Hedda a généré un **jeu d'essai d'environ 2 000 questions vectorisées** déposé dans `s3a://gold/ml/sample_features`. Vous pouvez coder et valider le streaming sans attendre que les 50 000 questions soient traitées.
2. **Pipeline TF-IDF pour le Streaming (Tâche 6) :** Le modèle de pipeline TF-IDF ajusté est sauvegardé dans `s3a://models/tfidf_pipeline`. Dans votre job de streaming, vous n'avez qu'à faire :
   ```python
   from pyspark.ml import PipelineModel
   tfidf = PipelineModel.load("s3a://models/tfidf_pipeline")
   stream_features = tfidf.transform(cleaned_stream)
   ```
3. **Entraînement One-vs-Rest (Tâche 4) :** Vous pouvez lire directement `s3a://gold/ml/train_features` et `s3a://gold/ml/top_tags`. Un script prêt à l'emploi est déjà disponible dans `jobs/ml/train.py`.
4. **Évaluation (Tâche 5) :** Le script `jobs/ml/evaluate.py` évalue vos modèles sur `s3a://gold/ml/test_features` et écrit les métriques dans `s3a://gold/model_metrics`.

---

## 3. Détail des 3 Tâches réalisées par Hedda

### Tâche 1 : Nettoyage des questions (`jobs/transform/`)
- **Fichier :** `jobs/transform/cleaning.py`
- **Fonction principale :** `clean_questions(df)`
- **Traitements appliqués :**
  - **Suppression du HTML :** Utilisation d'expressions régulières Spark natives `<[^>]+>` (très rapide, exécuté dans le moteur Catalyst sans ralentissement UDF Python).
  - **Décodage des entités HTML :** Remplacement de `&amp;` en `&`, `&lt;` en `<`, `&gt;` en `>`, `&quot;` en `"`, etc.
  - **Minuscules & Fusion :** Le titre et le corps sont convertis en minuscules et regroupés dans une colonne unique `text`.
  - **Nettoyage des tags :** Passage en minuscules, suppression des espaces inutiles, élimination des tags vides et exclusion des questions sans aucun tag.
  - **Dédoublonnage :** Élimination des identifiants nuls et dédoublonnage strict sur la clé `id`.

### Tâche 2 : Requêtes Spark SQL Gold (`jobs/sql/`)
- **Fichiers :** `jobs/sql/queries.sql`, `jobs/sql/sql_queries.py`, `jobs/sql/gold_trends.py`
- **Requetes implémentées :**
  1. **Volume par tag et par mois :** Aplatit le tableau de tags avec `EXPLODE(tags)` et calcule le volume de questions, le score moyen, minimum et maximum par technologie et par mois.
  2. **Classement des technologies (`DENSE_RANK`) :**
     - Classement global sur tout l'historique (`global_rank`).
     - Classement mensuel de chaque tag (`rank_in_month`).
  3. **Croissance mensuelle (`LAG`) :**
     - Compare le volume du mois actuel avec le mois précédent via `LAG(nombre_questions, 1) OVER (PARTITION BY tag ORDER BY year_month)`.
     - Calcule la croissance absolue (`growth_abs = nombre_questions - prev_questions`).
     - Calcule le pourcentage de croissance (`growth_rate_pct`).
     - Attribue un indicateur clair : `'HAUSSE'`, `'BAISSE'`, `'STABLE'` ou `'NOUVEAU'`.

### Tâche 3 : Pipeline MLlib Features TF-IDF (`jobs/ml/`)
- **Fichier :** `jobs/ml/features.py`
- **Architecture du pipeline :**
  1. `get_top_tags(df, n_tags=20)` : Identifie les 20 technologies les plus fréquentes.
  2. `RegexTokenizer` : Découpe les phrases en mots (tokens) de longueur $\ge 2$.
  3. `StopWordsRemover` : Supprime les mots vides de la langue anglaise + les mots parasites de programmation (`http`, `error`, `using`, etc.).
  4. `HashingTF` : Projette les mots dans un espace vectoriel de dimension $2^{14} = 16384$.
  5. `IDF` : Calcule la fréquence inverse de document pour donner plus de poids aux mots rares et discriminants.
- **Séparation Train / Test :** 80% entraînement, 20% test (avec `seed=42` pour que tout le monde obtienne exactement les mêmes résultats).
- **Export MinIO :**
  - Modèle TF-IDF : `s3a://models/tfidf_pipeline`
  - Top tags : `s3a://gold/ml/top_tags`
  - Jeux vectorisés : `s3a://gold/ml/train_features` et `test_features`
  - Jeu d'essai Phase 2 : `s3a://gold/ml/sample_features`

---

## 4. Guide d'exécution pas à pas (Commandes Docker)

Toutes les commandes sont à lancer depuis la racine du projet sur votre terminal :

```powershell
# 1. Lancer la Tâche 1 : Nettoyage Bronze -> Silver
docker compose run --rm --no-deps airflow python /opt/project/jobs/lakehouse/bronze_to_silver.py

# 2. Lancer la Tâche 2 : Requêtes Spark SQL Gold
docker compose run --rm --no-deps airflow python /opt/project/jobs/sql/gold_trends.py

# 3. Lancer la Tâche 3 : Génération des features TF-IDF
docker compose run --rm --no-deps airflow python /opt/project/jobs/ml/features.py

# 4. Lancer l'entraînement One-vs-Rest de Mariam (Tâche 4)
docker compose run --rm --no-deps airflow python /opt/project/jobs/ml/train.py

# 5. Lancer l'évaluation multi-métriques (Tâche 5)
docker compose run --rm --no-deps airflow python /opt/project/jobs/ml/evaluate.py

# 6. Lancer le Structured Streaming en temps réel (Tâche 6)
docker compose run --rm --no-deps airflow python /opt/project/jobs/streaming/stream_predict.py
```

---

## 5. Tests unitaires

Pour tester la logique sans dépendre de MinIO ni de Kafka, 3 suites de tests unitaires ont été écrites :

- `jobs/transform/test_cleaning.py` : Teste le nettoyage HTML, les doublons et les tags.
- `jobs/sql/test_sql.py` : Teste les requêtes SQL (DENSE_RANK, LAG, calculs de croissance).
- `jobs/ml/test_features.py` : Teste le découpage en tokens, le TF-IDF et la dimension des vecteurs.

---

## 6. Arborescence des fichiers livrés

```text
bigdata-stackoverflow-il06/
├── documentation/
│   └── README.md                     <-- Ce document explicatif pour l'équipe
├── docs/
│   └── couche_spark_hedda.md         <-- Rapport technique détaillé & questions jury
├── jobs/
│   ├── transform/                    <-- TÂCHE 1 (Hedda)
│   │   ├── __init__.py
│   │   ├── cleaning.py               <-- Fonction clean_questions(df)
│   │   └── test_cleaning.py          <-- Tests unitaires nettoyage
│   ├── sql/                          <-- TÂCHE 2 (Hedda)
│   │   ├── __init__.py
│   │   ├── queries.sql               <-- Requêtes Spark SQL pures
│   │   ├── sql_queries.py            <-- Module Python d'exécution SQL
│   │   ├── gold_trends.py            <-- Job Spark SQL Gold (Hudi + Parquet)
│   │   └── test_sql.py               <-- Tests unitaires SQL
│   ├── ml/                           <-- TÂCHE 3 (Hedda) & TÂCHES 4-5 (Mariam)
│   │   ├── __init__.py
│   │   ├── features.py               <-- Pipeline TF-IDF, Top Tags, Sample Phase 2
│   │   ├── test_features.py          <-- Tests unitaires TF-IDF
│   │   ├── train.py                  <-- Tâche 4 : One-vs-Rest (Mariam)
│   │   └── evaluate.py               <-- Tâche 5 : Évaluation F1/AUC (Mariam)
│   ├── streaming/                    <-- TÂCHE 6 (Mariam)
│   │   ├── __init__.py
│   │   └── stream_predict.py         <-- Structured Streaming branché au TF-IDF
│   ├── silver.py                     <-- Mis à jour pour appeler cleaning.py
│   ├── gold.py                       <-- Mis à jour pour appeler gold_trends.py
│   └── train_model.py                <-- Mis à jour pour utiliser ml/features.py
```
