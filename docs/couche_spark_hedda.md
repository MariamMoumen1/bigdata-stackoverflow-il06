# Couche 3 : Spark - Nettoyage, Requêtes SQL et Features TF-IDF

**Responsable :** Mohammed Hedda  
**Périmètre :** Tâches Spark 1, 2 et 3  
**Dossiers :** `jobs/transform/`, `jobs/sql/`, `jobs/ml/`  
**Branche Git :** `feature/spark-features`

---

## 1. Vue d'ensemble de la couche

Cette couche constitue le cœur du traitement analytique et prédictif de la plateforme Big Data. Elle intervient à trois moments clés du cycle de vie de la donnée :

```
Kafka ─► Bronze (Parquet) ─► [Tâche 1 : clean_questions] ─► Silver (Hudi)
                                                                │
                 ┌──────────────────────────────────────────────┴─────────────────────────┐
                 ▼                                                                        ▼
   [Tâche 2 : Spark SQL (Gold)]                                            [Tâche 3 : MLlib Features (TF-IDF)]
                 │                                                                        │
        ┌────────┴────────┐                                                      ┌────────┴────────┐
        ▼                 ▼                                                      ▼                 ▼
Hudi Gold           Parquet Gold                                            Modèle Pipeline     Jeux vectorisés
(tag_trends_hudi)   (top_tags, tag_trends)                                  (s3a://models/)     (train/test_features)
        │                 │                                                                        │
        ▼                 ▼                                                                        ▼
  Lakehouse Yassien  Dashboard Hasnaoui                                                     Entraînement Mariam
```

---

## 2. Contrats d'interface (Qui passe quoi à qui)

| De | Vers | Contrat d'interface et livrable |
|---|---|---|
| **Hedda (Tâche 1)** | **Yassien (Silver)** | Fonction `clean_questions(df)` dans `jobs/transform/cleaning.py`. Reçoit le DataFrame Bronze (champs `id`, `owner_user_id`, `creation_date`, `score`, `title`, `body`, `tags`, `kafka_ts`), élimine les doublons d'id, nettoie le HTML et les tags, et produit le schéma prêt pour l'upsert Hudi. |
| **Hedda (Tâche 2)** | **Yassien (Gold)** | Table Gold Hudi `tag_trends_hudi` dans `s3a://gold/tag_trends_hudi`, avec clé composite `tag_month_id = tag\|year_month`, pré-combinaison `updated_at`, partitionnée par `year_month` via `hudi_utils.write_hudi`. |
| **Hedda (Tâche 2)** | **Hasnaoui (Dashboard)** | Tables Parquet dans `s3a://gold/` : `top_tags` (avec `tag`, `nb_questions`, `avg_score`, `global_rank`) et `tag_trends` (avec `year_month`, `tag`, `nb_questions`, `avg_score`, `growth_rate_pct`, `rank_in_month`). |
| **Hedda (Tâche 3)** | **Mariam (ML & Streaming)** | Pipeline TF-IDF ajusté sauvegardé dans `s3a://models/tfidf_pipeline`, liste des top tags dans `s3a://gold/ml/top_tags`, et DataFrames train/test vectorisés dans `s3a://gold/ml/train_features` et `test_features`. |

---

## 3. Tâche 1 : Nettoyage des questions (`jobs/transform/`)

Fichier : `jobs/transform/cleaning.py`  
Test unitaire : `jobs/transform/test_cleaning.py`

### 3.1 Problématiques traitées
Les questions extraites de Stack Overflow contiennent des imperfections majeures qui polluent les analyses SQL et faussent l'apprentissage automatique :
- Corps de texte (`body`) brut en HTML contenant des balises (`<p>`, `<code>`, `<a>`, `<div>`) et des entités HTML encodées (`&amp;`, `&lt;`, `&gt;`, `&quot;`, `&#39;`).
- Casse hétérogène (majuscules/minuscules) provoquant des doublons sémantiques.
- Séparation artificielle entre `title` et `body` : les mots-clés décisifs figurent souvent dans le titre alors que les détails sont dans le corps.
- Tags sous forme brute parfois mal normalisés (espaces résiduels, casse non uniforme, tags vides).
- Présence potentielle de doublons d'identifiants (`id`) ou d'enregistrements sans identifiant.

### 3.2 Implémentation de `clean_questions(df)`
1. **Validation et dédoublonnage de la clé primaire** :
   ```python
   df.dropna(subset=["id"]).dropDuplicates(["id"])
   ```
2. **Élimination du HTML et normalisation des entités** :
   - Expression régulière `<[^>]+>` pour supprimer les balises.
   - Remplacement ciblé des entités HTML (`&amp;` -> `&`, `&lt;` -> `<`, etc.).
   - Suppression des espaces blancs multiples, tabulations et sauts de ligne `\r`, `\n`.
3. **Mise en minuscules et fusion titre + corps** :
   ```python
   body_clean = lower(clean_html("body"))
   title_clean = lower(trim(coalesce(col("title"), lit(""))))
   text = trim(concat_ws(" ", title_clean, body_clean))
   ```
4. **Normalisation des tags** :
   - Passage de tous les tags en minuscules.
   - Suppression des chaînes vides et suppression des questions n'ayant aucun tag valide.
5. **Enrichissement temporel** :
   - Horodatage `creation_ts` (TimestampType) et partition mensuelle `year_month` (`yyyy-MM`).

---

## 4. Tâche 2 : Requêtes Spark SQL Gold (`jobs/sql/`)

Fichiers :
- `jobs/sql/queries.sql` : Script SQL documenté
- `jobs/sql/sql_queries.py` : Module Python des requêtes SQL et fonction `compute_gold_tables`
- `jobs/sql/gold_trends.py` : Job Spark SQL complet d'écriture Gold (Hudi + Parquet)
- `jobs/sql/test_sql.py` : Test unitaire de validation

### 4.1 Requête 1 : Volume par tag et par mois (Gold Principal)
Aplatissement des tags via `EXPLODE(tags)` et agrégation groupée par `tag` et `year_month` :
```sql
SELECT
    tag,
    year_month,
    COUNT(*) AS nombre_questions,
    COUNT(*) AS nb_questions,
    ROUND(AVG(score), 2) AS avg_score,
    CONCAT(tag, '|', year_month) AS tag_month_id,
    CURRENT_TIMESTAMP() AS updated_at
FROM question_tags
GROUP BY tag, year_month;
```
*Clé Hudi :* `tag_month_id` garantit l'unicité stricte au niveau du Lakehouse sans risquer d'écraser des tags différents au cours du même mois.

### 4.2 Requête 2 : Classements des tags (Global et Mensuel)
Utilisation de fonctions fenêtres Spark SQL pour ordonnancer les technologies :
- **Classement Global** :
  ```sql
  SELECT tag, COUNT(*) AS nb_questions, ROUND(AVG(score), 2) AS avg_score,
         DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS global_rank
  FROM question_tags GROUP BY tag ORDER BY nb_questions DESC;
  ```
- **Classement Mensuel** :
  ```sql
  SELECT tag, year_month, nombre_questions,
         DENSE_RANK() OVER (PARTITION BY year_month ORDER BY nombre_questions DESC) AS rank_in_month
  FROM tag_monthly_volume;
  ```

### 4.3 Requête 3 : Croissance mensuelle des technologies
La fonction fenêtre `LAG()` permet d'observer l'évolution temporelle d'un tag d'un mois sur l'autre :
```sql
WITH with_lag AS (
    SELECT
        tag,
        year_month,
        nombre_questions,
        LAG(nombre_questions, 1) OVER (PARTITION BY tag ORDER BY year_month) AS prev_month_questions
    FROM tag_monthly_ranking
)
SELECT
    tag,
    year_month,
    nombre_questions,
    COALESCE(prev_month_questions, 0) AS prev_month_questions,
    (nombre_questions - COALESCE(prev_month_questions, 0)) AS growth_abs,
    ROUND(((nombre_questions - prev_month_questions) * 100.0) / NULLIF(prev_month_questions, 0), 2) AS growth_rate_pct,
    CASE
        WHEN prev_month_questions IS NULL THEN 'NOUVEAU'
        WHEN nombre_questions > prev_month_questions THEN 'HAUSSE'
        WHEN nombre_questions < prev_month_questions THEN 'BAISSE'
        ELSE 'STABLE'
    END AS trend_status
FROM with_lag;
```

---

## 5. Tâche 3 : Pipeline MLlib Features TF-IDF (`jobs/ml/`)

Fichiers :
- `jobs/ml/features.py` : Construction du pipeline, sélection des top tags, train/test split, vectorisation et export MinIO
- `jobs/ml/test_features.py` : Test unitaire de validation

### 5.1 Architecture du Pipeline MLlib
Le texte naturel des questions est transformé en vecteurs numériques pondérés exploitables par les algorithmes de classification :
```
Text brut nettoyé
      │
      ▼
RegexTokenizer (pattern: \W+, minTokenLength: 2) -> Découpage en tokens
      │
      ▼
StopWordsRemover (mots vides anglais + spécifiques dev) -> clean_tokens
      │
      ▼
HashingTF (numFeatures: 16384, hashing trick) -> Vecteur de fréquences brutes (TF)
      │
      ▼
IDF (minDocFreq: 2) -> Vecteur pondéré TF-IDF (features)
```

### 5.2 Pourquoi HashingTF + IDF ?
1. **HashingTF** applique le *hashing trick* (MurmurHash3) : il ne nécessite pas de maintenir un dictionnaire global en mémoire sur le driver Spark, ce qui permet un passage à l'échelle massif et une exécution hautement distribuée.
2. **IDF (Inverse Document Frequency)** atténue le poids des termes omniprésents dans tous les sujets pour faire ressortir les termes spécifiques à chaque langage ou framework.

### 5.3 Séparation Train / Test et Export
- Découpage strict 80% train / 20% test avec `seed=42` pour garantir la reproductibilité.
- Le pipeline TF-IDF est ajusté (**fit**) **uniquement sur le jeu train** afin d'éviter tout *data leakage* (fuite d'information).
- Le modèle ajusté `PipelineModel` est exporté dans `s3a://models/tfidf_pipeline` :
  - **Mariam (Tâches 4 & 5)** peut ainsi s'abstraire de la préparation textuelle pour entraîner directement ses classifieurs logistiques One-vs-Rest.
  - **Mariam (Tâche 6)** peut charger ce modèle directement dans son job Structured Streaming pour prédire les tags d'un flux de messages entrants en temps réel.

---

## 6. Guide de lancement et commandes de vérification

Toutes les commandes s'exécutent depuis le conteneur Airflow (qui embarque Spark 3.5.1, Java 17 et les dépendances).

### 6.1 Exécution de la Tâche 1 (Nettoyage Bronze -> Silver)
```bash
docker compose run --rm --no-deps airflow python /opt/project/jobs/lakehouse/bronze_to_silver.py
```
*Sortie attendue :*
```
clean_questions : version de Hedda
SILVER OK : 50000 questions dans s3a://silver/questions
```

### 6.2 Exécution de la Tâche 2 (Requêtes Spark SQL Gold)
```bash
docker compose run --rm --no-deps airflow python /opt/project/jobs/sql/gold_trends.py
```
*Sortie attendue :*
```
[1/3] Ecriture Hudi dans s3a://gold/tag_trends_hudi...
[2/3] Ecriture Parquet top_tags dans s3a://gold/top_tags...
[3/3] Ecriture Parquet tag_trends dans s3a://gold/tag_trends...
--- TOP 10 DES TAGS GLOBAUX ---
--- EXEMPLE DE TENDANCES ET CROISSANCE MENSUELLE ---
GOLD OK : Tables Gold generees avec succes dans s3a://gold.
```

### 6.3 Exécution de la Tâche 3 (Features TF-IDF MLlib)
```bash
docker compose run --rm --no-deps airflow python /opt/project/jobs/ml/features.py
```
*Sortie attendue :*
```
Top 20 tags retenus pour la modelisation : [...]
Jeu Train : ... observations vectorisees
Jeu Test  : ... observations vectorisees
ARTEFACTS DEPOSES DANS MINIO POUR MARIAM :
  - pipeline_model : s3a://models/tfidf_pipeline
  - top_tags : s3a://gold/ml/top_tags
  - train_features : s3a://gold/ml/train_features
  - test_features : s3a://gold/ml/test_features
FEATURES OK : Pipeline TF-IDF et datasets prets avec succes.
```

### 6.4 Lancement du réentraînement ML complet (Mariam)
```bash
docker compose run --rm --no-deps airflow python /opt/project/jobs/train_model.py
```

---

## 7. Préparation à la soutenance technique (Questions Jury)

### Q1 : Pourquoi nettoyer le HTML avec des regex Spark plutôt qu'avec BeautifulSoup en UDF Python ?
**Réponse :** Les fonctions natives de PySpark (`regexp_replace`, `concat_ws`, `lower`) s'exécutent directement dans le moteur Catalyst et le runtime JVM Tungsten sans nécessiter de sérialisation/désérialisation Python-JVM (évite les surcoûts liés à Py4J et aux UDF Python). Cela multiplie les performances de traitement par 5 à 10 sur des gros volumes.

### Q2 : Pourquoi une clé composite `tag_month_id` dans la table Gold Hudi ?
**Réponse :** Dans la couche Gold, l'unité de granularité est le couple `(tag, mois)`. Si nous utilisions uniquement le `tag` comme clé Hudi, chaque nouvelle entrée mensuelle viendrait écraser les mois précédents. La clé `CONCAT(tag, '|', year_month)` combinée à la partition `year_month` assure l'unicité stricte, permet les requêtes incrémentales et garantit l'idempotence des jobs Airflow.

### Q3 : Pourquoi calculer la croissance mensuelle avec `LAG()` plutôt qu'avec une auto-jointure (self-join) ?
**Réponse :** Une auto-jointure sur une grande table temporelle implique un produit cartésien partiel et un shuffle lourd sur le cluster. La fonction fenêtre `LAG() OVER (PARTITION BY tag ORDER BY year_month)` ne nécessite qu'un simple shuffle partitionné par tag, où les données de chaque tag sont triées localement en mémoire sur chaque exécuteur, avec une complexité en $O(N \log N)$ au lieu de $O(N^2)$.

### Q4 : Quelle est la différence entre HashingTF et CountVectorizer ?
**Réponse :** `CountVectorizer` doit effectuer une passe complète sur l'ensemble du corpus pour construire et stocker un vocabulaire exact. `HashingTF` applique une fonction de hachage sur les chaînes de caractères pour déterminer directement l'index de chaque token dans un espace fixe ($2^{14} = 16384$), ce qui élimine l'état global et permet de traiter des flux de streaming infinis sans état dictionnaire.
