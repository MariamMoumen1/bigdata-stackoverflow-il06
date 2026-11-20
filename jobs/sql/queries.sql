-- ==============================================================================
-- PROJET IL-06 : Analyse des questions Stack Overflow (Couche Gold Spark SQL)
-- Tache 2 - Responsable : Mohammed Hedda
-- Contrats : 
--   - Hedda -> Yassien (Gold) : Ecriture Hudi avec cle composite tag_month_id
--   - Hedda -> Hasnaoui (Dashboard) : Tables Gold tag_trends et top_tags
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Vue intermediaire aplatie : question_tags
-- Eclate le tableau de tags pour obtenir une ligne par couple (question, tag)
-- Filtre les partitions invalides et les tags vides
-- ------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW question_tags AS
SELECT
    id,
    score,
    creation_date,
    year_month,
    EXPLODE(tags) AS tag
FROM questions_silver
WHERE year_month IS NOT NULL 
  AND year_month != 'inconnu'
  AND tags IS NOT NULL;


-- ------------------------------------------------------------------------------
-- 2. REQUETE 1 : Volume par tag et par mois (Gold Principal)
-- Agregation mensuelle par tag avec statistiques de score et cle Hudi
-- ------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW tag_monthly_volume AS
SELECT
    tag,
    year_month,
    COUNT(*) AS nombre_questions,
    COUNT(*) AS nb_questions,
    ROUND(AVG(score), 2) AS avg_score,
    MIN(score) AS min_score,
    MAX(score) AS max_score,
    CONCAT(tag, '|', year_month) AS tag_month_id,
    CURRENT_TIMESTAMP() AS updated_at
FROM question_tags
GROUP BY tag, year_month;


-- ------------------------------------------------------------------------------
-- 3. REQUETE 2 : Classements des tags (Global et Mensuel)
-- 3.1 Classement Global : Top tags sur l'ensemble de l'historique
-- ------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW top_tags_ranking AS
SELECT
    tag,
    COUNT(*) AS nb_questions,
    COUNT(*) AS nombre_questions,
    ROUND(AVG(score), 2) AS avg_score,
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS global_rank
FROM question_tags
GROUP BY tag
ORDER BY nb_questions DESC;


-- 3.2 Classement Mensuel : Rang de chaque tag au sein de chaque mois
CREATE OR REPLACE TEMPORARY VIEW tag_monthly_ranking AS
SELECT
    tag,
    year_month,
    nombre_questions,
    avg_score,
    tag_month_id,
    updated_at,
    DENSE_RANK() OVER (PARTITION BY year_month ORDER BY nombre_questions DESC) AS rank_in_month
FROM tag_monthly_volume;


-- ------------------------------------------------------------------------------
-- 4. REQUETE 3 : Croissance mensuelle des tags (Croissance mois par mois)
-- Utilisation de la fonction fenetre LAG pour comparer au mois precedent
-- Calcul de la variation absolue et du pourcentage de croissance
-- ------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW tag_monthly_growth AS
WITH lag_data AS (
    SELECT
        tag,
        year_month,
        nombre_questions,
        avg_score,
        rank_in_month,
        tag_month_id,
        updated_at,
        LAG(nombre_questions, 1) OVER (
            PARTITION BY tag 
            ORDER BY year_month
        ) AS prev_month_questions,
        LAG(year_month, 1) OVER (
            PARTITION BY tag 
            ORDER BY year_month
        ) AS prev_month
    FROM tag_monthly_ranking
)
SELECT
    tag,
    year_month,
    nombre_questions,
    nombre_questions AS nb_questions,
    avg_score,
    rank_in_month,
    tag_month_id,
    updated_at,
    COALESCE(prev_month_questions, 0) AS prev_month_questions,
    CASE 
        WHEN prev_month_questions IS NULL THEN 0
        ELSE (nombre_questions - prev_month_questions)
    END AS growth_abs,
    CASE 
        WHEN prev_month_questions IS NULL OR prev_month_questions = 0 THEN 0.0
        ELSE ROUND(((nombre_questions - prev_month_questions) * 100.0) / prev_month_questions, 2)
    END AS growth_rate_pct,
    CASE
        WHEN prev_month_questions IS NULL THEN 'NOUVEAU'
        WHEN nombre_questions > prev_month_questions THEN 'HAUSSE'
        WHEN nombre_questions < prev_month_questions THEN 'BAISSE'
        ELSE 'STABLE'
    END AS trend_status
FROM lag_data;
