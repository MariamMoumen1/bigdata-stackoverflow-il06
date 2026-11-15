# Schéma du message : topic "questions"

Un message = une question, au format JSON. Pas de clé Kafka (répartition entre les 3 partitions).

| Champ         | Type            | Exemple                 | Remarque                           |
|---------------|-----------------|-------------------------|------------------------------------|
| id            | entier          | 80                      | identifiant unique de la question  |
| owner_user_id | texte ou null   | "26"                    | peut être null                     |
| creation_date | texte           | "2008-08-01 13:57:07"   | format YYYY-MM-DD HH:MM:SS, ou null|
| score         | entier ou null  | 26                      |                                    |
| title         | texte           | "How to sort a list?"   | "" si absent                       |
| body          | texte (HTML)    | "<p>I have...</p>"      | HTML non nettoyé, tronqué à 5000 car. |
| tags          | liste de textes | ["python", "list"]      | liste vide possible                |

Topic : `questions` (3 partitions). Débit réglé par `PRODUCER_DELAY` (secondes entre deux messages).
