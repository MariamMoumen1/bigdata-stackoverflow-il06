# Essai preliminaire du code (5 000 questions, 5 tags)

But : verifier que le code fonctionne de bout en bout avant le volume reel. Configuration : MAX_ROWS=5000, N_TAGS=5. 1 590 lignes d'entrainement, 5 modeles.

| Tag | Positifs (train) | Precision | Rappel | F1 | AUC |
|---|---|---|---|---|---|
| javascript | 382 | 0.704 | 0.544 | 0.614 | 0.845 |
| java | 378 | 0.508 | 0.408 | 0.453 | 0.734 |
| php | 318 | 0.746 | 0.524 | 0.615 | 0.842 |
| c# | 320 | 0.556 | 0.333 | 0.417 | 0.798 |
| android | 276 | 0.700 | 0.438 | 0.539 | 0.849 |
| Moyenne macro | | 0.643 | 0.449 | 0.527 | 0.814 |

Streaming sur ces 5 000 messages : 3 micro-lots (1 998, 1 998 et 1 004 questions), par exemple [c#, serialization] predit [c#].

Un test de code supplementaire avec N_TAGS=3 (10 861 lignes d'entrainement) a donne un F1 de 0.80, mais il ne garde que les questions portant javascript, java ou c# : la tache est plus facile, ces chiffres ne sont pas comparables aux resultats finaux.

Conclusion : le code est valide, java et c# se confondent avec peu de donnees (vocabulaire proche).