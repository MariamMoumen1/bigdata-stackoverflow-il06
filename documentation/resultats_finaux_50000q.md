# Resultats finaux (50 000 questions, 20 tags)

Configuration : MAX_ROWS=50000, N_TAGS=20, seuil 0.5. 27 178 lignes d'entrainement, 20 modeles (gold/models/lr_00 a lr_19).

| Tag | Positifs (train) | Precision | Rappel | F1 | AUC |
|---|---|---|---|---|---|
| javascript | 3848 | 0.590 | 0.668 | 0.626 | 0.878 |
| java | 3702 | 0.520 | 0.583 | 0.550 | 0.837 |
| c# | 3337 | 0.508 | 0.595 | 0.548 | 0.855 |
| php | 3212 | 0.586 | 0.673 | 0.627 | 0.890 |
| android | 2898 | 0.647 | 0.746 | 0.693 | 0.920 |
| jquery | 2477 | 0.589 | 0.708 | 0.643 | 0.909 |
| python | 1942 | 0.625 | 0.637 | 0.631 | 0.918 |
| html | 1866 | 0.379 | 0.550 | 0.449 | 0.843 |
| c++ | 1511 | 0.435 | 0.484 | 0.458 | 0.860 |
| ios | 1482 | 0.543 | 0.614 | 0.576 | 0.920 |
| mysql | 1454 | 0.488 | 0.585 | 0.532 | 0.885 |
| css | 1382 | 0.517 | 0.693 | 0.592 | 0.933 |
| sql | 1186 | 0.408 | 0.535 | 0.463 | 0.894 |
| asp.net | 1030 | 0.302 | 0.390 | 0.341 | 0.850 |
| objective-c | 866 | 0.392 | 0.437 | 0.413 | 0.884 |
| ruby-on-rails | 865 | 0.592 | 0.548 | 0.569 | 0.952 |
| c | 814 | 0.476 | 0.544 | 0.508 | 0.918 |
| .net | 822 | 0.121 | 0.168 | 0.141 | 0.732 |
| iphone | 689 | 0.367 | 0.452 | 0.405 | 0.886 |
| arrays | 600 | 0.320 | 0.255 | 0.284 | 0.818 |
| Moyenne macro | | 0.470 | 0.543 | 0.502 | 0.879 |

Les memes chiffres ont ete obtenus avant et apres la separation du code dans ml/train.py et ml/evaluate.py : le resultat est reproductible.

## Comparaison avec l'essai (5 000 questions, 5 tags)
L'AUC moyenne passe de 0.81 a 0.88 avec plus de donnees. Le F1 baisse legerement (0.53 a 0.50) car la tache est plus difficile avec 20 tags proches. Le rappel depasse la precision : la ponderation des positifs pousse le modele a predire plus de tags.

Meilleurs F1 : android, jquery, python, php, javascript. Plus difficiles : .net, asp.net, iphone (tags lies a c#, .net et ios) et arrays (tag generique present avec tous les langages). Piste d'amelioration : regler le seuil par tag.

## Streaming
50 000 questions traitees en mode lot, puis 500 envoyees pendant que le job tournait : 50 500 predictions dans gold/streaming/predictions. Exemples : [android, menu, android-toolbar] predit [android] ; [javascript, jquery, html, drop-down-menu] predit [javascript, jquery, html]. Les erreurs viennent de tags que le modele ne connait pas (il n'en modelise que 20). Le streaming rejoue des questions utilisees pour l'entrainement : il demontre le temps reel, l'evaluation du modele est celle du tableau.