# Inventaire des sources — ce qui est ouvert, ce qui ne l'est pas

Relevé effectué le 2026-08-27 contre les portails en direct. Le projet n'exploitait
que 4 jeux de données sur les 96 du catalogue IDFM ; ce document recense ce qui
existe réellement et ce qui vaut la peine d'être collecté.

## Corrections apportées par la collecte réelle

Trois affirmations de ce document, écrites d'après les métadonnées, se sont
révélées fausses une fois les données téléchargées.

**Les comptages routiers ne remontent pas à 2010 dans le jeu courant.**
`comptages-routiers-permanents` est une **fenêtre glissante d'environ 14 mois**
(vérifié : 2025-07-01 → 2026-08-26 pour ses 28,9 M lignes). L'historique existe
bien de 2010 à 2024, mais dans un jeu séparé,
`comptages-routiers-permanents-historique`, sous forme de quinze ZIP annuels
attachés — `/records` y renvoie zéro. C'est le motif exact des validations IDFM.

Conséquence stratégique : **le flux routier courant est périssable**. Ce qui sort
de la fenêtre n'est pas repris dans l'archive avant la publication annuelle
suivante. Il rejoint Vélib' et PRIM dans la catégorie « à collecter maintenant ».

**Le flux national DATEX II ne couvre pas l'Île-de-France.** « Circulation en temps
réel — réseau routier non concédé » publie vitesses moyennes et débits pour
1 376 points, rafraîchis toutes les 6 minutes, sans clé — mais les gestionnaires
présents sont DIRCE, DIRA, DIRMED, DIRN, DIRSO, DIRO, DIRNO, DIRCO, DIRE et DIRMC.
**Aucun DIRIF.** Le réseau structurant francilien (A86, A6, périphérique amont) en
est absent. Sytadin, qui l'exploite, ne publie pas de flux ouvert exploitable.
C'est une lacune réelle du projet, pas une source à intégrer.

**Le parc de capteurs routiers est à moitié mort.** 2024 donne 27 333 593 lignes sur
3 305 capteurs — à lui seul plus que tout l'historique ferré. Mais **57 % des
débits sont nuls**, et la panne n'est pas uniforme :

| Disponibilité | Capteurs |
|---|---|
| < 10 % | **1 412** |
| 10–50 % | 428 |
| 50–90 % | 651 |
| > 90 % | **814** |

Le taux de nuls est identique quel que soit `etat_barre` (57 %, 57 %, 71 %), donc
le drapeau de validité ne permet pas de filtrer. Le plan annonçait « 3 000+
tronçons » : c'est vrai nominalement, mais le réseau réellement exploitable
compte ~1 465 capteurs, dont 814 fiables. **Toute agrégation doit pondérer par la
disponibilité du capteur**, sinon elle mesure surtout quels capteurs marchaient.

Les données elles-mêmes sont saines : profil horaire cohérent (creux à 161 véh/h à
4 h, plateau à ~830 de 9 h à 20 h). Le plateau plutôt qu'une double pointe est
caractéristique de Paris intra-muros, où la capacité sature et plafonne la pointe.

**Les gros exports ODS ne tiennent pas en une requête.** L'export global du
comptage multimodal (12,3 M lignes) ne rend pas la main dans un délai utilisable ;
découpé par mois il répond en ~35 s pour ~300 k lignes. Le découpage n'est pas une
optimisation, c'est la seule façon d'obtenir le jeu. Il rend en outre la reprise
gratuite, chaque mois étant sa propre partition.

**Le comptage multimodal contient des horodatages aberrants** — le minimum publié
est l'an **0001**. Le balayage part de 2018 plutôt que du minimum réel, sinon il
génère des milliers de requêtes vides.

**Le volume routier est dominé par une colonne inutile.** Une année d'archive fait
186 Mo compressés et ~7 Go de texte, essentiellement à cause de `dessin`, qui
répète la géométrie complète du tronçon sur *chaque ligne horaire*. Elle est
retirée à la collecte ; la géométrie vient de `referentiel-comptages-routiers`
(3 739 lignes) une seule fois.

## Décision : pas de temps réel (2026-08-27)

**Le projet se limite aux données historiques.** La clé PRIM ne sera pas demandée
et aucun collecteur temps réel ne tourne en boucle. Le code de
`sources/prim_realtime.py` reste en place, inerte, et `parismob fetch velib`
reste appelable ponctuellement.

Une conséquence factuelle à connaître, parce qu'elle concerne l'historique et non
le temps réel : **il existe un trou de janvier à juin 2025 dans les comptages
routiers.** L'archive annuelle s'arrête à 2024 et la fenêtre glissante commence au
2025-07-01 ; ce semestre n'est actuellement dans aucun des deux. Il devrait
apparaître à la publication de l'archive 2025, vraisemblablement début 2027. Rien à
faire d'ici là, mais toute analyse routière couvrant 2025 doit en tenir compte.

## La question de la clé API, tranchée

**La clé PRIM ne sert qu'à trois endpoints temps réel.** Tout le reste du
catalogue IDFM — 86 jeux sur 96, plus de 10 millions de lignes — est accessible
sans compte, sans clé, sans inscription.

| Endpoint PRIM | Protocole | Ce qu'il donne | Sans clé |
|---|---|---|---|
| `/marketplace/stop-monitoring` | SIRI StopMonitoring | Prochains passages à un arrêt : horaire théorique **et** attendu | `401` |
| `/marketplace/estimated-timetable` | SIRI EstimatedTimetable | Idem mais sur le réseau entier, par ligne | `401` |
| `/marketplace/general-message` | SIRI GeneralMessage | Messages de perturbation affichés sur les écrans | `401` |

C'est tout. La clé n'ouvre rien d'autre. Ce qu'elle apporte est précis et
irremplaçable : **l'écart entre l'horaire prévu et l'horaire réel, à la minute, par
course**. Aucun jeu historique ne contient cela.

Inscription : [prim.iledefrance-mobilites.fr](https://prim.iledefrance-mobilites.fr/)
— gratuit, quota de l'ordre de 20 000 requêtes/jour.

### Le substitut partiel, sans clé

`indicateurs-qualite-service-sncf-ratp` (7 721 lignes, ODbL) donne la ponctualité
et la régularité **par ligne et par trimestre**, avec l'objectif contractuel et
l'indication de pénalité. Champs : `operatorname, theme, indicator, id_line,
name_line, trimester, year, percent_result, objectif_reference_contrat, penality`.

Ce n'est pas la même granularité, mais c'est complémentaire dans le bon sens :
PRIM donne la minute **à partir de maintenant**, les indicateurs donnent le
trimestre **en remontant dans le passé**. Le second est disponible immédiatement.

## Les trois jeux réellement bloqués

Sous « Licence Mobilité » — `/records` renvoie `ForbiddenAccess`, l'export CSV
renvoie l'en-tête seul :

| Jeu | Lignes | Intérêt | Contournement |
|---|---|---|---|
| `perimetre-des-donnees-tr-disponibles-plateforme-idfm` | 74 477 | Liste des arrêts couverts par le temps réel **et format exact des `MonitoringRef` SIRI** | aucun |
| `etat-des-ascenseurs` | 944 | État des ascenseurs, mis à jour en continu — périssable | aucun |
| `offre-horaires-tc-gtfs-idfm` | 1 | GTFS complet | **contourné** — voir ci-dessous |

Le GTFS bloqué côté IDFM est téléchargeable librement sur data.gouv.fr :
`https://www.data.gouv.fr/api/1/datasets/r/413988ed-d340-467b-8be2-7b999fcd207a`
— 143 Mo compressés, ~1,3 Go décompressés, `stop_times.txt` à lui seul fait 996 Mo.
Vérifié : `http=200`, archive valide.

## Attention aux licences CC BY-NC-ND

Six jeux sont sous CC BY-NC-ND (dont `grands-travaux-ete-2026`). **ND = pas de
dérivés.** Un modèle entraîné dessus est une œuvre dérivée. Volume négligeable
(222 lignes au total, essentiellement des PNG), mais à ne pas intégrer au lac par
réflexe.

Répartition du catalogue : 51 jeux en Licence Ouverte Etalab (4,1 M lignes),
29 en ODbL (5,3 M), 6 en Licence Ouverte v1 (1,0 M), 6 en CC BY-NC-ND,
3 en Licence Mobilité, 1 en Open License v1.

## Ce qui manque au lac et qui devrait y être

Par ordre de valeur décroissante. Tout est ouvert et vérifié accessible.

### 1. Comptages routiers permanents de Paris — 28,9 M lignes

`opendata.paris.fr/…/comptages-routiers-permanents`. Débit horaire (`q`), taux
d'occupation (`k`) et état de trafic par tronçon, avec géométrie, **depuis 1996**.

C'est la contrepartie routière des validations, et de très loin le plus gros jeu
disponible. Le plan le mentionnait comme alternative à Google Maps sans mesurer ce
qu'il représente : c'est un jeu de labels routiers complet, gratuit, et déjà
historique. Attention : dépasse largement le plafond ODS de 10 000 offsets, il faut
découper par compteur ou par période.

### 2. Offre hebdomadaire moyenne — 3,3 M lignes sur trois jeux

`offre_hebdomadaire_moyenne_hors_vacances` (1 311 578),
`…_vacances_scolaires` (1 075 261), `…_vacances_ete` (916 970).

Nombre de courses par arrêt, par ligne, par tranche horaire, **par jour de la
semaine**, avec coordonnées. C'est l'**offre**, quand les validations sont la
**demande** — leur rapport donne un taux de charge, ce qui change la nature de ce
qui est modélisable.

Le découpage en trois versions (hors vacances / vacances scolaires / vacances été)
est en soi une validation empirique des features calendaires : IDFM planifie déjà
son offre selon cette typologie.

### 3. Validations du réseau de surface — ~2,8 M lignes/an

`histo-validations-reseau-surface` (2015-2024, même structure que le ferré) plus
les jeux trimestriels courants. Bus et tram. Le lac ne contient aujourd'hui que le
ferré, soit environ la moitié du réseau.

### 4. Validations ferré 2025 — ~1,9 M lignes

Quatre jeux trimestriels. **Schéma renommé une fois de plus** : `ida` au lieu de
`ID_REFA_LDA`, colonnes en minuscules. Attention, `ida` n'est pas garanti être le
même identifiant que `id_ref_zdc` — à vérifier avant de joindre.

### 5. GTFS complet — `stop_times.txt` de 996 Mo

Horaires théoriques au niveau de la course. Indispensable dès qu'on veut comparer
prévu et réalisé, et prérequis à toute construction de graphe réseau.

### 6. Vélib' temps réel — 1 519 stations

`opendata.paris.fr/…/velib-disponibilite-en-temps-reel`. Vélos et bornes
disponibles par station, mécaniques et électriques séparés. **Périssable** : même
argument que PRIM, personne ne l'archive, et il ne nécessite aucune clé. Le
collecteur est donc réalisable immédiatement, contrairement à PRIM.

### 7. Comptage vélo de Paris — 1,05 M lignes

Comptages horaires par totem, avec date d'installation.

### 8. Le reste, par ordre d'intérêt

| Jeu | Lignes | Intérêt |
|---|---|---|
| `offre-cumulee-bus-sur-les-troncons-routiers` | 200 125 | Offre bus projetée sur le réseau routier |
| `amenagements-velo-en-ile-de-france` | 151 130 | Infrastructure cyclable |
| `arrets-lignes` | 74 632 | Topologie arrêts↔lignes, base d'un graphe réseau |
| `arrets` / `zones-d-arrets` / `zones-de-correspondance` | 37 941 / 18 008 / 15 545 | Référentiel complet des arrêts |
| `indicateurs-qualite-service-parcours-voyageur` | 10 935 | Qualité perçue du parcours |
| `indicateurs-qualite-service-sncf-ratp` | 7 721 | **Ponctualité par ligne/trimestre** (voir plus haut) |
| `positionnement-dans-la-rame` | 5 239 | Répartition de la charge dans les rames |
| `frequentation-mesuree-parking-velo` | 4 913 | Fréquentation des parkings vélo |
| `referentiel-des-lignes` | 2 122 | Référentiel des lignes |
| `traces-des-lignes-de-transport-en-commun-idfm` | 2 028 | Géométries des lignes |
| `qualite-de-lair-dans-le-reseau…` | 663 | Qualité de l'air en station |
| `actualites_locales_idfm` | 216 | Actualités locales, mises à jour quotidiennement |

## Sources externes trouvées via le méta-jeu IDFM

`recensement-dautres-donnees-ouvertes-dinteret-pour-les-hackathons-idfm` (61 lignes,
ouvert) est une liste curatée par IDFM d'autres sources utiles. Les plus
intéressantes, toutes vérifiées accessibles sans compte :

| Source | Lignes | Portail | Pourquoi |
|---|---|---|---|
| **Comptage multimodal Paris** | **12 314 877** | opendata.paris.fr | Vélo, trottinette, 2RM, VL, PL, autobus — par site, trajectoire et horodatage. Le seul jeu qui sépare les modes. |
| **Comptage voyageurs montants Transilien** | 6 537 | ressources.data.sncf.com | Montées comptées **à bord**, par gare, ligne, tranche horaire et type de jour. Mesure indépendante des validations — permet de calibrer le taux de capture. |
| **Chantiers à Paris** | 4 937 | opendata.paris.fr | Travaux avec `date_debut`/`date_fin`. Choc exogène **connu à l'avance**, exactement la catégorie que le plan vise. |
| **Migration alternante des actifs** (INSEE) | — | data-iau-idf | Matrice origine-destination domicile-travail par commune. |
| **Flux GBFS Vélib' Métropole** | temps réel | velib-metropole-opendata | Format GBFS standard, Licence Ouverte. Alternative à l'API PRIM Vélib'. |

Le comptage multimodal mérite d'être souligné : 12,3 millions de lignes, et c'est
la seule source de la région qui distingue trottinettes et deux-roues motorisés.

## Sources ajoutées au second passage

| `fetch` | Source | Lignes | Intérêt |
|---|---|---|---|
| `meteo` | Météo-France quotidien, 8 départements, 357 postes, 1950-2026 | 3 771 028 | Pluie, températures, vent |
| `accidents` | BAAC, Ministère de l'intérieur, 2005-2024, 4 tables/an | ~2,5 M | Choc local géolocalisé + **motif du déplacement** |
| `insee-od` | Matrice origine-destination domicile-travail 2021 et 2022 | 389 836 | **Les seuls flux du lac** |
| `sncf-ponctualite` | Régularité mensuelle Transilien, Intercités, TGV | 20 554 | Maille mensuelle, mesure transporteur |
| `sncf-gares` | Fréquentation annuelle + référentiel gares | 9 490 | Complément aux validations |
| `air` | Qualité de l'air en station IDFM | 663 | |

**La matrice OD est la seule source de flux.** Tout le reste mesure des passages en
un point ; elle seule dit *d'où à où*. C'est aussi le jeu directement comparable à
ce que produit eqasim — une population synthétique se valide contre elle. Les plus
gros flux 2022 sont cohérents : le 8e arrondissement domine comme destination,
alimenté par les 17e, 15e, 16e et 18e.

Le champ `trajet` des usagers BAAC distingue domicile-travail, domicile-école,
courses et loisirs : une des rares sources publiques qui qualifie le **motif** d'un
déplacement réellement observé.

### Dérives de nommage rencontrées au second passage

Le motif se répète à chaque source, et coûte à chaque fois des données perdues en
silence si on n'y prend pas garde :

* **Compression ZIP** — les archives routières 2021 et 2023 sont en **deflate64**,
  2020/2022/2024 en deflate ordinaire. `zipfile` refuse la première avec
  `That compression method is not supported`. Dépendance `zipfile-deflate64` ajoutée.
* **Faute de frappe conservée** — les fichiers BAAC 2021 et 2022 s'appellent
  `carcteristiques-*.csv`, sans le second `a`. Un motif énumérant les orthographes
  exactes perdait ces deux années sans rien signaler.
* **Séparateur d'URL** — l'INSEE écrit `...-2021-csv.zip` mais `...-2022_csv.zip`.
  Les URL sont donc notées en toutes lettres et non construites depuis un modèle.

## Conséquence sur la stratégie du projet

`PLAN.md` affirme que l'avantage du projet réside dans les données périssables, et
en déduit qu'il faut PRIM. La première moitié est juste, la seconde est trop
étroite : **Vélib' temps réel est tout aussi périssable et ne demande aucune clé**.
Il peut tourner ce soir.

Par ailleurs, le plan traitait les données statiques comme sans valeur puisque
« disponibles pour tout le monde ». Les 28,9 millions de lignes de comptages
routiers montrent que le facteur limitant n'est pas la disponibilité mais le fait
que presque personne ne les assemble. Un lac qui joint validations ferré, surface,
offre planifiée, comptages routiers et comptages vélo sur un référentiel d'arrêts
commun n'existe nulle part ailleurs — et rien de tout cela n'exige de clé.
