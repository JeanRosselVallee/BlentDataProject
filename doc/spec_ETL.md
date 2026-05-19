# Mise en place d'un pipeline ETL depuis une base MongoDB

## Description du projet

La description du projet permet de fournir tout un ensemble d'information par rapport au contexte et au secteur cible. Certaines informations essentielles, comme les contraintes du projet ou encore les objectifs à atteindre, y sont définies.

Une enseigne de jeux vidéos cherche à améliorer son catalogue de vente en ligne. Pour cela, elle veut proposer sur sa page d’accueil et dans ses campagnes de communication (newsletter, réseaux sociaux) une liste des jeux les mieux notés et les plus appréciés de la communauté sur les derniers jours.

Afin de refléter au mieux l’avis des internautes, elle souhaite récupérer les avis les plus récents de ses propres clients en ligne pour déterminer les jeux les mieux notés. Les développeurs Web de l’entreprise souhaitent pouvoir requêter ces informations sur une base de données SQL qui va historiser au jour le jour les jeux les mieux notés.

Les données brutes stockées dans une base MongoDB, et il est supposé que celles-ci sont ajoutées au fur et à mesure par d’autres programmes (API backend). L’objectif est de construire un pipeline de données qui va alimenter automatiquement un Data Warehouse (représenté par une base de données SQL) tous les jours en utilisant les données depuis la base MongoDB. Ce pipeline de données doit être développé en Python.

## Contraintes

- Les informations des jeux les mieux notées vont êtres disponibles sur le site Web, ainsi que dans les campagnes de communication. Elles doivent donc être à la fois exactes et faciles à manipuler pour les différents métiers qui vont l’utiliser.
- Le Data Warehouse doit être une base de données compatible SQL : MySQL, PostgreSQL ou MariaDB.
- Le pipeline doit être capable de gérer les situations où les données sont déjà présentes et éviter les doublons (les valeurs existantes doivent être remplacées).
- Chaque jour, les 15 jeux les mieux notés sur les 6 derniers mois seront ajoutés dans le Data Warehouse. Il ne faut donc pas prendre en compte les avis de plus de 6 mois d'antériorité.

## Description des données

Les données sont disponible sous forme de fichier compressé au format JSON. Chaque observation contient les caractéristiques suivantes.

- **reviewerID** : identifiant unique de l'utilisateur.
- **verified** : indique si l'utilisateur est un utilisateur vérifié (et non un robot).
- **asin** : identifiant unique du produit.
- **reviewerName** : nom/pseudo de l'utilisateur.
- **vote** : nombre de votes associés à l'avis de l'utilisateur.
- **style** : style associé au jeu vidéo.
- **reviewText** : description complète de l'avis.
- **overall** : note attribuée par l'utilisateur au jeu vidéo.
- **summary** : résumé de l'avis.
- **unixReviewTime** : timestamp de l'avis.
- **reviewTime** : date de l'avis.
- **image** : URL des images jointes par l'utilisateur.

## Étapes du projet

Les étapes du projet donnent des indications pas-à-pas pour mener à bien le projet. Elles permettent ainsi de segmenter la réalisation d'un projet en plusieurs tâches tout en s'assurant que les contraintes du projet sont respectées au fur et à mesure de son déroulement.

### Ajouter les données brutes dans une base MongoDB

Les données brutes seront ajoutées dans une collection de la base MongoDB. Cette dernière peut être installée en local ou dans un serveur sur le Cloud.

Il est également possible d'utiliser MongoDB Atlas pour exécuter une base MongoDB managée et utiliser Compass comme interface graphique.

### Créer la base de données SQL avec le schéma associé

La base de données SQL, qui matérialise le Data Warehouse, peut être installée en local ou dans le Cloud. Une table avec un schéma associée doit être créée pour historiser les jeux les mieux notées avec les informations suivantes.

- Identifiant unique du jeu vidéo.
- Note moyenne.
- Nombre d'utilisateurs ayant noté le jeu vidéo.
- Note la plus ancienne (sur les 6 derniers mois).
- Note la plus récente.

Attention : le calcul ne fait intervenir que les avis qui ont été publiés au cours des 6 derniers mois.

### Développer le script Python du pipeline ETL

Le pipeline consiste en un script Python qui va effectuer les opérations suivantes.

- Connexion à la base de données (utiliser un pilote MongoDB comme PyMongo ou Motor).
- Récupération des avis des 6 derniers mois.
- Agrégation des notes et des avis pour chaque jeu vidéo.
- Insertion des résultats dans la base SQL.

À noter qu'il est possible d'utiliser un Jupyter Notebook pour évaluer et tester les différentes étapes du pipeline.

### Automatiser le pipeline avec un outil de planification

Le script Python doit être adapté pour être utilisable par un outil de planification. On vérifiera que le workflow fonctionne correctement en l'exécutant sur des périodes antérieures différentes.

### Publier le code source et les résultats sur GitHub

Une fois le projet terminé, tu dois publier ton code source et tes résultats sur GitHub.

Tu dois organiser le projet selon le modèle Git et remplir le contenu avant de le publier.

Attention : il ne faut pas placer des données sur GitHub, car ce dernier ne doit contenir que des scripts, Jupyter Notebooks ou fichiers de configuration. Tu dois plutôt mettre les données sur un système de stockage tiers (Google Drive, OneDrive, AWS S3) et faire référence à ces données depuis ton dépôt Git.

