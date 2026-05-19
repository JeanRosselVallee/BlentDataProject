# Documentation Technique & Opérationnelle : Pipeline ETL Jeux-Vidéo

<img src="./image_infographics.png" alt="Schéma d'architecture" width="80%">

## 1. Présentation générale
Ce document définit l'architecture, la configuration et l'exploitation du pipeline ETL automatisé. L'objectif est d'extraire quotidiennement les avis bruts stockés sur une base NoSQL, d'identifier les tendances de la communauté, et d'alimenter un Data Warehouse (DWH) relationnel.

### Objectifs Business
*   **Optimisation du catalogue** : Mettre en avant sur la page d'accueil et dans les campagnes de communication (newsletters, réseaux sociaux) les jeux les mieux notés.
*   **Fraîcheur des données** : Historiser jour par jour les 15 jeux les mieux notés en se basant exclusivement sur les avis des 6 derniers mois.

## 2. Cahier des charges

### 2.1 Besoins Métiers & Règles de Gestion
*   **Fenêtre Glissante** : Exclusion stricte de tout avis ayant plus de 6 mois d'antériorité par rapport à la date d'exécution.
*   **Top 15** : Calcul quotidien basé sur la note moyenne et le volume d'avis pour extraire exactement les 15 meilleures références.
*   **Idempotence & Unicité** : Tolérance zéro pour les doublons. Si le pipeline s'exécute plusieurs fois pour la même journée, les anciennes données de cette journée doivent être écrasées et remplacées (**Stratégie Upsert / Replace**).

### 2.2 Spécifications Techniques des Données
Les données brutes proviennent d'un flux JSON compressé intégré dans MongoDB Atlas.

*   **Source Flux (fichier JSON)**

    Chaque entrée représente un avis client avec les champs suivants :
    *   **reviewerID** : Identifiant unique de l'utilisateur.
    *   **verified** : Booléen indiquant si l'achat est vérifié (critère de filtrage ETL).
    *   **asin** : Identifiant unique du produit (Amazon Standard Identification Number).
    *   **reviewerName** : Nom ou pseudo de l'auteur.
    *   **vote** : Nombre de votes "utiles" sur l'avis.
    *   **style** : Dictionnaire décrivant le format (ex: "Digital Download").
    *   **reviewText** : Contenu textuel de l'avis (supprimé lors de l'ingestion pour optimiser le stockage).
    *   **overall** : Note numérique de 1 à 5.
    *   **summary** : Titre ou résumé de l'avis.
    *   **unixReviewTime** : Timestamp Unix (utilisé pour le calcul de la fenêtre glissante de 6 mois).
    *   **reviewTime** : Date formatée (ex: "05 22, 2024").
    *   **image** : Liste d'URLs vers les images fournies par le client.

*   **Datalake (MongoDB No-SQL)**

    La collection doit contenir les informations requises par la cible DWH. 
    Il faut récupérer seulement les champs nécessaires du fichier.

*   **Cible DWH (SQL)**
    La table cible doit comporter ces colonnes :
    * Identifiant unique du jeu (ASIN)
    * Note moyenne calculée
    * Nombre d'utilisateurs ayant noté le jeu
    * Note la plus ancienne retenue sur la fenêtre
    * Note la plus récente enregistrée
    * Date d'exécution du calcul

## 3. Solution Technique & Architecture

### 3.1 Outils
*   **Langage** : Python 3.11, JavaScript
*   **Stockage** : MongoDB Atlas (Source), PostgreSQL Render (Cible).
*   **Calcul** : Pandas & MongoDB Aggregation Framework.
*   **Orchestration** : Apache Airflow (Local/Serveur).
*   **IDE** : Visual Studio Code
    *   **Extensions** : 
        *   MongoDB for VS Code
        *   SQLTools 
        *   SQLTools PostgreSQL/Cockroach Driver 
        *   SQLite Viewer
*   **Système d'exploitation** : Ubuntu-Linux. Airflow Scheduler requires a Unix-like OS (os.fork).


### 3.2 Arborescence du Projet
```text
BlentDataProject/
├── .venv_airflow/            # Environnement virtuel dédié à Airflow
├── .venv_etl/                # Environnement virtuel dédié au script ETL
├── airflow_home/             # Répertoire de travail Airflow (Logs, DB locale)
│   ├── airflow.db            # Base de données SQLite de l'orchestrateur
│   └── dags/                 # Dossier des DAGs Airflow
│       └── dag_task_etl.py   # Définition du DAG Airflow 2.3 & Doc intégrée
├── doc/                      # Spécifications techniques et fonctionnelles
├── queries/                  # Scripts de maintenance (Migration MongoDB/SQL)
├── scripts/
│   └── run_etl.py            # Script Python (Extraction, Calculs, Chargement)
├── src/                      # Coeur logique (lib_etl.py, config.py)
├── .env.template             # Modèle des secrets (MongoDB, Postgres)
├── .gitignore                # Exclusion des environnements, logs et .env
├── airflow.env               # Variables de chemins (PROJECT_ROOT, AIRFLOW_HOME)
├── airflow_run_etl.sh        # Script de pilotage (Serveurs & Modes d'exécution)
├── README.md                 # Vue d'ensemble et guide rapide
├── requirements_airflow.txt  # Dépendances de l'orchestrateur
└── requirements_etl.txt      # Dépendances du script ETL
```

### 3.3 Diagramme d'Architecture
Cette section détaille les flux de données et les interactions selon les différents profils et besoins métiers.

#### 1. Ingestion et Préparation (Profil : Développeur)
*    **Objectif** : Initialiser le Datalake avec des données exploitables pour le développement.  
*    **Processus** : Chargement du fichier JSON source via la fonction `seed_datalake` suivie d'un postdatage des documents pour simuler des avis récents (fenêtre de 6 mois).
*    *Cf. Schéma 1 : Flux d'ingestion et script de postdatage*

#### 2. Exécution Manuelle du Pipeline (Profil : Développeur)
*   **Objectif** : Validation technique unitaire ou test de performance.  
*   **Processus** : Lancement direct du script `run_etl.py` via CLI avec les arguments `--scan_date` et `--platform`.
*   *Cf. Schéma 2 : Pipeline d'extraction, transformation et chargement direct*

#### 3. Orchestration et Déploiement (Profil : Administrateur)
*   **Objectif** : Gestion de l'infrastructure et automatisation de la production.  
*   **Composants** : Script `airflow_run_etl.sh` pilotant 2 serveurs (Webserver & Scheduler) et la base `airflow.db`.  
*   **Modes** : Daily Schedule (Automatique), Test (Unitaire), Backfill (Historique).
*   *Cf. Schéma 3 : Architecture d'orchestration Airflow et modes d'exécution*

#### 4. Exploitation de la Donnée (Profil : Analystes & Décideurs)
*   **Objectif** : Consultation des tendances et aide à la décision.  
*   **Flux** : DWH PostgreSQL (Render) ➔ Requêtes SQL ➔ Rapports / Newsletters / Interface Web.
*   *Cf. Schéma 4 : Flux de consommation finale des données SQL*

<img src="./schema_architecture.png" alt="Schéma d'architecture" width="50%">


### 3.4 Schéma de données

**Datalake (MongoDB No-SQL) :**
La collection contient les informations requises par la cible DWH dans ces colonnes :
| Colonne | Type | Description |
| :--- | :--- | :--- |
| `asin` | str | Identifiant unique du produit. |
| `reviewerID` | str | Identifiant unique de l'utilisateur. |
| `overall` | float | Note numérique de 1 à 5. |
| `verified` | bool | Booléen indiquant si l'achat est vérifié. |
| `unixReviewTime` | int | Timestamp Unix. |
| `reviewTime` | str | Date formatée. |

**DWH :**
La table cible `daily_snapshot` possède le schéma suivant pour garantir l'historisation et l'idempotence :
| Colonne | Type | Description |
| :--- | :--- | :--- |
| `product_id` | VARCHAR(50) | Identifiant unique du jeu (ASIN). |
| `snapshot_date` | DATE | Date d'exécution du calcul (Clé Primaire avec product_id). |
| `nb_reviews` | INTEGER | Nombre d'avis collectés sur 6 mois. |
| `average_rating` | NUMERIC(3,2) | Note moyenne (1.00 à 5.00). |
| `oldest_rating` | NUMERIC(3,2) | Note du premier avis de la période. |
| `newest_rating` | NUMERIC(3,2) | Note du dernier avis de la période. |


### 3.5 Matrice de traçabilité
| Segment d'Exigence | Exigence Spécifique | Script / Localisation (sous RootDir) | Statut d'Implémentation & Commentaires |
| :--- | :--- | :--- | :--- |
| **Données Source** | Données brutes dans MongoDB | `src/lib_etl.py` | Implémenté. `connect_mongo` et `extract_and_transform` utilisent `pymongo` pour interroger la collection source. |
| **Data Warehouse** | Compatible SQL (PostgreSQL) | `src/lib_etl.py` | Implémenté. Utilise `sqlalchemy` et `psycopg2` (via DSN) pour se connecter à PostgreSQL (configuré pour Render). |
| **Filtrage des Données** | Seuls les avis des 6 derniers mois | `src/lib_etl.py` | Implémenté. `get_timeframe_start` calcule le delta de 6 mois, et `extract_and_transform` utilise `$match` avec `unixReviewTime`. |
| **Agrégation** | Top 15 des jeux les mieux notés | `src/lib_etl.py` | Implémenté. Le pipeline utilise `$limit: top_n` (par défaut 15). La logique trie par `average_rating` et `nb_reviews`. |
| **Schéma des Données** | ID Produit, Note Moyenne, Compte, Note la plus Ancienne/Récente | `src/lib_etl.py` | Implémenté. `init_dwh` crée la table `reviews` avec les colonnes : `product_id`, `nb_reviews`, `average_rating`, `oldest_rating`, `newest_rating`. |
| **Idempotence** | Gérer les doublons / Remplacer les valeurs existantes | `src/lib_etl.py` | Implémenté. `upsert_dwh` effectue un `DELETE` pour la `snapshot_date` spécifique avant d'effectuer un `append`. |
| **Logique du Pipeline** | Script Python pour l'ETL | `scripts/run_etl.py` | Implémenté. Le script point d'entrée orchestre les phases d'Extraction, Transformation et Chargement. |
| **Automatisation** | Outil d'Orchestration / Planification | `airflow_run_etl.sh` | Implémenté. Le script Shell gère la configuration de l'environnement Airflow, la gestion des serveurs et le backfill. |
| **Intégrité des Données** | Utiliser uniquement les utilisateurs vérifiés | `src/lib_etl.py` | Implémenté. Le pipeline d'agrégation MongoDB filtre sur `verified: True`. |

**Bilan :**
*   **Gestion des Dates** : La transition du jeu de données hérité (2017) vers un contexte "actuel" a été correctement gérée en utilisant le script `queries/datalake/change_dates.mongodb.js` afin de garantir que la logique de fenêtre glissante de 6 mois retourne effectivement des données pendant le développement.
*   **Cohérence du Schéma** : La fonction `init_dwh` dans `src/lib_etl.py` inclut une `PRIMARY KEY (product_id, snapshot_date)`. Cela correspond parfaitement à l'exigence d'éviter les doublons tout en permettant le suivi historique du même jeu sur différentes journées.


### 3.6 Choix d'amélioration
1.  **Performance** : L'utilisation de `aggregate` côté MongoDB réduit le volume de données transférées vers Python.
2.  **Robustesse** : Utilisation de SQL Alchemy avec gestion de transactions (`db_dwh.begin()`) pour garantir l'intégrité des chargements.
3.  **Sécurité** : Configuration par variables d'environnement (dotenv).

## 4. Guides

### 4.1 Guide de Déploiement (Administrateur)

#### Étape 1 : Préparation des Infrastructures Cloud
1.  **MongoDB Atlas (Source) :**
    *   Créer un Cluster (Shared/Gratuit).
    *   Dans **Network Access**, ajouter l'IP du serveur (ou `0.0.0.0/0` pour le test).
    *   Dans **Database Access**, créer un utilisateur avec les droits `readWrite` (nécessaire pour le seeding initial).
    *   Récupérer l'URI de connexion (`mongodb+srv://...`).
2.  **Render Postgres (DWH) :**
    *   Créer une nouvelle instance **PostgreSQL**.
    *   Créer un utilisateur
    *   Noter l'**External Connection String**.

#### Étape 2 : Clonage et Configuration logicielle
```bash
# 1. Cloner le projet
git clone https://github.com/votre-compte/BlentDataProject.git
cd BlentDataProject

# 2. Configurer les secrets
cp .env.template .env
# Éditer .env avec vos accès MongoDB et Postgres

# 3. Créer les environnements virtuels
python3.11 -m venv .venv_airflow
python3.13 -m venv .venv_etl

# 4. Installer les dépendances
source .venv_airflow/bin/activate && pip install -r requirements_airflow.txt && deactivate
source .venv_etl/bin/activate && pip install -r requirements_etl.txt && deactivate
```

#### Étape 3 : Mise en service d'Airflow et Initialisation
Exécuter le script de pilotage pour démarrer l'écosystème :
```bash
chmod +x airflow_run_etl.sh
./airflow_run_etl.sh
```
Ce script automatise :
1.  La création du répertoire `airflow_home` et de la base `airflow.db`.
2.  La création de l'utilisateur `admin`.
3.  Le lancement du **Scheduler** et du **Webserver** (Port 8080) en mode démon.

### 4.2 Guide du Développeur (Usage manuel)

#### Lancement du script ETL en direct
Le développeur peut tester le pipeline sans passer par l'interface Airflow :
```bash
source .venv_etl/bin/activate

# Exécution pour la date du jour
python scripts/run_etl.py

# Exécution pour une date spécifique (format ISO)
python scripts/run_etl.py --scan_date 2024-05-20 --platform Terminal
```

#### Initialisation automatique des données
Le script `run_etl.py` gère l'initialisation au premier lancement :
*   **MongoDB (Seeding) :** Si la collection est vide, la fonction `seed_datalake` injecte automatiquement les données depuis le fichier source JSON.
*   **PostgreSQL (DDL) :** La fonction `init_dwh` crée automatiquement la table `daily_snapshot` et ses index si elle n'existe pas.

#### Maintenance des données (Postdatage)
Si les données sources sont trop anciennes pour le calcul des 6 mois glissants :
*   Utiliser l'extension MongoDB de VS Code
*   Ouvrir le fichier `queries/datalake/change_dates.mongodb.js`
*   Cliquer sur l'icône `Play`


### 4.3 Pour le développeur

**Diagramme de flux logique** :
1.  **Extract** : Requête MongoDB avec filtre sur `unixReviewTime` >= (J - 6 mois) et `verified: true`.
2.  **Transform** : 
    *   Tri chronologique.
    *   Groupement par `asin`.
    *   Calcul de la moyenne et récupération des notes aux bornes (`$first`, `$last`).
    *   Tri par note moyenne descendante et volume d'avis.
    *   Limitation aux 15 premiers résultats.
3.  **Load** :
    *   Ouverture transaction SQL.
    *   Suppression des données existantes pour la `snapshot_date`.
    *   Insertion du nouveau DataFrame.

**Algorithme d'agrégation** :
Le coeur du calcul réside dans le pipeline d'agrégation MongoDB au sein de `extract_and_transform`. Il combine filtrage, tri et calcul statistique en une seule opération côté serveur.

```python
# Extrait du pipeline
pipeline = [
    {"$match": {"unixReviewTime": {"$gte": timeframe_start}, "verified": True}},
    {"$sort": {"unixReviewTime": 1}},
    {"$group": {
        "_id": "$asin",
        "nb_reviews": {"$sum": 1},
        "average_rating": {"$avg": "$overall"},
        "oldest_rating": {"$first": "$overall"},
        "newest_rating": {"$last": "$overall"}
    }},
    {"$sort": {"average_rating": -1, "nb_reviews": -1}},
    {"$limit": 15}
]
```

### 4.4 Monitoring et Exploitation


#### Orchestrateur __Airflow__ :
*   **Modes d'exécution :**
    *   **Lancement des serveurs** : `./airflow_run_etl.sh` (sans argument).
    *   **Test sur une date unique** : Exécuter `./airflow_run_etl.sh YYYY-MM-DD` (ex: `./airflow_run_etl.sh 2024-05-20`). Cela lance un `airflow tasks test` pour vérifier la logique sans modifier l'état du scheduler.
    *   **Backfill** : Pour rattraper une période, utiliser `./airflow_run_etl.sh <start_date> <end_date>`.
        *   *Attention* : La `start_date` minimale supportée par le script est cofigurée dans `airflow.env`. Assurez-vous que les données sources existent dans MongoDB pour la période choisie.

*   **Monitoring :**
    *   **Interface Web :** Accéder à `http://localhost:8080`. 
        *   **Tableau de bord :**Activer le DAG `daily_scan`.

#### Vérification DWH :
Requête pour vérifier le Top 15 du dernier run ou d'une autre date.
```sql
SELECT * FROM public.daily_snapshot 
WHERE snapshot_date = <TARGET_DATE YYYY-MM-DD>;
```

---
*Document généré le : 2026-05-22*
