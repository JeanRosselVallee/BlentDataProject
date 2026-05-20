## Blent Data Project
Daily ETL that reads reviews from MongoDB (Atlas), computes the **Top 15** best-rated games using only the **last 6 months** of reviews, and upserts the daily snapshot into a PostgreSQL Data Warehouse (Render).

<img src="./doc/img/infographics.png" alt="Schéma d'architecture" width="80%">


### Technical & Operational Documentation
Cf. Technical & Operational Documentation [in English](./doc/md/doc_en.md) or in [French](./doc/md/doc_fr.md). It defines the architecture, configuration, and operation of the automated ETL pipeline. It details the technical specifications, data flows, and operational guides for the project.


### Data Referencing
In accordance with architectural requirements, no raw data or large files are stored in this Git repository. The repository is strictly reserved for scripts (Python and Shell) and configuration files.
To link your GitHub environment to these remote resources, follow this referencing structure:

- **1. Source Storage (Initial Injection)**
 
    The raw source containing the video game rating data is hosted on an object storage bucket (Amazon S3). It serves as the single point of entry for the Datalake's seeding (initial population) process.
  - Resource Type: Raw data file (JSON compressed in ZIP format)
  - Direct Download Link: [games_ratings.zip](https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/5df5dd/games_ratings.zip)
  - Dedicated Configuration Variable: SEED_FILE (contains the source file URL for the initialization phase).
- **2. Datalake (NoSQL Collection)**
 
    The ingestion and intermediate staging layer (Datalake) is deployed on a Cloud cluster to ensure horizontal scalability when receiving JSON streams. DB credentials are provided on request.
  - Hosting Platform: MongoDB Atlas (Database-as-a-Service)
  - Administration Console: [MongoDB Atlas Dashboard](https://cloud.mongodb.com/)
  - Database: [db_datalake](https://cloud.mongodb.com/v2/69fd914f804694f3a1654b14#/explorer/69ff52e6597f58338f652fcb/db_datalake)
  - Required Environment Variables (.env): MONGO_URI, MONGO_DB, and MONGO_COLLECTION
- **3. Data Warehouse (SQL Table)**
 
    The final destination layer, containing the modeled structures ready for business intelligence (BI) analysis, is hosted on a managed relational DB instance. DB credentials are provided on request.
  - Hosting Platform: PostgreSQL on the Render Cloud platform
  - Administration Console: [Render Dashboard](https://dashboard.render.com/)
  - Database: [db_dwh](https://dashboard.render.com/d/dpg-d7v2rvfaqgkc73d3su9g-a)
  - Required Environment Variables (.env): POSTGRES_DSN and POSTGRES_TABLE_NAME


### Architecture

<img src="./doc/img/schema_whole.png" alt="Schéma d'architecture" width="50%">


### Project Directory Tree

```text
BlentDataProject/
├── .venv_airflow/            # Dedicated virtual environment for Airflow
├── .venv_etl/                # Dedicated virtual environment for the ETL script
├── airflow_home/             # Airflow working directory (Logs, local DB)
│   ├── airflow.db            # Orchestrator SQLite database
│   └── dags/                 # Airflow DAGs folder
│       └── dag_task_etl.py   # Airflow 2.3 DAG definition & integrated documentation
├── doc/                      # Technical and functional specifications
│   └── md/
│       ├── doc_en.md         # Technical Documentation (English)
│       ├── doc_fr.md         # Documentation Technique (Français)
│       └── spec_fr.md        # Initial specifications and requirements
├── queries/                  # Maintenance scripts (MongoDB/SQL migrations)
│   └── datalake/
│       └── change_dates.mongodb.js # Date shifting script for MongoDB
├── scripts/
│   └── run_etl.py            # Python script (Extraction, Calculations, Loading)
├── src/                      # Core logic
│   ├── config.py             # Configuration and environment loading
│   └── lib_etl.py            # ETL library and helper functions
├── .env.template             # Secrets template (MongoDB, Postgres)
├── .gitignore                # Exclusions for virtual environments, logs, and .env
├── airflow.env.template      # Path environment variables (PROJECT_ROOT, AIRFLOW_HOME)
├── airflow_run_etl.sh        # Control script (Servers & execution modes)
├── README.md                 # Overview and quick-start guide
├── requirements_airflow.txt  # Orchestrator dependencies
└── requirements_etl.txt      # ETL script dependencies
```
