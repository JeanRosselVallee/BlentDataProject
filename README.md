## Blent Data Project

Daily ETL that reads reviews from MongoDB (Atlas), computes the **Top 15** best-rated games using only the **last 6 months** of reviews, and upserts the daily snapshot into a PostgreSQL Data Warehouse (Render).

<img src="./doc/image_infographics.png" alt="Schéma d'architecture" width="80%">


### Technical & Operational Documentation

Cf. [documentation_technique](./doc/doc_en.md). It defines the architecture, configuration, and operation of the automated ETL pipeline. It details the technical specifications, data flows, and operational guides for the project.

<img src="./doc/schema_architecture.png" alt="Schéma d'architecture" width="50%">


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
├── queries/                  # Maintenance scripts (MongoDB/SQL migrations)
├── scripts/
│   └── run_etl.py            # Python script (Extraction, Calculations, Loading)
├── src/                      # Core logic (lib_etl.py, config.py)
├── .env.template             # Secrets template (MongoDB, Postgres)
├── .gitignore                # Exclusions for virtual environments, logs, and .env
├── airflow.env               # Path environment variables (PROJECT_ROOT, AIRFLOW_HOME)
├── airflow_run_etl.sh        # Control script (Servers & execution modes)
├── README.md                 # Overview and quick-start guide
├── requirements_airflow.txt  # Orchestrator dependencies
└── requirements_etl.txt      # ETL script dependencies
```
