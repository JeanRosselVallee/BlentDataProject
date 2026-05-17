#!/bin/bash
# launch_airflow_etl.sh
# Syntax :
# chmod +x ./launch_airflow_etl.sh
# ./launch_airflow_etl.sh tasks test daily_scan task_run_etl YYYY-MM-DD

# Exit immediately if any command fails
set -e


# Get Project Root dir & Airflow Home dir
source airflow.env


# Activate Airflow environment
cd "$PROJECT_ROOT"
source .venv_airflow/bin/activate


# One-shot metadata DB creation
DB_FILE="$AIRFLOW_HOME/airflow.db"
if [ ! -f "$DB_FILE" ]; then
    echo "⚙️ 1st-time launch: initializing Airflow Metadata DB..."
    mkdir -p "$AIRFLOW_HOME"
    airflow db migrate
    echo "✅ DB is ready."
fi

# Create an Admin User
echo "👤 Checking for Admin user..."
# In case of nonexistent user
if ! airflow users list | grep -q "admin"; then
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname Dev \
        --role Admin \
        --email admin@example.com \
        --password admin
    echo "✅ User 'admin' created successfully (Pass:'admin')."
fi


# Run Airflow Scheduler & Web Server

# Restart Airflow Scheduler
if [ -f "$AIRFLOW_HOME/airflow-scheduler.pid" ]; then
    echo "Stopping existing Scheduler..."
    pkill -f "airflow scheduler" 2>/dev/null
    rm "$AIRFLOW_HOME/airflow-scheduler.pid"
    sleep 2  # Delay to free up network ports
fi
echo "🧠 Starting Airflow Scheduler in the background..."
airflow scheduler -D

# Restart Airflow Web Server
if [ -f "$AIRFLOW_HOME/airflow-webserver.pid" ]; then
    echo "Stopping existing Web Server..."
    pkill -f "airflow webserver" 2>/dev/null
    rm "$AIRFLOW_HOME/airflow-webserver.pid"
    sleep 2  # # Delay to free up network ports
fi
echo "🚀 Starting Airflow Web Server in the background..."
airflow webserver --port 8080 -D
echo "✨ Airflow is live! Access the UI at http://localhost:8080"


# Forward arguments to target script
if [ $# -eq 0 ]; then
    echo "⚠️ No arguments provided. Use standard airflow commands:"
    SCRIPT=$(basename "$0")    
    echo "./${SCRIPT} tasks test daily_scan task_run_etl YYYY-MM-DD"
else
    echo "🚀 Launching Airflow:"
    echo "airflow ${@}"
    airflow "$@"  # forward all arguments
fi
