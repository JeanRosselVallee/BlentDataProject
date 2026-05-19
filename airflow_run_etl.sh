#!/bin/bash
# airflow_run_etl.sh

# Exit immediately on failure
set -e


# Function: print timestamped messages 
print() {
    echo "$(date "+%Y-%m-%d %H:%M:%S") INFO - $1"
}


# --- VALIDATE PARAMETERS & DETECT AIRFLOW MODE ---

# Get Airflow Mode
if [ $# -eq 0 ]; then  # 0 parameters => Boot servers for Daily Schedule
    mode="servers_boot"
elif [ $# -eq 1 ]; then  # 1 parameter => Test Mode
    mode="test"
    scan_date="$1"
elif [ $# -eq 2 ]; then  # 2 parameters => Backfill Mode
    mode="backfill"
    start_date="$1"
    end_date="$2"
else  # Any other count exits
    print "❌ Error: Invalid number of parameters provided (got $#)."
    SCRIPT=$(basename "$0")    
    print "Usage syntax:"
    print "  ./${SCRIPT} <scan_date>   -> (Runs on a Single Date YYYY-MM-DD)"
    print "  ./${SCRIPT} <start_date> <end_date>  -> (Backfills a period)"
    exit 1
fi

# Check Parameters' Format & Values
# Date YYYY-MM-DD: 2000<=Year<=2039 & 01<=Month<=12 & 01<=Day<=31
date_format="^20([0-3][0-9])-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$"
for parameter in "$@" ; do
    if [[ ! "${parameter}" =~ ${date_format} ]];  # "=~": RegEx matching operator 
    then
        print "❌ Error: wrong date value or format (expected YYYY-MM-DD), got '${parameter}'."
        exit 1
    fi
done


# Get Project Root dir & Airflow Home dir
source airflow.env


# Activate Airflow environment
cd "$PROJECT_ROOT"
source .venv_airflow/bin/activate


# --- 1. ONE-SHOT METADATA DB CREATION ---
DB_FILE="$AIRFLOW_HOME/airflow.db"
if [ ! -f "$DB_FILE" ]; then
    print "⚙️ 1st-time launch: initializing Airflow Metadata DB..."
    mkdir -p "$AIRFLOW_HOME"
    airflow db migrate
    print "✅ DB is ready."
fi


# --- 2. CREATE AN ADMIN USER ---
print "👤 Checking for Admin user..."
# In case of nonexistent user
if ! airflow users list | grep -q "admin"; then
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname Dev \
        --role Admin \
        --email admin@example.com \
        --password admin
    print "✅ User 'admin' created successfully (Pass:'admin')."
fi


# --- 3. START AIRFLOW SERVERS ---

# Check & Start servers Scheduler & Web
for airflow_param in "scheduler" "webserver --port 8080"; do
    server_name=`echo ${airflow_param} | cut -d " " -f 1`

    # Start server if stopped
    ps_count=$(pgrep -f "airflow ${server_name}" | wc -l)  # get count of ps
    if [ "$ps_count" -eq 0 ]; then  # Case of no running ps
        print "🚀 Removing file locks & ${server_name} launching..."
        rm -f "$AIRFLOW_HOME/airflow-${server_name}.pid"
        airflow ${airflow_param} -D
        sleep 2  # Delay to free up network ports
    fi

    # Check server is running
    ps_count=$(pgrep -f "airflow ${server_name}" | wc -l)
    if [ "$ps_count" -eq 0 ]; then  
        print "❌ ${server_name} could not be launched."
        exit 1
    fi
done

print "✨ Airflow is live! Access the UI at http://localhost:8080"
print "💡 COMMAND TO STOP AIRFLOW SERVERS"
print "  pkill -f \"airflow\" && rm -f \${AIRFLOW_HOME}/*.pid"
if [ "$mode" == "servers_boot" ]; then exit 0; fi


# --- 4. AIRFLOW RUNS TARGET TASK ---
print "🚀 Launching Airflow:"
set +e  # allows output of airflow help in case of missing parameters

if [ "$mode" == "test" ]; then
    print "🚀 Running task test for single date ${scan_date}:"
    print "   airflow tasks test daily_scan task_run_etl ${scan_date}"
    airflow tasks test daily_scan task_run_etl "$scan_date"

elif [ "$mode" == "backfill" ]; then
    print "🔄 Running historical backfill from ${start_date} to ${end_date}:"
    print "airflow dags backfill --start-date ${start_date} --end-date ${end_date} daily_scan"
    airflow dags backfill --start-date "$start_date" --end-date "$end_date" daily_scan
fi

