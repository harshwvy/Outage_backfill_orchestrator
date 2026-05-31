Problem statement

ETL Pipeline Outage Detection & Automated Backfill Orchestrator
A data engineering solution to detect, diagnose, and automatically recover data gaps caused by server outages across the Oracle → MySQL → Tableau pipeline.

data sources

Oracle MES

target store

MySQL DB

visualization

Tableau

orchestration

Apache Airflow

The core problem

The ETL pipeline runs Python scripts via Airflow DAGs on a scheduled basis. Each script reads from Oracle MES tables using SQL files stored in a bin/ directory, then writes transformed data to MySQL — which feeds Tableau dashboards.

When Oracle, MySQL, or Tableau experiences a server outage, no data loads for that window. The result: silent data gaps in dashboards with no automated recovery. Engineers must manually identify the affected pipelines and re-run scripts — a slow, error-prone process.

What the solution does

1
Auto-discovery

Walks the DAG, script, and SQL directories to build a full dependency map — linking every Airflow DAG to its Python scripts, SQL files, Oracle source tables, MySQL target tables, and downstream Tableau dashboards.

2
Gap detection

For a given outage window, queries each Oracle and MySQL table for row counts. Zero rows in the window = confirmed data gap. Reports the exact tables, time range, and likely root cause.

3
Targeted backfill

Only the affected DAGs are re-triggered — via direct subprocess, Airflow CLI, or Airflow REST API — with the exact outage start/end passed as parameters. Runs in parallel with configurable retries.

4
Tableau refresh

After successful backfills, triggers a server-side data source refresh on all affected Tableau dashboards via the Tableau REST API — ensuring end users see recovered data without manual intervention.

Directory structure expected

dag_and_scripts/
├── dags/               ← Airflow DAG definitions (.py)
└── scripts/
    ├── *.py            ← ETL Python scripts
    └── bin/
        └── *.sql       ← SQL files called by scripts
Usage

Map the full pipeline

python outage_backfill_orchestrator.py discover
Check data gaps for an outage window

python outage_backfill_orchestrator.py check --start "2024-06-01 02:00" --end "2024-06-01 06:00"
Full recovery — detect, backfill, and refresh Tableau

python outage_backfill_orchestrator.py full --start "2024-06-01 02:00" --end "2024-06-01 06:00"
Dry-run mode
Parallel backfills
Auto retry logic
JSON report output
Email alerting
Graceful degradation


*********************************************************************************************************************

Title: ETL Pipeline Outage Detection & Automated Backfill Orchestrator
Background:
The data platform runs scheduled Airflow DAGs that invoke Python ETL scripts. Each script reads from Oracle MES tables via SQL files stored in a bin/ directory, transforms the data, and loads it into MySQL — which then feeds Tableau dashboards used for business reporting.
Problem:
When Oracle, MySQL, or the Tableau server experiences an outage, data simply fails to load for that window. There is no automatic detection, no alerting on the gap, and no self-healing. Engineers must manually trace which DAGs were affected, identify the missing time range, re-run scripts in the right order, and then manually refresh Tableau — all of which is slow, inconsistent, and relies on someone noticing the gap in the first place.
Solution:
A single orchestrator script that:

Automatically maps every DAG → Python script → SQL file → Oracle table → MySQL table → Tableau dashboard
Accepts an outage window (--start / --end) and queries both Oracle and MySQL to confirm which tables have missing data
Re-triggers only the affected pipelines — in parallel, with retries — passing the exact window as backfill parameters
Triggers a Tableau data source refresh after recovery so dashboards self-heal end to end
Produces a timestamped JSON report and optional email alert for every run

Impact: Reduces mean time to recovery from hours of manual investigation to a single command, eliminates silent data gaps in dashboards, and provides a full audit trail of every backfill operation.

*********************************************************************************************************************
