<div align="center">

# 🔧 ETL Outage Backfill Orchestrator

**Detects data gaps caused by Oracle / MySQL / Tableau outages and automatically backfills the missing window — end to end.**

![Python](https://img.shields.io/badge/Python-3.8+-blue?style=flat-square&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Oracle](https://img.shields.io/badge/Oracle-MES-F80000?style=flat-square&logo=oracle&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-Target-4479A1?style=flat-square&logo=mysql&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-Dashboard-E97627?style=flat-square&logo=tableau&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

</div>

---

## ⚡ The Problem

Airflow DAGs call Python scripts that read **Oracle MES tables** via `bin/*.sql` files and write transformed data to **MySQL** — which then feeds **Tableau dashboards** for business reporting.

> When Oracle, MySQL, or Tableau experiences a server outage, data silently vanishes for that window.  
> **No detection. No recovery. No one notices until a stakeholder asks why numbers look wrong.**

---

## 🔄 Pipeline Flow

```
Oracle MES  ──►  Airflow DAGs  ──►  MySQL  ──►  Tableau
(Source)        (Python + SQL)     (Target)    (Dashboards)
```

---

## 🛠️ How It Works

| Step | Phase | What happens |
|------|-------|-------------|
| **1** | 🔍 Discovery | Walks DAG, script, and SQL directories to build a full dependency map: DAG → script → SQL → Oracle tables → MySQL tables → Tableau dashboards |
| **2** | 📊 Gap check | Queries Oracle and MySQL for zero-row windows within the outage period to confirm missing data |
| **3** | 🔁 Backfill | Re-triggers only the affected DAGs in parallel, with configurable retries and the exact window passed as parameters |
| **4** | 🖥️ Tableau refresh | Triggers a server-side data source refresh via Tableau REST API so dashboards self-heal |

---

## 📁 Directory Structure

```
dag_and_scripts/
├── dags/               ← Airflow DAG definitions (.py)
└── scripts/
    ├── *.py            ← ETL Python scripts
    └── bin/
        └── *.sql       ← SQL files called by scripts
```

---

## 🚀 Usage

```bash
# 1. Map the full pipeline (no credentials needed)
python outage_backfill_orchestrator.py discover

# 2. Check for data gaps in an outage window
python outage_backfill_orchestrator.py check \
  --start "2024-06-01 02:00" --end "2024-06-01 06:00"

# 3. Backfill only — no Tableau refresh
python outage_backfill_orchestrator.py backfill \
  --start "2024-06-01 02:00" --end "2024-06-01 06:00"

# 4. Full recovery: discover → check → backfill → Tableau refresh
python outage_backfill_orchestrator.py full \
  --start "2024-06-01 02:00" --end "2024-06-01 06:00"

# Preview without executing anything
python outage_backfill_orchestrator.py full \
  --start "2024-06-01 02:00" --end "2024-06-01 06:00" --dry-run
```

### Trigger modes

```bash
# Run scripts directly (default)
--mode subprocess

# Use Airflow CLI
--mode airflow_cli

# Use Airflow REST API
--mode airflow_api
```

---

## ⚙️ Installation

```bash
# Install dependencies
pip install -r requirements_backfill.txt

# Copy and fill in your credentials
cp backfill_config.json my_config.json
# Edit: oracle.dsn, oracle.user, mysql.host, tableau.token_value, etc.

# Run
python outage_backfill_orchestrator.py full \
  --config my_config.json \
  --start "2024-06-01 02:00" \
  --end   "2024-06-01 06:00"
```

---

## ✅ Features

- 🔍 **Auto-discovery** — zero config needed for pipeline mapping; reads your existing files
- 📊 **Dual-source gap detection** — checks Oracle and MySQL independently
- ⚡ **Parallel backfills** — configurable worker count for fast recovery
- 🔁 **Auto retry** — configurable attempts and delay per DAG
- 👁️ **Dry-run mode** — preview exactly what will run without touching anything
- 📝 **JSON reports** — timestamped audit trail in `reports/` for every run
- 📧 **Email alerts** — optional SMTP notification on completion
- 🛡️ **Graceful degradation** — works without optional drivers (`cx_Oracle`, `tableauserverclient`) installed

---

## 📋 Requirements

| Package | Purpose | Required |
|---------|---------|----------|
| `cx_Oracle` | Oracle MES connectivity | For Oracle checks |
| `mysql-connector-python` | MySQL connectivity | For MySQL checks |
| `tableauserverclient` | Tableau REST API | For dashboard refresh |

> The script runs in discovery + dry-run mode even without database drivers installed.

---

## 📄 License

MIT — see [LICENSE](LICENSE) for details.
