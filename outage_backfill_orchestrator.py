#!/usr/bin/env python3
"""
=============================================================================
OUTAGE BACKFILL ORCHESTRATOR
=============================================================================
Detects data gaps caused by Oracle / MySQL / Tableau server outages,
identifies which DAGs → Python scripts → SQL files → dashboards are
affected, and backfills missing data for the exact outage window.

Directory assumptions (override via config or CLI):
    dag_and_scripts/
        dags/       ← Airflow DAG files
        scripts/    ← Python ETL scripts
            bin/    ← SQL files called by the scripts

Usage:
    python outage_backfill_orchestrator.py --help
    python outage_backfill_orchestrator.py discover
    python outage_backfill_orchestrator.py check   --start "2024-06-01 00:00" --end "2024-06-01 06:00"
    python outage_backfill_orchestrator.py backfill --start "2024-06-01 00:00" --end "2024-06-01 06:00"
    python outage_backfill_orchestrator.py full     --start "2024-06-01 00:00" --end "2024-06-01 06:00"
=============================================================================
"""

import os
import re
import ast
import sys
import json
import time
import logging
import argparse
import subprocess
import importlib.util
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── optional heavy deps (gracefully degrade if not installed) ──────────────
try:
    import cx_Oracle                    # pip install cx_Oracle
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False

try:
    import mysql.connector              # pip install mysql-connector-python
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import tableauserverclient as TSC   # pip install tableauserverclient
    TABLEAU_AVAILABLE = True
except ImportError:
    TABLEAU_AVAILABLE = False

try:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  (edit this section or supply a config.json)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG: Dict[str, Any] = {
    # ── paths ──────────────────────────────────────────────────────────────
    "base_dir":     "dag_and_scripts",
    "dags_dir":     "dag_and_scripts/dags",
    "scripts_dir":  "dag_and_scripts/scripts",
    "bin_dir":      "dag_and_scripts/scripts/bin",
    "log_dir":      "logs",
    "report_dir":   "reports",

    # ── Oracle source ──────────────────────────────────────────────────────
    "oracle": {
        "dsn":      "oracle-host:1521/ORCL",   # or TNS alias
        "user":     "mes_user",
        "password": "mes_password",
        # tables whose row-counts are used for availability checks
        "audit_tables": [],    # auto-discovered; or list explicit ones
        # column name that holds the load / event timestamp
        "timestamp_col": "CREATED_DATE"
    },

    # ── MySQL target ───────────────────────────────────────────────────────
    "mysql": {
        "host":     "mysql-host",
        "port":     3306,
        "database": "etl_db",
        "user":     "etl_user",
        "password": "etl_password",
        "audit_tables": [],
        "timestamp_col": "created_at"
    },

    # ── Tableau ────────────────────────────────────────────────────────────
    "tableau": {
        "server_url":  "https://tableau-server",
        "site_id":     "",
        "token_name":  "my_token",
        "token_value": "my_token_value",
    },

    # ── backfill behaviour ─────────────────────────────────────────────────
    "backfill": {
        "max_workers":       4,
        "retry_attempts":    3,
        "retry_delay_secs":  30,
        "dry_run":           False,
        "trigger_via":       "subprocess",  # "subprocess" | "airflow_cli" | "airflow_api"
        "airflow_api_url":   "http://localhost:8080",
        "airflow_api_user":  "airflow",
        "airflow_api_pass":  "airflow",
    },

    # ── alerting ───────────────────────────────────────────────────────────
    "alerts": {
        "enabled": False,
        "smtp_host": "smtp.example.com",
        "smtp_port": 587,
        "from_addr": "etl-alerts@example.com",
        "to_addrs":  ["oncall@example.com"],
        "subject_prefix": "[ETL-BACKFILL]"
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_dir: str) -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"backfill_orchestrator_{ts}.log"

    fmt = "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ]
    )
    logger = logging.getLogger("BackfillOrchestrator")
    logger.info(f"Log file: {log_file}")
    return logger


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SqlFile:
    path: str
    name: str
    source_tables: List[str] = field(default_factory=list)   # Oracle MES tables referenced
    target_tables: List[str] = field(default_factory=list)   # MySQL tables written to

@dataclass
class EtlScript:
    path: str
    name: str
    sql_files: List[SqlFile] = field(default_factory=list)
    oracle_tables: List[str] = field(default_factory=list)
    mysql_tables:  List[str] = field(default_factory=list)

@dataclass
class DagInfo:
    path: str
    dag_id: str
    scripts: List[EtlScript] = field(default_factory=list)
    schedule_interval: Optional[str] = None
    dashboards: List[str] = field(default_factory=list)

@dataclass
class DataGap:
    source: str          # "oracle" | "mysql"
    table: str
    start: datetime
    end: datetime
    missing_rows: int = 0
    note: str = ""

@dataclass
class BackfillResult:
    dag_id: str
    script: str
    status: str          # "success" | "failed" | "skipped" | "dry_run"
    start: datetime = field(default_factory=datetime.now)
    end: Optional[datetime] = None
    error: str = ""
    rows_backfilled: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    cfg = DEFAULT_CONFIG.copy()
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            override = json.load(f)
        _deep_merge(cfg, override)
    return cfg

def _deep_merge(base: dict, override: dict) -> None:
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERY ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class DiscoveryEngine:
    """
    Walks the DAG, script, and SQL directories to build a complete dependency
    map:   DAG → Python script(s) → SQL file(s) → Oracle tables (source)
                                                 → MySQL tables  (target)
    Then cross-references with Tableau workbooks/data-sources to map
    each pipeline to one or more dashboards.
    """

    # patterns to find SQL file references inside Python scripts
    SQL_REF_PATTERNS = [
        r"""['"]([\w/\\.-]+\.sql)['"]\s*""",
        r"""open\s*\(\s*['"]([\w/\\.-]+\.sql)['"]\s*\)""",
        r"""sql_file\s*=\s*['"]([\w/\\.-]+\.sql)['"]\s*""",
        r"""exec(?:ute)?_sql\s*\([^)]*['"]([\w/\\.-]+\.sql)['"]\s*[,)]""",
    ]

    # patterns to extract table names from SQL
    SQL_TABLE_PATTERNS = {
        "source": [
            r"FROM\s+([\w.\"]+)",
            r"JOIN\s+([\w.\"]+)",
        ],
        "target": [
            r"INSERT\s+(?:INTO\s+)?([\w.\"]+)",
            r"MERGE\s+INTO\s+([\w.\"]+)",
            r"UPDATE\s+([\w.\"]+)",
            r"TRUNCATE\s+TABLE\s+([\w.\"]+)",
        ]
    }

    # DAG patterns
    DAG_ID_PATTERN   = re.compile(r'dag_id\s*=\s*["\']([^"\']+)["\']')
    SCRIPT_PATTERNS  = [
        re.compile(r"""PythonOperator.*?python_callable\s*=\s*(\w+)""", re.S),
        re.compile(r"""BashOperator.*?bash_command\s*=\s*['"](.*?)['"]""", re.S),
        re.compile(r"""(?:subprocess|os\.system|exec).*?['"]([\w/\\.-]+\.py)['"]\s*"""),
    ]
    SCHEDULE_PATTERN = re.compile(r'schedule_interval\s*=\s*["\']([^"\']+)["\']')

    def __init__(self, cfg: Dict[str, Any], logger: logging.Logger):
        self.cfg    = cfg
        self.log    = logger
        self.dags_dir    = Path(cfg["dags_dir"])
        self.scripts_dir = Path(cfg["scripts_dir"])
        self.bin_dir     = Path(cfg["bin_dir"])

    # ── public entry point ─────────────────────────────────────────────────

    def discover_all(self) -> List[DagInfo]:
        self.log.info("=== DISCOVERY PHASE ===")
        dags = self._discover_dags()
        self.log.info(f"  Found {len(dags)} DAG(s)")
        for dag in dags:
            dag.scripts = self._discover_scripts_for_dag(dag)
            for script in dag.scripts:
                script.sql_files = self._discover_sql_for_script(script)
                for sf in script.sql_files:
                    sf.source_tables, sf.target_tables = self._parse_sql_tables(sf)
                script.oracle_tables = list({t for sf in script.sql_files for t in sf.source_tables})
                script.mysql_tables  = list({t for sf in script.sql_files for t in sf.target_tables})
            dag.dashboards = self._discover_dashboards_for_dag(dag)
        return dags

    # ── DAG discovery ──────────────────────────────────────────────────────

    def _discover_dags(self) -> List[DagInfo]:
        dags = []
        for f in self.dags_dir.rglob("*.py"):
            try:
                text = f.read_text(errors="ignore")
                m = self.DAG_ID_PATTERN.search(text)
                if not m:
                    continue
                dag_id = m.group(1)
                sched  = self.SCHEDULE_PATTERN.search(text)
                dags.append(DagInfo(
                    path=str(f),
                    dag_id=dag_id,
                    schedule_interval=sched.group(1) if sched else None
                ))
                self.log.debug(f"  DAG: {dag_id}  ({f.name})")
            except Exception as e:
                self.log.warning(f"  Could not parse {f}: {e}")
        return dags

    # ── script discovery ───────────────────────────────────────────────────

    def _discover_scripts_for_dag(self, dag: DagInfo) -> List[EtlScript]:
        scripts = []
        seen = set()
        text = Path(dag.path).read_text(errors="ignore")

        # 1. explicit .py references inside the DAG file
        for pat in [re.compile(r"""['"]([\w/\\.-]+\.py)['"]\s*""")]:
            for hit in pat.findall(text):
                candidate = self._resolve_script(hit)
                if candidate and str(candidate) not in seen:
                    seen.add(str(candidate))
                    scripts.append(EtlScript(path=str(candidate), name=candidate.stem))

        # 2. if nothing found, look for all .py in scripts_dir with same stem as dag_id
        if not scripts:
            for f in self.scripts_dir.rglob("*.py"):
                if dag.dag_id.replace("-", "_").lower() in f.stem.lower():
                    if str(f) not in seen:
                        seen.add(str(f))
                        scripts.append(EtlScript(path=str(f), name=f.stem))

        # 3. scan callable names referenced in BashOperator / PythonOperator
        for pat in self.SCRIPT_PATTERNS:
            for hit in pat.findall(text):
                hit = hit.strip().split()[0]   # first token only
                if hit.endswith(".py"):
                    candidate = self._resolve_script(hit)
                    if candidate and str(candidate) not in seen:
                        seen.add(str(candidate))
                        scripts.append(EtlScript(path=str(candidate), name=candidate.stem))

        if not scripts:
            self.log.warning(f"  No scripts found for DAG '{dag.dag_id}'")
        return scripts

    def _resolve_script(self, ref: str) -> Optional[Path]:
        """Try several locations to resolve a script reference."""
        candidates = [
            Path(ref),
            self.scripts_dir / ref,
            self.scripts_dir / Path(ref).name,
        ]
        for c in candidates:
            if c.exists():
                return c
        return None

    # ── SQL discovery ──────────────────────────────────────────────────────

    def _discover_sql_for_script(self, script: EtlScript) -> List[SqlFile]:
        sql_files = []
        seen = set()
        try:
            text = Path(script.path).read_text(errors="ignore")
        except Exception:
            return sql_files

        for pat_str in self.SQL_REF_PATTERNS:
            for hit in re.findall(pat_str, text, re.IGNORECASE):
                candidate = self._resolve_sql(hit)
                if candidate and str(candidate) not in seen:
                    seen.add(str(candidate))
                    sql_files.append(SqlFile(path=str(candidate), name=candidate.name))
                    self.log.debug(f"    SQL: {candidate.name}")

        # fallback: scan bin_dir for sql files that match script stem
        if not sql_files:
            for sf in self.bin_dir.rglob("*.sql"):
                if script.name.lower() in sf.stem.lower():
                    if str(sf) not in seen:
                        seen.add(str(sf))
                        sql_files.append(SqlFile(path=str(sf), name=sf.name))

        return sql_files

    def _resolve_sql(self, ref: str) -> Optional[Path]:
        candidates = [
            Path(ref),
            self.bin_dir / ref,
            self.bin_dir / Path(ref).name,
            self.scripts_dir / ref,
        ]
        for c in candidates:
            if c.exists():
                return c
        return None

    # ── SQL table extraction ───────────────────────────────────────────────

    def _parse_sql_tables(self, sf: SqlFile) -> Tuple[List[str], List[str]]:
        try:
            sql = Path(sf.path).read_text(errors="ignore").upper()
        except Exception:
            return [], []

        # strip comments
        sql = re.sub(r'--[^\n]*', ' ', sql)
        sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.S)

        sources, targets = set(), set()
        for pat in self.SQL_TABLE_PATTERNS["source"]:
            for m in re.finditer(pat, sql):
                t = m.group(1).strip('"').strip("'")
                if "." in t or len(t) > 2:
                    sources.add(t)
        for pat in self.SQL_TABLE_PATTERNS["target"]:
            for m in re.finditer(pat, sql):
                t = m.group(1).strip('"').strip("'")
                if "." in t or len(t) > 2:
                    targets.add(t)

        # remove SQL keywords that got captured
        KEYWORDS = {"WHERE", "SET", "VALUES", "SELECT", "WITH", "ON", "AS", "BY"}
        sources -= KEYWORDS
        targets -= KEYWORDS
        return sorted(sources), sorted(targets)

    # ── Tableau dashboard discovery ────────────────────────────────────────

    def _discover_dashboards_for_dag(self, dag: DagInfo) -> List[str]:
        """
        Heuristic: look for Tableau workbook/view names that match DAG-id or
        target-table names.  With a live Tableau connection we query the REST API.
        Without it we return placeholder names so the pipeline map is still useful.
        """
        all_mysql_tables = [
            t for s in dag.scripts for t in s.mysql_tables
        ]
        if not TABLEAU_AVAILABLE:
            return [f"dashboard_{dag.dag_id}"] if all_mysql_tables else []

        tcfg = self.cfg["tableau"]
        try:
            server = TSC.Server(tcfg["server_url"], use_server_version=True)
            auth   = TSC.PersonalAccessTokenAuth(
                tcfg["token_name"], tcfg["token_value"], site_id=tcfg["site_id"]
            )
            dashboards = []
            with server.auth.sign_in(auth):
                for wb in TSC.Pager(server.workbooks):
                    name_lower = wb.name.lower()
                    match = (
                        dag.dag_id.replace("_", " ").lower() in name_lower
                        or any(t.lower() in name_lower for t in all_mysql_tables)
                    )
                    if match:
                        dashboards.append(wb.name)
            return dashboards
        except Exception as e:
            self.log.warning(f"  Tableau discovery failed for {dag.dag_id}: {e}")
            return [f"dashboard_{dag.dag_id}"]


# ─────────────────────────────────────────────────────────────────────────────
# AVAILABILITY CHECKER
# ─────────────────────────────────────────────────────────────────────────────

class AvailabilityChecker:
    """
    For each DAG, queries Oracle (source) and MySQL (target) to detect rows
    that should have been loaded during the outage window but are missing.
    """

    def __init__(self, cfg: Dict[str, Any], logger: logging.Logger):
        self.cfg = cfg
        self.log = logger

    # ── public ─────────────────────────────────────────────────────────────

    def check_all(
        self,
        dags: List[DagInfo],
        start: datetime,
        end: datetime
    ) -> List[DataGap]:
        self.log.info("=== AVAILABILITY CHECK ===")
        self.log.info(f"  Window: {start} → {end}")
        gaps: List[DataGap] = []

        oracle_tables = list({t for d in dags for s in d.scripts for t in s.oracle_tables})
        mysql_tables  = list({t for d in dags for s in d.scripts for t in s.mysql_tables})

        if oracle_tables:
            gaps += self._check_oracle(oracle_tables, start, end)
        if mysql_tables:
            gaps += self._check_mysql(mysql_tables, start, end)

        if gaps:
            self.log.warning(f"  {len(gaps)} data gap(s) detected!")
            for g in gaps:
                self.log.warning(
                    f"  [{g.source.upper()}] {g.table}: "
                    f"~{g.missing_rows} rows missing  ({g.note})"
                )
        else:
            self.log.info("  No data gaps detected.")
        return gaps

    # ── Oracle ─────────────────────────────────────────────────────────────

    def _check_oracle(
        self, tables: List[str], start: datetime, end: datetime
    ) -> List[DataGap]:
        if not ORACLE_AVAILABLE:
            self.log.warning("  cx_Oracle not installed – skipping Oracle check")
            return self._mock_gaps("oracle", tables, start, end)

        ocfg = self.cfg["oracle"]
        gaps = []
        try:
            conn = cx_Oracle.connect(
                user=ocfg["user"],
                password=ocfg["password"],
                dsn=ocfg["dsn"]
            )
            cur = conn.cursor()
            ts_col = ocfg.get("timestamp_col", "CREATED_DATE")
            for table in tables:
                try:
                    cur.execute(
                        f"""SELECT COUNT(*) FROM {table}
                            WHERE {ts_col} BETWEEN :1 AND :2""",
                        [start, end]
                    )
                    count = cur.fetchone()[0]
                    if count == 0:
                        gaps.append(DataGap(
                            source="oracle", table=table,
                            start=start, end=end, missing_rows=count,
                            note=f"0 rows in window (outage suspected)"
                        ))
                    else:
                        self.log.info(f"  Oracle {table}: {count} rows present ✓")
                except Exception as e:
                    self.log.warning(f"  Oracle check failed for {table}: {e}")
                    gaps.append(DataGap(
                        source="oracle", table=table,
                        start=start, end=end, missing_rows=-1,
                        note=f"Query failed: {e}"
                    ))
            cur.close()
            conn.close()
        except Exception as e:
            self.log.error(f"  Could not connect to Oracle: {e}")
            return self._mock_gaps("oracle", tables, start, end, error=str(e))
        return gaps

    # ── MySQL ──────────────────────────────────────────────────────────────

    def _check_mysql(
        self, tables: List[str], start: datetime, end: datetime
    ) -> List[DataGap]:
        if not MYSQL_AVAILABLE:
            self.log.warning("  mysql-connector not installed – skipping MySQL check")
            return self._mock_gaps("mysql", tables, start, end)

        mcfg = self.cfg["mysql"]
        gaps = []
        try:
            conn = mysql.connector.connect(
                host=mcfg["host"], port=mcfg["port"],
                database=mcfg["database"],
                user=mcfg["user"], password=mcfg["password"]
            )
            cur = conn.cursor()
            ts_col = mcfg.get("timestamp_col", "created_at")
            for table in tables:
                try:
                    cur.execute(
                        f"SELECT COUNT(*) FROM `{table}` "
                        f"WHERE `{ts_col}` BETWEEN %s AND %s",
                        (start, end)
                    )
                    count = cur.fetchone()[0]
                    if count == 0:
                        gaps.append(DataGap(
                            source="mysql", table=table,
                            start=start, end=end, missing_rows=count,
                            note="0 rows in window (outage suspected)"
                        ))
                    else:
                        self.log.info(f"  MySQL {table}: {count} rows present ✓")
                except Exception as e:
                    self.log.warning(f"  MySQL check failed for {table}: {e}")
                    gaps.append(DataGap(
                        source="mysql", table=table,
                        start=start, end=end, missing_rows=-1,
                        note=f"Query failed: {e}"
                    ))
            cur.close()
            conn.close()
        except Exception as e:
            self.log.error(f"  Could not connect to MySQL: {e}")
            return self._mock_gaps("mysql", tables, start, end, error=str(e))
        return gaps

    # ── mock gaps (when drivers not installed) ─────────────────────────────

    @staticmethod
    def _mock_gaps(
        source: str, tables: List[str],
        start: datetime, end: datetime,
        error: str = "driver not installed"
    ) -> List[DataGap]:
        return [
            DataGap(
                source=source, table=t,
                start=start, end=end, missing_rows=-1,
                note=f"[MOCK] {error}"
            )
            for t in tables
        ]


# ─────────────────────────────────────────────────────────────────────────────
# BACKFILL ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class BackfillEngine:
    """
    Triggers the ETL scripts / DAGs for the affected pipelines to reprocess
    the missing data window.  Supports three trigger modes:
        subprocess    – run the Python script directly
        airflow_cli   – airflow dags backfill ...
        airflow_api   – POST to Airflow REST API
    """

    def __init__(self, cfg: Dict[str, Any], logger: logging.Logger):
        self.cfg  = cfg
        self.log  = logger
        self.bcfg = cfg["backfill"]

    # ── public ─────────────────────────────────────────────────────────────

    def backfill(
        self,
        dags: List[DagInfo],
        gaps: List[DataGap],
        start: datetime,
        end: datetime
    ) -> List[BackfillResult]:
        self.log.info("=== BACKFILL PHASE ===")

        # find DAGs that actually have gaps
        affected_tables = {g.table for g in gaps}
        affected_dags = [
            d for d in dags
            if any(
                t in affected_tables
                for s in d.scripts
                for t in (s.oracle_tables + s.mysql_tables)
            )
        ]

        if not affected_dags:
            self.log.info("  No affected DAGs – nothing to backfill.")
            return []

        self.log.info(f"  {len(affected_dags)} DAG(s) to backfill:")
        for d in affected_dags:
            self.log.info(f"    {d.dag_id}")

        results: List[BackfillResult] = []
        mode = self.bcfg.get("trigger_via", "subprocess")

        with ThreadPoolExecutor(max_workers=self.bcfg.get("max_workers", 4)) as pool:
            futures = {
                pool.submit(
                    self._trigger_dag, dag, start, end, mode
                ): dag
                for dag in affected_dags
            }
            for fut in as_completed(futures):
                dag = futures[fut]
                try:
                    res = fut.result()
                    results.append(res)
                except Exception as e:
                    results.append(BackfillResult(
                        dag_id=dag.dag_id,
                        script="",
                        status="failed",
                        error=str(e),
                        end=datetime.now()
                    ))

        # summary
        ok  = [r for r in results if r.status in ("success", "dry_run")]
        err = [r for r in results if r.status == "failed"]
        self.log.info(f"  Backfill complete: {len(ok)} succeeded, {len(err)} failed")
        return results

    # ── trigger dispatch ───────────────────────────────────────────────────

    def _trigger_dag(
        self, dag: DagInfo, start: datetime, end: datetime, mode: str
    ) -> BackfillResult:
        result = BackfillResult(
            dag_id=dag.dag_id, script="",
            status="pending", start=datetime.now()
        )

        if self.bcfg.get("dry_run", False):
            self.log.info(f"  [DRY-RUN] Would backfill {dag.dag_id} ({start} → {end})")
            result.status = "dry_run"
            result.end = datetime.now()
            return result

        for attempt in range(1, self.bcfg.get("retry_attempts", 3) + 1):
            try:
                if mode == "airflow_cli":
                    self._trigger_via_airflow_cli(dag, start, end)
                elif mode == "airflow_api":
                    self._trigger_via_airflow_api(dag, start, end)
                else:  # subprocess (default)
                    self._trigger_via_subprocess(dag, start, end, result)

                result.status = "success"
                result.end = datetime.now()
                self.log.info(f"  ✓ {dag.dag_id} backfilled successfully")
                return result

            except Exception as e:
                self.log.warning(
                    f"  Attempt {attempt} failed for {dag.dag_id}: {e}"
                )
                if attempt < self.bcfg.get("retry_attempts", 3):
                    time.sleep(self.bcfg.get("retry_delay_secs", 30))
                else:
                    result.status = "failed"
                    result.error  = str(e)
                    result.end    = datetime.now()
                    self.log.error(f"  ✗ {dag.dag_id} backfill FAILED after {attempt} attempts")

        return result

    # ── subprocess mode ────────────────────────────────────────────────────

    def _trigger_via_subprocess(
        self, dag: DagInfo, start: datetime, end: datetime, result: BackfillResult
    ) -> None:
        for script in dag.scripts:
            result.script = script.name
            cmd = [
                sys.executable, script.path,
                "--start", start.strftime("%Y-%m-%d %H:%M:%S"),
                "--end",   end.strftime("%Y-%m-%d %H:%M:%S"),
                "--backfill"
            ]
            self.log.info(f"  Executing: {' '.join(cmd)}")
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=3600
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"Script {script.name} exited {proc.returncode}:\n{proc.stderr}"
                )
            self.log.debug(f"  stdout: {proc.stdout[-500:]}")

    # ── Airflow CLI mode ───────────────────────────────────────────────────

    def _trigger_via_airflow_cli(
        self, dag: DagInfo, start: datetime, end: datetime
    ) -> None:
        cmd = [
            "airflow", "dags", "backfill",
            dag.dag_id,
            "--start-date", start.strftime("%Y-%m-%dT%H:%M:%S"),
            "--end-date",   end.strftime("%Y-%m-%dT%H:%M:%S"),
            "--reset-dagruns",
        ]
        self.log.info(f"  Airflow CLI: {' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if proc.returncode != 0:
            raise RuntimeError(f"airflow CLI failed: {proc.stderr}")

    # ── Airflow REST API mode ──────────────────────────────────────────────

    def _trigger_via_airflow_api(
        self, dag: DagInfo, start: datetime, end: datetime
    ) -> None:
        try:
            import urllib.request, base64
        except ImportError:
            raise RuntimeError("urllib not available")

        api_url  = self.bcfg.get("airflow_api_url", "http://localhost:8080")
        user     = self.bcfg.get("airflow_api_user", "airflow")
        password = self.bcfg.get("airflow_api_pass", "airflow")
        creds    = base64.b64encode(f"{user}:{password}".encode()).decode()

        payload = json.dumps({
            "dag_run_id": f"backfill_{dag.dag_id}_{int(time.time())}",
            "logical_date": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "conf": {
                "backfill_start": start.isoformat(),
                "backfill_end":   end.isoformat(),
            }
        }).encode()

        url = f"{api_url}/api/v1/dags/{dag.dag_id}/dagRuns"
        req = urllib.request.Request(
            url, data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {creds}"
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status not in (200, 201):
                raise RuntimeError(f"API returned {resp.status}")
        self.log.info(f"  DAG run triggered via Airflow API for {dag.dag_id}")


# ─────────────────────────────────────────────────────────────────────────────
# TABLEAU REFRESH
# ─────────────────────────────────────────────────────────────────────────────

class TableauRefresher:
    """After backfill, triggers a data-source refresh on all affected views."""

    def __init__(self, cfg: Dict[str, Any], logger: logging.Logger):
        self.cfg = cfg
        self.log = logger

    def refresh_dashboards(self, dags: List[DagInfo]) -> Dict[str, str]:
        dashboards = list({d for dag in dags for d in dag.dashboards})
        results: Dict[str, str] = {}

        if not TABLEAU_AVAILABLE:
            self.log.warning("  tableauserverclient not installed – skipping refresh")
            for d in dashboards:
                results[d] = "skipped (no client)"
            return results

        tcfg = self.cfg["tableau"]
        try:
            server = TSC.Server(tcfg["server_url"], use_server_version=True)
            auth   = TSC.PersonalAccessTokenAuth(
                tcfg["token_name"], tcfg["token_value"], site_id=tcfg["site_id"]
            )
            with server.auth.sign_in(auth):
                ds_all, _ = server.datasources.get()
                for ds in ds_all:
                    if any(d.lower() in ds.name.lower() for d in dashboards):
                        try:
                            server.datasources.refresh(ds)
                            results[ds.name] = "refreshed"
                            self.log.info(f"  Tableau refresh triggered: {ds.name} ✓")
                        except Exception as e:
                            results[ds.name] = f"failed: {e}"
                            self.log.warning(f"  Tableau refresh failed for {ds.name}: {e}")
        except Exception as e:
            self.log.error(f"  Tableau connection failed: {e}")
            for d in dashboards:
                results[d] = f"failed: {e}"

        return results


# ─────────────────────────────────────────────────────────────────────────────
# REPORTER
# ─────────────────────────────────────────────────────────────────────────────

class Reporter:
    def __init__(self, cfg: Dict[str, Any], logger: logging.Logger):
        self.cfg    = cfg
        self.log    = logger
        self.outdir = Path(cfg["report_dir"])
        self.outdir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        dags:       List[DagInfo],
        gaps:       List[DataGap],
        results:    List[BackfillResult],
        tab_refresh: Dict[str, str],
        start: datetime,
        end:   datetime,
    ) -> str:
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = self.outdir / f"backfill_report_{ts}.json"

        report = {
            "generated_at": datetime.now().isoformat(),
            "outage_window": {
                "start": start.isoformat(),
                "end":   end.isoformat(),
                "duration_minutes": int((end - start).total_seconds() / 60)
            },
            "pipeline_map": [
                {
                    "dag_id": d.dag_id,
                    "dag_file": d.path,
                    "schedule": d.schedule_interval,
                    "dashboards": d.dashboards,
                    "scripts": [
                        {
                            "name": s.name,
                            "path": s.path,
                            "oracle_tables": s.oracle_tables,
                            "mysql_tables":  s.mysql_tables,
                            "sql_files": [
                                {
                                    "name": sf.name,
                                    "path": sf.path,
                                    "source_tables": sf.source_tables,
                                    "target_tables": sf.target_tables,
                                }
                                for sf in s.sql_files
                            ],
                        }
                        for s in d.scripts
                    ],
                }
                for d in dags
            ],
            "data_gaps": [
                {
                    "source": g.source,
                    "table":  g.table,
                    "start":  g.start.isoformat(),
                    "end":    g.end.isoformat(),
                    "missing_rows": g.missing_rows,
                    "note":   g.note,
                }
                for g in gaps
            ],
            "backfill_results": [
                {
                    "dag_id":  r.dag_id,
                    "script":  r.script,
                    "status":  r.status,
                    "start":   r.start.isoformat(),
                    "end":     r.end.isoformat() if r.end else None,
                    "error":   r.error,
                    "rows_backfilled": r.rows_backfilled,
                }
                for r in results
            ],
            "tableau_refresh": tab_refresh,
            "summary": {
                "dags_total":       len(dags),
                "gaps_found":       len(gaps),
                "backfills_ok":     len([r for r in results if r.status in ("success","dry_run")]),
                "backfills_failed": len([r for r in results if r.status == "failed"]),
                "tableau_refreshed": len([v for v in tab_refresh.values() if v == "refreshed"]),
            }
        }

        with open(out, "w") as f:
            json.dump(report, f, indent=2)

        self._print_summary(report)
        self.log.info(f"  Report saved: {out}")

        if self.cfg["alerts"]["enabled"]:
            self._send_email(report, str(out))

        return str(out)

    def _print_summary(self, r: dict) -> None:
        s = r["summary"]
        sep = "─" * 60
        print(f"\n{sep}")
        print("  BACKFILL ORCHESTRATOR  –  SUMMARY")
        print(sep)
        print(f"  Outage window  : {r['outage_window']['start']}  →  {r['outage_window']['end']}")
        print(f"  Duration       : {r['outage_window']['duration_minutes']} minutes")
        print(f"  DAGs scanned   : {s['dags_total']}")
        print(f"  Data gaps      : {s['gaps_found']}")
        print(f"  Backfills OK   : {s['backfills_ok']}")
        print(f"  Backfills FAIL : {s['backfills_failed']}")
        print(f"  Tableau refresh: {s['tableau_refreshed']}")
        print(sep + "\n")

    def _send_email(self, report: dict, report_path: str) -> None:
        if not EMAIL_AVAILABLE:
            return
        acfg = self.cfg["alerts"]
        s    = report["summary"]
        subject = (
            f"{acfg['subject_prefix']} "
            f"Backfill complete – {s['backfills_ok']} OK / {s['backfills_failed']} FAIL"
        )
        body = f"""
Backfill Orchestrator Report
==============================
Outage: {report['outage_window']['start']} → {report['outage_window']['end']}
DAGs:   {s['dags_total']}
Gaps:   {s['gaps_found']}
OK:     {s['backfills_ok']}
FAIL:   {s['backfills_failed']}
Tableau refreshed: {s['tableau_refreshed']}

Full report: {report_path}
"""
        try:
            msg = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"]    = acfg["from_addr"]
            msg["To"]      = ", ".join(acfg["to_addrs"])
            msg.attach(MIMEText(body, "plain"))
            with smtplib.SMTP(acfg["smtp_host"], acfg["smtp_port"]) as srv:
                srv.starttls()
                srv.sendmail(acfg["from_addr"], acfg["to_addrs"], msg.as_string())
            self.log.info("  Alert email sent.")
        except Exception as e:
            self.log.warning(f"  Email failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Outage Backfill Orchestrator – detect & repair ETL data gaps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  discover   Build pipeline map (DAG → script → SQL → tables → dashboards)
  check      Check data availability for an outage window
  backfill   Backfill missing data for an outage window
  full       discover + check + backfill + tableau refresh (recommended)

Examples:
  python outage_backfill_orchestrator.py discover
  python outage_backfill_orchestrator.py check   --start "2024-06-01 00:00" --end "2024-06-01 06:00"
  python outage_backfill_orchestrator.py backfill --start "2024-06-01 00:00" --end "2024-06-01 06:00" --dry-run
  python outage_backfill_orchestrator.py full     --start "2024-06-01 00:00" --end "2024-06-01 06:00"
"""
    )
    parser.add_argument(
        "command", choices=["discover", "check", "backfill", "full"],
        help="Action to perform"
    )
    parser.add_argument("--config", help="Path to config JSON file")
    parser.add_argument("--start",  help="Outage start  (YYYY-MM-DD HH:MM)")
    parser.add_argument("--end",    help="Outage end    (YYYY-MM-DD HH:MM)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without executing")
    parser.add_argument("--mode",
                        choices=["subprocess", "airflow_cli", "airflow_api"],
                        default="subprocess",
                        help="How to trigger the backfill scripts")
    parser.add_argument("--log-dir",    default="logs")
    parser.add_argument("--report-dir", default="reports")
    return parser.parse_args()


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse datetime: '{s}'")


def main() -> None:
    args = parse_args()
    cfg  = load_config(args.config)

    # apply CLI overrides
    cfg["log_dir"]    = args.log_dir
    cfg["report_dir"] = args.report_dir
    if args.dry_run:
        cfg["backfill"]["dry_run"] = True
    cfg["backfill"]["trigger_via"] = args.mode

    logger = setup_logging(cfg["log_dir"])
    logger.info(f"Command : {args.command}")
    logger.info(f"Dry-run : {cfg['backfill']['dry_run']}")

    # ── parse window ───────────────────────────────────────────────────────
    start = end = None
    if args.command in ("check", "backfill", "full"):
        if not args.start or not args.end:
            logger.error("--start and --end are required for this command")
            sys.exit(1)
        start = _parse_dt(args.start)
        end   = _parse_dt(args.end)
        if start >= end:
            logger.error("--start must be before --end")
            sys.exit(1)

    # ── components ─────────────────────────────────────────────────────────
    discovery = DiscoveryEngine(cfg, logger)
    checker   = AvailabilityChecker(cfg, logger)
    backfiller = BackfillEngine(cfg, logger)
    refresher  = TableauRefresher(cfg, logger)
    reporter   = Reporter(cfg, logger)

    # ── execute ────────────────────────────────────────────────────────────
    dags = gaps = results = tab = []

    if args.command == "discover":
        dags = discovery.discover_all()
        reporter.generate(dags, [], [], {}, datetime.now(), datetime.now())

    elif args.command == "check":
        dags = discovery.discover_all()
        gaps = checker.check_all(dags, start, end)
        reporter.generate(dags, gaps, [], {}, start, end)

    elif args.command == "backfill":
        dags    = discovery.discover_all()
        gaps    = checker.check_all(dags, start, end)
        results = backfiller.backfill(dags, gaps, start, end)
        reporter.generate(dags, gaps, results, {}, start, end)

    elif args.command == "full":
        dags    = discovery.discover_all()
        gaps    = checker.check_all(dags, start, end)
        results = backfiller.backfill(dags, gaps, start, end)
        ok_dags = [
            d for d in dags
            if any(r.dag_id == d.dag_id and r.status in ("success","dry_run")
                   for r in results)
        ]
        tab = refresher.refresh_dashboards(ok_dags)
        reporter.generate(dags, gaps, results, tab, start, end)

    logger.info("Done.")


if __name__ == "__main__":
    main()
