import typer
import sqlite3
import pandas as pd
import yaml
import json
import subprocess
import shlex
import sys
import os
import platform
from pathlib import Path
from typing import Optional, List
from enum import Enum
from datetime import datetime
from rich.console import Console
from rich.table import Table

from wrought_iron.cli.utils import _get_active_db

app = typer.Typer()
console = Console()

# --- Helpers ---

def _get_scipy_stats():
    from scipy import stats
    return stats

def _init_ops_tables(con: sqlite3.Connection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _wi_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            command TEXT NOT NULL,
            cron_expression TEXT,
            timeout INTEGER,
            retry_count INTEGER,
            on_fail_email TEXT,
            cpu_limit TEXT,
            log_level TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _wi_task_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            task_name TEXT,
            start_time TEXT,
            end_time TEXT,
            status TEXT,
            output TEXT,
            error TEXT,
            FOREIGN KEY(task_id) REFERENCES _wi_tasks(id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _wi_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

# --- Commands ---

@app.command(name="schedule")
def schedule_create(
    subcommand: str = typer.Argument(..., help="Subcommand (create, list, delete)."), # Hack to handle sub-sub commands with Typer nicely or just parse args manually if needed. 
    # Wait, Typer supports groups. But the user input is `wi ops schedule create`.
    # So `schedule` should be a group?
    # "wi ops schedule create" -> ops is the app, schedule is a group?
    # But "wi ops" is the module.
    # Let's restructure.
):
    pass

# Typer Grouping Structure:
# wi (main) -> ops (app) -> schedule (group) -> create (command)

# Re-defining the structure to match `wi ops schedule create`
schedule_app = typer.Typer()

@schedule_app.command(name="create")
def schedule_create_cmd(
    cmd: str = typer.Argument(..., help="Command to run."),
    name: str = typer.Option(..., "--name", help="Name of the job."),
    cron: str = typer.Option(..., "--cron", help="Cron expression (e.g. '0 3 * * *')."),
    timeout: int = typer.Option(3600, "--timeout", help="Timeout in seconds."),
    retry: int = typer.Option(0, "--retry", help="Number of retries."),
    on_fail_email: Optional[str] = typer.Option(None, "--on-fail-email", help="Email to alert on failure."),
    cpu_limit: Optional[str] = typer.Option(None, "--cpu-limit", help="CPU limit (e.g. 50%)."),
    log_level: str = typer.Option("INFO", "--log-level", help="Log verbosity."),
):
    """Register a command to the internal scheduler."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_ops_tables(con)
        try:
            con.execute(
                """
                INSERT INTO _wi_tasks 
                (name, command, cron_expression, timeout, retry_count, on_fail_email, cpu_limit, log_level, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (name, cmd, cron, timeout, retry, on_fail_email, cpu_limit, log_level, datetime.now().isoformat())
            )
            console.print(f"[green]Task '{name}' scheduled.[/green]")
        except sqlite3.IntegrityError:
            console.print(f"[red]Error: Task '{name}' already exists.[/red]")
            raise typer.Exit(1)

@schedule_app.command(name="list")
def schedule_list(
    status: Optional[str] = typer.Option(None, "--status", help="Filter by status (active|paused)."),
):
    """List active automated tasks."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_ops_tables(con)
        query = "SELECT id, name, cron_expression, status, command FROM _wi_tasks"
        params = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
            
        df = pd.read_sql_query(query, con, params=params)
        
    if df.empty:
        console.print("No tasks found.")
        return
        
    table = Table(title="Scheduled Tasks")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="magenta")
    table.add_column("Cron", style="green")
    table.add_column("Status")
    table.add_column("Command", style="white")
    
    for _, row in df.iterrows():
        table.add_row(str(row['id']), row['name'], row['cron_expression'], row['status'], row['command'])
    console.print(table)

@schedule_app.command(name="delete")
def schedule_delete(
    task_id: int = typer.Argument(..., help="Task ID to remove."),
    force: bool = typer.Option(False, "--force", help="Force delete."),
):
    """Remove a task."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_ops_tables(con)
        if not force:
            # Check existence
            res = con.execute("SELECT name FROM _wi_tasks WHERE id = ?", (task_id,)).fetchone()
            if not res:
                console.print(f"[red]Task ID {task_id} not found.[/red]")
                raise typer.Exit(1)
            if not typer.confirm(f"Delete task '{res[0]}' ?", default=False):
                raise typer.Abort()
                
        con.execute("DELETE FROM _wi_tasks WHERE id = ?", (task_id,))
        console.print(f"[green]Task {task_id} deleted.[/green]")


app.add_typer(schedule_app, name="schedule")

# --- Direct Ops Commands ---

@app.command(name="pipeline") # Pipeline is grouping "run"
def pipeline_run_wrapper(
     # This is a bit tricky. "wi ops pipeline run" suggests pipeline is a group.
     # But "pipeline" isn't a command itself.
     # Typer handling:
     ctx: typer.Context
):
    pass

pipeline_app = typer.Typer()

@pipeline_app.command(name="run")
def pipeline_run(
    yaml_file: str = typer.Argument(..., help="Path to YAML workflow file."),
    continue_on_error: bool = typer.Option(False, "--continue-on-error", help="Continue even if a step fails."),
):
    """Execute a multi-step workflow defined in YAML."""
    if not Path(yaml_file).exists():
        console.print(f"[red]File {yaml_file} not found.[/red]")
        raise typer.Exit(1)
        
    with open(yaml_file, 'r') as f:
        try:
            workflow = yaml.safe_load(f)
        except yaml.YAMLError as e:
            console.print(f"[red]Invalid YAML: {e}[/red]")
            raise typer.Exit(1)
            
    steps = workflow.get('steps', [])
    if not steps:
        console.print("[yellow]No steps found in workflow.[/yellow]")
        return

    console.print(f"[bold]Starting Pipeline: {workflow.get('name', 'Untitled')}[/bold]")
    
    for i, step in enumerate(steps):
        name = step.get('name', f"Step {i+1}")
        cmd = step.get('command')
        if not cmd:
            console.print(f"[red]Step '{name}' missing command.[/red]")
            continue
            
        console.print(f"Running: [cyan]{name}[/cyan]...")
        
        # Execute
        try:
            # We use shell=True to allow chaining like in the examples?
            # But secure? "wi clean" etc are internal commands.
            # Using subprocess.run with shell=True for maximum compatibility with "cmd" string
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"[green]✓ Success[/green]")
            else:
                console.print(f"[red]✗ Failed (Exit Code {result.returncode})[/red]")
                console.print(f"Stderr: {result.stderr.strip()}")
                if not continue_on_error:
                    raise typer.Exit(1)
                    
        except Exception as e:
            console.print(f"[red]Error executing step: {e}[/red]")
            if not continue_on_error:
                raise typer.Exit(1)

    console.print("[bold green]Pipeline Completed.[/bold green]")

app.add_typer(pipeline_app, name="pipeline")

@app.command(name="trigger")
def trigger(
    task_id: int = typer.Argument(..., help="Task ID to run."),
    wait: bool = typer.Option(False, "--wait", help="Wait for completion."),
):
    """Force run a scheduled task immediately."""
    active_db = _get_active_db()
    task = None
    with sqlite3.connect(active_db) as con:
        _init_ops_tables(con)
        cursor = con.cursor()
        cursor.execute("SELECT name, command, timeout FROM _wi_tasks WHERE id = ?", (task_id,))
        task = cursor.fetchone()
        
    if not task:
        console.print(f"[red]Task ID {task_id} not found.[/red]")
        raise typer.Exit(1)
        
    name, cmd, timeout = task
    console.print(f"Triggering task '{name}': [bold]{cmd}[/bold]")
    
    # Record start
    start_time = datetime.now()
    
    try:
        # If wait is True, we block. If False, we technically should background it.
        # But CLI usually blocks unless explicitly detached.
        # The prompt "Wait for completion" implies the default might be async?
        # But implementing async background job management in a simple CLI is complex.
        # Let's assume synchronous execution for now, or "wait" just affects output streaming.
        # Actually, standard CLI behavior: blocking.
        # If "wait" is an option, maybe the default is to detach?
        # Implementing detach on Windows/Linux portably is hard without external service.
        # For this prototype, we will block (Wait=True implicitly).
        
        process = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        
        status = "success" if process.returncode == 0 else "failed"
        output = process.stdout
        error = process.stderr
        
    except subprocess.TimeoutExpired:
        status = "timeout"
        output = ""
        error = "Execution timed out."
    except Exception as e:
        status = "error"
        output = ""
        error = str(e)
        
    end_time = datetime.now()
    
    # Log result
    with sqlite3.connect(active_db) as con:
        con.execute(
            """
            INSERT INTO _wi_task_logs (task_id, task_name, start_time, end_time, status, output, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, name, start_time.isoformat(), end_time.isoformat(), status, output, error)
        )
        
    color = "green" if status == "success" else "red"
    console.print(f"Task finished with status: [{color}]{status}[/{color}]")
    if error:
        console.print(f"Error: {error}")

@app.command(name="logs")
def logs(
    job_id: Optional[int] = typer.Option(None, "--job-id", help="Filter by specific Task ID."),
    errors_only: bool = typer.Option(False, "--errors-only", help="Show only failed runs."),
    limit: int = typer.Option(20, "--limit", help="Number of rows."),
):
    """View Job History."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_ops_tables(con)
        query = "SELECT id, task_name, start_time, status, error FROM _wi_task_logs"
        conds = []
        params = []
        
        if job_id:
            conds.append("task_id = ?")
            params.append(job_id)
        if errors_only:
            conds.append("status != 'success'")
            
        if conds:
            query += " WHERE " + " AND ".join(conds)
            
        query += f" ORDER BY start_time DESC LIMIT {limit}"
        
        df = pd.read_sql_query(query, con, params=params)
        
    if df.empty:
        console.print("No logs found.")
        return
        
    table = Table(title="Job History")
    table.add_column("Log ID", style="cyan")
    table.add_column("Task", style="magenta")
    table.add_column("Start Time", style="blue")
    table.add_column("Status")
    table.add_column("Error Snippet", style="red")
    
    for _, row in df.iterrows():
        status_color = "green" if row['status'] == "success" else "red"
        err = row['error'][:50] + "..." if row['error'] and len(row['error']) > 50 else row['error']
        table.add_row(str(row['id']), row['task_name'], row['start_time'], f"[{status_color}]{row['status']}[/{status_color}]", err)
        
    console.print(table)

@app.command(name="drift-check")
def drift_check(
    table_name: str = typer.Argument(..., help="Table to check."),
    baseline: str = typer.Option(..., "--baseline", help="Snapshot Name/ID (Baseline)."),
    threshold: float = typer.Option(0.05, "--threshold", help="Max P-value diff (significance level)."),
):
    """Quality Control / Drift Detection."""
    active_db = _get_active_db()
    snapshot_table = f"_snapshot_{baseline}_{table_name}"
    
    stats = _get_scipy_stats()
    
    with sqlite3.connect(active_db) as con:
        try:
            # Check existence
            cursor = con.cursor()
            if not cursor.execute(f"SELECT name FROM sqlite_master WHERE name='{snapshot_table}'").fetchone():
                console.print(f"[red]Snapshot '{baseline}' not found for table '{table_name}'.[/red]")
                raise typer.Exit(1)
                
            df_curr = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
            df_base = pd.read_sql_query(f"SELECT * FROM {snapshot_table}", con)
            
        except Exception as e:
            console.print(f"[red]DB Error: {e}[/red]")
            raise typer.Exit(1)
            
    # Compare numeric columns
    numeric_cols = df_curr.select_dtypes(include=['number']).columns
    
    drift_detected = False
    
    table = Table(title=f"Drift Check: {table_name} vs {baseline}")
    table.add_column("Column", style="cyan")
    table.add_column("Test", style="white")
    table.add_column("Statistic", style="blue")
    table.add_column("P-Value", style="magenta")
    table.add_column("Result", style="bold")
    
    for col in numeric_cols:
        if col not in df_base.columns:
            continue
            
        # KS Test (Kolmogorov-Smirnov) for distribution difference
        # Null hypothesis: The two distributions are the same.
        # If p-value < threshold (e.g. 0.05), we REJECT null hypothesis -> Drift Detected.
        
        curr_data = df_curr[col].dropna()
        base_data = df_base[col].dropna()
        
        if len(curr_data) == 0 or len(base_data) == 0:
            table.add_row(col, "KS", "N/A", "N/A", "[yellow]Insufficient Data[/yellow]")
            continue
            
        ks_stat, p_value = stats.ks_2samp(curr_data, base_data)
        
        is_drift = p_value < threshold
        result_str = "[red]DRIFT[/red]" if is_drift else "[green]OK[/green]"
        if is_drift:
            drift_detected = True
            
        table.add_row(col, "KS-2Samp", f"{ks_stat:.4f}", f"{p_value:.4f}", result_str)
        
    console.print(table)
    
    if drift_detected:
        console.print(f"[red]Drift detected (P-Value < {threshold})![/red]")
        raise typer.Exit(1)
    else:
        console.print("[green]No significant drift detected.[/green]")

@app.command(name="alert-config")
def alert_config(
    email: str = typer.Option(..., "--email", help="Email address for alerts."),
    log_file: Optional[str] = typer.Option(None, "--log-file", help="Path to log file."),
):
    """Configure local hooks for failure alerts."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_ops_tables(con)
        con.execute("INSERT OR REPLACE INTO _wi_settings (key, value) VALUES ('alert_email', ?)", (email,))
        if log_file:
            con.execute("INSERT OR REPLACE INTO _wi_settings (key, value) VALUES ('log_file', ?)", (log_file,))
            
    console.print(f"[green]Alert config updated. Email: {email}[/green]")

@app.command(name="maintenance")
def maintenance(
    reindex: bool = typer.Option(False, "--reindex", help="Rebuild indices."),
    analyze: bool = typer.Option(False, "--analyze", help="Update optimizer statistics."),
):
    """DB Optimization."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        console.print(f"Performing maintenance on {active_db}...")
        
        # VACUUM is standard maintenance
        console.print("Running VACUUM...")
        con.execute("VACUUM")
        
        if analyze:
            console.print("Running ANALYZE...")
            con.execute("ANALYZE")
            
        if reindex:
            console.print("Running REINDEX...")
            con.execute("REINDEX")
            
    console.print("[green]Maintenance complete.[/green]")

@app.command(name="export-status")
def export_status(
    format: str = typer.Option("json", "--format", help="Output format (json|xml)."),
):
    """Output system health JSON."""
    active_db = _get_active_db()
    status = {
        "timestamp": datetime.now().isoformat(),
        "database": str(active_db),
        "size_bytes": active_db.stat().st_size,
        "tasks_count": 0,
        "active_tasks": 0,
        "integrity": "unknown"
    }
    
    with sqlite3.connect(active_db) as con:
        _init_ops_tables(con)
        
        # Task stats
        res = con.execute("SELECT count(*) FROM _wi_tasks").fetchone()
        status["tasks_count"] = res[0]
        
        res = con.execute("SELECT count(*) FROM _wi_tasks WHERE status='active'").fetchone()
        status["active_tasks"] = res[0]
        
        # Integrity check (quick)
        try:
            cursor = con.cursor()
            cursor.execute("PRAGMA quick_check")
            res = cursor.fetchone()
            status["integrity"] = res[0] if res else "error"
        except Exception as e:
            status["integrity"] = str(e)
            
    if format == 'json':
        print(json.dumps(status, indent=2))
    elif format == 'xml':
        # Simple XML construction
        xml = "<status>\n"
        for k, v in status.items():
            xml += f"  <{k}>{v}</{k}>\n"
        xml += "</status>"
        print(xml)
