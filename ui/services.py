import sqlite3
import pandas as pd
import json
import sys
import os
import subprocess
from wrought_iron.cli.utils import _get_active_db
from pathlib import Path

def get_active_db_path():
    try:
        return str(_get_active_db())
    except Exception:
        return None

def get_db_connection():
    db_path = get_active_db_path()
    if not db_path:
        raise Exception("No active database configured.")
    return sqlite3.connect(db_path)

def get_tables():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_wi_%'")
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    finally:
        conn.close()

def get_table_preview(table_name, limit=100):
    conn = get_db_connection()
    try:
        # Use pandas for easy dataframe handling
        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT {limit}", conn)
        return df.to_dict(orient='records'), df.columns.tolist()
    except Exception as e:
        return None, None
    finally:
        conn.close()

def get_table_schema(table_name):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        # cid, name, type, notnull, dflt_value, pk
        columns = [
            {
                "cid": col[0],
                "name": col[1],
                "type": col[2],
                "notnull": bool(col[3]),
                "default": col[4],
                "pk": bool(col[5])
            }
            for col in cursor.fetchall()
        ]
        return columns
    finally:
        conn.close()

def execute_custom_sql(query):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return {"columns": columns, "data": data, "status": "success"}
        else:
            conn.commit()
            return {"status": "success", "message": "Query executed successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

def get_audit_log(limit=20):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Check if table exists first
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='_wi_audit_log_'")
        if not cursor.fetchone():
            return []
        
        cursor.execute("SELECT * FROM _wi_audit_log_ ORDER BY timestamp DESC LIMIT ?", (limit,))
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()

def get_db_stats():
    path = get_active_db_path()
    if not path:
        return {}
    
    p = Path(path)
    if not p.exists():
        return {"path": path, "status": "Missing"}

    size_bytes = p.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    
    return {
        "path": path,
        "size": f"{size_mb:.2f} MB",
        "name": p.name,
        "status": "Active"
    }

def run_cli_command(module_name: str, command_name: str, args: dict):
    """
    Executes a Wrought Iron CLI command via subprocess.
    Constructs: python src/main.py <module_name> <command_name> [args]
    """
    cmd = [sys.executable, "-X", "utf8", "src/main.py", module_name]
    
    # Handle subcommand nesting like 'view save' -> module: collab, command: 'view save'
    # Actually, my CLI structure in main.py is: app.add_typer(collab.app, name="collab")
    # And inside collab.py: app.add_typer(view_app, name="view")
    # So the CLI args are: python src/main.py collab view save ...
    
    parts = command_name.split() # e.g. "view save" -> ["view", "save"]
    cmd.extend(parts)
    
    for key, value in args.items():
        if not value:
            continue
            
        # Boolean flags (checkboxes)
        if value is True or value == "on":
            # Check if key starts with --, otherwise append it
            flag = key if key.startswith("-") else f"--{key}"
            cmd.append(flag)
            continue
            
        # Positional arguments (don't have --name)
        # BUT in my config I named them.
        # If the key in args dict doesn't start with dash, it's positional?
        # WAIT. My config keys might be "path" or "--force".
        # If key is "path" (no dash), it's positional.
        # If key is "--force" (dashes), it's a flag/option.
        
        if key.startswith("-"):
            cmd.append(key)
            cmd.append(str(value))
        else:
            cmd.append(str(value))

    try:
        # Set environment to force UTF-8 for CLI output
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        
        # Run command from project root
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            encoding='utf-8',
            env=env,
            cwd=str(Path(__file__).parent.parent) # Go up to repo root
        )
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "command": " ".join(cmd)
        }
    except Exception as e:
        return {
            "success": False,
            "stderr": str(e),
            "command": " ".join(cmd)
        }