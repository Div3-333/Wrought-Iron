import sqlite3
import pandas as pd
import json
import sys
import os
import io
from contextlib import redirect_stdout, redirect_stderr
from wrought_iron.cli.utils import _get_active_db
from pathlib import Path

# Use the Typer app directly
from wrought_iron.main import app as wi_app

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

def get_all_table_schemas():
    """
    Returns a dictionary of { table_name: [column_names...] } for all tables in the DB.
    Used for client-side dynamic dropdowns.
    """
    conn = get_db_connection()
    schemas = {}
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_wi_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            try:
                # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
                cursor.execute(f"PRAGMA table_info('{table}')")
                columns = [col[1] for col in cursor.fetchall()]
                schemas[table] = columns
            except:
                schemas[table] = []
        return schemas
    except Exception:
        return {}
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

def get_table_counts():
    conn = get_db_connection()
    stats = []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_wi_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            try:
                count = cursor.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                stats.append({"name": table, "count": count})
            except:
                stats.append({"name": table, "count": "Error"})
        return stats
    except Exception:
        return []
    finally:
        conn.close()

def run_cli_command(module_name: str, command_name: str, args: dict):
    """
    Executes a Wrought Iron CLI command by calling the Typer app directly.
    Captures stdout and stderr.
    """
    # Build command list for Typer
    cmd_list = [module_name]
    cmd_list.extend(command_name.split())
    
    for key, value in args.items():
        if not value:
            continue
            
        if value is True or value == "on":
            flag = key if key.startswith("-") else f"--{key}"
            cmd_list.append(flag)
            continue
            
        if key.startswith("-"):
            cmd_list.append(key)
            cmd_list.append(str(value))
        else:
            # Check if it should be a flag (e.g. from UI)
            cmd_list.append(str(value))

    f_stdout = io.StringIO()
    f_stderr = io.StringIO()
    
    success = True
    try:
        with redirect_stdout(f_stdout), redirect_stderr(f_stderr):
            # We use wi_app(cmd_list) to trigger Typer's parsing
            # standalone_mode=False prevents it from calling sys.exit()
            wi_app(cmd_list, standalone_mode=False)
    except Exception as e:
        success = False
        f_stderr.write(str(e))

    return {
        "success": success,
        "stdout": f_stdout.getvalue(),
        "stderr": f_stderr.getvalue(),
        "command": "wi " + " ".join(cmd_list)
    }