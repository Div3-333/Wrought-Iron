import typer
import sqlite3
import pandas as pd
import json
import zipfile
import os
from pathlib import Path
from typing import Optional
from datetime import datetime
from rich.console import Console
from rich.table import Table

from wrought_iron.cli.utils import _get_active_db, log_action

app = typer.Typer()
view_app = typer.Typer()
config_app = typer.Typer()
recipe_app = typer.Typer()
notes_app = typer.Typer()

console = Console()

def _init_collab_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _wi_views (
            name TEXT PRIMARY KEY,
            sql TEXT NOT NULL,
            description TEXT,
            created_at TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _wi_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            message TEXT NOT NULL,
            author TEXT,
            created_at TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _wi_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS _wi_aliases_ (
            name TEXT PRIMARY KEY,
            path TEXT NOT NULL
        )
    """)

@view_app.command(name="save")
def view_save(
    name: str = typer.Argument(..., help="Name of the view."),
    query: str = typer.Option(..., "--query", help="SQL Query."),
    desc: str = typer.Option("", "--desc", help="Description."),
):
    """Save a complex filter as a Virtual View."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_collab_tables(con)
        try:
            con.execute(
                "INSERT INTO _wi_views (name, sql, description, created_at) VALUES (?, ?, ?, ?)",
                (name, query, desc, datetime.now().isoformat())
            )
            try:
                con.execute(f'DROP VIEW IF EXISTS "{name}"')
                con.execute(f'CREATE VIEW "{name}" AS {query}')
                log_action(con, "collab", "view-save", f"Saved view '{name}'")
                console.print(f"[green]View '{name}' saved and created in DB.[/green]")
            except sqlite3.Error as e:
                console.print(f"[yellow]Saved metadata, but failed to create SQLite VIEW: {e}[/yellow]")
                
        except sqlite3.IntegrityError:
            console.print(f"[red]View '{name}' already exists.[/red]")
            raise typer.Exit(1)

@view_app.command(name="list")
def view_list(filter: str = typer.Option(None, "--filter", help="Filter views by name.")):
    """List saved views."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_collab_tables(con)
        query = "SELECT name, description, created_at FROM _wi_views"
        params = []
        if filter:
            query += " WHERE name LIKE ?"
            params.append(f"%{filter}%")
        
        try:
            df = pd.read_sql_query(query, con, params=params)
        except:
             console.print("No views found.")
             return

    if df.empty:
        console.print("No views found.")
        return

    table = Table(title="Saved Views")
    table.add_column("Name", style="cyan")
    table.add_column("Description", style="white")
    table.add_column("Created At", style="blue")
    
    for _, row in df.iterrows():
        table.add_row(row['name'], row['description'], row['created_at'])
        
    console.print(table)

@view_app.command(name="load")
def view_load(
    name: str = typer.Argument(..., help="Name of the view to load."),
    as_table: str = typer.Option(..., "--as-table", help="Materialize as table."),
):
    """Load View."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_collab_tables(con)
        res = con.execute("SELECT sql FROM _wi_views WHERE name = ?", (name,)).fetchone()
        if not res:
            console.print(f"[red]View '{name}' not found.[/red]")
            raise typer.Exit(1)
        
        sql = res[0]
        console.print(f"Materializing view '{name}' into table '{as_table}'...")
        try:
            con.execute(f"CREATE TABLE {as_table} AS {sql}")
            log_action(con, "collab", "view-load", f"Materialized view '{name}' to table '{as_table}'")
            console.print(f"[green]Table '{as_table}' created.[/green]")
        except sqlite3.Error as e:
            console.print(f"[red]Error materializing view: {e}[/red]")
            raise typer.Exit(1)

app.add_typer(view_app, name="view")

@config_app.command(name="export")
def config_export(
    cmd: str = typer.Argument("project", help="Command Scope (default: project)."),
    file: str = typer.Argument(..., help="Output JSON file."),
    include_secrets: bool = typer.Option(False, "--include-secrets", help="Include secrets."),
):
    """Export configuration to JSON."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_collab_tables(con)
        try:
            df = pd.read_sql_query("SELECT key, value FROM _wi_settings", con)
            settings = dict(zip(df['key'], df['value']))
        except:
            settings = {}
        
        # Also get aliases
        try:
            aliases_df = pd.read_sql_query("SELECT name, path FROM _wi_aliases_", con)
            aliases = aliases_df.to_dict(orient='records')
        except:
            aliases = []
            
    data = {
        "scope": cmd,
        "settings": settings,
        "aliases": aliases
    }
    
    with open(file, 'w') as f:
        json.dump(data, f, indent=2)
        
    console.print(f"[green]Configuration exported to {file}[/green]")

@config_app.command(name="import")
def config_import(
    file_path: str = typer.Argument(..., help="Input file path."),
    scope: str = typer.Option("project", "--scope", help="Scope (user|project)."),
):
    """Import settings."""
    if not Path(file_path).exists():
        console.print(f"[red]File {file_path} not found.[/red]")
        raise typer.Exit(1)
        
    with open(file_path, 'r') as f:
        data = json.load(f)
        
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        con.execute("CREATE TABLE IF NOT EXISTS _wi_settings (key TEXT PRIMARY KEY, value TEXT)")
        
        settings = data.get("settings", {})
        for k, v in settings.items():
            con.execute("INSERT OR REPLACE INTO _wi_settings (key, value) VALUES (?, ?)", (k, v))
            
        if "aliases" in data:
            con.execute("CREATE TABLE IF NOT EXISTS _wi_aliases_ (name TEXT PRIMARY KEY, path TEXT)")
            for alias in data["aliases"]:
                con.execute("INSERT OR REPLACE INTO _wi_aliases_ (name, path) VALUES (?, ?)", (alias['name'], alias['path']))
        
        log_action(con, "collab", "config-import", f"Imported config from {file_path}")
                
    console.print(f"[green]Config imported from {file_path}[/green]")

app.add_typer(config_app, name="config")

@recipe_app.command(name="bundle")
def recipe_bundle(
    name: str = typer.Argument(..., help="Bundle name."),
    out: str = typer.Option(None, "--out", help="Output zip file."),
):
    """Bundle project files."""
    if not out:
        out = f"{name}.zip"
        
    # Zip current directory content, excluding venv, __pycache__, .git
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk("."):
            # Filter dirs
            dirs[:] = [d for d in dirs if d not in [".git", "__pycache__", "venv", ".venv", "dist", "build"]]
            for file in files:
                if file == out: continue
                if file.endswith(".pyc"): continue
                
                file_path = Path(root) / file
                zf.write(file_path, arcname=file_path.relative_to("."))
                
    console.print(f"[green]Project bundled into {out}[/green]")

@recipe_app.command(name="install")
def recipe_install(
    file_path: str = typer.Argument(..., help="Zip file path."),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing files."),
):
    """Unzip Project."""
    if not Path(file_path).exists():
        console.print(f"[red]File {file_path} not found.[/red]")
        raise typer.Exit(1)
        
    with zipfile.ZipFile(file_path, 'r') as zf:
        dest_dir = Path.cwd().resolve()
        safe_members = []
        for member in zf.namelist():
            member_path = (dest_dir / member).resolve()
            if not str(member_path).startswith(str(dest_dir)):
                console.print(f"[red]Security Error: Zip member '{member}' attempts to break out of destination.[/red]")
                raise typer.Exit(1)
            safe_members.append(member)

        if not overwrite:
            for name in safe_members:
                if Path(name).exists():
                    console.print(f"[red]File '{name}' already exists. Use --overwrite.[/red]")
                    raise typer.Exit(1)
        
        zf.extractall(members=safe_members)
        
        active_db = _get_active_db()
        with sqlite3.connect(active_db) as con:
             log_action(con, "collab", "recipe-install", f"Installed recipe from {file_path}")

        console.print(f"[green]Project installed from {file_path}[/green]")

app.add_typer(recipe_app, name="recipe")

@notes_app.command(name="add")
def notes_add(
    table_name: str = typer.Argument(..., help="Table to annotate."),
    msg: str = typer.Option(..., "--msg", help="Note message."),
    author: str = typer.Option("user", "--author", help="Author name."),
):
    """Annotate a table."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_collab_tables(con)
        con.execute(
            "INSERT INTO _wi_notes (table_name, message, author, created_at) VALUES (?, ?, ?, ?)",
            (table_name, msg, author, datetime.now().isoformat())
        )
        log_action(con, "collab", "notes-add", f"Added note to {table_name}")
    console.print(f"[green]Note added to {table_name}.[/green]")

@notes_app.command(name="show")
def notes_show(
    table_name: str = typer.Argument(..., help="Table to show notes for."),
):
    """Show annotations for a table."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        _init_collab_tables(con)
        try:
            df = pd.read_sql_query("SELECT * FROM _wi_notes WHERE table_name = ?", con, params=(table_name,))
        except:
             console.print(f"No notes table found.")
             return

    if df.empty:
        console.print(f"No notes found for table '{table_name}'.")
        return

    table = Table(title=f"Notes for {table_name}")
    table.add_column("Author", style="cyan")
    table.add_column("Message", style="white")
    table.add_column("Date", style="blue")
    
    for _, row in df.iterrows():
        table.add_row(row['author'], row['message'], row['created_at'])
        
    console.print(table)

app.add_typer(notes_app, name="notes")

# --- Workspace ---

@app.command(name="workspace")
def workspace_dump(
    dump_cmd: str = typer.Argument("dump", help="Command (dump)."), # Hack for "workspace dump" structure
    full: bool = typer.Option(False, "--full", help="Include data + config + logs."),
):
    """State Export."""
    # Since "dump" is the subcommand
    if dump_cmd != "dump":
        return # Or handle error
        
    active_db = _get_active_db()
    out_file = f"workspace_dump_{datetime.now().strftime('%Y%m%d')}.zip"
    
    # Same as bundle basically, but maybe more comprehensive
    with zipfile.ZipFile(out_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(active_db, arcname=active_db.name)
        # If we had external config files, we'd add them here.
        
    console.print(f"[green]Workspace dumped to {out_file}[/green]")