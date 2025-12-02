import typer
import os
import sqlite3
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.table import Table
from enum import Enum
import pandas as pd
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
import base64
from wrought_iron.cli.utils import (
    _get_config_dir,
    _get_global_history_db_path,
    _initialize_global_history_db,
    _is_wrought_iron_db,
    _get_active_db,
)

app = typer.Typer()

class SortOptions(str, Enum):
    access_time = "access_time"
    size = "size"

class MergeStrategy(str, Enum):
    append = "append"
    replace = "replace"
    ignore = "ignore"

@app.command()
def file(
    path: str = typer.Argument(..., help="Path to the database file."),
    read_only: bool = typer.Option(False, "--read-only", help="Open the database in immutable mode."),
    check_wal: bool = typer.Option(False, "--check-wal", help="Verify that the Write-Ahead-Log exists."),
):
    """
    Register an existing database as the active target.
    """
    db_path = Path(path)
    if not db_path.exists():
        print(f"Error: File not found at '{db_path}'")
        raise typer.Exit(code=1)

    if check_wal and not Path(f"{db_path}-wal").exists():
        print(f"Error: WAL file not found for '{db_path}'")
        raise typer.Exit(code=1)

    # Initialize WI metadata if missing (upgrade plain SQLite to WI)
    try:
        with sqlite3.connect(str(db_path)) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS _wi_aliases_ (
                    name TEXT PRIMARY KEY,
                    path TEXT NOT NULL
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS _wi_audit_log_ (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    user TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT
                )
            """)
    except sqlite3.Error as e:
        print(f"Error initializing Wrought Iron metadata in '{db_path}': {e}")
        raise typer.Exit(code=1)

    try:
        # Set active database
        config_dir = _get_config_dir()
        config_file = config_dir / "config"
        with open(config_file, "w") as f:
            f.write(str(db_path.resolve()))

        # Update history
        _initialize_global_history_db()
        history_db_path = _get_global_history_db_path()
        with sqlite3.connect(history_db_path) as con:
            con.execute(
                "INSERT OR REPLACE INTO history (path, last_accessed, size) VALUES (?, ?, ?)",
                (str(db_path.resolve()), datetime.now().isoformat(), db_path.stat().st_size),
            )

        print(f"Active database set to '{db_path.resolve()}'")
    except Exception as e:
        print(f"Error setting active database: {e}")
        raise typer.Exit(code=1)

@app.command()
def new(
    path: str = typer.Argument(..., help="Path to the new database file."),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite the file if it already exists."),
    page_size: int = typer.Option(4096, "--page-size", help="Set the SQLite page size.",
                                  min=4096, max=16384, clamp=True),
):
    """
    Initialize a fresh, empty Wrought Iron schema.
    """
    if os.path.exists(path):
        if force:
            os.remove(path)
        else:
            print(f"Error: File already exists at '{path}'. Use --force to overwrite.")
            raise typer.Exit(code=1)

    try:
        with sqlite3.connect(path) as con:
            con.execute(f"PRAGMA page_size = {page_size}")
            print("Creating Wrought Iron metadata tables...")
            con.execute("""
                CREATE TABLE _wi_aliases_ (
                    name TEXT PRIMARY KEY,
                    path TEXT NOT NULL
                )
            """)
            con.execute("""
                CREATE TABLE _wi_audit_log_ (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    user TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT
                )
            """)
        print(f"New Wrought Iron database created at '{path}'")
    except sqlite3.Error as e:
        print(f"Error creating database: {e}")
        raise typer.Exit(code=1)

@app.command()
def list(
    sort: SortOptions = typer.Option(SortOptions.access_time, "--sort", help="Sort order."),
    limit: int = typer.Option(10, "--limit", help="Max rows to show."),
):
    """
    Show history of accessed databases.
    """
    _initialize_global_history_db()
    history_db_path = _get_global_history_db_path()
    with sqlite3.connect(history_db_path) as con:
        cursor = con.cursor()
        order_by = "last_accessed DESC" if sort == SortOptions.access_time else "size DESC"
        cursor.execute(f"SELECT path, last_accessed, size FROM history ORDER BY {order_by} LIMIT {limit}")
        results = cursor.fetchall()

    if not results:
        print("No history found.")
        return

    table = Table(title="Wrought Iron: Accessed Databases")
    table.add_column("Path", style="cyan")
    table.add_column("Last Accessed", style="magenta")
    table.add_column("Size (bytes)", justify="right", style="green")

    for row in results:
        table.add_row(str(row[0]), str(row[1]), str(row[2]))

    console = Console()
    console.print(table)

@app.command()
def alias(
    name: str = typer.Argument(..., help="The alias name."),
    path: str = typer.Argument(..., help="The path to the database."),
    overwrite: bool = typer.Option(False, "--overwrite", help="Replace existing alias."),
):
    """
    Assign a short name to a deep file path.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        if overwrite:
            sql = "INSERT OR REPLACE INTO _wi_aliases_ (name, path) VALUES (?, ?)"
        else:
            sql = "INSERT INTO _wi_aliases_ (name, path) VALUES (?, ?)"
        try:
            cursor.execute(sql, (name, path))
            print(f"Alias '{name}' created for '{path}' in '{active_db}'")
        except sqlite3.IntegrityError:
            print(f"Error: Alias '{name}' already exists. Use --overwrite to replace it.")
            raise typer.Exit(code=1)

@app.command()
def merge(
    target_db: str = typer.Argument(..., help="Path to the target database."),
    source_db: str = typer.Argument(..., help="Path to the source database."),
    strategy: MergeStrategy = typer.Option(MergeStrategy.append, "--strategy", help="Conflict resolution strategy."),
    tables: str = typer.Option(None, "--tables", help="Comma-separated list of tables to merge."),
    chunk_size: int = typer.Option(50000, "--chunk-size", help="Rows per commit."),
):
    """
    ETL merge of two DB files.
    """
    if not Path(target_db).exists():
        print(f"Error: Target database not found at '{target_db}'")
        raise typer.Exit(code=1)
    if not Path(source_db).exists():
        print(f"Error: Source database not found at '{source_db}'")
        raise typer.Exit(code=1)

    source_con = sqlite3.connect(source_db)
    target_con = sqlite3.connect(target_db)

    try:
        source_cursor = source_con.cursor()
        source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        source_tables = [table[0] for table in source_cursor.fetchall() if not table[0].startswith("_wi_")]

        if tables:
            tables_to_merge = tables.split(",")
            for table in tables_to_merge:
                if table not in source_tables:
                    print(f"Error: Table '{table}' not found in source database.")
                    raise typer.Exit(code=1)
            source_tables = tables_to_merge

        for table_name in source_tables:
            print(f"Merging table '{table_name}'...")
            if strategy == MergeStrategy.replace:
                for chunk in pd.read_sql(f"SELECT * FROM {table_name}", source_con, chunksize=chunk_size):
                    chunk.to_sql(table_name, target_con, if_exists='replace', index=False)
            
            elif strategy == MergeStrategy.append:
                for chunk in pd.read_sql(f"SELECT * FROM {table_name}", source_con, chunksize=chunk_size):
                    chunk.to_sql(table_name, target_con, if_exists='append', index=False)

            elif strategy == MergeStrategy.ignore:
                target_cursor = target_con.cursor()
                target_cursor.execute(f"PRAGMA table_info('{table_name}')")
                pks = [info[1] for info in target_cursor.fetchall() if info[5]]
                if not pks:
                    print(f"Warning: Table '{table_name}' has no primary key. Cannot use 'ignore' strategy. Skipping.")
                    continue

                target_pks_df = pd.read_sql(f"SELECT {','.join(pks)} FROM {table_name}", target_con)
                
                for chunk in pd.read_sql(f"SELECT * FROM {table_name}", source_con, chunksize=chunk_size):
                    chunk = chunk.merge(target_pks_df, on=pks, how='left', indicator=True)
                    chunk = chunk[chunk['_merge'] == 'left_only'].drop(columns=['_merge'])
                    chunk.to_sql(table_name, target_con, if_exists='append', index=False)

        print("Merge complete.")

    finally:
        source_con.close()
        target_con.close()

@app.command()
def info(
    extended: bool = typer.Option(False, "--extended", help="Show WAL size, encoding, and user permissions."),
):
    """
    Display low-level metadata.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        cursor.execute("PRAGMA encoding")
        encoding = cursor.fetchone()[0]

    stat = active_db.stat()

    table = Table(title=f"Metadata for {active_db.name}")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Path", str(active_db))
    table.add_row("Size (bytes)", str(stat.st_size))
    table.add_row("Page Size (bytes)", str(page_size))
    table.add_row("Journal Mode", journal_mode.upper())

    if extended:
        wal_path = Path(f"{active_db}-wal")
        wal_size = wal_path.stat().st_size if wal_path.exists() else "N/A"
        table.add_row("WAL Size (bytes)", str(wal_size))
        table.add_row("Encoding", encoding)
        table.add_row("Permissions", oct(stat.st_mode)[-3:])

    console = Console()
    console.print(table)

@app.command()
def vacuum(
    into: str = typer.Option(None, "--into", help="Vacuum into a new file instead of in-place."),
):
    """
    Rebuild DB file to reclaim disk space.
    """
    active_db = _get_active_db()
    print(f"Vacuuming '{active_db}'...")
    with sqlite3.connect(active_db) as con:
        if into:
            con.execute(f"VACUUM INTO '{into}'")
            print(f"Database vacuumed into '{into}'.")
        else:
            con.execute("VACUUM")
            print("Database vacuumed in-place.")

@app.command(name="integrity-check")
def integrity_check(
    quick: bool = typer.Option(False, "--quick", help="Skip index verification for speed."),
):
    """
    Run SQLite corruption scan.
    """
    active_db = _get_active_db()
    print(f"Running integrity check on '{active_db}'...")
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        if quick:
            cursor.execute("PRAGMA quick_check")
        else:
            cursor.execute("PRAGMA integrity_check")
        
        results = cursor.fetchall()
        if len(results) == 1 and results[0][0] == 'ok':
            print("Integrity check passed.")
        else:
            print("Integrity check failed:")
            for row in results:
                print(row[0])

@app.command()
def encrypt(
    path: str = typer.Argument(..., help="Path to the database file."),
    key_file: str = typer.Option(None, "--key-file", help="Path to the encryption key file."),
    output: str = typer.Option(None, "--output", help="Path to write the encrypted file to."),
):
    """
    Encrypt database at rest (AES-256).
    """
    db_path = Path(path)
    if not db_path.exists():
        print(f"Error: File not found at '{db_path}'")
        raise typer.Exit(code=1)

    if key_file:
        with open(key_file, "rb") as f:
            key = f.read()
    else:
        password = typer.prompt("Enter a password for encryption", hide_input=True, confirmation_prompt=True)
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))

    fernet = Fernet(key)
    with open(db_path, "rb") as f:
        data = f.read()
    
    encrypted_data = fernet.encrypt(data)

    output_path = Path(output) if output else db_path
    with open(output_path, "wb") as f:
        if not key_file:
            f.write(salt)
        f.write(encrypted_data)

    print(f"File '{db_path}' encrypted to '{output_path}'.")

@app.command()
def decrypt(
    path: str = typer.Argument(..., help="Path to the encrypted database file."),
    key_file: str = typer.Option(None, "--key-file", help="Path to the encryption key file."),
):
    """
    Decrypt a WI-locked file.
    """
    db_path = Path(path)
    if not db_path.exists():
        print(f"Error: File not found at '{db_path}'")
        raise typer.Exit(code=1)

    if key_file:
        key_path = Path(key_file)
        if not key_path.exists():
            print(f"Error: Key file not found at '{key_path}'")
            raise typer.Exit(code=1)
        with open(key_path, "rb") as f:
            key = f.read()
    else:
        password = typer.prompt("Enter the password for decryption", hide_input=True)
        with open(db_path, "rb") as f:
            salt = f.read(16)
            encrypted_data = f.read()
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))

    fernet = Fernet(key)
    if 'encrypted_data' not in locals():
        with open(db_path, "rb") as f:
            encrypted_data = f.read()
        
    try:
        decrypted_data = fernet.decrypt(encrypted_data)
    except Exception as e:
        print(f"Error decrypting file: {e}")
        raise typer.Exit(code=1)

    with open(db_path, "wb") as f:
        f.write(decrypted_data)

    print(f"File '{db_path}' decrypted successfully.")
