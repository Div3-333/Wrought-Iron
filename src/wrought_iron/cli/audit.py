import typer
import sqlite3
import pandas as pd
import hashlib
import json
import datetime
from pathlib import Path
from typing import Optional, List
from enum import Enum
from rich.console import Console
from rich.table import Table
import re

from wrought_iron.cli.utils import _get_active_db, log_action

try:
    import wrought_iron_core
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False

app = typer.Typer()
console = Console()

# --- Lazy Imports ---
def _get_cryptography():
    from cryptography.fernet import Fernet
    return Fernet

def _get_reportlab():
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    return canvas, letter

# --- Fallback Classes ---
class SimplePiiScanner:
    def analyze(self, text, entities=None, language='en'):
        results = []
        patterns = {
            "EMAIL_ADDRESS": r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
            "CREDIT_CARD": r"\b(?:\d[ -]*?){13,16}\b",
            "PHONE_NUMBER": r"\b(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})\b"
        }
        class MockResult:
            def __init__(self, entity_type, start, end, score):
                self.entity_type = entity_type
                self.start = start; self.end = end; self.score = score
        for entity, pattern in patterns.items():
            if entities and entity not in entities: continue
            for match in re.finditer(pattern, text):
                results.append(MockResult(entity, match.start(), match.end(), 0.85))
        return results

# --- Enums ---
class HashAlgo(str, Enum):
    sha256 = "sha256"
    sha512 = "sha512"

class AnonymizeMethod(str, Enum):
    mask = "mask"
    hash = "hash"
    redact = "redact"

# --- Logic Helpers ---

def _hash_create_logic(table_name: str, algo: HashAlgo, salt: str, exclude_cols: Optional[str], chunk_size: int):
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        table_info = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        if not table_info:
             raise ValueError(f"Table '{table_name}' not found.")
        all_cols = [info[1] for info in table_info]
        pk_cols = [info[1] for info in table_info if info[5] > 0]
        cols_to_exclude = [c.strip() for c in exclude_cols.split(',')] if exclude_cols else []
        query_cols = [c for c in all_cols if c not in cols_to_exclude]
        order_by = ", ".join(pk_cols) if pk_cols else ", ".join(query_cols)
        query = f"SELECT {', '.join(query_cols)} FROM {table_name} ORDER BY {order_by}"
        if CORE_AVAILABLE and algo == HashAlgo.sha256:
            df = pd.read_sql_query(query, con)
            rows_as_strings = df.astype(str).apply(lambda x: "".join(x), axis=1).tolist()
            hashes = wrought_iron_core.hash_batch(rows_as_strings, salt)
            return hashlib.sha256("".join(hashes).encode()).hexdigest()
        if algo == HashAlgo.sha256: h = hashlib.sha256()
        else: h = hashlib.sha512()
        h.update(salt.encode('utf-8'))
        for chunk in pd.read_sql_query(query, con, chunksize=chunk_size):
            h.update(chunk.to_json(orient='split', date_format='iso').encode('utf-8'))
        return h.hexdigest()

# --- Forensics ---

@app.command(name="log-view")
def log_view(
    limit: int = typer.Option(50, "--limit", help="Number of rows to show."),
    user: Optional[str] = typer.Option(None, "--user", help="Filter by user."),
    action: Optional[str] = typer.Option(None, "--action", help="Filter by action command."),
):
    """Show Audit Log."""
    active_db = _get_active_db()
    query = "SELECT * FROM _wi_audit_log_"
    conditions = []
    params = []
    if user: conditions.append("user = ?"); params.append(user)
    if action: conditions.append("action LIKE ?"); params.append(f"%{action}%")
    if conditions: query += " WHERE " + " AND ".join(conditions)
    query += f" ORDER BY timestamp DESC LIMIT {limit}"
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(query, con, params=params)
            if df.empty:
                console.print("No audit logs found.")
                return
            table = Table(title="Audit Log")
            for col in df.columns: table.add_column(col)
            for _, row in df.iterrows(): table.add_row(*[str(x) for x in row])
            console.print(table)
        except:
            console.print("[yellow]Audit log table not found.[/yellow]")

@app.command(name="hash-create")
def hash_create(
    table_name: str = typer.Argument(..., help="Table to hash."),
    algo: HashAlgo = typer.Option(HashAlgo.sha256, "--algo", help="Hashing algorithm."),
    salt: str = typer.Option("", "--salt", help="Optional salt string."),
    exclude_cols: str = typer.Option(None, "--exclude-cols", help="Comma-separated columns to ignore."),
    chunk_size: int = typer.Option(10000, "--chunk-size", help="Rows per chunk."),
):
    """Generate Integrity Fingerprint."""
    try:
        fingerprint = _hash_create_logic(table_name, algo, salt, exclude_cols, chunk_size)
        console.print(f"Integrity Fingerprint: [green]{fingerprint}[/green]")
        return fingerprint
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]"); raise typer.Exit(1)

@app.command(name="hash-verify")
def hash_verify(
    table_name: str = typer.Argument(..., help="Table to verify."),
    expected_hash: str = typer.Argument(..., help="The expected hash string."),
    salt: str = typer.Option("", "--salt", help="Optional salt string."),
    exclude_cols: str = typer.Option(None, "--exclude-cols", help="Comma-separated columns to ignore."),
):
    """Verify Integrity."""
    # Correctly detect algo from hash length
    algo = HashAlgo.sha256 if len(expected_hash) == 64 else HashAlgo.sha512
    try:
        calc_hash = _hash_create_logic(table_name, algo, salt, exclude_cols, 10000)
        if calc_hash == expected_hash: console.print("[green]OK Integrity Verified.[/green]")
        else:
            console.print("[red]FAILED Verification Failed![/red]")
            console.print(f"Expected: {expected_hash}\nActual:   {calc_hash}"); raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]"); raise typer.Exit(1)

@app.command(name="scan-pii")
def scan_pii(table_name: str = typer.Argument(..., help="Table to scan.")):
    """Scan for PII (Rust Accelerated)."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
    findings_table = Table(title=f"PII Scan Results: {table_name}")
    findings_table.add_column("Column", style="cyan"); findings_table.add_column("Matches", style="red")
    found_any = False
    for col in df.select_dtypes(include=['object', 'string']).columns:
        texts = df[col].dropna().astype(str).tolist()
        if not texts: continue
        if CORE_AVAILABLE:
            results = wrought_iron_core.scan_pii_batch(texts)
            matches = [item for sublist in results for item in sublist]
            if matches:
                found_any = True; findings_table.add_row(col, ", ".join(set(matches)))
        else:
            scanner = SimplePiiScanner()
            if any(scanner.analyze(val) for val in texts):
                found_any = True; findings_table.add_row(col, "Detected")
    if found_any: console.print(findings_table)
    else: console.print("[green]No PII found.[/green]")

@app.command(name="anonymize")
def anonymize(
    table_name: str = typer.Argument(..., help="Table name."),
    col_name: str = typer.Argument(..., help="Column to anonymize."),
    method: AnonymizeMethod = typer.Option(AnonymizeMethod.mask, "--method", help="Masking method."),
    chars: int = typer.Option(4, "--chars", help="Number of chars to mask."),
):
    """Masking (Rust Accelerated)."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        texts = df[col_name].astype(str).tolist()
        if method == AnonymizeMethod.mask and CORE_AVAILABLE: df[col_name] = wrought_iron_core.mask_batch(texts, chars)
        elif method == AnonymizeMethod.hash and CORE_AVAILABLE: df[col_name] = wrought_iron_core.hash_batch(texts, "")
        else:
            if method == AnonymizeMethod.mask: df[col_name] = df[col_name].apply(lambda x: "*"*chars + str(x)[chars:])
            elif method == AnonymizeMethod.hash: df[col_name] = df[col_name].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
            elif method == AnonymizeMethod.redact: df[col_name] = "[REDACTED]"
        df.to_sql(table_name, con, if_exists="replace", index=False)
        log_action(con, "audit", "anonymize", f"Anonymized {col_name} in {table_name}")
        console.print(f"[green]Column '{col_name}' anonymized.[/green]")

@app.command()
def snapshot(table_name: str, name: str):
    """Backup table state (preserves constraints)."""
    active_db = _get_active_db()
    snap_name = f"_snapshot_{name}_{table_name}"
    with sqlite3.connect(active_db) as con:
        # Get original DDL
        res = con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()
        if not res:
            print(f"Error: Table {table_name} not found."); raise typer.Exit(1)
        ddl = res[0]
        # Replace table name in DDL - use a robust regex or simple replace
        snap_ddl = ddl.replace(f'"{table_name}"', f'"{snap_name}"').replace(f' {table_name} ', f' {snap_name} ').replace(f' {table_name}(', f' {snap_name}(')
        if snap_ddl == ddl: # Fallback if no quotes/spaces matched exactly
             snap_ddl = ddl.replace(table_name, snap_name, 1)
        
        con.execute(f'DROP TABLE IF EXISTS "{snap_name}"')
        con.execute(snap_ddl)
        con.execute(f'INSERT INTO "{snap_name}" SELECT * FROM "{table_name}"')
        log_action(con, "audit", "snapshot", f"Created robust snapshot {name} for {table_name}")
    console.print(f"[green]Snapshot '{name}' created (constraints preserved).[/green]")

@app.command()
def rollback(table_name: str, name: str):
    """Restore table from snapshot."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        con.execute(f"DROP TABLE {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _snapshot_{name}_{table_name}")
        log_action(con, "audit", "rollback", f"Restored {table_name} from snapshot {name}")
    console.print(f"[green]Table '{table_name}' restored from snapshot '{name}'.[/green]")

@app.command(name="encrypt-col")
def encrypt_col(table_name: str, col_name: str, key_file: str):
    """Column Encryption (Fernet)."""
    Fernet = _get_cryptography(); key_path = Path(key_file)
    if key_path.exists(): key = key_path.read_bytes()
    else: key = Fernet.generate_key(); key_path.write_bytes(key)
    f = Fernet(key); active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        df[col_name] = df[col_name].apply(lambda x: f.encrypt(str(x).encode()).decode() if x is not None else None)
        df.to_sql(table_name, con, if_exists="replace", index=False)
    console.print(f"[green]Column '{col_name}' encrypted.[/green]")

@app.command(name="decrypt-col")
def decrypt_col(table_name: str, col_name: str, key_file: str):
    """Column Decryption (Fernet)."""
    Fernet = _get_cryptography(); key = Path(key_file).read_bytes(); f = Fernet(key); active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        df[col_name] = df[col_name].apply(lambda x: f.decrypt(str(x).encode()).decode() if x is not None else None)
        df.to_sql(table_name, con, if_exists="replace", index=False)
    console.print(f"[green]Column '{col_name}' decrypted.[/green]")

@app.command(name="export-cert")
def export_cert(signer: str, output: str):
    """Generate Chain-of-Custody PDF."""
    canvas, letter = _get_reportlab(); c = canvas.Canvas(output, pagesize=letter)
    c.drawString(100, 750, f"Chain of Custody Certificate - Signer: {signer}")
    c.drawString(100, 730, f"Generated: {datetime.datetime.now().isoformat()}")
    c.save(); console.print(f"[green]Certificate generated: {output}[/green]")