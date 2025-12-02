import typer
import sqlite3
from rich.console import Console
from rich.table import Table
from wrought_iron.cli.utils import _get_active_db
from enum import Enum
import pandas as pd
import plotext as plt
import json

app = typer.Typer()

class DescribeFormat(str, Enum):
    table = "table"
    sql = "sql"

class CastType(str, Enum):
    integer = "integer"
    text = "text"
    real = "real"
    blob = "blob"

class OnError(str, Enum):
    nullify = "nullify"
    fail = "fail"
    ignore = "ignore"

class GraphFormat(str, Enum):
    mermaid = "mermaid"
    dot = "dot"

@app.command()
def list(
    show_views: bool = typer.Option(True, "--show-views/--no-views", help="Toggle view visibility."),
    show_sys: bool = typer.Option(False, "--show-sys/--no-sys", help="Toggle system table visibility."),
):
    """
    List all tables, views, and indexes.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        query = "SELECT type, name, tbl_name FROM sqlite_master"
        conditions = []
        if not show_views:
            conditions.append("type != 'view'")
        if not show_sys:
            conditions.append("name NOT LIKE 'sqlite_%'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        cursor.execute(query)
        results = cursor.fetchall()

    if not results:
        print("No database objects found.")
        return

    table = Table(title=f"Objects in {active_db.name}")
    table.add_column("Type", style="cyan")
    table.add_column("Name", style="magenta")
    table.add_column("Table", style="green")

    for row in results:
        table.add_row(row[0], row[1], row[2])

    console = Console()
    console.print(table)

@app.command()
def describe(
    table_name: str = typer.Argument(..., help="The name of the table to describe."),
    format: DescribeFormat = typer.Option(DescribeFormat.table, "--format", help="Output format."),
):
    """
    Show column names, inferred types, and primary/foreign keys.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        
        if format == DescribeFormat.sql:
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            sql = cursor.fetchone()
            if sql:
                print(sql[0])
            else:
                print(f"Error: Table '{table_name}' not found.")
                raise typer.Exit(code=1)
            return

        cursor.execute(f"PRAGMA table_info('{table_name}')")
        columns = cursor.fetchall()
        
        if not columns:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

        table = Table(title=f"Schema for {table_name}")
        table.add_column("CID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Type", style="green")
        table.add_column("Not Null", style="yellow")
        table.add_column("Default Value", style="blue")
        table.add_column("Primary Key", style="red")

        for col in columns:
            table.add_row(str(col[0]), col[1], col[2], str(bool(col[3])), str(col[4]), str(bool(col[5])))

        console = Console()
        console.print(table)

        cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
        fks = cursor.fetchall()
        if fks:
            fk_table = Table(title=f"Foreign Keys for {table_name}")
            fk_table.add_column("ID", style="cyan")
            fk_table.add_column("Seq", style="magenta")
            fk_table.add_column("Table", style="green")
            fk_table.add_column("From", style="yellow")
            fk_table.add_column("To", style="blue")
            fk_table.add_column("On Update", style="red")
            fk_table.add_column("On Delete", style="purple")
            fk_table.add_column("Match", style="orange3")
            for fk in fks:
                fk_table.add_row(*[str(v) for v in fk])
            console.print(fk_table)

@app.command()
def inspect(
    table_name: str = typer.Argument(..., help="The name of the table to inspect."),
    sample: float = typer.Option(1.0, "--sample", help="% of data to scan (Default: 1.0).", min=0.1, max=1.0),
    histogram: bool = typer.Option(False, "--histogram", help="Include mini-histogram for numeric columns."),
):
    """
    Profile: Calculate non-null %, unique %, and top 5 most common values per column.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            if sample < 1.0:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", con).sample(frac=sample)
            else:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    console = Console()
    for column in df.columns:
        stats_table = Table(title=f"Column: {column}", show_header=False)
        stats_table.add_column("Metric")
        stats_table.add_column("Value")
        
        non_null_pct = df[column].notna().mean() * 100
        unique_pct = df[column].nunique() / len(df[column]) * 100
        
        stats_table.add_row("Non-Null %", f"{non_null_pct:.2f}%")
        stats_table.add_row("Unique %", f"{unique_pct:.2f}%")

        top5 = df[column].value_counts().nlargest(5)
        top5_str = "\n".join([f"{val}: {count}" for val, count in top5.items()])
        stats_table.add_row("Top 5 Values", top5_str)

        console.print(stats_table)

        if histogram and pd.api.types.is_numeric_dtype(df[column]):
            plt.clf()
            plt.hist(df[column].dropna(), bins=20)
            plt.title("Histogram")
            plt.show()
            print("\n")

def _get_schema(db_path: str, table_name: str) -> dict:
    """Gets the schema of a table as a dictionary."""
    with sqlite3.connect(db_path) as con:
        cursor = con.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        columns = cursor.fetchall()
        if not columns:
            print(f"Error: Table '{table_name}' not found in '{db_path}'.")
            raise typer.Exit(code=1)
        return {col[1]: col[2:] for col in columns}

@app.command()
def diff(
    table_a: str = typer.Argument(..., help="The first table to compare."),
    table_b: str = typer.Argument(..., help="The second table to compare."),
    db_b: str = typer.Option(None, "--db-b", help="Path to the second database. If not provided, active database is used."),
):
    """
    Comparative analysis of two schemas (useful for checking drift).
    """
    db_a_path = _get_active_db()
    db_b_path = db_b if db_b else db_a_path

    schema_a = _get_schema(str(db_a_path), table_a)
    schema_b = _get_schema(str(db_b_path), table_b)

    cols_a = set(schema_a.keys())
    cols_b = set(schema_b.keys())

    added_cols = cols_b - cols_a
    removed_cols = cols_a - cols_b
    common_cols = cols_a & cols_b
    modified_cols = {
        col for col in common_cols if schema_a[col] != schema_b[col]
    }

    if not any([added_cols, removed_cols, modified_cols]):
        print("Schemas are identical.")
        return

    table = Table(title=f"Schema Differences: {table_a} vs {table_b}")
    table.add_column("Column", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Details (A -> B)", style="green")

    for col in removed_cols:
        table.add_row(col, "Removed", str(schema_a[col]) + " -> ")
    for col in added_cols:
        table.add_row(col, "Added", " -> " + str(schema_b[col]))
    for col in modified_cols:
        table.add_row(col, "Modified", str(schema_a[col]) + " -> " + str(schema_b[col]))
        
    console = Console()
    console.print(table)

def _get_json_depth(obj):
    """Recursively find the depth of a JSON object."""
    if hasattr(obj, 'items'):
        return 1 + (max([_get_json_depth(v) for v in obj.values()]) if obj else 0)
    if hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
        return 1 + (max([_get_json_depth(v) for v in obj]) if obj else 0)
    return 0

@app.command(name="detect-json")
def detect_json(
    table_name: str = typer.Argument(..., help="The name of the table to scan."),
    depth: int = typer.Option(None, "--depth", help="How deep to traverse nested JSON."),
    threshold: float = typer.Option(0.1, "--threshold", help="Min % of valid JSON rows to trigger detection (Default: 0.1)."),
):
    """
    Scan TEXT columns to identify valid JSON structures (The "Backpack" scan).
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)

    text_cols = [col for col, dtype in df.dtypes.items() if dtype == 'object']
    
    if not text_cols:
        print("No TEXT columns found to scan.")
        return
        
    results = []
    for col in text_cols:
        valid_count = 0
        total_count = 0
        for val in df[col].dropna():
            total_count += 1
            try:
                loaded_json = json.loads(val)
                if depth is None or _get_json_depth(loaded_json) <= depth:
                    valid_count += 1
            except json.JSONDecodeError:
                continue
        
        if total_count > 0:
            valid_pct = valid_count / total_count
            if valid_pct >= threshold:
                results.append((col, f"{valid_pct:.2f}"))

    if not results:
        print("No JSON columns detected.")
        return
        
    table = Table(title=f"Potential JSON Columns in {table_name}")
    table.add_column("Column", style="cyan")
    table.add_column("Valid JSON %", style="magenta")
    
    for row in results:
        table.add_row(*row)
        
    console = Console()
    console.print(table)

@app.command()
def flatten(
    table_name: str = typer.Argument(..., help="The name of the table to flatten."),
    column: str = typer.Argument(..., help="The name of the JSON column to flatten."),
    prefix: str = typer.Option(None, "--prefix", help="Prefix for new columns."),
    separator: str = typer.Option("_", "--separator", help="Nested key separator."),
    drop_original: bool = typer.Option(False, "--drop-original", help="Delete the source JSON column after flattening."),
):
    """
    Permanently explode a JSON column into distinct first-class columns.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)

        if column not in df.columns:
            print(f"Error: Column '{column}' not found in table '{table_name}'.")
            raise typer.Exit(code=1)

        # Ensure the column is of a type that can be parsed as JSON
        df[column] = df[column].apply(lambda x: json.loads(x) if isinstance(x, str) else None)
        
        normalized_df = pd.json_normalize(
            df[column].dropna(), 
            sep=separator,
            record_prefix=prefix if prefix else column + separator
        )

        df = df.join(normalized_df)

        if drop_original:
            df = df.drop(columns=[column])

        df.to_sql(table_name, con, if_exists="replace", index=False)

    print(f"Table '{table_name}' has been flattened.")

@app.command(name="rename-col")
def rename_col(
    table_name: str = typer.Argument(..., help="The name of the table."),
    old_name: str = typer.Argument(..., help="The current name of the column."),
    new_name: str = typer.Argument(..., help="The new name for the column."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Verify SQL generation without executing."),
):
    """
    Safely rename a column (handling SQLite limitations).
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)

        if old_name not in df.columns:
            print(f"Error: Column '{old_name}' not found in table '{table_name}'.")
            raise typer.Exit(code=1)

        df.rename(columns={old_name: new_name}, inplace=True)

        if dry_run:
            print(f"Dry run: Would rename column '{old_name}' to '{new_name}' in table '{table_name}'.")
            print("New columns would be:")
            for col in df.columns:
                print(f"- {col}")
            return

        df.to_sql(table_name, con, if_exists="replace", index=False)

    print(f"Column '{old_name}' has been renamed to '{new_name}' in table '{table_name}'.")

@app.command(name="drop-col")
def drop_col(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column_name: str = typer.Argument(..., help="The name of the column to drop."),
    vacuum: bool = typer.Option(False, "--vacuum", help="Auto-vacuum after drop to reclaim space."),
):
    """
    Remove a column and reconstruct the table.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)

        if column_name not in df.columns:
            print(f"Error: Column '{column_name}' not found in table '{table_name}'.")
            raise typer.Exit(code=1)

        df.drop(columns=[column_name], inplace=True)
        df.to_sql(table_name, con, if_exists="replace", index=False)
        
        if vacuum:
            print("Vacuuming database...")
            con.execute("VACUUM")

    print(f"Column '{column_name}' has been dropped from table '{table_name}'.")

@app.command()
def cast(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column_name: str = typer.Argument(..., help="The name of the column to cast."),
    type: CastType = typer.Argument(..., help="The new type for the column."),
    on_error: OnError = typer.Option(OnError.fail, "--on-error", help="Handling conversion failures."),
):
    """
    Force type conversion (e.g., String -> Integer) with error handling policies.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)

        if column_name not in df.columns:
            print(f"Error: Column '{column_name}' not found in table '{table_name}'.")
            raise typer.Exit(code=1)

        if type in [CastType.integer, CastType.real]:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce' if on_error == OnError.nullify else on_error.value)
        
        elif type == CastType.text:
            df[column_name] = df[column_name].astype(str)

        elif type == CastType.blob:
            def to_blob(x):
                try:
                    return str(x).encode()
                except Exception:
                    if on_error == OnError.fail:
                        raise
                    elif on_error == OnError.nullify:
                        return None
                    return x # ignore
            df[column_name] = df[column_name].apply(to_blob)
            
        df.to_sql(table_name, con, if_exists="replace", index=False)
        print(f"Column '{column_name}' in table '{table_name}' has been cast to '{type.value}'.")

class GraphFormat(str, Enum):
    mermaid = "mermaid"
    dot = "dot"

@app.command()
def graph(
    format: GraphFormat = typer.Option(GraphFormat.mermaid, "--format", help="Output format."),
):
    """
    Generate a text-based Entity Relationship Diagram (ERD) of foreign keys.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall() if not table[0].startswith("sqlite_")]

        if format == GraphFormat.mermaid:
            print("erDiagram")
            for table_name in tables:
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = cursor.fetchall()
                print(f"    {table_name} {{")
                for col in columns:
                    print(f"        {col[2]} {col[1]}")
                print("    }")

                cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
                fks = cursor.fetchall()
                for fk in fks:
                    print(f"    {table_name} }}--o{{ {fk[2]} : \"{fk[3]} -> {fk[4]}\"")

        elif format == GraphFormat.dot:
            print("digraph ERD {")
            print("    graph [rankdir=LR];")
            print("    node [shape=record];")
            for table_name in tables:
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = cursor.fetchall()
                label = f"{table_name} | {{ {' | '.join([f'<f{i}> {col[1]}' for i, col in enumerate(columns)])} }}"
                print(f'    "{table_name}" [label="{label}"];')

                cursor.execute(f"PRAGMA foreign_key_list('{table_name}')")
                fks = cursor.fetchall()
                for fk in fks:
                    print(f'    "{table_name}":f{fk[3]} -> "{fk[2]}":f{fk[4]};')
            print("}")
