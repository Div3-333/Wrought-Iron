import typer
import sqlite3
import pandas as pd
import polars as pl
import duckdb
from rich.console import Console
from rich.table import Table
from wrought_iron.cli.utils import _get_active_db
from enum import Enum
import json
import re

class QueryEngine(str, Enum):
    polars = "polars"
    duckdb = "duckdb"
    pandas = "pandas"

class SortOrder(str, Enum):
    asc = "asc"
    desc = "desc"

class SortAlgorithm(str, Enum):
    quicksort = "quicksort"
    mergesort = "mergesort"

class NullMode(str, Enum):
    any = "any"
    all = "all"

class KeepOptions(str, Enum):
    first = "first"
    last = "last"
    none = "none"

app = typer.Typer()

def _get_pl_df(table_name: str) -> pl.DataFrame:
    active_db = _get_active_db()
    # Use DuckDB to read SQLite for maximum speed
    conn = duckdb.connect()
    try:
        # DuckDB can scan SQLite directly and very fast
        df = conn.execute(f"SELECT * FROM sqlite_scan('{active_db}', '{table_name}')").pl()
        return df
    except Exception as e:
        # Fallback to standard sqlite3 if DuckDB extension is missing or fails
        with sqlite3.connect(active_db) as con:
            return pl.read_database(f"SELECT * FROM {table_name}", con)

@app.command()
def head(
    table_name: str = typer.Argument(..., help="The name of the table."),
    n: int = typer.Option(10, "-n", help="Number of rows (Default: 10)."),
):
    """
    Show first N rows (Powered by Polars).
    """
    try:
        df = _get_pl_df(table_name).head(n)
    except Exception as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Head of {table_name} (Polars Optimized)")
    
    for col in df.columns:
        table.add_column(col)
        
    for row in df.rows():
        table.add_row(*[str(item) for item in row])

    console.print(table)

@app.command()
def tail(
    table_name: str = typer.Argument(..., help="The name of the table."),
    n: int = typer.Option(10, "-n", help="Number of rows (Default: 10)."),
):
    """
    Show last N rows (Powered by Polars).
    """
    try:
        df = _get_pl_df(table_name).tail(n)
    except Exception as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Tail of {table_name} (Polars Optimized)")
    
    for col in df.columns:
        table.add_column(col)
        
    for row in df.rows():
        table.add_row(*[str(item) for item in row])

    console.print(table)

@app.command()
def sample(
    table_name: str = typer.Argument(..., help="The name of the table."),
    frac: float = typer.Option(None, "--frac", help="Fraction of rows to return (0.0-1.0)."),
    n: int = typer.Option(None, "-n", help="Number of rows."),
    seed: int = typer.Option(None, "--seed", help="Random seed for reproducibility."),
):
    """
    Get random subset (Powered by Polars).
    """
    try:
        df = _get_pl_df(table_name)
        if frac is not None:
            sample_df = df.sample(fraction=frac, seed=seed)
        elif n is not None:
            sample_df = df.sample(n=n, seed=seed)
        else:
            sample_df = df.sample(n=10, seed=seed)
    except Exception as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Sample of {table_name}")
    
    for col in sample_df.columns:
        table.add_column(col)
        
    for row in sample_df.rows():
        table.add_row(*[str(item) for item in row])

    console.print(table)

@app.command()
def filter(
    table_name: str = typer.Argument(..., help="The name of the table."),
    where: str = typer.Option(..., "--where", help="SQL-like filter string."),
    engine: QueryEngine = typer.Option(QueryEngine.duckdb, "--engine", help="Backend speed optimization."),
):
    """
    Apply boolean logic (Powered by DuckDB/Polars).
    """
    active_db = _get_active_db()
    console = Console()
    
    if engine == QueryEngine.duckdb:
        conn = duckdb.connect()
        try:
            query = f"SELECT * FROM sqlite_scan('{active_db}', '{table_name}') WHERE {where}"
            df = conn.execute(query).pl()
        except Exception as e:
            print(f"DuckDB filter error: {e}")
            raise typer.Exit(code=1)
    else:
        # Fallback to Polars lazy filter if it's a simple case, but DuckDB is better for string SQL filters
        df = _get_pl_df(table_name)
        # Polars doesn't have a direct 'query' string method like pandas, 
        # so for --where string we prefer DuckDB.
        print("Note: Engine fallback. DuckDB is recommended for string filters.")
        raise typer.Exit(code=1)

    table = Table(title=f"Filtered {table_name} (High-Speed)")
    for col in df.columns:
        table.add_column(col)
    for row in df.head(100).rows(): # Limit output for console
        table.add_row(*[str(item) for item in row])
    console.print(table)
    if len(df) > 100:
        console.print(f"... and {len(df) - 100} more rows.")

@app.command()
def sql(
    query: str = typer.Argument(..., help="The SQL query to execute."),
    params: str = typer.Option(None, "--params", help="JSON string of parameters."),
    engine: QueryEngine = typer.Option(QueryEngine.duckdb, "--engine", help="Execution engine."),
):
    """
    Execute raw SQL (Extremely fast via DuckDB).
    """
    active_db = _get_active_db()
    console = Console()
    
    if engine == QueryEngine.duckdb:
        conn = duckdb.connect()
        try:
            # Attach SQLite DB to DuckDB
            conn.execute(f"INSTALL sqlite; LOAD sqlite; ATTACH '{active_db}' AS sqlite_db (TYPE SQLITE);")
            # Replace table names in query with sqlite_db.table_name if needed, 
            # or just use sqlite_scan if it's a single table.
            # Easiest is to let user write standard SQL and use the attached DB.
            res = conn.execute(query)
            df = res.pl()
            
            if not df.is_empty():
                table = Table(title="SQL Query Results (DuckDB Optimized)")
                for col in df.columns:
                    table.add_column(col)
                for row in df.head(100).rows():
                    table.add_row(*[str(item) for item in row])
                console.print(table)
            else:
                print("Query executed successfully, no results to display.")
        except Exception as e:
            print(f"DuckDB Error: {e}")
            raise typer.Exit(code=1)
    else:
        # Standard sqlite3 fallback
        with sqlite3.connect(active_db) as con:
            cursor = con.cursor()
            try:
                if params:
                    p = json.loads(params)
                    cursor.execute(query, p)
                else:
                    cursor.execute(query)
                results = cursor.fetchall()
                if results:
                    column_names = [d[0] for d in cursor.description]
                    table = Table(title="SQL Query Results (Standard)")
                    for n in column_names: table.add_column(n)
                    for r in results[:100]: table.add_row(*[str(i) for i in r])
                    console.print(table)
            except Exception as e:
                print(f"SQLite Error: {e}")
                raise typer.Exit(code=1)

@app.command()
def search(
    table_name: str = typer.Argument(..., help="The name of the table."),
    term: str = typer.Argument(..., help="The term to search for."),
    cols: str = typer.Option(None, "--cols", help="Comma-separated list of columns."),
):
    """
    Global search (Powered by Polars).
    """
    df = _get_pl_df(table_name)
    
    search_cols = cols.split(",") if cols else df.columns
    
    # Filter for string columns
    str_cols = [c for c in search_cols if df[c].dtype == pl.Utf8]
    
    if not str_cols:
        print("No string columns found to search in.")
        return

    # Multi-column search in Polars
    mask = pl.any_horizontal([pl.col(c).str.contains(term) for c in str_cols])
    results_df = df.filter(mask)

    if results_df.is_empty():
        print("No matches found.")
        return

    console = Console()
    table = Table(title=f"Search results in {table_name}")
    for col in results_df.columns: table.add_column(col)
    for row in results_df.head(50).rows(): table.add_row(*[str(i) for i in row])
    console.print(table)

@app.command()
def sort(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to sort by."),
    descending: bool = typer.Option(False, "--desc", help="Sort descending."),
):
    """
    Order results (Powered by Polars).
    """
    df = _get_pl_df(table_name).sort(column, descending=descending)

    console = Console()
    table = Table(title=f"Sorted {table_name} by {column}")
    for col in df.columns: table.add_column(col)
    for row in df.head(100).rows(): table.add_row(*[str(i) for i in row])
    console.print(table)

@app.command()
def distinct(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to list unique values for."),
    counts: bool = typer.Option(False, "--counts", help="Include frequency count."),
):
    """
    List unique values (Powered by Polars).
    """
    df = _get_pl_df(table_name)
    
    if counts:
        res = df.group_by(column).count().sort("count", descending=True)
        table = Table(title=f"Unique values in {column}")
        table.add_column(column); table.add_column("Count")
        for row in res.rows(): table.add_row(str(row[0]), str(row[1]))
    else:
        res = df.select(column).unique()
        table = Table(title=f"Unique values in {column}")
        table.add_column(column)
        for row in res.rows(): table.add_row(str(row[0]))

    Console().print(table)

@app.command(name="find-nulls")
def find_nulls(
    table_name: str = typer.Argument(..., help="The name of the table."),
    cols: str = typer.Option(None, "--cols", help="Comma-separated list of columns."),
):
    """
    Find incomplete rows (Powered by Polars).
    """
    df = _get_pl_df(table_name)
    columns_to_check = cols.split(",") if cols else df.columns
    
    mask = pl.any_horizontal([pl.col(c).is_null() for c in columns_to_check])
    null_rows = df.filter(mask)

    if null_rows.is_empty():
        print("No incomplete rows found.")
        return

    table = Table(title=f"Incomplete rows in {table_name}")
    for col in null_rows.columns: table.add_column(col)
    for row in null_rows.head(50).rows(): table.add_row(*[str(i) for i in row])
    Console().print(table)

@app.command()
def dups(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(None, help="The column to check for duplicates."),
):
    """
    Identify duplicates (Powered by Polars).
    """
    df = _get_pl_df(table_name)
    subset = [column] if column else df.columns
    
    duplicated_rows = df.filter(df.select(subset).is_duplicated())

    if duplicated_rows.is_empty():
        print("No duplicate rows found.")
        return

    table = Table(title=f"Duplicate rows in {table_name}")
    for col in duplicated_rows.columns: table.add_column(col)
    for row in duplicated_rows.head(50).rows(): table.add_row(*[str(i) for i in row])
    Console().print(table)
