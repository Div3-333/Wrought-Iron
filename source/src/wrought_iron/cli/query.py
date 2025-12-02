import typer
import sqlite3
import pandas as pd
from rich.console import Console
from rich.table import Table
from wrought_iron.cli.utils import _get_active_db
from enum import Enum
import json
import re

class QueryEngine(str, Enum):
    numexpr = "numexpr"
    python = "python"

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

@app.command()
def head(
    table_name: str = typer.Argument(..., help="The name of the table."),
    n: int = typer.Option(10, "-n", help="Number of rows (Default: 10)."),
):
    """
    Show first N rows.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Head of {table_name}")
    
    # Add columns to the rich table from DataFrame columns
    for col in df.columns:
        table.add_column(col)
        
    # Add rows to the rich table from DataFrame rows
    for _, row in df.head(n).iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)

@app.command()
def tail(
    table_name: str = typer.Argument(..., help="The name of the table."),
    n: int = typer.Option(10, "-n", help="Number of rows (Default: 10)."),
):
    """
    Show last N rows.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Tail of {table_name}")
    
    # Add columns to the rich table from DataFrame columns
    for col in df.columns:
        table.add_column(col)
        
    # Add rows to the rich table from DataFrame rows
    for _, row in df.tail(n).iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)

@app.command()
def sample(
    table_name: str = typer.Argument(..., help="The name of the table."),
    frac: float = typer.Option(None, "--frac", help="Fraction of rows to return (0.0-1.0)."),
    n: int = typer.Option(None, "-n", help="Number of rows."),
    seed: int = typer.Option(None, "--seed", help="Random seed for reproducibility."),
):
    """
    Get random subset.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if frac is not None and (frac < 0.0 or frac > 1.0):
        print("Error: --frac must be between 0.0 and 1.0.")
        raise typer.Exit(code=1)
    if n is not None and n <= 0:
        print("Error: -n must be a positive integer.")
        raise typer.Exit(code=1)

    if frac is not None:
        sample_df = df.sample(frac=frac, random_state=seed)
    elif n is not None:
        sample_df = df.sample(n=n, random_state=seed)
    else:
        sample_df = df.sample(n=10, random_state=seed) # Default to 10 rows if neither frac nor n is provided

    console = Console()
    table = Table(title=f"Sample of {table_name}")
    
    for col in sample_df.columns:
        table.add_column(col)
        
    for _, row in sample_df.iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)

@app.command()
def filter(
    table_name: str = typer.Argument(..., help="The name of the table."),
    where: str = typer.Option(..., "--where", help="Pandas query string (e.g., 'age > 20')."),
    engine: QueryEngine = typer.Option(QueryEngine.numexpr, "--engine", help="Backend speed optimization."),
):
    """
    Apply boolean logic.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    try:
        filtered_df = df.query(where, engine=engine.value)
    except Exception as e:
        print(f"Error applying filter: {e}")
        raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Filtered {table_name}")
    
    for col in filtered_df.columns:
        table.add_column(col)
        
    for _, row in filtered_df.iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)

@app.command()
def sql(
    query: str = typer.Argument(..., help="The SQL query to execute."),
    params: str = typer.Option(None, "--params", help="JSON string of parameters for safe injection."),
):
    """
    Execute raw SQL.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        try:
            if params:
                parsed_params = json.loads(params)
                if isinstance(parsed_params, dict):
                    # For named parameters
                    cursor.execute(query, parsed_params)
                elif isinstance(parsed_params, list):
                    # For positional parameters
                    cursor.execute(query, tuple(parsed_params))
                else:
                    print("Error: --params must be a JSON dictionary or list.")
                    raise typer.Exit(code=1)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            if results:
                # Get column names from cursor description
                column_names = [description[0] for description in cursor.description]
                
                table = Table(title="SQL Query Results")
                for col_name in column_names:
                    table.add_column(col_name)
                
                for row in results:
                    table.add_row(*[str(item) for item in row])
                
                console = Console()
                console.print(table)
            else:
                print("Query executed successfully, no results to display.")

        except json.JSONDecodeError:
            print("Error: --params is not a valid JSON string.")
            raise typer.Exit(code=1)
        except sqlite3.Error as e:
            print(f"Error executing SQL query: {e}")
            raise typer.Exit(code=1)

@app.command()
def search(
    table_name: str = typer.Argument(..., help="The name of the table."),
    term: str = typer.Argument(..., help="The term to search for."),
    cols: str = typer.Option(None, "--cols", help="Comma-separated list of columns to search."),
    case_sensitive: bool = typer.Option(False, "--case-sensitive/--ignore-case", help="Search is case-sensitive."),
    regex: bool = typer.Option(False, "--regex", help="Treat term as Regex."),
):
    """
    Global full-text search.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if cols:
        search_cols = cols.split(",")
        for col in search_cols:
            if col not in df.columns:
                print(f"Error: Column '{col}' not found in table '{table_name}'.")
                raise typer.Exit(code=1)
        df = df[search_cols]
    
    # Filter for string columns only
    string_df = df.select_dtypes(include=['object'])

    if string_df.empty:
        print("No string columns found to search in.")
        return

    # Prepare search pattern
    if not regex:
        term = re.escape(term)
    
    flags = 0 if case_sensitive else re.IGNORECASE
    pattern = re.compile(term, flags)

    # Perform search
    mask = string_df.apply(lambda col: col.astype(str).str.contains(pattern, na=False)).any(axis=1)
    results_df = df[mask]

    if results_df.empty:
        print("No matches found.")
        return

    console = Console()
    table = Table(title=f"Search results in {table_name}")
    
    for col in results_df.columns:
        table.add_column(col)
        
    for _, row in results_df.iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)

@app.command()
def sort(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to sort by."),
    order: SortOrder = typer.Option(SortOrder.asc, "--order", help="Sort order."),
    alg: SortAlgorithm = typer.Option(SortAlgorithm.quicksort, "--alg", help="Sorting algorithm."),
):
    """
    Order results.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if column not in df.columns:
        print(f"Error: Column '{column}' not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    ascending = True if order == SortOrder.asc else False
    
    try:
        sorted_df = df.sort_values(by=column, ascending=ascending, kind=alg.value)
    except Exception as e:
        print(f"Error sorting data: {e}")
        raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Sorted {table_name} by {column}")
    
    for col in sorted_df.columns:
        table.add_column(col)
        
    for _, row in sorted_df.iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)

@app.command()
def distinct(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to list unique values for."),
    counts: bool = typer.Option(False, "--counts", help="Include frequency count per value."),
):
    """
    List unique values.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if column not in df.columns:
        print(f"Error: Column '{column}' not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    console = Console()
    table = Table(title=f"Unique values in {column} of {table_name}")
    
    if counts:
        value_counts = df[column].value_counts().reset_index()
        value_counts.columns = [column, "Count"]
        
        table.add_column(column)
        table.add_column("Count", justify="right")
        
        for _, row in value_counts.iterrows():
            table.add_row(str(row[column]), str(row["Count"]))
    else:
        unique_values = df[column].unique()
        table.add_column(column)
        
        for val in unique_values:
            table.add_row(str(val))

    console.print(table)

@app.command(name="find-nulls")
def find_nulls(
    table_name: str = typer.Argument(..., help="The name of the table."),
    cols: str = typer.Option(None, "--cols", help="Comma-separated list of columns to check."),
    mode: NullMode = typer.Option(NullMode.any, "--mode", help="Row is null if any or all cols are null."),
):
    """
    Find incomplete rows.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    columns_to_check = df.columns.tolist()
    if cols:
        columns_to_check = cols.split(",")
        for col in columns_to_check:
            if col not in df.columns:
                print(f"Error: Column '{col}' not found in table '{table_name}'.")
                raise typer.Exit(code=1)
    
    if mode == NullMode.any:
        null_rows = df[df[columns_to_check].isnull().any(axis=1)]
    else: # mode == NullMode.all
        null_rows = df[df[columns_to_check].isnull().all(axis=1)]

    if null_rows.empty:
        print("No incomplete rows found.")
        return

    console = Console()
    table = Table(title=f"Incomplete rows in {table_name}")
    
    for col in null_rows.columns:
        table.add_column(col)
        
    for _, row in null_rows.iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)

@app.command()
def dups(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(None, help="The column to check for duplicates. If none, checks all columns."),
    keep: KeepOptions = typer.Option(KeepOptions.first, "--keep", help="Which duplicates to mark."),
):
    """
    Identify duplicates.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if column and column not in df.columns:
        print(f"Error: Column '{column}' not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    subset = [column] if column else None
    keep_value = keep.value if keep.value != 'none' else False
    duplicated_rows = df[df.duplicated(subset=subset, keep=keep_value)]

    if duplicated_rows.empty:
        print("No duplicate rows found.")
        return

    console = Console()
    table = Table(title=f"Duplicate rows in {table_name}")
    
    for col in duplicated_rows.columns:
        table.add_column(col)
        
    for _, row in duplicated_rows.iterrows():
        table.add_row(*[str(item) for item in row.values])

    console.print(table)
