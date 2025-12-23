import typer
import sqlite3
import pandas as pd
import polars as pl
import duckdb
from rich.console import Console
from rich.table import Table
from wrought_iron.cli.utils import _get_active_db
from enum import Enum
from typing import Optional

try:
    import wrought_iron_core
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False

class PivotFunc(str, Enum):
    mean = "mean"
    sum = "sum"
    count = "count"
    max = "max"
    min = "min"

class Normalize(str, Enum):
    index = "index"
    columns = "columns"
    all = "all"

app = typer.Typer()
console = Console()

def _get_pl_df(table_name: str) -> pl.DataFrame:
    active_db = _get_active_db()
    conn = duckdb.connect()
    try:
        return conn.execute(f"SELECT * FROM sqlite_scan('{active_db}', '{table_name}')").pl()
    except:
        with sqlite3.connect(active_db) as con:
            return pl.read_database(f"SELECT * FROM {table_name}", con)

@app.command()
def groupby(
    table_name: str = typer.Argument(..., help="The name of the table."),
    group_cols: str = typer.Argument(..., help="Columns to group by (comma-separated)."),
    agg: str = typer.Option(None, "--agg", help="Aggregations (e.g. 'val:sum,age:mean')."),
):
    """Group and summarize (High-Speed Polars)."""
    df = _get_pl_df(table_name)
    g_cols = [c.strip() for c in group_cols.split(",")]
    
    if agg:
        agg_exprs = []
        for pair in agg.split(","):
            col, func = pair.split(":")
            col = col.strip()
            func = func.strip()
            if func == "sum": agg_exprs.append(pl.col(col).sum())
            elif func == "mean": agg_exprs.append(pl.col(col).mean())
            elif func == "count": agg_exprs.append(pl.col(col).count())
            elif func == "max": agg_exprs.append(pl.col(col).max())
            elif func == "min": agg_exprs.append(pl.col(col).min())
        res = df.group_by(g_cols).agg(agg_exprs)
    else:
        res = df.group_by(g_cols).count()
    console.print(res)

@app.command()
def pivot(
    table_name: str = typer.Argument(..., help="The name of the table."),
    index: str = typer.Argument(..., help="Index column."),
    columns: str = typer.Argument(..., help="Columns to pivot."),
    values: str = typer.Argument(..., help="Values to aggregate."),
    func: PivotFunc = typer.Option(PivotFunc.mean, "--func", help="Aggregation function."),
):
    """Create pivot table (Polars Optimized)."""
    df = _get_pl_df(table_name)
    res = df.pivot(values=values, index=index, columns=columns, aggregate_function=func.value)
    console.print(res)

@app.command()
def describe(table_name: str = typer.Argument(..., help="The name of the table.")):
    """Summary statistics (Polars Optimized)."""
    df = _get_pl_df(table_name)
    console.print(df.describe())

@app.command()
def corr(table_name: str = typer.Argument(..., help="The name of the table.")):
    """Correlation matrix."""
    df = _get_pl_df(table_name)
    numeric_df = df.select(pl.col(pl.NUMERIC_DTYPES))
    res = numeric_df.to_pandas().corr()
    console.print(res)

@app.command(name="moving-avg")
def moving_avg(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to calculate."),
    window: int = typer.Option(5, "--window", help="Window size."),
):
    """Rolling window (Polars Optimized)."""
    df = _get_pl_df(table_name)
    res = df.with_columns([pl.col(column).rolling_mean(window_size=window).alias(f"{column}_moving_avg")])
    console.print(res.select([column, f"{column}_moving_avg"]).head(100))

@app.command()
def crosstab(
    table_name: str = typer.Argument(..., help="Table name."),
    row: str = typer.Argument(..., help="Row column."),
    col: str = typer.Argument(..., help="Column column."),
    normalize: Optional[Normalize] = typer.Option(None, "--normalize", help="Normalize results."),
    margins: bool = typer.Option(False, "--margins", help="Show margins."),
):
    """Frequency matrix (Pandas fallback)."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT {row}, {col} FROM {table_name}", con)
    res = pd.crosstab(df[row], df[col], normalize=normalize.value if normalize else False, margins=margins)
    console.print(res)

@app.command()
def skew(table_name: str = typer.Argument(..., help="Table name.")):
    """Calculate skewness (Rust Accelerated)."""
    df = _get_pl_df(table_name)
    for col in df.select(pl.col(pl.NUMERIC_DTYPES)).columns:
        data = df[col].drop_nulls().to_list()
        if CORE_AVAILABLE:
            stats = wrought_iron_core.calculate_stats_batch(data)
            console.print(f"{col}: [bold]{stats.get('skew', 0.0):.4f}[/bold]")
        else:
            console.print(f"{col}: {pl.Series(data).skew()}")

@app.command()
def kurtosis(table_name: str = typer.Argument(..., help="Table name.")):
    """Calculate kurtosis (Rust Accelerated)."""
    df = _get_pl_df(table_name)
    for col in df.select(pl.col(pl.NUMERIC_DTYPES)).columns:
        data = df[col].drop_nulls().to_list()
        if CORE_AVAILABLE:
            stats = wrought_iron_core.calculate_stats_batch(data)
            console.print(f"{col}: [bold]{stats.get('kurtosis', 0.0):.4f}[/bold]")
        else:
            console.print(f"{col}: {pl.Series(data).kurtosis()}")

@app.command()
def rank(
    table_name: str = typer.Argument(..., help="Table name."),
    column: str = typer.Argument(..., help="Column to rank."),
    desc: bool = typer.Option(False, "--desc", help="Rank descending."),
):
    """Rank rows (Polars)."""
    df = _get_pl_df(table_name)
    res = df.with_columns([pl.col(column).rank(descending=desc).alias(f"{column}_rank")])
    console.print(res.select([column, f"{column}_rank"]).head(100))

@app.command()
def bin(
    table_name: str = typer.Argument(..., help="Table name."),
    column: str = typer.Argument(..., help="Column to bin."),
    bins: int = typer.Option(10, "--bins", help="Number of bins."),
):
    """Discretize numbers (Polars)."""
    # Polars doesn't have cut yet in the same way, we use a simple approach or pandas
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT {column} FROM {table_name}", con)
    df[f"{column}_bin"] = pd.cut(df[column], bins=bins)
    console.print(df.head(100))