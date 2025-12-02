import typer
import sqlite3
import pandas as pd
from rich.console import Console
from rich.table import Table
from wrought_iron.cli.utils import _get_active_db
import json
from enum import Enum

class PivotFunc(str, Enum):
    mean = "mean"
    sum = "sum"
    count = "count"
    max = "max"

class Normalize(str, Enum):
    index = "index"
    columns = "columns"
    all = "all"

class IncludeOptions(str, Enum):
    all = "all"
    number = "number"
    object = "object"

class CorrMethod(str, Enum):
    pearson = "pearson"
    spearman = "spearman"
    kendall = "kendall"

class RankMethod(str, Enum):
    average = "average"
    min = "min"
    max = "max"
    first = "first"
    dense = "dense"

app = typer.Typer()

@app.command()
def groupby(
    table_name: str = typer.Argument(..., help="The name of the table."),
    group_cols: str = typer.Argument(..., help="Comma-separated list of columns to group by."),
    agg: str = typer.Option(None, "--agg", help="e.g., \"age:mean,salary:sum\""),
    pivot: str = typer.Option(None, "--pivot", help="Pivot result to wide format."),
):
    """
    Group and summarize.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    group_by_cols = [col.strip() for col in group_cols.split(",")]
    
    if agg:
        agg_dict = {}
        try:
            for item in agg.split(','):
                col, func = item.split(':')
                if col not in df.columns:
                    print(f"Error: Column '{col}' not found in table '{table_name}'.")
                    raise typer.Exit(code=1)
                agg_dict[col.strip()] = func.strip()
        except ValueError:
            print("Error: --agg format must be 'column:function,column:function,...'")
            raise typer.Exit(code=1)
        
        grouped_df = df.groupby(group_by_cols).agg(agg_dict)
    else:
        grouped_df = df.groupby(group_by_cols).size().reset_index(name='counts')

    if pivot:
        if len(group_by_cols) < 2:
            print("Error: --pivot requires at least two group-by columns.")
            raise typer.Exit(code=1)
        grouped_df = grouped_df.unstack(level=-1)

    console = Console()
    # a rich table for a multi-index dataframe is not trivial
    # for now, just print the dataframe
    console.print(grouped_df)

@app.command()
def pivot(
    table_name: str = typer.Argument(..., help="The name of the table."),
    index: str = typer.Argument(..., help="Column to use for the new index."),
    columns: str = typer.Argument(..., help="Column to use for the new columns."),
    values: str = typer.Argument(..., help="Column to aggregate."),
    func: PivotFunc = typer.Option(PivotFunc.mean, "--func", help="Aggregation function."),
    fill_value: str = typer.Option(None, "--fill-value", help="Value to replace missing values with."),
):
    """
    Create pivot table.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    try:
        pivot_df = df.pivot_table(
            index=index,
            columns=columns,
            values=values,
            aggfunc=func.value,
            fill_value=fill_value,
        )
    except Exception as e:
        print(f"Error creating pivot table: {e}")
        raise typer.Exit(code=1)

    console = Console()
    console.print(pivot_df)

@app.command()
def crosstab(
    table_name: str = typer.Argument(..., help="The name of the table."),
    row: str = typer.Argument(..., help="Column to display as rows."),
    col: str = typer.Argument(..., help="Column to display as columns."),
    normalize: Normalize = typer.Option(None, "--normalize", help="Show percentages."),
    margins: bool = typer.Option(False, "--margins", help="Add row/column subtotals."),
):
    """
    Frequency matrix.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if row not in df.columns or col not in df.columns:
        print(f"Error: One or both columns not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    crosstab_df = pd.crosstab(
        df[row],
        df[col],
        normalize=normalize.value if normalize else False,
        margins=margins,
    )

    console = Console()
    console.print(crosstab_df)

@app.command()
def describe(
    table_name: str = typer.Argument(..., help="The name of the table."),
    percentiles: str = typer.Option(None, "--percentiles", help="Comma-separated list of percentiles (0.0-1.0). e.g., '0.1,0.5,0.9'"),
    include: IncludeOptions = typer.Option(IncludeOptions.all, "--include", help="Include options."),
):
    """
    Summary statistics.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    percentiles_list = None
    if percentiles:
        try:
            percentiles_list = [float(p) for p in percentiles.split(',')]
            for p in percentiles_list:
                if not (0.0 <= p <= 1.0):
                    print("Error: Percentiles must be between 0.0 and 1.0.")
                    raise typer.Exit(code=1)
        except ValueError:
            print("Error: --percentiles must be a comma-separated list of floats.")
            raise typer.Exit(code=1)
    
    if include == IncludeOptions.all:
        include_param = 'all'
    elif include == IncludeOptions.number:
        include_param = ['number']
    else: # include == IncludeOptions.object
        include_param = ['object']

    described_df = df.describe(percentiles=percentiles_list, include=include_param)

    console = Console()
    console.print(described_df)

@app.command()
def corr(
    table_name: str = typer.Argument(..., help="The name of the table."),
    method: CorrMethod = typer.Option(CorrMethod.pearson, "--method", help="Method of correlation."),
    min_periods: int = typer.Option(None, "--min-periods", help="Minimum number of observations per pair of columns to have a valid result."),
):
    """
    Correlation matrix.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    numeric_df = df.select_dtypes(include=['number'])
    if numeric_df.empty:
        print("No numeric columns found for correlation calculation.")
        return

    if method == CorrMethod.spearman and min_periods is None:
        min_periods = 1
    
    correlation_matrix = numeric_df.corr(method=method.value, min_periods=min_periods)

    console = Console()
    console.print(correlation_matrix)

@app.command()
def skew(
    table_name: str = typer.Argument(..., help="The name of the table."),
    numeric_only: bool = typer.Option(False, "--numeric-only", help="Skip text columns."),
):
    """
    Calculate skewness.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    skewness = df.skew(numeric_only=numeric_only)

    console = Console()
    console.print(skewness)

@app.command()
def kurtosis(
    table_name: str = typer.Argument(..., help="The name of the table."),
    numeric_only: bool = typer.Option(False, "--numeric-only", help="Skip text columns."),
):
    """
    Calculate kurtosis.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    kurt = df.kurtosis(numeric_only=numeric_only)

    console = Console()
    console.print(kurt)

@app.command(name="moving-avg")
def moving_avg(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to calculate moving average for."),
    window: int = typer.Option(None, "--window", help="Size of the moving window."),
    center: bool = typer.Option(False, "--center", help="Center the window."),
    min_periods: int = typer.Option(None, "--min-periods", help="Minimum number of observations in window required to have a value."),
):
    """
    Rolling window calculation.
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

    if not pd.api.types.is_numeric_dtype(df[column]):
        print(f"Error: Column '{column}' is not numeric. Moving average can only be calculated for numeric columns.")
        raise typer.Exit(code=1)

    if window is None:
        print("Error: --window is required.")
        raise typer.Exit(code=1)
    
    moving_average = df[column].rolling(
        window=window,
        center=center,
        min_periods=min_periods
    ).mean()

    result_df = pd.DataFrame({
        column: df[column],
        f"{column}_moving_avg": moving_average
    })

    console = Console()
    console.print(result_df)

@app.command()
def rank(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to calculate rank for."),
    method: RankMethod = typer.Option(RankMethod.average, "--method", help="Ranking method."),
    pct: bool = typer.Option(False, "--pct", help="Compute percentile rank."),
):
    """
    Rank rows.
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

    ranked_series = df[column].rank(method=method.value, pct=pct)

    result_df = pd.DataFrame({
        column: df[column],
        f"{column}_rank": ranked_series
    })

    console = Console()
    console.print(result_df)

@app.command()
def bin(
    table_name: str = typer.Argument(..., help="The name of the table."),
    column: str = typer.Argument(..., help="The column to discretize."),
    bins: int = typer.Option(10, "--bins", help="Number of buckets."),
    labels: str = typer.Option(None, "--labels", help="Comma-separated custom names for buckets."),
):
    """
    Discretize numbers.
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

    if not pd.api.types.is_numeric_dtype(df[column]):
        print(f"Error: Column '{column}' is not numeric. Binning can only be applied to numeric columns.")
        raise typer.Exit(code=1)

    labels_list = None
    if labels:
        labels_list = labels.split(',')

    binned_series = pd.cut(df[column], bins=bins, labels=labels_list)
    
    result_df = pd.DataFrame({
        column: df[column],
        f"{column}_bin": binned_series
    })

    console = Console()
    console.print(result_df)
