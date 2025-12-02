import typer
import sqlite3
import pandas as pd
import plotext as plt
from wrought_iron.cli.utils import _get_active_db, _get_saved_theme, _save_theme
from enum import Enum
from typing import Optional

class AggFunc(str, Enum):
    sum = "sum"
    mean = "mean"
    count = "count"
    min = "min"
    max = "max"
    median = "median"

app = typer.Typer()

def _apply_common_styles(
    title: Optional[str],
    xlabel: Optional[str],
    ylabel: Optional[str],
    grid: bool,
    default_title: str,
    default_x: str,
    default_y: str
):
    plt.theme(_get_saved_theme())
    plt.limit_size(False, False)
    plt.plotsize(100, 30)
    
    plt.title(title if title else default_title)
    plt.xlabel(xlabel if xlabel else default_x)
    plt.ylabel(ylabel if ylabel else default_y)
    plt.grid(grid, grid)

@app.command()
def bar(
    table_name: str = typer.Argument(..., help="The name of the table."),
    cat: str = typer.Argument(..., help="The categorical column."),
    num: str = typer.Argument(..., help="The numerical column."),
    agg: AggFunc = typer.Option(None, "--agg", help="Pre-aggregation function."),
    stack: str = typer.Option(None, "--stack", help="Stack bars by this column."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    xlabel: str = typer.Option(None, "--xlabel", help="Override X-axis label."),
    ylabel: str = typer.Option(None, "--ylabel", help="Override Y-axis label."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
    color: str = typer.Option(None, "--color", help="Set bar color."),
):
    """
    Vertical Bar Chart.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if cat not in df.columns or num not in df.columns:
        print(f"Error: One or both columns not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    if stack and stack not in df.columns:
        print(f"Error: Stack column '{stack}' not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    plt.clf()
    _apply_common_styles(title, xlabel, ylabel, grid, f"Bar chart for {cat} vs {num}", cat, num)

    kwargs = {}
    if color:
        kwargs['color'] = color

    if agg:
        if stack:
            grouped = df.groupby([cat, stack])[num].agg(agg.value).unstack()
            labels = grouped.columns.tolist()
            data = [grouped[col].fillna(0).tolist() for col in labels]
            plt.multiple_bar(grouped.index.tolist(), data, labels=labels, **kwargs)
        else:
            agg_df = df.groupby(cat)[num].agg(agg.value)
            plt.bar(agg_df.index.tolist(), agg_df.values.tolist(), **kwargs)
    else:
        if stack:
            pivot_df = df.pivot(index=cat, columns=stack, values=num)
            labels = pivot_df.columns.tolist()
            data = [pivot_df[col].fillna(0).tolist() for col in labels]
            plt.multiple_bar(pivot_df.index.tolist(), data, labels=labels, **kwargs)
        else:
            plt.bar(df[cat].tolist(), df[num].tolist(), **kwargs)
    
    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def barh(
    table_name: str = typer.Argument(..., help="The name of the table."),
    cat: str = typer.Argument(..., help="The categorical column."),
    num: str = typer.Argument(..., help="The numerical column."),
    agg: AggFunc = typer.Option(None, "--agg", help="Pre-aggregation function."),
    stack: str = typer.Option(None, "--stack", help="Stack bars by this column."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    xlabel: str = typer.Option(None, "--xlabel", help="Override X-axis label."),
    ylabel: str = typer.Option(None, "--ylabel", help="Override Y-axis label."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
    color: str = typer.Option(None, "--color", help="Set bar color."),
):
    """
    Horizontal Bar Chart.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    if cat not in df.columns or num not in df.columns:
        print(f"Error: One or both columns not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    if stack and stack not in df.columns:
        print(f"Error: Stack column '{stack}' not found in table '{table_name}'.")
        raise typer.Exit(code=1)

    plt.clf()
    _apply_common_styles(title, xlabel, ylabel, grid, f"Horizontal Bar chart for {cat} vs {num}", num, cat)

    kwargs = {'orientation': 'h'}
    if color:
        kwargs['color'] = color

    if agg:
        if stack:
            grouped = df.groupby([cat, stack])[num].agg(agg.value).unstack()
            labels = grouped.columns.tolist()
            data = [grouped[col].fillna(0).tolist() for col in labels]
            plt.multiple_bar(grouped.index.tolist(), data, labels=labels, **kwargs)
        else:
            agg_df = df.groupby(cat)[num].agg(agg.value)
            plt.bar(agg_df.index.tolist(), agg_df.values.tolist(), **kwargs)
    else:
        if stack:
            pivot_df = df.pivot(index=cat, columns=stack, values=num)
            labels = pivot_df.columns.tolist()
            data = [pivot_df[col].fillna(0).tolist() for col in labels]
            plt.multiple_bar(pivot_df.index.tolist(), data, labels=labels, **kwargs)
        else:
            plt.bar(df[cat].tolist(), df[num].tolist(), **kwargs)
    
    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def hist(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to plot."),
    bins: int = typer.Option(10, "--bins", help="Number of bins."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    xlabel: str = typer.Option(None, "--xlabel", help="Override X-axis label."),
    ylabel: str = typer.Option(None, "--ylabel", help="Override Y-axis label."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
    color: str = typer.Option(None, "--color", help="Set bar color."),
):
    """
    Histogram.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {col} FROM {table_name}", con)
        except Exception:
            print(f"Error: Table '{table_name}' or column '{col}' not found.")
            raise typer.Exit(code=1)

    plt.clf()
    _apply_common_styles(title, xlabel, ylabel, grid, f"Histogram of {col}", col, "Frequency")
    
    kwargs = {}
    if color:
        kwargs['color'] = color

    plt.hist(df[col].tolist(), bins=bins, **kwargs)
    
    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def scatter(
    table_name: str = typer.Argument(..., help="The name of the table."),
    x: str = typer.Argument(..., help="X-axis column."),
    y: str = typer.Argument(..., help="Y-axis column."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    xlabel: str = typer.Option(None, "--xlabel", help="Override X-axis label."),
    ylabel: str = typer.Option(None, "--ylabel", help="Override Y-axis label."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
    color: str = typer.Option(None, "--color", help="Set point color."),
    marker: str = typer.Option(None, "--marker", help="Set point marker style (e.g., sd, hd, dot, heart)."),
):
    """
    Scatter Plot.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {x}, {y} FROM {table_name}", con)
        except Exception:
            print(f"Error: Columns '{x}' or '{y}' not found in '{table_name}'.")
            raise typer.Exit(code=1)

    plt.clf()
    _apply_common_styles(title, xlabel, ylabel, grid, f"Scatter plot: {x} vs {y}", x, y)
    
    kwargs = {}
    if color:
        kwargs['color'] = color
    if marker:
        kwargs['marker'] = marker
    
    plt.scatter(df[x].tolist(), df[y].tolist(), **kwargs)

    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def line(
    table_name: str = typer.Argument(..., help="The name of the table."),
    x: str = typer.Argument(..., help="X-axis column."),
    y: str = typer.Argument(..., help="Y-axis column."),
    sort: bool = typer.Option(True, "--sort", help="Sort by X axis."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    xlabel: str = typer.Option(None, "--xlabel", help="Override X-axis label."),
    ylabel: str = typer.Option(None, "--ylabel", help="Override Y-axis label."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
    color: str = typer.Option(None, "--color", help="Set line color."),
    marker: str = typer.Option(None, "--marker", help="Set point marker style."),
):
    """
    Line Chart.
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {x}, {y} FROM {table_name}", con)
        except Exception:
            print(f"Error: Columns not found.")
            raise typer.Exit(code=1)

    if sort:
        df = df.sort_values(by=x)

    plt.clf()
    _apply_common_styles(title, xlabel, ylabel, grid, f"Line chart: {x} vs {y}", x, y)

    kwargs = {}
    if color:
        kwargs['color'] = color
    if marker:
        kwargs['marker'] = marker

    try:
        plt.plot(df[x].tolist(), df[y].tolist(), **kwargs)
    except ValueError as e:
        if "Date Form" in str(e):
            print(f"Error: Date format mismatch. {e}")
            print("Try ensuring your date column matches the default format or use a string format.")
            # Attempt to auto-fix if it's standard ISO
            try:
                plt.date_form("Y-m-d") # Try ISO format if default fails
                plt.plot(df[x].tolist(), df[y].tolist(), **kwargs)
            except:
                 raise typer.Exit(code=1)
        else:
            print(f"Error plotting: {e}")
            raise typer.Exit(code=1)

    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def box(
    table_name: str = typer.Argument(..., help="The name of the table."),
    cols: str = typer.Argument(..., help="Comma-separated columns to plot."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    xlabel: str = typer.Option(None, "--xlabel", help="Override X-axis label."),
    ylabel: str = typer.Option(None, "--ylabel", help="Override Y-axis label."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
    color: str = typer.Option(None, "--color", help="Set box color."),
):
    """
    Box Plot.
    """
    active_db = _get_active_db()
    columns = cols.split(",")
    with sqlite3.connect(active_db) as con:
        try:
            # Sanitize columns? Assuming trusted input for now in CLI tool
            df = pd.read_sql_query(f"SELECT {cols} FROM {table_name}", con)
        except Exception:
            print(f"Error: Could not read columns.")
            raise typer.Exit(code=1)

    data = [df[col].dropna().tolist() for col in columns]
    
    plt.clf()
    _apply_common_styles(title, xlabel, ylabel, grid, f"Box plot of {cols}", "Columns", "Values")
    
    kwargs = {}
    if color:
        kwargs['color'] = color

    plt.box(data, **kwargs)
    # Set x-ticks to column names
    plt.xticks(range(1, len(columns) + 1), columns)
    
    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def heatmap(
    table_name: str = typer.Argument(..., help="The name of the table."),
    x: str = typer.Argument(..., help="X-axis column."),
    y: str = typer.Argument(..., help="Y-axis column."),
    bins: int = typer.Option(10, "--bins", help="Number of bins."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    xlabel: str = typer.Option(None, "--xlabel", help="Override X-axis label."),
    ylabel: str = typer.Option(None, "--ylabel", help="Override Y-axis label."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
):
    """
    Density Heatmap (2D Histogram).
    """
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {x}, {y} FROM {table_name}", con)
        except Exception:
            print(f"Error: Columns '{x}' or '{y}' not found.")
            raise typer.Exit(code=1)

    # Create 2D histogram using pandas/numpy logic
    df['x_bin'] = pd.cut(df[x], bins=bins, labels=False)
    df['y_bin'] = pd.cut(df[y], bins=bins, labels=False)
    
    matrix_df = df.groupby(['y_bin', 'x_bin']).size().unstack(fill_value=0)
    # Sort index to ensure correct orientation (high y at top usually, but matrix plot starts top-left)
    # We might need to flip if we want Cartesian y-axis behavior
    matrix_data = matrix_df.sort_index(ascending=False).values.tolist()

    plt.clf()
    _apply_common_styles(title, xlabel, ylabel, grid, f"Density Heatmap: {x} vs {y}", x, y)
    
    plt.matrix_plot(matrix_data)
    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def matrix(
    table_name: str = typer.Argument(..., help="The name of the table."),
    cols: str = typer.Argument(..., help="Comma-separated columns to plot."),
    out: str = typer.Option(None, "--out", help="Save the plot to a file."),
    # Deep Arguments
    title: str = typer.Option(None, "--title", help="Override chart title."),
    grid: bool = typer.Option(True, "--grid/--no-grid", help="Show grid lines."),
):
    """
    Scatter Matrix (Pair Plot).
    """
    active_db = _get_active_db()
    columns = cols.split(",")
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {cols} FROM {table_name}", con)
        except Exception:
            print(f"Error: Could not read columns.")
            raise typer.Exit(code=1)

    n = len(columns)
    plt.clf()
    _apply_common_styles(title, None, None, grid, "Scatter Matrix", "", "")
    
    plt.subplots(n, n)
    
    for i, y_col in enumerate(columns):
        for j, x_col in enumerate(columns):
            plt.subplot(i + 1, j + 1)
            if i == j:
                plt.hist(df[x_col].tolist())
                plt.title(x_col)
            else:
                plt.scatter(df[x_col].tolist(), df[y_col].tolist())
    
    if out:
        plt.savefig(out)
        print(f"Plot saved to {out}")
    else:
        plt.show()

@app.command()
def save():
    """
    Export chart info.
    """
    print("Note: To save a chart, use the --out option on the specific plot command.")
    print("Example: wi plot bar users age count --out my_chart.html")

@app.command()
def theme(
    name: str = typer.Argument(None, help="Theme name to preview or set (persisted)."),
):
    """
    List available themes.
    """
    # Access internal dictionary for theme names
    import plotext._dict
    themes = list(plotext._dict.themes.keys())
    
    if name:
        if name in themes:
            _save_theme(name)
            print(f"Theme set to '{name}'.")
        else:
            print(f"Theme '{name}' not found.")
    
    current = _get_saved_theme()
    print(f"Current theme: {current}")
    print("Available themes:")
    for t in themes:
        mark = "*" if t == current else " "
        print(f"{mark} {t}")
