import typer
import sqlite3
import pandas as pd
import subprocess
import sys
import json
import os
from rich.table import Table
from wrought_iron.cli.utils import _get_active_db

app = typer.Typer()

def _get_textual():
    try:
        from textual.app import App, ComposeResult
        from textual.widgets import (
            Header, Footer, DataTable, Static, TabbedContent, TabPane, 
            Input, Button, Label, Select, RichLog, Switch, Checkbox, LoadingIndicator
        )
        from textual.containers import Container, Horizontal, Vertical, VerticalScroll, Grid
        from textual.screen import Screen
        return App, ComposeResult, Header, Footer, DataTable, Static, TabbedContent, TabPane, Input, Button, Label, Container, Horizontal, Vertical, VerticalScroll, Select, RichLog, Switch, Checkbox, Grid, LoadingIndicator, Screen
    except ImportError:
        return None

REGISTRY = {
    "connect": {
        "file": [{"name": "path", "type": "pos", "help": "Path to DB file"}, {"name": "--read-only", "type": "flag", "help": "Read-only mode"}],
        "new": [{"name": "path", "type": "pos", "help": "New DB path"}, {"name": "--force", "type": "flag", "help": "Overwrite"}],
        "list": [{"name": "--limit", "type": "opt", "help": "Max rows", "default": "10"}],
        "alias": [{"name": "name", "type": "pos", "help": "Alias Name"}, {"name": "path", "type": "pos", "help": "DB Path"}],
        "merge": [{"name": "target_db", "type": "pos", "help": "Target"}, {"name": "source_db", "type": "pos", "help": "Source"}],
        "info": [{"name": "--extended", "type": "flag", "help": "Show WAL/Meta"}],
        "vacuum": [{"name": "--into", "type": "opt", "help": "Vacuum to file"}],
        "integrity-check": [{"name": "--quick", "type": "flag", "help": "Quick scan"}],
        "encrypt": [{"name": "path", "type": "pos", "help": "DB Path"}, {"name": "--output", "type": "opt", "help": "Output Path"}],
        "decrypt": [{"name": "path", "type": "pos", "help": "Encrypted DB Path"}]
    },
    "schema": {
        "list": [],
        "describe": [{"name": "table", "type": "pos", "help": "Table Name"}],
        "inspect": [{"name": "table", "type": "pos", "help": "Table Name"}],
        "diff": [{"name": "table_a", "type": "pos", "help": "Table A"}, {"name": "table_b", "type": "pos", "help": "Table B"}],
        "detect-json": [{"name": "table", "type": "pos", "help": "Table Name"}],
        "flatten": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "JSON Col"}],
        "rename-col": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "old", "type": "pos", "help": "Old"}, {"name": "new", "type": "pos", "help": "New"}],
        "drop-col": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Column"}],
        "cast": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--type", "type": "opt", "help": "New Type"}],
        "graph": []
    },
    "query": {
        "head": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "-n", "type": "opt", "help": "Rows", "default": "10"}],
        "tail": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "-n", "type": "opt", "help": "Rows", "default": "10"}],
        "sample": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "-n", "type": "opt", "help": "Rows", "default": "10"}],
        "filter": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "--where", "type": "opt", "help": "Query"}],
        "sql": [{"name": "query", "type": "pos", "help": "SQL"}],
        "search": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "term", "type": "pos", "help": "Search Term"}],
        "sort": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Column"}],
        "distinct": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Column"}],
        "find-nulls": [{"name": "table", "type": "pos", "help": "Table"}],
        "dups": [{"name": "table", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Column"}]
    },
    "aggregate": {
        "groupby": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "group_cols", "type": "pos", "help": "Cols"}, {"name": "--agg", "type": "opt", "help": "Agg Logic"}],
        "pivot": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "index", "type": "pos", "help": "Index"}, {"name": "columns", "type": "pos", "help": "Cols"}, {"name": "values", "type": "pos", "help": "Vals"}],
        "describe": [{"name": "table_name", "type": "pos", "help": "Table"}],
        "corr": [{"name": "table_name", "type": "pos", "help": "Table"}],
        "moving-avg": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "column", "type": "pos", "help": "Col"}, {"name": "--window", "type": "opt", "default": "5"}],
        "crosstab": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "row", "type": "pos", "help": "Row"}, {"name": "col", "type": "pos", "help": "Col"}],
        "skew": [{"name": "table_name", "type": "pos", "help": "Table"}],
        "kurtosis": [{"name": "table_name", "type": "pos", "help": "Table"}],
        "rank": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "column", "type": "pos", "help": "Col"}, {"name": "--desc", "type": "flag"}],
        "bin": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "column", "type": "pos", "help": "Col"}, {"name": "--bins", "type": "opt", "default": "10"}]
    },
    "clean": {
        "impute-mode": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}],
        "impute-group": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "target", "type": "pos", "help": "Target"}, {"name": "group_col", "type": "pos", "help": "Group"}],
        "ml-impute": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}],
        "dedupe": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}],
        "harmonize": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}],
        "regex-replace": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}, {"name": "pattern", "type": "pos", "help": "Regex"}, {"name": "repl", "type": "pos", "help": "Repl"}],
        "drop-outliers": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}, {"name": "--sigma", "type": "opt", "default": "3.0"}],
        "map": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}, {"name": "file", "type": "pos", "help": "Map CSV"}],
        "trim": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col", "type": "pos", "help": "Col"}],
        "validate-schema": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "rules_file", "type": "pos", "help": "JSON Rules"}]
    },
    "geo": {
        "validate": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}],
        "geocode": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "addr_col", "type": "pos", "help": "Address"}, {"name": "lookup_file", "type": "pos", "help": "CSV"}],
        "reverse": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}, {"name": "lookup_file", "type": "pos", "help": "CSV"}],
        "distance": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}, {"name": "--target-lat", "type": "opt"}, {"name": "--target-lon", "type": "opt"}],
        "cluster": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}],
        "centroid": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}],
        "bounds": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}],
        "heatmap": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}],
        "nearest": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}, {"name": "target_file", "type": "pos", "help": "POI CSV"}],
        "export-geojson": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "lat_col", "type": "pos", "help": "Lat"}, {"name": "lon_col", "type": "pos", "help": "Lon"}]
    },
    "audit": {
        "log-view": [{"name": "--limit", "type": "opt", "default": "50"}],
        "hash-create": [{"name": "table_name", "type": "pos", "help": "Table"}],
        "hash-verify": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "expected_hash", "type": "pos", "help": "Hash"}],
        "scan-pii": [{"name": "table_name", "type": "pos", "help": "Table"}],
        "anonymize": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col_name", "type": "pos", "help": "Col"}],
        "snapshot": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "name", "type": "pos", "help": "Snapshot Name"}],
        "rollback": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "name", "type": "pos", "help": "Snapshot Name"}],
        "encrypt-col": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col_name", "type": "pos", "help": "Col"}, {"name": "key_file", "type": "pos", "help": "Key Path"}],
        "decrypt-col": [{"name": "table_name", "type": "pos", "help": "Table"}, {"name": "col_name", "type": "pos", "help": "Col"}, {"name": "key_file", "type": "pos", "help": "Key Path"}],
        "export-cert": [{"name": "signer", "type": "pos", "help": "Name"}, {"name": "output", "type": "pos", "help": "PDF Path"}]
    }
}

@app.callback(invoke_without_command=True)
def interact(ctx: typer.Context):
    """Launch the Command Center TUI."""
    if ctx.invoked_subcommand is not None: return
    res = _get_textual()
    if not res:
        print("Error: 'textual' not found."); raise typer.Exit(1)
    App, ComposeResult, Header, Footer, DataTable, Static, TabbedContent, TabPane, Input, Button, Label, Container, Horizontal, Vertical, VerticalScroll, Select, RichLog, Switch, Checkbox, Grid, LoadingIndicator, Screen = res

    class DashboardHeader(Static):
        def compose(self) -> ComposeResult:
            yield Button("Refresh", id="refresh_btn", variant="primary")
            yield Label("Loading...", id="db_info")
        def update_info(self):
            db = _get_active_db()
            if db.exists():
                size_mb = db.stat().st_size / (1024 * 1024)
                self.query_one("#db_info", Label).update(f"  Connected: [bold]{db.name}[/bold] ({size_mb:.2f} MB)")
            else:
                self.query_one("#db_info", Label).update("[red]  No Database Connected[/red]")

    class TablesWidget(Static):
        def compose(self) -> ComposeResult:
            yield Label("[bold]Tables & Views[/bold]"); yield DataTable(id="tables_table")
        def on_mount(self):
            dt = self.query_one(DataTable); dt.add_columns("Name", "Rows"); dt.cursor_type = "row"
        def refresh_data(self):
            dt = self.query_one(DataTable); dt.clear(); db = _get_active_db()
            if not db.exists(): return
            with sqlite3.connect(db) as con:
                tables = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
                for (t,) in tables:
                    try: count = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                    except: count = "?"
                    dt.add_row(t, str(count))

    class CommandRunner(VerticalScroll):
        def compose(self) -> ComposeResult:
            yield Label("Select Module"); yield Select([(m, m) for m in REGISTRY.keys()], id="module_select")
            yield Label("Select Command"); yield Select([], id="command_select", disabled=True)
            yield Label("Arguments"); yield Container(id="args_container")
            yield Button("Execute", variant="primary", id="exec_btn", disabled=True)
            yield RichLog(id="output_log", highlight=True, markup=True)

        async def on_select_changed(self, event: Select.Changed) -> None:
            if event.select.id == "module_select":
                module = event.value
                if not module or module == Select.BLANK: return
                commands = [(c, c) for c in REGISTRY[module].keys()]
                cmd_select = self.query_one("#command_select", Select)
                cmd_select.set_options(commands); cmd_select.disabled = False
            elif event.select.id == "command_select":
                module = self.query_one("#module_select", Select).value
                cmd = event.value
                container = self.query_one("#args_container"); await container.remove_children()
                for arg in REGISTRY[module][cmd]:
                    container.mount(Label(f"{arg['name']} ({arg.get('help','')})"))
                    if arg['type'] == "flag": container.mount(Checkbox(id=f"field_{arg['name']}"))
                    else: container.mount(Input(placeholder=arg.get('help',''), id=f"field_{arg['name']}"))
                self.query_one("#exec_btn").disabled = False

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "exec_btn":
                module = self.query_one("#module_select", Select).value
                cmd_name = self.query_one("#command_select", Select).value
                full_cmd = [sys.executable, "-m", "wrought_iron.main", module, cmd_name]
                for arg in REGISTRY[module][cmd_name]:
                    widget = self.query_one(f"#field_{arg['name']}")
                    if arg['type'] == "flag":
                        if widget.value: full_cmd.append(arg['name'])
                    elif widget.value:
                        if arg['type'] == "pos": full_cmd.append(widget.value)
                        else: full_cmd.extend([arg['name'], widget.value])
                log = self.query_one("#output_log", RichLog)
                log.write(f"[yellow]Running: {' '.join(full_cmd)}[/yellow]")
                res = subprocess.run(full_cmd, capture_output=True, text=True)
                if res.stdout: log.write(res.stdout)
                if res.stderr: log.write(f"[red]{res.stderr}[/red]")

    class WroughtIronApp(App):
        CSS = "Screen { layout: vertical; } #dashboard { layout: grid; grid-size: 2 1; } CommandRunner { border: solid $accent; padding: 1; }"
        def compose(self) -> ComposeResult:
            yield Header()
            with TabbedContent():
                with TabPane("Dashboard"):
                    yield DashboardHeader()
                    yield Container(TablesWidget(), id="dashboard")
                with TabPane("Command Runner"): yield CommandRunner()
            yield Footer()
        def on_mount(self) -> None:
            self.title = "Wrought Iron Command Center"
            self.query_one(DashboardHeader).update_info()
            self.query_one(TablesWidget).refresh_data()

    WroughtIronApp().run()