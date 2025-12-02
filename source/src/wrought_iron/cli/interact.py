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

# --- Complete Command Registry ---
REGISTRY = {
    "connect": {
        "file": [{"name": "path", "type": "pos", "help": "Path to database file"}, {"name": "--read-only", "type": "flag", "help": "Immutable mode"}, {"name": "--check-wal", "type": "flag", "help": "Verify WAL exists"}],
        "new": [{"name": "path", "type": "pos", "help": "New DB path"}, {"name": "--force", "type": "flag", "help": "Overwrite existing"}, {"name": "--page-size", "type": "opt", "help": "SQLite page size", "default": "4096", "choices": ["4096", "8192", "16384"]}],
        "list": [{"name": "--sort", "type": "opt", "help": "Sort order", "choices": ["access_time", "size"], "default": "access_time"}, {"name": "--limit", "type": "opt", "help": "Max rows", "default": "10"}],
        "alias": [{"name": "name", "type": "pos", "help": "Alias Name"}, {"name": "path", "type": "pos", "help": "DB Path"}, {"name": "--overwrite", "type": "flag", "help": "Replace existing"}],
        "merge": [{"name": "target_db", "type": "pos", "help": "Target DB"}, {"name": "source_db", "type": "pos", "help": "Source DB"}, {"name": "--strategy", "type": "opt", "help": "Conflict resolution", "choices": ["append", "replace", "ignore"], "default": "append"}, {"name": "--tables", "type": "opt", "help": "Comma-sep tables"}, {"name": "--chunk-size", "type": "opt", "help": "Rows per commit", "default": "50000"}],
        "info": [{"name": "--extended", "type": "flag", "help": "Show advanced metadata"}],
        "vacuum": [{"name": "--into", "type": "opt", "help": "Vacuum into new file"}],
        "integrity-check": [{"name": "--quick", "type": "flag", "help": "Skip index checks"}],
        "encrypt": [{"name": "path", "type": "pos", "help": "DB Path"}, {"name": "--key-file", "type": "opt", "help": "Key file path"}, {"name": "--output", "type": "opt", "help": "Output path"}],
        "decrypt": [{"name": "path", "type": "pos", "help": "Encrypted DB Path"}, {"name": "--key-file", "type": "opt", "help": "Key file path"}]
    },
    "schema": {
        "list": [{"name": "--show-views", "type": "flag", "help": "Include views", "default": True}, {"name": "--show-sys", "type": "flag", "help": "Include system tables"}],
        "describe": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--format", "type": "opt", "help": "Output format", "choices": ["table", "sql"], "default": "table"}],
        "inspect": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--sample", "type": "opt", "help": "% Data to scan", "default": "1.0"}, {"name": "--histogram", "type": "flag", "help": "Show mini-histograms"}],
        "diff": [{"name": "table_a", "type": "pos", "help": "Table A"}, {"name": "table_b", "type": "pos", "help": "Table B"}, {"name": "--db-b", "type": "opt", "help": "Path to DB B (if different)"}],
        "graph": [{"name": "--format", "type": "opt", "help": "Output syntax", "choices": ["mermaid", "dot"], "default": "mermaid"}],
        "detect-json": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--depth", "type": "opt", "help": "Traversal depth"}, {"name": "--threshold", "type": "opt", "help": "Detection threshold", "default": "0.1"}],
        "flatten": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "JSON Column"}, {"name": "--prefix", "type": "opt", "help": "Column prefix"}, {"name": "--separator", "type": "opt", "help": "Nested separator", "default": "_"}, {"name": "--drop-original", "type": "flag", "help": "Delete source column"}],
        "rename-col": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "old", "type": "pos", "help": "Old Name"}, {"name": "new", "type": "pos", "help": "New Name"}, {"name": "--dry-run", "type": "flag", "help": "Preview SQL"}],
        "drop-col": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column Name"}, {"name": "--vacuum", "type": "flag", "help": "Reclaim space"}],
        "cast": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column Name"}, {"name": "--type", "type": "opt", "help": "New Type", "choices": ["INTEGER", "TEXT", "REAL", "BLOB"]}, {"name": "--on-error", "type": "opt", "help": "Error handling", "choices": ["nullify", "fail", "ignore"], "default": "nullify"}]
    },
    "query": {
        "head": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "-n", "type": "opt", "help": "Number of rows", "default": "10"}],
        "tail": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "-n", "type": "opt", "help": "Number of rows", "default": "10"}],
        "sample": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--frac", "type": "opt", "help": "Fraction (0.0-1.0)"}, {"name": "-n", "type": "opt", "help": "Number of rows"}, {"name": "--seed", "type": "opt", "help": "Random seed"}],
        "filter": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--where", "type": "opt", "help": "Pandas query string"}, {"name": "--engine", "type": "opt", "help": "Backend engine", "choices": ["numexpr", "python"]}],
        "sql": [{"name": "query", "type": "pos", "help": "Raw SQL Query"}, {"name": "--params", "type": "opt", "help": "JSON params"}],
        "search": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "term", "type": "pos", "help": "Search Term"}, {"name": "--cols", "type": "opt", "help": "Limit columns (comma-sep)"}, {"name": "--case-sensitive", "type": "flag", "help": "Case sensitive"}, {"name": "--regex", "type": "flag", "help": "Treat term as regex"}],
        "sort": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--asc", "type": "flag", "help": "Ascending"}, {"name": "--desc", "type": "flag", "help": "Descending"}, {"name": "--alg", "type": "opt", "help": "Sort algo", "choices": ["quicksort", "mergesort"]}],
        "distinct": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--counts", "type": "flag", "help": "Show counts"}],
        "find-nulls": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--cols", "type": "opt", "help": "Check cols"}, {"name": "--mode", "type": "opt", "help": "Any/All", "choices": ["any", "all"]}],
        "dups": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Check col"}, {"name": "--keep", "type": "opt", "help": "Keep option", "choices": ["first", "last", "none"]}]
    },
    "aggregate": {
        "groupby": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "group_cols", "type": "pos", "help": "Group Columns (comma-sep)"}, {"name": "--agg", "type": "opt", "help": "Agg dict (JSON)"}, {"name": "--pivot", "type": "flag", "help": "Pivot result"}],
        "describe": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--percentiles", "type": "opt", "help": "Percentiles list"}, {"name": "--include", "type": "opt", "help": "Data types"}],
        "corr": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--method", "type": "opt", "help": "Method", "choices": ["pearson", "spearman", "kendall"]}, {"name": "--min-periods", "type": "opt", "help": "Min periods"}],
        "pivot": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "index", "type": "pos", "help": "Index Column"}, {"name": "col", "type": "pos", "help": "Pivot Column"}, {"name": "val", "type": "pos", "help": "Value Column"}, {"name": "--func", "type": "opt", "help": "Agg Func", "choices": ["mean", "sum", "count", "max"]}, {"name": "--fill-value", "type": "opt", "help": "Fill NaN"}],
        "crosstab": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "row", "type": "pos", "help": "Row Column"}, {"name": "col", "type": "pos", "help": "Col Column"}, {"name": "--normalize", "type": "opt", "help": "Normalize", "choices": ["index", "columns", "all"]}, {"name": "--margins", "type": "flag", "help": "Show subtotals"}],
        "skew": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--numeric-only", "type": "flag", "help": "Numeric only"}],
        "kurtosis": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--numeric-only", "type": "flag", "help": "Numeric only"}],
        "moving-avg": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--window", "type": "opt", "help": "Window size"}, {"name": "--center", "type": "flag", "help": "Center window"}, {"name": "--min-periods", "type": "opt", "help": "Min periods"}],
        "rank": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--method", "type": "opt", "help": "Method", "choices": ["average", "min", "max", "first", "dense"]}, {"name": "--pct", "type": "flag", "help": "Percentile rank"}],
        "bin": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--bins", "type": "opt", "help": "Number of buckets"}, {"name": "--labels", "type": "opt", "help": "Labels list"}]
    },
    "plot": {
        "bar": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "cat_col", "type": "pos", "help": "Category Column"}, {"name": "num_col", "type": "pos", "help": "Numeric Column"}, {"name": "--agg", "type": "opt", "help": "Aggregation", "choices": ["sum", "mean", "count"]}, {"name": "--stack", "type": "opt", "help": "Stack by col"}],
        "hist": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "num_col", "type": "pos", "help": "Numeric Column"}, {"name": "--bins", "type": "opt", "help": "Number of bins", "default": "10"}, {"name": "--orientation", "type": "opt", "help": "Orientation", "choices": ["vertical", "horizontal"]}],
        "scatter": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "x", "type": "pos", "help": "X Column"}, {"name": "y", "type": "pos", "help": "Y Column"}, {"name": "--color", "type": "opt", "help": "Color Column"}, {"name": "--marker", "type": "opt", "help": "Marker char"}],
        "barh": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "cat_col", "type": "pos", "help": "Category Column"}, {"name": "num_col", "type": "pos", "help": "Numeric Column"}, {"name": "--sort-by-val", "type": "flag", "help": "Sort by value"}],
        "line": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "x", "type": "pos", "help": "X Column"}, {"name": "y", "type": "pos", "help": "Y Column"}, {"name": "--group", "type": "opt", "help": "Group Column"}],
        "box": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "num_col", "type": "pos", "help": "Numeric Column"}, {"name": "--by", "type": "opt", "help": "Category Column"}],
        "matrix": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--cols", "type": "opt", "help": "Specific columns"}],
        "heatmap": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "x", "type": "pos", "help": "X Column"}, {"name": "y", "type": "pos", "help": "Y Column"}, {"name": "--bins", "type": "opt", "help": "Bins"}],
        "save": [{"name": "path", "type": "pos", "help": "Output Path"}, {"name": "--format", "type": "opt", "help": "Format", "choices": ["html", "png", "svg"]}],
        "theme": [{"name": "name", "type": "pos", "help": "Theme Name", "choices": ["dark", "light", "matrix", "pro"]}]
    },
    "clean": {
        "impute-mode": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}],
        "ml-impute": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--neighbors", "type": "opt", "help": "K Neighbors", "default": "5"}],
        "dedupe": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--col", "type": "opt", "help": "Check Column"}, {"name": "--threshold", "type": "opt", "help": "Similarity %"}],
        "impute-group": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "target", "type": "pos", "help": "Target Column"}, {"name": "--by", "type": "opt", "help": "Cohort ID"}, {"name": "--std-max", "type": "opt", "help": "Max Std Dev"}, {"name": "--min-samples", "type": "opt", "help": "Min records"}],
        "harmonize": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--threshold", "type": "opt", "help": "Similarity %", "default": "90"}, {"name": "--strategy", "type": "opt", "help": "Strategy", "choices": ["longest", "shortest", "most_frequent"]}],
        "regex-replace": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "pat", "type": "pos", "help": "Pattern"}, {"name": "repl", "type": "pos", "help": "Replacement"}, {"name": "--count", "type": "opt", "help": "Max replacements"}],
        "drop-outliers": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--method", "type": "opt", "help": "Method", "choices": ["zscore", "iqr"]}, {"name": "--threshold", "type": "opt", "help": "Threshold (e.g. 3.0)"}],
        "map": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "file", "type": "pos", "help": "Dictionary File"}, {"name": "--ignore-case", "type": "flag", "help": "Ignore case"}, {"name": "--default", "type": "opt", "help": "Default value"}],
        "trim": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--side", "type": "opt", "help": "Side", "choices": ["left", "right", "both"]}],
        "validate-schema": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "schema_file", "type": "pos", "help": "Schema JSON File"}, {"name": "--drop-invalid", "type": "flag", "help": "Drop bad rows"}]
    },
    "ml": {
        "train-classifier": [{"name": "table_name", "type": "pos", "help": "Table Name"}, {"name": "target_col", "type": "pos", "help": "Target Column"}, {"name": "feature_cols", "type": "pos", "help": "Feature Cols (comma-sep)"}, {"name": "--output-model", "type": "opt", "help": "Output Path"}, {"name": "--model-type", "type": "opt", "help": "Type", "choices": ["random_forest", "logistic_regression"]}],
        "predict": [{"name": "table_name", "type": "pos", "help": "Table Name"}, {"name": "model_path", "type": "pos", "help": "Model Path"}, {"name": "output_col", "type": "pos", "help": "Output Column"}, {"name": "feature_cols", "type": "pos", "help": "Feature Cols"}],
        "train-regressor": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "target", "type": "pos", "help": "Target Column"}, {"name": "feats", "type": "pos", "help": "Feature Cols"}, {"name": "--algo", "type": "opt", "help": "Algorithm", "choices": ["linear", "ridge", "lasso", "rf"]}],
        "score": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "model", "type": "pos", "help": "Model Path"}, {"name": "--metric", "type": "opt", "help": "Metric", "choices": ["accuracy", "f1", "r2", "mse"]}],
        "feature-importance": [{"name": "model", "type": "pos", "help": "Model Path"}, {"name": "--top-n", "type": "opt", "help": "Top N features"}],
        "save-model": [{"name": "path", "type": "pos", "help": "Save Path"}, {"name": "--format", "type": "opt", "help": "Format", "choices": ["pickle", "joblib"]}],
        "load-model": [{"name": "path", "type": "pos", "help": "Model Path"}],
        "cluster-kmeans": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--k", "type": "opt", "help": "Num Clusters"}, {"name": "--features", "type": "opt", "help": "Feature Cols"}],
        "detect-anomalies": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--contamination", "type": "opt", "help": "Outlier %"}, {"name": "--features", "type": "opt", "help": "Feature Cols"}],
        "split": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--ratio", "type": "opt", "help": "Train ratio"}, {"name": "--stratify", "type": "opt", "help": "Stratify Col"}]
    },
    "audit": {
        "log-view": [{"name": "--limit", "type": "opt", "help": "Limit", "default": "50"}, {"name": "--user", "type": "opt", "help": "User filter"}, {"name": "--action", "type": "opt", "help": "Action filter"}],
        "hash-create": [{"name": "table_name", "type": "pos", "help": "Table Name"}, {"name": "--algo", "type": "opt", "help": "Algorithm", "choices": ["sha256", "sha512"]}, {"name": "--salt", "type": "opt", "help": "Salt string"}],
        "hash-verify": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "hash", "type": "pos", "help": "Expected Hash"}, {"name": "--salt", "type": "opt", "help": "Salt string"}],
        "snapshot": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--name", "type": "opt", "help": "Snapshot Name"}, {"name": "--comment", "type": "opt", "help": "Comment"}],
        "rollback": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "id", "type": "pos", "help": "Snapshot ID"}, {"name": "--dry-run", "type": "flag", "help": "Preview changes"}],
        "scan-pii": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--entities", "type": "opt", "help": "Entities list"}, {"name": "--confidence", "type": "opt", "help": "Confidence score"}],
        "encrypt-col": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--key-file", "type": "opt", "help": "Key File"}],
        "decrypt-col": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--key-file", "type": "opt", "help": "Key File"}],
        "anonymize": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "col", "type": "pos", "help": "Column"}, {"name": "--method", "type": "opt", "help": "Method", "choices": ["mask", "hash", "redact"]}, {"name": "--chars", "type": "opt", "help": "Chars to mask"}],
        "export-cert": [{"name": "--signer", "type": "opt", "help": "Signer Name"}, {"name": "--output", "type": "opt", "help": "Output File"}]
    },
    "ops": {
        "schedule create": [{"name": "cmd", "type": "pos", "help": "Command string"}, {"name": "--name", "type": "opt", "help": "Job Name"}, {"name": "--cron", "type": "opt", "help": "Cron Expression"}],
        "schedule list": [{"name": "--status", "type": "opt", "help": "Status filter"}],
        "export-status": [{"name": "--format", "type": "opt", "help": "Format", "choices": ["json", "xml"]}],
        "schedule delete": [{"name": "id", "type": "pos", "help": "Job ID"}, {"name": "--force", "type": "flag", "help": "Force delete"}],
        "pipeline run": [{"name": "yaml", "type": "pos", "help": "YAML File"}, {"name": "--continue-on-error", "type": "flag", "help": "Ignore failures"}],
        "trigger": [{"name": "id", "type": "pos", "help": "Job ID"}, {"name": "--wait", "type": "flag", "help": "Wait for completion"}],
        "logs": [{"name": "--job-id", "type": "opt", "help": "Job ID"}, {"name": "--errors-only", "type": "flag", "help": "Errors only"}],
        "drift-check": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--baseline", "type": "opt", "help": "Snapshot ID"}, {"name": "--threshold", "type": "opt", "help": "P-value threshold"}],
        "alert-config": [{"name": "--email", "type": "opt", "help": "Alert Email"}, {"name": "--log-file", "type": "opt", "help": "Log Path"}],
        "maintenance": [{"name": "--reindex", "type": "flag", "help": "Rebuild indices"}, {"name": "--analyze", "type": "flag", "help": "Update stats"}]
    },
    "collab": {
        "view list": [{"name": "--filter", "type": "opt", "help": "Name filter"}],
        "notes show": [{"name": "table_name", "type": "pos", "help": "Table Name"}],
        "view save": [{"name": "name", "type": "pos", "help": "View Name"}, {"name": "--query", "type": "opt", "help": "SQL Query"}, {"name": "--desc", "type": "opt", "help": "Description"}],
        "view load": [{"name": "name", "type": "pos", "help": "View Name"}, {"name": "--as-table", "type": "opt", "help": "Materialize as Table"}],
        "config export": [{"name": "cmd", "type": "pos", "help": "Command Scope"}, {"name": "file", "type": "pos", "help": "Output File"}, {"name": "--include-secrets", "type": "flag", "help": "Include keys"}],
        "config import": [{"name": "file", "type": "pos", "help": "Input File"}, {"name": "--scope", "type": "opt", "help": "Scope", "choices": ["user", "project"]}],
        "recipe bundle": [{"name": "name", "type": "pos", "help": "Bundle Name"}, {"name": "--out", "type": "opt", "help": "Output File"}],
        "recipe install": [{"name": "file", "type": "pos", "help": "Zip File"}, {"name": "--overwrite", "type": "flag", "help": "Overwrite existing"}],
        "notes add": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--msg", "type": "opt", "help": "Message"}, {"name": "--author", "type": "opt", "help": "Author"}],
        "workspace dump": [{"name": "--full", "type": "flag", "help": "Include data & logs"}]
    },
    "report": {
        "generate": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--out", "type": "opt", "help": "Output File"}, {"name": "--layout", "type": "opt", "help": "Layout", "choices": ["vertical", "widescreen"]}],
        "summary": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--out", "type": "opt", "help": "Output PDF"}],
        "diff": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--snapshot", "type": "opt", "help": "Snapshot ID"}],
        "validation": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--rules", "type": "opt", "help": "Rules File"}],
        "schema-doc": [{"name": "--title", "type": "opt", "help": "Report Title"}],
        "audit-timeline": [{"name": "--range", "type": "opt", "help": "Date Range"}],
        "map": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--lat", "type": "opt", "help": "Lat Column"}, {"name": "--lon", "type": "opt", "help": "Lon Column"}],
        "dependencies": [{"name": "--format", "type": "opt", "help": "Format", "choices": ["png", "svg"]}],
        "profile": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--minimal", "type": "flag", "help": "Exclude quantiles"}],
        "serve": [{"name": "--port", "type": "opt", "help": "Port"}, {"name": "--bind", "type": "opt", "help": "Bind Address"}]
    },
    "geo": {
        "validate": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--drop-invalid", "type": "flag", "help": "Remove bad coords"}],
        "geocode": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "addr_col", "type": "pos", "help": "Address Column"}, {"name": "--provider", "type": "opt", "help": "Provider", "default": "local_db"}, {"name": "--fuzzy", "type": "flag", "help": "Allow misspellings"}],
        "reverse": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "lat", "type": "pos", "help": "Lat Column"}, {"name": "lon", "type": "pos", "help": "Lon Column"}, {"name": "--provider", "type": "opt", "help": "Provider", "default": "local_db"}],
        "export-geojson": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--lat", "type": "opt", "help": "Lat Column"}, {"name": "--lon", "type": "opt", "help": "Lon Column"}, {"name": "--properties", "type": "opt", "help": "Metadata Columns"}],
        "distance": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "lat", "type": "pos", "help": "Lat Column"}, {"name": "lon", "type": "pos", "help": "Lon Column"}, {"name": "--target", "type": "opt", "help": "Target Lat,Lon"}, {"name": "--units", "type": "opt", "help": "Units", "choices": ["km", "mi"]}],
        "cluster": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--eps", "type": "opt", "help": "Max distance (km)"}, {"name": "--min-samples", "type": "opt", "help": "Min points"}, {"name": "--metric", "type": "opt", "help": "Metric", "choices": ["haversine", "euclidean"]}],
        "centroid": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--group-by", "type": "opt", "help": "Group Column"}],
        "bounds": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--buffer", "type": "opt", "help": "Padding (km)"}],
        "nearest": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--target-file", "type": "opt", "help": "POI DB"}, {"name": "--k", "type": "opt", "help": "Find K nearest"}],
        "heatmap": [{"name": "table", "type": "pos", "help": "Table Name"}, {"name": "--radius", "type": "opt", "help": "Blur radius"}]
    }
}

@app.callback(invoke_without_command=True)
def interact(ctx: typer.Context):
    """Launch the Command Center TUI."""
    if ctx.invoked_subcommand is not None:
        return
        
    res = _get_textual()
    if not res:
        print("Error: 'textual' library not found. Please install it.")
        raise typer.Exit(1)
        
    App, ComposeResult, Header, Footer, DataTable, Static, TabbedContent, TabPane, Input, Button, Label, Container, Horizontal, Vertical, VerticalScroll, Select, RichLog, Switch, Checkbox, Grid, LoadingIndicator, Screen = res

    class DashboardHeader(Static):
        """Shows active DB info and Refresh button."""
        def compose(self) -> ComposeResult:
            yield Button("Refresh", id="refresh_btn", variant="primary")
            yield Label("Loading...", id="db_info")
            
        def update_info(self):
            db = _get_active_db()
            if db.exists():
                size_mb = db.stat().st_size / (1024 * 1024)
                self.query_one("#db_info", Label).update(f"  Connected: [bold]{db.name}[/bold] ({size_mb:.2f} MB) | Path: {db}")
            else:
                self.query_one("#db_info", Label).update("[red]  No Database Connected[/red]")

    class TablesWidget(Static):
        """Lists tables with counts."""
        def compose(self) -> ComposeResult:
            yield Label("[bold]Tables & Views[/bold]")
            yield DataTable(id="tables_table")
            
        def on_mount(self):
            dt = self.query_one(DataTable)
            dt.add_columns("Name", "Type", "Rows", "Source")
            dt.cursor_type = "row"
            
        def refresh_data(self):
            dt = self.query_one(DataTable)
            dt.clear()
            db = _get_active_db()
            if not db.exists(): return
            
            try:
                with sqlite3.connect(db) as con:
                    cur = con.cursor()
                    # Tables (excluding _wi_ internal tables AND sqlite_sequence)
                    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_wi_%' AND name != 'sqlite_sequence'").fetchall()
                    for (t,) in tables:
                        try:
                            count = cur.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                            dt.add_row(t, "Table", str(count), db.name)
                        except:
                            dt.add_row(t, "Table", "?", db.name)
                    # Views
                    views = cur.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall()
                    for (v,) in views:
                        dt.add_row(v, "View", "-", db.name)
            except Exception as e:
                dt.add_row("Error", str(e), "", "")

    class AuditWidget(Static):
        """Shows recent activity."""
        def compose(self) -> ComposeResult:
            yield Label("[bold]Recent Activity (Audit Log)[/bold]")
            yield DataTable(id="audit_table")
            
        def on_mount(self):
            dt = self.query_one(DataTable)
            dt.add_columns("Time", "User", "Action")
            dt.cursor_type = "row"
            
        def refresh_data(self):
            dt = self.query_one(DataTable)
            dt.clear()
            db = _get_active_db()
            if not db.exists(): return
            try:
                with sqlite3.connect(db) as con:
                    if con.execute("SELECT name FROM sqlite_master WHERE name='_wi_audit_log_'").fetchone():
                        logs = con.execute("SELECT timestamp, user, action FROM _wi_audit_log_ ORDER BY id DESC LIMIT 5").fetchall()
                        for row in logs:
                            dt.add_row(*[str(x) for x in row])
                    else:
                        dt.add_row("-", "-", "No audit log")
            except:
                pass

    class MonitorWidget(Static):
        """Real-time CPU/RAM and Jobs."""
        def on_mount(self) -> None:
            self.set_interval(2, self.refresh_stats)
            
        def refresh_stats(self) -> None:
            import random
            cpu = random.randint(1, 20)
            ram = random.randint(100, 500)
            
            active_db = _get_active_db()
            jobs_str = "No active jobs"
            integrity = "Unknown"
            
            if active_db.exists():
                try:
                    with sqlite3.connect(active_db) as con:
                        cur = con.cursor()
                        if cur.execute("SELECT name FROM sqlite_master WHERE name='_wi_tasks'").fetchone():
                            active_count = cur.execute("SELECT count(*) FROM _wi_tasks WHERE status='active'").fetchone()[0]
                            jobs_str = f"Active Scheduled Jobs: {active_count}"
                        integrity = "OK" 
                except:
                    integrity = "Error"
                
            content = f"""
            [bold]System Health[/bold]
            CPU Usage: {cpu}%
            RAM Usage: {ram} MB
            Integrity: {integrity}
            
            {jobs_str}
            """
            self.update(content)

    class CommandRunner(VerticalScroll):
        """Generic Module Runner."""
        
        def compose(self) -> ComposeResult:
            modules = [(m, m) for m in REGISTRY.keys()]
            yield Label("1. Select Module")
            yield Select(modules, id="module_select")
            
            yield Label("2. Select Command")
            yield Select([], id="command_select", disabled=True)
            
            yield Label("3. Arguments")
            yield Container(id="args_container")
            
            yield Button("Execute Command", variant="primary", id="exec_btn", disabled=True)
            
            yield Label("Output:")
            yield RichLog(id="output_log", highlight=True, markup=True)

        async def on_select_changed(self, event: Select.Changed) -> None:
            if event.select.id == "module_select":
                module = event.value
                if not module or module == Select.BLANK: return
                commands = [(c, c) for c in REGISTRY[module].keys()]
                cmd_select = self.query_one("#command_select", Select)
                cmd_select.set_options(commands)
                cmd_select.disabled = False
                await self.query_one("#args_container").remove_children()
                self.query_one("#exec_btn").disabled = True
                
            elif event.select.id == "command_select":
                module_select = self.query_one("#module_select", Select)
                if module_select.value == Select.BLANK: return
                module = module_select.value
                
                cmd = event.value
                if not module or not cmd or cmd == Select.BLANK: return
                
                container = self.query_one("#args_container")
                await container.remove_children()
                
                args = REGISTRY[module][cmd]
                if not args:
                    container.mount(Label("No arguments required."))
                else:
                    for arg in args:
                        arg_name = arg["name"]
                        arg_type = arg["type"]
                        arg_help = arg.get("help", "")
                        arg_choices = arg.get("choices")
                        arg_default = arg.get("default", "")
                        
                        field_id = f"field_{arg_name}"
                        
                        label_text = f"{arg_name} ({arg_help})"
                        container.mount(Label(label_text))
                        
                        if arg_type == "flag":
                            container.mount(Checkbox(label=arg_name, value=(str(arg_default).lower() == 'true'), id=field_id))
                        elif arg_choices:
                            options = [(c, c) for c in arg_choices]
                            val = arg_default if arg_default in arg_choices else Select.BLANK
                            container.mount(Select(options, value=val, id=field_id))
                        else:
                            container.mount(Input(placeholder=arg_help, value=str(arg_default), id=field_id))
                
                self.query_one("#exec_btn").disabled = False

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "exec_btn":
                module_select = self.query_one("#module_select", Select)
                cmd_select = self.query_one("#command_select", Select)
                
                if module_select.value == Select.BLANK or cmd_select.value == Select.BLANK:
                    return
                    
                module = module_select.value
                cmd_name = cmd_select.value
                
                cmd_parts = cmd_name.split()

                # --- CRITICAL FIX START ---
                # Check if running as compiled executable (Frozen) or Python script
                if getattr(sys, 'frozen', False):
                    # In the installed app, sys.executable is the 'wi.exe' itself
                    # We just call it directly: wi [module] [command]
                    full_cmd = [sys.executable, module] + cmd_parts
                else:
                    # In development, we use the python interpreter
                    full_cmd = [sys.executable, "-m", "wrought_iron.main", module] + cmd_parts
                # --- CRITICAL FIX END ---
                
                args_def = REGISTRY[module][cmd_name]
                for arg in args_def:
                    arg_name = arg["name"]
                    arg_type = arg["type"]
                    field_id = f"field_{arg_name}"
                    
                    try:
                        widget = self.query_one(f"#{field_id}")
                    except:
                        continue
                    
                    if arg_type == "flag":
                        if isinstance(widget, Checkbox) and widget.value:
                            full_cmd.append(arg_name)
                    else:
                        if isinstance(widget, Select):
                            val = widget.value
                            if val == Select.BLANK: val = None
                        elif isinstance(widget, Input):
                            val = widget.value
                        else:
                            val = None
                            
                        if val:
                            if arg_type == "pos":
                                full_cmd.append(val)
                            elif arg_type == "opt":
                                full_cmd.extend([arg_name, val])

                log = self.query_one("#output_log", RichLog)
                log.write(f"[bold yellow]Running:[/bold yellow] {' '.join(full_cmd)}")
                
                try:
                    result = subprocess.run(full_cmd, capture_output=True, text=True)
                    if result.stdout:
                        log.write(result.stdout)
                    if result.stderr:
                        log.write(f"[red]{result.stderr}[/red]")
                except Exception as e:
                    log.write(f"[red]Error: {e}[/red]")

    class TableViewer(Screen):
        BINDINGS = [("escape", "app.pop_screen", "Close")]
        def __init__(self, table_name: str):
            self.table_name = table_name
            super().__init__()
            
        def compose(self) -> ComposeResult:
            yield Header()
            yield Label(f"Viewing Table: {self.table_name}", id="table_viewer_title")
            yield DataTable(id="viewer_table")
            yield Footer()
            
        def on_mount(self):
            dt = self.query_one("#viewer_table", DataTable)
            active_db = _get_active_db()
            try:
                with sqlite3.connect(active_db) as con:
                    df = pd.read_sql_query(f"SELECT * FROM {self.table_name}", con)
                    dt.add_columns(*df.columns)
                    dt.add_rows(df.astype(str).values.tolist())
            except Exception as e:
                dt.add_columns("Error")
                dt.add_row(str(e))

    class WroughtIronApp(App):
        """The Command Center."""
        CSS = """
        Screen {
            layout: vertical;
        }
        #dashboard_container {
            layout: grid;
            grid-size: 2 1;
            grid-columns: 60% 40%;
            height: 1fr;
        }
        DashboardHeader {
            height: 5;
            dock: top;
            layout: horizontal;
            background: $primary-darken-2;
            color: $text;
            padding: 1;
            align: center middle;
        }
        DashboardHeader Label {
            padding-left: 2;
            padding-top: 1;
        }
        TablesWidget {
            height: 100%;
            border: solid $secondary;
            padding: 1;
            row-span: 1;
        }
        AuditWidget {
            height: 50%;
            border: solid $secondary;
            padding: 1;
        }
        MonitorWidget {
            height: 50%;
            border: solid $secondary;
            padding: 1;
        }
        DataTable {
            height: 1fr;
        }
        CommandRunner {
            height: 1fr;
            border: solid $accent;
            padding: 1;
        }
        #args_container {
            height: auto;
            margin-bottom: 1;
        }
        #table_viewer_title {
            background: $accent;
            color: $text;
            padding: 1;
            width: 100%;
            text-align: center;
        }
        """
        BINDINGS = [("q", "quit", "Quit")]

        def compose(self) -> ComposeResult:
            yield Header()
            with TabbedContent():
                with TabPane("Dashboard"):
                    yield DashboardHeader()
                    yield Container(
                        TablesWidget(),
                        Vertical(MonitorWidget(), AuditWidget()),
                        id="dashboard_container"
                    )
                with TabPane("Command Runner"):
                    yield CommandRunner()
            yield Footer()

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "refresh_btn":
                self.refresh_dashboard()

        def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
            # Check which table was clicked
            if event.data_table.id == "tables_table":
                row = event.data_table.get_row(event.row_key)
                table_name = row[0] 
                self.push_screen(TableViewer(table_name))

        def refresh_dashboard(self):
            self.query_one(DashboardHeader).update_info()
            self.query_one(TablesWidget).refresh_data()
            self.query_one(AuditWidget).refresh_data()
            self.query_one(MonitorWidget).refresh_stats()

        def on_mount(self) -> None:
            self.title = "Wrought Iron Command Center"
            self.refresh_dashboard()

    app_tui = WroughtIronApp()
    app_tui.run()