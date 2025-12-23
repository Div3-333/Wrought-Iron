import typer
import sqlite3
import pandas as pd
import json
import os
import http.server
import socketserver
import webbrowser
from pathlib import Path
from typing import Optional
from datetime import datetime
from rich.console import Console
from rich.table import Table

from wrought_iron.cli.utils import _get_active_db

app = typer.Typer()
console = Console()

# --- Lazy Imports ---
def _get_sweetviz():
    import sweetviz as sv
    return sv

def _get_reportlab():
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    return canvas, letter

@app.command(name="generate")
def generate(
    table_name: str = typer.Argument(..., help="Table to analyze."),
    out_file: str = typer.Option(..., "--out", help="Output HTML file."),
    layout: str = typer.Option("widescreen", "--layout", help="Layout (vertical|widescreen)."),
):
    """Sweetviz EDA."""
    sv = _get_sweetviz()
    active_db = _get_active_db()
    
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        
    report = sv.analyze(df)
    report.show_html(filepath=out_file, layout=layout, open_browser=False)
    console.print(f"[green]Report generated at {out_file}[/green]")

@app.command(name="diff")
def diff(
    table_name: str = typer.Argument(..., help="Table to compare."),
    snapshot: str = typer.Option(..., "--snapshot", help="Snapshot ID/Name."),
    out_file: str = typer.Option("diff_report.html", "--out", help="Output HTML file."),
):
    """Before/After Report."""
    sv = _get_sweetviz()
    active_db = _get_active_db()
    snapshot_table = f"_snapshot_{snapshot}_{table_name}"
    
    with sqlite3.connect(active_db) as con:
        try:
            df_curr = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
            df_snap = pd.read_sql_query(f"SELECT * FROM {snapshot_table}", con)
        except Exception as e:
            console.print(f"[red]Error reading tables: {e}[/red]")
            raise typer.Exit(1)
            
    report = sv.compare([df_snap, "Snapshot"], [df_curr, "Current"])
    report.show_html(filepath=out_file, open_browser=False)
    console.print(f"[green]Diff report generated at {out_file}[/green]")

@app.command(name="validation")
def validation(
    table_name: str = typer.Argument(..., help="Table to validate."),
    out_file: str = typer.Option("validation_report.html", "--out", help="Output HTML file."),
    rules_file: Optional[str] = typer.Option(None, "--rules", help="JSON rules file."),
):
    """Quality Report."""
    # Using Sweetviz as a base for Quality Report
    generate(table_name=table_name, out_file=out_file, layout='vertical')
    console.print(f"[green]Validation report (EDA) generated at {out_file}.[/green]")

@app.command(name="schema-doc")
def schema_doc(
    title: str = typer.Option("Data Dictionary", "--title", help="Report title."),
    out_file: str = typer.Option("schema_doc.html", "--out", help="Output HTML file."),
):
    """Generate Data Dictionary HTML."""
    active_db = _get_active_db()
    html = f"<html><head><title>{title}</title><style>table {{ border-collapse: collapse; width: 100%; }} th, td {{ border: 1px solid black; padding: 8px; }} th {{ background-color: #f2f2f2; }}</style></head><body>"
    html += f"<h1>{title}</h1>"
    html += f"<p>Generated: {datetime.now()}</p>"
    
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_wi_%'").fetchall()
        
        for (tbl,) in tables:
            html += f"<h2>Table: {tbl}</h2>"
            html += "<table><tr><th>CID</th><th>Name</th><th>Type</th><th>NotNull</th><th>Default</th><th>PK</th></tr>"
            infos = cursor.execute(f"PRAGMA table_info({tbl})").fetchall()
            for info in infos:
                html += "<tr>" + "".join(f"<td>{x}</td>" for x in info) + "</tr>"
            html += "</table>"
            
    html += "</body></html>"
    
    with open(out_file, "w") as f:
        f.write(html)
    console.print(f"[green]Schema doc generated at {out_file}[/green]")

@app.command(name="audit-timeline")
def audit_timeline(
    start: Optional[str] = typer.Option(None, "--range-start", help="Start date."),
    end: Optional[str] = typer.Option(None, "--range-end", help="End date."),
    out_file: str = typer.Option("audit_timeline.html", "--out", help="Output HTML file."),
):
    """Generate visual timeline of data changes."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        query = "SELECT * FROM _wi_audit_log_ ORDER BY timestamp DESC" # Try new name
        try:
            df = pd.read_sql_query(query, con)
        except:
            # Fallback to old name if exists
            try:
                df = pd.read_sql_query("SELECT * FROM _wi_audit_log ORDER BY timestamp DESC", con)
            except:
                 console.print("[red]No audit log found.[/red]")
                 return
                 
    # Simple HTML timeline
    html = "<html><body><h1>Audit Timeline</h1><ul>"
    for _, row in df.iterrows():
        html += f"<li><strong>{row['timestamp']}</strong> - {row['user']}: {row['action']} ({row['details']})</li>"
    html += "</ul></body></html>"
    
    with open(out_file, "w") as f:
        f.write(html)
    console.print(f"[green]Timeline generated at {out_file}[/green]")

@app.command(name="map")
def map_report(
    table_name: str = typer.Argument(..., help="Table with coords."),
    lat_col: str = typer.Option(..., "--lat", help="Latitude column."),
    lon_col: str = typer.Option(..., "--lon", help="Longitude column."),
    out_file: str = typer.Option("map.html", "--out", help="Output HTML file."),
):
    """Offline Leaflet Map."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT {lat_col}, {lon_col} FROM {table_name} LIMIT 1000", con)
        
    # Simple Leaflet Template
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Map: {table_name}</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
        <style>#map {{ height: 600px; }}</style>
    </head>
    <body>
        <h1>Map: {table_name}</h1>
        <div id="map"></div>
        <script>
            var map = L.map('map').setView([0, 0], 2);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                attribution: 'OSM'
            }}).addTo(map);
            
            var points = {json.dumps(df[[lat_col, lon_col]].values.tolist())};
            var bounds = L.latLngBounds();
            
            points.forEach(function(p) {{
                if (p[0] && p[1]) {{
                    L.marker(p).addTo(map);
                    bounds.extend(p);
                }}
            }});
            
            if (points.length > 0) {{
                map.fitBounds(bounds);
            }}
        </script>
    </body>
    </html>
    """
    
    with open(out_file, "w") as f:
        f.write(html)
    console.print(f"[green]Map generated at {out_file}[/green]")

@app.command(name="summary")
def summary(
    table_name: str = typer.Argument(..., help="Table to summarize."),
    out_file: str = typer.Option("summary.pdf", "--out", help="Output PDF file."),
    include_charts: bool = typer.Option(False, "--include-charts", help="Include charts (placeholder)."),
):
    """Executive PDF."""
    canvas, letter = _get_reportlab()
    active_db = _get_active_db()
    
    c = canvas.Canvas(out_file, pagesize=letter)
    width, height = letter
    
    c.setFont("Helvetica-Bold", 20)
    c.drawString(50, height - 50, f"Executive Summary: {table_name}")
    c.setFont("Helvetica", 12)
    c.drawString(50, height - 80, f"Generated: {datetime.now()}")
    
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        
    c.drawString(50, height - 120, f"Total Rows: {len(df)}")
    c.drawString(50, height - 140, f"Total Columns: {len(df.columns)}")
    c.drawString(50, height - 160, f"Columns: {', '.join(df.columns[:5])}...")
    
    y = height - 200
    desc = df.describe().to_string().split('\n')
    c.setFont("Courier", 8)
    for line in desc:
        if y < 50:
            c.showPage()
            y = height - 50
            c.setFont("Courier", 8)
        c.drawString(50, y, line)
        y -= 12
        
    c.save()
    console.print(f"[green]PDF Summary generated at {out_file}[/green]")

@app.command(name="dependencies")
def dependencies(
    format: str = typer.Option("png", "--format", help="Output format."),
):
    """Lineage DAG."""
    # We don't have graphviz, so we print text-based or simple HTML/Mermaid
    console.print("Generating Lineage DAG (Text Representation)...")
    active_db = _get_active_db()
    
    # Inferred lineage: Views depend on tables in their SQL.
    # We can't easily parse SQL for dependencies without a parser.
    # We'll list views and what we *think* they might use (naive string match)
    
    with sqlite3.connect(active_db) as con:
        cursor = con.cursor()
        views = cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='view'").fetchall()
        tables = [t[0] for t in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        
        console.print("[bold]Nodes:[/bold]")
        for t in tables:
            console.print(f"  Table: {t}")
        for v in views:
            console.print(f"  View: {v[0]}")
            
        console.print("\n[bold]Edges (Inferred):[/bold]")
        for v_name, v_sql in views:
            for t in tables:
                if t in v_sql:
                    console.print(f"  {t} -> {v_name}")

@app.command(name="profile")
def profile(
    table_name: str = typer.Argument(..., help="Table to profile."),
    minimal: bool = typer.Option(False, "--minimal", help="Exclude quantiles."),
):
    """JSON Stats Dump."""
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        
    desc = df.describe(include='all').to_json()
    print(desc)

@app.command(name="serve")
def serve(
    port: int = typer.Option(8000, "--port", help="Port number."),
    bind: str = typer.Option("127.0.0.1", "--bind", help="Bind address."),
):
    """Localhost Preview."""
    # Serve current directory
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer((bind, port), handler) as httpd:
        console.print(f"[green]Serving at http://{bind}:{port}[/green]")
        console.print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
