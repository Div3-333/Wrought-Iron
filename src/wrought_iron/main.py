import typer
from wrought_iron.cli import connect, schema, query, aggregate, plot, clean, geo, ml, audit, ops, collab, report, interact

app = typer.Typer()
app.add_typer(connect.app, name="connect")
app.add_typer(schema.app, name="schema")
app.add_typer(query.app, name="query")
app.add_typer(aggregate.app, name="aggregate")
app.add_typer(plot.app, name="plot")
app.add_typer(clean.app, name="clean")
app.add_typer(geo.app, name="geo")
app.add_typer(ml.app, name="ml")
app.add_typer(audit.app, name="audit")
app.add_typer(ops.app, name="ops")
app.add_typer(collab.app, name="collab")
app.add_typer(report.app, name="report")
app.add_typer(interact.app, name="interact")

@app.callback()
def callback():
    """
    Wrought Iron: The Military-Grade, Air-Gapped Data Foundry
    """

if __name__ == "__main__":
    app()
