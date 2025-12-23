import typer
import sqlite3
import json
from pathlib import Path
from enum import Enum
from wrought_iron.cli.utils import _get_active_db, log_action

try:
    import wrought_iron_core
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False

app = typer.Typer()

class Weights(str, Enum):
    uniform = "uniform"
    distance = "distance"

def _get_pandas():
    import pandas as pd
    return pd

def _get_sklearn_impute():
    from sklearn.impute import KNNImputer
    return KNNImputer

def _get_rapidfuzz():
    import rapidfuzz
    return rapidfuzz

@app.command(name="impute-mode")
def impute_mode(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to fill NaNs."),
):
    """Fill NaN with the most frequent value."""
    pd = _get_pandas()
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if col not in df.columns:
            print(f"Error: Column '{col}' not found.")
            raise typer.Exit(code=1)
            
        mode_val = df[col].mode()
        if not mode_val.empty:
            fill_val = mode_val[0]
            df[col].fillna(fill_val, inplace=True)
            df.to_sql(table_name, con, if_exists="replace", index=False)
            log_action(con, "clean", "impute-mode", f"Imputed {col} in {table_name} with mode: {fill_val}")
            print(f"Imputed '{col}' with mode: {fill_val}")
        else:
            print(f"No mode found for '{col}' (all null?).")

@app.command(name="impute-group")
def impute_group(
    table_name: str = typer.Argument(..., help="The name of the table."),
    target: str = typer.Argument(..., help="The column to impute."),
    group_col: str = typer.Argument(..., help="The column to group by."),
    std_max: float = typer.Option(None, "--std-max", help="Max standard deviation allowed for imputation safety."),
):
    """Cohort Imputation: Fill missing values based on group mode/stats."""
    pd = _get_pandas()
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if target not in df.columns or group_col not in df.columns:
            print(f"Error: Columns not found.")
            raise typer.Exit(code=1)

        if std_max is not None:
            # check std dev if numeric
            if pd.api.types.is_numeric_dtype(df[target]):
                std = df.groupby(group_col)[target].std()
                if (std > std_max).any():
                    print(f"Aborting: Groups exceed max standard deviation {std_max}.")
                    print(std[std > std_max])
                    raise typer.Exit(code=1)
            else:
                print("Warning: --std-max ignored for non-numeric target.")

        # Impute with mode per group
        def fill_mode(x):
            m = x.mode()
            return x.fillna(m[0] if not m.empty else x)

        df[target] = df.groupby(group_col)[target].transform(fill_mode)
        df.to_sql(table_name, con, if_exists="replace", index=False)
        log_action(con, "clean", "impute-group", f"Imputed {target} in {table_name} grouped by {group_col}")
        print(f"Imputed '{target}' grouped by '{group_col}'.")

@app.command(name="ml-impute")
def ml_impute(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to impute."),
    neighbors: int = typer.Option(5, "--neighbors", help="Number of neighbors."),
    weights: Weights = typer.Option(Weights.uniform, "--weights", help="Weight function."),
):
    """KNN Imputation using Scikit-Learn."""
    pd = _get_pandas()
    KNNImputer = _get_sklearn_impute()
    active_db = _get_active_db()
    
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if col not in df.columns:
            print(f"Error: Column '{col}' not found.")
            raise typer.Exit(code=1)
            
        # Select numeric columns for KNN
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if col not in numeric_cols:
            print(f"Error: Target column '{col}' must be numeric for KNN.")
            raise typer.Exit(code=1)
            
        imputer = KNNImputer(n_neighbors=neighbors, weights=weights.value)
        
        # Impute
        try:
            imputed_data = imputer.fit_transform(df[numeric_cols])
        except Exception as e:
            print(f"Error running KNN Imputation: {e}")
            raise typer.Exit(code=1)

        imputed_df = pd.DataFrame(imputed_data, columns=numeric_cols)
        
        # Update original df
        df[col] = imputed_df[col]
        
        df.to_sql(table_name, con, if_exists="replace", index=False)
        log_action(con, "clean", "ml-impute", f"KNN imputed {col} in {table_name} with {neighbors} neighbors")
        print(f"KNN Imputation complete for '{col}'.")

@app.command()
def dedupe(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to check for fuzzy duplicates."),
    threshold: int = typer.Option(90, "--threshold", help="Fuzzy match threshold (0-100)."),
    interactive: bool = typer.Option(True, "--interactive/--no-interactive", help="Confirm merges interactively."),
):
    """Interactive fuzzy deduplication session."""
    pd = _get_pandas()
    rapidfuzz = _get_rapidfuzz()
    process = rapidfuzz.process
    fuzz = rapidfuzz.fuzz
    
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
    
    if col not in df.columns:
        print(f"Error: Column '{col}' not found.")
        raise typer.Exit(code=1)
        
    unique_vals = df[col].dropna().unique().astype(str).tolist()
    
    clusters = []
    
    # RUST OPTIMIZATION CHECK
    if CORE_AVAILABLE:
        try:
            print("Running Rust-accelerated deduplication...")
            # Rust func signature: find_duplicates(values: Vec<String>, threshold: f64) -> Vec<(usize, String)>
            # threshold is 0-100
            
            # This returns for EACH value, a best match. 
            # We need to invert this to groups.
            results = wrought_iron_core.find_duplicates(unique_vals, float(threshold))
            
            # Map canonical -> [variants]
            groups = {}
            for i, match in results:
                val = unique_vals[i]
                if val == match: continue # Matched self
                
                if match not in groups:
                    groups[match] = set()
                groups[match].add(val)
                
            for canonical, variants in groups.items():
                cluster = list(variants)
                cluster.insert(0, canonical)
                clusters.append(cluster)
                
        except Exception as e:
            print(f"Rust dedupe failed ({e}), falling back to Python.")
            CORE_AVAILABLE_LOCAL = False
    else:
        CORE_AVAILABLE_LOCAL = False
        
    if not CORE_AVAILABLE or 'CORE_AVAILABLE_LOCAL' in locals():
        # Fallback Python Logic
        seen = set()
        print("Scanning for duplicates (Python)...")
        for val in unique_vals:
            if val in seen:
                continue
            
            matches = process.extract(val, unique_vals, scorer=fuzz.ratio, limit=None, score_cutoff=threshold)
            
            group = [m[0] for m in matches if m[0] not in seen]
            if len(group) > 1:
                clusters.append(group)
                seen.update(group)
            else:
                seen.add(val)
            
    if not clusters:
        print("No fuzzy duplicates found.")
        return
    
    changes_made = False
    for group in clusters:
        print(f"\nFound group: {group}")
        if interactive:
            if not typer.confirm("Do you want to merge these?", default=True):
                continue
            
            print("Select canonical value:")
            for i, v in enumerate(group):
                print(f"{i}: {v}")
            
            idx = typer.prompt("Enter index", type=int)
            if not (0 <= idx < len(group)):
                print("Invalid index, skipping.")
                continue
            canonical = group[idx]
        else:
            print("Skipping non-interactive merge.")
            continue

        # Replace in DB
        for v in group:
            if v != canonical:
                df.loc[df[col] == v, col] = canonical
        print(f"Merged to '{canonical}'.")
        changes_made = True

    if changes_made:
        if not interactive or typer.confirm("Save changes to database?", default=True):
            with sqlite3.connect(active_db) as con:
                df.to_sql(table_name, con, if_exists="replace", index=False)
                log_action(con, "clean", "dedupe", f"Deduped {col} in {table_name}")
            print("Database updated.")
        else:
            print("Changes discarded.")
    else:
        print("No changes made.")

@app.command()
def harmonize(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to harmonize."),
    threshold: int = typer.Option(90, "--threshold", help="Similarity threshold."),
):
    """Cluster similar text variations and standardize automatically (Rust Accelerated)."""
    pd = _get_pandas()
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

        if col not in df.columns:
            print(f"Error: Column '{col}' not found.")
            raise typer.Exit(code=1)

        unique_vals = df[col].dropna().unique().astype(str).tolist()

        if CORE_AVAILABLE:
            print("Running Rust-accelerated harmonization...")
            mapping = wrought_iron_core.harmonize_batch(unique_vals, float(threshold))
        else:
            rapidfuzz = _get_rapidfuzz()
            process = rapidfuzz.process
            fuzz = rapidfuzz.fuzz
            mapping = {}
            processed = set()
            for val in unique_vals:
                if val in processed: continue
                matches = process.extract(val, unique_vals, scorer=fuzz.ratio, limit=None, score_cutoff=threshold)
                for m in matches:
                    mapping[m[0]] = val
                    processed.add(m[0])

        df[col] = df[col].astype(str).map(lambda x: mapping.get(x, x))

        df.to_sql(table_name, con, if_exists="replace", index=False)
        log_action(con, "clean", "harmonize", f"Harmonized {col} in {table_name}")
        print(f"Harmonized column '{col}'.")

@app.command(name="regex-replace")
def regex_replace(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to modify."),
    pattern: str = typer.Argument(..., help="Regex pattern."),
    repl: str = typer.Argument(..., help="Replacement text."),
):
    """Advanced string substitution (Rust Accelerated)."""
    pd = _get_pandas()
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

        if col not in df.columns:
            print(f"Error: Column '{col}' not found.")
            raise typer.Exit(code=1)

        if CORE_AVAILABLE:
            texts = df[col].astype(str).tolist()
            df[col] = wrought_iron_core.regex_replace_batch(texts, pattern, repl)
        else:
            df[col] = df[col].astype(str).str.replace(pattern, repl, regex=True)

        df.to_sql(table_name, con, if_exists="replace", index=False)
        log_action(con, "clean", "regex-replace", f"Regex replaced '{pattern}' with '{repl}' in {table_name}.{col}")
        print(f"Applied regex replacement on '{col}'.")

@app.command(name="drop-outliers")
def drop_outliers(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to check."),
    sigma: float = typer.Option(3.0, "--sigma", help="Standard deviations threshold."),
):
    """Nullify values exceeding N standard deviations (Rust Accelerated)."""
    pd = _get_pandas()
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

        if col not in df.columns:
            print(f"Error: Column '{col}' not found.")
            raise typer.Exit(code=1)

        if not pd.api.types.is_numeric_dtype(df[col]):
            print(f"Error: Column '{col}' is not numeric.")
            raise typer.Exit(code=1)

        data = df[col].astype(float).tolist()
        if CORE_AVAILABLE:
            mask = wrought_iron_core.detect_outliers_zscore(data, sigma)
            count = sum(mask)
        else:
            mean = df[col].mean()
            std = df[col].std()
            mask = (abs(df[col] - mean) / std) > sigma
            count = mask.sum()

        if count > 0:
            df.loc[mask, col] = None
            df.to_sql(table_name, con, if_exists="replace", index=False)
            log_action(con, "clean", "drop-outliers", f"Dropped {count} outliers in {table_name}.{col} (> {sigma} sigma)")
            print(f"Nullified {count} outliers in '{col}' (> {sigma} sigma).")
        else:
            print("No outliers found.")
@app.command(name="map")
def map_values(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to map."),
    file: str = typer.Argument(..., help="Path to CSV file (key,value)."),
):
    """Apply a CSV-based dictionary mapping/lookup."""
    pd = _get_pandas()
    active_db = _get_active_db()
    
    map_path = Path(file)
    if not map_path.exists():
        print(f"Error: File '{file}' not found.")
        raise typer.Exit(code=1)
        
    try:
        map_df = pd.read_csv(map_path, header=None)
        if map_df.shape[1] < 2:
             print("Error: Map file must have at least 2 columns.")
             raise typer.Exit(code=1)
        mapping = dict(zip(map_df[0].astype(str), map_df[1]))
    except Exception as e:
        print(f"Error reading map file: {e}")
        raise typer.Exit(code=1)

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

        if col not in df.columns:
            print(f"Error: Column '{col}' not found.")
            raise typer.Exit(code=1)
            
        df[col] = df[col].astype(str).replace(mapping)
        
        df.to_sql(table_name, con, if_exists="replace", index=False)
        log_action(con, "clean", "map", f"Mapped values in {table_name}.{col} using {file}")
        print(f"Applied mapping to '{col}'.")

@app.command()
def trim(
    table_name: str = typer.Argument(..., help="The name of the table."),
    col: str = typer.Argument(..., help="The column to trim."),
):
    """Remove leading/trailing whitespace."""
    pd = _get_pandas()
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if col not in df.columns:
            print(f"Error: Column '{col}' not found.")
            raise typer.Exit(code=1)
            
        if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].str.strip()
            df.to_sql(table_name, con, if_exists="replace", index=False)
            log_action(con, "clean", "trim", f"Trimmed whitespace in {table_name}.{col}")
            print(f"Trimmed whitespace in '{col}'.")
        else:
            print(f"Column '{col}' is not text type.")

@app.command(name="validate-schema")
def validate_schema(
    table_name: str = typer.Argument(..., help="The name of the table."),
    rules_file: str = typer.Argument(..., help="Path to JSON schema file."),
):
    """Check data against a JSON schema definition."""
    pd = _get_pandas()
    active_db = _get_active_db()
    
    path = Path(rules_file)
    if not path.exists():
        print(f"Error: File '{rules_file}' not found.")
        raise typer.Exit(code=1)
        
    try:
        with open(path, 'r') as f:
            schema = json.load(f)
    except Exception as e:
        print(f"Error parsing JSON schema: {e}")
        raise typer.Exit(code=1)
        
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
    
    errors = []
    
    for col, rule in schema.items():
        if col not in df.columns:
            errors.append(f"Missing column: {col}")
            continue
            
        target_type = rule
        if isinstance(rule, dict):
            target_type = rule.get("type")
            
        if target_type == "int":
            if not pd.api.types.is_integer_dtype(df[col]):
                 errors.append(f"Column '{col}' expected int, found {df[col].dtype}")
        elif target_type == "float":
            if not pd.api.types.is_float_dtype(df[col]):
                 errors.append(f"Column '{col}' expected float, found {df[col].dtype}")
        elif target_type == "str":
             if not pd.api.types.is_string_dtype(df[col]) and not pd.api.types.is_object_dtype(df[col]):
                 errors.append(f"Column '{col}' expected str, found {df[col].dtype}")
                 
    if errors:
        print("Schema validation failed:")
        for e in errors:
            print(f"- {e}")
        raise typer.Exit(code=1)
    else:
        print("Schema validation passed.")