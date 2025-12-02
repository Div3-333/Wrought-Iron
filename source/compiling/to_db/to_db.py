import os
import sys
import argparse
import sqlite3
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

# try importing optional libraries to avoid hard crashes if specific drivers aren't needed
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

def get_table_name(source_path, provided_name=None):
    """
    Determines the table name based on the filename if no name is provided.
    """
    if provided_name:
        return provided_name
    
    # If it's a file path, take the stem (e.g., 'data/users.csv' -> 'users')
    # If it's a BigQuery table ID (project.dataset.table), take the last part
    base = str(source_path).split('/')[-1].split('\\')[-1]
    name = base.split('.')[0] if '.' in base else base
    
    # Clean string to make it SQL-safe (basic cleaning)
    return "".join(c if c.isalnum() else "_" for c in name)

def infer_format(input_path):
    """
    Attempts to guess the format based on file extension or structure.
    """
    ext = Path(input_path).suffix.lower()
    if ext == '.csv': return 'csv'
    elif ext == '.json': return 'json'
    elif ext == '.parquet': return 'parquet'
    elif ext in ['.db', '.sqlite', '.sqlite3']: return 'sqlite'
    elif ext in ['.xlsx', '.xls']: return 'excel'
    elif '.' not in input_path and ':' in input_path or '-' in input_path: 
        # Heuristic for BigQuery projects (e.g. project-id.dataset.table)
        return 'bigquery'
    return None

def read_source(source, source_format, bq_query=None):
    """
    Reads the source data into a Pandas DataFrame.
    """
    print(f"Reading data from: {source} ({source_format})...")
    
    if source_format == 'csv':
        # low_memory=False handles mixed types better in large files
        return pd.read_csv(source, low_memory=False)
    
    elif source_format == 'json':
        # Try line-delimited JSON first (common in data pipelines), then standard JSON
        try:
            return pd.read_json(source, lines=True)
        except ValueError:
            return pd.read_json(source)
            
    elif source_format == 'parquet':
        return pd.read_parquet(source)
        
    elif source_format == 'excel' or source_format == 'xlsx':
        return pd.read_excel(source)
        
    elif source_format == 'sqlite':
        src_conn = sqlite3.connect(source)
        cursor = src_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            raise ValueError("Source SQLite database has no tables.")
            
        dfs = {}
        for table_name in tables:
            t = table_name[0]
            print(f"  - Found table: {t}")
            dfs[t] = pd.read_sql_query(f"SELECT * FROM {t}", src_conn)
        src_conn.close()
        return dfs 

    elif source_format == 'bigquery':
        if bigquery is None:
            raise ImportError("google-cloud-bigquery library is not installed. Run `pip install google-cloud-bigquery`.")
        
        client = bigquery.Client()
        
        if bq_query:
            print("  - Executing custom BigQuery SQL...")
            return client.query(bq_query).to_dataframe()
        else:
            print(f"  - Fetching table {source}...")
            table = client.get_table(source)
            rows = client.list_rows(table)
            return rows.to_dataframe()

    else:
        raise ValueError(f"Unsupported format: {source_format}")

def interactive_mode():
    """
    Runs a friendly wizard for users who didn't provide arguments.
    """
    print("\n" + "="*50)
    print("   UNIVERSAL DATA CONVERTER WIZARD")
    print("="*50)
    print("Welcome! I can help you turn your raw data into a .db file.\n")
    
    # 1. Get Input File
    while True:
        source_path = input("Step 1: Drag and drop your file here (or type path): ").strip()
        # Remove quotes that terminals sometimes add when dragging/dropping
        source_path = source_path.strip("'").strip('"')
        
        if not source_path:
            print("   Please enter a path.")
            continue
            
        # Check existence (skip check for BigQuery strings)
        if not os.path.exists(source_path) and '.' in source_path and ':' not in source_path:
             print(f"   Error: File '{source_path}' not found. Try again.")
             continue
        break

    # 2. Detect/Ask Format
    detected = infer_format(source_path)
    if detected:
        print(f"   Detected format: {detected.upper()}")
        confirm = input(f"   Is this correct? [Y/n]: ").strip().lower()
        if confirm != 'n':
            fmt = detected
        else:
            fmt = input("   Please type the format (csv, json, parquet, excel, bigquery): ").strip().lower()
    else:
        fmt = input("   Could not detect format. Please type it (csv, json, parquet, excel, bigquery): ").strip().lower()

    # 3. Output File
    default_out = "converted.db"
    out_path = input(f"\nStep 2: Output file name [default: {default_out}]: ").strip()
    if not out_path:
        out_path = default_out
    if not out_path.endswith(".db"):
        out_path += ".db"

    # 4. Table Name
    default_tbl = get_table_name(source_path)
    tbl_name = input(f"\nStep 3: Table name inside DB [default: {default_tbl}]: ").strip()
    if not tbl_name:
        tbl_name = default_tbl

    # 5. Execute
    print("\n" + "-"*30)
    try:
        data = read_source(source_path, fmt)
        
        db_path = str(Path(out_path).resolve())
        engine = create_engine(f'sqlite:///{db_path}')
        
        print(f"Writing to {out_path}...")
        
        if isinstance(data, dict):
            for t, df in data.items():
                print(f"  - Writing table '{t}' ({len(df)} rows)...")
                df.to_sql(t, engine, if_exists='replace', index=False)
        else:
            print(f"  - Writing table '{tbl_name}' ({len(data)} rows)...")
            data.to_sql(tbl_name, engine, if_exists='replace', index=False)
            
        print("\nSUCCESS! Your data is ready.")
        print(f"File location: {db_path}")
        
    except Exception as e:
        print(f"\nERROR: Something went wrong.\n{e}")
    
    input("\nPress Enter to exit...")

def main():
    parser = argparse.ArgumentParser(description="Convert CSV, JSON, Parquet, BigQuery, etc. to SQLite (.db)")
    
    # We make arguments optional (nargs='?') to detect if we should run interactive mode
    parser.add_argument('input', nargs='?', help="Input file path or BigQuery Table ID")
    parser.add_argument('output', nargs='?', help="Output .db file path")
    parser.add_argument('--format', '-f', help="Force input format.")
    parser.add_argument('--table-name', '-t', help="Name of the table to create.")
    parser.add_argument('--bq-query', '-q', help="Optional SQL query for BigQuery.")
    parser.add_argument('--append', action='store_true', help="Append to table if it exists.")
    
    args = parser.parse_args()
    
    # If no input is provided, switch to interactive mode
    if args.input is None:
        interactive_mode()
        sys.exit(0)

    # --- CLI MODE EXECUTION ---
    
    # 1. Determine Format
    source_format = args.format
    if not source_format:
        if args.bq_query:
            source_format = 'bigquery'
        else:
            source_format = infer_format(args.input)
            if not source_format:
                print("Could not infer format. Please use --format.")
                sys.exit(1)

    # 2. Read Data
    try:
        data = read_source(args.input, source_format, args.bq_query)
    except Exception as e:
        print(f"Error reading source: {e}")
        sys.exit(1)

    # 3. Connect to Output Database
    db_path = str(Path(args.output).resolve())
    engine = create_engine(f'sqlite:///{db_path}')

    # 4. Write Data
    if_exists_action = 'append' if args.append else 'replace'
    
    print(f"Writing to {args.output}...")
    
    try:
        if isinstance(data, dict):
            for tbl, df in data.items():
                print(f"  - Writing table '{tbl}' ({len(df)} rows)...")
                df.to_sql(tbl, engine, if_exists=if_exists_action, index=False)
        else:
            tbl_name = get_table_name(args.input, args.table_name)
            print(f"  - Writing table '{tbl_name}' ({len(data)} rows)...")
            data.to_sql(tbl_name, engine, if_exists=if_exists_action, index=False)
            
        print("Success! Conversion complete.")
        
    except Exception as e:
        print(f"Error writing to database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()