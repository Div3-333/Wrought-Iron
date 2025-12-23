MODULE_CONFIG = {
    "connect": {
        "icon": "fas fa-plug",
        "description": "Manage database connections.",
        "commands": {
            "file": {
                "help": "Register an existing database as the active target.",
                "args": [
                    {"name": "path", "type": "pos", "help": "Path to DB file"},
                    {"name": "--read-only", "type": "flag", "help": "Read-only mode"},
                    {"name": "--check-wal", "type": "flag", "help": "Verify Write-Ahead-Log exists."}
                ]
            },
            "new": {
                "help": "Initialize a fresh, empty Wrought Iron schema.",
                "args": [
                    {"name": "path", "type": "pos", "help": "New DB path"},
                    {"name": "--force", "type": "flag", "help": "Overwrite"},
                    {"name": "--page-size", "type": "select", "options": [4096, 8192, 16384], "default": 4096, "help": "SQLite page size."}
                ]
            },
            "list": {
                "help": "Show history of accessed databases.",
                "args": [
                    {"name": "--limit", "type": "opt", "default": 10, "help": "Max rows to show."},
                    {"name": "--sort", "type": "select", "options": ["access_time", "size"], "default": "access_time", "help": "Sort order."}
                ]
            },
            "alias": {
                "help": "Assign a short name to a deep file path.",
                "args": [
                    {"name": "name", "type": "pos", "help": "Alias Name"},
                    {"name": "path", "type": "pos", "help": "DB Path"},
                    {"name": "--overwrite", "type": "flag", "help": "Replace existing alias."}
                ]
            },
            "merge": {
                "help": "ETL merge of two DB files.",
                "args": [
                    {"name": "target_db", "type": "pos", "help": "Target"},
                    {"name": "source_db", "type": "pos", "help": "Source"},
                    {"name": "--strategy", "type": "select", "options": ["append", "replace", "ignore"], "default": "append", "help": "Conflict resolution strategy."},
                    {"name": "--tables", "type": "opt", "help": "Comma-separated list of tables to merge."},
                    {"name": "--chunk-size", "type": "opt", "default": 50000, "help": "Rows per commit."}
                ]
            },
            "info": {
                "help": "Display low-level metadata.",
                "args": [
                    {"name": "--extended", "type": "flag", "help": "Show WAL size, encoding, and permissions."}
                ]
            },
            "vacuum": {
                "help": "Rebuild DB file to reclaim disk space.",
                "args": [
                    {"name": "--into", "type": "opt", "help": "Vacuum into a new file instead of in-place."}
                ]
            },
            "integrity-check": {
                "help": "Run SQLite corruption scan.",
                "args": [
                    {"name": "--quick", "type": "flag", "help": "Skip index verification for speed."}
                ]
            },
            "encrypt": {
                "help": "Encrypt database at rest (AES-256).",
                "args": [
                    {"name": "path", "type": "pos", "help": "DB Path"},
                    {"name": "--key-file", "type": "opt", "help": "Path to the encryption key file."},
                    {"name": "--output", "type": "opt", "help": "Path to write the encrypted file to."}
                ]
            },
            "decrypt": {
                "help": "Decrypt a WI-locked file.",
                "args": [
                    {"name": "path", "type": "pos", "help": "Encrypted DB Path"},
                    {"name": "--key-file", "type": "opt", "help": "Path to the encryption key file."}
                ]
            }
        }
    },
    "schema": {
        "icon": "fas fa-project-diagram",
        "description": "Inspect and modify database structure.",
        "commands": {
            "list": {
                "help": "List all tables, views, and indexes.",
                "args": [
                    {"name": "--show-views", "type": "flag", "default": True, "help": "Toggle view visibility."},
                    {"name": "--show-sys", "type": "flag", "help": "Toggle system table visibility."}
                ]
            },
            "describe": {
                "help": "Show column names and types.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table Name"},
                    {"name": "--format", "type": "select", "options": ["table", "sql"], "default": "table", "help": "Output format."}
                ]
            },
            "inspect": {
                "help": "Profile data stats (nulls, unique, etc).",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table Name"},
                    {"name": "--sample", "type": "opt", "default": 1.0, "help": "% of data to scan (0.1-1.0)."},
                    {"name": "--histogram", "type": "flag", "help": "Include mini-histogram."}
                ]
            },
            "diff": {
                "help": "Compare schemas between two tables.",
                "args": [
                    {"name": "table_a", "type": "pos", "help": "Table A"},
                    {"name": "table_b", "type": "pos", "help": "Table B"},
                    {"name": "--db-b", "type": "opt", "help": "Optional second DB path."}
                ]
            },
            "detect-json": {
                "help": "Scan TEXT columns for JSON content.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table Name"},
                    {"name": "--depth", "type": "opt", "help": "Max traversal depth."},
                    {"name": "--threshold", "type": "opt", "default": 0.1, "help": "Min % valid rows."}
                ]
            },
            "flatten": {
                "help": "Permanently explode a JSON column into distinct columns.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table Name"},
                    {"name": "column", "type": "pos", "help": "JSON Col"},
                    {"name": "--prefix", "type": "opt", "help": "Prefix for new columns."},
                    {"name": "--separator", "type": "opt", "default": "_", "help": "Nested key separator."},
                    {"name": "--drop-original", "type": "flag", "help": "Delete the source JSON column."}
                ]
            },
            "rename-col": {
                "help": "Safely rename a column.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "old_name", "type": "pos", "help": "Old"},
                    {"name": "new_name", "type": "pos", "help": "New"},
                    {"name": "--dry-run", "type": "flag", "help": "Verify SQL generation without executing."}
                ]
            },
            "drop-col": {
                "help": "Remove a column and reconstruct the table.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column_name", "type": "pos", "help": "Column"},
                    {"name": "--vacuum", "type": "flag", "help": "Auto-vacuum after drop."}
                ]
            },
            "cast": {
                "help": "Force type conversion.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column_name", "type": "pos", "help": "Column"},
                    {"name": "type", "type": "select", "options": ["integer", "text", "real", "blob"], "help": "New Type."},
                    {"name": "--on-error", "type": "select", "options": ["fail", "nullify", "ignore"], "default": "fail", "help": "Handling failures."}
                ]
            },
            "graph": {
                "help": "Generate ERD text graph.",
                "args": [
                    {"name": "--format", "type": "select", "options": ["mermaid", "dot"], "default": "mermaid", "help": "Output format."}
                ]
            }
        }
    },
    "query": {
        "icon": "fas fa-search",
        "description": "Data retrieval and searching.",
        "commands": {
            "head": {
                "help": "Show first N rows.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "-n", "type": "opt", "default": 10, "help": "Rows"}
                ]
            },
            "tail": {
                "help": "Show last N rows.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "-n", "type": "opt", "default": 10, "help": "Rows"}
                ]
            },
            "sample": {
                "help": "Get random subset.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--frac", "type": "opt", "help": "Fraction (0.0-1.0)."},
                    {"name": "-n", "type": "opt", "default": 10, "help": "Rows"},
                    {"name": "--seed", "type": "opt", "help": "Random seed."}
                ]
            },
            "filter": {
                "help": "Apply boolean logic.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--where", "type": "opt", "help": "Query (e.g. age > 25)."},
                    {"name": "--engine", "type": "select", "options": ["duckdb", "polars", "pandas"], "default": "duckdb", "help": "Backend speed optimization."}
                ]
            },
            "sql": {
                "help": "Execute raw SQL.",
                "args": [
                    {"name": "query", "type": "pos", "help": "SQL Query"},
                    {"name": "--params", "type": "opt", "help": "JSON string of parameters."},
                    {"name": "--engine", "type": "select", "options": ["duckdb", "sqlite"], "default": "duckdb", "help": "Execution engine."}
                ]
            },
            "search": {
                "help": "Global search across columns.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "term", "type": "pos", "help": "Search Term"},
                    {"name": "--cols", "type": "opt", "help": "Comma-separated columns."}
                ]
            },
            "sort": {
                "help": "Order results.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column", "type": "pos", "help": "Column"},
                    {"name": "--desc", "type": "flag", "help": "Sort descending."}
                ]
            },
            "distinct": {
                "help": "List unique values.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column", "type": "pos", "help": "Column"},
                    {"name": "--counts", "type": "flag", "help": "Include frequency count."}
                ]
            },
            "find-nulls": {
                "help": "Find incomplete rows.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--cols", "type": "opt", "help": "Comma-separated columns."}
                ]
            },
            "dups": {
                "help": "Identify duplicates.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column", "type": "pos", "help": "Column"}
                ]
            }
        }
    },
    "aggregate": {
        "icon": "fas fa-calculator",
        "description": "Group, pivot, and summarize data.",
        "commands": {
            "groupby": {
                "help": "Group and summarize.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "group_cols", "type": "pos", "help": "Cols"},
                    {"name": "--agg", "type": "opt", "help": "Agg Logic"}
                ]
            },
            "pivot": {
                "help": "Create pivot table.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "index", "type": "pos", "help": "Index"},
                    {"name": "columns", "type": "pos", "help": "Cols"},
                    {"name": "values", "type": "pos", "help": "Vals"},
                    {"name": "--func", "type": "select", "options": ["mean", "sum", "count", "min", "max"], "default": "mean", "help": "Aggregation function."}
                ]
            },
            "describe": {
                "help": "Summary statistics.",
                "args": [{"name": "table_name", "type": "pos", "help": "Table"}]
            },
            "corr": {
                "help": "Correlation matrix.",
                "args": [{"name": "table_name", "type": "pos", "help": "Table"}]
            },
            "moving-avg": {
                "help": "Rolling window.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column", "type": "pos", "help": "Col"},
                    {"name": "--window", "type": "opt", "default": 5, "help": "Window size."}
                ]
            },
            "crosstab": {
                "help": "Frequency matrix.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "row", "type": "pos", "help": "Row"},
                    {"name": "col", "type": "pos", "help": "Col"},
                    {"name": "--normalize", "type": "select", "options": ["index", "columns", "all"], "help": "Normalize results."},
                    {"name": "--margins", "type": "flag", "help": "Show margins."}
                ]
            },
            "skew": {
                "help": "Calculate skewness.",
                "args": [{"name": "table_name", "type": "pos", "help": "Table"}]
            },
            "kurtosis": {
                "help": "Calculate kurtosis.",
                "args": [{"name": "table_name", "type": "pos", "help": "Table"}]
            },
            "rank": {
                "help": "Rank rows.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column", "type": "pos", "help": "Col"},
                    {"name": "--desc", "type": "flag", "help": "Rank descending."}
                ]
            },
            "bin": {
                "help": "Discretize numbers.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "column", "type": "pos", "help": "Col"},
                    {"name": "--bins", "type": "opt", "default": 10, "help": "Number of bins."}
                ]
            }
        }
    },
    "clean": {
        "icon": "fas fa-broom",
        "description": "Data cleaning and imputation.",
        "commands": {
            "impute-mode": {
                "help": "Fill NaN with most frequent value.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"}
                ]
            },
            "impute-group": {
                "help": "Cohort Imputation: Fill missing values based on group mode/stats.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "target", "type": "pos", "help": "Target"},
                    {"name": "group_col", "type": "pos", "help": "Group"},
                    {"name": "--std-max", "type": "opt", "help": "Max standard deviation allowed."}
                ]
            },
            "ml-impute": {
                "help": "KNN Imputation using Scikit-Learn.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"},
                    {"name": "--neighbors", "type": "opt", "default": 5, "help": "Number of neighbors."},
                    {"name": "--weights", "type": "select", "options": ["uniform", "distance"], "default": "uniform", "help": "Weight function."}
                ]
            },
            "dedupe": {
                "help": "Interactive fuzzy deduplication session.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"},
                    {"name": "--threshold", "type": "opt", "default": 90, "help": "Fuzzy match threshold (0-100)."},
                    {"name": "--interactive", "type": "flag", "default": True, "help": "Confirm merges interactively."}
                ]
            },
            "harmonize": {
                "help": "Cluster similar text variations and standardize automatically.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"},
                    {"name": "--threshold", "type": "opt", "default": 90, "help": "Similarity threshold."}
                ]
            },
            "regex-replace": {
                "help": "Advanced string substitution.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"},
                    {"name": "pattern", "type": "pos", "help": "Regex"},
                    {"name": "repl", "type": "pos", "help": "Repl"}
                ]
            },
            "drop-outliers": {
                "help": "Nullify values exceeding N standard deviations.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"},
                    {"name": "--sigma", "type": "opt", "default": 3.0, "help": "Standard deviations threshold."}
                ]
            },
            "map": {
                "help": "Apply a CSV-based dictionary mapping/lookup.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"},
                    {"name": "file", "type": "pos", "help": "Map CSV"}
                ]
            },
            "trim": {
                "help": "Remove leading/trailing whitespace.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Col"}
                ]
            },
            "validate-schema": {
                "help": "Check data against a JSON schema definition.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "rules_file", "type": "pos", "help": "JSON Rules"}
                ]
            }
        }
    },
    "geo": {
        "icon": "fas fa-globe",
        "description": "Geospatial operations.",
        "commands": {
            "validate": {
                "help": "Validate coordinates fall within global bounds.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"}
                ]
            },
            "geocode": {
                "help": "Offline Geocoding: Map address to Lat/Lon.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "addr_col", "type": "pos", "help": "Address"},
                    {"name": "lookup_file", "type": "pos", "help": "CSV"},
                    {"name": "--new-lat-col", "type": "opt", "default": "latitude"},
                    {"name": "--new-lon-col", "type": "opt", "default": "longitude"}
                ]
            },
            "reverse": {
                "help": "Reverse Geocoding: Convert coordinates to address.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"},
                    {"name": "lookup_file", "type": "pos", "help": "CSV"},
                    {"name": "--new-addr-col", "type": "opt", "default": "address"}
                ]
            },
            "distance": {
                "help": "Calculate Haversine distance.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"},
                    {"name": "--target-lat", "type": "opt", "help": "Target Lat"},
                    {"name": "--target-lon", "type": "opt", "help": "Target Lon"},
                    {"name": "--new-dist-col", "type": "opt", "default": "distance_km"}
                ]
            },
            "cluster": {
                "help": "DBSCAN: Group records into spatial clusters.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"},
                    {"name": "--eps", "type": "opt", "default": 0.5, "help": "Max distance (km)."},
                    {"name": "--min-samples", "type": "opt", "default": 5, "help": "Min points."},
                    {"name": "--metric", "type": "select", "options": ["euclidean", "haversine"], "default": "euclidean", "help": "Distance metric."},
                    {"name": "--algorithm", "type": "select", "options": ["auto", "ball_tree", "kd_tree", "brute"], "default": "auto", "help": "Algorithm."}
                ]
            },
            "centroid": {
                "help": "Calculate the geometric center.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"}
                ]
            },
            "bounds": {
                "help": "Return the bounding box.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"}
                ]
            },
            "heatmap": {
                "help": "Render an ASCII density map.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"},
                    {"name": "--bins", "type": "opt", "default": 20}
                ]
            },
            "nearest": {
                "help": "Find nearest Point of Interest.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"},
                    {"name": "target_file", "type": "pos", "help": "POI CSV"},
                    {"name": "--k", "type": "opt", "default": 1}
                ]
            },
            "export-geojson": {
                "help": "Export data to standard GeoJSON format.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "lat_col", "type": "pos", "help": "Lat"},
                    {"name": "lon_col", "type": "pos", "help": "Lon"},
                    {"name": "--id-col", "type": "opt", "help": "ID property."},
                    {"name": "--properties", "type": "opt", "help": "Included properties."}
                ]
            }
        }
    },
    "audit": {
        "icon": "fas fa-shield-alt",
        "description": "Security and audit logs.",
        "commands": {
            "log-view": {
                "help": "Show Audit Log.",
                "args": [
                    {"name": "--limit", "type": "opt", "default": 50},
                    {"name": "--user", "type": "opt"},
                    {"name": "--action", "type": "opt"}
                ]
            },
            "hash-create": {
                "help": "Generate Integrity Fingerprint.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--algo", "type": "select", "options": ["sha256", "sha512"], "default": "sha256"},
                    {"name": "--salt", "type": "opt", "default": ""}
                ]
            },
            "hash-verify": {
                "help": "Verify Integrity.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "expected_hash", "type": "pos", "help": "Hash"},
                    {"name": "--salt", "type": "opt", "default": ""}
                ]
            },
            "scan-pii": {
                "help": "Scan for PII (Personally Identifiable Information).",
                "args": [{"name": "table_name", "type": "pos", "help": "Table"}]
            },
            "anonymize": {
                "help": "Masking.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col_name", "type": "pos", "help": "Col"},
                    {"name": "--method", "type": "select", "options": ["mask", "hash", "redact"], "default": "mask"},
                    {"name": "--chars", "type": "opt", "default": 4}
                ]
            },
            "snapshot": {
                "help": "Backup table state.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "name", "type": "pos", "help": "Snapshot Name"}
                ]
            },
            "rollback": {
                "help": "Restore table from snapshot.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "name", "type": "pos", "help": "Snapshot Name"}
                ]
            },
            "encrypt-col": {
                "help": "Column Encryption.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col_name", "type": "pos", "help": "Col"},
                    {"name": "key_file", "type": "pos", "help": "Key Path"}
                ]
            },
            "decrypt-col": {
                "help": "Column Decryption.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col_name", "type": "pos", "help": "Col"},
                    {"name": "key_file", "type": "pos", "help": "Key Path"}
                ]
            },
            "export-cert": {
                "help": "Generate Chain-of-Custody PDF.",
                "args": [
                    {"name": "signer", "type": "pos", "help": "Name"},
                    {"name": "output", "type": "pos", "help": "PDF Path"}
                ]
            }
        }
    },
    "ml": {
        "icon": "fas fa-brain",
        "description": "Machine Learning.",
        "commands": {
            "train-classifier": {
                "help": "Train a classification model.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "target_col", "type": "pos", "help": "Target"},
                    {"name": "feature_cols", "type": "pos", "help": "Features (comma-separated)"},
                    {"name": "--model-type", "type": "select", "options": ["random_forest", "logistic_regression"], "default": "random_forest", "help": "Model type."},
                    {"name": "--output-model", "type": "opt", "help": "Output model path."},
                    {"name": "--test-size", "type": "opt", "default": 0.2},
                    {"name": "--n-estimators", "type": "opt", "default": 100}
                ]
            },
            "train-regressor": {
                "help": "Train a regression model.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "target_col", "type": "pos", "help": "Target"},
                    {"name": "feature_cols", "type": "pos", "help": "Features (comma-separated)"},
                    {"name": "--model-type", "type": "select", "options": ["linear_regression", "ridge", "random_forest"], "default": "linear_regression"},
                    {"name": "--output-model", "type": "opt", "help": "Output model path."},
                    {"name": "--test-size", "type": "opt", "default": 0.2}
                ]
            },
            "predict": {
                "help": "Apply a saved model.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "model_path", "type": "pos", "help": "Model Path"},
                    {"name": "output_col", "type": "pos", "help": "Output Col"},
                    {"name": "feature_cols", "type": "pos", "help": "Features"},
                    {"name": "--threshold", "type": "opt", "help": "Threshold (0.0-1.0)."}
                ]
            },
            "score": {
                "help": "Calculate Accuracy/R2/F1 Score.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "model_path", "type": "pos", "help": "Model Path"},
                    {"name": "target_col", "type": "pos", "help": "Target"},
                    {"name": "feature_cols", "type": "pos", "help": "Features"},
                    {"name": "--metrics", "type": "select", "options": ["accuracy", "precision", "recall", "f1", "r2", "mae", "mse"], "default": "accuracy", "help": "Metrics."}
                ]
            },
            "feature-importance": {
                "help": "List which columns drive the prediction.",
                "args": [
                    {"name": "model_path", "type": "pos", "help": "Model Path"},
                    {"name": "--top-n", "type": "opt", "default": 10}
                ]
            },
            "cluster-kmeans": {
                "help": "Unsupervised K-Means clustering.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "feature_cols", "type": "pos", "help": "Features"},
                    {"name": "--k", "type": "opt", "default": 3},
                    {"name": "--init", "type": "select", "options": ["k-means++", "random"], "default": "k-means++"},
                    {"name": "--output-col", "type": "opt", "default": "cluster_id"}
                ]
            },
            "detect-anomalies": {
                "help": "Isolation Forest: Flag statistical outliers.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "feature_cols", "type": "pos", "help": "Features"},
                    {"name": "--contamination", "type": "opt", "default": "auto"},
                    {"name": "--output-col", "type": "opt", "default": "anomaly_score"}
                ]
            },
            "split": {
                "help": "Split dataset into Train/Test subsets.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "output_train_table", "type": "pos", "help": "Train Table"},
                    {"name": "output_test_table", "type": "pos", "help": "Test Table"},
                    {"name": "--train-size", "type": "opt", "default": 0.75}
                ]
            },
            "save-model": { "help": "Serialize model.", "args": [{"name": "model_path", "type": "pos", "help": "Path"}] },
            "load-model": { "help": "Hydrate model.", "args": [{"name": "model_path", "type": "pos", "help": "Path"}] }
        }
    },
    "ops": {
        "icon": "fas fa-cogs",
        "description": "Operations and scheduling.",
        "commands": {
            "schedule create": {
                "help": "Register a command to the internal scheduler.",
                "args": [
                    {"name": "cmd", "type": "pos", "help": "Command"},
                    {"name": "--name", "type": "opt", "help": "Name"},
                    {"name": "--cron", "type": "opt", "help": "Cron Expression"},
                    {"name": "--on-fail-email", "type": "opt", "help": "Email alert"}
                ]
            },
            "schedule list": {
                "help": "List active automated tasks.",
                "args": [{"name": "--status", "type": "select", "options": ["active", "paused"], "help": "Filter status"}]
            },
            "schedule delete": {
                "help": "Remove a task.",
                "args": [{"name": "task_id", "type": "pos", "help": "Task ID"}]
            },
            "pipeline run": {
                "help": "Execute a multi-step workflow defined in YAML.",
                "args": [
                    {"name": "yaml_file", "type": "pos", "help": "YAML File"},
                    {"name": "--continue-on-error", "type": "flag"}
                ]
            },
            "trigger": {
                "help": "Force run a scheduled task immediately.",
                "args": [{"name": "task_id", "type": "pos", "help": "Task ID"}]
            },
            "logs": {
                "help": "View Job History.",
                "args": [
                    {"name": "--job-id", "type": "opt", "help": "Filter Job ID"},
                    {"name": "--errors-only", "type": "flag"},
                    {"name": "--limit", "type": "opt", "default": 20}
                ]
            },
            "drift-check": {
                "help": "Quality Control / Drift Detection.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--baseline", "type": "opt", "help": "Snapshot Name"},
                    {"name": "--threshold", "type": "opt", "default": 0.05}
                ]
            },
            "alert-config": {
                "help": "Configure local hooks for failure alerts.",
                "args": [
                    {"name": "--email", "type": "opt", "help": "Email address"},
                    {"name": "--log-file", "type": "opt", "help": "Log file path"}
                ]
            },
            "maintenance": {
                "help": "DB Optimization.",
                "args": [
                    {"name": "--reindex", "type": "flag"},
                    {"name": "--analyze", "type": "flag"}
                ]
            },
            "export-status": {
                "help": "Output system health.",
                "args": [{"name": "--format", "type": "select", "options": ["json", "xml"], "default": "json"}]
            }
        }
    },
    "collab": {
        "icon": "fas fa-users",
        "description": "Collaboration tools.",
        "commands": {
            "view save": {
                "help": "Save a complex filter as a Virtual View.",
                "args": [
                    {"name": "name", "type": "pos", "help": "Name"},
                    {"name": "--query", "type": "opt", "help": "SQL Query"},
                    {"name": "--desc", "type": "opt", "help": "Description"}
                ]
            },
            "view list": {
                "help": "List saved views.",
                "args": [{"name": "--filter", "type": "opt", "help": "Filter by name"}]
            },
            "view load": {
                "help": "Load View into a table.",
                "args": [
                    {"name": "name", "type": "pos", "help": "View Name"},
                    {"name": "--as-table", "type": "opt", "help": "Target Table"}
                ]
            },
            "config export": {
                "help": "Export configuration to JSON.",
                "args": [
                    {"name": "cmd", "type": "select", "options": ["project", "user"], "default": "project", "help": "Scope"},
                    {"name": "file", "type": "pos", "help": "Output File"},
                    {"name": "--include-secrets", "type": "flag"}
                ]
            },
            "config import": {
                "help": "Import settings.",
                "args": [
                    {"name": "file_path", "type": "pos", "help": "File Path"},
                    {"name": "--scope", "type": "select", "options": ["project", "user"], "default": "project"}
                ]
            },
            "recipe bundle": {
                "help": "Bundle project files.",
                "args": [
                    {"name": "name", "type": "pos", "help": "Bundle Name"},
                    {"name": "--out", "type": "opt", "help": "Output Zip"}
                ]
            },
            "recipe install": {
                "help": "Unzip Project.",
                "args": [
                    {"name": "file_path", "type": "pos", "help": "Zip File"},
                    {"name": "--overwrite", "type": "flag"}
                ]
            },
            "notes add": {
                "help": "Annotate a table.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--msg", "type": "opt", "help": "Message"},
                    {"name": "--author", "type": "opt", "default": "user"}
                ]
            },
            "notes show": {
                "help": "Show annotations for a table.",
                "args": [{"name": "table_name", "type": "pos", "help": "Table"}]
            },
            "workspace dump": {
                "help": "State Export.",
                "args": [{"name": "--full", "type": "flag"}]
            }
        }
    },
    "report": {
        "icon": "fas fa-file-alt",
        "description": "Generate reports.",
        "commands": {
            "generate": {
                "help": "Sweetviz EDA Report.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--out", "type": "opt", "default": "report.html", "help": "Output File"},
                    {"name": "--layout", "type": "select", "options": ["widescreen", "vertical"], "default": "widescreen"}
                ]
            },
            "diff": {
                "help": "Before/After Report.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--snapshot", "type": "opt", "help": "Snapshot ID"},
                    {"name": "--out", "type": "opt", "default": "diff_report.html"}
                ]
            },
            "validation": {
                "help": "Quality Report.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--out", "type": "opt", "default": "validation_report.html"}
                ]
            },
            "schema-doc": {
                "help": "Generate Data Dictionary HTML.",
                "args": [
                    {"name": "--title", "type": "opt", "default": "Data Dictionary"},
                    {"name": "--out", "type": "opt", "default": "schema_doc.html"}
                ]
            },
            "audit-timeline": {
                "help": "Generate visual timeline of data changes.",
                "args": [
                    {"name": "--out", "type": "opt", "default": "audit_timeline.html"}
                ]
            },
            "map": {
                "help": "Offline Leaflet Map.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--lat", "type": "opt", "help": "Lat Col"},
                    {"name": "--lon", "type": "opt", "help": "Lon Col"},
                    {"name": "--out", "type": "opt", "default": "map.html"}
                ]
            },
            "summary": {
                "help": "Executive PDF.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--out", "type": "opt", "default": "summary.pdf"}
                ]
            },
            "dependencies": {
                "help": "Lineage DAG.",
                "args": [{"name": "--format", "type": "select", "options": ["png", "svg"], "default": "png"}]
            },
            "profile": {
                "help": "JSON Stats Dump.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "--minimal", "type": "flag"}
                ]
            },
            "serve": {
                "help": "Localhost Preview.",
                "args": [
                    {"name": "--port", "type": "opt", "default": 8000},
                    {"name": "--bind", "type": "opt", "default": "127.0.0.1"}
                ]
            }
        }
    },
    "plot": {
        "icon": "fas fa-chart-bar",
        "description": "Visualizations.",
        "commands": {
            "bar": {
                "help": "Vertical Bar Chart.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "cat", "type": "pos", "help": "Category Col"},
                    {"name": "num", "type": "pos", "help": "Numeric Col"},
                    {"name": "--agg", "type": "select", "options": ["mean", "sum", "count", "min", "max", "median"], "help": "Aggregation"},
                    {"name": "--stack", "type": "opt", "help": "Stack Col"},
                    {"name": "--out", "type": "opt", "help": "Output File"},
                    {"name": "--title", "type": "opt", "help": "Title"},
                    {"name": "--color", "type": "opt", "help": "Color"}
                ]
            },
            "barh": {
                "help": "Horizontal Bar Chart.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "cat", "type": "pos", "help": "Category Col"},
                    {"name": "num", "type": "pos", "help": "Numeric Col"},
                    {"name": "--agg", "type": "select", "options": ["mean", "sum", "count", "min", "max", "median"], "help": "Aggregation"},
                    {"name": "--out", "type": "opt", "help": "Output File"}
                ]
            },
            "hist": {
                "help": "Histogram.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "col", "type": "pos", "help": "Column"},
                    {"name": "--bins", "type": "opt", "default": 10},
                    {"name": "--out", "type": "opt", "help": "Output File"}
                ]
            },
            "scatter": {
                "help": "Scatter Plot.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "x", "type": "pos", "help": "X Col"},
                    {"name": "y", "type": "pos", "help": "Y Col"},
                    {"name": "--out", "type": "opt", "help": "Output File"},
                    {"name": "--marker", "type": "opt", "help": "Marker"}
                ]
            },
            "line": {
                "help": "Line Chart.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "x", "type": "pos", "help": "X Col"},
                    {"name": "y", "type": "pos", "help": "Y Col"},
                    {"name": "--sort", "type": "flag", "default": True},
                    {"name": "--out", "type": "opt", "help": "Output File"}
                ]
            },
            "box": {
                "help": "Box Plot.",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "cols", "type": "pos", "help": "Cols (comma-separated)"},
                    {"name": "--out", "type": "opt", "help": "Output File"}
                ]
            },
            "heatmap": {
                "help": "Density Heatmap (2D Histogram).",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "x", "type": "pos", "help": "X Col"},
                    {"name": "y", "type": "pos", "help": "Y Col"},
                    {"name": "--bins", "type": "opt", "default": 10},
                    {"name": "--out", "type": "opt", "help": "Output File"}
                ]
            },
            "matrix": {
                "help": "Scatter Matrix (Pair Plot).",
                "args": [
                    {"name": "table_name", "type": "pos", "help": "Table"},
                    {"name": "cols", "type": "pos", "help": "Cols (comma-separated)"},
                    {"name": "--out", "type": "opt", "help": "Output File"}
                ]
            },
            "save": { "help": "Export chart info.", "args": [] },
            "theme": {
                "help": "List or set theme.",
                "args": [{"name": "name", "type": "pos", "help": "Theme Name"}]
            }
        }
    }
}