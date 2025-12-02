# Wrought Iron: The Air-Gapped Data Foundry

**Version 1.0.0** | **Classification:** Enterprise / Industrial | **Platform:** Offline-First

Wrought Iron (WI) is a military-grade DataOps CLI designed for secure, air-gapped environments where data cannot leave the local machine. It unifies the power of Python's data science stack (`pandas`, `scikit-learn`, `cryptography`) into a single, monolithic binary that manages SQLite databases with zero external dependencies.

This manual serves as the definitive operational guide.

---

## Table of Contents

*   [Installation & Setup](#installation--setup)
*   [Core Concepts](#core-concepts)
*   [Module 1: Infrastructure (`wi connect`)](#module-1-infrastructure-wi-connect)
*   [Module 2: Schema Management (`wi schema`)](#module-2-schema-management-wi-schema)
*   [Module 3: Data Exploration (`wi query`)](#module-3-data-exploration-wi-query)
*   [Module 4: Analytics (`wi aggregate`)](#module-4-analytics-wi-aggregate)
*   [Module 5: Visualization (`wi plot`)](#module-5-visualization-wi-plot)
*   [Module 6: Data Wrangling (`wi clean`)](#module-6-data-wrangling-wi-clean)
*   [Module 7: Geospatial Analysis (`wi geo`)](#module-7-geospatial-analysis-wi-geo)
*   [Module 8: Machine Learning (`wi ml`)](#module-8-machine-learning-wi-ml)
*   [Module 9: Audit & Security (`wi audit`)](#module-9-audit--security-wi-audit)
*   [Module 10: Operations (`wi ops`)](#module-10-operations-wi-ops)
*   [Module 11: Collaboration (`wi collab`)](#module-11-collaboration-wi-collab)
*   [Module 12: Reporting (`wi report`)](#module-12-reporting-wi-report)
*   [Module 13: The Command Center (`wi interact`)](#module-13-the-command-center-wi-interact)

---

## Installation & Setup

Wrought Iron is distributed as a single package.

**Standard Install:**
```bash
pip install .
```

**Verifying Installation:**
Run the help command to ensure the CLI is linked correctly:
```bash
wi --help
```

**First Run:**
Wrought Iron maintains a local state in `~/.wrought_iron/`. No manual configuration file creation is required; the system initializes itself upon the first `connect` command.

---

## Core Concepts

1.  **The Active Connection:** WI does not require you to pass the database path to every command. You `connect` once, and that state persists across your session until you switch databases.
2.  **The "Backpack" Pattern:** WI treats JSON stored in text columns as first-class citizens, allowing for introspection and flattening of semi-structured data.
3.  **Immutable Audit:** Every data modification command (clean, geo, ml) automatically logs to an internal `_wi_audit_log_` table within the database file itself, ensuring the audit trail travels with the data.

---

## Module 1: Infrastructure (`wi connect`)

**Responsibility:** Connection Lifecycle, Storage Optimization, and Physical Security.

The Infrastructure module is the foundation of Wrought Iron. It handles the "physical" layer of data management: locating database files on disk, optimizing their storage footprint, ensuring their binary integrity, and securing them with military-grade encryption. Unlike traditional database servers that require network ports and user accounts, `wi connect` operates directly on the file system, making it ideal for air-gapped environments where data exists as isolated artifacts.

### 1. `file`
**Purpose:** Establishes a persistent connection to an existing SQLite database file.
*   **Detailed Description:** This command registers a specific `.db` file as the "Active Target" for all subsequent Wrought Iron operations. It is smart enough to detect "plain" SQLite databases and seamlessly upgrade them by injecting Wrought Iron's internal metadata tables (such as the immutable audit log) without altering existing data. This connection state persists across terminal sessions until changed.
*   **Scenarios:**
    *   *Forensic Analysis:* An investigator receives a USB drive containing a suspect's data dump and needs to attach to it immediately for inspection.
    *   *Audit Compliance:* An auditor connects to a production replica in "Read-Only" mode to verify financial records without risking accidental modification or timestamp tampering.
*   **Usage:** `wi connect file [PATH] [OPTIONS]`
*   **Arguments:**
    *   `PATH`: The absolute or relative path to the `.db` file.
    *   `--read-only`: Opens the database in immutable mode. Any attempt to write (INSERT/UPDATE/DELETE) will be blocked by the engine.
    *   `--check-wal`: Enforces a strict check for the Write-Ahead-Log (`.db-wal`) file. If the WAL is missing, the connection is refused. This prevents working on corrupt or incomplete file transfers where the main DB file exists but the transaction log was lost.
*   **Examples:**
    *   `wi connect file ./mission_data.db` (Standard connection)
    *   `wi connect file C:/Sensitive/logs.db --read-only` (Safe inspection mode)
    *   `wi connect file /mnt/transfer/incomplete.db --check-wal` (Verify transfer integrity before connecting)

### 2. `new`
**Purpose:** Initializes a brand new, empty Wrought Iron-compliant database repository.
*   **Detailed Description:** Instead of just creating a 0-byte file, this command scaffolds a complete Wrought Iron schema. It sets optimal SQLite PRAGMA settings (like page size and journaling mode) and pre-creates the `_wi_audit_log_` and `_wi_aliases_` system tables. This ensures that any data imported later is immediately subject to audit tracking.
*   **Scenarios:**
    *   *ETL Development:* A data engineer needs a fresh, clean container to start a new data consolidation project.
    *   *Unit Testing:* A developer creates a temporary database to run a suite of regression tests.
*   **Usage:** `wi connect new [PATH] [OPTIONS]`
*   **Arguments:**
    *   `PATH`: The path where the new file should be created.
    *   `--force`: If a file already exists at the path, this flag authorizes Wrought Iron to delete it and create a new one. **Use with caution.**
    *   `--page-size [4096|8192|16384]`: Allows tuning the underlying database page size. Larger page sizes (8192+) are efficient for analytics-heavy workloads (OLAP), while the default (4096) is balanced for general use.
*   **Examples:**
    *   `wi connect new ./analytics_store.db` (Default 4KB pages)
    *   `wi connect new /data/warehouse.db --page-size 16384` (Optimized for large aggregation queries)
    *   `wi connect new temp_test.db --force` (Overwrite existing test file)

### 3. `list`
**Purpose:** Displays a historical registry of all databases accessed on the local machine.
*   **Detailed Description:** Wrought Iron maintains a local registry of every connection event. This command prints a tabular history, showing file paths, file sizes, and the last access timestamp. This is crucial for managing "database sprawl" on a local workstation where an analyst might work with dozens of different files over a month.
*   **Scenarios:**
    *   *Workflow Resumption:* A user returns to work on Monday and needs to remember which file they were working on last Friday.
    *   *Storage Management:* Quickly identifying which local databases are consuming the most disk space.
*   **Usage:** `wi connect list [OPTIONS]`
*   **Arguments:**
    *   `--sort [access_time|size]`: Determines the ordering of the list. Default is `access_time` (most recent first). `size` is useful for finding large files.
    *   `--limit [INT]`: Limits the output to the top N rows. Default is 10.
*   **Examples:**
    *   `wi connect list` (Show 10 most recent)
    *   `wi connect list --sort size --limit 5` (Show top 5 largest databases)

### 4. `alias`
**Purpose:** Maps a long, complex file path to a short, memorable mnemonic.
*   **Detailed Description:** In secure environments, data is often buried deep within directory structures (e.g., `C:/Users/Admin/Secure/Projects/2025/Q1/Alpha/data.db`). Typing this repeatedly is error-prone. The `alias` command creates a persistent shortcut, allowing you to refer to the database simply by name in future sessions.
*   **Scenarios:**
    *   *Daily Operations:* Mapping the current production snapshot to the alias `prod` and the development version to `dev`.
    *   *Team Standardization:* Ensuring all team members refer to the "Master Client List" simply as `clients`, regardless of where they saved the file.
*   **Usage:** `wi connect alias [NAME] [PATH] [OPTIONS]`
*   **Arguments:**
    *   `NAME`: The short alias to create (e.g., `prod`).
    *   `PATH`: The full path to the target database.
    *   `--overwrite`: If the alias `NAME` already exists, this flag updates it to point to the new `PATH`.
*   **Examples:**
    *   `wi connect alias prod /mnt/secure/prod_v4.db`
    *   `wi connect alias q1_sales D:/Archives/2025/January/sales.db --overwrite`

### 5. `merge`
**Purpose:** Performs a high-speed, transactional bulk merge of two separate SQLite files into one.
*   **Detailed Description:** This is a file-to-file ETL (Extract, Transform, Load) operation. It reads tables from a "Source" database and writes them into a "Target" database. It handles schema conflicts intelligently and wraps the operation in transactions to ensure data consistency.
*   **Scenarios:**
    *   *Field Operations:* A field agent returns with a laptop containing a "Daily Sync" database. This command merges that data into the central "Headquarters" database.
    *   *Consolidation:* Combining `january.db`, `february.db`, and `march.db` into a single `q1_summary.db`.
*   **Usage:** `wi connect merge [TARGET_DB] [SOURCE_DB] [OPTIONS]`
*   **Arguments:**
    *   `TARGET_DB`: The destination database that will receive the data.
    *   `SOURCE_DB`: The database from which data is read.
    *   `--strategy [append|replace|ignore]`:
        *   `append`: Adds rows to existing tables. Fails if Primary Key conflicts occur (unless `ignore` logic is implicit in some contexts, but typically strictly appends).
        *   `replace`: Drops the table in Target and replaces it entirely with the Source table.
        *   `ignore`: Attempts to append, but silently skips rows where a Primary Key conflict occurs (deduplication).
    *   `--tables [LIST]`: A comma-separated list of specific table names to merge. If omitted, merges all tables.
    *   `--chunk-size [INT]`: Number of rows per transaction commit. Tunable for memory vs. speed (default 50,000).
*   **Examples:**
    *   `wi connect merge master.db daily.db --strategy append` (Standard accumulation)
    *   `wi connect merge master.db update.db --strategy replace --tables inventory` (Full refresh of inventory table only)
    *   `wi connect merge archive.db new_data.db --strategy ignore` (Add only new unique records)

### 6. `info`
**Purpose:** Inspects the physical header and configuration of the active database file.
*   **Detailed Description:** Provides a "health check" view of the database file structure. It reports the Page Size (crucial for performance), the Journal Mode (WAL vs Delete), text encoding (UTF-8/16), and file system permissions.
*   **Scenarios:**
    *   *Performance Tuning:* A DBA checks if a database is using 4KB pages vs 16KB pages to explain slow I/O.
    *   *Concurrency Check:* Verifying if `journal_mode` is `WAL` (Write-Ahead Logging), which allows simultaneous readers and writers.
*   **Usage:** `wi connect info [OPTIONS]`
*   **Arguments:**
    *   `--extended`: Calculates and displays additional heavy metrics, such as the current size of the WAL file and detailed file system permission bits (chmod).
*   **Examples:**
    *   `wi connect info` (Basic metadata)
    *   `wi connect info --extended` (Deep inspection including WAL size)

### 7. `vacuum`
**Purpose:** Rebuilds the entire database file to remove fragmentation and reclaim unused disk space.
*   **Detailed Description:** When rows are deleted in SQLite, the file size does *not* shrink; the space is marked as "free pages" for future reuse. `vacuum` forces a rewrite of the entire database, compacting it to the minimum possible size.
*   **Scenarios:**
    *   *Post-Archival Cleanup:* After deleting 5 years of historical data from a 100GB database, running `vacuum` might shrink the file to 20GB, saving massive storage space.
    *   *Optimization:* Defragmenting indices for faster query performance.
*   **Usage:** `wi connect vacuum [OPTIONS]`
*   **Arguments:**
    *   `--into [PATH]`: Instead of rebuilding the file in-place (which requires 2x disk space temporarily and risks corruption on power loss), this option generates a *new*, vacuumed copy at `PATH`. This is the safer production pattern.
*   **Examples:**
    *   `wi connect vacuum` (Standard in-place optimization)
    *   `wi connect vacuum --into optimized_copy.db` (Safe, copy-on-write optimization)

### 8. `integrity-check`
**Purpose:** Triggers SQLite's internal B-Tree corruption scanning algorithms.
*   **Detailed Description:** This command scans the database file for physical corruption, such as broken pointers, malformed pages, or index inconsistencies. It is the first line of defense when a database behaves erratically.
*   **Scenarios:**
    *   *Disaster Recovery:* Validating a database file recovered from a failing hard drive.
    *   *Routine Maintenance:* A scheduled weekly job to ensure long-term storage reliability.
*   **Usage:** `wi connect integrity-check [OPTIONS]`
*   **Arguments:**
    *   `--quick`: Performs a lighter scan that verifies page linkage but skips the computationally expensive verification of index content matching table content.
*   **Examples:**
    *   `wi connect integrity-check` (Full, deep scan)
    *   `wi connect integrity-check --quick` (Fast surface scan)

### 9. `encrypt`
**Purpose:** Secures the database file at rest using military-grade AES-256 encryption.
*   **Detailed Description:** Encrypts the entire database file content using the Fernet symmetric encryption standard. Once encrypted, the file is structurally indistinguishable from random noise and cannot be opened by standard SQLite tools. This is essential for transporting sensitive data across unsecure channels (e.g., email, USB).
*   **Scenarios:**
    *   *Data Transport:* An analyst needs to mail a database to another site. They encrypt it first to prevent interception.
    *   *Cold Storage:* Archiving highly sensitive data to a shared drive.
*   **Usage:** `wi connect encrypt [PATH] [OPTIONS]`
*   **Arguments:**
    *   `PATH`: The path to the database file to encrypt.
    *   `--output [PATH]`: The path where the encrypted file will be written. If omitted, it may overwrite or append an extension.
    *   `--key-file [PATH]`: The path to save the generated encryption key. If omitted, the system prompts for a password to derive the key.
*   **Examples:**
    *   `wi connect encrypt sensitive.db --output sensitive.db.enc --key-file secret.key` (Key-based)
    *   `wi connect encrypt sensitive.db --output safe.db.enc` (Password-based prompt)

### 10. `decrypt`
**Purpose:** Unlocks a Wrought Iron-encrypted file, restoring it to a usable SQLite database.
*   **Detailed Description:** Reverses the `encrypt` process. It takes an encrypted blob and a key (or password), validates the integrity of the payload, and reconstructs the original `.db` file.
*   **Scenarios:**
    *   *Receiving Data:* A recipient decrypts the file received from the analyst using the shared secret key.
*   **Usage:** `wi connect decrypt [PATH] [OPTIONS]`
*   **Arguments:**
    *   `PATH`: The path to the encrypted (`.enc`) file.
    *   `--key-file [PATH]`: The path to the key file used for encryption. If omitted, the system prompts for the password.
*   **Examples:**
    *   `wi connect decrypt sensitive.db.enc --key-file secret.key`
    *   `wi connect decrypt safe.db.enc` (Prompts for password)

---

## Module 2: Schema Management (`wi schema`)

**Responsibility:** Introspect structure, detect hidden data patterns, and evolve schema definitions safely.

The Schema Management module is your central toolkit for understanding, analyzing, and modifying the structure of your SQLite databases. It goes beyond simple table listings, providing deep insights into column properties, relationships between tables (via foreign keys), and even the ability to detect and manipulate semi-structured JSON data embedded within text columns. This module is essential for data governance, ensuring schema consistency across environments, and safely adapting your database structure as data requirements evolve, all while adhering to SQLite's unique architectural considerations.

### 1. `list`
**Purpose:** Displays a comprehensive list of all database objects (tables, views, indexes) within the active Wrought Iron database.
*   **Detailed Description:** This command provides an overview of the database's structure. It queries the `sqlite_master` table to enumerate existing objects. By default, it excludes system tables (like `sqlite_sequence`) and views to focus on user-defined data structures, but these can be included with specific flags.
*   **Scenarios:**
    *   *Initial Database Overview:* Quickly grasping the contents of an unfamiliar database.
    *   *Schema Audit:* Verifying the existence of expected tables or identifying unexpected ones.
*   **Usage:** `wi schema list [OPTIONS]`
*   **Arguments:**
    *   `--show-views / --no-views`: Toggle visibility of database views (Default: `--show-views`).
    *   `--show-sys / --no-sys`: Toggle visibility of SQLite's internal system tables (Default: `--no-sys`).
*   **Examples:**
    *   `wi schema list` (Show user tables and views)
    *   `wi schema list --no-views` (Only show tables, hide views)
    *   `wi schema list --show-sys` (Show all objects, including system tables)

### 2. `describe`
**Purpose:** Provides detailed structural information for a specified table, including column names, data types, nullability, default values, and primary/foreign key constraints.
*   **Detailed Description:** This command leverages SQLite's `PRAGMA table_info` and `PRAGMA foreign_key_list` to expose the internal definition of a table. It presents this information in an easy-to-read tabular format or, optionally, as the raw SQL DDL statement used to create the table. This is crucial for understanding how data is structured and constrained within a table.
*   **Scenarios:**
    *   *Data Modeling Review:* Examining a table's design to ensure it meets requirements.
    *   *Debugging Query Issues:* Quickly checking column names and types when a SQL query is failing.
    *   *Schema Migration Planning:* Understanding existing constraints before making changes.
*   **Usage:** `wi schema describe [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The exact name of the table to describe.
    *   `--format [table|sql]`: Specifies the output format. `table` displays a formatted grid; `sql` outputs the original `CREATE TABLE` statement (Default: `table`).
*   **Examples:**
    *   `wi schema describe users` (Show tabular description for the 'users' table)
    *   `wi schema describe products --format sql` (Display the DDL for the 'products' table)

### 3. `inspect`
**Purpose:** Generates a detailed profile of each column within a specified table, including non-null percentage, unique value percentage, and the top 5 most frequent values. Optionally, it can display mini-histograms for numeric columns.
*   **Detailed Description:** This command helps quickly understand the data quality and distribution within a table without needing to write complex SQL queries. It's particularly useful for exploratory data analysis on new or undocumented datasets. The sampling option allows for efficient profiling of very large tables.
*   **Scenarios:**
    *   *Data Discovery:* Gaining an initial understanding of an unknown dataset's characteristics.
    *   *Data Quality Assessment:* Identifying columns with a high percentage of nulls, low uniqueness (potential candidates for lookup tables), or unexpected value distributions.
    *   *Feature Engineering:* Identifying potential categorical or numerical features for machine learning models.
*   **Usage:** `wi schema inspect [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to inspect.
    *   `--sample [FLOAT]`: The percentage of data to scan for profiling (e.g., `0.1` for 10%, `1.0` for 100%). Useful for large tables to speed up inspection (Default: `1.0`).
    *   `--histogram`: If set, generates and displays simple ASCII-art histograms for numeric columns to visualize their distribution.
*   **Examples:**
    *   `wi schema inspect customers` (Full inspection of the 'customers' table)
    *   `wi schema inspect transactions --sample 0.05` (Inspect 5% of the 'transactions' table)
    *   `wi schema inspect sensor_data --histogram` (Inspect 'sensor_data' and show histograms for numeric columns)

### 4. `diff`
**Purpose:** Compares the schemas of two tables (either within the same database or across two different database files) and highlights differences such as added, removed, or modified columns.
*   **Detailed Description:** This command is invaluable for tracking schema evolution and ensuring consistency across development, testing, and production environments. It performs a column-level comparison, reporting changes in column presence, data types, nullability, and other properties. This helps identify "schema drift" that could lead to application errors or data integrity issues.
*   **Scenarios:**
    *   *DevOps & CI/CD:* Automatically comparing a development database schema against a production baseline as part of a deployment pipeline.
    *   *Schema Evolution:* Understanding the changes between different versions of a dataset or database.
    *   *Troubleshooting Data Inconsistencies:* Pinpointing subtle schema differences that might be causing data loading or processing failures.
*   **Usage:** `wi schema diff [TABLE_A] [TABLE_B] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_A`: The name of the first table for comparison.
    *   `TABLE_B`: The name of the second table for comparison.
    *   `--db-b [PATH]`: Optional path to a second database file if `TABLE_B` resides in a different database than the currently active one. If omitted, both tables are assumed to be in the active database.
*   **Examples:**
    *   `wi schema diff users users_v2` (Compare 'users' table with 'users_v2' in the same active database)
    *   `wi schema diff employees employees_archive --db-b /mnt/backup/archive.db` (Compare 'employees' table in active DB with 'employees_archive' in a separate backup file)

### 5. `graph`
**Purpose:** Generates a text-based Entity Relationship Diagram (ERD) of foreign key relationships within the active database.
*   **Detailed Description:** This command visualizes the relationships between tables as defined by foreign key constraints. It supports output in either Mermaid.js syntax (for easy rendering in Markdown and web pages) or DOT graph description language (for use with Graphviz). Understanding these relationships is fundamental for complex database queries and data modeling.
*   **Scenarios:**
    *   *Data Model Documentation:* Quickly generating a visual representation of the database schema for reports or presentations.
    *   *Impact Analysis:* Understanding how changes to one table might affect related tables.
    *   *New Team Member Onboarding:* Providing a clear overview of the database structure to new developers or analysts.
*   **Usage:** `wi schema graph [OPTIONS]`
*   **Arguments:**
    *   `--format [mermaid|dot]`: Specifies the output format for the ERD. `mermaid` produces Mermaid.js syntax; `dot` produces DOT language (Default: `mermaid`).
*   **Examples:**
    *   `wi schema graph --format mermaid` (Generate Mermaid ERD, which can be pasted into Mermaid.js live editor or GitHub Markdown)
    *   `wi schema graph --format dot > erd.dot && dot -Tpng erd.dot -o erd.png` (Generate DOT graph and render to PNG using Graphviz)

### 6. `detect-json`
**Purpose:** Scans `TEXT` columns in a specified table to identify fields that contain valid JSON structures, supporting the "Backpack" pattern for semi-structured data.
*   **Detailed Description:** This command intelligently probes text columns, attempting to parse their contents as JSON. It reports the percentage of valid JSON entries found in each column, allowing users to discover hidden semi-structured data. An optional depth parameter can limit the traversal of nested JSON objects, and a threshold can filter out columns with insufficient valid JSON.
*   **Scenarios:**
    *   *Data Cleaning & Discovery:* Pinpointing columns where raw API responses or configuration data might have been stored as JSON strings.
    *   *Schema Refinement:* Identifying candidates for flattening (using `wi schema flatten`) to promote semi-structured data into first-class columns.
    *   *Data Validation:* Quickly assessing the consistency of JSON data within a column.
*   **Usage:** `wi schema detect-json [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to scan.
    *   `--depth [INT]`: Maximum depth to traverse nested JSON objects when detecting. If `None`, there is no depth limit.
    *   `--threshold [FLOAT]`: Minimum percentage (0.0 to 1.0) of valid JSON rows in a column required to trigger detection (Default: `0.1`).
*   **Examples:**
    *   `wi schema detect-json api_logs` (Scan 'api_logs' for JSON columns with default threshold)
    *   `wi schema detect-json user_profiles --depth 3 --threshold 0.5` (Scan 'user_profiles' for JSON up to 3 levels deep, requiring 50% valid JSON)

### 7. `flatten`
**Purpose:** Transforms a JSON "Backpack" column into multiple distinct, first-class SQLite columns, effectively "exploding" the semi-structured data into a relational format.
*   **Detailed Description:** This powerful command deserializes JSON strings stored in a designated column and promotes its key-value pairs into new, individual columns within the table. It enables direct SQL querying and analysis of previously nested data. It handles nested JSON objects and arrays, creating new column names based on the original column and nested keys, with configurable prefixes and separators. The original JSON column can optionally be dropped after flattening.
*   **Scenarios:**
    *   *Data Normalization:* Converting semi-structured log data or API responses into a fully relational schema for easier reporting and analysis.
    *   *Feature Extraction:* Extracting specific data points from JSON blobs to be used as features in machine learning models.
    *   *Simplifying Queries:* Eliminating the need for complex JSON functions in SQL queries by making nested data directly accessible.
*   **Usage:** `wi schema flatten [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the JSON column.
    *   `COLUMN_NAME`: The name of the `TEXT` column containing JSON strings to flatten.
    *   `--prefix [STR]`: A string to prepend to the names of newly created columns (Default: `COLUMN_NAME` + `separator`).
    *   `--separator [STR]`: The string used to separate nested keys in the new column names (Default: `_`).
    *   `--drop-original`: If set, the original JSON column will be removed from the table after flattening is complete.
*   **Examples:**
    *   `wi schema flatten events payload` (Flatten the 'payload' column in 'events', using default prefixes and separator)
    *   `wi schema flatten users user_data --prefix profile_ --separator . --drop-original` (Flatten 'user_data', prefixing new columns with 'profile.', using '.' as separator, and dropping the original 'user_data' column)

### 8. `rename-col`
**Purpose:** Safely renames a column within a specified table, handling SQLite's limitations by performing a table reconstruction if necessary to preserve data and integrity.
*   **Detailed Description:** SQLite databases natively lack a direct `ALTER TABLE RENAME COLUMN` command in older versions, and even when available, it might not handle all edge cases (e.g., views, triggers, foreign keys). This command intelligently manages the renaming process, typically by creating a new table with the updated schema, migrating all data, and then replacing the old table. It ensures that data types, constraints, and existing data are preserved during the operation.
*   **Scenarios:**
    *   *Schema Refactoring:* Improving data clarity by updating ambiguous or inconsistent column names (e.g., `dob` to `date_of_birth`).
    *   *Standardization:* Aligning column names to a predefined data dictionary or naming convention.
    *   *Correcting Errors:* Rectifying typos or incorrect naming decisions made during initial table creation.
*   **Usage:** `wi schema rename-col [TABLE_NAME] [OLD_COLUMN_NAME] [NEW_COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the column to rename.
    *   `OLD_COLUMN_NAME`: The current name of the column.
    *   `NEW_COLUMN_NAME`: The desired new name for the column.
    *   `--dry-run`: If set, the command will only display the SQL statements that *would* be executed without actually modifying the database. Useful for verifying the operation before committing changes.
*   **Examples:**
    *   `wi schema rename-col customers cust_id customer_id` (Rename 'cust_id' to 'customer_id' in the 'customers' table)
    *   `wi schema rename-col products price_usd unit_price --dry-run` (Preview renaming 'price_usd' to 'unit_price' without executing)

### 9. `drop-col`
**Purpose:** Removes a specified column from a table, reconstructing the table if necessary to adhere to SQLite's schema modification limitations.
*   **Detailed Description:** Similar to renaming columns, SQLite's native `ALTER TABLE DROP COLUMN` command has limitations. This `drop-col` command provides a safe and robust way to remove a column by creating a new table with the desired schema (excluding the dropped column), migrating all data, and then replacing the original table. This ensures data integrity and proper schema enforcement while removing unnecessary columns.
*   **Scenarios:**
    *   *Data Pruning:* Removing sensitive or obsolete columns to reduce storage footprint or comply with data retention policies.
    *   *Schema Simplification:* Streamlining the database schema by eliminating unused or redundant columns.
    *   *Performance Optimization:* Removing large, infrequently accessed columns that might hinder query performance.
*   **Usage:** `wi schema drop-col [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table from which to drop the column.
    *   `COLUMN_NAME`: The name of the column to be dropped.
    *   `--vacuum`: If set, the database will be vacuumed immediately after the column is dropped. This reclaims unused disk space, which is often considerable after a column deletion.
*   **Examples:**
    *   `wi schema drop-col logs old_debug_info` (Drop 'old_debug_info' column from 'logs' table)
    *   `wi schema drop-col user_metrics temp_id --vacuum` (Drop 'temp_id' and reclaim space)

### 10. `cast`
**Purpose:** Forces type conversion for a specified column, transforming its data into a new type with configurable error handling policies.
*   **Detailed Description:** This command allows you to change the storage class of a column's data (e.g., from `TEXT` to `INTEGER`, `REAL`, or `BLOB`). It's crucial for ensuring data consistency and enabling numeric operations on columns that might contain numbers stored as text. The `on-error` policy dictates how non-convertible values are handled: they can be `nullify` (converted to `NULL`), `fail` (the operation aborts), or `ignore` (non-convertible values are left as is).
*   **Scenarios:**
    *   *Data Type Correction:* Fixing columns where numeric data was mistakenly imported as text, preventing mathematical calculations.
    *   *Data Standardization:* Ensuring all data in a column adheres to a specific type requirement.
    *   *Preparation for Analysis:* Converting columns to appropriate types before statistical analysis or machine learning.
*   **Usage:** `wi schema cast [TABLE_NAME] [COLUMN_NAME] [TYPE] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the column to cast.
    *   `COLUMN_NAME`: The name of the column whose type will be converted.
    *   `TYPE`: The target data type for the column (`integer`, `text`, `real`, `blob`).
    *   `--on-error [nullify|fail|ignore]`: Policy for handling values that cannot be converted to the new type (Default: `fail`).
        *   `nullify`: Replaces non-convertible values with `NULL`.
        *   `fail`: Aborts the operation if any value cannot be converted.
        *   `ignore`: Keeps non-convertible values in their original form (useful for mixed-type columns).
*   **Examples:**
    *   `wi schema cast products price real --on-error nullify` (Convert 'price' column to REAL, turning errors into NULLs)
    *   `wi schema cast user_ids id integer` (Convert 'id' column to INTEGER, failing on any conversion error)

---

## Module 3: Data Exploration (`wi query`)

**Responsibility:** High-speed retrieval, filtering, and boolean logic operations.

The Data Exploration module provides a suite of powerful commands for quickly inspecting, filtering, searching, and understanding the data within your Wrought Iron databases. It leverages highly optimized operations to enable fast ad-hoc querying and discovery, helping users gain immediate insights into their datasets without the need for complex SQL or external tools. This module is essential for initial data assessment, debugging, and preparing data for further analysis.

### 1. `head`
**Purpose:** Displays the first N rows of a specified table.
*   **Detailed Description:** This command is analogous to the `HEAD` command in Unix-like systems for files, providing a quick glance at the beginning of a dataset. It's useful for rapidly understanding the structure and initial contents of a table.
*   **Scenarios:**
    *   *Quick Data Preview:* Checking the format and columns of a newly loaded table.
    *   *Debugging ETL:* Verifying that data starts flowing correctly into a table after an ingestion process.
*   **Usage:** `wi query head [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to inspect.
    *   `-n [INT]`: The number of rows to display from the beginning of the table (Default: `10`).
*   **Examples:**
    *   `wi query head sales_data` (Show first 10 rows of 'sales_data')
    *   `wi query head sensor_logs -n 5` (Show first 5 rows of 'sensor_logs')

### 2. `tail`
**Purpose:** Displays the last N rows of a specified table.
*   **Detailed Description:** Similar to `head`, this command provides a view of the end of a dataset. It's particularly useful for time-series data or logs, where the most recent entries are often at the end of the table.
*   **Scenarios:**
    *   *Monitoring Recent Activity:* Quickly checking the latest entries in a log table.
    *   *Verifying Appends:* Confirming that new data is being added to the end of a table.
*   **Usage:** `wi query tail [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to inspect.
    *   `-n [INT]`: The number of rows to display from the end of the table (Default: `10`).
*   **Examples:**
    *   `wi query tail event_log -n 20` (Show last 20 events)
    *   `wi query tail transactions` (Show last 10 transactions)

### 3. `sample`
**Purpose:** Retrieves a random subset of rows from a table.
*   **Detailed Description:** This command is essential for working with large datasets where inspecting or processing all rows would be inefficient. It allows you to extract a representative sample, either by a fraction of the total rows or a fixed number of rows, with optional reproducibility via a random seed.
*   **Scenarios:**
    *   *Exploratory Data Analysis:* Quickly analyzing a small portion of a large dataset to understand its characteristics.
    *   *Debugging:* Isolating a smaller, random set of data that exhibits an issue without dealing with the entire dataset.
    *   *Model Training (Prototyping):* Creating a quick sample for initial machine learning model development.
*   **Usage:** `wi query sample [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to sample from.
    *   `--frac [FLOAT]`: The fraction of rows to return (e.g., `0.1` for 10%). Mutually exclusive with `-n`.
    *   `-n [INT]`: The exact number of rows to return. Mutually exclusive with `--frac`.
    *   `--seed [INT]`: A random seed to ensure reproducibility of the sample.
*   **Examples:**
    *   `wi query sample customer_feedback --frac 0.05` (Get a 5% random sample)
    *   `wi query sample large_dataset -n 1000 --seed 42` (Get 1000 random rows, reproducible)

### 4. `filter`
**Purpose:** Applies a Pythonic boolean logic expression to filter rows in a table.
*   **Detailed Description:** This command allows for powerful and intuitive filtering using a syntax similar to Python's boolean expressions, applied directly to the table's columns. It supports various operators and can be optimized using different execution engines for performance.
*   **Scenarios:**
    *   *Ad-hoc Filtering:* Quickly isolating records that meet specific criteria (e.g., all customers from 'USA' with `age > 30`).
    *   *Data Quality Checks:* Identifying rows that violate business rules or expected value ranges.
*   **Usage:** `wi query filter [TABLE_NAME] --where "[QUERY_STRING]" [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to filter.
    *   `--where "[QUERY_STRING]"`: The boolean expression to apply. Column names can be used directly (e.g., `"column_a > 10 and column_b == 'value'"`).
    *   `--engine [numexpr|python]`: The backend engine for executing the query string. `numexpr` is generally faster for numerical operations (Default: `numexpr`).
*   **Examples:**
    *   `wi query filter employees --where "salary > 50000 and department == 'IT'"`
    *   `wi query filter orders --where "status != 'completed' or amount < 100"`

### 5. `sql`
**Purpose:** Executes a raw SQL query directly against the active database and displays the results.
*   **Detailed Description:** This command provides a direct interface to SQLite's SQL engine, allowing experienced users to run any valid SQL statement. It supports parameterized queries for enhanced security and to prevent SQL injection. Results are presented in a formatted table.
*   **Scenarios:**
    *   *Advanced Querying:* Performing complex joins, aggregations, or subqueries not covered by other `wi query` commands.
    *   *Database Administration:* Executing DDL (Data Definition Language) or DML (Data Manipulation Language) commands (use with caution, as `wi query` is typically for read operations).
    *   *Troubleshooting:* Directly inspecting database state with custom SQL.
*   **Usage:** `wi query sql "[SQL_QUERY]" [OPTIONS]`
*   **Arguments:**
    *   `[SQL_QUERY]`: The full SQL query string to execute.
    *   `--params "[JSON_STRING]"`: A JSON string representing parameters for the SQL query. Can be a JSON object for named parameters (e.g., `{"name": "value"}`) or a JSON array for positional parameters (e.g., `["value1", "value2"]`).
*   **Examples:**
    *   `wi query sql "SELECT department, AVG(salary) FROM employees GROUP BY department"`
    *   `wi query sql "UPDATE products SET price = :new_price WHERE id = :product_id" --params '{"new_price": 25.99, "product_id": 101}'`

### 6. `search`
**Purpose:** Performs a global full-text search across specified (or all) columns in a table.
*   **Detailed Description:** This command helps locate specific text fragments within a table. It can search across multiple columns and supports case-sensitive/insensitive and regular expression matching, making it a flexible tool for forensic analysis or data discovery.
*   **Scenarios:**
    *   *Finding Specific Records:* Locating a transaction ID, error message, or user comment without knowing the exact column.
    *   *Compliance Checks:* Searching for keywords or patterns related to sensitive information across a dataset.
    *   *Data Profiling:* Identifying common patterns or unexpected data entries.
*   **Usage:** `wi query search [TABLE_NAME] [TERM] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to search.
    *   `TERM`: The string or regular expression to search for.
    *   `--cols "[COMMA_SEPARATED_COLUMNS]"`: A comma-separated list of column names to search within. If omitted, all `TEXT` columns will be searched.
    *   `--case-sensitive / --ignore-case`: Determines if the search should be case-sensitive (Default: `--ignore-case`).
    *   `--regex`: If set, the `TERM` will be treated as a regular expression pattern.
*   **Examples:**
    *   `wi query search audit_logs "FAILURE" --cols message,status`
    *   `wi query search documents "confidential|secret" --regex --case-sensitive`

### 7. `sort`
**Purpose:** Sorts the rows of a table based on the values in a specified column.
*   **Detailed Description:** This command arranges the table's data in ascending or descending order according to the values in one column. It offers different sorting algorithms for performance tuning on various data distributions.
*   **Scenarios:**
    *   *Ordered Reporting:* Preparing data for reports that require results to be ordered by date, ID, or value.
    *   *Top/Bottom N Analysis:* Quickly identifying the highest or lowest values in a column after sorting.
    *   *Data Presentation:* Arranging data for better readability and understanding.
*   **Usage:** `wi query sort [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to sort.
    *   `COLUMN_NAME`: The name of the column to sort by.
    *   `--order [asc|desc]`: The sorting order (ascending or descending) (Default: `asc`).
    *   `--alg [quicksort|mergesort]`: The sorting algorithm to use. `quicksort` is generally faster but not stable; `mergesort` is stable but potentially slower (Default: `quicksort`).
*   **Examples:**
    *   `wi query sort users age --order desc` (Sort 'users' table by 'age' in descending order)
    *   `wi query sort products price --alg mergesort` (Sort 'products' by 'price' using mergesort)

### 8. `distinct`
**Purpose:** Lists all unique values present in a specified column, with an option to include their frequency counts.
*   **Detailed Description:** This command helps understand the cardinality and distribution of values within a column. It can quickly reveal the range of categories, common entries, or potential data entry errors.
*   **Scenarios:**
    *   *Categorical Data Analysis:* Identifying all unique categories in a column (e.g., all distinct product types, countries).
    *   *Data Quality Check:* Spotting inconsistent spellings or unexpected values in a column.
    *   *Frequency Analysis:* Understanding the prevalence of different values when `--counts` is used.
*   **Usage:** `wi query distinct [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to analyze.
    *   `COLUMN_NAME`: The name of the column to find distinct values for.
    *   `--counts`: If set, also displays the frequency count for each unique value.
*   **Examples:**
    *   `wi query distinct orders status` (List unique order statuses)
    *   `wi query distinct users country --counts` (List unique countries and their counts)

### 9. `find-nulls`
**Purpose:** Identifies and displays rows that contain null or missing values in specified columns.
*   **Detailed Description:** This command is crucial for data quality assessment and identifying incomplete records. It can check for nulls in a specific set of columns or across all columns, and allows defining whether a row is considered "null" if *any* or *all* of the checked columns are null.
*   **Scenarios:**
    *   *Data Cleaning:* Quickly finding records that require imputation or correction due to missing information.
    *   *Identifying Incomplete Entries:* Locating user profiles missing essential contact details.
    *   *Validation:* Checking if mandatory fields have been populated.
*   **Usage:** `wi query find-nulls [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to scan for nulls.
    *   `--cols "[COMMA_SEPARATED_COLUMNS]"`: A comma-separated list of columns to check for null values. If omitted, all columns will be checked.
    *   `--mode [any|all]`: Determines the condition for a row to be considered "null".
        *   `any`: A row is displayed if *any* of the specified columns contain a null value (Default: `any`).
        *   `all`: A row is displayed only if *all* of the specified columns contain null values.
*   **Examples:**
    *   `wi query find-nulls customers --cols email,phone` (Find customers missing email or phone)
    *   `wi query find-nulls sensor_data --mode all` (Find sensor readings where all columns are null)

### 10. `dups`
**Purpose:** Identifies and displays duplicate rows in a table based on a specific column or across all columns.
*   **Detailed Description:** This command is essential for maintaining data integrity and ensuring uniqueness where required. It can detect exact duplicate rows, or duplicates based on the values in a specific column. It also allows control over which duplicate occurrences (first, last, or all) are reported.
*   **Scenarios:**
    *   *Data Deduplication:* Finding and reviewing redundant entries before cleaning them.
    *   *Identifying Data Entry Errors:* Spotting accidental multiple entries for the same record.
    *   *Primary Key Violation Checks:* Ensuring the uniqueness of candidate primary key columns.
*   **Usage:** `wi query dups [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to check for duplicates.
    *   `COLUMN_NAME`: The name of the single column to check for duplicate values. If omitted, entire rows are checked for duplication.
    *   `--keep [first|last|none]`: Determines which duplicate occurrences to mark.
        *   `first`: Marks all duplicates except the first occurrence (Default: `first`).
        *   `last`: Marks all duplicates except the last occurrence.
        *   `none`: Marks all duplicate occurrences.
*   **Examples:**
    *   `wi query dups user_accounts email` (Find duplicate 'email' entries in 'user_accounts')
    *   `wi query dups raw_data --keep none` (Show all completely duplicated rows)

---

## Module 4: Analytics (`wi aggregate`)

**Responsibility:** Statistical computing and aggregations.

The Analytics module provides a robust set of tools for performing statistical analysis and aggregations on your data. Leveraging the power of libraries like Pandas, it enables users to summarize, transform, and understand the distributional properties of their datasets directly within the CLI. From basic descriptive statistics to complex pivot tables and correlation matrices, this module is indispensable for data scientists and analysts working in air-gapped environments.

### 1. `groupby`
**Purpose:** Groups data by one or more columns and applies specified aggregation functions to other columns.
*   **Detailed Description:** This command is the cornerstone of data aggregation, allowing you to segment your data by categorical variables and compute summary statistics for numerical columns within each segment. It supports multiple grouping columns and various aggregation functions (e.g., mean, sum, count, min, max). The result can optionally be pivoted for a wider format.
*   **Scenarios:**
    *   *Sales Analysis:* Grouping sales data by region and product to find average sales or total revenue.
    *   *Customer Segmentation:* Analyzing customer demographics (e.g., age, location) to understand purchasing behavior.
    *   *Performance Metrics:* Calculating key performance indicators (KPIs) for different teams or projects.
*   **Usage:** `wi aggregate groupby [TABLE_NAME] [GROUP_COLS] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to aggregate.
    *   `GROUP_COLS`: A comma-separated list of column names to group the data by.
    *   `--agg "[JSON_STRING]"`: A JSON string defining the aggregation functions to apply. Format: `"column1:function1,column2:function2"`. Example: `"age:mean,salary:sum"`. Supported functions typically include: `mean`, `sum`, `count`, `min`, `max`, `std`, `var`, `median`.
    *   `--pivot`: If set, attempts to pivot the resulting grouped data to a wide format, requiring at least two `GROUP_COLS`.
*   **Examples:**
    *   `wi aggregate groupby employees department --agg "salary:mean"` (Calculate average salary per department)
    *   `wi aggregate groupby sales_transactions region,product_type --agg "amount:sum,quantity:count"` (Total sales amount and item count per region and product type)
    *   `wi aggregate groupby customer_activity month --agg "user_id:count" --pivot` (Monthly unique user counts, pivoted)

### 2. `pivot`
**Purpose:** Reshapes data from a "long" format to a "wide" format, creating a pivot table that summarizes data.
*   **Detailed Description:** A pivot table is a powerful summarization tool that reorganizes selected columns into a new table, allowing for dynamic grouping and aggregation. It requires specifying an index (rows), columns, and values to aggregate, along with an aggregation function. Missing values in the new structure can be filled with a specified value.
*   **Scenarios:**
    *   *Financial Reporting:* Summarizing monthly expenses by category and sub-category.
    *   *Survey Analysis:* Cross-tabulating responses to different questions.
    *   *Comparative Analysis:* Comparing performance metrics across different dimensions (e.g., sales of products across different regions).
*   **Usage:** `wi aggregate pivot [TABLE_NAME] [INDEX_COLUMN] [COLUMNS_COLUMN] [VALUES_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to pivot.
    *   `INDEX_COLUMN`: The column whose unique values will form the new index (rows) of the pivot table.
    *   `COLUMNS_COLUMN`: The column whose unique values will form the new columns of the pivot table.
    *   `VALUES_COLUMN`: The column whose values will be aggregated in the pivot table cells.
    *   `--func [mean|sum|count|max|min]`: The aggregation function to apply to the `VALUES_COLUMN` (Default: `mean`).
    *   `--fill-value [VALUE]`: A value to replace `NaN` (Not a Number) entries in the pivot table (e.g., `0`, `"N/A"`).
*   **Examples:**
    *   `wi aggregate pivot sales_data date region amount --func sum` (Total sales amount per date per region)
    *   `wi aggregate pivot website_traffic visitor_type device_type session_duration --func mean --fill-value 0` (Average session duration by visitor type and device type, filling empty cells with 0)

### 3. `crosstab`
**Purpose:** Generates a frequency matrix (cross-tabulation) of two or more factors (columns) to show their joint distribution.
*   **Detailed Description:** A cross-tabulation table provides a powerful way to understand the relationship between two categorical variables. It counts the occurrences of each unique combination of values from the specified row and column. Options include normalizing counts to percentages and adding marginal sums.
*   **Scenarios:**
    *   *Market Research:* Analyzing the relationship between customer demographics (e.g., age group, gender) and product preferences.
    *   *Quality Control:* Cross-tabulating defect types by production line to identify patterns.
    *   *Survey Analysis:* Understanding how responses to one question correlate with responses to another.
*   **Usage:** `wi aggregate crosstab [TABLE_NAME] [ROW_COLUMN] [COLUMN_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to analyze.
    *   `ROW_COLUMN`: The column whose unique values will form the rows of the crosstab table.
    *   `COLUMN_COLUMN`: The column whose unique values will form the columns of the crosstab table.
    *   `--normalize [index|columns|all]`: Specifies how to normalize the values to percentages.
        *   `index`: Normalize over each row.
        *   `columns`: Normalize over each column.
        *   `all`: Normalize over all values.
    *   `--margins`: If set, adds row and column subtotals (margins) to the table.
*   **Examples:**
    *   `wi aggregate crosstab customer_survey gender product_rating --normalize all` (Percentage distribution of product ratings by gender)
    *   `wi aggregate crosstab employee_data department job_satisfaction --margins` (Counts of job satisfaction per department with totals)

### 4. `describe`
**Purpose:** Generates summary statistics for the columns of a table, including count, mean, standard deviation, min/max values, and quartiles.
*   **Detailed Description:** This command provides a quick, high-level overview of the central tendency, dispersion, and shape of a dataset's distribution. It's an essential first step in exploratory data analysis. Users can specify custom percentiles and choose to include only numerical or object (text) columns.
*   **Scenarios:**
    *   *Initial Data Scan:* Quickly understanding the basic characteristics of a new dataset.
    *   *Data Quality Check:* Identifying potential outliers (min/max values), data entry errors, or unexpected distributions.
    *   *Reporting:* Generating standard summary statistics for data reports.
*   **Usage:** `wi aggregate describe [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to describe.
    *   `--percentiles "[COMMA_SEPARATED_FLOATS]"`: A comma-separated list of percentiles to calculate (e.g., `"0.05,0.25,0.75,0.95"`). Values must be between 0.0 and 1.0.
    *   `--include [all|number|object]`: Specifies which column types to include in the description.
        *   `all`: Include all columns (numeric and object).
        *   `number`: Include only numeric columns.
        *   `object`: Include only object (text) columns.
*   **Examples:**
    *   `wi aggregate describe sensor_readings` (Default summary statistics for numeric columns)
    *   `wi aggregate describe customer_demographics --include object` (Summary for categorical columns like 'city', 'country')
    *   `wi aggregate describe financial_data --percentiles "0.01,0.99"` (Include 1st and 99th percentiles)

### 5. `corr`
**Purpose:** Calculates the pairwise correlation matrix between numerical columns in a table.
*   **Detailed Description:** This command helps identify linear relationships (Pearson) or monotonic relationships (Spearman, Kendall) between pairs of numerical variables. The correlation matrix is a fundamental tool in multivariate analysis, showing how changes in one variable relate to changes in another.
*   **Scenarios:**
    *   *Feature Selection:* Identifying highly correlated features that might cause multicollinearity issues in statistical models.
    *   *Relationship Discovery:* Uncovering potential dependencies between different measurements (e.g., temperature and pressure readings).
    *   *Data Understanding:* Gaining insights into the structure of numerical data.
*   **Usage:** `wi aggregate corr [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to analyze.
    *   `--method [pearson|spearman|kendall]`: The method of correlation to compute (Default: `pearson`).
        *   `pearson`: Standard linear correlation coefficient.
        *   `spearman`: Spearman's rank correlation coefficient (non-parametric).
        *   `kendall`: Kendall's Tau correlation coefficient (non-parametric).
    *   `--min-periods [INT]`: Minimum number of observations required per pair of columns to have a valid result. Pairs with fewer observations will result in `NaN`.
*   **Examples:**
    *   `wi aggregate corr stock_prices` (Pearson correlation between all numeric columns)
    *   `wi aggregate corr survey_ranks --method spearman` (Spearman correlation for ranked survey data)

### 6. `skew`
**Purpose:** Calculates the skewness of numerical columns in a table, indicating the asymmetry of their probability distribution.
*   **Detailed Description:** Skewness measures the degree to which a distribution deviates from symmetry. A positive skew indicates a "tail" extending to the right (more extreme positive values), while a negative skew indicates a tail extending to the left (more extreme negative values). This is useful for understanding data shape and often informs data transformation decisions.
*   **Scenarios:**
    *   *Data Preprocessing:* Identifying columns that might need transformation (e.g., log transformation) to achieve a more normal distribution for statistical models.
    *   *Understanding Data Distribution:* Characterizing the shape of financial returns, customer spending, or sensor error distributions.
*   **Usage:** `wi aggregate skew [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to analyze.
    *   `--numeric-only`: If set, only calculates skewness for numerical columns, ignoring non-numeric ones.
*   **Examples:**
    *   `wi aggregate skew sales_figures` (Calculate skewness for all numeric columns in 'sales_figures')
    *   `wi aggregate skew customer_lifetime_value --numeric-only` (Calculate skewness, ensuring only numeric columns are processed)

### 7. `kurtosis`
**Purpose:** Calculates the kurtosis of numerical columns in a table, indicating the "tailedness" of their probability distribution.
*   **Detailed Description:** Kurtosis measures whether the data are heavy-tailed or light-tailed relative to a normal distribution. High kurtosis implies more extreme outliers (heavy tails), while low kurtosis implies fewer extreme outliers (light tails). This provides further insight into the shape of data distributions, complementing skewness.
*   **Scenarios:**
    *   *Risk Assessment:* Analyzing financial data for tail risk (high kurtosis).
    *   *Outlier Detection:* Helping to understand the propensity of a dataset to produce extreme values.
    *   *Statistical Modeling:* Informing the choice of statistical models, as many assume normality (low kurtosis).
*   **Usage:** `wi aggregate kurtosis [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to analyze.
    *   `--numeric-only`: If set, only calculates kurtosis for numerical columns, ignoring non-numeric ones.
*   **Examples:**
    *   `wi aggregate kurtosis stock_returns` (Calculate kurtosis for stock returns)
    *   `wi aggregate kurtosis error_magnitudes --numeric-only` (Calculate kurtosis for error magnitudes, focusing on numeric data)

### 8. `moving-avg`
**Purpose:** Calculates the rolling (moving) average for a specified numerical column over a defined window.
*   **Detailed Description:** This command is primarily used for time-series analysis to smooth out short-term fluctuations and highlight longer-term trends or cycles. It computes the average of data points within a "window" that moves across the dataset. Users can specify the window size, whether to center the window, and the minimum number of observations required in the window.
*   **Scenarios:**
    *   *Time Series Smoothing:* Analyzing stock prices, sensor readings, or website traffic to identify trends by reducing noise.
    *   *Signal Processing:* Extracting underlying patterns from noisy data.
    *   *Forecasting Preparation:* Creating smoothed features for predictive models.
*   **Usage:** `wi aggregate moving-avg [TABLE_NAME] [COLUMN_NAME] --window [INT] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `COLUMN_NAME`: The name of the numerical column for which to calculate the moving average.
    *   `--window [INT]`: The size of the moving window (number of observations). This is a mandatory argument.
    *   `--center`: If set, the label of the window's result is placed at the center of the window; otherwise, it's placed at the end.
    *   `--min-periods [INT]`: Minimum number of observations in the window required to have a value (otherwise result is `NaN`). If not specified, defaults to the window size.
*   **Examples:**
    *   `wi aggregate moving-avg temperature_sensors reading --window 7` (7-day moving average of temperature readings)
    *   `wi aggregate moving-avg stock_data close --window 20 --center` (20-period centered moving average of closing prices)

### 9. `rank`
**Purpose:** Assigns a rank to each value in a specified numerical column.
*   **Detailed Description:** Ranking converts numerical values into their ordinal positions within a sorted sequence. This is useful for comparing items relative to each other rather than by their absolute values. Various methods are available to handle ties (identical values). The command can also compute percentile rank.
*   **Scenarios:**
    *   *Leaderboards:* Ranking players by score or employees by performance.
    *   *Competitive Analysis:* Comparing products or companies based on various metrics.
    *   *Non-parametric Statistics:* Preparing data for rank-based statistical tests.
*   **Usage:** `wi aggregate rank [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `COLUMN_NAME`: The name of the numerical column to rank.
    *   `--method [average|min|max|first|dense]`: The method to use for ranking tied values (Default: `average`).
        *   `average`: Assigns the average of ranks to tied values.
        *   `min`: Assigns the minimum rank to tied values.
        *   `max`: Assigns the maximum rank to tied values.
        *   `first`: Assigns ranks in order of appearance.
        *   `dense`: Similar to `min`, but ranks are consecutive without gaps.
    *   `--pct`: If set, computes percentile ranks (0-1) instead of standard ranks.
*   **Examples:**
    *   `wi aggregate rank student_grades score --method dense` (Rank students by score, using dense ranking for ties)
    *   `wi aggregate rank product_sales revenue --pct` (Calculate percentile rank of product revenues)

### 10. `bin`
**Purpose:** Discretizes a numerical column into discrete intervals (bins).
*   **Detailed Description:** Binning transforms continuous numerical data into categorical data by grouping values into a specified number of bins. This simplifies complex distributions and can be useful for visualization, or for preparing data for models that prefer categorical inputs. Custom labels can be provided for the bins.
*   **Scenarios:**
    *   *Age Groups:* Converting continuous ages into discrete age groups (e.g., "0-18", "19-35", "36-60").
    *   *Income Brackets:* Grouping continuous income values into predefined brackets for demographic analysis.
    *   *Histogram Preparation:* Preparing data for frequency distribution charts.
*   **Usage:** `wi aggregate bin [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `COLUMN_NAME`: The name of the numerical column to discretize.
    *   `--bins [INT]`: The number of equal-width bins to create (Default: `10`).
    *   `--labels "[COMMA_SEPARATED_STRINGS]"`: A comma-separated list of custom labels for the bins. The number of labels must match the number of bins.
*   **Examples:**
    *   `wi aggregate bin customer_data age --bins 5 --labels "Young,Adult,Middle-Aged,Senior,Elderly"` (Bin age into 5 custom-labeled groups)
    *   `wi aggregate bin transaction_amounts amount --bins 10` (Bin transaction amounts into 10 default bins)

---

## Module 5: Visualization (`wi plot`)

**Responsibility:** Generate ASCII/Unicode charts directly in the terminal or save them to files.

The Visualization module allows users to generate various types of plots directly within the terminal, leveraging the `plotext` library. This is particularly useful for quickly visualizing data distributions, trends, and relationships in environments where graphical user interfaces might be limited or unavailable. Users can create bar charts, histograms, scatter plots, line charts, box plots, heatmaps, and scatter matrices, and customize their appearance with themes and output options.

### 1. `bar`
**Purpose:** Generates a vertical bar chart to visualize the relationship between a categorical and a numerical column.
*   **Detailed Description:** This command is ideal for comparing discrete categories. It can display raw numerical values or aggregated values (e.g., sum, mean) per category. Bars can also be stacked by an additional categorical column for more complex comparisons. Plots can be displayed in the terminal or saved to a file.
*   **Scenarios:**
    *   *Sales by Region:* Visualizing total sales for different geographical regions.
    *   *Product Category Performance:* Comparing the mean rating of different product categories.
    *   *Grouped Counts:* Showing the number of occurrences of different items.
*   **Usage:** `wi plot bar [TABLE_NAME] [CATEGORICAL_COLUMN] [NUMERICAL_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `CATEGORICAL_COLUMN`: The column to use for categories on the X-axis.
    *   `NUMERICAL_COLUMN`: The column to use for bar heights (numerical values).
    *   `--agg [sum|mean|count|min|max|median]`: An aggregation function to apply to the `NUMERICAL_COLUMN` for each category (e.g., `sum`, `mean`).
    *   `--stack [STACKING_COLUMN]`: An additional categorical column to stack the bars by.
    *   `--out [FILE_PATH]`: Path to save the plot (e.g., `chart.html`, `chart.png`). If omitted, the plot displays in the terminal.
    *   `--title [TITLE_STRING]`: Override the default chart title.
    *   `--xlabel [LABEL_STRING]`: Override the default X-axis label.
    *   `--ylabel [LABEL_STRING]`: Override the default Y-axis label.
    *   `--grid / --no-grid`: Show or hide grid lines (Default: `grid`).
    *   `--color [COLOR_NAME]`: Set the color of the bars (e.g., `red`, `blue`, `green`).
*   **Examples:**
    *   `wi plot bar products category price --agg mean --title "Average Price by Category"`
    *   `wi plot bar sales_data region revenue --agg sum --out regional_sales.html`
    *   `wi plot bar website_visitors day_of_week visitors --stack device_type`

### 2. `barh`
**Purpose:** Generates a horizontal bar chart, useful for comparing categories with long labels.
*   **Detailed Description:** This command is similar to `bar` but displays bars horizontally, which is often more readable when category names are long. It also supports aggregation, stacking, and all the styling options of the vertical bar chart.
*   **Scenarios:**
    *   *Survey Results:* Displaying satisfaction scores for various detailed feedback categories.
    *   *Project Status:* Visualizing progress for numerous projects with lengthy names.
*   **Usage:** `wi plot barh [TABLE_NAME] [CATEGORICAL_COLUMN] [NUMERICAL_COLUMN] [OPTIONS]`
*   **Arguments:** (Same as `bar`, but X-axis becomes the numerical axis and Y-axis is categorical)
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `CATEGORICAL_COLUMN`: The column to use for categories on the Y-axis.
    *   `NUMERICAL_COLUMN`: The column to use for bar lengths (numerical values) on the X-axis.
    *   (Other options like `--agg`, `--stack`, `--out`, `--title`, `--xlabel`, `--ylabel`, `--grid`, `--color` are identical to `bar`.)
*   **Examples:**
    *   `wi plot barh countries country population --agg sum --title "Population by Country"`
    *   `wi plot barh research_topics topic citation_count --out topic_citations.png --color yellow`

### 3. `hist`
**Purpose:** Creates a histogram to visualize the distribution of a single numerical column.
*   **Detailed Description:** Histograms divide the data into a series of "bins" and count how many data points fall into each bin, providing a visual representation of the frequency distribution. This helps identify the shape of the data, central tendency, spread, and presence of outliers.
*   **Scenarios:**
    *   *Age Distribution:* Understanding the spread of ages in a customer database.
    *   *Exam Scores:* Visualizing the distribution of scores in a test.
    *   *Sensor Reading Deviations:* Checking if sensor readings are normally distributed or skewed.
*   **Usage:** `wi plot hist [TABLE_NAME] [NUMERICAL_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `NUMERICAL_COLUMN`: The numerical column whose distribution is to be plotted.
    *   `--bins [INT]`: The number of bins to use for the histogram (Default: `10`).
    *   `--out [FILE_PATH]`: Path to save the plot.
    *   `--title [TITLE_STRING]`: Override the default chart title.
    *   `--xlabel [LABEL_STRING]`: Override the default X-axis label (typically the numerical column name).
    *   `--ylabel [LABEL_STRING]`: Override the default Y-axis label (typically "Frequency").
    *   `--grid / --no-grid`: Show or hide grid lines.
    *   `--color [COLOR_NAME]`: Set the color of the histogram bars.
*   **Examples:**
    *   `wi plot hist customer_data age --bins 20 --title "Age Distribution"`
    *   `wi plot hist transaction_amounts amount --out transaction_hist.html`

### 4. `scatter`
**Purpose:** Generates a scatter plot to show the relationship between two numerical columns.
*   **Detailed Description:** A scatter plot displays individual data points as markers on a two-dimensional plane, where each axis represents a numerical variable. It is used to observe the correlation or patterns between two variables, or to identify clusters and outliers.
*   **Scenarios:**
    *   *Correlation Analysis:* Investigating the relationship between height and weight, or advertising spend and sales.
    *   *Outlier Detection:* Identifying data points that deviate significantly from the general pattern.
    *   *Clustering Visualization:* Observing natural groupings within the data.
*   **Usage:** `wi plot scatter [TABLE_NAME] [X_COLUMN] [Y_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `X_COLUMN`: The numerical column for the X-axis.
    *   `Y_COLUMN`: The numerical column for the Y-axis.
    *   `--out [FILE_PATH]`: Path to save the plot.
    *   `--title [TITLE_STRING]`: Override the default chart title.
    *   `--xlabel [LABEL_STRING]`: Override the default X-axis label.
    *   `--ylabel [LABEL_STRING]`: Override the default Y-axis label.
    *   `--grid / --no-grid`: Show or hide grid lines.
    *   `--color [COLOR_NAME]`: Set the color of the scatter points.
    *   `--marker [MARKER_STYLE]`: Set the style of the points (e.g., `sd` for square dot, `hd` for half dot, `dot`, `heart`).
*   **Examples:**
    *   `wi plot scatter sensor_readings temperature pressure --title "Temperature vs Pressure"`
    *   `wi plot scatter employee_performance hours_worked productivity --marker hd --color green`

### 5. `line`
**Purpose:** Creates a line chart to visualize trends over time or across an ordered numerical variable.
*   **Detailed Description:** Line charts connect data points with lines, making them excellent for displaying trends, particularly in time-series data. The X-axis often represents time or an ordered category, while the Y-axis represents a numerical value. The data can be sorted by the X-axis for clearer trend visualization.
*   **Scenarios:**
    *   *Stock Price Trends:* Tracking the performance of a stock over a period.
    *   *Website Traffic:* Monitoring daily or hourly website visitors.
    *   *Sensor Data Over Time:* Observing changes in sensor readings over an interval.
*   **Usage:** `wi plot line [TABLE_NAME] [X_COLUMN] [Y_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `X_COLUMN`: The column for the X-axis (e.g., date, time, or an ordered numerical category).
    *   `Y_COLUMN`: The numerical column for the Y-axis.
    *   `--sort`: If set, sorts the data by the X-axis column before plotting (Default: `True`).
    *   `--out [FILE_PATH]`: Path to save the plot.
    *   `--title [TITLE_STRING]`: Override the default chart title.
    *   `--xlabel [LABEL_STRING]`: Override the default X-axis label.
    *   `--ylabel [LABEL_STRING]`: Override the default Y-axis label.
    *   `--grid / --no-grid`: Show or hide grid lines.
    *   `--color [COLOR_NAME]`: Set the color of the line.
    *   `--marker [MARKER_STYLE]`: Set the style of the points on the line.
*   **Examples:**
    *   `wi plot line weather_data date temperature --title "Daily Temperature Trend"`
    *   `wi plot line production_metrics month_year output --out production_trend.png --color cyan`

### 6. `box`
**Purpose:** Generates box plots for one or more numerical columns to visualize their distribution and identify outliers.
*   **Detailed Description:** A box plot (or box-and-whisker plot) displays the distribution of a dataset based on a five-number summary: minimum, first quartile (Q1), median (Q2), third quartile (Q3), and maximum. It's excellent for comparing the spread and central tendency of different variables or groups, and for highlighting outliers.
*   **Scenarios:**
    *   *Comparing Distributions:* Analyzing the distribution of salaries across different departments.
    *   *Outlier Detection:* Easily spotting unusually high or low values.
    *   *Data Skewness:* Gaining a quick visual understanding of data skewness.
*   **Usage:** `wi plot box [TABLE_NAME] [COMMA_SEPARATED_COLUMNS] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `COMMA_SEPARATED_COLUMNS`: A comma-separated list of numerical columns to plot. Each column will get its own box plot.
    *   `--out [FILE_PATH]`: Path to save the plot.
    *   `--title [TITLE_STRING]`: Override the default chart title.
    *   `--xlabel [LABEL_STRING]`: Override the default X-axis label (typically "Columns").
    *   `--ylabel [LABEL_STRING]`: Override the default Y-axis label (typically "Values").
    *   `--grid / --no-grid`: Show or hide grid lines.
    *   `--color [COLOR_NAME]`: Set the color of the box plots.
*   **Examples:**
    *   `wi plot box student_scores math,science,english --title "Subject Score Distributions"`
    *   `wi plot box sensor_data sensor1,sensor2 --out sensor_boxes.html`

### 7. `heatmap`
**Purpose:** Creates a 2D density heatmap (similar to a 2D histogram) to visualize the joint distribution of two numerical columns.
*   **Detailed Description:** A heatmap divides the 2D space defined by two numerical columns into a grid of bins. The color intensity of each bin represents the density or frequency of data points falling into that region. This is useful for identifying areas of high concentration and patterns in bivariate distributions.
*   **Scenarios:**
    *   *Spatial Density:* Visualizing the density of events based on their X and Y coordinates.
    *   *Bivariate Distribution:* Understanding where the most common combinations of two variables occur.
    *   *Traffic Patterns:* Showing areas of high traffic flow on a grid.
*   **Usage:** `wi plot heatmap [TABLE_NAME] [X_COLUMN] [Y_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `X_COLUMN`: The numerical column for the X-axis.
    *   `Y_COLUMN`: The numerical column for the Y-axis.
    *   `--bins [INT]`: The number of bins to use for each axis of the grid (Default: `10`).
    *   `--out [FILE_PATH]`: Path to save the plot.
    *   `--title [TITLE_STRING]`: Override the default chart title.
    *   `--xlabel [LABEL_STRING]`: Override the default X-axis label.
    *   `--ylabel [LABEL_STRING]`: Override the default Y-axis label.
    *   `--grid / --no-grid`: Show or hide grid lines.
*   **Examples:**
    *   `wi plot heatmap crime_data longitude latitude --bins 20 --title "Crime Hotspots"`
    *   `wi plot heatmap sensor_readings temp humidity --out temp_humidity_density.png`

### 8. `matrix`
**Purpose:** Generates a scatter matrix (pair plot) for multiple numerical columns, showing pairwise scatter plots and individual histograms.
*   **Detailed Description:** A scatter matrix is a grid of plots where each numerical column is plotted against every other numerical column in a scatter plot. The diagonal usually contains histograms for individual column distributions. This provides a comprehensive overview of all pairwise relationships and individual distributions within a dataset.
*   **Scenarios:**
    *   *Multi-variate Data Exploration:* Quickly assessing relationships and distributions in a dataset with several numerical features.
    *   *Feature Engineering:* Identifying potential correlations or non-linear relationships between features.
    *   *Data Quality Review:* Spotting unusual patterns or outliers across multiple dimensions.
*   **Usage:** `wi plot matrix [TABLE_NAME] [COMMA_SEPARATED_COLUMNS] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data.
    *   `COMMA_SEPARATED_COLUMNS`: A comma-separated list of numerical columns to include in the scatter matrix.
    *   `--out [FILE_PATH]`: Path to save the plot.
    *   `--title [TITLE_STRING]`: Override the default chart title.
    *   `--grid / --no-grid`: Show or hide grid lines.
*   **Examples:**
    *   `wi plot matrix iris_dataset sepal_length,sepal_width,petal_length,petal_width --title "Iris Flower Feature Relationships"`
    *   `wi plot matrix financial_metrics revenue,profit,expenses --out financial_pairs.html`

### 9. `save`
**Purpose:** Provides instructions on how to save generated charts to a file.
*   **Detailed Description:** This command itself does not save a plot but serves as a help utility. It explains that saving plots is achieved by using the `--out` option available on each specific plot generation command (e.g., `bar`, `hist`, `scatter`). Plots can typically be saved in various formats like HTML or PNG.
*   **Scenarios:**
    *   *Documentation:* Including plots in reports or presentations.
    *   *Sharing Results:* Distributing visualizations to colleagues.
*   **Usage:** `wi plot save`
*   **Examples:**
    *   `wi plot save`
    *   (Refer to individual plot commands for `--out` usage, e.g., `wi plot line data date value --out my_line_chart.png`)

### 10. `theme`
**Purpose:** Lists available plot themes and allows setting a persistent theme for visualizations.
*   **Detailed Description:** This command enables users to customize the aesthetic appearance of their plots. It can display a list of all built-in themes from the `plotext` library. A selected theme can be set as the default, and it will persist across Wrought Iron sessions.
*   **Scenarios:**
    *   *Personalization:* Setting a preferred look for all generated plots.
    *   *Accessibility:* Choosing themes with better contrast for users with visual impairments.
    *   *Branding:* Aligning plot aesthetics with organizational style guidelines.
*   **Usage:** `wi plot theme [THEME_NAME]`
*   **Arguments:**
    *   `THEME_NAME`: (Optional) The name of the theme to set. If omitted, lists all available themes.
*   **Examples:**
    *   `wi plot theme` (List all available themes)
    *   `wi plot theme dark` (Set the plot theme to 'dark')
    *   `wi plot theme matrix` (Set the plot theme to 'matrix')

---

## Module 6: Data Wrangling (`wi clean`)

**Responsibility:** Repair, harmonize, and validate messy data.

The Data Wrangling module offers a comprehensive set of tools designed to clean, standardize, and transform raw, messy data into a usable format. In air-gapped environments, where external data sources might be unreliable or inconsistent, these commands are crucial for ensuring data quality and preparing datasets for analysis, modeling, and reporting. From handling missing values and duplicates to standardizing text and validating against schemas, this module helps you achieve a clean and reliable dataset.

### 1. `impute-mode`
**Purpose:** Fills missing (NaN) values in a specified column with its most frequent value (mode).
*   **Detailed Description:** This is a basic but effective imputation strategy for handling missing categorical or numerical data. By replacing `NaN`s with the mode, you maintain the column's most common value, which can be suitable for variables where the mode is a strong central tendency indicator. This operation modifies the table in-place.
*   **Scenarios:**
    *   *Missing Categorical Data:* Filling in missing `city` or `product_category` entries with the most common one.
    *   *Simple Numerical Imputation:* For discrete numerical columns where the mode is representative.
*   **Usage:** `wi clean impute-mode [TABLE_NAME] [COLUMN_NAME]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `COLUMN_NAME`: The name of the column where `NaN` values will be imputed.
*   **Examples:**
    *   `wi clean impute-mode customer_data preferred_contact_method`
    *   `wi clean impute-mode survey_responses q5_satisfaction_level`

### 2. `impute-group`
**Purpose:** Fills missing values in a target column based on the mode or statistics of specific groups defined by another column.
*   **Detailed Description:** This "cohort imputation" method is more sophisticated than simple mode imputation. Instead of using the global mode, it calculates the mode (or other statistics) for each subgroup and uses that to fill missing values within that specific group. This preserves variations across groups. An optional safety check can abort imputation if a group's standard deviation is too high, preventing imputation into highly variable groups.
*   **Scenarios:**
    *   *Regional Data:* Imputing missing `sales_target` for a specific `region` using the mode `sales_target` of that same region.
    *   *Product Lines:* Filling missing `material_cost` for `product_type A` with the mode `material_cost` of other `product_type A` items.
*   **Usage:** `wi clean impute-group [TABLE_NAME] [TARGET_COLUMN] [GROUP_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `TARGET_COLUMN`: The column with missing values to be imputed.
    *   `GROUP_COLUMN`: The column used to define the groups for imputation.
    *   `--std-max [FLOAT]`: (Optional) If the `TARGET_COLUMN` is numerical, imputation will be aborted for any group where the standard deviation of `TARGET_COLUMN` values exceeds this threshold. This prevents imputing into highly dispersed groups.
*   **Examples:**
    *   `wi clean impute-group employee_salaries bonus_pct department --std-max 0.1` (Impute bonus % by department, abort if department bonus % std dev > 0.1)
    *   `wi clean impute-group patient_records blood_type region` (Impute blood type by region)

### 3. `ml-impute`
**Purpose:** Performs K-Nearest Neighbors (KNN) imputation for missing numerical values, using Scikit-Learn.
*   **Detailed Description:** This advanced imputation method predicts missing values by considering the values of `k` nearest neighbors for a given row. It's particularly effective for numerical data where a relationship exists between columns, allowing for more intelligent filling of missing data than simple statistical methods. It requires the target column and other relevant columns to be numerical.
*   **Scenarios:**
    *   *Sensor Data:* Imputing missing `temperature` readings based on correlations with `humidity` and `pressure` readings from the same sensor.
    *   *Customer Demographics:* Filling missing `income` values based on similar customers' `age`, `education`, and `zip_code`.
*   **Usage:** `wi clean ml-impute [TABLE_NAME] [TARGET_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `TARGET_COLUMN`: The name of the numerical column with missing values to be imputed.
    *   `--neighbors [INT]`: The number of neighbors to consider for imputation (Default: `5`).
    *   `--weights [uniform|distance]`: The weighting strategy for neighbors.
        *   `uniform`: All neighbors are weighted equally.
        *   `distance`: Neighbors are weighted by the inverse of their distance.
*   **Examples:**
    *   `wi clean ml-impute sales_leads conversion_rate --neighbors 3`
    *   `wi clean ml-impute housing_prices square_footage --weights distance`

### 4. `dedupe`
**Purpose:** Interactively identifies and resolves fuzzy duplicates in a specified text column.
*   **Detailed Description:** This command helps clean up inconsistencies in text data, such as misspelled names or variations in company names. It uses fuzzy matching algorithms (`rapidfuzz`) to find similar entries. When duplicates are found within a configurable threshold, it initiates an interactive session, allowing the user to select a canonical value and merge all similar entries to that standard.
*   **Scenarios:**
    *   *CRM Cleaning:* Standardizing `customer_name` entries like "Google Inc.", "Google", "Google Inc".
    *   *Inventory Management:* Deduplicating `product_description` entries that have minor variations.
    *   *Research Data:* Harmonizing author names or organizational affiliations in text fields.
*   **Usage:** `wi clean dedupe [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `COLUMN_NAME`: The name of the text column to check for fuzzy duplicates.
    *   `--threshold [INT]`: A similarity score (0-100) below which strings are considered distinct (Default: `90`).
    *   `--interactive / --no-interactive`: If `--no-interactive` is set, the tool will find duplicates but will not prompt for merges (Default: `interactive`).
*   **Examples:**
    *   `wi clean dedupe contacts company_name --threshold 85`
    *   `wi clean dedupe product_catalog item_description --no-interactive` (Finds duplicates but does not merge)

### 5. `harmonize`
**Purpose:** Automatically clusters similar text variations in a column and standardizes them to a single canonical form.
*   **Detailed Description:** This command is designed for automated standardization of text data. It identifies groups of text entries that are similar (above a given threshold) and replaces all entries in each group with a single, chosen canonical value (currently, the first encountered value from the most frequent cluster). This is a non-interactive version of deduplication, suitable for large-scale, automated cleaning.
*   **Scenarios:**
    *   *Country Codes:* Standardizing "USA", "U.S.A", "United States" to "USA".
    *   *Job Titles:* Harmonizing "Software Engineer", "Software Dev", "Dev (Software)" to a consistent "Software Engineer".
*   **Usage:** `wi clean harmonize [TABLE_NAME] [COLUMN_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `COLUMN_NAME`: The name of the text column to harmonize.
    *   `--threshold [INT]`: A similarity score (0-100) for clustering similar strings (Default: `90`).
*   **Examples:**
    *   `wi clean harmonize product_reviews sentiment_keywords --threshold 95`
    *   `wi clean harmonize geo_locations city_name`

### 6. `regex-replace`
**Purpose:** Performs advanced string substitution in a column using regular expressions.
*   **Detailed Description:** This command provides fine-grained control over text transformation. It finds patterns matching a given regular expression in a specified text column and replaces them with a new string. This is extremely powerful for extracting, cleaning, or reformatting complex text data.
*   **Scenarios:**
    *   *Phone Number Cleaning:* Removing all non-numeric characters from phone numbers.
    *   *Extracting Information:* Pulling out specific data (e.g., ID numbers) embedded within longer text strings.
    *   *URL Standardization:* Removing query parameters or specific subdomains from URLs.
*   **Usage:** `wi clean regex-replace [TABLE_NAME] [COLUMN_NAME] [REGEX_PATTERN] [REPLACEMENT_STRING]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `COLUMN_NAME`: The name of the text column to apply replacements to.
    *   `REGEX_PATTERN`: The regular expression pattern to search for.
    *   `REPLACEMENT_STRING`: The string to replace matched patterns with. Can include backreferences (e.g., `\1`).
*   **Examples:**
    *   `wi clean regex-replace user_contacts phone_number "\D" ""` (Removes all non-digits from 'phone_number')
    *   `wi clean regex-replace event_logs message "ERROR: (\d+)" "Error Code: \1"` (Extracts error codes)

### 7. `drop-outliers`
**Purpose:** Identifies and nullifies numerical values that exceed a specified standard deviation threshold from the mean.
*   **Detailed Description:** This command helps manage extreme values (outliers) in numerical data by replacing them with `NULL`. It's based on the common statistical practice of considering values beyond a certain number of standard deviations from the mean as outliers. This can be crucial for robust statistical analysis or machine learning models sensitive to extreme values.
*   **Scenarios:**
    *   *Sensor Data Cleaning:* Nullifying erroneous sensor readings that are significantly outside the expected range.
    *   *Financial Data:* Handling extreme spikes or drops in transaction amounts.
    *   *Data Quality Control:* Ensuring data conforms to expected statistical properties.
*   **Usage:** `wi clean drop-outliers [TABLE_NAME] [NUMERICAL_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `NUMERICAL_COLUMN`: The name of the numerical column to check for outliers.
    *   `--sigma [FLOAT]`: The standard deviation threshold. Values more than `N` standard deviations from the mean will be nullified (Default: `3.0`).
*   **Examples:**
    *   `wi clean drop-outliers experiment_results measurement --sigma 2.5`
    *   `wi clean drop-outliers network_logs latency_ms --sigma 4.0`

### 8. `map`
**Purpose:** Applies a dictionary-based mapping to a column, typically loaded from a CSV file.
*   **Detailed Description:** This command allows you to standardize or translate values in a column using an external mapping file. The mapping file should be a CSV with two columns: the original value (key) and the new value (value). This is highly flexible for complex standardization tasks.
*   **Scenarios:**
    *   *Code Translation:* Converting internal status codes (e.g., "1", "2") to human-readable labels ("Pending", "Completed").
    *   *Unit Conversion:* Mapping different units of measurement (e.g., "kg", "lb") to a single standard unit.
    *   *Abbreviation Expansion:* Replacing "USA" with "United States" based on a lookup table.
*   **Usage:** `wi clean map [TABLE_NAME] [COLUMN_NAME] [MAPPING_FILE_PATH]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `COLUMN_NAME`: The name of the column to apply the mapping to.
    *   `MAPPING_FILE_PATH`: The path to a CSV file (without header) where the first column contains the old values and the second column contains the new values.
*   **Examples:**
    *   `wi clean map product_data old_category_id category_mapping.csv`
    *   `wi clean map survey_responses province_code province_names.csv`

### 9. `trim`
**Purpose:** Removes leading and trailing whitespace from values in a specified text column.
*   **Detailed Description:** Whitespace is a common culprit for data inconsistencies, especially in text fields. This command provides a simple yet effective way to normalize text data by removing any spaces, tabs, or newlines from the beginning and end of strings. This helps ensure accurate comparisons and merges.
*   **Scenarios:**
    *   *Text Field Normalization:* Cleaning `user_input` fields that might have accidental leading/trailing spaces.
    *   *Data Consistency:* Ensuring `product_name` or `city` entries are consistent before joining tables.
*   **Usage:** `wi clean trim [TABLE_NAME] [COLUMN_NAME]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to modify.
    *   `COLUMN_NAME`: The name of the text column to trim.
*   **Examples:**
    *   `wi clean trim customer_details customer_name`
    *   `wi clean trim log_entries event_description`

### 10. `validate-schema`
**Purpose:** Checks data in a table against a user-defined JSON schema, reporting any violations.
*   **Detailed Description:** This command allows for formal data validation, ensuring that the data types and structure of your table conform to a predefined schema. The schema is defined in a JSON file, specifying expected data types for each column. This is a crucial step for maintaining data quality, especially when integrating data from various sources.
*   **Scenarios:**
    *   *Data Ingestion Validation:* Ensuring incoming data matches the expected schema before it's used in critical processes.
    *   *Data Quality Monitoring:* Regularly checking a table's compliance with data standards.
    *   *API Data Validation:* Verifying that data received from an external API conforms to its documentation.
*   **Usage:** `wi clean validate-schema [TABLE_NAME] [RULES_FILE_PATH]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to validate.
    *   `RULES_FILE_PATH`: The path to a JSON file defining the schema rules. The JSON structure should map column names to their expected types (e.g., `{"column_a": "int", "column_b": "str"}`).
*   **Examples:**
    *   `wi clean validate-schema user_profiles user_schema.json`
    *   `wi clean validate-schema transaction_records transaction_rules.json`

---

## Module 7: Geospatial Analysis (`wi geo`)

**Responsibility:** Offline geocoding and spatial calculations.

The Geospatial Analysis module provides a suite of tools for working with location-based data within the air-gapped environment. It enables tasks such as validating coordinates, performing offline geocoding (address to Lat/Lon) and reverse geocoding (Lat/Lon to address) using local lookup files, calculating distances, clustering spatial data, and exporting to standard geospatial formats like GeoJSON. This module is critical for operations requiring location intelligence without external network access.

### 1. `validate`
**Purpose:** Validates whether latitude and longitude coordinates fall within their globally accepted bounds.
*   **Detailed Description:** This command checks if all latitude values are between -90 and 90 degrees, and all longitude values are between -180 and 180 degrees. It's a fundamental data quality check for any geospatial dataset, helping to identify malformed or erroneous coordinate entries. The command will report any invalid coordinates found.
*   **Scenarios:**
    *   *Data Ingestion Validation:* Ensuring incoming location data adheres to geographic standards.
    *   *Error Detection:* Pinpointing typos or incorrect sensor readings in geospatial fields.
    *   *Preprocessing:* Cleaning coordinate data before performing spatial calculations.
*   **Usage:** `wi geo validate [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the coordinates.
    *   `LATITUDE_COLUMN`: The column containing latitude values.
    *   `LONGITUDE_COLUMN`: The column containing longitude values.
*   **Examples:**
    *   `wi geo validate sensor_locations lat lon`
    *   `wi geo validate incident_reports incident_lat incident_lon`

### 2. `geocode`
**Purpose:** Converts human-readable addresses into latitude and longitude coordinates using a local lookup file.
*   **Detailed Description:** This command performs offline geocoding. Instead of relying on external API services, it matches addresses from a specified table column against a local CSV file that contains pre-geocoded addresses and their corresponding coordinates. This is essential for air-gapped environments where external API access is restricted. New latitude and longitude columns are added to the table.
*   **Scenarios:**
    *   *Address Standardization:* Converting a list of facility addresses to coordinates for internal mapping.
    *   *Legacy Data:* Geocoding old address records using an internal, pre-built address database.
    *   *Field Operations:* Enabling location analysis on addresses without internet connectivity.
*   **Usage:** `wi geo geocode [TABLE_NAME] [ADDRESS_COLUMN] [LOOKUP_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to add coordinates to.
    *   `ADDRESS_COLUMN`: The column in `TABLE_NAME` containing the addresses to be geocoded.
    *   `LOOKUP_FILE_PATH`: Path to a local CSV file. This file must contain columns named `address`, `lat`, and `lon`.
    *   `--new-lat-col [COLUMN_NAME]`: Name for the new latitude column (Default: `latitude`).
    *   `--new-lon-col [COLUMN_NAME]`: Name for the new longitude column (Default: `longitude`).
*   **Examples:**
    *   `wi geo geocode client_locations full_address master_addresses.csv`
    *   `wi geo geocode delivery_manifest pickup_address local_geocode_db.csv --new-lat-col p_lat --new-lon-col p_lon`

### 3. `reverse`
**Purpose:** Converts latitude and longitude coordinates into human-readable addresses using a local lookup file.
*   **Detailed Description:** This command performs offline reverse geocoding. It takes latitude and longitude pairs from a specified table and attempts to match them against a local CSV file containing coordinates and their corresponding addresses. This allows for identifying locations by textual descriptions within an air-gapped system. A new address column is added to the table.
*   **Scenarios:**
    *   *Event Location Identification:* Translating sensor coordinates into known facility names or street addresses.
    *   *Forensic Analysis:* Identifying the real-world locations corresponding to GPS traces.
    *   *Data Enrichment:* Adding address context to datasets that only contain coordinates.
*   **Usage:** `wi geo reverse [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN] [LOOKUP_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to add address information to.
    *   `LATITUDE_COLUMN`: The column in `TABLE_NAME` containing latitude values.
    *   `LONGITUDE_COLUMN`: The column in `TABLE_NAME` containing longitude values.
    *   `LOOKUP_FILE_PATH`: Path to a local CSV file. This file must contain columns named `lat`, `lon`, and `address`.
    *   `--new-addr-col [COLUMN_NAME]`: Name for the new address column (Default: `address`).
*   **Examples:**
    *   `wi geo reverse asset_tracking current_lat current_lon geo_db.csv`
    *   `wi geo reverse environmental_sensors reading_lat reading_lon named_points.csv --new-addr-col sensor_location_name`

### 4. `distance`
**Purpose:** Calculates the Haversine distance (great-circle distance) in kilometers between each point in a table and a specified target point.
*   **Detailed Description:** The Haversine formula is used to calculate the shortest distance between two points on a sphere (like Earth) given their longitudes and latitudes. This command adds a new column to your table containing the calculated distance in kilometers for each record to a fixed target `(latitude, longitude)` pair.
*   **Scenarios:**
    *   *Proximity Analysis:* Finding all records within a certain radius of a key location.
    *   *Logistics Optimization:* Calculating travel distances from a depot to various delivery points.
    *   *Emergency Response:* Determining the distance of incidents from a command center.
*   **Usage:** `wi geo distance [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN] --target-lat [LAT] --target-lon [LON] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the points.
    *   `LATITUDE_COLUMN`: The column containing latitude values.
    *   `LONGITUDE_COLUMN`: The column containing longitude values.
    *   `--target-lat [FLOAT]`: The latitude of the target point.
    *   `--target-lon [FLOAT]`: The longitude of the target point.
    *   `--new-dist-col [COLUMN_NAME]`: Name for the new column to store the calculated distances (Default: `distance_km`).
*   **Examples:**
    *   `wi geo distance field_agents agent_lat agent_lon --target-lat 34.0522 --target-lon -118.2437` (Distance to Los Angeles)
    *   `wi geo distance sensor_deployments deployment_lat deployment_lon --target-lat 0.0 --target-lon 0.0 --new-dist-col dist_from_equator_prime_meridian`

### 5. `cluster`
**Purpose:** Groups geospatial records into clusters based on their density using the DBSCAN algorithm.
*   **Detailed Description:** DBSCAN (Density-Based Spatial Clustering of Applications with Noise) identifies clusters of varying shapes in spatial data by finding areas of high density separated by areas of lower density. It's particularly effective at finding arbitrarily shaped clusters and identifying "noise" points that don't belong to any cluster. The command adds a `cluster_id` column to your table, with noise points typically labeled `-1`.
*   **Scenarios:**
    *   *Event Hotspot Detection:* Identifying areas with a high concentration of incidents, crimes, or sensor anomalies.
    *   *Urban Planning:* Finding natural groupings of population or infrastructure.
    *   *Resource Deployment:* Optimizing the placement of resources based on existing activity clusters.
*   **Usage:** `wi geo cluster [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the points.
    *   `LATITUDE_COLUMN`: The column containing latitude values.
    *   `LONGITUDE_COLUMN`: The column containing longitude values.
    *   `--eps [FLOAT]`: The maximum distance (in kilometers) between two samples for one to be considered as in the neighborhood of the other (Default: `0.5`).
    *   `--min-samples [INT]`: The number of samples (or total weight) in a neighborhood for a point to be considered as a core point (Default: `5`).
    *   `--metric [euclidean|haversine]`: The distance metric to use. `haversine` is recommended for geographical coordinates (Default: `euclidean`).
    *   `--algorithm [auto|ball_tree|kd_tree|brute]`: Algorithm used to compute nearest neighbors (Default: `auto`).
    *   `--n-jobs [INT]`: Number of parallel jobs to run for neighbors search (-1 means use all available CPU cores) (Default: `-1`).
    *   `--out-col [COLUMN_NAME]`: Name for the new column to store cluster IDs (Default: `cluster_id`).
    *   `--noise-label [VALUE]`: Value to assign to noise points (Default: `-1`).
    *   `--dry-run`: If set, performs the clustering and displays cluster counts but does not save changes to the database.
*   **Examples:**
    *   `wi geo cluster event_locations event_lat event_lon --eps 0.1 --min-samples 10 --metric haversine`
    *   `wi geo cluster customer_visits visit_lat visit_lon --eps 1.0 --noise-label "OUTLIER" --dry-run`

### 6. `centroid`
**Purpose:** Calculates the geometric centroid (mean latitude and longitude) of all points in a dataset.
*   **Detailed Description:** This command finds the average geographic center of all points defined by the latitude and longitude columns. It provides a single representative coordinate for the entire dataset, useful for understanding the overall spatial distribution.
*   **Scenarios:**
    *   *Central Location Identification:* Finding the average location of a fleet, sensor network, or customer base.
    *   *Reference Point:* Establishing a central reference point for further spatial analysis.
*   **Usage:** `wi geo centroid [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the points.
    *   `LATITUDE_COLUMN`: The column containing latitude values.
    *   `LONGITUDE_COLUMN`: The column containing longitude values.
*   **Examples:**
    *   `wi geo centroid asset_positions current_lat current_lon`
    *   `wi geo centroid sales_outlets outlet_lat outlet_lon`

### 7. `bounds`
**Purpose:** Determines the geographical bounding box (minimum/maximum latitude and longitude) for all points in a dataset.
*   **Detailed Description:** This command calculates the extreme northern, southern, eastern, and western boundaries of your geospatial data. This defines the smallest rectangle that encompasses all your points, which is useful for setting map extents, filtering data, or understanding the spatial coverage of a dataset.
*   **Scenarios:**
    *   *Map Framing:* Determining the appropriate zoom level and center for displaying data on a map.
    *   *Data Filtering:* Quickly selecting data points that fall within a specific geographical area.
    *   *Spatial Coverage Assessment:* Understanding the extent of a survey or sensor deployment area.
*   **Usage:** `wi geo bounds [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the points.
    *   `LATITUDE_COLUMN`: The column containing latitude values.
    *   `LONGITUDE_COLUMN`: The column containing longitude values.
*   **Examples:**
    *   `wi geo bounds flight_paths current_lat current_lon`
    *   `wi geo bounds environmental_samples sample_lat sample_lon`

### 8. `heatmap`
**Purpose:** Renders an ASCII-based density heatmap of geographical points directly in the terminal.
*   **Detailed Description:** This command visualizes the density distribution of points across a geographical area using ASCII characters. It effectively creates a 2D histogram of your latitude and longitude coordinates, allowing you to quickly spot areas of high concentration (hotspots) without requiring a graphical map interface. The resolution of the heatmap can be adjusted with the `bins` parameter.
*   **Scenarios:**
    *   *Quick Hotspot Identification:* Rapidly seeing where events or data points are most concentrated.
    *   *Terminal-Based Analysis:* Performing spatial density analysis in environments without GUI access.
    *   *Initial Data Exploration:* Getting a visual feel for the spatial patterns in a dataset.
*   **Usage:** `wi geo heatmap [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the points.
    *   `LATITUDE_COLUMN`: The column containing latitude values.
    *   `LONGITUDE_COLUMN`: The column containing longitude values.
    *   `--bins [INT]`: The number of bins to use for the heatmap grid along each dimension (Default: `20`). Higher values mean finer resolution.
*   **Examples:**
    *   `wi geo heatmap sensor_readings lat lon --bins 30`
    *   `wi geo heatmap asset_deployments deploy_lat deploy_lon`

### 9. `nearest`
**Purpose:** Finds the `k` nearest Points of Interest (POIs) for each record in a table from a separate POI lookup file.
*   **Detailed Description:** This command performs a spatial nearest-neighbor search. For each record in your primary table, it identifies the `k` closest points from a secondary CSV file containing POI data (ID, latitude, longitude). It adds new columns to your table, storing the IDs of the nearest POIs and their corresponding Haversine distances in kilometers.
*   **Scenarios:**
    *   *Customer Proximity:* Finding the nearest store, hospital, or service center for each customer.
    *   *Asset Allocation:* Determining which maintenance depot is closest to each asset in the field.
    *   *Spatial Matching:* Linking events to the closest known geographical features.
*   **Usage:** `wi geo nearest [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN] [POI_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table for which to find nearest POIs.
    *   `LATITUDE_COLUMN`: The column containing latitude values in `TABLE_NAME`.
    *   `LONGITUDE_COLUMN`: The column containing longitude values in `TABLE_NAME`.
    *   `POI_FILE_PATH`: Path to a CSV file containing Points of Interest. This file must contain columns named `id`, `lat`, and `lon`.
    *   `--k [INT]`: The number of nearest POIs to find for each record (Default: `1`).
    *   `--out-col-id [COLUMN_NAME]`: Name for the new column(s) to store the nearest POI ID(s) (Default: `nearest_poi_id`).
    *   `--out-col-dist [COLUMN_NAME]`: Name for the new column(s) to store the nearest POI distance(s) in km (Default: `nearest_poi_dist_km`).
*   **Examples:**
    *   `wi geo nearest clients client_lat client_lon hospitals.csv --k 1 --out-col-id nearest_hospital_id`
    *   `wi geo nearest incidents event_lat event_lon police_stations.csv --k 3 --out-col-id top3_police --out-col-dist top3_police_dist`

### 10. `export-geojson`
**Purpose:** Exports geospatial data from a table into the standard GeoJSON format.
*   **Detailed Description:** GeoJSON is a widely used format for encoding a variety of geographic data structures. This command converts the latitude and longitude columns of your table into GeoJSON `Point` features. You can optionally include an `id` column and other data columns as `properties` within each GeoJSON feature, making the output compatible with most GIS software and web mapping libraries.
*   **Scenarios:**
    *   *GIS Integration:* Preparing data for import into geographical information systems.
    *   *Web Mapping:* Generating data for interactive maps in web applications.
    *   *Standardized Data Exchange:* Sharing geospatial data in a universally recognized format.
*   **Usage:** `wi geo export-geojson [TABLE_NAME] [LATITUDE_COLUMN] [LONGITUDE_COLUMN] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to export.
    *   `LATITUDE_COLUMN`: The column containing latitude values.
    *   `LONGITUDE_COLUMN`: The column containing longitude values.
    *   `--id-col [COLUMN_NAME]`: Optional. A column from the table to use as the `id` property for each GeoJSON feature.
    *   `--properties "[COMMA_SEPARATED_COLUMNS]"`: Optional. A comma-separated list of columns whose values should be included as properties within each GeoJSON feature.
    *   `--output-file [FILE_NAME]`: The name of the output GeoJSON file (Default: `output.geojson`).
*   **Examples:**
    *   `wi geo export-geojson checkpoints point_lat point_lon --output-file my_points.geojson`
    *   `wi geo export-geojson sensor_locations lat lon --id-col sensor_id --properties "type,status" --output-file sensors.geojson`

---

## Module 8: Machine Learning (`wi ml`)

**Responsibility:** Predictive modeling, classification, regression, clustering, and anomaly detection.

The Machine Learning module brings powerful analytical capabilities directly into your air-gapped environment. Leveraging a subset of `scikit-learn` functionalities, it enables users to train, evaluate, and apply various machine learning models for tasks such as classification, regression, and unsupervised learning (clustering, anomaly detection). This module is essential for extracting predictive insights and patterns from your data without ever exposing it to external networks.

### 1. `train-classifier`
**Purpose:** Trains a classification model to predict a categorical target variable.
*   **Detailed Description:** This command allows you to build supervised learning models to categorize data. You can choose between popular algorithms like Random Forest and Logistic Regression. The model is trained on a portion of your data (train set) and then automatically saved to a specified path for future predictions or evaluation. It also provides an immediate accuracy score on a held-out test set.
*   **Scenarios:**
    *   *Customer Churn Prediction:* Training a model to predict which customers are likely to churn.
    *   *Fraud Detection:* Building a classifier to identify fraudulent transactions.
    *   *Image Classification (Feature-based):* Categorizing images based on extracted numerical features.
*   **Usage:** `wi ml train-classifier [TABLE_NAME] [TARGET_COLUMN] [FEATURE_COLUMNS] --output-model [PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the training data.
    *   `TARGET_COLUMN`: The name of the categorical column to predict (e.g., `churned`, `is_fraud`).
    *   `FEATURE_COLUMNS`: A comma-separated list of columns to use as features for training.
    *   `--output-model [PATH]`: The file path where the trained model (pickled object) will be saved.
    *   `--model-type [random_forest|logistic_regression]`: The type of classification model to train (Default: `random_forest`).
    *   `--test-size [FLOAT]`: The proportion of the dataset to include in the test split (0.0-1.0) (Default: `0.2`).
    *   `--random-state [INT]`: Controls the randomness of the data splitting and model initialization for reproducibility.
    *   `--n-estimators [INT]`: (Random Forest only) The number of trees in the forest (Default: `100`).
    *   `--max-depth [INT]`: (Random Forest only) The maximum depth of the tree. If `None`, nodes are expanded until all leaves are pure (Default: `None`).
    *   `--solver [STR]`: (Logistic Regression only) Algorithm to use in the optimization problem (e.g., `lbfgs`, `liblinear`) (Default: `lbfgs`).
    *   `--max-iter [INT]`: (Logistic Regression only) Maximum number of iterations taken for the solvers to converge (Default: `100`).
*   **Examples:**
    *   `wi ml train-classifier customer_data churned age,income,num_products --output-model churn_rf_model.pkl`
    *   `wi ml train-classifier email_spam is_spam num_words,num_links --model-type logistic_regression --output-model spam_lr_model.pkl --random-state 42`

### 2. `train-regressor`
**Purpose:** Trains a regression model to predict a continuous numerical target variable.
*   **Detailed Description:** This command allows you to build supervised learning models to estimate or predict continuous values. Options include Linear Regression, Ridge Regression (with L2 regularization), and Random Forest Regressor. The trained model is saved, and an R2 score on a test set is provided as an immediate evaluation.
*   **Scenarios:**
    *   *House Price Prediction:* Predicting the price of a house based on its features (size, location, number of rooms).
    *   *Sales Forecasting:* Estimating future sales volumes based on historical data and marketing spend.
    *   *System Load Prediction:* Forecasting server load based on usage patterns.
*   **Usage:** `wi ml train-regressor [TABLE_NAME] [TARGET_COLUMN] [FEATURE_COLUMNS] --output-model [PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the training data.
    *   `TARGET_COLUMN`: The name of the numerical column to predict (e.g., `house_price`, `sales_amount`).
    *   `FEATURE_COLUMNS`: A comma-separated list of columns to use as features for training.
    *   `--output-model [PATH]`: The file path where the trained model (pickled object) will be saved.
    *   `--model-type [linear_regression|ridge|random_forest]`: The type of regression model to train (Default: `linear_regression`).
    *   `--test-size [FLOAT]`: The proportion of the dataset to include in the test split (0.0-1.0) (Default: `0.2`).
    *   `--random-state [INT]`: Controls the randomness of the data splitting and model initialization for reproducibility.
    *   `--alpha [FLOAT]`: (Ridge Regression only) Regularization strength; must be a positive float (Default: `1.0`).
*   **Examples:**
    *   `wi ml train-regressor real_estate_data price sq_ft,bedrooms,bathrooms --output-model house_price_lr.pkl`
    *   `wi ml train-regressor sales_history revenue marketing_spend,promotions --model-type random_forest --output-model sales_rf_model.pkl`

### 3. `predict`
**Purpose:** Applies a previously trained and saved machine learning model to generate predictions on new or existing data.
*   **Detailed Description:** This command deserializes a pickled model file and uses it to make predictions for a specified output column in your database table. For classification models, you can optionally provide a `threshold` to convert probabilities into binary class labels. This allows you to operationalize your trained models and populate prediction results directly into your database.
*   **Scenarios:**
    *   *Real-time Scoring (Batch):* Applying a churn model to a new batch of customer data to identify at-risk customers.
    *   *Data Enrichment:* Adding predicted categories (e.g., `customer_segment`) or values (e.g., `estimated_value`) to existing records.
    *   *Filling Missing Values (Advanced):* Using a model to predict and fill `NaN`s in a target column based on other features.
*   **Usage:** `wi ml predict [TABLE_NAME] [MODEL_PATH] [OUTPUT_COLUMN] [FEATURE_COLUMNS] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table where predictions will be added.
    *   `MODEL_PATH`: The file path to the previously saved trained model (e.g., `churn_rf_model.pkl`).
    *   `OUTPUT_COLUMN`: The name of the new column to store the prediction results.
    *   `FEATURE_COLUMNS`: A comma-separated list of columns from `TABLE_NAME` that correspond to the features the model was trained on.
    *   `--threshold [FLOAT]`: (Optional, for classification models that output probabilities) A probability threshold (0.0-1.0) to convert probabilities into binary class labels.
*   **Examples:**
    *   `wi ml predict customer_profiles churn_rf_model.pkl predicted_churn_status age,income,num_products --threshold 0.5`
    *   `wi ml predict new_houses house_price_lr.pkl estimated_value sq_ft,bedrooms,bathrooms`

### 4. `score`
**Purpose:** Evaluates the performance of a trained machine learning model using various metrics.
*   **Detailed Description:** This command helps you understand how well your model is performing on a given dataset. It loads a trained model, generates predictions, and then calculates and displays specified evaluation metrics by comparing the model's predictions against the actual target values. Supported metrics include accuracy, R2 score, Mean Absolute Error (MAE), Mean Squared Error (MSE), precision, recall, and F1-score.
*   **Scenarios:**
    *   *Model Validation:* Assessing a model's performance on a dedicated validation or test set.
    *   *Model Comparison:* Comparing different models or hyperparameters based on their metric scores.
    *   *Performance Monitoring:* Periodically checking if a deployed model's performance is degrading.
*   **Usage:** `wi ml score [TABLE_NAME] [MODEL_PATH] [TARGET_COLUMN] [FEATURE_COLUMNS] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the data for scoring (including actual target and features).
    *   `MODEL_PATH`: The file path to the trained model.
    *   `TARGET_COLUMN`: The name of the column containing the actual target values.
    *   `FEATURE_COLUMNS`: A comma-separated list of columns from `TABLE_NAME` used as features.
    *   `--metrics "[COMMA_SEPARATED_METRICS]"`: A comma-separated list of evaluation metrics to calculate (e.g., `accuracy`, `r2`, `mae`, `precision`, `recall`, `f1`). (Default: `accuracy`).
*   **Examples:**
    *   `wi ml score test_customer_data churn_rf_model.pkl churned age,income,num_products --metrics "accuracy,precision,recall,f1"`
    *   `wi ml score validation_houses house_price_lr.pkl price sq_ft,bedrooms --metrics "r2,mae,mse"`

### 5. `feature-importance`
**Purpose:** Analyzes a trained model to determine the relative importance of each feature in making predictions.
*   **Detailed Description:** This command provides insights into model interpretability by quantifying how much each input feature contributes to the model's output. For tree-based models (like Random Forest), it leverages `feature_importances_`. For linear models (like Logistic Regression, Linear Regression, Ridge), it typically uses the absolute values of the coefficients. Understanding feature importance can help refine models, reduce dimensionality, and explain predictions.
*   **Scenarios:**
    *   *Model Interpretability:* Understanding which factors are most influential in a predictive model.
    *   *Feature Selection:* Identifying redundant or uninformative features that could be removed to simplify the model or reduce data collection efforts.
    *   *Business Insights:* Gaining knowledge about the underlying drivers of a business outcome.
*   **Usage:** `wi ml feature-importance [MODEL_PATH] [OPTIONS]`
*   **Arguments:**
    *   `MODEL_PATH`: The file path to the trained model.
    *   `--top-n [INT]`: The number of top features to display (Default: `10`).
*   **Examples:**
    *   `wi ml feature-importance churn_rf_model.pkl`
    *   `wi ml feature-importance house_price_lr.pkl --top-n 5`

### 6. `save-model`
**Purpose:** (Informational) This command primarily explains how models are saved. Direct saving occurs via `--output-model` in training commands.
*   **Detailed Description:** While there's no direct `save-model` operation in `wi ml` to save an *already loaded* model, this entry serves to clarify that models are automatically saved to disk when using the `--output-model` argument during `train-classifier` or `train-regressor` commands. For custom Python scripts, standard `pickle` operations are used for serialization.
*   **Scenarios:**
    *   *Understanding Model Persistence:* Clarifying how models are made persistent.
*   **Usage:** `wi ml save-model`
*   **Arguments:** (None, informational)
*   **Examples:** (Refer to `train-classifier` and `train-regressor` examples)

### 7. `load-model`
**Purpose:** Loads a previously saved machine learning model from a `.pkl` file into memory.
*   **Detailed Description:** This command deserializes a pickled model file from disk, making the trained model available for inspection or subsequent operations (though directly using it for prediction is usually done via `wi ml predict`). It verifies the existence of the model file and reports its type upon successful loading.
*   **Scenarios:**
    *   *Model Inspection:* Loading a model to examine its internal structure (if done programmatically).
    *   *Preparation for Scripting:* Loading a model within a larger Python script that might use `wrought_iron` components.
*   **Usage:** `wi ml load-model [MODEL_PATH]`
*   **Arguments:**
    *   `MODEL_PATH`: The file path to the trained model (`.pkl` file).
*   **Examples:**
    *   `wi ml load-model churn_rf_model.pkl`

### 8. `cluster-kmeans`
**Purpose:** Performs K-Means clustering, an unsupervised learning algorithm, to group data points into `k` distinct clusters.
*   **Detailed Description:** K-Means partitions data into `k` clusters based on similarity, aiming to minimize variance within each cluster. This command adds a `cluster_id` column to your table, assigning each row to its respective cluster. It's a fundamental technique for data segmentation and pattern discovery without a predefined target variable.
*   **Scenarios:**
    *   *Customer Segmentation:* Grouping customers into distinct segments based on purchasing behavior or demographics.
    *   *Document Clustering:* Organizing a collection of documents into thematic groups.
    *   *Anomaly Detection (as a precursor):* Identifying small, isolated clusters that might represent anomalies.
*   **Usage:** `wi ml cluster-kmeans [TABLE_NAME] [FEATURE_COLUMNS] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to cluster.
    *   `FEATURE_COLUMNS`: A comma-separated list of columns to use for clustering.
    *   `--k [INT]`: The number of clusters to form (Default: `3`).
    *   `--init [k-means++|random]`: Method for initialization of centroids (Default: `k-means++`).
    *   `--n-init [INT]`: Number of times the k-means algorithm is run with different centroid seeds (Default: `10`).
    *   `--max-iter [INT]`: Maximum number of iterations for the K-Means algorithm (Default: `300`).
    *   `--random-state [INT]`: Controls the randomness of centroid initialization for reproducibility.
    *   `--output-col [COLUMN_NAME]`: Name of the new column to store the assigned cluster IDs (Default: `cluster_id`).
*   **Examples:**
    *   `wi ml cluster-kmeans customer_data age,income,spending --k 4 --output-col customer_segment`
    *   `wi ml cluster-kmeans sensor_readings temp,humidity --k 5 --init random --random-state 10`

### 9. `detect-anomalies`
**Purpose:** Flags statistical outliers or anomalies in a dataset using the Isolation Forest algorithm.
*   **Detailed Description:** Isolation Forest is an unsupervised anomaly detection algorithm that works by explicitly isolating anomalies instead of profiling normal data points. It is particularly effective for high-dimensional datasets. This command adds an `anomaly_score` column to your table, where lower scores typically indicate a higher likelihood of being an anomaly.
*   **Scenarios:**
    *   *Intrusion Detection:* Identifying unusual network traffic patterns.
    *   *Manufacturing Quality Control:* Detecting defective products that deviate significantly from the norm.
    *   *Fraud Detection:* Flagging unusual spending behaviors.
*   **Usage:** `wi ml detect-anomalies [TABLE_NAME] [FEATURE_COLUMNS] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to scan for anomalies.
    *   `FEATURE_COLUMNS`: A comma-separated list of columns to use for anomaly detection.
    *   `--n-estimators [INT]`: The number of base estimators (trees) in the forest (Default: `100`).
    *   `--max-samples [auto|INT|FLOAT]`: The number of samples to draw from X to train each base estimator. Can be `auto`, an integer, or a float (Default: `auto`).
    *   `--contamination [auto|FLOAT]`: The proportion of outliers in the dataset. Used to define the threshold for anomaly scores (Default: `auto`).
    *   `--random-state [INT]`: Controls the randomness for reproducibility.
    *   `--output-col [COLUMN_NAME]`: Name of the new column to store the anomaly scores (Default: `anomaly_score`).
*   **Examples:**
    *   `wi ml detect-anomalies server_logs cpu_usage,memory_usage,disk_io --contamination 0.01`
    *   `wi ml detect-anomalies transaction_data amount,frequency --output-col fraud_likelihood --random-state 42`

### 10. `split`
**Purpose:** Splits a table into training and testing subsets, optionally with stratification.
*   **Detailed Description:** This fundamental machine learning utility divides your dataset into two distinct parts: a training set (used to train models) and a testing set (used to evaluate models). This helps prevent overfitting and provides an unbiased evaluation of model performance. You can control the size of the splits, ensure reproducibility with a random state, and use stratification to maintain class proportions across the splits for classification tasks.
*   **Scenarios:**
    *   *Model Development Workflow:* Preparing data for any supervised machine learning task.
    *   *Cross-validation Preparation:* Creating distinct datasets for robust model testing.
    *   *Fair Evaluation:* Ensuring that model performance metrics are not biased by training on the same data used for testing.
*   **Usage:** `wi ml split [TABLE_NAME] [OUTPUT_TRAIN_TABLE_NAME] [OUTPUT_TEST_TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the original table to split.
    *   `OUTPUT_TRAIN_TABLE_NAME`: The name for the new table containing the training data.
    *   `OUTPUT_TEST_TABLE_NAME`: The name for the new table containing the testing data.
    *   `--train-size [FLOAT]`: The proportion of the dataset to include in the training split (0.0-1.0) (Default: `0.75`).
    *   `--random-state [INT]`: Controls the randomness of the splitting process for reproducibility.
    *   `--stratify-col [COLUMN_NAME]`: (Optional) The name of a categorical column to use for stratified sampling. This ensures that the proportion of classes in this column is roughly the same in both training and testing sets. Crucial for imbalanced classification problems.
*   **Examples:**
    *   `wi ml split customer_data customer_train customer_test --train-size 0.8 --random-state 42`
    *   `wi ml split churn_data churn_train churn_test --train-size 0.7 --stratify-col churned`

---

## Module 9: Audit & Security (`wi audit`)

**Responsibility:** Forensics, integrity verification, data protection, and chain-of-custody.

The Audit & Security module is paramount in Wrought Iron's design for air-gapped, high-security environments. It provides tools to ensure data integrity, track changes, protect sensitive information, and establish a verifiable chain of custody. From cryptographic hashing to PII scanning and robust encryption, this module helps maintain compliance, detect tampering, and safeguard critical data assets.

### 1. `log-view`
**Purpose:** Displays entries from Wrought Iron's internal audit log, tracking system activities and data modifications.
*   **Detailed Description:** Wrought Iron maintains an immutable audit log (`_wi_audit_log_` table) within each database to record significant actions, user activities, and system events. This command provides a structured view of this log, allowing users to review who did what, when, and to which data, with filtering options for focused investigations.
*   **Scenarios:**
    *   *Forensic Investigation:* Tracing the history of changes made to a critical dataset.
    *   *Compliance Audit:* Demonstrating adherence to data governance policies by reviewing activity logs.
    *   *Troubleshooting:* Identifying recent operations that might have led to an unexpected database state.
*   **Usage:** `wi audit log-view [OPTIONS]`
*   **Arguments:**
    *   `--limit [INT]`: The maximum number of recent log entries to display (Default: `50`).
    *   `--user [USERNAME]`: Filters log entries to show actions performed by a specific user.
    *   `--action [COMMAND_NAME]`: Filters log entries to show activities related to a specific Wrought Iron command (e.g., `connect file`, `ml train-classifier`).
*   **Examples:**
    *   `wi audit log-view` (Show the 50 most recent audit log entries)
    *   `wi audit log-view --user "admin" --action "connect"` (View connection events by 'admin')
    *   `wi audit log-view --limit 100 --action "clean"` (Show the last 100 data cleaning operations)

### 2. `hash-create`
**Purpose:** Generates a cryptographic integrity fingerprint (hash) of a table's data and optionally its schema.
*   **Detailed Description:** This command computes a cryptographically secure hash (SHA256 or SHA512) of a specified table's content, allowing for robust integrity verification. To ensure reproducibility and guard against subtle changes, it sorts data and can exclude specific columns (like timestamps). An optional salt can be added for increased security. This hash represents the exact state of the data at the time of creation.
*   **Scenarios:**
    *   *Data Integrity Verification:* Creating a baseline hash after a critical data import.
    *   *Tamper Detection:* Generating a hash periodically to detect unauthorized modifications.
    *   *Secure Data Transfer:* Providing a verifiable fingerprint of data sent to another party.
*   **Usage:** `wi audit hash-create [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to generate a hash for.
    *   `--algo [sha256|sha512]`: The hashing algorithm to use (Default: `sha256`).
    *   `--salt [STRING]`: An optional string to add to the data before hashing, enhancing security and preventing rainbow table attacks.
    *   `--exclude-cols "[COMMA_SEPARATED_COLUMNS]"`: A comma-separated list of columns to exclude from the hashing process (e.g., automatically generated timestamps, audit columns not relevant for data content integrity).
    *   `--chunk-size [INT]`: The number of rows to process in each chunk, optimizing memory usage for very large tables (Default: `10000`).
*   **Examples:**
    *   `wi audit hash-create financial_records --salt "mysecretkey"`
    *   `wi audit hash-create sensor_data --exclude-cols "last_updated"`
    *   `wi audit hash-create critical_config --algo sha512`

### 3. `hash-verify`
**Purpose:** Verifies the integrity of a table by comparing its current cryptographic hash against an expected hash.
*   **Detailed Description:** This command re-computes the hash of a specified table, using the same parameters (`algo`, `salt`, `exclude-cols`) as `hash-create`, and then compares the calculated hash to a provided `expected_hash`. It reports whether the data's integrity has been verified or if tampering is detected. A `strict` mode can also hash the schema definition itself. Reports can be generated in PDF format.
*   **Scenarios:**
    *   *Post-Transfer Validation:* Verifying that a transferred database file has not been corrupted or tampered with.
    *   *Routine Integrity Checks:* Scheduled checks to ensure the ongoing integrity of critical datasets.
    *   *Incident Response:* Confirming if data in question has been altered.
*   **Usage:** `wi audit hash-verify [TABLE_NAME] [EXPECTED_HASH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to verify.
    *   `EXPECTED_HASH`: The cryptographic hash string that the table's current state is expected to match.
    *   `--salt [STRING]`: The same salt string used during hash creation.
    *   `--exclude-cols "[COMMA_SEPARATED_COLUMNS]"`: The same list of excluded columns used during hash creation.
    *   `--chunk-size [INT]`: The same chunk size used during hash creation (Default: `10000`).
    *   `--strict`: If set, the table's schema definition (DDL) is also included in the hashing process, providing a more stringent integrity check.
    *   `--report-format [pdf]`: If set to `pdf`, generates a PDF integrity report detailing the verification outcome.
    *   `--signer-key [PATH]`: (For report generation) Path to a private key file for digital signing of the PDF report (future implementation/placeholder).
*   **Examples:**
    *   `wi audit hash-verify financial_records "a1b2c3d4..." --salt "mysecretkey"`
    *   `wi audit hash-verify data_export "x9y8z7..." --exclude-cols "export_timestamp" --report-format pdf`

### 4. `export-cert`
**Purpose:** Generates a Chain-of-Custody PDF certificate for documented actions.
*   **Detailed Description:** This command creates a basic PDF document that serves as a digital certificate for auditing purposes. It records details like the generation timestamp, the signer's name, and the source database. While currently a placeholder for a more complex signing process, it provides a structured document for formalizing audit trails.
*   **Scenarios:**
    *   *Legal Compliance:* Documenting the handling and processing of sensitive data for legal or regulatory requirements.
    *   *Evidence Preservation:* Creating a formal record of data access and integrity checks for digital forensics.
    *   *Operational Documentation:* Formalizing the completion of critical data processing steps.
*   **Usage:** `wi audit export-cert --signer [NAME] --output [FILE_PATH]`
*   **Arguments:**
    *   `--signer [NAME]`: The name of the individual or entity signing the certificate.
    *   `--output [FILE_PATH]`: The file path where the PDF certificate will be saved.
*   **Examples:**
    *   `wi audit export-cert --signer "John Doe, Lead Analyst" --output audit_certificate_2024.pdf`

### 5. `snapshot`
**Purpose:** Creates a named, internal backup (snapshot) of a table's current state within the same database file.
*   **Detailed Description:** This command provides a lightweight versioning capability. It duplicates a specified table, including its data and schema, into a new, hidden table (prefixed with `_snapshot_`) and records metadata about the snapshot. This allows you to revert to a previous state without external backup files.
*   **Scenarios:**
    *   *Pre-Modification Backup:* Creating a checkpoint before performing a risky data transformation or schema change.
    *   *A/B Testing Baselines:* Saving different versions of a dataset for comparative analysis.
    *   *Data Recovery:* Providing an immediate recovery point in case of accidental data loss or corruption.
*   **Usage:** `wi audit snapshot [TABLE_NAME] --name [SNAPSHOT_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to create a snapshot of.
    *   `--name [SNAPSHOT_NAME]`: A unique identifier for this snapshot.
    *   `--comment [STRING]`: An optional descriptive comment for the snapshot.
*   **Examples:**
    *   `wi audit snapshot customer_data --name "pre_cleanup_backup" --comment "Backup before running clean module"`
    *   `wi audit snapshot financial_transactions --name "EOD_20240315"`

### 6. `rollback`
**Purpose:** Restores a table to a previously saved snapshot state.
*   **Detailed Description:** This command reverts a table to a specific state captured by a snapshot. It effectively replaces the current version of the table with the data and schema from the chosen snapshot. A dry-run option is available to preview the impact of the rollback before executing it.
*   **Scenarios:**
    *   *Undo Changes:* Reverting a table to a known good state after an erroneous operation.
    *   *Version Control:* Switching between different versions of a dataset for analysis or testing.
    *   *Disaster Recovery:* Recovering a table from a local backup in case of data integrity issues.
*   **Usage:** `wi audit rollback [TABLE_NAME] [SNAPSHOT_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to restore.
    *   `SNAPSHOT_NAME`: The unique identifier of the snapshot to restore from.
    *   `--dry-run`: If set, the command will only preview the changes (e.g., row count differences) that would occur during the rollback, without actually modifying the table.
*   **Examples:**
    *   `wi audit rollback customer_data pre_cleanup_backup`
    *   `wi audit rollback sales_forecast "v1.1_model_output" --dry-run`

### 7. `scan-pii`
**Purpose:** Scans text columns in a table to detect the presence of Personally Identifiable Information (PII) using advanced NLP techniques (Presidio) or basic regex.
*   **Detailed Description:** This command helps identify sensitive data like phone numbers, email addresses, and credit card numbers embedded within free-text fields. It attempts to use the Presidio library for more robust detection but falls back to simpler regex patterns if Presidio is unavailable. Findings are reported with column, row index, entity type, and the detected snippet, helping users pinpoint and address PII leakage.
*   **Scenarios:**
    *   *Data Privacy Compliance:* Identifying where PII might be stored to ensure GDPR, CCPA, or other regulations are met.
    *   *Security Audits:* Discovering unintended PII exposure in logs or unstructured data.
    *   *Data Masking Preparation:* Locating fields that need anonymization or redaction.
*   **Usage:** `wi audit scan-pii [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to scan for PII.
    *   `--entities "[COMMA_SEPARATED_ENTITIES]"`: A comma-separated list of specific PII entity types to scan for (e.g., `PHONE_NUMBER`, `EMAIL_ADDRESS`, `CREDIT_CARD`). If omitted, all supported entities will be scanned.
    *   `--confidence [FLOAT]`: A confidence score threshold (0.0-1.0). Only PII detections above this confidence will be reported (Default: `0.5`).
*   **Examples:**
    *   `wi audit scan-pii customer_feedback_notes`
    *   `wi audit scan-pii log_files --entities "EMAIL_ADDRESS,PHONE_NUMBER" --confidence 0.8`

### 8. `encrypt-col`
**Purpose:** Encrypts the data within a specified column using strong symmetric encryption (Fernet).
*   **Detailed Description:** This command provides column-level encryption for sensitive data. It uses Fernet (AES-128 in CBC mode with HMAC SHA256) for authenticated encryption. A key file is used to store the encryption key; if the file doesn't exist, a new key is generated and saved. Once encrypted, the data in the column becomes unreadable without the correct key, protecting it at rest.
*   **Scenarios:**
    *   *Sensitive Data Protection:* Encrypting `credit_card_numbers` or `national_ID_numbers` within a database.
    *   *Data Sharing Security:* Ensuring that specific columns remain confidential even if the database file is accessed by unauthorized parties.
    *   *Compliance Requirements:* Meeting regulatory requirements for securing specific data elements.
*   **Usage:** `wi audit encrypt-col [TABLE_NAME] [COLUMN_NAME] --key-file [KEY_FILE_PATH]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the column to encrypt.
    *   `COLUMN_NAME`: The name of the column whose data will be encrypted.
    *   `--key-file [FILE_PATH]`: The path to the encryption key file. A new key will be generated and saved here if the file does not exist.
*   **Examples:**
    *   `wi audit encrypt-col user_profiles ssn --key-file my_ssn_key.key`
    *   `wi audit encrypt-col payment_info card_number --key-file payment_encryption.key`

### 9. `decrypt-col`
**Purpose:** Decrypts data in a specified column that was previously encrypted using Fernet.
*   **Detailed Description:** This command reverses the `encrypt-col` operation. It requires the same key file that was used for encryption to successfully decrypt the column's data. If the key is incorrect or the data is not encrypted, it attempts to return the original value. This allows authorized users to access the protected information when needed.
*   **Scenarios:**
    *   *Authorized Access:* Decrypting data for analysis by authorized personnel.
    *   *Data Migration:* Decrypting data before moving it to another secure system.
    *   *Reporting:* Temporarily decrypting data to generate reports with full information.
*   **Usage:** `wi audit decrypt-col [TABLE_NAME] [COLUMN_NAME] --key-file [KEY_FILE_PATH]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the column to decrypt.
    *   `COLUMN_NAME`: The name of the column whose data will be decrypted.
    *   `--key-file [FILE_PATH]`: The path to the encryption key file that was used for encryption.
*   **Examples:**
    *   `wi audit decrypt-col user_profiles ssn --key-file my_ssn_key.key`
    *   `wi audit decrypt-col payment_info card_number --key-file payment_encryption.key`

### 10. `anonymize`
**Purpose:** Anonymizes data in a specified column using masking, hashing, or redaction techniques.
*   **Detailed Description:** This command helps reduce the identifiability of individuals in your dataset, crucial for privacy and compliance. It offers three methods: `mask` (replaces parts of the data with characters like `*`), `hash` (replaces data with a one-way cryptographic hash), and `redact` (replaces data with a generic placeholder like `[REDACTED]`).
*   **Scenarios:**
    *   *Data Sharing (Limited):* Preparing a dataset for sharing with third parties by removing direct identifiers.
    *   *Testing Environments:* Creating anonymized versions of production data for development and testing.
    *   *Privacy by Design:* Implementing built-in anonymization for certain data elements.
*   **Usage:** `wi audit anonymize [TABLE_NAME] [COLUMN_NAME] --method [mask|hash|redact] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the column to anonymize.
    *   `COLUMN_NAME`: The name of the column to anonymize.
    *   `--method [mask|hash|redact]`: The anonymization technique to apply (Default: `mask`).
        *   `mask`: Replaces a portion of the string with asterisks (e.g., `*****1234`).
        *   `hash`: Replaces the original value with its SHA256 hash.
        *   `redact`: Replaces the original value with `[REDACTED]`.
    *   `--chars [INT]`: (Only for `mask` method) The number of characters from the beginning of the string to mask with asterisks (Default: `4`).
*   **Examples:**
    *   `wi audit anonymize customer_contact email --method hash`
    *   `wi audit anonymize user_logs ip_address --method redact`
    *   `wi audit anonymize employee_details phone_number --method mask --chars 6`

---

## Module 10: Operations (`wi ops`)

**Responsibility:** Automation, maintenance, workflow orchestration, and operational monitoring.

The Operations module provides the tools necessary to manage the lifecycle of data processes, ensuring reliability, performance, and adherence to operational standards within an air-gapped environment. It includes functionalities for scheduling tasks, executing multi-step pipelines, monitoring job history, detecting data drift, and maintaining database health. This module helps automate routine tasks and provides visibility into the operational state of your data foundry.

### 1. `schedule create`
**Purpose:** Registers a Wrought Iron command to be executed automatically by an internal scheduler based on a cron expression.
*   **Detailed Description:** This command allows you to set up recurring tasks, such as daily data backups, weekly integrity checks, or hourly report generation. You define the Wrought Iron command to run, assign a name to the job, and specify a cron expression for its execution frequency. Optional parameters include timeouts, retry policies, and failure notifications.
*   **Scenarios:**
    *   *Automated Backups:* Scheduling `wi connect vacuum --into backup.db` nightly.
    *   *Regular Data Cleaning:* Running `wi clean impute-mode sales_data product_category` every morning.
    *   *Scheduled Reports:* Generating `wi report generate daily_summary.html` daily.
*   **Usage:** `wi ops schedule create [COMMAND_TO_RUN] --name [JOB_NAME] --cron "[CRON_EXPRESSION]" [OPTIONS]`
*   **Arguments:**
    *   `COMMAND_TO_RUN`: The full Wrought Iron command string to be executed (e.g., `"wi connect vacuum"`).
    *   `--name [STRING]`: A unique name for the scheduled job.
    *   `--cron "[CRON_EXPRESSION]"`: A standard cron expression defining the schedule (e.g., `"0 3 * * *"` for 3 AM daily).
    *   `--timeout [INT]`: Maximum execution time in seconds for the command before it's terminated (Default: `3600`).
    *   `--retry [INT]`: Number of times the job should be retried if it fails (Default: `0`).
    *   `--on-fail-email [EMAIL]`: An email address to notify upon job failure.
    *   `--cpu-limit [STRING]`: (Future/Placeholder) A string representing CPU usage limits (e.g., `"50%"`).
    *   `--log-level [STRING]`: Log verbosity for the task's execution (e.g., `INFO`, `DEBUG`) (Default: `INFO`).
*   **Examples:**
    *   `wi ops schedule create "wi connect vacuum --into nightly_backup.db" --name "nightly_db_backup" --cron "0 2 * * *"`
    *   `wi ops schedule create "wi ml train-classifier customer_data churned features --output-model churn_model.pkl" --name "retrain_churn_model" --cron "0 1 * * 0" --on-fail-email "devops@example.com"`

### 2. `schedule list`
**Purpose:** Displays a list of all currently registered automated tasks, along with their status and scheduling details.
*   **Detailed Description:** This command provides an overview of your automated operations. It lists the job ID, name, cron expression, current status (active, paused), and the command associated with each scheduled task, helping you monitor and manage your automation.
*   **Scenarios:**
    *   *Monitoring Automation:* Quickly checking what tasks are scheduled and their status.
    *   *Troubleshooting:* Identifying which cron jobs are active or might be causing issues.
    *   *Audit Review:* Confirming that critical automated processes are in place.
*   **Usage:** `wi ops schedule list [OPTIONS]`
*   **Arguments:**
    *   `--status [active|paused]`: Filters the list to show only tasks with a specific status.
*   **Examples:**
    *   `wi ops schedule list` (List all scheduled tasks)
    *   `wi ops schedule list --status active` (List only currently active tasks)

### 3. `schedule delete`
**Purpose:** Removes a scheduled task from the internal scheduler.
*   **Detailed Description:** This command allows you to deactivate and permanently remove a previously scheduled job using its unique task ID. A confirmation prompt is provided to prevent accidental deletion, which can be bypassed with a force flag.
*   **Scenarios:**
    *   *Disabling Obsolete Tasks:* Removing scheduled jobs that are no longer needed.
    *   *Correcting Errors:* Deleting a misconfigured task before it runs again.
    *   *Maintenance Mode:* Temporarily pausing tasks by deleting and re-creating them, or by implementing a pause feature if available.
*   **Usage:** `wi ops schedule delete [TASK_ID] [OPTIONS]`
*   **Arguments:**
    *   `TASK_ID`: The unique ID of the task to be deleted. This ID can be found using `wi ops schedule list`.
    *   `--force`: Skips the confirmation prompt and immediately deletes the task. **Use with caution.**
*   **Examples:**
    *   `wi ops schedule delete 123` (Delete task with ID 123 after confirmation)
    *   `wi ops schedule delete 456 --force` (Force delete task with ID 456 without confirmation)

### 4. `pipeline run`
**Purpose:** Executes a multi-step workflow defined in a YAML configuration file.
*   **Detailed Description:** This command enables the orchestration of complex data processing workflows. You define a sequence of Wrought Iron commands or shell commands in a YAML file, and `pipeline run` executes them sequentially. It supports conditional execution (e.g., `continue-on-error`) to manage failures within the pipeline.
*   **Scenarios:**
    *   *ETL Workflows:* Defining a pipeline for data extraction, cleaning, transformation, and loading.
    *   *ML Model Retraining:* Automating the steps for data preprocessing, model training, evaluation, and deployment.
    *   *Reporting Automation:* Chaining commands to generate reports from multiple data sources.
*   **Usage:** `wi ops pipeline run [YAML_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `YAML_FILE_PATH`: The path to the YAML file defining the workflow steps.
    *   `--continue-on-error`: If set, the pipeline will continue executing subsequent steps even if an earlier step fails. Otherwise, the pipeline will halt on the first error.
*   **Examples:**
    *   `wi ops pipeline run daily_etl.yaml`
    *   `wi ops pipeline run ml_retrain_workflow.yaml --continue-on-error`

### 5. `trigger`
**Purpose:** Forcefully executes a previously scheduled task immediately, regardless of its defined cron schedule.
*   **Detailed Description:** This command allows for manual, on-demand execution of a task registered with `schedule create`. It's useful for testing, debugging, or immediately running a task outside of its normal schedule. The execution is logged in the job history.
*   **Scenarios:**
    *   *Testing Scheduled Jobs:* Running a cron job manually to verify its functionality.
    *   *Emergency Execution:* Running a critical data refresh immediately when needed.
    *   *Ad-hoc Reporting:* Triggering a report generation pipeline outside its regular schedule.
*   **Usage:** `wi ops trigger [TASK_ID]`
*   **Arguments:**
    *   `TASK_ID`: The unique ID of the scheduled task to trigger.
*   **Examples:**
    *   `wi ops trigger 101` (Immediately run the task with ID 101)

### 6. `logs`
**Purpose:** Displays the execution history and status of automated tasks.
*   **Detailed Description:** This command provides a view of the `_wi_task_logs` table, which records each run of a scheduled task. It shows details such as start/end times, execution status (success, failed, timeout), and any error messages, providing crucial debugging information for automated workflows.
*   **Scenarios:**
    *   *Debugging Failed Jobs:* Quickly identifying why a scheduled task failed by reviewing its error logs.
    *   *Performance Monitoring:* Reviewing execution times of tasks to identify bottlenecks.
    *   *Operational Audit:* Verifying that automated tasks are running as expected.
*   **Usage:** `wi ops logs [OPTIONS]`
*   **Arguments:**
    *   `--job-id [INT]`: Filters the logs to show entries for a specific scheduled task ID.
    *   `--errors-only`: If set, only displays log entries for tasks that failed or timed out.
    *   `--limit [INT]`: The maximum number of log entries to display (Default: `20`).
*   **Examples:**
    *   `wi ops logs --job-id 101` (View logs for task ID 101)
    *   `wi ops logs --errors-only --limit 50` (Show the last 50 failed job runs)

### 7. `drift-check`
**Purpose:** Detects statistical data drift by comparing the distribution of numerical columns against a baseline snapshot.
*   **Detailed Description:** This command helps ensure data quality and consistency over time. It compares the current data distribution of numerical columns in a table against a previously created snapshot (baseline) using statistical tests like the Kolmogorov-Smirnov (KS) test. If the p-value of the test falls below a specified threshold, it indicates a significant change in distribution, suggesting data drift.
*   **Scenarios:**
    *   *Data Quality Monitoring:* Regularly checking if newly ingested data deviates significantly from historical patterns.
    *   *Machine Learning Model Monitoring:* Detecting changes in feature distributions that could impact model performance.
    *   *Data Governance:* Ensuring that data transformations or system updates do not inadvertently alter fundamental data characteristics.
*   **Usage:** `wi ops drift-check [TABLE_NAME] --baseline [SNAPSHOT_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the current table to check for drift.
    *   `--baseline [SNAPSHOT_NAME]`: The name of the snapshot (created with `wi audit snapshot`) to use as the baseline for comparison.
    *   `--threshold [FLOAT]`: The p-value threshold for the statistical test. A p-value below this threshold indicates significant drift (Default: `0.05`).
*   **Examples:**
    *   `wi ops drift-check sensor_data --baseline "post_calibration_snapshot"`
    *   `wi ops drift-check user_behavior --baseline "Q1_2023_baseline" --threshold 0.01`

### 8. `alert-config`
**Purpose:** Configures the email address for receiving failure alerts from automated tasks.
*   **Detailed Description:** This command sets up the destination for notifications when scheduled tasks or pipeline steps fail. By centralizing alert configurations, operations teams can quickly respond to issues and maintain system reliability.
*   **Scenarios:**
    *   *Incident Response:* Ensuring the right personnel are notified immediately when automated processes encounter errors.
    *   *System Monitoring:* Integrating Wrought Iron's operational alerts into existing monitoring systems.
*   **Usage:** `wi ops alert-config --email [EMAIL_ADDRESS]`
*   **Arguments:**
    *   `--email [EMAIL_ADDRESS]`: The email address to which failure alerts will be sent.
    *   `--log-file [PATH]`: (Future/Placeholder) Path to a specific log file for alerts.
*   **Examples:**
    *   `wi ops alert-config --email "ops_team@example.com"`

### 9. `maintenance`
**Purpose:** Performs various database optimization and maintenance tasks, including VACUUM, ANALYZE, and REINDEX.
*   **Detailed Description:** This command ensures the long-term performance and efficiency of your SQLite databases. `VACUUM` rebuilds the database to reclaim space and defragment, `ANALYZE` updates the database statistics used by the query optimizer, and `REINDEX` rebuilds indices. These operations are crucial for maintaining query speed and minimizing disk usage.
*   **Scenarios:**
    *   *Routine Database Care:* Scheduling weekly or monthly maintenance to prevent performance degradation.
    *   *Post-Mass-Update Optimization:* Running after large data imports or deletions to re-optimize the database.
*   **Usage:** `wi ops maintenance [OPTIONS]`
*   **Arguments:**
    *   `--reindex`: If set, all indices in the database will be rebuilt.
    *   `--analyze`: If set, the database statistics will be updated for the query optimizer.
*   **Examples:**
    *   `wi ops maintenance` (Performs VACUUM only)
    *   `wi ops maintenance --reindex --analyze` (Performs VACUUM, REINDEX, and ANALYZE)

### 10. `export-status`
**Purpose:** Exports a snapshot of the system's operational health and status in a structured format (JSON or XML).
*   **Detailed Description:** This command provides a programmatic way to retrieve key operational metrics, such as database size, number of scheduled tasks, active tasks, and a quick integrity check result. It's designed for integration with external monitoring systems or for quick programmatic health checks.
*   **Scenarios:**
    *   *System Monitoring Integration:* Feeding operational status into a dashboard or enterprise monitoring solution.
    *   *Automated Health Checks:* Including in scripts to programmatically check the status of Wrought Iron deployments.
    *   *Troubleshooting Diagnostics:* Getting a quick overview of the system's health.
*   **Usage:** `wi ops export-status [OPTIONS]`
*   **Arguments:**
    *   `--format [json|xml]`: The desired output format for the status information (Default: `json`).
*   **Examples:**
    *   `wi ops export-status`
    *   `wi ops export-status --format xml`

---

## Module 11: Collaboration (`wi collab`)

**Responsibility:** Knowledge sharing, configuration management, and project portability.

The Collaboration module facilitates teamwork and project management within Wrought Iron's air-gapped environment. It provides mechanisms for sharing reusable queries (views), documenting datasets with notes, exporting and importing configurations, and bundling entire database projects for transfer. This ensures consistency, reduces redundant work, and improves the overall efficiency of collaborative data analysis.

### 1. `view save`
**Purpose:** Saves a complex SQL query as a named "Virtual View" for later reuse.
*   **Detailed Description:** This command stores a SQL `SELECT` statement with an associated name and description. It also attempts to create a corresponding SQLite `VIEW` in the database, allowing the saved query to be treated like a regular table in subsequent operations. This promotes reusability and standardization of common data access patterns.
*   **Scenarios:**
    *   *Standardized Reports:* Defining a complex query for a frequently accessed report that can be easily rerun by name.
    *   *Data Abstraction:* Creating a simplified view of underlying tables for less technical users.
    *   *Complex Filter Preservation:* Saving a sophisticated filtering query that is too long or error-prone to retype.
*   **Usage:** `wi collab view save [VIEW_NAME] --query "[SQL_QUERY]" [OPTIONS]`
*   **Arguments:**
    *   `VIEW_NAME`: A unique name for the virtual view.
    *   `--query "[SQL_QUERY]"`: The full SQL `SELECT` statement to be saved as the view definition.
    *   `--desc "[DESCRIPTION_STRING]"`: An optional description for the view, explaining its purpose or logic.
*   **Examples:**
    *   `wi collab view save "active_customers" --query "SELECT * FROM customers WHERE status = 'active' AND last_purchase_date > DATE('now', '-1 year')"`
    *   `wi collab view save "high_value_transactions" --query "SELECT * FROM transactions WHERE amount > 1000 AND category = 'Electronics'" --desc "All transactions over $1000 in Electronics"`

### 2. `view list`
**Purpose:** Lists all previously saved Virtual Views in the active database.
*   **Detailed Description:** This command provides an overview of all defined virtual views, showing their names, descriptions, and creation timestamps. It allows users to quickly find and reference available saved queries.
*   **Scenarios:**
    *   *Discovering Available Views:* Checking what shared queries are available in a database.
    *   *Documentation Review:* Reviewing the purpose of existing views.
    *   *Management:* Identifying views that might need updating or deletion.
*   **Usage:** `wi collab view list [OPTIONS]`
*   **Arguments:**
    *   `--filter [STRING]`: Filters the list to show views whose names contain the specified string.
*   **Examples:**
    *   `wi collab view list` (List all saved views)
    *   `wi collab view list --filter "customer"` (List views related to customers)

### 3. `view load`
**Purpose:** Materializes a saved Virtual View into a new, physical table in the database.
*   **Detailed Description:** This command executes the SQL query associated with a saved virtual view and saves its results as a brand new, persistent table. This is useful for creating static copies of query results, optimizing performance for frequently accessed complex views, or preparing data for tools that require physical tables.
*   **Scenarios:**
    *   *Performance Optimization:* Creating a materialized view of a slow, complex query.
    *   *Data Export:* Generating a flat table from a view for export to another system.
    *   *Snapshotting Query Results:* Saving the results of a query at a specific point in time.
*   **Usage:** `wi collab view load [VIEW_NAME] --as-table [NEW_TABLE_NAME]`
*   **Arguments:**
    *   `VIEW_NAME`: The name of the virtual view to load.
    *   `--as-table [NEW_TABLE_NAME]`: The name for the new physical table that will be created from the view's results.
*   **Examples:**
    *   `wi collab view load "active_customers" --as-table "current_active_customer_snapshot"`
    *   `wi collab view load "daily_summary_report" --as-table "daily_report_20240320"`

### 4. `config export`
**Purpose:** Exports Wrought Iron's configuration settings (e.g., database aliases) to a JSON file.
*   **Detailed Description:** This command allows you to save your personalized Wrought Iron settings, such as named database aliases, to a human-readable JSON file. This is useful for backing up configurations, migrating settings between machines, or sharing standardized setups within a team. You can optionally include sensitive information like API keys if needed.
*   **Scenarios:**
    *   *Configuration Backup:* Creating a backup of important aliases and settings.
    *   *New User Setup:* Quickly configuring a new Wrought Iron instance for a team member.
    *   *System Migration:* Moving Wrought Iron and its associated settings to a different environment.
*   **Usage:** `wi collab config export [COMMAND_SCOPE|all] [FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `COMMAND_SCOPE|all`: Specifies which command's settings to export (e.g., `connect` for aliases, or `all` for all known settings).
    *   `FILE_PATH`: The path to the output JSON file where settings will be saved.
    *   `--include-secrets`: If set, any potentially sensitive settings (like API keys) will be included in the export. **Use with caution, as this exposes credentials.**
*   **Examples:**
    *   `wi collab config export all my_wi_config.json`
    *   `wi collab config export connect aliases.json`

### 5. `config import`
**Purpose:** Imports configuration settings from a JSON file into Wrought Iron.
*   **Detailed Description:** This command loads previously exported Wrought Iron settings from a JSON file, applying them to the current instance. It can update existing aliases and other configurations, streamlining setup and ensuring consistent environments.
*   **Scenarios:**
    *   *Restoring Configuration:* Loading a backed-up configuration after a system reset.
    *   *Team Standardization:* Distributing a common set of aliases and settings to multiple users.
    *   *Automated Deployment:* Integrating configuration loading into automated setup scripts.
*   **Usage:** `wi collab config import [FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `FILE_PATH`: The path to the JSON file containing the settings to import.
    *   `--scope [user|project]`: (Future/Placeholder) Specifies the scope for imported settings (user-specific or project-specific).
*   **Examples:**
    *   `wi collab config import shared_team_config.json`

### 6. `recipe bundle`
**Purpose:** Creates a zip archive containing the active database and its associated files (WAL, SHM) for easy transfer.
*   **Detailed Description:** This command packages your entire Wrought Iron project—specifically the active SQLite database file and its auxiliary Write-Ahead Log (WAL) and Shared Memory (SHM) files—into a single, portable `.zip` archive. This makes it simple to move a complete project between machines or share it with collaborators, ensuring all necessary components are included.
*   **Scenarios:**
    *   *Project Handover:* Bundling a project for a colleague or client.
    *   *Offline Transfer:* Moving a database via USB drive or other air-gapped method.
    *   *Archiving:* Creating a self-contained archive of a completed project.
*   **Usage:** `wi collab recipe bundle [BUNDLE_NAME] --out [OUTPUT_FILE_PATH]`
*   **Arguments:**
    *   `BUNDLE_NAME`: A name for the bundle (used for internal tracking or descriptive purposes).
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output `.zip` archive.
*   **Examples:**
    *   `wi collab recipe bundle "sales_analysis_2024" --out "sales_project.zip"`
    *   `wi collab recipe bundle "sensor_data_Q3" --out "sensor_data_archive.zip"`

### 7. `recipe install`
**Purpose:** Extracts a Wrought Iron project from a zip archive, placing the database files into the current directory.
*   **Detailed Description:** This command unzips a bundled Wrought Iron project archive (`.zip` file) and extracts the database (`.db`), WAL, and SHM files into the current working directory. It includes a safety check to prevent overwriting existing files, which can be bypassed with the `--overwrite` flag.
*   **Scenarios:**
    *   *Receiving a Project:* Setting up a Wrought Iron project that was shared by a colleague.
    *   *Restoring from Archive:* Deploying a previously archived project.
*   **Usage:** `wi collab recipe install [ZIP_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `ZIP_FILE_PATH`: The path to the `.zip` archive containing the bundled project.
    *   `--overwrite`: If set, existing files with the same names in the current directory will be overwritten. **Use with caution.**
*   **Examples:**
    *   `wi collab recipe install sales_project.zip`
    *   `wi collab recipe install sensor_data_archive.zip --overwrite`

### 8. `notes add`
**Purpose:** Adds a textual note or annotation to a specific table within the active database.
*   **Detailed Description:** This command allows users to attach metadata or free-form comments directly to tables. Notes are stored internally and can include an author and timestamp, providing a simple yet effective way to document data sources, known issues, or important context about a table.
*   **Scenarios:**
    *   *Data Lineage:* Documenting the source or transformation steps for a table.
    *   *Known Issues:* Adding a note about data quality problems or inconsistencies in a table.
    *   *Contextual Information:* Providing insights or warnings for future users of the table.
*   **Usage:** `wi collab notes add [TABLE_NAME] --msg "[NOTE_MESSAGE]" [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to annotate.
    *   `--msg "[NOTE_MESSAGE]"`: The text of the note to add.
    *   `--author [STRING]`: An optional name of the author for the note (Default: `user`).
*   **Examples:**
    *   `wi collab notes add raw_api_data --msg "Data imported directly from API, contains raw JSON blobs." --author "Data Eng Team"`
    *   `wi collab notes add clean_customers --msg "Nulls imputed using mode for 'country' and KNN for 'age'."`

### 9. `notes show`
**Purpose:** Displays all notes associated with a specified table.
*   **Detailed Description:** This command retrieves and presents all notes that have been added to a particular table, ordered by the most recent. It's a quick way to access documentation and contextual information embedded within your database.
*   **Scenarios:**
    *   *Understanding a Table:* Quickly getting up to speed on the history or characteristics of a table.
    *   *Auditing Changes:* Reviewing annotations made during data cleaning or transformation.
*   **Usage:** `wi collab notes show [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table whose notes are to be displayed.
    *   `--limit [INT]`: The maximum number of notes to display (Default: `10`).
*   **Examples:**
    *   `wi collab notes show raw_api_data`
    *   `wi collab notes show processed_transactions --limit 5`

### 10. `workspace dump`
**Purpose:** Exports the current Wrought Iron workspace state, including the active database and potentially other configuration files, into a portable archive.
*   **Detailed Description:** This command creates a comprehensive snapshot of your current working environment. It typically bundles the active database file (and its WAL/SHM), along with any relevant internal configuration settings, into a single zip file. This allows for full project backups or the ability to recreate an exact working state on another machine.
*   **Scenarios:**
    *   *Full Project Backup:* Creating a complete archive of your workspace before a major change or at project milestones.
    *   *Sharing Complete Environments:* Providing a fully configured and populated Wrought Iron environment to a collaborator.
    *   *Disaster Recovery:* Creating a recovery point for the entire working context.
*   **Usage:** `wi collab workspace dump [DUMP_COMMAND] [OPTIONS]`
*   **Arguments:**
    *   `DUMP_COMMAND`: The subcommand for workspace operations, currently only `dump`.
    *   `--full`: If set, attempts to include all possible relevant data, config files, and logs (future implementation/placeholder for more comprehensive exports).
*   **Examples:**
    *   `wi collab workspace dump dump`
    *   `wi collab workspace dump dump --full` (For a more extensive export)

---

## Module 12: Reporting (`wi report`)

**Responsibility:** Offline intelligence generation, data summarization, and interactive data visualization.

The Reporting module enables Wrought Iron users to generate comprehensive, human-readable reports and visualizations directly from their air-gapped data. It provides tools for automated Exploratory Data Analysis (EDA), comparison reports, schema documentation, audit timelines, and interactive maps, ensuring that critical insights can be extracted and shared without external connectivity. This module transforms raw data into actionable intelligence.

### 1. `generate`
**Purpose:** Generates a comprehensive Exploratory Data Analysis (EDA) report using Sweetviz, outputting an interactive HTML file.
*   **Detailed Description:** This command automates the process of generating detailed statistical and graphical insights into your dataset. Sweetviz reports provide visualizations of data distributions, associations, missing values, and unique values for each feature, offering a holistic view of data quality and characteristics. The output is an interactive HTML file that can be viewed in any web browser.
*   **Scenarios:**
    *   *Initial Data Assessment:* Rapidly understanding a new dataset's properties and potential issues.
    *   *Data Quality Review:* Identifying missing values, high cardinality columns, or skewed distributions.
    *   *Data Presentation:* Sharing a self-contained, interactive report of data characteristics with non-technical stakeholders.
*   **Usage:** `wi report generate [TABLE_NAME] --out [OUTPUT_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to analyze.
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output HTML report (e.g., `my_eda_report.html`).
    *   `--layout [vertical|widescreen]`: Specifies the layout of the generated report (Default: `widescreen`).
*   **Examples:**
    *   `wi report generate customer_demographics --out customer_eda.html`
    *   `wi report generate transaction_data --out transaction_analysis.html --layout vertical`

### 2. `diff`
**Purpose:** Generates a comparison (Before/After) report between two versions of a table (current vs. snapshot) using Sweetviz.
*   **Detailed Description:** This command is invaluable for understanding changes in data over time or between different processing stages. It leverages Sweetviz to compare the characteristics of the current table against a historical snapshot, highlighting differences in distributions, statistics, and missing values. The output is an interactive HTML report.
*   **Scenarios:**
    *   *Data Pipeline Validation:* Verifying the impact of data cleaning or transformation steps.
    *   *Time-Series Comparison:* Analyzing changes in a dataset between two specific points in time.
    *   *A/B Testing Analysis:* Comparing two versions of a dataset (e.g., control vs. experiment groups).
*   **Usage:** `wi report diff [TABLE_NAME] --snapshot [SNAPSHOT_NAME] --out [OUTPUT_FILE_PATH]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the current table for comparison.
    *   `--snapshot [SNAPSHOT_NAME]`: The name of a previously created snapshot (using `wi audit snapshot`) to compare against.
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output HTML comparison report (Default: `diff_report.html`).
*   **Examples:**
    *   `wi report diff customer_data --snapshot "pre_cleanup_backup" --out customer_data_diff.html`
    *   `wi report diff sales_metrics --snapshot "EOM_March" --out sales_month_over_month.html`

### 3. `validation`
**Purpose:** Generates a quality report based on data validation, typically presented as an EDA report.
*   **Detailed Description:** While the current implementation primarily generates a Sweetviz EDA report (similar to `generate`), this command is conceptually designed for a more formal quality assurance workflow. Future enhancements may integrate with explicit rule files (`--rules`) to highlight data points that violate predefined quality standards, thereby providing a focused report on data quality issues.
*   **Scenarios:**
    *   *Data Quality Assurance:* Providing a baseline quality assessment of a dataset.
    *   *Compliance Reporting:* Documenting data quality metrics against internal standards.
*   **Usage:** `wi report validation [TABLE_NAME] --out [OUTPUT_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to validate.
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output HTML validation report (Default: `validation_report.html`).
    *   `--rules [RULES_FILE_PATH]`: (Future/Placeholder) Path to a JSON rules file for explicit data quality checks.
*   **Examples:**
    *   `wi report validation product_catalog --out product_quality_report.html`

### 4. `schema-doc`
**Purpose:** Generates a static HTML data dictionary documenting the schema of all user tables in the active database.
*   **Detailed Description:** This command creates a self-contained HTML file listing all user-defined tables and their respective columns, including column names, data types, nullability, default values, and primary key status. It acts as a comprehensive reference guide for the database structure, crucial for understanding and navigating complex schemas.
*   **Scenarios:**
    *   *Database Documentation:* Creating up-to-date documentation for a database schema.
    *   *Developer Onboarding:* Providing new team members with a clear overview of the data model.
    *   *Data Governance:* Maintaining a consistent record of data definitions.
*   **Usage:** `wi report schema-doc --out [OUTPUT_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `--title [REPORT_TITLE]`: Custom title for the HTML data dictionary (Default: `Data Dictionary`).
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output HTML file (Default: `schema_doc.html`).
*   **Examples:**
    *   `wi report schema-doc --title "Customer Database Schema" --out customer_schema.html`

### 5. `audit-timeline`
**Purpose:** Generates a visual timeline (HTML) of significant audit log events related to data changes.
*   **Detailed Description:** This command creates an HTML report that visualizes the history of actions recorded in Wrought Iron's internal audit log. It helps in understanding the sequence of operations, identifying key milestones, and tracing data lineage. The timeline can be filtered by date ranges for focused analysis.
*   **Scenarios:**
    *   *Incident Response:* Quickly reviewing recent data modifications during a security incident.
    *   *Project Tracking:* Visualizing the progress of data processing or cleanup tasks.
    *   *Compliance Reporting:* Providing a chronological record of data handling activities.
*   **Usage:** `wi report audit-timeline --out [OUTPUT_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `--range-start [DATE]`: (Future/Placeholder) Start date for filtering log entries (e.g., `YYYY-MM-DD`).
    *   `--range-end [DATE]`: (Future/Placeholder) End date for filtering log entries.
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output HTML timeline report (Default: `audit_timeline.html`).
*   **Examples:**
    *   `wi report audit-timeline --out my_project_audit_log.html`

### 6. `map`
**Purpose:** Generates an interactive offline Leaflet.js HTML map from geographical coordinates in a table.
*   **Detailed Description:** This command creates a self-contained HTML file embedding an interactive map that displays data points from your table based on latitude and longitude columns. It uses Leaflet.js, a popular open-source JavaScript library for mobile-friendly interactive maps. The map operates completely offline once generated, making it ideal for air-gapped visualization of spatial data.
*   **Scenarios:**
    *   *Field Operations Mapping:* Visualizing sensor deployments or incident locations without internet access.
    *   *Geospatial Data Exploration:* Interactively exploring the spatial distribution of data points.
    *   *Presentation:* Embedding maps in reports or presentations for offline viewing.
*   **Usage:** `wi report map [TABLE_NAME] --lat [LATITUDE_COLUMN] --lon [LONGITUDE_COLUMN] --out [OUTPUT_FILE_PATH]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table containing the geographical coordinates.
    *   `--lat [LATITUDE_COLUMN]`: The column containing latitude values.
    *   `--lon [LONGITUDE_COLUMN]`: The column containing longitude values.
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output HTML map file (Default: `map.html`).
*   **Examples:**
    *   `wi report map sensor_deployments --lat deploy_lat --lon deploy_lon --out sensor_map.html`
    *   `wi report map incident_locations --lat event_latitude --lon event_longitude`

### 7. `summary`
**Purpose:** Generates a concise executive PDF summary report for a specified table.
*   **Detailed Description:** This command creates a basic PDF document providing key statistics and information about a table. It includes details like total rows, columns, and a summarized `describe()` output from Pandas. This is useful for quick, print-ready overviews of datasets.
*   **Scenarios:**
    *   *Executive Briefings:* Providing high-level data summaries to management.
    *   *Quick Reference:* Creating a printable summary of a dataset's basic characteristics.
    *   *Compliance Documentation:* Generating formal summary documents.
*   **Usage:** `wi report summary [TABLE_NAME] --out [OUTPUT_FILE_PATH] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to summarize.
    *   `--out [OUTPUT_FILE_PATH]`: The path and filename for the output PDF summary (Default: `summary.pdf`).
    *   `--include-charts`: (Future/Placeholder) If set, will attempt to include graphical summaries (e.g., histograms).
*   **Examples:**
    *   `wi report summary budget_allocations --out budget_summary.pdf`
    *   `wi report summary project_status --out project_overview.pdf`

### 8. `dependencies`
**Purpose:** Generates a text-based representation of data lineage and dependencies, particularly for views.
*   **Detailed Description:** This command attempts to infer and display relationships between database objects, focusing on which tables are used by which views. It helps users understand the data flow and impact of changes by showing a simplified Directed Acyclic Graph (DAG) of dependencies. While currently a text-based representation, it provides foundational insight into data lineage.
*   **Scenarios:**
    *   *Data Governance:* Documenting dependencies for critical data assets.
    *   *Impact Analysis:* Understanding which views might be affected by changes to underlying tables.
    *   *System Understanding:* Gaining insight into the structure and interconnections of the database.
*   **Usage:** `wi report dependencies [OPTIONS]`
*   **Arguments:**
    *   `--format [png]`: (Future/Placeholder) Output format for a graphical DAG (e.g., `png`).
*   **Examples:**
    *   `wi report dependencies` (Shows text-based dependencies in the console)

### 9. `profile`
**Purpose:** Dumps detailed statistical profiles of a table's columns as a JSON string.
*   **Detailed Description:** This command provides a machine-readable output of descriptive statistics for all columns in a table. It leverages Pandas' `describe()` method with an option to include all column types, making it suitable for programmatic consumption or integration into custom dashboards.
*   **Scenarios:**
    *   *Automated Data Quality Checks:* Integrating statistical profiles into automated scripts.
    *   *Custom Reporting:* Using the JSON output to generate tailored reports or visualizations.
    *   *Data Cataloging:* Storing programmatic summaries of datasets.
*   **Usage:** `wi report profile [TABLE_NAME] [OPTIONS]`
*   **Arguments:**
    *   `TABLE_NAME`: The name of the table to profile.
    *   `--minimal`: If set, excludes quantiles (25%, 50%, 75%) from the output for a more concise summary.
*   **Examples:**
    *   `wi report profile sensor_readings`
    *   `wi report profile customer_transactions --minimal`

### 10. `serve`
**Purpose:** Starts a simple local HTTP server to preview generated HTML reports in a web browser.
*   **Detailed Description:** Many Wrought Iron reports (e.g., EDA, Map) are generated as HTML files. This command provides a convenient way to view these reports in a web browser by serving the current directory (or a specified path) over a local HTTP server. This eliminates the need to manually open files and provides a consistent viewing environment.
*   **Scenarios:**
    *   *Interactive Review:* Easily viewing generated HTML reports like Sweetviz or Leaflet maps.
    *   *Collaboration (Local):* Showing reports to a colleague on the same machine.
*   **Usage:** `wi report serve [OPTIONS]`
*   **Arguments:**
    *   `--port [INT]`: The port number on which the server will listen (Default: `8000`).
    *   `--bind [IP_ADDRESS]`: The IP address to bind the server to (Default: `127.0.0.1` for localhost).
*   **Examples:**
    *   `wi report serve` (Starts server on port 8000)
    *   `wi report serve --port 8080`

---

## Module 13: The Command Center (`wi interact`)

**Responsibility:** Provides an interactive Terminal User Interface (TUI) for intuitive command execution, data exploration, and system monitoring.

The Command Center is Wrought Iron's graphical interface, designed to streamline complex operations and enhance user experience within the terminal. It consolidates various functionalities into an interactive environment, allowing users to browse databases, construct and execute commands dynamically, view data, and monitor system health without constantly typing CLI commands. This TUI makes Wrought Iron more accessible and efficient, especially for complex workflows and data exploration.

### 1. `interact`
**Purpose:** Launches the interactive Terminal User Interface (TUI) for Wrought Iron.
*   **Detailed Description:** This command initiates a full-screen, text-based interactive application that serves as a central hub for all Wrought Iron operations. The TUI provides several key sections:
    *   **Dashboard**: Offers a live overview of the active database, including file size, tables and their row counts, recent entries from the audit log, and system resource monitoring (CPU/RAM usage, active scheduled jobs).
    *   **Command Runner**: A dynamic interface where users can select any Wrought Iron module and command from dropdowns. Based on the selection, it generates input fields for positional arguments, options, and flags. Users can fill in these fields and execute the constructed command, with output displayed in a dedicated log panel.
    *   **Table Viewer**: Allows users to select any table or view from the Dashboard and instantly display its contents in a scrollable data table.
*   **Scenarios:**
    *   *Interactive Data Exploration:* Browsing table schemas and data content quickly.
    *   *Dynamic Command Building:* Constructing and testing complex commands with argument prompts instead of manual typing.
    *   *Real-time Monitoring:* Keeping an eye on database status, audit trails, and system resources.
    *   *Guided Workflow:* A more user-friendly way to navigate and execute Wrought Iron's extensive feature set.
*   **Usage:** `wi interact`
*   **Arguments:** (No direct arguments; all interactions are within the TUI)
*   **Examples:**
    *   `wi interact` (Launches the main TUI application)

#### **TUI Components:**

*   **Dashboard**
    *   **Active Database Information**: Shows the currently connected database file, its size, and path. Includes a "Refresh" button to update statistics.
    *   **Tables & Views**: A list of all user-defined tables and views in the active database, along with their row counts (for tables) and source database. Clicking on an entry opens the Table Viewer.
    *   **Recent Activity (Audit Log)**: Displays the most recent entries from the internal audit log, showing timestamps, users, and actions.
    *   **System Health**: Provides a real-time (simulated for now) view of system resources like CPU and RAM usage, status of active scheduled jobs, and a quick database integrity check.

*   **Command Runner**
    *   **Module Selection**: A dropdown to choose any top-level Wrought Iron module (e.g., `connect`, `schema`, `query`, `ml`).
    *   **Command Selection**: After a module is selected, a second dropdown populates with all commands available within that module.
    *   **Argument Builder**: Dynamically generated input fields, checkboxes, and dropdowns based on the selected command's required arguments and options. Includes help text for each argument.
    *   **Execute Button**: Runs the constructed Wrought Iron command as a subprocess.
    *   **Output Log**: Displays the standard output and standard error from the executed command, with syntax highlighting for readability.

*   **Table Viewer**
    *   A dedicated screen that appears when a table or view is selected from the Dashboard.
    *   Displays the full contents of the selected table in a scrollable `DataTable`.
    *   Allows for easy inspection of rows and columns.
    *   Can be closed to return to the main TUI dashboard.

