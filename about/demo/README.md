# Wrought Iron Demo Environment

This folder contains a generated sandbox for testing every module of the **Wrought Iron CLI**. 
Each subfolder corresponds to a module and contains realistic SQLite databases (`.db`) and configuration files tailored for specific commands.

## How to Use
1. Open your terminal.
2. Navigate to the specific module folder: `cd demo/module_XX_...`
3. Run the suggested commands below.

---

## Module 1: Infrastructure (`wi connect`)
**Folder:** `module_01_infrastructure`

*   **`source.db`**: Contains a `users` table with 50 records.
*   **`target.db`**: Contains an empty schema to test merging.
*   **`deep/nested/folder/deep_archive.db`**: A hidden DB to test aliasing.

**Test Commands:**
```bash
# 1. Connection & Info
wi connect file source.db
wi connect info --extended

# 2. Maintenance
wi connect vacuum --into vacuumed_source.db
wi connect integrity-check --quick

# 3. ETL (Merge source into target)
wi connect merge target.db source.db --strategy append

# 4. Aliasing (Shorten deep paths)
wi connect alias archive deep/nested/folder/deep_archive.db

# 5. Security (Encryption)
wi connect encrypt source.db --output encrypted.db
# (You will be prompted for a password)
```

---

## Module 2: Schema Management (`wi schema`)
**Folder:** `module_02_schema`

*   **`complex.db`**: Contains `orders`, `items` (FKs), `order_summary` (View), and `raw_json` (JSON column).
*   **`v1.db`** & **`v2.db`**: Two versions of a schema to test diffing.

**Test Commands:**
```bash
wi connect file complex.db

# 1. Introspection
wi schema list --show-views
wi schema describe orders --format sql
wi schema inspect orders --histogram

# 2. ER Diagram
wi schema graph --format mermaid

# 3. JSON Handling ("Backpack" Pattern)
wi schema detect-json raw_json
wi schema flatten raw_json payload --prefix user_ 

# 4. Schema Evolution
wi connect file v1.db
wi schema diff t1 t1 --db-b v2.db
```

---

## Module 3: Data Exploration (`wi query`)
**Folder:** `module_03_data_exploration`

*   **`exploration.db`**:
    *   `customers`: 5,000 rows for sampling/filtering.
    *   `logs`: Unstructured text messages for search.

**Test Commands:**
```bash
wi connect file exploration.db

# 1. Basic Retrieval
wi query head customers -n 5
wi query sample customers --frac 0.01

# 2. Filtering & SQL
wi query filter customers --where "age > 30 and age < 40"
wi query sql "SELECT count(*) FROM customers WHERE email LIKE '%.net'"

# 3. Text Search
wi query search logs "Error" --case-sensitive

# 4. Inspection
wi query distinct customers age --counts
wi query find-nulls customers
```

---

## Module 4: Analytics (`wi aggregate`)
**Folder:** `module_04_analytics`

*   **`analytics.db`**: `sales` table (date, product, store, qty, price, total).

**Test Commands:**
```bash
wi connect file analytics.db

# 1. Group By & Pivot
wi aggregate groupby sales store,product --agg "{\"total\": \"sum\"}"
wi aggregate pivot sales date store total --func sum

# 2. Statistics
wi aggregate describe sales
wi aggregate corr sales

# 3. Window Functions
wi aggregate moving-avg sales total --window 7
wi aggregate rank sales total --method dense
```

---

## Module 5: Visualization (`wi plot`)
**Folder:** `module_05_visualization`

*   **`weather.db`**: `daily` table (date, temp, humidity) with 365 days of data.

**Test Commands:**
```bash
wi connect file weather.db

# 1. Time Series
wi plot line daily date temp

# 2. Distributions
wi plot hist daily humidity --bins 20

# 3. Correlations
wi plot scatter daily temp humidity

# 4. Heatmap
wi plot heatmap daily temp humidity --bins 15
```

---

## Module 6: Data Wrangling (`wi clean`)
**Folder:** `module_06_data_wrangling`

*   **`dirty.db`**: `raw` table containing nulls, duplicates, outliers, and bad formatting.
*   **`schema_rules.json`**: Validation rules.

**Test Commands:**
```bash
wi connect file dirty.db

# 1. Imputation
wi clean impute-mode raw salary

# 2. Deduplication
wi clean dedupe raw --col name --threshold 90

# 3. Standardization
wi clean trim raw name --side both
wi clean harmonize raw country --strategy most_frequent

# 4. Outliers
wi clean drop-outliers raw salary --method iqr

# 5. Validation
wi clean validate-schema raw schema_rules.json
```

---

## Module 7: Geospatial (`wi geo`)
**Folder:** `module_07_geospatial`

*   **`geo.db`**: `cities` table with Lat/Lon coordinates (and one invalid point).

**Test Commands:**
```bash
wi connect file geo.db

# 1. Validation
wi geo validate cities --drop-invalid

# 2. Analysis
wi geo distance cities lat lon --target 40.7128,-74.0060
wi geo cluster cities --eps 500 --min-samples 2

# 3. Bounding Box
wi geo bounds cities --buffer 10

# 4. Export
wi geo export-geojson cities --lat lat --lon lon
```

---

## Module 8: Machine Learning (`wi ml`)
**Folder:** `module_08_machine_learning`

*   **`ml.db`**: `dataset` (feature1, feature2, label) for classification.

**Test Commands:**
```bash
wi connect file ml.db

# 1. Train Model
wi ml train-classifier dataset label feature1,feature2 --output-model model.pkl --model-type random_forest

# 2. Evaluation
wi ml score dataset model.pkl --metric accuracy

# 3. Prediction
wi ml predict dataset model.pkl prediction feature1,feature2

# 4. Unsupervised
wi ml cluster-kmeans dataset --k 3 --features feature1,feature2
wi ml detect-anomalies dataset --features feature1,feature2
```

---

## Module 9: Audit & Security (`wi audit`)
**Folder:** `module_09_audit_security`

*   **`secure.db`**: `employees` table with PII (Credit Cards, Emails).

**Test Commands:**
```bash
wi connect file secure.db

# 1. PII Scanning
wi audit scan-pii employees

# 2. Encryption & Masking
wi audit encrypt-col employees cc --key-file key.key
wi audit anonymize employees email --method mask

# 3. Forensics
wi audit hash-create employees
wi audit snapshot employees --name backup_v1
wi audit log-view
```

---

## Module 10: Operations (`wi ops`)
**Folder:** `module_10_operations`

*   **`ops.db`**: Metrics table.
*   **`pipeline.yaml`**: A multi-step maintenance workflow.

**Test Commands:**
```bash
wi connect file ops.db

# 1. Scheduling
wi ops schedule create "wi connect vacuum" --name "DailyVac" --cron "0 0 * * *"
wi ops schedule list

# 2. Pipelines
wi ops pipeline run pipeline.yaml

# 3. Monitoring
wi ops drift-check metrics --baseline baseline_snap
wi ops export-status --format json
```

---

## Module 11: Collaboration (`wi collab`)
**Folder:** `module_11_collaboration`

*   **`project.db`**: Shared project data.
*   **`team_config.json`**: Shared configuration settings.

**Test Commands:**
```bash
wi connect file project.db

# 1. Views & Notes
wi collab view save active_tasks --query "SELECT * FROM tasks WHERE status='active'"
wi collab notes add tasks --msg "Review pending items" --author "Admin"

# 2. Configuration
wi collab config import team_config.json
wi collab config export all exported_config.json

# 3. Bundling
wi collab recipe bundle full_project --out project_bundle.zip
```

---

## Module 12: Reporting (`wi report`)
**Folder:** `module_12_reporting`

*   **`report_source.db`**: Rich sales data suitable for generating reports.

**Test Commands:**
```bash
wi connect file report_source.db

# 1. EDA Dashboard (Sweetviz)
wi report generate sales --out dashboard.html

# 2. PDF Summary
wi report summary sales --out executive_summary.pdf

# 3. Documentation
wi report schema-doc --title "Sales Schema"
wi report dependencies
```