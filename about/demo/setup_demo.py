import os
import sqlite3
import json
import csv
import random
import string
import time
from pathlib import Path
from datetime import datetime, timedelta

# Configuration
ROOT_DIR = Path("demo")
MODULES = [
    "module_01_infrastructure",
    "module_02_schema",
    "module_03_data_exploration",
    "module_04_analytics",
    "module_05_visualization",
    "module_06_data_wrangling",
    "module_07_geospatial",
    "module_08_machine_learning",
    "module_09_audit_security",
    "module_10_operations",
    "module_11_collaboration",
    "module_12_reporting"
]

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def create_db(path, schema_sql, data_sql=None, params=None):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.executescript(schema_sql)
    if data_sql and params:
        cursor.executemany(data_sql, params)
    conn.commit()
    conn.close()

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

# --- Generators ---

def gen_users(n=100):
    names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"]
    surnames = ["Smith", "Jones", "Williams", "Brown", "Taylor", "Davies", "Evans", "Wilson", "Thomas", "Roberts"]
    domains = ["example.com", "test.org", "demo.net"]
    data = []
    for i in range(n):
        first = random.choice(names)
        last = random.choice(surnames)
        email = f"{first.lower()}.{last.lower()}@{random.choice(domains)}"
        age = random.randint(18, 80)
        joined = random_date(datetime(2020, 1, 1), datetime(2023, 12, 31)).isoformat()
        data.append((i+1, f"{first} {last}", email, age, joined))
    return data

def gen_sales(n=1000):
    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Doohickey Z"]
    stores = ["Store 1", "Store 2", "Store 3"]
    data = []
    start = datetime(2023, 1, 1)
    end = datetime(2023, 12, 31)
    for i in range(n):
        date = random_date(start, end).date().isoformat()
        prod = random.choice(products)
        store = random.choice(stores)
        qty = random.randint(1, 50)
        price = random.uniform(10.0, 500.0)
        total = qty * price
        data.append((i+1, date, prod, store, qty, round(price, 2), round(total, 2)))
    return data

# --- Main Setup ---

print("Setting up demo environment...")

if os.path.exists(ROOT_DIR):
    import shutil
    shutil.rmtree(ROOT_DIR)
ensure_dir(ROOT_DIR)

# 1. Infrastructure
m1_dir = ROOT_DIR / MODULES[0]
ensure_dir(m1_dir)
print(f"Generating {MODULES[0]}...")

# Source DB
create_db(m1_dir / "source.db", 
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, joined TEXT);", 
    "INSERT INTO users VALUES (?,?,?,?,?)", gen_users(50))

# Target DB (empty users table)
create_db(m1_dir / "target.db", 
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, joined TEXT);")

# Deep path for alias test
ensure_dir(m1_dir / "deep" / "nested" / "folder")
create_db(m1_dir / "deep" / "nested" / "folder" / "deep_archive.db", 
    "CREATE TABLE archive (id INTEGER, note TEXT);", 
    "INSERT INTO archive VALUES (?,?)", [(1, "Hidden treasure")])


# 2. Schema
m2_dir = ROOT_DIR / MODULES[1]
ensure_dir(m2_dir)
print(f"Generating {MODULES[1]}...")

# Complex DB
create_db(m2_dir / "complex.db", """
    CREATE TABLE orders (order_id INTEGER PRIMARY KEY, user_id INTEGER, total REAL);
    CREATE TABLE items (item_id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT, FOREIGN KEY(order_id) REFERENCES orders(order_id));
    CREATE INDEX idx_user ON orders(user_id);
    CREATE VIEW order_summary AS SELECT order_id, total FROM orders;
    CREATE TABLE raw_json (id INTEGER, payload TEXT);
""")
# Insert JSON data
conn = sqlite3.connect(m2_dir / "complex.db")
conn.execute("INSERT INTO raw_json VALUES (1, '{\"user\": {\"name\": \"Alice\", \"id\": 101}, \"actions\": [\"login\", \"view\"]}')")
conn.execute("INSERT INTO raw_json VALUES (2, '{\"user\": {\"name\": \"Bob\", \"id\": 102}, \"actions\": [\"logout\"]}')")
conn.commit()
conn.close()

# DB A and DB B for diff
create_db(m2_dir / "v1.db", "CREATE TABLE t1 (a INT, b INT); CREATE TABLE t2 (x TEXT);")
create_db(m2_dir / "v2.db", "CREATE TABLE t1 (a INT, b INT, c INT); CREATE TABLE t3 (y REAL);") # t1 changed, t2 dropped, t3 added


# 3. Data Exploration
m3_dir = ROOT_DIR / MODULES[2]
ensure_dir(m3_dir)
print(f"Generating {MODULES[2]}...")

# Large Query DB
users_large = gen_users(5000)
create_db(m3_dir / "exploration.db", 
    "CREATE TABLE customers (id INTEGER, name TEXT, email TEXT, age INTEGER, joined TEXT);", 
    "INSERT INTO customers VALUES (?,?,?,?,?)", users_large)

# Text Search DB
conn = sqlite3.connect(m3_dir / "exploration.db")
conn.execute("CREATE TABLE logs (id INTEGER, message TEXT);")
conn.executemany("INSERT INTO logs VALUES (?,?)", [
    (1, "Error: Connection timeout at 10:00"),
    (2, "Info: User login successful"),
    (3, "Warning: Disk space low (90%)"),
    (4, "Error: Null pointer exception in module X"),
    (5, "Debug: Variable state dumped")
])
conn.commit()
conn.close()


# 4. Analytics
m4_dir = ROOT_DIR / MODULES[3]
ensure_dir(m4_dir)
print(f"Generating {MODULES[3]}...")

sales_data = gen_sales(2000)
create_db(m4_dir / "analytics.db", 
    "CREATE TABLE sales (id INTEGER, date TEXT, product TEXT, store TEXT, qty INTEGER, price REAL, total REAL);", 
    "INSERT INTO sales VALUES (?,?,?,?,?,?,?)", sales_data)


# 5. Visualization
m5_dir = ROOT_DIR / MODULES[4]
ensure_dir(m5_dir)
print(f"Generating {MODULES[4]}...")

# Weather Data
weather_data = []
start = datetime(2023, 1, 1)
for i in range(365):
    date = (start + timedelta(days=i)).date().isoformat()
    temp = 20 + 10 * math.sin(i / 365 * 2 * 3.14) + random.uniform(-5, 5) if 'math' in locals() else 20 + random.uniform(-10, 10)
    humidity = random.uniform(30, 90)
    weather_data.append((date, round(temp, 1), round(humidity, 1)))

import math
weather_data = []
for i in range(365):
    date = (start + timedelta(days=i)).date().isoformat()
    temp = 15 + 15 * math.sin(i * 2 * 3.14159 / 365) + random.gauss(0, 2)
    humidity = max(0, min(100, 60 + 20 * math.cos(i * 2 * 3.14159 / 365) + random.gauss(0, 5)))
    weather_data.append((date, round(temp, 1), round(humidity, 1)))

create_db(m5_dir / "weather.db", 
    "CREATE TABLE daily (date TEXT, temp REAL, humidity REAL);", 
    "INSERT INTO daily VALUES (?,?,?)", weather_data)


# 6. Data Wrangling
m6_dir = ROOT_DIR / MODULES[5]
ensure_dir(m6_dir)
print(f"Generating {MODULES[5]}...")

# Messy Data
dirty_data = [
    (1, "John Doe", "USA", 1000),
    (2, "Jane Doe", "U.S.A.", 1200),
    (3, "Bob Smith", "United States", None), # Missing val
    (4, "Alice Jones", "USA", 9999999), # Outlier
    (5, "John Doe", "USA", 1000), # Duplicate
    (6, "  Pad Me  ", "UK", 500) # Whitespace
]
create_db(m6_dir / "dirty.db", 
    "CREATE TABLE raw (id INTEGER, name TEXT, country TEXT, salary REAL);", 
    "INSERT INTO raw VALUES (?,?,?,?)", dirty_data)

# Schema Validation
with open(m6_dir / "schema_rules.json", "w") as f:
    json.dump({"salary": {"min": 0, "max": 10000}}, f)


# 7. Geospatial
m7_dir = ROOT_DIR / MODULES[6]
ensure_dir(m7_dir)
print(f"Generating {MODULES[6]}...")

# Cities
cities = [
    ("New York", 40.7128, -74.0060),
    ("Los Angeles", 34.0522, -118.2437),
    ("Chicago", 41.8781, -87.6298),
    ("London", 51.5074, -0.1278),
    ("Paris", 48.8566, 2.3522),
    ("Invalid City", 100.0, 200.0) # Invalid coords
]
create_db(m7_dir / "geo.db", 
    "CREATE TABLE cities (name TEXT, lat REAL, lon REAL);", 
    "INSERT INTO cities VALUES (?,?,?)", cities)


# 8. Machine Learning
m8_dir = ROOT_DIR / MODULES[7]
ensure_dir(m8_dir)
print(f"Generating {MODULES[7]}...")

# Classification Data (Iris-like mock)
ml_data = []
for _ in range(200):
    # Class 0: Small values
    f1 = random.gauss(2, 0.5)
    f2 = random.gauss(2, 0.5)
    ml_data.append((f1, f2, 0))
for _ in range(200):
    # Class 1: Large values
    f1 = random.gauss(6, 0.5)
    f2 = random.gauss(6, 0.5)
    ml_data.append((f1, f2, 1))

random.shuffle(ml_data)
create_db(m8_dir / "ml.db", 
    "CREATE TABLE dataset (feature1 REAL, feature2 REAL, label INTEGER);", 
    "INSERT INTO dataset VALUES (?,?,?)", ml_data)


# 9. Audit
m9_dir = ROOT_DIR / MODULES[8]
ensure_dir(m9_dir)
print(f"Generating {MODULES[8]}...")

audit_data = [
    (1, "Alice", "4000-1234-5678-9010", "alice@secret.com", 50000),
    (2, "Bob", "4111-1111-1111-1111", "bob@corp.net", 60000)
]
create_db(m9_dir / "secure.db", 
    """
    CREATE TABLE employees (id INTEGER, name TEXT, cc TEXT, email TEXT, salary INTEGER);
    CREATE TABLE _wi_audit_log_ (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, user TEXT, action TEXT, details TEXT);
    """, 
    "INSERT INTO employees VALUES (?,?,?,?,?)", audit_data)


# 10. Ops
m10_dir = ROOT_DIR / MODULES[9]
ensure_dir(m10_dir)
print(f"Generating {MODULES[9]}...")

create_db(m10_dir / "ops.db", "CREATE TABLE metrics (cpu REAL, ram REAL, time TEXT);")
# Pipeline file
with open(m10_dir / "pipeline.yaml", "w") as f:
    f.write("""
name: "Daily Maintenance"
steps:
  - name: "Vacuum DB"
    command: "echo Vacuuming..."
  - name: "Backup"
    command: "echo Backing up..."
""")


# 11. Collab
m11_dir = ROOT_DIR / MODULES[10]
ensure_dir(m11_dir)
print(f"Generating {MODULES[10]}...")

create_db(m11_dir / "project.db", "CREATE TABLE tasks (id INTEGER, title TEXT, status TEXT);")
with open(m11_dir / "team_config.json", "w") as f:
    json.dump({"default_db": "project.db", "theme": "dark"}, f)


# 12. Reporting
m12_dir = ROOT_DIR / MODULES[11]
ensure_dir(m12_dir)
print(f"Generating {MODULES[11]}...")

# Rich dataset for report
report_data = gen_sales(500)
create_db(m12_dir / "report_source.db", 
    "CREATE TABLE sales (id INTEGER, date TEXT, product TEXT, store TEXT, qty INTEGER, price REAL, total REAL);", 
    "INSERT INTO sales VALUES (?,?,?,?,?,?,?)", report_data)

print("Demo setup complete.")
