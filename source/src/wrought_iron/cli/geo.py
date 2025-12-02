import typer
import sqlite3
import json
import math
import numpy as np
from pathlib import Path
from enum import Enum
from rich.console import Console
from rich.table import Table
from wrought_iron.cli.utils import _get_active_db

app = typer.Typer()

# Lazy loading for heavy libraries
def _get_pandas():
    import pandas as pd
    return pd

def _get_sklearn_cluster():
    from sklearn.cluster import DBSCAN
    return DBSCAN

def _get_sklearn_neighbors():
    from sklearn.neighbors import BallTree
    return BallTree

def _get_plotext():
    import plotext as plt
    return plt

# Constants for Earth's radius in kilometers for Haversine
EARTH_RADIUS_KM = 6371.0

class DBSCANMetric(str, Enum):
    euclidean = "euclidean"
    haversine = "haversine" # Custom haversine if not in sklearn
    # Add more as needed by sklearn DBSCAN metrics

class DBSCANAlgorithm(str, Enum):
    auto = "auto"
    ball_tree = "ball_tree"
    kd_tree = "kd_tree"
    brute = "brute"

@app.command()
def validate(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
):
    """Validate coordinates fall within global bounds (-90 to 90 lat, -180 to 180 lon)."""
    pd = _get_pandas()
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {lat_col}, {lon_col} FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        invalid_lat = (df[lat_col] < -90) | (df[lat_col] > 90)
        invalid_lon = (df[lon_col] < -180) | (df[lon_col] > 180)
        
        invalid_coords = df[invalid_lat | invalid_lon]
        
        if invalid_coords.empty:
            print("All coordinates are valid.")
        else:
            print(f"Found {len(invalid_coords)} invalid coordinates:")
            console = Console()
            table = Table(title=f"Invalid Coordinates in {table_name}")
            table.add_column(lat_col, style="cyan")
            table.add_column(lon_col, style="magenta")
            for _, row in invalid_coords.iterrows():
                table.add_row(str(row[lat_col]), str(row[lon_col]))
            console.print(table)

@app.command()
def geocode(
    table_name: str = typer.Argument(..., help="The name of the table."),
    addr_col: str = typer.Argument(..., help="The address column to geocode."),
    lookup_file: str = typer.Argument(..., help="Path to local CSV lookup file (address,lat,lon)."),
    new_lat_col: str = typer.Option("latitude", "--new-lat-col", help="Name for new latitude column."),
    new_lon_col: str = typer.Option("longitude", "--new-lon-col", help="Name for new longitude column."),
):
    """Offline Geocoding: Map address to Lat/Lon using a local lookup table."""
    pd = _get_pandas()
    active_db = _get_active_db()
    
    lookup_path = Path(lookup_file)
    if not lookup_path.exists():
        print(f"Error: Lookup file '{lookup_file}' not found.")
        raise typer.Exit(code=1)
        
    try:
        lookup_df = pd.read_csv(lookup_path)
        if 'address' not in lookup_df.columns or 'lat' not in lookup_df.columns or 'lon' not in lookup_df.columns:
            print("Error: Lookup file must contain 'address', 'lat', 'lon' columns.")
            raise typer.Exit(code=1)
        lookup_dict = lookup_df.set_index('address')[['lat', 'lon']].T.to_dict('series')
    except Exception as e:
        print(f"Error reading lookup file: {e}")
        raise typer.Exit(code=1)

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if addr_col not in df.columns:
            print(f"Error: Address column '{addr_col}' not found.")
            raise typer.Exit(code=1)
            
        df[new_lat_col] = df[addr_col].map(lambda x: lookup_dict.get(x, {}).get('lat'))
        df[new_lon_col] = df[addr_col].map(lambda x: lookup_dict.get(x, {}).get('lon'))
        
        df.to_sql(table_name, con, if_exists="replace", index=False)
        print(f"Geocoded '{addr_col}' to '{new_lat_col}', '{new_lon_col}'.")

@app.command()
def reverse(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
    lookup_file: str = typer.Argument(..., help="Path to local CSV lookup file (lat,lon,address)."),
    new_addr_col: str = typer.Option("address", "--new-addr-col", help="Name for new address column."),
):
    """Reverse Geocoding: Convert coordinates to address using a local lookup table."""
    pd = _get_pandas()
    active_db = _get_active_db()
    
    lookup_path = Path(lookup_file)
    if not lookup_path.exists():
        print(f"Error: Lookup file '{lookup_file}' not found.")
        raise typer.Exit(code=1)
        
    try:
        lookup_df = pd.read_csv(lookup_path)
        if 'lat' not in lookup_df.columns or 'lon' not in lookup_df.columns or 'address' not in lookup_df.columns:
            print("Error: Lookup file must contain 'lat', 'lon', 'address' columns.")
            raise typer.Exit(code=1)
        # Create a tuple key for mapping
        lookup_df['coords'] = list(zip(lookup_df['lat'], lookup_df['lon']))
        lookup_dict = lookup_df.set_index('coords')['address'].to_dict()
    except Exception as e:
        print(f"Error reading lookup file: {e}")
        raise typer.Exit(code=1)

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        df['coords'] = list(zip(df[lat_col], df[lon_col]))
        df[new_addr_col] = df['coords'].map(lambda x: lookup_dict.get(x))
        df.drop(columns=['coords'], inplace=True) # Drop temp column
        
        df.to_sql(table_name, con, if_exists="replace", index=False)
        print(f"Reverse geocoded coordinates to '{new_addr_col}'.")

@app.command()
def distance(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
    target_lat: float = typer.Option(..., "--target-lat", help="Target latitude."),
    target_lon: float = typer.Option(..., "--target-lon", help="Target longitude."),
    new_dist_col: str = typer.Option("distance_km", "--new-dist-col", help="Name for new distance column."),
):
    """Calculate Haversine distance to a target point."""
    pd = _get_pandas()
    active_db = _get_active_db()
    
    def haversine(lat1, lon1, lat2, lon2):
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return EARTH_RADIUS_KM * c

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        df[new_dist_col] = df.apply(
            lambda row: haversine(row[lat_col], row[lon_col], target_lat, target_lon),
            axis=1
        )
        
        df.to_sql(table_name, con, if_exists="replace", index=False)
        print(f"Calculated Haversine distance to ({target_lat}, {target_lon}) in '{new_dist_col}'.")

@app.command()
def cluster(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
    eps: float = typer.Option(0.5, "--eps", help="DBSCAN: Max distance between points (km)."),
    min_samples: int = typer.Option(5, "--min-samples", help="DBSCAN: Min points to form a cluster."),
    metric: DBSCANMetric = typer.Option(DBSCANMetric.euclidean, "--metric", help="DBSCAN: Distance metric."),
    algorithm: DBSCANAlgorithm = typer.Option(DBSCANAlgorithm.auto, "--algorithm", help="DBSCAN: Optimization method."),
    n_jobs: int = typer.Option(-1, "--n-jobs", help="DBSCAN: Use all CPU cores (-1)."),
    out_col: str = typer.Option("cluster_id", "--out-col", help="Name of the new cluster ID column."),
    noise_label: str = typer.Option("-1", "--noise-label", help="Label for noise points."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview cluster sizes without saving."),
):
    """DBSCAN: Group records into spatial clusters based on density."""
    pd = _get_pandas()
    DBSCAN = _get_sklearn_cluster()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        coords = df[[lat_col, lon_col]].values
        
        # Haversine requires input in radians
        if metric == DBSCANMetric.haversine:
            coords_radians = np.radians(coords)
            # eps needs to be in radians for haversine metric
            # radius_of_earth in km
            eps_radians = eps / EARTH_RADIUS_KM
            dbscan_metric = "haversine"
        else:
            coords_radians = coords # Euclidean works with degrees/km directly
            dbscan_metric = metric.value
            eps_radians = eps # Assuming eps is in appropriate units for Euclidean (e.g., km)

        db = DBSCAN(
            eps=eps_radians,
            min_samples=min_samples,
            metric=dbscan_metric,
            algorithm=algorithm.value,
            n_jobs=n_jobs
        ).fit(coords_radians)
        
        labels = db.labels_
        df[out_col] = labels
        
        if noise_label != "-1": # Replace default noise label if user provided one
            df[out_col] = df[out_col].replace(-1, noise_label)
        
        cluster_counts = df[out_col].value_counts().sort_index()
        
        console = Console()
        table = Table(title=f"Cluster Sizes for {table_name}")
        table.add_column(out_col, style="cyan")
        table.add_column("Count", style="magenta")
        for cluster_id, count in cluster_counts.items():
            table.add_row(str(cluster_id), str(count))
        console.print(table)

        if not dry_run:
            df.to_sql(table_name, con, if_exists="replace", index=False)
            print(f"Clusters saved to '{out_col}'.")
        else:
            print("Dry run: Changes not saved.")

@app.command()
def centroid(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
):
    """Calculate the geometric center (mean Lat/Lon) of the dataset."""
    pd = _get_pandas()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {lat_col}, {lon_col} FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        centroid_lat = df[lat_col].mean()
        centroid_lon = df[lon_col].mean()
        
        print(f"Centroid Latitude: {centroid_lat}")
        print(f"Centroid Longitude: {centroid_lon}")

@app.command()
def bounds(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
):
    """Return the North/South/East/West bounding box (min/max Lat/Lon)."""
    pd = _get_pandas()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {lat_col}, {lon_col} FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        min_lat = df[lat_col].min()
        max_lat = df[lat_col].max()
        min_lon = df[lon_col].min()
        max_lon = df[lon_col].max()
        
        print(f"Min Latitude (South): {min_lat}")
        print(f"Max Latitude (North): {max_lat}")
        print(f"Min Longitude (West): {min_lon}")
        print(f"Max Longitude (East): {max_lon}")

@app.command(name="heatmap")
def geo_heatmap(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
    bins: int = typer.Option(20, "--bins", help="Number of bins for the heatmap grid."),
):
    """Render an ASCII density map in the terminal (2D Histogram of points)."""
    pd = _get_pandas()
    plt = _get_plotext()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {lat_col}, {lon_col} FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
        
        # Create 2D histogram
        df_plot = df[[lat_col, lon_col]].dropna()
        if df_plot.empty:
            print("No valid coordinates to plot.")
            return

        # Use pd.cut to create bins for lat and lon
        df_plot['lat_bin'] = pd.cut(df_plot[lat_col], bins=bins, labels=False, include_lowest=True)
        df_plot['lon_bin'] = pd.cut(df_plot[lon_col], bins=bins, labels=False, include_lowest=True)

        # Count points in each bin to create density
        density_matrix = df_plot.groupby(['lat_bin', 'lon_bin']).size().unstack(fill_value=0)

        # Plotext matrix_plot expects rows to correspond to y-axis and columns to x-axis
        # And y-axis increasing upwards. So lat_bin should be reversed.
        matrix_data = density_matrix.sort_index(ascending=False).values.tolist()
        
        plt.clf()
        plt.theme("default") # Not using persistent theme as this is a specific visualization
        plt.limit_size(False, False)
        plt.plotsize(100, 30)
        plt.matrix_plot(matrix_data)
        plt.title(f"Geo Heatmap: {lat_col} vs {lon_col}")
        plt.xlabel(lon_col)
        plt.ylabel(lat_col)
        plt.show()

@app.command()
def nearest(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
    target_file: str = typer.Argument(..., help="Path to CSV file with POI (id,lat,lon)."),
    k: int = typer.Option(1, "--k", help="Number of nearest neighbors to find."),
    out_col_id: str = typer.Option("nearest_poi_id", "--out-col-id", help="Name for new nearest POI ID column."),
    out_col_dist: str = typer.Option("nearest_poi_dist_km", "--out-col-dist", help="Name for new nearest POI distance column."),
):
    """Find nearest Point of Interest (e.g., Hospital) for each record."""
    pd = _get_pandas()
    BallTree = _get_sklearn_neighbors()
    active_db = _get_active_db()
    
    poi_path = Path(target_file)
    if not poi_path.exists():
        print(f"Error: POI file '{target_file}' not found.")
        raise typer.Exit(code=1)

    try:
        poi_df = pd.read_csv(poi_path)
        if 'id' not in poi_df.columns or 'lat' not in poi_df.columns or 'lon' not in poi_df.columns:
            print("Error: POI file must contain 'id', 'lat', 'lon' columns.")
            raise typer.Exit(code=1)
    except Exception as e:
        print(f"Error reading POI file: {e}")
        raise typer.Exit(code=1)

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        # Convert degrees to radians for Haversine
        coords_radians = np.radians(df[[lat_col, lon_col]].dropna().values)
        poi_coords_radians = np.radians(poi_df[['lat', 'lon']].values)

        tree = BallTree(poi_coords_radians, metric='haversine')
        
        # Query the tree for nearest neighbors
        # Returns distances in radians, need to convert to km
        distances_radians, indices = tree.query(coords_radians, k=k)
        
        # Convert distances back to kilometers
        distances_km = distances_radians * EARTH_RADIUS_KM
        
        # Handle k=1 vs k>1
        if k == 1:
            df[out_col_id] = poi_df['id'].iloc[indices.flatten()].values
            df[out_col_dist] = distances_km.flatten()
        else:
            # For k > 1, store lists of IDs and distances
            df[out_col_id] = [poi_df['id'].iloc[idx].tolist() for idx in indices]
            df[out_col_dist] = [dist.tolist() for dist in distances_km]
            
        df.to_sql(table_name, con, if_exists="replace", index=False)
        print(f"Found {k} nearest POIs in '{out_col_id}' with distances in '{out_col_dist}'.")

@app.command(name="export-geojson")
def export_geojson(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
    id_col: str = typer.Option(None, "--id-col", help="Optional column to use as GeoJSON 'id' property."),
    properties: str = typer.Option(None, "--properties", help="Comma-separated list of columns to include as GeoJSON properties."),
    output_file: str = typer.Option("output.geojson", "--output-file", help="Output GeoJSON file name."),
):
    """Export data to standard GeoJSON format."""
    pd = _get_pandas()
    active_db = _get_active_db()
    
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        features = []
        for _, row in df.iterrows():
            props = {}
            if properties:
                for prop_col in properties.split(','):
                    if prop_col in row.index:
                        props[prop_col] = row[prop_col]
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row[lon_col], row[lat_col]] # GeoJSON is [lon, lat]
                },
                "properties": props
            }
            if id_col and id_col in row.index:
                feature["id"] = row[id_col]
            features.append(feature)
            
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        output_path = Path(output_file)
        with open(output_path, 'w') as f:
            json.dump(geojson, f, indent=2)
            
        print(f"Data exported to '{output_file}'.")

@app.command()
def bounds(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
):
    """Return the North/South/East/West bounding box (min/max Lat/Lon)."""
    pd = _get_pandas()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {lat_col}, {lon_col} FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        min_lat = df[lat_col].min()
        max_lat = df[lat_col].max()
        min_lon = df[lon_col].min()
        max_lon = df[lon_col].max()
        
        print(f"Min Latitude (South): {min_lat}")
        print(f"Max Latitude (North): {max_lat}")
        print(f"Min Longitude (West): {min_lon}")
        print(f"Max Longitude (East): {max_lon}")

@app.command()
def centroid(
    table_name: str = typer.Argument(..., help="The name of the table."),
    lat_col: str = typer.Argument(..., help="The latitude column."),
    lon_col: str = typer.Argument(..., help="The longitude column."),
):
    """Calculate the geometric center (mean Lat/Lon) of the dataset."""
    pd = _get_pandas()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT {lat_col}, {lon_col} FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
        
        if lat_col not in df.columns or lon_col not in df.columns:
            print(f"Error: Latitude or longitude column not found.")
            raise typer.Exit(code=1)
            
        centroid_lat = df[lat_col].mean()
        centroid_lon = df[lon_col].mean()
        
        print(f"Centroid Latitude: {centroid_lat}")
        print(f"Centroid Longitude: {centroid_lon}")
