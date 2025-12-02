import typer
import sqlite3
import json
import pickle # For saving/loading models
from pathlib import Path
from enum import Enum
from rich.console import Console
from rich.table import Table
from typing import Optional, List
import numpy as np # Needed for feature importance np.mean

from wrought_iron.cli.utils import _get_active_db # Added this import

app = typer.Typer() # This line was missing

def _get_pandas():
    import pandas as pd
    return pd

def _get_sklearn_model_selection():
    from sklearn.model_selection import train_test_split
    return train_test_split

def _get_sklearn_ensemble():
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, IsolationForest
    return RandomForestClassifier, RandomForestRegressor, IsolationForest

def _get_sklearn_linear_model():
    from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge
    return LogisticRegression, LinearRegression, Ridge

def _get_sklearn_metrics():
    from sklearn import metrics
    return metrics

def _get_sklearn_cluster():
    from sklearn.cluster import KMeans
    return KMeans

# Enums for deep arguments
class ClassifierType(str, Enum):
    random_forest = "random_forest"
    logistic_regression = "logistic_regression"

class RegressorType(str, Enum):
    linear_regression = "linear_regression"
    ridge = "ridge"
    random_forest = "random_forest"

class InitMethod(str, Enum):
    kmeans_pp = "k-means++"
    random = "random"

@app.command(name="train-classifier")
def train_classifier(
    table_name: str = typer.Argument(..., help="The name of the table."),
    target_col: str = typer.Argument(..., help="The target column (to predict)."),
    feature_cols: str = typer.Argument(..., help="Comma-separated list of feature columns."),
    model_type: ClassifierType = typer.Option(ClassifierType.random_forest, "--model-type", help="Type of classifier."),
    output_model_path: str = typer.Option(..., "--output-model", help="Path to save the trained model."),
    # Common hyperparameters
    test_size: float = typer.Option(0.2, "--test-size", help="Fraction of data for testing (0.0-1.0)."),
    random_state: Optional[int] = typer.Option(None, "--random-state", help="Random seed for reproducibility."),
    # RandomForest specific
    n_estimators: int = typer.Option(100, "--n-estimators", help="Number of trees in RandomForest."),
    max_depth: Optional[int] = typer.Option(None, "--max-depth", help="Max depth of the tree."),
    # LogisticRegression specific
    solver: str = typer.Option("lbfgs", "--solver", help="Algorithm to use in the optimization problem for LogisticRegression."),
    max_iter: int = typer.Option(100, "--max-iter", help="Maximum number of iterations for LogisticRegression solvers."),
):
    """Train a classification model."""
    pd = _get_pandas()
    train_test_split = _get_sklearn_model_selection()
    RFClassifier, _, _ = _get_sklearn_ensemble()
    LogisticRegression, _, _ = _get_sklearn_linear_model()
    
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
    
    features = [col.strip() for col in feature_cols.split(',')]
    if not all(col in df.columns for col in features + [target_col]):
        print("Error: One or more specified columns not found.")
        raise typer.Exit(code=1)

    X = df[features]
    y = df[target_col]
    
    # Handle non-numeric features - simple one-hot encoding for now
    X = pd.get_dummies(X, drop_first=True)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    
    model = None
    if model_type == ClassifierType.random_forest:
        model = RFClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=random_state)
    elif model_type == ClassifierType.logistic_regression:
        model = LogisticRegression(solver=solver, max_iter=max_iter, random_state=random_state)
    
    if model is None:
        print(f"Error: Unknown model type '{model_type}'.")
        raise typer.Exit(code=1)

    model.fit(X_train, y_train)
    
    # Save model
    with open(output_model_path, 'wb') as f:
        pickle.dump(model, f)
    
    print(f"Model trained and saved to '{output_model_path}'.")
    
    # Optional: print score on test set
    metrics = _get_sklearn_metrics()
    y_pred = model.predict(X_test)
    accuracy = metrics.accuracy_score(y_test, y_pred)
    print(f"Test Set Accuracy: {accuracy:.4f}")

@app.command(name="train-regressor")
def train_regressor(
    table_name: str = typer.Argument(..., help="The name of the table."),
    target_col: str = typer.Argument(..., help="The target column (to predict)."),
    feature_cols: str = typer.Argument(..., help="Comma-separated list of feature columns."),
    model_type: RegressorType = typer.Option(RegressorType.linear_regression, "--model-type", help="Type of regressor."),
    output_model_path: str = typer.Option(..., "--output-model", help="Path to save the trained model."),
    # Common hyperparameters
    test_size: float = typer.Option(0.2, "--test-size", help="Fraction of data for testing (0.0-1.0)."),
    random_state: Optional[int] = typer.Option(None, "--random-state", help="Random seed for reproducibility."),
    # Ridge specific
    alpha: float = typer.Option(1.0, "--alpha", help="Regularization strength for Ridge Regression."),
):
    """Train a regression model."""
    pd = _get_pandas()
    train_test_split = _get_sklearn_model_selection()
    _, RFRegressor, _ = _get_sklearn_ensemble() # Reuse Random Forest
    _, LinearRegression, Ridge = _get_sklearn_linear_model()
    
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
    
    features = [col.strip() for col in feature_cols.split(',')]
    if not all(col in df.columns for col in features + [target_col]):
        print("Error: One or more specified columns not found.")
        raise typer.Exit(code=1)

    X = df[features]
    y = df[target_col]
    
    # Handle non-numeric features - simple one-hot encoding for now
    X = pd.get_dummies(X, drop_first=True)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    
    model = None
    if model_type == RegressorType.linear_regression:
        model = LinearRegression()
    elif model_type == RegressorType.ridge:
        model = Ridge(alpha=alpha, random_state=random_state)
    elif model_type == RegressorType.random_forest:
        model = RFRegressor(random_state=random_state)
    
    if model is None:
        print(f"Error: Unknown model type '{model_type}'.")
        raise typer.Exit(code=1)

    model.fit(X_train, y_train)
    
    # Save model
    with open(output_model_path, 'wb') as f:
        pickle.dump(model, f)
    
    print(f"Model trained and saved to '{output_model_path}'.")
    
    # Optional: print score on test set
    metrics = _get_sklearn_metrics()
    y_pred = model.predict(X_test)
    r2 = metrics.r2_score(y_test, y_pred)
    print(f"Test Set R2 Score: {r2:.4f}")

@app.command()
def predict(
    table_name: str = typer.Argument(..., help="The name of the table to make predictions on."),
    model_path: str = typer.Argument(..., help="Path to the trained model."),
    output_col: str = typer.Argument(..., help="Name of the new column for predictions."),
    feature_cols: str = typer.Argument(..., help="Comma-separated list of feature columns."),
    threshold: Optional[float] = typer.Option(None, "--threshold", help="Threshold for classification probability (0.0-1.0)."),
):
    """Apply a saved model to fill empty columns or make new predictions."""
    pd = _get_pandas()
    
    if not Path(model_path).exists():
        print(f"Error: Model file '{model_path}' not found.")
        raise typer.Exit(code=1)
        
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
        
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
            
    features = [col.strip() for col in feature_cols.split(',')]
    if not all(col in df.columns for col in features):
        print("Error: One or more specified feature columns not found.")
        raise typer.Exit(code=1)

    X = df[features]
    X = pd.get_dummies(X, drop_first=True) # Ensure consistent encoding
    
    predictions = model.predict(X)
    
    if threshold is not None and hasattr(model, 'predict_proba'):
        probabilities = model.predict_proba(X)[:, 1] # Probability of positive class
        df[output_col] = (probabilities >= threshold).astype(int)
        print(f"Predictions made and saved to '{output_col}' using threshold {threshold}.")
    else:
        df[output_col] = predictions
        print(f"Predictions made and saved to '{output_col}'.")
    
    df.to_sql(table_name, con, if_exists="replace", index=False)

@app.command()
def score(
    table_name: str = typer.Argument(..., help="The name of the table containing actual and predicted values."),
    model_path: str = typer.Argument(..., help="Path to the trained model."),
    target_col: str = typer.Argument(..., help="The actual target column."),
    feature_cols: str = typer.Argument(..., help="Comma-separated list of feature columns."),
    metrics: str = typer.Option("accuracy", "--metrics", help="Comma-separated list of metrics (e.g., accuracy, precision, r2, mae)."),
):
    """Calculate Accuracy/R2/F1 Score."""
    pd = _get_pandas()
    sklearn_metrics = _get_sklearn_metrics()
    
    if not Path(model_path).exists():
        print(f"Error: Model file '{model_path}' not found.")
        raise typer.Exit(code=1)
        
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
        
    active_db = _get_active_db()
    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)

    features = [col.strip() for col in feature_cols.split(',')]
    if not all(col in df.columns for col in features + [target_col]):
        print("Error: One or more specified columns not found.")
        raise typer.Exit(code=1)

    X = df[features]
    y_true = df[target_col]
    
    X = pd.get_dummies(X, drop_first=True) # Ensure consistent encoding

    y_pred = model.predict(X)
    
    console = Console()
    table = Table(title=f"Model Score for {Path(model_path).name}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")
    
    for metric_name in [m.strip() for m in metrics.split(',')]:
        score_val = None
        try:
            if metric_name == "accuracy":
                score_val = sklearn_metrics.accuracy_score(y_true, y_pred)
            elif metric_name == "precision":
                score_val = sklearn_metrics.precision_score(y_true, y_pred, average='weighted', zero_division=0)
            elif metric_name == "recall":
                score_val = sklearn_metrics.recall_score(y_true, y_pred, average='weighted', zero_division=0)
            elif metric_name == "f1":
                score_val = sklearn_metrics.f1_score(y_true, y_pred, average='weighted', zero_division=0)
            elif metric_name == "r2":
                score_val = sklearn_metrics.r2_score(y_true, y_pred)
            elif metric_name == "mae":
                score_val = sklearn_metrics.mean_absolute_error(y_true, y_pred)
            elif metric_name == "mse":
                score_val = sklearn_metrics.mean_squared_error(y_true, y_pred)
            
            if score_val is not None:
                table.add_row(metric_name, f"{score_val:.4f}")
            else:
                table.add_row(metric_name, "N/A (unsupported)")
        except Exception as e:
            table.add_row(metric_name, f"Error: {e}")
            
    console.print(table)


@app.command(name="feature-importance")
def feature_importance(
    model_path: str = typer.Argument(..., help="Path to the trained model."),
    top_n: int = typer.Option(10, "--top-n", help="Number of top features to show."),
):
    """List which columns drive the prediction."""
    pd = _get_pandas()
    
    if not Path(model_path).exists():
        print(f"Error: Model file '{model_path}' not found.")
        raise typer.Exit(code=1)
        
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    if not hasattr(model, 'feature_importances_') and not hasattr(model, 'coef_'):
        print("Error: Model does not have feature importances or coefficients.")
        raise typer.Exit(code=1)
    
    feature_names = None
    if hasattr(model, 'feature_names_in_'): # sklearn >= 1.0
        feature_names = model.feature_names_in_
    elif hasattr(model, 'n_features_in_'): # Fallback
        feature_names = [f"feature_{i}" for i in range(model.n_features_in_)] # Generic names

    if feature_names is None:
        print("Error: Could not retrieve feature names from the model.")
        raise typer.Exit(code=1)

    importances = None
    if hasattr(model, 'feature_importances_'):
        importances = model.feature_importances_
    elif hasattr(model, 'coef_'):
        # For linear models, coef_ is usually 1D for binary, 2D for multi-class
        if model.coef_.ndim > 1:
            importances = np.mean(abs(model.coef_), axis=0) # Average abs coef for multi-class
        else:
            importances = abs(model.coef_)
            
    if importances is None:
        print("Error: No feature importance data found in model.")
        raise typer.Exit(code=1)

    feature_imp_df = pd.DataFrame({'Feature': feature_names, 'Importance': importances})
    feature_imp_df = feature_imp_df.sort_values(by='Importance', ascending=False).head(top_n)
    
    console = Console()
    table = Table(title=f"Top {top_n} Feature Importances for {Path(model_path).name}")
    table.add_column("Feature", style="cyan")
    table.add_column("Importance", style="magenta")
    
    for _, row in feature_imp_df.iterrows():
        table.add_row(str(row['Feature']), f"{row['Importance']:.4f}")
        
    console.print(table)

@app.command(name="save-model")
def save_model(
    model_path: str = typer.Argument(..., help="Path to the model file to save."),
):
    """Serialize model to .pk1 (pickle format)."""
    print("This command is typically used to save a model that has just been trained.")
    print("For direct saving from 'train' commands, use the --output-model option.")
    print("If you have a model object in a Python script, you can pickle it to this path.")

@app.command(name="load-model")
def load_model(
    model_path: str = typer.Argument(..., help="Path to the model file to load."),
):
    """Hydrate model from disk."""
    if not Path(model_path).exists():
        print(f"Error: Model file '{model_path}' not found.")
        raise typer.Exit(code=1)
        
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    print(f"Model loaded from '{model_path}'. Type: {type(model)}")

@app.command(name="cluster-kmeans")
def cluster_kmeans(
    table_name: str = typer.Argument(..., help="The name of the table."),
    feature_cols: str = typer.Argument(..., help="Comma-separated list of feature columns."),
    k: int = typer.Option(3, "--k", help="Number of clusters."),
    init: InitMethod = typer.Option(InitMethod.kmeans_pp, "--init", help="Method for initialization."),
    n_init: int = typer.Option(10, "--n-init", help="Number of times to run k-means with different centroids."),
    max_iter: int = typer.Option(300, "--max-iter", help="Maximum number of iterations."),
    random_state: Optional[int] = typer.Option(None, "--random-state", help="Random seed for reproducibility."),
    output_col: str = typer.Option("cluster_id", "--output-col", help="Name of the new cluster ID column."),
):
    """Unsupervised K-Means clustering."""
    pd = _get_pandas()
    KMeans = _get_sklearn_cluster()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
    
    features = [col.strip() for col in feature_cols.split(',')]
    if not all(col in df.columns for col in features):
        print("Error: One or more specified feature columns not found.")
        raise typer.Exit(code=1)
        
    X = df[features]
    X = pd.get_dummies(X, drop_first=True) # Handle categorical features
    
    kmeans = KMeans(n_clusters=k, init=init.value, n_init=n_init, max_iter=max_iter, random_state=random_state, verbose=0)
    
    df[output_col] = kmeans.fit_predict(X)
    
    df.to_sql(table_name, con, if_exists="replace", index=False)
    print(f"K-Means clustering complete. Cluster IDs saved to '{output_col}'.")

@app.command(name="detect-anomalies")
def detect_anomalies(
    table_name: str = typer.Argument(..., help="The name of the table."),
    feature_cols: str = typer.Argument(..., help="Comma-separated list of feature columns."),
    n_estimators: int = typer.Option(100, "--n-estimators", help="Number of base estimators in IsolationForest."),
    max_samples: str = typer.Option("auto", "--max-samples", help="Number of samples to draw from X to train each base estimator."),
    contamination: str = typer.Option("auto", "--contamination", help="The amount of contamination of the dataset."),
    random_state: Optional[int] = typer.Option(None, "--random-state", help="Random seed for reproducibility."),
    output_col: str = typer.Option("anomaly_score", "--output-col", help="Name of the new column for anomaly scores."),
):
    """Isolation Forest: Flag statistical outliers."""
    pd = _get_pandas()
    IsolationForest = _get_sklearn_ensemble()[2] # Get IsolationForest
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
    
    features = [col.strip() for col in feature_cols.split(',')]
    if not all(col in df.columns for col in features):
        print("Error: One or more specified feature columns not found.")
        raise typer.Exit(code=1)
        
    X = df[features]
    X = pd.get_dummies(X, drop_first=True) # Handle categorical features
    
    # max_samples and contamination can be float or 'auto'
    _max_samples = max_samples
    try:
        if isinstance(max_samples, str) and max_samples.lower() != 'auto':
            _max_samples = float(max_samples)
        elif not isinstance(max_samples, str):
            _max_samples = float(max_samples)
    except ValueError:
        print("Error: --max-samples must be 'auto' or a float/int.")
        raise typer.Exit(code=1)

    _contamination = contamination
    try:
        if isinstance(contamination, str) and contamination.lower() != 'auto':
            _contamination = float(contamination)
        elif not isinstance(contamination, str):
            _contamination = float(contamination)
    except ValueError:
        print("Error: --contamination must be 'auto' or a float (0.0-0.5).")
        raise typer.Exit(code=1)
    
    # Convert 'auto' to appropriate types for IF
    if _max_samples == 'auto': _max_samples = "auto"
    if _contamination == 'auto': _contamination = "auto"

    model = IsolationForest(
        n_estimators=n_estimators,
        max_samples=_max_samples,
        contamination=_contamination,
        random_state=random_state
    )
    
    # Fit and predict anomaly scores
    model.fit(X)
    df[output_col] = model.decision_function(X) # raw anomaly score
    # Optionally, flag anomalies directly: df[output_col] = model.predict(X) which gives -1 for anomaly, 1 for normal
    
    df.to_sql(table_name, con, if_exists="replace", index=False)
    print(f"Anomaly detection complete. Scores saved to '{output_col}'.")

@app.command()
def split(
    table_name: str = typer.Argument(..., help="The name of the table."),
    output_train_table: str = typer.Argument(..., help="Name for the new training data table."),
    output_test_table: str = typer.Argument(..., help="Name for the new testing data table."),
    train_size: float = typer.Option(0.75, "--train-size", help="Fraction of data for training (0.0-1.0)."),
    random_state: Optional[int] = typer.Option(None, "--random-state", help="Random seed for reproducibility."),
    stratify_col: Optional[str] = typer.Option(None, "--stratify-col", help="Column to use for stratified splitting."),
):
    """Split dataset into Train/Test subsets."""
    pd = _get_pandas()
    train_test_split = _get_sklearn_model_selection()
    active_db = _get_active_db()

    with sqlite3.connect(active_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        except pd.io.sql.DatabaseError:
            print(f"Error: Table '{table_name}' not found.")
            raise typer.Exit(code=1)
            
        stratify_data = None
        if stratify_col:
            if stratify_col not in df.columns:
                print(f"Error: Stratify column '{stratify_col}' not found.")
                raise typer.Exit(code=1)
            stratify_data = df[stratify_col]

        df_train, df_test = train_test_split(df, train_size=train_size, random_state=random_state, stratify=stratify_data)
        
        df_train.to_sql(output_train_table, con, if_exists="replace", index=False)
        df_test.to_sql(output_test_table, con, if_exists="replace", index=False)
        print(f"Data split into '{output_train_table}' ({len(df_train)} rows) and '{output_test_table}' ({len(df_test)} rows).")