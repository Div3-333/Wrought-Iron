from pathlib import Path
import sqlite3
import typer

def _get_config_dir() -> Path:
    """Get the config directory, creating it if it doesn't exist."""
    config_dir = Path.home() / ".wi"
    config_dir.mkdir(exist_ok=True)
    return config_dir

def _get_global_history_db_path() -> Path:
    """Get the path to the global history database."""
    return _get_config_dir() / "history.db"

def _initialize_global_history_db():
    """Create the global history database and table if they don't exist."""
    db_path = _get_global_history_db_path()
    if not db_path.exists():
        with sqlite3.connect(db_path) as con:
            con.execute("""
                CREATE TABLE history (
                    path TEXT PRIMARY KEY,
                    last_accessed TEXT NOT NULL,
                    size INTEGER
                )
            """)

def _is_wrought_iron_db(path: str) -> bool:
    """Check if a database is a valid Wrought Iron database."""
    try:
        with sqlite3.connect(path) as con:
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='_wi_audit_log_'")
            if cursor.fetchone():
                return True
            return False
    except sqlite3.Error:
        return False

def _get_active_db() -> Path:
    """Get the active database path."""
    config_file = _get_config_dir() / "config"
    if not config_file.exists() or not config_file.read_text():
        print("Error: No active database set. Use 'wi connect file' to set one.")
        raise typer.Exit(code=1)
    return Path(config_file.read_text())

def _get_theme_file() -> Path:
    return _get_config_dir() / "theme"

def _get_saved_theme() -> str:
    theme_file = _get_theme_file()
    if theme_file.exists():
        return theme_file.read_text().strip()
    return "default"

def _save_theme(theme_name: str):
    _get_theme_file().write_text(theme_name)
