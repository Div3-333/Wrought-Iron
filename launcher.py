import os
import sys
import webbrowser
import threading
import time
import shutil
from pathlib import Path
from waitress import serve

# Configure Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'WroughtIronFullstack.settings')

if getattr(sys, 'frozen', False):
    # Running in a PyInstaller bundle
    bundle_dir = Path(sys._MEIPASS)
    data_dir = Path.home() / ".wi"
    data_dir.mkdir(exist_ok=True)
    
    db_path = data_dir / 'db.sqlite3'
    if not db_path.exists():
        bundled_db = bundle_dir / 'db.sqlite3'
        if bundled_db.exists():
            shutil.copy(bundled_db, db_path)
else:
    # Add src to path if needed for development
    sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from django.core.wsgi import get_wsgi_application

def open_browser():
    time.sleep(1.5)
    webbrowser.open('http://127.0.0.1:8080')

if __name__ == "__main__":
    application = get_wsgi_application()

    print("Starting Wrought Iron Server on http://127.0.0.1:8080")
    
    # Launch browser in a separate thread
    threading.Thread(target=open_browser).start()
    
    # Serve using Waitress
    serve(application, host='127.0.0.1', port=8080)
