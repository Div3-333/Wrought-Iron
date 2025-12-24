# -*- mode: python ; coding: utf-8 -*-
import os
import sys
from PyInstaller.utils.hooks import collect_all, collect_submodules

# Add the current directory and src to path for analysis
sys.path.append(os.path.abspath('.'))
sys.path.append(os.path.abspath('src'))

datas = [
    ('ui/templates', 'ui/templates'),
    ('staticfiles', 'static'),
    ('db.sqlite3', '.'),
    ('src/wrought_iron/cli/read_all.txt', 'wrought_iron/cli'),
]

binaries = []
hiddenimports = [
    'ui', 'ui.apps', 'ui.models', 'ui.views', 'ui.urls', 'ui.templatetags.custom_filters',
    'WroughtIronFullstack', 'WroughtIronFullstack.middleware', 'WroughtIronFullstack.urls', 'WroughtIronFullstack.settings', 'WroughtIronFullstack.wsgi',
    'wrought_iron', 'wrought_iron.main', 'wrought_iron.cli',
    'whitenoise.middleware', 'waitress',
    'django.db.backends.sqlite3.base',
    'django.db.backends.sqlite3.features',
    'django.db.backends.sqlite3.introspection',
    'django.db.backends.sqlite3.operations',
    'django.db.backends.sqlite3.schema',
    'django.contrib.admin.apps',
    'django.contrib.auth.apps',
    'django.contrib.contenttypes.apps',
    'django.contrib.sessions.apps',
    'django.contrib.messages.apps',
    'django.contrib.staticfiles.apps',
]

# Collect everything for heavy dependencies
deps = [
    'wrought_iron_core', 'pandas', 'polars', 'duckdb', 'sklearn', 'typer', 
    'textual', 'sweetviz', 'reportlab', 'cryptography', 'rapidfuzz',
    'presidio_analyzer', 'presidio_anonymizer', 'numexpr', 'pyarrow', 'plotext'
]

for dep in deps:
    try:
        tmp_ret = collect_all(dep)
        datas += tmp_ret[0]
        binaries += tmp_ret[1]
        hiddenimports += tmp_ret[2]
    except Exception:
        # If it fails to collect, just add to hiddenimports as a fallback
        hiddenimports.append(dep)

# Also add all wrought_iron and ui submodules
hiddenimports += collect_submodules('wrought_iron')
hiddenimports += collect_submodules('ui')
hiddenimports += collect_submodules('WroughtIronFullstack')

a = Analysis(
    ['launcher.py'],
    pathex=['.', 'src'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='WroughtIron',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
