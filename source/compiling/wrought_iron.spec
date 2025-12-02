# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

block_cipher = None

# --- 1. Hidden Imports ---
hidden_imports = [
    'typer', 'typer.core', 'typer.models', 'rich', 'textual', 'textual.drivers.windows',
    'pandas', 'polars', 'numpy', 'numexpr',
    'plotext', 'sweetviz',
    'sklearn', 'scipy',
    'cryptography', 'rapidfuzz', 'presidio_analyzer', 'presidio_anonymizer', 'reportlab',
]
hidden_imports += collect_submodules('sklearn')
hidden_imports += collect_submodules('scipy')
hidden_imports += collect_submodules('presidio_analyzer')
hidden_imports += collect_submodules('textual')

# --- 2. Data Files ---
datas = []
datas += collect_data_files('textual')
datas += collect_data_files('sweetviz')
datas += collect_data_files('presidio_analyzer')

# --- 3. Analysis ---
a = Analysis(
    ['wrought_iron/main.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False, 
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# --- 4. EXE (Launcher Only) ---
# NOTE: In onedir mode, this only builds the small .exe stub.
# We set exclude_binaries=True so libraries aren't packed inside it.
exe = EXE(
    pyz,
    a.scripts,
    [], # List of binaries is empty here for onedir
    exclude_binaries=True, # CRITICAL: Tells PyInstaller to put libs outside
    name='wrought_iron',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='wrought_iron.ico'
)

# --- 5. COLLECT (The Directory Builder) ---
# This block is what creates the folder. It combines the EXE with all the
# binaries, zips, and data files.
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='wrought_iron', # Name of the output folder
)