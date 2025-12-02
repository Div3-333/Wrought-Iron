# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['to_db.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['pandas._libs.tslibs.np_datetime', 'pandas._libs.tslibs.nattype', 'sqlalchemy.sql.default_comparator'],
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
    name='universal-converter',
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
    icon=['db.ico'],
)
