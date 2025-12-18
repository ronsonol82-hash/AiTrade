# build_all.spec
# -*- mode: python ; coding: utf-8 -*-
import sys, os
from PyInstaller.utils.hooks import collect_all

# --- ИМПОРТИРУЕМ МОДУЛИ ДЛЯ ПОЛУЧЕНИЯ ПУТЕЙ ---
import torch
import pandas
import transformers

# --- СПИСОК ВСЕХ СКРИПТОВ ---
scripts_to_build = [
    ('fund_manager.py', 'AiTrade_Main', False),
    ('async_strategy_runner.py', 'AiTrade_Bot', True),
    ('gui_settings.py', 'AiTrade_Settings', False),
    ('optimizer.py', 'optimizer', True),
    ('signal_generator.py', 'signal_generator', True),
    ('debug_replayer.py', 'debug_replayer', True),
    ('test_gpu.py', 'test_gpu', True),
    ('leak_test.py', 'leak_test', True),
    ('noise_radar.py', 'noise_radar', True),
    ('stat_analyzer.py', 'stat_analyzer', True),
    ('check_balance.py', 'check_balance', True),
    ('inspect_probs.py', 'inspect_probs', True),
    ('debug_core.py', 'debug_core', True),
    ('feature_benchmark.py', 'feature_benchmark', True),
    ('plot_equity.py', 'plot_equity', True),
    ('validation_report.py', 'validation_report', True),
    ('get_instruments.py', 'get_instruments', True),
    ('test_connections.py', 'test_connections', True),
    ('test_full_cycle.py', 'test_full_cycle', True),
    ('test_async_bitget.py', 'test_async_bitget', True),
    ('test_core_no_lookahead.py', 'test_core_no_lookahead', True),
    ('check_channels.py', 'check_channels', True),
    ('visualizer.py', 'visualizer', True),
    ('signal_script.py', 'signal_script', True),
]

# --- РУЧНОЕ КОПИРОВАНИЕ ТЯЖЕЛЫХ БИБЛИОТЕК ---
raw_datas = []

# 1. Torch
torch_path = os.path.dirname(torch.__file__)
raw_datas.append((torch_path, 'torch'))

# 2. Pandas
pandas_path = os.path.dirname(pandas.__file__)
raw_datas.append((pandas_path, 'pandas'))

# 3. Transformers
trans_path = os.path.dirname(transformers.__file__)
raw_datas.append((trans_path, 'transformers'))

# --- СКРЫТЫЕ ИМПОРТЫ ---
hiddenimports = [
    'sklearn.utils._cython_blas', 
    'sklearn.neighbors.typedefs',
    'sklearn.tree', 
    'engineio.async_drivers.aiohttp',
    'numpy', 
    'scipy',
    # --- ДОБАВЛЕНО ДЛЯ ИСПРАВЛЕНИЯ ОШИБКИ ---
    'pytz',       # Pandas требует pytz
    'dateutil',   # Pandas часто требует dateutil
]

# --- ИСКЛЮЧЕНИЯ ---
excludes = [
    'torch', 'pandas', 'transformers', 
    'torch.utils.tensorboard', 'pytest', 'unittest'
]

# --- ЦИКЛ СБОРКИ ---
exes = []
all_datas = []
all_binaries = []

for i, (script_file, exe_name, show_console) in enumerate(scripts_to_build):
    my_datas = raw_datas if i == 0 else []
    
    a = Analysis(
        [script_file],
        pathex=[],
        binaries=[], 
        datas=my_datas, 
        hiddenimports=hiddenimports,
        hookspath=[],
        hooksconfig={},
        runtime_hooks=[],
        excludes=excludes, 
        noarchive=False,
    )
    
    pyz = PYZ(a.pure)
    
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        name=exe_name,
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True, 
        console=show_console, 
    )
    exes.append(exe)
    
    all_datas += a.datas
    all_binaries += a.binaries

# --- ФИНАЛЬНЫЙ СБОР ---
coll = COLLECT(
    *exes,
    all_binaries,
    all_datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='AiTrade_Suite',
)