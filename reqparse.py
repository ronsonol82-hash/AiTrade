import ast
import sys
from pathlib import Path

# Маппинг: имя импорта -> имя пакета в pip
IMPORT_TO_PACKAGE = {
    'PIL': 'pillow',
    'cv2': 'opencv-python',
    'sklearn': 'scikit-learn',
    'yaml': 'pyyaml',
    'bs4': 'beautifulsoup4',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'telegram': 'python-telegram-bot',
    'telethon': 'Telethon',
    'serial': 'pyserial',
    'usb': 'pyusb',
}

# Модули, которые нужно игнорировать (внутренние, устаревшие, платформо-специфичные)
IGNORE_IMPORTS = {
    # Однобуквенные и мусор
    'A', 'B', 'C', 'D', 'T', 'P', 'Q', 'R', 'S',
    # Python 2 / устаревшие
    'ConfigParser', 'HTMLParser', 'Queue', 'StringIO', 'cPickle',
    'httplib', 'urlparse', 'urllib2', 'xmlrpclib', '__builtin__',
    'htmlentitydefs', 'dummy_thread', 'dummy_threading', '_winreg',
    'cgi',
    # Внутренние модули
    '_cffi', '_pytest', '_speedups', '_subprocess', '_typeshed', '_abcoll',
    '_cmsgpack',
    # Платформо-специфичные
    '__pypy__', 'java', 'jnius', 'org', 'com', 'clr', 'System',
    'win32api', 'win32com', 'win32con', 'win32clipboard', 'win32security',
    'ntsecuritycon', 'pyodide', 'js',
    # Опциональные / тестовые
    'hypothesis', 'mock', 'pytest', 'atheris', 'pyperf',
    # Редкие / специфичные
    'dl', 'gobject', 'gi', 'imp', 'pathlib2', 'contextlib2',
    'compression', 'annotationlib',
}

def get_imports_from_file(filepath: str) -> set[str]:
    """Извлекает все импорты из Python файла."""
    imports = set()
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            tree = ast.parse(f.read(), filename=filepath)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    except SyntaxError:
        pass
    return imports

def get_stdlib_modules() -> set[str]:
    """Получает список модулей стандартной библиотеки."""
    if hasattr(sys, 'stdlib_module_names'):
        return set(sys.stdlib_module_names)
    return set()

def scan_project(project_path: str) -> dict:
    """Сканирует проект и возвращает все зависимости."""
    all_imports = set()
    local_modules = set()
    files_scanned = 0
    
    project_path = Path(project_path)
    
    # Папки, которые ОБЯЗАТЕЛЬНО игнорируем
    ignore_dirs = {
        '__pycache__', '.venv', 'venv', 'env', '.git', 'node_modules', 
        '.tox', 'site-packages', 'dist-packages', 'Lib', 'lib',
        '.eggs', 'build', 'dist', '.mypy_cache', '.pytest_cache',
        'eggs', 'parts', 'sdist', 'develop-eggs', 'downloads',
    }
    
    def should_skip(path: Path) -> bool:
        """Проверяет, нужно ли пропустить путь."""
        parts_lower = [p.lower() for p in path.parts]
        return any(d.lower() in parts_lower for d in ignore_dirs)
    
    # Собираем имена локальных модулей
    for py_file in project_path.rglob('*.py'):
        if should_skip(py_file):
            continue
        local_modules.add(py_file.stem)
        for parent in py_file.relative_to(project_path).parents:
            if parent.name:
                local_modules.add(parent.name)
    
    # Собираем все импорты
    for py_file in project_path.rglob('*.py'):
        if should_skip(py_file):
            continue
        all_imports.update(get_imports_from_file(py_file))
        files_scanned += 1
    
    # Фильтруем
    stdlib = get_stdlib_modules()
    external_deps = all_imports - stdlib - local_modules - IGNORE_IMPORTS
    
    # Убираем приватные модули (начинаются с _)
    external_deps = {dep for dep in external_deps if not dep.startswith('_')}
    
    return {
        'all_imports': all_imports,
        'external': external_deps,
        'stdlib': all_imports & stdlib,
        'local': all_imports & local_modules,
        'files_scanned': files_scanned
    }

def format_package_name(import_name: str) -> str:
    """Конвертирует имя импорта в имя pip-пакета."""
    return IMPORT_TO_PACKAGE.get(import_name, import_name)

if __name__ == '__main__':
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    
    print(f"🔍 Сканирую: {Path(path).absolute()}\n")
    
    result = scan_project(path)
    
    print("📦 Внешние зависимости (нужно установить):")
    if result['external']:
        for dep in sorted(result['external'], key=str.lower):
            pkg = format_package_name(dep)
            if pkg != dep:
                print(f"  {dep} → pip install {pkg}")
            else:
                print(f"  {dep}")
    else:
        print("  (не найдено)")
    
    print(f"\n📊 Статистика:")
    print(f"  Файлов просканировано: {result['files_scanned']}")
    print(f"  Внешних зависимостей: {len(result['external'])}")
    
    # Генерируем requirements.txt
    if result['external']:
        print("\n💾 Сохранить в requirements.txt? (y/n): ", end="")
        if input().strip().lower() == 'y':
            packages = [format_package_name(dep) for dep in sorted(result['external'], key=str.lower)]
            with open('requirements.txt', 'w') as f:
                f.write('\n'.join(packages))
            print("✅ Сохранено!")