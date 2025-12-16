import os
from dotenv import load_dotenv, set_key, find_dotenv

class SettingsManager:
    def __init__(self, env_path=".env"):
        self.env_path = find_dotenv(env_path)
        if not self.env_path:
            # Если файла нет, создаем пустой, чтобы не крашилось
            with open(env_path, "w") as f:
                f.write("")
            self.env_path = env_path
        
        # Загружаем переменные сразу при инициализации
        load_dotenv(self.env_path, override=True)

    def get_setting(self, key, default=""):
        """Получить значение переменной (из памяти или env)"""
        return os.getenv(key, default)

    def save_setting(self, key, value):
        """Записать переменную в файл и обновить в памяти"""
        # 1. Пишем в файл
        set_key(self.env_path, key, str(value))
        # 2. Обновляем в текущей сессии (чтобы бот сразу увидел изменения)
        os.environ[key] = str(value)

    def reload_all(self):
        """Принудительная перезагрузка всех переменных"""
        load_dotenv(self.env_path, override=True)

# Синглтон для удобного импорта
settings = SettingsManager()