from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QFormLayout, QLineEdit, 
                             QPushButton, QLabel, QMessageBox, QCheckBox, 
                             QDialogButtonBox, QWidget, QScrollArea)
from PyQt5.QtCore import Qt
from settings_manager import settings

# --- СТИЛИ (Единый Dark Theme) ---
SETTINGS_STYLESHEET = """
/* Базовый стиль диалога и шрифты */
QDialog {
    background-color: #1e1e1e;
    color: #e0e0e0;
    font-family: 'Segoe UI', sans-serif;
    font-size: 10pt;
}

/* --- Исправление белого фона --- */
/* Задаем фон конкретно для контейнера внутри скролла по его ID */
#SettingsContainer {
    background-color: #1e1e1e;
}

QScrollArea {
    border: none;
    background-color: #1e1e1e;
}

/* --- Стилизация Скроллбара (чтобы не был белым) --- */
QScrollBar:vertical {
    border: none;
    background: #2d2d2d;
    width: 10px;
    margin: 0px 0px 0px 0px;
}
QScrollBar::handle:vertical {
    background: #555;
    min-height: 20px;
    border-radius: 5px;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    border: none;
    background: none;
}

/* --- Остальные виджеты --- */
QLabel {
    color: #e0e0e0;
}
QLabel[role="header"] {
    font-weight: bold;
    color: #007acc;
    margin-top: 15px;
    margin-bottom: 5px;
    font-size: 11pt;
    border-bottom: 1px solid #333;
    padding-bottom: 3px;
}
QLineEdit {
    background-color: #2d2d2d;
    color: #ffffff;
    border: 1px solid #454545;
    padding: 4px 6px;
    border-radius: 3px;
    min-height: 20px;
}
QLineEdit:focus {
    border: 1px solid #007acc;
}
QCheckBox {
    color: #e0e0e0;
    spacing: 5px;
    padding: 4px;
}
QCheckBox::indicator {
    width: 16px;
    height: 16px;
    border: 1px solid #555;
    background: #2d2d2d;
    border-radius: 3px;
}
QCheckBox::indicator:checked {
    background-color: #007acc;
    border: 1px solid #007acc;
    /* Иконка галочки (можно заменить на свою картинку, но пока так) */
    image: url(data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='white' stroke-width='3' stroke-linecap='round' stroke-linejoin='round'><polyline points='20 6 9 17 4 12'/></svg>);
}
QPushButton {
    background-color: #3c3c3c;
    border: 1px solid #555555;
    color: #ffffff;
    padding: 8px 16px;
    border-radius: 4px;
    min-width: 80px;
}
QPushButton:hover {
    background-color: #4a4a4a;
    border-color: #007acc;
}
QPushButton:pressed {
    background-color: #333333;
    border-color: #005c99;
}
QDialogButtonBox {
    margin-top: 10px;
}
"""

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки бота (.env Configuration)")
        self.setModal(True)
        # Немного увеличил дефолтный размер
        self.resize(600, 800)
        
        self.setStyleSheet(SETTINGS_STYLESHEET)

        # Основной лейоут диалога
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(15)

        # Создаем ScrollArea
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame) # Убираем стандартную рамку скролла
        
        # Контейнер для содержимого внутри скролла
        content_widget = QWidget()
        # ВАЖНО: Даем имя объекту, чтобы привязать к нему стиль #SettingsContainer
        content_widget.setObjectName("SettingsContainer")
        
        self.form_layout = QFormLayout(content_widget)
        self.form_layout.setVerticalSpacing(12) # Чуть больше воздуха между строками
        self.form_layout.setLabelAlignment(Qt.AlignLeft)
        self.form_layout.setContentsMargins(10, 10, 15, 10) # Отступы внутри скролла
        
        scroll.setWidget(content_widget)
        main_layout.addWidget(scroll)

        # --- КАРТА ПАРАМЕТРОВ ---
        # Формат: (Label, ENV_KEY, Type, Placeholder/Hint)
        self.params = [
            # --- TELEGRAM ---
            ("--- Telegram Infrastructure ---", None, "header", ""),
            ("API ID (App)", "TELEGRAM_API_ID", "number", "e.g. 123456"),
            ("API Hash", "TELEGRAM_API_HASH", "secret", "b6ea..."),
            ("Alert Bot Token", "ALERT_TG_BOT_TOKEN", "secret", "BotFather token"),
            ("Alert Chat ID", "ALERT_TG_CHAT_ID", "text", "User/Channel ID"),

            # --- REDIS ---
            ("--- Data Backbone (Redis) ---", None, "header", ""),
            ("Redis Host", "REDIS_HOST", "text", "localhost"),
            ("Redis Port", "REDIS_PORT", "number", "6379"),

            # --- BITGET ---
            ("--- Bitget (Crypto) ---", None, "header", ""),
            ("API Key", "BITGET_API_KEY", "text", "bg_..."),
            ("Secret Key", "BITGET_API_SECRET", "secret", "Your secret key"),
            ("Passphrase", "BITGET_API_PASSPHRASE", "secret", "Passphrase"),
            ("Native Protection", "BITGET_NATIVE_PROTECTION", "bool", "Enable exchange-side SL/TP"),

            # --- TINKOFF ---
            ("--- Tinkoff (Stocks/Moex) ---", None, "header", ""),
            ("API Token", "TINKOFF_API_TOKEN", "secret", "t.RDF..."),
            ("Sandbox Mode", "TINKOFF_SANDBOX", "bool", "Use Sandbox environment"),

            # --- GLOBAL MODE ---
            ("--- System & Safety ---", None, "header", ""),
            ("Execution Mode", "EXECUTION_MODE", "text", "backtest / paper / live"),
            ("Allow Live Trading", "ALLOW_LIVE", "bool", "DANGER: Enables real orders"),
            ("Use Telegram Crypto", "USE_TG_CRYPTO", "bool", "Parse crypto channels"),
            ("Use Telegram Stocks", "USE_TG_STOCKS", "bool", "Parse stock channels"),
            
            # --- TIMEOUTS & LIMITS ---
            ("--- Limits ---", None, "header", ""),
            ("Order Timeout (s)", "ORDER_CONFIRM_TIMEOUT_S", "number", "30"),
            ("Kill-Switch Poll (s)", "KILL_SWITCH_POLL_SECONDS", "number", "1"),
        ]

        self.widgets = {}
        self._build_ui()

        # Кнопки (внизу, вне скролла)
        button_box = QDialogButtonBox()
        self.btn_save = QPushButton("Сохранить (.env)")
        self.btn_cancel = QPushButton("Отмена")
        self.btn_save.setCursor(Qt.PointingHandCursor)
        self.btn_cancel.setCursor(Qt.PointingHandCursor)
        
        button_box.addButton(self.btn_save, QDialogButtonBox.AcceptRole)
        button_box.addButton(self.btn_cancel, QDialogButtonBox.RejectRole)
        
        self.btn_save.clicked.connect(self.save_settings)
        self.btn_cancel.clicked.connect(self.reject)
        
        main_layout.addWidget(button_box)

    def _build_ui(self):
        for label_text, key, p_type, hint in self.params:
            if p_type == "header":
                lbl = QLabel(label_text)
                lbl.setProperty("role", "header")
                self.form_layout.addRow(lbl)
                continue

            # Получаем текущее значение
            current_val = settings.get_setting(key, "")

            if p_type == "bool":
                widget = QCheckBox()
                widget.setCursor(Qt.PointingHandCursor)
                # Обработка строковых "true"/"1" из .env
                is_checked = str(current_val).lower() in ("true", "1", "yes", "on")
                widget.setChecked(is_checked)
                # Для чекбокса лейбл добавляем прямо в него, чтобы кликалось по тексту
                widget.setText(label_text)
                self.widgets[key] = widget
                # Добавляем виджет, занимающий обе колонки
                self.form_layout.addRow(widget)
            else:
                widget = QLineEdit(str(current_val))
                widget.setPlaceholderText(hint)
                
                if p_type == "secret":
                    widget.setEchoMode(QLineEdit.Password)
                
                widget.setToolTip(f"ENV KEY: {key}")
                
                self.widgets[key] = widget
                self.form_layout.addRow(label_text, widget)

    def save_settings(self):
        try:
            changes_count = 0
            for key, widget in self.widgets.items():
                val = None
                if isinstance(widget, QCheckBox):
                    # Сохраняем "1" для True и "0" для False (надежнее для .env)
                    val = "1" if widget.isChecked() else "0"
                elif isinstance(widget, QLineEdit):
                    val = widget.text().strip()
                
                if val is not None:
                    # Сохраняем через менеджер
                    settings.save_setting(key, val)
                    changes_count += 1
            
            QMessageBox.information(self, "Перезагрузка", 
                                    f"Сохранено параметров: {changes_count}.\n\n"
                                    "⚠ ВНИМАНИЕ: Для применения новых API-ключей и режимов "
                                    "необходимо ПЕРЕЗАПУСТИТЬ приложение!")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка сохранения", f"Не удалось записать в .env:\n{e}")