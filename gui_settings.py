from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QFormLayout, QLineEdit, 
                             QPushButton, QLabel, QMessageBox, QCheckBox, QDialogButtonBox, QWidget)
from PyQt5.QtCore import Qt
from settings_manager import settings

# --- СТИЛИ (Копия из fund_manager.py для единства дизайна) ---
SETTINGS_STYLESHEET = """
QDialog {
    background-color: #1e1e1e;
    color: #e0e0e0;
    font-family: 'Segoe UI', sans-serif;
    font-size: 10pt;
}
QLabel {
    color: #e0e0e0;
}
QLineEdit {
    background-color: #2d2d2d;
    color: #ffffff;
    border: 1px solid #454545;
    padding: 4px 6px;
    border-radius: 3px;
    selection-background-color: #007acc;
    selection-color: #ffffff;
}
QLineEdit:focus {
    border: 1px solid #007acc;
}
QCheckBox {
    color: #e0e0e0;
    spacing: 5px;
}
QCheckBox::indicator {
    width: 13px;
    height: 13px;
    border: 1px solid #555;
    background: #2d2d2d;
    border-radius: 2px;
}
QCheckBox::indicator:checked {
    background-color: #007acc;
    border: 1px solid #007acc;
}
QPushButton {
    background-color: #3c3c3c;
    border: 1px solid #555555;
    color: #ffffff;
    padding: 6px 12px;
    border-radius: 3px;
    min-width: 80px;
}
QPushButton:hover {
    background-color: #4a4a4a;
    border-color: #007acc;
}
QPushButton:pressed {
    background-color: #333333;
}
/* Заголовки разделов */
QLabel[role="header"] {
    font-weight: bold;
    color: #007acc;
    margin-top: 10px;
    margin-bottom: 5px;
    font-size: 11pt;
}
"""

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки бота (Config & Keys)")
        self.setModal(True)
        self.resize(450, 600)
        
        # Применяем темную тему
        self.setStyleSheet(SETTINGS_STYLESHEET)

        self.layout = QVBoxLayout(self)
        self.layout.setSpacing(10)
        self.layout.setContentsMargins(20, 20, 20, 20)
        
        self.form_layout = QFormLayout()
        self.form_layout.setVerticalSpacing(12)  # Больше воздуха между строками
        self.form_layout.setLabelAlignment(Qt.AlignLeft)

        # --- СПИСОК ПАРАМЕТРОВ ---
        self.params = [
            ("--- API Ключи ---", None, "header", ""),
            ("API Key", "EXCHANGE_API_KEY", "secret", "Ваш ключ биржи"),
            ("Secret Key", "EXCHANGE_SECRET_KEY", "secret", "Секретный ключ"),
            ("Passphrase", "EXCHANGE_PASSPHRASE", "secret", "Пароль фраза"),
            
            ("--- Торговля ---", None, "header", ""),
            ("Order Size (USDT)", "ORDER_SIZE", "number", "Размер ордера"),
            ("Leverage", "LEVERAGE", "number", "Плечо"),
            ("Take Profit %", "TAKE_PROFIT_PCT", "number", "Тейк профит"),
            ("Stop Loss %", "STOP_LOSS_PCT", "number", "Стоп лосс"),
            
            ("--- Риск ---", None, "header", ""),
            ("Max Daily Loss", "MAX_DAILY_LOSS", "number", "Лимит потерь"),
            ("Trailing Stop", "USE_TRAILING", "bool", "Включить трейлинг")
        ]

        self.widgets = {}
        self._build_ui()

    def _build_ui(self):
        for label, key, p_type, hint in self.params:
            if p_type == "header":
                # Красивый заголовок с отдельным стилем
                lbl = QLabel(label)
                lbl.setProperty("role", "header") # Используем свойство для CSS селектора
                self.form_layout.addRow(lbl)
                continue

            current_val = settings.get_setting(key, "")

            if p_type == "bool":
                widget = QCheckBox()
                widget.setChecked(str(current_val).lower() == 'true')
                # Для чекбокса лейбл ставим справа, а слева пусто (или можно переделать логику)
                self.form_layout.addRow(label, widget)
            else:
                widget = QLineEdit(str(current_val))
                widget.setPlaceholderText(hint)
                if p_type == "secret":
                    widget.setEchoMode(QLineEdit.Password)
                
                self.widgets[key] = widget
                self.form_layout.addRow(label, widget)

        self.layout.addLayout(self.form_layout)
        self.layout.addStretch()

        # Кнопки
        btn_layout = QFormLayout() # Или QHBoxLayout, но через DialogButtonBox проще
        
        self.btn_save = QPushButton("Сохранить")
        self.btn_cancel = QPushButton("Отмена")
        
        self.btn_save.clicked.connect(self.save_settings)
        self.btn_cancel.clicked.connect(self.reject)
        
        # Контейнер для кнопок справа
        button_box = QDialogButtonBox()
        button_box.addButton(self.btn_save, QDialogButtonBox.AcceptRole)
        button_box.addButton(self.btn_cancel, QDialogButtonBox.RejectRole)
        
        self.layout.addWidget(button_box)

    def save_settings(self):
        try:
            for key, widget in self.widgets.items():
                if isinstance(widget, QCheckBox):
                    val = str(widget.isChecked())
                elif isinstance(widget, QLineEdit):
                    val = widget.text().strip()
                else:
                    continue
                
                settings.save_setting(key, val)
            
            QMessageBox.information(self, "Успех", "Настройки сохранены! ✅")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить: {e}")