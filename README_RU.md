# AiTrade — документация проекта

> Репозиторий/архив: **AiTrade-main (архив AiTrade-main.zip)**  
> Назначение: единая система для **генерации сигналов**, **walk-forward обучения/валидации**, и **исполнения сделок** через несколько брокеров (Bitget + Tinkoff) с единым контролем рисков и GUI.

---

## 1) Что внутри (коротко)

**AiTrade** состоит из 4 больших слоёв:

1. **Data / Features**
   - `data_loader.py` — загрузка котировок/портфеля, подготовка данных.
   - `indicators.py`, `features_lib.py` — индикаторы и фичи.

2. **Model / Signals**
   - `model_engine.py` — модельный движок.
   - `signal_generator.py` — генерация сигналов (walk-forward + universal режимы).
   - `optimizer.py` — генетический оптимизатор параметров стратегии.
   - `production_signals_v1.pkl` (в `data_cache/`) — основной артефакт сигналов для исполнения.

3. **Execution (торговля / бумага / бэктест)**
   - `execution_router.py` — асинхронный роутер: символ → брокер → исполнение.
   - `brokers/` — клиенты брокеров:
     - `bitget_client.py`
     - `tinkoff_client.py`
     - `simulated_client.py`
   - `async_strategy_runner.py` — прод-раннер для PAPER/LIVE:
     - ledger + идемпотентность
     - protections (native / synthetic fallback)
     - kill-switch + heartbeat

4. **GUI / Ops**
   - `fund_manager.py` — управляющая панель (CONTROL / WAR ROOM / LIVE MONITOR / DATA FACTORY)
   - `live_monitor.py` — вспомогательный мониторинг (если используется отдельно)

---

## 2) Установка

### Требования
- Python 3.x
- Windows/Linux (GUI на PySide6)
- Доступ в интернет для загрузки данных и связи с API брокеров
- (Опционально) GPU для тяжёлых моделей — в requirements присутствует `torch` с CUDA.

### Установка зависимостей
```bash
pip install -r requirements.txt
```

### Файл окружения `.env`
Проект читает переменные окружения и `.env` (через `python-dotenv` в `config.py`).

Минимальный пример `.env`:

```env
# режимы
EXECUTION_MODE=backtest   # backtest / paper / live
UNIVERSE_MODE=both        # crypto / stocks / both
ALLOW_LIVE=0              # 1 чтобы разрешить реальную торговлю

# Bitget
BITGET_API_KEY=...
BITGET_API_SECRET=...
BITGET_API_PASSPHRASE=...
BITGET_BASE_URL=https://api.bitget.com

# Tinkoff
TINKOFF_API_TOKEN=...
TINKOFF_SANDBOX=1         # 1=песочница, 0=боевой

# алерты (опционально)
ALERTS_ENABLED=0
ALERT_TG_BOT_TOKEN=...
ALERT_TG_CHAT_ID=...
```

---

## 3) Конфигурация и режимы

### Режим исполнения (ExecutionMode)
Определяется через `EXECUTION_MODE`:
- `backtest` — никаких реальных ордеров, генерация/оценка.
- `paper` — “как live”, но без реальных денег (реализация зависит от брокера/симулятора).
- `live` — реальная торговля.

⚠️ Важно: даже если `EXECUTION_MODE=live`, **пока `ALLOW_LIVE=0`**, конфиг принудительно откатит режим в `paper`.

### Юниверс (UniverseMode)
Определяется через `UNIVERSE_MODE`:
- `crypto` — крипто-символы (Bitget)
- `stocks` — биржевые инструменты (Tinkoff/MOEX)
- `both` — совместно

### Маршрутизация тикеров к брокеру
В `config.py` есть `Config.ASSET_ROUTING`:
- например: `"BTCUSDT": "bitget"`, `"SBER": "tinkoff"`.

Для Tinkoff используется `Config.TINKOFF_FIGI_MAP` — соответствие тикер → FIGI.

---

## 4) Файлы состояния (state/) — важно для продакшена

По умолчанию раннер хранит “операционную память” в папке `state/`:

- `state/trades.sqlite` — **TradeLedger** (SQLite), база сделок и идемпотентность по `client_id`.
- `state/protections.json` — защиты позиции (SL/TP), включая native id’шники.
- `state/runner_state.json` — состояние раннера (последние метки, снапшоты и т.п.).
- `state/kill_switch.json` — kill-switch (ручной/аварийный стоп).
- `state/runner_heartbeat.json` — heartbeat для watchdog’а.

---

## 5) Kill-switch (аварийный стоп)

Раннер читает `state/kill_switch.json` и если там:
```json
{"enabled": true, "reason": "manual"}
```
то он:
1) отменяет **native protections** (если были),
2) закрывает **все позиции** (через `ExecutionRouter.close_all_positions()`),
3) очищает protections и завершает работу.

Чтобы снять kill-switch:
```json
{"enabled": false}
```

---

## 6) Быстрый запуск

### GUI (рекомендуется)
```bash
python fund_manager.py
```

### Генерация сигналов из CLI
Walk-forward:
```bash
python signal_generator.py --mode walk --reset --train_window 800 --trade_window 400
```

Universal Brain:
```bash
python signal_generator.py --mode universal --preset grinder --cross_asset_wf --train_window 800 --trade_window 400
```

### Запуск прод-раннера (PAPER/LIVE)
```bash
python async_strategy_runner.py --signals data_cache/production_signals_v1.pkl --loop --sleep 10
```

Полезно ограничить инструменты:
```bash
python async_strategy_runner.py --loop --assets BTCUSDT,ETHUSDT
```

---

## 7) Диагностика и тесты

Скрипты-диагносты (есть кнопки в GUI):
- `test_connections.py` — проверка подключений (Bitget/Tinkoff/ENV)
- `test_gpu.py` — проверка GPU окружения
- `test_async_bitget.py` — асинхронная проверка Bitget-клиента
- `test_core_no_lookahead.py` — sanity на “нет заглядывания в будущее”
- `test_full_cycle.py` — полный цикл пайплайна (если настроен)

---

## 8) Логи
- Файл: `trade_bot.log` (с ротацией) — настраивается в `config.py` через `setup_logging()`.

---

## 9) Расширение проекта (куда “в будущем”)
- Добавление нового брокера: реализовать интерфейс из `brokers/base.py`, зарегистрировать в `ExecutionRouter.get_broker()`, добавить маршрутизацию в `Config.ASSET_ROUTING`.
- Улучшение прод-безопасности: строгая нормализация price/qty под биржевые правила, отдельные risk-гварды по брокерам и отдельные процессы под crypto/stocks.
