# Fund Manager — инструкция по управлению (GUI)

Файл: `fund_manager.py`  
Запуск:
```bash
python fund_manager.py
```

Fund Manager — это “пульт управления”: он запускает скрипты проекта **в отдельных потоках**, прокидывает важные ENV-флаги и показывает консольный вывод внутри GUI.

---

## 0) Базовая логика “как думать про GUI”

В интерфейсе есть две ортогональные настройки:

1) **Execution Mode** (как исполняем):  
- BACKTEST → анализ/генерация, без live-сессии  
- PAPER / DEMO → исполняем через раннер (похоже на live по логике)  
- LIVE / REAL → реальная торговля (только если ALLOW_LIVE=ON)

2) **Universe Mode** (что торгуем):  
- Крипта (Bitget)  
- Биржа (MOEX через Tinkoff)  
- Крипта + Биржа (оба)

---

## 1) Вкладка CONTROL CENTER

### 1.1 OPTIMIZER CONFIGURATION (GENOME)
Это параметры “генома” оптимизатора (профиль стратегии):

- `SL min / SL max`
- `TP min / TP max`
- `CONF min / CONF max`
- `PULLBACK min / PULLBACK max`

Кнопка:
- **SAVE CONFIGURATION** — сохраняет параметры в `optimizer_settings.json` (профильно: crypto/stocks/both).

Также есть:
- **Optimizer profile**: `AUTO / CRYPTO / STOCKS / BOTH`  
  Это управляет тем, какие дефолтные диапазоны оптимизатора применять (удобно, когда рынок разный).

### 1.2 Telegram HTF и “лидеры рынка”
В этой же вкладке есть переключатели:
- **Telegram HTF → Crypto** / **Telegram HTF → Stocks**  
  Они управляют ENV:
  - `USE_TG_CRYPTO`
  - `USE_TG_STOCKS`

И “лидеры”:
- **Use leader for Crypto / Stocks**
- выпадающий список лидера (например BTCUSDT/ETHUSDT для крипты; MOEX/RTS/SBER для акций)

Это влияет на то, какие “ведущие” символы используются при сборе данных/признаков.

### 1.3 DIAGNOSTICS & ANALYTICS
Кнопки запускают соответствующие скрипты и выводят лог в консоль GUI:

- **GPU Check** → `test_gpu.py`
- **Leak Test** → `leak_test.py`
- **Noise Radar** → `noise_radar.py`
- **Stat Analyzer** → `stat_analyzer.py`
- **Balance Check** → `check_balance.py`
- **Prob Audit** → `inspect_probs.py`
- **Core Debug** → `debug_core.py`
- **Core No-Lookahead** → `test_core_no_lookahead.py`
- **Async Bitget** → `test_async_bitget.py`
- **Full Cycle Test** → `test_full_cycle.py`
- **Get Instruments** → `get_instruments.py`
- **Test Connections** → `test_connections.py`
- **Valid Rep** → `validation_report.py`
- **Debug Replay (no plots)** / **Debug Replay + Charts** → `debug_replayer.py` (с `--plot` или без)

**Практика перед реальным депозитом:**  
1) `Test Connections`  
2) `Balance Check`  
3) `Core No-Lookahead`  
4) `Debug Replay + Charts` на свежих данных

### 1.4 CORE PRODUCTION PIPELINE
Это основной “боевой” пайплайн:

- **1. SIGNAL GENERATOR (WALK, Full Reset)**  
  Запускает:
  ```bash
  python signal_generator.py --mode walk --reset --train_window N --trade_window M
  ```

- **1U. UNIVERSAL BRAIN (Cross-Asset WF)**  
  Запускает universal-генерацию.  
  В BACKTEST — синхронный режим.  
  В PAPER/LIVE — добавляет `--async_mode` и выбирает `--broker` исходя из выбранного юниверса.

- **1U-C. Train Crypto Brain**  
  Быстрый режим: переключает юниверс на CRYPTO и запускает universal.

- **1U-S. Train Stocks Brain**  
  Аналогично, но STOCKS.

- **2. GENETIC OPTIMIZER (Sniper Mode)**  
  Сначала сохраняет “геном”, затем запускает:
  ```bash
  python optimizer.py --mode sniper
  ```

- **3. DEBUG REPLAYER (Trace Report)**  
  Запускает `debug_replayer.py` (есть отдельные кнопки только crypto / только stocks).

---

## 2) WFO / Brain Surgery (слайдеры train/trade)

В интерфейсе есть слайдеры:
- **Train Window (Memory)** — сколько свечей используется как “память” (обучающий кусок).
- **Trade Window** — сколько свечей торгуем до следующего рефита.

GUI формирует CLI:
```bash
--train_window N --trade_window M
```

---

## 3) Вкладка WAR ROOM

Зона визуального анализа:
- выбор инструмента (Asset Selection)
- график цены/свечей
- график вероятностей/сигналов
- индикаторная панель

---

## 4) Вкладка LIVE MONITOR

Подключается к `ExecutionRouter` и показывает агрегированное состояние по брокерам.

### 4.1 Режим обновления
Кнопка **REFRESH NOW** обновляет снапшот вручную.  
Авто-обновление включается, когда Execution Mode = PAPER/LIVE.

### 4.2 Риск-панель (APPLY & SAVE)
Поля:
- **ALLOW_LIVE (arm real trading)**  
- `RISK_PER_TRADE`
- `MAX_OPEN_POSITIONS`
- `MAX_POSITION_NOTIONAL`
- `MAX_DAILY_DRAWDOWN`

Кнопка **APPLY & SAVE** делает:
- `Config.set_runtime(...)` + запись в `runtime_settings.json`

### 4.3 Таблицы POSITIONS / ORDERS
- POSITIONS — свод по позициям (Broker/Symbol/Qty/Avg/Last/PnL)
- ORDERS — текущие ордера (если брокер отдаёт)

---

## 5) Вкладка DATA FACTORY

Служебные снимки:
- signals snapshot (из `data_cache/production_signals_v1.pkl`)
- validation snapshot (из `validation_report.json`)

Кнопки:
- **REFRESH DATA SNAPSHOT**
- **REFRESH VALIDATION SNAPSHOT**

---

## 6) Как запускать торговую сессию из GUI

1) В CONTROL CENTER выбери:
   - Execution Mode = **PAPER** (сначала)  
   - Universe Mode = **CRYPTO** или **STOCKS** (лучше по одному)

2) Нажми:
   - **▶ START TRADING SESSION**

3) Открывай LIVE MONITOR и смотри:
   - equity/pnl
   - позиции
   - лог

4) Остановка:
   - **⏹ STOP TRADING**

---

## 7) Аварийная остановка (Kill-switch)

Файл: `state/kill_switch.json`

Включить:
```json
{"enabled": true, "reason": "manual"}
```

Выключить:
```json
{"enabled": false}
```

---

## 8) Рекомендация для безопасного роста депозита

- Один юниверс = один процесс (crypto отдельно, stocks отдельно).
- Микро-риск: `RISK_PER_TRADE = 0.1–0.25%`, `MAX_OPEN_POSITIONS = 1–2`.
- Сначала PAPER 1–2 дня, потом LIVE на минималке.
- Kill-switch держи под рукой — это нормально и профессионально.
