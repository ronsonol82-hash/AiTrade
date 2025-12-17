# async_strategy_runner.py
from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import time
import os
import gc
import re
import pandas as pd

from datetime import datetime
from typing import Dict, Any
from utils.redis_connector import RedisSignalBus
from config import Config
from execution_router import ExecutionRouter
from risk_utils import calc_position_size
from state_store import atomic_read_json, atomic_write_json, atomic_read_pickle
from trade_ledger import TradeLedger
from notifier import TelegramAlerter


class AsyncStrategyRunner:
    """
    Прод-раннер (P0.8):
      - Ledger (SQLite) + idempotency на client_id
      - Atomic states: last_seen / snapshots / last_processed_ts
      - Protections: native (plan orders) если возможно, иначе synthetic fallback
    """

    def __init__(self,  router: ExecutionRouter | None = None, signals_file: str = "data_cache/production_signals_v1.pkl"):
        self.signals_file = signals_file
        self.signals: Dict[str, pd.DataFrame] = {}
        self.redis_bus = RedisSignalBus()
        self.router = router if router is not None else ExecutionRouter()

        # LIVE safety: сериализуем торговые действия + блокируем новые ордера при kill-switch
        self._trading_lock = asyncio.Lock()
        self._kill_switch_active = False

        self.assets_filter: list[str] | None = None
        self._protections: dict[str, dict] = {}

        self._state_dir = getattr(Config, "STATE_DIR", "state")
        self._runner_state_file = getattr(Config, "RUNNER_STATE_FILE", os.path.join(self._state_dir, "runner_state.json"))
        self._protections_file = getattr(Config, "PROTECTIONS_FILE", os.path.join(self._state_dir, "protections.json"))
        self._ledger_db = getattr(Config, "TRADE_DB_FILE", os.path.join(self._state_dir, "trades.sqlite"))

        self.ledger = TradeLedger(self._ledger_db)
        self.ledger.initialize()

        self._runner_state: dict[str, Any] = atomic_read_json(
            self._runner_state_file,
            {"last_seen": {}, "snapshots": {}, "last_processed_ts": {}},
        )
       
        # --- alerts ---
        self.alerter = TelegramAlerter(
            bot_token=getattr(Config, "ALERT_TG_BOT_TOKEN", ""),
            chat_id=getattr(Config, "ALERT_TG_CHAT_ID", ""),
            enabled=bool(getattr(Config, "ALERTS_ENABLED", False)),
        )

        # --- heartbeat (для watchdog) ---
        self._heartbeat_file = getattr(
            Config, "HEARTBEAT_FILE", os.path.join(self._state_dir, "runner_heartbeat.json")
        )
        self._heartbeat_every_s = float(getattr(Config, "HEARTBEAT_EVERY_S", 5.0) or 5.0)
        self._last_heartbeat_ts = 0.0

    # --- Сверка позиций ---
    async def reconcile_state(self):
        """
        Критически важная функция. Синхронизирует память бота с реальностью (брокером).
        Вызывается ПЕРЕД началом торгов.
        """
        print("🔄 [RECONCILE] Starting state reconciliation...")
        try:
            # 1. Запрашиваем у брокера (Bitget или Simulator), что у нас открыто
            # Важно: router.broker должен быть уже инициализирован
            real_positions = await self.router.broker.list_open_positions()
            
            # 2. Очищаем память роутера о позициях
            self.router._active_positions = {} 
            
            count = 0
            for pos in real_positions:
                # Фильтруем мусорные остатки (пыль)
                if abs(pos.quantity) > 0:
                    print(f"   Found existing position: {pos.symbol} Size: {pos.quantity:.4f} @ {pos.avg_price}")
                    
                    # 3. Восстанавливаем позицию в структуру роутера
                    # Структура должна совпадать с тем, что ждет execute_trade
                    self.router._active_positions[pos.symbol] = {
                        'size': pos.quantity,
                        'entry_price': pos.avg_price, 
                        'side': 'long' if pos.quantity > 0 else 'short',
                        'last_update': datetime.utcnow()
                    }
                    count += 1
                    
            print(f"✅ [RECONCILE] Complete. Restored {count} active positions.")
            
        except Exception as e:
            print(f"❌ [RECONCILE FATAL ERROR]: {e}")
            # Если сверка упала — лучше не торговать, иначе наломаем дров
            raise e

    async def initialize(self) -> None:
        await self.router.initialize()
        self.load_signals()
        
        # --- Reconcile Router Memory ---
        # Сначала восстанавливаем память роутера, чтобы он знал о позициях
        await self.reconcile_state() 
        
        # --- Reconcile Protections ---
        self._protections = atomic_read_json(self._protections_file, {}) or {}
        self._reconcile_protections()

        # (4) reconcile ledger (это старый метод, он сверяет базу данных сделок)
        await self._reconcile_on_startup()
    
    def _reconcile_protections(self) -> None:
        """
        Синхронизирует protections с реальными позициями роутера.
        - Удаляет protections для символов без позиции (SL/TP сработал пока бот был выключен)
        - Предупреждает о позициях без protections (открыты вручную или потеряны)
        """
        if not self._protections:
            return
        
        # Получаем символы с реальными позициями
        active_symbols = set(self.router._active_positions.keys())
        protection_symbols = set(self._protections.keys())
        
        # 1) Удаляем "мёртвые" protections (позиция закрылась на бирже)
        orphaned_protections = protection_symbols - active_symbols
        for sym in orphaned_protections:
            prot = self._protections.pop(sym, {})
            print(f"🧹 [RECONCILE] Removed orphaned protection for {sym} (position closed on exchange)")
            
            # Закрываем trade в ledger если есть
            trade_id = prot.get("trade_id")
            if trade_id:
                try:
                    # Используем 0.0 как exit_price т.к. не знаем реальную цену закрытия
                    self.ledger.close_trade(trade_id, 0.0, "reconcile_protection_orphaned")
                except Exception as e:
                    print(f"   [WARN] Failed to close trade {trade_id}: {e}")
        
        # 2) Предупреждаем о позициях без protections
        unprotected_positions = active_symbols - protection_symbols
        for sym in unprotected_positions:
            pos_info = self.router._active_positions.get(sym, {})
            print(f"⚠️  [RECONCILE] Position {sym} has NO protections! Size: {pos_info.get('size', '?')}")
            # TODO: можно автоматически поставить synthetic SL на основе ATR
        
        # 3) Синхронизируем qty/entry_price в существующих protections
        for sym in (protection_symbols & active_symbols):
            pos_info = self.router._active_positions.get(sym, {})
            prot = self._protections.get(sym, {})
            
            real_qty = pos_info.get('size', 0.0)
            real_entry = pos_info.get('entry_price', 0.0)
            
            # Обновляем если есть расхождение
            if prot.get("qty") != real_qty:
                print(f"🔄 [RECONCILE] {sym}: qty {prot.get('qty')} -> {real_qty}")
                prot["qty"] = real_qty
            
            if real_entry > 0 and prot.get("entry_price", 0) != real_entry:
                print(f"🔄 [RECONCILE] {sym}: entry_price {prot.get('entry_price')} -> {real_entry}")
                prot["entry_price"] = real_entry
        
        # 4) Сохраняем очищенные protections
        if orphaned_protections:
            self._persist_protections()
            print(f"🛡️  Reconciled protections: {len(self._protections)} active")
        elif self._protections:
            print(f"🛡️  Restored protections: {len(self._protections)}")

    def set_assets(self, assets: list[str]):
        self.assets_filter = list(assets) if assets else None

    @staticmethod
    def _safe_ts(val: Any) -> str:
        try:
            if isinstance(val, (pd.Timestamp, datetime)):
                return val.isoformat()
            return str(val)
        except Exception:
            return "na"

    def _make_signal_id(self, symbol: str, df: pd.DataFrame, last_row: pd.Series) -> str:
        try:
            ts = df.index[-1]
        except Exception:
            ts = last_row.get("timestamp") or last_row.get("ts")
        raw = "|".join([symbol, self._safe_ts(ts), str(last_row.get("p_long", "")), str(last_row.get("p_short", ""))])
        h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
        return f"{symbol}-{h}"

    def _make_trade_id(self, broker: str, symbol: str, signal_id: str) -> str:
        raw = f"{broker}|{symbol}|{signal_id}"
        return "tr-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]

    def _make_client_id(self, broker: str, symbol: str, role: str, signal_id: str) -> str:
        raw = f"{broker}|{symbol}|{role}|{signal_id}"
        h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
        b = re.sub(r"[^A-Za-z0-9]", "", broker)[:6]
        s = re.sub(r"[^A-Za-z0-9]", "", symbol)[:10]
        r = re.sub(r"[^A-Za-z0-9]", "", role)[:6]
        return f"{b}{s}{r}{h}"

    async def _router_execute_order(self, *, symbol: str, side: str, quantity: float, order_type: str, client_id: str | None = None):
        if getattr(self, "_kill_switch_active", False):
            raise RuntimeError("KILL-SWITCH active: new orders blocked")    
        
        fn = getattr(self.router, "execute_order")
        sig = inspect.signature(fn)
        kwargs = {"symbol": symbol, "side": side, "quantity": quantity, "order_type": order_type}
        if client_id and "client_id" in sig.parameters:
            kwargs["client_id"] = client_id
        return await fn(**kwargs)
    
    @staticmethod
    def _mode_value() -> str:
        mode_obj = getattr(Config, "EXECUTION_MODE", None)
        return mode_obj.value if hasattr(mode_obj, "value") else str(mode_obj or "").lower()

    def _strict_protections_enabled(self) -> bool:
        # по умолчанию: в LIVE защиты обязательны
        strict = bool(getattr(Config, "STRICT_PROTECTIONS_LIVE", True))
        return self._mode_value() == "live" and strict

    async def _panic_close_unprotected(
        self,
        *,
        symbol: str,
        broker_name: str,
        trade_id: str,
        reason: str,
        signal_id: str | None = None,
    ) -> None:
        """
        Если защиты (SL/TP) не поставились — это не LIVE.
        Пытаемся закрыть позицию немедленно и корректно записать это в ledger.
        """
        # (1) узнаём реальный объём позиции
        try:
            positions = await self.router.list_all_positions()
        except Exception:
            positions = []

        p = next((x for x in positions if x.symbol == symbol and float(getattr(x, "quantity", 0.0) or 0.0) > 0), None)
        if p is None:
            print(f"ℹ️  PANIC-CLOSE: позиции уже нет {symbol}")
            self._protections.pop(symbol, None)
            self._persist_protections()
            return

        qty_to_close = float(getattr(p, "quantity", 0.0) or 0.0)
        if qty_to_close <= 0:
            print(f"ℹ️  PANIC-CLOSE: qty=0 {symbol}")
            self._protections.pop(symbol, None)
            self._persist_protections()
            return

        # (2) идемпотентный client_id на аварийный выход
        sid = (signal_id or (self.ledger.get_open_trade(broker_name, symbol) or {}).get("signal_id") or "panic")
        exit_client_id = self._make_client_id(broker_name, symbol, "pxit", sid)

        if not self.ledger.reserve_order(
            exit_client_id,
            broker=broker_name,
            symbol=symbol,
            role="panic_exit",
            side="sell",
            payload={"reason": reason, "qty": qty_to_close},
        ):
            print(f"🧾 Ledger: PANIC EXIT уже делали (client_id={exit_client_id}) → пропуск")
            return

        # (3) MARKET SELL
        try:
            res = await self._router_execute_order(
                symbol=symbol,
                side="sell",
                quantity=qty_to_close,
                order_type="market",
                client_id=exit_client_id,
            )
        except Exception as e:
            self.ledger.mark_order_final(exit_client_id, "failed", payload={"error": str(e), "reason": reason})
            print(f"❌ PANIC EXIT submit failed {symbol}: {e}")
            return

        self.ledger.mark_order_submitted(
            exit_client_id, str(getattr(res, "order_id", "")), payload={"qty": qty_to_close, "reason": reason}
        )

        st = (str(getattr(res, "status", "")) or "").lower()
        px = float(getattr(res, "price", 0.0) or 0.0)

        final_statuses = {"filled", "canceled", "cancelled", "rejected", "failed"}

        if st in final_statuses:
            st2 = "canceled" if st in {"canceled", "cancelled"} else st
            self.ledger.mark_order_final(exit_client_id, st2, payload={"price": px, "reason": reason})
            if st2 == "filled":
                try:
                    self.ledger.close_trade(trade_id, px, reason)
                except Exception:
                    pass
        else:
            # pending/unknown — reconcile добьёт
            print(f"⏳ PANIC EXIT {symbol}: status={st or 'unknown'} → ждём reconcile")

        # локально гасим/чистим защиты
        if symbol in self._protections:
            self._protections.pop(symbol, None)
            self._persist_protections()

    def load_signals(self) -> None:
        # Пробуем читать из Redis
        redis_signals = self.redis_bus.get_signals()
        
        if redis_signals:
            self.signals = redis_signals
            print(f"📊 [REDIS] Signals loaded for {len(self.signals)} assets")
        else:
            # Fallback на файл, если Redis пуст
            print("⚠️ [REDIS] Empty, falling back to file...")
            self.signals = atomic_read_pickle(self.signals_file, {}) or {}
            
        try:
            self._signals_mtime = os.path.getmtime(self.signals_file)
        except Exception:
            self._signals_mtime = None

    def _maybe_reload_signals(self) -> None:
        """
        Читаем свежие сигналы из Redis на каждом цикле.
        Это быстро, так как Redis in-memory.
        """
        # Просто вызываем load_signals, который теперь ходит в Redis
        # Можно добавить проверку флага или TTL, но чтение из Redis дешевое.
        new_signals = self.redis_bus.get_signals()
        if new_signals:
            self.signals = new_signals
        try:
            mtime = os.path.getmtime(self.signals_file)
        except Exception:
            return
        if getattr(self, "_signals_mtime", None) is None:
            self._signals_mtime = mtime
            return
        if mtime > self._signals_mtime:
            self.load_signals()

    def _persist_state(self) -> None:
        atomic_write_json(self._runner_state_file, self._runner_state)

    def _touch_heartbeat(self, status: str, *, note: str = "", extra: dict | None = None) -> None:
        """
        Пишем heartbeat-файл, который внешний watchdog может мониторить.
        status: alive/ok/error/stopped
        """
        now_ts = time.time()
        if (now_ts - float(getattr(self, "_last_heartbeat_ts", 0.0) or 0.0)) < self._heartbeat_every_s:
            return

        self._last_heartbeat_ts = now_ts

        payload = {
            "updated_at": datetime.utcnow().isoformat(),
            "ts": now_ts,
            "pid": os.getpid(),
            "status": str(status),
            "note": str(note or ""),
            "mode": self._mode_value(),
            "universe": str(getattr(Config, "UNIVERSE_MODE", "")),
        }
        if extra:
            payload["extra"] = extra

        atomic_write_json(self._heartbeat_file, payload)

    def _persist_protections(self) -> None:
        atomic_write_json(self._protections_file, self._protections)

    def _read_kill_switch(self) -> dict:
        path = getattr(Config, "KILL_SWITCH_FILE", os.path.join(self._state_dir, "kill_switch.json"))
        return atomic_read_json(path, {}) or {}

    def _kill_switch_enabled(self) -> bool:
        data = self._read_kill_switch()
        return bool(data.get("enabled", False))

    async def _handle_kill_switch(self, reason: str = "manual") -> None:
        await self.alerter.send(f"🧯 KILL-SWITCH: {reason}")
        print(f"🧯 KILL-SWITCH ENABLED: {reason}")

        # 0) блокируем любые новые ордера из стратегии/защит
        self._kill_switch_active = True

        # 1) сериализуем действия (защиты/закрытия) — чтобы не было гонок
        async with self._trading_lock:
            try:
                # (A) сначала гасим НАТИВНЫЕ plan-ордера (SL/TP)
                if self._protections:
                    for sym, prot in list(self._protections.items()):
                        if not prot or prot.get("mode") != "native":
                            continue
                        try:
                            br = await self.router.get_broker_for_symbol(sym)
                            await self._cancel_native_protections(sym, br, prot)
                        except Exception as e:
                            print(f"[WARN] kill-switch: cancel native protections failed for {sym}: {e}")

                # (B) затем закрываем позиции (router сам отменяет обычные активные ордера)
                await self.router.close_all_positions(reason=reason)

            finally:
                self._protections = {}
                self._persist_protections()

    async def _cancel_native_protections(self, symbol: str, broker, prot: dict) -> None:
        """
        (3) Отмена нативных защит (если они есть).
        Структура prot должна содержать:
          prot["native"]["sl"]["order_id"], prot["native"]["tp"]["order_id"]
        """
        if not prot or prot.get("mode") != "native":
            return
        native = prot.get("native", {}) or {}
        for leg in ("sl", "tp"):
            od = (native.get(leg) or {}).get("order_id")
            if not od:
                continue
            if hasattr(broker, "cancel_plan_order"):
                try:
                    await broker.cancel_plan_order(order_id=str(od))
                    print(f"🧹 native {symbol} {leg} cancelled: {od}")
                except Exception as e:
                    print(f"[WARN] cancel_plan_order failed {symbol} {leg}: {e}")

    async def _reconcile_on_startup(self) -> None:
        """
        (4) reconcile при старте:
          - если в брокере есть позиция, а в ledger нет open trade -> создаём 'orphan' trade
          - если в ledger есть open trade, а позиции нет -> закрываем trade (reason=reconcile_missing_position)
          - если есть native protections и позиция уже закрыта -> чистим protections
        """
        try:
            positions = await self.router.list_all_positions()
        except Exception:
            positions = []
        pos_map = {p.symbol: p for p in positions}

        open_trades = self.ledger.list_open_trades()

        # 1) ledger open, но позиции нет -> закрыть
        for t in open_trades:
            sym = t.get("symbol")
            broker_name = (t.get("broker") or "").lower()
            if not sym:
                continue
            p = pos_map.get(sym)
            if not p or float(getattr(p, "quantity", 0.0) or 0.0) <= 0:
                # закрываем “как факт”: позиции нет
                try:
                    br = await self.router.get_broker_for_symbol(sym)
                    px = float(await br.get_current_price(sym))
                except Exception:
                    px = float(t.get("entry_price") or 0.0)
                self.ledger.close_trade(t["trade_id"], px, "reconcile_missing_position")
                self._protections.pop(sym, None)

        # 2) позиция есть, но ledger open trade нет -> создать orphan trade
        for sym, p in pos_map.items():

            self._protections.pop(sym, None)

            broker_name = (getattr(p, "broker", "") or "").lower() or "router"
            if self.ledger.has_open_trade(broker_name, sym):
                continue
            qty = float(getattr(p, "quantity", 0.0) or 0.0)
            if qty <= 0:
                continue

            trade_id = f"reconcile-{broker_name}-{sym}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            entry_client_id = f"reconcile-entry-{broker_name}-{sym}"
            self.ledger.upsert_trade(
                trade_id=trade_id,
                strategy_id=getattr(Config, "STRATEGY_ID", "universal"),
                broker=broker_name,
                symbol=sym,
                side="buy",
                signal_id="reconcile_orphan_position",
                entry_client_id=entry_client_id,
            )
            self.ledger.set_trade_entry(trade_id, float(getattr(p, "avg_price", 0.0) or 0.0), qty)
            print(f"🧾 Reconcile: created orphan trade for {broker_name}:{sym} qty={qty}")

        self._persist_protections()

    @staticmethod
    def _compute_risk_per_trade(confidence: float, base_risk: float, max_risk: float, threshold: float) -> float:
        if confidence is None:
            return base_risk
        scale = (confidence - threshold) / (1.0 - threshold + 1e-6)
        scale = max(0.0, min(1.0, scale))
        risk = base_risk + (max_risk - base_risk) * scale
        return max(base_risk, min(max_risk, risk))
    
    async def _update_dynamic_trailing(self, symbol: str, current_price: float, prot: dict, is_whale_active: bool = False) -> bool:
        """
        [LIVE-READY] Moon Mode Lite: динамический трейлинг защит.

        Поддерживает ДВА режима:
        1) mode="synthetic":
            - двигаем prot["sl"] (реальный выход MARKET делает _check_protective_exits)
        2) mode="native" (РЕАЛЬНАЯ ТОРГОВЛЯ):
            - двигаем реальный SL план-ордер у брокера:
                    cancel старого SL -> place нового SL (tp не трогаем)

        Возвращает True, если что-то реально обновили (чтобы вызывающий код поставил dirty=True).
        """

        # --- safety guards ---
        if getattr(self, "_kill_switch_active", False):
            return False
        if not isinstance(prot, dict):
            return False

        mode = (str(prot.get("mode", "synthetic")) or "synthetic").lower().strip()
        if mode not in ("synthetic", "native"):
            return False

        trade_id = str(prot.get("trade_id") or "").strip()
        if not trade_id:
            return False

        # --- safe float helper ---
        def _f(x, default: float = 0.0) -> float:
            try:
                v = float(x)
                if v != v:  # NaN
                    return default
                return v
            except Exception:
                return default

        cp = _f(current_price, 0.0)
        if cp <= 0:
            return False

        sl_price = _f(prot.get("sl"), 0.0)
        atr = _f(prot.get("atr"), 0.0)
        qty = _f(prot.get("qty"), 0.0)

        # без SL/ATR/qty двигать нечего
        if sl_price <= 0 or atr <= 0 or qty <= 0:
            return False

        # --- config knobs (все с дефолтами, чтобы не падать если нет в Config) ---
        enabled = bool(getattr(Config, "DYNAMIC_TRAILING_ENABLED", True))
        if not enabled:
            return False

        breakeven_atr = _f(getattr(Config, "DYNAMIC_TRAIL_BREAKEVEN_ATR", 1.0), 1.0)
        breakeven_buffer_atr = _f(getattr(Config, "DYNAMIC_TRAIL_BREAKEVEN_BUFFER_ATR", 0.05), 0.05)

        trigger_dist_atr = _f(getattr(Config, "DYNAMIC_TRAIL_TRIGGER_ATR", 2.5), 2.5)
        trail_offset_atr = _f(getattr(Config, "DYNAMIC_TRAIL_OFFSET_ATR", 0.8), 0.8)

        min_step_atr = _f(getattr(Config, "DYNAMIC_TRAIL_MIN_STEP_ATR", 0.10), 0.10)
        cooldown_s = _f(getattr(Config, "DYNAMIC_TRAIL_COOLDOWN_S", 5.0), 5.0)

        # минимальный зазор от текущей цены (спрэд/шум)
        min_gap_pct = _f(getattr(Config, "DYNAMIC_TRAIL_MIN_GAP_PCT", 0.001), 0.001)  # 0.1%
        min_gap = max(cp * min_gap_pct, atr * 0.05)

        # --- anti-chatter: cooldown ---
        now_ts = time.time()
        last_ts = _f(prot.get("trail_last_ts"), 0.0)
        if cooldown_s > 0 and last_ts > 0 and (now_ts - last_ts) < cooldown_s:
            return False

        # --- local price watermark (max for LONG, min for SHORT) ---
        if qty > 0:
            # LONG: отслеживаем максимум цены
            prev_max = _f(prot.get("max_price"), 0.0)
            if prev_max <= 0:
                prev_max = cp
            max_price = max(prev_max, cp)
            prot["max_price"] = max_price
            watermark_price = max_price
        else:
            # SHORT: отслеживаем минимум цены
            prev_min = _f(prot.get("min_price"), 0.0)
            if prev_min <= 0:
                prev_min = cp
            min_price = min(prev_min, cp)
            prot["min_price"] = min_price
            watermark_price = min_price

        # --- entry price: кешируем из prot / ledger / open_trade ---
        entry_price = _f(prot.get("entry_price"), 0.0)

        if entry_price <= 0 and hasattr(self.ledger, "get_trade_entry_price"):
            try:
                entry_price = _f(self.ledger.get_trade_entry_price(trade_id), 0.0)
            except Exception:
                entry_price = 0.0

        if entry_price <= 0:
            broker_name_guess = (str(prot.get("broker") or "")).lower().strip()
            if broker_name_guess and hasattr(self.ledger, "get_open_trade"):
                try:
                    ot = self.ledger.get_open_trade(broker_name_guess, symbol) or {}
                    entry_price = _f(ot.get("entry_price"), 0.0)
                except Exception:
                    entry_price = 0.0

        if entry_price > 0:
            prot["entry_price"] = entry_price

        # --- stage logic: выбираем лучший кандидат SL (только вверх) ---
        new_sl_candidate: float | None = None

        # 1) breakeven стадия
        if entry_price > 0:
            if qty > 0:
                # LONG: профит = цена выросла
                profit = cp - entry_price
            else:
                # SHORT: профит = цена упала
                profit = entry_price - cp
            
            profit_atr = (profit / atr) if atr > 0 else 0.0
            
            if profit_atr >= breakeven_atr:
                if qty > 0:
                    # LONG: SL чуть выше entry
                    be_sl = entry_price + (atr * breakeven_buffer_atr)
                    be_sl = min(be_sl, cp - min_gap)
                    if be_sl > sl_price:
                        new_sl_candidate = be_sl
                else:
                    # SHORT: SL чуть ниже entry
                    be_sl = entry_price - (atr * breakeven_buffer_atr)
                    be_sl = max(be_sl, cp + min_gap)
                    if be_sl < sl_price:
                        new_sl_candidate = be_sl

        # 2) агрессивный трейл (SQUEEZE LOGIC)
        current_dist = abs(cp - sl_price)
        if current_dist > (atr * trigger_dist_atr):
            
            # Достаем TP из protections
            tp_price_val = _f(prot.get("tp"), 0.0)
            # [FIX START] Логика ширины трейлинга
            # Дефолтный отступ
            base_offset = atr * trail_offset_atr
            
            # Если обнаружен КИТ (Whale), даем цене дышать (как в execution_core: 4.5 ATR вместо узкого стопа)
            if is_whale_active:
                # В execution_core было: current_trail_mult = 4.5
                # Здесь мы динамически расширяем оффсет
                base_offset = atr * 4.5 
                print(f"🐋 WHALE DETECTED on {symbol}: Widening trail to 4.5 ATR")
            
            # Если TP есть, считаем Squeeze (сжатие пружины), но не зажимаем кита
            if tp_price_val > 0 and not is_whale_active:
                if qty > 0:  # LONG
                    dist_remain = tp_price_val - watermark_price
                    total_run = tp_price_val - entry_price
                else:  # SHORT
                    dist_remain = watermark_price - tp_price_val  # watermark_price = min_price
                    total_run = entry_price - tp_price_val
                
                if total_run <= 0: squeeze_factor = 1.0
                else: squeeze_factor = dist_remain / total_run
                
                # Клиппинг фактора (0..1)
                squeeze_factor = max(0.0, min(1.0, squeeze_factor))
                
                # Динамический отступ: сужаем базу, но не меньше 10% от базы
                dynamic_offset = base_offset * squeeze_factor
                dynamic_offset = max(dynamic_offset, base_offset * 0.1)
            else:
                # Если TP нет (Moon Mode?), используем линейный отступ
                dynamic_offset = base_offset

            if qty > 0:  # LONG
                trail_sl = watermark_price - dynamic_offset
                trail_sl = min(trail_sl, cp - min_gap)  # Защита от пересечения цены
                if new_sl_candidate is None:
                    new_sl_candidate = trail_sl
                else:
                    new_sl_candidate = max(new_sl_candidate, trail_sl)
            
            else:  # SHORT
                trail_sl = watermark_price + dynamic_offset  # watermark_price = min_price для SHORT
                trail_sl = max(trail_sl, cp + min_gap)  # SL должен быть ВЫШЕ текущей цены
                if new_sl_candidate is None:
                    new_sl_candidate = trail_sl
                else:
                    new_sl_candidate = min(new_sl_candidate, trail_sl)

            # Защита: стоп не должен пересекать цену (gap)
            if qty > 0:
                trail_sl = min(trail_sl, cp - min_gap)
            else:
                trail_sl = max(trail_sl, cp + min_gap)

            if new_sl_candidate is None:
                new_sl_candidate = trail_sl
            else:
                # Для лонга тянем вверх (max), для шорта тянем вниз (min)
                if qty > 0:
                    new_sl_candidate = max(new_sl_candidate, trail_sl)
                else:
                    new_sl_candidate = min(new_sl_candidate, trail_sl)

        if new_sl_candidate is None:
            return False

        # --- финальные проверки: шаг и направление ---
        new_sl = float(new_sl_candidate)
        
        if qty > 0:
            # LONG: SL должен быть ниже цены, двигаем только вверх
            new_sl = min(new_sl, cp - min_gap)  # не в упор к цене
            min_step = atr * min_step_atr
            if new_sl <= (sl_price + min_step):
                return False
        else:
            # SHORT: SL должен быть выше цены, двигаем только вниз
            new_sl = max(new_sl, cp + min_gap)  # не в упор к цене
            min_step = atr * min_step_atr
            if new_sl >= (sl_price - min_step):
                return False

        # =========================
        # MODE: SYNTHETIC
        # =========================
        if mode == "synthetic":
            prot["sl"] = new_sl
            prot["trail_last_ts"] = now_ts
            prot["trail_count"] = int(_f(prot.get("trail_count"), 0.0)) + 1

            print(
                f"🚀 MOON MODE {symbol} [synthetic {'SHORT' if qty < 0 else 'LONG'}]: SL {sl_price:.6f} -> {new_sl:.6f} "
                f"(cp={cp:.6f}, atr={atr:.6f}, wm={watermark_price:.6f})"
            )
            
            # CRITICAL: Немедленный persist после изменения SL
            self._persist_protections()
            
            return True

        # =========================
        # MODE: NATIVE (REAL TRADING)
        # =========================
        # В не-LIVE не трогаем биржу, но локально обновим для отладки/симуляции.
        if self._mode_value() != "live":
            prot["sl"] = new_sl
            prot["trail_last_ts"] = now_ts
            prot["trail_count"] = int(_f(prot.get("trail_count"), 0.0)) + 1
            print(
                f"🚀 MOON MODE {symbol} [native-sim {'SHORT' if qty < 0 else 'LONG'}]: SL {sl_price:.6f} -> {new_sl:.6f} (no-broker, mode={self._mode_value()})"
            )
            # CRITICAL: Немедленный persist после изменения SL
            self._persist_protections()
            return True

        # LIVE native: cancel+replace SL только под lock (чтобы не было гонок с kill-switch/exit)
        async with self._trading_lock:
            # 1) получаем брокера
            try:
                broker = await self.router.get_broker_for_symbol(symbol)
            except Exception as e:
                print(f"[WARN] {symbol}: native trail skipped (broker resolve failed): {e}")
                return False

            broker_name = (str(getattr(broker, "name", "")) or str(prot.get("broker") or "router")).lower().strip()

            # 2) нормализуем цену, если брокер умеет (не обяз.)
            if hasattr(broker, "normalize_price"):
                try:
                    new_sl = float(broker.normalize_price(symbol, new_sl))
                except Exception:
                    pass

            # 3) достаем текущий SL order_id из prot["native"]["sl"]["order_id"]
            native = prot.get("native", {}) or {}
            old_sl_order_id = str(((native.get("sl") or {}).get("order_id") or "")).strip()

            # 4) генерим новый client_id (уникальный) и пишем в ledger как отдельный action
            base_sid = str(prot.get("signal_id") or "na")
            trail_sid = f"{base_sid}|trail|{int(new_sl * 1_000_000)}"
            new_sl_client_id = self._make_client_id(broker_name, symbol, "slt", trail_sid)

            # reserve (если не прошло — значит уже пытались этот конкретный шаг)
            try:
                ok = self.ledger.reserve_order(
                    new_sl_client_id,
                    broker=broker_name,
                    symbol=symbol,
                    role="sl_trail",
                    side="sell",
                    payload={"sl": new_sl, "qty": qty, "prev_sl": sl_price, "prev_order_id": old_sl_order_id},
                )
            except Exception:
                ok = True  # если ledger по какой-то причине недоступен — не блокируем трейлинг

            if not ok:
                return False

            # 5) отменяем старый SL (TP НЕ трогаем)
            if old_sl_order_id and hasattr(broker, "cancel_plan_order"):
                try:
                    await broker.cancel_plan_order(order_id=str(old_sl_order_id))
                    # старый client_id (если был) финализируем как canceled
                    old_client_id = prot.get("sl_client_id")
                    if old_client_id:
                        try:
                            self.ledger.mark_order_final(old_client_id, "canceled", payload={"replaced_by": new_sl_client_id})
                        except Exception:
                            pass
                except Exception as e:
                    try:
                        self.ledger.mark_order_final(new_sl_client_id, "failed", payload={"error": f"cancel_old_sl_failed: {e}"})
                    except Exception:
                        pass
                    print(f"[WARN] {symbol}: native trail cancel_old_sl failed: {e}")
                    return False

            # 6) ставим новый SL (tp_price=None — чтобы TP остался как был)
            if not hasattr(broker, "place_protection_orders"):
                try:
                    self.ledger.mark_order_final(new_sl_client_id, "failed", payload={"error": "broker_has_no_place_protection_orders"})
                except Exception:
                    pass
                print(f"[WARN] {symbol}: native trail skipped (broker has no place_protection_orders)")
                return False

            # qty normalize (как у тебя в entry)
            try:
                qty_n = float(getattr(broker, "normalize_qty", lambda s, q, p=None: q)(symbol, qty, cp))
            except Exception:
                qty_n = qty
            if qty_n <= 0:
                qty_n = qty

            try:
                r = await broker.place_protection_orders(
                    symbol,
                    qty=float(qty_n),
                    sl_price=float(new_sl),
                    tp_price=None,
                    sl_client_oid=new_sl_client_id,
                    tp_client_oid=None,
                )

                new_order_id = str(((r or {}).get("sl") or {}).get("order_id") or "").strip()
                if not new_order_id:
                    raise RuntimeError("Native SL placement returned empty order_id")

                try:
                    self.ledger.mark_order_submitted(new_sl_client_id, new_order_id, payload={"sl": new_sl, "qty": qty_n})
                except Exception:
                    pass

                # 7) обновляем prot
                prot["sl"] = float(new_sl)
                prot["sl_client_id"] = new_sl_client_id
                prot["trail_last_ts"] = now_ts
                prot["trail_count"] = int(_f(prot.get("trail_count"), 0.0)) + 1

                prot["native"] = prot.get("native", {}) or {}
                prot["native"]["sl"] = {
                    "order_id": str(new_order_id),
                    "prev_order_id": str(old_sl_order_id) if old_sl_order_id else None,
                    "updated_at": datetime.utcnow().isoformat(),
                }

                print(
                    f"🚀 MOON MODE {symbol} [native {'SHORT' if qty < 0 else 'LONG'}]: SL {sl_price:.6f} -> {new_sl:.6f} "
                    f"(cp={cp:.6f}, atr={atr:.6f}, wm={watermark_price:.6f}, oid={new_order_id})"
                )
                
                # CRITICAL: Немедленный persist после изменения SL на бирже
                self._persist_protections()
                
                return True

            except Exception as e:
                try:
                    self.ledger.mark_order_final(new_sl_client_id, "failed", payload={"error": str(e), "sl": new_sl})
                except Exception:
                    pass

                # Если мы в LIVE strict — лучше закрыться, чем остаться без защиты
                if self._strict_protections_enabled():
                    try:
                        await self._panic_close_unprotected(
                            symbol=symbol,
                            broker_name=broker_name,
                            trade_id=trade_id,
                            reason="native_sl_trail_failed",
                            signal_id=str(prot.get("signal_id") or "na"),
                        )
                    except Exception:
                        pass

                print(f"[WARN] {symbol}: native trail failed: {e}")
                return False

    async def _check_protective_exits(self) -> None:
        if not self._protections:
            return

        try:
            positions = await self.router.list_all_positions()
        except Exception:
            positions = []
        pos_map = {p.symbol: p for p in positions}

        to_remove: list[str] = []
        dirty = False

        for symbol, prot in list(self._protections.items()):
            mode = prot.get("mode", "synthetic")
            broker_name = (prot.get("broker") or "").lower() or "router"
            trade_id = prot.get("trade_id")

            pos = pos_map.get(symbol)
            qty_pos = float(getattr(pos, "quantity", 0.0) or 0.0)

            # 1) ВСЕГДА получаем broker + current_price в начале цикла
            try:
                broker = await self.router.get_broker_for_symbol(symbol)
                current_price = float(await broker.get_current_price(symbol))
            except Exception:
                continue

            # [FIX START] Пытаемся понять, есть ли след кита на последней свече
            is_whale = False
            # Ищем символ в загруженных сигналах (self.signals)
            if symbol in self.signals and not self.signals[symbol].empty:
                try:
                    # Берем последнюю строку
                    last_row = self.signals[symbol].iloc[-1]
                    # Проверяем флаг (если он есть в features_lib)
                    if last_row.get('whale_footprint', 0) > 0:
                        is_whale = True
                except Exception:
                    pass

            # [FIX] Вызов динамического трейлинга
            if await self._update_dynamic_trailing(symbol, current_price, prot):
                dirty = True

            # сохраняем last_price (полезно для reconcile/логов)
            prot["last_price"] = current_price
            dirty = True

            # --- PENDING ENTRY MODE ---
            if mode == "pending_entry":
                # пока позиции нет — просто ждём
                if qty_pos <= 0:
                    created_at = prot.get("created_at")
                    if created_at:
                        try:
                            created_dt = datetime.fromisoformat(str(created_at).replace("Z", ""))
                        except Exception:
                            created_dt = None
                    else:
                        created_dt = None

                    age_s = (datetime.utcnow() - created_dt).total_seconds() if created_dt else 0.0

                    if age_s > float(getattr(Config, "PENDING_ENTRY_MAX_AGE_S", 120.0) or 120.0):
                        entry_client_id = prot.get("entry_client_id")
                        order_id = prot.get("order_id")

                        final_status = None
                        try:
                            if hasattr(broker, "wait_for_order_final"):
                                fin = await broker.wait_for_order_final(
                                    order_id=order_id,
                                    client_id=entry_client_id,
                                    symbol=symbol,
                                    timeout_s=2.0,
                                    poll_s=0.5,
                                )
                                final_status = (str(getattr(fin, "status", "")) or "").lower()
                        except Exception:
                            final_status = None

                        if entry_client_id and final_status in {"canceled", "cancelled", "rejected", "failed"}:
                            st2 = "canceled" if final_status in {"canceled", "cancelled"} else final_status
                            try:
                                self.ledger.mark_order_final(entry_client_id, st2, payload={"reason": "pending_entry_ttl"})
                            except Exception:
                                pass

                        if trade_id:
                            try:
                                self.ledger.abort_trade(trade_id, f"pending_entry_timeout:{final_status or 'unknown'}")
                            except Exception:
                                pass

                        print(f"⚠️  {symbol}: pending_entry TTL exceeded ({age_s:.0f}s) → abort trade & drop protections")
                        to_remove.append(symbol)

                    continue

                # позиция появилась -> считаем entry подтверждённым
                entry_price = float(getattr(pos, "avg_price", 0.0) or current_price)
                entry_qty = float(qty_pos)

                entry_client_id = prot.get("entry_client_id")

                if trade_id:
                    try:
                        self.ledger.set_trade_entry(trade_id, entry_price, entry_qty)
                    except Exception as e:
                        print(f"[WARN] pending_entry: set_trade_entry failed {symbol}: {e}")

                # аккуратно финализируем entry ордер как filled (инференс по позиции)
                if entry_client_id:
                    try:
                        self.ledger.mark_order_final(
                            entry_client_id,
                            "filled",
                            payload={"price": entry_price, "filled_qty": entry_qty, "_inferred_from_position": True},
                        )
                    except Exception:
                        pass

                atr = float(prot.get("atr", 0.0) or 0.0)
                sl_m = float(prot.get("sl_mult", 0.0) or 0.0)
                tp_m = float(prot.get("tp_mult", 0.0) or 0.0)

                sl_price = (entry_price - atr * sl_m) if (atr > 0 and sl_m > 0) else None
                tp_price = (entry_price + atr * tp_m) if (atr > 0 and tp_m > 0) else None

                use_native = bool(prot.get("use_native", True))
                broker_name2 = (prot.get("broker") or broker_name).lower() or broker_name
                signal_id2 = prot.get("signal_id", "na") or "na"

                if not (sl_price or tp_price):
                    # LIVE: без SL/TP нельзя оставлять позицию открытой
                    if self._strict_protections_enabled():
                        await self._panic_close_unprotected(
                            symbol=symbol,
                            broker_name=broker_name2,
                            trade_id=trade_id or "",
                            reason="protections_missing_prices",
                            signal_id=signal_id2,
                        )
                    else:
                        print(f"[WARN] {symbol}: pending entry finalized, but no SL/TP (atr={atr}) → protections skipped")
                    to_remove.append(symbol)
                    continue

                native_ok = False

                # пробуем native protections
                if use_native and hasattr(broker, "place_protection_orders"):
                    sl_client_id = self._make_client_id(broker_name2, symbol, "sl", signal_id2) if sl_price else None
                    tp_client_id = self._make_client_id(broker_name2, symbol, "tp", signal_id2) if tp_price else None

                    if sl_client_id:
                        self.ledger.reserve_order(
                            sl_client_id, broker=broker_name2, symbol=symbol, role="sl", side="sell",
                            payload={"sl": sl_price, "qty": entry_qty},
                        )
                    if tp_client_id:
                        self.ledger.reserve_order(
                            tp_client_id, broker=broker_name2, symbol=symbol, role="tp", side="sell",
                            payload={"tp": tp_price, "qty": entry_qty},
                        )

                    try:
                        r = await broker.place_protection_orders(
                            symbol,
                            qty=float(entry_qty),
                            sl_price=float(sl_price) if sl_price else None,
                            tp_price=float(tp_price) if tp_price else None,
                            sl_client_oid=sl_client_id,
                            tp_client_oid=tp_client_id,
                        )

                        # sanity: если брокер вернул пустой order_id — считаем что защита не поставилась
                        if sl_price and not str(((r or {}).get("sl") or {}).get("order_id") or "").strip():
                            raise RuntimeError("Native SL placement returned empty order_id")
                        if tp_price and not str(((r or {}).get("tp") or {}).get("order_id") or "").strip():
                            raise RuntimeError("Native TP placement returned empty order_id")

                        if sl_client_id:
                            self.ledger.mark_order_submitted(
                                sl_client_id, str(((r or {}).get("sl") or {}).get("order_id") or ""),
                                payload={"sl": sl_price, "qty": entry_qty},
                            )
                        if tp_client_id:
                            self.ledger.mark_order_submitted(
                                tp_client_id, str(((r or {}).get("tp") or {}).get("order_id") or ""),
                                payload={"tp": tp_price, "qty": entry_qty},
                            )

                        native_ok = True
                        self._protections[symbol] = {
                            "mode": "native",
                            "broker": broker_name2,
                            "trade_id": trade_id,
                            "signal_id": signal_id2,
                            "qty": entry_qty,
                            "sl": sl_price,
                            "tp": tp_price,
                            "sl_client_id": sl_client_id,
                            "tp_client_id": tp_client_id,
                            "native": r or {},
                            "last_price": current_price,
                            "created_at": datetime.utcnow().isoformat(),
                        }
                        dirty = True
                        print(f"🛡️  {symbol}: pending→native protections placed (SL={sl_price}, TP={tp_price})")

                    except Exception as e:
                        if sl_client_id:
                            self.ledger.mark_order_final(sl_client_id, "failed", payload={"error": str(e)})
                        if tp_client_id:
                            self.ledger.mark_order_final(tp_client_id, "failed", payload={"error": str(e)})
                        native_ok = False
                        print(f"⚠️  {symbol}: pending native protections failed → fallback synthetic. err={e}")

                        # LIVE strict: если брокер умеет native SL/TP и они не поставились — закрываем позицию
                        if self._strict_protections_enabled() and hasattr(broker, "place_protection_orders"):
                            await self._panic_close_unprotected(
                                symbol=symbol,
                                broker_name=broker_name2,
                                trade_id=trade_id or "",
                                reason="native_protections_failed",
                                signal_id=signal_id2,
                            )
                            to_remove.append(symbol)
                            continue

                # fallback synthetic
                if not native_ok:
                    self._protections[symbol] = {
                        "mode": "synthetic",
                        "broker": broker_name2,
                        "trade_id": trade_id,
                        "signal_id": signal_id2,
                        "qty": entry_qty,
                        "sl": sl_price,
                        "tp": tp_price,
                        "sl_client_id": self._make_client_id(broker_name2, symbol, "sl", signal_id2) if sl_price else None,
                        "tp_client_id": self._make_client_id(broker_name2, symbol, "tp", signal_id2) if tp_price else None,
                        "last_price": current_price,
                        "created_at": datetime.utcnow().isoformat(),
                    }
                    dirty = True
                    print(f"🛡️  {symbol}: pending→synthetic protections armed (SL={sl_price}, TP={tp_price})")

                continue

            # --- NATIVE MODE ---
            if mode == "native":
                # если позиции уже нет -> считаем что выход случился
                if qty_pos <= 0:
                    if trade_id:
                        self.ledger.close_trade(trade_id, float(current_price), "native_exit_reconcile")
                    to_remove.append(symbol)
                    continue

                native = prot.get("native", {}) or {}
                sl_id = (native.get("sl") or {}).get("order_id")
                tp_id = (native.get("tp") or {}).get("order_id")

                fired = None
                try:
                    if hasattr(broker, "get_plan_sub_order"):
                        for oid, tag in ((sl_id, "sl"), (tp_id, "tp")):
                            if not oid:
                                continue
                            subs = await broker.get_plan_sub_order(str(oid))
                            if subs:
                                fired = tag
                                break
                except Exception:
                    fired = None

                if fired:
                    # отменяем остаточные защиты (чтобы не осталось висящих планов)
                    try:
                        await self._cancel_native_protections(symbol, broker, prot)
                    except Exception:
                        pass

                    if trade_id:
                        self.ledger.close_trade(trade_id, float(current_price), f"native_{fired}")
                    to_remove.append(symbol)
                continue

            # --- SYNTHETIC MODE ---
            sl = float(prot.get("sl", 0.0) or 0.0)
            tp = float(prot.get("tp", 0.0) or 0.0)
            qty = float(prot.get("qty", qty_pos) or qty_pos)

            if qty_pos <= 0:
                to_remove.append(symbol)
                continue

            # === [PATCH 3 START] TIME EXIT ===
            try:
                strat_params = Config.get_strategy_params()
                max_hold_bars = int(strat_params.get("max_hold", 48))
                
                # Определяем секунды в баре
                tf_str = getattr(Config, "TIMEFRAME_LTF", "4h")
                tf_seconds = 3600 # Default 1h
                if "4h" in tf_str: tf_seconds = 3600 * 4
                elif "15m" in tf_str: tf_seconds = 60 * 15
                elif "1d" in tf_str: tf_seconds = 86400
                
                max_seconds = max_hold_bars * tf_seconds
                
                created_at_str = prot.get("created_at")
                if created_at_str:
                    created_dt = datetime.fromisoformat(created_at_str.replace("Z", ""))
                    age_seconds = (datetime.utcnow() - created_dt).total_seconds()
                    
                    if age_seconds > max_seconds:
                        print(f"⏰ {symbol}: TIME EXIT triggered (Age: {age_seconds/3600:.1f}h > {max_seconds/3600:.1f}h)")
                        exit_client_id = self._make_client_id(broker_name, symbol, "exit_time", prot.get("signal_id", "na"))
                        
                        if self.ledger.reserve_order(exit_client_id, broker=broker_name, symbol=symbol, role="time_exit", side="sell", payload={"reason": "time_exit"}):
                            await self._router_execute_order(symbol=symbol, side="sell", quantity=qty, order_type="market", client_id=exit_client_id)
                            if trade_id: self.ledger.close_trade(trade_id, current_price, "time_exit")
                            to_remove.append(symbol)
                            dirty = True
                            continue 
            except Exception as e:
                print(f"[WARN] Time Exit check failed for {symbol}: {e}")
            # === [PATCH 3 END] ===

            hit_sl = sl > 0 and current_price <= sl
            hit_tp = tp > 0 and current_price >= tp
            if not (hit_sl or hit_tp):
                continue

            reason = "sl" if hit_sl else "tp"
            role = reason
            exit_client_id = prot.get(f"{reason}_client_id") or self._make_client_id(
                broker_name, symbol, role, prot.get("signal_id", "na")
            )

            # если reserve не прошёл — НЕ удаляем protection (иначе останешься без защиты)
            if not self.ledger.reserve_order(
                exit_client_id,
                broker=broker_name,
                symbol=symbol,
                role=role,
                side="sell",
                payload={"reason": reason, "qty": qty, "price": current_price},
            ):
                continue

            try:
                res = await self._router_execute_order(
                    symbol=symbol, side="sell", quantity=qty, order_type="market", client_id=exit_client_id
                )
                self.ledger.mark_order_submitted(exit_client_id, str(getattr(res, "order_id", "")), payload={"qty": qty})

                st = (str(getattr(res, "status", "")) or "").lower()
                px = float(getattr(res, "price", 0.0) or current_price)

                # финал пишем только если статус финальный
                if st in {"filled", "canceled", "cancelled", "rejected", "failed"}:
                    st2 = "canceled" if st in {"canceled", "cancelled"} else st
                    self.ledger.mark_order_final(exit_client_id, st2, payload={"price": px})

                if st == "filled":
                    if trade_id:
                        self.ledger.close_trade(trade_id, px, reason)
                    to_remove.append(symbol)
                    print(f"🛡️  {symbol}: {reason.upper()} hit → закрыли MARKET (qty={qty}, price={px})")
                else:
                    print(f"⚠️  {symbol}: protective exit not filled (status={st or 'unknown'}) → ждём reconcile")

            except Exception as e:
                self.ledger.mark_order_final(exit_client_id, "failed", payload={"error": str(e)})
                print(f"⚠️  {symbol}: protective exit failed: {e}")

        if to_remove:
            for s in to_remove:
                self._protections.pop(s, None)
            dirty = True

        if dirty:
            self._persist_protections()

    async def run_strategy(self, risk_per_trade: float | None = None):
        await self._check_protective_exits()
        self._maybe_reload_signals()

        if not self.signals:
            print("❌ Нет сигналов для торговли")
            return

        try:
            params = Config.get_strategy_params()
        except Exception:
            params = getattr(Config, "DEFAULT_STRATEGY", {}) or {}

        threshold = float(params.get("conf", 0.6))
        base_risk = float(risk_per_trade if risk_per_trade is not None else getattr(Config, "RISK_PER_TRADE", 0.01))
        max_risk = float(getattr(Config, "MAX_RISK_PER_TRADE", 0.03) or 0.03)
        tp_mult = float(params.get("tp", 3.5) or 3.5)
        sl_mult = float(params.get("sl", 2.0) or 2.0)

        try:
            positions = await self.router.list_all_positions()
        except Exception:
            positions = []
        pos_map = {p.symbol: p for p in positions}
        # (LIVE safety) MAX_OPEN_POSITIONS: считаем текущие открытые слоты (позиции ∪ open-trades)
        max_pos = int(getattr(Config, "MAX_OPEN_POSITIONS", 0) or 0)

        open_symbols: set[str] = set()
        for p in positions:
            try:
                if float(getattr(p, "quantity", 0.0) or 0.0) > 0:
                    open_symbols.add(str(getattr(p, "symbol", "")))
            except Exception:
                pass

        try:
            for t in self.ledger.list_open_trades():
                sym = str((t or {}).get("symbol") or "")
                if sym:
                    open_symbols.add(sym)
        except Exception:
            pass

        open_count = len({s for s in open_symbols if s})

        for symbol, df in self.signals.items():
            if df is None or df.empty:
                continue
            if self.assets_filter and symbol not in self.assets_filter:
                continue

            last_signal = df.iloc[-1]
            signal_id = self._make_signal_id(symbol, df, last_signal)

            if self._runner_state.get("last_seen", {}).get(symbol) == signal_id:
                continue

            p_long = float(last_signal.get("p_long", 0.0) or 0.0)
            p_short = float(last_signal.get("p_short", 0.0) or 0.0)
            confidence = max(p_long, p_short)
            risk_this_trade = self._compute_risk_per_trade(confidence, base_risk, max_risk, threshold)

            pos = pos_map.get(symbol)
            pos_qty = float(getattr(pos, "quantity", 0.0) or 0.0)

            if p_long > threshold and pos_qty <= 0:
                if max_pos > 0 and open_count >= max_pos:
                    print(f"⛔ MAX_OPEN_POSITIONS={max_pos} reached (open={open_count}) → skip BUY {symbol}")
                else:
                    await self.execute_trade(
                        symbol=symbol,
                        side="buy",
                        probability=p_long,
                        risk_per_trade=risk_this_trade,
                        signal_id=signal_id,
                        signal_data=last_signal,
                        sl_mult=sl_mult,
                        tp_mult=tp_mult,
                    )
                    # если trade реально стал open (включая pending_entry) — считаем слот занятым
                    try:
                        broker_guess = (self.router.get_broker_name_for_symbol(symbol) or "").lower() or "router"
                        if self.ledger.has_open_trade(broker_guess, symbol):
                            open_symbols.add(symbol)
                            open_count = len({s for s in open_symbols if s})
                    except Exception:
                        pass
                    
            if p_short > threshold and pos_qty > 0:
                await self.execute_trade(symbol=symbol, side="sell", probability=p_short, risk_per_trade=risk_this_trade, signal_id=signal_id, signal_data=last_signal, sl_mult=sl_mult, tp_mult=tp_mult)

            self._runner_state.setdefault("last_seen", {})[symbol] = signal_id
            try:
                self._runner_state.setdefault("last_processed_ts", {})[symbol] = self._safe_ts(df.index[-1])
            except Exception:
                pass
            self._runner_state.setdefault("snapshots", {})[symbol] = {
                "p_long": p_long,
                "p_short": p_short,
                "confidence": confidence,
                "position_qty": pos_qty,
                "updated_at": datetime.utcnow().isoformat(),
            }

        self._persist_state()
        # [FIX] Принудительная очистка мусора после цикла
        gc.collect()

    async def execute_trade(self, *, symbol: str, side: str, probability: float, risk_per_trade: float, signal_id: str, signal_data: pd.Series, sl_mult: float, tp_mult: float) -> None:
        broker = await self.router.get_broker_for_symbol(symbol)
        broker_name = getattr(broker, "name", broker.__class__.__name__).lower()
        strategy_id = getattr(Config, "STRATEGY_ID", "universal")

        current_price = float(await broker.get_current_price(symbol))

        if side == "sell":
            positions = await self.router.list_all_positions()
            p = next((x for x in positions if x.symbol == symbol and float(x.quantity or 0.0) > 0), None)
            if p is None:
                print(f"ℹ️  SELL skip: позиции уже нет {symbol}")
                return

            qty_to_close = float(p.quantity)

            open_trade = self.ledger.get_open_trade(broker_name, symbol)
            trade_id = (open_trade or {}).get("trade_id") or self._make_trade_id(broker_name, symbol, signal_id)
            exit_client_id = self._make_client_id(
                broker_name, symbol, "exit", (open_trade or {}).get("signal_id") or signal_id
            )

            if not self.ledger.reserve_order(
                exit_client_id,
                broker=broker_name,
                symbol=symbol,
                role="exit",
                side="sell",
                payload={"reason": "signal_exit", "qty": qty_to_close, "signal_id": signal_id},
            ):
                print(f"🧾 Ledger: EXIT уже делали (client_id={exit_client_id}) → пропуск")
                return

            # 1) отправляем EXIT
            try:
                res = await self._router_execute_order(
                    symbol=symbol,
                    side="sell",
                    quantity=qty_to_close,
                    order_type="market",
                    client_id=exit_client_id,
                )
            except Exception as e:
                # важно: если submit упал — не закрываем trade
                self.ledger.mark_order_final(exit_client_id, "failed", payload={"error": str(e)})
                print(f"❌ EXIT submit failed {symbol}: {e}")
                return

            self.ledger.mark_order_submitted(
                exit_client_id, str(getattr(res, "order_id", "")), payload={"qty": qty_to_close}
            )

            st = (str(getattr(res, "status", "")) or "").lower()
            px = float(getattr(res, "price", 0.0) or current_price)

            # 2) финализируем только если статус финальный
            if st in {"filled", "canceled", "cancelled", "rejected", "failed"}:
                st2 = "canceled" if st in {"canceled", "cancelled"} else st
                self.ledger.mark_order_final(exit_client_id, st2, payload={"price": px})
            else:
                # pending/unknown — НЕ закрываем trade, ждём reconcile
                print(f"⏳ EXIT {symbol}: status={st or 'unknown'} → ждём reconcile")
                return

            # 3) trade закрываем только если реально filled
            if st != "filled":
                print(f"⚠️  EXIT {symbol}: not filled (status={st}) → trade НЕ закрыт")
                return

            self.ledger.close_trade(trade_id, px, "signal_exit")

            # 4) после успешного EXIT — гасим/чистим защиты (native/synthetic)
            if symbol in self._protections:
                prot = self._protections.get(symbol) or {}
                try:
                    await self._cancel_native_protections(symbol, broker, prot)
                except Exception as e:
                    print(f"[WARN] native protections cancel failed {symbol}: {e}")
                self._protections.pop(symbol, None)
                self._persist_protections()

            print(f"✅ EXIT {symbol} done (qty={qty_to_close}, price={px})")
            return

        # === [PATCH 1 START] PULLBACK LOGIC ===
        # Эмуляция лимитного входа. Если цена хуже расчетной - пропускаем цикл.
        try:
            strat_params = Config.get_strategy_params()
            pullback_mult = float(strat_params.get("pullback", 0.0))
        except:
            pullback_mult = 0.0

        atr_val = float(signal_data.get("atr", 0.0) or 0.0)

        # Проверяем только если pullback включен (>0)
        if pullback_mult > 0.001 and atr_val > 0:
            # Вариант "Строгий": требуем цену лучше, чем (Signal Close +/- Pullback)
            sig_close = float(signal_data.get("close", current_price))
            
            if side == "buy":
                target_price = sig_close - (atr_val * pullback_mult)
                # Если мы ВЫШЕ цели (дороже) -> ждем
                if current_price > target_price:
                    print(f"⏳ {symbol} PULLBACK: Curr {current_price:.4f} > Target {target_price:.4f} (Wait)")
                    return # Выходим, не отправляя ордер
            
            elif side == "sell":
                target_price = sig_close + (atr_val * pullback_mult)
                # Если мы НИЖЕ цели (дешевле) -> ждем
                if current_price < target_price:
                    print(f"⏳ {symbol} PULLBACK: Curr {current_price:.4f} < Target {target_price:.4f} (Wait)")
                    return
        # === [PATCH 1 END] ===

        broker_state = await broker.get_account_state()
        equity = float(getattr(broker_state, "equity", 0.0) or 0.0)

        atr_value = float(signal_data.get("atr", 0.0) or 0.0)
        max_notional = getattr(Config, "MAX_POSITION_NOTIONAL", None)

        ps = calc_position_size(
            equity=equity,
            atr=atr_value,
            risk_per_trade=float(risk_per_trade),
            sl_mult=float(sl_mult),
            price=float(current_price),
            max_notional=max_notional,
        )
        qty_raw = float(ps.size)
        qty = float(getattr(broker, "normalize_qty", lambda s, q, p=None: q)(symbol, qty_raw, current_price))

        if qty <= 0:
            print(f"⚠️  {symbol}: qty=0 после нормализации → skip")
            return

        trade_id = self._make_trade_id(broker_name, symbol, signal_id)
        entry_client_id = self._make_client_id(broker_name, symbol, "entry", signal_id)

        if not self.ledger.reserve_order(entry_client_id, broker=broker_name, symbol=symbol, role="entry", side="buy", payload={"qty": qty, "price": current_price, "signal_id": signal_id, "p": probability}):
            print(f"🧾 Ledger: ENTRY уже делали (client_id={entry_client_id}) → пропуск")
            return

        self.ledger.upsert_trade(trade_id=trade_id, strategy_id=strategy_id, broker=broker_name, symbol=symbol, side="buy", signal_id=signal_id, entry_client_id=entry_client_id)

        try:
            res = await self._router_execute_order(
                symbol=symbol, side="buy", quantity=qty, order_type="market", client_id=entry_client_id
            )
            self.ledger.mark_order_submitted(entry_client_id, str(getattr(res, "order_id", "")), payload={"qty": qty})

            st = (str(getattr(res, "status", "")) or "").lower()
            fill_price = float(getattr(res, "price", 0.0) or current_price)

            final_statuses = {"filled", "canceled", "cancelled", "rejected", "failed"}

            # 1) если статус финальный — фиксируем его в ledger
            if st in final_statuses:
                st2 = "canceled" if st in {"canceled", "cancelled"} else st
                self.ledger.mark_order_final(entry_client_id, st2, payload={"price": fill_price})

                # финально НЕ filled -> абортим trade (это реально не зашли)
                if st2 != "filled":
                    self.ledger.abort_trade(trade_id, f"entry_not_filled:{st2}")
                    print(f"⚠️  ENTRY {symbol}: not filled (status={st2}) → abort trade")
                    return

                # filled -> фиксируем entry
                self.ledger.set_trade_entry(trade_id, fill_price, qty)

            else:
                # 2) pending/unknown: НЕ abort'им! Позиция могла исполниться, но confirm не дошёл.
                # Ставим "pending_entry" и даём reconcile/следующему циклу поставить защиты.
                self._protections[symbol] = {
                    "mode": "pending_entry",
                    "broker": broker_name,
                    "trade_id": trade_id,
                    "signal_id": signal_id,
                    "entry_client_id": entry_client_id,
                    "order_id": str(getattr(res, "order_id", "")) or None,
                    "qty_expected": float(qty),
                    "atr": float(atr_value),
                    "sl_mult": float(sl_mult),
                    "tp_mult": float(tp_mult),
                    "use_native": bool(getattr(Config, "USE_NATIVE_PROTECTIONS", True)),
                    "last_price": float(current_price),
                    "created_at": datetime.utcnow().isoformat(),
                }
                self._persist_protections()

                # попытка “самовылечиться” сразу: вдруг позиция уже появилась в брокере
                try:
                    await self._check_protective_exits()
                except Exception:
                    pass

                print(f"⏳ ENTRY {symbol}: status={st or 'unknown'} → ждём подтверждения; защиты поставятся при появлении позиции")
                return

            self.ledger.set_trade_entry(trade_id, fill_price, qty)

        except Exception as e:
            self.ledger.mark_order_final(entry_client_id, "failed", payload={"error": str(e)})
            self.ledger.abort_trade(trade_id, f"entry_failed: {e}")
            print(f"❌ ENTRY failed {symbol}: {e}")
            return

        base_price = float(self.ledger.get_trade_entry_price(trade_id) or current_price) if hasattr(self.ledger, "get_trade_entry_price") else fill_price
        sl_price = (base_price - atr_value * float(sl_mult)) if atr_value > 0 else None
        tp_price = (base_price + atr_value * float(tp_mult)) if atr_value > 0 else None

        # LIVE strict: без SL/TP нельзя оставлять позицию открытой
        if not (sl_price or tp_price) and self._strict_protections_enabled():
            await self._panic_close_unprotected(
                symbol=symbol,
                broker_name=broker_name,
                trade_id=trade_id,
                reason="protections_missing_prices",
                signal_id=signal_id,
            )
            return

        use_native = bool(getattr(Config, "USE_NATIVE_PROTECTIONS", True))
        native_ok = False

        if use_native and hasattr(broker, "place_protection_orders") and (sl_price or tp_price):
            sl_client_id = self._make_client_id(broker_name, symbol, "sl", signal_id) if sl_price else None
            tp_client_id = self._make_client_id(broker_name, symbol, "tp", signal_id) if tp_price else None

            if sl_client_id:
                self.ledger.reserve_order(sl_client_id, broker=broker_name, symbol=symbol, role="sl", side="sell", payload={"sl": sl_price, "qty": qty})
            if tp_client_id:
                self.ledger.reserve_order(tp_client_id, broker=broker_name, symbol=symbol, role="tp", side="sell", payload={"tp": tp_price, "qty": qty})

            try:
                r = await broker.place_protection_orders(symbol, qty=float(qty), sl_price=float(sl_price) if sl_price else None, tp_price=float(tp_price) if tp_price else None, sl_client_oid=sl_client_id, tp_client_oid=tp_client_id)
                # sanity: если брокер вернул пустой order_id — считаем что защита не поставилась
                if sl_price and not str(((r or {}).get("sl") or {}).get("order_id") or "").strip():
                    raise RuntimeError("Native SL placement returned empty order_id")
                if tp_price and not str(((r or {}).get("tp") or {}).get("order_id") or "").strip():
                    raise RuntimeError("Native TP placement returned empty order_id")
                if sl_client_id:
                    self.ledger.mark_order_submitted(sl_client_id, str(((r or {}).get("sl") or {}).get("order_id") or ""), payload={"sl": sl_price, "qty": qty})
                if tp_client_id:
                    self.ledger.mark_order_submitted(tp_client_id, str(((r or {}).get("tp") or {}).get("order_id") or ""), payload={"tp": tp_price, "qty": qty})

                native_ok = True
                self._protections[symbol] = {"mode": "native", "broker": broker_name, "trade_id": trade_id, "signal_id": signal_id, "qty": qty, "sl": sl_price, "tp": tp_price, "sl_client_id": sl_client_id, "tp_client_id": tp_client_id, "native": r or {}, "last_price": current_price, "created_at": datetime.utcnow().isoformat()}
                self._persist_protections()
                print(f"🛡️  {symbol}: native protections placed (SL={sl_price}, TP={tp_price})")
            except Exception as e:
                if sl_client_id:
                    self.ledger.mark_order_final(sl_client_id, "failed", payload={"error": str(e)})
                if tp_client_id:
                    self.ledger.mark_order_final(tp_client_id, "failed", payload={"error": str(e)})
                native_ok = False
                print(f"⚠️  {symbol}: native protections failed → fallback synthetic. err={e}")

        if not native_ok and (sl_price or tp_price):
            self._protections[symbol] = {"mode": "synthetic", "broker": broker_name, "trade_id": trade_id, "signal_id": signal_id, "qty": qty, "sl": sl_price, "tp": tp_price, "sl_client_id": self._make_client_id(broker_name, symbol, "sl", signal_id) if sl_price else None, "tp_client_id": self._make_client_id(broker_name, symbol, "tp", signal_id) if tp_price else None, "last_price": current_price, "created_at": datetime.utcnow().isoformat()}
            self._persist_protections()
            print(f"🛡️  {symbol}: synthetic protections armed (SL={sl_price}, TP={tp_price})")

        # LIVE strict: если брокер умеет native SL/TP и они не поставились — закрываем позицию
        if self._strict_protections_enabled() and hasattr(broker, "place_protection_orders") and (sl_price or tp_price) and not native_ok:
            await self._panic_close_unprotected(
                symbol=symbol,
                broker_name=broker_name,
                trade_id=trade_id,
                reason="native_protections_failed",
                signal_id=signal_id,
            )
            return

        print(f"✅ ENTRY {symbol}: qty={qty} price≈{current_price} p={probability:.3f} risk={risk_per_trade:.4f}")

    def request_stop(self):
            """
            Мягкая остановка цикла run_forever.
            """
            self._keep_running = False

    async def run_forever(self, risk_per_trade: float | None = None, sleep_interval: float = 10.0) -> None:
        """
        Бесконечный цикл запуска стратегии (для GUI и CLI).
        Содержит логику Watchdog Heartbeat, Kill-Switch и обработки ошибок.
        """
        self._keep_running = True
        
        # Читаем лимит ошибок из конфига
        max_errors = int(getattr(Config, "RUNNER_MAX_CONSECUTIVE_ERRORS", 5) or 5)
        if max_errors < 1: 
            max_errors = 1
        consecutive_errors = 0
        
        kill_path = getattr(Config, "KILL_SWITCH_FILE", os.path.join(self._state_dir, "kill_switch.json"))
        os.makedirs(os.path.dirname(kill_path) or ".", exist_ok=True)
        
        print(f"🧯 Auto kill-switch armed: {max_errors} consecutive errors → close all & exit")

        while self._keep_running:
            # 1. Heartbeat (начало цикла)
            self._touch_heartbeat("alive", note="loop_top")

            # 2. Kill-Switch Check
            if self._kill_switch_enabled():
                self._touch_heartbeat("stopped", note="kill_switch_enabled")
                await self._handle_kill_switch(reason="manual_or_guard")
                return

            # 3. Strategy Execution
            try:
                await self.run_strategy(risk_per_trade=risk_per_trade)
                
                # Успех -> сбрасываем счетчик ошибок
                consecutive_errors = 0 
                self._touch_heartbeat("ok", note="cycle_ok", extra={"consecutive_errors": consecutive_errors})
                
            except asyncio.CancelledError:
                self._touch_heartbeat("stopped", note="cancelled")
                # Пробрасываем отмену, чтобы корректно выйти из таска
                raise 
                
            except Exception as e:
                # Обработка ошибок
                await self.alerter.send(f"🔴 Runner ERROR ({consecutive_errors}/{max_errors}): {e}")
                consecutive_errors += 1
                self._touch_heartbeat("error", note="cycle_error", extra={"error": str(e), "consecutive_errors": consecutive_errors})
                print(f"[FATAL] runner loop error ({consecutive_errors}/{max_errors}): {e}")

                # Если превышен лимит ошибок -> Kill Switch
                if consecutive_errors >= max_errors:
                    reason = f"auto_max_consecutive_errors:{consecutive_errors}"
                    atomic_write_json(
                        kill_path,
                        {
                            "enabled": True,
                            "reason": reason,
                            "enabled_at": datetime.utcnow().isoformat(),
                            "consecutive_errors": consecutive_errors,
                            "last_error": str(e),
                        },
                    )
                    self._touch_heartbeat("stopped", note="auto_kill_switch", extra={"reason": reason})
                    await self._handle_kill_switch(reason=reason)
                    return

            # 4. Sleep
            self._touch_heartbeat("alive", note="sleeping", extra={"sleep_s": sleep_interval})
            try:
                # Спим, проверяя флаг остановки каждые 1 сек (для отзывчивости), 
                # либо просто await asyncio.sleep(sleep_interval), т.к. CancelledError прервет сон.
                await asyncio.sleep(sleep_interval)
            except asyncio.CancelledError:
                self._touch_heartbeat("stopped", note="cancelled_sleep")
                raise

async def _amain():
    # [FIX] Включаем логирование
    from config import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(description="Async Strategy Runner")
    parser.add_argument("--signals", type=str, default="data_cache/production_signals_v1.pkl", help="Path to signals pickle")
    parser.add_argument("--assets", type=str, default="", help="Comma-separated tickers to trade (optional)")
    parser.add_argument("--risk_level", type=float, default=None, help="Override base risk per trade (e.g., 0.001)")
    parser.add_argument("--loop", action="store_true", help="Run forever loop")
    parser.add_argument("--sleep", type=float, default=10.0, help="Sleep seconds for loop mode")
    args = parser.parse_args()

    runner = AsyncStrategyRunner(signals_file=args.signals)
    if args.assets.strip():
        runner.set_assets([a.strip() for a in args.assets.split(",") if a.strip()])

    await runner.initialize()

    await runner.alerter.send(
        f"🟢 Runner START\nmode={runner._mode_value()}\nuniverse={getattr(Config,'UNIVERSE_MODE',None)}"
    )

    if not args.loop:
        await runner.run_strategy(risk_per_trade=args.risk_level)
        return
    max_errors = int(getattr(Config, "RUNNER_MAX_CONSECUTIVE_ERRORS", 5) or 5)
    if max_errors < 1:
        max_errors = 1
    consecutive_errors = 0

    kill_path = getattr(Config, "KILL_SWITCH_FILE", os.path.join(runner._state_dir, "kill_switch.json"))
    os.makedirs(os.path.dirname(kill_path) or ".", exist_ok=True)
    print(f"🧯 Auto kill-switch armed: {max_errors} consecutive errors → close all & exit")

    while True:
        # HEARTBEAT: раннер жив (начало цикла)
        runner._touch_heartbeat("alive", note="loop_top")

        # (2) kill-switch: если включили — закрываем всё и выходим
        if runner._kill_switch_enabled():
            runner._touch_heartbeat("stopped", note="kill_switch_enabled")
            await runner._handle_kill_switch(reason="manual_or_guard")
            return

        try:
            await runner.run_strategy(risk_per_trade=args.risk_level)
            consecutive_errors = 0  # успех → сброс
            runner._touch_heartbeat("ok", note="cycle_ok", extra={"consecutive_errors": consecutive_errors})
        except asyncio.CancelledError:
            runner._touch_heartbeat("stopped", note="cancelled")
            raise
        except Exception as e:
            await runner.alerter.send(f"🔴 Runner ERROR ({consecutive_errors}/{max_errors}): {e}")
            consecutive_errors += 1
            runner._touch_heartbeat("error", note="cycle_error", extra={"error": str(e), "consecutive_errors": consecutive_errors})
            print(f"[FATAL] runner loop error ({consecutive_errors}/{max_errors}): {e}")

            if consecutive_errors >= max_errors:
                reason = f"auto_max_consecutive_errors:{consecutive_errors}"
                atomic_write_json(
                    kill_path,
                    {
                        "enabled": True,
                        "reason": reason,
                        "enabled_at": datetime.utcnow().isoformat(),
                        "consecutive_errors": consecutive_errors,
                        "last_error": str(e),
                    },
                )
                runner._touch_heartbeat("stopped", note="auto_kill_switch", extra={"reason": reason})
                await runner._handle_kill_switch(reason=reason)
                return

        # HEARTBEAT: перед сном тоже отметимся (полезно при большом sleep)
        runner._touch_heartbeat("alive", note="sleeping", extra={"sleep_s": float(args.sleep)})
        await asyncio.sleep(float(args.sleep))


if __name__ == "__main__":
    asyncio.run(_amain())
