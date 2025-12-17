# brokers/simulated_client.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List
import asyncio
import random
import json             # <--- Добавлено
import os               # <--- Добавлено
import pandas as pd
from config import Config
from .base import BrokerAPI, OrderRequest, OrderResult, Position, AccountState


@dataclass
class _SimPositionState:
    """
    Внутреннее состояние позиции для симулятора.
    """
    quantity: float = 0.0
    avg_price: float = 0.0


class SimulatedBroker(BrokerAPI):
    """
    Универсальный симулятор счёта.

    Идея:
      - для market-data используем реальный брокер (Bitget/Tinkoff),
        переданный как data_broker (async BrokerAPI);
      - ордера, позиции и PnL считаем локально;
      - один и тот же движок (стратегия) может работать
        как с криптой, так и с акциями, не зная, что это симулятор.
    """

    def __init__(
        self,
        name: str,
        data_broker: BrokerAPI,
        starting_equity: float,
        currency: str = "USDT",
    ):
        # "логическое" имя: bitget_sim / tinkoff_sim
        self.name = f"{name}_sim"
        self._underlying = data_broker
        self._currency = currency

        # Начальный капитал, реализованный PnL и позиции
        self._starting_equity = float(starting_equity)
        self._realized_pnl = 0.0
        self._positions: Dict[str, _SimPositionState] = {}

        # Простая генерация ID ордеров
        self._order_seq = 0
        
        # --- [NEW] Persistence ---
        # Файл состояния, чтобы выжить после перезагрузки
        self.state_file = f"state/{self.name}_state.json"
        if not os.path.exists("state"):
            os.makedirs("state", exist_ok=True)
        self._load_state()  # Пробуем восстановиться при старте

    # ------------------------------------------------------------------
    # PERSISTENCE (Сохранение/Загрузка)
    # ------------------------------------------------------------------
    def _save_state(self):
        """Сохраняем Equity, Позиции и (ВАЖНО!) счетчик ордеров."""
        data = {
            "equity": self._starting_equity,
            "realized_pnl": self._realized_pnl,
            "order_seq": self._order_seq,  # <--- Чтобы не было дублей ID
            "positions": {
                sym: {"qty": p.quantity, "avg": p.avg_price}
                for sym, p in self._positions.items() if p.quantity != 0
            }
        }
        try:
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[SIM] ⚠️ Failed to save state: {e}")

    def _load_state(self):
        """Восстанавливаем состояние."""
        if not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)
            
            self._starting_equity = data.get("equity", self._starting_equity)
            self._realized_pnl = data.get("realized_pnl", 0.0)
            self._order_seq = int(data.get("order_seq", 0))
            
            loaded_pos = data.get("positions", {})
            self._positions = {}
            for sym, p_data in loaded_pos.items():
                self._positions[sym] = _SimPositionState(
                    quantity=p_data["qty"],
                    avg_price=p_data["avg"]
                )
            print(f"♻️ [SIM] State restored! Eq: {self._starting_equity + self._realized_pnl:.2f}, Orders: {self._order_seq}")
        except Exception as e:
            print(f"[SIM] ⚠️ Failed to load state: {e}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        # пробрасываем инициализацию вниз, если нужно
        if hasattr(self._underlying, "initialize"):
            await self._underlying.initialize()

    async def close(self) -> None:
        if hasattr(self._underlying, "close"):
            await self._underlying.close()

    # ------------------------------------------------------------------
    # MARKET DATA (делегируем реальному брокеру)
    # ------------------------------------------------------------------

    async def get_historical_klines(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        return await self._underlying.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
        )

    async def get_current_price(self, symbol: str) -> float:
        return await self._underlying.get_current_price(symbol)

    # ------------------------------------------------------------------
    # Вспомогательное: переоценка позиций
    # ------------------------------------------------------------------

    async def _revalue_positions(self) -> Dict[str, Position]:
        """
        Пересчитываем нереализованный PnL по всем инструментам.
        Возвращаем snapshot позиций в виде Position.
        """
        result: Dict[str, Position] = {}

        for symbol, state in self._positions.items():
            if state.quantity == 0:
                continue

            last_price = await self.get_current_price(symbol)
            qty = state.quantity
            avg = state.avg_price

            if qty > 0:
                # long
                unrealized = (last_price - avg) * qty
            else:
                # short
                unrealized = (avg - last_price) * abs(qty)

            result[symbol] = Position(
                symbol=symbol,
                quantity=qty,
                avg_price=avg,
                unrealized_pnl=unrealized,
                broker=self.name,
            )

        return result

    # ------------------------------------------------------------------
    # ACCOUNT / PORTFOLIO
    # ------------------------------------------------------------------

    async def get_account_state(self) -> AccountState:
        """
        Возвращаем сводное состояние счёта:
          - equity = стартовый капитал + реализованный PnL + суммарный нереализованный PnL
          - balance = стартовый капитал + реализованный PnL (без учёта плавающего)
        """
        positions = await self._revalue_positions()
        total_unrealized = sum(
            p.unrealized_pnl or 0.0 for p in positions.values()
        )

        balance = self._starting_equity + self._realized_pnl
        equity = balance + total_unrealized

        return AccountState(
            equity=equity,
            balance=balance,
            currency=self._currency,
            margin_used=0.0,
            broker=self.name,
        )

    async def list_open_positions(self) -> List[Position]:
        positions = await self._revalue_positions()
        return list(positions.values())

    # ------------------------------------------------------------------
    # TRADING LOGIC (упрощённый каркас)
    # ------------------------------------------------------------------

    def _next_order_id(self) -> str:
        self._order_seq += 1
        return f"{self.name}-ord-{self._order_seq}"

    async def place_order(self, order: OrderRequest) -> OrderResult:
        """
        Реалистичная симуляция:
          1. Задержка сети (latency)
          2. Проскальзывание (slippage) для Market-ордеров
          3. Margin Call проверка
          4. Сохранение состояния на диск
        """
        # 0. Margin Call Check
        acc = await self.get_account_state()
        if acc.equity <= 0:
             raise RuntimeError(f"GAME OVER: Equity is {acc.equity}. Margin Call.")

        # 1. Latency simulation (50ms - 300ms)
        await asyncio.sleep(random.uniform(0.05, 0.3))

        symbol = order.symbol
        side = order.side
        qty = float(order.quantity)

        if qty <= 0:
            raise ValueError("SimulatedBroker: quantity must be > 0")

        # 2. Price & Slippage simulation
        current_market_price = float(await self.get_current_price(symbol))
        
        if order.price is not None:
            # Limit order - исполняем по заявленной (упрощение)
            trade_price = float(order.price)
        else:
            # Market order - добавляем Slippage
            # Берем настройку из конфига или дефолт 0.1%
            slippage_pct = getattr(Config, 'SLIPPAGE', 0.001) 
            
            # Эмуляция: покупка всегда чуть дороже, продажа чуть дешевле
            if side == "buy":
                trade_price = current_market_price * (1 + slippage_pct)
            else:
                trade_price = current_market_price * (1 - slippage_pct)

        # 3. Update Position Logic
        state = self._positions.get(symbol, _SimPositionState())
        signed_qty = qty if side == "buy" else -qty

        if state.quantity == 0:
            state.quantity = signed_qty
            state.avg_price = trade_price
        else:
            old_qty = state.quantity
            old_avg = state.avg_price

            if (old_qty > 0 and signed_qty > 0) or (old_qty < 0 and signed_qty < 0):
                # Усреднение
                new_qty = old_qty + signed_qty
                if new_qty == 0:
                    state.quantity = 0.0; state.avg_price = 0.0
                else:
                    state.avg_price = (old_avg * old_qty + trade_price * signed_qty) / new_qty
                    state.quantity = new_qty
            else:
                # Закрытие / Разворот
                closing_qty = min(abs(old_qty), abs(signed_qty))
                realized = (trade_price - old_avg) * closing_qty if old_qty > 0 else (old_avg - trade_price) * closing_qty
                
                self._realized_pnl += realized

                new_qty = old_qty + signed_qty
                if new_qty == 0:
                    state.quantity = 0.0; state.avg_price = 0.0
                else:
                    state.quantity = new_qty
                    state.avg_price = trade_price # Хвост по новой цене

        self._positions[symbol] = state
        
        # 4. Generate ID & SAVE STATE
        order_id = self._next_order_id()
        self._save_state()  # <--- ВАЖНО: Сохраняем сразу после сделки

        return OrderResult(
            order_id=order_id,
            symbol=symbol,
            side=side,
            quantity=qty,
            price=trade_price,
            status="filled",
            broker=self.name,
        )

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> None:
        """
        В текущем каркасе ордеры исполняются мгновенно, так что отмена —
        no-op. Позже можно будет хранить pending-ордера и реально их отменять.
        """
        return None

    async def get_open_orders(self, symbol: str) -> List[OrderResult]:
        """
        В текущем каркасе все ордера исполняются мгновенно, так что
        "открытых" ордеров нет.
        """
        return []
