# execution_router.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional
from risk_utils import calc_position_size
from config import Config, ExecutionMode
from brokers import (
    get_broker,
    BrokerAPI,
    OrderRequest,
    OrderResult,
    AccountState,
    Position,
)


@dataclass
class GlobalAccountState:
    """
    Сводное состояние портфеля по всем брокерам.
    """
    equity: float
    balance: float
    details: Dict[str, AccountState]


class ExecutionRouter:
    """
    Асинхронный роутер исполнения ордеров и сигналов.
    
    Задачи:
      - знает, какой символ к какому брокеру относится (Config.ASSET_ROUTING);
      - лениво поднимает брокеров через get_broker(name);
      - агрегирует состояние счёта и позиции по всем брокерам;
      - даёт простые async методы execute_order / execute_signal.
    """

    def __init__(
        self,
        asset_routing: Optional[Dict[str, str]] = None,
        default_broker: Optional[str] = None,
    ):
        # Если ASSET_ROUTING/DEFAULT_BROKER ещё не заведены в Config,
        # подтянем безопасные значения по умолчанию.
        self.asset_routing: Dict[str, str] = asset_routing or getattr(
            Config, "ASSET_ROUTING", {}
        )
        self.default_broker: str = default_broker or getattr(
            Config, "DEFAULT_BROKER", "bitget"
        )

        # Локальный кеш брокеров: "bitget" -> BrokerAPI
        self._brokers: Dict[str, BrokerAPI] = {}
        self._daily_anchor_date: str | None = None
        self._daily_anchor_equity: float | None = None
        self._daily_anchor_by_broker: dict[str, float] = {}

    # ---------- Lifecycle ----------
    
    async def initialize(self) -> None:
        """
        Асинхронная инициализация нужных брокеров.
        """
        # Определяем режим
        mode_obj = getattr(Config, "EXECUTION_MODE", ExecutionMode.BACKTEST)
        mode = mode_obj.value if isinstance(mode_obj, ExecutionMode) else str(mode_obj).lower()

        # Берём только брокеров, которые реально нужны под текущий universe/assets
        assets = getattr(Config, "ASSETS", None) or []
        if assets:
            broker_names = {self.get_broker_name_for_symbol(sym) for sym in assets}
        else:
            broker_names = set(self.asset_routing.values())

        broker_names.add(self.default_broker)

        for name in sorted(broker_names):
            try:
                broker = get_broker(name)
                await broker.initialize()
                self._brokers[name] = broker
            except Exception as e:
                # В LIVE лучше падать сразу, чем "жить полумёртвым"
                if mode == "live":
                    raise RuntimeError(f"ExecutionRouter: failed to init broker '{name}': {e}") from e
                print(f"[WARN] ExecutionRouter: failed to init broker '{name}': {e}")

    async def close(self) -> None:
        """
        Корректное закрытие всех брокеров.
        """
        for name, broker in list(self._brokers.items()):
            try:
                await broker.close()
            except Exception as e:
                print(f"[WARN] ExecutionRouter: failed to close broker '{name}': {e}")
            finally:
                self._brokers.pop(name, None)

    # ---------- Вспомогательные методы ----------

    async def _ensure_daily_anchor(self) -> None:
        """Фиксируем equity на начало текущего дня (для MAX_DAILY_DRAWDOWN)."""
        today = date.today().isoformat()
        if self._daily_anchor_date != today:
            snap = await self.get_global_account_state()
            self._daily_anchor_date = today
            self._daily_anchor_equity = float(snap.equity or 0.0)

            # NEW: якоря по каждому брокеру отдельно (без валютных конверсий)
            self._daily_anchor_by_broker = {}
            for name, st in (snap.details or {}).items():
                try:
                    self._daily_anchor_by_broker[name] = float(getattr(st, "equity", 0.0) or 0.0)
                except Exception:
                    continue

    async def _check_daily_drawdown_guard(self) -> None:
        """
        В LIVE запрещает новые ордера при превышении MAX_DAILY_DRAWDOWN
        (в процентах от утреннего equity).
        """
        mode_obj = getattr(Config, "EXECUTION_MODE", ExecutionMode.BACKTEST)
        mode = mode_obj.value if isinstance(mode_obj, ExecutionMode) else str(mode_obj).lower()

        if mode != "live":
            return

        max_dd = float(getattr(Config, "MAX_DAILY_DRAWDOWN", 0.0) or 0.0)
        if max_dd <= 0:
            return

        await self._ensure_daily_anchor()
        anchor = float(self._daily_anchor_equity or 0.0)
        if anchor <= 0:
            return

        snap = await self.get_global_account_state()

        # NEW: проверяем дневной DD по каждому брокеру
        for name, st in (snap.details or {}).items():
            anchor_b = float(self._daily_anchor_by_broker.get(name, 0.0) or 0.0)
            cur_b = float(getattr(st, "equity", 0.0) or 0.0)
            if anchor_b > 0 and cur_b > 0:
                dd_b = (anchor_b - cur_b) / anchor_b
                if dd_b >= max_dd:
                    raise RuntimeError(
                        f"[RISK] MAX_DAILY_DRAWDOWN reached for {name}: {dd_b:.2%} >= {max_dd:.2%}. "
                        f"New orders blocked until next day."
                    )

        # Fallback: если почему-то нет детализации — старый глобальный вариант
        equity = float(snap.equity or 0.0)
        dd = (anchor - equity) / anchor
        if dd >= max_dd:
            raise RuntimeError(
                f"[RISK] MAX_DAILY_DRAWDOWN reached: {dd:.2%} >= {max_dd:.2%}. "
                f"New orders blocked until next day."
            )

    def get_broker_name_for_symbol(self, symbol: str) -> str:
        """
        Вернуть имя брокера для данного тикера.
        Если тикер не прописан явно — используем default_broker.
        """
        return self.asset_routing.get(symbol, self.default_broker)

    async def get_broker_for_symbol(self, symbol: str) -> BrokerAPI:
        """
        Получить инстанс брокера для тикера (ленивая инициализация).
        """
        name = self.get_broker_name_for_symbol(symbol)
        if name not in self._brokers:
            try:
                broker = get_broker(name)
                await broker.initialize()
                self._brokers[name] = broker
            except Exception as e:
                raise RuntimeError(f"Failed to initialize broker '{name}' for symbol '{symbol}': {e}")
        return self._brokers[name]

    # ---------- Высокоуровневые операции ----------

    async def execute_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str = "market",
        client_id: str | None = None,
    ) -> OrderResult:
        """
        Унифицированное выполнение ордера через правильного брокера.
        place_order -> (если возможно) wait_for_order_final
        """
        mode_obj = getattr(Config, "EXECUTION_MODE", ExecutionMode.BACKTEST)
        mode = mode_obj.value if isinstance(mode_obj, ExecutionMode) else str(mode_obj).lower()

        if mode == "live":
            # DD-guard должен блокировать только риск-увеличивающие ордера (входы),
            # но НЕ мешать закрытию позиций.
            if str(side).lower() in {"buy"}:
                await self._check_daily_drawdown_guard()

        if quantity <= 0:
            raise ValueError("ExecutionRouter.execute_order: quantity must be > 0")

        broker = await self.get_broker_for_symbol(symbol)

        order = OrderRequest(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=order_type,
            client_id=client_id,
        )

        res = await broker.place_order(order)

        timeout_s = float(getattr(Config, "ORDER_CONFIRM_TIMEOUT_S", 30.0))

        # Пытаемся дождаться финального статуса
        try:
            final = await broker.wait_for_order_final(
                order_id=getattr(res, "order_id", None) or None,
                client_id=client_id,
                symbol=symbol,
                timeout_s=timeout_s,
            )
            return final
        except NotImplementedError:
            return res
        except Exception as e:
            print(f"[WARN] ExecutionRouter: wait_for_order_final failed: {e}")
            return res

    async def cancel_all_orders(self, symbols: list[str] | None = None) -> None:
        """
        (2) Kill-switch helper: отменяет активные ордера по известным символам.
        Если symbols=None -> берём из открытых позиций.
        """
        if symbols is None:
            try:
                positions = await self.list_all_positions()
                symbols = sorted({p.symbol for p in positions})
            except Exception:
                symbols = []

        if not symbols:
            return

        for name, broker in self._brokers.items():
            for sym in symbols:
                try:
                    orders = await broker.get_open_orders(sym)
                except NotImplementedError:
                    continue
                except Exception as e:
                    print(f"[WARN] cancel_all_orders: get_open_orders failed for {name}/{sym}: {e}")
                    continue

                for o in orders:
                    oid = getattr(o, "order_id", None)
                    if not oid:
                        continue
                    try:
                        await broker.cancel_order(str(oid), symbol=sym)
                    except NotImplementedError:
                        continue
                    except Exception as e:
                        print(f"[WARN] cancel_all_orders: cancel_order failed for {name}/{sym}/{oid}: {e}")

    async def close_all_positions(self, reason: str = "kill-switch") -> None:
        """
        (2) Kill-switch: закрывает ВСЕ позиции на всех брокерах.

        Алгоритм:
          1) отменяем активные ордера (по символам позиций)
          2) закрываем позиции MARKET (если брокер умеет close_position)
        """
        try:
            await self.cancel_all_orders()
        except Exception as e:
            print(f"[WARN] close_all_positions: cancel_all_orders failed: {e}")

        positions = await self.list_all_positions()
        if not positions:
            return

        for p in positions:
            br = None
            try:
                pb = str(getattr(p, "broker", "") or "").lower()
                if "tinkoff" in pb and "tinkoff" in self._brokers:
                    br = self._brokers["tinkoff"]
                elif "bitget" in pb and "bitget" in self._brokers:
                    br = self._brokers["bitget"]
            except Exception:
                br = None

            if br is None:
                try:
                    br = await self.get_broker_for_symbol(p.symbol)
                except Exception:
                    br = None

            if not br:
                continue

            try:
                await br.close_position(p.symbol, reason=reason)
                print(f"🧨 Closed position: {p.symbol} @ broker={getattr(p, 'broker', 'unknown')} reason={reason}")
            except NotImplementedError:
                print(f"[WARN] close_all_positions: {getattr(p,'broker','?')} close_position not implemented")
            except Exception as e:
                print(f"[WARN] close_all_positions: failed closing {p.symbol}: {e}")

    async def get_global_account_state(self) -> GlobalAccountState:
        """
        Агрегирует состояние по всем брокерам.
        """
        total_equity = 0.0
        total_balance = 0.0
        details: Dict[str, AccountState] = {}

        # Используем только уже инициализированных брокеров
        for name, broker in self._brokers.items():
            try:
                state = await broker.get_account_state()
            except NotImplementedError:
                continue
            except Exception as e:
                print(f"[WARN] ExecutionRouter: get_account_state failed for {name}: {e}")
                continue

            total_equity += state.equity
            total_balance += state.balance
            details[name] = state

        return GlobalAccountState(
            equity=total_equity,
            balance=total_balance,
            details=details,
        )

    async def list_all_positions(self) -> List[Position]:
        """
        Собирает все открытые позиции по всем брокерам.
        """
        positions: List[Position] = []

        for name, broker in self._brokers.items():
            try:
                broker_positions = await broker.list_open_positions()
            except NotImplementedError:
                continue
            except Exception as e:
                print(f"[WARN] ExecutionRouter: list_open_positions failed for {name}: {e}")
                continue

            for p in broker_positions:
                # Если брокер не проставил имя сам — проставим здесь
                if not getattr(p, "broker", None):
                    p.broker = name
                positions.append(p)

        return positions