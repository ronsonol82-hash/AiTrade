# brokers/tinkoff_client.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import asyncio
import json
import time
import math  # <--- Добавили для корректного округления

import pandas as pd
import aiohttp

from .base import BrokerAPI, OrderRequest, OrderResult, Position, AccountState

class TinkoffV2Broker(BrokerAPI):
    """
    Асинхронный клиент для Tinkoff Invest API v2 (aiohttp).
    Версия: Production-Ready (с защитой от Race Condition и Float Dust).
    """

    name = "tinkoff"

    def __init__(self, config: Dict[str, Any]):
        token = config.get("token") or ""
        if not token:
            raise RuntimeError(
                "TinkoffV2Broker: не задан токен. "
                "Укажи его в Config.BROKERS['tinkoff']['token']."
            )

        self.token = token
        self.sandbox = bool(config.get("sandbox", False))

        default_base = "https://sandbox-invest-public-api.tbank.ru/rest" if self.sandbox else "https://invest-public-api.tbank.ru/rest"
        self.base_url: str = config.get("base_url", default_base)

        self.session: Optional[aiohttp.ClientSession] = None
        
        # --- Rate Limits ---
        self._history_min_interval = 60.0 / 25.0  # ~2.4с между запросами свечей
        self._last_history_call_ts: float = 0.0
        self._rate_limit_lock = asyncio.Lock()  # <--- ЗАЩИТА ОТ ГОНКИ ПОТОКОВ

        # --- Caches ---
        self._lot_sizes: Dict[str, int] = {}
        self._figi_cache: Dict[str, str] = {} 

    # =====================================================================
    # Lifecycle
    # =====================================================================

    async def initialize(self) -> None:
        if self.session is None or self.session.closed:
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            # Timeouts: connect 5s (быстро падаем если нет сети), total 30s (на большие пейлоады)
            timeout = aiohttp.ClientTimeout(total=30, connect=5)
            self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    # =====================================================================
    # Helpers
    # =====================================================================

    @staticmethod
    def _to_rfc3339(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    @staticmethod
    def _interval_to_v2_enum(interval: str) -> str:
        norm = interval.lower()
        mapping = {
            "1m": "CANDLE_INTERVAL_1_MIN", "1min": "CANDLE_INTERVAL_1_MIN",
            "5m": "CANDLE_INTERVAL_5_MIN", "5min": "CANDLE_INTERVAL_5_MIN",
            "15m": "CANDLE_INTERVAL_15_MIN", "15min": "CANDLE_INTERVAL_15_MIN",
            "30m": "CANDLE_INTERVAL_30_MIN", "30min": "CANDLE_INTERVAL_30_MIN",
            "1h": "CANDLE_INTERVAL_HOUR", "60m": "CANDLE_INTERVAL_HOUR", "hour": "CANDLE_INTERVAL_HOUR",
            "1d": "CANDLE_INTERVAL_DAY", "day": "CANDLE_INTERVAL_DAY", "24h": "CANDLE_INTERVAL_DAY"
        }
        return mapping.get(norm, "CANDLE_INTERVAL_HOUR")

    @staticmethod
    def _max_delta_for_interval(interval: str) -> timedelta:
        norm = interval.lower()
        if norm in ("1m", "1min", "2m", "3m", "5m", "10m", "15m"):
            return timedelta(days=1)
        if norm in ("30m", "30min"):
            return timedelta(days=2)
        if norm in ("1h", "60m", "hour"):
            return timedelta(days=7)
        return timedelta(days=365)

    @staticmethod
    def _q_to_float(q: Dict[str, Any]) -> float:
        if not q: return 0.0
        return float(q.get("units", 0)) + float(q.get("nano", 0)) / 1e9

    def _resolve_figi(self, symbol: str) -> str:
        if symbol in self._figi_cache:
            return self._figi_cache[symbol]
        from config import Config
        figi_map = getattr(Config, "TINKOFF_FIGI_MAP", {})
        figi = figi_map.get(symbol)
        if not figi:
            raise KeyError(f"TinkoffV2Broker: не найден FIGI для '{symbol}'.")
        self._figi_cache[symbol] = figi
        return figi

    def _resolve_ticker(self, figi: str) -> str:
        from config import Config
        figi_map = getattr(Config, "TINKOFF_FIGI_MAP", {}) or {}
        for k, v in figi_map.items():
            if v == figi:
                return k
        return figi

    async def _post_with_backoff(self, url: str, payload: Dict[str, Any], is_history: bool = False) -> Dict[str, Any]:
        if self.session is None:
            await self.initialize()

        max_attempts = 5
        backoff = 0.5

        for attempt in range(1, max_attempts + 1):
            # --- Rate Limit Logic with Lock ---
            if is_history:
                async with self._rate_limit_lock:  # Блокируем, чтобы только один поток проверял таймер
                    now = time.time()
                    elapsed = now - self._last_history_call_ts
                    wait = self._history_min_interval - elapsed
                    if wait > 0:
                        await asyncio.sleep(wait)
                    self._last_history_call_ts = time.time() # Обновляем время ПОСЛЕ сна, но внутри лока

            try:
                async with self.session.post(url, json=payload) as resp:
                    if resp.status == 429:
                        print(f"[TINKOFF] 429 Rate Limit. Waiting {backoff:.2f}s...")
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    if resp.status >= 500:
                        print(f"[TINKOFF] Server Error {resp.status}. Retry...")
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    
                    resp.raise_for_status()
                    return await resp.json()

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < max_attempts:
                    print(f"[TINKOFF] Network error: {e}. Retry {attempt}...")
                    await asyncio.sleep(backoff)
                    backoff *= 2
                else:
                    raise

        raise RuntimeError(f"TinkoffV2Broker: Failed request to {url}")

    async def _get_lot_size(self, figi: str) -> int:
        if figi in self._lot_sizes:
            return self._lot_sizes[figi]
        url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetInstrumentBy"
        payload = {"idType": "INSTRUMENT_ID_TYPE_FIGI", "id": figi}
        try:
            data = await self._post_with_backoff(url, payload)
            item = data.get("instrument", {})
            lot = int(item.get("lot", 1))
            self._lot_sizes[figi] = max(1, lot)
            return self._lot_sizes[figi]
        except Exception as e:
            print(f"[WARN] Failed to get lot size for {figi}: {e}")
            return 1

    # =====================================================================
    # Market Data
    # =====================================================================

    async def get_historical_klines(self, symbol: str, interval: str, start: datetime, end: datetime) -> pd.DataFrame:
        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=timezone.utc)
        
        # Важно: сохраняем структуру колонок для совместимости с стратегиями
        columns = ["open", "high", "low", "close", "volume", "taker_buy_base", "funding_rate", "imbalance"]
        
        if end <= start:
            return pd.DataFrame(columns=columns)

        figi = self._resolve_figi(symbol)
        interval_enum = self._interval_to_v2_enum(interval)
        max_delta = self._max_delta_for_interval(interval)
        
        url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
        
        all_rows = []
        cur_from = start
        
        chunk_idx = 0
        max_chunks = 2000 # Защита от бесконечного цикла

        while cur_from < end and chunk_idx < max_chunks:
            chunk_idx += 1
            cur_to = min(cur_from + max_delta, end)
            
            payload = {
                "figi": figi,
                "from": self._to_rfc3339(cur_from),
                "to": self._to_rfc3339(cur_to),
                "interval": interval_enum,
            }

            try:
                data = await self._post_with_backoff(url, payload, is_history=True)
                candles = data.get("candles", [])
                
                if not candles:
                    cur_from = cur_to
                    continue

                for c in candles:
                    ts_str = c["time"]
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).replace(tzinfo=None)
                    
                    row = {
                        "open_time": ts,
                        "open": self._q_to_float(c.get("open")),
                        "high": self._q_to_float(c.get("high")),
                        "low": self._q_to_float(c.get("low")),
                        "close": self._q_to_float(c.get("close")),
                        "volume": float(c.get("volume", 0)),
                        # Заглушки для совместимости с крипто-стратегиями
                        "taker_buy_base": 0.0,
                        "funding_rate": 0.0,
                        "imbalance": 0.0,
                    }
                    all_rows.append(row)
                
                cur_from = cur_to

            except Exception as e:
                print(f"[TINKOFF] Error fetching candles: {e}")
                break

        if not all_rows:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(all_rows)
        df.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
        df.sort_values("open_time", inplace=True)
        df.set_index("open_time", inplace=True)
        
        return df[columns]

    async def get_current_price(self, symbol: str) -> float:
        figi = self._resolve_figi(symbol)
        url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices"
        payload = {"instrumentId": [figi]}
        data = await self._post_with_backoff(url, payload)
        prices = data.get("lastPrices", [])
        if not prices:
            raise RuntimeError(f"No price for {symbol}")
        return self._q_to_float(prices[0].get("price"))

    # =====================================================================
    # Trading / Account
    # =====================================================================

    async def get_account_state(self) -> AccountState:
        url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"
        data = await self._post_with_backoff(url, {})
        accounts = data.get("accounts", [])
        if not accounts:
             return AccountState(equity=0.0, balance=0.0, currency="RUB", margin_used=0.0, broker=self.name)

        aid = accounts[0].get("id")
        pf_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.OperationsService/GetPortfolio"
        pf_data = await self._post_with_backoff(pf_url, {"accountId": aid})
        
        total = self._q_to_float(pf_data.get("totalAmountPortfolio", {}))
        cash = self._q_to_float(pf_data.get("totalAmountCurrencies", {}))
        
        return AccountState(equity=total, balance=cash, currency="RUB", margin_used=0.0, broker=self.name)

    async def list_open_positions(self) -> List[Position]:
        acc_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"
        acc_data = await self._post_with_backoff(acc_url, {})
        accounts = acc_data.get("accounts", [])
        if not accounts: return []
        aid = accounts[0].get("id")

        pf_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.OperationsService/GetPortfolio"
        pf_data = await self._post_with_backoff(pf_url, {"accountId": aid})
        
        raw_pos = pf_data.get("positions", [])
        result = []
        for p in raw_pos:
            figi = p.get("figi")
            qty = self._q_to_float(p.get("quantity", {}))
            if qty == 0: continue
            
            avg = self._q_to_float(p.get("averagePositionPrice", {}))
            last = self._q_to_float(p.get("currentPrice", {})) 
            symbol = self._resolve_ticker(figi)
            
            pos = Position(
                symbol=symbol,
                instrument_id=figi,
                quantity=qty,
                avg_price=avg,
                last_price=last if last > 0 else avg,
                unrealized_pnl=self._q_to_float(p.get("expectedYield", {})),
                broker=self.name
            )
            result.append(pos)
        return result

    async def place_order(self, order: OrderRequest) -> OrderResult:
        # 1. Account
        acc_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"
        acc_data = await self._post_with_backoff(acc_url, {})
        if not acc_data.get("accounts"): raise RuntimeError("No accounts")
        aid = acc_data["accounts"][0]["id"]

        # 2. Prep
        figi = self._resolve_figi(order.symbol)
        lot_size = await self._get_lot_size(figi)
        
        # [CRITICAL FIX] Защита от floating point dust (9.9999 -> 10.0)
        # Добавляем epsilon перед делением, чтобы компенсировать погрешность вниз
        raw_qty = float(order.quantity)
        lots = int((raw_qty + 1e-9) // lot_size) 
        
        if lots <= 0:
            raise ValueError(f"Quantity {raw_qty} is less than 1 lot ({lot_size}) for {order.symbol}")
            
        direction = "ORDER_DIRECTION_BUY" if order.side == "buy" else "ORDER_DIRECTION_SELL"
        o_type = "ORDER_TYPE_MARKET" if order.order_type == "market" else "ORDER_TYPE_LIMIT"
        
        payload = {
            "accountId": aid,
            "figi": figi,
            "quantity": lots,
            "direction": direction,
            "orderType": o_type,
            "orderId": order.client_id or f"bot-{int(time.time()*1000)}"
        }
        
        if order.order_type == "limit":
            pr = float(order.price)
            u = int(pr)
            n = int(round((pr - u) * 1e9))
            payload["price"] = {"units": str(u), "nano": n}

        # 3. Exec
        post_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.OrdersService/PostOrder"
        resp_data = await self._post_with_backoff(post_url, payload)
        
        oid = resp_data.get("orderId")
        exec_price = self._q_to_float(resp_data.get("executedOrderPrice", {}))
        
        return OrderResult(
            order_id=oid,
            symbol=order.symbol,
            side=order.side,
            quantity=float(lots * lot_size),
            price=exec_price if exec_price > 0 else float(order.price or 0),
            status="filled",
            broker=self.name
        )

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> None:
        acc_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"
        acc_data = await self._post_with_backoff(acc_url, {})
        if not acc_data.get("accounts"): return
        aid = acc_data["accounts"][0]["id"]

        url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.OrdersService/CancelOrder"
        await self._post_with_backoff(url, {"accountId": aid, "orderId": order_id})

    async def get_open_orders(self, symbol: str) -> List[OrderResult]:
        acc_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"
        acc_data = await self._post_with_backoff(acc_url, {})
        if not acc_data.get("accounts"): return []
        aid = acc_data["accounts"][0]["id"]

        url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.OrdersService/GetOrders"
        data = await self._post_with_backoff(url, {"accountId": aid})
        
        orders = data.get("orders", [])
        res = []
        for o in orders:
            res.append(OrderResult(
                order_id=o.get("orderId"),
                symbol=symbol,
                side="buy" if o.get("direction") == "ORDER_DIRECTION_BUY" else "sell",
                quantity=float(o.get("lotsRequested", 0)),
                price=self._q_to_float(o.get("initialSecurityPrice", {})),
                status="new",
                broker=self.name
            ))
        return res

    # =====================================================================
    # [RESTORED] Missing Methods for GUI / Kill-Switch
    # =====================================================================

    async def list_active_orders(self, symbol: str | None = None) -> List[OrderResult]:
        return await self.get_open_orders(symbol or "")

    async def get_order_info(self, *, order_id: str | None = None, client_id: str | None = None, symbol: str | None = None) -> dict:
        if not order_id and not client_id:
            raise ValueError("get_order_info: order_id or client_id required")

        acc_url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"
        acc_data = await self._post_with_backoff(acc_url, {})
        if not acc_data.get("accounts"): return {}
        aid = acc_data["accounts"][0]["id"]

        url = f"{self.base_url}/tinkoff.public.invest.api.contract.v1.OrdersService/GetOrderState"
        payload = {
            "accountId": aid,
            "orderId": str(order_id or client_id),
            "orderIdType": "ORDER_ID_TYPE_REQUEST",
        }
        resp = await self._post_with_backoff(url, payload)
        return resp or {}

    async def wait_for_order_final(self, *, order_id: str | None = None, client_id: str | None = None, symbol: str | None = None, timeout_s: float = 30.0, poll_s: float = 0.7) -> OrderResult:
        deadline = time.time() + float(timeout_s)
        last: dict = {}

        while time.time() < deadline:
            try:
                last = await self.get_order_info(order_id=order_id, client_id=client_id, symbol=symbol)
                status = str(last.get("executionReportStatus") or "").upper()
                if any(x in status for x in ("FILL", "CANCEL", "REJECT")):
                    break
            except Exception:
                pass
            await asyncio.sleep(float(poll_s))

        status_raw = str(last.get("executionReportStatus") or "").upper() or "UNKNOWN"
        lots_exec = float(last.get("lotsExecuted") or 0)
        lots_req = float(last.get("lotsRequested") or 0)

        norm_status = "unknown"
        if "REJECT" in status_raw: norm_status = "rejected"
        elif "CANCEL" in status_raw: norm_status = "canceled"
        elif "FILL" in status_raw: norm_status = "filled"
        elif lots_exec > 0: norm_status = "submitted"

        direction = str(last.get("direction") or "")
        side = "buy" if direction == "ORDER_DIRECTION_BUY" else "sell" if direction == "ORDER_DIRECTION_SELL" else "buy"
        
        price_q = last.get("executedOrderPrice") or {}
        try: avg = float(self._q_to_float(price_q))
        except: avg = 0.0

        figi = str(last.get("figi") or "")
        sym = symbol or (self._resolve_ticker(figi) if figi else "")
        qty = lots_exec if lots_exec > 0 else lots_req
        oid = str(last.get("orderId") or (order_id or client_id or ""))

        return OrderResult(
            order_id=oid, symbol=sym, side=side, quantity=float(qty),
            price=float(avg), status=norm_status, broker=self.name
        )

    async def close_position(self, symbol: str, reason: str = "") -> None:
        """
        Закрыть позицию. 
        ВАЖНО: Метод теперь отправляет сырое количество (абсолютное значение),
        а place_order сам разбирается с округлением до лотов через (raw + epsilon) // lot_size.
        """
        positions = await self.list_open_positions()
        pos = next((p for p in positions if p.symbol == symbol), None)
        if not pos: return
        qty = abs(float(pos.quantity))
        if qty <= 1e-9: return # Защита от микро-пыли

        side = "sell" if float(pos.quantity) > 0 else "buy"
        client_id = f"kill-{int(time.time()*1000)}-{symbol}"
        
        # Просто передаем количество, place_order сделает всё остальное.
        req = OrderRequest(
            symbol=symbol, 
            side=side, 
            quantity=qty, 
            order_type="market", 
            client_id=client_id
        )
        await self.place_order(req)