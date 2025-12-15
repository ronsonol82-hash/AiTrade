# brokers/bitget_client.py
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import random
import time
from datetime import datetime
from rate_limiter import AsyncTokenBucket
from typing import List, Dict, Any, Optional, Literal, Union
from decimal import Decimal, ROUND_DOWN

import aiohttp
import pandas as pd

from .base import BrokerAPI, OrderRequest, OrderResult, Position, AccountState

logger = logging.getLogger(__name__)

class BitgetHTTPError(RuntimeError):
    def __init__(self, status: int, body: str, *, retry_after_s: float | None = None):
        super().__init__(f"Bitget HTTP {status}: {body}")
        self.status = int(status)
        self.body = body
        self.retry_after_s = retry_after_s


class BitgetAPIError(RuntimeError):
    def __init__(self, code: str, msg: str):
        super().__init__(f"Bitget API error {code}: {msg}")
        self.code = str(code)
        self.msg = str(msg)


class BitgetBroker(BrokerAPI):
    """
    Async-брокер для Bitget (SPOT, V2 API).

    Реализовано:
      - get_historical_klines (через /api/v2/spot/market/candles)
      - get_current_price   (через /api/v2/spot/market/tickers)
      - get_account_state   (через /api/v2/spot/account/assets)
      - place_order         (через /api/v2/spot/trade/place-order)
      - cancel_order        (через /api/v2/spot/trade/cancel-order)
      - get_open_orders     (через /api/v2/spot/trade/unfilled-orders)
      - list_open_positions (по балансу спотовых монет)

    В проде всё это ещё придётся полировать под твои конкретные настройки.
    """

    name = "bitget"

    def __init__(self, config: Dict[str, Any]):
        self.api_key: str = config.get("api_key", "")
        self.api_secret: str = config.get("api_secret", "")
        self.passphrase: str = config.get("passphrase", "")
        self.base_url: str = config.get("base_url", "https://api.bitget.com")

        self.session: Optional[aiohttp.ClientSession] = None
        self.timeout = aiohttp.ClientTimeout(total=10)

        # LIVE hardening: retry/backoff (можно переопределять через config dict)
        self.http_max_retries: int = int(config.get("http_max_retries", 4) or 4)
        self.http_backoff_base_s: float = float(config.get("http_backoff_base_s", 0.5) or 0.5)
        self.http_backoff_cap_s: float = float(config.get("http_backoff_cap_s", 5.0) or 5.0)

        # --- client-side rate limit guard ---
        rps = float(config.get("http_rps", 8.0) or 8.0)         # запросов/сек
        burst = int(config.get("http_burst", 12) or 12)         # “короткий всплеск”
        max_inflight = int(config.get("http_max_inflight", 4) or 4)

        self._rl = AsyncTokenBucket(rate_per_sec=rps, burst=burst)
        self._inflight = asyncio.Semaphore(max_inflight)

        # [NEW] Хранилище правил торговли: symbol -> precision (int)
        self._symbol_rules: Dict[str, int] = {}
        self._price_rules: Dict[str, int] = {}       # price precision
    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=self.timeout)
            logger.info("BitgetBroker: async session initialized.")
        
        # [FIX] Загружаем правила торговли при старте
        await self._refresh_symbol_rules()

    async def _refresh_symbol_rules(self) -> None:
        """Загружает точность (quantityPrecision) для всех пар."""
        try:
            # V2 Endpoint для публичной инфы
            data = await self._request("GET", "/api/v2/spot/public/symbols", signed=False)
            if not data: 
                return
            
            # data - это список словарей
            for item in data:
                s = item.get("symbol", "")
                # Bitget возвращает 'quantityPrecision' строкой или числом
                p_qty = int(item.get("quantityPrecision", 4) or 4)
                p_px  = int(item.get("pricePrecision", 6) or 6)

                self._symbol_rules[s] = p_qty
                self._price_rules[s] = p_px
            
            logger.info(f"Bitget: loaded rules for {len(self._symbol_rules)} symbols")
        except Exception as e:
            logger.error(f"Bitget: failed to load symbol rules: {e}")

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None
            logger.info("BitgetBroker: async session closed.")

    # ------------------------------------------------------------------
    # Вспомогательные мапперы и подпись
    # ------------------------------------------------------------------

    @staticmethod
    def _to_bitget_symbol(symbol: str) -> str:
        """
        Внутренний BTCUSDT -> биржевой тикер Bitget.
        Для spot V2 Bitget использует просто 'BTCUSDT'.
        """
        return symbol

    @staticmethod
    def _interval_to_granularity(interval: str) -> str:
        """
        Маппинг внутренних таймфреймов на granularity Bitget.
        """
        normalized = interval.lower()

        mapping = {
            "1m": "1min",
            "3m": "3min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "1h",
            "4h": "4h",
            "6h": "6h",
            "12h": "12h",
            "1d": "1day",
            "1day": "1day",
            "3d": "3day",
            "1w": "1week",
            "1wk": "1week",
            "1week": "1week",
            "1mo": "1M",
            "1mth": "1M",
        }

        return mapping.get(normalized, normalized)

    def _generate_signature(
        self,
        method: str,
        path: str,
        query: str,
        body: str,
        timestamp: str,
    ) -> str:
        """
        Bitget V2 signature:
          sign = base64( HMAC_SHA256( secret, ts + method + requestPath + body ) )
        где requestPath = path + '?' + query (если есть query).
        """
        request_path = path
        if query:
            request_path = f"{path}?{query}"

        message = f"{timestamp}{method.upper()}{request_path}{body}"
        mac = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            digestmod=hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    async def _raw_request_once(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Dict[str, Any]:
        """
        Один HTTP вызов к Bitget без ретраев.
        Возвращает весь payload (dict), чтобы вызывающий мог решить что делать дальше.
        """
        if self.session is None:
            await self.initialize()
        assert self.session is not None

        url = f"{self.base_url}{endpoint}"
        ts = str(int(time.time() * 1000))

        headers = {
            "Content-Type": "application/json",
            "Locale": "en-US",
        }

        body_str = ""
        query_str = ""

        params_list = None
        if params:
            from urllib.parse import urlencode
            params_list = [(str(k), str(v)) for k, v in sorted(params.items(), key=lambda x: x[0])]
            query_str = urlencode(params_list)

        if data:
            body_str = json.dumps(data, separators=(",", ":"), sort_keys=True)

        if signed:
            if not (self.api_key and self.api_secret and self.passphrase):
                raise ValueError("BitgetBroker: API credentials missing for signed request.")
            signature = self._generate_signature(method, endpoint, query_str, body_str, ts)
            headers.update(
                {
                    "ACCESS-KEY": self.api_key,
                    "ACCESS-SIGN": signature,
                    "ACCESS-TIMESTAMP": ts,
                    "ACCESS-PASSPHRASE": self.passphrase,
                }
            )

        try:
            async with self.session.request(
                method.upper(),
                url,
                params=params_list if method.upper() == "GET" else None,
                data=body_str if method.upper() in {"POST", "DELETE"} else None,
                headers=headers,
            ) as resp:
                text = await resp.text()

                if resp.status != 200:
                    retry_after = None
                    try:
                        ra = resp.headers.get("Retry-After")
                        if ra:
                            retry_after = float(ra)
                    except Exception:
                        retry_after = None
                    raise BitgetHTTPError(resp.status, text, retry_after_s=retry_after)

                payload = json.loads(text)
                return payload

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Bitget JSON decode failed: {e}")


    @staticmethod
    def _is_retryable_api_error(code: str, msg: str) -> bool:
        c = str(code or "").strip()
        m = (msg or "").lower()
        if c in {"429"}:
            return True
        if "too many" in m or "rate" in m or "frequency" in m or "busy" in m:
            return True
        return False


    def _calc_backoff_s(self, attempt: int, *, retry_after_s: float | None = None) -> float:
        if retry_after_s is not None and retry_after_s > 0:
            return float(min(retry_after_s, self.http_backoff_cap_s))
        base = float(self.http_backoff_base_s)
        cap = float(self.http_backoff_cap_s)
        exp = min(cap, base * (2 ** max(0, attempt)))
        jitter = 0.5 + random.random() * 0.5  # 0.5..1.0
        return float(min(cap, exp * jitter))


    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Dict[str, Any]:
        """
        LIVE hardening:
        - retry/backoff на 429 и 5xx
        - retry/backoff на сетевые/таймаут ошибки
        """
        max_retries = int(getattr(self, "http_max_retries", 4) or 4)
        retryable_http = {408, 425, 429, 500, 502, 503, 504}

        last_err: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                await self._inflight.acquire()
                try:
                    await self._rl.acquire()  # <= вот он, “глушитель 429”
                    payload = await self._raw_request_once(
                        method, endpoint, params=params, data=data, signed=signed
                    )
                finally:
                    self._inflight.release()

                if payload.get("code") != "00000":
                    code = str(payload.get("code") or "")
                    msg = payload.get("msg", "Unknown error")
                    if attempt < max_retries and self._is_retryable_api_error(code, str(msg)):
                        sleep_s = self._calc_backoff_s(attempt)
                        logger.warning(f"Bitget API retryable error {code}: {msg}. retry in {sleep_s:.2f}s")
                        await asyncio.sleep(sleep_s)
                        continue
                    raise BitgetAPIError(code, str(msg))

                return payload.get("data", {})

            except BitgetHTTPError as e:
                last_err = e
                if e.status in retryable_http and attempt < max_retries:
                    sleep_s = self._calc_backoff_s(attempt, retry_after_s=getattr(e, "retry_after_s", None))
                    logger.warning(f"Bitget HTTP {e.status} retry in {sleep_s:.2f}s (endpoint={endpoint})")
                    await asyncio.sleep(sleep_s)
                    continue
                logger.error(f"Bitget HTTP {e.status}: {e.body}")
                raise

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                if attempt < max_retries:
                    sleep_s = self._calc_backoff_s(attempt)
                    logger.warning(f"Bitget network error: {e} retry in {sleep_s:.2f}s (endpoint={endpoint})")
                    await asyncio.sleep(sleep_s)
                    continue
                logger.exception(f"Bitget request failed (final): {method} {endpoint}: {e}")
                raise

            except Exception as e:
                last_err = e
                logger.exception(f"Bitget request failed: {method} {endpoint}: {e}")
                raise

        if last_err:
            raise last_err
        raise RuntimeError("Bitget request failed: unknown error")

    # ------------------------------------------------------------------
    # BrokerAPI: MARKET DATA
    # ------------------------------------------------------------------

    async def get_historical_klines(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        bg_symbol = self._to_bitget_symbol(symbol)
        granularity = self._interval_to_granularity(interval)

        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        params = {
            "symbol": bg_symbol,
            "granularity": granularity,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000,
        }

        data = await self._request(
            "GET",
            "/api/v2/spot/market/candles",
            params=params,
            signed=False,
        )

        if not data or (isinstance(data, list) and len(data) == 0):
            return pd.DataFrame(
                columns=[
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "taker_buy_base",
                    "funding_rate",
                    "imbalance",
                ]
            )

        rows = []
        for item in data:
            if isinstance(item, dict):
                ts_raw = item.get("ts")
                ts_ms = int(float(ts_raw))
                open_ = float(item["open"])
                high = float(item["high"])
                low = float(item["low"])
                close = float(item["close"])
                base_vol = float(item.get("baseVol", item.get("baseVolume", 0.0)))
            else:
                # ожидаемый формат: [ts, open, high, low, close, baseVol, quoteVol, usdtVol]
                if len(item) < 6:
                    logger.warning(f"Bitget candles: unexpected array length={len(item)}: {item}")
                    continue
                ts_ms = int(float(item[0]))
                open_ = float(item[1])
                high = float(item[2])
                low = float(item[3])
                close = float(item[4])
                base_vol = float(item[5])

            ts = datetime.utcfromtimestamp(ts_ms / 1000.0)

            rows.append(
                {
                    "open_time": ts,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": base_vol,
                    "taker_buy_base": 0.0,
                    "funding_rate": 0.0,
                    "imbalance": 0.0,
                }
            )

        if not rows:
            return pd.DataFrame(
                columns=[
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "taker_buy_base",
                    "funding_rate",
                    "imbalance",
                ]
            )

        df = pd.DataFrame(rows)
        df.sort_values("open_time", inplace=True)
        df.set_index("open_time", inplace=True)

        return df[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "taker_buy_base",
                "funding_rate",
                "imbalance",
            ]
        ]

    async def get_current_price(self, symbol: str) -> float:
        bg_symbol = self._to_bitget_symbol(symbol)
        params = {"symbol": bg_symbol}

        data = await self._request(
            "GET",
            "/api/v2/spot/market/tickers",
            params=params,
            signed=False,
        )

        if not data or (isinstance(data, list) and len(data) == 0):
            raise RuntimeError(f"Bitget ticker empty for symbol={bg_symbol}")

        ticker = data[0]
        last_pr = ticker.get("lastPr", "0")
        if not last_pr:
            raise RuntimeError(f"Bitget ticker missing lastPr for symbol={bg_symbol}")
        
        return float(last_pr)

    # ------------------------------------------------------------------
    # BrokerAPI: ACCOUNT / PORTFOLIO
    # ------------------------------------------------------------------

    async def get_account_state(self) -> AccountState:
        """
        Упрощённо считаем equity по USDT.
        Остальные монеты сейчас можно считать как "дополнительно сверху".
        """
        data = await self._request(
            "GET",
            "/api/v2/spot/account/assets",
            signed=True,
        )

        if not data or (isinstance(data, list) and len(data) == 0):
            logger.warning("Bitget account assets returned empty data")
            return AccountState(
                equity=0.0,
                balance=0.0,
                currency="USDT",
                margin_used=0.0,
                broker=self.name,
            )

        details: Dict[str, Dict[str, float]] = {}
        usdt_total = 0.0

        for asset in data:
            coin = asset.get("coin")
            if not coin:
                continue

            available = float(asset.get("available", 0))
            frozen = float(asset.get("frozen", 0))
            total = available + frozen

            details[coin] = {
                "available": available,
                "total": total,
            }

            # FIX: USDT берём здесь, внутри цикла
            if coin == "USDT":
                usdt_total = total

        # NEW: считаем equity как USDT + оценка монет в USDT
        equity_total = float(usdt_total)

        for coin, st in details.items():
            if coin == "USDT":
                continue
            qty = float((st or {}).get("total", 0.0) or 0.0)
            if qty <= 0:
                continue

            sym = f"{coin}USDT"
            try:
                px = float(await self.get_current_price(sym))
            except Exception:
                continue

            equity_total += qty * px

        return AccountState(
            equity=equity_total,
            balance=usdt_total,   # cash
            currency="USDT",
            margin_used=0.0,
            broker=self.name,
        )

    async def list_open_positions(self) -> List[Position]:
        """
        Для спота считаем "позициями" любые монеты с total > 0, кроме USDT.
        Пока без средней цены входа (avg_price=0) и unrealized_pnl=0.
        """
        data = await self._request(
            "GET",
            "/api/v2/spot/account/assets",
            signed=True,
        )

        if not data or (isinstance(data, list) and len(data) == 0):
            return []

        positions: List[Position] = []
        for asset in data:
            coin = asset.get("coin")
            if not coin or coin == "USDT":
                continue
                
            available = float(asset.get("available", 0))
            frozen = float(asset.get("frozen", 0))
            total = available + frozen
            
            if total <= 0:
                continue

            positions.append(
                Position(
                    symbol=f"{coin}USDT",
                    quantity=total,
                    avg_price=0.0,
                    unrealized_pnl=0.0,
                    broker=self.name,
                )
            )

        return positions

    # ------------------------------------------------------------------
    # BrokerAPI: TRADING
    # ------------------------------------------------------------------

    async def place_order(self, order: OrderRequest) -> OrderResult:
        endpoint = "/api/v2/spot/trade/place-order"

        side_map: Dict[str, str] = {"buy": "buy", "sell": "sell"}
        type_map: Dict[str, str] = {"limit": "limit", "market": "market"}

        payload: Dict[str, Any] = {
            "symbol": self._to_bitget_symbol(order.symbol),
            "side": side_map.get(order.side, "buy"),
            "orderType": type_map.get(order.order_type, "limit"),
        }

        # size:
        # - limit и market-sell: base coin
        # - market-buy: quote coin (например USDT)
        if order.order_type == "market" and order.side == "buy":
            px = float(order.price) if order.price else float(await self.get_current_price(order.symbol))
            quote_size = float(order.quantity) * px
            payload["size"] = str(round(quote_size, 6))
        else:
            payload["size"] = self._qty_str(order.symbol, float(order.quantity))

        if order.order_type == "limit":
            payload["force"] = "gtc"

        if order.order_type == "limit":
            if order.price is None:
                raise ValueError("BitgetBroker.place_order: price required for limit order.")
            payload["price"] = self._price_str(order.symbol, float(order.price))

        client_oid = order.client_id or None
        if client_oid:
            payload["clientOid"] = client_oid

        async def _try_lookup_existing() -> dict | None:
            if not client_oid:
                return None
            try:
                info = await self.get_order_info(client_id=client_oid, symbol=order.symbol)
                if info and (info.get("orderId") or info.get("clientOid")):
                    return info
            except Exception:
                return None
            return None

        def _order_result_from_info(info: dict) -> OrderResult:
            st = (str(info.get("status") or "")).lower()
            if st == "filled":
                norm_status = "filled"
            elif st in {"cancelled", "canceled"}:
                norm_status = "canceled"
            elif st == "rejected":
                norm_status = "rejected"
            elif st in {"live", "partially_filled"}:
                norm_status = "submitted"
            else:
                norm_status = st or "submitted"

            avg = 0.0
            try:
                if str(info.get("priceAvg") or "").strip():
                    avg = float(info.get("priceAvg") or 0.0)
            except Exception:
                avg = 0.0

            px = avg
            if px <= 0:
                try:
                    px = float(info.get("price") or 0.0)
                except Exception:
                    px = 0.0

            qty_exec = 0.0
            try:
                if str(info.get("baseVolume") or "").strip():
                    qty_exec = float(info.get("baseVolume") or 0.0)
            except Exception:
                qty_exec = 0.0

            qty_req = 0.0
            try:
                if str(info.get("size") or "").strip():
                    qty_req = float(info.get("size") or 0.0)
            except Exception:
                qty_req = 0.0

            qty = qty_exec if qty_exec > 0 else (order.quantity if order.quantity > 0 else qty_req)

            return OrderResult(
                order_id=str(info.get("orderId") or ""),
                symbol=order.symbol,
                side=order.side,
                quantity=float(qty),
                price=float(px),
                status=norm_status,
                broker=self.name,
            )

        # --- ambiguity-safe retry loop ---
        max_retries = int(getattr(self, "http_max_retries", 4) or 4)
        retryable_http = {408, 425, 429, 500, 502, 503, 504}

        last_err: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                await self._inflight.acquire()
                try:
                    await self._rl.acquire()  # <= тот же “глушитель 429”, что и в _request()
                    env = await self._raw_request_once("POST", endpoint, data=payload, signed=True)
                finally:
                    self._inflight.release()

                if env.get("code") != "00000":
                    code = str(env.get("code") or "")
                    msg = env.get("msg", "Unknown error")

                    # перед ретраем — сначала пробуем найти ордер по clientOid
                    if attempt < max_retries and self._is_retryable_api_error(code, str(msg)):
                        existing = await _try_lookup_existing()
                        if existing:
                            return _order_result_from_info(existing)

                        sleep_s = self._calc_backoff_s(attempt)
                        logger.warning(f"Bitget place_order API retryable {code}: {msg}. retry in {sleep_s:.2f}s")
                        await asyncio.sleep(sleep_s)
                        continue

                    raise BitgetAPIError(code, str(msg))

                data = env.get("data", {}) or {}
                order_id = str(data.get("orderId", "") or "")
                return OrderResult(
                    order_id=order_id,
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    price=order.price or 0.0,
                    status="new",
                    broker=self.name,
                )

            except BitgetHTTPError as e:
                last_err = e
                if e.status in retryable_http and attempt < max_retries:
                    existing = await _try_lookup_existing()
                    if existing:
                        return _order_result_from_info(existing)

                    sleep_s = self._calc_backoff_s(attempt, retry_after_s=getattr(e, "retry_after_s", None))
                    logger.warning(f"Bitget place_order HTTP {e.status}. retry in {sleep_s:.2f}s")
                    await asyncio.sleep(sleep_s)
                    continue
                raise

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                if attempt < max_retries:
                    existing = await _try_lookup_existing()
                    if existing:
                        return _order_result_from_info(existing)

                    sleep_s = self._calc_backoff_s(attempt)
                    logger.warning(f"Bitget place_order network error: {e}. retry in {sleep_s:.2f}s")
                    await asyncio.sleep(sleep_s)
                    continue
                raise

            except Exception as e:
                last_err = e
                raise

        if last_err:
            raise last_err
        raise RuntimeError("Bitget place_order failed: unknown error")

    async def get_order_info(
        self,
        *,
        order_id: str | None = None,
        client_id: str | None = None,
        symbol: str | None = None,
    ) -> dict:
        """Сырой статус ордера (Bitget v2 spot)."""
        if not order_id and not client_id:
            raise ValueError("get_order_info: order_id or client_id required")

        params: dict[str, Any] = {}
        if order_id:
            params["orderId"] = str(order_id)
        else:
            # Важно: Bitget ограничивает ретеншн при запросе по clientOid (см. changelog).
            params["clientOid"] = str(client_id)

        data = await self._request("GET", "/api/v2/spot/trade/orderInfo", params=params, signed=True)
        # API возвращает список
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
        return {}

    async def wait_for_order_final(
        self,
        *,
        order_id: str | None = None,
        client_id: str | None = None,
        symbol: str | None = None,
        timeout_s: float = 30.0,
        poll_s: float = 0.5,
    ) -> OrderResult:
        deadline = time.time() + float(timeout_s)
        last: dict = {}

        while time.time() < deadline:
            try:
                last = await self.get_order_info(order_id=order_id, client_id=client_id, symbol=symbol)
                st = str(last.get("status") or "").lower()
                if st in {"filled", "cancelled", "canceled", "rejected"}:
                    break
            except Exception:
                pass
            await asyncio.sleep(float(poll_s))

        status = str(last.get("status") or "").lower() or "unknown"

        if status == "filled":
            norm_status = "filled"
        elif status in {"cancelled", "canceled"}:
            norm_status = "canceled"
        elif status == "rejected":
            norm_status = "rejected"
        elif status in {"live", "partially_filled"}:
            norm_status = "submitted"
        else:
            norm_status = status

        oid = str(last.get("orderId") or (order_id or ""))
        side_raw = str(last.get("side") or "").lower()
        side = "buy" if side_raw == "buy" else "sell" if side_raw == "sell" else "buy"

        avg = 0.0
        try:
            if str(last.get("priceAvg") or "").strip():
                avg = float(last.get("priceAvg") or 0.0)
        except Exception:
            avg = 0.0

        qty_exec = 0.0
        try:
            if str(last.get("baseVolume") or "").strip():
                qty_exec = float(last.get("baseVolume") or 0.0)
        except Exception:
            qty_exec = 0.0

        qty_req = 0.0
        try:
            if str(last.get("size") or "").strip():
                qty_req = float(last.get("size") or 0.0)
        except Exception:
            qty_req = 0.0

        qty = qty_exec if qty_exec > 0 else qty_req

        price = avg
        if price <= 0:
            try:
                price = float(last.get("price") or 0.0)
            except Exception:
                price = 0.0

        return OrderResult(
            order_id=oid,
            symbol=symbol or (self._to_bitget_symbol(str(last.get("symbol") or "")) or ""),
            side=side,
            quantity=float(qty),
            price=float(price),
            status=norm_status,
            broker=self.name,
        )

    async def list_active_orders(self, symbol: str | None = None) -> List[OrderResult]:
        """Активные лимит/маркет-ордера (не план)."""
        if not symbol:
            return []
        return await self.get_open_orders(symbol)

    async def get_plan_sub_order(self, plan_order_id: str) -> list[dict]:
        """Если план-ордер сработал — здесь появятся суб-ордера."""
        params = {"planOrderId": str(plan_order_id)}
        data = await self._request("GET", "/api/v2/spot/trade/plan-sub-order", params=params, signed=True)
        return data if isinstance(data, list) else []

    async def place_plan_order(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        size: float,
        *,
        order_type: str = "market",
        trigger_type: str = "market_price",
        execute_price: float | None = None,
        client_oid: str | None = None,
    ) -> dict:
        endpoint = "/api/v2/spot/trade/place-plan-order"
        payload = {
            "symbol": self._to_bitget_symbol(symbol),
            "side": side,  # buy/sell
            "triggerPrice": self._price_str(symbol, trigger_price),
            "orderType": order_type,          # limit/market
            "triggerType": trigger_type,      # fill_price/mark_price
            "planType": "amount",             # size в base coin (нам это нужно для закрытия позиции)
            "size": self._qty_str(symbol, size),
        }
        if execute_price is not None:
            payload["executePrice"] = self._price_str(symbol, execute_price)
        if client_oid:
            payload["clientOid"] = client_oid

        data = await self._request("POST", endpoint, data=payload, signed=True)
        return data
    
    async def place_protection_orders(
        self,
        symbol: str,
        qty: float,
        sl_price: float | None,
        tp_price: float | None,
        *,
        sl_client_oid: str | None = None,
        tp_client_oid: str | None = None,
        trigger_type: str = "mark_price",
    ) -> dict:
        """
        Ставит защитные триггер-ордера (SL/TP) для спота.
        Для long позиции: SL/TP — это sell plan orders.
        Возвращает структуру, которую ожидает AsyncStrategyRunner:
          {"sl": {"order_id": ...}, "tp": {"order_id": ...}}
        """
        out: dict = {}

        if sl_price:
            r_sl = await self.place_plan_order(
                symbol=symbol,
                side="sell",
                trigger_price=float(sl_price),
                size=float(qty),
                order_type="market",
                trigger_type=trigger_type,
                client_oid=sl_client_oid,
            )
            out["sl"] = {"order_id": (r_sl or {}).get("orderId")}

        if tp_price:
            r_tp = await self.place_plan_order(
                symbol=symbol,
                side="sell",
                trigger_price=float(tp_price),
                size=float(qty),
                order_type="market",
                trigger_type=trigger_type,
                client_oid=tp_client_oid,
            )
            out["tp"] = {"order_id": (r_tp or {}).get("orderId")}

        return out

    async def cancel_plan_order_legacy(self, *, order_id: str | None = None, client_oid: str | None = None) -> None:
        endpoint = "/api/v2/spot/trade/cancel-plan-order"
        payload: Dict[str, Any] = {}
        if order_id:
            payload["orderId"] = order_id
        if client_oid:
            payload["clientOid"] = client_oid
        if not payload:
            raise ValueError("BitgetBroker.cancel_plan_order: order_id or client_oid required")

        await self._request("POST", endpoint, data=payload, signed=True)

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> None:
        if not symbol:
            raise ValueError("BitgetBroker.cancel_order: symbol is required")

        endpoint = "/api/v2/spot/trade/cancel-order"
        payload = {
            "symbol": self._to_bitget_symbol(symbol),
            "orderId": order_id,
        }
        await self._request("POST", endpoint, data=payload, signed=True)

    async def get_open_orders_legacy(self, symbol: str) -> List[OrderResult]:
        params = {"symbol": self._to_bitget_symbol(symbol)}
        data = await self._request(
            "GET",
            "/api/v2/spot/trade/unfilled-orders",
            params=params,
            signed=True,
        )

        if not data or (isinstance(data, list) and len(data) == 0):
            return []

        results: List[OrderResult] = []
        for item in data:
            try:
                ord_id = item.get("orderId", "")
                side_str = item.get("side", "buy").lower()
                side: Literal["buy", "sell"] = "buy" if side_str == "buy" else "sell"

                qty = float(item.get("size", 0))
                price = float(item.get("priceAvg", item.get("price", 0)))
                status = item.get("status", "open")

                status_map = {
                    "new": "new",
                    "partially_filled": "partial",
                    "filled": "filled",
                    "cancelled": "canceled",
                }
                internal_status = status_map.get(status, "open")

                ts_ms = int(item.get("cTime", 0))
                create_time = datetime.fromtimestamp(ts_ms / 1000.0) if ts_ms > 0 else None

                results.append(
                    OrderResult(
                        order_id=ord_id,
                        symbol=symbol,
                        side=side,
                        quantity=qty,
                        price=price,
                        status=internal_status,
                        broker=self.name,
                        create_time=create_time,
                    )
                )
            except Exception as e:
                logger.warning(f"BitgetBroker.get_open_orders: skip bad item {item}: {e}")
                continue

        return results


    async def cancel_plan_order(self, *, order_id: str | None = None, client_oid: str | None = None) -> None:
        endpoint = "/api/spot/v1/plan/cancelPlan"
        payload = {}
        if order_id:
            payload["orderId"] = order_id
        if client_oid:
            payload["clientOid"] = client_oid
        if not payload:
            raise ValueError("cancel_plan_order: order_id or client_oid required")

        await self._request("POST", endpoint, data=payload, signed=True)

    async def cancel_order_legacy(self, order_id: str, symbol: str | None = None) -> None:
        """
        Отмена ордера на Bitget (требует symbol). Если symbol не передан — бросаем ValueError.
        """
        if not symbol:
            raise ValueError("BitgetBroker.cancel_order: symbol is required")

        endpoint = "/api/v2/spot/trade/cancel-order"
        payload = {
            "symbol": self._to_bitget_symbol(symbol),
            "orderId": order_id,
        }
        
        try:
            await self._request("POST", endpoint, data=payload, signed=True)
            logger.info(f"Order {order_id} for {symbol} cancelled successfully")
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id} for {symbol}: {e}")
            raise

    async def get_open_orders(self, symbol: str) -> List[OrderResult]:
        params = {"symbol": self._to_bitget_symbol(symbol)}
        data = await self._request(
            "GET",
            "/api/v2/spot/trade/unfilled-orders",
            params=params,
            signed=True,
        )

        if not data or (isinstance(data, list) and len(data) == 0):
            return []

        results: List[OrderResult] = []
        for item in data:
            try:
                ord_id = item.get("orderId", "")
                side_str = item.get("side", "buy").lower()
                side: Literal["buy", "sell"] = "buy" if side_str == "buy" else "sell"

                qty = float(item.get("size", 0))
                price = float(item.get("priceAvg", item.get("price", 0)))
                status = item.get("status", "open")

                # Приводим статус Bitget к внутреннему представлению
                status_map = {
                    "new": "new",
                    "partially_filled": "partial",
                    "filled": "filled",
                    "cancelled": "canceled",
                }
                internal_status = status_map.get(status, "open")

                # Получаем timestamp создания ордера
                ts_ms = int(item.get("cTime", 0))
                create_time = datetime.fromtimestamp(ts_ms / 1000.0) if ts_ms > 0 else None

                results.append(
                    OrderResult(
                        order_id=ord_id,
                        symbol=symbol,
                        side=side,
                        quantity=qty,
                        price=price,
                        status=internal_status,
                        broker=self.name,
                        create_time=create_time,
                    )
                )
            except Exception as e:
                logger.warning(f"BitgetBroker.get_open_orders: skip bad item {item}: {e}")
                continue

        return results
    
    def _q_str(self, value: float, precision: int) -> str:
        q = Decimal(10) ** (-precision)
        d = Decimal(str(value)).quantize(q, rounding=ROUND_DOWN)
        return format(d, "f")  # без scientific notation

    def normalize_qty(self, symbol: str, qty: float, price: float | None = None) -> float:
        bg_symbol = self._to_bitget_symbol(symbol)
        precision = int(self._symbol_rules.get(bg_symbol, 4) or 4)
        return float(self._q_str(float(qty), precision))

    def normalize_price(self, symbol: str, price: float) -> float:
        bg_symbol = self._to_bitget_symbol(symbol)
        precision = int(self._price_rules.get(bg_symbol, 6) or 6)
        return float(self._q_str(float(price), precision))

    def _qty_str(self, symbol: str, qty: float) -> str:
        bg_symbol = self._to_bitget_symbol(symbol)
        precision = int(self._symbol_rules.get(bg_symbol, 4) or 4)
        return self._q_str(float(qty), precision)

    def _price_str(self, symbol: str, price: float) -> str:
        bg_symbol = self._to_bitget_symbol(symbol)
        precision = int(self._price_rules.get(bg_symbol, 6) or 6)
        return self._q_str(float(price), precision)

    async def close_position(self, symbol: str, reason: str = "") -> None:
        # P0: spot close = SELL доступного количества монеты (base asset)
        # Берём позиции через list_open_positions (у тебя это по балансу монет)
        positions = await self.list_open_positions()
        pos = next((p for p in positions if p.symbol == symbol and float(p.quantity or 0.0) > 0), None)
        if pos is None:
            return

        order = OrderRequest(symbol=symbol, side="sell", quantity=float(pos.quantity), order_type="market")
        await self.place_order(order)