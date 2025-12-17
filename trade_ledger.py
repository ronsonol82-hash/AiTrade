# trade_ledger.py
from __future__ import annotations

import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any


@dataclass
class OrderRecord:
    client_id: str
    broker: str
    symbol: str
    role: str              # entry / exit / sl / tp / other
    side: str              # buy / sell
    status: str            # reserved / submitted / filled / canceled / failed
    order_id: Optional[str] = None
    payload: Optional[dict] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class TradeLedger:
    """
    Минимальный прод-леджер (SQLite):
    - orders: идемпотентность по client_id (PRIMARY KEY)
    - trades: контекст сделки, чтобы после рестарта знать: почему мы в позиции
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def initialize(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                client_id  TEXT PRIMARY KEY,
                broker     TEXT NOT NULL,
                symbol     TEXT NOT NULL,
                role       TEXT NOT NULL,
                side       TEXT NOT NULL,
                status     TEXT NOT NULL,
                order_id   TEXT,
                payload    TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id         TEXT PRIMARY KEY,
                strategy_id      TEXT NOT NULL,
                broker           TEXT NOT NULL,
                symbol           TEXT NOT NULL,
                side             TEXT NOT NULL,
                signal_id        TEXT NOT NULL,
                entry_client_id  TEXT NOT NULL,
                status           TEXT NOT NULL,  -- open/closed/aborted
                entry_price      REAL,
                entry_qty        REAL,
                exit_price       REAL,
                exit_reason      TEXT,
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(broker, symbol, status);")
            self._conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat()

    # ---------------- Orders / idempotency ----------------

    def reserve_order(
        self,
        client_id: str,
        *,
        broker: str,
        symbol: str,
        role: str,
        side: str,
        payload: Optional[dict] = None,
    ) -> bool:
        """
        True  -> можно отправлять (мы зарезервировали client_id)
        False -> уже было (повторять НЕЛЬЗЯ)

        Retry policy:
        - Разрешаем повтор, если прошлый статус финально-негативный:
            failed / canceled / rejected
        - При ретрае очищаем order_id и пишем метаданные ретрая в payload.
        """
        now = self._now()

        new_payload: dict = dict(payload or {})
        payload_s = json.dumps(new_payload, ensure_ascii=False)

        with self._lock:
            cur = self._conn.cursor()

            row = cur.execute(
                "SELECT status, order_id, payload FROM orders WHERE client_id = ?",
                (client_id,),
            ).fetchone()

            if row:
                status = (row["status"] or "").lower()

                # ✅ allow retry after failed/rejected/canceled
                if status in ("failed", "canceled", "cancelled", "rejected"):
                    try:
                        prev_payload = json.loads(row["payload"] or "{}")
                    except Exception:
                        prev_payload = {}

                    retry_n = int(prev_payload.get("_retry_n", 0)) + 1

                    merged = {}
                    # сохраняем историю + накладываем новые поля (новые важнее)
                    merged.update(prev_payload)
                    merged.update(new_payload)

                    merged["_retry_n"] = retry_n
                    merged["_retry_at"] = now
                    merged["_prev_status"] = status

                    payload_s = json.dumps(merged, ensure_ascii=False)

                    cur.execute(
                        "UPDATE orders SET status=?, order_id=NULL, payload=?, updated_at=? WHERE client_id=?",
                        ("reserved", payload_s, now, client_id),
                    )
                    self._conn.commit()
                    return True

                # иначе: это active/neutral статус (reserved/submitted/filled/...)
                return False

            # новый client_id -> резервируем
            cur.execute(
                "INSERT INTO orders(client_id,broker,symbol,role,side,status,order_id,payload,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (client_id, broker, symbol, role, side, "reserved", None, payload_s, now, now),
            )
            self._conn.commit()
            return True

    def mark_order_submitted(self, client_id: str, order_id: str, payload: Optional[dict] = None) -> None:
            now = self._now()
            new_payload = dict(payload or {})

            with self._lock:
                row = self._conn.execute(
                    "SELECT payload FROM orders WHERE client_id=?",
                    (client_id,),
                ).fetchone()

                if not row:
                    raise KeyError(f"mark_order_submitted: order not found for client_id={client_id}")

                # merge payload: old + new (new overrides old)
                try:
                    prev = json.loads(row["payload"] or "{}")
                except Exception:
                    prev = {}

                if not isinstance(prev, dict):
                    prev = {"_prev_payload_raw": prev}

                merged = dict(prev)
                merged.update(new_payload)
                merged["_updated_at"] = now
                merged["_event"] = "submitted"

                payload_s = json.dumps(merged, ensure_ascii=False)

                self._conn.execute(
                    "UPDATE orders SET status=?, order_id=?, payload=?, updated_at=? WHERE client_id=?",
                    ("submitted", str(order_id), payload_s, now, client_id),
                )
                self._conn.commit()

    def mark_order_final(self, client_id: str, status: str, payload: Optional[dict] = None) -> None:
            now = self._now()
            new_payload = dict(payload or {})

            with self._lock:
                row = self._conn.execute(
                    "SELECT payload FROM orders WHERE client_id=?",
                    (client_id,),
                ).fetchone()

                if not row:
                    raise KeyError(f"mark_order_final: order not found for client_id={client_id}")

                # merge payload: old + new (new overrides old)
                try:
                    prev = json.loads(row["payload"] or "{}")
                except Exception:
                    prev = {}

                if not isinstance(prev, dict):
                    prev = {"_prev_payload_raw": prev}

                merged = dict(prev)
                merged.update(new_payload)
                merged["_updated_at"] = now
                merged["_event"] = f"final:{status}"

                payload_s = json.dumps(merged, ensure_ascii=False)

                self._conn.execute(
                    "UPDATE orders SET status=?, payload=?, updated_at=? WHERE client_id=?",
                    (str(status), payload_s, now, client_id),
                )
                self._conn.commit()

    def get_trade_entry_price(self, trade_id: str) -> Optional[float]:
        """
        Возвращает entry_price для указанного trade_id.
        Используется в trailing логике для расчёта breakeven.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT entry_price FROM trades WHERE trade_id = ?",
                (trade_id,),
            ).fetchone()
            if row and row["entry_price"] is not None:
                return float(row["entry_price"])
            return None

    def get_order(self, client_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM orders WHERE client_id=?", (client_id,)).fetchone()
            return dict(row) if row else None

    def list_reserved_orders(self, broker: str) -> List[OrderRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM orders WHERE broker=? AND status='reserved' ORDER BY created_at ASC",
                (broker,),
            ).fetchall()
        out: List[OrderRecord] = []
        for r in rows:
            out.append(
                OrderRecord(
                    client_id=r["client_id"],
                    broker=r["broker"],
                    symbol=r["symbol"],
                    role=r["role"],
                    side=r["side"],
                    status=r["status"],
                    order_id=r["order_id"],
                    payload=json.loads(r["payload"] or "{}"),
                    created_at=r["created_at"],
                    updated_at=r["updated_at"],
                )
            )
        return out

    # ---------------- Trades ----------------

    def upsert_trade(
        self,
        *,
        trade_id: str,
        strategy_id: str,
        broker: str,
        symbol: str,
        side: str,
        signal_id: str,
        entry_client_id: str,
    ) -> None:
        now = self._now()
        with self._lock:
            cur = self._conn.cursor()
            row = cur.execute("SELECT trade_id FROM trades WHERE trade_id=?", (trade_id,)).fetchone()
            if row:
                cur.execute("UPDATE trades SET updated_at=? WHERE trade_id=?", (now, trade_id))
            else:
                cur.execute(
                    "INSERT INTO trades(trade_id,strategy_id,broker,symbol,side,signal_id,entry_client_id,status,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (trade_id, strategy_id, broker, symbol, side, signal_id, entry_client_id, "open", now, now),
                )
            self._conn.commit()

    def set_trade_entry(self, trade_id: str, entry_price: float, entry_qty: float) -> None:
        now = self._now()
        with self._lock:
            self._conn.execute(
                "UPDATE trades SET entry_price=?, entry_qty=?, updated_at=? WHERE trade_id=?",
                (float(entry_price), float(entry_qty), now, trade_id),
            )
            self._conn.commit()

    def close_trade(self, trade_id: str, exit_price: float, reason: str) -> None:
        now = self._now()
        with self._lock:
            self._conn.execute(
                "UPDATE trades SET status='closed', exit_price=?, exit_reason=?, updated_at=? WHERE trade_id=?",
                (float(exit_price), reason, now, trade_id),
            )
            self._conn.commit()

    def abort_trade(self, trade_id: str, reason: str) -> None:
        now = self._now()
        with self._lock:
            self._conn.execute(
                "UPDATE trades SET status='aborted', exit_reason=?, updated_at=? WHERE trade_id=?",
                (reason, now, trade_id),
            )
            self._conn.commit()

    def get_open_trade(self, broker: str, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM trades WHERE broker=? AND symbol=? AND status='open' ORDER BY created_at DESC LIMIT 1",
                (broker, symbol),
            ).fetchone()
            return dict(row) if row else None

    def list_open_trades(self, broker: str | None = None) -> List[Dict[str, Any]]:
        with self._lock:
            if broker:
                rows = self._conn.execute(
                    "SELECT * FROM trades WHERE broker=? AND status='open' ORDER BY created_at DESC",
                    (broker,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM trades WHERE status='open' ORDER BY created_at DESC"
                ).fetchall()
        return [dict(r) for r in rows]

    def has_open_trade(self, broker: str, symbol: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM trades WHERE broker=? AND symbol=? AND status='open' LIMIT 1",
                (broker, symbol),
            ).fetchone()
            return bool(row)
