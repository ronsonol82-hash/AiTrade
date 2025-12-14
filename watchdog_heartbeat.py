from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _utcnow_ts() -> float:
    return time.time()


def _read_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _write_json(path: str, obj: dict) -> None:
    try:
        d = os.path.dirname(path) or "."
        os.makedirs(d, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = Request(url, data=data, method="POST")
    with urlopen(req, timeout=10) as resp:
        _ = resp.read()


def main() -> int:
    ap = argparse.ArgumentParser(description="Watchdog for runner heartbeat file")
    ap.add_argument("--heartbeat", type=str, default="state/runner_heartbeat.json", help="Path to heartbeat json")
    ap.add_argument("--stale", type=float, default=90.0, help="Seconds without heartbeat to consider runner dead/hung")
    ap.add_argument("--interval", type=float, default=10.0, help="Check interval seconds")
    ap.add_argument("--alert_every", type=float, default=300.0, help="Min seconds between repeated alerts")
    ap.add_argument("--tag", type=str, default="AITrade", help="Prefix tag for alerts")
    args = ap.parse_args()

    # берём креды из ENV (универсально для Windows Task Scheduler)
    bot_token = os.getenv("ALERT_TG_BOT_TOKEN", "").strip()
    chat_id = os.getenv("ALERT_TG_CHAT_ID", "").strip()

    state_path = os.path.join(os.path.dirname(args.heartbeat) or ".", ".watchdog_state.json")
    wd_state = _read_json(state_path)
    last_alert_ts = float(wd_state.get("last_alert_ts", 0.0) or 0.0)
    last_was_stale = bool(wd_state.get("last_was_stale", False))

    while True:
        hb = _read_json(args.heartbeat)
        hb_ts = float(hb.get("ts", 0.0) or 0.0)
        hb_pid = hb.get("pid", "na")
        hb_status = str(hb.get("status", "na"))
        hb_note = str(hb.get("note", ""))

        now = _utcnow_ts()
        age = (now - hb_ts) if hb_ts > 0 else 1e18
        is_stale = age > float(args.stale)

        if is_stale:
            can_alert = (now - last_alert_ts) >= float(args.alert_every)
            # алертим при переходе OK->STALE, и дальше не чаще alert_every
            if (not last_was_stale) or can_alert:
                msg = (
                    f"🚨 {args.tag} WATCHDOG\n"
                    f"Runner heartbeat STALE\n"
                    f"age={age:.0f}s (stale>{float(args.stale):.0f}s)\n"
                    f"pid={hb_pid}\n"
                    f"last_status={hb_status}\n"
                    f"note={hb_note}\n"
                    f"time={datetime.utcnow().isoformat()}Z"
                )
                try:
                    _send_telegram(bot_token, chat_id, msg)
                except Exception:
                    pass
                last_alert_ts = now
                last_was_stale = True
        else:
            # можно опционально слать "recovered", но обычно это лишний шум
            last_was_stale = False

        _write_json(state_path, {"last_alert_ts": last_alert_ts, "last_was_stale": last_was_stale})
        time.sleep(float(args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
