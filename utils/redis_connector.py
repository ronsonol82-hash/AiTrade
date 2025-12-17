# utils/redis_connector.py
import redis
import json
import pickle
from config import Config

class RedisSignalBus:
    def __init__(self, host='localhost', port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=False)
        self.key = "trade_signals:latest"

    def publish_signals(self, signals_dict):
        """Pickle dump и запись в Redis с TTL 300 сек"""
        try:
            packed = pickle.dumps(signals_dict)
            self.r.set(self.key, packed, ex=300)
            print(f"[Redis] Published signals for {len(signals_dict)} keys")
        except Exception as e:
            print(f"[Redis] Publish ERROR: {e}")

    def get_signals(self):
        """Чтение и Pickle load"""
        try:
            data = self.r.get(self.key)
            if data:
                return pickle.loads(data)
        except Exception as e:
             print(f"[Redis] Read ERROR: {e}")
        return {}