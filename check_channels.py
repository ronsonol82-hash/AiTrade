import asyncio
import os
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from datetime import datetime, timezone

# Пытаемся импортировать настройки
try:
    from config import Config
    # Получаем список каналов (крипта + акции)
    CHANNELS = Config.TG_CRYPTO_CHANNELS + Config.TG_STOCK_CHANNELS
except ImportError:
    print("❌ Config не найден. Использую тестовый список.")
    CHANNELS = ['unusual_whales', 'tree_of_alpha', 'whale_alert']

# Учетные данные (проверь .env или вставь свои для теста)
API_ID = os.getenv('TELEGRAM_API_ID') 
API_HASH = os.getenv('TELEGRAM_API_HASH')

if not API_ID:
    # Fallback если не в env, попробуем хардкод (не оставляй так в проде!)
    # Вставь свои значения если .env не подтягивается
    API_ID = 123456 
    API_HASH = 'your_hash_here'

async def check_channel_health(client, channel_name):
    """Проверяет доступность и свежесть канала."""
    try:
        entity = await client.get_entity(channel_name)
        
        # Получаем последний пост
        history = await client(GetHistoryRequest(
            peer=entity,
            limit=1,
            offset_date=None,
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0
        ))
        
        if not history.messages:
            return f"⚠️ {channel_name}: Пустой (нет сообщений)."

        last_msg = history.messages[0]
        last_date = last_msg.date
        
        # Считаем сколько прошло времени
        now = datetime.now(timezone.utc)
        diff = now - last_date
        
        status = "✅ Живой"
        if diff.days > 7:
            status = "💀 Мертвый ( > 7 дней)"
        elif diff.days > 1:
            status = "💤 Спит ( > 1 дня)"
            
        return f"{status} | {channel_name:<20} | Последний пост: {last_date.strftime('%Y-%m-%d %H:%M')} (UTC)"

    except ValueError:
        return f"❌ {channel_name}: Не найден (неверный юзернейм?)"
    except Exception as e:
        return f"❌ {channel_name}: Ошибка доступа ({str(e)})"

async def main():
    if not API_ID:
        print("ОШИБКА: Не заданы API_ID и API_HASH")
        return

    print(f"🔍 Начинаю проверку {len(CHANNELS)} каналов...\n")
    
    async with TelegramClient('anon_checker', API_ID, API_HASH) as client:
        for channel in CHANNELS:
            report = await check_channel_health(client, channel)
            print(report)

if __name__ == '__main__':
    # Загружаем .env если нужно
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except:
        pass
        
    asyncio.run(main())