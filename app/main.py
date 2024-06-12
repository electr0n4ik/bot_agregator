import os
import asyncio
import dotenv
import logging

from aiogram.client.bot import Bot
from aiogram.types.message import Message
from aiogram.dispatcher.dispatcher import Dispatcher

from service import get_aggregate_data

dotenv.load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "database")
MONGODB_COLL = os.getenv("MONGODB_COLL", "collection")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()


def chunks(lst, n):
    """
    Дробление текста на куски
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

async def send_result(chat_id, result):
    """
    Отправка агрегированных данных в сообщениях
    с учетом длины сообщения Telegram
    """
    for chunk in chunks(result, 4096):
        await bot.send_message(chat_id, chunk)

@dp.message()
async def send_message(message):
    """
    Обработка входящих сообщений
    """
    result = await get_aggregate_data(message.text,
                                      MONGODB_URL, 
                                      MONGODB_DB, 
                                      MONGODB_COLL)
    await send_result(message.chat.id, str(result))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dp.run_polling(bot)
