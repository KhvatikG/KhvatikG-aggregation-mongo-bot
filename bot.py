import json
import os
import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from pydantic import ValidationError

from core.config import settings
from core.message_validation import ValidIncomingMessage
from core.loger_setup import setup_logger
from aggregations import aggregate_sum_from_date
from core.mongo_db import db

if not os.getenv("AMB_BOT_TOKEN"):
    raise Exception("""
    The AMB_BOT_TOKEN environment variable is not set. It is necessary to set the variable for connecting to the bot,
    or specify it in core/config.Settings.BOT_TOKEN (not recommended).
    """)

TOKEN = settings.BOT_TOKEN
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(
        f"Hello, <a href='tg://user?id={message.from_user.id}'> {message.from_user.full_name} </a>!"
    )


@dp.message()
async def aggregate_sum_from_date_handler(message: Message):
    try:
        # Валидация данных с помощью Pydantic
        data = ValidIncomingMessage.model_validate_json(message.text)

        answer = await aggregate_sum_from_date(
            db_=db,
            dt_from=data.dt_from,
            dt_upto=data.dt_upto,
            group_type=data.group_type)

        answer = json.dumps(answer)

        await bot.send_message(chat_id=message.from_user.id, text=answer)
    except json.JSONDecodeError:
        await message.reply("Ошибка в формате данных. Отправьте данные в формате JSON.")
    except ValidationError as e:
        logging.warning(f"Ошибка валидации {e}")
        # Если данные не валидны, отправляем сообщение с описанием ошибки
        await message.reply(f"Ошибка в данных: {e}")


async def main() -> None:
    await dp.start_polling(bot)


if __name__ == '__main__':
    setup_logger()
    asyncio.run(main())
