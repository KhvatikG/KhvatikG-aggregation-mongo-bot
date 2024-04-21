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
    """
    Приветствие пользователя после команды /start.

    :param message: Объект сообщения от пользователя.
    :return: None
    """
    await message.answer(
        f"Hello, <a href='tg://user?id={message.from_user.id}'> {message.from_user.full_name} </a>!"
    )


@dp.message()
async def aggregate_sum_from_date_handler(message: Message) -> None:
    """
     Обрабатывает сообщения от пользователей бота, ожидая получить данные в JSON формате,
    валидирует их, делает запрос к базе данных и отправляет обработанный ответ обратно пользователю.

    :param message: объект сообщения от пользователя, содержащий данные.
    """
    try:
        # Извлечение данных из обьекта message и их валидация с помощью Pydantic.
        data = ValidIncomingMessage.model_validate_json(message.text)

        # Передаём полученные данные в агрегирующую функцию.
        answer = await aggregate_sum_from_date(
            db_=db,
            dt_from=data.dt_from,
            dt_upto=data.dt_upto,
            group_type=data.group_type)

        # Подготавливаем ответ.
        answer = json.dumps(answer)

        # Отправляем ответ пользователю.
        await bot.send_message(chat_id=message.from_user.id, text=answer)
    except json.JSONDecodeError:
        # Если данные пришли не в json, отправляем сообщение об ошибке.
        await message.reply("Ошибка в формате данных. Отправьте данные в формате JSON.")
    except ValidationError as e:
        logging.warning(f"Ошибка валидации входящих данных {e}")
        # Если данные невалидны, отправляем сообщение с описанием ошибки.
        await message.reply(f"Ошибка в данных: {e}")


async def main() -> None:
    await dp.start_polling(bot)  # Запускаем бота


if __name__ == '__main__':
    setup_logger()  # Устанавливаем loguru основным логером
    asyncio.run(main())
