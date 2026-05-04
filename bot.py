import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters


SLOT_MACHINE_EMOJI = "🎰"
SLOT_MACHINE_JACKPOT_VALUE = 64
SLOT_MACHINE_THREE_OF_KIND_VALUES = {1, 22, 43}
SLOT_MACHINE_TWO_SEVENS_FIRST_VALUES = {16, 32, 48}


@dataclass(frozen=True)
class BotConfig:
    token: str
    jackpot_reply_text: str
    three_of_kind_reply_text: str
    two_sevens_reply_text: str


def read_config() -> BotConfig:
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    jackpot_reply_text = os.getenv("JACKPOT_REPLY_TEXT", "777! Выпал jackpot.")
    three_of_kind_reply_text = os.getenv("THREE_OF_KIND_REPLY_TEXT", "не совсем то")
    two_sevens_reply_text = os.getenv("TWO_SEVENS_REPLY_TEXT", "срочно додэп")

    if not token:
        raise RuntimeError(
            "Не задан TELEGRAM_BOT_TOKEN. Создайте .env на основе .env.example."
        )

    return BotConfig(
        token=token,
        jackpot_reply_text=jackpot_reply_text,
        three_of_kind_reply_text=three_of_kind_reply_text,
        two_sevens_reply_text=two_sevens_reply_text,
    )


async def react_to_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.dice:
        return

    config: BotConfig = context.application.bot_data["config"]
    dice = update.message.dice

    if dice.emoji != SLOT_MACHINE_EMOJI:
        return

    if dice.value == SLOT_MACHINE_JACKPOT_VALUE:
        await update.message.reply_text(config.jackpot_reply_text)
    elif dice.value in SLOT_MACHINE_TWO_SEVENS_FIRST_VALUES:
        await update.message.reply_text(config.two_sevens_reply_text)
    elif dice.value in SLOT_MACHINE_THREE_OF_KIND_VALUES:
        await update.message.reply_text(config.three_of_kind_reply_text)


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        level=logging.INFO,
    )

    config = read_config()
    application = Application.builder().token(config.token).build()
    application.bot_data["config"] = config

    application.add_handler(MessageHandler(filters.ALL, react_to_message))

    logging.info("Bot started. Waiting for messages...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
