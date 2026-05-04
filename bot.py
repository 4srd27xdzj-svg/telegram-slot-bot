import asyncio
import json
import logging
import os
import random
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update, User
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters


SLOT_MACHINE_EMOJI = "🎰"
SLOT_MACHINE_JACKPOT_VALUE = 64
SLOT_MACHINE_TWO_SEVENS_FIRST_VALUES = {16, 32, 48}

SLOT_MACHINE_COMBINATIONS = {
    1: "three_bars",
    22: "three_grapes",
    43: "three_lemons",
    64: "jackpot",
}

COMBINATION_TITLES = {
    "jackpot": "777",
    "two_sevens": "77X",
    "three_bars": "три BAR",
    "three_grapes": "три винограда",
    "three_lemons": "три лимона",
}

DEFAULT_SMALL_GIFTS = ["сердечко", "медведь", "подарок", "роза"]


@dataclass(frozen=True)
class BotConfig:
    token: str
    db_path: Path
    allowed_chat_ids: set[int]
    owner_user_ids: set[int]
    small_gifts: list[str]


def parse_ids(value: str | None) -> set[int]:
    if not value:
        return set()

    ids = set()
    for item in value.split(","):
        item = item.strip().lstrip("=")
        if item:
            ids.add(int(item))
    return ids


def parse_list(value: str | None) -> list[str]:
    if not value:
        return []

    return [item.strip() for item in value.split(",") if item.strip()]


def read_config() -> BotConfig:
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    db_path = Path(os.getenv("DATABASE_PATH", "bot_stats.sqlite3"))
    allowed_chat_ids = parse_ids(os.getenv("ALLOWED_CHAT_IDS"))
    owner_user_ids = parse_ids(os.getenv("OWNER_USER_IDS"))
    small_gifts = parse_list(os.getenv("SMALL_GIFTS")) or DEFAULT_SMALL_GIFTS

    if not token:
        raise RuntimeError(
            "Не задан TELEGRAM_BOT_TOKEN. Создайте .env на основе .env.example."
        )

    if not allowed_chat_ids:
        logging.warning("ALLOWED_CHAT_IDS is empty. Slot tracking and /stats are disabled.")

    return BotConfig(
        token=token,
        db_path=db_path,
        allowed_chat_ids=allowed_chat_ids,
        owner_user_ids=owner_user_ids,
        small_gifts=small_gifts,
    )


class StatsDatabase:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.init_schema()

    def init_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS slot_stats (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                total_spins INTEGER NOT NULL DEFAULT 0,
                jackpots INTEGER NOT NULL DEFAULT 0,
                two_sevens INTEGER NOT NULL DEFAULT 0,
                three_bars INTEGER NOT NULL DEFAULT 0,
                three_grapes INTEGER NOT NULL DEFAULT 0,
                three_lemons INTEGER NOT NULL DEFAULT 0,
                other_spins INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, user_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            """
        )
        self.connection.commit()

    def remember_user(self, user: User) -> None:
        self.connection.execute(
            """
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                last_seen_at = CURRENT_TIMESTAMP
            """,
            (user.id, user.username, user.first_name, user.last_name),
        )
        self.connection.commit()

    def record_spin(self, chat_id: int, user_id: int, result: str) -> None:
        counters = {
            "jackpot": "jackpots",
            "two_sevens": "two_sevens",
            "three_bars": "three_bars",
            "three_grapes": "three_grapes",
            "three_lemons": "three_lemons",
            "other": "other_spins",
        }
        counter = counters[result]

        self.connection.execute(
            """
            INSERT INTO slot_stats (chat_id, user_id, total_spins)
            VALUES (?, ?, 0)
            ON CONFLICT(chat_id, user_id) DO NOTHING
            """,
            (chat_id, user_id),
        )
        self.connection.execute(
            f"""
            UPDATE slot_stats
            SET total_spins = total_spins + 1,
                {counter} = {counter} + 1
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_id, user_id),
        )
        self.connection.commit()

    def get_chat_totals(self, chat_id: int) -> sqlite3.Row:
        row = self.connection.execute(
            """
            SELECT
                COALESCE(SUM(total_spins), 0) AS total_spins,
                COALESCE(SUM(jackpots), 0) AS jackpots,
                COALESCE(SUM(two_sevens), 0) AS two_sevens,
                COALESCE(SUM(three_bars), 0) AS three_bars,
                COALESCE(SUM(three_grapes), 0) AS three_grapes,
                COALESCE(SUM(three_lemons), 0) AS three_lemons,
                COALESCE(SUM(other_spins), 0) AS other_spins
            FROM slot_stats
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("Stats query returned no totals.")
        return row

    def get_user_rows(self, chat_id: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    s.total_spins,
                    s.jackpots,
                    s.two_sevens,
                    s.three_bars,
                    s.three_grapes,
                    s.three_lemons,
                    s.other_spins,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM slot_stats s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.chat_id = ?
                ORDER BY s.total_spins DESC, s.jackpots DESC
                LIMIT 20
                """,
                (chat_id,),
            )
        )

    def reset_chat_stats(self, chat_id: int) -> None:
        self.connection.execute("DELETE FROM slot_stats WHERE chat_id = ?", (chat_id,))
        self.connection.commit()


def is_allowed_chat(config: BotConfig, chat_id: int | None) -> bool:
    return chat_id is not None and chat_id in config.allowed_chat_ids


def is_owner(config: BotConfig, user_id: int | None) -> bool:
    return user_id is not None and user_id in config.owner_user_ids


def get_display_name(row: sqlite3.Row) -> str:
    if row["username"]:
        return f"@{row['username']}"

    name_parts = [row["first_name"], row["last_name"]]
    return " ".join(part for part in name_parts if part) or "Без имени"


def get_user_display_name(user: User) -> str:
    if user.username:
        return f"@{user.username}"

    name_parts = [user.first_name, user.last_name]
    return " ".join(part for part in name_parts if part) or "игрок"


def classify_slot_value(value: int) -> str:
    if value in SLOT_MACHINE_TWO_SEVENS_FIRST_VALUES:
        return "two_sevens"

    return SLOT_MACHINE_COMBINATIONS.get(value, "other")


def telegram_api_call(token: str, method: str, params: dict[str, object]) -> dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(params).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API error: {details}") from error

    if not payload.get("ok"):
        description = payload.get("description", "unknown error")
        raise RuntimeError(f"Telegram API error: {description}")

    return payload["result"]


def fetch_owner_gifts(token: str, owner_user_ids: set[int]) -> list[dict]:
    gifts = []

    for owner_user_id in sorted(owner_user_ids):
        offset = ""
        for _ in range(10):
            result = telegram_api_call(
                token,
                "getUserGifts",
                {
                    "user_id": owner_user_id,
                    "limit": 100,
                    "offset": offset,
                },
            )

            for owned_gift in result.get("gifts", []):
                owned_gift["owner_user_id"] = owner_user_id
                gifts.append(owned_gift)

            offset = result.get("next_offset") or ""
            if not offset:
                break

    return gifts


def extract_gift_card(owned_gift: dict) -> dict[str, str] | None:
    gift = owned_gift.get("gift") or {}

    if owned_gift.get("type") == "unique":
        name = gift.get("name")
        base_name = gift.get("base_name") or name or "уникальный подарок"
        number = gift.get("number")
        title = f"{base_name} #{number}" if number else str(base_name)
        url = f"https://t.me/nft/{urllib.parse.quote(str(name), safe='')}" if name else ""
        return {"title": title, "url": url}

    sticker = gift.get("sticker") or {}
    emoji = sticker.get("emoji")
    gift_id = gift.get("id")
    title = f"обычный подарок {emoji}" if emoji else "обычный подарок"
    if gift_id:
        title = f"{title} ({gift_id})"

    return {"title": title, "url": ""}


def choose_owner_gift(owned_gifts: list[dict]) -> dict[str, str] | None:
    gift_cards = [card for gift in owned_gifts if (card := extract_gift_card(gift))]
    linked_gifts = [card for card in gift_cards if card["url"]]

    if linked_gifts:
        return random.choice(linked_gifts)

    if gift_cards:
        return random.choice(gift_cards)

    return None


async def build_jackpot_message(config: BotConfig, user: User) -> tuple[str, dict[str, str] | None]:
    player = get_user_display_name(user)

    try:
        owned_gifts = await asyncio.to_thread(
            fetch_owner_gifts,
            config.token,
            config.owner_user_ids,
        )
    except RuntimeError as error:
        logging.warning("Failed to fetch owner gifts: %s", error)
        owned_gifts = []

    gift = choose_owner_gift(owned_gifts)
    if gift:
        gift_line = f"{gift['title']}\n{gift['url']}" if gift["url"] else gift["title"]
        return (
            f"{player} выбил 777!\n\n"
            f"Гифт owner: {gift_line}\n\n"
            "Поздравляем, это jackpot."
        ), gift

    return (
        f"{player} выбил 777!\n\n"
        "Jackpot есть, но бот не нашел видимых подарков на аккаунте owner.\n"
        "Проверьте OWNER_USER_IDS и видимость подарков в профиле."
    ), None


def build_two_sevens_message(user: User) -> str:
    player = get_user_display_name(user)
    return (
        f"{player} выбил 77X.\n\n"
        "Первые две семерки на месте.\n"
        "Срочно додэп."
    )


def build_three_of_kind_message(config: BotConfig, user: User, result: str) -> tuple[str, str]:
    player = get_user_display_name(user)
    combination = COMBINATION_TITLES[result]
    gift = random.choice(config.small_gifts)

    return (
        f"{player} выбил {combination}.\n\n"
        f"Вы выиграли: {gift}.\n"
        "Не совсем jackpot, но уже красиво."
    ), gift


def get_chat_label(update: Update) -> str:
    chat = update.effective_chat
    if not chat:
        return "неизвестный чат"

    return chat.title or chat.username or str(chat.id)


async def notify_owners(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    message: str,
) -> None:
    for owner_user_id in sorted(config.owner_user_ids):
        try:
            await context.bot.send_message(chat_id=owner_user_id, text=message)
        except TelegramError as error:
            logging.warning("Failed to notify owner %s: %s", owner_user_id, error)


async def notify_owners_about_jackpot(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    gift: dict[str, str] | None,
) -> None:
    if not update.effective_user:
        return

    player = get_user_display_name(update.effective_user)
    chat_label = get_chat_label(update)
    gift_text = (
        f"{gift['title']}\n{gift['url']}" if gift and gift["url"]
        else gift["title"] if gift
        else "подарок не найден"
    )

    await notify_owners(
        update,
        context,
        config,
        (
            "Jackpot 777\n\n"
            f"Игрок: {player}\n"
            f"Чат: {chat_label}\n"
            f"Выигранный gift: {gift_text}"
        ),
    )


async def notify_owners_about_small_gift(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    result: str,
    gift: str,
) -> None:
    if not update.effective_user:
        return

    player = get_user_display_name(update.effective_user)
    chat_label = get_chat_label(update)
    combination = COMBINATION_TITLES[result]

    await notify_owners(
        update,
        context,
        config,
        (
            "Выигрыш за три в ряд\n\n"
            f"Игрок: {player}\n"
            f"Чат: {chat_label}\n"
            f"Комбинация: {combination}\n"
            f"Приз: {gift}"
        ),
    )


async def show_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if config.owner_user_ids and not is_owner(config, update.effective_user.id):
        return

    await update.message.reply_text(
        f"ID этого чата: {update.effective_chat.id}\n"
        f"Твой user ID: {update.effective_user.id}"
    )


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    totals = db.get_chat_totals(update.effective_chat.id)
    user_rows = db.get_user_rows(update.effective_chat.id)

    lines = [
        "Статистика слотов:",
        f"Всего спинов: {totals['total_spins']}",
        f"777: {totals['jackpots']}",
        f"77X: {totals['two_sevens']}",
        f"Три BAR: {totals['three_bars']}",
        f"Три винограда: {totals['three_grapes']}",
        f"Три лимона: {totals['three_lemons']}",
        f"Другие спины: {totals['other_spins']}",
    ]

    if user_rows:
        lines.append("")
        lines.append("По пользователям:")
        for row in user_rows:
            lines.append(
                f"{get_display_name(row)}: "
                f"спины {row['total_spins']}, "
                f"777 {row['jackpots']}, "
                f"77X {row['two_sevens']}, "
                f"BAR {row['three_bars']}, "
                f"виноград {row['three_grapes']}, "
                f"лимоны {row['three_lemons']}"
            )

    await update.message.reply_text("\n".join(lines))


async def reset_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    if not is_owner(config, update.effective_user.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.reset_chat_stats(update.effective_chat.id)

    await update.message.reply_text("Статистика этого чата обнулена.")


async def react_to_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    if not update.message or not update.message.dice:
        return

    dice = update.message.dice
    if dice.emoji != SLOT_MACHINE_EMOJI:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.remember_user(update.effective_user)

    result = classify_slot_value(dice.value)
    db.record_spin(update.effective_chat.id, update.effective_user.id, result)

    if result == "jackpot":
        message, gift = await build_jackpot_message(config, update.effective_user)
        await update.message.reply_text(message)
        await notify_owners_about_jackpot(update, context, config, gift)
    elif result == "two_sevens":
        await update.message.reply_text(build_two_sevens_message(update.effective_user))
    elif result in {"three_bars", "three_grapes", "three_lemons"}:
        message, gift = build_three_of_kind_message(config, update.effective_user, result)
        await update.message.reply_text(message)
        await notify_owners_about_small_gift(update, context, config, result, gift)


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        level=logging.INFO,
    )

    config = read_config()
    db = StatsDatabase(config.db_path)
    application = Application.builder().token(config.token).build()
    application.bot_data["config"] = config
    application.bot_data["db"] = db

    application.add_handler(CommandHandler("chatid", show_chat_id))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("resetstats", reset_stats))
    application.add_handler(MessageHandler(filters.ALL, react_to_message))

    logging.info("Bot started. Waiting for messages...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
