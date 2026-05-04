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
from telegram import MessageEntity, Update, User
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

DEFAULT_SMALL_GIFTS = ["роза", "мишка", "сердце", "подарок"]

TEMPLATE_KEY_ALIASES = {
    "777": "jackpot",
    "jackpot": "jackpot",
    "джекпот": "jackpot",
    "77x": "two_sevens",
    "77х": "two_sevens",
    "two_sevens": "two_sevens",
    "додэп": "two_sevens",
    "triple": "three_of_kind",
    "three": "three_of_kind",
    "3": "three_of_kind",
    "ряд": "three_of_kind",
    "три": "three_of_kind",
    "three_of_kind": "three_of_kind",
}

TEMPLATE_LABELS = {
    "jackpot": "777",
    "two_sevens": "77X",
    "three_of_kind": "три в ряд",
}


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

            CREATE TABLE IF NOT EXISTS message_templates (
                template_key TEXT PRIMARY KEY,
                text TEXT NOT NULL,
                entities_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

    def set_message_template(
        self,
        template_key: str,
        text: str,
        entities_data: list[dict],
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO message_templates (template_key, text, entities_json)
            VALUES (?, ?, ?)
            ON CONFLICT(template_key) DO UPDATE SET
                text = excluded.text,
                entities_json = excluded.entities_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (template_key, text, json.dumps(entities_data, ensure_ascii=False)),
        )
        self.connection.commit()

    def get_message_template(self, template_key: str) -> tuple[str, list[dict]] | None:
        row = self.connection.execute(
            """
            SELECT text, entities_json
            FROM message_templates
            WHERE template_key = ?
            """,
            (template_key,),
        ).fetchone()
        if not row:
            return None

        return row["text"], json.loads(row["entities_json"])

    def get_message_templates(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT template_key, text, updated_at
                FROM message_templates
                ORDER BY template_key
                """
            )
        )


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


def normalize_template_key(value: str) -> str | None:
    return TEMPLATE_KEY_ALIASES.get(value.strip().lower())


def get_template_key_for_result(result: str) -> str | None:
    if result == "jackpot":
        return "jackpot"

    if result == "two_sevens":
        return "two_sevens"

    if result in {"three_bars", "three_grapes", "three_lemons"}:
        return "three_of_kind"

    return None


def utf16_len(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def char_index_to_utf16_offset(value: str, index: int) -> int:
    return utf16_len(value[:index])


def serialize_entities(
    entities: tuple[MessageEntity, ...] | list[MessageEntity] | None,
    start_utf16: int = 0,
) -> list[dict]:
    if not entities:
        return []

    result = []
    for entity in entities:
        entity_end = entity.offset + entity.length
        if entity_end <= start_utf16:
            continue

        entity_data = entity.to_dict()
        entity_data["offset"] = entity.offset - start_utf16
        if entity_data["offset"] >= 0:
            result.append(entity_data)
    return result


def deserialize_entities(entities_data: list[dict]) -> list[MessageEntity]:
    return [MessageEntity.de_json(entity, None) for entity in entities_data]


def find_placeholder_replacements(text: str, values: dict[str, str]) -> list[dict[str, int | str]]:
    replacements = []
    for token, replacement in values.items():
        start = 0
        while True:
            index = text.find(token, start)
            if index == -1:
                break
            replacements.append(
                {
                    "start_char": index,
                    "end_char": index + len(token),
                    "start_utf16": char_index_to_utf16_offset(text, index),
                    "end_utf16": char_index_to_utf16_offset(text, index + len(token)),
                    "token": token,
                    "replacement": replacement,
                }
            )
            start = index + len(token)

    replacements.sort(key=lambda item: int(item["start_char"]))
    filtered = []
    last_end = -1
    for item in replacements:
        if int(item["start_char"]) >= last_end:
            filtered.append(item)
            last_end = int(item["end_char"])
    return filtered


def apply_template_values(
    text: str,
    entities_data: list[dict],
    values: dict[str, str],
) -> tuple[str, list[dict]]:
    replacements = find_placeholder_replacements(text, values)
    if not replacements:
        return text, entities_data

    pieces = []
    cursor = 0
    for replacement in replacements:
        start_char = int(replacement["start_char"])
        end_char = int(replacement["end_char"])
        pieces.append(text[cursor:start_char])
        pieces.append(str(replacement["replacement"]))
        cursor = end_char
    pieces.append(text[cursor:])
    rendered_text = "".join(pieces)

    adjusted_entities = []
    for entity in entities_data:
        entity_start = int(entity["offset"])
        entity_end = entity_start + int(entity["length"])

        overlaps_placeholder = any(
            int(replacement["start_utf16"]) < entity_end
            and int(replacement["end_utf16"]) > entity_start
            for replacement in replacements
        )
        if overlaps_placeholder:
            continue

        offset_delta = 0
        for replacement in replacements:
            replacement_end = int(replacement["end_utf16"])
            if replacement_end <= entity_start:
                old_len = replacement_end - int(replacement["start_utf16"])
                new_len = utf16_len(str(replacement["replacement"]))
                offset_delta += new_len - old_len

        adjusted_entity = dict(entity)
        adjusted_entity["offset"] = entity_start + offset_delta
        adjusted_entities.append(adjusted_entity)

    return rendered_text, adjusted_entities


async def send_text_with_entities(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    entities_data: list[dict] | None = None,
) -> None:
    entities = deserialize_entities(entities_data or [])
    await context.bot.send_message(chat_id=chat_id, text=text, entities=entities or None)


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


async def choose_owner_gift_from_api(config: BotConfig) -> dict[str, str] | None:
    try:
        owned_gifts = await asyncio.to_thread(
            fetch_owner_gifts,
            config.token,
            config.owner_user_ids,
        )
    except RuntimeError as error:
        logging.warning("Failed to fetch owner gifts: %s", error)
        owned_gifts = []

    return choose_owner_gift(owned_gifts)


async def render_saved_template(
    config: BotConfig,
    db: StatsDatabase,
    template_key: str,
    user: User,
    result: str,
    owner_gift: dict[str, str] | None = None,
    small_gift: str | None = None,
) -> tuple[str, list[dict], dict[str, str] | None, str | None] | None:
    template = db.get_message_template(template_key)
    if not template:
        return None

    text, entities_data = template
    if owner_gift is None and ("nft_url" in text or "gift_title" in text):
        owner_gift = await choose_owner_gift_from_api(config)

    values = {
        "username": get_user_display_name(user),
        "nft_url": owner_gift["url"] if owner_gift and owner_gift["url"] else "",
        "gift_title": owner_gift["title"] if owner_gift else "",
        "giftr": small_gift or "",
        "combination": COMBINATION_TITLES.get(result, result),
    }
    rendered_text, rendered_entities = apply_template_values(text, entities_data, values)
    return rendered_text, rendered_entities, owner_gift, small_gift


async def build_jackpot_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
) -> tuple[str, list[dict], dict[str, str] | None]:
    player = get_user_display_name(user)
    gift = await choose_owner_gift_from_api(config)

    rendered = await render_saved_template(
        config,
        db,
        "jackpot",
        user,
        "jackpot",
        owner_gift=gift,
    )
    if rendered:
        text, entities_data, gift, _ = rendered
        return text, entities_data, gift

    if gift:
        gift_line = f"{gift['title']}\n{gift['url']}" if gift["url"] else gift["title"]
        return (
            f"{player} выбил 777!\n\n"
            f"Гифт owner: {gift_line}\n\n"
            "Поздравляем, это jackpot."
        ), [], gift

    return (
        f"{player} выбил 777!\n\n"
        "Jackpot есть, но бот не нашел видимых подарков на аккаунте owner.\n"
        "Проверьте OWNER_USER_IDS и видимость подарков в профиле."
    ), [], None


async def build_two_sevens_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(config, db, "two_sevens", user, "two_sevens")
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    player = get_user_display_name(user)
    return (
        f"{player} выбил 77X.\n\n"
        "Первые две семерки на месте.\n"
        "Срочно додэп."
    ), []


async def build_three_of_kind_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    result: str,
) -> tuple[str, list[dict], str]:
    player = get_user_display_name(user)
    combination = COMBINATION_TITLES[result]
    gift = random.choice(config.small_gifts)

    rendered = await render_saved_template(
        config,
        db,
        "three_of_kind",
        user,
        result,
        small_gift=gift,
    )
    if rendered:
        text, entities_data, _, gift = rendered
        return text, entities_data, gift or ""

    return (
        f"{player} выбил {combination}.\n\n"
        f"Вы выиграли: {gift}.\n"
        "Не совсем jackpot, но уже красиво."
    ), [], gift


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


async def show_custom_emoji_ids(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if config.owner_user_ids and not is_owner(config, update.effective_user.id):
        return

    target_message = update.message.reply_to_message
    if not target_message:
        await update.message.reply_text(
            "Ответьте командой /emojiid на сообщение с Telegram custom emoji."
        )
        return

    entities = list(target_message.entities or [])
    entities.extend(target_message.caption_entities or [])
    custom_entities = [
        entity for entity in entities
        if entity.type == "custom_emoji" and entity.custom_emoji_id
    ]

    if not custom_entities:
        await update.message.reply_text("В этом сообщении нет Telegram custom emoji.")
        return

    lines = ["Custom emoji ID:"]
    for index, entity in enumerate(custom_entities, start=1):
        lines.append(f"{index}. {entity.custom_emoji_id}")

    await update.message.reply_text("\n".join(lines))


def extract_template_from_update(update: Update) -> tuple[str, str, list[dict]] | None:
    if not update.message or not update.message.text:
        return None

    parts = update.message.text.split(maxsplit=2)
    if len(parts) < 2:
        return None

    template_key = normalize_template_key(parts[1])
    if not template_key:
        return None

    if len(parts) >= 3:
        template_text = parts[2]
        start_char = update.message.text.find(template_text)
        start_utf16 = char_index_to_utf16_offset(update.message.text, start_char)
        entities_data = serialize_entities(update.message.entities, start_utf16=start_utf16)
        return template_key, template_text, entities_data

    target_message = update.message.reply_to_message
    if not target_message:
        return None

    if target_message.text:
        return (
            template_key,
            target_message.text,
            serialize_entities(target_message.entities),
        )

    if target_message.caption:
        return (
            template_key,
            target_message.caption,
            serialize_entities(target_message.caption_entities),
        )

    return None


async def set_message_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not is_owner(config, update.effective_user.id):
        return

    extracted = extract_template_from_update(update)
    if not extracted:
        await update.message.reply_text(
            "Формат:\n"
            "/settext 777 текст\n"
            "/settext 77x текст\n"
            "/settext triple текст\n\n"
            "Для длинного текста: отправьте сообщение-шаблон и ответьте на него /settext 777."
        )
        return

    template_key, template_text, entities_data = extracted
    db: StatsDatabase = context.application.bot_data["db"]
    db.set_message_template(template_key, template_text, entities_data)

    await update.message.reply_text(
        f"Шаблон для {TEMPLATE_LABELS[template_key]} сохранен.\n\n"
        "Доступные placeholders:\n"
        "username - имя победителя\n"
        "nft_url - ссылка на случайный gift owner\n"
        "giftr - роза/мишка/сердце/подарок\n"
        "combination - выпавшая комбинация"
    )


async def show_message_templates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not is_owner(config, update.effective_user.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    templates = db.get_message_templates()
    if not templates:
        await update.message.reply_text("Сохраненных шаблонов пока нет.")
        return

    lines = ["Сохраненные шаблоны:"]
    for row in templates:
        label = TEMPLATE_LABELS.get(row["template_key"], row["template_key"])
        preview = row["text"].replace("\n", " ")
        if len(preview) > 120:
            preview = f"{preview[:117]}..."
        lines.append(f"{label}: {preview}")

    await update.message.reply_text("\n".join(lines))


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
        message, entities_data, gift = await build_jackpot_message(
            config,
            db,
            update.effective_user,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
        await notify_owners_about_jackpot(update, context, config, gift)
    elif result == "two_sevens":
        message, entities_data = await build_two_sevens_message(
            config,
            db,
            update.effective_user,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
    elif result in {"three_bars", "three_grapes", "three_lemons"}:
        message, entities_data, gift = await build_three_of_kind_message(
            config,
            db,
            update.effective_user,
            result,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
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
    application.add_handler(CommandHandler("emojiid", show_custom_emoji_ids))
    application.add_handler(CommandHandler("settext", set_message_template))
    application.add_handler(CommandHandler("texts", show_message_templates))
    application.add_handler(MessageHandler(filters.ALL, react_to_message))

    logging.info("Bot started. Waiting for messages...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
