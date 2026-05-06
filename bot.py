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
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity, Update, User
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


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
TEM_REWARDS_BY_RESULT = {
    "jackpot": 25,
    "two_sevens": 5,
    "three_bars": 10,
    "three_grapes": 10,
    "three_lemons": 10,
    "other": 0,
}
DEFAULT_RANK_NAMES = [
    "новичок",
    "искатель",
    "крутящий",
    "азартный",
    "охотник",
    "мастер",
    "легенда",
]

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
    "stats": "stats",
    "stat": "stats",
    "стата": "stats",
    "статистика": "stats",
    "mystats": "personal_stats",
    "my_stats": "personal_stats",
    "personal": "personal_stats",
    "personal_stats": "personal_stats",
    "личная": "personal_stats",
    "личная_статистика": "personal_stats",
    "welcome": "welcome",
    "start": "welcome",
    "приветствие": "welcome",
    "привет": "welcome",
    "milestone": "milestone",
    "mile": "milestone",
    "спины": "milestone",
    "рубеж": "milestone",
    "dailybonus": "daily_bonus",
    "daily_bonus": "daily_bonus",
    "bonus": "daily_bonus",
    "бонус": "daily_bonus",
    "dailyreminder": "daily_reminder",
    "daily_reminder": "daily_reminder",
    "reminder": "daily_reminder",
    "напоминание": "daily_reminder",
    "chance": "chance_hint",
    "шанс": "chance_hint",
    "help": "help",
    "помощь": "help",
}

TEMPLATE_LABELS = {
    "jackpot": "777",
    "two_sevens": "77X",
    "three_of_kind": "три в ряд",
    "stats": "общая статистика",
    "personal_stats": "личная статистика",
    "welcome": "приветствие",
    "milestone": "рубеж спинов",
    "daily_bonus": "ежедневный бонус",
    "daily_reminder": "напоминание daily bonus",
    "chance_hint": "подсказка шанса",
    "help": "help",
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

            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                username TEXT,
                chat_type TEXT,
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

            CREATE TABLE IF NOT EXISTS user_message_templates (
                user_id INTEGER NOT NULL,
                template_key TEXT NOT NULL,
                text TEXT NOT NULL,
                entities_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, template_key)
            );

            CREATE TABLE IF NOT EXISTS bot_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS user_wallets (
                user_id INTEGER PRIMARY KEY,
                tem_balance INTEGER NOT NULL DEFAULT 0,
                last_daily_bonus_date TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS private_subscribers (
                user_id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                last_daily_reminder_date TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.connection.commit()

    def remember_chat(self, chat) -> None:
        self.connection.execute(
            """
            INSERT INTO chats (chat_id, title, username, chat_type)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                username = excluded.username,
                chat_type = excluded.chat_type,
                last_seen_at = CURRENT_TIMESTAMP
            """,
            (chat.id, chat.title, chat.username, chat.type),
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

    def remember_private_subscriber(self, user: User, chat_id: int) -> None:
        self.remember_user(user)
        self.connection.execute(
            """
            INSERT INTO private_subscribers (user_id, chat_id)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                chat_id = excluded.chat_id,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user.id, chat_id),
        )
        self.connection.commit()

    def get_due_daily_reminder_subscribers(self, today: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    p.user_id,
                    p.chat_id,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM private_subscribers p
                JOIN users u ON u.user_id = p.user_id
                WHERE p.last_daily_reminder_date IS NULL
                   OR p.last_daily_reminder_date != ?
                ORDER BY p.user_id
                """,
                (today,),
            )
        )

    def mark_daily_reminder_sent(self, user_id: int, today: str) -> None:
        self.connection.execute(
            """
            UPDATE private_subscribers
            SET last_daily_reminder_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """,
            (today, user_id),
        )
        self.connection.commit()

    def ensure_wallet(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO user_wallets (user_id, tem_balance)
            VALUES (?, 0)
            ON CONFLICT(user_id) DO NOTHING
            """,
            (user_id,),
        )
        self.connection.commit()

    def get_tem_balance(self, user_id: int) -> int:
        self.ensure_wallet(user_id)
        row = self.connection.execute(
            """
            SELECT tem_balance
            FROM user_wallets
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        return int(row["tem_balance"]) if row else 0

    def add_tem(self, user_id: int, amount: int) -> int:
        self.ensure_wallet(user_id)
        self.connection.execute(
            """
            UPDATE user_wallets
            SET tem_balance = tem_balance + ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """,
            (amount, user_id),
        )
        self.connection.commit()
        return self.get_tem_balance(user_id)

    def claim_daily_bonus(self, user_id: int, bonus_amount: int, today: str) -> tuple[bool, int]:
        self.ensure_wallet(user_id)
        row = self.connection.execute(
            """
            SELECT last_daily_bonus_date
            FROM user_wallets
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        if row and row["last_daily_bonus_date"] == today:
            self.connection.commit()
            return False, self.get_tem_balance(user_id)

        self.connection.execute(
            """
            UPDATE user_wallets
            SET tem_balance = tem_balance + ?,
                last_daily_bonus_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """,
            (bonus_amount, today, user_id),
        )
        self.connection.commit()
        return True, self.get_tem_balance(user_id)

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

    def get_known_chats(self, allowed_chat_ids: set[int]) -> list[sqlite3.Row]:
        if not allowed_chat_ids:
            return []

        placeholders = ",".join("?" for _ in allowed_chat_ids)
        return list(
            self.connection.execute(
                f"""
                SELECT chat_id, title, username, chat_type
                FROM chats
                WHERE chat_id IN ({placeholders})
                ORDER BY COALESCE(title, username, CAST(chat_id AS TEXT))
                """,
                tuple(sorted(allowed_chat_ids)),
            )
        )

    def get_user_rows(self, chat_id: int, limit: int = 20) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    s.user_id,
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
                LIMIT ?
                """,
                (chat_id, limit),
            )
        )

    def get_luckiest_by_jackpots(self, chat_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT
                s.user_id,
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
            WHERE s.chat_id = ? AND s.jackpots > 0
            ORDER BY s.jackpots DESC, s.total_spins ASC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()

    def get_luckiest_by_ratio(self, chat_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT
                s.user_id,
                s.total_spins,
                s.jackpots,
                s.two_sevens,
                s.three_bars,
                s.three_grapes,
                s.three_lemons,
                s.other_spins,
                u.username,
                u.first_name,
                u.last_name,
                (s.jackpots * 1.0 / s.total_spins) AS jackpot_ratio
            FROM slot_stats s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.chat_id = ? AND s.total_spins > 0 AND s.jackpots > 0
            ORDER BY jackpot_ratio DESC, s.jackpots DESC, s.total_spins DESC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()

    def get_user_stats(self, chat_id: int, user_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT
                s.user_id,
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
            WHERE s.chat_id = ? AND s.user_id = ?
            """,
            (chat_id, user_id),
        ).fetchone()

    def reset_chat_stats(self, chat_id: int) -> None:
        self.connection.execute("DELETE FROM slot_stats WHERE chat_id = ?", (chat_id,))
        self.connection.commit()

    def reset_user_stats(self, chat_id: int, user_id: int) -> None:
        self.connection.execute(
            "DELETE FROM slot_stats WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
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

    def set_user_message_template(
        self,
        user_id: int,
        template_key: str,
        text: str,
        entities_data: list[dict],
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO user_message_templates (user_id, template_key, text, entities_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, template_key) DO UPDATE SET
                text = excluded.text,
                entities_json = excluded.entities_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, template_key, text, json.dumps(entities_data, ensure_ascii=False)),
        )
        self.connection.commit()

    def get_user_message_template(
        self,
        user_id: int,
        template_key: str,
    ) -> tuple[str, list[dict]] | None:
        row = self.connection.execute(
            """
            SELECT text, entities_json
            FROM user_message_templates
            WHERE user_id = ? AND template_key = ?
            """,
            (user_id, template_key),
        ).fetchone()
        if not row:
            return None

        return row["text"], json.loads(row["entities_json"])

    def get_user_message_templates(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    t.user_id,
                    t.template_key,
                    t.text,
                    t.updated_at,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM user_message_templates t
                LEFT JOIN users u ON u.user_id = t.user_id
                ORDER BY t.updated_at DESC
                """
            )
        )

    def set_bot_setting(self, setting_key: str, setting_value: object) -> None:
        self.connection.execute(
            """
            INSERT INTO bot_settings (setting_key, setting_value)
            VALUES (?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET
                setting_value = excluded.setting_value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (setting_key, json.dumps(setting_value, ensure_ascii=False)),
        )
        self.connection.commit()

    def get_bot_setting(self, setting_key: str, default: object) -> object:
        row = self.connection.execute(
            """
            SELECT setting_value
            FROM bot_settings
            WHERE setting_key = ?
            """,
            (setting_key,),
        ).fetchone()
        if not row:
            return default

        try:
            return json.loads(row["setting_value"])
        except json.JSONDecodeError:
            return default

    def set_rank_cards(self, rank_cards: list[dict]) -> None:
        self.set_bot_setting("rank_names", rank_cards)

    def set_rank_names(self, rank_names: list[str]) -> None:
        self.set_rank_cards([{"text": rank, "entities": []} for rank in rank_names])

    def get_rank_cards(self) -> list[dict]:
        row = self.connection.execute(
            """
            SELECT setting_value
            FROM bot_settings
            WHERE setting_key = 'rank_names'
            """
        ).fetchone()
        if not row:
            return [{"text": rank, "entities": []} for rank in DEFAULT_RANK_NAMES]

        try:
            rank_data = json.loads(row["setting_value"])
        except json.JSONDecodeError:
            return [{"text": rank, "entities": []} for rank in DEFAULT_RANK_NAMES]

        if not isinstance(rank_data, list):
            return [{"text": rank, "entities": []} for rank in DEFAULT_RANK_NAMES]

        cleaned = []
        for item in rank_data:
            if isinstance(item, dict):
                text = str(item.get("text", "")).strip()
                entities = item.get("entities", [])
                if text:
                    cleaned.append(
                        {
                            "text": text,
                            "entities": entities if isinstance(entities, list) else [],
                        }
                    )
            else:
                text = str(item).strip()
                if text:
                    cleaned.append({"text": text, "entities": []})

        return cleaned or [{"text": rank, "entities": []} for rank in DEFAULT_RANK_NAMES]

    def get_rank_names(self) -> list[str]:
        return [rank["text"] for rank in self.get_rank_cards()]


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


def get_row_display_name(row: sqlite3.Row) -> str:
    if row["username"]:
        return f"@{row['username']}"

    name_parts = [row["first_name"], row["last_name"]]
    return " ".join(part for part in name_parts if part) or "игрок"


def stats_value(stats: sqlite3.Row | dict[str, int], key: str) -> int:
    return int(stats[key] or 0)


def calculate_spin_reward(result: str) -> int:
    return 1 + TEM_REWARDS_BY_RESULT[result]


def choose_daily_bonus_amount() -> int:
    roll = random.random()
    if roll < 0.80:
        return random.randint(1, 5)
    if roll < 0.95:
        return 10
    return 25


def get_chance_multiplier(db: StatsDatabase) -> float:
    value = db.get_bot_setting("chance_multiplier", 1)
    try:
        multiplier = float(value)
    except (TypeError, ValueError):
        return 1.0

    return max(0.1, multiplier)


def get_chance_average_spins(db: StatsDatabase) -> int:
    value = db.get_bot_setting("chance_average_spins", 5)
    try:
        average_spins = int(value)
    except (TypeError, ValueError):
        return 5

    return max(1, average_spins)


def should_send_chance_hint(db: StatsDatabase) -> bool:
    return random.random() < (1 / get_chance_average_spins(db))


def format_chance_percent(value: float) -> str:
    if value >= 10:
        return f"{value:.0f}%"

    return f"{value:.1f}%"


def choose_jackpot_chance_percent(db: StatsDatabase) -> str:
    if random.random() < 0.88:
        base_chance = random.choice([1.4, 1.5, 1.6, 1.7, 1.8, 2.0, 2.2])
    else:
        base_chance = random.choice([7, 9, 12, 17, 25])

    return format_chance_percent(base_chance * get_chance_multiplier(db))


def choose_jackpot_chance_text(chance_percent: str) -> str:
    return f"Ощущение по барабанам: сейчас шанс 777 примерно {chance_percent}. Можно крутить."


def get_rank_card(total_spins: int, rank_cards: list[dict]) -> dict:
    rank_index = total_spins // 100
    if rank_index < len(rank_cards):
        return rank_cards[rank_index]

    return rank_cards[-1]


def get_rank_name(total_spins: int, rank_names: list[str]) -> str:
    rank_index = total_spins // 100
    if rank_index < len(rank_names):
        return rank_names[rank_index]

    return rank_names[-1]


def rank_values(total_spins: int, rank_cards: list[dict]) -> dict[str, str]:
    return {"rank": get_rank_card(total_spins, rank_cards)["text"]}


def rank_value_entities(total_spins: int, rank_cards: list[dict]) -> dict[str, list[dict]]:
    return {"rank": get_rank_card(total_spins, rank_cards)["entities"]}


def three_of_kind_total(stats: sqlite3.Row | dict[str, int]) -> int:
    return (
        stats_value(stats, "three_bars")
        + stats_value(stats, "three_grapes")
        + stats_value(stats, "three_lemons")
    )


def empty_user_stats(user: User) -> dict[str, int | str | None]:
    return {
        "user_id": user.id,
        "total_spins": 0,
        "jackpots": 0,
        "two_sevens": 0,
        "three_bars": 0,
        "three_grapes": 0,
        "three_lemons": 0,
        "other_spins": 0,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
    }


def format_top_spin_rows(rows: list[sqlite3.Row], rank_names: list[str]) -> str:
    if not rows:
        return "Пока нет спинов."

    lines = []
    for index, row in enumerate(rows, start=1):
        lines.append(
            f"{index}. {get_display_name(row)} - "
            f"{row['total_spins']} спинов, "
            f"ранг: {get_rank_name(row['total_spins'], rank_names)}, "
            f"777: {row['jackpots']}, "
            f"77X: {row['two_sevens']}"
        )
    return "\n".join(lines)


def format_luckiest_by_jackpots(row: sqlite3.Row | None) -> str:
    if not row:
        return "Пока никто не выбивал 777."

    return f"{get_display_name(row)} - 777: {row['jackpots']}, спины: {row['total_spins']}"


def format_luckiest_by_ratio(row: sqlite3.Row | None) -> str:
    if not row:
        return "Пока нет игроков с 777."

    ratio = float(row["jackpot_ratio"] or 0) * 100
    return (
        f"{get_display_name(row)} - {ratio:.2f}% "
        f"({row['jackpots']} из {row['total_spins']})"
    )


def is_spin_milestone(total_spins: int) -> bool:
    return total_spins > 0 and total_spins % 25 == 0


def is_chat_stats_milestone(total_spins: int) -> bool:
    return total_spins > 0 and total_spins % 100 == 0


def is_forwarded_message(message) -> bool:
    return any(
        getattr(message, attribute, None)
        for attribute in (
            "forward_origin",
            "forward_from",
            "forward_from_chat",
            "forward_sender_name",
            "forward_date",
        )
    )


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
    value_entities: dict[str, list[dict]] | None = None,
) -> tuple[str, list[dict]]:
    value_entities = value_entities or {}
    replacements = find_placeholder_replacements(text, values)
    if not replacements:
        return text, entities_data

    pieces = []
    cursor = 0
    inserted_entities = []
    rendered_utf16_cursor = 0
    for replacement in replacements:
        start_char = int(replacement["start_char"])
        end_char = int(replacement["end_char"])
        unchanged_piece = text[cursor:start_char]
        replacement_text = str(replacement["replacement"])
        pieces.append(unchanged_piece)
        rendered_utf16_cursor += utf16_len(unchanged_piece)

        for entity in value_entities.get(str(replacement["token"]), []):
            adjusted_entity = dict(entity)
            adjusted_entity["offset"] = int(entity["offset"]) + rendered_utf16_cursor
            inserted_entities.append(adjusted_entity)

        pieces.append(replacement_text)
        rendered_utf16_cursor += utf16_len(replacement_text)
        cursor = end_char
    tail_piece = text[cursor:]
    pieces.append(tail_piece)
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

    adjusted_entities.extend(inserted_entities)
    adjusted_entities.sort(key=lambda entity: int(entity["offset"]))
    return rendered_text, adjusted_entities


async def send_text_with_entities(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    entities_data: list[dict] | None = None,
) -> None:
    entities = deserialize_entities(entities_data or [])
    await context.bot.send_message(chat_id=chat_id, text=text, entities=entities or None)


async def send_application_text_with_entities(
    application: Application,
    chat_id: int,
    text: str,
    entities_data: list[dict] | None = None,
) -> None:
    entities = deserialize_entities(entities_data or [])
    await application.bot.send_message(chat_id=chat_id, text=text, entities=entities or None)


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
    stats: sqlite3.Row | dict[str, int] | None = None,
    owner_gift: dict[str, str] | None = None,
    small_gift: str | None = None,
) -> tuple[str, list[dict], dict[str, str] | None, str | None] | None:
    template = db.get_message_template(template_key)
    if not template:
        return None

    text, entities_data = template
    if owner_gift is None and ("nft_url" in text or "gift_title" in text):
        owner_gift = await choose_owner_gift_from_api(config)

    total_spins = stats_value(stats, "total_spins") if stats else 0
    rank_cards = db.get_rank_cards()
    balance = db.get_tem_balance(user.id)
    values = {
        "username": get_user_display_name(user),
        "nft_url": owner_gift["url"] if owner_gift and owner_gift["url"] else "",
        "gift_title": owner_gift["title"] if owner_gift else "",
        "giftr": small_gift or "",
        "combination": COMBINATION_TITLES.get(result, result),
        "total_spins": str(total_spins),
        "balance": str(balance),
        "tem_balance": str(balance),
        **rank_values(total_spins, rank_cards),
    }
    rendered_text, rendered_entities = apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(total_spins, rank_cards),
    )
    return rendered_text, rendered_entities, owner_gift, small_gift


async def build_jackpot_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict], dict[str, str] | None]:
    player = get_user_display_name(user)
    gift = await choose_owner_gift_from_api(config)

    rendered = await render_saved_template(
        config,
        db,
        "jackpot",
        user,
        "jackpot",
        stats=stats,
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
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "two_sevens",
        user,
        "two_sevens",
        stats=stats,
    )
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
    stats: sqlite3.Row | dict[str, int] | None = None,
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
        stats=stats,
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


def build_default_chat_stats_message(
    totals: sqlite3.Row,
    top_rows: list[sqlite3.Row],
    rank_names: list[str],
    luckiest_by_jackpots: sqlite3.Row | None,
    luckiest_by_ratio: sqlite3.Row | None,
) -> str:
    return (
        "Статистика слотов\n\n"
        "Общая статистика:\n"
        f"Всего спинов: {totals['total_spins']}\n"
        f"777: {totals['jackpots']}\n"
        f"77X: {totals['two_sevens']}\n"
        f"Три BAR: {totals['three_bars']}\n"
        f"Три винограда: {totals['three_grapes']}\n"
        f"Три лимона: {totals['three_lemons']}\n\n"
        "Топ 5 по спинам:\n"
        f"{format_top_spin_rows(top_rows, rank_names)}\n\n"
        "Самый везучий по количеству 777:\n"
        f"{format_luckiest_by_jackpots(luckiest_by_jackpots)}\n\n"
        "Самый везучий по отношению 777 к спинам:\n"
        f"{format_luckiest_by_ratio(luckiest_by_ratio)}"
    )


def build_chat_stats_message(
    db: StatsDatabase,
    chat_label: str,
    totals: sqlite3.Row,
    top_rows: list[sqlite3.Row],
    luckiest_by_jackpots: sqlite3.Row | None,
    luckiest_by_ratio: sqlite3.Row | None,
) -> tuple[str, list[dict]]:
    rank_cards = db.get_rank_cards()
    rank_names = [rank["text"] for rank in rank_cards]
    template = db.get_message_template("stats")
    if not template:
        return build_default_chat_stats_message(
            totals,
            top_rows,
            rank_names,
            luckiest_by_jackpots,
            luckiest_by_ratio,
        ), []

    text, entities_data = template
    values = {
        "chat": chat_label,
        "total_spins": str(totals["total_spins"]),
        "jackpots": str(totals["jackpots"]),
        "two_sevens": str(totals["two_sevens"]),
        "three_bars": str(totals["three_bars"]),
        "three_grapes": str(totals["three_grapes"]),
        "three_lemons": str(totals["three_lemons"]),
        "three_in_row": str(three_of_kind_total(totals)),
        "other_spins": str(totals["other_spins"]),
        "top5": format_top_spin_rows(top_rows, rank_names),
        "luckiest_777": format_luckiest_by_jackpots(luckiest_by_jackpots),
        "luckiest_ratio": format_luckiest_by_ratio(luckiest_by_ratio),
    }
    return apply_template_values(text, entities_data, values)


def build_default_personal_stats_message(
    user: User,
    stats: sqlite3.Row | dict[str, int],
    balance: int,
    rank_names: list[str],
) -> str:
    return (
        f"Личная статистика {get_user_display_name(user)}\n\n"
        f"Всего спинов: {stats_value(stats, 'total_spins')}\n"
        f"Ранг: {get_rank_name(stats_value(stats, 'total_spins'), rank_names)}\n"
        f"Баланс: {balance} TEM\n"
        f"777: {stats_value(stats, 'jackpots')}\n"
        f"77X: {stats_value(stats, 'two_sevens')}\n"
        f"Три в ряд: {three_of_kind_total(stats)}"
    )


def build_personal_stats_message(
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int],
) -> tuple[str, list[dict]]:
    template = (
        db.get_user_message_template(user.id, "personal_stats")
        or db.get_message_template("personal_stats")
    )
    rank_cards = db.get_rank_cards()
    rank_names = [rank["text"] for rank in rank_cards]
    balance = db.get_tem_balance(user.id)
    if not template:
        return build_default_personal_stats_message(user, stats, balance, rank_names), []

    text, entities_data = template
    values = {
        "username": get_user_display_name(user),
        "total_spins": str(stats_value(stats, "total_spins")),
        "jackpots": str(stats_value(stats, "jackpots")),
        "two_sevens": str(stats_value(stats, "two_sevens")),
        "three_in_row": str(three_of_kind_total(stats)),
        "three_bars": str(stats_value(stats, "three_bars")),
        "three_grapes": str(stats_value(stats, "three_grapes")),
        "three_lemons": str(stats_value(stats, "three_lemons")),
        "other_spins": str(stats_value(stats, "other_spins")),
        "balance": str(balance),
        "tem_balance": str(balance),
        **rank_values(stats_value(stats, "total_spins"), rank_cards),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(stats_value(stats, "total_spins"), rank_cards),
    )


def build_default_milestone_message(
    user: User,
    stats: sqlite3.Row | dict[str, int],
    balance: int,
    rank_names: list[str],
) -> str:
    total_spins = stats_value(stats, "total_spins")
    return (
        f"{get_user_display_name(user)} достиг {total_spins} спинов.\n\n"
        f"Ранг: {get_rank_name(total_spins, rank_names)}\n"
        f"Баланс: {balance} TEM\n"
        f"777: {stats_value(stats, 'jackpots')}\n"
        f"77X: {stats_value(stats, 'two_sevens')}\n"
        f"Три в ряд: {three_of_kind_total(stats)}"
    )


def build_milestone_message(
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int],
) -> tuple[str, list[dict]]:
    template = db.get_message_template("milestone")
    rank_cards = db.get_rank_cards()
    rank_names = [rank["text"] for rank in rank_cards]
    balance = db.get_tem_balance(user.id)
    if not template:
        return build_default_milestone_message(user, stats, balance, rank_names), []

    text, entities_data = template
    values = {
        "username": get_user_display_name(user),
        "milestone": str(stats_value(stats, "total_spins")),
        "total_spins": str(stats_value(stats, "total_spins")),
        "jackpots": str(stats_value(stats, "jackpots")),
        "two_sevens": str(stats_value(stats, "two_sevens")),
        "three_in_row": str(three_of_kind_total(stats)),
        "three_bars": str(stats_value(stats, "three_bars")),
        "three_grapes": str(stats_value(stats, "three_grapes")),
        "three_lemons": str(stats_value(stats, "three_lemons")),
        "other_spins": str(stats_value(stats, "other_spins")),
        "balance": str(balance),
        "tem_balance": str(balance),
        **rank_values(stats_value(stats, "total_spins"), rank_cards),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(stats_value(stats, "total_spins"), rank_cards),
    )


def build_welcome_message(
    db: StatsDatabase,
    user: User,
    chat_label: str | None = None,
) -> tuple[str, list[dict]]:
    template = db.get_message_template("welcome")
    if not template:
        return (
            f"Привет, {get_user_display_name(user)}.\n\n"
            "Я слежу за слотами в разрешенных чатах и считаю статистику."
        ), []

    text, entities_data = template
    balance = db.get_tem_balance(user.id)
    values = {
        "username": get_user_display_name(user),
        "chat": chat_label or "",
        "balance": str(balance),
        "tem_balance": str(balance),
    }
    return apply_template_values(text, entities_data, values)


def build_daily_bonus_message(
    db: StatsDatabase,
    user: User,
    bonus_amount: int,
    balance: int,
) -> tuple[str, list[dict]]:
    template = db.get_message_template("daily_bonus")
    if not template:
        return (
            f"{get_user_display_name(user)}, ежедневный бонус получен.\n\n"
            f"+{bonus_amount} TEM\n"
            f"Баланс: {balance} TEM"
        ), []

    text, entities_data = template
    values = {
        "username": get_user_display_name(user),
        "daily_bonus": str(bonus_amount),
        "bonus": str(bonus_amount),
        "balance": str(balance),
        "tem_balance": str(balance),
    }
    return apply_template_values(text, entities_data, values)


def build_daily_bonus_wait_message(db: StatsDatabase, user: User) -> tuple[str, list[dict]]:
    balance = db.get_tem_balance(user.id)
    return (
        f"{get_user_display_name(user)}, daily bonus уже забран сегодня.\n\n"
        f"Баланс: {balance} TEM\n"
        "Следующий бонус будет завтра."
    ), []


def build_daily_bonus_reminder_message(
    db: StatsDatabase,
    subscriber: sqlite3.Row,
) -> tuple[str, list[dict]]:
    balance = db.get_tem_balance(subscriber["user_id"])
    template = db.get_message_template("daily_reminder")
    if not template:
        return (
            f"{get_row_display_name(subscriber)}, ежедневная награда уже доступна.\n\n"
            "Напиши /dailybonus, чтобы забрать TEM."
        ), []

    text, entities_data = template
    values = {
        "username": get_row_display_name(subscriber),
        "balance": str(balance),
        "tem_balance": str(balance),
    }
    return apply_template_values(text, entities_data, values)


def build_chance_hint_message(db: StatsDatabase, user: User) -> tuple[str, list[dict]]:
    chance_percent = choose_jackpot_chance_percent(db)
    template = db.get_message_template("chance_hint")
    if not template:
        return (
            f"{get_user_display_name(user)}, "
            f"{choose_jackpot_chance_text(chance_percent)}"
        ), []

    text, entities_data = template
    balance = db.get_tem_balance(user.id)
    values = {
        "username": get_user_display_name(user),
        "chance": chance_percent,
        "chance_percent": chance_percent,
        "balance": str(balance),
        "tem_balance": str(balance),
    }
    return apply_template_values(text, entities_data, values)


def build_help_message(
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    template = db.get_message_template("help")
    if not template:
        return (
            "Команды бота\n\n"
            "/stats - общая статистика чата\n"
            "/mystats - личная статистика\n"
            "/dailybonus - ежедневный бонус TEM\n"
            "/help - помощь\n\n"
            "Крути Telegram слот, а я буду считать спины, TEM и выигрыши."
        ), []

    total_spins = stats_value(stats, "total_spins") if stats else 0
    balance = db.get_tem_balance(user.id)
    rank_cards = db.get_rank_cards()
    text, entities_data = template
    values = {
        "username": get_user_display_name(user),
        "total_spins": str(total_spins),
        "balance": str(balance),
        "tem_balance": str(balance),
        **rank_values(total_spins, rank_cards),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(total_spins, rank_cards),
    )


def get_chat_label(update: Update) -> str:
    chat = update.effective_chat
    if not chat:
        return "неизвестный чат"

    return chat.title or chat.username or str(chat.id)


def get_chat_row_label(row: sqlite3.Row) -> str:
    return row["title"] or row["username"] or str(row["chat_id"])


def remember_update_chat(update: Update, db: StatsDatabase, config: BotConfig) -> None:
    if update.effective_chat and is_allowed_chat(config, update.effective_chat.id):
        db.remember_chat(update.effective_chat)


def build_chat_picker_keyboard(
    db: StatsDatabase,
    config: BotConfig,
    callback_prefix: str,
) -> InlineKeyboardMarkup:
    rows = []
    known_chats = db.get_known_chats(config.allowed_chat_ids)
    known_chat_ids = {row["chat_id"] for row in known_chats}

    for row in known_chats:
        rows.append(
            [
                InlineKeyboardButton(
                    get_chat_row_label(row),
                    callback_data=f"{callback_prefix}:{row['chat_id']}",
                )
            ]
        )

    for chat_id in sorted(config.allowed_chat_ids - known_chat_ids):
        rows.append(
            [
                InlineKeyboardButton(
                    str(chat_id),
                    callback_data=f"{callback_prefix}:{chat_id}",
                )
            ]
        )

    return InlineKeyboardMarkup(rows)


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


def build_chat_stats_payload(
    db: StatsDatabase,
    chat_id: int,
    chat_label: str,
) -> tuple[str, list[dict]]:
    totals = db.get_chat_totals(chat_id)
    top_rows = db.get_user_rows(chat_id, limit=5)
    luckiest_by_jackpots = db.get_luckiest_by_jackpots(chat_id)
    luckiest_by_ratio = db.get_luckiest_by_ratio(chat_id)
    return build_chat_stats_message(
        db,
        chat_label,
        totals,
        top_rows,
        luckiest_by_jackpots,
        luckiest_by_ratio,
    )


async def send_chat_stats_to_chat(
    context: ContextTypes.DEFAULT_TYPE,
    db: StatsDatabase,
    chat_id: int,
    chat_label: str,
) -> None:
    message, entities_data = build_chat_stats_payload(db, chat_id, chat_label)
    await send_text_with_entities(context, chat_id, message, entities_data)


async def send_chat_stats_to_chat_from_application(
    application: Application,
    db: StatsDatabase,
    chat_id: int,
    chat_label: str,
) -> None:
    message, entities_data = build_chat_stats_payload(db, chat_id, chat_label)
    await send_application_text_with_entities(application, chat_id, message, entities_data)


async def send_hourly_stats_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(3600)

        config: BotConfig = application.bot_data["config"]
        db: StatsDatabase = application.bot_data["db"]
        known_chats = db.get_known_chats(config.allowed_chat_ids)
        labels_by_chat_id = {row["chat_id"]: get_chat_row_label(row) for row in known_chats}

        for chat_id in sorted(config.allowed_chat_ids):
            try:
                await send_chat_stats_to_chat_from_application(
                    application,
                    db,
                    chat_id,
                    labels_by_chat_id.get(chat_id, str(chat_id)),
                )
            except TelegramError as error:
                logging.warning("Failed to send hourly stats to %s: %s", chat_id, error)


async def send_daily_bonus_reminders_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(3600)

        db: StatsDatabase = application.bot_data["db"]
        today = date.today().isoformat()
        subscribers = db.get_due_daily_reminder_subscribers(today)

        for subscriber in subscribers:
            try:
                message, entities_data = build_daily_bonus_reminder_message(db, subscriber)
                await send_application_text_with_entities(
                    application,
                    subscriber["chat_id"],
                    message,
                    entities_data,
                )
                db.mark_daily_reminder_sent(subscriber["user_id"], today)
            except TelegramError as error:
                logging.warning(
                    "Failed to send daily bonus reminder to %s: %s",
                    subscriber["user_id"],
                    error,
                )


async def on_startup(application: Application) -> None:
    application.bot_data["hourly_stats_task"] = application.create_task(
        send_hourly_stats_loop(application)
    )
    application.bot_data["daily_bonus_reminders_task"] = application.create_task(
        send_daily_bonus_reminders_loop(application)
    )


async def on_shutdown(application: Application) -> None:
    for task_key in ("hourly_stats_task", "daily_bonus_reminders_task"):
        task = application.bot_data.get(task_key)
        if task:
            task.cancel()


async def show_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if config.owner_user_ids and not is_owner(config, update.effective_user.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    remember_update_chat(update, db, config)

    await update.message.reply_text(
        f"ID этого чата: {update.effective_chat.id}\n"
        f"Твой user ID: {update.effective_user.id}"
    )


async def show_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if config.owner_user_ids and not is_owner(config, update.effective_user.id):
        return

    target_user = update.effective_user
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_user = update.message.reply_to_message.from_user

    await update.message.reply_text(
        f"Пользователь: {get_user_display_name(target_user)}\n"
        f"User ID: {target_user.id}"
    )


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    remember_update_chat(update, db, config)
    await send_chat_stats_to_chat(
        context,
        db,
        update.effective_chat.id,
        get_chat_label(update),
    )


async def show_personal_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]

    if update.effective_chat.type == "private":
        keyboard = build_chat_picker_keyboard(db, config, "mystats")
        if not keyboard.inline_keyboard:
            await update.message.reply_text(
                "Я пока не знаю разрешенных чатов. Напишите /stats в нужном чате или сделайте там спин."
            )
            return

        await update.message.reply_text("Выберите чат:", reply_markup=keyboard)
        return

    if not is_allowed_chat(config, update.effective_chat.id):
        return

    remember_update_chat(update, db, config)
    target_user = update.effective_user
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_user = update.message.reply_to_message.from_user

    row = db.get_user_stats(update.effective_chat.id, target_user.id)
    stats = row if row else empty_user_stats(target_user)
    message, entities_data = build_personal_stats_message(db, target_user, stats)

    await send_text_with_entities(context, update.effective_chat.id, message, entities_data)


async def handle_stats_chat_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]

    prefix, chat_id_text = query.data.split(":", 1)
    chat_id = int(chat_id_text)
    if prefix != "mystats" or not is_allowed_chat(config, chat_id):
        await query.answer()
        return

    row = db.get_user_stats(chat_id, query.from_user.id)
    stats = row if row else empty_user_stats(query.from_user)
    message, entities_data = build_personal_stats_message(db, query.from_user, stats)

    await query.answer()
    await send_text_with_entities(context, query.message.chat_id, message, entities_data)


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


async def reset_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    if not is_owner(config, update.effective_user.id):
        return

    target_user = None
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_user = update.message.reply_to_message.from_user

    target_user_id: int | None = target_user.id if target_user else None
    if target_user_id is None and context.args:
        try:
            target_user_id = int(context.args[0].lstrip("="))
        except ValueError:
            target_user_id = None

    if target_user_id is None:
        await update.message.reply_text(
            "Ответьте /resetuserstats на сообщение пользователя или напишите /resetuserstats USER_ID."
        )
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.reset_user_stats(update.effective_chat.id, target_user_id)

    if target_user:
        user_label = get_user_display_name(target_user)
    else:
        user_label = str(target_user_id)

    await update.message.reply_text(f"Личная статистика пользователя {user_label} обнулена.")


async def daily_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.remember_private_subscriber(update.effective_user, update.effective_chat.id)

    bonus_amount = choose_daily_bonus_amount()
    claimed, balance = db.claim_daily_bonus(
        update.effective_user.id,
        bonus_amount,
        date.today().isoformat(),
    )

    if not claimed:
        message, entities_data = build_daily_bonus_wait_message(db, update.effective_user)
    else:
        message, entities_data = build_daily_bonus_message(
            db,
            update.effective_user,
            bonus_amount,
            balance,
        )

    await send_text_with_entities(context, update.effective_chat.id, message, entities_data)


async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    if update.effective_chat.type == "private":
        db.remember_private_subscriber(update.effective_user, update.effective_chat.id)
    else:
        db.remember_user(update.effective_user)

    stats = None
    if update.effective_chat.type != "private":
        if not is_allowed_chat(config, update.effective_chat.id):
            return

        remember_update_chat(update, db, config)
        stats = db.get_user_stats(update.effective_chat.id, update.effective_user.id)

    message, entities_data = build_help_message(db, update.effective_user, stats)
    await send_text_with_entities(context, update.effective_chat.id, message, entities_data)


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


def extract_help_template_from_update(update: Update) -> tuple[str, list[dict]] | None:
    if not update.message or not update.message.text:
        return None

    payload = extract_text_payload_after_prefix(update.message.text, 1)
    if payload:
        help_text, start_utf16 = payload
        return help_text, serialize_entities(update.message.entities, start_utf16=start_utf16)

    target_message = update.message.reply_to_message
    if not target_message:
        return None

    if target_message.text:
        return target_message.text, serialize_entities(target_message.entities)

    if target_message.caption:
        return target_message.caption, serialize_entities(target_message.caption_entities)

    return None


def extract_text_payload_after_prefix(
    message_text: str,
    prefix_parts_count: int,
) -> tuple[str, int] | None:
    parts = message_text.split(maxsplit=prefix_parts_count)
    if len(parts) <= prefix_parts_count:
        return None

    payload = parts[prefix_parts_count]
    start_char = message_text.find(payload)
    return payload, char_index_to_utf16_offset(message_text, start_char)


def extract_user_template_from_update(update: Update) -> tuple[int, str, str, list[dict]] | None:
    if not update.message or not update.message.text:
        return None

    parts = update.message.text.split(maxsplit=3)
    if len(parts) < 2:
        return None

    target_user_id: int | None = None
    template_key: str | None = None
    prefix_parts_count = 2

    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_user_id = update.message.reply_to_message.from_user.id
        template_key = normalize_template_key(parts[1])
        prefix_parts_count = 2
    elif len(parts) >= 3:
        try:
            target_user_id = int(parts[1].lstrip("="))
        except ValueError:
            return None

        template_key = normalize_template_key(parts[2])
        prefix_parts_count = 3

    if target_user_id is None or template_key != "personal_stats":
        return None

    payload = extract_text_payload_after_prefix(update.message.text, prefix_parts_count)
    if payload:
        template_text, start_utf16 = payload
        entities_data = serialize_entities(update.message.entities, start_utf16=start_utf16)
        return target_user_id, template_key, template_text, entities_data

    target_message = update.message.reply_to_message
    if target_message and target_message.text and not target_message.text.startswith("/setusertext"):
        return (
            target_user_id,
            template_key,
            target_message.text,
            serialize_entities(target_message.entities),
        )

    if target_message and target_message.caption:
        return (
            target_user_id,
            template_key,
            target_message.caption,
            serialize_entities(target_message.caption_entities),
        )

    return None


def parse_rank_names_from_text(text: str) -> list[str]:
    return [
        rank.strip()
        for rank in text.replace("\n", ",").split(",")
        if rank.strip()
    ]


def parse_chance_settings(args: list[str]) -> tuple[float | None, int | None]:
    multiplier = None
    average_spins = None

    for arg in args:
        normalized = arg.strip().lower().replace(",", ".")
        if normalized.startswith("perc"):
            value = normalized.removeprefix("perc")
            multiplier = float(value)
        elif normalized.startswith("spin"):
            value = normalized.removeprefix("spins").removeprefix("spin")
            average_spins = int(value)
        elif normalized.startswith("freq"):
            value = normalized.removeprefix("freq")
            average_spins = int(value)
        elif normalized.startswith("every"):
            value = normalized.removeprefix("every")
            average_spins = int(value)

    return multiplier, average_spins


def parse_rank_cards_from_text(text: str, entities_data: list[dict]) -> list[dict]:
    rank_cards = []
    segment_start = 0

    for index, char in enumerate(text):
        if char not in {",", "\n"}:
            continue

        rank_cards.extend(
            build_rank_card_from_segment(text, segment_start, index, entities_data)
        )
        segment_start = index + 1

    rank_cards.extend(
        build_rank_card_from_segment(text, segment_start, len(text), entities_data)
    )
    return rank_cards


def build_rank_card_from_segment(
    text: str,
    segment_start: int,
    segment_end: int,
    entities_data: list[dict],
) -> list[dict]:
    rank_text = text[segment_start:segment_end]
    leading_spaces = len(rank_text) - len(rank_text.lstrip())
    trailing_spaces = len(rank_text) - len(rank_text.rstrip())
    rank_start_char = segment_start + leading_spaces
    rank_end_char = segment_end - trailing_spaces
    clean_text = text[rank_start_char:rank_end_char]

    if not clean_text:
        return []

    rank_start_utf16 = char_index_to_utf16_offset(text, rank_start_char)
    rank_end_utf16 = char_index_to_utf16_offset(text, rank_end_char)
    rank_entities = []
    for entity in entities_data:
        entity_start = int(entity["offset"])
        entity_end = entity_start + int(entity["length"])
        if entity_start >= rank_start_utf16 and entity_end <= rank_end_utf16:
            adjusted_entity = dict(entity)
            adjusted_entity["offset"] = entity_start - rank_start_utf16
            rank_entities.append(adjusted_entity)

    return [{"text": clean_text, "entities": rank_entities}]


def extract_rank_cards_from_update(update: Update) -> list[dict] | None:
    if not update.message or not update.message.text:
        return None

    payload = extract_text_payload_after_prefix(update.message.text, 1)
    if payload:
        rank_text, start_utf16 = payload
        entities_data = serialize_entities(update.message.entities, start_utf16=start_utf16)
        return parse_rank_cards_from_text(rank_text, entities_data)

    target_message = update.message.reply_to_message
    if not target_message:
        return None

    if target_message.text:
        return parse_rank_cards_from_text(
            target_message.text,
            serialize_entities(target_message.entities),
        )

    if target_message.caption:
        return parse_rank_cards_from_text(
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
            "/settext triple текст\n"
            "/settext stats текст\n"
            "/settext mystats текст\n\n"
            "/settext welcome текст\n\n"
            "/settext milestone текст\n\n"
            "/settext dailybonus текст\n\n"
            "/settext dailyreminder текст\n\n"
            "/settext chance текст\n\n"
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
        "combination - выпавшая комбинация\n"
        "total_spins - всего спинов\n"
        "jackpots - 777\n"
        "two_sevens - 77X\n"
        "three_bars - три BAR\n"
        "three_grapes - три винограда\n"
        "three_lemons - три лимона\n"
        "three_in_row - всего три в ряд\n"
        "top5 - топ 5 по спинам\n"
        "luckiest_777 - самый везучий по количеству 777\n"
        "luckiest_ratio - самый везучий по проценту 777\n"
        "milestone - текущий рубеж спинов\n"
        "balance/tem_balance - баланс TEM\n"
        "daily_bonus/bonus - ежедневный бонус TEM\n"
        "chance/chance_percent - примерный шанс 777\n"
        "rank - текущий ранг"
    )


async def set_help_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not is_owner(config, update.effective_user.id):
        return

    extracted = extract_help_template_from_update(update)
    if not extracted:
        await update.message.reply_text(
            "Формат:\n"
            "/sethelp текст помощи\n\n"
            "Для Telegram custom emoji отправьте сообщение с нужным оформлением и ответьте на него /sethelp.\n\n"
            "Placeholders: username, balance, tem_balance, total_spins, rank"
        )
        return

    help_text, entities_data = extracted
    db: StatsDatabase = context.application.bot_data["db"]
    db.set_message_template("help", help_text, entities_data)

    await update.message.reply_text("Help-сообщение сохранено.")


async def set_user_message_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not is_owner(config, update.effective_user.id):
        return

    extracted = extract_user_template_from_update(update)
    if not extracted:
        await update.message.reply_text(
            "Формат:\n"
            "/setusertext USER_ID mystats текст\n\n"
            "Или ответьте на сообщение пользователя:\n"
            "/setusertext mystats текст\n\n"
            "Индивидуальный шаблон работает только для личной статистики."
        )
        return

    target_user_id, template_key, template_text, entities_data = extracted
    db: StatsDatabase = context.application.bot_data["db"]
    db.set_user_message_template(target_user_id, template_key, template_text, entities_data)

    await update.message.reply_text(
        f"Индивидуальный шаблон личной статистики для {target_user_id} сохранен."
    )


async def set_ranks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not is_owner(config, update.effective_user.id):
        return

    rank_cards = extract_rank_cards_from_update(update)
    if not rank_cards:
        await update.message.reply_text(
            "Формат:\n"
            "/setranks новичок, искатель, крутящий, азартный, мастер, легенда\n\n"
            "Можно использовать Telegram custom emoji. Для длинного списка можно отправить ранги отдельным сообщением и ответить на него /setranks."
        )
        return

    if len(rank_cards) < 2:
        await update.message.reply_text("Нужно указать минимум 2 ранга через запятую.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.set_rank_cards(rank_cards)

    lines = ["Ранги сохранены:"]
    for index, rank in enumerate(rank_cards):
        lines.append(f"{index * 100}+ спинов: {rank['text']}")

    await update.message.reply_text("\n".join(lines))


async def set_chance_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not is_owner(config, update.effective_user.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    if not context.args:
        await update.message.reply_text(
            "Формат:\n"
            "/setchance perc10 spins5\n\n"
            "perc10 - умножить проценты на 10\n"
            "perc2 - умножить проценты на 2\n"
            "spins5 - слать примерно раз в 5 спинов\n\n"
            f"Сейчас: perc{get_chance_multiplier(db):g}, spins{get_chance_average_spins(db)}"
        )
        return

    try:
        multiplier, average_spins = parse_chance_settings(context.args)
    except ValueError:
        await update.message.reply_text("Не понял настройки. Пример: /setchance perc10 spins5")
        return

    if multiplier is not None:
        db.set_bot_setting("chance_multiplier", multiplier)

    if average_spins is not None:
        db.set_bot_setting("chance_average_spins", average_spins)

    await update.message.reply_text(
        "Настройки chance-сообщения сохранены.\n\n"
        f"Множитель процентов: perc{get_chance_multiplier(db):g}\n"
        f"Средняя частота: раз в {get_chance_average_spins(db)} спинов"
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
    user_templates = db.get_user_message_templates()

    lines = [
        "Сохраненные шаблоны:",
        "",
        "Команды установки:",
        "/settext 777",
        "/settext 77x",
        "/settext triple",
        "/settext stats",
        "/settext mystats",
        "/settext welcome",
        "/settext milestone",
        "/settext dailybonus",
        "/settext dailyreminder",
        "/settext chance",
        "/sethelp",
        "/setranks",
        f"/setchance perc{get_chance_multiplier(db):g} spins{get_chance_average_spins(db)}",
        "",
    ]
    rank_names = db.get_rank_names()
    lines.append("Ранги:")
    for index, rank in enumerate(rank_names):
        lines.append(f"{index * 100}+ спинов: {rank}")
    lines.append("")

    for row in templates:
        label = TEMPLATE_LABELS.get(row["template_key"], row["template_key"])
        preview = row["text"].replace("\n", " ")
        if len(preview) > 120:
            preview = f"{preview[:117]}..."
        lines.append(f"{label}: {preview}")

    if user_templates:
        lines.append("")
        lines.append("Индивидуальные шаблоны:")
        for row in user_templates:
            label = TEMPLATE_LABELS.get(row["template_key"], row["template_key"])
            user_name = get_display_name(row) if row["username"] or row["first_name"] else row["user_id"]
            preview = row["text"].replace("\n", " ")
            if len(preview) > 80:
                preview = f"{preview[:77]}..."
            lines.append(f"{user_name} / {label}: {preview}")

    buttons = []
    template_keys = list(TEMPLATE_LABELS)
    for index in range(0, len(template_keys), 2):
        row_buttons = []
        for template_key in template_keys[index:index + 2]:
            row_buttons.append(
                InlineKeyboardButton(
                    TEMPLATE_LABELS[template_key],
                    callback_data=f"textcfg:template:{template_key}",
                )
            )
        buttons.append(row_buttons)

    buttons.append(
        [
            InlineKeyboardButton("chance настройки", callback_data="textcfg:chance"),
            InlineKeyboardButton("ранги", callback_data="textcfg:ranks"),
        ]
    )

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_text_setting_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, query.from_user.id):
        await query.answer()
        return

    db: StatsDatabase = context.application.bot_data["db"]
    parts = query.data.split(":", 2)
    if len(parts) < 2 or parts[0] != "textcfg":
        await query.answer()
        return

    setting_type = parts[1]
    await query.answer()

    if setting_type == "template" and len(parts) == 3:
        template_key = parts[2]
        label = TEMPLATE_LABELS.get(template_key, template_key)
        template = db.get_message_template(template_key)
        if not template:
            await query.message.reply_text(f"{label}\n\nШаблон не настроен.")
            return

        text, entities_data = template
        await query.message.reply_text(f"{label}\nТекущий текст:")
        await send_text_with_entities(context, query.message.chat_id, text, entities_data)
        return

    if setting_type == "chance":
        await query.message.reply_text(
            "chance настройки\n\n"
            f"Множитель процентов: perc{get_chance_multiplier(db):g}\n"
            f"Средняя частота: раз в {get_chance_average_spins(db)} спинов\n\n"
            "Изменить: /setchance perc10 spins5"
        )
        return

    if setting_type == "ranks":
        rank_names = db.get_rank_names()
        lines = ["Ранги:"]
        for index, rank in enumerate(rank_names):
            lines.append(f"{index * 100}+ спинов: {rank}")
        lines.append("")
        lines.append("Изменить: /setranks ранг1, ранг2, ранг3")
        await query.message.reply_text("\n".join(lines))


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user:
        return

    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.remember_private_subscriber(update.effective_user, update.effective_chat.id)
    message, entities_data = build_welcome_message(db, update.effective_user)
    await send_text_with_entities(context, update.effective_chat.id, message, entities_data)


async def welcome_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or not update.message.new_chat_members:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    remember_update_chat(update, db, config)
    chat_label = get_chat_label(update)

    for member in update.message.new_chat_members:
        if member.is_bot:
            continue

        db.remember_user(member)
        message, entities_data = build_welcome_message(db, member, chat_label=chat_label)
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)


async def react_to_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    remember_update_chat(update, db, config)

    if not update.message or not update.message.dice:
        return

    if is_forwarded_message(update.message):
        return

    dice = update.message.dice
    if dice.emoji != SLOT_MACHINE_EMOJI:
        return

    db.remember_user(update.effective_user)

    result = classify_slot_value(dice.value)
    db.record_spin(update.effective_chat.id, update.effective_user.id, result)
    db.add_tem(update.effective_user.id, calculate_spin_reward(result))
    user_stats = db.get_user_stats(update.effective_chat.id, update.effective_user.id)
    chat_totals = db.get_chat_totals(update.effective_chat.id)

    if result == "jackpot":
        message, entities_data, gift = await build_jackpot_message(
            config,
            db,
            update.effective_user,
            stats=user_stats,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
        await notify_owners_about_jackpot(update, context, config, gift)
    elif result == "two_sevens":
        message, entities_data = await build_two_sevens_message(
            config,
            db,
            update.effective_user,
            stats=user_stats,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
    elif result in {"three_bars", "three_grapes", "three_lemons"}:
        message, entities_data, gift = await build_three_of_kind_message(
            config,
            db,
            update.effective_user,
            result,
            stats=user_stats,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
        await notify_owners_about_small_gift(update, context, config, result, gift)

    if should_send_chance_hint(db):
        message, entities_data = build_chance_hint_message(db, update.effective_user)
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)

    if user_stats and is_spin_milestone(stats_value(user_stats, "total_spins")):
        message, entities_data = build_milestone_message(
            db,
            update.effective_user,
            user_stats,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)

    if is_chat_stats_milestone(stats_value(chat_totals, "total_spins")):
        await send_chat_stats_to_chat(
            context,
            db,
            update.effective_chat.id,
            get_chat_label(update),
        )


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        level=logging.INFO,
    )

    config = read_config()
    db = StatsDatabase(config.db_path)
    application = (
        Application.builder()
        .token(config.token)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )
    application.bot_data["config"] = config
    application.bot_data["db"] = db

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("chatid", show_chat_id))
    application.add_handler(CommandHandler("userid", show_user_id))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("mystats", show_personal_stats))
    application.add_handler(CommandHandler("me", show_personal_stats))
    application.add_handler(CommandHandler("dailybonus", daily_bonus))
    application.add_handler(CommandHandler("help", show_help))
    application.add_handler(CommandHandler("resetstats", reset_stats))
    application.add_handler(CommandHandler("resetuserstats", reset_user_stats))
    application.add_handler(CommandHandler("emojiid", show_custom_emoji_ids))
    application.add_handler(CommandHandler("settext", set_message_template))
    application.add_handler(CommandHandler("sethelp", set_help_template))
    application.add_handler(CommandHandler("setusertext", set_user_message_template))
    application.add_handler(CommandHandler("setranks", set_ranks))
    application.add_handler(CommandHandler("setchance", set_chance_settings))
    application.add_handler(CommandHandler("texts", show_message_templates))
    application.add_handler(CallbackQueryHandler(handle_text_setting_choice, pattern="^textcfg:"))
    application.add_handler(CallbackQueryHandler(handle_stats_chat_choice, pattern="^mystats:"))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_chat_members))
    application.add_handler(MessageHandler(filters.ALL, react_to_message))

    logging.info("Bot started. Waiting for messages...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
