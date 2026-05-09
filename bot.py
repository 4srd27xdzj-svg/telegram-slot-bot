import asyncio
import json
import logging
import os
import random
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MessageEntity,
    ReplyKeyboardMarkup,
    Update,
    User,
)
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
DEFAULT_TOURNAMENT_DAYS = 7
TOURNAMENT_REMINDER_SECONDS = 8 * 3600
TOURNAMENT_LOOP_SECONDS = 15 * 60
REFERRAL_BUTTON_TEXT = "Рефералы"
ALL_CONTESTS_BUTTON_TEXT = "Все конкурсы"
AUTO_BOLD_PLACEHOLDERS = {
    "username",
    "chance",
    "chance_percent",
    "giftr",
    "daily_bonus",
    "bonus",
    "balance",
    "tem_balance",
    "rank",
    "gift_title",
    "luckiest_777",
    "luckiest_ratio",
    "total_tickets",
    "prize_places",
}
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
    "777progress": "jackpot_progress",
    "jackpotprogress": "jackpot_progress",
    "jackpot_progress": "jackpot_progress",
    "первый777": "jackpot_progress",
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
    "tripleprogress": "three_of_kind_progress",
    "threeprogress": "three_of_kind_progress",
    "three_of_kind_progress": "three_of_kind_progress",
    "ряд_progress": "three_of_kind_progress",
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
    "dailybonuswait": "daily_bonus_wait",
    "daily_bonus_wait": "daily_bonus_wait",
    "bonuswait": "daily_bonus_wait",
    "уже_забран": "daily_bonus_wait",
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
    "jackpot_progress": "первый 777 без gift",
    "two_sevens": "77X",
    "three_of_kind": "три в ряд",
    "three_of_kind_progress": "три в ряд без giftr",
    "stats": "общая статистика",
    "personal_stats": "личная статистика",
    "welcome": "приветствие",
    "milestone": "рубеж спинов",
    "daily_bonus": "ежедневный бонус",
    "daily_bonus_wait": "ежедневный бонус уже забран",
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

    return {int(item) for item in re.findall(r"-?\d+", value)}


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

            CREATE TABLE IF NOT EXISTS user_rank_overrides (
                user_id INTEGER PRIMARY KEY,
                rank_text TEXT NOT NULL,
                entities_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS rating_excluded_users (
                user_id INTEGER PRIMARY KEY,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

            CREATE TABLE IF NOT EXISTS tournaments (
                tournament_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                owner_user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                prize_places INTEGER NOT NULL,
                prizes_json TEXT NOT NULL DEFAULT '[]',
                announcement_text TEXT NOT NULL,
                announcement_entities_json TEXT NOT NULL DEFAULT '[]',
                started_at TEXT NOT NULL,
                ends_at TEXT NOT NULL,
                baseline_spins_json TEXT NOT NULL DEFAULT '{}',
                last_reminder_at TEXT,
                finished_at TEXT,
                winners_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS referral_contests (
                contest_id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                prize_places INTEGER NOT NULL,
                prizes_json TEXT NOT NULL DEFAULT '[]',
                announcement_text TEXT NOT NULL,
                announcement_entities_json TEXT NOT NULL DEFAULT '[]',
                started_at TEXT NOT NULL,
                ends_at TEXT,
                max_participants INTEGER,
                finished_at TEXT,
                winners_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS referral_submissions (
                submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
                contest_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                submitted_at TEXT NOT NULL,
                reviewed_at TEXT,
                UNIQUE(contest_id, user_id),
                FOREIGN KEY (contest_id) REFERENCES referral_contests(contest_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            """
        )
        self.connection.commit()
        self.ensure_schema_migrations()

    def ensure_schema_migrations(self) -> None:
        tournament_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(tournaments)").fetchall()
        }
        if "baseline_spins_json" not in tournament_columns:
            self.connection.execute(
                "ALTER TABLE tournaments ADD COLUMN baseline_spins_json TEXT NOT NULL DEFAULT '{}'"
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

    def get_user_by_username(self, username: str) -> sqlite3.Row | None:
        normalized_username = username.strip().lstrip("@").lower()
        if not normalized_username:
            return None

        return self.connection.execute(
            """
            SELECT user_id, username, first_name, last_name
            FROM users
            WHERE LOWER(username) = ?
            """,
            (normalized_username,),
        ).fetchone()

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
                LEFT JOIN rating_excluded_users e ON e.user_id = s.user_id
                WHERE s.chat_id = ? AND e.user_id IS NULL
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
            LEFT JOIN rating_excluded_users e ON e.user_id = s.user_id
            WHERE s.chat_id = ? AND s.jackpots > 0 AND e.user_id IS NULL
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
            LEFT JOIN rating_excluded_users e ON e.user_id = s.user_id
            WHERE s.chat_id = ?
              AND s.total_spins > 0
              AND s.jackpots > 0
              AND e.user_id IS NULL
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

    def exclude_user_from_rating(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO rating_excluded_users (user_id)
            VALUES (?)
            ON CONFLICT(user_id) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id,),
        )
        self.connection.commit()

    def include_user_in_rating(self, user_id: int) -> None:
        self.connection.execute(
            "DELETE FROM rating_excluded_users WHERE user_id = ?",
            (user_id,),
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

    def set_user_rank_card(self, user_id: int, rank_card: dict) -> None:
        self.connection.execute(
            """
            INSERT INTO user_rank_overrides (user_id, rank_text, entities_json)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                rank_text = excluded.rank_text,
                entities_json = excluded.entities_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                user_id,
                rank_card["text"],
                json.dumps(rank_card.get("entities", []), ensure_ascii=False),
            ),
        )
        self.connection.commit()

    def get_user_rank_card(self, user_id: int) -> dict | None:
        row = self.connection.execute(
            """
            SELECT rank_text, entities_json
            FROM user_rank_overrides
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        if not row:
            return None

        return {
            "text": row["rank_text"],
            "entities": json.loads(row["entities_json"]),
        }

    def get_user_rank_overrides(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    r.user_id,
                    r.rank_text,
                    r.updated_at,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM user_rank_overrides r
                LEFT JOIN users u ON u.user_id = r.user_id
                ORDER BY r.updated_at DESC
                """
            )
        )

    def create_tournament(
        self,
        chat_id: int,
        owner_user_id: int,
        prize_places: int,
        prizes: list[str],
        announcement_text: str,
        announcement_entities: list[dict],
        started_at: str,
        ends_at: str,
        baseline_spins: dict[str, int],
    ) -> int:
        self.connection.execute(
            """
            INSERT INTO tournaments (
                chat_id,
                owner_user_id,
                prize_places,
                prizes_json,
                announcement_text,
                announcement_entities_json,
                started_at,
                ends_at,
                baseline_spins_json,
                last_reminder_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chat_id,
                owner_user_id,
                prize_places,
                json.dumps(prizes, ensure_ascii=False),
                announcement_text,
                json.dumps(announcement_entities, ensure_ascii=False),
                started_at,
                ends_at,
                json.dumps(baseline_spins, ensure_ascii=False),
                started_at,
            ),
        )
        self.connection.commit()
        return int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    def get_active_tournament_for_chat(self, chat_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM tournaments
            WHERE chat_id = ? AND status IN ('active', 'pending_approval')
            ORDER BY tournament_id DESC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()

    def get_active_tournaments(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT *
                FROM tournaments
                WHERE status = 'active'
                ORDER BY tournament_id
                """
            )
        )

    def get_tournament(self, tournament_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM tournaments
            WHERE tournament_id = ?
            """,
            (tournament_id,),
        ).fetchone()

    def mark_tournament_reminded(self, tournament_id: int, reminded_at: str) -> None:
        self.connection.execute(
            """
            UPDATE tournaments
            SET last_reminder_at = ?
            WHERE tournament_id = ?
            """,
            (reminded_at, tournament_id),
        )
        self.connection.commit()

    def update_tournament_text(
        self,
        tournament_id: int,
        announcement_text: str,
        announcement_entities: list[dict],
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE tournaments
            SET announcement_text = ?,
                announcement_entities_json = ?
            WHERE tournament_id = ? AND status = 'active'
            """,
            (
                announcement_text,
                json.dumps(announcement_entities, ensure_ascii=False),
                tournament_id,
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def update_tournament_ends_at(self, tournament_id: int, ends_at: str) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE tournaments
            SET ends_at = ?
            WHERE tournament_id = ? AND status = 'active'
            """,
            (ends_at, tournament_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def update_tournament_prizes(
        self,
        tournament_id: int,
        prize_places: int,
        prizes: list[str],
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE tournaments
            SET prize_places = ?,
                prizes_json = ?
            WHERE tournament_id = ? AND status = 'active'
            """,
            (prize_places, json.dumps(prizes, ensure_ascii=False), tournament_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def update_tournament_prize_places(
        self,
        tournament_id: int,
        prize_places: int,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE tournaments
            SET prize_places = ?
            WHERE tournament_id = ? AND status = 'active'
            """,
            (prize_places, tournament_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def finish_tournament(
        self,
        tournament_id: int,
        result_payload: object,
        finished_at: str,
    ) -> None:
        self.connection.execute(
            """
            UPDATE tournaments
            SET status = 'finished',
                winners_json = ?,
                finished_at = ?
            WHERE tournament_id = ?
            """,
            (json.dumps(result_payload, ensure_ascii=False), finished_at, tournament_id),
        )
        self.connection.commit()

    def set_tournament_pending_approval(
        self,
        tournament_id: int,
        result_payload: object,
        finished_at: str,
    ) -> None:
        self.connection.execute(
            """
            UPDATE tournaments
            SET status = 'pending_approval',
                winners_json = ?,
                finished_at = ?
            WHERE tournament_id = ? AND status = 'active'
            """,
            (json.dumps(result_payload, ensure_ascii=False), finished_at, tournament_id),
        )
        self.connection.commit()

    def update_tournament_result_payload(
        self,
        tournament_id: int,
        result_payload: object,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE tournaments
            SET winners_json = ?
            WHERE tournament_id = ? AND status = 'pending_approval'
            """,
            (json.dumps(result_payload, ensure_ascii=False), tournament_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def cancel_tournament(self, tournament_id: int) -> None:
        self.connection.execute(
            """
            UPDATE tournaments
            SET status = 'cancelled'
            WHERE tournament_id = ? AND status = 'active'
            """,
            (tournament_id,),
        )
        self.connection.commit()

    def get_chat_spin_baseline(self, chat_id: int) -> dict[str, int]:
        rows = self.connection.execute(
            """
            SELECT user_id, total_spins
            FROM slot_stats
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchall()
        return {str(row["user_id"]): int(row["total_spins"] or 0) for row in rows}

    def get_tournament_ticket_rows(self, tournament: sqlite3.Row) -> list[dict]:
        chat_id = tournament["chat_id"]
        try:
            baseline = json.loads(tournament["baseline_spins_json"] or "{}")
        except json.JSONDecodeError:
            baseline = {}

        raw_rows = self.connection.execute(
            """
            SELECT
                s.user_id,
                s.total_spins,
                u.username,
                u.first_name,
                u.last_name
            FROM slot_stats s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.chat_id = ?
            ORDER BY s.total_spins DESC, s.user_id
            """,
            (chat_id,),
        ).fetchall()
        rows = []
        for row in raw_rows:
            total_spins = int(row["total_spins"] or 0)
            baseline_spins = int(baseline.get(str(row["user_id"]), 0) or 0)
            tickets = max(0, total_spins - baseline_spins)
            if tickets <= 0:
                continue

            rows.append(
                {
                    "user_id": row["user_id"],
                    "total_spins": total_spins,
                    "baseline_spins": baseline_spins,
                    "tickets": tickets,
                    "username": row["username"],
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                }
            )

        rows.sort(key=lambda row: (-row["tickets"], row["user_id"]))
        return rows

    def get_chat_total_spin_rows(self, chat_id: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    s.user_id,
                    s.total_spins,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM slot_stats s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.chat_id = ? AND s.total_spins > 0
                ORDER BY s.total_spins DESC, s.user_id
                """,
                (chat_id,),
            )
        )

    def create_referral_contest(
        self,
        owner_user_id: int,
        prize_places: int,
        prizes: list[str],
        announcement_text: str,
        announcement_entities: list[dict],
        started_at: str,
        ends_at: str | None,
        max_participants: int | None,
    ) -> int:
        self.connection.execute(
            """
            INSERT INTO referral_contests (
                owner_user_id,
                prize_places,
                prizes_json,
                announcement_text,
                announcement_entities_json,
                started_at,
                ends_at,
                max_participants
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_user_id,
                prize_places,
                json.dumps(prizes, ensure_ascii=False),
                announcement_text,
                json.dumps(announcement_entities, ensure_ascii=False),
                started_at,
                ends_at,
                max_participants,
            ),
        )
        self.connection.commit()
        return int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    def get_referral_contest(self, contest_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM referral_contests
            WHERE contest_id = ?
            """,
            (contest_id,),
        ).fetchone()

    def get_active_referral_contests(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT *
                FROM referral_contests
                WHERE status = 'active'
                ORDER BY contest_id
                """
            )
        )

    def cancel_referral_contest(self, contest_id: int) -> None:
        self.connection.execute(
            """
            UPDATE referral_contests
            SET status = 'cancelled'
            WHERE contest_id = ? AND status = 'active'
            """,
            (contest_id,),
        )
        self.connection.commit()

    def update_referral_contest_text(
        self,
        contest_id: int,
        announcement_text: str,
        announcement_entities: list[dict],
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE referral_contests
            SET announcement_text = ?,
                announcement_entities_json = ?
            WHERE contest_id = ? AND status = 'active'
            """,
            (
                announcement_text,
                json.dumps(announcement_entities, ensure_ascii=False),
                contest_id,
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def update_referral_contest_finish(
        self,
        contest_id: int,
        ends_at: str | None,
        max_participants: int | None,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE referral_contests
            SET ends_at = ?,
                max_participants = ?
            WHERE contest_id = ? AND status = 'active'
            """,
            (ends_at, max_participants, contest_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def update_referral_contest_prizes(
        self,
        contest_id: int,
        prize_places: int,
        prizes: list[str],
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE referral_contests
            SET prize_places = ?,
                prizes_json = ?
            WHERE contest_id = ? AND status = 'active'
            """,
            (prize_places, json.dumps(prizes, ensure_ascii=False), contest_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def update_referral_contest_prize_places(
        self,
        contest_id: int,
        prize_places: int,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE referral_contests
            SET prize_places = ?
            WHERE contest_id = ? AND status = 'active'
            """,
            (prize_places, contest_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def finish_referral_contest(
        self,
        contest_id: int,
        result_payload: object,
        finished_at: str,
    ) -> None:
        self.connection.execute(
            """
            UPDATE referral_contests
            SET status = 'finished',
                winners_json = ?,
                finished_at = ?
            WHERE contest_id = ?
            """,
            (json.dumps(result_payload, ensure_ascii=False), finished_at, contest_id),
        )
        self.connection.commit()

    def set_referral_contest_pending_approval(
        self,
        contest_id: int,
        result_payload: object,
        finished_at: str,
    ) -> None:
        self.connection.execute(
            """
            UPDATE referral_contests
            SET status = 'pending_approval',
                winners_json = ?,
                finished_at = ?
            WHERE contest_id = ? AND status = 'active'
            """,
            (json.dumps(result_payload, ensure_ascii=False), finished_at, contest_id),
        )
        self.connection.commit()

    def update_referral_result_payload(
        self,
        contest_id: int,
        result_payload: object,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE referral_contests
            SET winners_json = ?
            WHERE contest_id = ? AND status = 'pending_approval'
            """,
            (json.dumps(result_payload, ensure_ascii=False), contest_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def get_referral_submission_for_user(
        self,
        contest_id: int,
        user_id: int,
    ) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM referral_submissions
            WHERE contest_id = ? AND user_id = ?
            """,
            (contest_id, user_id),
        ).fetchone()

    def save_referral_submission(
        self,
        contest_id: int,
        user_id: int,
        file_id: str,
        submitted_at: str,
    ) -> sqlite3.Row:
        existing = self.get_referral_submission_for_user(contest_id, user_id)
        if existing and existing["status"] in {"pending", "accepted"}:
            return existing

        self.connection.execute(
            """
            INSERT INTO referral_submissions (contest_id, user_id, file_id, status, submitted_at)
            VALUES (?, ?, ?, 'pending', ?)
            ON CONFLICT(contest_id, user_id) DO UPDATE SET
                file_id = excluded.file_id,
                status = 'pending',
                submitted_at = excluded.submitted_at,
                reviewed_at = NULL
            """,
            (contest_id, user_id, file_id, submitted_at),
        )
        self.connection.commit()
        row = self.get_referral_submission_for_user(contest_id, user_id)
        if row is None:
            raise RuntimeError("Referral submission was not saved.")
        return row

    def get_referral_submission(self, submission_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT
                s.*,
                u.username,
                u.first_name,
                u.last_name
            FROM referral_submissions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.submission_id = ?
            """,
            (submission_id,),
        ).fetchone()

    def review_referral_submission(
        self,
        submission_id: int,
        status: str,
        reviewed_at: str,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE referral_submissions
            SET status = ?,
                reviewed_at = ?
            WHERE submission_id = ? AND status = 'pending'
            """,
            (status, reviewed_at, submission_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def get_referral_participant_rows(self, contest_id: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    s.submission_id,
                    s.user_id,
                    s.submitted_at,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM referral_submissions s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.contest_id = ? AND s.status = 'accepted'
                ORDER BY s.submitted_at, s.user_id
                """,
                (contest_id,),
            )
        )

    def count_referral_participants(self, contest_id: int) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM referral_submissions
            WHERE contest_id = ? AND status = 'accepted'
            """,
            (contest_id,),
        ).fetchone()
        return int(row["count"] or 0) if row else 0

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


def get_effective_rank_card(
    total_spins: int,
    rank_cards: list[dict],
    user_rank_card: dict | None = None,
) -> dict:
    return user_rank_card or get_rank_card(total_spins, rank_cards)


def get_rank_name(total_spins: int, rank_names: list[str]) -> str:
    rank_index = total_spins // 100
    if rank_index < len(rank_names):
        return rank_names[rank_index]

    return rank_names[-1]


def rank_values(
    total_spins: int,
    rank_cards: list[dict],
    user_rank_card: dict | None = None,
) -> dict[str, str]:
    return {"rank": get_effective_rank_card(total_spins, rank_cards, user_rank_card)["text"]}


def rank_value_entities(
    total_spins: int,
    rank_cards: list[dict],
    user_rank_card: dict | None = None,
) -> dict[str, list[dict]]:
    return {
        "rank": get_effective_rank_card(total_spins, rank_cards, user_rank_card)["entities"]
    }


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


def bold_entity(offset: int, text: str) -> dict | None:
    length = utf16_len(text)
    if length <= 0:
        return None

    return {"type": "bold", "offset": offset, "length": length}


def bold_entities_excluding(
    offset: int,
    text: str,
    protected_entities: list[dict] | None = None,
) -> list[dict]:
    protected_entities = protected_entities or []
    text_length = utf16_len(text)
    if text_length <= 0:
        return []

    protected_ranges = sorted(
        (
            int(entity["offset"]),
            int(entity["offset"]) + int(entity["length"]),
        )
        for entity in protected_entities
    )
    bold_entities = []
    cursor = 0
    for start, end in protected_ranges:
        if start > cursor:
            bold_entities.append(
                {"type": "bold", "offset": offset + cursor, "length": start - cursor}
            )
        cursor = max(cursor, end)

    if cursor < text_length:
        bold_entities.append(
            {"type": "bold", "offset": offset + cursor, "length": text_length - cursor}
        )

    return [entity for entity in bold_entities if entity["length"] > 0]


def shift_entities(entities: list[dict], offset: int) -> list[dict]:
    shifted = []
    for entity in entities:
        shifted_entity = dict(entity)
        shifted_entity["offset"] = int(entity["offset"]) + offset
        shifted.append(shifted_entity)
    return shifted


def append_text_with_entities(
    pieces: list[str],
    entities: list[dict],
    text: str,
    extra_entities: list[dict] | None = None,
    bold: bool = False,
) -> None:
    offset = utf16_len("".join(pieces))
    pieces.append(text)
    if bold:
        entities.extend(bold_entities_excluding(offset, text, extra_entities))
    if extra_entities:
        entities.extend(shift_entities(extra_entities, offset))


def format_top_spin_rows_with_entities(
    rows: list[sqlite3.Row],
    db: StatsDatabase,
    rank_cards: list[dict],
) -> tuple[str, list[dict]]:
    if not rows:
        return "Пока нет спинов.", []

    lines = []
    all_entities = []
    for index, row in enumerate(rows, start=1):
        pieces = []
        entities = []
        user_rank_card = db.get_user_rank_card(row["user_id"])
        rank_card = get_effective_rank_card(row["total_spins"], rank_cards, user_rank_card)

        append_text_with_entities(pieces, entities, f"{index}. ")
        append_text_with_entities(pieces, entities, get_display_name(row), bold=True)
        append_text_with_entities(pieces, entities, " - ")
        append_text_with_entities(pieces, entities, str(row["total_spins"]), bold=True)
        append_text_with_entities(pieces, entities, " спинов, ранг: ")
        append_text_with_entities(
            pieces,
            entities,
            rank_card["text"],
            extra_entities=rank_card.get("entities", []),
            bold=True,
        )
        append_text_with_entities(pieces, entities, ", 777: ")
        append_text_with_entities(pieces, entities, str(row["jackpots"]), bold=True)
        append_text_with_entities(pieces, entities, ", 77X: ")
        append_text_with_entities(pieces, entities, str(row["two_sevens"]), bold=True)

        line = "".join(pieces)
        previous_text = "\n".join(lines)
        line_offset = utf16_len(previous_text) + (1 if previous_text else 0)
        all_entities.extend(shift_entities(entities, line_offset))
        lines.append(line)

    return "\n".join(lines), all_entities


def format_top_spin_rows(rows: list[sqlite3.Row], db: StatsDatabase, rank_cards: list[dict]) -> str:
    return format_top_spin_rows_with_entities(rows, db, rank_cards)[0]


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


def utc_now() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def datetime_to_storage(value: datetime) -> str:
    return value.isoformat()


def datetime_from_storage(value: str) -> datetime:
    return datetime.fromisoformat(value)


def format_datetime_for_message(value: str) -> str:
    return datetime_from_storage(value).strftime("%d.%m.%Y %H:%M UTC")


def parse_duration_token(token: str, default_unit: str = "days") -> timedelta:
    normalized = token.strip().lower().replace(",", ".")
    if not normalized:
        raise ValueError("empty duration")

    if normalized.replace(".", "", 1).isdigit():
        amount = float(normalized)
        unit = default_unit
    elif normalized.startswith("minutes"):
        amount = float(normalized.removeprefix("minutes"))
        unit = "minutes"
    elif normalized.startswith("minute"):
        amount = float(normalized.removeprefix("minute"))
        unit = "minutes"
    elif normalized.startswith("mins"):
        amount = float(normalized.removeprefix("mins"))
        unit = "minutes"
    elif normalized.startswith("min"):
        amount = float(normalized.removeprefix("min"))
        unit = "minutes"
    elif normalized.endswith("m"):
        amount = float(normalized[:-1])
        unit = "minutes"
    elif normalized.startswith("hours"):
        amount = float(normalized.removeprefix("hours"))
        unit = "hours"
    elif normalized.startswith("hour"):
        amount = float(normalized.removeprefix("hour"))
        unit = "hours"
    elif normalized.startswith("hrs"):
        amount = float(normalized.removeprefix("hrs"))
        unit = "hours"
    elif normalized.startswith("hr"):
        amount = float(normalized.removeprefix("hr"))
        unit = "hours"
    elif normalized.startswith("h"):
        amount = float(normalized.removeprefix("h"))
        unit = "hours"
    elif normalized.endswith("h"):
        amount = float(normalized[:-1])
        unit = "hours"
    elif normalized.startswith("days"):
        amount = float(normalized.removeprefix("days"))
        unit = "days"
    elif normalized.startswith("day"):
        amount = float(normalized.removeprefix("day"))
        unit = "days"
    elif normalized.startswith("d"):
        amount = float(normalized.removeprefix("d"))
        unit = "days"
    elif normalized.endswith("d"):
        amount = float(normalized[:-1])
        unit = "days"
    else:
        raise ValueError("bad duration")

    if amount <= 0:
        raise ValueError("duration must be positive")

    if unit == "minutes":
        return timedelta(minutes=amount)
    if unit == "hours":
        return timedelta(hours=amount)
    if unit == "days":
        return timedelta(days=amount)
    raise ValueError("bad duration unit")


def format_duration_for_message(duration: timedelta) -> str:
    total_seconds = int(duration.total_seconds())
    if total_seconds % 86400 == 0:
        days = total_seconds // 86400
        return f"{days} дн."
    if total_seconds % 3600 == 0:
        hours = total_seconds // 3600
        return f"{hours} ч."
    minutes = max(1, total_seconds // 60)
    return f"{minutes} мин."


def tournament_prizes(tournament: sqlite3.Row) -> list[str]:
    try:
        prizes = json.loads(tournament["prizes_json"])
    except json.JSONDecodeError:
        return []

    if not isinstance(prizes, list):
        return []

    return [str(prize) for prize in prizes]


def tournament_announcement_entities(tournament: sqlite3.Row) -> list[dict]:
    try:
        entities = json.loads(tournament["announcement_entities_json"])
    except json.JSONDecodeError:
        return []

    return entities if isinstance(entities, list) else []


def format_prize_lines(prizes: list[str], prize_places: int) -> str:
    lines = []
    for index in range(prize_places):
        prize = prizes[index] if index < len(prizes) else "приз не указан"
        lines.append(f"{index + 1} место: {prize}")
    return "\n".join(lines)


def format_tournament_ticket_rows(rows: list[sqlite3.Row], limit: int = 5) -> str:
    if not rows:
        return "Пока нет билетов."

    lines = []
    for index, row in enumerate(rows[:limit], start=1):
        lines.append(f"{index}. {get_display_name(row)} - {row['tickets']} билетов")
    return "\n".join(lines)


def get_tournament_total_tickets(rows: list[sqlite3.Row]) -> int:
    return sum(int(row["tickets"] or 0) for row in rows)


def build_tournament_message(
    db: StatsDatabase,
    tournament: sqlite3.Row,
    reminder: bool = False,
) -> tuple[str, list[dict]]:
    ticket_rows = db.get_tournament_ticket_rows(tournament)
    total_tickets = get_tournament_total_tickets(ticket_rows)
    prizes = tournament_prizes(tournament)
    prize_places = int(tournament["prize_places"])
    prize_lines = format_prize_lines(prizes, prize_places)
    top_lines = format_tournament_ticket_rows(ticket_rows)

    announcement_text = tournament["announcement_text"]
    announcement_entities = tournament_announcement_entities(tournament)
    values = {
        "tournament_id": str(tournament["tournament_id"]),
        "prize_places": str(prize_places),
        "prizes": prize_lines,
        "total_tickets": str(total_tickets),
        "top5": top_lines,
        "started_at": format_datetime_for_message(tournament["started_at"]),
        "ends_at": format_datetime_for_message(tournament["ends_at"]),
    }
    announcement_text, announcement_entities = apply_template_values(
        announcement_text,
        announcement_entities,
        values,
    )

    prefix = "Напоминание о турнире\n\n" if reminder else ""
    suffix = (
        "\n\n"
        "Условия турнира:\n"
        "1 прокрут = 1 билет.\n"
        "Считаются только прокруты после запуска турнира.\n"
        f"Призовых мест: {prize_places}\n"
        f"Старт: {format_datetime_for_message(tournament['started_at'])}\n"
        f"Финиш: {format_datetime_for_message(tournament['ends_at'])}\n\n"
        "Призы:\n"
        f"{prize_lines}\n\n"
        f"Всего билетов сейчас: {total_tickets}\n\n"
        "Топ по билетам:\n"
        f"{top_lines}\n\n"
        "Победители будут выбраны случайно по билетам: чем больше билетов, тем выше шанс."
    )

    return (
        prefix + announcement_text + suffix,
        shift_entities(announcement_entities, utf16_len(prefix)),
    )


def choose_tournament_winners(
    ticket_rows: list[sqlite3.Row],
    prize_places: int,
    prizes: list[str],
) -> list[dict]:
    candidates = [
        {"row": row, "tickets": int(row["tickets"] or 0)}
        for row in ticket_rows
        if int(row["tickets"] or 0) > 0
    ]
    winners = []

    for place in range(1, prize_places + 1):
        total_tickets = sum(candidate["tickets"] for candidate in candidates)
        if total_tickets <= 0:
            break

        winning_ticket = random.randint(1, total_tickets)
        cursor = 0
        winner_index = 0
        for index, candidate in enumerate(candidates):
            cursor += candidate["tickets"]
            if winning_ticket <= cursor:
                winner_index = index
                break

        winner = candidates.pop(winner_index)
        row = winner["row"]
        winners.append(
            {
                "place": place,
                "user_id": row["user_id"],
                "username": row["username"],
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "tickets": winner["tickets"],
                "prize": prizes[place - 1] if place - 1 < len(prizes) else "",
            }
        )

    return winners


def build_tournament_results_message(
    tournament: sqlite3.Row,
    winners: list[dict],
    total_tickets: int,
) -> str:
    lines = [
        "Турнир завершен",
        "",
        f"Всего билетов участвовало: {total_tickets}",
        "",
        "Победители:",
    ]

    if not winners:
        lines.append("Победителей нет: пока не было билетов.")
    else:
        for winner in winners:
            user_label = (
                f"@{winner['username']}" if winner.get("username")
                else " ".join(
                    part for part in [winner.get("first_name"), winner.get("last_name")]
                    if part
                ) or str(winner["user_id"])
            )
            prize = winner.get("prize") or "приз не указан"
            lines.append(
                f"{winner['place']} место: {user_label} - "
                f"{winner['tickets']} билетов\n"
                f"Приз: {prize}"
            )

    lines.extend(
        [
            "",
            "Каждый билет имел одинаковый шанс выиграть.",
            "Чем больше было билетов, тем выше был шанс попасть в призовые места.",
        ]
    )
    return "\n".join(lines)


def make_result_payload(
    winners: list[dict],
    total_count: int,
    candidates: list[dict] | None = None,
) -> dict:
    payload = {"winners": winners, "total_count": total_count}
    if candidates is not None:
        payload["candidates"] = candidates
    return payload


def parse_result_payload(payload_json: str) -> tuple[list[dict], int | None, list[dict]]:
    try:
        payload = json.loads(payload_json or "[]")
    except json.JSONDecodeError:
        return [], None, []

    if isinstance(payload, list):
        return payload, None, []

    if isinstance(payload, dict):
        winners = payload.get("winners", [])
        total_count = payload.get("total_count")
        candidates = payload.get("candidates", [])
        return (
            winners if isinstance(winners, list) else [],
            int(total_count) if isinstance(total_count, int) else None,
            candidates if isinstance(candidates, list) else [],
        )

    return [], None, []


def tournament_result_keyboard(tournament_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Опубликовать",
                    callback_data=f"tourresult:{tournament_id}:publish",
                ),
                InlineKeyboardButton(
                    "Перероллить",
                    callback_data=f"tourresult:{tournament_id}:reroll",
                ),
            ]
        ]
    )


def referral_result_keyboard(contest_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Подтвердить",
                    callback_data=f"refresult:{contest_id}:publish",
                ),
                InlineKeyboardButton(
                    "Перероллить",
                    callback_data=f"refresult:{contest_id}:reroll",
                ),
            ]
        ]
    )


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[REFERRAL_BUTTON_TEXT, ALL_CONTESTS_BUTTON_TEXT]],
        resize_keyboard=True,
    )


def referral_contest_prizes(contest: sqlite3.Row) -> list[str]:
    try:
        prizes = json.loads(contest["prizes_json"])
    except json.JSONDecodeError:
        return []

    if not isinstance(prizes, list):
        return []

    return [str(prize) for prize in prizes]


def referral_contest_entities(contest: sqlite3.Row) -> list[dict]:
    try:
        entities = json.loads(contest["announcement_entities_json"])
    except json.JSONDecodeError:
        return []

    return entities if isinstance(entities, list) else []


def format_referral_contest_finish_rule(contest: sqlite3.Row) -> str:
    if contest["max_participants"]:
        return f"до {contest['max_participants']} принятых участников"

    if contest["ends_at"]:
        return f"до {format_datetime_for_message(contest['ends_at'])}"

    return "до ручной остановки owner"


def format_referral_participant_rows(rows: list[sqlite3.Row], limit: int = 5) -> str:
    if not rows:
        return "Пока нет принятых участников."

    lines = []
    for index, row in enumerate(rows[:limit], start=1):
        lines.append(f"{index}. {get_display_name(row)}")
    return "\n".join(lines)


def build_referral_contest_message(
    db: StatsDatabase,
    contest: sqlite3.Row,
) -> tuple[str, list[dict]]:
    participants = db.get_referral_participant_rows(contest["contest_id"])
    participant_count = len(participants)
    prizes = referral_contest_prizes(contest)
    prize_places = int(contest["prize_places"])
    prize_lines = format_prize_lines(prizes, prize_places)
    participants_top = format_referral_participant_rows(participants)

    text = contest["announcement_text"]
    entities_data = referral_contest_entities(contest)
    values = {
        "contest_id": str(contest["contest_id"]),
        "participant_count": str(participant_count),
        "max_participants": str(contest["max_participants"] or ""),
        "prize_places": str(prize_places),
        "prizes": prize_lines,
        "top5": participants_top,
        "started_at": format_datetime_for_message(contest["started_at"]),
        "ends_at": format_datetime_for_message(contest["ends_at"]) if contest["ends_at"] else "",
    }
    text, entities_data = apply_template_values(text, entities_data, values)

    suffix = (
        "\n\n"
        "Условия referral-конкурса:\n"
        "1 принятая заявка = участие в конкурсе.\n"
        "Отправьте скрин/фото через кнопку «Рефералы», owner проверит заявку.\n"
        f"Завершение: {format_referral_contest_finish_rule(contest)}\n"
        f"Призовых мест: {prize_places}\n\n"
        "Призы:\n"
        f"{prize_lines}\n\n"
        f"Принятых участников: {participant_count}\n\n"
        "Участники:\n"
        f"{participants_top}"
    )
    return text + suffix, entities_data


def build_referral_contests_keyboard(contests: list[sqlite3.Row]) -> InlineKeyboardMarkup:
    rows = []
    for contest in contests:
        rows.append(
            [
                InlineKeyboardButton(
                    f"Referral #{contest['contest_id']}",
                    callback_data=f"refcontest:select:{contest['contest_id']}",
                )
            ]
        )
    return InlineKeyboardMarkup(rows)


def choose_referral_winners(
    participant_rows: list[sqlite3.Row],
    prize_places: int,
    prizes: list[str],
) -> list[dict]:
    winners = []
    selected_rows = random.sample(participant_rows, min(prize_places, len(participant_rows)))
    for place, row in enumerate(selected_rows, start=1):
        winners.append(
            {
                "place": place,
                "user_id": row["user_id"],
                "username": row["username"],
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "prize": prizes[place - 1] if place - 1 < len(prizes) else "",
            }
        )
    return winners


def build_referral_results_message(
    contest: sqlite3.Row,
    winners: list[dict],
    participant_count: int,
) -> str:
    lines = [
        f"Referral-конкурс #{contest['contest_id']} завершен",
        "",
        f"Принятых участников: {participant_count}",
        "",
        "Победители:",
    ]

    if not winners:
        lines.append("Победителей нет: не было принятых участников.")
    else:
        for winner in winners:
            user_label = (
                f"@{winner['username']}" if winner.get("username")
                else " ".join(
                    part for part in [winner.get("first_name"), winner.get("last_name")]
                    if part
                ) or str(winner["user_id"])
            )
            prize = winner.get("prize") or "приз не указан"
            lines.append(f"{winner['place']} место: {user_label}\nПриз: {prize}")

    return "\n".join(lines)


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


def count_custom_emoji_entities(entities_data: list[dict]) -> int:
    total = 0
    for entity in entities_data:
        entity_type = entity.get("type")
        if getattr(entity_type, "value", entity_type) == "custom_emoji":
            total += 1
    return total


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

        token_entities = value_entities.get(str(replacement["token"]), [])
        for entity in token_entities:
            adjusted_entity = dict(entity)
            adjusted_entity["offset"] = int(entity["offset"]) + rendered_utf16_cursor
            inserted_entities.append(adjusted_entity)

        if str(replacement["token"]) in AUTO_BOLD_PLACEHOLDERS and replacement_text:
            inserted_entities.extend(
                bold_entities_excluding(
                    rendered_utf16_cursor,
                    replacement_text,
                    token_entities,
                )
            )

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
    extra_values: dict[str, str] | None = None,
) -> tuple[str, list[dict], dict[str, str] | None, str | None] | None:
    template = db.get_message_template(template_key)
    if not template:
        return None

    text, entities_data = template
    if owner_gift is None and ("nft_url" in text or "gift_title" in text):
        owner_gift = await choose_owner_gift_from_api(config)

    total_spins = stats_value(stats, "total_spins") if stats else 0
    rank_cards = db.get_rank_cards()
    user_rank_card = db.get_user_rank_card(user.id)
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
        **rank_values(total_spins, rank_cards, user_rank_card),
    }
    if extra_values:
        values.update(extra_values)
    rendered_text, rendered_entities = apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(total_spins, rank_cards, user_rank_card),
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


async def build_jackpot_progress_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "jackpot_progress",
        user,
        "jackpot",
        stats=stats,
        extra_values={
            "progress_count": "1",
            "remaining": "1",
            "needed": "2",
        },
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    return (
        f"{get_user_display_name(user)} выбил первый 777.\n\n"
        "Для gift нужно выбить 777 еще 1 раз."
    ), []


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


async def build_three_of_kind_progress_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    result: str,
    progress_count: int,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    remaining = 3 - progress_count
    rendered = await render_saved_template(
        config,
        db,
        "three_of_kind_progress",
        user,
        result,
        stats=stats,
        extra_values={
            "progress_count": str(progress_count),
            "remaining": str(remaining),
            "needed": "3",
        },
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    suffix = "раза" if remaining == 2 else "раз"
    return (
        f"{get_user_display_name(user)} выбил {COMBINATION_TITLES[result]}.\n\n"
        f"До giftr осталось выбить три в ряд еще {remaining} {suffix}."
    ), []


def build_default_chat_stats_message(
    totals: sqlite3.Row,
    top_rows: list[sqlite3.Row],
    db: StatsDatabase,
    rank_cards: list[dict],
    luckiest_by_jackpots: sqlite3.Row | None,
    luckiest_by_ratio: sqlite3.Row | None,
) -> tuple[str, list[dict]]:
    top5_text, top5_entities = format_top_spin_rows_with_entities(top_rows, db, rank_cards)
    prefix = (
        "Статистика слотов\n\n"
        "Общая статистика:\n"
        f"Всего спинов: {totals['total_spins']}\n"
        f"777: {totals['jackpots']}\n"
        f"77X: {totals['two_sevens']}\n"
        f"Три BAR: {totals['three_bars']}\n"
        f"Три винограда: {totals['three_grapes']}\n"
        f"Три лимона: {totals['three_lemons']}\n\n"
        "Топ 5 по спинам:\n"
    )
    suffix = (
        "\n\n"
        "Самый везучий по количеству 777:\n"
        f"{format_luckiest_by_jackpots(luckiest_by_jackpots)}\n\n"
        "Самый везучий по отношению 777 к спинам:\n"
        f"{format_luckiest_by_ratio(luckiest_by_ratio)}"
    )
    return prefix + top5_text + suffix, shift_entities(top5_entities, utf16_len(prefix))


def build_chat_stats_message(
    db: StatsDatabase,
    chat_label: str,
    totals: sqlite3.Row,
    top_rows: list[sqlite3.Row],
    luckiest_by_jackpots: sqlite3.Row | None,
    luckiest_by_ratio: sqlite3.Row | None,
) -> tuple[str, list[dict]]:
    rank_cards = db.get_rank_cards()
    top5_text, top5_entities = format_top_spin_rows_with_entities(top_rows, db, rank_cards)
    template = db.get_message_template("stats")
    if not template:
        return build_default_chat_stats_message(
            totals,
            top_rows,
            db,
            rank_cards,
            luckiest_by_jackpots,
            luckiest_by_ratio,
        )

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
        "top5": top5_text,
        "luckiest_777": format_luckiest_by_jackpots(luckiest_by_jackpots),
        "luckiest_ratio": format_luckiest_by_ratio(luckiest_by_ratio),
    }
    return apply_template_values(text, entities_data, values, {"top5": top5_entities})


def build_default_personal_stats_message(
    user: User,
    stats: sqlite3.Row | dict[str, int],
    balance: int,
    rank_text: str,
) -> str:
    return (
        f"Личная статистика {get_user_display_name(user)}\n\n"
        f"Всего спинов: {stats_value(stats, 'total_spins')}\n"
        f"Ранг: {rank_text}\n"
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
    user_rank_card = db.get_user_rank_card(user.id)
    balance = db.get_tem_balance(user.id)
    if not template:
        rank_text = get_effective_rank_card(
            stats_value(stats, "total_spins"),
            rank_cards,
            user_rank_card,
        )["text"]
        return build_default_personal_stats_message(user, stats, balance, rank_text), []

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
        **rank_values(stats_value(stats, "total_spins"), rank_cards, user_rank_card),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(stats_value(stats, "total_spins"), rank_cards, user_rank_card),
    )


def build_default_milestone_message(
    user: User,
    stats: sqlite3.Row | dict[str, int],
    balance: int,
    rank_text: str,
) -> str:
    total_spins = stats_value(stats, "total_spins")
    return (
        f"{get_user_display_name(user)} достиг {total_spins} спинов.\n\n"
        f"Ранг: {rank_text}\n"
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
    user_rank_card = db.get_user_rank_card(user.id)
    balance = db.get_tem_balance(user.id)
    if not template:
        rank_text = get_effective_rank_card(
            stats_value(stats, "total_spins"),
            rank_cards,
            user_rank_card,
        )["text"]
        return build_default_milestone_message(user, stats, balance, rank_text), []

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
        **rank_values(stats_value(stats, "total_spins"), rank_cards, user_rank_card),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(stats_value(stats, "total_spins"), rank_cards, user_rank_card),
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
    template = db.get_message_template("daily_bonus_wait")
    if template:
        text, entities_data = template
        values = {
            "username": get_user_display_name(user),
            "balance": str(balance),
            "tem_balance": str(balance),
        }
        return apply_template_values(text, entities_data, values)

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
    user_rank_card = db.get_user_rank_card(user.id)
    text, entities_data = template
    values = {
        "username": get_user_display_name(user),
        "total_spins": str(total_spins),
        "balance": str(balance),
        "tem_balance": str(balance),
        **rank_values(total_spins, rank_cards, user_rank_card),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(total_spins, rank_cards, user_rank_card),
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
        await asyncio.sleep(4 * 3600)

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


async def finish_tournament_from_application(
    application: Application,
    db: StatsDatabase,
    tournament: sqlite3.Row,
) -> None:
    ticket_rows = db.get_tournament_ticket_rows(tournament)
    prizes = tournament_prizes(tournament)
    winners = choose_tournament_winners(
        ticket_rows,
        int(tournament["prize_places"]),
        prizes,
    )
    total_tickets = get_tournament_total_tickets(ticket_rows)
    finished_at = datetime_to_storage(utc_now())
    result_payload = make_result_payload(winners, total_tickets, ticket_rows)
    db.set_tournament_pending_approval(
        tournament["tournament_id"],
        result_payload,
        finished_at,
    )

    message = (
        f"Черновик итогов слот-турнира #{tournament['tournament_id']}\n\n"
        "Бот не опубликует победителей без твоего подтверждения.\n"
        "Можно нажать «Опубликовать», «Перероллить» или вручную задать победителей:\n"
        f"/tournament winners {tournament['tournament_id']} @username1 @username2\n\n"
        f"{build_tournament_results_message(tournament, winners, total_tickets)}"
    )
    await application.bot.send_message(
        chat_id=tournament["owner_user_id"],
        text=message,
        reply_markup=tournament_result_keyboard(tournament["tournament_id"]),
    )


async def finish_referral_contest_from_application(
    application: Application,
    db: StatsDatabase,
    contest: sqlite3.Row,
) -> None:
    participant_rows = db.get_referral_participant_rows(contest["contest_id"])
    prizes = referral_contest_prizes(contest)
    winners = choose_referral_winners(
        participant_rows,
        int(contest["prize_places"]),
        prizes,
    )
    finished_at = datetime_to_storage(utc_now())
    participant_candidates = referral_candidate_dicts(participant_rows)
    result_payload = make_result_payload(winners, len(participant_rows), participant_candidates)
    db.set_referral_contest_pending_approval(
        contest["contest_id"],
        result_payload,
        finished_at,
    )

    message = (
        f"Черновик итогов referral-конкурса #{contest['contest_id']}\n\n"
        "Бот не уведомит победителей без твоего подтверждения.\n"
        "Можно нажать «Подтвердить», «Перероллить» или вручную задать победителей:\n"
        f"/refcontest winners {contest['contest_id']} @username1 @username2\n\n"
        f"{build_referral_results_message(contest, winners, len(participant_rows))}"
    )
    owner_user_id = int(contest["owner_user_id"])
    await application.bot.send_message(
        chat_id=owner_user_id,
        text=message,
        reply_markup=referral_result_keyboard(contest["contest_id"]),
    )


async def publish_tournament_results_from_application(
    application: Application,
    db: StatsDatabase,
    tournament: sqlite3.Row,
) -> None:
    winners, total_tickets, _ = parse_result_payload(tournament["winners_json"])
    if total_tickets is None:
        total_tickets = get_tournament_total_tickets(db.get_tournament_ticket_rows(tournament))

    finished_at = tournament["finished_at"] or datetime_to_storage(utc_now())
    db.finish_tournament(
        tournament["tournament_id"],
        make_result_payload(winners, total_tickets),
        finished_at,
    )
    message = build_tournament_results_message(tournament, winners, total_tickets)
    await application.bot.send_message(chat_id=tournament["chat_id"], text=message)


async def publish_referral_results_from_application(
    application: Application,
    db: StatsDatabase,
    contest: sqlite3.Row,
) -> None:
    winners, participant_count, _ = parse_result_payload(contest["winners_json"])
    if participant_count is None:
        participant_count = db.count_referral_participants(contest["contest_id"])

    finished_at = contest["finished_at"] or datetime_to_storage(utc_now())
    db.finish_referral_contest(
        contest["contest_id"],
        make_result_payload(winners, participant_count),
        finished_at,
    )
    owner_message = build_referral_results_message(contest, winners, participant_count)
    await application.bot.send_message(chat_id=contest["owner_user_id"], text=owner_message)

    for winner in winners:
        prize = winner.get("prize") or "приз не указан"
        try:
            await application.bot.send_message(
                chat_id=winner["user_id"],
                text=(
                    f"Referral-конкурс #{contest['contest_id']} завершен.\n\n"
                    f"Ты занял {winner['place']} место.\n"
                    f"Приз: {prize}"
                ),
            )
        except TelegramError as error:
            logging.warning(
                "Failed to notify referral winner %s: %s",
                winner["user_id"],
                error,
            )


async def send_tournament_reminders_loop(application: Application) -> None:
    while True:
        await asyncio.sleep(TOURNAMENT_LOOP_SECONDS)

        db: StatsDatabase = application.bot_data["db"]
        now = utc_now()

        for tournament in db.get_active_tournaments():
            try:
                ends_at = datetime_from_storage(tournament["ends_at"])
                if now >= ends_at:
                    await finish_tournament_from_application(application, db, tournament)
                    continue

                last_reminder_text = tournament["last_reminder_at"] or tournament["started_at"]
                last_reminder_at = datetime_from_storage(last_reminder_text)
                if now - last_reminder_at < timedelta(seconds=TOURNAMENT_REMINDER_SECONDS):
                    continue

                message, entities_data = build_tournament_message(
                    db,
                    tournament,
                    reminder=True,
                )
                await send_application_text_with_entities(
                    application,
                    tournament["chat_id"],
                    message,
                    entities_data,
                )
                db.mark_tournament_reminded(
                    tournament["tournament_id"],
                    datetime_to_storage(now),
                )
            except TelegramError as error:
                logging.warning(
                    "Failed to process tournament %s: %s",
                    tournament["tournament_id"],
                    error,
                )

        for contest in db.get_active_referral_contests():
            try:
                should_finish = False
                if contest["ends_at"] and now >= datetime_from_storage(contest["ends_at"]):
                    should_finish = True
                if contest["max_participants"]:
                    participant_count = db.count_referral_participants(contest["contest_id"])
                    if participant_count >= int(contest["max_participants"]):
                        should_finish = True

                if should_finish:
                    await finish_referral_contest_from_application(application, db, contest)
            except TelegramError as error:
                logging.warning(
                    "Failed to process referral contest %s: %s",
                    contest["contest_id"],
                    error,
                )


async def on_startup(application: Application) -> None:
    application.bot_data["hourly_stats_task"] = application.create_task(
        send_hourly_stats_loop(application)
    )
    application.bot_data["daily_bonus_reminders_task"] = application.create_task(
        send_daily_bonus_reminders_loop(application)
    )
    application.bot_data["tournament_reminders_task"] = application.create_task(
        send_tournament_reminders_loop(application)
    )


async def on_shutdown(application: Application) -> None:
    for task_key in (
        "hourly_stats_task",
        "daily_bonus_reminders_task",
        "tournament_reminders_task",
    ):
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


async def owner_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    user_id = update.effective_user.id
    owner_ids = ", ".join(str(owner_id) for owner_id in sorted(config.owner_user_ids)) or "не заданы"
    status = "да" if is_owner(config, user_id) else "нет"

    await update.message.reply_text(
        f"Твой Telegram user ID: {user_id}\n"
        f"OWNER_USER_IDS распознаны ботом: {owner_ids}\n"
        f"Ты owner: {status}\n\n"
        "Если 'Ты owner: нет', скопируй свой user ID в OWNER_USER_IDS на Bothost и сделай redeploy."
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


def resolve_target_user_id_from_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[int | None, str | None]:
    if update.message and update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_user = update.message.reply_to_message.from_user
        return target_user.id, get_user_display_name(target_user)

    if context.args:
        try:
            target_user_id = int(context.args[0].lstrip("="))
        except ValueError:
            return None, None

        return target_user_id, str(target_user_id)

    return None, None


async def hide_user_from_rating(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            "Напишите username пользователя:\n/hiderating @username"
        )
        return

    db: StatsDatabase = context.application.bot_data["db"]
    user_row = db.get_user_by_username(context.args[0])
    if user_row is None:
        await update.message.reply_text(
            "Я пока не знаю такого username. Пользователь должен иметь username и хотя бы раз попасть в базу бота."
        )
        return

    user_label = get_display_name(user_row)
    db.exclude_user_from_rating(user_row["user_id"])
    await update.message.reply_text(f"Пользователь {user_label} скрыт из рейтинга.")


async def show_user_in_rating(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            "Напишите username пользователя:\n/showrating @username"
        )
        return

    db: StatsDatabase = context.application.bot_data["db"]
    user_row = db.get_user_by_username(context.args[0])
    if user_row is None:
        await update.message.reply_text(
            "Я пока не знаю такого username. Пользователь должен иметь username и хотя бы раз попасть в базу бота."
        )
        return

    user_label = get_display_name(user_row)
    db.include_user_in_rating(user_row["user_id"])
    await update.message.reply_text(f"Пользователь {user_label} возвращен в рейтинг.")


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


def extract_user_rank_card_from_update(update: Update) -> tuple[int, dict] | None:
    if not update.message or not update.message.text:
        return None

    parts = update.message.text.split(maxsplit=2)
    if len(parts) < 2:
        return None

    target_user_id: int | None = None
    rank_text = ""
    entities_data: list[dict] = []

    try:
        target_user_id = int(parts[1].lstrip("="))
    except ValueError:
        if update.message.reply_to_message and update.message.reply_to_message.from_user:
            target_user_id = update.message.reply_to_message.from_user.id
            rank_text = update.message.text.split(maxsplit=1)[1]
            start_char = update.message.text.find(rank_text)
            start_utf16 = char_index_to_utf16_offset(update.message.text, start_char)
            entities_data = serialize_entities(update.message.entities, start_utf16=start_utf16)

    if target_user_id is None:
        return None

    if len(parts) >= 3:
        rank_text = parts[2]
        start_char = update.message.text.find(rank_text)
        start_utf16 = char_index_to_utf16_offset(update.message.text, start_char)
        entities_data = serialize_entities(update.message.entities, start_utf16=start_utf16)
    elif update.message.reply_to_message:
        target_message = update.message.reply_to_message
        if target_message.text:
            rank_text = target_message.text
            entities_data = serialize_entities(target_message.entities)
        elif target_message.caption:
            rank_text = target_message.caption
            entities_data = serialize_entities(target_message.caption_entities)

    cards = build_rank_card_from_segment(rank_text, 0, len(rank_text), entities_data)
    if not cards:
        return None

    return target_user_id, cards[0]


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
            "/settext 777progress текст\n"
            "/settext 77x текст\n"
            "/settext triple текст\n"
            "/settext tripleprogress текст\n"
            "/settext stats текст\n"
            "/settext mystats текст\n\n"
            "/settext welcome текст\n\n"
            "/settext milestone текст\n\n"
            "/settext dailybonus текст\n\n"
            "/settext dailybonuswait текст\n\n"
            "/settext dailyreminder текст\n\n"
            "/settext chance текст\n\n"
            "Для длинного текста: отправьте сообщение-шаблон и ответьте на него /settext 777."
        )
        return

    template_key, template_text, entities_data = extracted
    db: StatsDatabase = context.application.bot_data["db"]
    db.set_message_template(template_key, template_text, entities_data)

    custom_emoji_count = count_custom_emoji_entities(entities_data)
    await update.message.reply_text(
        f"Шаблон для {TEMPLATE_LABELS[template_key]} сохранен.\n\n"
        f"Telegram custom emoji сохранено: {custom_emoji_count}\n\n"
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
        "progress_count - текущий прогресс до выдачи приза\n"
        "remaining - сколько осталось до выдачи приза\n"
        "needed - сколько всего нужно для выдачи приза\n"
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

    custom_emoji_count = count_custom_emoji_entities(entities_data)
    await update.message.reply_text(
        f"Help-сообщение сохранено.\n"
        f"Telegram custom emoji сохранено: {custom_emoji_count}"
    )


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

    custom_emoji_count = count_custom_emoji_entities(entities_data)
    await update.message.reply_text(
        f"Индивидуальный шаблон личной статистики для {target_user_id} сохранен.\n"
        f"Telegram custom emoji сохранено: {custom_emoji_count}"
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

    custom_emoji_count = sum(
        count_custom_emoji_entities(rank.get("entities", []))
        for rank in rank_cards
    )
    lines = ["Ранги сохранены:"]
    for index, rank in enumerate(rank_cards):
        lines.append(f"{index * 100}+ спинов: {rank['text']}")
    lines.append(f"Telegram custom emoji сохранено: {custom_emoji_count}")

    await update.message.reply_text("\n".join(lines))


async def set_user_rank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not is_owner(config, update.effective_user.id):
        return

    extracted = extract_user_rank_card_from_update(update)
    if not extracted:
        await update.message.reply_text(
            "Формат:\n"
            "/setuserrank USER_ID ранг\n\n"
            "Для Telegram custom emoji отправьте сообщение с рангом и ответьте на него /setuserrank USER_ID."
        )
        return

    target_user_id, rank_card = extracted
    db: StatsDatabase = context.application.bot_data["db"]
    db.set_user_rank_card(target_user_id, rank_card)

    custom_emoji_count = count_custom_emoji_entities(rank_card.get("entities", []))
    await update.message.reply_text(
        f"Индивидуальный ранг для {target_user_id} сохранен: {rank_card['text']}\n"
        f"Telegram custom emoji сохранено: {custom_emoji_count}"
    )


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
    user_rank_overrides = db.get_user_rank_overrides()

    lines = [
        "Сохраненные шаблоны:",
        "",
        "Команды установки:",
        "/settext 777",
        "/settext 777progress",
        "/settext 77x",
        "/settext triple",
        "/settext tripleprogress",
        "/settext stats",
        "/settext mystats",
        "/settext welcome",
        "/settext milestone",
        "/settext dailybonus",
        "/settext dailybonuswait",
        "/settext dailyreminder",
        "/settext chance",
        "/sethelp",
        "/setranks",
        "/setuserrank USER_ID",
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

    if user_rank_overrides:
        lines.append("")
        lines.append("Индивидуальные ранги:")
        for row in user_rank_overrides:
            user_name = get_display_name(row) if row["username"] or row["first_name"] else row["user_id"]
            lines.append(f"{user_name}: {row['rank_text']}")

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


def tournament_usage() -> str:
    return (
        "Команды турнира работают только в личке owner.\n\n"
        "Запуск:\n"
        "1. Отправьте боту текст объявления турнира.\n"
        "2. Ответьте на него командой:\n"
        "/tournament start CHAT_ID days7 3\n"
        "или /tournament start CHAT_ID hours12 3\n"
        "или /tournament start CHAT_ID minutes30 3\n"
        "https://t.me/nft/prize1\n"
        "https://t.me/nft/prize2\n"
        "https://t.me/nft/prize3\n\n"
        "Где days7/hours12/minutes30 - длительность, 3 - призовых места.\n\n"
        "Другие команды:\n"
        "/tournament status - активные турниры\n"
        "/tournament participants ID - список участников\n"
        "/tournament stop ID - отменить турнир\n"
        "/tournament edit ID time hours12 - изменить время\n"
        "/tournament edit ID gifts - изменить подарки, ссылки ниже\n"
        "/tournament edit ID places 3 - изменить число мест\n"
        "/tournament edit ID text - изменить текст ответом на новое сообщение\n\n"
        "После черновика итогов:\n"
        "/tournament winners ID @username1 @username2 - выбрать победителей вручную\n\n"
        "В тексте объявления можно использовать placeholders:\n"
        "total_tickets, top5, prizes, prize_places, started_at, ends_at"
    )


def extract_tournament_announcement(update: Update) -> tuple[str, list[dict]] | None:
    if not update.message or not update.message.reply_to_message:
        return None

    target_message = update.message.reply_to_message
    if target_message.text:
        return target_message.text, serialize_entities(target_message.entities)

    if target_message.caption:
        return target_message.caption, serialize_entities(target_message.caption_entities)

    return None


def parse_tournament_start_command(message_text: str) -> tuple[int, timedelta, int, list[str]]:
    lines = message_text.splitlines()
    first_line_parts = lines[0].split()
    args = first_line_parts[1:]
    if len(args) < 3 or args[0].lower() != "start":
        raise ValueError("bad format")

    chat_id = int(args[1].lstrip("="))
    if len(args) >= 4:
        duration = parse_duration_token(args[2], default_unit="days")
        prize_places = int(args[3])
        inline_prizes = args[4:]
    else:
        duration = timedelta(days=DEFAULT_TOURNAMENT_DAYS)
        prize_places = int(args[2])
        inline_prizes = []

    line_prizes = [line.strip() for line in lines[1:] if line.strip()]
    prizes = line_prizes or inline_prizes
    if duration.total_seconds() <= 0 or prize_places <= 0:
        raise ValueError("bad numbers")

    return chat_id, duration, prize_places, prizes


async def start_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]

    announcement = extract_tournament_announcement(update)
    if not announcement:
        await update.message.reply_text(
            "Сначала отправьте текст объявления турнира, затем ответьте на него командой запуска.\n\n"
            f"{tournament_usage()}"
        )
        return

    try:
        chat_id, duration, prize_places, prizes = parse_tournament_start_command(
            update.message.text or ""
        )
    except ValueError:
        await update.message.reply_text(tournament_usage())
        return

    if not is_allowed_chat(config, chat_id):
        await update.message.reply_text(
            "Этот чат не указан в ALLOWED_CHAT_IDS. Турнир можно запускать только в разрешенных чатах."
        )
        return

    if len(prizes) < prize_places:
        await update.message.reply_text(
            f"Нужно указать минимум {prize_places} призов/ссылок, по одному на каждое место."
        )
        return

    if db.get_active_tournament_for_chat(chat_id):
        await update.message.reply_text(
            "В этом чате уже есть активный турнир. Сначала остановите его через /tournament stop ID."
        )
        return

    announcement_text, announcement_entities = announcement
    started_at = utc_now()
    ends_at = started_at + duration
    tournament_id = db.create_tournament(
        chat_id=chat_id,
        owner_user_id=update.effective_user.id,
        prize_places=prize_places,
        prizes=prizes[:prize_places],
        announcement_text=announcement_text,
        announcement_entities=announcement_entities,
        started_at=datetime_to_storage(started_at),
        ends_at=datetime_to_storage(ends_at),
        baseline_spins=db.get_chat_spin_baseline(chat_id),
    )
    tournament = db.get_tournament(tournament_id)
    if tournament is None:
        await update.message.reply_text("Не получилось создать турнир в базе.")
        return

    message, entities_data = build_tournament_message(db, tournament)
    try:
        await send_text_with_entities(context, chat_id, message, entities_data)
    except TelegramError as error:
        db.cancel_tournament(tournament_id)
        await update.message.reply_text(
            f"Турнир создан, но бот не смог отправить сообщение в чат: {error}\n"
            "Я отменил этот турнир, чтобы он не завис в активных."
        )
        return

    custom_emoji_count = count_custom_emoji_entities(announcement_entities)
    await update.message.reply_text(
        f"Турнир #{tournament_id} запущен.\n"
        f"Чат: {chat_id}\n"
        f"Длительность: {format_duration_for_message(duration)}\n"
        f"Призовых мест: {prize_places}\n"
        f"Telegram custom emoji в объявлении сохранено: {custom_emoji_count}\n\n"
        "Бот будет напоминать о турнире каждые 8 часов и автоматически подведет итоги в конце."
    )


async def show_tournament_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    tournaments = db.get_active_tournaments()
    if not tournaments:
        await update.message.reply_text("Активных турниров сейчас нет.")
        return

    lines = ["Активные турниры:"]
    for tournament in tournaments:
        ticket_rows = db.get_tournament_ticket_rows(tournament)
        lines.extend(
            [
                "",
                f"#{tournament['tournament_id']}",
                f"Чат: {tournament['chat_id']}",
                f"Финиш: {format_datetime_for_message(tournament['ends_at'])}",
                f"Призовых мест: {tournament['prize_places']}",
                f"Билетов сейчас: {get_tournament_total_tickets(ticket_rows)}",
            ]
        )

    await update.message.reply_text("\n".join(lines))


async def show_tournament_participants(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or len(context.args) < 2:
        if update.message:
            await update.message.reply_text("Формат: /tournament participants ID")
        return

    try:
        tournament_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID турнира должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    tournament = db.get_tournament(tournament_id)
    if not tournament:
        await update.message.reply_text("Турнир с таким ID не найден.")
        return

    rows = db.get_tournament_ticket_rows(tournament)
    total_tickets = get_tournament_total_tickets(rows)
    lines = [
        f"Участники слот-турнира #{tournament_id}:",
        f"Всего участников: {len(rows)}",
        f"Всего билетов: {total_tickets}",
        "",
    ]
    if not rows:
        lines.append("Пока нет участников.")
    else:
        for index, row in enumerate(rows[:100], start=1):
            lines.append(f"{index}. {get_display_name(row)} - {row['tickets']} билетов")
        if len(rows) > 100:
            lines.append(f"...и еще {len(rows) - 100} участников.")

    await update.message.reply_text("\n".join(lines))


async def stop_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not context.args or len(context.args) < 2:
        if update.message:
            await update.message.reply_text("Формат: /tournament stop ID")
        return

    try:
        tournament_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID турнира должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    tournament = db.get_tournament(tournament_id)
    if not tournament or tournament["status"] != "active":
        await update.message.reply_text("Активный турнир с таким ID не найден.")
        return

    db.cancel_tournament(tournament_id)
    await update.message.reply_text(f"Турнир #{tournament_id} отменен.")


def extract_command_line_items(message_text: str, fallback_items: list[str]) -> list[str]:
    lines = message_text.splitlines()
    line_items = [line.strip() for line in lines[1:] if line.strip()]
    return line_items or [item.strip() for item in fallback_items if item.strip()]


async def edit_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or len(context.args) < 3:
        if update.message:
            await update.message.reply_text(
                "Формат:\n"
                "/tournament edit ID time hours12\n"
                "/tournament edit ID gifts\nссылки ниже\n"
                "/tournament edit ID places 3\n"
                "/tournament edit ID text - ответом на новое сообщение"
            )
        return

    try:
        tournament_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID турнира должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    tournament = db.get_tournament(tournament_id)
    if not tournament or tournament["status"] != "active":
        await update.message.reply_text("Активный турнир с таким ID не найден.")
        return

    field = context.args[2].lower()
    if field in {"time", "duration", "время"}:
        if len(context.args) < 4:
            await update.message.reply_text("Формат: /tournament edit ID time hours12")
            return
        try:
            duration = parse_duration_token(context.args[3], default_unit="days")
        except ValueError:
            await update.message.reply_text("Не понял время. Примеры: minutes30, hours12, days7.")
            return

        ends_at = datetime_to_storage(utc_now() + duration)
        db.update_tournament_ends_at(tournament_id, ends_at)
        await update.message.reply_text(
            f"Время турнира #{tournament_id} изменено.\n"
            f"Новый финиш: {format_datetime_for_message(ends_at)}"
        )
        return

    if field in {"gifts", "prizes", "подарки", "призы"}:
        prizes = extract_command_line_items(update.message.text or "", context.args[3:])
        if not prizes:
            await update.message.reply_text(
                "Укажите подарки ссылками после команды или строками ниже:\n"
                "/tournament edit ID gifts\n"
                "https://t.me/nft/prize1"
            )
            return

        db.update_tournament_prizes(tournament_id, len(prizes), prizes)
        await update.message.reply_text(
            f"Подарки турнира #{tournament_id} изменены.\n"
            f"Призовых мест теперь: {len(prizes)}"
        )
        return

    if field in {"places", "place", "места"}:
        if len(context.args) < 4:
            await update.message.reply_text("Формат: /tournament edit ID places 3")
            return
        try:
            prize_places = int(context.args[3])
        except ValueError:
            await update.message.reply_text("Количество мест должно быть числом.")
            return
        if prize_places <= 0:
            await update.message.reply_text("Количество мест должно быть больше 0.")
            return

        db.update_tournament_prize_places(tournament_id, prize_places)
        await update.message.reply_text(
            f"Количество призовых мест турнира #{tournament_id} изменено: {prize_places}."
        )
        return

    if field in {"text", "текст"}:
        announcement = extract_tournament_announcement(update)
        if not announcement:
            await update.message.reply_text(
                "Отправьте новый текст турнира и ответьте на него:\n"
                "/tournament edit ID text"
            )
            return

        announcement_text, announcement_entities = announcement
        db.update_tournament_text(tournament_id, announcement_text, announcement_entities)
        await update.message.reply_text(
            f"Текст турнира #{tournament_id} изменен.\n"
            f"Telegram custom emoji сохранено: {count_custom_emoji_entities(announcement_entities)}"
        )
        return

    await update.message.reply_text("Не понял, что редактировать. Доступно: time, gifts, places, text.")


def find_candidate_by_user_token(candidates: list[dict], token: str) -> dict | None:
    normalized = token.strip().lstrip("@").lower()
    if not normalized:
        return None

    for candidate in candidates:
        username = str(candidate.get("username") or "").lower()
        if username and username == normalized:
            return candidate

        if str(candidate.get("user_id")) == normalized:
            return candidate

    return None


def build_manual_winners_from_tokens(
    candidates: list[dict],
    tokens: list[str],
    prizes: list[str],
    max_places: int,
) -> list[dict]:
    winners = []
    used_user_ids = set()
    for token in tokens[:max_places]:
        candidate = find_candidate_by_user_token(candidates, token)
        if candidate is None:
            raise ValueError(f"Участник {token} не найден среди участников конкурса.")

        user_id = candidate["user_id"]
        if user_id in used_user_ids:
            continue

        place = len(winners) + 1
        winners.append(
            {
                "place": place,
                "user_id": user_id,
                "username": candidate.get("username"),
                "first_name": candidate.get("first_name"),
                "last_name": candidate.get("last_name"),
                "tickets": int(candidate.get("tickets", 1) or 1),
                "prize": prizes[place - 1] if place - 1 < len(prizes) else "",
            }
        )
        used_user_ids.add(user_id)

    if not winners:
        raise ValueError("Не получилось выбрать победителей.")

    return winners


def referral_candidate_dicts(rows: list[sqlite3.Row]) -> list[dict]:
    return [
        {
            "submission_id": row["submission_id"],
            "user_id": row["user_id"],
            "submitted_at": row["submitted_at"],
            "username": row["username"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
        }
        for row in rows
    ]


async def set_tournament_winners(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or len(context.args) < 3:
        if update.message:
            await update.message.reply_text(
                "Формат: /tournament winners ID @username1 @username2"
            )
        return

    try:
        tournament_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID турнира должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    tournament = db.get_tournament(tournament_id)
    if not tournament or tournament["status"] != "pending_approval":
        await update.message.reply_text(
            "Победителей можно менять только после того, как бот пришлет черновик итогов."
        )
        return

    _, total_tickets, candidates = parse_result_payload(tournament["winners_json"])
    if not candidates:
        candidates = db.get_tournament_ticket_rows(tournament)
    if total_tickets is None:
        total_tickets = get_tournament_total_tickets(candidates)

    try:
        winners = build_manual_winners_from_tokens(
            candidates,
            context.args[2:],
            tournament_prizes(tournament),
            int(tournament["prize_places"]),
        )
    except ValueError as error:
        await update.message.reply_text(str(error))
        return

    db.update_tournament_result_payload(
        tournament_id,
        make_result_payload(winners, total_tickets, candidates),
    )
    tournament = db.get_tournament(tournament_id)
    await update.message.reply_text(
        "Победители изменены. Проверь черновик и подтверди публикацию:"
    )
    await update.message.reply_text(
        build_tournament_results_message(tournament, winners, total_tickets),
        reply_markup=tournament_result_keyboard(tournament_id),
    )


async def handle_tournament_result_approval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, query.from_user.id):
        await query.answer()
        return

    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return

    _, tournament_id_text, action = parts
    tournament_id = int(tournament_id_text)
    db: StatsDatabase = context.application.bot_data["db"]
    tournament = db.get_tournament(tournament_id)
    if not tournament or tournament["status"] != "pending_approval":
        await query.answer("Итоги уже обработаны или турнир не найден.", show_alert=True)
        return

    if action == "publish":
        await publish_tournament_results_from_application(context.application, db, tournament)
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except TelegramError:
            pass
        await query.answer("Итоги опубликованы.", show_alert=True)
        await query.message.reply_text(f"Итоги турнира #{tournament_id} опубликованы в чат.")
        return

    if action == "reroll":
        _, total_tickets, candidates = parse_result_payload(tournament["winners_json"])
        if not candidates:
            candidates = db.get_tournament_ticket_rows(tournament)
        if total_tickets is None:
            total_tickets = get_tournament_total_tickets(candidates)
        winners = choose_tournament_winners(
            candidates,
            int(tournament["prize_places"]),
            tournament_prizes(tournament),
        )
        db.update_tournament_result_payload(
            tournament_id,
            make_result_payload(winners, total_tickets, candidates),
        )
        await query.answer("Победители перероллены.", show_alert=True)
        await query.message.reply_text(
            build_tournament_results_message(tournament, winners, total_tickets),
            reply_markup=tournament_result_keyboard(tournament_id),
        )
        return

    await query.answer()


async def manage_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(tournament_usage())
        return

    action = context.args[0].lower()
    if action == "start":
        await start_tournament(update, context)
        return

    if action in {"status", "list"}:
        await show_tournament_status(update, context)
        return

    if action in {"participants", "members", "users"}:
        await show_tournament_participants(update, context)
        return

    if action in {"stop", "cancel"}:
        await stop_tournament(update, context)
        return

    if action in {"edit", "change"}:
        await edit_tournament(update, context)
        return

    if action in {"winners", "winner"}:
        await set_tournament_winners(update, context)
        return

    await update.message.reply_text(tournament_usage())


def refcontest_usage() -> str:
    return (
        "Referral-конкурс запускается только в личке owner.\n\n"
        "Запуск по времени:\n"
        "1. Отправьте боту текст referral-конкурса.\n"
        "2. Ответьте на него:\n"
        "/refcontest start days7 3\n"
        "или /refcontest start hours12 3\n"
        "или /refcontest start minutes30 3\n"
        "https://t.me/nft/prize1\n"
        "https://t.me/nft/prize2\n"
        "https://t.me/nft/prize3\n\n"
        "Запуск по количеству принятых участников:\n"
        "/refcontest start people100 3\n"
        "https://t.me/nft/prize1\n"
        "https://t.me/nft/prize2\n"
        "https://t.me/nft/prize3\n\n"
        "В тексте можно использовать placeholders:\n"
        "participant_count, max_participants, prizes, prize_places, top5, started_at, ends_at\n\n"
        "Другие команды:\n"
        "/refcontest status\n"
        "/refcontest participants ID\n"
        "/refcontest stop ID\n"
        "/refcontest edit ID time hours12\n"
        "/refcontest edit ID time people100\n"
        "/refcontest edit ID gifts - подарки ссылками ниже\n"
        "/refcontest edit ID places 3\n"
        "/refcontest edit ID text - ответом на новое сообщение\n"
        "/refcontest winners ID @username1 @username2 - выбрать победителей вручную после черновика"
    )


def parse_referral_finish_token(token: str) -> tuple[timedelta | None, int | None]:
    normalized = token.strip().lower()
    if normalized.startswith("people"):
        return None, int(normalized.removeprefix("people"))
    if normalized.startswith("users"):
        return None, int(normalized.removeprefix("users"))
    if normalized.startswith("members"):
        return None, int(normalized.removeprefix("members"))
    if normalized.endswith("p"):
        return None, int(normalized[:-1])
    return parse_duration_token(normalized, default_unit="days"), None


def parse_refcontest_start_command(
    message_text: str,
) -> tuple[timedelta | None, int | None, int, list[str]]:
    lines = message_text.splitlines()
    first_line_parts = lines[0].split()
    args = first_line_parts[1:]
    if len(args) < 3 or args[0].lower() != "start":
        raise ValueError("bad format")

    duration, max_participants = parse_referral_finish_token(args[1])
    prize_places = int(args[2])
    inline_prizes = args[3:]
    line_prizes = [line.strip() for line in lines[1:] if line.strip()]
    prizes = line_prizes or inline_prizes
    if prize_places <= 0:
        raise ValueError("bad prize places")
    if duration is not None and duration.total_seconds() <= 0:
        raise ValueError("bad duration")
    if max_participants is not None and max_participants <= 0:
        raise ValueError("bad max participants")

    return duration, max_participants, prize_places, prizes


async def start_referral_contest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return

    announcement = extract_tournament_announcement(update)
    if not announcement:
        await update.message.reply_text(
            "Сначала отправьте текст referral-конкурса, затем ответьте на него командой запуска.\n\n"
            f"{refcontest_usage()}"
        )
        return

    try:
        duration, max_participants, prize_places, prizes = parse_refcontest_start_command(
            update.message.text or ""
        )
    except ValueError:
        await update.message.reply_text(refcontest_usage())
        return

    if len(prizes) < prize_places:
        await update.message.reply_text(
            f"Нужно указать минимум {prize_places} призов/ссылок, по одному на каждое место."
        )
        return

    db: StatsDatabase = context.application.bot_data["db"]
    announcement_text, announcement_entities = announcement
    started_at = utc_now()
    ends_at = datetime_to_storage(started_at + duration) if duration else None
    contest_id = db.create_referral_contest(
        owner_user_id=update.effective_user.id,
        prize_places=prize_places,
        prizes=prizes[:prize_places],
        announcement_text=announcement_text,
        announcement_entities=announcement_entities,
        started_at=datetime_to_storage(started_at),
        ends_at=ends_at,
        max_participants=max_participants,
    )

    custom_emoji_count = count_custom_emoji_entities(announcement_entities)
    finish_rule = (
        format_duration_for_message(duration) if duration
        else f"{max_participants} принятых участников"
    )
    await update.message.reply_text(
        f"Referral-конкурс #{contest_id} запущен.\n"
        f"Завершение: {finish_rule}\n"
        f"Призовых мест: {prize_places}\n"
        f"Telegram custom emoji в объявлении сохранено: {custom_emoji_count}\n\n"
        "Теперь пользователи увидят его по кнопке «Рефералы»."
    )


async def show_refcontest_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    contests = db.get_active_referral_contests()
    if not contests:
        await update.message.reply_text("Активных referral-конкурсов сейчас нет.")
        return

    lines = ["Активные referral-конкурсы:"]
    for contest in contests:
        participant_count = db.count_referral_participants(contest["contest_id"])
        lines.extend(
            [
                "",
                f"#{contest['contest_id']}",
                f"Завершение: {format_referral_contest_finish_rule(contest)}",
                f"Принято участников: {participant_count}",
                f"Призовых мест: {contest['prize_places']}",
            ]
        )

    await update.message.reply_text("\n".join(lines))


async def show_refcontest_participants(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or len(context.args) < 2:
        if update.message:
            await update.message.reply_text("Формат: /refcontest participants ID")
        return

    try:
        contest_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID конкурса должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    contest = db.get_referral_contest(contest_id)
    if not contest:
        await update.message.reply_text("Referral-конкурс с таким ID не найден.")
        return

    rows = db.get_referral_participant_rows(contest_id)
    lines = [
        f"Участники referral-конкурса #{contest_id}:",
        f"Принято участников: {len(rows)}",
        "",
    ]
    if not rows:
        lines.append("Пока нет принятых участников.")
    else:
        for index, row in enumerate(rows[:100], start=1):
            lines.append(f"{index}. {get_display_name(row)}")
        if len(rows) > 100:
            lines.append(f"...и еще {len(rows) - 100} участников.")

    await update.message.reply_text("\n".join(lines))


async def stop_refcontest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not context.args or len(context.args) < 2:
        if update.message:
            await update.message.reply_text("Формат: /refcontest stop ID")
        return

    try:
        contest_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID конкурса должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    contest = db.get_referral_contest(contest_id)
    if not contest or contest["status"] != "active":
        await update.message.reply_text("Активный referral-конкурс с таким ID не найден.")
        return

    db.cancel_referral_contest(contest_id)
    await update.message.reply_text(f"Referral-конкурс #{contest_id} отменен.")


async def edit_refcontest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or len(context.args) < 3:
        if update.message:
            await update.message.reply_text(
                "Формат:\n"
                "/refcontest edit ID time hours12\n"
                "/refcontest edit ID time people100\n"
                "/refcontest edit ID gifts\nссылки ниже\n"
                "/refcontest edit ID places 3\n"
                "/refcontest edit ID text - ответом на новое сообщение"
            )
        return

    try:
        contest_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID конкурса должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    contest = db.get_referral_contest(contest_id)
    if not contest or contest["status"] != "active":
        await update.message.reply_text("Активный referral-конкурс с таким ID не найден.")
        return

    field = context.args[2].lower()
    if field in {"time", "duration", "время"}:
        if len(context.args) < 4:
            await update.message.reply_text("Формат: /refcontest edit ID time hours12")
            return
        try:
            duration, max_participants = parse_referral_finish_token(context.args[3])
        except ValueError:
            await update.message.reply_text(
                "Не понял условие завершения. Примеры: minutes30, hours12, days7, people100."
            )
            return

        ends_at = datetime_to_storage(utc_now() + duration) if duration else None
        db.update_referral_contest_finish(contest_id, ends_at, max_participants)
        if ends_at:
            finish_text = format_datetime_for_message(ends_at)
        else:
            finish_text = f"{max_participants} принятых участников"
        await update.message.reply_text(
            f"Условие завершения referral-конкурса #{contest_id} изменено.\n"
            f"Новое завершение: {finish_text}"
        )
        return

    if field in {"gifts", "prizes", "подарки", "призы"}:
        prizes = extract_command_line_items(update.message.text or "", context.args[3:])
        if not prizes:
            await update.message.reply_text(
                "Укажите подарки ссылками после команды или строками ниже:\n"
                "/refcontest edit ID gifts\n"
                "https://t.me/nft/prize1"
            )
            return

        db.update_referral_contest_prizes(contest_id, len(prizes), prizes)
        await update.message.reply_text(
            f"Подарки referral-конкурса #{contest_id} изменены.\n"
            f"Призовых мест теперь: {len(prizes)}"
        )
        return

    if field in {"places", "place", "места"}:
        if len(context.args) < 4:
            await update.message.reply_text("Формат: /refcontest edit ID places 3")
            return
        try:
            prize_places = int(context.args[3])
        except ValueError:
            await update.message.reply_text("Количество мест должно быть числом.")
            return
        if prize_places <= 0:
            await update.message.reply_text("Количество мест должно быть больше 0.")
            return

        db.update_referral_contest_prize_places(contest_id, prize_places)
        await update.message.reply_text(
            f"Количество призовых мест referral-конкурса #{contest_id} изменено: {prize_places}."
        )
        return

    if field in {"text", "текст"}:
        announcement = extract_tournament_announcement(update)
        if not announcement:
            await update.message.reply_text(
                "Отправьте новый текст referral-конкурса и ответьте на него:\n"
                "/refcontest edit ID text"
            )
            return

        announcement_text, announcement_entities = announcement
        db.update_referral_contest_text(contest_id, announcement_text, announcement_entities)
        await update.message.reply_text(
            f"Текст referral-конкурса #{contest_id} изменен.\n"
            f"Telegram custom emoji сохранено: {count_custom_emoji_entities(announcement_entities)}"
        )
        return

    await update.message.reply_text("Не понял, что редактировать. Доступно: time, gifts, places, text.")


async def set_refcontest_winners(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or len(context.args) < 3:
        if update.message:
            await update.message.reply_text(
                "Формат: /refcontest winners ID @username1 @username2"
            )
        return

    try:
        contest_id = int(context.args[1].lstrip("="))
    except ValueError:
        await update.message.reply_text("ID конкурса должен быть числом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    contest = db.get_referral_contest(contest_id)
    if not contest or contest["status"] != "pending_approval":
        await update.message.reply_text(
            "Победителей можно менять только после того, как бот пришлет черновик итогов."
        )
        return

    _, participant_count, candidates = parse_result_payload(contest["winners_json"])
    if not candidates:
        candidates = referral_candidate_dicts(db.get_referral_participant_rows(contest_id))
    if participant_count is None:
        participant_count = len(candidates)

    try:
        winners = build_manual_winners_from_tokens(
            candidates,
            context.args[2:],
            referral_contest_prizes(contest),
            int(contest["prize_places"]),
        )
    except ValueError as error:
        await update.message.reply_text(str(error))
        return

    db.update_referral_result_payload(
        contest_id,
        make_result_payload(winners, participant_count, candidates),
    )
    contest = db.get_referral_contest(contest_id)
    await update.message.reply_text(
        "Победители изменены. Проверь черновик и подтверди:"
    )
    await update.message.reply_text(
        build_referral_results_message(contest, winners, participant_count),
        reply_markup=referral_result_keyboard(contest_id),
    )


async def handle_referral_result_approval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, query.from_user.id):
        await query.answer()
        return

    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return

    _, contest_id_text, action = parts
    contest_id = int(contest_id_text)
    db: StatsDatabase = context.application.bot_data["db"]
    contest = db.get_referral_contest(contest_id)
    if not contest or contest["status"] != "pending_approval":
        await query.answer("Итоги уже обработаны или конкурс не найден.", show_alert=True)
        return

    if action == "publish":
        await publish_referral_results_from_application(context.application, db, contest)
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except TelegramError:
            pass
        await query.answer("Итоги подтверждены.", show_alert=True)
        await query.message.reply_text(f"Итоги referral-конкурса #{contest_id} подтверждены.")
        return

    if action == "reroll":
        _, participant_count, candidates = parse_result_payload(contest["winners_json"])
        if not candidates:
            candidates = referral_candidate_dicts(db.get_referral_participant_rows(contest_id))
        if participant_count is None:
            participant_count = len(candidates)
        winners = choose_referral_winners(
            candidates,
            int(contest["prize_places"]),
            referral_contest_prizes(contest),
        )
        db.update_referral_result_payload(
            contest_id,
            make_result_payload(winners, participant_count, candidates),
        )
        await query.answer("Победители перероллены.", show_alert=True)
        await query.message.reply_text(
            build_referral_results_message(contest, winners, participant_count),
            reply_markup=referral_result_keyboard(contest_id),
        )
        return

    await query.answer()


async def manage_refcontest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(refcontest_usage())
        return

    action = context.args[0].lower()
    if action == "start":
        await start_referral_contest(update, context)
        return

    if action in {"status", "list"}:
        await show_refcontest_status(update, context)
        return

    if action in {"participants", "members", "users"}:
        await show_refcontest_participants(update, context)
        return

    if action in {"stop", "cancel"}:
        await stop_refcontest(update, context)
        return

    if action in {"edit", "change"}:
        await edit_refcontest(update, context)
        return

    if action in {"winners", "winner"}:
        await set_refcontest_winners(update, context)
        return

    await update.message.reply_text(refcontest_usage())


async def show_referral_contests(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not update.effective_chat:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    contests = db.get_active_referral_contests()
    if not contests:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Сейчас нет активных referral-конкурсов.",
            reply_markup=main_menu_keyboard(),
        )
        return

    lines = ["Referral-конкурсы:"]
    for contest in contests:
        participant_count = db.count_referral_participants(contest["contest_id"])
        lines.append(
            f"#{contest['contest_id']} - {participant_count} участников, "
            f"завершение: {format_referral_contest_finish_rule(contest)}"
        )

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="\n".join(lines),
        reply_markup=build_referral_contests_keyboard(contests),
    )


async def show_all_contests(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not update.effective_chat:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    spin_tournaments = db.get_active_tournaments()
    referral_contests = db.get_active_referral_contests()
    if not spin_tournaments and not referral_contests:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Сейчас нет активных конкурсов.",
            reply_markup=main_menu_keyboard(),
        )
        return

    lines = ["Все активные конкурсы:"]
    if spin_tournaments:
        lines.append("")
        lines.append("Слот-турниры:")
        for tournament in spin_tournaments:
            ticket_rows = db.get_tournament_ticket_rows(tournament)
            lines.append(
                f"#{tournament['tournament_id']} - "
                f"{get_tournament_total_tickets(ticket_rows)} билетов, "
                f"финиш {format_datetime_for_message(tournament['ends_at'])}"
            )

    if referral_contests:
        lines.append("")
        lines.append("Referral-конкурсы:")
        for contest in referral_contests:
            participant_count = db.count_referral_participants(contest["contest_id"])
            lines.append(
                f"#{contest['contest_id']} - {participant_count} участников, "
                f"завершение: {format_referral_contest_finish_rule(contest)}"
            )

    reply_markup = build_referral_contests_keyboard(referral_contests) if referral_contests else main_menu_keyboard()
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="\n".join(lines),
        reply_markup=reply_markup,
    )


async def handle_private_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.remember_private_subscriber(update.effective_user, update.effective_chat.id)

    text = (update.message.text or "").strip()
    if text == REFERRAL_BUTTON_TEXT:
        await show_referral_contests(update, context)
        return

    if text == ALL_CONTESTS_BUTTON_TEXT:
        await show_all_contests(update, context)
        return


async def handle_referral_contest_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    parts = query.data.split(":")
    if len(parts) != 3 or parts[1] != "select":
        await query.answer()
        return

    contest_id = int(parts[2])
    db: StatsDatabase = context.application.bot_data["db"]
    contest = db.get_referral_contest(contest_id)
    if not contest or contest["status"] != "active":
        await query.answer("Конкурс уже не активен.", show_alert=True)
        return

    existing = db.get_referral_submission_for_user(contest_id, query.from_user.id)
    if existing and existing["status"] == "accepted":
        await query.answer("Ты уже принят в этот конкурс.", show_alert=True)
        return
    if existing and existing["status"] == "pending":
        await query.answer("Твоя заявка уже на проверке.", show_alert=True)
        return

    context.user_data["referral_contest_id"] = contest_id
    message, entities_data = build_referral_contest_message(db, contest)
    await query.answer()
    await send_text_with_entities(context, query.message.chat_id, message, entities_data)
    await query.message.reply_text(
        "Теперь отправь сюда фото или скрин для проверки. Я передам его owner."
    )


async def notify_owners_about_referral_submission(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    contest: sqlite3.Row,
    submission: sqlite3.Row,
    file_id: str,
) -> None:
    if not update.effective_user:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    participant_count = db.count_referral_participants(contest["contest_id"])
    caption = (
        f"Заявка в referral-конкурс #{contest['contest_id']}\n\n"
        f"Пользователь: {get_user_display_name(update.effective_user)}\n"
        f"User ID: {update.effective_user.id}\n"
        f"Принято участников сейчас: {participant_count}\n"
        f"Завершение: {format_referral_contest_finish_rule(contest)}"
    )
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Принять",
                    callback_data=f"refreview:{contest['contest_id']}:{submission['submission_id']}:accept",
                ),
                InlineKeyboardButton(
                    "Отклонить",
                    callback_data=f"refreview:{contest['contest_id']}:{submission['submission_id']}:reject",
                ),
            ]
        ]
    )

    for owner_user_id in sorted(config.owner_user_ids):
        try:
            await context.bot.send_photo(
                chat_id=owner_user_id,
                photo=file_id,
                caption=caption,
                reply_markup=keyboard,
            )
        except TelegramError as error:
            logging.warning("Failed to send referral submission to owner %s: %s", owner_user_id, error)


async def handle_referral_photo_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        return

    contest_id = context.user_data.get("referral_contest_id")
    if not contest_id:
        await update.message.reply_text(
            "Сначала нажми «Рефералы» и выбери конкурс, куда отправляешь скрин.",
            reply_markup=main_menu_keyboard(),
        )
        return

    db: StatsDatabase = context.application.bot_data["db"]
    config: BotConfig = context.application.bot_data["config"]
    contest = db.get_referral_contest(int(contest_id))
    if not contest or contest["status"] != "active":
        context.user_data.pop("referral_contest_id", None)
        await update.message.reply_text(
            "Этот конкурс уже не активен.",
            reply_markup=main_menu_keyboard(),
        )
        return

    existing = db.get_referral_submission_for_user(contest["contest_id"], update.effective_user.id)
    if existing and existing["status"] == "accepted":
        await update.message.reply_text("Ты уже принят в этот конкурс.")
        return
    if existing and existing["status"] == "pending":
        await update.message.reply_text("Твоя заявка уже на проверке.")
        return

    db.remember_private_subscriber(update.effective_user, update.effective_chat.id)
    photo = update.message.photo[-1]
    submission = db.save_referral_submission(
        contest["contest_id"],
        update.effective_user.id,
        photo.file_id,
        datetime_to_storage(utc_now()),
    )
    await notify_owners_about_referral_submission(
        update,
        context,
        config,
        contest,
        submission,
        photo.file_id,
    )
    context.user_data.pop("referral_contest_id", None)
    await update.message.reply_text(
        "Заявка отправлена owner на проверку. Я напишу, когда ее примут или отклонят.",
        reply_markup=main_menu_keyboard(),
    )


async def review_referral_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_owner(config, query.from_user.id):
        await query.answer()
        return

    parts = query.data.split(":")
    if len(parts) != 4:
        await query.answer()
        return

    _, contest_id_text, submission_id_text, action = parts
    contest_id = int(contest_id_text)
    submission_id = int(submission_id_text)
    status = "accepted" if action == "accept" else "rejected"

    db: StatsDatabase = context.application.bot_data["db"]
    contest = db.get_referral_contest(contest_id)
    submission = db.get_referral_submission(submission_id)
    if not contest or not submission or submission["contest_id"] != contest_id:
        await query.answer("Заявка не найдена.", show_alert=True)
        return
    if contest["status"] != "active":
        await query.answer("Этот конкурс уже не активен.", show_alert=True)
        return

    changed = db.review_referral_submission(submission_id, status, datetime_to_storage(utc_now()))
    if not changed:
        await query.answer("Эта заявка уже обработана.", show_alert=True)
        return

    user_label = get_display_name(submission)
    if status == "accepted":
        await context.bot.send_message(
            chat_id=submission["user_id"],
            text=f"Твоя заявка в referral-конкурс #{contest_id} принята.",
        )
        owner_text = f"Заявка {user_label} принята."
    else:
        await context.bot.send_message(
            chat_id=submission["user_id"],
            text=f"Твоя заявка в referral-конкурс #{contest_id} отклонена.",
        )
        owner_text = f"Заявка {user_label} отклонена."

    try:
        await query.message.edit_reply_markup(reply_markup=None)
    except TelegramError:
        pass

    await query.answer(owner_text, show_alert=True)
    await query.message.reply_text(owner_text)

    if status == "accepted" and contest["status"] == "active" and contest["max_participants"]:
        participant_count = db.count_referral_participants(contest_id)
        if participant_count >= int(contest["max_participants"]):
            await finish_referral_contest_from_application(context.application, db, contest)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user:
        return

    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.remember_private_subscriber(update.effective_user, update.effective_chat.id)
    message, entities_data = build_welcome_message(db, update.effective_user)
    await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Выбери раздел:",
        reply_markup=main_menu_keyboard(),
    )


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
        jackpot_count = stats_value(user_stats, "jackpots") if user_stats else 0
        if jackpot_count % 2 == 0:
            message, entities_data, gift = await build_jackpot_message(
                config,
                db,
                update.effective_user,
                stats=user_stats,
            )
            await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
            await notify_owners_about_jackpot(update, context, config, gift)
        else:
            message, entities_data = await build_jackpot_progress_message(
                config,
                db,
                update.effective_user,
                stats=user_stats,
            )
            await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
    elif result == "two_sevens":
        message, entities_data = await build_two_sevens_message(
            config,
            db,
            update.effective_user,
            stats=user_stats,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
    elif result in {"three_bars", "three_grapes", "three_lemons"}:
        three_count = three_of_kind_total(user_stats) if user_stats else 0
        progress_count = three_count % 3
        if progress_count == 0:
            message, entities_data, gift = await build_three_of_kind_message(
                config,
                db,
                update.effective_user,
                result,
                stats=user_stats,
            )
            await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
            await notify_owners_about_small_gift(update, context, config, result, gift)
        else:
            message, entities_data = await build_three_of_kind_progress_message(
                config,
                db,
                update.effective_user,
                result,
                progress_count,
                stats=user_stats,
            )
            await send_text_with_entities(context, update.effective_chat.id, message, entities_data)

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
    application.add_handler(CommandHandler("ownercheck", owner_check))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("mystats", show_personal_stats))
    application.add_handler(CommandHandler("me", show_personal_stats))
    application.add_handler(CommandHandler("dailybonus", daily_bonus))
    application.add_handler(CommandHandler("help", show_help))
    application.add_handler(CommandHandler("resetstats", reset_stats))
    application.add_handler(CommandHandler("resetuserstats", reset_user_stats))
    application.add_handler(CommandHandler("hiderating", hide_user_from_rating))
    application.add_handler(CommandHandler("showrating", show_user_in_rating))
    application.add_handler(CommandHandler("emojiid", show_custom_emoji_ids))
    application.add_handler(CommandHandler("settext", set_message_template))
    application.add_handler(CommandHandler("sethelp", set_help_template))
    application.add_handler(CommandHandler("setusertext", set_user_message_template))
    application.add_handler(CommandHandler("setranks", set_ranks))
    application.add_handler(CommandHandler("setuserrank", set_user_rank))
    application.add_handler(CommandHandler("setchance", set_chance_settings))
    application.add_handler(CommandHandler("texts", show_message_templates))
    application.add_handler(CommandHandler("tournament", manage_tournament))
    application.add_handler(CommandHandler("refcontest", manage_refcontest))
    application.add_handler(CallbackQueryHandler(handle_tournament_result_approval, pattern="^tourresult:"))
    application.add_handler(CallbackQueryHandler(handle_referral_result_approval, pattern="^refresult:"))
    application.add_handler(CallbackQueryHandler(handle_referral_contest_choice, pattern="^refcontest:"))
    application.add_handler(CallbackQueryHandler(review_referral_submission, pattern="^refreview:"))
    application.add_handler(CallbackQueryHandler(handle_text_setting_choice, pattern="^textcfg:"))
    application.add_handler(CallbackQueryHandler(handle_stats_chat_choice, pattern="^mystats:"))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.PHOTO, handle_referral_photo_submission))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_private_menu_text))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_chat_members))
    application.add_handler(MessageHandler(filters.ALL, react_to_message))

    logging.info("Bot started. Waiting for messages...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
