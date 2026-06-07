from __future__ import annotations

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
TOURNAMENT_LOOP_SECONDS = 60
BLOCKED_GIFTS_SETTING = "blocked_owner_gifts"
EXTRA_NOTIFY_USERS_SETTING = "extra_notify_user_ids"
PAYOUT_CHAT_IDS_SETTING = "payout_chat_ids"
MILESTONE_USER_VALUES_SETTING = "milestone_user_values"
MILESTONE_CHAT_VALUES_SETTING = "milestone_chat_values"
SCAM_WARNING_INTERVAL_SETTING = "scam_warning_interval"
DEFAULT_MILESTONE_USER_VALUES = [25]
DEFAULT_MILESTONE_CHAT_VALUES = [100]
DEFAULT_SCAM_WARNING_INTERVAL = 50
MIN_STARS_WITHDRAW = 50
GAME_MODE_CLASSIC = "classic"
GAME_MODE_JACKPOT_BUTTONS = "jackpot_buttons"
GAME_MODE_SETTING = "game_mode"
SPIN_PRICE_STARS_SETTING = "spin_price_stars"
DEFAULT_SPIN_PRICE_STARS = 2
GAME_PRICE_PRESETS = (1, 2, 5, 10, 25)
RANK_POINT_STARS = 5
JACKPOT_BUTTONS_START_SETTING = "jackpot_buttons_start"
JACKPOT_BUTTONS_MIN_SETTING = "jackpot_buttons_min"
JACKPOT_BUTTONS_DECREASE_SETTING = "jackpot_buttons_decrease"
JACKPOT_BUTTON_STARS_MIN_PRICE_SETTING = "jackpot_button_stars_min_price"
JACKPOT_BUTTON_NFT_CHANCE_SETTING = "jackpot_button_nft_chance_denominator"
JACKPOT_BUTTONS_START = 9
JACKPOT_BUTTONS_MIN = 4
JACKPOT_BUTTONS_MISS_DECREASE = 1
DEFAULT_JACKPOT_BUTTON_STARS_MIN_PRICE = 5
DEFAULT_JACKPOT_BUTTON_NFT_CHANCE_DENOMINATOR = 0
JACKPOT_BUTTON_SMALL_PRIZES = [15, 25]
LUCK_MIN_SPINS_SETTING = "luck_min_spins"
DEFAULT_LUCK_MIN_SPINS = 0
TASK_SCOPES = {"all", "optin", "users"}
TASK_METRICS = {"spins", "777", "77x", "three", "nfts", "rank_points", "tem"}
TASK_COMPLETION_MODES = {"auto", "manual", "time", "people"}
TASK_DEFAULT_COMPLETION_MODE = "auto"
PAYOUT_STATUS_PENDING = "pending"
PAYOUT_STATUS_PAID = "paid"
PAYOUT_STATUS_DISPUTED = "disputed"
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
    "stars_balance",
    "stars",
    "amount",
    "withdraw_min",
    "withdraw_amount",
    "payout_id",
    "payout_type",
    "source_type",
    "prize",
    "prize_text",
    "rank",
    "rank_points",
    "spin_price",
    "rank_points_gain",
    "stars_box_min",
    "star_prize_min",
    "gift_title",
    "luckiest_777",
    "luckiest_ratio",
    "total_tickets",
    "prize_places",
    "button_count",
    "next_button_count",
    "selected_box",
    "small_prize",
    "button_prize",
    "nft_chance",
    "nft_chance_denominator",
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
    "777buttons": "jackpot_buttons",
    "777_buttons": "jackpot_buttons",
    "jackpotbuttons": "jackpot_buttons",
    "jackpot_buttons": "jackpot_buttons",
    "buttons": "jackpot_buttons",
    "box": "jackpot_buttons",
    "boxes": "jackpot_buttons",
    "кнопки": "jackpot_buttons",
    "777buttonsnft": "jackpot_buttons_nft",
    "777_buttons_nft": "jackpot_buttons_nft",
    "jackpotbuttonsnft": "jackpot_buttons_nft",
    "jackpot_buttons_nft": "jackpot_buttons_nft",
    "buttonsnft": "jackpot_buttons_nft",
    "buttons_nft": "jackpot_buttons_nft",
    "boxnft": "jackpot_buttons_nft",
    "777buttonsmiss": "jackpot_buttons_miss",
    "777_buttons_miss": "jackpot_buttons_miss",
    "jackpotbuttonsmiss": "jackpot_buttons_miss",
    "jackpot_buttons_miss": "jackpot_buttons_miss",
    "buttonsmiss": "jackpot_buttons_miss",
    "buttons_miss": "jackpot_buttons_miss",
    "boxmiss": "jackpot_buttons_miss",
    "777buttonsempty": "jackpot_buttons_empty_miss",
    "777_buttons_empty": "jackpot_buttons_empty_miss",
    "jackpotbuttonsempty": "jackpot_buttons_empty_miss",
    "jackpot_buttons_empty": "jackpot_buttons_empty_miss",
    "buttonsempty": "jackpot_buttons_empty_miss",
    "buttons_empty": "jackpot_buttons_empty_miss",
    "boxempty": "jackpot_buttons_empty_miss",
    "777buttonsnogift": "jackpot_buttons_no_gift",
    "777_buttons_no_gift": "jackpot_buttons_no_gift",
    "jackpot_buttons_no_gift": "jackpot_buttons_no_gift",
    "buttonsnogift": "jackpot_buttons_no_gift",
    "buttons_no_gift": "jackpot_buttons_no_gift",
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
    "triplenogift": "three_of_kind_no_gift",
    "three_nogift": "three_of_kind_no_gift",
    "three_no_gift": "three_of_kind_no_gift",
    "threeofkindnogift": "three_of_kind_no_gift",
    "three_of_kind_no_gift": "three_of_kind_no_gift",
    "ряд_без_giftr": "three_of_kind_no_gift",
    "без_giftr": "three_of_kind_no_gift",
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
    "scam": "scam_warning",
    "scam_warning": "scam_warning",
    "антискам": "scam_warning",
    "скам": "scam_warning",
    "starbalance": "star_balance",
    "star_balance": "star_balance",
    "starsbalance": "star_balance",
    "stars_balance": "star_balance",
    "балансзвезд": "star_balance",
    "звездыбаланс": "star_balance",
    "withdraw": "withdraw_request",
    "withdrawrequest": "withdraw_request",
    "withdraw_request": "withdraw_request",
    "вывод": "withdraw_request",
    "запросвывода": "withdraw_request",
    "payoutadmin": "payout_admin",
    "payout_admin": "payout_admin",
    "выплатаадмин": "payout_admin",
    "payoutuser": "payout_user",
    "payout_user": "payout_user",
    "выплатапользователь": "payout_user",
    "payoutdone": "payout_done",
    "payout_done": "payout_done",
    "выплатаготово": "payout_done",
    "payoutchat": "payout_chat",
    "payout_chat": "payout_chat",
    "выплатачат": "payout_chat",
    "чатвыплата": "payout_chat",
    "help": "help",
    "помощь": "help",
}

TEMPLATE_LABELS = {
    "jackpot": "777",
    "jackpot_progress": "первый 777 без gift",
    "jackpot_buttons": "777-кнопки старт",
    "jackpot_buttons_nft": "777-кнопки NFT",
    "jackpot_buttons_miss": "777-кнопки промах",
    "jackpot_buttons_empty_miss": "777-кнопки пустая коробка",
    "jackpot_buttons_no_gift": "777-кнопки нет gift",
    "two_sevens": "77X",
    "three_of_kind": "три в ряд",
    "three_of_kind_no_gift": "три в ряд без giftr",
    "three_of_kind_progress": "три в ряд без giftr",
    "stats": "общая статистика",
    "personal_stats": "личная статистика",
    "welcome": "приветствие",
    "milestone": "рубеж спинов",
    "daily_bonus": "ежедневный бонус",
    "daily_bonus_wait": "ежедневный бонус уже забран",
    "daily_reminder": "напоминание daily bonus",
    "chance_hint": "подсказка шанса",
    "scam_warning": "антискам напоминание",
    "star_balance": "Stars на баланс",
    "withdraw_request": "запрос вывода Stars",
    "payout_admin": "уведомление о выплате админам",
    "payout_user": "уведомление о выплате игроку",
    "payout_done": "выплата завершена",
    "payout_chat": "уведомление о NFT-выплате в чат",
    "help": "help",
}


@dataclass(frozen=True)
class BotConfig:
    token: str
    db_path: Path
    allowed_chat_ids: set[int]
    owner_user_ids: set[int]
    small_gifts: list[str]


@dataclass(frozen=True)
class UserIdentity:
    id: int
    username: str | None
    first_name: str
    last_name: str | None


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
                stars_balance INTEGER NOT NULL DEFAULT 0,
                rank_points REAL NOT NULL DEFAULT 0,
                last_daily_bonus_date TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS jackpot_button_progress (
                user_id INTEGER PRIMARY KEY,
                button_count INTEGER NOT NULL DEFAULT 9,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS jackpot_button_rounds (
                round_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                button_count INTEGER NOT NULL,
                nft_position INTEGER NOT NULL,
                nft_armed INTEGER NOT NULL DEFAULT 1,
                message_id INTEGER,
                selected_position INTEGER,
                small_prize_stars INTEGER,
                nft_title TEXT,
                nft_url TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                resolved_at TEXT
            );

            CREATE TABLE IF NOT EXISTS small_gift_progress (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                eligible_three_of_kind_spins INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS reward_tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                scope TEXT NOT NULL,
                metric TEXT NOT NULL,
                goal REAL NOT NULL,
                prize_type TEXT NOT NULL,
                prize_value TEXT,
                chat_id INTEGER,
                min_spins INTEGER NOT NULL DEFAULT 0,
                completion_mode TEXT NOT NULL DEFAULT 'auto',
                ends_at TEXT,
                max_completions INTEGER,
                status TEXT NOT NULL DEFAULT 'active',
                created_by INTEGER NOT NULL,
                completed_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS reward_task_allowed_users (
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY (task_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS reward_task_participants (
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                chat_id INTEGER,
                baseline_value REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT,
                payout_id INTEGER,
                PRIMARY KEY (task_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS payouts (
                payout_id INTEGER PRIMARY KEY AUTOINCREMENT,
                payout_type TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id INTEGER,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                prize_text TEXT NOT NULL,
                gift_title TEXT,
                gift_url TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                proof_file_id TEXT,
                dispute_text TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS payout_messages (
                payout_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY (payout_id, chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS star_prize_choices (
                round_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                stars INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                payout_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                resolved_at TEXT
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

        wallet_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(user_wallets)").fetchall()
        }
        if "rank_points" not in wallet_columns:
            self.connection.execute(
                "ALTER TABLE user_wallets ADD COLUMN rank_points REAL NOT NULL DEFAULT 0"
            )
            self.connection.execute(
                """
                INSERT INTO user_wallets (user_id, tem_balance, rank_points)
                SELECT user_id, 0, COALESCE(SUM(total_spins), 0)
                FROM slot_stats
                GROUP BY user_id
                ON CONFLICT(user_id) DO UPDATE SET
                    rank_points = CASE
                        WHEN user_wallets.rank_points = 0 THEN excluded.rank_points
                        ELSE user_wallets.rank_points
                    END
                """
            )
            self.connection.commit()

        wallet_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(user_wallets)").fetchall()
        }
        if "stars_balance" not in wallet_columns:
            self.connection.execute(
                "ALTER TABLE user_wallets ADD COLUMN stars_balance INTEGER NOT NULL DEFAULT 0"
            )
            self.connection.commit()

        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS jackpot_button_progress (
                user_id INTEGER PRIMARY KEY,
                button_count INTEGER NOT NULL DEFAULT 9,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS jackpot_button_rounds (
                round_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                button_count INTEGER NOT NULL,
                nft_position INTEGER NOT NULL,
                nft_armed INTEGER NOT NULL DEFAULT 1,
                message_id INTEGER,
                selected_position INTEGER,
                small_prize_stars INTEGER,
                nft_title TEXT,
                nft_url TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                resolved_at TEXT
            );

            CREATE TABLE IF NOT EXISTS small_gift_progress (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                eligible_three_of_kind_spins INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS reward_tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                scope TEXT NOT NULL,
                metric TEXT NOT NULL,
                goal REAL NOT NULL,
                prize_type TEXT NOT NULL,
                prize_value TEXT,
                chat_id INTEGER,
                min_spins INTEGER NOT NULL DEFAULT 0,
                completion_mode TEXT NOT NULL DEFAULT 'auto',
                ends_at TEXT,
                max_completions INTEGER,
                status TEXT NOT NULL DEFAULT 'active',
                created_by INTEGER NOT NULL,
                completed_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS reward_task_allowed_users (
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY (task_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS reward_task_participants (
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                chat_id INTEGER,
                baseline_value REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT,
                payout_id INTEGER,
                PRIMARY KEY (task_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS payouts (
                payout_id INTEGER PRIMARY KEY AUTOINCREMENT,
                payout_type TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_id INTEGER,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                prize_text TEXT NOT NULL,
                gift_title TEXT,
                gift_url TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                proof_file_id TEXT,
                dispute_text TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS payout_messages (
                payout_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY (payout_id, chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS star_prize_choices (
                round_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                stars INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                payout_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                resolved_at TEXT
            );
            """
        )
        self.connection.commit()

        progress_rows = self.connection.execute(
            "SELECT COUNT(*) AS count FROM small_gift_progress"
        ).fetchone()
        if int(progress_rows["count"] or 0) == 0:
            self.connection.execute(
                """
                INSERT INTO small_gift_progress (
                    chat_id,
                    user_id,
                    eligible_three_of_kind_spins
                )
                SELECT
                    chat_id,
                    user_id,
                    three_bars + three_grapes + three_lemons
                FROM slot_stats
                WHERE three_bars + three_grapes + three_lemons > 0
                """
            )
            self.connection.commit()

        button_round_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(jackpot_button_rounds)").fetchall()
        }
        if "message_id" not in button_round_columns:
            self.connection.execute(
                "ALTER TABLE jackpot_button_rounds ADD COLUMN message_id INTEGER"
            )
            self.connection.commit()
            button_round_columns.add("message_id")
        if "nft_armed" not in button_round_columns:
            self.connection.execute(
                "ALTER TABLE jackpot_button_rounds ADD COLUMN nft_armed INTEGER NOT NULL DEFAULT 1"
            )
            self.connection.commit()
            button_round_columns.add("nft_armed")

        task_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(reward_tasks)").fetchall()
        }
        if "min_spins" not in task_columns:
            self.connection.execute(
                "ALTER TABLE reward_tasks ADD COLUMN min_spins INTEGER NOT NULL DEFAULT 0"
            )
            self.connection.commit()
            task_columns.add("min_spins")

        task_migrations = {
            "completion_mode": "ALTER TABLE reward_tasks ADD COLUMN completion_mode TEXT NOT NULL DEFAULT 'auto'",
            "ends_at": "ALTER TABLE reward_tasks ADD COLUMN ends_at TEXT",
            "max_completions": "ALTER TABLE reward_tasks ADD COLUMN max_completions INTEGER",
            "completed_at": "ALTER TABLE reward_tasks ADD COLUMN completed_at TEXT",
        }
        for column_name, statement in task_migrations.items():
            if column_name not in task_columns:
                self.connection.execute(statement)
                self.connection.commit()
                task_columns.add(column_name)

        task_participant_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(reward_task_participants)").fetchall()
        }
        if "chat_id" not in task_participant_columns:
            self.connection.execute("ALTER TABLE reward_task_participants ADD COLUMN chat_id INTEGER")
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

    def get_user_by_id(self, user_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT user_id, username, first_name, last_name
            FROM users
            WHERE user_id = ?
            """,
            (user_id,),
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

    def get_private_chat_id(self, user_id: int) -> int | None:
        row = self.connection.execute(
            """
            SELECT chat_id
            FROM private_subscribers
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        return int(row["chat_id"]) if row else None

    def has_private_subscriber(self, user_id: int) -> bool:
        return self.get_private_chat_id(user_id) is not None

    def ensure_wallet(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO user_wallets (user_id, tem_balance, stars_balance, rank_points)
            VALUES (?, 0, 0, 0)
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

    def get_stars_balance(self, user_id: int) -> int:
        self.ensure_wallet(user_id)
        row = self.connection.execute(
            """
            SELECT stars_balance
            FROM user_wallets
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        return int(row["stars_balance"] or 0) if row else 0

    def add_stars_balance(self, user_id: int, amount: int) -> int:
        self.ensure_wallet(user_id)
        self.connection.execute(
            """
            UPDATE user_wallets
            SET stars_balance = stars_balance + ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """,
            (amount, user_id),
        )
        self.connection.commit()
        return self.get_stars_balance(user_id)

    def reserve_stars_withdraw(self, user_id: int, amount: int) -> bool:
        self.ensure_wallet(user_id)
        cursor = self.connection.execute(
            """
            UPDATE user_wallets
            SET stars_balance = stars_balance - ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND stars_balance >= ?
            """,
            (amount, user_id, amount),
        )
        self.connection.commit()
        return cursor.rowcount > 0

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

    def get_rank_points(self, user_id: int) -> float:
        self.ensure_wallet(user_id)
        row = self.connection.execute(
            """
            SELECT rank_points
            FROM user_wallets
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        return float(row["rank_points"] or 0) if row else 0.0

    def add_rank_points(self, user_id: int, points: float) -> float:
        self.ensure_wallet(user_id)
        self.connection.execute(
            """
            UPDATE user_wallets
            SET rank_points = rank_points + ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """,
            (points, user_id),
        )
        self.connection.commit()
        return self.get_rank_points(user_id)

    def get_jackpot_button_count(self, user_id: int) -> int:
        start_count = int(self.get_bot_setting(JACKPOT_BUTTONS_START_SETTING, JACKPOT_BUTTONS_START))
        min_count = int(self.get_bot_setting(JACKPOT_BUTTONS_MIN_SETTING, JACKPOT_BUTTONS_MIN))
        start_count = max(1, start_count)
        min_count = max(1, min(min_count, start_count))
        row = self.connection.execute(
            """
            SELECT button_count
            FROM jackpot_button_progress
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        if not row:
            return start_count

        return max(min_count, min(start_count, int(row["button_count"] or 0)))

    def set_jackpot_button_count(self, user_id: int, button_count: int) -> None:
        start_count = int(self.get_bot_setting(JACKPOT_BUTTONS_START_SETTING, JACKPOT_BUTTONS_START))
        min_count = int(self.get_bot_setting(JACKPOT_BUTTONS_MIN_SETTING, JACKPOT_BUTTONS_MIN))
        start_count = max(1, start_count)
        min_count = max(1, min(min_count, start_count))
        clamped_count = max(min_count, min(start_count, button_count))
        self.connection.execute(
            """
            INSERT INTO jackpot_button_progress (user_id, button_count)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                button_count = excluded.button_count,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, clamped_count),
        )
        self.connection.commit()

    def create_jackpot_button_round(
        self,
        chat_id: int,
        user_id: int,
        button_count: int,
        nft_position: int,
        nft_armed: bool,
        gift: dict[str, str] | None,
    ) -> int:
        self.connection.execute(
            """
            INSERT INTO jackpot_button_rounds (
                chat_id,
                user_id,
                button_count,
                nft_position,
                nft_armed,
                nft_title,
                nft_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chat_id,
                user_id,
                button_count,
                nft_position,
                1 if nft_armed else 0,
                gift["title"] if gift else "",
                gift["url"] if gift else "",
            ),
        )
        self.connection.commit()
        return int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    def get_jackpot_button_round(self, round_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM jackpot_button_rounds
            WHERE round_id = ?
            """,
            (round_id,),
        ).fetchone()

    def set_jackpot_button_round_message_id(self, round_id: int, message_id: int) -> None:
        self.connection.execute(
            """
            UPDATE jackpot_button_rounds
            SET message_id = ?
            WHERE round_id = ?
            """,
            (message_id, round_id),
        )
        self.connection.commit()

    def get_open_jackpot_button_round(self, chat_id: int, user_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM jackpot_button_rounds
            WHERE chat_id = ? AND user_id = ? AND status = 'open'
            ORDER BY round_id DESC
            LIMIT 1
            """,
            (chat_id, user_id),
        ).fetchone()

    def expire_open_jackpot_button_rounds(self, chat_id: int, user_id: int) -> sqlite3.Row | None:
        latest_open_round = self.get_open_jackpot_button_round(chat_id, user_id)
        if not latest_open_round:
            return None

        self.connection.execute(
            """
            UPDATE jackpot_button_rounds
            SET status = 'expired',
                resolved_at = CURRENT_TIMESTAMP
            WHERE chat_id = ? AND user_id = ? AND status = 'open'
            """,
            (chat_id, user_id),
        )
        self.connection.commit()
        new_count = max(
            int(self.get_bot_setting(JACKPOT_BUTTONS_MIN_SETTING, JACKPOT_BUTTONS_MIN)),
            int(latest_open_round["button_count"])
            - int(self.get_bot_setting(JACKPOT_BUTTONS_DECREASE_SETTING, JACKPOT_BUTTONS_MISS_DECREASE)),
        )
        self.set_jackpot_button_count(user_id, new_count)
        return latest_open_round

    def resolve_jackpot_button_round(
        self,
        round_id: int,
        selected_position: int,
        small_prize_stars: int | None,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE jackpot_button_rounds
            SET selected_position = ?,
                small_prize_stars = ?,
                status = 'resolved',
                resolved_at = CURRENT_TIMESTAMP
            WHERE round_id = ? AND status = 'open'
            """,
            (selected_position, small_prize_stars, round_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def record_small_gift_progress(self, chat_id: int, user_id: int) -> int:
        self.connection.execute(
            """
            INSERT INTO small_gift_progress (
                chat_id,
                user_id,
                eligible_three_of_kind_spins
            )
            VALUES (?, ?, 0)
            ON CONFLICT(chat_id, user_id) DO NOTHING
            """,
            (chat_id, user_id),
        )
        self.connection.execute(
            """
            UPDATE small_gift_progress
            SET eligible_three_of_kind_spins = eligible_three_of_kind_spins + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_id, user_id),
        )
        self.connection.commit()
        row = self.connection.execute(
            """
            SELECT eligible_three_of_kind_spins
            FROM small_gift_progress
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_id, user_id),
        ).fetchone()
        return int(row["eligible_three_of_kind_spins"] or 0) if row else 0

    def create_payout(
        self,
        payout_type: str,
        source_type: str,
        source_id: int | None,
        chat_id: int,
        user_id: int,
        prize_text: str,
        gift_title: str | None = None,
        gift_url: str | None = None,
    ) -> int:
        self.connection.execute(
            """
            INSERT INTO payouts (
                payout_type,
                source_type,
                source_id,
                chat_id,
                user_id,
                prize_text,
                gift_title,
                gift_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payout_type,
                source_type,
                source_id,
                chat_id,
                user_id,
                prize_text,
                gift_title,
                gift_url,
            ),
        )
        self.connection.commit()
        return int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    def get_payout(self, payout_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT
                p.*,
                u.username,
                u.first_name,
                u.last_name
            FROM payouts p
            JOIN users u ON u.user_id = p.user_id
            WHERE p.payout_id = ?
            """,
            (payout_id,),
        ).fetchone()

    def complete_payout(self, payout_id: int, proof_file_id: str | None = None) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE payouts
            SET status = ?,
                proof_file_id = COALESCE(?, proof_file_id),
                completed_at = CURRENT_TIMESTAMP
            WHERE payout_id = ? AND status IN (?, ?)
            """,
            (
                PAYOUT_STATUS_PAID,
                proof_file_id,
                payout_id,
                PAYOUT_STATUS_PENDING,
                PAYOUT_STATUS_DISPUTED,
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def open_payout_dispute(self, payout_id: int, user_id: int, dispute_text: str) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE payouts
            SET status = ?,
                dispute_text = ?,
                completed_at = NULL
            WHERE payout_id = ? AND user_id = ?
            """,
            (
                PAYOUT_STATUS_DISPUTED,
                dispute_text,
                payout_id,
                user_id,
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def save_payout_message(self, payout_id: int, chat_id: int, message_id: int) -> None:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO payout_messages (payout_id, chat_id, message_id)
            VALUES (?, ?, ?)
            """,
            (payout_id, chat_id, message_id),
        )
        self.connection.commit()

    def get_payout_messages(self, payout_id: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT chat_id, message_id
                FROM payout_messages
                WHERE payout_id = ?
                """,
                (payout_id,),
            )
        )

    def create_star_prize_choice(
        self,
        round_id: int,
        user_id: int,
        chat_id: int,
        stars: int,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO star_prize_choices (
                round_id,
                user_id,
                chat_id,
                stars,
                status
            )
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (round_id, user_id, chat_id, stars),
        )
        self.connection.commit()

    def get_star_prize_choice(self, round_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM star_prize_choices
            WHERE round_id = ?
            """,
            (round_id,),
        ).fetchone()

    def resolve_star_prize_choice(
        self,
        round_id: int,
        status: str,
        payout_id: int | None = None,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE star_prize_choices
            SET status = ?,
                payout_id = ?,
                resolved_at = CURRENT_TIMESTAMP
            WHERE round_id = ? AND status = 'pending'
            """,
            (status, payout_id, round_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def create_reward_task(
        self,
        title: str,
        scope: str,
        metric: str,
        goal: float,
        prize_type: str,
        prize_value: str,
        chat_id: int | None,
        created_by: int,
        min_spins: int = 0,
        completion_mode: str = TASK_DEFAULT_COMPLETION_MODE,
        ends_at: str | None = None,
        max_completions: int | None = None,
    ) -> int:
        if completion_mode not in TASK_COMPLETION_MODES:
            completion_mode = TASK_DEFAULT_COMPLETION_MODE

        self.connection.execute(
            """
            INSERT INTO reward_tasks (
                title,
                scope,
                metric,
                goal,
                prize_type,
                prize_value,
                chat_id,
                min_spins,
                completion_mode,
                ends_at,
                max_completions,
                created_by
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                scope,
                metric,
                goal,
                prize_type,
                prize_value,
                chat_id,
                max(0, int(min_spins)),
                completion_mode,
                ends_at,
                max_completions if max_completions and max_completions > 0 else None,
                created_by,
            ),
        )
        self.connection.commit()
        return int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    def add_reward_task_allowed_user(self, task_id: int, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO reward_task_allowed_users (task_id, user_id)
            VALUES (?, ?)
            """,
            (task_id, user_id),
        )
        self.connection.commit()

    def get_reward_task(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM reward_tasks
            WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()

    def get_active_reward_tasks_for_user(self, chat_id: int, user_id: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT DISTINCT t.*
                FROM reward_tasks t
                LEFT JOIN reward_task_allowed_users a
                    ON a.task_id = t.task_id AND a.user_id = ?
                WHERE t.status = 'active'
                  AND (t.chat_id IS NULL OR t.chat_id = ?)
                  AND (
                    t.scope IN ('all', 'optin')
                    OR (t.scope = 'users' AND a.user_id IS NOT NULL)
                  )
                ORDER BY t.task_id
                """,
                (user_id, chat_id),
            )
        )

    def get_visible_reward_tasks_for_user(self, user_id: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT DISTINCT t.*
                FROM reward_tasks t
                LEFT JOIN reward_task_allowed_users a
                    ON a.task_id = t.task_id AND a.user_id = ?
                WHERE t.status = 'active'
                  AND (
                    t.scope IN ('all', 'optin')
                    OR (t.scope = 'users' AND a.user_id IS NOT NULL)
                  )
                ORDER BY t.task_id
                """,
                (user_id,),
            )
        )

    def get_reward_tasks(self, include_inactive: bool = False) -> list[sqlite3.Row]:
        where = "" if include_inactive else "WHERE status = 'active'"
        return list(
            self.connection.execute(
                f"""
                SELECT *
                FROM reward_tasks
                {where}
                ORDER BY status, task_id DESC
                """
            )
        )

    def get_reward_tasks_by_status(self, status: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT *
                FROM reward_tasks
                WHERE status = ?
                ORDER BY task_id DESC
                """,
                (status,),
            )
        )

    def get_due_time_reward_tasks(self, now_text: str) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT *
                FROM reward_tasks
                WHERE status = 'active'
                  AND completion_mode = 'time'
                  AND ends_at IS NOT NULL
                  AND ends_at <= ?
                ORDER BY task_id
                """,
                (now_text,),
            )
        )

    def stop_reward_task(self, task_id: int) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE reward_tasks
            SET status = 'stopped'
            WHERE task_id = ? AND status = 'active'
            """,
            (task_id,),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def complete_reward_task(self, task_id: int) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE reward_tasks
            SET status = 'completed',
                completed_at = CURRENT_TIMESTAMP
            WHERE task_id = ? AND status = 'active'
            """,
            (task_id,),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def get_reward_task_participant(self, task_id: int, user_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM reward_task_participants
            WHERE task_id = ? AND user_id = ?
            """,
            (task_id, user_id),
        ).fetchone()

    def ensure_reward_task_participant(
        self,
        task_id: int,
        user_id: int,
        baseline_value: float,
        chat_id: int | None = None,
    ) -> sqlite3.Row:
        self.connection.execute(
            """
            INSERT INTO reward_task_participants (
                task_id,
                user_id,
                chat_id,
                baseline_value
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(task_id, user_id) DO NOTHING
            """,
            (task_id, user_id, chat_id, baseline_value),
        )
        self.connection.commit()
        row = self.get_reward_task_participant(task_id, user_id)
        if row is None:
            raise RuntimeError("Reward task participant was not created.")
        return row

    def mark_reward_task_participant_ready(self, task_id: int, user_id: int) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE reward_task_participants
            SET status = 'ready'
            WHERE task_id = ? AND user_id = ? AND status = 'active'
            """,
            (task_id, user_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def complete_reward_task_participant(
        self,
        task_id: int,
        user_id: int,
        payout_id: int,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE reward_task_participants
            SET status = 'completed',
                completed_at = CURRENT_TIMESTAMP,
                payout_id = ?
            WHERE task_id = ? AND user_id = ? AND status IN ('active', 'ready')
            """,
            (payout_id, task_id, user_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def count_completed_reward_task_participants(self, task_id: int) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM reward_task_participants
            WHERE task_id = ? AND status = 'completed'
            """,
            (task_id,),
        ).fetchone()
        return int(row["count"] or 0) if row else 0

    def get_reward_task_participant_rows(self, task_id: int) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    p.*,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM reward_task_participants p
                JOIN users u ON u.user_id = p.user_id
                WHERE p.task_id = ?
                ORDER BY
                    CASE p.status
                        WHEN 'ready' THEN 0
                        WHEN 'active' THEN 1
                        WHEN 'completed' THEN 2
                        ELSE 3
                    END,
                    p.started_at DESC
                """,
                (task_id,),
            )
        )

    def get_ready_reward_task_participants(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    p.*,
                    t.title,
                    t.prize_type,
                    t.prize_value,
                    t.status AS task_status,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM reward_task_participants p
                JOIN reward_tasks t ON t.task_id = p.task_id
                JOIN users u ON u.user_id = p.user_id
                WHERE p.status = 'ready'
                  AND t.status != 'stopped'
                ORDER BY p.started_at
                """
            )
        )

    def get_user_reward_task_rows(self, user_id: int, statuses: set[str] | None = None) -> list[sqlite3.Row]:
        status_filter = ""
        params: list[object] = [user_id]
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            status_filter = f"AND p.status IN ({placeholders})"
            params.extend(sorted(statuses))

        return list(
            self.connection.execute(
                f"""
                SELECT
                    t.*,
                    p.status AS participant_status,
                    p.baseline_value,
                    p.started_at,
                    p.completed_at AS participant_completed_at,
                    p.payout_id
                FROM reward_task_participants p
                JOIN reward_tasks t ON t.task_id = p.task_id
                WHERE p.user_id = ?
                  {status_filter}
                ORDER BY p.started_at DESC
                """,
                tuple(params),
            )
        )

    def get_box_nft_count(self, chat_id: int, user_id: int) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM jackpot_button_rounds
            WHERE chat_id = ?
              AND user_id = ?
              AND status = 'resolved'
              AND nft_armed = 1
              AND selected_position = nft_position
            """,
            (chat_id, user_id),
        ).fetchone()
        return int(row["count"] or 0) if row else 0

    def get_user_metric_value(self, chat_id: int, user_id: int, metric: str) -> float:
        if metric == "nfts":
            return float(self.get_box_nft_count(chat_id, user_id))
        if metric == "rank_points":
            return self.get_rank_points(user_id)
        if metric == "tem":
            return float(self.get_tem_balance(user_id))

        stats = self.get_user_stats(chat_id, user_id)
        if not stats:
            return 0.0
        if metric == "spins":
            return float(stats["total_spins"] or 0)
        if metric == "777":
            return float(stats["jackpots"] or 0)
        if metric == "77x":
            return float(stats["two_sevens"] or 0)
        if metric == "three":
            return float(three_of_kind_total(stats))
        return 0.0

    def get_luck_rows(
        self,
        chat_ids: set[int],
        metric: str,
        reverse: bool,
        min_total_spins: int = 0,
    ) -> sqlite3.Row | None:
        if not chat_ids:
            return None

        placeholders = ",".join("?" for _ in chat_ids)
        order_direction = "DESC" if reverse else "ASC"
        min_total_spins = max(0, int(min_total_spins))
        if metric == "777":
            return self.connection.execute(
                f"""
                SELECT
                    s.user_id,
                    COALESCE(SUM(s.total_spins), 0) AS attempts,
                    COALESCE(SUM(s.jackpots), 0) AS wins,
                    u.username,
                    u.first_name,
                    u.last_name,
                    (COALESCE(SUM(s.jackpots), 0) * 1.0 / COALESCE(SUM(s.total_spins), 0)) AS ratio
                FROM slot_stats s
                JOIN users u ON u.user_id = s.user_id
                LEFT JOIN rating_excluded_users e ON e.user_id = s.user_id
                WHERE s.chat_id IN ({placeholders})
                  AND e.user_id IS NULL
                GROUP BY s.user_id, u.username, u.first_name, u.last_name
                HAVING attempts > 0 AND attempts >= ?
                ORDER BY ratio {order_direction}, attempts DESC
                LIMIT 1
                """,
                tuple(sorted(chat_ids)) + (min_total_spins,),
            ).fetchone()

        return self.connection.execute(
            f"""
            WITH spin_totals AS (
                SELECT
                    user_id,
                    COALESCE(SUM(total_spins), 0) AS total_spins
                FROM slot_stats
                WHERE chat_id IN ({placeholders})
                GROUP BY user_id
            )
            SELECT
                r.user_id,
                COUNT(*) AS attempts,
                SUM(CASE WHEN r.nft_armed = 1 AND r.selected_position = r.nft_position THEN 1 ELSE 0 END) AS wins,
                st.total_spins AS total_spins,
                u.username,
                u.first_name,
                u.last_name,
                (SUM(CASE WHEN r.nft_armed = 1 AND r.selected_position = r.nft_position THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) AS ratio
            FROM jackpot_button_rounds r
            JOIN users u ON u.user_id = r.user_id
            JOIN spin_totals st ON st.user_id = r.user_id
            LEFT JOIN rating_excluded_users e ON e.user_id = r.user_id
            WHERE r.chat_id IN ({placeholders})
              AND r.status = 'resolved'
              AND e.user_id IS NULL
            GROUP BY r.user_id, st.total_spins, u.username, u.first_name, u.last_name
            HAVING attempts > 0 AND st.total_spins >= ?
            ORDER BY ratio {order_direction}, attempts DESC
            LIMIT 1
            """,
            tuple(sorted(chat_ids)) + tuple(sorted(chat_ids)) + (min_total_spins,),
        ).fetchone()

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

    def get_user_aggregate_stats(
        self,
        user_id: int,
        chat_ids: set[int],
    ) -> sqlite3.Row | None:
        if not chat_ids:
            return None

        placeholders = ",".join("?" for _ in chat_ids)
        return self.connection.execute(
            f"""
            SELECT
                s.user_id,
                COALESCE(SUM(s.total_spins), 0) AS total_spins,
                COALESCE(SUM(s.jackpots), 0) AS jackpots,
                COALESCE(SUM(s.two_sevens), 0) AS two_sevens,
                COALESCE(SUM(s.three_bars), 0) AS three_bars,
                COALESCE(SUM(s.three_grapes), 0) AS three_grapes,
                COALESCE(SUM(s.three_lemons), 0) AS three_lemons,
                COALESCE(SUM(s.other_spins), 0) AS other_spins,
                u.username,
                u.first_name,
                u.last_name
            FROM slot_stats s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.user_id = ? AND s.chat_id IN ({placeholders})
            GROUP BY s.user_id, u.username, u.first_name, u.last_name
            """,
            (user_id, *tuple(sorted(chat_ids))),
        ).fetchone()

    def get_full_user_stat_rows(self, chat_ids: set[int]) -> list[sqlite3.Row]:
        if not chat_ids:
            return []

        placeholders = ",".join("?" for _ in chat_ids)
        return list(
            self.connection.execute(
                f"""
                SELECT
                    s.user_id,
                    COALESCE(SUM(s.total_spins), 0) AS total_spins,
                    COALESCE(SUM(s.jackpots), 0) AS jackpots,
                    COALESCE(SUM(s.two_sevens), 0) AS two_sevens,
                    COALESCE(SUM(s.three_bars), 0) AS three_bars,
                    COALESCE(SUM(s.three_grapes), 0) AS three_grapes,
                    COALESCE(SUM(s.three_lemons), 0) AS three_lemons,
                    COALESCE(SUM(s.other_spins), 0) AS other_spins,
                    COALESCE(w.tem_balance, 0) AS tem_balance,
                    COALESCE(w.stars_balance, 0) AS stars_balance,
                    COALESCE(w.rank_points, 0) AS rank_points,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM slot_stats s
                JOIN users u ON u.user_id = s.user_id
                LEFT JOIN user_wallets w ON w.user_id = s.user_id
                WHERE s.chat_id IN ({placeholders})
                GROUP BY
                    s.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    w.tem_balance,
                    w.stars_balance,
                    w.rank_points
                HAVING COALESCE(SUM(s.total_spins), 0) > 0
                ORDER BY total_spins DESC, jackpots DESC, s.user_id
                """,
                tuple(sorted(chat_ids)),
            )
        )

    def reset_chat_stats(self, chat_id: int) -> None:
        self.connection.execute("DELETE FROM slot_stats WHERE chat_id = ?", (chat_id,))
        self.connection.execute("DELETE FROM small_gift_progress WHERE chat_id = ?", (chat_id,))
        self.connection.commit()

    def reset_user_stats(self, chat_id: int, user_id: int) -> None:
        self.connection.execute(
            "DELETE FROM slot_stats WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        self.connection.execute(
            "DELETE FROM small_gift_progress WHERE chat_id = ? AND user_id = ?",
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

    def get_open_tournaments(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT *
                FROM tournaments
                WHERE status IN ('active', 'pending_approval')
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

    def cancel_tournament(self, tournament_id: int) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE tournaments
            SET status = 'cancelled'
            WHERE tournament_id = ? AND status IN ('active', 'pending_approval')
            """,
            (tournament_id,),
        )
        self.connection.commit()
        return cursor.rowcount > 0

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


def has_owner_access(config: BotConfig, db: StatsDatabase, user_id: int | None) -> bool:
    if user_id is None:
        return False

    return is_owner(config, user_id) or user_id in get_extra_notify_user_ids(db)


def get_display_name(row: sqlite3.Row) -> str:
    if row["username"]:
        return f"@{row['username']}"

    name_parts = [row["first_name"], row["last_name"]]
    return " ".join(part for part in name_parts if part) or "Без имени"


def get_user_display_name(user: User | UserIdentity) -> str:
    if user.username:
        return f"@{user.username}"

    name_parts = [user.first_name, user.last_name]
    return " ".join(part for part in name_parts if part) or "игрок"


def user_identity_from_row(row: sqlite3.Row | dict) -> UserIdentity:
    return UserIdentity(
        id=int(row["user_id"]),
        username=row["username"],
        first_name=row["first_name"] or row["username"] or str(row["user_id"]),
        last_name=row["last_name"],
    )


def resolve_known_user(db: StatsDatabase, token: str) -> sqlite3.Row | None:
    cleaned = token.strip()
    if not cleaned:
        return None

    if cleaned.startswith("@"):
        return db.get_user_by_username(cleaned)

    if re.fullmatch(r"=?\d+", cleaned):
        return db.get_user_by_id(int(cleaned.lstrip("=")))

    return db.get_user_by_username(cleaned)


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


def get_game_mode(db: StatsDatabase) -> str:
    value = str(db.get_bot_setting(GAME_MODE_SETTING, GAME_MODE_CLASSIC))
    if value in {GAME_MODE_CLASSIC, GAME_MODE_JACKPOT_BUTTONS}:
        return value
    return GAME_MODE_CLASSIC


def get_spin_price_stars(db: StatsDatabase) -> int:
    value = db.get_bot_setting(SPIN_PRICE_STARS_SETTING, DEFAULT_SPIN_PRICE_STARS)
    try:
        price = int(value)
    except (TypeError, ValueError):
        return DEFAULT_SPIN_PRICE_STARS

    return max(1, price)


def save_spin_price_stars(db: StatsDatabase, price: int) -> None:
    if price < 1:
        raise ValueError("Цена прокрута должна быть 1⭐ или выше.")

    db.set_bot_setting(SPIN_PRICE_STARS_SETTING, price)


def get_jackpot_buttons_start(db: StatsDatabase) -> int:
    value = db.get_bot_setting(JACKPOT_BUTTONS_START_SETTING, JACKPOT_BUTTONS_START)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return JACKPOT_BUTTONS_START


def get_jackpot_buttons_min(db: StatsDatabase) -> int:
    value = db.get_bot_setting(JACKPOT_BUTTONS_MIN_SETTING, JACKPOT_BUTTONS_MIN)
    try:
        return max(1, min(int(value), get_jackpot_buttons_start(db)))
    except (TypeError, ValueError):
        return min(JACKPOT_BUTTONS_MIN, get_jackpot_buttons_start(db))


def get_jackpot_buttons_decrease(db: StatsDatabase) -> int:
    value = db.get_bot_setting(JACKPOT_BUTTONS_DECREASE_SETTING, JACKPOT_BUTTONS_MISS_DECREASE)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return JACKPOT_BUTTONS_MISS_DECREASE


def get_jackpot_button_stars_min_price(db: StatsDatabase) -> int:
    value = db.get_bot_setting(
        JACKPOT_BUTTON_STARS_MIN_PRICE_SETTING,
        DEFAULT_JACKPOT_BUTTON_STARS_MIN_PRICE,
    )
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return DEFAULT_JACKPOT_BUTTON_STARS_MIN_PRICE


def get_jackpot_button_nft_chance_denominator(
    db: StatsDatabase,
    button_count: int | None = None,
) -> int:
    fallback = max(1, int(button_count or get_jackpot_buttons_start(db)))
    value = db.get_bot_setting(
        JACKPOT_BUTTON_NFT_CHANCE_SETTING,
        DEFAULT_JACKPOT_BUTTON_NFT_CHANCE_DENOMINATOR,
    )
    try:
        denominator = int(value)
    except (TypeError, ValueError):
        return fallback

    if denominator <= 0:
        return fallback

    return max(fallback, denominator)


def should_arm_jackpot_button_nft(db: StatsDatabase, button_count: int) -> bool:
    denominator = get_jackpot_button_nft_chance_denominator(db, button_count)
    return random.random() < (max(1, button_count) / denominator)


def save_jackpot_button_settings(
    db: StatsDatabase,
    start_count: int | None = None,
    min_count: int | None = None,
    decrease: int | None = None,
    stars_min_price: int | None = None,
    nft_chance_denominator: int | None = None,
) -> None:
    if start_count is not None:
        if start_count < 1:
            raise ValueError("Количество кнопок должно быть 1 или больше.")
        db.set_bot_setting(JACKPOT_BUTTONS_START_SETTING, start_count)

    if min_count is not None:
        if min_count < 1:
            raise ValueError("Минимум кнопок должен быть 1 или больше.")
        db.set_bot_setting(
            JACKPOT_BUTTONS_MIN_SETTING,
            min(min_count, get_jackpot_buttons_start(db)),
        )

    if decrease is not None:
        if decrease < 1:
            raise ValueError("Уменьшение кнопок должно быть 1 или больше.")
        db.set_bot_setting(JACKPOT_BUTTONS_DECREASE_SETTING, decrease)

    if stars_min_price is not None:
        if stars_min_price < 1:
            raise ValueError("Порог Stars в коробках должен быть 1⭐ или больше.")
        db.set_bot_setting(JACKPOT_BUTTON_STARS_MIN_PRICE_SETTING, stars_min_price)

    if nft_chance_denominator is not None:
        if nft_chance_denominator < 1:
            raise ValueError("Шанс NFT задается как знаменатель 1/N, N должен быть 1 или больше.")
        db.set_bot_setting(JACKPOT_BUTTON_NFT_CHANCE_SETTING, nft_chance_denominator)


def get_rank_points_per_spin(db: StatsDatabase) -> float:
    return get_spin_price_stars(db) / RANK_POINT_STARS


def format_rank_points(points: float) -> str:
    if points == int(points):
        return str(int(points))
    return f"{points:.1f}".rstrip("0").rstrip(".")


def game_mode_label(mode: str) -> str:
    if mode == GAME_MODE_JACKPOT_BUTTONS:
        return "777-кнопки"
    return "классический"


def should_award_small_giftr(db: StatsDatabase) -> bool:
    return (
        get_game_mode(db) == GAME_MODE_CLASSIC
        and get_spin_price_stars(db) >= 5
    )


def should_award_jackpot_button_stars(db: StatsDatabase) -> bool:
    return get_spin_price_stars(db) >= get_jackpot_button_stars_min_price(db)


def get_luck_min_spins(db: StatsDatabase) -> int:
    value = db.get_bot_setting(LUCK_MIN_SPINS_SETTING, DEFAULT_LUCK_MIN_SPINS)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return DEFAULT_LUCK_MIN_SPINS


def save_luck_min_spins(db: StatsDatabase, min_spins: int) -> None:
    if min_spins < 0:
        raise ValueError("Минимум прокрутов не может быть отрицательным.")

    db.set_bot_setting(LUCK_MIN_SPINS_SETTING, int(min_spins))


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


def get_rank_card(rank_points: float, rank_cards: list[dict]) -> dict:
    rank_index = int(rank_points // 100)
    if rank_index < len(rank_cards):
        return rank_cards[rank_index]

    return rank_cards[-1]


def get_effective_rank_card(
    rank_points: float,
    rank_cards: list[dict],
    user_rank_card: dict | None = None,
) -> dict:
    return user_rank_card or get_rank_card(rank_points, rank_cards)


def get_rank_name(rank_points: float, rank_names: list[str]) -> str:
    rank_index = int(rank_points // 100)
    if rank_index < len(rank_names):
        return rank_names[rank_index]

    return rank_names[-1]


def rank_values(
    rank_points: float,
    rank_cards: list[dict],
    user_rank_card: dict | None = None,
) -> dict[str, str]:
    return {
        "rank": get_effective_rank_card(rank_points, rank_cards, user_rank_card)["text"],
        "rank_points": format_rank_points(rank_points),
    }


def rank_value_entities(
    rank_points: float,
    rank_cards: list[dict],
    user_rank_card: dict | None = None,
) -> dict[str, list[dict]]:
    return {
        "rank": get_effective_rank_card(rank_points, rank_cards, user_rank_card)["entities"]
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
        rank_points = db.get_rank_points(row["user_id"])
        rank_card = get_effective_rank_card(rank_points, rank_cards, user_rank_card)

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
        append_text_with_entities(pieces, entities, ", очки ранга: ")
        append_text_with_entities(pieces, entities, format_rank_points(rank_points), bold=True)

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


def parse_positive_int_list(value: object, default: list[int]) -> list[int]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = re.findall(r"\d+", value)
    else:
        return default

    parsed = []
    for item in items:
        try:
            number = int(item)
        except (TypeError, ValueError):
            continue
        if number > 0 and number not in parsed:
            parsed.append(number)

    return parsed or default


def get_user_milestone_values(db: StatsDatabase) -> list[int]:
    return parse_positive_int_list(
        db.get_bot_setting(MILESTONE_USER_VALUES_SETTING, DEFAULT_MILESTONE_USER_VALUES),
        DEFAULT_MILESTONE_USER_VALUES,
    )


def get_chat_milestone_values(db: StatsDatabase) -> list[int]:
    return parse_positive_int_list(
        db.get_bot_setting(MILESTONE_CHAT_VALUES_SETTING, DEFAULT_MILESTONE_CHAT_VALUES),
        DEFAULT_MILESTONE_CHAT_VALUES,
    )


def get_scam_warning_interval(db: StatsDatabase) -> int:
    value = db.get_bot_setting(SCAM_WARNING_INTERVAL_SETTING, DEFAULT_SCAM_WARNING_INTERVAL)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return DEFAULT_SCAM_WARNING_INTERVAL


def is_milestone_hit(total_spins: int, values: list[int]) -> bool:
    return total_spins > 0 and any(total_spins % value == 0 for value in values)


def is_spin_milestone(db: StatsDatabase, total_spins: int) -> bool:
    return is_milestone_hit(total_spins, get_user_milestone_values(db))


def is_chat_stats_milestone(db: StatsDatabase, total_spins: int) -> bool:
    return is_milestone_hit(total_spins, get_chat_milestone_values(db))


def is_scam_warning_milestone(db: StatsDatabase, total_spins: int) -> bool:
    return is_milestone_hit(total_spins, [get_scam_warning_interval(db)])


def utc_now() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def datetime_to_storage(value: datetime) -> str:
    return value.isoformat()


def datetime_from_storage(value: str) -> datetime:
    return datetime.fromisoformat(value)


def format_datetime_for_message(value: str) -> str:
    return datetime_from_storage(value).strftime("%d.%m.%Y %H:%M UTC")


def parse_integer_from_token(token: str, label: str) -> int:
    match = re.search(r"-?\d+", token)
    if not match:
        raise ValueError(f"не нашел число в поле: {label}")
    return int(match.group())


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


def build_tournament_tickets_text(tournament: sqlite3.Row, rows: list[dict]) -> str:
    total_tickets = get_tournament_total_tickets(rows)
    status_labels = {
        "active": "активен",
        "pending_approval": "ждет подтверждения итогов",
        "finished": "завершен",
        "cancelled": "отменен",
    }
    lines = [
        f"Билеты турнира #{tournament['tournament_id']}",
        f"Статус: {status_labels.get(tournament['status'], tournament['status'])}",
        f"Чат: {tournament['chat_id']}",
        f"Всего участников: {len(rows)}",
        f"Всего билетов: {total_tickets}",
        "",
    ]

    if not rows:
        lines.append("Пока нет билетов.")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        lines.append(
            f"{index}. {get_display_name(row)} - "
            f"{row['tickets']} билетов "
            f"(спинов всего: {row['total_spins']}, было до старта: {row['baseline_spins']})"
        )

    return "\n".join(lines)


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
    normalized = value.strip().lower()
    milestone_match = re.fullmatch(r"(?:milestone|mile|рубеж)(\d+)", normalized)
    if milestone_match:
        return f"milestone_{milestone_match.group(1)}"

    scam_match = re.fullmatch(r"(?:scam|scam_warning|антискам|скам)(\d+)", normalized)
    if scam_match:
        return f"scam_warning_{scam_match.group(1)}"

    return TEMPLATE_KEY_ALIASES.get(normalized)


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

    replacements.sort(
        key=lambda item: (
            int(item["start_char"]),
            -(int(item["end_char"]) - int(item["start_char"])),
        )
    )
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


def render_message_template(
    db: StatsDatabase,
    template_key: str,
    values: dict[str, str | int | float],
    value_entities: dict[str, list[dict]] | None = None,
) -> tuple[str, list[dict]] | None:
    template = db.get_message_template(template_key)
    if not template:
        return None

    text, entities_data = template
    string_values = {key: str(value) for key, value in values.items()}
    return apply_template_values(text, entities_data, string_values, value_entities)


def payout_template_values(
    user: User | UserIdentity | sqlite3.Row,
    payout_id: int | None,
    prize_text: str,
    payout_type: str,
    source_type: str,
    chat_id: int,
    extra_values: dict[str, str | int | float] | None = None,
) -> dict[str, str | int | float]:
    if isinstance(user, sqlite3.Row):
        username = get_display_name(user)
        user_id = int(user["user_id"])
    else:
        username = get_user_display_name(user)
        user_id = user.id

    values: dict[str, str | int | float] = {
        "username": username,
        "user_id": user_id,
        "chat_id": chat_id,
        "payout_id": payout_id or "",
        "payout_type": payout_type,
        "source_type": source_type,
        "prize": prize_text,
        "prize_text": prize_text,
    }
    if extra_values:
        values.update(extra_values)
    return values


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


def split_long_text(text: str, limit: int = 3900) -> list[str]:
    chunks = []
    current = ""
    for line in text.splitlines():
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
        current = line

    if current:
        chunks.append(current)

    return chunks or [""]


async def reply_long_text(message, text: str) -> None:
    for chunk in split_long_text(text):
        await message.reply_text(chunk)


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


def normalize_gift_token(value: str | None) -> str:
    if not value:
        return ""

    text = value.strip()
    parsed = urllib.parse.urlparse(text)
    if parsed.netloc.lower() in {"t.me", "telegram.me"} and parsed.path.startswith("/nft/"):
        text = urllib.parse.unquote(parsed.path.removeprefix("/nft/"))

    return " ".join(text.casefold().split())


def gift_card_tokens(gift_card: dict[str, str]) -> set[str]:
    return {
        token
        for token in (
            normalize_gift_token(gift_card.get("url")),
            normalize_gift_token(gift_card.get("title")),
            normalize_gift_token(gift_card.get("name")),
            normalize_gift_token(gift_card.get("base_name")),
            normalize_gift_token(gift_card.get("gift_id")),
        )
        if token
    }


def get_blocked_gift_tokens(db: StatsDatabase) -> list[str]:
    raw_tokens = db.get_bot_setting(BLOCKED_GIFTS_SETTING, [])
    if not isinstance(raw_tokens, list):
        return []

    tokens = []
    for token in raw_tokens:
        normalized = normalize_gift_token(str(token))
        if normalized and normalized not in tokens:
            tokens.append(normalized)
    return tokens


def set_blocked_gift_tokens(db: StatsDatabase, tokens: list[str]) -> None:
    normalized_tokens = []
    for token in tokens:
        normalized = normalize_gift_token(token)
        if normalized and normalized not in normalized_tokens:
            normalized_tokens.append(normalized)
    db.set_bot_setting(BLOCKED_GIFTS_SETTING, normalized_tokens)


def is_gift_blocked(gift_card: dict[str, str], blocked_tokens: set[str]) -> bool:
    return bool(gift_card_tokens(gift_card) & blocked_tokens)


def extract_gift_card(owned_gift: dict) -> dict[str, str] | None:
    gift = owned_gift.get("gift") or {}

    if owned_gift.get("type") == "unique":
        name = gift.get("name")
        base_name = gift.get("base_name") or name or "уникальный подарок"
        number = gift.get("number")
        title = f"{base_name} #{number}" if number else str(base_name)
        url = f"https://t.me/nft/{urllib.parse.quote(str(name), safe='')}" if name else ""
        return {
            "title": title,
            "url": url,
            "name": str(name or ""),
            "base_name": str(base_name or ""),
            "gift_id": str(gift.get("id") or ""),
        }

    sticker = gift.get("sticker") or {}
    emoji = sticker.get("emoji")
    gift_id = gift.get("id")
    title = f"обычный подарок {emoji}" if emoji else "обычный подарок"
    if gift_id:
        title = f"{title} ({gift_id})"

    return {
        "title": title,
        "url": "",
        "name": "",
        "base_name": "",
        "gift_id": str(gift_id or ""),
    }


def choose_owner_gift(
    owned_gifts: list[dict],
    blocked_tokens: list[str] | None = None,
) -> dict[str, str] | None:
    blocked_set = set(blocked_tokens or [])
    gift_cards = [card for gift in owned_gifts if (card := extract_gift_card(gift))]
    gift_cards = [card for card in gift_cards if not is_gift_blocked(card, blocked_set)]
    linked_gifts = [card for card in gift_cards if card["url"]]

    if linked_gifts:
        return random.choice(linked_gifts)

    if gift_cards:
        return random.choice(gift_cards)

    return None


async def choose_owner_gift_from_api(config: BotConfig, db: StatsDatabase) -> dict[str, str] | None:
    try:
        owned_gifts = await asyncio.to_thread(
            fetch_owner_gifts,
            config.token,
            config.owner_user_ids,
        )
    except RuntimeError as error:
        logging.warning("Failed to fetch owner gifts: %s", error)
        owned_gifts = []

    return choose_owner_gift(owned_gifts, get_blocked_gift_tokens(db))


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
        owner_gift = await choose_owner_gift_from_api(config, db)

    total_spins = stats_value(stats, "total_spins") if stats else 0
    rank_cards = db.get_rank_cards()
    user_rank_card = db.get_user_rank_card(user.id)
    balance = db.get_tem_balance(user.id)
    stars_balance = db.get_stars_balance(user.id)
    rank_points = db.get_rank_points(user.id)
    rank_points_gain = get_rank_points_per_spin(db)
    values = {
        "username": get_user_display_name(user),
        "nft_url": owner_gift["url"] if owner_gift and owner_gift["url"] else "",
        "gift_title": owner_gift["title"] if owner_gift else "",
        "giftr": small_gift or "",
        "combination": COMBINATION_TITLES.get(result, result),
        "total_spins": str(total_spins),
        "balance": str(balance),
        "tem_balance": str(balance),
        "stars_balance": str(stars_balance),
        "spin_price": str(get_spin_price_stars(db)),
        "rank_points_gain": format_rank_points(rank_points_gain),
        "stars_box_min": str(get_jackpot_button_stars_min_price(db)),
        "star_prize_min": str(get_jackpot_button_stars_min_price(db)),
        "nft_chance": f"1/{get_jackpot_button_nft_chance_denominator(db)}",
        "nft_chance_denominator": str(get_jackpot_button_nft_chance_denominator(db)),
        **rank_values(rank_points, rank_cards, user_rank_card),
    }
    if extra_values:
        values.update(extra_values)
    rendered_text, rendered_entities = apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(rank_points, rank_cards, user_rank_card),
    )
    return rendered_text, rendered_entities, owner_gift, small_gift


async def build_jackpot_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict], dict[str, str] | None]:
    player = get_user_display_name(user)
    gift = await choose_owner_gift_from_api(config, db)

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
        "Проверьте OWNER_USER_IDS, видимость подарков в профиле и список /giftblock."
    ), [], None


def jackpot_button_keyboard(round_id: int, button_count: int) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton("🎁", callback_data=f"jpbtn:{round_id}:{position}")
        for position in range(1, button_count + 1)
    ]
    rows = [buttons[index:index + 3] for index in range(0, len(buttons), 3)]
    return InlineKeyboardMarkup(rows)


def jackpot_button_reveal_keyboard(
    round_id: int,
    button_count: int,
    nft_position: int,
    nft_armed: bool,
    selected_position: int,
    selected_small_prize: int | None,
    show_small_prizes: bool = True,
) -> InlineKeyboardMarkup:
    prize_random = random.Random(round_id)
    buttons = []
    for position in range(1, button_count + 1):
        prefix = "✅ " if position == selected_position else ""
        if nft_armed and position == nft_position:
            label = f"{prefix}NFT"
        elif not show_small_prizes:
            label = f"{prefix}Пусто"
        else:
            prize = (
                selected_small_prize
                if position == selected_position and selected_small_prize is not None
                else prize_random.choice(JACKPOT_BUTTON_SMALL_PRIZES)
            )
            label = f"{prefix}{prize}⭐"
        buttons.append(
            InlineKeyboardButton(label, callback_data=f"jpbtn:{round_id}:{position}")
        )

    rows = [buttons[index:index + 3] for index in range(0, len(buttons), 3)]
    return InlineKeyboardMarkup(rows)


def gift_from_jackpot_button_round(row: sqlite3.Row) -> dict[str, str]:
    return {
        "title": row["nft_title"] or "NFT",
        "url": row["nft_url"] or "",
    }


async def build_jackpot_button_no_gift_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "jackpot_buttons_no_gift",
        user,
        "jackpot",
        stats=stats,
        owner_gift={"title": "", "url": ""},
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    return (
        f"{get_user_display_name(user)} выбил 777!\n\n"
        "Сейчас нет доступного NFT для 777-кнопок. Проверьте банк owner и /giftblock."
    ), []


async def build_jackpot_button_challenge_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    gift: dict[str, str],
    button_count: int,
    next_button_count: int,
    stats: sqlite3.Row | dict[str, int] | None = None,
    expired_previous: bool = False,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "jackpot_buttons",
        user,
        "jackpot",
        stats=stats,
        owner_gift=gift,
        extra_values={
            "button_count": str(button_count),
            "next_button_count": str(next_button_count),
            "nft_chance": f"1/{get_jackpot_button_nft_chance_denominator(db, button_count)}",
            "nft_chance_denominator": str(get_jackpot_button_nft_chance_denominator(db, button_count)),
            "expired_previous": "1" if expired_previous else "0",
        },
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    previous_text = (
        "\nПрошлый 777-бокс не был открыт, поэтому кнопок стало меньше."
        if expired_previous else ""
    )
    nft_chance = f"1/{get_jackpot_button_nft_chance_denominator(db, button_count)}"
    if should_award_jackpot_button_stars(db):
        prize_line = f"Шанс NFT: {nft_chance}. Остальные выигрыши: 15⭐ или 25⭐.\n"
    else:
        prize_line = (
            f"Шанс NFT: {nft_chance}. Остальные коробки без Stars-приза, "
            f"потому что цена прокрута меньше {get_jackpot_button_stars_min_price(db)}⭐.\n"
        )
    return (
        f"{get_user_display_name(user)} выбил 777!\n\n"
        f"Выбери одну из {button_count} коробок.\n"
        f"{prize_line}"
        f"Если NFT не выпадет, следующий 777 будет с {next_button_count} кнопками."
        f"{previous_text}"
    ), []


async def build_jackpot_button_nft_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    gift: dict[str, str],
    selected_position: int,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "jackpot_buttons_nft",
        user,
        "jackpot",
        stats=stats,
        owner_gift=gift,
        extra_values={
            "selected_box": str(selected_position),
            "button_count": str(get_jackpot_buttons_start(db)),
            "next_button_count": str(get_jackpot_buttons_start(db)),
        },
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    gift_text = f"{gift['title']}\n{gift['url']}" if gift["url"] else gift["title"]
    return (
        f"{get_user_display_name(user)} открыл коробку #{selected_position} и забрал NFT.\n\n"
        f"{gift_text}\n\n"
        f"Следующий 777 снова начнется с {get_jackpot_buttons_start(db)} кнопок."
    ), []


async def build_jackpot_button_miss_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    gift: dict[str, str],
    selected_position: int,
    small_prize: int,
    next_button_count: int,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "jackpot_buttons_miss",
        user,
        "jackpot",
        stats=stats,
        owner_gift=gift,
        small_gift=str(small_prize),
        extra_values={
            "selected_box": str(selected_position),
            "small_prize": str(small_prize),
            "button_prize": f"{small_prize}⭐",
            "next_button_count": str(next_button_count),
        },
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    return (
        f"{get_user_display_name(user)} открыл коробку #{selected_position}.\n\n"
        f"Выпал подарок на {small_prize}⭐.\n"
        f"NFT не найден. Следующий 777: {next_button_count} кнопок."
    ), []


async def build_jackpot_button_empty_miss_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    gift: dict[str, str],
    selected_position: int,
    next_button_count: int,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "jackpot_buttons_empty_miss",
        user,
        "jackpot",
        stats=stats,
        owner_gift=gift,
        extra_values={
            "selected_box": str(selected_position),
            "small_prize": "0",
            "button_prize": "",
            "next_button_count": str(next_button_count),
        },
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    return (
        f"{get_user_display_name(user)} открыл коробку #{selected_position}.\n\n"
        f"NFT не найден. При цене прокрута меньше {get_jackpot_button_stars_min_price(db)}⭐ "
        "Stars-приза в коробках нет.\n"
        f"Следующий 777: {next_button_count} кнопок."
    ), []


def star_prize_choice_keyboard(round_id: int, stars: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"Получить {stars}⭐ на баланс",
                    callback_data=f"starprize:balance:{round_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    f"Получить подарок за {stars}⭐",
                    callback_data=f"starprize:gift:{round_id}",
                )
            ],
        ]
    )


async def handle_star_prize_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return

    action = parts[1]
    try:
        round_id = int(parts[2])
    except ValueError:
        await query.answer()
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    choice = db.get_star_prize_choice(round_id)
    if not choice:
        await query.answer("Выбор не найден.", show_alert=True)
        return

    if int(choice["user_id"]) != query.from_user.id:
        await query.answer("Это не твой приз.", show_alert=True)
        return

    if choice["status"] != "pending":
        await query.answer("Этот приз уже выбран.", show_alert=True)
        return

    stars = int(choice["stars"])
    if action == "balance":
        if not db.resolve_star_prize_choice(round_id, "balance"):
            await query.answer("Этот приз уже выбран.", show_alert=True)
            return
        balance = db.add_stars_balance(query.from_user.id, stars)
        await query.answer(f"+{stars}⭐ на баланс.", show_alert=True)
        rendered = render_message_template(
            db,
            "star_balance",
            {
                "username": get_user_display_name(query.from_user),
                "stars": stars,
                "amount": stars,
                "stars_balance": balance,
                "balance": balance,
                "withdraw_min": MIN_STARS_WITHDRAW,
            },
        )
        try:
            if rendered:
                text, entities_data = rendered
                await query.message.edit_text(
                    text,
                    entities=deserialize_entities(entities_data) or None,
                )
            else:
                await query.message.edit_text(
                    f"{get_user_display_name(query.from_user)} забрал {stars}⭐ на баланс.\n\n"
                    f"Баланс: {balance}⭐\n"
                    f"Вывод доступен от {MIN_STARS_WITHDRAW}⭐: /withdraw"
                )
        except TelegramError:
            await query.message.reply_text(
                f"{stars}⭐ добавлены на баланс. Сейчас: {balance}⭐"
            )
        return

    if action == "gift":
        payout_id = await create_and_notify_payout(
            context,
            config,
            db,
            int(choice["chat_id"]),
            query.from_user,
            "stars_gift",
            "jackpot_buttons",
            round_id,
            f"Подарок за {stars}⭐",
        )
        if not db.resolve_star_prize_choice(round_id, "gift", payout_id):
            await query.answer("Этот приз уже выбран.", show_alert=True)
            return
        await query.answer("Запрос на подарок отправлен.", show_alert=True)
        try:
            await query.message.edit_text(
                f"{get_user_display_name(query.from_user)} выбрал подарок за {stars}⭐.\n"
                "Запрос отправлен на выдачу."
            )
        except TelegramError:
            pass
        return

    await query.answer()


async def send_jackpot_button_challenge(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    db: StatsDatabase,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    gift = await choose_owner_gift_from_api(config, db)
    if not gift:
        message, entities_data = await build_jackpot_button_no_gift_message(
            config,
            db,
            update.effective_user,
            stats=stats,
        )
        await update.message.reply_text(
            message,
            entities=deserialize_entities(entities_data) or None,
        )
        return

    expired_round = db.expire_open_jackpot_button_rounds(
        update.effective_chat.id,
        update.effective_user.id,
    )
    button_count = db.get_jackpot_button_count(update.effective_user.id)
    nft_position = random.randint(1, button_count)
    nft_armed = should_arm_jackpot_button_nft(db, button_count)
    round_id = db.create_jackpot_button_round(
        update.effective_chat.id,
        update.effective_user.id,
        button_count,
        nft_position,
        nft_armed,
        gift,
    )

    next_count = max(
        get_jackpot_buttons_min(db),
        button_count - get_jackpot_buttons_decrease(db),
    )
    message, entities_data = await build_jackpot_button_challenge_message(
        config,
        db,
        update.effective_user,
        gift,
        button_count,
        next_count,
        stats=stats,
        expired_previous=expired_round is not None,
    )
    sent_message = await update.message.reply_text(
        message,
        entities=deserialize_entities(entities_data) or None,
        reply_markup=jackpot_button_keyboard(round_id, button_count),
    )
    db.set_jackpot_button_round_message_id(round_id, sent_message.message_id)


async def handle_jackpot_button_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return

    try:
        round_id = int(parts[1])
        selected_position = int(parts[2])
    except ValueError:
        await query.answer()
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    round_row = db.get_jackpot_button_round(round_id)
    if not round_row:
        await query.answer("Раунд не найден.", show_alert=True)
        return

    if int(round_row["user_id"]) != query.from_user.id:
        await query.answer("Это не твой 777.", show_alert=True)
        return

    if round_row["status"] != "open":
        if round_row["status"] == "expired":
            await query.answer(
                "Этот 777-бокс уже сгорел, потому что был выбит новый 777.",
                show_alert=True,
            )
        else:
            await query.answer("Этот 777 уже был открыт.", show_alert=True)
        return

    is_nft = (
        bool(int(round_row["nft_armed"] or 0))
        and selected_position == int(round_row["nft_position"])
    )
    award_star_prizes = should_award_jackpot_button_stars(db)
    small_prize = (
        None
        if is_nft or not award_star_prizes
        else random.choice(JACKPOT_BUTTON_SMALL_PRIZES)
    )
    if not db.resolve_jackpot_button_round(round_id, selected_position, small_prize):
        await query.answer("Этот 777 уже был открыт.", show_alert=True)
        return

    try:
        await query.message.edit_reply_markup(
            reply_markup=jackpot_button_reveal_keyboard(
                round_id,
                int(round_row["button_count"]),
                int(round_row["nft_position"]),
                bool(int(round_row["nft_armed"] or 0)),
                selected_position,
                small_prize,
                award_star_prizes,
            )
        )
    except TelegramError:
        pass

    player = get_user_display_name(query.from_user)
    user_stats = db.get_user_stats(int(round_row["chat_id"]), query.from_user.id)
    gift = gift_from_jackpot_button_round(round_row)
    if is_nft:
        db.set_jackpot_button_count(query.from_user.id, get_jackpot_buttons_start(db))
        message, entities_data = await build_jackpot_button_nft_message(
            config,
            db,
            query.from_user,
            gift,
            selected_position,
            stats=user_stats,
        )
        await query.answer("NFT найден!", show_alert=True)
        await send_text_with_entities(context, round_row["chat_id"], message, entities_data)
        await create_and_notify_payout(
            context,
            config,
            db,
            int(round_row["chat_id"]),
            query.from_user,
            "nft",
            "jackpot_buttons",
            round_id,
            gift["title"] or "NFT из 777-кнопок",
            gift,
        )
        await process_reward_tasks(update, context, nft_win=True)
        return

    new_count = max(
        get_jackpot_buttons_min(db),
        int(round_row["button_count"]) - get_jackpot_buttons_decrease(db),
    )
    db.set_jackpot_button_count(query.from_user.id, new_count)
    if small_prize is None:
        message, entities_data = await build_jackpot_button_empty_miss_message(
            config,
            db,
            query.from_user,
            gift,
            selected_position,
            new_count,
            stats=user_stats,
        )
        await query.answer("NFT не найден.", show_alert=True)
        await send_text_with_entities(context, round_row["chat_id"], message, entities_data)
        return

    message, entities_data = await build_jackpot_button_miss_message(
        config,
        db,
        query.from_user,
        gift,
        selected_position,
        small_prize,
        new_count,
        stats=user_stats,
    )
    await query.answer(f"Подарок на {small_prize}⭐", show_alert=True)
    await send_text_with_entities(context, round_row["chat_id"], message, entities_data)
    db.create_star_prize_choice(
        round_id,
        query.from_user.id,
        int(round_row["chat_id"]),
        small_prize,
    )
    await context.bot.send_message(
        chat_id=round_row["chat_id"],
        text=(
            f"{get_user_display_name(query.from_user)}, выбери что сделать с {small_prize}⭐."
        ),
        reply_markup=star_prize_choice_keyboard(round_id, small_prize),
    )


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


async def build_three_of_kind_no_gift_message(
    config: BotConfig,
    db: StatsDatabase,
    user: User,
    result: str,
    stats: sqlite3.Row | dict[str, int] | None = None,
) -> tuple[str, list[dict]]:
    rendered = await render_saved_template(
        config,
        db,
        "three_of_kind_no_gift",
        user,
        result,
        stats=stats,
    )
    if rendered:
        text, entities_data, _, _ = rendered
        return text, entities_data

    return (
        f"{get_user_display_name(user)} выбил {COMBINATION_TITLES[result]}.\n\n"
        "Не совсем то."
    ), []


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
    user: User | UserIdentity,
    stats: sqlite3.Row | dict[str, int],
    balance: int,
    stars_balance: int,
    rank_text: str,
    rank_points: float,
) -> str:
    return (
        f"Личная статистика {get_user_display_name(user)}\n\n"
        f"Всего спинов: {stats_value(stats, 'total_spins')}\n"
        f"Ранг: {rank_text}\n"
        f"Очки ранга: {format_rank_points(rank_points)}\n"
        f"Баланс: {balance} TEM\n"
        f"Stars: {stars_balance}⭐\n"
        f"777: {stats_value(stats, 'jackpots')}\n"
        f"77X: {stats_value(stats, 'two_sevens')}\n"
        f"Три в ряд: {three_of_kind_total(stats)}"
    )


def build_personal_stats_message(
    db: StatsDatabase,
    user: User | UserIdentity,
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
    stars_balance = db.get_stars_balance(user.id)
    rank_points = db.get_rank_points(user.id)
    if not template:
        rank_text = get_effective_rank_card(
            rank_points,
            rank_cards,
            user_rank_card,
        )["text"]
        return build_default_personal_stats_message(
            user,
            stats,
            balance,
            stars_balance,
            rank_text,
            rank_points,
        ), []

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
        "stars_balance": str(stars_balance),
        **rank_values(rank_points, rank_cards, user_rank_card),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(rank_points, rank_cards, user_rank_card),
    )


def build_full_stats_text(
    db: StatsDatabase,
    rows: list[sqlite3.Row],
    scope_label: str,
) -> str:
    total_spins = sum(stats_value(row, "total_spins") for row in rows)
    total_jackpots = sum(stats_value(row, "jackpots") for row in rows)
    total_two_sevens = sum(stats_value(row, "two_sevens") for row in rows)
    total_three_bars = sum(stats_value(row, "three_bars") for row in rows)
    total_three_grapes = sum(stats_value(row, "three_grapes") for row in rows)
    total_three_lemons = sum(stats_value(row, "three_lemons") for row in rows)
    rank_names = db.get_rank_names()

    lines = [
        f"Полная статистика: {scope_label}",
        "",
        f"Игроков со спинами: {len(rows)}",
        f"Всего спинов: {total_spins}",
        f"777: {total_jackpots}",
        f"77X: {total_two_sevens}",
        f"Три BAR: {total_three_bars}",
        f"Три винограда: {total_three_grapes}",
        f"Три лимона: {total_three_lemons}",
        "",
    ]

    if not rows:
        lines.append("Пока нет игроков со спинами.")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        total = stats_value(row, "total_spins")
        rank_points = float(row["rank_points"] or 0)
        rank = get_rank_name(rank_points, rank_names)
        lines.append(
            f"{index}. {get_display_name(row)} - "
            f"спины {total}, "
            f"777 {stats_value(row, 'jackpots')}, "
            f"77X {stats_value(row, 'two_sevens')}, "
            f"три в ряд {three_of_kind_total(row)}, "
            f"TEM {stats_value(row, 'tem_balance')}, "
            f"Stars {stats_value(row, 'stars_balance')}, "
            f"ранг {rank}, "
            f"очки {format_rank_points(rank_points)}"
        )

    return "\n".join(lines)


def build_default_milestone_message(
    user: User,
    stats: sqlite3.Row | dict[str, int],
    balance: int,
    stars_balance: int,
    rank_text: str,
    rank_points: float,
) -> str:
    total_spins = stats_value(stats, "total_spins")
    return (
        f"{get_user_display_name(user)} достиг {total_spins} спинов.\n\n"
        f"Ранг: {rank_text}\n"
        f"Очки ранга: {format_rank_points(rank_points)}\n"
        f"Баланс: {balance} TEM\n"
        f"Stars: {stars_balance}⭐\n"
        f"777: {stats_value(stats, 'jackpots')}\n"
        f"77X: {stats_value(stats, 'two_sevens')}\n"
        f"Три в ряд: {three_of_kind_total(stats)}"
    )


def build_milestone_message(
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int],
) -> tuple[str, list[dict]]:
    total_spins = stats_value(stats, "total_spins")
    template = db.get_message_template(f"milestone_{total_spins}") or db.get_message_template("milestone")
    rank_cards = db.get_rank_cards()
    rank_names = [rank["text"] for rank in rank_cards]
    user_rank_card = db.get_user_rank_card(user.id)
    balance = db.get_tem_balance(user.id)
    stars_balance = db.get_stars_balance(user.id)
    rank_points = db.get_rank_points(user.id)
    if not template:
        rank_text = get_effective_rank_card(
            rank_points,
            rank_cards,
            user_rank_card,
        )["text"]
        return build_default_milestone_message(user, stats, balance, stars_balance, rank_text, rank_points), []

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
        "stars_balance": str(stars_balance),
        **rank_values(rank_points, rank_cards, user_rank_card),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(rank_points, rank_cards, user_rank_card),
    )


def build_scam_warning_message(
    db: StatsDatabase,
    user: User,
    stats: sqlite3.Row | dict[str, int],
) -> tuple[str, list[dict]]:
    total_spins = stats_value(stats, "total_spins")
    template = (
        db.get_message_template(f"scam_warning_{total_spins}")
        or db.get_message_template("scam_warning")
    )
    if not template:
        return (
            f"{get_user_display_name(user)}, важное напоминание.\n\n"
            "Если тебе пишут в личку и предлагают перейти играть в другой чат, это скамеры. "
            "Играй только в официальном чате проекта."
        ), []

    text, entities_data = template
    values = {
        "username": get_user_display_name(user),
        "milestone": str(total_spins),
        "total_spins": str(total_spins),
        "stars_balance": str(db.get_stars_balance(user.id)),
        "balance": str(db.get_tem_balance(user.id)),
        "tem_balance": str(db.get_tem_balance(user.id)),
    }
    return apply_template_values(text, entities_data, values)


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
            "/mystats @username - статистика другого игрока\n"
            "/fullstat - полная статистика всех игроков в личке от 50 спинов\n"
            "/tickets - билеты текущего турнира в личке\n"
            "/tasks - активные задания\n"
            "/luck - удача/невезение по 777 и NFT\n"
            "/dailybonus - ежедневный бонус TEM\n"
            "/balance - баланс TEM и Stars\n"
            "/withdraw - запросить вывод Stars от 50⭐\n"
            "/help - помощь\n\n"
            "Крути Telegram слот, а я буду считать спины, TEM и выигрыши."
        ), []

    total_spins = stats_value(stats, "total_spins") if stats else 0
    balance = db.get_tem_balance(user.id)
    stars_balance = db.get_stars_balance(user.id)
    rank_cards = db.get_rank_cards()
    user_rank_card = db.get_user_rank_card(user.id)
    rank_points = db.get_rank_points(user.id)
    text, entities_data = template
    values = {
        "username": get_user_display_name(user),
        "total_spins": str(total_spins),
        "balance": str(balance),
        "tem_balance": str(balance),
        "stars_balance": str(stars_balance),
        **rank_values(rank_points, rank_cards, user_rank_card),
    }
    return apply_template_values(
        text,
        entities_data,
        values,
        rank_value_entities(rank_points, rank_cards, user_rank_card),
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
    db: StatsDatabase = context.application.bot_data["db"]
    for owner_user_id in sorted(payout_admin_ids(config, db)):
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


def get_extra_notify_user_ids(db: StatsDatabase) -> set[int]:
    raw_ids = db.get_bot_setting(EXTRA_NOTIFY_USERS_SETTING, [])
    if not isinstance(raw_ids, list):
        return set()
    return {int(item) for item in raw_ids if str(item).lstrip("-").isdigit()}


def set_extra_notify_user_ids(db: StatsDatabase, user_ids: set[int]) -> None:
    db.set_bot_setting(EXTRA_NOTIFY_USERS_SETTING, sorted(user_ids))


def get_payout_chat_ids(db: StatsDatabase) -> set[int]:
    raw_ids = db.get_bot_setting(PAYOUT_CHAT_IDS_SETTING, [])
    if not isinstance(raw_ids, list):
        return set()
    return {int(item) for item in raw_ids if str(item).lstrip("-").isdigit()}


def set_payout_chat_ids(db: StatsDatabase, chat_ids: set[int]) -> None:
    db.set_bot_setting(PAYOUT_CHAT_IDS_SETTING, sorted(chat_ids))


def payout_admin_ids(config: BotConfig, db: StatsDatabase) -> set[int]:
    return set(config.owner_user_ids) | get_extra_notify_user_ids(db)


def payout_admin_keyboard(payout: sqlite3.Row) -> InlineKeyboardMarkup:
    if payout["status"] == PAYOUT_STATUS_PAID:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("✅ Выплата завершена", callback_data=f"payout:noop:{payout['payout_id']}")]]
        )

    if payout["payout_type"] == "nft":
        action_button = InlineKeyboardButton(
            "📸 Завершить NFT со скрином",
            callback_data=f"payout:proof:{payout['payout_id']}",
        )
    else:
        action_button = InlineKeyboardButton(
            "✅ Завершить выплату",
            callback_data=f"payout:done:{payout['payout_id']}",
        )

    return InlineKeyboardMarkup([[action_button]])


def payout_user_keyboard(payout_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Открыть спор", callback_data=f"payout:dispute:{payout_id}")]]
    )


def payout_label(payout: sqlite3.Row) -> str:
    return f"#{payout['payout_id']} {payout['prize_text']}"


async def create_and_notify_payout(
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    db: StatsDatabase,
    chat_id: int,
    user: User | UserIdentity,
    payout_type: str,
    source_type: str,
    source_id: int | None,
    prize_text: str,
    gift: dict[str, str] | None = None,
    winner_text: str | None = None,
    winner_entities_data: list[dict] | None = None,
) -> int:
    db.remember_user(user)
    payout_id = db.create_payout(
        payout_type,
        source_type,
        source_id,
        chat_id,
        user.id,
        prize_text,
        gift.get("title") if gift else None,
        gift.get("url") if gift else None,
    )
    payout = db.get_payout(payout_id)
    if payout is None:
        return payout_id

    template_values = payout_template_values(
        user,
        payout_id,
        prize_text,
        payout_type,
        source_type,
        chat_id,
        {
            "gift_title": gift.get("title") if gift else "",
            "nft_url": gift.get("url") if gift else "",
        },
    )
    rendered_admin = render_message_template(db, "payout_admin", template_values)
    if rendered_admin:
        admin_text, admin_entities = rendered_admin
    else:
        admin_text = (
            f"Новая выплата {payout_label(payout)}\n\n"
            f"Игрок: {get_user_display_name(user)}\n"
            f"User ID: {user.id}\n"
            f"Чат: {chat_id}\n"
            f"Тип: {payout_type}\n"
            f"Источник: {source_type}"
        )
        if gift and gift.get("url"):
            admin_text += f"\nNFT: {gift['url']}"
        admin_entities = []

    for admin_id in sorted(payout_admin_ids(config, db)):
        try:
            sent = await context.bot.send_message(
                chat_id=admin_id,
                text=admin_text,
                entities=deserialize_entities(admin_entities) or None,
                reply_markup=payout_admin_keyboard(payout),
            )
            db.save_payout_message(payout_id, admin_id, sent.message_id)
        except TelegramError as error:
            logging.warning("Failed to notify payout admin %s: %s", admin_id, error)

    private_chat_id = db.get_private_chat_id(user.id)
    if private_chat_id:
        rendered_user = render_message_template(db, "payout_user", template_values)
        if winner_text:
            user_text = winner_text
            user_entities = winner_entities_data or []
        elif rendered_user:
            user_text, user_entities = rendered_user
        else:
            user_text = (
                f"Ты выиграл: {prize_text}\n\n"
                "Когда выдача будет завершена, я пришлю уведомление. "
                "Если возникнет проблема, можно открыть спор."
            )
            user_entities = []

        try:
            await context.bot.send_message(
                chat_id=private_chat_id,
                text=user_text,
                entities=deserialize_entities(user_entities) or None,
                reply_markup=payout_user_keyboard(payout_id),
            )
        except TelegramError as error:
            logging.warning("Failed to notify payout winner %s: %s", user.id, error)

    return payout_id


async def complete_payout_workflow(
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    db: StatsDatabase,
    payout_id: int,
    proof_file_id: str | None = None,
) -> None:
    db.complete_payout(payout_id, proof_file_id)
    payout = db.get_payout(payout_id)
    if payout is None:
        return

    completed_text = (
        f"Выплата завершена: {payout_label(payout)}\n\n"
        f"Игрок: {get_display_name(payout)}"
    )
    payout_values = payout_template_values(
        payout,
        int(payout["payout_id"]),
        payout["prize_text"],
        payout["payout_type"],
        payout["source_type"],
        int(payout["chat_id"]),
        {
            "gift_title": payout["gift_title"] or "",
            "nft_url": payout["gift_url"] or "",
        },
    )
    private_chat_id = db.get_private_chat_id(payout["user_id"])
    if private_chat_id:
        rendered_done = render_message_template(
            db,
            "payout_done",
            payout_values,
        )
        if rendered_done:
            done_text, done_entities = rendered_done
        else:
            done_text = f"Выплата завершена: {payout['prize_text']}"
            done_entities = []
        try:
            await context.bot.send_message(
                chat_id=private_chat_id,
                text=done_text,
                entities=deserialize_entities(done_entities) or None,
            )
        except TelegramError as error:
            logging.warning("Failed to notify completed payout winner %s: %s", payout["user_id"], error)

    target_chats = {int(payout["chat_id"])} | get_payout_chat_ids(db)
    if proof_file_id:
        rendered_chat = render_message_template(db, "payout_chat", payout_values)
        if rendered_chat:
            chat_text, chat_entities = rendered_chat
        else:
            chat_text = completed_text
            chat_entities = []

        for chat_id in sorted(target_chats):
            try:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=proof_file_id,
                    caption=chat_text,
                    caption_entities=deserialize_entities(chat_entities) or None,
                )
            except TelegramError as error:
                logging.warning("Failed to publish payout proof to %s: %s", chat_id, error)

    completed_keyboard = payout_admin_keyboard(payout)
    for message in db.get_payout_messages(payout_id):
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=message["chat_id"],
                message_id=message["message_id"],
                reply_markup=completed_keyboard,
            )
        except TelegramError:
            pass


async def handle_payout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data:
        return

    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return

    action = parts[1]
    try:
        payout_id = int(parts[2])
    except ValueError:
        await query.answer()
        return

    db: StatsDatabase = context.application.bot_data["db"]
    config: BotConfig = context.application.bot_data["config"]
    payout = db.get_payout(payout_id)
    if payout is None:
        await query.answer("Выплата не найдена.", show_alert=True)
        return

    if action == "noop":
        await query.answer("Уже завершено.", show_alert=True)
        return

    if action == "dispute":
        if payout["user_id"] != query.from_user.id:
            await query.answer("Спор может открыть только победитель.", show_alert=True)
            return
        context.user_data["awaiting_payout_dispute"] = payout_id
        await query.answer()
        await query.message.reply_text("Опиши проблему одним сообщением. Я передам ее owner.")
        return

    if query.from_user.id not in payout_admin_ids(config, db):
        await query.answer()
        return

    if action == "proof":
        context.user_data["awaiting_payout_proof"] = payout_id
        await query.answer()
        await query.message.reply_text("Отправь сюда скрин выдачи NFT одним фото.")
        return

    if action == "done":
        await complete_payout_workflow(context, config, db, payout_id)
        await query.answer("Выплата завершена.", show_alert=True)
        return

    await query.answer()


async def handle_payout_proof_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user or not update.message:
        return False
    if update.effective_chat.type != "private":
        return False

    payout_id = context.user_data.get("awaiting_payout_proof")
    if not payout_id:
        return False

    db: StatsDatabase = context.application.bot_data["db"]
    config: BotConfig = context.application.bot_data["config"]
    if update.effective_user.id not in payout_admin_ids(config, db):
        return False

    photo = update.message.photo[-1]
    await complete_payout_workflow(context, config, db, int(payout_id), photo.file_id)
    context.user_data.pop("awaiting_payout_proof", None)
    await update.message.reply_text("Скрин принят. Выплата отмечена завершенной.")
    return True


async def handle_payout_dispute_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat or not update.effective_user or not update.message:
        return False
    if update.effective_chat.type != "private":
        return False

    payout_id = context.user_data.get("awaiting_payout_dispute")
    if not payout_id:
        return False

    db: StatsDatabase = context.application.bot_data["db"]
    config: BotConfig = context.application.bot_data["config"]
    text = (update.message.text or "").strip()
    if not text:
        return True

    if db.open_payout_dispute(int(payout_id), update.effective_user.id, text):
        payout = db.get_payout(int(payout_id))
        for admin_id in sorted(payout_admin_ids(config, db)):
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"Открыт спор по выплате #{payout_id}\n\n"
                        f"Игрок: {get_user_display_name(update.effective_user)}\n"
                        f"Проблема: {text}"
                    ),
                    reply_markup=payout_admin_keyboard(payout) if payout else None,
                )
            except TelegramError as error:
                logging.warning("Failed to notify payout dispute admin %s: %s", admin_id, error)
        await update.message.reply_text("Спор открыт. Owner получил описание проблемы.")
    else:
        await update.message.reply_text(
            "Не смог открыть спор: выплата не найдена или она не принадлежит твоему аккаунту."
        )

    context.user_data.pop("awaiting_payout_dispute", None)
    return True


def normalize_task_metric(value: str) -> str | None:
    normalized = value.strip().lower()
    aliases = {
        "spin": "spins",
        "spins": "spins",
        "total": "spins",
        "прокруты": "spins",
        "крутки": "spins",
        "777": "777",
        "jackpot": "777",
        "jackpots": "777",
        "77x": "77x",
        "77х": "77x",
        "three": "three",
        "triple": "three",
        "три": "three",
        "ряд": "three",
        "nft": "nfts",
        "nfts": "nfts",
        "нфт": "nfts",
        "rank": "rank_points",
        "rank_points": "rank_points",
        "tem": "tem",
        "balance": "tem",
    }
    return aliases.get(normalized)


def task_metric_label(metric: str) -> str:
    return {
        "spins": "прокруты",
        "777": "777",
        "77x": "77X",
        "three": "три в ряд",
        "nfts": "NFT из боксов",
        "rank_points": "очки ранга",
        "tem": "TEM",
    }.get(metric, metric)


def normalize_task_scope(value: str) -> str | None:
    normalized = value.strip().lower()
    if normalized in {"all", "все", "everyone", "global"}:
        return "all"
    if normalized in {"optin", "optional", "join", "take", "хочу", "выбор"}:
        return "optin"
    if normalized in {"users", "user", "personal", "individual", "люди", "юзеры"}:
        return "users"
    return None


def parse_task_prize(token: str) -> tuple[str, str]:
    normalized = token.strip().lower()
    if normalized in {"nft", "randomnft", "gift", "гифт", "нфт"}:
        return "nft", ""

    stars_match = re.search(r"(\d+)", normalized)
    if normalized.startswith(("stars", "star", "звезды", "звёзды", "⭐")) and stars_match:
        return "stars", stars_match.group(1)

    return "text", token.strip()


def task_prize_label(task: sqlite3.Row) -> str:
    if task["prize_type"] == "nft":
        return "рандом NFT из банка"
    if task["prize_type"] == "stars":
        return f"{task['prize_value']}⭐"
    return task["prize_value"] or "приз"


def task_min_spins_label(task: sqlite3.Row) -> str:
    min_spins = int(task["min_spins"] or 0)
    if min_spins <= 0:
        return ""
    return f"мин. {min_spins} спинов"


def task_completion_mode_label(mode: str) -> str:
    return {
        "auto": "авто по выполнению",
        "manual": "ручное подтверждение",
        "time": "до времени",
        "people": "до числа победителей",
    }.get(mode, mode)


def task_status_label(status: str) -> str:
    return {
        "active": "активно",
        "ready": "ждет подтверждения",
        "completed": "завершено",
        "stopped": "остановлено",
    }.get(status, status)


def normalize_task_completion_mode(value: str) -> str | None:
    normalized = value.strip().lower().replace("-", "_")
    aliases = {
        "auto": "auto",
        "авто": "auto",
        "automatic": "auto",
        "manual": "manual",
        "ручное": "manual",
        "руками": "manual",
        "confirm": "manual",
        "review": "manual",
        "time": "time",
        "timer": "time",
        "время": "time",
        "по_времени": "time",
        "people": "people",
        "persons": "people",
        "users": "people",
        "люди": "people",
        "участники": "people",
        "победители": "people",
    }
    return aliases.get(normalized)


def extract_task_options(tokens: list[str]) -> tuple[int, str, str | None, int | None, list[str]]:
    min_spins = 0
    completion_mode = TASK_DEFAULT_COMPLETION_MODE
    ends_at: str | None = None
    max_completions: int | None = None
    title_tokens: list[str] = []
    index = 0
    option_names = {
        "minspins",
        "minspin",
        "min_spins",
        "spinsmin",
        "минимум",
        "минспины",
        "минспин",
        "минпрокруты",
        "минимумспинов",
        "минимумпрокрутов",
    }
    time_option_names = {"time", "timer", "ends", "end", "до", "время", "длительность"}
    people_option_names = {"people", "persons", "users", "max", "limit", "победители", "люди", "участники"}

    while index < len(tokens):
        token = tokens[index]
        normalized = token.strip().lower().strip(",;")
        if normalized in option_names and index + 1 < len(tokens):
            try:
                min_spins = max(0, parse_integer_from_token(tokens[index + 1], "minspins"))
                index += 2
                continue
            except ValueError:
                pass

        match = re.fullmatch(
            r"(?:minspins|minspin|min_spins|spinsmin|минспины|минспин|минпрокруты|минимумспинов|минимумпрокрутов)=?(\d+)",
            normalized,
        )
        if match:
            min_spins = max(0, int(match.group(1)))
            index += 1
            continue

        mode_value = normalize_task_completion_mode(normalized)
        if mode_value and normalized not in time_option_names and normalized not in people_option_names:
            completion_mode = mode_value
            index += 1
            continue

        mode_match = re.fullmatch(r"(?:mode|режим|тип)=?([a-zа-я_]+)", normalized)
        if mode_match:
            mode_value = normalize_task_completion_mode(mode_match.group(1))
            if mode_value:
                completion_mode = mode_value
                index += 1
                continue

        if normalized in time_option_names and index + 1 < len(tokens):
            try:
                ends_at = datetime_to_storage(utc_now() + parse_duration_token(tokens[index + 1], "hours"))
                completion_mode = "time"
                index += 2
                continue
            except ValueError:
                pass

        time_match = re.fullmatch(
            r"(?:time|timer|ends|end|до|время)(?:=)?(.+)",
            normalized,
        )
        if time_match:
            try:
                ends_at = datetime_to_storage(utc_now() + parse_duration_token(time_match.group(1), "hours"))
                completion_mode = "time"
                index += 1
                continue
            except ValueError:
                pass

        if normalized in people_option_names and index + 1 < len(tokens):
            try:
                max_completions = max(1, parse_integer_from_token(tokens[index + 1], "количество выполнений"))
                completion_mode = "people"
                index += 2
                continue
            except ValueError:
                pass

        people_match = re.fullmatch(
            r"(?:people|persons|users|max|limit|победители|люди|участники)(?:=)?(\d+)",
            normalized,
        )
        if people_match:
            max_completions = max(1, int(people_match.group(1)))
            completion_mode = "people"
            index += 1
            continue

        title_tokens.append(token)
        index += 1

    if completion_mode == "time" and ends_at is None:
        raise ValueError("Для режима time укажи время: например time hours72 или time minutes30.")
    if completion_mode == "people" and not max_completions:
        raise ValueError("Для режима people укажи количество: например people10.")

    return min_spins, completion_mode, ends_at, max_completions, title_tokens


def task_progress_line(db: StatsDatabase, task: sqlite3.Row, user_id: int) -> str:
    participant = db.get_reward_task_participant(task["task_id"], user_id)
    if not participant:
        if task["scope"] == "optin":
            return "не взято"
        return "начнется с первого подходящего действия"

    progress_chat_id = int(participant["chat_id"] or task["chat_id"] or 0)
    current = db.get_user_metric_value(
        progress_chat_id,
        user_id,
        task["metric"],
    ) if progress_chat_id else 0.0
    progress = max(0.0, current - float(participant["baseline_value"] or 0))
    progress_text = f"{format_rank_points(progress)}/{format_rank_points(float(task['goal']))}"
    min_spins = int(task["min_spins"] or 0)
    if min_spins > 0 and progress_chat_id:
        current_spins = int(db.get_user_metric_value(progress_chat_id, user_id, "spins"))
        progress_text += f", минимум спинов {current_spins}/{min_spins}"
    return progress_text


def task_metric_increment(metric: str, result: str | None, rank_gain: float = 0.0, nft_win: bool = False) -> float:
    if metric == "spins" and result:
        return 1.0
    if metric == "777" and result == "jackpot":
        return 1.0
    if metric == "77x" and result == "two_sevens":
        return 1.0
    if metric == "three" and result in {"three_bars", "three_grapes", "three_lemons"}:
        return 1.0
    if metric == "rank_points" and result:
        return rank_gain
    if metric == "tem" and result:
        return float(calculate_spin_reward(result))
    if metric == "nfts" and nft_win:
        return 1.0
    return 0.0


async def create_task_payout(
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    db: StatsDatabase,
    task: sqlite3.Row,
    chat_id: int,
    user: User | UserIdentity,
) -> int:
    if task["prize_type"] == "nft":
        gift = await choose_owner_gift_from_api(config, db)
        prize_text = f"Задание #{task['task_id']}: {gift['title'] if gift else 'NFT'}"
        return await create_and_notify_payout(
            context,
            config,
            db,
            chat_id,
            user,
            "nft",
            "task",
            int(task["task_id"]),
            prize_text,
            gift,
        )

    if task["prize_type"] == "stars":
        prize_text = f"Задание #{task['task_id']}: {task['prize_value']}⭐"
        return await create_and_notify_payout(
            context,
            config,
            db,
            chat_id,
            user,
            "stars",
            "task",
            int(task["task_id"]),
            prize_text,
        )

    prize_text = f"Задание #{task['task_id']}: {task['prize_value'] or task['title']}"
    return await create_and_notify_payout(
        context,
        config,
        db,
        chat_id,
        user,
        "gift",
        "task",
        int(task["task_id"]),
        prize_text,
    )


async def complete_task_participant_with_payout(
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    db: StatsDatabase,
    task: sqlite3.Row,
    chat_id: int,
    user: User | UserIdentity,
) -> tuple[int, bool]:
    payout_id = await create_task_payout(context, config, db, task, chat_id, user)
    db.complete_reward_task_participant(task["task_id"], user.id, payout_id)

    task_closed = False
    if task["completion_mode"] == "people" and task["max_completions"]:
        completed_count = db.count_completed_reward_task_participants(task["task_id"])
        if completed_count >= int(task["max_completions"]):
            task_closed = db.complete_reward_task(task["task_id"])

    return payout_id, task_closed


def task_ready_admin_keyboard(task_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Выдать приз", callback_data=f"taskdone:{task_id}:{user_id}")]]
    )


async def notify_task_ready(
    context: ContextTypes.DEFAULT_TYPE,
    config: BotConfig,
    db: StatsDatabase,
    task: sqlite3.Row,
    chat_id: int,
    user: User,
) -> None:
    text = (
        f"Игрок выполнил задание #{task['task_id']}.\n\n"
        f"Задание: {task['title']}\n"
        f"Игрок: {get_user_display_name(user)}\n"
        f"Чат: {chat_id}\n"
        f"Приз: {task_prize_label(task)}\n\n"
        "Выдача произойдет только после подтверждения."
    )
    for admin_id in sorted(payout_admin_ids(config, db)):
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=text,
                reply_markup=task_ready_admin_keyboard(int(task["task_id"]), user.id),
            )
        except TelegramError as error:
            logging.warning("Failed to notify task admin %s: %s", admin_id, error)


async def process_reward_tasks(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    result: str | None = None,
    nft_win: bool = False,
) -> None:
    if not update.effective_chat or not update.effective_user:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not is_allowed_chat(config, update.effective_chat.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    rank_gain = get_rank_points_per_spin(db)
    for task in db.get_active_reward_tasks_for_user(update.effective_chat.id, update.effective_user.id):
        current = db.get_user_metric_value(update.effective_chat.id, update.effective_user.id, task["metric"])
        participant = db.get_reward_task_participant(task["task_id"], update.effective_user.id)
        if participant is None:
            if task["scope"] == "optin":
                continue
            increment = task_metric_increment(task["metric"], result, rank_gain, nft_win)
            baseline = max(0.0, current - increment)
            participant = db.ensure_reward_task_participant(
                task["task_id"],
                update.effective_user.id,
                baseline,
                update.effective_chat.id,
            )

        if participant["status"] != "active":
            continue

        progress = current - float(participant["baseline_value"] or 0)
        if progress < float(task["goal"]):
            continue

        min_spins = int(task["min_spins"] or 0)
        if min_spins > 0:
            current_spins = db.get_user_metric_value(
                update.effective_chat.id,
                update.effective_user.id,
                "spins",
            )
            if current_spins < min_spins:
                continue

        if db.mark_reward_task_participant_ready(task["task_id"], update.effective_user.id):
            await notify_task_ready(
                context,
                config,
                db,
                task,
                update.effective_chat.id,
                update.effective_user,
            )
            if update.message:
                await update.message.reply_text(
                    f"{get_user_display_name(update.effective_user)} выполнил условия задания "
                    f"#{task['task_id']}.\n"
                    "Owner должен подтвердить выдачу."
                )


def task_usage() -> str:
    return (
        "Задания owner:\n\n"
        "/task add all spins 500 nft minspins300 Название\n"
        "/task add optin 777 2 stars25 manual Название\n"
        "/task add all spins 100 nft time hours72 Название\n"
        "/task add all spins 50 stars25 people10 Название\n"
        "/task add users @user1 @user2 spins 500 nft Название\n"
        "/task list\n"
        "/task active\n"
        "/task completed\n"
        "/task requests\n"
        "/task finish ID\n"
        "/task stop ID\n\n"
        "scope: all, optin, users\n"
        "metrics: spins, 777, 77x, three, nfts, rank, tem\n"
        "modes: auto, manual, time, people\n"
        "prize: nft, stars25, любой_текст\n"
        "minspins300: приз выдастся только если у игрока уже есть 300 прокрутов в чате"
    )


def parse_task_create_args(
    db: StatsDatabase,
    args: list[str],
) -> tuple[str, str, float, str, str, str, int, str, str | None, int | None, list[int]]:
    if len(args) < 5:
        raise ValueError(task_usage())

    scope = normalize_task_scope(args[1])
    if not scope:
        raise ValueError("scope должен быть all, optin или users.")

    allowed_user_ids: list[int] = []
    metric_index = 2
    if scope == "users":
        metric_index = None
        for index, token in enumerate(args[2:], start=2):
            if normalize_task_metric(token):
                metric_index = index
                break
            for user_token in [part for part in token.split(",") if part.strip()]:
                user_row = resolve_known_user(db, user_token.rstrip(","))
                if not user_row:
                    raise ValueError(f"Не знаю пользователя {user_token}. Он должен попасть в базу бота.")
                allowed_user_ids.append(int(user_row["user_id"]))
        if metric_index is None or not allowed_user_ids:
            raise ValueError("Для users укажи пользователей, потом metric.")

    metric = normalize_task_metric(args[metric_index])
    if not metric:
        raise ValueError("Не понял metric. Доступно: spins, 777, 77x, three, nfts, rank, tem.")

    if len(args) <= metric_index + 2:
        raise ValueError(task_usage())

    goal = float(parse_integer_from_token(args[metric_index + 1], "цель задания"))
    prize_type, prize_value = parse_task_prize(args[metric_index + 2])
    min_spins, completion_mode, ends_at, max_completions, title_tokens = extract_task_options(
        args[metric_index + 3:]
    )
    title = " ".join(title_tokens).strip()
    if not title:
        title = f"{task_metric_label(metric)} x{format_rank_points(goal)}"

    return (
        scope,
        metric,
        goal,
        prize_type,
        prize_value,
        title,
        min_spins,
        completion_mode,
        ends_at,
        max_completions,
        allowed_user_ids,
    )


async def manage_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    if not context.args:
        await update.message.reply_text(task_usage())
        return

    action = context.args[0].lower()
    if action in {"create", "add", "new", "создать"}:
        try:
            (
                scope,
                metric,
                goal,
                prize_type,
                prize_value,
                title,
                min_spins,
                completion_mode,
                ends_at,
                max_completions,
                allowed_user_ids,
            ) = parse_task_create_args(db, context.args)
        except ValueError as error:
            await update.message.reply_text(str(error))
            return

        task_id = db.create_reward_task(
            title,
            scope,
            metric,
            goal,
            prize_type,
            prize_value,
            None,
            update.effective_user.id,
            min_spins,
            completion_mode,
            ends_at,
            max_completions,
        )
        for user_id in allowed_user_ids:
            db.add_reward_task_allowed_user(task_id, user_id)
        min_text = f"\nМинимум для приза: {min_spins} спинов" if min_spins else ""
        finish_text = ""
        if completion_mode == "time" and ends_at:
            finish_text = f"\nФиниш: {format_datetime_for_message(ends_at)}"
        elif completion_mode == "people" and max_completions:
            finish_text = f"\nЗакрытие: после {max_completions} выполнений"
        await update.message.reply_text(
            f"Задание #{task_id} создано.\n"
            f"{title}\n"
            f"Тип: {scope}, цель: {format_rank_points(goal)} {task_metric_label(metric)}, "
            f"приз: {prize_type} {prize_value}\n"
            f"Режим завершения: {task_completion_mode_label(completion_mode)}"
            f"{min_text}"
            f"{finish_text}"
        )
        return

    if action in {"list", "список", "active", "активные", "completed", "done", "завершенные", "завершённые"}:
        if action in {"active", "активные"}:
            tasks = db.get_reward_tasks_by_status("active")
            title = "Активные задания:"
        elif action in {"completed", "done", "завершенные", "завершённые"}:
            tasks = db.get_reward_tasks_by_status("completed")
            title = "Завершенные задания:"
        else:
            tasks = db.get_reward_tasks(include_inactive=True)
            title = "Задания:"
        if not tasks:
            await update.message.reply_text("Заданий пока нет.")
            return
        lines = [title]
        buttons = []
        for task in tasks:
            min_text = f", {task_min_spins_label(task)}" if task_min_spins_label(task) else ""
            finish_parts = [task_completion_mode_label(task["completion_mode"])]
            if task["ends_at"]:
                finish_parts.append(format_datetime_for_message(task["ends_at"]))
            if task["max_completions"]:
                finish_parts.append(f"{db.count_completed_reward_task_participants(task['task_id'])}/{task['max_completions']}")
            lines.append(
                f"#{task['task_id']} [{task['status']}] {task['title']} - "
                f"{task['scope']}, {format_rank_points(float(task['goal']))} {task_metric_label(task['metric'])}, "
                f"приз: {task_prize_label(task)}{min_text}, режим: {', '.join(finish_parts)}"
            )
            buttons.append(
                [InlineKeyboardButton(f"Открыть #{task['task_id']}", callback_data=f"taskadmin:open:{task['task_id']}")]
            )
        if buttons:
            buttons.append([InlineKeyboardButton("Заявки/готовые", callback_data="taskadmin:requests")])
        await reply_long_text(update.message, "\n".join(lines))
        if buttons:
            await update.message.reply_text("Открыть задание:", reply_markup=InlineKeyboardMarkup(buttons))
        return

    if action in {"requests", "заявки", "готовые"}:
        rows = db.get_ready_reward_task_participants()
        if not rows:
            await update.message.reply_text("Готовых к подтверждению заданий пока нет.")
            return
        lines = ["Заявки / готовые задания:"]
        buttons = []
        for row in rows[:30]:
            lines.append(
                f"#{row['task_id']} {row['title']} - {get_display_name(row)} "
                f"({task_prize_label(row)})"
            )
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"Выдать #{row['task_id']} {get_display_name(row)}",
                        callback_data=f"taskdone:{row['task_id']}:{row['user_id']}",
                    )
                ]
            )
        await reply_long_text(update.message, "\n".join(lines))
        await update.message.reply_text("Подтвердить:", reply_markup=InlineKeyboardMarkup(buttons))
        return

    if action in {"finish", "complete", "завершить"} and len(context.args) >= 2:
        try:
            task_id = parse_integer_from_token(context.args[1], "ID задания")
        except ValueError as error:
            await update.message.reply_text(str(error))
            return
        if db.complete_reward_task(task_id):
            await update.message.reply_text(f"Задание #{task_id} завершено.")
        else:
            await update.message.reply_text("Не нашел активное задание с таким ID.")
        return

    if action in {"stop", "cancel"} and len(context.args) >= 2:
        try:
            task_id = parse_integer_from_token(context.args[1], "ID задания")
        except ValueError as error:
            await update.message.reply_text(str(error))
            return
        if db.stop_reward_task(task_id):
            await update.message.reply_text(f"Задание #{task_id} остановлено.")
        else:
            await update.message.reply_text("Не нашел активное задание с таким ID.")
        return

    await update.message.reply_text(task_usage())


async def show_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    if update.effective_chat.type == "private":
        db.remember_private_subscriber(update.effective_user, update.effective_chat.id)

    await update.message.reply_text(
        "Задания\n\nВыбери раздел:",
        reply_markup=task_menu_keyboard(),
    )


def task_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Активные задания", callback_data="taskmenu:active")],
            [InlineKeyboardButton("Запросить задание", callback_data="taskmenu:request")],
            [InlineKeyboardButton("Завершенные задания", callback_data="taskmenu:completed")],
        ]
    )


def task_back_keyboard(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    keyboard_rows = list(rows)
    keyboard_rows.append([InlineKeyboardButton("Назад к заданиям", callback_data="taskmenu:main")])
    return InlineKeyboardMarkup(keyboard_rows)


def task_finish_rule_text(task: sqlite3.Row) -> str:
    mode = task["completion_mode"]
    if mode == "time" and task["ends_at"]:
        return f"{task_completion_mode_label(mode)}: {format_datetime_for_message(task['ends_at'])}"
    if mode == "people" and task["max_completions"]:
        return (
            f"{task_completion_mode_label(mode)}: "
            f"{task['max_completions']} выполнений"
        )
    return task_completion_mode_label(mode)


def task_detail_text(db: StatsDatabase, task: sqlite3.Row, user_id: int | None = None) -> str:
    lines = [
        f"Задание #{task['task_id']}",
        "",
        str(task["title"]),
        "",
        f"Статус: {task_status_label(task['status'])}",
        f"Тип доступа: {task['scope']}",
        f"Завершение: {task_finish_rule_text(task)}",
        f"Цель: {format_rank_points(float(task['goal']))} {task_metric_label(task['metric'])}",
        f"Приз: {task_prize_label(task)}",
    ]
    if int(task["min_spins"] or 0):
        lines.append(f"Минимум для приза: {int(task['min_spins'])} спинов")
    if task["completion_mode"] == "people" and task["max_completions"]:
        lines.append(
            f"Выполнено участников: "
            f"{db.count_completed_reward_task_participants(task['task_id'])}/{task['max_completions']}"
        )

    if user_id is not None:
        participant = db.get_reward_task_participant(task["task_id"], user_id)
        if participant:
            lines.append("")
            lines.append(f"Твой статус: {task_status_label(participant['status'])}")
            if participant["status"] != "completed":
                lines.append(f"Прогресс: {task_progress_line(db, task, user_id)}")
        elif task["scope"] == "optin":
            lines.append("")
            lines.append("Твой статус: можно запросить")
        else:
            lines.append("")
            lines.append("Твой статус: будет считаться после первого подходящего действия")

    return "\n".join(lines)


def visible_task_sections(db: StatsDatabase, user_id: int) -> tuple[list[sqlite3.Row], list[sqlite3.Row], list[sqlite3.Row]]:
    visible_tasks = db.get_visible_reward_tasks_for_user(user_id)
    active_tasks = []
    request_tasks = []
    for task in visible_tasks:
        participant = db.get_reward_task_participant(task["task_id"], user_id)
        if participant and participant["status"] == "completed":
            continue
        if task["scope"] == "optin" and participant is None:
            request_tasks.append(task)
        else:
            active_tasks.append(task)

    completed_tasks = db.get_user_reward_task_rows(user_id, {"completed"})
    return active_tasks, request_tasks, completed_tasks


def build_task_section(
    db: StatsDatabase,
    user_id: int,
    section: str,
) -> tuple[str, InlineKeyboardMarkup]:
    active_tasks, request_tasks, completed_tasks = visible_task_sections(db, user_id)
    if section == "request":
        tasks = request_tasks
        title = "Запросить задание"
        empty = "Сейчас нет заданий, которые нужно отдельно запрашивать."
    elif section == "completed":
        tasks = completed_tasks
        title = "Завершенные задания"
        empty = "У тебя пока нет завершенных заданий."
    else:
        tasks = active_tasks
        title = "Активные задания"
        empty = "Активных заданий пока нет."

    if not tasks:
        return f"{title}\n\n{empty}", task_back_keyboard()

    lines = [title, ""]
    buttons = []
    for task in tasks[:30]:
        participant = db.get_reward_task_participant(task["task_id"], user_id)
        if section == "completed":
            status = "завершено"
        elif participant:
            status = task_status_label(participant["status"])
        elif task["scope"] == "optin":
            status = "можно запросить"
        else:
            status = "активно"
        lines.append(
            f"#{task['task_id']} {task['title']} - {status}, "
            f"{format_rank_points(float(task['goal']))} {task_metric_label(task['metric'])}"
        )
        buttons.append(
            [InlineKeyboardButton(f"Открыть #{task['task_id']}", callback_data=f"taskopen:{task['task_id']}")]
        )

    return "\n".join(lines), task_back_keyboard(*buttons)


def task_user_detail_keyboard(db: StatsDatabase, task: sqlite3.Row, user_id: int) -> InlineKeyboardMarkup:
    participant = db.get_reward_task_participant(task["task_id"], user_id)
    rows = []
    if task["status"] == "active" and task["scope"] == "optin" and participant is None:
        rows.append([InlineKeyboardButton("Запросить / взять задание", callback_data=f"taskjoin:{task['task_id']}")])
    return task_back_keyboard(*rows)


def task_admin_detail_keyboard(db: StatsDatabase, task: sqlite3.Row) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Участники", callback_data=f"taskadmin:participants:{task['task_id']}")],
        [InlineKeyboardButton("Заявки/готовые", callback_data="taskadmin:requests")],
    ]
    if task["status"] == "active":
        rows.insert(0, [InlineKeyboardButton("Завершить задание", callback_data=f"taskadmin:finish:{task['task_id']}")])
    return owner_panel_back_keyboard(*rows)


def resolve_task_join_chat_id(config: BotConfig, task: sqlite3.Row, query) -> int | None:
    if task["chat_id"]:
        return int(task["chat_id"])
    if query.message and query.message.chat.type != "private":
        return int(query.message.chat_id)
    if len(config.allowed_chat_ids) == 1:
        return next(iter(config.allowed_chat_ids))
    return None


async def handle_task_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    config: BotConfig = context.application.bot_data["config"]
    parts = query.data.split(":")
    action_group = parts[0]

    if action_group == "taskmenu":
        section = parts[1] if len(parts) > 1 else "main"
        if section == "main":
            await query.answer()
            if query.message:
                await query.message.edit_text("Задания\n\nВыбери раздел:", reply_markup=task_menu_keyboard())
            return

        text, keyboard = build_task_section(db, query.from_user.id, section)
        await query.answer()
        if query.message:
            await query.message.edit_text(text, reply_markup=keyboard)
        return

    if action_group == "taskopen":
        try:
            task_id = int(parts[1])
        except (IndexError, ValueError):
            await query.answer()
            return
        task = db.get_reward_task(task_id)
        if not task:
            await query.answer("Задание не найдено.", show_alert=True)
            return
        await query.answer()
        if query.message:
            await query.message.edit_text(
                task_detail_text(db, task, query.from_user.id),
                reply_markup=task_user_detail_keyboard(db, task, query.from_user.id),
            )
        return

    if action_group == "taskjoin":
        try:
            task_id = int(parts[1])
        except (IndexError, ValueError):
            await query.answer()
            return

        task = db.get_reward_task(task_id)
        if not task or task["status"] != "active" or task["scope"] != "optin":
            await query.answer("Задание недоступно.", show_alert=True)
            return

        chat_id = resolve_task_join_chat_id(config, task, query)
        if chat_id is None:
            await query.answer(
                "Не понял, к какому чату привязать прогресс. Возьми задание из игрового чата.",
                show_alert=True,
            )
            return

        baseline = db.get_user_metric_value(chat_id, query.from_user.id, task["metric"])
        db.ensure_reward_task_participant(task_id, query.from_user.id, baseline, chat_id)
        await query.answer("Задание взято.", show_alert=True)
        if query.message:
            await query.message.edit_text(
                task_detail_text(db, task, query.from_user.id),
                reply_markup=task_user_detail_keyboard(db, task, query.from_user.id),
            )
        return

    if action_group == "taskadmin":
        if not has_owner_access(config, db, query.from_user.id):
            await query.answer()
            return

        action = parts[1] if len(parts) > 1 else ""
        if action == "requests":
            rows = db.get_ready_reward_task_participants()
            if not rows:
                await query.answer("Готовых заявок нет.", show_alert=True)
                return
            lines = ["Заявки / готовые задания:"]
            buttons = []
            for row in rows[:30]:
                lines.append(f"#{row['task_id']} {row['title']} - {get_display_name(row)}")
                buttons.append(
                    [
                        InlineKeyboardButton(
                            f"Выдать #{row['task_id']} {get_display_name(row)}",
                            callback_data=f"taskdone:{row['task_id']}:{row['user_id']}",
                        )
                    ]
                )
            await query.answer()
            if query.message:
                await query.message.edit_text("\n".join(lines), reply_markup=owner_panel_back_keyboard(*buttons))
            return

        try:
            task_id = int(parts[2])
        except (IndexError, ValueError):
            await query.answer()
            return
        task = db.get_reward_task(task_id)
        if not task:
            await query.answer("Задание не найдено.", show_alert=True)
            return

        if action == "open":
            await query.answer()
            if query.message:
                await query.message.edit_text(
                    task_detail_text(db, task),
                    reply_markup=task_admin_detail_keyboard(db, task),
                )
            return

        if action == "finish":
            if db.complete_reward_task(task_id):
                await query.answer("Задание завершено.", show_alert=True)
            else:
                await query.answer("Задание уже не активно.", show_alert=True)
            if query.message:
                updated_task = db.get_reward_task(task_id) or task
                await query.message.edit_text(
                    task_detail_text(db, updated_task),
                    reply_markup=task_admin_detail_keyboard(db, updated_task),
                )
            return

        if action == "participants":
            rows = db.get_reward_task_participant_rows(task_id)
            lines = [f"Участники задания #{task_id}:"]
            buttons = []
            if not rows:
                lines.append("Пока нет участников.")
            for row in rows[:30]:
                lines.append(f"{get_display_name(row)} - {task_status_label(row['status'])}")
                if row["status"] == "ready":
                    buttons.append(
                        [
                            InlineKeyboardButton(
                                f"Выдать {get_display_name(row)}",
                                callback_data=f"taskdone:{task_id}:{row['user_id']}",
                            )
                        ]
                    )
            await query.answer()
            if query.message:
                await query.message.edit_text("\n".join(lines), reply_markup=owner_panel_back_keyboard(*buttons))
            return

        await query.answer()
        return

    if action_group == "taskdone":
        if not has_owner_access(config, db, query.from_user.id):
            await query.answer()
            return
        try:
            task_id = int(parts[1])
            user_id = int(parts[2])
        except (IndexError, ValueError):
            await query.answer()
            return

        task = db.get_reward_task(task_id)
        participant = db.get_reward_task_participant(task_id, user_id)
        user_row = db.get_user_by_id(user_id)
        if not task or not participant or not user_row or participant["status"] != "ready":
            await query.answer("Нечего подтверждать.", show_alert=True)
            return

        user = user_identity_from_row(user_row)
        chat_id = int(participant["chat_id"] or task["chat_id"] or 0)
        if chat_id == 0 and len(config.allowed_chat_ids) == 1:
            chat_id = next(iter(config.allowed_chat_ids))
        _, task_closed = await complete_task_participant_with_payout(context, config, db, task, chat_id, user)
        text = f"Приз по заданию #{task_id} выдан игроку {get_display_name(user_row)}."
        if task_closed:
            text += "\nЗадание закрыто по количеству выполнений."
        await query.answer("Готово.", show_alert=True)
        if query.message:
            await query.message.edit_text(text, reply_markup=owner_panel_back_keyboard())
        return

    await query.answer()


async def handle_task_join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await handle_task_callback(update, context)


def format_luck_row(row: sqlite3.Row | None) -> str:
    if not row:
        return "пока нет данных"
    ratio = float(row["ratio"] or 0) * 100
    return f"{get_display_name(row)} - {ratio:.2f}% ({int(row['wins'] or 0)} из {int(row['attempts'] or 0)})"


async def show_luck_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    if update.effective_chat.type == "private":
        chat_ids = set(config.allowed_chat_ids)
        scope = "все разрешенные чаты"
    else:
        if not is_allowed_chat(config, update.effective_chat.id):
            return
        chat_ids = {update.effective_chat.id}
        scope = get_chat_label(update)

    min_spins = get_luck_min_spins(db)
    min_line = f"Минимум для рейтинга: {min_spins} спинов\n\n" if min_spins else ""
    await update.message.reply_text(
        f"Удача: {scope}\n\n"
        f"{min_line}"
        f"Самый везучий по 777:\n{format_luck_row(db.get_luck_rows(chat_ids, '777', True, min_spins))}\n\n"
        f"Самый невезучий по 777:\n{format_luck_row(db.get_luck_rows(chat_ids, '777', False, min_spins))}\n\n"
        f"Самый везучий по NFT-кнопкам:\n{format_luck_row(db.get_luck_rows(chat_ids, 'nfts', True, min_spins))}\n\n"
        f"Самый невезучий по NFT-кнопкам:\n{format_luck_row(db.get_luck_rows(chat_ids, 'nfts', False, min_spins))}"
    )


async def set_luck_min_spins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    if not has_owner_access(config, db, update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            f"Сейчас минимум для /luck: {get_luck_min_spins(db)} спинов.\n"
            "Изменить: /luckmin 300"
        )
        return

    try:
        min_spins = parse_integer_from_token(context.args[0], "минимум прокрутов")
        save_luck_min_spins(db, min_spins)
    except ValueError as error:
        await update.message.reply_text(str(error))
        return

    await update.message.reply_text(
        f"Готово. Теперь /luck считает игроков только от {get_luck_min_spins(db)} спинов."
    )


async def manage_notify_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    action = context.args[0].lower() if context.args else "list"
    user_ids = get_extra_notify_user_ids(db)

    if action in {"list", "список"}:
        lines = ["Дополнительные аккаунты уведомлений:"]
        if not user_ids:
            lines.append("пока нет")
        else:
            lines.extend(str(user_id) for user_id in sorted(user_ids))
        lines.append("")
        lines.append("/notify add USER_ID или @username")
        lines.append("/notify remove USER_ID или @username")
        await update.message.reply_text("\n".join(lines))
        return

    if action in {"add", "remove", "delete"} and len(context.args) >= 2:
        target_row = resolve_known_user(db, context.args[1])
        try:
            target_id = int(context.args[1].lstrip("=")) if target_row is None else int(target_row["user_id"])
        except ValueError:
            await update.message.reply_text("Не понял пользователя. Дай USER_ID или уже известный @username.")
            return

        if action == "add":
            user_ids.add(target_id)
            set_extra_notify_user_ids(db, user_ids)
            await update.message.reply_text(f"Добавлен аккаунт уведомлений: {target_id}")
        else:
            user_ids.discard(target_id)
            set_extra_notify_user_ids(db, user_ids)
            await update.message.reply_text(f"Удален аккаунт уведомлений: {target_id}")
        return

    await update.message.reply_text("Формат: /notify add USER_ID или /notify remove USER_ID")


async def manage_payout_chats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    if not has_owner_access(config, db, update.effective_user.id):
        return

    action = context.args[0].lower() if context.args else "list"
    chat_ids = get_payout_chat_ids(db)

    if action in {"list", "список"}:
        lines = ["Дополнительные чаты выдач:"]
        if not chat_ids:
            lines.append("пока нет")
        else:
            lines.extend(str(chat_id) for chat_id in sorted(chat_ids))
        lines.append("")
        lines.append("/payoutchat add CHAT_ID")
        lines.append("/payoutchat remove CHAT_ID")
        if update.effective_chat.type != "private":
            lines.append("")
            lines.append("В этом чате можно написать /payoutchat add без ID, и я добавлю текущий чат.")
        await update.message.reply_text("\n".join(lines))
        return

    if action in {"add", "remove", "delete"}:
        payload = " ".join(context.args[1:])
        if payload:
            try:
                chat_id = parse_integer_from_token(payload, "ID чата")
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        elif update.effective_chat.type != "private":
            chat_id = update.effective_chat.id
        else:
            await update.message.reply_text("Формат: /payoutchat add CHAT_ID или /payoutchat remove CHAT_ID")
            return

        if action == "add":
            chat_ids.add(chat_id)
            set_payout_chat_ids(db, chat_ids)
            await update.message.reply_text(f"Чат выдач добавлен: {chat_id}")
        else:
            chat_ids.discard(chat_id)
            set_payout_chat_ids(db, chat_ids)
            await update.message.reply_text(f"Чат выдач удален: {chat_id}")
        return

    await update.message.reply_text("Формат: /payoutchat add CHAT_ID или /payoutchat remove CHAT_ID")


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
    db.set_tournament_pending_approval(
        tournament["tournament_id"],
        result_payload,
        finished_at,
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
    db.set_referral_contest_pending_approval(
        contest["contest_id"],
        result_payload,
        finished_at,
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
    message = build_tournament_results_message(tournament, winners, total_tickets)
    await application.bot.send_message(chat_id=tournament["chat_id"], text=message)
    db.finish_tournament(
        tournament["tournament_id"],
        make_result_payload(winners, total_tickets),
        finished_at,
    )


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
            except Exception:
                logging.exception(
                    "Unexpected error while processing tournament %s",
                    tournament["tournament_id"],
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
            except Exception:
                logging.exception(
                    "Unexpected error while processing referral contest %s",
                    contest["contest_id"],
                )

        config: BotConfig = application.bot_data["config"]
        for reward_task in db.get_due_time_reward_tasks(datetime_to_storage(now)):
            try:
                if not db.complete_reward_task(reward_task["task_id"]):
                    continue
                text = (
                    f"Задание #{reward_task['task_id']} завершено по времени.\n\n"
                    f"{reward_task['title']}"
                )
                notify_ids = set(payout_admin_ids(config, db)) | {int(reward_task["created_by"])}
                for admin_id in sorted(notify_ids):
                    try:
                        await application.bot.send_message(chat_id=admin_id, text=text)
                    except TelegramError as error:
                        logging.warning("Failed to notify timed task admin %s: %s", admin_id, error)
            except Exception:
                logging.exception(
                    "Unexpected error while processing reward task %s",
                    reward_task["task_id"],
                )

        await asyncio.sleep(TOURNAMENT_LOOP_SECONDS)


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
    db: StatsDatabase = context.application.bot_data["db"]
    if (config.owner_user_ids or get_extra_notify_user_ids(db)) and not has_owner_access(config, db, update.effective_user.id):
        return

    remember_update_chat(update, db, config)

    await update.message.reply_text(
        f"ID этого чата: {update.effective_chat.id}\n"
        f"Твой user ID: {update.effective_user.id}"
    )


async def show_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    if (config.owner_user_ids or get_extra_notify_user_ids(db)) and not has_owner_access(config, db, update.effective_user.id):
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
    db: StatsDatabase = context.application.bot_data["db"]
    user_id = update.effective_user.id
    owner_ids = ", ".join(str(owner_id) for owner_id in sorted(config.owner_user_ids)) or "не заданы"
    extra_ids = ", ".join(str(admin_id) for admin_id in sorted(get_extra_notify_user_ids(db))) or "не заданы"
    status = "да" if has_owner_access(config, db, user_id) else "нет"

    await update.message.reply_text(
        f"Твой Telegram user ID: {user_id}\n"
        f"OWNER_USER_IDS распознаны ботом: {owner_ids}\n"
        f"Доп. админы из /notify: {extra_ids}\n"
        f"У тебя полный owner-доступ: {status}\n\n"
        "Если доступа нет, добавь user ID в OWNER_USER_IDS или через /notify add."
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
        if context.args:
            target_row = resolve_known_user(db, context.args[0])
            if not target_row:
                await update.message.reply_text(
                    "Я пока не знаю такого пользователя. Он должен хотя бы раз написать в чат или сделать спин."
                )
                return

            target_user = user_identity_from_row(target_row)
            row = db.get_user_aggregate_stats(target_user.id, config.allowed_chat_ids)
            stats = row if row else empty_user_stats(target_user)
            message, entities_data = build_personal_stats_message(db, target_user, stats)
            await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
            return

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


async def show_full_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    if update.effective_chat.type != "private":
        await update.message.reply_text("Полную статистику можно смотреть в личке с ботом.")
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]
    if not config.allowed_chat_ids:
        await update.message.reply_text("ALLOWED_CHAT_IDS пустой, статистику показать не из чего.")
        return

    chat_ids = set(config.allowed_chat_ids)
    scope_label = "все разрешенные чаты"
    if context.args:
        try:
            chat_id = parse_integer_from_token(context.args[0], "ID чата")
        except ValueError as error:
            await update.message.reply_text(str(error))
            return

        if not is_allowed_chat(config, chat_id):
            await update.message.reply_text("Этот чат не указан в ALLOWED_CHAT_IDS.")
            return

        chat_ids = {chat_id}
        known_rows = db.get_known_chats({chat_id})
        scope_label = get_chat_row_label(known_rows[0]) if known_rows else str(chat_id)

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        own_stats = db.get_user_aggregate_stats(update.effective_user.id, chat_ids)
        own_spins = stats_value(own_stats, "total_spins") if own_stats else 0
        if own_spins < 50:
            await update.message.reply_text(
                f"/fullstat доступна от 50 прокрутов.\n"
                f"Сейчас у тебя: {own_spins}."
            )
            return

    rows = db.get_full_user_stat_rows(chat_ids)
    await reply_long_text(update.message, build_full_stats_text(db, rows, scope_label))


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

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    if update.effective_chat.type != "private":
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
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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


async def show_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    db.remember_private_subscriber(update.effective_user, update.effective_chat.id)
    await update.message.reply_text(
        f"Баланс:\n\n"
        f"TEM: {db.get_tem_balance(update.effective_user.id)}\n"
        f"Stars: {db.get_stars_balance(update.effective_user.id)}⭐\n\n"
        f"Вывод Stars доступен от {MIN_STARS_WITHDRAW}⭐: /withdraw"
    )


async def withdraw_stars(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    config: BotConfig = context.application.bot_data["config"]
    db.remember_private_subscriber(update.effective_user, update.effective_chat.id)

    balance = db.get_stars_balance(update.effective_user.id)
    if balance < MIN_STARS_WITHDRAW:
        await update.message.reply_text(
            f"Вывод доступен от {MIN_STARS_WITHDRAW}⭐.\n"
            f"Сейчас на балансе: {balance}⭐."
        )
        return

    amount = balance
    if context.args:
        try:
            amount = parse_integer_from_token(context.args[0], "сумма вывода")
        except ValueError as error:
            await update.message.reply_text(str(error))
            return

    if amount < MIN_STARS_WITHDRAW:
        await update.message.reply_text(f"Минимальный вывод: {MIN_STARS_WITHDRAW}⭐.")
        return
    if amount > balance:
        await update.message.reply_text(f"Недостаточно Stars. Баланс: {balance}⭐.")
        return

    if not db.reserve_stars_withdraw(update.effective_user.id, amount):
        await update.message.reply_text("Не удалось зарезервировать Stars. Попробуй еще раз.")
        return

    remaining_balance = db.get_stars_balance(update.effective_user.id)
    rendered_withdraw = render_message_template(
        db,
        "withdraw_request",
        {
            "username": get_user_display_name(update.effective_user),
            "stars": amount,
            "amount": amount,
            "withdraw_amount": amount,
            "stars_balance": remaining_balance,
            "balance": remaining_balance,
            "withdraw_min": MIN_STARS_WITHDRAW,
        },
    )
    if rendered_withdraw:
        withdraw_text, withdraw_entities = rendered_withdraw
    else:
        withdraw_text = (
            f"Запрос на вывод {amount}⭐ отправлен.\n\n"
            "Stars зарезервированы до подтверждения выплаты. "
            "Если возникнет проблема, можно открыть спор кнопкой ниже."
        )
        withdraw_entities = []

    await create_and_notify_payout(
        context,
        config,
        db,
        update.effective_chat.id,
        update.effective_user,
        "stars_withdraw",
        "withdraw",
        None,
        f"Вывод {amount}⭐",
        winner_text=withdraw_text,
        winner_entities_data=withdraw_entities,
    )
    await update.message.reply_text(
        f"Запрос на вывод {amount}⭐ отправлен owner.\n"
        f"Остаток баланса: {remaining_balance}⭐."
    )


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
    db: StatsDatabase = context.application.bot_data["db"]
    if (config.owner_user_ids or get_extra_notify_user_ids(db)) and not has_owner_access(config, db, update.effective_user.id):
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

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    extracted = extract_template_from_update(update)
    if not extracted:
        await update.message.reply_text(
            "Формат:\n"
            "/settext 777 текст\n"
            "/settext 777progress текст\n"
            "/settext 777buttons текст\n"
            "/settext buttonsnft текст\n"
            "/settext buttonsmiss текст\n"
            "/settext buttonsempty текст\n"
            "/settext buttonsnogift текст\n"
            "/settext 77x текст\n"
            "/settext triple текст\n"
            "/settext triplenogift текст\n"
            "/settext tripleprogress текст\n"
            "/settext stats текст\n"
            "/settext mystats текст\n\n"
            "/settext welcome текст\n\n"
            "/settext milestone текст\n\n"
            "/settext dailybonus текст\n\n"
            "/settext dailybonuswait текст\n\n"
            "/settext dailyreminder текст\n\n"
            "/settext chance текст\n\n"
            "/settext scam текст\n"
            "/settext scam50 текст\n"
            "/settext starbalance текст\n"
            "/settext withdraw текст\n"
            "/settext payoutadmin текст\n"
            "/settext payoutuser текст\n"
            "/settext payoutdone текст\n"
            "/settext payoutchat текст\n"
            "/settext milestone50 текст\n\n"
            "Для длинного текста: отправьте сообщение-шаблон и ответьте на него /settext 777."
        )
        return

    template_key, template_text, entities_data = extracted
    db: StatsDatabase = context.application.bot_data["db"]
    db.set_message_template(template_key, template_text, entities_data)

    custom_emoji_count = count_custom_emoji_entities(entities_data)
    template_label = TEMPLATE_LABELS.get(template_key, template_key)
    await update.message.reply_text(
        f"Шаблон для {template_label} сохранен.\n\n"
        f"Telegram custom emoji сохранено: {custom_emoji_count}\n\n"
        "Доступные placeholders:\n"
        "username - имя победителя\n"
        "nft_url - ссылка на случайный gift owner\n"
        "giftr - роза/мишка/сердце/подарок\n"
        "triplenogift - текст для три в ряд без выдачи giftr\n"
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
        "stars_balance - баланс Stars\n"
        "stars/amount - количество Stars\n"
        "withdraw_amount - сумма вывода Stars\n"
        "withdraw_min - минимальная сумма вывода\n"
        "payout_id - ID выплаты\n"
        "payout_type/source_type - тип выплаты/источник\n"
        "prize/prize_text - название выплаты или приза\n"
        "user_id/chat_id - ID пользователя/чата\n"
        "daily_bonus/bonus - ежедневный бонус TEM\n"
        "chance/chance_percent - примерный шанс 777\n"
        "progress_count - текущий прогресс до выдачи приза\n"
        "remaining - сколько осталось до выдачи приза\n"
        "needed - сколько всего нужно для выдачи приза\n"
        "rank - текущий ранг\n"
        "rank_points - очки ранга\n"
        "spin_price - текущая цена прокрута в Stars\n"
        "rank_points_gain - сколько очков ранга дает один прокрут\n"
        "stars_box_min/star_prize_min - цена прокрута, с которой в коробках есть 15/25 Stars\n"
        "nft_chance/nft_chance_denominator - шанс NFT в 777-кнопках\n"
        "button_count - сколько кнопок в 777-боксе\n"
        "next_button_count - сколько кнопок будет в следующем 777-боксе\n"
        "selected_box - номер выбранной кнопки\n"
        "small_prize/button_prize - приз 15/25 Stars"
    )


async def set_help_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    extracted = extract_help_template_from_update(update)
    if not extracted:
        await update.message.reply_text(
            "Формат:\n"
            "/sethelp текст помощи\n\n"
            "Для Telegram custom emoji отправьте сообщение с нужным оформлением и ответьте на него /sethelp.\n\n"
            "Placeholders: username, balance, tem_balance, stars_balance, total_spins, rank, rank_points"
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

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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
        lines.append(f"{index * 100}+ очков ранга: {rank['text']}")
    lines.append(f"Telegram custom emoji сохранено: {custom_emoji_count}")

    await update.message.reply_text("\n".join(lines))


async def set_user_rank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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


async def manage_gift_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    command_name = (update.message.text or "").split(maxsplit=1)[0].split("@", 1)[0].lower()
    default_action = "bank" if command_name == "/giftbank" else "list"
    action = context.args[0].lower() if context.args else default_action

    if action in {"list", "список"}:
        blocked_tokens = get_blocked_gift_tokens(db)
        if not blocked_tokens:
            await update.message.reply_text(
                "Заблокированных gifts сейчас нет.\n\n"
                "Добавить: /giftblock add ссылка_или_название\n"
                "Посмотреть банк: /giftblock bank"
            )
            return

        lines = ["Заблокированные gifts:"]
        for index, token in enumerate(blocked_tokens, start=1):
            lines.append(f"{index}. {token}")
        lines.append("")
        lines.append("Удалить из блока: /giftblock remove ссылка_или_название")
        await update.message.reply_text("\n".join(lines))
        return

    if action in {"add", "block", "добавить", "заблокировать"}:
        payload = extract_text_payload_after_prefix(update.message.text or "", 2)
        if not payload:
            await update.message.reply_text(
                "Формат:\n"
                "/giftblock add https://t.me/nft/...\n"
                "или /giftblock add Diamond Ring #123\n\n"
                "Точный список банка: /giftblock bank"
            )
            return

        token_text, _ = payload
        token = normalize_gift_token(token_text)
        blocked_tokens = get_blocked_gift_tokens(db)
        if token not in blocked_tokens:
            blocked_tokens.append(token)
            set_blocked_gift_tokens(db, blocked_tokens)

        await update.message.reply_text(
            f"Gift заблокирован для выдачи: {token}\n\n"
            "Он может оставаться в банке owner, но бот не будет выбирать его при 777."
        )
        return

    if action in {"remove", "unblock", "delete", "убрать", "разблокировать"}:
        payload = extract_text_payload_after_prefix(update.message.text or "", 2)
        if not payload:
            await update.message.reply_text("Формат: /giftblock remove ссылка_или_название")
            return

        token_text, _ = payload
        token = normalize_gift_token(token_text)
        blocked_tokens = [item for item in get_blocked_gift_tokens(db) if item != token]
        set_blocked_gift_tokens(db, blocked_tokens)
        await update.message.reply_text(f"Gift убран из блокировки: {token}")
        return

    if action in {"clear", "очистить"}:
        set_blocked_gift_tokens(db, [])
        await update.message.reply_text("Список заблокированных gifts очищен.")
        return

    if action in {"bank", "банк", "gifts"}:
        try:
            owned_gifts = await asyncio.to_thread(
                fetch_owner_gifts,
                config.token,
                config.owner_user_ids,
            )
        except RuntimeError as error:
            await update.message.reply_text(f"Не смог получить gifts owner: {error}")
            return

        gift_cards = [card for gift in owned_gifts if (card := extract_gift_card(gift))]
        blocked_tokens = set(get_blocked_gift_tokens(db))
        if not gift_cards:
            await update.message.reply_text("Банк owner пуст или gifts скрыты.")
            return

        lines = ["Банк owner gifts:"]
        for index, card in enumerate(gift_cards, start=1):
            blocked = "заблокирован" if is_gift_blocked(card, blocked_tokens) else "доступен"
            block_value = card["url"] or card["name"] or card["gift_id"] or card["title"]
            lines.append("")
            lines.append(f"{index}. {card['title']} - {blocked}")
            if card["url"]:
                lines.append(card["url"])
            lines.append(f"Заблокировать: /giftblock add {block_value}")

        await reply_long_text(update.message, "\n".join(lines))
        return

    await update.message.reply_text(
        "Команды giftblock:\n"
        "/giftblock list - список заблокированных\n"
        "/giftblock bank - показать gifts owner\n"
        "/giftblock add ссылка_или_название - запретить выдачу\n"
        "/giftblock remove ссылка_или_название - вернуть выдачу\n"
        "/giftblock clear - очистить блокировку"
    )


def game_settings_text(db: StatsDatabase) -> str:
    mode = get_game_mode(db)
    spin_price = get_spin_price_stars(db)
    rank_gain = get_rank_points_per_spin(db)
    return (
        "Настройки игры\n\n"
        f"Режим: {game_mode_label(mode)}\n"
        f"Цена прокрута: {spin_price}⭐\n"
        f"Очки ранга за прокрут: +{format_rank_points(rank_gain)}\n\n"
        f"777-кнопки: старт {get_jackpot_buttons_start(db)}, "
        f"минимум {get_jackpot_buttons_min(db)}, "
        f"минус {get_jackpot_buttons_decrease(db)} за промах.\n"
        f"Шанс NFT в 777-кнопках: 1/{get_jackpot_button_nft_chance_denominator(db)}\n"
        f"15/25⭐ в коробках: от {get_jackpot_button_stars_min_price(db)}⭐ за прокрут.\n\n"
        "Формула ранга: 1 очко = 5⭐.\n"
        "Любая цена: /game price 37\n"
        "Любое число кнопок: /game buttons 12\n"
        "Шанс NFT: /game nftchance 18\n"
        "Порог Stars в коробках: /game starsmin 1"
    )


def game_settings_keyboard(db: StatsDatabase) -> InlineKeyboardMarkup:
    mode = get_game_mode(db)
    spin_price = get_spin_price_stars(db)
    mode_buttons = [
        InlineKeyboardButton(
            f"{'✓ ' if mode == GAME_MODE_CLASSIC else ''}Классика",
            callback_data=f"game:mode:{GAME_MODE_CLASSIC}",
        ),
        InlineKeyboardButton(
            f"{'✓ ' if mode == GAME_MODE_JACKPOT_BUTTONS else ''}777-кнопки",
            callback_data=f"game:mode:{GAME_MODE_JACKPOT_BUTTONS}",
        ),
    ]
    price_options = list(GAME_PRICE_PRESETS)
    if spin_price not in price_options:
        price_options.insert(0, spin_price)

    price_buttons = [
        InlineKeyboardButton(
            f"{'✓ ' if spin_price == price else ''}{price}⭐",
            callback_data=f"game:price:{price}",
        )
        for price in price_options
    ]
    rows = [mode_buttons]
    rows.extend(price_buttons[index:index + 3] for index in range(0, len(price_buttons), 3))
    rows.append(
        [
            InlineKeyboardButton(
                "Другая цена",
                callback_data="game:custom_price:0",
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


async def manage_game_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    if update.effective_chat.type != "private":
        return

    db: StatsDatabase = context.application.bot_data["db"]
    if context.args:
        action = context.args[0].lower()
        if action in {"classic", "old", "классика", "обычный"} and len(context.args) == 1:
            db.set_bot_setting(GAME_MODE_SETTING, GAME_MODE_CLASSIC)
        elif action in {"buttons", "button", "777", "кнопки"} and len(context.args) == 1:
            db.set_bot_setting(GAME_MODE_SETTING, GAME_MODE_JACKPOT_BUTTONS)
        elif action in {"mode", "режим"} and len(context.args) >= 2:
            requested_mode = context.args[1].lower()
            if requested_mode in {"classic", "old", "классика", "обычный"}:
                db.set_bot_setting(GAME_MODE_SETTING, GAME_MODE_CLASSIC)
            elif requested_mode in {"buttons", "button", "777", "кнопки"}:
                db.set_bot_setting(GAME_MODE_SETTING, GAME_MODE_JACKPOT_BUTTONS)
            else:
                await update.message.reply_text("Режимы: classic или buttons.")
                return
        elif action in {"price", "stars", "цена"} and len(context.args) >= 2:
            try:
                price = parse_integer_from_token(context.args[1], "цена прокрута")
                save_spin_price_stars(db, price)
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        elif action in {"buttons", "button", "boxes", "box", "кнопки"} and len(context.args) >= 2:
            try:
                button_count = parse_integer_from_token(context.args[1], "количество кнопок")
                save_jackpot_button_settings(db, start_count=button_count)
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        elif action in {"minbuttons", "minbutton", "minboxes", "минимум"} and len(context.args) >= 2:
            try:
                min_count = parse_integer_from_token(context.args[1], "минимум кнопок")
                save_jackpot_button_settings(db, min_count=min_count)
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        elif action in {"decrease", "minus", "минус"} and len(context.args) >= 2:
            try:
                decrease = parse_integer_from_token(context.args[1], "уменьшение кнопок")
                save_jackpot_button_settings(db, decrease=decrease)
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        elif action in {"nftchance", "chance_nft", "nft", "nftшанс", "шанснфт"} and len(context.args) >= 2:
            try:
                denominator = parse_integer_from_token(context.args[1], "шанс NFT 1/N")
                save_jackpot_button_settings(db, nft_chance_denominator=denominator)
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        elif action in {
            "starsmin",
            "starmin",
            "boxstars",
            "starsboxes",
            "prizemin",
            "giftmin",
            "порог",
            "звезды",
        } and len(context.args) >= 2:
            try:
                stars_min_price = parse_integer_from_token(
                    " ".join(context.args[1:]),
                    "порог Stars в коробках",
                )
                save_jackpot_button_settings(db, stars_min_price=stars_min_price)
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        elif len(context.args) == 1:
            try:
                price = parse_integer_from_token(context.args[0], "цена прокрута")
                save_spin_price_stars(db, price)
            except ValueError as error:
                await update.message.reply_text(str(error))
                return
        else:
            await update.message.reply_text(
                "Формат:\n"
                "/game\n"
                "/game mode classic\n"
                "/game mode buttons\n"
                "/game price 37\n"
                "/game 37\n"
                "/game buttons 12\n"
                "/game minbuttons 4\n"
                "/game decrease 1\n"
                "/game nftchance 18\n"
                "/game starsmin 1"
            )
            return

    await update.message.reply_text(
        game_settings_text(db),
        reply_markup=game_settings_keyboard(db),
    )


async def manage_milestones(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    db: StatsDatabase = context.application.bot_data["db"]
    if not context.args or context.args[0].lower() in {"list", "show", "список"}:
        await update.message.reply_text(
            "Настройки рубежей:\n\n"
            f"Индивидуальные: {', '.join(map(str, get_user_milestone_values(db)))}\n"
            f"Общие по чату: {', '.join(map(str, get_chat_milestone_values(db)))}\n"
            f"Антискам: каждые {get_scam_warning_interval(db)} спинов\n\n"
            "Изменить:\n"
            "/milestones user 25,50,100\n"
            "/milestones chat 100,500\n"
            "/milestones scam 50\n\n"
            "Тексты:\n"
            "/settext milestone50 текст\n"
            "/settext scam50 текст"
        )
        return

    setting = context.args[0].lower()
    payload = " ".join(context.args[1:])
    values = parse_positive_int_list(payload, [])
    if not values:
        await update.message.reply_text("Укажи числа через запятую. Например: /milestones user 25,50,100")
        return

    if setting in {"user", "users", "personal", "личные"}:
        db.set_bot_setting(MILESTONE_USER_VALUES_SETTING, values)
        await update.message.reply_text(f"Индивидуальные рубежи сохранены: {', '.join(map(str, values))}")
        return

    if setting in {"chat", "global", "общие", "чат"}:
        db.set_bot_setting(MILESTONE_CHAT_VALUES_SETTING, values)
        await update.message.reply_text(f"Общие рубежи чата сохранены: {', '.join(map(str, values))}")
        return

    if setting in {"scam", "anti", "антискам", "скам"}:
        db.set_bot_setting(SCAM_WARNING_INTERVAL_SETTING, values[0])
        await update.message.reply_text(f"Антискам-напоминание будет каждые {values[0]} спинов.")
        return

    await update.message.reply_text("Формат: /milestones user 25,50,100 или /milestones scam 50")


async def handle_game_settings_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], query.from_user.id):
        await query.answer()
        return

    parts = query.data.split(":")
    if len(parts) != 3 or parts[0] != "game":
        await query.answer()
        return

    db: StatsDatabase = context.application.bot_data["db"]
    setting_type, value = parts[1], parts[2]
    if setting_type == "mode":
        if value not in {GAME_MODE_CLASSIC, GAME_MODE_JACKPOT_BUTTONS}:
            await query.answer()
            return
        db.set_bot_setting(GAME_MODE_SETTING, value)
    elif setting_type == "price":
        try:
            price = int(value)
            save_spin_price_stars(db, price)
        except ValueError:
            await query.answer("Цена должна быть 1⭐ или выше.", show_alert=True)
            return
    elif setting_type == "custom_price":
        await query.answer(
            "Напиши в личке: /game price 37",
            show_alert=True,
        )
        return
    else:
        await query.answer()
        return

    await query.answer("Сохранено.", show_alert=True)
    try:
        await query.message.edit_text(
            game_settings_text(db),
            reply_markup=game_settings_keyboard(db),
        )
    except TelegramError:
        await query.message.reply_text(
            game_settings_text(db),
            reply_markup=game_settings_keyboard(db),
        )


async def show_message_templates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if update.effective_chat.type != "private":
        return

    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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
        "/settext 777buttons",
        "/settext buttonsnft",
        "/settext buttonsmiss",
        "/settext buttonsempty",
        "/settext buttonsnogift",
        "/settext 77x",
        "/settext triple",
        "/settext triplenogift",
        "/settext tripleprogress",
        "/settext stats",
        "/settext mystats",
        "/settext welcome",
        "/settext milestone",
        "/settext dailybonus",
        "/settext dailybonuswait",
        "/settext dailyreminder",
        "/settext chance",
        "/settext scam",
        "/settext scam50",
        "/settext starbalance",
        "/settext withdraw",
        "/settext payoutadmin",
        "/settext payoutuser",
        "/settext payoutdone",
        "/settext payoutchat",
        "/settext milestone50",
        "/milestones",
        "/sethelp",
        "/setranks",
        "/setuserrank USER_ID",
        f"/setchance perc{get_chance_multiplier(db):g} spins{get_chance_average_spins(db)}",
        f"/game mode {get_game_mode(db)}",
        f"/game price {get_spin_price_stars(db)}",
        "/game price любое_число",
        "",
    ]
    rank_names = db.get_rank_names()
    lines.append("Ранги:")
    for index, rank in enumerate(rank_names):
        lines.append(f"{index * 100}+ очков ранга: {rank}")
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
    if not has_owner_access(config, context.application.bot_data["db"], query.from_user.id):
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
            lines.append(f"{index * 100}+ очков ранга: {rank}")
        lines.append("")
        lines.append("Изменить: /setranks ранг1, ранг2, ранг3")
        await query.message.reply_text("\n".join(lines))


def owner_panel_back_keyboard(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    keyboard_rows = list(rows)
    keyboard_rows.append([InlineKeyboardButton("Назад в owner-панель", callback_data="ownerpanel:main")])
    return InlineKeyboardMarkup(keyboard_rows)


def owner_panel_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Игра", callback_data="ownerpanel:game"),
                InlineKeyboardButton("Тексты", callback_data="ownerpanel:texts"),
            ],
            [
                InlineKeyboardButton("Статистика", callback_data="ownerpanel:stats"),
                InlineKeyboardButton("Турниры", callback_data="ownerpanel:tournaments"),
            ],
            [
                InlineKeyboardButton("Рефералы", callback_data="ownerpanel:refcontests"),
                InlineKeyboardButton("Задания", callback_data="ownerpanel:tasks"),
            ],
            [
                InlineKeyboardButton("Gifts и выплаты", callback_data="ownerpanel:gifts"),
                InlineKeyboardButton("Ранги и рубежи", callback_data="ownerpanel:ranks"),
            ],
            [
                InlineKeyboardButton("Система", callback_data="ownerpanel:system"),
                InlineKeyboardButton("Все команды", callback_data="ownerpanel:commands"),
            ],
            [InlineKeyboardButton("Обновить", callback_data="ownerpanel:main")],
        ]
    )


def owner_panel_wrap_keyboard(
    keyboard: InlineKeyboardMarkup | None = None,
    *rows: list[InlineKeyboardButton],
) -> InlineKeyboardMarkup:
    keyboard_rows = list(keyboard.inline_keyboard) if keyboard else []
    keyboard_rows.extend(rows)
    keyboard_rows.append([InlineKeyboardButton("Назад в owner-панель", callback_data="ownerpanel:main")])
    return InlineKeyboardMarkup(keyboard_rows)


def owner_panel_status_text(config: BotConfig, db: StatsDatabase) -> str:
    active_tasks = [task for task in db.get_reward_tasks() if task["status"] == "active"]
    open_tournaments = db.get_open_tournaments()
    active_refcontests = db.get_active_referral_contests()
    templates_count = len(db.get_message_templates())
    user_templates_count = len(db.get_user_message_templates())
    blocked_gifts_count = len(get_blocked_gift_tokens(db))
    notify_count = len(get_extra_notify_user_ids(db))
    payout_chats_count = len(get_payout_chat_ids(db))
    known_chats = db.get_known_chats(config.allowed_chat_ids)

    return (
        "Owner-панель\n\n"
        f"Режим игры: {game_mode_label(get_game_mode(db))}\n"
        f"Цена прокрута: {get_spin_price_stars(db)}⭐\n"
        f"Очки ранга за прокрут: +{format_rank_points(get_rank_points_per_spin(db))}\n"
        f"777-кнопки: {get_jackpot_buttons_start(db)} старт, "
        f"{get_jackpot_buttons_min(db)} минимум, "
        f"-{get_jackpot_buttons_decrease(db)} за промах\n"
        f"Шанс NFT в 777-кнопках: 1/{get_jackpot_button_nft_chance_denominator(db)}\n"
        f"15/25⭐ в коробках: от {get_jackpot_button_stars_min_price(db)}⭐ за прокрут\n\n"
        f"Минимум для /luck: {get_luck_min_spins(db)} спинов\n\n"
        f"Разрешенных чатов: {len(config.allowed_chat_ids)}\n"
        f"Известных чатов в базе: {len(known_chats)}\n"
        f"Активных слот-турниров/черновиков: {len(open_tournaments)}\n"
        f"Активных referral-конкурсов: {len(active_refcontests)}\n"
        f"Активных заданий: {len(active_tasks)}\n\n"
        f"Шаблонов общих: {templates_count}\n"
        f"Шаблонов индивидуальных: {user_templates_count}\n"
        f"Заблокированных gifts: {blocked_gifts_count}\n"
        f"Доп. аккаунтов уведомлений: {notify_count}\n"
        f"Чатов выдач: {payout_chats_count}\n\n"
        "Выбери раздел кнопками ниже."
    )


def owner_panel_texts_overview(db: StatsDatabase) -> str:
    templates = db.get_message_templates()
    user_templates = db.get_user_message_templates()
    lines = [
        "Тексты и оформление",
        "",
        "Главные команды:",
        "/texts - таблица всех сохраненных текстов с кнопками просмотра",
        "/settext 777 текст",
        "/settext 77x текст",
        "/settext triple текст",
        "/settext stats текст",
        "/settext mystats текст",
        "/settext welcome текст",
        "/settext milestone50 текст",
        "/settext scam50 текст",
        "/settext starbalance текст",
        "/settext withdraw текст",
        "/settext payoutadmin текст",
        "/settext payoutuser текст",
        "/settext payoutdone текст",
        "/settext payoutchat текст",
        "/sethelp текст",
        "/setusertext USER_ID mystats текст",
        "",
        f"Сейчас общих шаблонов: {len(templates)}",
        f"Индивидуальных шаблонов: {len(user_templates)}",
        "",
        "Чтобы сохранить Telegram custom emoji, отправь боту готовый текст и ответь на него командой /settext нужный_тип.",
    ]
    return "\n".join(lines)


def owner_panel_stats_overview(config: BotConfig, db: StatsDatabase) -> str:
    known_chats = db.get_known_chats(config.allowed_chat_ids)
    chat_lines = []
    for row in known_chats[:10]:
        totals = db.get_chat_totals(row["chat_id"])
        chat_lines.append(
            f"{get_chat_row_label(row)}: {stats_value(totals, 'total_spins')} спинов"
        )
    if len(known_chats) > 10:
        chat_lines.append(f"...и еще {len(known_chats) - 10} чатов")

    return (
        "Статистика и база\n\n"
        "Команды просмотра:\n"
        "/stats - общая статистика в чате\n"
        "/fullstat - полная статистика в личке\n"
        "/mystats @username - чужая личная статистика\n"
        "/luck - удача/невезение\n"
        "/tickets ID - билеты турнира\n\n"
        "Команды управления:\n"
        "/luckmin 300 - минимум спинов для рейтинга удачи/невезения\n"
        "/resetstats - обнулить статистику текущего чата\n"
        "/resetuserstats USER_ID - обнулить игрока в текущем чате\n"
        "/hiderating @username - убрать из рейтинга\n"
        "/showrating @username - вернуть в рейтинг\n\n"
        "Известные чаты:\n"
        + ("\n".join(chat_lines) if chat_lines else "Пока нет известных чатов.")
    )


def owner_panel_tournaments_overview(db: StatsDatabase) -> str:
    tournaments = db.get_open_tournaments()
    lines = [
        "Слот-турниры",
        "",
        "Запуск: ответь на пост турнира командой:",
        "/tournament start CHAT_ID hours72 3",
        "и ниже добавь ссылки на призы.",
        "",
        "Управление:",
        "/tournament status",
        "/tournament participants ID",
        "/tournament edit ID time hours12",
        "/tournament edit ID gifts",
        "/tournament edit ID places 3",
        "/tournament edit ID text",
        "/tournament winners ID @user1 @user2",
        "/tournament stop ID",
        "",
        f"Открытых турниров/черновиков: {len(tournaments)}",
    ]
    for tournament in tournaments[:8]:
        lines.append(
            f"#{tournament['tournament_id']} чат {tournament['chat_id']} "
            f"статус {tournament['status']}"
        )
    return "\n".join(lines)


def owner_panel_refcontests_overview(db: StatsDatabase) -> str:
    contests = db.get_active_referral_contests()
    lines = [
        "Referral-конкурсы",
        "",
        "Запуск: ответь на текст конкурса командой:",
        "/refcontest start days7 3",
        "или /refcontest start people100 3",
        "и ниже добавь ссылки на призы.",
        "",
        "Управление:",
        "/refcontest status",
        "/refcontest participants ID",
        "/refcontest edit ID time hours12",
        "/refcontest edit ID time people100",
        "/refcontest edit ID gifts",
        "/refcontest edit ID places 3",
        "/refcontest edit ID text",
        "/refcontest winners ID @user1 @user2",
        "/refcontest stop ID",
        "",
        f"Активных referral-конкурсов: {len(contests)}",
    ]
    for contest in contests[:8]:
        lines.append(
            f"#{contest['contest_id']} - {format_referral_contest_finish_rule(contest)}"
        )
    return "\n".join(lines)


def owner_panel_tasks_overview(db: StatsDatabase) -> str:
    tasks = db.get_reward_tasks(include_inactive=True)
    active_count = sum(1 for task in tasks if task["status"] == "active")
    lines = [
        "Задания",
        "",
        "Создание:",
        "/task add all spins 500 nft minspins300 Название",
        "/task add optin 777 3 stars25 manual Название",
        "/task add all spins 100 nft time hours72 Название",
        "/task add all spins 50 stars25 people10 Название",
        "/task add users @user1,@user2 spins 100 nft Название",
        "",
        "Управление:",
        "/task list",
        "/task active",
        "/task completed",
        "/task requests",
        "/task finish ID",
        "/task stop ID",
        "",
        f"Активных заданий: {active_count}",
        f"Всего заданий в базе: {len(tasks)}",
    ]
    for task in tasks[:8]:
        min_text = f", minspins {int(task['min_spins'])}" if int(task["min_spins"] or 0) else ""
        finish_text = f", {task['completion_mode']}"
        if task["max_completions"]:
            finish_text += f" {db.count_completed_reward_task_participants(task['task_id'])}/{task['max_completions']}"
        lines.append(
            f"#{task['task_id']} {task['status']} {task['scope']} "
            f"{task['metric']} {task['goal']} -> {task['prize_type']}{min_text}{finish_text}"
        )
    return "\n".join(lines)


def owner_panel_gifts_overview(db: StatsDatabase) -> str:
    blocked = get_blocked_gift_tokens(db)
    notify_ids = sorted(get_extra_notify_user_ids(db))
    payout_chats = sorted(get_payout_chat_ids(db))
    lines = [
        "Gifts, выплаты и уведомления",
        "",
        "Банк и блокировка gifts:",
        "/giftbank - показать банк owner",
        "/giftblock list",
        "/giftblock add ссылка_или_название",
        "/giftblock remove ссылка_или_название",
        "/giftblock clear",
        "",
        "Уведомления о выигрышах:",
        "/notify - список доп. аккаунтов",
        "/notify add USER_ID или @username",
        "/notify remove USER_ID или @username",
        "",
        "Чаты выдач:",
        "/payoutchat - список",
        "/payoutchat add CHAT_ID",
        "/payoutchat remove CHAT_ID",
        "",
        f"Заблокировано gifts: {len(blocked)}",
        f"Доп. аккаунты: {', '.join(map(str, notify_ids)) if notify_ids else 'нет'}",
        f"Чаты выдач: {', '.join(map(str, payout_chats)) if payout_chats else 'нет'}",
    ]
    if blocked:
        lines.append("")
        lines.append("Первые блокировки:")
        lines.extend(f"- {token}" for token in blocked[:8])
    return "\n".join(lines)


def owner_panel_ranks_overview(db: StatsDatabase) -> str:
    rank_names = db.get_rank_names()
    lines = [
        "Ранги, шансы и рубежи",
        "",
        f"Множитель шанса: perc{get_chance_multiplier(db):g}",
        f"Средняя частота подсказки шанса: раз в {get_chance_average_spins(db)} спинов",
        f"Индивидуальные рубежи: {', '.join(map(str, get_user_milestone_values(db)))}",
        f"Общие рубежи чата: {', '.join(map(str, get_chat_milestone_values(db)))}",
        f"Антискам: каждые {get_scam_warning_interval(db)} спинов",
        "",
        "Команды:",
        "/setranks ранг1, ранг2, ранг3",
        "/setuserrank USER_ID ранг",
        "/setchance perc10 spins5",
        "/milestones user 25,50,100",
        "/milestones chat 100,500",
        "/milestones scam 50",
        "",
        "Текущие ранги:",
    ]
    for index, rank in enumerate(rank_names[:20]):
        lines.append(f"{index * 100}+ очков: {rank}")
    return "\n".join(lines)


def owner_panel_system_overview(config: BotConfig, db: StatsDatabase) -> str:
    allowed_ids = ", ".join(str(chat_id) for chat_id in sorted(config.allowed_chat_ids)) or "не заданы"
    owner_ids = ", ".join(str(user_id) for user_id in sorted(config.owner_user_ids)) or "не заданы"
    extra_admin_ids = ", ".join(str(user_id) for user_id in sorted(get_extra_notify_user_ids(db))) or "не заданы"
    known_chats = db.get_known_chats(config.allowed_chat_ids)
    return (
        "Система и диагностика\n\n"
        f"OWNER_USER_IDS: {owner_ids}\n"
        f"Доп. админы из /notify: {extra_admin_ids}\n"
        f"ALLOWED_CHAT_IDS: {allowed_ids}\n"
        f"Путь базы: {config.db_path}\n"
        f"Известных разрешенных чатов: {len(known_chats)}\n\n"
        "Команды:\n"
        "/ownercheck - проверить, видит ли бот тебя owner\n"
        "/chatid - узнать ID чата\n"
        "/userid - узнать свой user ID\n"
        "/emojiid - ответом на сообщение узнать custom emoji ID\n"
        "/texts - посмотреть все шаблоны и настройки\n"
        "/game - настройки игры"
    )


def owner_panel_commands_text() -> str:
    return (
        "Все owner-команды\n\n"
        "Панель:\n"
        "/owner или /panel\n\n"
        "Игра:\n"
        "/game, /game mode classic, /game mode buttons, /game price 37, "
        "/game buttons 12, /game minbuttons 4, /game decrease 1, /game nftchance 18\n\n"
        "Тексты:\n"
        "/texts, /settext, /sethelp, /setusertext, /emojiid\n\n"
        "Ранги/шансы/рубежи:\n"
        "/setranks, /setuserrank, /setchance, /milestones, /luckmin\n\n"
        "Статистика:\n"
        "/stats, /fullstat, /mystats @username, /luck, /tickets, "
        "/resetstats, /resetuserstats, /hiderating, /showrating\n\n"
        "Gifts/выплаты:\n"
        "/giftbank, /giftblock, /notify, /payoutchat\n\n"
        "Задания:\n"
        "/task add, /task list, /task active, /task completed, /task requests, /task finish, /task stop\n\n"
        "Слот-турниры:\n"
        "/tournament start/check/status/participants/edit/winners/stop\n\n"
        "Referral-конкурсы:\n"
        "/refcontest start/status/participants/edit/winners/stop\n\n"
        "Диагностика:\n"
        "/ownercheck, /chatid, /userid"
    )


def owner_panel_page(
    page: str,
    config: BotConfig,
    db: StatsDatabase,
) -> tuple[str, InlineKeyboardMarkup]:
    if page == "main":
        return owner_panel_status_text(config, db), owner_panel_main_keyboard()

    if page == "game":
        return game_settings_text(db), owner_panel_wrap_keyboard(
            game_settings_keyboard(db),
            [InlineKeyboardButton("Ручная цена: /game price 37", callback_data="ownerpanel:noop")],
        )

    if page == "texts":
        return owner_panel_texts_overview(db), owner_panel_wrap_keyboard(
            None,
            [
                InlineKeyboardButton("Таблица /texts", callback_data="ownerpanel:texts_full"),
                InlineKeyboardButton("777", callback_data="textcfg:template:jackpot"),
            ],
            [
                InlineKeyboardButton("Статистика", callback_data="textcfg:template:stats"),
                InlineKeyboardButton("Личная стата", callback_data="textcfg:template:personal_stats"),
            ],
            [
                InlineKeyboardButton("Welcome", callback_data="textcfg:template:welcome"),
                InlineKeyboardButton("Help", callback_data="textcfg:template:help"),
            ],
            [
                InlineKeyboardButton("Chance", callback_data="textcfg:chance"),
                InlineKeyboardButton("Ранги", callback_data="textcfg:ranks"),
            ],
        )

    if page == "stats":
        return owner_panel_stats_overview(config, db), owner_panel_back_keyboard()

    if page == "tournaments":
        return owner_panel_tournaments_overview(db), owner_panel_wrap_keyboard(
            None,
            [
                InlineKeyboardButton("Статус турниров", callback_data="ownerpanel:tournaments_status"),
                InlineKeyboardButton("Инструкция", callback_data="ownerpanel:tournament_usage"),
            ],
        )

    if page == "refcontests":
        return owner_panel_refcontests_overview(db), owner_panel_wrap_keyboard(
            None,
            [
                InlineKeyboardButton("Статус referral", callback_data="ownerpanel:ref_status"),
                InlineKeyboardButton("Инструкция", callback_data="ownerpanel:ref_usage"),
            ],
        )

    if page == "tasks":
        return owner_panel_tasks_overview(db), owner_panel_wrap_keyboard(
            None,
            [InlineKeyboardButton("Список заданий", callback_data="ownerpanel:tasks_status")],
        )

    if page == "gifts":
        return owner_panel_gifts_overview(db), owner_panel_wrap_keyboard(
            None,
            [
                InlineKeyboardButton("Блок gifts", callback_data="ownerpanel:gifts_blocked"),
                InlineKeyboardButton("Уведомления", callback_data="ownerpanel:gifts_notify"),
            ],
        )

    if page == "ranks":
        return owner_panel_ranks_overview(db), owner_panel_wrap_keyboard(
            None,
            [
                InlineKeyboardButton("Chance", callback_data="textcfg:chance"),
                InlineKeyboardButton("Ранги", callback_data="textcfg:ranks"),
            ],
            [InlineKeyboardButton("Рубежи", callback_data="ownerpanel:milestones")],
        )

    if page == "system":
        return owner_panel_system_overview(config, db), owner_panel_back_keyboard()

    if page == "commands":
        return owner_panel_commands_text(), owner_panel_back_keyboard()

    if page == "tournament_usage":
        return tournament_usage(), owner_panel_back_keyboard()

    if page == "ref_usage":
        return refcontest_usage(), owner_panel_back_keyboard()

    if page == "milestones":
        return (
            "Настройки рубежей\n\n"
            f"Индивидуальные: {', '.join(map(str, get_user_milestone_values(db)))}\n"
            f"Общие по чату: {', '.join(map(str, get_chat_milestone_values(db)))}\n"
            f"Антискам: каждые {get_scam_warning_interval(db)} спинов\n\n"
            "Изменить:\n"
            "/milestones user 25,50,100\n"
            "/milestones chat 100,500\n"
            "/milestones scam 50",
            owner_panel_back_keyboard(),
        )

    if page == "texts_full":
        templates = db.get_message_templates()
        lines = ["Сохраненные тексты:"]
        if not templates:
            lines.append("Шаблоны пока не настроены.")
        for row in templates:
            label = TEMPLATE_LABELS.get(row["template_key"], row["template_key"])
            preview = row["text"].replace("\n", " ")
            if len(preview) > 110:
                preview = f"{preview[:107]}..."
            lines.append(f"{label}: {preview}")
        return "\n".join(lines), owner_panel_back_keyboard()

    if page == "tournaments_status":
        return owner_panel_tournaments_overview(db), owner_panel_back_keyboard()

    if page == "ref_status":
        return owner_panel_refcontests_overview(db), owner_panel_back_keyboard()

    if page == "tasks_status":
        return owner_panel_tasks_overview(db), owner_panel_back_keyboard()

    if page == "gifts_blocked":
        blocked = get_blocked_gift_tokens(db)
        lines = ["Заблокированные gifts:"]
        if not blocked:
            lines.append("Список пуст.")
        else:
            lines.extend(f"{index}. {token}" for index, token in enumerate(blocked, start=1))
        lines.append("")
        lines.append("Управление: /giftblock add/remove/clear")
        return "\n".join(lines), owner_panel_back_keyboard()

    if page == "gifts_notify":
        notify_ids = sorted(get_extra_notify_user_ids(db))
        payout_chats = sorted(get_payout_chat_ids(db))
        return (
            "Уведомления и чаты выдач\n\n"
            f"Доп. аккаунты: {', '.join(map(str, notify_ids)) if notify_ids else 'нет'}\n"
            f"Чаты выдач: {', '.join(map(str, payout_chats)) if payout_chats else 'нет'}\n\n"
            "Команды:\n"
            "/notify add USER_ID\n"
            "/notify remove USER_ID\n"
            "/payoutchat add CHAT_ID\n"
            "/payoutchat remove CHAT_ID",
            owner_panel_back_keyboard(),
        )

    return owner_panel_status_text(config, db), owner_panel_main_keyboard()


async def show_owner_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.effective_user or not update.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    if update.effective_chat.type != "private":
        await update.message.reply_text("Owner-панель открывается в личке с ботом: /owner")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    text, keyboard = owner_panel_page("main", config, db)
    await update.message.reply_text(text, reply_markup=keyboard)


async def handle_owner_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.data or not query.message:
        return

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], query.from_user.id):
        await query.answer()
        return

    page = query.data.split(":", 1)[1] if ":" in query.data else "main"
    if page == "noop":
        await query.answer("Эту настройку меняй командой из текста.", show_alert=True)
        return

    db: StatsDatabase = context.application.bot_data["db"]
    text, keyboard = owner_panel_page(page, config, db)
    await query.answer()
    try:
        await query.message.edit_text(text, reply_markup=keyboard)
    except TelegramError:
        await query.message.reply_text(text, reply_markup=keyboard)


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
        "/tournament check CHAT_ID - проверить чат перед запуском\n"
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
        raise ValueError("нужен формат: /tournament start CHAT_ID minutes30 3")

    chat_id = parse_integer_from_token(args[1], "ID чата")
    if len(args) >= 4:
        duration = parse_duration_token(args[2], default_unit="days")
        prize_places = parse_integer_from_token(args[3], "количество призовых мест")
        inline_prizes = args[4:]
    else:
        duration = timedelta(days=DEFAULT_TOURNAMENT_DAYS)
        prize_places = parse_integer_from_token(args[2], "количество призовых мест")
        inline_prizes = []

    line_prizes = [line.strip() for line in lines[1:] if line.strip()]
    prizes = line_prizes or inline_prizes
    if duration.total_seconds() <= 0 or prize_places <= 0:
        raise ValueError("длительность и количество мест должны быть больше 0")

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
    except ValueError as error:
        await update.message.reply_text(
            f"Не смог разобрать команду запуска: {error}\n\n"
            f"{tournament_usage()}"
        )
        return

    if not is_allowed_chat(config, chat_id):
        allowed_ids = ", ".join(str(chat_id) for chat_id in sorted(config.allowed_chat_ids)) or "не заданы"
        await update.message.reply_text(
            f"Этот чат не указан в ALLOWED_CHAT_IDS.\n\n"
            f"Я распознал CHAT_ID: {chat_id}\n"
            f"ALLOWED_CHAT_IDS сейчас: {allowed_ids}\n\n"
            "Добавьте этот CHAT_ID в переменные Bothost и сделайте redeploy."
        )
        return

    if len(prizes) < prize_places:
        await update.message.reply_text(
            f"Нужно указать минимум {prize_places} призов/ссылок, по одному на каждое место."
        )
        return

    existing_tournament = db.get_active_tournament_for_chat(chat_id)
    if existing_tournament:
        status = "активный"
        if existing_tournament["status"] == "pending_approval":
            status = "ждет подтверждения итогов"
        await update.message.reply_text(
            f"В этом чате уже есть турнир #{existing_tournament['tournament_id']} "
            f"со статусом: {status}.\n"
            f"Сначала завершите его кнопкой публикации или отмените:\n"
            f"/tournament stop {existing_tournament['tournament_id']}"
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


async def check_tournament_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or len(context.args) < 2:
        if update.message:
            await update.message.reply_text("Формат: /tournament check CHAT_ID")
        return

    config: BotConfig = context.application.bot_data["config"]
    db: StatsDatabase = context.application.bot_data["db"]

    try:
        chat_id = parse_integer_from_token(context.args[1], "ID чата")
    except ValueError as error:
        await update.message.reply_text(str(error))
        return

    allowed_ids = ", ".join(str(item) for item in sorted(config.allowed_chat_ids)) or "не заданы"
    lines = [
        "Проверка чата для турнира:",
        f"CHAT_ID: {chat_id}",
        f"Есть в ALLOWED_CHAT_IDS: {'да' if is_allowed_chat(config, chat_id) else 'нет'}",
        f"ALLOWED_CHAT_IDS: {allowed_ids}",
    ]

    existing_tournament = db.get_active_tournament_for_chat(chat_id)
    if existing_tournament:
        lines.append(
            f"Турнир в этом чате уже есть: #{existing_tournament['tournament_id']} "
            f"({existing_tournament['status']})"
        )
    else:
        lines.append("Активных турниров в этом чате нет.")

    try:
        chat = await context.bot.get_chat(chat_id)
        chat_title = chat.title or chat.username or str(chat.id)
        lines.append(f"Бот видит чат: да, {chat_title}")
    except TelegramError as error:
        lines.append(f"Бот видит чат: нет ({error})")

    await update.message.reply_text("\n".join(lines))


async def show_tournament_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    db: StatsDatabase = context.application.bot_data["db"]
    tournaments = db.get_open_tournaments()
    if not tournaments:
        await update.message.reply_text("Активных турниров или черновиков итогов сейчас нет.")
        return

    lines = ["Турниры:"]
    pending_tournaments = []
    for tournament in tournaments:
        ticket_rows = db.get_tournament_ticket_rows(tournament)
        status = "активен"
        if tournament["status"] == "pending_approval":
            status = "ждет подтверждения итогов"
            pending_tournaments.append(tournament)
        lines.extend(
            [
                "",
                f"#{tournament['tournament_id']}",
                f"Статус: {status}",
                f"Чат: {tournament['chat_id']}",
                f"Финиш: {format_datetime_for_message(tournament['ends_at'])}",
                f"Призовых мест: {tournament['prize_places']}",
                f"Билетов сейчас: {get_tournament_total_tickets(ticket_rows)}",
            ]
        )

    await update.message.reply_text("\n".join(lines))
    for tournament in pending_tournaments:
        winners, total_tickets, _ = parse_result_payload(tournament["winners_json"])
        if total_tickets is None:
            total_tickets = get_tournament_total_tickets(db.get_tournament_ticket_rows(tournament))
        await update.message.reply_text(
            "Черновик итогов. Можно опубликовать или перероллить:\n\n"
            f"{build_tournament_results_message(tournament, winners, total_tickets)}",
            reply_markup=tournament_result_keyboard(tournament["tournament_id"]),
        )


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


async def show_tournament_tickets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    if update.effective_chat.type != "private":
        await update.message.reply_text("Билеты турнира можно смотреть в личке с ботом.")
        return

    db: StatsDatabase = context.application.bot_data["db"]
    tournament = None
    if context.args:
        try:
            tournament_id = parse_integer_from_token(context.args[0], "ID турнира")
        except ValueError as error:
            await update.message.reply_text(str(error))
            return

        tournament = db.get_tournament(tournament_id)
        if not tournament:
            await update.message.reply_text("Турнир с таким ID не найден.")
            return
    else:
        tournaments = db.get_open_tournaments()
        if not tournaments:
            await update.message.reply_text(
                "Сейчас нет активных турниров. Если нужен завершенный турнир, укажите ID: /tickets ID"
            )
            return
        if len(tournaments) > 1:
            lines = ["Есть несколько турниров, укажите ID:", ""]
            for item in tournaments:
                lines.append(
                    f"#{item['tournament_id']} - чат {item['chat_id']}, статус {item['status']}"
                )
            lines.append("")
            lines.append("Формат: /tickets ID")
            await update.message.reply_text("\n".join(lines))
            return
        tournament = tournaments[0]

    rows = db.get_tournament_ticket_rows(tournament)
    await reply_long_text(update.message, build_tournament_tickets_text(tournament, rows))


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
    if not tournament or tournament["status"] not in {"active", "pending_approval"}:
        await update.message.reply_text("Активный турнир или черновик итогов с таким ID не найден.")
        return

    if db.cancel_tournament(tournament_id):
        await update.message.reply_text(f"Турнир #{tournament_id} отменен.")
    else:
        await update.message.reply_text("Не получилось отменить турнир. Проверьте ID через /tournament status.")


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
    if not has_owner_access(config, context.application.bot_data["db"], query.from_user.id):
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

    config: BotConfig = context.application.bot_data["config"]
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
        return

    if update.effective_chat.type != "private":
        return

    if not context.args:
        await update.message.reply_text(tournament_usage())
        return

    action = context.args[0].lower()
    if action == "start":
        await start_tournament(update, context)
        return

    if action in {"check", "test", "проверить"}:
        await check_tournament_chat(update, context)
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
    if not has_owner_access(config, context.application.bot_data["db"], query.from_user.id):
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
    if not has_owner_access(config, context.application.bot_data["db"], update.effective_user.id):
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

    if await handle_payout_dispute_text(update, context):
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

    for owner_user_id in sorted(payout_admin_ids(config, db)):
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

    if await handle_payout_proof_photo(update, context):
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
    if not has_owner_access(config, context.application.bot_data["db"], query.from_user.id):
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


async def remind_user_to_start_bot(
    update: Update,
    db: StatsDatabase,
    user_stats: sqlite3.Row | dict[str, int] | None,
) -> None:
    if not update.message or not update.effective_user:
        return

    if db.has_private_subscriber(update.effective_user.id):
        return

    total_spins = stats_value(user_stats, "total_spins") if user_stats else 0
    should_remind = total_spins in {1, 5, 10} or (total_spins > 10 and (total_spins - 10) % 15 == 0)
    if not should_remind:
        return

    await update.message.reply_text(
        f"{get_user_display_name(update.effective_user)}, запусти бота в личке через /start.\n"
        "Иначе я не смогу присылать тебе уведомления о выигрышах, выплатах и спорах."
    )


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
    db.add_rank_points(update.effective_user.id, get_rank_points_per_spin(db))
    user_stats = db.get_user_stats(update.effective_chat.id, update.effective_user.id)
    chat_totals = db.get_chat_totals(update.effective_chat.id)
    await remind_user_to_start_bot(update, db, user_stats)

    if result == "jackpot":
        if get_game_mode(db) == GAME_MODE_JACKPOT_BUTTONS:
            await send_jackpot_button_challenge(
                update,
                context,
                config,
                db,
                stats=user_stats,
            )
        else:
            jackpot_count = stats_value(user_stats, "jackpots") if user_stats else 0
            if jackpot_count % 2 == 0:
                message, entities_data, gift = await build_jackpot_message(
                    config,
                    db,
                    update.effective_user,
                    stats=user_stats,
                )
                await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
                if gift:
                    await create_and_notify_payout(
                        context,
                        config,
                        db,
                        update.effective_chat.id,
                        update.effective_user,
                        "nft",
                        "jackpot",
                        None,
                        gift["title"],
                        gift,
                    )
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
        if not should_award_small_giftr(db):
            message, entities_data = await build_three_of_kind_no_gift_message(
                config,
                db,
                update.effective_user,
                result,
                stats=user_stats,
            )
            await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
        else:
            eligible_three_count = db.record_small_gift_progress(
                update.effective_chat.id,
                update.effective_user.id,
            )
            progress_count = eligible_three_count % 3
            if progress_count == 0:
                message, entities_data, gift = await build_three_of_kind_message(
                    config,
                    db,
                    update.effective_user,
                    result,
                    stats=user_stats,
                )
                await send_text_with_entities(context, update.effective_chat.id, message, entities_data)
                await create_and_notify_payout(
                    context,
                    config,
                    db,
                    update.effective_chat.id,
                    update.effective_user,
                    "gift",
                    "three_of_kind",
                    None,
                    gift,
                )
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

    await process_reward_tasks(update, context, result=result)

    if should_send_chance_hint(db):
        message, entities_data = build_chance_hint_message(db, update.effective_user)
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)

    user_total_spins = stats_value(user_stats, "total_spins") if user_stats else 0
    if user_stats and is_spin_milestone(db, user_total_spins):
        message, entities_data = build_milestone_message(
            db,
            update.effective_user,
            user_stats,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)

    if user_stats and is_scam_warning_milestone(db, user_total_spins):
        message, entities_data = build_scam_warning_message(
            db,
            update.effective_user,
            user_stats,
        )
        await send_text_with_entities(context, update.effective_chat.id, message, entities_data)

    if is_chat_stats_milestone(db, stats_value(chat_totals, "total_spins")):
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
    application.add_handler(CommandHandler("owner", show_owner_panel))
    application.add_handler(CommandHandler("panel", show_owner_panel))
    application.add_handler(CommandHandler("admin", show_owner_panel))
    application.add_handler(CommandHandler("chatid", show_chat_id))
    application.add_handler(CommandHandler("userid", show_user_id))
    application.add_handler(CommandHandler("ownercheck", owner_check))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("fullstat", show_full_stats))
    application.add_handler(CommandHandler("fullstats", show_full_stats))
    application.add_handler(CommandHandler("mystats", show_personal_stats))
    application.add_handler(CommandHandler("me", show_personal_stats))
    application.add_handler(CommandHandler("luck", show_luck_stats))
    application.add_handler(CommandHandler("luckmin", set_luck_min_spins))
    application.add_handler(CommandHandler("tasks", show_tasks))
    application.add_handler(CommandHandler("tickets", show_tournament_tickets))
    application.add_handler(CommandHandler("dailybonus", daily_bonus))
    application.add_handler(CommandHandler("balance", show_balance))
    application.add_handler(CommandHandler("withdraw", withdraw_stars))
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
    application.add_handler(CommandHandler("giftblock", manage_gift_block))
    application.add_handler(CommandHandler("giftbank", manage_gift_block))
    application.add_handler(CommandHandler("game", manage_game_settings))
    application.add_handler(CommandHandler("milestones", manage_milestones))
    application.add_handler(CommandHandler("task", manage_tasks))
    application.add_handler(CommandHandler("notify", manage_notify_users))
    application.add_handler(CommandHandler("payoutchat", manage_payout_chats))
    application.add_handler(CommandHandler("texts", show_message_templates))
    application.add_handler(CommandHandler("tournament", manage_tournament))
    application.add_handler(CommandHandler("refcontest", manage_refcontest))
    application.add_handler(CallbackQueryHandler(handle_owner_panel_callback, pattern="^ownerpanel:"))
    application.add_handler(CallbackQueryHandler(handle_jackpot_button_choice, pattern="^jpbtn:"))
    application.add_handler(CallbackQueryHandler(handle_star_prize_choice, pattern="^starprize:"))
    application.add_handler(CallbackQueryHandler(handle_game_settings_choice, pattern="^game:"))
    application.add_handler(CallbackQueryHandler(handle_payout_callback, pattern="^payout:"))
    application.add_handler(CallbackQueryHandler(handle_task_callback, pattern="^(taskmenu|taskopen|taskjoin|taskadmin|taskdone):"))
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
