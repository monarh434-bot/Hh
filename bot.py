import asyncio
import html
import io
import logging
import re
import sqlite3
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile, CallbackQuery, FSInputFile, Message, ForceReply
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =========================================================
# CONFIG - ALL IN ONE FILE
# =========================================================
BOT_TOKEN = "8731355621:AAGBnukT61jO9OOjZFepx_Tqgk1-w3n1gg4"
DB_PATH = "bot.db"
BOT_USERNAME_FALLBACK = "Seamusstest_bot"

# Roles
CHIEF_ADMIN_ID = 626387429
BOOTSTRAP_ADMINS = [123456789]
BOOTSTRAP_OPERATORS = []

WITHDRAW_CHANNEL_ID = -1003785698154
LOG_CHANNEL_ID = 0
MIN_WITHDRAW = 10.0
DEFAULT_HOLD_MINUTES = 15
DEFAULT_TREASURY_BALANCE = 0.0

# Crypto Bot / Crypto Pay API
CRYPTO_PAY_TOKEN = "561528:AALC6ucd7Ge10ZgaYiPhpITrc7nRUQhBr1N"  # configured
CRYPTO_PAY_BASE_URL = "https://pay.crypt.bot/api"
CRYPTO_PAY_ASSET = "USDT"
CRYPTO_PAY_PIN_CHECK_TO_USER = False  # True -> check pinned to telegram user

OPERATORS = {
    "mts": {"title": "МТС", "price": 4.00, "command": "/mts"},
    "bil": {"title": "Билайн", "price": 4.50, "command": "/bil"},
    "mega": {"title": "Мегафон", "price": 5.00, "command": "/mega"},
    "t2": {"title": "Tele2", "price": 4.20, "command": "/t2"},
    "vtb": {"title": "ВТБ", "price": 4.80, "command": "/vtb"},
    "gaz": {"title": "Газпром", "price": 4.90, "command": "/gaz"},
}
# =========================================================

START_BANNER = "start_banner.jpg"
PROFILE_BANNER = "profile_banner.jpg"
MY_NUMBERS_BANNER = "my_numbers_banner.jpg"
WITHDRAW_BANNER = "withdraw_banner.jpg"
MSK_OFFSET = timedelta(hours=3)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", handlers=[logging.StreamHandler(), logging.FileHandler("bot.log", mode="a", encoding="utf-8")])
router = Router()

LIVE_MIRROR_TASKS = {}
LIVE_DP = None


def msk_now() -> datetime:
    return datetime.utcnow() + MSK_OFFSET

def now_str() -> str:
    return msk_now().strftime("%Y-%m-%d %H:%M:%S")


class SubmitStates(StatesGroup):
    waiting_mode = State()
    waiting_operator = State()
    waiting_qr = State()


class WithdrawStates(StatesGroup):
    waiting_amount = State()
    waiting_payment_link = State()

class MirrorStates(StatesGroup):
    waiting_token = State()

class EmojiLookupStates(StatesGroup):
    waiting_target = State()



class AdminStates(StatesGroup):
    waiting_hold = State()
    waiting_min_withdraw = State()
    waiting_treasury_add = State()
    waiting_treasury_sub = State()
    waiting_treasury_invoice = State()
    waiting_operator_price = State()
    waiting_group_finance_amount = State()
    waiting_group_price_value = State()
    waiting_role_user = State()
    waiting_role_kind = State()
    waiting_start_text = State()
    waiting_ad_text = State()
    waiting_broadcast_text = State()
    waiting_user_action_id = State()
    waiting_user_action_value = State()
    waiting_user_action_text = State()
    waiting_user_custom_price_text = State()
    waiting_user_stats_lookup = State()
    waiting_user_price_lookup = State()
    waiting_user_price_value = State()
    waiting_group_stats_lookup = State()
    waiting_db_upload = State()
    waiting_channel_value = State()
    waiting_backup_channel = State()


@dataclass
class QueueItem:
    id: int
    user_id: int
    username: str
    full_name: str
    operator_key: str
    phone_label: str
    normalized_phone: str
    qr_file_id: str
    status: str
    price: float
    created_at: str
    taken_by_admin: Optional[int]
    taken_at: Optional[str]
    hold_until: Optional[str]
    work_started_at: Optional[str]
    mode: str
    started_notice_sent: int
    work_chat_id: Optional[int]
    work_thread_id: Optional[int]
    work_message_id: Optional[int]
    work_started_by: Optional[int]
    fail_reason: Optional[str]
    completed_at: Optional[str]
    timer_last_render: Optional[str]
    submit_bot_token: Optional[str] = None


class Database:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
        self.seed_defaults()

    def create_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                balance REAL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS roles (
                user_id INTEGER PRIMARY KEY,
                role TEXT NOT NULL,
                assigned_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS workspaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                mode TEXT NOT NULL,
                added_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(chat_id, thread_id, mode)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                operator_key TEXT NOT NULL,
                phone_label TEXT NOT NULL,
                normalized_phone TEXT NOT NULL,
                qr_file_id TEXT NOT NULL,
                status TEXT NOT NULL,
                price REAL NOT NULL,
                created_at TEXT NOT NULL,
                taken_by_admin INTEGER,
                taken_at TEXT,
                hold_until TEXT,
                work_started_at TEXT,
                mode TEXT NOT NULL DEFAULT 'hold',
                started_notice_sent INTEGER DEFAULT 0,
                work_chat_id INTEGER,
                work_thread_id INTEGER,
                work_message_id INTEGER,
                work_started_by INTEGER,
                fail_reason TEXT,
                completed_at TEXT,
                timer_last_render TEXT
            )
            """
        )


        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_prices (
                user_id INTEGER NOT NULL,
                operator_key TEXT NOT NULL,
                mode TEXT NOT NULL,
                price REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, operator_key, mode)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS payout_accounts (
                user_id INTEGER PRIMARY KEY,
                payout_link TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                decided_at TEXT,
                admin_id INTEGER,
                payout_check TEXT,
                payout_note TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mirrors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_user_id INTEGER NOT NULL,
                owner_username TEXT,
                token TEXT NOT NULL UNIQUE,
                bot_id INTEGER,
                bot_username TEXT,
                bot_title TEXT,
                status TEXT NOT NULL DEFAULT 'saved',
                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS treasury_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount REAL NOT NULL,
                crypto_invoice_id TEXT,
                pay_url TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                paid_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS group_finance (
                chat_id INTEGER NOT NULL,
                thread_id INTEGER,
                treasury_balance REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, thread_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS group_operator_prices (
                chat_id INTEGER NOT NULL,
                thread_id INTEGER,
                operator_key TEXT NOT NULL,
                mode TEXT NOT NULL,
                price REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, thread_id, operator_key, mode)
            )
            """
        )
        self.conn.commit()

    def seed_defaults(self):
        defaults = {
            "hold_minutes": str(DEFAULT_HOLD_MINUTES),
            "min_withdraw": str(MIN_WITHDRAW),
            "treasury_balance": str(DEFAULT_TREASURY_BALANCE),
            "start_title": "ESIM Service X",
            "start_subtitle": "Премиум сервис приёма номеров",
            "start_description": "🚀 <b>Быстрый приём заявок</b> • 💎 <b>Стабильные выплаты</b> • 🛡 <b>Контроль статусов</b>",
            "announcement_text": "",
            "backup_channel_id": "0",
            "backup_enabled": "0",
        }
        for key, value in defaults.items():
            self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
        for key, data in OPERATORS.items():
            self.conn.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (f"price_{key}", str(data["price"])),
            )
            self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_hold_{key}", "1"))
            self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_no_hold_{key}", "1"))
            self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_hold_{key}", "1"))
            self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_no_hold_{key}", "1"))
        self.conn.execute(
            "INSERT OR IGNORE INTO roles (user_id, role, assigned_at) VALUES (?, 'chief_admin', ?)",
            (CHIEF_ADMIN_ID, now_str()),
        )
        for uid in BOOTSTRAP_ADMINS:
            if uid != CHIEF_ADMIN_ID:
                self.conn.execute(
                    "INSERT OR IGNORE INTO roles (user_id, role, assigned_at) VALUES (?, 'admin', ?)",
                    (uid, now_str()),
                )
        for uid in BOOTSTRAP_OPERATORS:
            self.conn.execute(
                "INSERT OR IGNORE INTO roles (user_id, role, assigned_at) VALUES (?, 'operator', ?)",
                (uid, now_str()),
            )
        self.conn.commit()


    def save_mirror(self, owner_user_id: int, owner_username: str, token: str, bot_id: int, bot_username: str, bot_title: str):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO mirrors (owner_user_id, owner_username, token, bot_id, bot_username, bot_title, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'active', ?)
            ON CONFLICT(token) DO UPDATE SET
                owner_user_id=excluded.owner_user_id,
                owner_username=excluded.owner_username,
                bot_id=excluded.bot_id,
                bot_username=excluded.bot_username,
                bot_title=excluded.bot_title,
                status='active'
            """,
            (owner_user_id, owner_username, token, bot_id, bot_username, bot_title, now_str()),
        )
        self.conn.commit()
        return cur.lastrowid

    def user_mirrors(self, owner_user_id: int):
        return self.conn.execute(
            "SELECT * FROM mirrors WHERE owner_user_id=? ORDER BY id DESC LIMIT 10",
            (owner_user_id,),
        ).fetchall()

    def all_active_mirrors(self):
        return self.conn.execute(
            "SELECT * FROM mirrors WHERE status IN ('saved','active') ORDER BY id ASC"
        ).fetchall()

    def get_setting(self, key: str, default: Optional[str] = None) -> str:
        row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str):
        self.conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def upsert_user(self, user_id: int, username: str, full_name: str):
        self.conn.execute(
            """
            INSERT INTO users (user_id, username, full_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name
            """,
            (user_id, username, full_name),
        )
        self.conn.commit()


    def find_user_by_username(self, username: str):
        username = (username or "").lstrip("@").strip().lower()
        if not username:
            return None
        return self.conn.execute("SELECT * FROM users WHERE lower(username)=?", (username,)).fetchone()

    def find_last_user_by_phone(self, phone: str):
        normalized = normalize_phone(phone) if phone else None
        if not normalized:
            return None
        return self.conn.execute(
            "SELECT u.* FROM queue_items q JOIN users u ON u.user_id=q.user_id WHERE q.normalized_phone=? ORDER BY q.id DESC LIMIT 1",
            (normalized,),
        ).fetchone()

    def all_user_ids(self):
        rows = self.conn.execute("SELECT user_id FROM users ORDER BY user_id ASC").fetchall()
        return [int(r["user_id"]) for r in rows]

    def export_usernames(self) -> str:
        rows = self.conn.execute("SELECT username FROM users WHERE username IS NOT NULL AND username != '' ORDER BY username COLLATE NOCASE").fetchall()
        return "\n".join(f"@{r['username'].lstrip('@')}" for r in rows)

    def get_user(self, user_id: int):
        return self.conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

    def add_balance(self, user_id: int, amount: float):
        self.conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        self.conn.commit()

    def subtract_balance(self, user_id: int, amount: float):
        self.conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
        self.conn.commit()

    def set_role(self, user_id: int, role: str):
        current = self.get_role(user_id)
        if current == "chief_admin" and role != "chief_admin":
            return False
        self.conn.execute(
            "INSERT INTO roles (user_id, role, assigned_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET role=excluded.role, assigned_at=excluded.assigned_at",
            (user_id, role, now_str()),
        )
        self.conn.commit()
        return True

    def remove_role(self, user_id: int):
        if user_id == CHIEF_ADMIN_ID:
            return False
        self.conn.execute("DELETE FROM roles WHERE user_id = ?", (user_id,))
        self.conn.commit()
        return True

    def get_role(self, user_id: int) -> str:
        if user_id == CHIEF_ADMIN_ID:
            return "chief_admin"
        row = self.conn.execute("SELECT role FROM roles WHERE user_id = ?", (user_id,)).fetchone()
        return row["role"] if row else "user"

    def list_roles(self):
        return self.conn.execute("SELECT * FROM roles ORDER BY CASE role WHEN 'chief_admin' THEN 0 WHEN 'admin' THEN 1 WHEN 'operator' THEN 2 ELSE 3 END, user_id ASC").fetchall()

    def get_operator_price(self, operator_key: str) -> float:
        return float(self.get_setting(f"price_{operator_key}", str(OPERATORS[operator_key]["price"])))

    def create_queue_item(self, user_id: int, username: str, full_name: str, operator_key: str, normalized_phone: str, qr_file_id: str, mode: str):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO queue_items (
                user_id, username, full_name, operator_key, phone_label, normalized_phone,
                qr_file_id, status, price, created_at, mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
            """,
            (
                user_id,
                username,
                full_name,
                operator_key,
                pretty_phone(normalized_phone),
                normalized_phone,
                qr_file_id,
                get_mode_price(operator_key, mode, user_id),
                now_str(),
                mode,
            ),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_queue_item(self, item_id: int):
        row = self.conn.execute("SELECT * FROM queue_items WHERE id = ?", (item_id,)).fetchone()
        return QueueItem(**row) if row else None

    def get_next_queue_item(self, operator_key: str):
        row = self.conn.execute(
            "SELECT * FROM queue_items WHERE operator_key = ? AND status = 'queued' ORDER BY id ASC LIMIT 1",
            (operator_key,),
        ).fetchone()
        return QueueItem(**row) if row else None

    def count_waiting(self, operator_key: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM queue_items WHERE operator_key=? AND status='queued'",
            (operator_key,),
        ).fetchone()
        return int(row["c"] or 0)

    def mark_taken(self, item_id: int, user_id: int):
        self.conn.execute(
            "UPDATE queue_items SET status='taken', taken_by_admin=?, taken_at=? WHERE id=? AND status='queued'",
            (user_id, now_str(), item_id),
        )
        self.conn.commit()

    def mark_error_before_start(self, item_id: int):
        self.conn.execute(
            "UPDATE queue_items SET status='failed', fail_reason='error_before_start', completed_at=? WHERE id=?",
            (now_str(), item_id),
        )
        self.conn.commit()

    def start_work(self, item_id: int, worker_id: int, mode: str, chat_id: int, thread_id: Optional[int], message_id: int):
        start_dt = msk_now()
        hold_until = None
        if mode == "hold":
            hold_minutes = int(float(self.get_setting("hold_minutes", str(DEFAULT_HOLD_MINUTES))))
            hold_until = fmt_dt(start_dt + timedelta(minutes=hold_minutes))
        self.conn.execute(
            """
            UPDATE queue_items
            SET status='in_progress', work_started_at=?, hold_until=?, started_notice_sent=1,
                work_chat_id=?, work_thread_id=?, work_message_id=?, work_started_by=?, timer_last_render=?
            WHERE id=?
            """,
            (fmt_dt(start_dt), hold_until, chat_id, thread_id, message_id, worker_id, fmt_dt(start_dt), item_id),
        )
        self.conn.commit()

    def fail_after_start(self, item_id: int, reason: str):
        self.conn.execute(
            "UPDATE queue_items SET status='failed', fail_reason=?, completed_at=? WHERE id=?",
            (reason, now_str(), item_id),
        )
        self.conn.commit()

    def complete_queue_item(self, item_id: int):
        self.conn.execute(
            "UPDATE queue_items SET status='completed', completed_at=? WHERE id=?",
            (now_str(), item_id),
        )
        self.conn.commit()

    def get_expired_holds(self):
        rows = self.conn.execute(
            "SELECT * FROM queue_items WHERE status='in_progress' AND mode='hold' AND hold_until IS NOT NULL AND hold_until <= ?",
            (now_str(),),
        ).fetchall()
        return [QueueItem(**row) for row in rows]

    def get_active_holds_for_render(self):
        rows = self.conn.execute(
            "SELECT * FROM queue_items WHERE status='in_progress' AND mode='hold' AND hold_until IS NOT NULL AND work_chat_id IS NOT NULL AND work_message_id IS NOT NULL"
        ).fetchall()
        return [QueueItem(**row) for row in rows]

    def touch_timer_render(self, item_id: int):
        self.conn.execute("UPDATE queue_items SET timer_last_render=? WHERE id=?", (now_str(), item_id))
        self.conn.commit()



    def set_user_price(self, user_id: int, operator_key: str, mode: str, price: float):
        self.conn.execute(
            "INSERT INTO user_prices (user_id, operator_key, mode, price, updated_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id, operator_key, mode) DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at",
            (user_id, operator_key, mode, price, now_str()),
        )
        self.conn.commit()

    def delete_user_price(self, user_id: int, operator_key: str, mode: str):
        self.conn.execute(
            "DELETE FROM user_prices WHERE user_id=? AND operator_key=? AND mode=?",
            (user_id, operator_key, mode),
        )
        self.conn.commit()

    def get_user_price(self, user_id: int, operator_key: str, mode: str):
        row = self.conn.execute(
            "SELECT price FROM user_prices WHERE user_id=? AND operator_key=? AND mode=?",
            (user_id, operator_key, mode),
        ).fetchone()
        return float(row["price"]) if row else None

    def list_user_prices(self, user_id: int):
        return self.conn.execute(
            "SELECT * FROM user_prices WHERE user_id=? ORDER BY operator_key, mode",
            (user_id,),
        ).fetchall()

    def set_payout_link(self, user_id: int, payout_link: str):
        self.conn.execute(
            "INSERT INTO payout_accounts (user_id, payout_link, updated_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET payout_link=excluded.payout_link, updated_at=excluded.updated_at",
            (user_id, payout_link, now_str()),
        )
        self.conn.commit()

    def get_payout_link(self, user_id: int) -> Optional[str]:
        row = self.conn.execute("SELECT payout_link FROM payout_accounts WHERE user_id=?", (user_id,)).fetchone()
        return row["payout_link"] if row else None

    def create_withdrawal(self, user_id: int, amount: float):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES (?, ?, 'pending', ?)",
            (user_id, amount, now_str()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_withdrawal(self, withdraw_id: int):
        return self.conn.execute("SELECT * FROM withdrawals WHERE id = ?", (withdraw_id,)).fetchone()

    def set_withdrawal_status(self, withdraw_id: int, status: str, admin_id: int, payout_check: Optional[str] = None, payout_note: Optional[str] = None):
        self.conn.execute(
            "UPDATE withdrawals SET status=?, decided_at=?, admin_id=?, payout_check=?, payout_note=? WHERE id=?",
            (status, now_str(), admin_id, payout_check, payout_note, withdraw_id),
        )
        self.conn.commit()

    def count_pending_withdrawals(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM withdrawals WHERE status='pending'").fetchone()
        return int(row["c"] or 0)


    def create_treasury_invoice(self, amount: float, crypto_invoice_id: Optional[str], pay_url: Optional[str], created_by: int):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO treasury_invoices (amount, crypto_invoice_id, pay_url, status, created_by, created_at) VALUES (?, ?, ?, 'active', ?, ?)",
            (amount, str(crypto_invoice_id or ''), pay_url or '', created_by, now_str()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_treasury_invoice(self, invoice_id: int):
        return self.conn.execute("SELECT * FROM treasury_invoices WHERE id = ?", (invoice_id,)).fetchone()

    def mark_treasury_invoice_paid(self, invoice_id: int):
        self.conn.execute("UPDATE treasury_invoices SET status='paid', paid_at=? WHERE id=?", (now_str(), invoice_id))
        self.conn.commit()

    def list_recent_treasury_invoices(self, limit: int = 10):
        return self.conn.execute("SELECT * FROM treasury_invoices ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

    def get_treasury(self) -> float:
        return float(self.get_setting("treasury_balance", str(DEFAULT_TREASURY_BALANCE)))

    def add_treasury(self, amount: float):
        self.set_setting("treasury_balance", str(self.get_treasury() + amount))

    def subtract_treasury(self, amount: float):
        self.set_setting("treasury_balance", str(self.get_treasury() - amount))

    def _thread_key(self, thread_id: Optional[int]):
        return -1 if thread_id is None else int(thread_id)

    def get_group_balance(self, chat_id: int, thread_id: Optional[int]) -> float:
        row = self.conn.execute(
            "SELECT treasury_balance FROM group_finance WHERE chat_id=? AND thread_id=?",
            (int(chat_id), self._thread_key(thread_id)),
        ).fetchone()
        return float(row["treasury_balance"]) if row else 0.0

    def set_group_balance(self, chat_id: int, thread_id: Optional[int], balance: float):
        self.conn.execute(
            "INSERT INTO group_finance (chat_id, thread_id, treasury_balance, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(chat_id, thread_id) DO UPDATE SET treasury_balance=excluded.treasury_balance, updated_at=excluded.updated_at",
            (int(chat_id), self._thread_key(thread_id), float(balance), now_str()),
        )
        self.conn.commit()

    def add_group_balance(self, chat_id: int, thread_id: Optional[int], amount: float):
        self.set_group_balance(chat_id, thread_id, self.get_group_balance(chat_id, thread_id) + float(amount))

    def subtract_group_balance(self, chat_id: int, thread_id: Optional[int], amount: float):
        self.set_group_balance(chat_id, thread_id, self.get_group_balance(chat_id, thread_id) - float(amount))

    def get_group_price(self, chat_id: int, thread_id: Optional[int], operator_key: str, mode: str):
        row = self.conn.execute(
            "SELECT price FROM group_operator_prices WHERE chat_id=? AND thread_id=? AND operator_key=? AND mode=?",
            (int(chat_id), self._thread_key(thread_id), operator_key, mode),
        ).fetchone()
        return float(row["price"]) if row else None

    def set_group_price(self, chat_id: int, thread_id: Optional[int], operator_key: str, mode: str, price: float):
        self.conn.execute(
            "INSERT INTO group_operator_prices (chat_id, thread_id, operator_key, mode, price, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(chat_id, thread_id, operator_key, mode) DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at",
            (int(chat_id), self._thread_key(thread_id), operator_key, mode, float(price), now_str()),
        )
        self.conn.commit()

    def reserve_queue_item_for_group(self, item_id: int, taker_id: int, chat_id: int, thread_id: Optional[int], amount: float) -> bool:
        current_balance = self.get_group_balance(chat_id, thread_id)
        if current_balance + 1e-9 < float(amount):
            return False
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE queue_items SET status='taken', taken_by_admin=?, taken_at=?, charge_chat_id=?, charge_thread_id=?, charge_amount=? WHERE id=? AND status='queued'",
            (taker_id, now_str(), int(chat_id), self._thread_key(thread_id), float(amount), item_id),
        )
        if cur.rowcount <= 0:
            self.conn.rollback()
            return False
        self.conn.execute(
            "INSERT INTO group_finance (chat_id, thread_id, treasury_balance, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(chat_id, thread_id) DO UPDATE SET treasury_balance=excluded.treasury_balance, updated_at=excluded.updated_at",
            (int(chat_id), self._thread_key(thread_id), current_balance - float(amount), now_str()),
        )
        self.conn.commit()
        return True

    def release_item_reservation(self, item_id: int) -> float:
        row = self.conn.execute("SELECT charge_chat_id, charge_thread_id, charge_amount FROM queue_items WHERE id=?", (item_id,)).fetchone()
        if not row or row["charge_chat_id"] is None or row["charge_amount"] is None:
            return 0.0
        amount = float(row["charge_amount"] or 0)
        thread_id = None if int(row["charge_thread_id"]) == -1 else int(row["charge_thread_id"])
        self.add_group_balance(int(row["charge_chat_id"]), thread_id, amount)
        self.conn.execute("UPDATE queue_items SET charge_chat_id=NULL, charge_thread_id=NULL, charge_amount=NULL WHERE id=?", (item_id,))
        self.conn.commit()
        return amount

    def enable_workspace(self, chat_id: int, thread_id: Optional[int], mode: str, added_by: int):
        self.conn.execute(
            "INSERT INTO workspaces (chat_id, thread_id, mode, added_by, created_at, is_enabled) VALUES (?, ?, ?, ?, ?, 1) ON CONFLICT(chat_id, thread_id, mode) DO UPDATE SET is_enabled=1, added_by=excluded.added_by, created_at=excluded.created_at",
            (chat_id, thread_id, mode, added_by, now_str()),
        )
        self.conn.commit()

    def disable_workspace(self, chat_id: int, thread_id: Optional[int], mode: str):
        self.conn.execute(
            "UPDATE workspaces SET is_enabled=0 WHERE chat_id=? AND ((thread_id IS NULL AND ? IS NULL) OR thread_id=?) AND mode=?",
            (chat_id, thread_id, thread_id, mode),
        )
        self.conn.commit()

    def is_workspace_enabled(self, chat_id: int, thread_id: Optional[int], mode: str) -> bool:
        row = self.conn.execute(
            "SELECT is_enabled FROM workspaces WHERE chat_id=? AND ((thread_id IS NULL AND ? IS NULL) OR thread_id=?) AND mode=?",
            (chat_id, thread_id, thread_id, mode),
        ).fetchone()
        return bool(row and row["is_enabled"])

    def list_workspaces(self):
        return self.conn.execute("SELECT * FROM workspaces WHERE is_enabled=1 ORDER BY chat_id, thread_id").fetchall()

    def user_stats(self, user_id: int):
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued,
                SUM(CASE WHEN status='taken' THEN 1 ELSE 0 END) AS taken,
                SUM(CASE WHEN status='in_progress' THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slipped,
                SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS earned
            FROM queue_items WHERE user_id=?
            """,
            (user_id,),
        ).fetchone()
        return row

    def user_operator_stats(self, user_id: int):
        return self.conn.execute(
            "SELECT operator_key, COUNT(*) AS total, SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS earned FROM queue_items WHERE user_id=? GROUP BY operator_key ORDER BY total DESC",
            (user_id,),
        ).fetchall()


    def recover_after_restart(self):
        # Return items that were merely taken but never started back into the queue
        self.conn.execute(
            """
            UPDATE queue_items
            SET status='queued',
                taken_by_admin=NULL,
                taken_at=NULL
            WHERE status='taken' AND (work_started_at IS NULL OR work_started_at='')
            """
        )
        # Force timer re-render on active holds after restart
        self.conn.execute(
            "UPDATE queue_items SET timer_last_render=NULL WHERE status='in_progress' AND mode='hold'"
        )
        self.conn.commit()

    def group_stats(self, chat_id: int, thread_id: Optional[int]):
        return self.conn.execute(
            """
            SELECT
                COUNT(*) AS taken_total,
                SUM(CASE WHEN work_started_at IS NOT NULL THEN 1 ELSE 0 END) AS started,
                SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS spent_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
            FROM queue_items
            WHERE charge_chat_id=? AND charge_thread_id=?
            """,
            (int(chat_id), self._thread_key(thread_id)),
        ).fetchone()


db = Database(DB_PATH)


def msk_now() -> datetime:
    return datetime.utcnow() + MSK_OFFSET

def now_str() -> str:
    return msk_now().strftime("%Y-%m-%d %H:%M:%S")


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def usd(amount: float) -> str:
    return f"${float(amount or 0):.2f}"


def user_role(user_id: int) -> str:
    return db.get_role(user_id)


def is_admin(user_id: int) -> bool:
    return user_role(user_id) in {"chief_admin", "admin"}


def is_operator_or_admin(user_id: int) -> bool:
    return user_role(user_id) in {"chief_admin", "admin", "operator"}


def is_chief_admin(user_id: int) -> bool:
    return user_role(user_id) == "chief_admin"

def is_backup_enabled() -> bool:
    return db.get_setting("backup_enabled", "0") == "1"

def set_backup_enabled(enabled: bool):
    db.set_setting("backup_enabled", "1" if enabled else "0")

def backup_channel_id() -> int:
    try:
        return int(db.get_setting("backup_channel_id", "0") or 0)
    except Exception:
        return 0


def normalize_phone(raw: str) -> Optional[str]:
    text = (raw or "").strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if text.startswith("+"):
        text = text[1:]
    if len(text) == 11 and text.isdigit() and text[0] in {"7", "8"}:
        return "7" + text[1:]
    return None


def pretty_phone(normalized: str) -> str:
    return f"+{normalized}" if normalized else "-"


def progress_bar(hold_until: Optional[str], started_at: Optional[str], size: int = 10) -> str:
    start = parse_dt(started_at)
    end = parse_dt(hold_until)
    if not start or not end:
        return ""
    total = max((end - start).total_seconds(), 1)
    left = max((end - msk_now()).total_seconds(), 0)
    done = max(total - left, 0)
    filled = min(size, max(0, round(done / total * size)))
    return "🟩" * filled + "⬜" * (size - filled)


def time_left_text(hold_until: Optional[str]) -> str:
    end = parse_dt(hold_until)
    if not end:
        return "—"
    left = end - msk_now()
    if left.total_seconds() <= 0:
        return "00:00"
    total = int(left.total_seconds())
    minutes = total // 60
    seconds = total % 60
    return f"{minutes:02d}:{seconds:02d}"


def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Сдать номер", callback_data="menu:submit")
    kb.button(text="📦 Мои номера", callback_data="menu:my")
    kb.button(text="👤 Профиль", callback_data="menu:profile")
    kb.button(text="💸 Вывод средств", callback_data="menu:withdraw")
    kb.button(text="🪞 Зеркало", callback_data="menu:mirror")
    kb.adjust(1)
    return kb.as_markup()


def profile_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📦 Мои номера", callback_data="menu:my")
    kb.button(text="💳 Изменить счёт", callback_data="menu:payout_link")
    kb.button(text="💸 Вывод средств", callback_data="menu:withdraw")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def my_numbers_kb(items):
    kb = InlineKeyboardBuilder()
    for item in items[:10]:
        if item['status'] == 'queued':
            kb.button(text=f"🗑 Убрать #{item['id']}", callback_data=f"myremove:{item['id']}")
    kb.button(text="↻ Обновить", callback_data="menu:my")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()



def quick_submit_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Сдать ещё номер", callback_data="menu:submit")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()

def mirror_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать зеркало", callback_data="mirror:create")
    kb.button(text="📋 Мои зеркала", callback_data="mirror:list")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()
def cancel_inline_kb(back: str = "menu:home"):
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data=back)
    kb.adjust(1)
    return kb.as_markup()


def operators_kb(mode: str = "hold", prefix: str = "op", back_cb: str = "mode:back", user_id: int | None = None):
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        q = count_waiting_mode(key, mode)
        price = get_mode_price(key, mode, user_id)
        prefix_mark = "🚫 " if not is_operator_mode_enabled(key, mode) else ""
        kb.button(text=f"{prefix_mark}{op_text(key)} ({q}) • {usd(price)}", callback_data=f"{prefix}:{key}:{mode}")
    kb.button(text="↩️ Назад", callback_data=back_cb)
    kb.adjust(1)
    return kb.as_markup()

def esim_mode_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="esim_mode:hold")
    kb.button(text="⚡ БезХолд", callback_data="esim_mode:no_hold")
    kb.button(text="🏠 Закрыть", callback_data="noop")
    kb.adjust(2, 1)
    return kb.as_markup()


def mode_inline_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="mode:hold")
    kb.button(text="⚡ БезХолд", callback_data="mode:no_hold")
    kb.button(text="↩️ Назад", callback_data="menu:submit")
    kb.adjust(2, 1)
    return kb.as_markup()


def mode_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="mode:hold")
    kb.button(text="⚡ БезХолд", callback_data="mode:no_hold")
    kb.button(text="↩️ Назад", callback_data="mode:back")
    kb.adjust(2, 1)
    return kb.as_markup()

def submit_result_kb(operator_key: str, mode: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Сдать ещё", callback_data=f"submit_more:{operator_key}:{mode}")
    kb.button(text="✅ Я закончил загрузку", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def admin_queue_kb(item: QueueItem):
    kb = InlineKeyboardBuilder()
    if item.status in {"queued", "taken"}:
        kb.button(text="✅ Встал", callback_data=f"take_start:{item.id}")
        kb.button(text="⚠️ Ошибка", callback_data=f"error_pre:{item.id}")
        kb.adjust(1)
    elif item.status == "in_progress":
        if item.mode == "no_hold":
            kb.button(text="💸 Оплатить", callback_data=f"instant_pay:{item.id}")
        kb.button(text="❌ Слет", callback_data=f"slip:{item.id}")
        kb.adjust(1)
    return kb.as_markup()


def confirm_withdraw_kb(amount: float):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить", callback_data=f"withdraw_confirm:{amount}")
    kb.button(text="↩️ Назад", callback_data="withdraw_cancel")
    kb.adjust(1)
    return kb.as_markup()


def withdraw_back_kb():
    return None


def withdraw_admin_kb(withdraw_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить", callback_data=f"wd_ok:{withdraw_id}")
    kb.button(text="❌ Отклонить", callback_data=f"wd_no:{withdraw_id}")
    kb.adjust(2)
    return kb.as_markup()

def withdraw_paid_kb(withdraw_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Оплачено", callback_data=f"wd_paid:{withdraw_id}")
    kb.adjust(1)
    return kb.as_markup()


def admin_root_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Сводка", callback_data="admin:summary")
    kb.button(text="📈 Стата групп", callback_data="admin:group_stats_panel")
    kb.button(text="💸 Выводы", callback_data="admin:withdraws")
    kb.button(text="🏦 Казна групп", callback_data="admin:group_finance_panel")
    kb.button(text="⏳ Холд", callback_data="admin:hold")
    kb.button(text="💎 Прайсы", callback_data="admin:prices")
    kb.button(text="👥 Роли", callback_data="admin:roles")
    kb.button(text="🛰 Рабочие зоны", callback_data="admin:workspaces")
    kb.button(text="📦 Очередь", callback_data="admin:queues")
    kb.button(text="👤 Пользователь", callback_data="admin:user_tools")
    kb.button(text="⚙️ Настройки", callback_data="admin:settings")
    kb.adjust(2,2,2,2,1)
    return kb.as_markup()


def admin_back_kb(target: str = "admin:home"):
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data=target)
    return kb.as_markup()

def cancel_inline_kb(target: str = "admin:user_tools"):
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data=target)
    kb.adjust(1)
    return kb.as_markup()

def group_stats_list_kb():
    kb = InlineKeyboardBuilder()
    seen = set()
    try:
        rows = db.list_workspaces()
    except Exception:
        rows = db.conn.execute(
            "SELECT chat_id, thread_id, mode FROM workspaces WHERE is_enabled=1 ORDER BY chat_id DESC, thread_id DESC"
        ).fetchall()

    for row in rows:
        chat_id = int(row["chat_id"])
        thread_id = row["thread_id"]
        mode = row["mode"] if "mode" in row.keys() else None
        key = (chat_id, thread_id)
        if key in seen:
            continue
        seen.add(key)
        prefix = "⏳ " if mode == "hold" else ("⚡ " if mode == "no_hold" else "💬 ")
        label = f"{prefix}{chat_id}" + (f" / topic {thread_id}" if thread_id else "")
        kb.button(text=label[:60], callback_data=f"admin:groupstat:{chat_id}:{thread_id or 0}")

    if not seen:
        kb.button(text="• Пока нет рабочих групп", callback_data="admin:home")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()

def group_finance_list_kb():
    kb = InlineKeyboardBuilder()
    seen = set()
    for row in db.list_workspaces():
        chat_id = int(row['chat_id'])
        raw_thread = row['thread_id']
        thread_id = None if raw_thread in (None, -1) else int(raw_thread)
        key = (chat_id, thread_id)
        if key in seen:
            continue
        seen.add(key)
        label = f"💬 {chat_id}" + (f" / topic {thread_id}" if thread_id else "")
        kb.button(text=label[:60], callback_data=f"admin:groupfin:{chat_id}:{thread_id or 0}")
    if not seen:
        kb.button(text="• Пока нет рабочих групп", callback_data="admin:home")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()

def group_finance_manage_kb(chat_id: int, thread_id: int | None):
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Пополнить", callback_data=f"admin:groupfin_add:{chat_id}:{thread_id or 0}")
    kb.button(text="➖ Списать", callback_data=f"admin:groupfin_sub:{chat_id}:{thread_id or 0}")
    for mode in ('hold', 'no_hold'):
        for key in OPERATORS:
            icon = '⏳' if mode == 'hold' else '⚡'
            kb.button(text=f"{icon} {op_text(key)}", callback_data=f"admin:groupprice:{chat_id}:{thread_id or 0}:{mode}:{key}")
    kb.button(text="↩️ К списку групп", callback_data="admin:group_finance_panel")
    kb.adjust(2,2,2,2,2,2,1)
    return kb.as_markup()

def render_single_group_stats(chat_id: int, thread_id: int | None) -> str:
    totals = db.group_stats(chat_id, thread_id)

    per_operator = db.conn.execute(
        """
        SELECT
            operator_key,
            COUNT(*) AS total,
            SUM(CASE WHEN mode='hold' THEN 1 ELSE 0 END) AS hold_total,
            SUM(CASE WHEN mode='no_hold' THEN 1 ELSE 0 END) AS no_hold_total,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS spent_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
        FROM queue_items
        WHERE charge_chat_id=? AND charge_thread_id=?
        GROUP BY operator_key
        ORDER BY total DESC, operator_key ASC
        """,
        (int(chat_id), db._thread_key(thread_id)),
    ).fetchall()

    per_taker = db.conn.execute(
        """
        SELECT
            taken_by_admin AS taker_user_id,
            COUNT(*) AS total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_total,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total
        FROM queue_items
        WHERE work_chat_id=? AND ((work_thread_id IS NULL AND ? IS NULL) OR work_thread_id=?) AND taken_by_admin IS NOT NULL
        GROUP BY taken_by_admin
        ORDER BY total DESC
        """,
        (chat_id, thread_id, thread_id),
    ).fetchall()

    op_lines = []
    for row in per_operator:
        op_lines.append(
            f"• {op_text(row['operator_key'])}: <b>{int(row['total'] or 0)}</b> "
            f"(⏳ {int(row['hold_total'] or 0)} / ⚡ {int(row['no_hold_total'] or 0)}) • "
            f"💰 юзеру <b>{usd(row['paid_total'] or 0)}</b> • 🏦 <b>{usd(row['spent_total'] or 0)}</b> • 📈 <b>{usd(row['margin_total'] or 0)}</b>"
        )
    if not op_lines:
        op_lines = ["• Пока пусто"]

    taker_lines = []
    for row in per_taker:
        uid = int(row["taker_user_id"])
        user = db.get_user(uid)
        name = escape(user["full_name"]) if user and user["full_name"] else str(uid)
        taker_lines.append(
            f"• <b>{name}</b> — взял: {int(row['total'] or 0)}, "
            f"успешно: {int(row['completed_total'] or 0)}, "
            f"оплат: <b>{usd(row['paid_total'] or 0)}</b>"
        )
    if not taker_lines:
        taker_lines = ["• Пока никто не брал номера"]

    where_label = f"<code>{chat_id}</code>" + (f" / topic <code>{thread_id}</code>" if thread_id else "")
    return (
        "<b>📈 Статистика группы</b>\n\n"
        f"💬 Группа: {where_label}\n\n"
        f"📦 Взято всего: <b>{int(totals['taken_total'] or 0)}</b>\n"
        f"🚀 Начато: <b>{int(totals['started'] or 0)}</b>\n"
        f"✅ Успешно: <b>{int(totals['success'] or 0)}</b>\n"
        f"❌ Слеты: <b>{int(totals['slips'] or 0)}</b>\n"
        f"⚠️ Ошибки: <b>{int(totals['errors'] or 0)}</b>\n"
        f"💰 Выплачено юзерам: <b>{usd(totals['paid_total'] or 0)}</b>\n"
        f"🏦 Списано с казны: <b>{usd(totals['spent_total'] or 0)}</b>\n"
        f"📈 Маржа группы: <b>{usd(totals['margin_total'] or 0)}</b>\n\n"
        "<b>📱 По операторам</b>\n" + "\n".join(op_lines) + "\n\n"
        "<b>👥 Кто сколько взял</b>\n" + "\n".join(taker_lines)
    )

def single_group_stats_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ К списку групп", callback_data="admin:group_stats_panel")
    kb.adjust(1)
    return kb.as_markup()

def user_price_operator_kb(target_user_id: int):
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        kb.button(text=op_text(key), callback_data=f"admin:user_price_op:{target_user_id}:{key}")
    kb.button(text="❌ Отмена", callback_data="admin:user_tools")
    kb.adjust(1)
    return kb.as_markup()

def user_price_mode_kb(target_user_id: int, operator_key: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data=f"admin:user_price_mode:{target_user_id}:{operator_key}:hold")
    kb.button(text="⚡ БезХолд", callback_data=f"admin:user_price_mode:{target_user_id}:{operator_key}:no_hold")
    kb.button(text="❌ Отмена", callback_data="admin:user_tools")
    kb.adjust(2,1)
    return kb.as_markup()

def user_admin_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика пользователя", callback_data="admin:user_stats")
    kb.button(text="💎 Персональный прайс", callback_data="admin:user_set_price")
    kb.button(text="✉️ Написать в ЛС", callback_data="admin:user_pm")
    kb.button(text="➕ Начислить деньги", callback_data="admin:user_add_balance")
    kb.button(text="➖ Снять деньги", callback_data="admin:user_sub_balance")
    kb.button(text="⛔ Заблокировать", callback_data="admin:user_ban")
    kb.button(text="✅ Разблокировать", callback_data="admin:user_unban")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def queue_manage_kb():
    kb = InlineKeyboardBuilder()
    for item in latest_queue_items(10):
        kb.button(text=f"🗑 #{item['id']} {op_text(item['operator_key'])} {mode_label(item['mode'])}", callback_data=f"admin:queue_remove:{item['id']}")
    kb.button(text="↻ Обновить", callback_data="admin:queues")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def roles_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="👑 Назначить главного", callback_data="admin:role:chief_admin")
    kb.button(text="🛡 Назначить админа", callback_data="admin:role:admin")
    kb.button(text="🎧 Назначить оператора", callback_data="admin:role:operator")
    kb.button(text="🗑 Снять роль", callback_data="admin:role:remove")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def workspaces_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить рабочую группу", callback_data="admin:ws_help_group")
    kb.button(text="➕ Добавить топик", callback_data="admin:ws_help_topic")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def design_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Изменить старт", callback_data="admin:set_start_text")
    kb.button(text="📣 Изменить объявление", callback_data="admin:set_ad_text")
    kb.button(text="🧩 Шаблоны", callback_data="admin:templates")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def broadcast_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📨 Написать рассылку", callback_data="admin:broadcast_write")
    kb.button(text="👀 Превью объявления", callback_data="admin:broadcast_preview")
    kb.button(text="🚀 Разослать объявление", callback_data="admin:broadcast_send_ad")
    kb.button(text="📥 Скачать username", callback_data="admin:usernames")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def escape(value: Optional[str]) -> str:
    return html.escape(str(value or "-"))


def queue_caption(item: QueueItem) -> str:
    text = (
        f"📱 {op_html(item.operator_key)}\n\n"
        f"🧾 Заявка: <b>{item.id}</b>\n"
        f"👤 От: <b>{escape(item.full_name)}</b>\n"
        f"🆔 ID: <code>{item.user_id}</code>\n"
        f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
        f"💰 Цена: <b>{usd(item.price)}</b>\n"
        f"🔄 Режим: <b>{'Холд' if item.mode == 'hold' else 'БезХолд'}</b>"
    )
    if item.status == "in_progress":
        text += "\n\n🚀 <b>Работа началась</b>"
        if item.mode == "hold":
            hold_minutes = int(float(db.get_setting("hold_minutes", str(DEFAULT_HOLD_MINUTES))))
            text += (
                f"\n⏳ Холд: <b>{hold_minutes} мин.</b>"
                f"\n📊 {progress_bar(item.hold_until, item.work_started_at)}"
                f"\n⏱ Осталось: <b>{time_left_text(item.hold_until)}</b>"
                f"\n🕓 До: <b>{escape(item.hold_until)}</b>"
            )
        else:
            text += "\n⚡ Режим БезХолд."
    return text


def render_start(user_id: int) -> str:
    user = db.get_user(user_id)
    balance = usd(float(user["balance"] if user else 0))
    username = f"@{escape(user['username'])}" if user and user["username"] else "—"
    title = escape(db.get_setting("start_title", "ESIM Service X"))
    subtitle = escape(db.get_setting("start_subtitle", "Премиум сервис приёма номеров"))
    description = db.get_setting("start_description", "🚀 <b>Быстрый приём заявок</b> • 💎 <b>Стабильные выплаты</b> • 🛡 <b>Контроль статусов</b>")
    price_lines = [
        f"{op_emoji_html('mts')} <b>МТС</b> — <b>{usd(get_mode_price('mts', 'hold', user_id))}</b> / <b>{usd(get_mode_price('mts', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('bil')} <b>Билайн</b> — <b>{usd(get_mode_price('bil', 'hold', user_id))}</b> / <b>{usd(get_mode_price('bil', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('mega')} <b>Мегафон</b> — <b>{usd(get_mode_price('mega', 'hold', user_id))}</b> / <b>{usd(get_mode_price('mega', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('t2')} <b>Tele2</b> — <b>{usd(get_mode_price('t2', 'hold', user_id))}</b> / <b>{usd(get_mode_price('t2', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('vtb')} <b>ВТБ</b> — <b>{usd(get_mode_price('vtb', 'hold', user_id))}</b> / <b>{usd(get_mode_price('vtb', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('gaz')} <b>Газпром</b> — <b>{usd(get_mode_price('gaz', 'hold', user_id))}</b> / <b>{usd(get_mode_price('gaz', 'no_hold', user_id))}</b>",
    ]
    queue_lines = [
        f"{op_emoji_html('mts')} <b>МТС:</b> {count_waiting_mode('mts', 'hold')} / {count_waiting_mode('mts', 'no_hold')}",
        f"{op_emoji_html('bil')} <b>Билайн:</b> {count_waiting_mode('bil', 'hold')} / {count_waiting_mode('bil', 'no_hold')}",
        f"{op_emoji_html('mega')} <b>Мегафон:</b> {count_waiting_mode('mega', 'hold')} / {count_waiting_mode('mega', 'no_hold')}",
        f"{op_emoji_html('t2')} <b>Tele2:</b> {count_waiting_mode('t2', 'hold')} / {count_waiting_mode('t2', 'no_hold')}",
        f"{op_emoji_html('vtb')} <b>ВТБ:</b> {count_waiting_mode('vtb', 'hold')} / {count_waiting_mode('vtb', 'no_hold')}",
        f"{op_emoji_html('gaz')} <b>Газпром:</b> {count_waiting_mode('gaz', 'hold')} / {count_waiting_mode('gaz', 'no_hold')}",
    ]
    return (
        f"<b>💫 {title} 💫</b>\n"
        f"{subtitle}\n\n"
        f"{description}\n\n"
        f"🔗 <b>Username:</b> {username}\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"💰 <b>Баланс:</b> <b>{balance}</b>\n\n"
        f"<b>💎 Прайсы:</b>\n"
        + quote_block(price_lines)
        + "\n\n<b>📤 Очереди:</b>\n"
        + quote_block(queue_lines)
        + "\n\n<b>Вы находитесь в главном меню.</b>\n👇 <b>Выберите нужное действие ниже:</b>"
    )

def render_profile(user_id: int) -> str:
    user = db.get_user(user_id)
    stats = db.user_stats(user_id)
    ops = db.user_operator_stats(user_id)
    current_queue = int((stats['queued'] or 0) + (stats['taken'] or 0) + (stats['in_progress'] or 0))
    username = f"@{escape(user['username'])}" if user and user['username'] else "—"
    full_name = escape(user['full_name'] if user else '')
    payout_link = db.get_payout_link(user_id)
    payout_status = "✅ Привязан" if payout_link else "❌ Не привязан"
    ops_text = "\n".join(
        f"• {op_html(row['operator_key'])}: {row['total']} шт. / <b>{usd(row['earned'] or 0)}</b>"
        for row in ops
    ) or "• <i>Пока пусто</i>"
    personal_price_lines = [
        f"{op_emoji_html('mts')} <b>МТС</b> — <b>{usd(get_mode_price('mts', 'hold', user_id))}</b> / <b>{usd(get_mode_price('mts', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('bil')} <b>Билайн</b> — <b>{usd(get_mode_price('bil', 'hold', user_id))}</b> / <b>{usd(get_mode_price('bil', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('mega')} <b>Мегафон</b> — <b>{usd(get_mode_price('mega', 'hold', user_id))}</b> / <b>{usd(get_mode_price('mega', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('t2')} <b>Tele2</b> — <b>{usd(get_mode_price('t2', 'hold', user_id))}</b> / <b>{usd(get_mode_price('t2', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('vtb')} <b>ВТБ</b> — <b>{usd(get_mode_price('vtb', 'hold', user_id))}</b> / <b>{usd(get_mode_price('vtb', 'no_hold', user_id))}</b>",
        f"{op_emoji_html('gaz')} <b>Газпром</b> — <b>{usd(get_mode_price('gaz', 'hold', user_id))}</b> / <b>{usd(get_mode_price('gaz', 'no_hold', user_id))}</b>",
    ]
    return (
        "<b>👤 Личный кабинет - ESIM Service X 💫</b>\n\n"
        + quote_block([
            f"🔘 <b>Имя:</b> {full_name}",
            f"™️ <b>Username:</b> {username}",
            f"®️ <b>ID:</b> <code>{user_id}</code>",
            f"💲 <b>Баланс:</b> <b>{usd(user['balance'] if user else 0)}</b>",
            f"💳 <b>Счёт CryptoBot:</b> {payout_status}",
        ])
        + "\n\n<b>💎 Ваши прайсы</b>\n"
        + quote_block(personal_price_lines)
        + "\n\n<b>📊 Ваша статистика:</b>\n"
        + quote_block([
            f"🧾 <b>Всего заявок:</b> {int(stats['total'] or 0)}",
            f"✅ <b>Успешно:</b> {int(stats['completed'] or 0)}",
            f"❌ <b>Слеты:</b> {int(stats['slipped'] or 0)}",
            f"⚠️ <b>Ошибки:</b> {int(stats['errors'] or 0)}",
            f"💰 <b>Всего заработано:</b> <b>{usd(stats['earned'] or 0)}</b>",
            f"📤 <b>Сейчас в очередях:</b> {current_queue}",
        ])
        + "\n\n<b>📱 Разбивка по операторам</b>\n"
        + quote_block([ops_text])
        + "\n\n<i>Профиль обновляется автоматически по мере работы в боте.</i>"
    )

def render_withdraw(user_id: int) -> str:
    user = db.get_user(user_id)
    balance = usd(float(user['balance'] if user else 0))
    minimum = usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))
    return (
        "<b>💸 Вывод средств - ESIM Service X 💫</b>\n\n"
        + quote_block([
            f"🔻 <b>Минимальный вывод:</b> {minimum}",
            f"💰 <b>Ваш баланс:</b> {balance}",
        ])
        + "\n\n🔹 <b>Введите сумму вывода в $:</b>"
    )

def render_withdraw_setup() -> str:
    return (
        "<b>Вывод средств - ESIM Service X 💫</b>\n\n"
        "<b>💳 Настройка оплаты (CryptoBot)</b>\n\n"
        "Для получения выплат мне необходима ваша ссылка на многоразовый счет.\n\n"
        "<b>Инструкция:</b>\n"
        "Способ 1: напишите <b>@send</b> и выберите <b>Создать многоразовый счет</b>. Сумму не указывайте.\n\n"
        "Способ 2: В <b>@CryptoBot</b> пропишите <code>/invoices</code> — Создать счёт — Многоразовый — USDT — Далее и скопируйте ссылку.\n\n"
        "👉 <b>Просто отправьте скопированную ссылку прямо мне в чат, и я её запомню.</b>"
    )

def render_my_numbers(user_id: int) -> str:
    items = user_today_queue_items(user_id)
    if not items:
        body = "• За сегодня заявок пока нет."
    else:
        rows = []
        for row in items[:10]:
            pos = queue_position(row['id']) if row['status'] == 'queued' else None
            pos_text = f" • <b>позиция:</b> {pos}" if pos else ""
            rows.append(
                f"#{row['id']} • {op_text(row['operator_key'])} • {mode_label(row['mode'])} • "
                f"{pretty_phone(row['normalized_phone'])} • <b>{status_label_from_row(row)}</b>{pos_text}"
            )
        body = "\n".join(rows)
    return (
        "<b>📦 Мои номера — сегодня</b>\n\n"
        + quote_block([body])
        + "\n\n<i>Здесь можно посмотреть свои заявки за день и убрать из очереди те, что ещё не взяты в работу.</i>"
    )

def render_mirror_menu(user_id: int) -> str:
    rows = db.user_mirrors(user_id)
    if rows:
        body = "\n".join(
            f"• @{escape(row['bot_username'] or 'unknown_bot')} — <b>{'запущено' if row['status'] == 'active' else escape(row['status'])}</b>"
            for row in rows
        )
    else:
        body = "• Пока зеркал нет."
    return (
        "<b>🪞 Зеркало бота</b>\n\n"
        "Здесь можно сохранить токен нового бота от <b>@BotFather</b> и подготовить зеркало.\n"
        "Зеркало не даёт владельцу никаких админ-прав и работает на общей базе.\n\n"
        "<b>Ваши зеркала:</b>\n"
        + body
    )


def render_group_stats_panel() -> str:
    totals = db.conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN taken_by_admin IS NOT NULL THEN 1 ELSE 0 END) AS taken_total,
            SUM(CASE WHEN work_started_at IS NOT NULL THEN 1 ELSE 0 END) AS started,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total
        FROM queue_items
        WHERE work_chat_id IS NOT NULL
        """
    ).fetchone()

    per_operator = db.conn.execute(
        """
        SELECT
            operator_key,
            COUNT(*) AS total,
            SUM(CASE WHEN mode='hold' THEN 1 ELSE 0 END) AS hold_total,
            SUM(CASE WHEN mode='no_hold' THEN 1 ELSE 0 END) AS no_hold_total,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total
        FROM queue_items
        WHERE work_chat_id IS NOT NULL
        GROUP BY operator_key
        ORDER BY total DESC, operator_key ASC
        """
    ).fetchall()

    per_taker = db.conn.execute(
        """
        SELECT
            taken_by_admin AS taker_user_id,
            COUNT(*) AS total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_total,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total
        FROM queue_items
        WHERE work_chat_id IS NOT NULL AND taken_by_admin IS NOT NULL
        GROUP BY taken_by_admin
        ORDER BY total DESC
        """
    ).fetchall()

    op_lines = []
    for row in per_operator:
        op_lines.append(
            f"• {op_text(row['operator_key'])}: <b>{int(row['total'] or 0)}</b> "
            f"(⏳ {int(row['hold_total'] or 0)} / ⚡ {int(row['no_hold_total'] or 0)}) • "
            f"💰 юзеру <b>{usd(row['paid_total'] or 0)}</b> • 🏦 <b>{usd(row['spent_total'] or 0)}</b> • 📈 <b>{usd(row['margin_total'] or 0)}</b>"
        )
    if not op_lines:
        op_lines = ["• Пока пусто"]

    taker_lines = []
    for row in per_taker:
        uid = int(row["taker_user_id"])
        user = db.get_user(uid)
        name = escape(user["full_name"]) if user and user["full_name"] else str(uid)
        taker_lines.append(
            f"• <b>{name}</b> — взял: {int(row['total'] or 0)}, "
            f"успешно: {int(row['completed_total'] or 0)}, "
            f"оплат: <b>{usd(row['paid_total'] or 0)}</b>"
        )
    if not taker_lines:
        taker_lines = ["• Пока никто не брал номера"]

    return (
        "<b>📈 Стата групп</b>\n\n"
        f"📦 Всего заявок в рабочих группах: <b>{int(totals['total'] or 0)}</b>\n"
        f"🙋 Взято: <b>{int(totals['taken_total'] or 0)}</b>\n"
        f"🚀 Начато: <b>{int(totals['started'] or 0)}</b>\n"
        f"✅ Успешно: <b>{int(totals['success'] or 0)}</b>\n"
        f"❌ Слеты: <b>{int(totals['slips'] or 0)}</b>\n"
        f"⚠️ Ошибки: <b>{int(totals['errors'] or 0)}</b>\n"
        f"💰 Тотал оплат: <b>{usd(totals['paid_total'] or 0)}</b>\n\n"
        "<b>📱 По операторам</b>\n" + "\n".join(op_lines) + "\n\n"
        "<b>👥 Кто сколько взял</b>\n" + "\n".join(taker_lines)
    )

def render_admin_home() -> str:
    return (
        "<b>⚙️ Admin Panel — ESIM Service X</b>\n\n"
        f"👑 Главный админ: <code>{CHIEF_ADMIN_ID}</code>\n"
        f"💸 Заявок на вывод: <b>{db.count_pending_withdrawals()}</b>\n"
        f"⏳ Холд: <b>{db.get_setting('hold_minutes')}</b> мин.\n"
        f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
        f"📥 Сдача номеров: <b>{'Включена' if is_numbers_enabled() else 'Выключена'}</b>\n"
        f"🔐 Ваша роль: <b>{user_role(CHIEF_ADMIN_ID)}</b>"
    )


def render_admin_summary() -> str:
    lines = []
    for key, data in OPERATORS.items():
        lines.append(f"• {op_text(key)}: {db.count_waiting(key)}")
    return "<b>📊 Сводка очередей</b>\n\n" + "\n".join(lines)


def render_admin_treasury() -> str:
    recent = db.list_recent_treasury_invoices(5)
    extra = ""
    if recent:
        extra = "\n\n<b>Последние инвойсы:</b>\n" + "\n".join(
            f"• #{row['id']} — {usd(row['amount'])} — <b>{row['status']}</b>" for row in recent
        )
    return f"<b>🏦 Казна</b>\n\n💰 Баланс казны: <b>{usd(db.get_treasury())}</b>{extra}"


def render_admin_withdraws() -> str:
    return f"<b>💸 Выводы</b>\n\n📬 В ожидании: <b>{db.count_pending_withdrawals()}</b>"


def render_admin_hold() -> str:
    return f"<b>⏳ Холд</b>\n\nТекущее время Холд: <b>{db.get_setting('hold_minutes')}</b> мин."


def render_admin_settings() -> str:
    return (
        "<b>⚙️ Настройки системы</b>\n\n"
        f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
        f"📥 Приём номеров: <b>{'Включен' if is_numbers_enabled() else 'Выключен'}</b>\n"
        f"📝 Старт-заголовок: <b>{escape(db.get_setting('start_title', 'ESIM Service X'))}</b>\n"
        f"💸 Канал выплат: <code>{escape(db.get_setting('withdraw_channel_id', str(WITHDRAW_CHANNEL_ID)))}</code>\n"
        f"🧾 Канал логов: <code>{escape(db.get_setting('log_channel_id', str(LOG_CHANNEL_ID)))}</code>\n"
        f"🗄 Канал автобэкапа: <code>{escape(db.get_setting('backup_channel_id', '0'))}</code>\n"
        f"🔁 Автовыгрузка БД: <b>{'Включена' if is_backup_enabled() else 'Выключена'}</b>\n"
        f"📣 Рассылка: <b>{'задана' if db.get_setting('broadcast_text', '').strip() else 'пусто'}</b>"
    )

def render_operator_modes() -> str:
    lines = [f"📥 <b>Общий приём номеров:</b> {'✅ Включен' if is_numbers_enabled() else '🚫 Выключен'}", ""]
    for key in OPERATORS:
        hold_status = "✅" if is_operator_mode_enabled(key, "hold") else "🚫"
        nh_status = "✅" if is_operator_mode_enabled(key, "no_hold") else "🚫"
        lines.append(f"{op_text(key)}\n• Холд: {hold_status}\n• БезХолд: {nh_status}")
    return "<b>🎛 Приём номеров по операторам</b>\n\n" + "\n\n".join(lines)

def hold_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Изменить время Холд", callback_data="admin:set_hold")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()

def prices_kb():
    kb = InlineKeyboardBuilder()
    for mode in ("hold", "no_hold"):
        mode_label_text = "⏳ Холд" if mode == "hold" else "⚡ БезХолд"
        for key in OPERATORS:
            kb.button(text=f"{mode_label_text} • {op_text(key)}", callback_data=f"admin:set_price:{mode}:{key}")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()

def settings_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Мин. вывод", callback_data="admin:set_min_withdraw")
    kb.button(text="📥 Вкл/Выкл приём номеров", callback_data="admin:toggle_numbers")
    kb.button(text="🎛 Приём номеров по операторам", callback_data="admin:operator_modes")
    kb.button(text="✍️ Старт-текст", callback_data="admin:set_start_text")
    kb.button(text="📣 Рассылка", callback_data="admin:broadcast")
    kb.button(text="💳 Канал выплат", callback_data="admin:set_withdraw_channel")
    kb.button(text="🧾 Канал логов", callback_data="admin:set_log_channel")
    kb.button(text="🗄 Канал автобэкапа", callback_data="admin:set_backup_channel")
    kb.button(text="🔁 Автовыгрузка БД", callback_data="admin:toggle_backup")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()

def operator_modes_kb():
    kb = InlineKeyboardBuilder()
    for mode in ("hold", "no_hold"):
        mode_label_text = "⏳ Холд" if mode == "hold" else "⚡ БезХолд"
        for key in OPERATORS:
            status = "✅" if is_operator_mode_enabled(key, mode) else "🚫"
            kb.button(text=f"{status} {mode_label_text} • {op_text(key)}", callback_data=f"admin:toggle_avail:{mode}:{key}")
    kb.button(text="↩️ Назад", callback_data="admin:settings")
    kb.adjust(1)
    return kb.as_markup()



def render_design() -> str:
    return (
        "<b>🎨 Дизайн и тексты</b>\n\n"
        f"🪪 Заголовок: <b>{escape(db.get_setting('start_title', 'DIAMOND HUB'))}</b>\n"
        f"💬 Подзаголовок: <b>{escape(db.get_setting('start_subtitle', ''))}</b>\n"
        f"📣 Рассылка: <b>{'есть' if db.get_setting('announcement_text', '').strip() else 'нет'}</b>\n\n"
        "Здесь можно менять оформление главного экрана и текст рассылки.\n"
        "Поддерживается HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
    )


def render_templates() -> str:
    return (
        "<b>🧩 Шаблоны для объявлений</b>\n\n"
        "<b>Шаблон 1 — премиум:</b>\n"
        "<code>&lt;b&gt;💎 DIAMOND HUB&lt;/b&gt;\n&lt;i&gt;Премиум сервис приёма номеров&lt;/i&gt;\n\n🚀 Быстрый старт • 💰 Выплаты • 🛡 Контроль&lt;/code&gt;\n\n"
        "<b>Шаблон 2 — рассылка:</b>\n"
        "<code>&lt;b&gt;📣 Новое объявление&lt;/b&gt;\n\n• пункт 1\n• пункт 2\n• пункт 3&lt;/code&gt;\n\n"
        "<b>Шаблон 3 — оффер:</b>\n"
        "<code>&lt;b&gt;⚡ Акция дня&lt;/b&gt;\n&lt;blockquote&gt;Короткое описание предложения&lt;/blockquote&gt;&lt;/code&gt;"
    )


def render_broadcast() -> str:
    count = len(db.all_user_ids())
    return (
        "<b>📣 Объявления и рассылки</b>\n\n"
        f"👥 База пользователей: <b>{count}</b>\n"
        f"🔗 Username собрано: <b>{sum(1 for line in db.export_usernames().splitlines() if line.startswith('@'))}</b>\n\n"
        "Здесь можно написать красивое объявление, сохранить его и разослать всем пользователям."
    )


def render_admin_prices() -> str:
    hold_lines = [f"• {op_text(key)}: <b>{usd(get_mode_price(key, 'hold'))}</b>" for key, data in OPERATORS.items()]
    no_hold_lines = [f"• {op_text(key)}: <b>{usd(get_mode_price(key, 'no_hold'))}</b>" for key, data in OPERATORS.items()]
    return "<b>💎 Прайсы</b>\n\n<b>⏳ Холд</b>\n" + "\n".join(hold_lines) + "\n\n<b>⚡ БезХолд</b>\n" + "\n".join(no_hold_lines)


def render_roles() -> str:
    rows = db.list_roles()
    body = []
    for row in rows:
        emoji = "👑" if row["role"] == "chief_admin" else "🛡" if row["role"] == "admin" else "🎧"
        body.append(f"{emoji} <code>{row['user_id']}</code> — <b>{row['role']}</b>")
    return "<b>👥 Роли</b>\n\n" + ("\n".join(body) if body else "Пока пусто")


def render_workspaces() -> str:
    rows = db.list_workspaces()
    if not rows:
        body = "Нет активных рабочих зон.\n\n• /work — включить или выключить группу\n• /topic — включить или выключить топик"
    else:
        body = "\n".join(
            f"• chat <code>{row['chat_id']}</code> | thread <code>{row['thread_id'] or 0}</code> | {row['mode']}"
            for row in rows
        )
    return "<b>🛰 Рабочие зоны</b>\n\n" + body




def mode_label(mode: str) -> str:
    return "Холд" if mode == "hold" else "БезХолд"


def mode_emoji(mode: str) -> str:
    return "⏳" if mode == "hold" else "⚡"


def status_label(status: str, fail_reason: Optional[str] = None) -> str:
    if status == "queued":
        return "В очереди"
    if status == "taken":
        return "Взято"
    if status == "in_progress":
        return "На холде" if fail_reason != "instant" else "В работе"
    if status == "completed":
        return "Успешно"
    if status == "failed":
        if fail_reason and "error" in str(fail_reason):
            return "Ошибка"
        if fail_reason == "slip":
            return "Слет"
        if fail_reason == "admin_removed":
            return "Удалено админом"
        if fail_reason == "user_removed":
            return "Удалено пользователем"
        return "Неуспешно"
    return status

def status_label_from_row(row) -> str:
    return status_label(row["status"], row["fail_reason"] if "fail_reason" in row.keys() else None)

def looks_like_payout_link(raw: str) -> bool:
    raw = (raw or "").strip()
    lowered = raw.lower()
    patterns = [
        "t.me/send?start=",
        "https://t.me/send?start=",
        "http://t.me/send?start=",
        "telegram.me/send?start=",
        "https://telegram.me/send?start=",
        "send?start=iv",
        "start=iv",
    ]
    if any(p in lowered for p in patterns):
        return True
    if "@send" in lowered or "@cryptobot" in lowered:
        return True
    return False


def msk_day_window() -> tuple[str, str]:
    now = msk_now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return fmt_dt(start), fmt_dt(end)


def ensure_extra_schema():
    cur = db.conn.cursor()
    user_cols = {r['name'] for r in cur.execute("PRAGMA table_info(users)").fetchall()}
    if 'is_blocked' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER NOT NULL DEFAULT 0")
    if 'last_seen_at' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_seen_at TEXT")
    wd_cols = {r['name'] for r in cur.execute("PRAGMA table_info(withdrawals)").fetchall()}
    qi_cols = {r['name'] for r in cur.execute("PRAGMA table_info(queue_items)").fetchall()}
    if 'submit_bot_token' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN submit_bot_token TEXT")
    if 'charge_chat_id' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN charge_chat_id INTEGER")
    if 'charge_thread_id' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN charge_thread_id INTEGER")
    if 'charge_amount' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN charge_amount REAL")
    if 'payout_check_id' not in wd_cols:
        cur.execute("ALTER TABLE withdrawals ADD COLUMN payout_check_id INTEGER")
    defaults = {
        'numbers_enabled': '1',
        'start_banner_path': START_BANNER,
        'profile_banner_path': PROFILE_BANNER,
        'my_numbers_banner_path': MY_NUMBERS_BANNER,
        'withdraw_banner_path': WITHDRAW_BANNER,
        'withdraw_channel_id': str(WITHDRAW_CHANNEL_ID),
        'log_channel_id': str(LOG_CHANNEL_ID),
    }
    for mode in ('hold','no_hold'):
        for key,data in OPERATORS.items():
            defaults[f'price_{mode}_{key}'] = str(data['price'])
    for k,v in defaults.items():
        cur.execute("INSERT OR IGNORE INTO settings(key,value) VALUES (?,?)", (k,v))
    db.conn.commit()


ensure_extra_schema()


def create_queue_item_ext(user_id: int, username: str, full_name: str, operator_key: str, normalized_phone: str, qr_file_id: str, mode: str, submit_bot_token: str | None = None):
    cur = db.conn.cursor()
    cur.execute(
        """
        INSERT INTO queue_items (
            user_id, username, full_name, operator_key, phone_label, normalized_phone,
            qr_file_id, status, price, created_at, mode, submit_bot_token
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
        """,
        (
            user_id, username, full_name, operator_key, pretty_phone(normalized_phone), normalized_phone,
            qr_file_id, get_mode_price(operator_key, mode, user_id), now_str(), mode, submit_bot_token or BOT_TOKEN
        ),
    )
    db.conn.commit()
    return cur.lastrowid


def get_mode_price(operator_key: str, mode: str, user_id: int | None = None) -> float:
    if user_id is not None:
        custom = db.get_user_price(user_id, operator_key, mode)
        if custom is not None:
            return float(custom)
    legacy = db.get_setting(f"price_{operator_key}", str(OPERATORS[operator_key]['price']))
    return float(db.get_setting(f"price_{mode}_{operator_key}", legacy))


def count_waiting_mode(operator_key: str, mode: str) -> int:
    row = db.conn.execute("SELECT COUNT(*) AS c FROM queue_items WHERE operator_key=? AND mode=? AND status='queued'", (operator_key, mode)).fetchone()
    return int((row['c'] if row else 0) or 0)


def get_next_queue_item_mode(operator_key: str, mode: str):
    row = db.conn.execute("SELECT * FROM queue_items WHERE operator_key=? AND mode=? AND status='queued' ORDER BY id ASC LIMIT 1", (operator_key, mode)).fetchone()
    return QueueItem(**row) if row else None


def latest_queue_items(limit: int = 10):
    return db.conn.execute("SELECT * FROM queue_items WHERE status='queued' ORDER BY id DESC LIMIT ?", (limit,)).fetchall()


def is_numbers_enabled() -> bool:
    return db.get_setting('numbers_enabled', '1') == '1'


def set_numbers_enabled(flag: bool):
    db.set_setting('numbers_enabled', '1' if flag else '0')

def is_operator_mode_enabled(operator_key: str, mode: str) -> bool:
    return db.get_setting(f"allow_{mode}_{operator_key}", "1") == "1"

def set_operator_mode_enabled(operator_key: str, mode: str, flag: bool):
    db.set_setting(f"allow_{mode}_{operator_key}", "1" if flag else "0")


def is_user_blocked(user_id: int) -> bool:
    row = db.conn.execute("SELECT is_blocked FROM users WHERE user_id=?", (user_id,)).fetchone()
    return bool(row and row['is_blocked'])


def set_user_blocked(user_id: int, flag: bool):
    db.conn.execute("UPDATE users SET is_blocked=? WHERE user_id=?", (1 if flag else 0, user_id))
    db.conn.commit()


def queue_item_submit_token(item) -> str:
    token = getattr(item, "submit_bot_token", None)
    if token is None and hasattr(item, 'keys'):
        token = item["submit_bot_token"] if "submit_bot_token" in item.keys() else None
    return (token or BOT_TOKEN).strip() or BOT_TOKEN

async def send_item_user_message(preferred_bot: Bot | None, item, text: str):
    token = queue_item_submit_token(item)
    bot_to_use = None
    close_after = False
    try:
        if preferred_bot is not None and getattr(preferred_bot, 'token', None) == token:
            bot_to_use = preferred_bot
        else:
            live = LIVE_MIRROR_TASKS.get(token)
            bot_to_use = live.get('bot') if live else None
            if bot_to_use is None:
                bot_to_use = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
                close_after = True
        uid = int(getattr(item, 'user_id', item['user_id']))
        await bot_to_use.send_message(uid, text)
    finally:
        if close_after and bot_to_use is not None:
            await bot_to_use.session.close()


async def send_queue_item_photo_to_chat(target_bot: Bot, chat_id: int, item, caption: str, reply_markup=None, message_thread_id: int | None = None):
    token = queue_item_submit_token(item)
    source_bot = None
    close_after = False
    photo = getattr(item, 'qr_file_id', None)
    if photo is None and hasattr(item, 'keys'):
        photo = item['qr_file_id']
    try:
        if token == getattr(target_bot, 'token', None):
            try:
                return await target_bot.send_photo(chat_id, photo, caption=caption, reply_markup=reply_markup, message_thread_id=message_thread_id)
            except Exception:
                logging.exception('send_photo by file_id failed, trying download+reupload')
        live = LIVE_MIRROR_TASKS.get(token)
        source_bot = live.get('bot') if live else None
        if source_bot is None:
            source_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            close_after = True
        telegram_file = await source_bot.get_file(photo)
        file_bytes = io.BytesIO()
        await source_bot.download_file(telegram_file.file_path, destination=file_bytes)
        file_bytes.seek(0)
        upload = BufferedInputFile(file_bytes.read(), filename=f"queue_{getattr(item, 'id', 'item')}.jpg")
        return await target_bot.send_photo(chat_id, upload, caption=caption, reply_markup=reply_markup, message_thread_id=message_thread_id)
    finally:
        if close_after and source_bot is not None:
            await source_bot.session.close()

def group_price_for_take(chat_id: int, thread_id: int | None, operator_key: str, mode: str) -> float:
    price = db.get_group_price(chat_id, thread_id, operator_key, mode)
    if price is not None:
        return float(price)
    return float(get_mode_price(operator_key, mode, None))

def render_group_finance(chat_id: int, thread_id: int | None) -> str:
    where_label = f"<code>{chat_id}</code>" + (f" / topic <code>{thread_id}</code>" if thread_id else "")
    balance = db.get_group_balance(chat_id, thread_id)
    reserved_row = db.conn.execute(
        "SELECT SUM(charge_amount) AS s FROM queue_items WHERE charge_chat_id=? AND charge_thread_id=? AND status IN ('taken','in_progress')",
        (int(chat_id), db._thread_key(thread_id)),
    ).fetchone()
    reserved = float(reserved_row['s'] or 0)
    lines = [
        "<b>🏦 Казна группы</b>",
        "",
        f"💬 Группа: {where_label}",
        f"💰 Доступно: <b>{usd(balance)}</b>",
        f"🔒 В резерве: <b>{usd(reserved)}</b>",
        "",
        "<b>Прайсы группы для операторов</b>",
    ]
    for key in OPERATORS:
        lines.append(f"• {op_text(key)} — ⏳ {usd(group_price_for_take(chat_id, thread_id, key, 'hold'))} / ⚡ {usd(group_price_for_take(chat_id, thread_id, key, 'no_hold'))}")
    return "\n".join(lines)

def touch_user(user_id: int, username: str, full_name: str):
    db.upsert_user(user_id, username or '', full_name or '')
    db.conn.execute("UPDATE users SET last_seen_at=? WHERE user_id=?", (now_str(), user_id))
    db.conn.commit()


def phone_locked_until_next_msk_day(normalized_phone: str) -> bool:
    start, end = msk_day_window()
    row = db.conn.execute(
        "SELECT COUNT(*) AS c FROM queue_items WHERE normalized_phone=? AND work_started_at IS NOT NULL AND work_started_at >= ? AND work_started_at < ?",
        (normalized_phone, start, end),
    ).fetchone()
    return int((row["c"] if row else 0) or 0) >= 2


def user_today_queue_items(user_id: int):
    start, end = msk_day_window()
    return db.conn.execute(
        "SELECT * FROM queue_items WHERE user_id=? AND created_at >= ? AND created_at < ? ORDER BY id DESC",
        (user_id, start, end),
    ).fetchall()


def queue_position(item_id: int):
    row = db.conn.execute("SELECT operator_key, mode, status FROM queue_items WHERE id=?", (item_id,)).fetchone()
    if not row or row['status'] != 'queued':
        return None
    pos = db.conn.execute(
        "SELECT COUNT(*) AS c FROM queue_items WHERE operator_key=? AND mode=? AND status='queued' AND id <= ?",
        (row['operator_key'], row['mode'], item_id),
    ).fetchone()
    return int((pos['c'] if pos else 0) or 0)


def remove_queue_item(item_id: int, reason: str = 'removed', admin_id: int | None = None):
    db.conn.execute("UPDATE queue_items SET status='failed', fail_reason=?, completed_at=? WHERE id=? AND status='queued'", (reason, now_str(), item_id))
    db.conn.commit()


def get_user_full_stats(target_user_id: int):
    user = db.get_user(target_user_id)
    stats = db.user_stats(target_user_id)
    ops = db.user_operator_stats(target_user_id)
    return user, stats, ops


def find_user_text(target_user_id: int) -> str:
    user, stats, ops = get_user_full_stats(target_user_id)
    if not user:
        return "❌ Пользователь не найден в базе."
    ops_text = "\n".join([f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}" for row in ops]) or "• Пока пусто"
    return (
        f"<b>👤 Пользователь</b>\n\n"
        f"🆔 <code>{target_user_id}</code>\n"
        f"🔗 Username: <b>{escape(user['username']) or '—'}</b>\n"
        f"👤 Имя: <b>{escape(user['full_name'])}</b>\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n"
        f"⛔ Статус: <b>{'Заблокирован' if user['is_blocked'] else 'Активен'}</b>\n\n"
        f"📊 Всего заявок: <b>{int(stats['total'] or 0)}</b>\n"
        f"✅ Успешно: <b>{int(stats['completed'] or 0)}</b>\n"
        f"❌ Слеты: <b>{int(stats['slipped'] or 0)}</b>\n"
        f"⚠️ Ошибки: <b>{int(stats['errors'] or 0)}</b>\n"
        f"💵 Заработано: <b>{usd(stats['earned'] or 0)}</b>\n\n"
        f"<blockquote>{ops_text}</blockquote>"
    )


def quote_block(lines: list[str]) -> str:
    return '<blockquote>' + '\n'.join(lines) + '</blockquote>'


def cancel_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="submit:cancel")
    kb.adjust(1)
    return kb.as_markup()

async def safe_edit_or_send(callback: CallbackQuery, text: str, reply_markup=None):
    msg = callback.message
    try:
        if getattr(msg, "photo", None):
            await msg.edit_caption(caption=text, reply_markup=reply_markup)
        else:
            await msg.edit_text(text=text, reply_markup=reply_markup)
    except Exception:
        await msg.answer(text, reply_markup=reply_markup)


CUSTOM_OPERATOR_EMOJI = {
    "mts": ("5312126452043363774", "🔴"),
    "mega": ("5229218997521631084", "🟢"),
    "bil": ("5280919528908267119", "🟡"),
    "t2": ("5244453379664534900", "⚫"),
    "vtb": ("5427154326294376920", "🔵"),
    "gaz": ("5280751174780199841", "🔷"),
}

def op_emoji_html(operator_key: str) -> str:
    emoji_id, fallback = CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'
    return fallback

def op_html(operator_key: str) -> str:
    return f"{op_emoji_html(operator_key)} <b>{escape(OPERATORS[operator_key]['title'])}</b>"

def op_text(operator_key: str) -> str:
    fallback = CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))[1]
    return f"{fallback} {OPERATORS[operator_key]['title']}"


async def send_banner_message(entity, banner_path: str, caption: str, reply_markup=None):
    if Path(banner_path).exists():
        if hasattr(entity, 'answer_photo'):
            return await entity.answer_photo(FSInputFile(banner_path), caption=caption, reply_markup=reply_markup)
        return await entity.message.answer_photo(FSInputFile(banner_path), caption=caption, reply_markup=reply_markup)
    if hasattr(entity, 'answer'):
        return await entity.answer(caption, reply_markup=reply_markup)
    return await entity.message.answer(caption, reply_markup=reply_markup)


async def replace_banner_message(callback: CallbackQuery, banner_path: str, caption: str, reply_markup=None):
    try:
        await callback.message.delete()
    except Exception:
        pass
    return await send_banner_message(callback, banner_path, caption, reply_markup)

async def remove_reply_keyboard(entity):
    try:
        if hasattr(entity, 'answer'):
            await entity.answer(' ', reply_markup=ReplyKeyboardRemove())
        else:
            await entity.message.answer(' ', reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass


def blocked_text() -> str:
    return "<b>⛔ Доступ ограничен</b>\n\nВаш аккаунт заблокирован администрацией."

async def notify_user(bot: Bot, user_id: int, text: str):
    try:
        await bot.send_message(user_id, text)
    except Exception:
        logging.exception("notify_user failed")



async def send_db_backup(bot: Bot, reason: str = "auto"):
    channel_id = backup_channel_id()
    if not channel_id:
        return False
    db_path = Path(DB_PATH)
    if not db_path.exists():
        logging.warning("DB backup skipped: DB file not found")
        return False
    backup_dir = Path("db_backups")
    backup_dir.mkdir(exist_ok=True)
    stamp = msk_now().strftime("%Y%m%d_%H%M%S")
    target = backup_dir / f"botdb_{reason}_{stamp}.db"
    try:
        target.write_bytes(db_path.read_bytes())
        caption = (
            "<b>🗄 Автовыгрузка базы данных</b>\n\n"
            f"🕒 {escape(now_str())}\n"
            f"🔖 Причина: <b>{escape(reason)}</b>"
        )
        await bot.send_document(channel_id, FSInputFile(str(target)), caption=caption)
        logging.info("DB backup sent to %s (%s)", channel_id, reason)
        return True
    except Exception:
        logging.exception("send_db_backup failed")
        return False

async def backup_watcher(bot: Bot):
    while True:
        try:
            if is_backup_enabled() and backup_channel_id():
                await send_db_backup(bot, "auto_15m")
        except Exception:
            logging.exception("backup_watcher failed")
        await asyncio.sleep(900)

async def send_log(bot: Bot, text: str):
    logging.info(re.sub(r"<[^>]+>", "", text))
    channel_id = int(db.get_setting("log_channel_id", str(LOG_CHANNEL_ID) or "0") or 0)
    if channel_id:
        try:
            await bot.send_message(channel_id, text)
        except Exception:
            logging.exception("send_log failed")

def resolve_user_input(raw: str):
    raw = (raw or "").strip()
    if not raw:
        return None

    if raw.lstrip("-").isdigit():
        user = db.get_user(int(raw))
        if user:
            return user

    username = raw.lstrip("@").strip().lower()
    if username:
        user = db.find_user_by_username(username)
        if user:
            return user
        user = db.conn.execute(
            "SELECT * FROM users WHERE lower(username)=? OR lower(username) LIKE ? ORDER BY user_id DESC LIMIT 1",
            (username, f"%{username}%"),
        ).fetchone()
        if user:
            return user

    cleaned = re.sub(r"\D", "", raw)
    if cleaned:
        user = db.find_last_user_by_phone(cleaned)
        if user:
            return user
        variants = []
        if cleaned.startswith("8") and len(cleaned) == 11:
            variants += ["7" + cleaned[1:], "+" + "7" + cleaned[1:]]
        elif cleaned.startswith("7") and len(cleaned) == 11:
            variants += ["8" + cleaned[1:], "+" + cleaned]
        else:
            variants += ["+" + cleaned]
        for v in variants:
            user = db.find_last_user_by_phone(v)
            if user:
                return user
    return None


async def create_crypto_invoice(amount: float, description: str = "Treasury top up") -> tuple[Optional[str], Optional[str], str]:
    if not CRYPTO_PAY_TOKEN:
        return None, None, "CRYPTO_PAY_TOKEN не заполнен."
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    payload = {
        "asset": CRYPTO_PAY_ASSET,
        "amount": f"{amount:.2f}",
        "description": description[:1024],
        "allow_anonymous": True,
        "allow_comments": False,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_PAY_BASE_URL}/createInvoice", json=payload, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get("ok"):
            return None, None, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        result = data.get("result", {})
        return str(result.get("invoice_id") or ""), result.get("pay_url") or result.get("bot_invoice_url"), "Инвойс создан."
    except Exception as e:
        return None, None, f"Ошибка создания инвойса: {e}"

async def get_crypto_invoice(invoice_id: str) -> tuple[Optional[dict], str]:
    if not CRYPTO_PAY_TOKEN:
        return None, "CRYPTO_PAY_TOKEN не заполнен."
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{CRYPTO_PAY_BASE_URL}/getInvoices", params={"invoice_ids": str(invoice_id)}, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get("ok"):
            return None, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        items = data.get("result", {}).get("items", [])
        return (items[0] if items else None), "ok"
    except Exception as e:
        return None, f"Ошибка проверки инвойса: {e}"

async def create_crypto_check(amount: float, user_id: Optional[int] = None) -> tuple[Optional[int], Optional[str], str]:
    if not CRYPTO_PAY_TOKEN:
        return None, None, "CRYPTO_PAY_TOKEN не заполнен, поэтому выдана ручная заявка вместо чека."
    payload = {"asset": CRYPTO_PAY_ASSET, "amount": f"{amount:.2f}"}
    if CRYPTO_PAY_PIN_CHECK_TO_USER and user_id:
        payload["pin_to_user_id"] = int(user_id)
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_PAY_BASE_URL}/createCheck", json=payload, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get("ok"):
            return None, None, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        result = data.get("result", {})
        return result.get('check_id'), result.get("bot_check_url") or result.get("url"), "Чек создан через Crypto Bot."
    except Exception as e:
        return None, None, f"Ошибка создания чека: {e}"


async def delete_crypto_check(check_id: int) -> tuple[bool, str]:
    if not CRYPTO_PAY_TOKEN:
        return False, "CRYPTO_PAY_TOKEN не заполнен."
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_PAY_BASE_URL}/deleteCheck", json={"check_id": int(check_id)}, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get('ok'):
            return False, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        return True, "Чек удалён"
    except Exception as e:
        return False, f"Ошибка удаления чека: {e}"


@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    touch_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    await state.clear()
    if is_user_blocked(message.from_user.id):
        await remove_reply_keyboard(message)
        await message.answer(blocked_text())
        return
    await remove_reply_keyboard(message)
    await send_banner_message(message, db.get_setting('start_banner_path', START_BANNER), render_start(message.from_user.id), main_menu())


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()

@router.callback_query(F.data == "menu:home")
async def menu_home(callback: CallbackQuery, state: FSMContext):
    touch_user(callback.from_user.id, callback.from_user.username or "", callback.from_user.full_name)
    await state.clear()
    if is_user_blocked(callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
    else:
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
    await callback.answer()


@router.callback_query(F.data == "menu:mirror")
async def mirror_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        render_mirror_menu(callback.from_user.id),
        mirror_menu_kb(),
    )
    await callback.answer()

@router.callback_query(F.data == "mirror:list")
async def mirror_list(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        render_mirror_menu(callback.from_user.id),
        mirror_menu_kb(),
    )
    await callback.answer()

@router.callback_query(F.data == "mirror:create")
async def mirror_create(callback: CallbackQuery, state: FSMContext):
    await state.set_state(MirrorStates.waiting_token)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:mirror")
    kb.adjust(1)
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        "<b>🪞 Создание зеркала</b>\n\n"
        "Отправьте <b>API token</b> нового бота от <b>@BotFather</b>.\n"
        "Этот бот будет сохранён как зеркало сервиса без выдачи дополнительных прав.",
        kb.as_markup(),
    )
    await callback.answer()

@router.message(MirrorStates.waiting_token)
async def mirror_token_received(message: Message, state: FSMContext):
    token = (message.text or "").strip()
    if ":" not in token:
        await message.answer("⚠️ Отправьте корректный токен бота от @BotFather.")
        return
    try:
        test_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        me = await test_bot.get_me()
        await test_bot.session.close()
    except Exception:
        await message.answer("❌ Не удалось проверить токен. Проверьте его и попробуйте ещё раз.")
        return
    db.save_mirror(
        message.from_user.id,
        message.from_user.username or "",
        token,
        int(me.id),
        me.username or "",
        me.full_name or "",
    )
    started, info = await start_live_mirror(token)
    await state.clear()
    extra = "Зеркало сразу запущено и уже должно отвечать." if started else f"Зеркало сохранено, но автозапуск сейчас не удался: {escape(str(info))}"
    await send_banner_message(
        message,
        db.get_setting('start_banner_path', START_BANNER),
        "<b>✅ Зеркало сохранено</b>\n\n"
        f"🤖 Бот: @{escape(me.username or '')}\n"
        f"🆔 ID: <code>{me.id}</code>\n\n"
        f"{extra}",
        mirror_menu_kb(),
    )

@router.callback_query(F.data == "menu:my")
async def menu_my(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    items = user_today_queue_items(callback.from_user.id)
    await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id), my_numbers_kb(items))
    await callback.answer()

@router.callback_query(F.data == "menu:profile")
async def menu_profile(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await replace_banner_message(callback, db.get_setting('profile_banner_path', PROFILE_BANNER), render_profile(callback.from_user.id), profile_kb())
    await callback.answer()

@router.callback_query(F.data == "menu:withdraw")
async def menu_withdraw(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    payout_link = db.get_payout_link(callback.from_user.id)
    if not payout_link:
        kb = InlineKeyboardBuilder()
        kb.button(text="↩️ Назад", callback_data="menu:profile")
        kb.adjust(1)
        await replace_banner_message(callback, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw_setup(), kb.as_markup())
        await state.set_state(WithdrawStates.waiting_payment_link)
    else:
        await state.set_state(WithdrawStates.waiting_amount)
        await replace_banner_message(callback, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(callback.from_user.id), cancel_inline_kb("menu:profile"))
    await callback.answer()

@router.callback_query(F.data == "menu:payout_link")
async def payout_link_cb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_payment_link)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:profile")
    kb.adjust(1)
    await replace_banner_message(
        callback,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        render_withdraw_setup(),
        kb.as_markup(),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("submit_more:"))
async def submit_more(callback: CallbackQuery, state: FSMContext):
    if is_user_blocked(callback.from_user.id):
        await callback.answer("Аккаунт заблокирован", show_alert=True)
        return
    if not is_numbers_enabled():
        await callback.answer("Сдача номеров выключена", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return
    _, operator_key, mode = parts
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if mode not in {"hold", "no_hold"}:
        await callback.answer("Неизвестный режим", show_alert=True)
        return
    if not is_operator_mode_enabled(operator_key, mode):
        await callback.answer("Сдача по этому оператору и режиму сейчас выключена.", show_alert=True)
        return

    await state.update_data(operator_key=operator_key, mode=mode)
    await state.set_state(SubmitStates.waiting_qr)
    await callback.message.answer(
        "<b>📨 Загрузите следующий QR-код</b>\n\n"
        f"📱 <b>Оператор:</b> {op_html(operator_key)}\n"
        f"🔄 <b>Режим:</b> {mode_label(mode)}\n"
        f"💰 <b>Цена:</b> <b>{usd(get_mode_price(operator_key, mode, callback.from_user.id))}</b>\n\n"
        "Отправьте <b>ещё одно фото QR</b> с подписью-номером другого номера.\n"
        "Когда закончите, нажмите <b>«Я закончил загрузку»</b>.",
        reply_markup=cancel_inline_kb("menu:home"),
    )
    await callback.answer("Можно загружать следующий QR")

@router.callback_query(F.data == "menu:submit")
async def submit_start_cb(callback: CallbackQuery, state: FSMContext):
    if is_user_blocked(callback.from_user.id):
        await callback.answer("Аккаунт заблокирован", show_alert=True)
        return
    if not is_numbers_enabled():
        await callback.answer("Сдача номеров выключена", show_alert=True)
        return
    await state.set_state(SubmitStates.waiting_mode)
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b>💫 ESIM Service X 💫</b>\n\n<b>📲 Сдать номер - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
    await callback.answer()


@router.callback_query(F.data == "mode:back")
async def mode_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if is_user_blocked(callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
    else:
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
    await callback.answer()

@router.callback_query(F.data.startswith("mode:"))
async def choose_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split(":", 1)[1]
    if mode not in {"hold", "no_hold"}:
        await callback.answer()
        return
    await state.update_data(mode=mode)
    await state.set_state(SubmitStates.waiting_operator)
    mode_title = "⏳ Холд" if mode == "hold" else "⚡ БезХолд"
    mode_desc = (
        "🔥 <b>Холд</b> — режим работы с временной фиксацией номера.\n"
        "💰 Актуальные ставки смотрите в разделе <b>/start</b> — <b>«Прайсы»</b>."
        if mode == "hold"
        else "🔥 <b>БезХолд</b> — режим работы без времени работы, оплату по режимам смотрите в разделе <b>/start</b> — <b>«Прайсы»</b>."
    )
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        f"<b>Режим выбран: {mode_title}</b>\n\n{mode_desc}\n\n👇 <b>Теперь выберите оператора:</b>",
        operators_kb(mode, "op", "op:back", callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "op:back")
async def op_back(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubmitStates.waiting_mode)
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b>💫 ESIM Service X 💫</b>\n\n<b>📲 Сдать номер - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("op:"))
async def choose_operator(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    operator_key = parts[1]
    mode = parts[2] if len(parts) > 2 else (await state.get_data()).get("mode", "hold")
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if not is_operator_mode_enabled(operator_key, mode):
        await callback.answer("Сдача по этому оператору и режиму сейчас выключена.", show_alert=True)
        return
    await state.update_data(operator_key=operator_key, mode=mode)
    await state.set_state(SubmitStates.waiting_qr)
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        "<b>💫 ESIM Service X 💫</b>\n\n<b>📨 Отправьте QR-код - Фото сообщением</b>\n\n👉 <b>Требуется:</b>\n▫️ Фото QR\n▫️ В подписи укажите номер\n\n🔰 <b>Допустимый формат номера:</b>\n<blockquote>+79991234567  «+7»\n79991234567   «7»\n89991234567   «8»</blockquote>\n\nЕсли передумали нажмите ниже - Отмена",
        cancel_inline_kb("op:back"),
    )
    await callback.answer()


@router.message(WithdrawStates.waiting_amount, F.text == "↩️ Назад")
@router.message(WithdrawStates.waiting_payment_link, F.text == "↩️ Назад")
async def global_back(message: Message, state: FSMContext):
    await state.clear()
    await send_banner_message(message, db.get_setting('start_banner_path', START_BANNER), render_start(message.from_user.id), main_menu())


@router.message(SubmitStates.waiting_qr, F.photo)
async def submit_qr(message: Message, state: FSMContext):
    caption = (message.caption or "").strip()
    phone = normalize_phone(caption)
    if not phone:
        await message.answer(
            "⚠️ Номер должен быть только в формате:\n<code>+79991234567</code>\n<code>79991234567</code>\n<code>89991234567</code>",
            reply_markup=cancel_menu(),
        )
        return
    data = await state.get_data()
    operator_key = data.get("operator_key")
    mode = data.get("mode", "hold")
    if operator_key not in OPERATORS:
        await message.answer("⚠️ Оператор не выбран. Начните заново.", reply_markup=main_menu())
        await state.clear()
        return
    touch_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    if phone_locked_until_next_msk_day(phone):
        await message.answer("<b>⛔ Этот номер уже вставал сегодня.</b>\n\nПовторная сдача будет доступна после <b>00:00 МСК следующего дня</b>.", reply_markup=cancel_inline_kb())
        return
    file_id = message.photo[-1].file_id
    item_id = create_queue_item_ext(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.full_name,
        operator_key,
        phone,
        file_id,
        mode,
        getattr(message.bot, "token", BOT_TOKEN),
    )
    await state.clear()
    await message.answer(
        "<b>✅ Заявка принята</b>\n\n"
        f"🧾 ID заявки: <b>{item_id}</b>\n"
        f"📱 Оператор: {op_html(operator_key)}\n"
        f"📞 Номер: <code>{pretty_phone(phone)}</code>\n"
        f"💰 Цена: <b>{usd(get_mode_price(operator_key, mode, message.from_user.id))}</b>\n"
        f"🔄 Режим: <b>{'Холд' if mode == 'hold' else 'БезХолд'}</b>",
        reply_markup=submit_result_kb(operator_key, mode),
    )


@router.message(SubmitStates.waiting_qr)
async def submit_not_photo(message: Message):
    await message.answer("<b>⚠️ Отправьте именно фото QR-кода с подписью-номером.</b>", reply_markup=cancel_menu())


@router.message(F.text == "💸 Вывод средств")
async def withdraw_start(message: Message, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_amount)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:home")
    kb.adjust(1)
    await send_banner_message(message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(message.from_user.id), kb.as_markup())


@router.message(WithdrawStates.waiting_payment_link)
async def withdraw_payment_link(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not looks_like_payout_link(raw):
        await message.answer(
            "<b>⚠️ Ссылка не распознана.</b>\n\n"
            "Отправьте именно ссылку на многоразовый счёт CryptoBot.\n"
            "Пример: <code>https://t.me/send?start=IV...</code>",
            reply_markup=cancel_inline_kb("menu:profile"),
        )
        return
    db.set_payout_link(message.from_user.id, raw)
    await state.set_state(WithdrawStates.waiting_amount)
    await send_banner_message(
        message,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        "<b>✅ Счёт для выплат сохранён</b>\n\nТеперь можно оформить вывод.",
        None,
    )
    await send_banner_message(
        message,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        render_withdraw(message.from_user.id),
        cancel_inline_kb("menu:profile"),
    )

@router.message(WithdrawStates.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".")
    try:
        amount = float(raw)
    except Exception:
        user = db.get_user(message.from_user.id)
        balance = float(user["balance"] if user else 0)
        minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
        await message.answer(
            "<b>💸 Вывод средств</b>\n\n"
            f"📉 Минимальный вывод: <b>{usd(minimum)}</b>\n"
            f"💰 Ваш баланс: <b>{usd(balance)}</b>\n\n"
            "⚠️ Введите сумму числом. Например: <code>12.5</code>",
            reply_markup=cancel_inline_kb("menu:profile"),
        )
        return
    minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
    user = db.get_user(message.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount < minimum:
        await message.answer(f"⚠️ <b>Сумма меньше минимальной.</b> Минимум: <b>{usd(minimum)}</b>", reply_markup=cancel_inline_kb("menu:profile"))
        return
    if amount > balance:
        await message.answer("⚠️ <b>Недостаточно средств на балансе.</b>", reply_markup=cancel_inline_kb("menu:profile"))
        return
    await state.clear()
    await message.answer(
        "<b>Подтверждение вывода</b>\n\n"
        f"🗓 Дата: <b>{now_str()}</b>\n"
        f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
        "Подтвердить создание заявки?",
        reply_markup=confirm_withdraw_kb(amount),
    )


@router.callback_query(F.data == "withdraw_cancel")
async def withdraw_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Вывод отменён.")
    await send_banner_message(callback.message, db.get_setting('profile_banner_path', PROFILE_BANNER), render_profile(callback.from_user.id), profile_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("withdraw_confirm:"))
async def withdraw_confirm(callback: CallbackQuery):
    amount = float(callback.data.split(":", 1)[1])
    user = db.get_user(callback.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount > balance:
        await callback.answer("Недостаточно средств на балансе", show_alert=True)
        return
    db.subtract_balance(callback.from_user.id, amount)
    wd_id = db.create_withdrawal(callback.from_user.id, amount)
    payout_link = db.get_payout_link(callback.from_user.id) or "—"
    text = (
        "<b>📨 Новая заявка на вывод</b>\n\n"
        f"🧾 ID: <b>{wd_id}</b>\n"
        f"👤 Пользователь: <b>{escape(callback.from_user.full_name)}</b>\n"
        f"🆔 ID: <code>{callback.from_user.id}</code>\n"
        f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}"
    )
    try:
        await callback.bot.send_message(int(db.get_setting("withdraw_channel_id", str(WITHDRAW_CHANNEL_ID))), text, reply_markup=withdraw_admin_kb(wd_id))
    except Exception:
        logging.exception("send withdraw to channel failed")
    await callback.message.edit_text("✅ Заявка на вывод создана. Она отправлена в канал выплат.")
    await send_banner_message(callback.message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(callback.from_user.id), cancel_inline_kb("menu:profile"))
    await callback.answer()



@router.callback_query(F.data.startswith("wd_ok:"))
async def wd_ok(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    payout_link = db.get_payout_link(int(wd["user_id"])) or "—"
    db.set_withdrawal_status(withdraw_id, "approved", callback.from_user.id, payout_link, "approved_waiting_payment")

    await callback.message.edit_text(
        "<b>✅ Заявка на вывод одобрена</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}\n\n"
        "Статус: <b>Ожидает оплаты</b>",
        reply_markup=withdraw_paid_kb(withdraw_id),
    )
    await callback.answer("Одобрено")

@router.callback_query(F.data.startswith("wd_paid:"))
async def wd_paid(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] not in {"pending", "approved"}:
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    payout_link = db.get_payout_link(int(wd["user_id"])) or (wd["payout_check"] if "payout_check" in wd.keys() else "—")
    db.set_withdrawal_status(withdraw_id, "approved", callback.from_user.id, payout_link, "paid")

    try:
        await callback.bot.send_message(
            int(wd["user_id"]),
            "<b>✅ Выплата отправлена</b>\n\n"
            f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n"
            "Статус: <b>Оплачено</b>\n\n"
            "Средства отправлены на ваш привязанный счёт CryptoBot."
        )
    except Exception:
        logging.exception("send withdraw paid notify failed")

    await callback.message.edit_text(
        "<b>✅ Заявка на вывод обработана</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}\n\n"
        "Статус: <b>Оплачено</b>"
    )
    await callback.answer("Оплачено")

@router.callback_query(F.data.startswith("wd_no:"))
async def wd_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return
    db.add_balance(int(wd["user_id"]), float(wd["amount"]))
    db.set_withdrawal_status(withdraw_id, "rejected", callback.from_user.id, None, "rejected")
    try:
        await callback.bot.send_message(
            int(wd["user_id"]),
            "<b>❌ Заявка на вывод отклонена</b>\n\n"
            f"💸 Сумма возвращена на баланс: <b>{usd(float(wd['amount']))}</b>"
        )
    except Exception:
        logging.exception("send withdraw rejected failed")
    await callback.message.edit_text(
        "<b>❌ Заявка на вывод отклонена</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n"
        "Деньги возвращены на баланс пользователя."
    )
    await callback.answer("Отклонено")

@router.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer(render_admin_home(), reply_markup=admin_root_kb())


@router.callback_query(F.data == "admin:home")
async def admin_home(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text(render_admin_home(), reply_markup=admin_root_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:summary")
async def admin_summary(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_summary(), reply_markup=admin_back_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:treasury")
async def admin_treasury(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_treasury(), reply_markup=treasury_kb())
    await callback.answer()



@router.callback_query(F.data == "admin:treasury_check")
async def admin_treasury_check(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    added = 0.0
    for row in db.list_recent_treasury_invoices(10):
        if row["status"] != "active" or not row["crypto_invoice_id"]:
            continue
        info, _ = await get_crypto_invoice(row["crypto_invoice_id"])
        if info and str(info.get("status", "")).lower() == "paid":
            db.mark_treasury_invoice_paid(int(row["id"]))
            db.add_treasury(float(row["amount"]))
            added += float(row["amount"])
    await callback.message.edit_text(
        render_admin_treasury() + (f"\n\n✅ Подтверждено пополнений: <b>{usd(added)}</b>" if added else "\n\nПлатежей пока не найдено."),
        reply_markup=treasury_kb()
    )
    await callback.answer()

@router.callback_query(F.data == "admin:withdraws")
async def admin_withdraws(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_withdraws(), reply_markup=admin_back_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:hold")
async def admin_hold(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_hold(), reply_markup=hold_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:prices")
async def admin_prices(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_prices(), reply_markup=prices_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:roles")
async def admin_roles(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_roles(), reply_markup=roles_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:workspaces")
async def admin_workspaces(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_workspaces(), reply_markup=workspaces_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:group_stats_panel")
async def admin_group_stats_panel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, "<b>📈 Выберите группу / топик для статистики:</b>", reply_markup=group_stats_list_kb())
    await callback.answer()

@router.callback_query(F.data.startswith("admin:groupstat:"))
async def admin_groupstat_open(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id = callback.data.split(":")
    chat_id = int(chat_id)
    thread = int(thread_id)
    thread = None if thread == 0 else thread
    await safe_edit_or_send(callback, render_single_group_stats(chat_id, thread), reply_markup=single_group_stats_kb())
    await callback.answer()

@router.callback_query(F.data == "admin:settings")
async def admin_settings(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_settings(), reply_markup=settings_kb())
    await callback.answer()



@router.callback_query(F.data == "admin:operator_modes")
async def admin_operator_modes(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
    await callback.answer()

@router.callback_query(F.data.startswith("admin:toggle_avail:"))
async def admin_toggle_avail(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, mode, operator_key = callback.data.split(":")
    set_operator_mode_enabled(operator_key, mode, not is_operator_mode_enabled(operator_key, mode))
    await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
    await callback.answer("Статус обновлён")


@router.callback_query(F.data == "admin:design")
async def admin_design(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_design(), reply_markup=design_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:templates")
async def admin_templates(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_templates(), reply_markup=design_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_broadcast(), reply_markup=broadcast_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:broadcast_write")
async def admin_broadcast_write(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_broadcast_text)
    await callback.message.answer(
        "Отправьте текст рассылки одним сообщением.\n\nМожно использовать HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
    )
    await callback.answer()


@router.callback_query(F.data == "admin:broadcast_preview")
async def admin_broadcast_preview(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ad = db.get_setting("broadcast_text", "").strip()
    await callback.message.answer(ad or "Рассылка пока пустая.")
    await callback.answer()


@router.callback_query(F.data == "admin:broadcast_send_ad")
async def admin_broadcast_send_ad(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ad = db.get_setting("broadcast_text", "").strip()
    if not ad:
        await callback.answer("Сначала сохрани рассылку", show_alert=True)
        return
    sent = 0
    for uid in db.all_user_ids():
        try:
            await callback.bot.send_message(uid, ad)
            sent += 1
        except Exception:
            pass
    await callback.message.answer(f"✅ Рассылка завершена. Доставлено: <b>{sent}</b>")
    await callback.answer()


@router.callback_query(F.data == "admin:usernames")
async def admin_usernames(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    content = db.export_usernames().encode("utf-8")
    file = BufferedInputFile(content, filename="usernames.txt")
    await callback.message.answer_document(file, caption="📥 Собранные username и user_id")
    await callback.answer()


@router.callback_query(F.data == "admin:set_start_text")
async def admin_set_start_text(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_start_text)
    await callback.message.answer(
        "Отправьте новый стартовый текст в формате:\n\n<code>Заголовок\nПодзаголовок\nОписание</code>\n\nПервые 2 строки пойдут в шапку, остальное в описание."
    )
    await callback.answer()


@router.callback_query(F.data == "admin:set_ad_text")
async def admin_set_ad_text(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_ad_text)
    await callback.message.answer(
        "Отправьте текст рассылки.\n\nМожно писать красивыми шаблонами и использовать HTML Telegram."
    )
    await callback.answer()


@router.callback_query(F.data == "admin:set_hold")
async def admin_set_hold(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_hold)
    await callback.message.answer("Введите новый Холд в минутах:")
    await callback.answer()


@router.callback_query(F.data == "admin:set_min_withdraw")
async def admin_set_min_withdraw(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_min_withdraw)
    await callback.message.answer("Введите новый минимальный вывод в $:")
    await callback.answer()


@router.callback_query(F.data == "admin:treasury_add")
async def admin_treasury_add(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_treasury_invoice)
    await callback.message.answer("Введите сумму пополнения казны в $ для создания <b>Crypto Bot invoice</b>:")
    await callback.answer()


@router.callback_query(F.data == "admin:treasury_sub")
async def admin_treasury_sub(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_treasury_sub)
    await callback.message.answer("Введите сумму вывода казны в $ — будет создан <b>реальный чек Crypto Bot</b>:")
    await callback.answer()


@router.callback_query(F.data.startswith("admin:set_price:"))
async def admin_set_price_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split(":")
    if len(parts) == 4:
        _, _, price_mode, operator_key = parts
    elif len(parts) == 5:
        _, _, _, price_mode, operator_key = parts
    else:
        await callback.answer("Некорректные данные прайса", show_alert=True)
        return
    if operator_key not in OPERATORS or price_mode not in {"hold", "no_hold"}:
        await callback.answer("Некорректные данные прайса", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_operator_price)
    await state.update_data(operator_key=operator_key, price_mode=price_mode)
    await callback.message.answer(f"Введите новую цену для {op_text(operator_key)} • <b>{mode_label(price_mode)}</b> в $:")
    await callback.answer()


@router.callback_query(F.data.startswith("admin:role:"))
async def admin_role_action(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    role = callback.data.split(":")[-1]
    if role == "chief_admin" and callback.from_user.id != CHIEF_ADMIN_ID:
        await callback.answer("Назначать главного админа может только главный админ.", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_role_user)
    await state.update_data(role_target=role)
    await callback.message.answer("Отправьте ID пользователя, которому нужно назначить роль. Для снятия роли тоже отправьте ID.")
    await callback.answer()


@router.callback_query(F.data == "admin:ws_help_group")
async def admin_ws_help_group(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("Чтобы добавить рабочую группу, зайдите в нужную группу и отправьте команду <code>/work</code>.")
    await callback.answer()


@router.callback_query(F.data == "admin:ws_help_topic")
async def admin_ws_help_topic(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("Чтобы добавить рабочий топик, зайдите в нужный топик и отправьте команду <code>/topic</code>.")
    await callback.answer()


@router.message(AdminStates.waiting_hold)
async def admin_hold_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = int(float((message.text or '').replace(',', '.')))
    except Exception:
        await message.answer("Введите число.")
        return
    db.set_setting("hold_minutes", str(value))
    await state.clear()
    await message.answer("✅ Холд обновлён.", reply_markup=admin_root_kb())


@router.message(AdminStates.waiting_min_withdraw)
async def admin_min_withdraw_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    db.set_setting("min_withdraw", str(value))
    await state.clear()
    await message.answer("✅ Минимальный вывод обновлён.")


@router.message(AdminStates.waiting_treasury_invoice)
async def admin_treasury_add_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    invoice_id, pay_url, status_msg = await create_crypto_invoice(value, "Treasury top up")
    if not invoice_id or not pay_url:
        await message.answer(f"❌ {status_msg}")
        return
    local_id = db.create_treasury_invoice(value, invoice_id, pay_url, message.from_user.id)
    await state.clear()
    await message.answer(
        "<b>✅ Инвойс на пополнение казны создан</b>\n\n"
        f"🧾 Локальный ID: <b>#{local_id}</b>\n"
        f"💸 Сумма: <b>{usd(value)}</b>\n"
        f"🔗 Ссылка на оплату:\n{pay_url}\n\n"
        "После оплаты зайдите в казну и нажмите <b>Проверить оплату</b>."
    )


@router.message(AdminStates.waiting_treasury_sub)
async def admin_treasury_sub_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    if value > db.get_treasury():
        await message.answer("⚠️ В казне недостаточно средств.")
        return
    check_id, check_url, status_msg = await create_crypto_check(value)
    if not check_id or not check_url:
        await message.answer(f"❌ {status_msg}")
        return
    db.subtract_treasury(value)
    await state.clear()
    await message.answer(
        "<b>✅ Вывод казны создан</b>\n\n"
        f"💸 Сумма: <b>{usd(value)}</b>\n"
        f"🎟 Чек: {check_url}\n"
        f"💰 Остаток казны: <b>{usd(db.get_treasury())}</b>"
    )


@router.message(AdminStates.waiting_operator_price)
async def admin_operator_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    data = await state.get_data()
    operator_key = data.get("operator_key")
    price_mode = data.get("price_mode", "hold")
    if operator_key not in OPERATORS or price_mode not in {"hold", "no_hold"}:
        await state.clear()
        await message.answer("Ошибка данных прайса. Откройте раздел прайсов заново.")
        return
    db.set_setting(f"price_{price_mode}_{operator_key}", str(value))
    await state.clear()
    await message.answer(
        f"✅ Прайс обновлён: {op_text(operator_key)} • <b>{mode_label(price_mode)}</b> = <b>{usd(value)}</b>",
        reply_markup=admin_root_kb(),
    )


@router.message(AdminStates.waiting_role_user)
async def admin_role_user_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        target_id = int((message.text or '').strip())
    except Exception:
        await message.answer("Нужен числовой ID.")
        return
    data = await state.get_data()
    role_target = data.get("role_target")
    if role_target == "remove":
        if target_id == CHIEF_ADMIN_ID:
            await message.answer("Главного админа снять нельзя.")
            await state.clear()
            return
        db.remove_role(target_id)
        await message.answer("✅ Роль снята.")
    else:
        if role_target == "chief_admin" and message.from_user.id != CHIEF_ADMIN_ID:
            await message.answer("Назначать главного админа может только главный админ.")
            await state.clear()
            return
        db.set_role(target_id, role_target)
        await message.answer(f"✅ Роль назначена: {role_target}")
    await state.clear()


@router.message(AdminStates.waiting_start_text)
async def admin_start_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = [x.strip() for x in (message.text or "").splitlines() if x.strip()]
    if len(parts) < 2:
        await message.answer("Нужно минимум 2 строки: заголовок и подзаголовок.")
        return
    db.set_setting("start_title", parts[0])
    db.set_setting("start_subtitle", parts[1])
    db.set_setting("start_description", "\n".join(parts[2:]) if len(parts) > 2 else "")
    await state.clear()
    await message.answer("✅ Стартовое оформление обновлено.")


@router.message(AdminStates.waiting_ad_text)
async def admin_ad_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.set_setting("broadcast_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Объявление сохранено.")


@router.message(AdminStates.waiting_broadcast_text)
async def admin_broadcast_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.set_setting("broadcast_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Текст сохранён как активная рассылка. Теперь его можно превьюнуть и разослать из /admin.")


@router.message(Command("work"))
async def enable_work_group(message: Message):
    if not is_admin(message.from_user.id) and user_role(message.from_user.id) != "chief_admin":
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Эта команда работает только в группе.")
        return
    if db.is_workspace_enabled(message.chat.id, None, "group"):
        db.disable_workspace(message.chat.id, None, "group")
        await message.answer("🛑 Работа в этой группе выключена.")
    else:
        db.enable_workspace(message.chat.id, None, "group", message.from_user.id)
        await message.answer("✅ Эта группа добавлена как рабочая. Операторы и админы теперь могут брать здесь номера.")


@router.message(Command("topic"))
async def enable_work_topic(message: Message):
    if not is_admin(message.from_user.id) and user_role(message.from_user.id) != "chief_admin":
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Эта команда работает только в топике группы.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.answer("Открой нужный топик и выполни /topic внутри него.")
        return
    if db.is_workspace_enabled(message.chat.id, thread_id, "topic"):
        db.disable_workspace(message.chat.id, thread_id, "topic")
        await message.answer("🛑 Работа в этом топике выключена.")
    else:
        db.enable_workspace(message.chat.id, thread_id, "topic", message.from_user.id)
        await message.answer("✅ Этот топик добавлен как рабочий.")


async def send_next_item_for_operator(message: Message, operator_key: str):
    if not is_operator_or_admin(message.from_user.id):
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Команда работает только в рабочей группе или топике.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    allowed = db.is_workspace_enabled(message.chat.id, thread_id, "topic") if thread_id else False
    if not allowed:
        allowed = db.is_workspace_enabled(message.chat.id, None, "group")
    if not allowed:
        await message.answer("Эта группа/топик не включены как рабочая зона. Используй /work или /topic от админа.")
        return
    item = db.get_next_queue_item(operator_key)
    if not item:
        await message.answer(f"📭 Для оператора {op_text(operator_key)} очередь пуста.")
        return
    group_price = group_price_for_take(message.chat.id, thread_id, item.operator_key, item.mode)
    if db.get_group_balance(message.chat.id, thread_id) + 1e-9 < group_price:
        await message.answer(f"Недостаточно средств в казне группы. Нужно {usd(group_price)}")
        return
    if not db.reserve_queue_item_for_group(item.id, message.from_user.id, message.chat.id, thread_id, group_price):
        await message.answer("Заявку уже забрали.")
        return
    item = db.get_queue_item(item.id)
    try:
        await send_queue_item_photo_to_chat(message.bot, message.chat.id, item, queue_caption(item), reply_markup=admin_queue_kb(item), message_thread_id=thread_id)
    except Exception:
        db.release_item_reservation(item.id)
        db.conn.execute("UPDATE queue_items SET status='queued', taken_by_admin=NULL, taken_at=NULL WHERE id=?", (item.id,))
        db.conn.commit()
        raise


@router.message(Command("mts", "mtc", "bil", "mega", "t2"))
async def legacy_take_commands(message: Message):
    if not is_operator_or_admin(message.from_user.id):
        return
    await message.answer("Команды /mts /bil /mega /t2 отключены. Используй <b>/esim</b>.")



def extract_custom_emoji_ids(message: Message) -> list[str]:
    ids = []
    entities = list(message.entities or []) + list(message.caption_entities or [])
    for ent in entities:
        if getattr(ent, "type", None) == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
            ids.append(ent.custom_emoji_id)
    return ids

def build_sticker_info_lines(sticker=None, custom_ids=None):
    lines = []
    if sticker:
        lines.append(f"<b>file_id:</b> <code>{sticker.file_id}</code>")
        lines.append(f"<b>file_unique_id:</b> <code>{sticker.file_unique_id}</code>")
        if getattr(sticker, 'set_name', None):
            lines.append(f"<b>set_name:</b> <code>{sticker.set_name}</code>")
        if getattr(sticker, 'emoji', None):
            lines.append(f"<b>emoji:</b> {escape(sticker.emoji)}")
        if getattr(sticker, 'custom_emoji_id', None):
            lines.append(f"<b>custom_emoji_id:</b> <code>{sticker.custom_emoji_id}</code>")
        if getattr(sticker, 'is_animated', None) is not None:
            lines.append(f"<b>animated:</b> <code>{sticker.is_animated}</code>")
        if getattr(sticker, 'is_video', None) is not None:
            lines.append(f"<b>video:</b> <code>{sticker.is_video}</code>")
    for cid in custom_ids or []:
        lines.append(f"<b>custom_emoji_id:</b> <code>{cid}</code>")
    return lines

@router.message(Command("stickerid"))
@router.message(Command("emojiid"))
async def stickerid_command(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    sticker = None
    custom_ids = []
    target = message.reply_to_message or message
    if getattr(target, 'sticker', None):
        sticker = target.sticker
    custom_ids.extend(extract_custom_emoji_ids(target))
    if sticker or custom_ids:
        lines = build_sticker_info_lines(sticker, custom_ids)
        await message.answer("<b>🎟 Данные стикера / emoji</b>\n\n" + "\n".join(lines))
        return
    await state.set_state(EmojiLookupStates.waiting_target)
    await message.answer("<b>🎟 Emoji ID режим</b>\n\nОтправь <b>премиум-стикер</b> или сообщение с <b>premium emoji</b>, и я покажу ID.")

@router.message(EmojiLookupStates.waiting_target)
async def emoji_lookup_waiting(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    sticker = message.sticker if getattr(message, 'sticker', None) else None
    custom_ids = extract_custom_emoji_ids(message)
    if not sticker and not custom_ids:
        await message.answer("Пришли <b>стикер</b> или сообщение с <b>premium emoji</b>.")
        return
    lines = build_sticker_info_lines(sticker, custom_ids)
    await state.clear()
    await message.answer("<b>🎟 Данные стикера / emoji</b>\n\n" + "\n".join(lines))
@router.message(Command("esim"))

async def esim_command(message: Message):
    if not is_operator_or_admin(message.from_user.id):
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer('Команда работает только в рабочей группе или топике.')
        return
    thread_id = getattr(message, 'message_thread_id', None)
    if thread_id:
        allowed = db.is_workspace_enabled(message.chat.id, thread_id, 'topic')
    else:
        allowed = db.is_workspace_enabled(message.chat.id, None, 'group')
    if not allowed:
        await message.answer('Эта группа или топик не включены как рабочая зона. Используй /work или /topic.')
        return
    await message.answer('<b>📥 Выбор номера ESIM</b>\n\nСначала выберите режим, который нужен:', reply_markup=esim_mode_kb())


@router.callback_query(F.data == "esim:back_mode")
async def esim_back_mode(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        return
    text = "<b>📥 Выбор номера ESIM</b>\n\nСначала выберите режим, который нужен:"
    await safe_edit_or_send(callback, text, reply_markup=esim_mode_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("esim_mode:"))
async def esim_choose_mode(callback: CallbackQuery):
    logging.info("esim_choose_mode callback=%s", callback.data)
    if not is_operator_or_admin(callback.from_user.id):
        return
    mode = callback.data.split(':', 1)[1]
    text = f"<b>📥 Выбор номера ESIM</b>\n\nВыбран режим: <b>{mode_label(mode)}</b>\n👇 Теперь выберите оператора:\n<i>Цена указана прямо в кнопках.</i>"
    await safe_edit_or_send(callback, text, reply_markup=operators_kb(mode, 'esim_take', 'esim:back_mode', callback.from_user.id))
    await callback.answer()


@router.callback_query(F.data.startswith("esim_take:"))
async def esim_take(callback: CallbackQuery):
    logging.info("esim_take callback=%s", callback.data)
    if not is_operator_or_admin(callback.from_user.id):
        return
    _, operator_key, mode = callback.data.split(':')
    thread_id = getattr(callback.message, 'message_thread_id', None)
    allowed = db.is_workspace_enabled(callback.message.chat.id, thread_id if thread_id else None, 'topic' if thread_id else 'group')
    if not allowed:
        await callback.answer('Рабочая зона не активирована', show_alert=True)
        return
    item = get_next_queue_item_mode(operator_key, mode)
    if not item:
        await callback.answer('В этой очереди пока пусто', show_alert=True)
        return
    if callback.message.chat.type == ChatType.PRIVATE:
        await callback.answer('Команда доступна только в группе', show_alert=True)
        return
    group_price = group_price_for_take(callback.message.chat.id, thread_id, item.operator_key, item.mode)
    if db.get_group_balance(callback.message.chat.id, thread_id) + 1e-9 < group_price:
        await callback.answer(f"Недостаточно средств в казне группы. Нужно {usd(group_price)}", show_alert=True)
        return
    if not db.reserve_queue_item_for_group(item.id, callback.from_user.id, callback.message.chat.id, thread_id, group_price):
        await callback.answer("Заявку уже забрали", show_alert=True)
        return
    fresh = db.get_queue_item(item.id)
    try:
        await send_queue_item_photo_to_chat(callback.bot, callback.message.chat.id, fresh, queue_caption(fresh), reply_markup=admin_queue_kb(fresh), message_thread_id=thread_id)
    except Exception:
        db.release_item_reservation(item.id)
        db.conn.execute("UPDATE queue_items SET status='queued', taken_by_admin=NULL, taken_at=NULL WHERE id=?", (item.id,))
        db.conn.commit()
        raise
    try:
        await send_item_user_message(
            callback.bot,
            fresh,
            f"<b>📥 Номер взят в обработку</b>\n\n🧾 <b>Заявка:</b> #{fresh.id}\n📱 <b>Оператор:</b> {op_html(fresh.operator_key)}\n📞 <b>Номер:</b> <code>{escape(pretty_phone(fresh.normalized_phone))}</code>\n🔄 <b>Режим:</b> {mode_label(fresh.mode)}"
        )
    except Exception:
        pass
    await callback.answer('Заявка выдана')


@router.callback_query(F.data.startswith("wd_delcheck:"))
async def wd_delcheck(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    wd_id = int(callback.data.split(':')[-1])
    wd = db.get_withdrawal(wd_id)
    if not wd or not wd['payout_check_id']:
        await callback.answer('Чек не найден', show_alert=True)
        return
    ok, note = await delete_crypto_check(int(wd['payout_check_id']))
    await callback.answer(note, show_alert=not ok)



async def mirror_polling_loop(bot: Bot):
    offset = 0
    while True:
        try:
            updates = await bot.get_updates(offset=offset, timeout=25, allowed_updates=["message", "callback_query"])
            for upd in updates:
                offset = upd.update_id + 1
                try:
                    await LIVE_DP.feed_update(bot, upd)
                except Exception:
                    logging.exception("mirror feed_update failed")
        except Exception:
            logging.exception("mirror polling loop failed")
            await asyncio.sleep(3)

async def start_live_mirror(token: str):
    global LIVE_DP
    token = (token or "").strip()
    if not token or token == BOT_TOKEN or token in LIVE_MIRROR_TASKS:
        return False, "already_started"
    if LIVE_DP is None:
        return False, "dispatcher_not_ready"
    try:
        mirror_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        me = await mirror_bot.get_me()
        task = asyncio.create_task(mirror_polling_loop(mirror_bot))
        LIVE_MIRROR_TASKS[token] = {"task": task, "username": me.username or "", "bot": mirror_bot}
        logging.info("Live mirror started as @%s", me.username or "unknown")
        return True, me.username or ""
    except Exception as e:
        logging.exception("Live mirror start failed: %s", e)
        return False, str(e)

async def hold_watcher(bot: Bot):
    while True:
        try:
            # update active hold captions every ~30 sec
            active_items = db.get_active_holds_for_render()
            for item in active_items:
                try:
                    if item.status != "in_progress":
                        continue
                    last = parse_dt(item.timer_last_render) if item.timer_last_render else None
                    now_dt = msk_now()
                    if last is None or (now_dt - last).total_seconds() >= 30:
                        await bot.edit_message_caption(
                            chat_id=item.work_chat_id,
                            message_id=item.work_message_id,
                            caption=queue_caption(item),
                            reply_markup=admin_queue_kb(item),
                        )
                        db.touch_timer_render(item.id)
                except Exception:
                    pass

            # complete expired holds
            expired_items = db.get_expired_holds()
            for item in expired_items:
                try:
                    db.complete_queue_item(item.id)
                    db.add_balance(item.user_id, float(item.price))
                    fresh_user = db.get_user(item.user_id)
                    balance = float(fresh_user["balance"] if fresh_user else 0.0)
                    try:
                        await send_item_user_message(
                            bot,
                            item,
                            "<b>✅ Оплата за номер</b>\n\n"
                            f"📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
                            f"💰 <b>Начислено:</b> {usd(item.price)}\n"
                            f"💲 <b>Ваш баланс:</b> {usd(balance)}"
                        )
                    except Exception:
                        pass
                    try:
                        await bot.edit_message_caption(
                            chat_id=item.work_chat_id,
                            message_id=item.work_message_id,
                            caption=queue_caption(db.get_queue_item(item.id) or item) + "\n\n✅ <b>Холд завершён. Номер оплачен.</b>",
                            reply_markup=None,
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            logging.exception("hold_watcher failed")
        await asyncio.sleep(5)


def render_admin_queue_text() -> str:
    items = latest_queue_items(10)
    if not items:
        return "<b>📦 Очередь</b>\n\n<i>Активных заявок в очереди нет.</i>"
    rows = []
    for item in items:
        pos = queue_position(item['id']) if item['status'] == 'queued' else None
        pos_text = f" • позиция {pos}" if pos else ""
        rows.append(f"#{item['id']} • {op_text(item['operator_key'])} • {mode_label(item['mode'])} • {pretty_phone(item['normalized_phone'])}{pos_text}")
    return "<b>📦 Очередь</b>\n\n" + quote_block(rows)

@router.callback_query(F.data == "admin:queues")
async def admin_queues(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_queue_text(), reply_markup=queue_manage_kb())
    await callback.answer()

@router.callback_query(F.data == "admin:user_tools")
async def admin_user_tools(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await safe_edit_or_send(
        callback,
        "<b>👤 Пользователь</b>\n\nВыберите действие ниже, затем отправьте ID, @username или номер следующим сообщением.",
        reply_markup=user_admin_kb(),
    )
    await callback.answer()

@router.callback_query(F.data.in_(["admin:user_stats", "admin:user_set_price", "admin:user_pm", "admin:user_add_balance", "admin:user_sub_balance", "admin:user_ban", "admin:user_unban"]))
async def admin_user_action_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    action_map = {
        "admin:user_stats": "stats",
        "admin:user_set_price": "set_price",
        "admin:user_pm": "pm",
        "admin:user_add_balance": "add_balance",
        "admin:user_sub_balance": "sub_balance",
        "admin:user_ban": "ban",
        "admin:user_unban": "unban",
    }
    action = action_map.get(callback.data, "")
    await state.clear()
    await state.update_data(user_action=action)
    await state.set_state(AdminStates.waiting_user_action_id)
    prompts = {
        "stats": "<b>Отправьте ID, @username или номер пользователя для просмотра статистики:</b>",
        "set_price": "<b>Отправьте ID, @username или номер пользователя для персонального прайса:</b>",
        "pm": "<b>Отправьте ID, @username или номер пользователя для сообщения в ЛС:</b>",
        "add_balance": "<b>Отправьте ID, @username или номер пользователя для начисления:</b>",
        "sub_balance": "<b>Отправьте ID, @username или номер пользователя для списания:</b>",
        "ban": "<b>Отправьте ID, @username или номер пользователя для блокировки:</b>",
        "unban": "<b>Отправьте ID, @username или номер пользователя для разблокировки:</b>",
    }
    await callback.message.answer(prompts.get(action, "<b>Отправьте ID, @username или номер пользователя:</b>"))
    await callback.answer()

@router.message(AdminStates.waiting_user_action_id)
async def admin_user_action_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    action = data.get("user_action")
    raw = (message.text or "").strip()
    logging.info("user-section lookup action=%s raw=%s", action, raw)
    user = resolve_user_input(raw)

    if not user:
        await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или номер ещё раз.")
        return

    target_user_id = int(user["user_id"])
    await state.update_data(target_user_id=target_user_id)
    logging.info("user-section found target_user_id=%s action=%s", target_user_id, action)

    if action == "stats":
        full_user, stats, ops = get_user_full_stats(target_user_id)
        ops_text = "\n".join(
            f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}"
            for row in ops
        ) or "• Пока пусто"
        custom_prices = db.list_user_prices(target_user_id) if hasattr(db, "list_user_prices") else []
        custom_text = "\n".join(
            f"• {op_text(row['operator_key'])} • {mode_label(row['mode'])} = <b>{usd(row['price'])}</b>"
            for row in custom_prices
        ) or "• Нет"
        await state.clear()
        await message.answer(
            f"<b>👤 Пользователь</b>\n\n"
            f"🆔 <code>{target_user_id}</code>\n"
            f"👤 <b>{escape(full_user['full_name'] or '')}</b>\n"
            f"🔗 @{escape(full_user['username']) if full_user['username'] else '—'}\n"
            f"💰 Баланс: <b>{usd(full_user['balance'])}</b>\n\n"
            f"📊 Всего: <b>{stats['total'] or 0}</b> | ✅ <b>{stats['completed'] or 0}</b> | ❌ <b>{stats['slipped'] or 0}</b> | ⚠️ <b>{stats['errors'] or 0}</b>\n"
            f"💵 Заработано: <b>{usd(stats['earned'] or 0)}</b>\n\n"
            f"<b>📱 По операторам</b>\n{ops_text}\n\n"
            f"<b>💎 Персональные прайсы</b>\n{custom_text}",
            reply_markup=admin_back_kb("admin:user_tools"),
        )
        return

    if action == "set_price":
        await state.set_state(AdminStates.waiting_user_price_lookup)
        await message.answer(
            "<b>✅ Пользователь найден</b>\n\n"
            f"👤 <b>{escape(user['full_name'] or '')}</b>\n"
            f"🆔 <code>{target_user_id}</code>\n"
            f"🔗 @{escape(user['username']) if user['username'] else '—'}\n\n"
            "<b>Выберите оператора:</b>",
            reply_markup=user_price_operator_kb(target_user_id),
        )
        return

    if action in {"add_balance", "sub_balance"}:
        await state.set_state(AdminStates.waiting_user_action_value)
        await message.answer("Введите сумму в $:")
        return

    if action == "pm":
        await state.set_state(AdminStates.waiting_user_action_text)
        await message.answer("Введите текст сообщения для пользователя:")
        return

    if action == "ban":
        set_user_blocked(target_user_id, True)
        await state.clear()
        await message.answer(f"✅ Пользователь <code>{target_user_id}</code> заблокирован.", reply_markup=admin_back_kb("admin:user_tools"))
        return

    if action == "unban":
        set_user_blocked(target_user_id, False)
        await state.clear()
        await message.answer(f"✅ Пользователь <code>{target_user_id}</code> разблокирован.", reply_markup=admin_back_kb("admin:user_tools"))
        return

@router.message(AdminStates.waiting_user_price_lookup)
async def admin_user_price_lookup(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    raw = (message.text or "").strip()
    if raw:
        user = resolve_user_input(raw)
        if user:
            target_user_id = int(user["user_id"])
            await state.update_data(target_user_id=target_user_id)
        else:
            await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или номер ещё раз.")
            return

    data = await state.get_data()
    target_user_id = int(data["target_user_id"])
    await message.answer("<b>Выберите оператора:</b>", reply_markup=user_price_operator_kb(target_user_id))

@router.callback_query(F.data.startswith("admin:user_price_op:"))
async def admin_user_price_op(callback: CallbackQuery):
    logging.info("admin_user_price_op callback=%s", callback.data)
    if not is_admin(callback.from_user.id):
        return
    await callback.answer()
    _, _, uid, operator_key = callback.data.split(":")
    await callback.message.answer(
        f"<b>Пользователь:</b> <code>{uid}</code>\n<b>Оператор:</b> {op_text(operator_key)}\n\n<b>Выберите режим:</b>",
        reply_markup=user_price_mode_kb(int(uid), operator_key),
    )

@router.callback_query(F.data.startswith("admin:user_price_mode:"))
async def admin_user_price_mode(callback: CallbackQuery, state: FSMContext):
    logging.info("admin_user_price_mode callback=%s", callback.data)
    if not is_admin(callback.from_user.id):
        return
    await callback.answer()
    _, _, uid, operator_key, mode = callback.data.split(":")
    await state.set_state(AdminStates.waiting_user_price_value)
    await state.update_data(target_user_id=int(uid), operator_key=operator_key, price_mode=mode)
    await callback.message.answer(
        f"<b>Пользователь:</b> <code>{uid}</code>\n"
        f"<b>Оператор:</b> {op_text(operator_key)}\n"
        f"<b>Режим:</b> {mode_label(mode)}\n\n"
        "Введите сумму числом или <code>reset</code> для удаления:",
        reply_markup=admin_back_kb("admin:user_tools"),
    )

@router.message(AdminStates.waiting_user_price_value)
async def admin_user_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    operator_key = data["operator_key"]
    mode = data["price_mode"]
    value_raw = (message.text or "").strip().lower()

    if value_raw in {"reset", "delete", "del", "none"}:
        if hasattr(db, "delete_user_price"):
            db.delete_user_price(uid, operator_key, mode)
        await state.clear()
        await message.answer(
            f"✅ Персональный прайс удалён\n\n"
            f"👤 Пользователь: <code>{uid}</code>\n"
            f"📱 Оператор: {op_text(operator_key)}\n"
            f"🔄 Режим: <b>{mode_label(mode)}</b>",
            reply_markup=admin_back_kb("admin:user_tools"),
        )
        return

    try:
        value = float(value_raw.replace(",", "."))
    except Exception:
        await message.answer("⚠️ Введите сумму числом или <code>reset</code>.")
        return

    db.set_user_price(uid, operator_key, mode, value)
    await state.clear()
    await message.answer(
        f"✅ Персональный прайс сохранён\n\n"
        f"👤 Пользователь: <code>{uid}</code>\n"
        f"📱 Оператор: {op_text(operator_key)}\n"
        f"🔄 Режим: <b>{mode_label(mode)}</b>\n"
        f"💰 Цена: <b>{usd(value)}</b>",
        reply_markup=admin_back_kb("admin:user_tools"),
    )

@router.message(AdminStates.waiting_user_action_value)
async def admin_user_action_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    action = data.get("user_action")
    try:
        value = float((message.text or "").replace(",", "."))
    except Exception:
        await message.answer("Введите сумму числом.")
        return

    if action == "add_balance":
        db.add_balance(uid, value)
        await state.clear()
        await message.answer(f"✅ Пользователю <code>{uid}</code> начислено <b>{usd(value)}</b>.", reply_markup=admin_back_kb("admin:user_tools"))
        return

    if action == "sub_balance":
        db.subtract_balance(uid, value)
        await state.clear()
        await message.answer(f"✅ У пользователя <code>{uid}</code> списано <b>{usd(value)}</b>.", reply_markup=admin_back_kb("admin:user_tools"))
        return

@router.message(AdminStates.waiting_user_action_text)
async def admin_user_action_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    try:
        await message.bot.send_message(uid, f"<b>📩 Сообщение от администрации</b>\n\n{escape(message.text)}")
        await message.answer("✅ Сообщение отправлено.", reply_markup=admin_back_kb("admin:user_tools"))
    except Exception:
        await message.answer("⚠️ Не удалось отправить сообщение.", reply_markup=admin_back_kb("admin:user_tools"))
    await state.clear()

@router.callback_query(F.data == "admin:toggle_numbers")
async def admin_toggle_numbers(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    set_numbers_enabled(not is_numbers_enabled())
    await safe_edit_or_send(callback, render_admin_settings(), reply_markup=settings_kb())
    await callback.answer("Статус обновлён")

@router.callback_query(F.data.startswith("admin:queue_remove:"))
async def admin_queue_remove(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    item_id = int(callback.data.split(":")[-1])
    remove_queue_item(item_id, reason='admin_removed', admin_id=callback.from_user.id)
    await safe_edit_or_send(callback, render_admin_queue_text(), reply_markup=queue_manage_kb())
    await callback.answer("Удалено из очереди")

@router.callback_query(F.data.startswith("myremove:"))
async def myremove_cb(callback: CallbackQuery, state: FSMContext):
    item_id = int(callback.data.split(":")[-1])
    row = db.conn.execute("SELECT * FROM queue_items WHERE id=? AND user_id=?", (item_id, callback.from_user.id)).fetchone()
    if not row:
        await callback.answer("Заявка не найдена", show_alert=True)
        return
    if row["status"] != "queued":
        await callback.answer("Убрать можно только номер из очереди", show_alert=True)
        return
    remove_queue_item(item_id, reason='user_removed')
    items = user_today_queue_items(callback.from_user.id)
    await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id), my_numbers_kb(items))
    await send_log(callback.bot, f"<b>🗑 Удаление из очереди</b>\n👤 {escape(callback.from_user.full_name)}\n🆔 <code>{callback.from_user.id}</code>\n🧾 Заявка: <b>#{item_id}</b>")
    await callback.answer("Номер убран")

@router.callback_query(F.data.startswith("take_start:"))
async def take_start_cb(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item or item.status not in {"queued", "taken"}:
        await callback.answer("Заявка уже неактуальна", show_alert=True)
        return
    thread_id = getattr(callback.message, 'message_thread_id', None)
    db.start_work(item.id, callback.from_user.id, item.mode, callback.message.chat.id, thread_id, callback.message.message_id)
    fresh = db.get_queue_item(item.id)
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=queue_caption(fresh), reply_markup=admin_queue_kb(fresh))
        else:
            await callback.message.edit_text(queue_caption(fresh), reply_markup=admin_queue_kb(fresh))
    except Exception:
        pass
    try:
        await send_item_user_message(
            callback.bot,
            fresh,
            "<b>✅ Номер — Встал ✅</b>\n\n"
            "🚀 <b>По вашему номеру началась работа</b>\n\n"
            f"📞 <b>Номер:</b> <code>{escape(pretty_phone(fresh.normalized_phone))}</code>\n"
            f"📱 <b>Оператор:</b> {op_html(fresh.operator_key)}\n"
            f"{mode_emoji(fresh.mode)} <b>Режим:</b> {mode_label(fresh.mode)}"
        )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>🚀 Работа началась</b>\n👤 Взял: {escape(callback.from_user.full_name)}\n🆔 <code>{callback.from_user.id}</code>\n🧾 Заявка: <b>#{fresh.id}</b>\n📱 {op_html(fresh.operator_key)}\n📞 <code>{escape(pretty_phone(fresh.normalized_phone))}</code>\n🔄 {mode_label(fresh.mode)}")
    await callback.answer("Работа началась")

@router.callback_query(F.data.startswith("error_pre:"))
async def error_pre_cb(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item:
        await callback.answer("Заявка не найдена", show_alert=True)
        return
    db.mark_error_before_start(item_id)
    db.release_item_reservation(item_id)
    fresh = db.get_queue_item(item_id) or item
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=queue_caption(fresh) + "\n\n⚠️ <b>Ошибка — номер не встал.</b>", reply_markup=None)
        else:
            await callback.message.edit_text(queue_caption(fresh) + "\n\n⚠️ <b>Ошибка — номер не встал.</b>", reply_markup=None)
    except Exception:
        pass
    try:
        await send_item_user_message(
            callback.bot,
            item,
            "<b>⚠️ Ошибка — номер не встал</b>\n\n"
            f"📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
            "❌ <b>Номер не принят в работу.</b>"
        )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>⚠️ Ошибка заявки</b>\n👤 {escape(callback.from_user.full_name)}\n🧾 Заявка: <b>#{item_id}</b>\n📱 {op_html(item.operator_key)}")
    await callback.answer("Помечено как ошибка")

@router.callback_query(F.data.startswith("instant_pay:"))
async def instant_pay_cb(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item or item.status != "in_progress" or item.mode != "no_hold":
        await callback.answer("Оплата недоступна", show_alert=True)
        return
    db.complete_queue_item(item_id)
    db.add_balance(item.user_id, float(item.price))
    user = db.get_user(item.user_id)
    balance = float(user["balance"] if user else 0)
    fresh = db.get_queue_item(item_id) or item
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=queue_caption(fresh) + "\n\n✅ <b>Оплачено.</b>", reply_markup=None)
        else:
            await callback.message.edit_text(queue_caption(fresh) + "\n\n✅ <b>Оплачено.</b>", reply_markup=None)
    except Exception:
        pass
    try:
        await send_item_user_message(
            callback.bot,
            item,
            "<b>✅ Оплата за номер</b>\n\n"
            f"📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
            f"💰 <b>Начислено:</b> {usd(item.price)}\n"
            f"💲 <b>Ваш баланс:</b> {usd(balance)}"
        )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>💸 Оплата номера</b>\n👤 {escape(callback.from_user.full_name)}\n🧾 Заявка: <b>#{item_id}</b>\n📱 {op_html(item.operator_key)}\n💰 {usd(item.price)}")
    await callback.answer("Оплачено")

@router.callback_query(F.data.startswith("slip:"))
async def slip_cb(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item or item.status != "in_progress":
        await callback.answer("Слет недоступен", show_alert=True)
        return
    started = parse_dt(item.work_started_at)
    worked = "00:00"
    if started:
        secs = max(int((msk_now() - started).total_seconds()), 0)
        worked = f"{secs//60:02d}:{secs%60:02d}"
    db.conn.execute("UPDATE queue_items SET status='failed', fail_reason='slip', completed_at=? WHERE id=?", (now_str(), item_id))
    db.conn.commit()
    db.release_item_reservation(item_id)
    fresh = db.get_queue_item(item_id) or item
    remain = time_left_text(item.hold_until) if item.mode == "hold" else "—"
    slip_text = queue_caption(fresh) + f"\n\n❌ <b>Номер слетел</b>\n⏱ <b>Время работы:</b> {worked}\n▫️ <b>Холд осталось:</b> {remain}\n\n❌ <b>Оплата за номер не начислена.</b>"
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=slip_text, reply_markup=None)
        else:
            await callback.message.edit_text(slip_text, reply_markup=None)
    except Exception:
        pass
    try:
        await send_item_user_message(
            callback.bot,
            item,
            f"<b>❌ Номер слетел</b>\n\n📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n⏱ <b>Время работы:</b> {worked}\n▫️ <b>Холд осталось:</b> {remain}\n\n❌ <b>Оплата за номер не начислена.</b>"
        )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>❌ Слет</b>\n👤 {escape(callback.from_user.full_name)}\n🧾 Заявка: <b>#{item_id}</b>\n📱 {op_html(item.operator_key)}")
    await callback.answer("Слет отмечен")

@router.callback_query(F.data.in_(["admin:user_stats", "admin:user_set_price", "admin:user_pm", "admin:user_add_balance", "admin:user_sub_balance", "admin:user_ban", "admin:user_unban"]))
async def admin_user_action_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    raw_action = callback.data.split(":")[-1]
    action_map = {
        "user_stats": "stats",
        "user_set_price": "set_price",
        "user_pm": "pm",
        "user_add_balance": "add_balance",
        "user_sub_balance": "sub_balance",
        "user_ban": "ban",
        "user_unban": "unban",
    }
    action = action_map.get(raw_action, raw_action)
    await state.clear()
    if action == "stats":
        await state.set_state(AdminStates.waiting_user_stats_lookup)
        await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя:</b>", reply_markup=ForceReply(selective=True))
        await callback.answer()
        return
    if action == "set_price":
        await state.set_state(AdminStates.waiting_user_price_lookup)
        await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя для персонального прайса:</b>", reply_markup=ForceReply(selective=True))
        await callback.answer()
        return
    await state.update_data(user_action=action)
    await state.set_state(AdminStates.waiting_user_action_id)
    await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя:</b>", reply_markup=ForceReply(selective=True))
    await callback.answer()

@router.message(AdminStates.waiting_user_stats_lookup)
async def admin_user_stats_lookup(message: Message, state: FSMContext):
    logging.info("admin_user_stats_lookup: %s", message.text)
    logging.info("user-section handler: stats | text=%s | user=%s", getattr(message if 'stats' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'stats' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    user = resolve_user_input(message.text)
    if not user:
        await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или сданный номер ещё раз.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return
    target_user_id = int(user["user_id"])
    user, stats, ops = get_user_full_stats(target_user_id)
    if not user:
        await message.answer("⚠️ Пользователь не найден. Попробуйте ещё раз.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return
    ops_text = "\n".join([f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}" for row in ops]) or "• Пока пусто"
    custom_prices = db.list_user_prices(target_user_id)
    custom_text = "\n".join(
        f"• {op_text(row['operator_key'])} • {mode_label(row['mode'])} = <b>{usd(row['price'])}</b>"
        for row in custom_prices
    ) or "• Нет"
    text_msg = (
        f"<b>👤 Пользователь</b>\n\n"
        f"🆔 <code>{target_user_id}</code>\n"
        f"👤 <b>{escape(user['full_name'] or '')}</b>\n"
        f"🔗 @{escape(user['username']) if user['username'] else '—'}\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n\n"
        f"📊 Всего: <b>{stats['total'] or 0}</b> | ✅ <b>{stats['completed'] or 0}</b> | ❌ <b>{stats['slipped'] or 0}</b> | ⚠️ <b>{stats['errors'] or 0}</b>\n"
        f"💵 Заработано: <b>{usd(stats['earned'] or 0)}</b>\n\n"
        f"<b>📱 По операторам</b>\n{ops_text}\n\n"
        f"<b>💎 Персональные прайсы</b>\n{custom_text}"
    )
    await state.clear()
    await message.answer(text_msg, reply_markup=admin_back_kb("admin:user_tools"))

@router.message(AdminStates.waiting_user_price_lookup)
async def admin_user_price_lookup(message: Message, state: FSMContext):
    logging.info("admin_user_price_lookup: %s", message.text)
    logging.info("user-section handler: lookup | text=%s | user=%s", getattr(message if 'lookup' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'lookup' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    user = resolve_user_input(raw)
    if not user:
        await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или сданный номер ещё раз.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return
    uid = int(user["user_id"])
    await state.clear()
    await message.answer(
        "<b>✅ Пользователь найден</b>\n\n"
        f"👤 <b>{escape(user['full_name'] or '')}</b>\n"
        f"🆔 <code>{uid}</code>\n"
        f"🔗 @{escape(user['username']) if user['username'] else '—'}\n\n"
        "<b>Выберите оператора:</b>",
        reply_markup=user_price_operator_kb(uid),
    )

@router.callback_query(F.data.startswith("admin:user_price_back_ops:"))
async def admin_user_price_back_ops(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    uid = int(callback.data.split(":")[-1])
    await safe_edit_or_send(callback, "<b>Выберите оператора:</b>", reply_markup=user_price_operator_kb(uid))
    await callback.answer()

@router.callback_query(F.data.startswith("admin:user_price_op:"))
async def admin_user_price_op(callback: CallbackQuery):
    logging.info("admin_user_price_op: %s", callback.data)
    logging.info("user-section handler: op | text=%s | user=%s", getattr(message if 'op' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'op' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(callback.from_user.id):
        return
    _, _, uid, operator_key = callback.data.split(":")
    await safe_edit_or_send(
        callback,
        f"<b>Пользователь:</b> <code>{uid}</code>\n<b>Оператор:</b> {op_text(operator_key)}\n\n<b>Выберите режим:</b>",
        reply_markup=user_price_mode_kb(int(uid), operator_key),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("admin:user_price_mode:"))
async def admin_user_price_mode(callback: CallbackQuery, state: FSMContext):
    logging.info("admin_user_price_mode: %s", callback.data)
    logging.info("user-section handler: mode | text=%s | user=%s", getattr(message if 'mode' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'mode' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(callback.from_user.id):
        return
    _, _, uid, operator_key, mode = callback.data.split(":")
    await state.set_state(AdminStates.waiting_user_price_value)
    await state.update_data(target_user_id=int(uid), operator_key=operator_key, price_mode=mode)
    await callback.message.answer(
        f"<b>Пользователь:</b> <code>{uid}</code>\n"
        f"<b>Оператор:</b> {op_text(operator_key)}\n"
        f"<b>Режим:</b> {mode_label(mode)}\n\n"
        "Введите сумму числом.\nЧтобы удалить персональный прайс, отправьте: <code>reset</code>",
        reply_markup=cancel_inline_kb("admin:user_tools"),
    )
    await callback.answer()

@router.message(AdminStates.waiting_user_price_value)
async def admin_user_price_value(message: Message, state: FSMContext):
    logging.info("admin_user_price_value: %s", message.text)
    logging.info("user-section handler: value | text=%s | user=%s", getattr(message if 'value' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'value' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    operator_key = data["operator_key"]
    mode = data["price_mode"]
    value_raw = (message.text or "").strip().lower()

    if value_raw in {"reset", "delete", "del", "none"}:
        db.delete_user_price(uid, operator_key, mode)
        await state.clear()
        await message.answer(
            f"✅ Персональный прайс удалён\n\n"
            f"👤 Пользователь: <code>{uid}</code>\n"
            f"📱 Оператор: {op_text(operator_key)}\n"
            f"🔄 Режим: <b>{mode_label(mode)}</b>",
            reply_markup=admin_back_kb("admin:user_tools"),
        )
        return

    try:
        value = float(value_raw.replace(",", "."))
    except Exception:
        await message.answer("⚠️ Введите сумму числом или <code>reset</code>.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return

    db.set_user_price(uid, operator_key, mode, value)
    await state.clear()
    await message.answer(
        f"✅ Персональный прайс сохранён\n\n"
        f"👤 Пользователь: <code>{uid}</code>\n"
        f"📱 Оператор: {op_text(operator_key)}\n"
        f"🔄 Режим: <b>{mode_label(mode)}</b>\n"
        f"💰 Цена: <b>{usd(value)}</b>",
        reply_markup=admin_back_kb("admin:user_tools"),
    )

@router.message(AdminStates.waiting_user_custom_price_text)
async def admin_user_custom_price_text_legacy(message: Message, state: FSMContext):
    await state.set_state(AdminStates.waiting_user_price_value)
    await admin_user_price_value(message, state)

@router.message(AdminStates.waiting_user_action_text)
async def admin_user_action_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    try:
        await message.bot.send_message(uid, f"<b>📩 Сообщение от администрации</b>\n\n{escape(message.text)}")
        await message.answer("Сообщение отправлено.")
    except Exception:
        await message.answer("Не удалось отправить сообщение.")
    await state.clear()


@router.message(Command("dbsqulite"))
async def db_sqlite_export(message: Message):
    if not is_admin(message.from_user.id):
        return
    path = Path(DB_PATH)
    if not path.exists():
        await message.answer("Файл базы пока не найден.")
        return
    await message.answer_document(FSInputFile(path), caption="<b>📦 SQLite база</b>")

@router.message(Command("dblog"))
async def db_log_export(message: Message):
    if not is_admin(message.from_user.id):
        return
    path = Path("bot.log")
    if not path.exists():
        path.write_text("Лог пока пуст.\n", encoding="utf-8")
    await message.answer_document(FSInputFile(path), caption="<b>🧾 Логи бота</b>")

@router.message(Command("dbusernames"))
async def export_usernames_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return
    data = db.export_usernames().strip() or "Нет username."
    path = Path("usernames.txt")
    path.write_text(data + ("\n" if not data.endswith("\n") else ""), encoding="utf-8")
    await message.answer_document(FSInputFile(path), caption="<b>👥 Username пользователей</b>")

@router.message(Command("uploadsqlite"))
@router.message(Command("dbupload"))
async def db_upload_command(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.waiting_db_upload)
    await message.answer("<b>📥 Загрузка базы</b>\n\nПришлите файл <code>.db</code> или <code>.sqlite</code>.")

@router.message(AdminStates.waiting_db_upload, F.document)
async def db_upload_receive(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    doc = message.document
    name = (doc.file_name or "").lower()
    if not (name.endswith(".db") or name.endswith(".sqlite")):
        await message.answer("Пришлите именно файл базы <code>.db</code> или <code>.sqlite</code>.")
        return
    temp_path = Path(DB_PATH + ".uploaded")
    await bot.download(doc, destination=temp_path)
    try:
        import sqlite3 as _sqlite3
        conn = _sqlite3.connect(str(temp_path))
        conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchall()
        conn.close()
    except Exception:
        temp_path.unlink(missing_ok=True)
        await message.answer("❌ Файл не похож на SQLite базу.")
        return
    backup_path = Path(DB_PATH + ".backup")
    if Path(DB_PATH).exists():
        shutil.copyfile(DB_PATH, backup_path)
    shutil.move(str(temp_path), DB_PATH)
    await state.clear()
    await message.answer("<b>✅ База загружена</b>\n\nПерезапустите Railway, чтобы бот подхватил новую базу.")

@router.message(AdminStates.waiting_db_upload)
async def db_upload_wrong(message: Message):
    await message.answer("Пришлите файл базы <code>.db</code> или <code>.sqlite</code>.")


@router.message(Command("stata"))
@router.message(Command("Stata"))
async def group_stata(message: Message):
    if user_role(message.from_user.id) not in {"chief_admin", "admin", "operator"}:
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Статистику групп смотрите через кнопку в /admin.")
        return
    try:
        chat_id = message.chat.id
        thread_id = getattr(message, "message_thread_id", None)
        totals = db.group_stats(chat_id, thread_id)

        per_operator = db.conn.execute(
            """
            SELECT
                operator_key,
                COUNT(*) AS total,
                SUM(CASE WHEN mode='hold' THEN 1 ELSE 0 END) AS hold_total,
                SUM(CASE WHEN mode='no_hold' THEN 1 ELSE 0 END) AS no_hold_total,
                SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS spent_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
            FROM queue_items
            WHERE charge_chat_id=? AND charge_thread_id=?
            GROUP BY operator_key
            ORDER BY total DESC, operator_key ASC
            """,
            (int(chat_id), db._thread_key(thread_id)),
        ).fetchall()

        lines = [f"📦 Взято всего: <b>{int(totals['taken_total'] or 0)}</b>",
                 f"🚀 Начато: <b>{int(totals['started'] or 0)}</b>",
                 f"✅ Успешно: <b>{int(totals['success'] or 0)}</b>",
                 f"❌ Слеты: <b>{int(totals['slips'] or 0)}</b>",
                 f"⚠️ Ошибки: <b>{int(totals['errors'] or 0)}</b>",
                 f"💰 Выплачено юзерам: <b>{usd(totals['paid_total'] or 0)}</b>",
                 f"🏦 Списано с казны: <b>{usd(totals['spent_total'] or 0)}</b>",
                 f"📈 Маржа группы: <b>{usd(totals['margin_total'] or 0)}</b>"]
        if per_operator:
            lines.append("")
            lines.append("<b>📱 По операторам</b>")
            for row in per_operator:
                lines.append(
                    f"• {op_text(row['operator_key'])}: <b>{int(row['total'] or 0)}</b> "
                    f"(⏳ {int(row['hold_total'] or 0)} / ⚡ {int(row['no_hold_total'] or 0)}) • "
                    f"💰 <b>{usd(row['paid_total'] or 0)}</b>"
                )
        await message.answer("<b>📊 Статистика этой группы / топика</b>\n\n" + "\n".join(lines))
    except Exception:
        await message.answer("⚠️ Не удалось собрать статистику группы. Смотрите её через кнопку в /admin.")


@router.callback_query(F.data == "admin:set_log_channel")
async def admin_set_log_channel(callback: CallbackQuery, state: FSMContext):
    if not is_chief_admin(callback.from_user.id):
        await callback.answer("Только главный админ", show_alert=True)
        return
    await state.update_data(channel_target="log_channel_id")
    await state.set_state(AdminStates.waiting_channel_value)
    await callback.message.answer("Введите новый <b>ID канала логов</b>:")
    await callback.answer()

@router.message(AdminStates.waiting_channel_value)
async def admin_channel_value(message: Message, state: FSMContext):
    if user_role(message.from_user.id) != "chief_admin":
        await state.clear()
        return
    raw = message.text.strip()
    if not raw.lstrip("-").isdigit():
        await message.answer("Введите ID канала числом.")
        return
    data = await state.get_data()
    key = data.get("channel_target")
    db.set_setting(key, raw)
    await state.clear()
    await message.answer("✅ Сохранено.")




@router.message(Command("kazna"))
async def kazna_command(message: Message):
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Команда работает только в рабочей группе или топике.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    await message.answer(render_group_finance(message.chat.id, thread_id))

@router.callback_query(F.data == "admin:group_finance_panel")
async def admin_group_finance_panel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, "<b>🏦 Выберите группу / топик для казны:</b>", reply_markup=group_finance_list_kb())
    await callback.answer()

@router.callback_query(F.data.startswith("admin:groupfin:"))
async def admin_group_finance_open(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id = callback.data.split(":")
    chat_id = int(chat_id)
    thread_id = None if int(thread_id) == 0 else int(thread_id)
    await safe_edit_or_send(callback, render_group_finance(chat_id, thread_id), reply_markup=group_finance_manage_kb(chat_id, thread_id))
    await callback.answer()

@router.callback_query(F.data.startswith("admin:groupfin_add:"))
@router.callback_query(F.data.startswith("admin:groupfin_sub:"))
async def admin_group_finance_change_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split(":")
    action = "add" if parts[1].endswith("add") else "sub"
    chat_id = int(parts[2])
    thread_id = None if int(parts[3]) == 0 else int(parts[3])
    await state.set_state(AdminStates.waiting_group_finance_amount)
    await state.update_data(group_fin_action=action, group_fin_chat_id=chat_id, group_fin_thread_id=thread_id)
    label = f"<code>{chat_id}</code>" + (f" / topic <code>{thread_id}</code>" if thread_id else "")
    await callback.message.answer(f"Введите сумму для действия <b>{'пополнить' if action == 'add' else 'списать'}</b> в группе {label}:")
    await callback.answer()

@router.message(AdminStates.waiting_group_finance_amount)
async def admin_group_finance_amount(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or '').replace(',', '.').replace('$', '').strip())
    except Exception:
        await message.answer("Введите сумму числом.")
        return
    if value <= 0:
        await message.answer("Сумма должна быть больше 0.")
        return
    data = await state.get_data()
    chat_id = int(data['group_fin_chat_id'])
    thread_id = data.get('group_fin_thread_id')
    if data.get('group_fin_action') == 'add':
        db.add_group_balance(chat_id, thread_id, value)
    else:
        if value > db.get_group_balance(chat_id, thread_id):
            await message.answer("Недостаточно средств в казне группы.")
            return
        db.subtract_group_balance(chat_id, thread_id, value)
    await state.clear()
    await message.answer(render_group_finance(chat_id, thread_id), reply_markup=group_finance_manage_kb(chat_id, thread_id))

@router.callback_query(F.data.startswith("admin:groupprice:"))
async def admin_group_price_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id, mode, operator_key = callback.data.split(":")
    thread_id = None if int(thread_id) == 0 else int(thread_id)
    await state.set_state(AdminStates.waiting_group_price_value)
    await state.update_data(group_price_chat_id=int(chat_id), group_price_thread_id=thread_id, price_mode=mode, operator_key=operator_key)
    label = f"<code>{chat_id}</code>" + (f" / topic <code>{thread_id}</code>" if thread_id else "")
    await callback.message.answer(f"Введите цену для группы {label}: {op_text(operator_key)} • <b>{mode_label(mode)}</b>")
    await callback.answer()

@router.message(AdminStates.waiting_group_price_value)
async def admin_group_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or '').replace(',', '.').replace('$', '').strip())
    except Exception:
        await message.answer("Введите цену числом.")
        return
    if value <= 0:
        await message.answer("Цена должна быть больше 0.")
        return
    data = await state.get_data()
    chat_id = int(data['group_price_chat_id'])
    thread_id = data.get('group_price_thread_id')
    db.set_group_price(chat_id, thread_id, data['operator_key'], data['price_mode'], value)
    await state.clear()
    await message.answer(render_group_finance(chat_id, thread_id), reply_markup=group_finance_manage_kb(chat_id, thread_id))

@router.message()
async def track_any_message(message: Message):
    try:
        if message.from_user:
            touch_user(message.from_user.id, message.from_user.username or '', message.from_user.full_name)
    except Exception:
        logging.exception("track_any_message failed")


async def main():
    global LIVE_DP
    if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Укажи BOT_TOKEN прямо в bot.py")

    db.recover_after_restart()
    primary_bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    LIVE_DP = dp

    asyncio.create_task(hold_watcher(primary_bot))
    asyncio.create_task(backup_watcher(primary_bot))

    try:
        me = await primary_bot.get_me()
        logging.info("Primary bot started as @%s", me.username or BOT_USERNAME_FALLBACK)
        logging.info("Anti-crash recovery complete; holds and queue state restored")
    except Exception:
        logging.exception("Primary bot get_me failed")

    for mirror in db.all_active_mirrors():
        token = (mirror["token"] or "").strip()
        if not token or token == BOT_TOKEN:
            continue
        await start_live_mirror(token)

    await dp.start_polling(primary_bot)


if __name__ == "__main__":
    asyncio.run(main())
