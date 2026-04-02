import asyncio
import contextlib
import html
import logging
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode, ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =====================
# CONFIG
# =====================
BOT_TOKEN = "8731355621:AAGBnukT61jO9OOjZFepx_Tqgk1-w3n1gg4"
DB_PATH = "bot.db"
BOT_USERNAME_FALLBACK = "Seamusstest_bot"

CHIEF_ADMIN_ID = 626387429
BOOTSTRAP_ADMINS = [123456789]
BOOTSTRAP_OPERATORS = []
WITHDRAW_CHANNEL_ID = -1003785698154

DEFAULT_HOLD_MINUTES = 15
MIN_WITHDRAW_USD = 10.0
DEFAULT_START_TITLE = "💫 ESIM Service X 💫"
DEFAULT_START_SUBTITLE = "Премиум сервис приёма номеров"
DEFAULT_ANNOUNCEMENT = "<b>📣 Объявление</b>\n\n<i>Текст объявления пока не задан.</i>"
CRYPTO_PAY_TOKEN = ""
CRYPTO_PAY_ASSET = "USDT"
CRYPTO_PAY_PIN_CHECK_TO_USER = False

OPERATORS = {
    "mts": {"label": "МТС", "emoji": "🔺", "cmd": "/mts", "price": 4.00},
    "bil": {"label": "Билайн", "emoji": "🔸", "cmd": "/bil", "price": 4.50},
    "mega": {"label": "Мегафон", "emoji": "▫️", "cmd": "/mega", "price": 5.00},
    "t2": {"label": "Tele2", "emoji": "▪️", "cmd": "/t2", "price": 4.20},
}
ROLE_CHIEF = "chief"
ROLE_ADMIN = "admin"
ROLE_OPERATOR = "operator"
HTML_MODE = ParseMode.HTML
LOG = logging.getLogger(__name__)
ROUTER = Router()
TIMER_TASKS: dict[int, asyncio.Task] = {}
SUCCESS_TASKS: dict[int, asyncio.Task] = {}


# =====================
# HELPERS
# =====================
def now() -> datetime:
    return datetime.now()


def now_str() -> str:
    return now().strftime("%Y-%m-%d %H:%M:%S")


def dt_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def clean_username(username: Optional[str]) -> str:
    if not username:
        return "—"
    return username.lstrip("@")


def format_money(value: Any) -> str:
    try:
        return f"${float(value):.2f}"
    except Exception:
        return "$0.00"


def role_label(role: str) -> str:
    return {
        ROLE_CHIEF: "Главный админ",
        ROLE_ADMIN: "Админ",
        ROLE_OPERATOR: "Оператор",
    }.get(role, "Пользователь")


def number_valid(number: str) -> bool:
    return bool(re.fullmatch(r"(?:\+7|7|8)\d{10}", number.strip()))


def quote_block(lines: list[str]) -> str:
    return "<blockquote>" + "\n".join(lines) + "</blockquote>"


def mode_label(mode: str) -> str:
    return "Холд" if mode == "hold" else "БезХолд"


def mode_fancy(mode: str) -> str:
    return "⏳ Холд" if mode == "hold" else "⚡ БезХолд"


def hold_progress_bar(start_at: datetime, end_at: datetime, slots: int = 10) -> str:
    total = max((end_at - start_at).total_seconds(), 1)
    left = max((end_at - now()).total_seconds(), 0)
    ratio = max(0.0, min(1.0, 1 - left / total))
    done = round(ratio * slots)
    return "🟩" * done + "⬜" * (slots - done)


def hms_left(end_at: datetime) -> str:
    left = max(int((end_at - now()).total_seconds()), 0)
    m, s = divmod(left, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


# =====================
# DB
# =====================
class Database:
    def __init__(self, path: str):
        self.path = path
        self._init()
        self.seed_defaults()

    def conn(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con

    def _init(self):
        with self.conn() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    full_name TEXT,
                    username TEXT,
                    balance REAL DEFAULT 0,
                    total_earned REAL DEFAULT 0,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS roles (
                    user_id INTEGER PRIMARY KEY,
                    role TEXT NOT NULL,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    full_name TEXT,
                    username TEXT,
                    operator_key TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    phone_number TEXT NOT NULL,
                    qr_file_id TEXT NOT NULL,
                    price REAL NOT NULL,
                    status TEXT NOT NULL,
                    work_status TEXT DEFAULT 'queued',
                    worker_id INTEGER,
                    started_at TEXT,
                    hold_until TEXT,
                    success_at TEXT,
                    error_at TEXT,
                    slip_at TEXT,
                    work_seconds INTEGER DEFAULT 0,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    work_chat_id INTEGER,
                    work_message_id INTEGER,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL,
                    crypto_check_url TEXT,
                    crypto_check_id TEXT,
                    channel_message_id INTEGER,
                    created_at TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS workspaces (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    UNIQUE(chat_id, topic_id)
                );

                CREATE TABLE IF NOT EXISTS texts (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                """
            )

    def seed_defaults(self):
        with self.conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO roles(user_id, role, created_at) VALUES (?, ?, ?)",
                (CHIEF_ADMIN_ID, ROLE_CHIEF, now_str()),
            )
            for admin_id in BOOTSTRAP_ADMINS:
                con.execute(
                    "INSERT OR IGNORE INTO roles(user_id, role, created_at) VALUES (?, ?, ?)",
                    (admin_id, ROLE_ADMIN, now_str()),
                )
            for op_id in BOOTSTRAP_OPERATORS:
                con.execute(
                    "INSERT OR IGNORE INTO roles(user_id, role, created_at) VALUES (?, ?, ?)",
                    (op_id, ROLE_OPERATOR, now_str()),
                )
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('hold_minutes', ?)", (str(DEFAULT_HOLD_MINUTES),))
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('min_withdraw', ?)", (str(MIN_WITHDRAW_USD),))
            con.execute("INSERT OR IGNORE INTO texts(key, value) VALUES ('start_title', ?)", (DEFAULT_START_TITLE,))
            con.execute("INSERT OR IGNORE INTO texts(key, value) VALUES ('start_subtitle', ?)", (DEFAULT_START_SUBTITLE,))
            con.execute("INSERT OR IGNORE INTO texts(key, value) VALUES ('announcement', ?)", (DEFAULT_ANNOUNCEMENT,))
            for key, item in OPERATORS.items():
                con.execute(
                    "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                    (f"price_{key}", str(item["price"])),
                )

    def get_setting(self, key: str, default: Any = None) -> Any:
        with self.conn() as con:
            row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_setting(self, key: str, value: Any):
        with self.conn() as con:
            con.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )

    def get_text(self, key: str, default: str = "") -> str:
        with self.conn() as con:
            row = con.execute("SELECT value FROM texts WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_text(self, key: str, value: str):
        with self.conn() as con:
            con.execute(
                "INSERT INTO texts(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def upsert_user(self, user_id: int, full_name: str, username: Optional[str]):
        current = now_str()
        with self.conn() as con:
            con.execute(
                """
                INSERT INTO users(user_id, full_name, username, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    full_name=excluded.full_name,
                    username=excluded.username,
                    updated_at=excluded.updated_at
                """,
                (user_id, full_name, username, current, current),
            )

    def get_user(self, user_id: int):
        with self.conn() as con:
            return con.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()

    def add_balance(self, user_id: int, amount: float):
        current = now_str()
        with self.conn() as con:
            con.execute(
                "UPDATE users SET balance=COALESCE(balance,0)+?, total_earned=COALESCE(total_earned,0)+?, updated_at=? WHERE user_id=?",
                (amount, amount, current, user_id),
            )

    def take_balance(self, user_id: int, amount: float):
        current = now_str()
        with self.conn() as con:
            con.execute("UPDATE users SET balance=balance-?, updated_at=? WHERE user_id=?", (amount, current, user_id))

    def get_role(self, user_id: int) -> str:
        with self.conn() as con:
            row = con.execute("SELECT role FROM roles WHERE user_id=?", (user_id,)).fetchone()
        return row[0] if row else "user"

    def set_role(self, user_id: int, role: str):
        if user_id == CHIEF_ADMIN_ID:
            return
        with self.conn() as con:
            con.execute(
                "INSERT INTO roles(user_id, role, created_at) VALUES(?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET role=excluded.role",
                (user_id, role, now_str()),
            )

    def counts(self) -> dict[str, int]:
        with self.conn() as con:
            users = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            active = con.execute("SELECT COUNT(*) FROM requests WHERE status='queued'").fetchone()[0]
            withdrawals = con.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'").fetchone()[0]
        return {"users": users, "active": active, "withdrawals": withdrawals}

    def operator_price(self, operator_key: str) -> float:
        return float(self.get_setting(f"price_{operator_key}", OPERATORS[operator_key]["price"]))

    def create_request(self, *, user_id: int, full_name: str, username: str | None, operator_key: str, mode: str, phone_number: str, qr_file_id: str) -> int:
        current = now_str()
        price = self.operator_price(operator_key)
        with self.conn() as con:
            cur = con.execute(
                """
                INSERT INTO requests(
                    user_id, full_name, username, operator_key, mode, phone_number, qr_file_id,
                    price, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
                """,
                (user_id, full_name, username, operator_key, mode, phone_number, qr_file_id, price, current, current),
            )
            return int(cur.lastrowid)

    def get_request(self, request_id: int):
        with self.conn() as con:
            return con.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()

    def queue_count(self, operator_key: str) -> int:
        with self.conn() as con:
            row = con.execute(
                "SELECT COUNT(*) FROM requests WHERE operator_key=? AND status='queued'",
                (operator_key,),
            ).fetchone()
        return int(row[0])

    def next_request(self, operator_key: str):
        with self.conn() as con:
            return con.execute(
                "SELECT * FROM requests WHERE operator_key=? AND status='queued' ORDER BY id ASC LIMIT 1",
                (operator_key,),
            ).fetchone()

    def mark_work_card(self, request_id: int, work_chat_id: int, work_message_id: int):
        with self.conn() as con:
            con.execute(
                "UPDATE requests SET work_chat_id=?, work_message_id=?, updated_at=? WHERE id=?",
                (work_chat_id, work_message_id, now_str(), request_id),
            )

    def begin_request(self, request_id: int, worker_id: int, hold_minutes: int):
        current = now()
        hold_until = current + timedelta(minutes=hold_minutes)
        with self.conn() as con:
            con.execute(
                """
                UPDATE requests SET status='started', work_status='started', worker_id=?, started_at=?, hold_until=?, updated_at=?
                WHERE id=?
                """,
                (worker_id, dt_str(current), dt_str(hold_until), now_str(), request_id),
            )

    def mark_error(self, request_id: int):
        with self.conn() as con:
            con.execute(
                "UPDATE requests SET status='error', work_status='error', error_at=?, updated_at=? WHERE id=?",
                (now_str(), now_str(), request_id),
            )

    def mark_slip(self, request_id: int):
        req = self.get_request(request_id)
        started_at = parse_dt(req["started_at"]) if req else None
        secs = max(int((now() - started_at).total_seconds()), 0) if started_at else 0
        with self.conn() as con:
            con.execute(
                "UPDATE requests SET status='slipped', work_status='slipped', slip_at=?, work_seconds=?, updated_at=? WHERE id=?",
                (now_str(), secs, now_str(), request_id),
            )

    def mark_paid(self, request_id: int):
        req = self.get_request(request_id)
        if not req or req["status"] == "paid":
            return
        with self.conn() as con:
            con.execute(
                "UPDATE requests SET status='paid', work_status='success', success_at=?, updated_at=? WHERE id=?",
                (now_str(), now_str(), request_id),
            )
        self.add_balance(int(req["user_id"]), float(req["price"]))

    def active_hold_requests(self):
        with self.conn() as con:
            return con.execute("SELECT * FROM requests WHERE status='started' AND mode='hold'").fetchall()

    def create_withdrawal(self, user_id: int, amount: float) -> int:
        current = now_str()
        with self.conn() as con:
            cur = con.execute(
                "INSERT INTO withdrawals(user_id, amount, status, created_at, updated_at) VALUES (?, ?, 'pending', ?, ?)",
                (user_id, amount, current, current),
            )
            return int(cur.lastrowid)

    def get_withdrawal(self, withdrawal_id: int):
        with self.conn() as con:
            return con.execute("SELECT * FROM withdrawals WHERE id=?", (withdrawal_id,)).fetchone()

    def bind_withdrawal_channel_message(self, withdrawal_id: int, message_id: int):
        with self.conn() as con:
            con.execute("UPDATE withdrawals SET channel_message_id=?, updated_at=? WHERE id=?", (message_id, now_str(), withdrawal_id))

    def approve_withdrawal(self, withdrawal_id: int, check_url: str | None = None, check_id: str | None = None):
        with self.conn() as con:
            con.execute(
                "UPDATE withdrawals SET status='approved', crypto_check_url=?, crypto_check_id=?, updated_at=? WHERE id=?",
                (check_url, check_id, now_str(), withdrawal_id),
            )

    def decline_withdrawal(self, withdrawal_id: int):
        with self.conn() as con:
            con.execute("UPDATE withdrawals SET status='declined', updated_at=? WHERE id=?", (now_str(), withdrawal_id))

    def add_workspace(self, chat_id: int, topic_id: int | None):
        with self.conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO workspaces(chat_id, topic_id, enabled, created_at) VALUES (?, ?, 1, ?)",
                (chat_id, topic_id, now_str()),
            )

    def list_workspaces(self):
        with self.conn() as con:
            return con.execute("SELECT * FROM workspaces WHERE enabled=1").fetchall()

    def is_workspace(self, chat_id: int, topic_id: int | None):
        with self.conn() as con:
            row = con.execute(
                "SELECT 1 FROM workspaces WHERE chat_id=? AND topic_id IS ? AND enabled=1 LIMIT 1",
                (chat_id, topic_id),
            ).fetchone()
        return bool(row)

    def user_stats(self, user_id: int) -> dict[str, Any]:
        with self.conn() as con:
            total = con.execute("SELECT COUNT(*) FROM requests WHERE user_id=?", (user_id,)).fetchone()[0]
            success = con.execute("SELECT COUNT(*) FROM requests WHERE user_id=? AND status='paid'", (user_id,)).fetchone()[0]
            slips = con.execute("SELECT COUNT(*) FROM requests WHERE user_id=? AND status='slipped'", (user_id,)).fetchone()[0]
            errors = con.execute("SELECT COUNT(*) FROM requests WHERE user_id=? AND status='error'", (user_id,)).fetchone()[0]
            in_queue = con.execute("SELECT COUNT(*) FROM requests WHERE user_id=? AND status IN ('queued','started')", (user_id,)).fetchone()[0]
            rows = con.execute(
                "SELECT operator_key, COUNT(*) as c FROM requests WHERE user_id=? GROUP BY operator_key ORDER BY c DESC",
                (user_id,),
            ).fetchall()
            total_earned = con.execute("SELECT COALESCE(total_earned,0) FROM users WHERE user_id=?", (user_id,)).fetchone()
        return {
            "total": total,
            "success": success,
            "slips": slips,
            "errors": errors,
            "in_queue": in_queue,
            "operators": rows,
            "earned": float(total_earned[0] if total_earned else 0),
        }

    def global_stata(self) -> dict[str, Any]:
        with self.conn() as con:
            rows = con.execute(
                "SELECT operator_key, COUNT(*) as c FROM requests WHERE status='queued' GROUP BY operator_key"
            ).fetchall()
            taken = con.execute("SELECT COUNT(*) FROM requests WHERE worker_id IS NOT NULL").fetchone()[0]
            started = con.execute("SELECT COUNT(*) FROM requests WHERE status IN ('started','paid','slipped')").fetchone()[0]
            errors = con.execute("SELECT COUNT(*) FROM requests WHERE status='error'").fetchone()[0]
            slips = con.execute("SELECT COUNT(*) FROM requests WHERE status='slipped'").fetchone()[0]
            success = con.execute("SELECT COUNT(*) FROM requests WHERE status='paid'").fetchone()[0]
            total_paid = con.execute("SELECT COALESCE(SUM(price),0) FROM requests WHERE status='paid'").fetchone()[0]
        return {
            "queues": {r["operator_key"]: r["c"] for r in rows},
            "taken": taken,
            "started": started,
            "errors": errors,
            "slips": slips,
            "success": success,
            "total_paid": float(total_paid or 0),
        }


db = Database(DB_PATH)


# =====================
# STATE
# =====================
class SellFlow(StatesGroup):
    choosing_mode = State()
    choosing_operator = State()
    waiting_photo = State()


class WithdrawFlow(StatesGroup):
    waiting_amount = State()


class AdminFlow(StatesGroup):
    waiting_broadcast = State()
    waiting_start_title = State()
    waiting_start_subtitle = State()
    waiting_hold_minutes = State()
    waiting_price = State()
    waiting_role = State()


# =====================
# KEYBOARDS
# =====================
def main_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Сдать номер", callback_data="menu:sell")
    kb.button(text="👤 Профиль", callback_data="menu:profile")
    kb.button(text="💸 Вывод средств", callback_data="menu:withdraw")
    kb.adjust(1)
    return kb.as_markup()


def back_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:home")
    return kb.as_markup()


def sell_mode_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="sellmode:hold")
    kb.button(text="⚡ БезХолд", callback_data="sellmode:nohold")
    kb.button(text="↩️ Назад", callback_data="menu:home")
    kb.adjust(2, 1)
    return kb.as_markup()


def operator_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key in ["mts", "bil", "mega", "t2"]:
        item = OPERATORS[key]
        kb.button(text=f"{item['emoji']} {item['label']}", callback_data=f"operator:{key}")
    kb.button(text="↩️ Назад", callback_data="menu:sell")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def cancel_submit_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="menu:home")
    return kb.as_markup()


def withdraw_confirm_kb(amount: float) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить", callback_data=f"wdok:{amount}")
    kb.button(text="❌ Отменить", callback_data="menu:withdraw")
    kb.adjust(2)
    return kb.as_markup()


def work_request_kb(request_id: int, stage: str, mode: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if stage == "queued":
        kb.button(text="✅ Встал", callback_data=f"reqstart:{request_id}")
        kb.button(text="⚠️ Ошибка", callback_data=f"reqerror:{request_id}")
        kb.adjust(2)
    elif stage == "started":
        if mode == "nohold":
            kb.button(text="💸 Оплатить", callback_data=f"reqpay:{request_id}")
        kb.button(text="❌ Слет", callback_data=f"reqslip:{request_id}")
        kb.adjust(2)
    return kb.as_markup()


def admin_main_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Сводка", callback_data="admin:summary")
    kb.button(text="💎 Прайсы", callback_data="admin:prices")
    kb.button(text="⏳ Холд", callback_data="admin:hold")
    kb.button(text="👥 Роли", callback_data="admin:roles")
    kb.button(text="📣 Рассылка", callback_data="admin:broadcast")
    kb.button(text="🧩 Тексты", callback_data="admin:texts")
    kb.button(text="🏢 Рабочая группа", callback_data="admin:workspace")
    kb.button(text="↩️ В меню", callback_data="menu:home")
    kb.adjust(2, 2, 2, 1, 1)
    return kb.as_markup()


def admin_prices_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key in ["mts", "bil", "mega", "t2"]:
        item = OPERATORS[key]
        kb.button(text=f"{item['emoji']} {item['label']}", callback_data=f"setprice:{key}")
    kb.button(text="↩️ Назад", callback_data="admin:main")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def admin_texts_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Заголовок /start", callback_data="text:start_title")
    kb.button(text="📝 Подзаголовок /start", callback_data="text:start_subtitle")
    kb.button(text="📣 Объявление", callback_data="text:announcement")
    kb.button(text="↩️ Назад", callback_data="admin:main")
    kb.adjust(1)
    return kb.as_markup()


def role_select_kb(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="👑 Главный админ", callback_data=f"role:{user_id}:chief")
    kb.button(text="⚙️ Админ", callback_data=f"role:{user_id}:admin")
    kb.button(text="🛠 Оператор", callback_data=f"role:{user_id}:operator")
    kb.button(text="↩️ Назад", callback_data="admin:main")
    kb.adjust(1)
    return kb.as_markup()


def withdrawal_admin_kb(withdrawal_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить", callback_data=f"wdadm:ok:{withdrawal_id}")
    kb.button(text="❌ Отклонить", callback_data=f"wdadm:no:{withdrawal_id}")
    kb.adjust(2)
    return kb.as_markup()


# =====================
# RENDERERS
# =====================
def render_start(user_id: int) -> str:
    user = db.get_user(user_id)
    username = clean_username(user["username"] if user else None)
    balance = float(user["balance"] if user else 0)
    title = esc(db.get_text("start_title", DEFAULT_START_TITLE))
    subtitle = esc(db.get_text("start_subtitle", DEFAULT_START_SUBTITLE))
    price_lines = []
    queue_lines = []
    for key in ["mts", "bil", "mega", "t2"]:
        op = OPERATORS[key]
        price_lines.append(f"{op['emoji']} <b>{op['label']}</b> — <b>{format_money(db.operator_price(key))}</b>")
        queue_lines.append(f"{op['emoji']} <b>{op['label']}:</b> {db.queue_count(key)}")
    return (
        f"<b>{title}</b>\n"
        f"<i>{subtitle}</i>\n\n"
        "🚀 <b>Быстрый приём заявок</b> • 💎 <b>Стабильные выплаты</b> • 🛡 <b>Контроль статусов</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        f"🔗 <b>Username:</b> @{esc(username)}\n"
        f"🆔 <b>ID:</b> {user_id}\n"
        f"💰 <b>Баланс:</b> {format_money(balance)}\n"
        "━━━━━━━━━━━━━━\n\n"
        "<b>💎 Прайсы:</b>\n"
        f"{quote_block(price_lines)}\n\n"
        "<b>📤 Очереди:</b>\n"
        f"{quote_block(queue_lines)}\n\n"
        "<i>Вы находитесь в главном меню.</i>\n"
        "👇 <b>Выберите нужное действие ниже:</b>"
    )


def render_profile(user_id: int) -> str:
    user = db.get_user(user_id)
    stats = db.user_stats(user_id)
    username = clean_username(user["username"] if user else None)
    name = esc(user["full_name"] if user else "—")
    balance = float(user["balance"] if user else 0)
    operator_lines = []
    for row in stats["operators"]:
        op = OPERATORS.get(row["operator_key"], {"label": row["operator_key"], "emoji": "•"})
        operator_lines.append(f"{op['emoji']} <b>{op['label']}</b> — {row['c']}")
    if not operator_lines:
        operator_lines = ["• <i>Пока пусто</i>"]

    profile_lines = [
        f"🔘 <b>Имя:</b> {name}",
        f"™️ <b>Username:</b> @{esc(username)}",
        f"®️ <b>ID:</b> {user_id}",
        f"💲 <b>Баланс:</b> {format_money(balance)}",
    ]
    stat_lines = [
        f"🧾 <b>Всего заявок:</b> {stats['total']}",
        f"✅ <b>Успешно:</b> {stats['success']}",
        f"❌ <b>Слеты:</b> {stats['slips']}",
        f"⚠️ <b>Ошибки:</b> {stats['errors']}",
        f"💰 <b>Всего заработано:</b> {format_money(stats['earned'])}",
        f"📤 <b>Сейчас в очередях:</b> {stats['in_queue']}",
    ]

    return (
        "<b>👤 Личный кабинет — ESIM Service X 💫</b>\n\n"
        f"{quote_block(profile_lines)}\n\n"
        "<b>📊 Ваша статистика:</b>\n"
        f"{quote_block(stat_lines)}\n\n"
        "<b>📱 Разбивка по операторам</b>\n"
        f"{quote_block(operator_lines)}\n\n"
        "<i>Профиль обновляется автоматически по мере работы в боте.</i>"
    )


def render_withdraw(user_id: int) -> str:
    user = db.get_user(user_id)
    balance = float(user["balance"] if user else 0)
    minimum = float(db.get_setting("min_withdraw", MIN_WITHDRAW_USD))
    return (
        "<b>💸 Вывод средств — ESIM Service X 💫</b>\n\n"
        f"{quote_block([f'🔻 <b>Минимальный вывод:</b> {format_money(minimum)}', f'💰 <b>Ваш баланс:</b> {format_money(balance)}'])}\n\n"
        "🔹 <b>Введите сумму вывода в $:</b>"
    )


def render_sell_intro() -> str:
    return (
        f"<b>{esc(db.get_text('start_title', DEFAULT_START_TITLE))}</b>\n\n"
        "<b>📲 Сдать номер — ЕСИМ</b>\n\n"
        "<i>Сначала выберите режим работы для новой заявки:</i>"
    )


def render_mode_pick(mode: str) -> str:
    if mode == "hold":
        body = (
            "<b>Режим выбран: ⏳ Холд</b>\n\n"
            "🔥 <b>Холд</b> — режим работы с временной фиксацией номера.\n"
            "💰 Актуальные ставки смотрите в разделе <b>/start</b> — <b>«Прайсы»</b>.\n\n"
            "👇 <b>Теперь выберите оператора:</b>"
        )
    else:
        body = (
            "<b>Режим выбран: ⚡ БезХолд</b>\n\n"
            "🔥 <b>БезХолд</b> — режим работы без времени работы, оплату на режимы смотрите в разделе <b>/start</b> «Прайсы».\n\n"
            "👇 <b>Теперь выберите оператора:</b>"
        )
    return body


def render_send_number(operator_key: str, mode: str) -> str:
    return (
        f"<b>{esc(db.get_text('start_title', DEFAULT_START_TITLE))}</b>\n\n"
        "<b>Отправьте QR-код — фото сообщением</b>\n\n"
        "👉 <b>Требуется:</b>\n"
        "▫️ <b>Фото QR</b>\n"
        "▫️ <b>В подписи укажите номер</b>\n\n"
        "🔰 <b>Допустимый формат номера:</b>\n"
        f"{quote_block(['+79991234567 «+7»', '79991234567 «7»', '89991234567 «8»'])}\n\n"
        "<i>Если передумали — нажмите ниже «Отмена».</i>"
    )


def render_request_accepted(req) -> str:
    op = OPERATORS[req["operator_key"]]
    return (
        "<b>✅ Заявка принята — номер в очереди</b>\n\n"
        "🧾 <b>Информация по заявке:</b>\n"
        f"{quote_block([f'🆔 <b>ID заявки:</b> #{req['id']}', f'{op['emoji']} <b>Оператор:</b> {op['label']}', f'📞 <b>Номер:</b> {esc(req['phone_number'])}', f'💰 <b>Цена:</b> {format_money(req['price'])}', f'🔄 <b>Режим:</b> {mode_label(req['mode'])}'])}"
    )


def render_work_card(req, started: bool = False) -> str:
    op = OPERATORS[req["operator_key"]]
    user_name = esc(req["full_name"] or "—")
    base = (
        f"<b>📱 Оператор — {op['label']} {op['emoji']}</b>\n\n"
        f"🧾 <b>Заявка:</b> #{req['id']}\n"
        "👤 <b>Пользователь:</b>\n"
        f"<b>От:</b> {user_name}\n"
        f"<b>ID:</b> {req['user_id']}\n\n"
        "⭐️ <b>ESim — Номер</b>\n"
        f"📞 <b>Номер:</b> {esc(req['phone_number'])}\n"
        f"💰 <b>Цена:</b> {format_money(req['price'])}\n"
        f"🔄 <b>Режим:</b> {mode_label(req['mode'])}"
    )
    if started and req["mode"] == "nohold":
        base += "\n\n🚀 <b>Работа началась</b>\n⚡ <b>Режим БезХолд</b> — номер можно оплатить кнопкой ниже:"
    elif started and req["mode"] == "hold":
        start_at = parse_dt(req["started_at"]) or now()
        end_at = parse_dt(req["hold_until"]) or now()
        base += (
            "\n\n🚀 <b>Работа началась</b>\n"
            f"⏳ <b>Холд:</b> {int(db.get_setting('hold_minutes', DEFAULT_HOLD_MINUTES))} мин.\n"
            f"🕓 <b>До:</b> {esc(req['hold_until'])}\n"
            f"📊 <b>Прогресс:</b> {hold_progress_bar(start_at, end_at)}\n"
            f"⏱ <b>Осталось:</b> {hms_left(end_at)}"
        )
    return base


def render_started_user(req) -> str:
    op = OPERATORS[req["operator_key"]]
    return (
        "<b>✅ Номер — Встал ✅</b>\n\n"
        "🚀 <b>По вашему номеру началась работа</b>\n\n"
        f"📞 <b>Номер:</b> {esc(req['phone_number'])}\n"
        f"📱 <b>Оператор — {op['label']} {op['emoji']}</b>\n"
        f"{('⚡' if req['mode']=='nohold' else '⏳')} <b>Режим:</b> {mode_label(req['mode'])}"
    )


def render_paid(req, balance: float) -> str:
    return (
        "<b>✅ Оплата за номер</b>\n\n"
        f"📞 <b>Номер:</b> {esc(req['phone_number'])}\n"
        f"💰 <b>Начислено:</b> {format_money(req['price'])}\n"
        f"💲 <b>Ваш баланс:</b> {format_money(balance)}"
    )


def render_error(req) -> str:
    return (
        "<b>⚠️ Ошибка — номер не встал</b>\n\n"
        f"📞 <b>Номер:</b> {esc(req['phone_number'])}\n"
        "❌ <b>Номер не принят в работу.</b>"
    )


def render_slip(req) -> str:
    work_seconds = int(req["work_seconds"] or 0)
    m, s = divmod(work_seconds, 60)
    hold_left = "—"
    if req["hold_until"]:
        end_at = parse_dt(req["hold_until"])
        if end_at:
            hold_left = hms_left(end_at)
    return (
        "<b>❌ Номер слетел</b>\n\n"
        f"📞 <b>Номер:</b> {esc(req['phone_number'])}\n"
        f"⏱ <b>Время работы:</b> {m:02d}:{s:02d}\n"
        f"▫️ <b>Холд осталось:</b> {hold_left}\n\n"
        "❌ <b>Оплата за номер не начислена.</b>"
    )


def render_admin_panel(user_id: int) -> str:
    counts = db.counts()
    return (
        "<b>⚙️ Admin Panel — ESIM Service X</b>\n\n"
        f"{quote_block([f'👑 <b>Роль:</b> {role_label(db.get_role(user_id))}', f'👥 <b>Пользователей:</b> {counts['users']}', f'📤 <b>Активных заявок:</b> {counts['active']}', f'💸 <b>Заявок на вывод:</b> {counts['withdrawals']}', f'⏳ <b>Стандартный Холд:</b> {db.get_setting('hold_minutes', DEFAULT_HOLD_MINUTES)} мин.'])}\n\n"
        "👇 <b>Выберите раздел управления:</b>"
    )


def render_stata() -> str:
    st = db.global_stata()
    qlines = []
    for key in ["mts", "bil", "mega", "t2"]:
        op = OPERATORS[key]
        qlines.append(f"{op['emoji']} <b>{op['label']}:</b> {st['queues'].get(key, 0)}")
    return (
        "<b>📊 Статистика рабочей группы</b>\n\n"
        f"{quote_block(qlines)}\n\n"
        "<b>📈 Показатели:</b>\n"
        f"{quote_block([f'📥 <b>Взято номеров:</b> {st['taken']}', f'✅ <b>Встало:</b> {st['started']}', f'⚠️ <b>Ошибок:</b> {st['errors']}', f'❌ <b>Слетов:</b> {st['slips']}', f'💎 <b>Успешно:</b> {st['success']}', f'💰 <b>Тотал оплат:</b> {format_money(st['total_paid'])}'])}"
    )


def render_withdraw_confirm(amount: float) -> str:
    return (
        "<b>💸 Подтверждение вывода</b>\n\n"
        f"{quote_block([f'📅 <b>Дата:</b> {now().strftime('%Y-%m-%d')}', f'🕓 <b>Время:</b> {now().strftime('%H:%M:%S')}', f'💰 <b>Сумма:</b> {format_money(amount)}'])}\n\n"
        "👇 <b>Подтвердите действие ниже:</b>"
    )


def render_withdraw_created(withdrawal_id: int, amount: float) -> str:
    return (
        "<b>✅ Заявка на вывод создана</b>\n\n"
        f"{quote_block([f'🧾 <b>ID заявки:</b> #{withdrawal_id}', f'💰 <b>Сумма:</b> {format_money(amount)}', '🕓 <b>Статус:</b> В обработке'])}\n\n"
        "<i>Ожидайте подтверждения администратора.</i>"
    )


# =====================
# ACCESS
# =====================
def is_admin(user_id: int) -> bool:
    return db.get_role(user_id) in {ROLE_CHIEF, ROLE_ADMIN}


def is_operator(user_id: int) -> bool:
    return db.get_role(user_id) in {ROLE_CHIEF, ROLE_ADMIN, ROLE_OPERATOR}


def private_only(message: Message) -> bool:
    return message.chat.type == ChatType.PRIVATE


# =====================
# CRYPTO BOT
# =====================
async def create_crypto_check(amount: float, user_id: int, username: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not CRYPTO_PAY_TOKEN:
        return None, None
    url = "https://pay.crypt.bot/api/createCheck"
    payload: dict[str, Any] = {"asset": CRYPTO_PAY_ASSET, "amount": str(round(amount, 2))}
    if CRYPTO_PAY_PIN_CHECK_TO_USER:
        payload["pin_to_user_id"] = user_id
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers, timeout=20) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                LOG.error("Crypto Pay error: %s", data)
                return None, None
            result = data.get("result", {})
            return result.get("bot_check_url"), str(result.get("check_id"))


# =====================
# WORK DISPATCH
# =====================
async def send_request_to_workspaces(bot: Bot, request_id: int):
    req = db.get_request(request_id)
    if not req:
        return
    sent_any = False
    for ws in db.list_workspaces():
        try:
            sent = await bot.send_photo(
                chat_id=ws["chat_id"],
                photo=req["qr_file_id"],
                message_thread_id=ws["topic_id"],
                caption=render_work_card(req, started=False),
                reply_markup=work_request_kb(request_id, "queued", req["mode"]),
            )
            db.mark_work_card(request_id, sent.chat.id, sent.message_id)
            sent_any = True
        except Exception as exc:
            LOG.exception("send workspace failed: %s", exc)
    if not sent_any:
        LOG.warning("No active workspaces to send request %s", request_id)


async def update_hold_message(bot: Bot, request_id: int):
    req = db.get_request(request_id)
    if not req or req["status"] != "started" or req["mode"] != "hold":
        return
    if not req["work_chat_id"] or not req["work_message_id"]:
        return
    try:
        await bot.edit_message_caption(
            chat_id=req["work_chat_id"],
            message_id=req["work_message_id"],
            caption=render_work_card(req, started=True),
            reply_markup=work_request_kb(request_id, "started", req["mode"]),
        )
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            LOG.warning("edit hold message: %s", exc)


async def hold_timer_loop(bot: Bot, request_id: int):
    while True:
        req = db.get_request(request_id)
        if not req or req["status"] != "started" or req["mode"] != "hold":
            return
        end_at = parse_dt(req["hold_until"])
        if not end_at:
            return
        if now() >= end_at:
            db.mark_paid(request_id)
            fresh = db.get_request(request_id)
            user = db.get_user(int(fresh["user_id"]))
            with contextlib.suppress(Exception):
                await bot.send_message(int(fresh["user_id"]), render_paid(fresh, float(user["balance"] if user else 0)))
            with contextlib.suppress(Exception):
                await bot.edit_message_caption(
                    chat_id=fresh["work_chat_id"],
                    message_id=fresh["work_message_id"],
                    caption=render_work_card(fresh, started=True) + "\n\n✅ <b>Холд завершён успешно</b>",
                )
            return
        await update_hold_message(bot, request_id)
        await asyncio.sleep(30)


# =====================
# HOOKS
# =====================
async def ensure_user(message: Message):
    db.upsert_user(message.from_user.id, message.from_user.full_name, message.from_user.username)


# =====================
# HANDLERS
# =====================
@ROUTER.message(Command("start"))
async def start_cmd(message: Message, state: FSMContext):
    await ensure_user(message)
    await state.clear()
    await message.answer(render_start(message.from_user.id), reply_markup=main_menu())


@ROUTER.callback_query(F.data == "menu:home")
async def menu_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text(render_start(call.from_user.id), reply_markup=main_menu())
    await call.answer()


@ROUTER.callback_query(F.data == "menu:profile")
async def menu_profile(call: CallbackQuery):
    await call.message.edit_text(render_profile(call.from_user.id), reply_markup=back_menu())
    await call.answer()


@ROUTER.callback_query(F.data == "menu:withdraw")
async def menu_withdraw(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(WithdrawFlow.waiting_amount)
    await call.message.edit_text(render_withdraw(call.from_user.id), reply_markup=back_menu())
    await call.answer()


@ROUTER.message(WithdrawFlow.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    await ensure_user(message)
    text = (message.text or "").replace(",", ".").strip()
    try:
        amount = float(text)
    except Exception:
        await message.answer("<b>⚠️ Введите сумму числом в долларах.</b>", reply_markup=back_menu())
        return
    user = db.get_user(message.from_user.id)
    balance = float(user["balance"] if user else 0)
    minimum = float(db.get_setting("min_withdraw", MIN_WITHDRAW_USD))
    if amount < minimum:
        await message.answer(f"<b>⚠️ Минимальный вывод:</b> {format_money(minimum)}", reply_markup=back_menu())
        return
    if amount > balance:
        await message.answer("<b>⚠️ Недостаточно средств на балансе.</b>", reply_markup=back_menu())
        return
    await state.update_data(withdraw_amount=amount)
    await message.answer(render_withdraw_confirm(amount), reply_markup=withdraw_confirm_kb(amount))


@ROUTER.callback_query(F.data.startswith("wdok:"))
async def withdraw_confirm(call: CallbackQuery, state: FSMContext, bot: Bot):
    amount = float(call.data.split(":", 1)[1])
    user = db.get_user(call.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount > balance:
        await call.answer("Недостаточно средств", show_alert=True)
        return
    db.take_balance(call.from_user.id, amount)
    withdrawal_id = db.create_withdrawal(call.from_user.id, amount)
    text = render_withdraw_created(withdrawal_id, amount)
    await call.message.edit_text(text, reply_markup=back_menu())
    admin_text = (
        "<b>💸 Новая заявка на вывод</b>\n\n"
        f"{quote_block([f'🧾 <b>ID:</b> #{withdrawal_id}', f'👤 <b>User ID:</b> {call.from_user.id}', f'🔗 <b>Username:</b> @{esc(clean_username(call.from_user.username))}', f'💰 <b>Сумма:</b> {format_money(amount)}'])}"
    )
    msg = await bot.send_message(WITHDRAW_CHANNEL_ID, admin_text, reply_markup=withdrawal_admin_kb(withdrawal_id))
    db.bind_withdrawal_channel_message(withdrawal_id, msg.message_id)
    await state.clear()
    await call.answer()


@ROUTER.callback_query(F.data.startswith("wdadm:"))
async def withdrawal_admin_action(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    _, action, wid_text = call.data.split(":")
    withdrawal_id = int(wid_text)
    wd = db.get_withdrawal(withdrawal_id)
    if not wd or wd["status"] != "pending":
        await call.answer("Уже обработано", show_alert=True)
        return
    if action == "ok":
        check_url, check_id = await create_crypto_check(float(wd["amount"]), int(wd["user_id"]), None)
        db.approve_withdrawal(withdrawal_id, check_url, check_id)
        text = (
            "<b>✅ Заявка на вывод одобрена</b>\n\n"
            f"💸 <b>Сумма:</b> {format_money(wd['amount'])}\n"
            + (f"🎟 <b>Чек Crypto Bot:</b> <a href=\"{esc(check_url)}\">получить чек</a>" if check_url else "🎟 <b>Чек:</b> не создан, заявка одобрена вручную")
        )
        with contextlib.suppress(Exception):
            await bot.send_message(int(wd["user_id"]), text)
        await call.message.edit_text(call.message.html_text + "\n\n✅ <b>Одобрено</b>")
    else:
        db.decline_withdrawal(withdrawal_id)
        db.add_balance(int(wd["user_id"]), float(wd["amount"]))
        with contextlib.suppress(Exception):
            await bot.send_message(int(wd["user_id"]), f"<b>❌ Заявка на вывод отклонена</b>\n\n💸 <b>Сумма:</b> {format_money(wd['amount'])}")
        await call.message.edit_text(call.message.html_text + "\n\n❌ <b>Отклонено</b>")
    await call.answer()


@ROUTER.callback_query(F.data == "menu:sell")
async def menu_sell(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(SellFlow.choosing_mode)
    await call.message.edit_text(render_sell_intro(), reply_markup=sell_mode_kb())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("sellmode:"))
async def choose_mode(call: CallbackQuery, state: FSMContext):
    mode = call.data.split(":", 1)[1]
    await state.update_data(mode=mode)
    await state.set_state(SellFlow.choosing_operator)
    await call.message.edit_text(render_mode_pick(mode), reply_markup=operator_kb())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("operator:"))
async def choose_operator(call: CallbackQuery, state: FSMContext):
    operator_key = call.data.split(":", 1)[1]
    data = await state.get_data()
    mode = data.get("mode", "nohold")
    await state.update_data(operator_key=operator_key)
    await state.set_state(SellFlow.waiting_photo)
    await call.message.edit_text(render_send_number(operator_key, mode), reply_markup=cancel_submit_kb())
    await call.answer()


@ROUTER.message(SellFlow.waiting_photo, F.photo)
async def photo_number_submit(message: Message, state: FSMContext, bot: Bot):
    await ensure_user(message)
    caption = (message.caption or "").strip()
    if not number_valid(caption):
        await message.answer(
            "<b>⚠️ Неверный формат номера</b>\n\n"
            + quote_block(["+79991234567", "79991234567", "89991234567"]),
            reply_markup=cancel_submit_kb(),
        )
        return
    data = await state.get_data()
    mode = data.get("mode")
    operator_key = data.get("operator_key")
    if not operator_key:
        await message.answer("<b>⚠️ Сначала выберите оператора.</b>")
        return
    request_id = db.create_request(
        user_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username,
        operator_key=operator_key,
        mode=mode,
        phone_number=caption,
        qr_file_id=message.photo[-1].file_id,
    )
    req = db.get_request(request_id)
    await message.answer(render_request_accepted(req), reply_markup=main_menu())
    await state.clear()
    await send_request_to_workspaces(bot, request_id)


@ROUTER.message(SellFlow.waiting_photo)
async def photo_expected(message: Message):
    await message.answer("<b>⚠️ Нужно отправить именно фото QR с номером в подписи.</b>", reply_markup=cancel_submit_kb())


@ROUTER.message(Command("admin"))
async def admin_cmd(message: Message):
    await ensure_user(message)
    if not is_admin(message.from_user.id):
        await message.answer("<b>⛔ Доступ запрещён</b>")
        return
    await message.answer(render_admin_panel(message.from_user.id), reply_markup=admin_main_kb())


@ROUTER.callback_query(F.data == "admin:main")
async def admin_main(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.edit_text(render_admin_panel(call.from_user.id), reply_markup=admin_main_kb())
    await call.answer()


@ROUTER.callback_query(F.data == "admin:summary")
async def admin_summary(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.message.edit_text(render_stata(), reply_markup=admin_main_kb())
    await call.answer()


@ROUTER.callback_query(F.data == "admin:prices")
async def admin_prices(call: CallbackQuery):
    lines = []
    for key in ["mts", "bil", "mega", "t2"]:
        op = OPERATORS[key]
        lines.append(f"{op['emoji']} <b>{op['label']}</b> — {format_money(db.operator_price(key))}")
    await call.message.edit_text("<b>💎 Управление прайсами</b>\n\n" + quote_block(lines), reply_markup=admin_prices_kb())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("setprice:"))
async def set_price_prompt(call: CallbackQuery, state: FSMContext):
    key = call.data.split(":", 1)[1]
    await state.set_state(AdminFlow.waiting_price)
    await state.update_data(price_operator=key)
    await call.message.edit_text(f"<b>💎 Новый прайс для {OPERATORS[key]['label']}</b>\n\nВведите сумму в $")
    await call.answer()


@ROUTER.message(AdminFlow.waiting_price)
async def set_price_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    key = data.get("price_operator")
    try:
        amount = float((message.text or "").replace(",", "."))
    except Exception:
        await message.answer("<b>⚠️ Введите число.</b>")
        return
    db.set_setting(f"price_{key}", amount)
    await state.clear()
    await message.answer(f"<b>✅ Прайс обновлён:</b> {OPERATORS[key]['label']} — {format_money(amount)}", reply_markup=admin_main_kb())


@ROUTER.callback_query(F.data == "admin:hold")
async def admin_hold(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminFlow.waiting_hold_minutes)
    await call.message.edit_text(
        f"<b>⏳ Настройка Холд</b>\n\nТекущее значение: <b>{db.get_setting('hold_minutes', DEFAULT_HOLD_MINUTES)} мин.</b>\n\nВведите новое количество минут."
    )
    await call.answer()


@ROUTER.message(AdminFlow.waiting_hold_minutes)
async def admin_hold_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        minutes = int((message.text or "").strip())
        if minutes < 1:
            raise ValueError
    except Exception:
        await message.answer("<b>⚠️ Введите целое число минут.</b>")
        return
    db.set_setting("hold_minutes", minutes)
    await state.clear()
    await message.answer(f"<b>✅ Холд обновлён:</b> {minutes} мин.", reply_markup=admin_main_kb())


@ROUTER.callback_query(F.data == "admin:texts")
async def admin_texts(call: CallbackQuery):
    await call.message.edit_text("<b>🧩 Управление текстами</b>", reply_markup=admin_texts_kb())
    await call.answer()


@ROUTER.callback_query(F.data.startswith("text:"))
async def admin_text_set_prompt(call: CallbackQuery, state: FSMContext):
    key = call.data.split(":", 1)[1]
    await state.update_data(text_key=key)
    if key == "start_title":
        await state.set_state(AdminFlow.waiting_start_title)
    elif key == "start_subtitle":
        await state.set_state(AdminFlow.waiting_start_subtitle)
    else:
        await state.set_state(AdminFlow.waiting_broadcast)
    await call.message.edit_text(f"<b>✍️ Отправьте новый текст для:</b> {esc(key)}")
    await call.answer()


@ROUTER.message(AdminFlow.waiting_start_title, AdminFlow.waiting_start_subtitle, AdminFlow.waiting_broadcast)
async def admin_text_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    key = data.get("text_key")
    db.set_text(key, message.html_text or esc(message.text or ""))
    await state.clear()
    await message.answer("<b>✅ Текст обновлён.</b>", reply_markup=admin_main_kb())


@ROUTER.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminFlow.waiting_broadcast)
    await state.update_data(text_key="announcement")
    await call.message.edit_text(
        "<b>📣 Рассылка</b>\n\nОтправьте текст объявления. Поддерживается безопасный HTML: <b>&lt;b&gt;</b>, <b>&lt;i&gt;</b>, <b>&lt;u&gt;</b>, <b>&lt;blockquote&gt;</b>."
    )
    await call.answer()


@ROUTER.callback_query(F.data == "admin:roles")
async def admin_roles(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminFlow.waiting_role)
    await call.message.edit_text("<b>👥 Роли</b>\n\nОтправьте <b>ID пользователя</b>, которому нужно назначить роль.", reply_markup=admin_main_kb())
    await call.answer()


@ROUTER.message(AdminFlow.waiting_role)
async def admin_role_choose(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        target = int((message.text or "").strip())
    except Exception:
        await message.answer("<b>⚠️ Нужен числовой user ID.</b>")
        return
    await state.clear()
    await message.answer(f"<b>Выберите роль для ID {target}</b>", reply_markup=role_select_kb(target))


@ROUTER.callback_query(F.data.startswith("role:"))
async def admin_role_apply(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    _, uid_text, role = call.data.split(":")
    uid = int(uid_text)
    if uid == CHIEF_ADMIN_ID and role != ROLE_CHIEF:
        await call.answer("Главного админа снять нельзя", show_alert=True)
        return
    if db.get_role(call.from_user.id) != ROLE_CHIEF and role == ROLE_CHIEF:
        await call.answer("Только главный админ может назначить главного", show_alert=True)
        return
    db.set_role(uid, role)
    await call.message.edit_text(f"<b>✅ Роль обновлена:</b> {uid} → {role_label(role)}", reply_markup=admin_main_kb())
    await call.answer()


@ROUTER.callback_query(F.data == "admin:workspace")
async def admin_workspace(call: CallbackQuery):
    text = (
        "<b>🏢 Рабочие зоны</b>\n\n"
        "<b>/work</b> — добавить текущую группу как рабочую\n"
        "<b>/topic</b> — добавить текущий топик как рабочий\n\n"
        "<i>Эти команды доступны только админу и главному админу.</i>"
    )
    await call.message.edit_text(text, reply_markup=admin_main_kb())
    await call.answer()


@ROUTER.message(Command("work"))
async def add_work_group(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("<b>⛔ Доступ запрещён</b>")
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("<b>⚠️ Команда работает только в группе.</b>")
        return
    db.add_workspace(message.chat.id, None)
    await message.answer("<b>✅ Рабочая группа добавлена.</b>")


@ROUTER.message(Command("topic"))
async def add_work_topic(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("<b>⛔ Доступ запрещён</b>")
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("<b>⚠️ Команда работает только в группе/топике.</b>")
        return
    db.add_workspace(message.chat.id, message.message_thread_id)
    await message.answer("<b>✅ Рабочий топик добавлен.</b>")


async def operator_take(message: Message, operator_key: str, bot: Bot):
    if not is_operator(message.from_user.id):
        await message.answer("<b>⛔ Доступ запрещён</b>")
        return
    if not db.is_workspace(message.chat.id, message.message_thread_id):
        await message.answer("<b>⚠️ Эта группа/топик не добавлены как рабочие.</b>")
        return
    req = db.next_request(operator_key)
    if not req:
        await message.answer(f"<b>📭 Очередь {OPERATORS[operator_key]['label']} пока пуста.</b>")
        return
    sent = await bot.send_photo(
        chat_id=message.chat.id,
        message_thread_id=message.message_thread_id,
        photo=req["qr_file_id"],
        caption=render_work_card(req, started=False),
        reply_markup=work_request_kb(req["id"], "queued", req["mode"]),
    )
    db.mark_work_card(req["id"], sent.chat.id, sent.message_id)


@ROUTER.message(Command("mts"))
async def cmd_mts(message: Message, bot: Bot):
    await operator_take(message, "mts", bot)


@ROUTER.message(Command("bil"))
async def cmd_bil(message: Message, bot: Bot):
    await operator_take(message, "bil", bot)


@ROUTER.message(Command("mega"))
async def cmd_mega(message: Message, bot: Bot):
    await operator_take(message, "mega", bot)


@ROUTER.message(Command("t2"))
async def cmd_t2(message: Message, bot: Bot):
    await operator_take(message, "t2", bot)


@ROUTER.message(Command("stata"))
async def cmd_stata(message: Message):
    if not is_operator(message.from_user.id):
        await message.answer("<b>⛔ Доступ запрещён</b>")
        return
    await message.answer(render_stata())


@ROUTER.callback_query(F.data.startswith("reqstart:"))
async def req_start(call: CallbackQuery, bot: Bot):
    if not is_operator(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    request_id = int(call.data.split(":", 1)[1])
    req = db.get_request(request_id)
    if not req or req["status"] != "queued":
        await call.answer("Заявка уже изменена", show_alert=True)
        return
    db.begin_request(request_id, call.from_user.id, int(db.get_setting("hold_minutes", DEFAULT_HOLD_MINUTES)))
    fresh = db.get_request(request_id)
    await call.message.edit_caption(caption=render_work_card(fresh, started=True), reply_markup=work_request_kb(request_id, "started", fresh["mode"]))
    with contextlib.suppress(Exception):
        await bot.send_message(int(fresh["user_id"]), render_started_user(fresh))
    if fresh["mode"] == "hold":
        task = asyncio.create_task(hold_timer_loop(bot, request_id))
        TIMER_TASKS[request_id] = task
    await call.answer("Работа началась")


@ROUTER.callback_query(F.data.startswith("reqerror:"))
async def req_error(call: CallbackQuery, bot: Bot):
    if not is_operator(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    request_id = int(call.data.split(":", 1)[1])
    req = db.get_request(request_id)
    if not req or req["status"] != "queued":
        await call.answer("Уже изменено", show_alert=True)
        return
    db.mark_error(request_id)
    fresh = db.get_request(request_id)
    await call.message.edit_caption(caption=render_work_card(fresh, started=False) + "\n\n⚠️ <b>Отмечено: ошибка</b>")
    with contextlib.suppress(Exception):
        await bot.send_message(int(fresh["user_id"]), render_error(fresh))
    await call.answer("Ошибка отмечена")


@ROUTER.callback_query(F.data.startswith("reqslip:"))
async def req_slip(call: CallbackQuery, bot: Bot):
    if not is_operator(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    request_id = int(call.data.split(":", 1)[1])
    req = db.get_request(request_id)
    if not req or req["status"] != "started":
        await call.answer("Уже изменено", show_alert=True)
        return
    db.mark_slip(request_id)
    if task := TIMER_TASKS.pop(request_id, None):
        task.cancel()
    fresh = db.get_request(request_id)
    await call.message.edit_caption(caption=render_work_card(fresh, started=True) + "\n\n❌ <b>Заявка завершена со слётом</b>")
    with contextlib.suppress(Exception):
        await bot.send_message(int(fresh["user_id"]), render_slip(fresh))
    await call.answer("Слет отмечен")


@ROUTER.callback_query(F.data.startswith("reqpay:"))
async def req_pay(call: CallbackQuery, bot: Bot):
    if not is_operator(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    request_id = int(call.data.split(":", 1)[1])
    req = db.get_request(request_id)
    if not req or req["status"] != "started":
        await call.answer("Уже изменено", show_alert=True)
        return
    db.mark_paid(request_id)
    fresh = db.get_request(request_id)
    user = db.get_user(int(fresh["user_id"]))
    await call.message.edit_caption(caption=render_work_card(fresh, started=True) + "\n\n✅ <b>Заявка оплачена</b>")
    with contextlib.suppress(Exception):
        await bot.send_message(int(fresh["user_id"]), render_paid(fresh, float(user["balance"] if user else 0)))
    await call.answer("Оплата начислена")


# =====================
# MAIN
# =====================
async def main():
    logging.basicConfig(level=logging.INFO)
    if not BOT_TOKEN:
        raise RuntimeError("Укажи BOT_TOKEN прямо в bot.py")
    bot = Bot(BOT_TOKEN, parse_mode=HTML_MODE)
    me = await bot.get_me()
    logging.info("Bot started as @%s", me.username or BOT_USERNAME_FALLBACK)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(ROUTER)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
