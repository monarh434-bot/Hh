import asyncio
import html
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile, CallbackQuery, KeyboardButton, Message, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

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
MIN_WITHDRAW = 10.0
DEFAULT_HOLD_MINUTES = 15
DEFAULT_TREASURY_BALANCE = 0.0

# Crypto Bot / Crypto Pay API
CRYPTO_PAY_TOKEN = ""  # fill to enable real checks
CRYPTO_PAY_BASE_URL = "https://pay.crypt.bot/api"
CRYPTO_PAY_ASSET = "USDT"
CRYPTO_PAY_PIN_CHECK_TO_USER = False  # True -> check pinned to telegram user

OPERATORS = {
    "mts": {"title": "МТС", "price": 4.00, "command": "/mts"},
    "bil": {"title": "Билайн", "price": 4.50, "command": "/bil"},
    "mega": {"title": "Мегафон", "price": 5.00, "command": "/mega"},
    "t2": {"title": "Tele2", "price": 4.20, "command": "/t2"},
}
# =========================================================

logging.basicConfig(level=logging.INFO)
router = Router()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SubmitStates(StatesGroup):
    waiting_mode = State()
    waiting_operator = State()
    waiting_qr = State()


class WithdrawStates(StatesGroup):
    waiting_amount = State()


class AdminStates(StatesGroup):
    waiting_hold = State()
    waiting_min_withdraw = State()
    waiting_treasury_add = State()
    waiting_treasury_sub = State()
    waiting_operator_price = State()
    waiting_role_user = State()
    waiting_role_kind = State()
    waiting_start_text = State()
    waiting_ad_text = State()
    waiting_broadcast_text = State()


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
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
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
        }
        for key, value in defaults.items():
            self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
        for key, data in OPERATORS.items():
            self.conn.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (f"price_{key}", str(data["price"])),
            )
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
                self.get_operator_price(operator_key),
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
        start_dt = datetime.now()
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

    def get_treasury(self) -> float:
        return float(self.get_setting("treasury_balance", str(DEFAULT_TREASURY_BALANCE)))

    def add_treasury(self, amount: float):
        self.set_setting("treasury_balance", str(self.get_treasury() + amount))

    def subtract_treasury(self, amount: float):
        self.set_setting("treasury_balance", str(self.get_treasury() - amount))

    def enable_workspace(self, chat_id: int, thread_id: Optional[int], mode: str, added_by: int):
        self.conn.execute(
            "INSERT INTO workspaces (chat_id, thread_id, mode, added_by, created_at, is_enabled) VALUES (?, ?, ?, ?, ?, 1) ON CONFLICT(chat_id, thread_id, mode) DO UPDATE SET is_enabled=1, added_by=excluded.added_by, created_at=excluded.created_at",
            (chat_id, thread_id, mode, added_by, now_str()),
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

    def group_stats(self, chat_id: int, thread_id: Optional[int]):
        return self.conn.execute(
            """
            SELECT
                COUNT(*) AS taken_total,
                SUM(CASE WHEN work_started_at IS NOT NULL THEN 1 ELSE 0 END) AS started,
                SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total
            FROM queue_items
            WHERE work_chat_id=? AND ((work_thread_id IS NULL AND ? IS NULL) OR work_thread_id=?)
            """,
            (chat_id, thread_id, thread_id),
        ).fetchone()


db = Database(DB_PATH)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    left = max((end - datetime.now()).total_seconds(), 0)
    done = max(total - left, 0)
    filled = min(size, max(0, round(done / total * size)))
    return "🟩" * filled + "⬜" * (size - filled)


def time_left_text(hold_until: Optional[str]) -> str:
    end = parse_dt(hold_until)
    if not end:
        return "—"
    left = end - datetime.now()
    if left.total_seconds() <= 0:
        return "00:00"
    total = int(left.total_seconds())
    minutes = total // 60
    seconds = total % 60
    return f"{minutes:02d}:{seconds:02d}"


def main_menu():
    kb = ReplyKeyboardBuilder()
    kb.button(text="📲 Сдать номер")
    kb.button(text="👤 Профиль")
    kb.button(text="💸 Вывод средств")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


def back_menu(label: str = "↩️ Назад"):
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=label)]], resize_keyboard=True)

def cancel_menu():
    return back_menu("❌ Отмена")


def mode_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="mode:hold")
    kb.button(text="⚡ БезХолд", callback_data="mode:no_hold")
    kb.button(text="↩️ Назад", callback_data="mode:back")
    kb.adjust(2, 1)
    return kb.as_markup()


def operators_kb():
    kb = InlineKeyboardBuilder()
    labels = {"mts": "🔺 МТС", "bil": "🔸 Билайн", "mega": "▫️ Мегафон", "t2": "▪️ Tele2"}
    for key in OPERATORS:
        kb.button(text=labels.get(key, OPERATORS[key]["title"]), callback_data=f"op:{key}")
    kb.button(text="↩️ Назад", callback_data="op:back")
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
    kb = ReplyKeyboardBuilder()
    kb.button(text="↩️ Назад")
    return kb.as_markup(resize_keyboard=True)


def withdraw_admin_kb(withdraw_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить", callback_data=f"wd_ok:{withdraw_id}")
    kb.button(text="❌ Отклонить", callback_data=f"wd_no:{withdraw_id}")
    kb.adjust(2)
    return kb.as_markup()


def admin_root_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Сводка", callback_data="admin:summary")
    kb.button(text="🏦 Казна", callback_data="admin:treasury")
    kb.button(text="💸 Выводы", callback_data="admin:withdraws")
    kb.button(text="⏳ Холд", callback_data="admin:hold")
    kb.button(text="💎 Прайсы", callback_data="admin:prices")
    kb.button(text="👥 Роли", callback_data="admin:roles")
    kb.button(text="🛰 Рабочие зоны", callback_data="admin:workspaces")
    kb.button(text="⚙️ Настройки", callback_data="admin:settings")
    kb.adjust(2, 2, 2, 2)
    return kb.as_markup()


def admin_back_kb(target: str = "admin:home"):
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data=target)
    return kb.as_markup()


def treasury_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Пополнить", callback_data="admin:treasury_add")
    kb.button(text="➖ Списать", callback_data="admin:treasury_sub")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2, 1)
    return kb.as_markup()


def hold_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Изменить Холд", callback_data="admin:set_hold")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def settings_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Мин. вывод", callback_data="admin:set_min_withdraw")
    kb.button(text="✍️ Старт-текст", callback_data="admin:set_start_text")
    kb.button(text="📣 Объявление", callback_data="admin:set_ad_text")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def prices_kb():
    kb = InlineKeyboardBuilder()
    for key, data in OPERATORS.items():
        kb.button(text=f"💎 {data['title']}", callback_data=f"admin:set_price:{key}")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2, 2, 1)
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
        f"📱 <b>{OPERATORS[item.operator_key]['title']}</b>\n\n"
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
    username = f"@{escape(user['username'])}" if user and user["username"] else "не указан"
    title = escape(db.get_setting("start_title", "ESIM Service X"))
    subtitle = escape(db.get_setting("start_subtitle", "Премиум сервис приёма номеров"))
    description = db.get_setting("start_description", "")
    price_lines = [
        f"🔺 <b>МТС</b> — <b>{usd(db.get_operator_price('mts'))}</b>",
        f"🔸 <b>Билайн</b> — <b>{usd(db.get_operator_price('bil'))}</b>",
        f"▫️ <b>Мегафон</b> — <b>{usd(db.get_operator_price('mega'))}</b>",
        f"▪️ <b>Tele2</b> — <b>{usd(db.get_operator_price('t2'))}</b>",
    ]
    queue_lines = [
        f"🔺 <b>МТС:</b> {db.count_waiting('mts')}",
        f"🔸 <b>Билайн:</b> {db.count_waiting('bil')}",
        f"▫️ <b>Мегафон:</b> {db.count_waiting('mega')}",
        f"▪️ <b>Tele2:</b> {db.count_waiting('t2')}",
    ]
    announcement = db.get_setting("announcement_text", "").strip()
    announce_block = f"\n\n<blockquote>{announcement}</blockquote>" if announcement else ""
    price_block = "\n".join(price_lines)
    queue_block = "\n".join(queue_lines)
    return f"""<b>💫 {title} 💫</b>
<i>{subtitle}</i>

{description}{announce_block}

━━━━━━━━━━━━━━
🔗 <b>Username:</b> {username}
🆔 <b>ID:</b> <code>{user_id}</code>
💰 <b>Баланс:</b> <b>{balance}</b>
━━━━━━━━━━━━━━

<b>💎 Прайсы:</b>
<blockquote>{price_block}</blockquote>

<b>📤 Очереди:</b>
<blockquote>{queue_block}</blockquote>

<i>Вы находитесь в главном меню.</i>
👇 <b>Выберите нужное действие ниже:</b>"""


def render_profile(user_id: int) -> str:
    user = db.get_user(user_id)
    stats = db.user_stats(user_id)
    ops = db.user_operator_stats(user_id)
    queue_total = sum(db.count_waiting(k) for k in OPERATORS)
    username = f"@{escape(user['username'])}" if user and user['username'] else "не указан"
    ops_text = "\n".join(
        f"• <b>{OPERATORS[row['operator_key']]['title']}</b>: {row['total']} шт. / <b>{usd(row['earned'] or 0)}</b>"
        for row in ops
    ) or "• <i>Пока пусто</i>"
    return f"""<b>👤 Личный кабинет — ESIM Service X 💫</b>

<blockquote>🔘 <b>Имя:</b> {escape(user['full_name'] if user else '')}\n™️ <b>Username:</b> {username}\n®️ <b>ID:</b> <code>{user_id}</code>\n💲 <b>Баланс:</b> <b>{usd(user['balance'] if user else 0)}</b></blockquote>

<b>📊 Ваша статистика:</b>
<blockquote>🧾 <b>Всего заявок:</b> {int(stats['total'] or 0)}\n✅ <b>Успешно:</b> {int(stats['completed'] or 0)}\n❌ <b>Слеты:</b> {int(stats['slipped'] or 0)}\n⚠️ <b>Ошибки:</b> {int(stats['errors'] or 0)}\n💰 <b>Всего заработано:</b> <b>{usd(stats['earned'] or 0)}</b>\n📤 <b>Сейчас в очередях:</b> {queue_total}</blockquote>

<b>📱 Разбивка по операторам</b>
<blockquote>{ops_text}</blockquote>

<i>Профиль обновляется автоматически по мере работы в боте.</i>"""


def render_admin_home() -> str:
    role = user_role(CHIEF_ADMIN_ID)
    return (
        "<b>⚙️ Admin Panel — ESIM Service X</b>\n\n"
        f"👑 Главный админ: <code>{CHIEF_ADMIN_ID}</code>\n"
        f"💸 Заявок на вывод: <b>{db.count_pending_withdrawals()}</b>\n"
        f"🏦 Казна: <b>{usd(db.get_treasury())}</b>\n"
        f"⏳ Холд: <b>{db.get_setting('hold_minutes')}</b> мин.\n"
        f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
        f"🔐 Ваша роль: <b>{role}</b>"
    )


def render_admin_summary() -> str:
    lines = []
    for key, data in OPERATORS.items():
        lines.append(f"• {data['title']}: {db.count_waiting(key)}")
    return "<b>📊 Сводка очередей</b>\n\n" + "\n".join(lines)


def render_admin_treasury() -> str:
    return f"<b>🏦 Казна</b>\n\n💰 Баланс казны: <b>{usd(db.get_treasury())}</b>"


def render_admin_withdraws() -> str:
    return f"<b>💸 Выводы</b>\n\n📬 В ожидании: <b>{db.count_pending_withdrawals()}</b>"


def render_admin_hold() -> str:
    return f"<b>⏳ Холд</b>\n\nТекущее время Холд: <b>{db.get_setting('hold_minutes')}</b> мин."


def render_admin_settings() -> str:
    return (
        "<b>⚙️ Настройки системы</b>\n\n"
        f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
        f"📝 Старт-заголовок: <b>{escape(db.get_setting('start_title', 'DIAMOND HUB'))}</b>\n"
        f"📣 Объявление: <b>{'задано' if db.get_setting('announcement_text', '').strip() else 'пусто'}</b>"
    )


def render_design() -> str:
    return (
        "<b>🎨 Дизайн и тексты</b>\n\n"
        f"🪪 Заголовок: <b>{escape(db.get_setting('start_title', 'DIAMOND HUB'))}</b>\n"
        f"💬 Подзаголовок: <b>{escape(db.get_setting('start_subtitle', ''))}</b>\n"
        f"📣 Объявление: <b>{'есть' if db.get_setting('announcement_text', '').strip() else 'нет'}</b>\n\n"
        "Здесь можно менять оформление главного экрана и текст объявления.\n"
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
    return "<b>💎 Прайсы</b>\n\n" + "\n".join(
        f"• {data['title']}: <b>{usd(db.get_operator_price(key))}</b>" for key, data in OPERATORS.items()
    )


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
        body = "Нет активных рабочих зон.\n\n• /work — включить группу\n• /topic — включить топик"
    else:
        body = "\n".join(
            f"• chat <code>{row['chat_id']}</code> | thread <code>{row['thread_id'] or 0}</code> | {row['mode']}"
            for row in rows
        )
    return "<b>🛰 Рабочие зоны</b>\n\n" + body


async def notify_user(bot: Bot, user_id: int, text: str):
    try:
        await bot.send_message(user_id, text)
    except Exception:
        logging.exception("notify_user failed")


async def create_crypto_check(amount: float, user_id: Optional[int] = None) -> tuple[Optional[str], str]:
    if not CRYPTO_PAY_TOKEN:
        return None, "CRYPTO_PAY_TOKEN не заполнен, поэтому выдана ручная заявка вместо чека."
    payload = {"asset": CRYPTO_PAY_ASSET, "amount": f"{amount:.2f}"}
    if CRYPTO_PAY_PIN_CHECK_TO_USER and user_id:
        payload["pin_to_user_id"] = int(user_id)
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_PAY_BASE_URL}/createCheck", json=payload, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get("ok"):
            return None, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        result = data.get("result", {})
        return result.get("bot_check_url") or result.get("url"), "Чек создан через Crypto Bot."
    except Exception as e:
        return None, f"Ошибка создания чека: {e}"


@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    db.upsert_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    await state.clear()
    await message.answer(render_start(message.from_user.id), reply_markup=main_menu())


@router.message(F.text == "👤 Профиль")
async def profile_view(message: Message, state: FSMContext):
    db.upsert_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    await state.clear()
    await message.answer(render_profile(message.from_user.id), reply_markup=main_menu())


@router.message(F.text == "📲 Сдать номер")
async def submit_start(message: Message, state: FSMContext):
    await state.set_state(SubmitStates.waiting_mode)
    await message.answer(
        "<b>📲 Сдать номер</b>\n\nСначала выберите режим работы для новой заявки:",
        reply_markup=mode_kb(),
    )


@router.callback_query(F.data == "mode:back")
async def mode_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(render_start(callback.from_user.id))
    await callback.message.answer("Главное меню", reply_markup=main_menu())
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
    await callback.message.edit_text(
        f"<b>Режим выбран: {mode_title}</b>\n\n{mode_desc}\n\n👇 <b>Теперь выберите оператора:</b>",
        reply_markup=operators_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "op:back")
async def op_back(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubmitStates.waiting_mode)
    await callback.message.edit_text("<b>📲 Сдать номер</b>\n\nВыберите режим:", reply_markup=mode_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("op:"))
async def choose_operator(callback: CallbackQuery, state: FSMContext):
    operator_key = callback.data.split(":", 1)[1]
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    await state.update_data(operator_key=operator_key)
    await state.set_state(SubmitStates.waiting_qr)
    await callback.message.edit_text(
        "<b>Отправьте QR-код</b>\n\n"
        "• фото QR\n"
        "• в подписи укажите номер\n\n"
        "Допустимый формат номера:\n"
        "<code>+79991234567</code>\n"
        "<code>79991234567</code>\n"
        "<code>89991234567</code>",
        reply_markup=None,
    )
    await callback.answer()


@router.message(SubmitStates.waiting_qr, F.text.in_({"↩️ Назад", "❌ Отмена"}))
@router.message(WithdrawStates.waiting_amount, F.text == "↩️ Назад")
async def global_back(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(render_start(message.from_user.id), reply_markup=main_menu())


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
    db.upsert_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    file_id = message.photo[-1].file_id
    item_id = db.create_queue_item(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.full_name,
        operator_key,
        phone,
        file_id,
        mode,
    )
    await state.clear()
    await message.answer(
        "<b>✅ Заявка принята</b>\n\n"
        f"🧾 ID заявки: <b>{item_id}</b>\n"
        f"📱 Оператор: <b>{OPERATORS[operator_key]['title']}</b>\n"
        f"📞 Номер: <code>{pretty_phone(phone)}</code>\n"
        f"💰 Цена: <b>{usd(db.get_operator_price(operator_key))}</b>\n"
        f"🔄 Режим: <b>{'Холд' if mode == 'hold' else 'БезХолд'}</b>",
        reply_markup=main_menu(),
    )


@router.message(SubmitStates.waiting_qr)
async def submit_not_photo(message: Message):
    await message.answer("<b>⚠️ Отправьте именно фото QR-кода с подписью-номером.</b>", reply_markup=cancel_menu())


@router.message(F.text == "💸 Вывод средств")
async def withdraw_start(message: Message, state: FSMContext):
    user = db.get_user(message.from_user.id)
    balance = float(user["balance"] if user else 0)
    minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
    await state.set_state(WithdrawStates.waiting_amount)
    await message.answer(
        "<b>💸 Вывод средств</b>\n\n"
        f"📉 Минимальный вывод: <b>{usd(minimum)}</b>\n"
        f"💰 Ваш баланс: <b>{usd(balance)}</b>\n\n"
        "Введите сумму вывода в $:",
        reply_markup=withdraw_back_kb(),
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
            reply_markup=withdraw_back_kb(),
        )
        return
    minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
    user = db.get_user(message.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount < minimum:
        await message.answer(f"⚠️ <b>Сумма меньше минимальной.</b> Минимум: <b>{usd(minimum)}</b>", reply_markup=withdraw_back_kb())
        return
    if amount > balance:
        await message.answer("⚠️ <b>Недостаточно средств на балансе.</b>", reply_markup=withdraw_back_kb())
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
    await callback.message.answer(render_start(callback.from_user.id), reply_markup=main_menu())
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
    text = (
        "<b>📨 Новая заявка на вывод</b>\n\n"
        f"🧾 ID: <b>{wd_id}</b>\n"
        f"👤 Пользователь: <b>{escape(callback.from_user.full_name)}</b>\n"
        f"🆔 ID: <code>{callback.from_user.id}</code>\n"
        f"💸 Сумма: <b>{usd(amount)}</b>"
    )
    try:
        await callback.bot.send_message(WITHDRAW_CHANNEL_ID, text, reply_markup=withdraw_admin_kb(wd_id))
    except Exception:
        logging.exception("send withdraw to channel failed")
    await callback.message.edit_text("✅ Заявка на вывод создана и отправлена на проверку.")
    await callback.message.answer(render_start(callback.from_user.id), reply_markup=main_menu())
    await callback.answer()


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
    await callback.message.edit_text(render_admin_hold(), reply_markup=hold_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:prices")
async def admin_prices(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_prices(), reply_markup=prices_kb())
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


@router.callback_query(F.data == "admin:settings")
async def admin_settings(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_settings(), reply_markup=settings_kb())
    await callback.answer()


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
    ad = db.get_setting("announcement_text", "").strip()
    await callback.message.answer(ad or "Объявление пока пустое.")
    await callback.answer()


@router.callback_query(F.data == "admin:broadcast_send_ad")
async def admin_broadcast_send_ad(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ad = db.get_setting("announcement_text", "").strip()
    if not ad:
        await callback.answer("Сначала сохрани объявление", show_alert=True)
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
        "Отправьте текст объявления.\n\nМожно писать красивыми шаблонами и использовать HTML Telegram."
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
    await state.set_state(AdminStates.waiting_treasury_add)
    await callback.message.answer("Введите сумму пополнения казны в $:")
    await callback.answer()


@router.callback_query(F.data == "admin:treasury_sub")
async def admin_treasury_sub(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_treasury_sub)
    await callback.message.answer("Введите сумму списания казны в $:")
    await callback.answer()


@router.callback_query(F.data.startswith("admin:set_price:"))
async def admin_set_price_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    operator_key = callback.data.split(":")[-1]
    await state.set_state(AdminStates.waiting_operator_price)
    await state.update_data(operator_key=operator_key)
    await callback.message.answer(f"Введите новую цену для {OPERATORS[operator_key]['title']} в $:")
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


@router.message(AdminStates.waiting_treasury_add)
async def admin_treasury_add_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    db.add_treasury(value)
    await state.clear()
    await message.answer(f"✅ Казна пополнена. Сейчас: {usd(db.get_treasury())}")


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
    db.subtract_treasury(value)
    await state.clear()
    await message.answer(f"✅ Средства списаны. Сейчас: {usd(db.get_treasury())}")


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
    db.set_setting(f"price_{operator_key}", str(value))
    await state.clear()
    await message.answer("✅ Прайс обновлён.")


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
    db.set_setting("announcement_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Объявление сохранено.")


@router.message(AdminStates.waiting_broadcast_text)
async def admin_broadcast_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.set_setting("announcement_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Текст сохранён как активное объявление. Теперь его можно разослать из /admin.")


@router.message(Command("work"))
async def enable_work_group(message: Message):
    if not is_admin(message.from_user.id) and user_role(message.from_user.id) != "chief_admin":
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Эта команда работает только в группе.")
        return
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
        await message.answer(f"📭 Для оператора {OPERATORS[operator_key]['title']} очередь пуста.")
        return
    db.mark_taken(item.id, message.from_user.id)
    item = db.get_queue_item(item.id)
    await message.answer_photo(item.qr_file_id, caption=queue_caption(item), reply_markup=admin_queue_kb(item))


@router.message(Command("mts", "mtc"))
async def queue_mts(message: Message):
    await send_next_item_for_operator(message, "mts")


@router.message(Command("bil"))
async def queue_bil(message: Message):
    await send_next_item_for_operator(message, "bil")


@router.message(Command("mega"))
async def queue_mega(message: Message):
    await send_next_item_for_operator(message, "mega")


@router.message(Command("t2"))
async def queue_t2(message: Message):
    await send_next_item_for_operator(message, "t2")


@router.message(Command("stata", "Stata"))
async def stata_cmd(message: Message):
    if not is_operator_or_admin(message.from_user.id):
        return
    thread_id = getattr(message, "message_thread_id", None)
    group = db.group_stats(message.chat.id, thread_id)
    queue_lines = [f"• {data['title']}: {db.count_waiting(key)}" for key, data in OPERATORS.items()]
    await message.answer(
        "<b>📊 Статистика рабочей зоны</b>\n\n"
        f"📤 Очередь по операторам:\n" + "\n".join(queue_lines) + "\n\n"
        f"📥 В группе взято: <b>{int(group['taken_total'] or 0)}</b>\n"
        f"✅ Встало: <b>{int(group['started'] or 0)}</b>\n"
        f"⚠️ Ошибок: <b>{int(group['errors'] or 0)}</b>\n"
        f"❌ Слетов: <b>{int(group['slips'] or 0)}</b>\n"
        f"💎 Успешно: <b>{int(group['success'] or 0)}</b>\n"
        f"💵 Тотал оплат: <b>{usd(group['paid_total'] or 0)}</b>"
    )


@router.callback_query(F.data.startswith("error_pre:"))
async def error_pre(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":", 1)[1])
    item = db.get_queue_item(item_id)
    if not item or item.status not in {"queued", "taken"}:
        await callback.answer("Действие уже недоступно", show_alert=True)
        return
    db.mark_error_before_start(item_id)
    db.add_balance(item.user_id, 0)
    await notify_user(
        callback.bot,
        item.user_id,
        "<b>⚠️ Заявка отмечена как ошибка</b>\n\n"
        f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
        "Номер не принят в работу.",
    )
    await callback.message.edit_caption(caption=queue_caption(item) + "\n\n⚠️ Отмечено как ошибка до старта.", reply_markup=None)
    await callback.answer("Помечено как ошибка")


@router.callback_query(F.data.startswith("take_start:"))
async def take_start(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":", 1)[1])
    item = db.get_queue_item(item_id)
    if not item:
        await callback.answer("Заявка не найдена", show_alert=True)
        return
    if item.status in {"in_progress", "completed", "failed"}:
        await callback.answer("По этой заявке действие уже выполнено", show_alert=True)
        return
    db.start_work(item_id, callback.from_user.id, item.mode, callback.message.chat.id, getattr(callback.message, 'message_thread_id', None), callback.message.message_id)
    item = db.get_queue_item(item_id)
    await notify_user(
        callback.bot,
        item.user_id,
        "<b>🚀 По вашему номеру началась работа</b>\n\n"
        f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
        f"📱 Оператор: <b>{OPERATORS[item.operator_key]['title']}</b>\n"
        + (f"⏳ Холд: <b>{db.get_setting('hold_minutes')} мин.</b>" if item.mode == 'hold' else "⚡ Режим: <b>БезХолд</b>"),
    )
    await callback.message.edit_caption(caption=queue_caption(item), reply_markup=admin_queue_kb(item))
    await callback.answer("Работа началась")


@router.callback_query(F.data.startswith("instant_pay:"))
async def instant_pay(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":", 1)[1])
    item = db.get_queue_item(item_id)
    if not item or item.status != "in_progress" or item.mode != "no_hold":
        await callback.answer("Оплата доступна только для режима БезХолд после старта.", show_alert=True)
        return
    db.complete_queue_item(item_id)
    db.add_balance(item.user_id, item.price)
    await notify_user(
        callback.bot,
        item.user_id,
        "<b>✅ Номер успешно принят</b>\n\n"
        f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
        f"💰 Начислено: <b>{usd(item.price)}</b>",
    )
    await callback.message.edit_caption(caption=queue_caption(item) + "\n\n✅ Номер оплачен.", reply_markup=None)
    await callback.answer("Оплачено")


@router.callback_query(F.data.startswith("slip:"))
async def slip_item(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":", 1)[1])
    item = db.get_queue_item(item_id)
    if not item or item.status != "in_progress":
        await callback.answer("Кнопка «Слет» доступна только после «Встал».", show_alert=True)
        return
    db.fail_after_start(item_id, "slip")
    item = db.get_queue_item(item_id)
    await notify_user(
        callback.bot,
        item.user_id,
        "<b>❌ Номер слетел</b>\n\n"
        f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
        f"⏱ Время работы: <b>{calc_work_time(item.work_started_at)}</b>\n\n"
        "Оплата за номер не начислена.",
    )
    await callback.message.edit_caption(
        caption=queue_caption(item) + f"\n\n❌ Отмечено как слет\n⏱ Время работы: <b>{calc_work_time(item.work_started_at)}</b>",
        reply_markup=None,
    )
    await callback.answer("Помечено как слет")


@router.callback_query(F.data.startswith("wd_ok:"))
async def wd_ok(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    wd_id = int(callback.data.split(":", 1)[1])
    wd = db.get_withdrawal(wd_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана", show_alert=True)
        return
    if db.get_treasury() < wd["amount"]:
        await callback.answer("В казне недостаточно средств", show_alert=True)
        return
    check_url, note = await create_crypto_check(float(wd["amount"]), wd["user_id"])
    db.subtract_treasury(float(wd["amount"]))
    db.set_withdrawal_status(wd_id, "approved", callback.from_user.id, payout_check=check_url, payout_note=note)
    if check_url:
        payout_text = f"🎟 Чек: {check_url}"
    else:
        payout_text = "🎟 Чек не создан автоматически. Проверь токен Crypto Pay API."
    await notify_user(
        callback.bot,
        wd["user_id"],
        "<b>✅ Заявка на вывод одобрена</b>\n\n"
        f"💸 Сумма: <b>{usd(wd['amount'])}</b>\n"
        f"{escape(payout_text)}\n"
        f"📝 {escape(note)}",
    )
    await callback.message.edit_text((callback.message.text or "") + f"\n\n✅ Одобрено\n{escape(payout_text)}\n🏦 Остаток казны: <b>{usd(db.get_treasury())}</b>")
    await callback.answer("Одобрено")


@router.callback_query(F.data.startswith("wd_no:"))
async def wd_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    wd_id = int(callback.data.split(":", 1)[1])
    wd = db.get_withdrawal(wd_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана", show_alert=True)
        return
    db.set_withdrawal_status(wd_id, "rejected", callback.from_user.id, payout_note="rejected by admin")
    db.add_balance(wd["user_id"], float(wd["amount"]))
    await notify_user(callback.bot, wd["user_id"], f"<b>❌ Заявка на вывод отклонена</b>\n\n💸 Сумма возвращена: <b>{usd(wd['amount'])}</b>")
    await callback.message.edit_text((callback.message.text or "") + "\n\n❌ Отклонено")
    await callback.answer("Отклонено")


def calc_work_time(started_at: Optional[str]) -> str:
    start = parse_dt(started_at)
    if not start:
        return "00:00"
    diff = datetime.now() - start
    total = int(diff.total_seconds())
    minutes = total // 60
    seconds = total % 60
    return f"{minutes:02d}:{seconds:02d}"


async def hold_watcher(bot: Bot):
    while True:
        try:
            for item in db.get_expired_holds():
                db.complete_queue_item(item.id)
                db.add_balance(item.user_id, item.price)
                try:
                    await bot.edit_message_caption(
                        chat_id=item.work_chat_id,
                        message_id=item.work_message_id,
                        caption=queue_caption(db.get_queue_item(item.id)) + "\n\n✅ Холд завершён. Номер успешно оплачен.",
                        reply_markup=None,
                    )
                except Exception:
                    pass
                await notify_user(
                    bot,
                    item.user_id,
                    "<b>✅ Номер успешно засчитан</b>\n\n"
                    f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
                    f"💰 Начислено: <b>{usd(item.price)}</b>",
                )
            for item in db.get_active_holds_for_render():
                rendered = parse_dt(item.timer_last_render)
                if rendered and (datetime.now() - rendered).total_seconds() < 30:
                    continue
                try:
                    fresh = db.get_queue_item(item.id)
                    if fresh and fresh.status == "in_progress":
                        await bot.edit_message_caption(
                            chat_id=fresh.work_chat_id,
                            message_id=fresh.work_message_id,
                            caption=queue_caption(fresh),
                            reply_markup=admin_queue_kb(fresh),
                        )
                        db.touch_timer_render(fresh.id)
                except Exception:
                    pass
        except Exception:
            logging.exception("hold_watcher error")
        await asyncio.sleep(5)


@router.message()
async def track_any_message(message: Message):
    if message.from_user:
        db.upsert_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)


async def main():
    if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Укажи BOT_TOKEN прямо в bot.py")
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    asyncio.create_task(hold_watcher(bot))
    me = await bot.get_me()
    logging.info("Bot started as @%s", me.username or BOT_USERNAME_FALLBACK)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
