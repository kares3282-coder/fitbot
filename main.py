"""
FitBot — Telegram-бот для похудения с реферальной системой, балансом,
тренировками для девушек и мужчин, подпиской через Telegram Stars
и админ-панелью. Использует Nscale (Llama 4 Scout).
"""

import asyncio
import base64
import json
import logging
import os
import ssl
from datetime import datetime, timedelta, timezone

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dotenv import load_dotenv

# ═══════════════════════════════════════════════════════════════
#  КОНФИГ
# ═══════════════════════════════════════════════════════════════

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
NSCALE_SERVICE_TOKEN = os.getenv("NSCALE_SERVICE_TOKEN", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "xydobotbot")

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
}

PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL", "https://your-app.onrender.com")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "fitbot-secret-2026")

DB_PATH = "fitbot.db"
SEP = "━━━━━━━━━━━━━━"

SUBSCRIPTION_STARS = int(os.getenv("SUBSCRIPTION_STARS", "100"))
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))
FREE_DAILY_LIMIT = int(os.getenv("FREE_DAILY_LIMIT", "3"))
PREMIUM_PRICE_RUB = int(os.getenv("PREMIUM_PRICE_RUB", "199"))
REFERRAL_BONUS = int(os.getenv("REFERRAL_BONUS", "50"))
WORKOUT_FREE_DAILY = int(os.getenv("WORKOUT_FREE_DAILY", "1"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("fitbot")


def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS


# ═══════════════════════════════════════════════════════════════
#  SSL-ПАТЧ
# ═══════════════════════════════════════════════════════════════

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

_orig_session_init = aiohttp.ClientSession.__init__


def _patched_session_init(self, *args, **kwargs):
    if "connector" not in kwargs:
        kwargs["connector"] = aiohttp.TCPConnector(ssl=_ssl_ctx)
    _orig_session_init(self, *args, **kwargs)


aiohttp.ClientSession.__init__ = _patched_session_init

# ═══════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                telegram_id      INTEGER PRIMARY KEY,
                username         TEXT,
                full_name        TEXT,
                registered_at    TEXT NOT NULL,
                sex              TEXT,
                age              INTEGER,
                height           REAL,
                weight_start     REAL,
                weight_current   REAL,
                weight_target    REAL,
                activity         REAL DEFAULT 1.375,
                calorie_goal     INTEGER,
                water_goal       INTEGER DEFAULT 8,
                onboarded        INTEGER DEFAULT 0,
                is_banned        INTEGER DEFAULT 0,
                premium_until    TEXT,
                free_used_today  INTEGER DEFAULT 0,
                free_used_date   TEXT,
                balance          REAL DEFAULT 0.0,
                referred_by      INTEGER,
                referrals_count  INTEGER DEFAULT 0,
                referrals_earned REAL DEFAULT 0.0,
                referral_paid    INTEGER DEFAULT 0
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS meals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                date        TEXT NOT NULL,
                meal_type   TEXT,
                description TEXT,
                calories    REAL DEFAULT 0,
                protein     REAL DEFAULT 0,
                fat         REAL DEFAULT 0,
                carbs       REAL DEFAULT 0,
                created_at  TEXT NOT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS weights (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                weight      REAL NOT NULL,
                created_at  TEXT NOT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS water (
                telegram_id INTEGER NOT NULL,
                date        TEXT NOT NULL,
                glasses     INTEGER DEFAULT 0,
                PRIMARY KEY (telegram_id, date)
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                amount      INTEGER NOT NULL,
                charge_id   TEXT,
                status      TEXT DEFAULT 'pending',
                created_at  TEXT NOT NULL,
                reviewed_at TEXT
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                inviter_id  INTEGER NOT NULL,
                invited_id  INTEGER NOT NULL UNIQUE,
                created_at  TEXT NOT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS workouts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id  INTEGER NOT NULL,
                workout_type TEXT NOT NULL,
                name         TEXT NOT NULL,
                duration_min INTEGER DEFAULT 0,
                calories     REAL DEFAULT 0,
                completed_at TEXT NOT NULL
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS workout_catalog (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                gender       TEXT NOT NULL,
                category     TEXT NOT NULL,
                name         TEXT NOT NULL,
                description  TEXT,
                duration_min INTEGER,
                calories     REAL,
                level        TEXT
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )""")
        await db.commit()

        # Миграции
        for migration in (
            "ALTER TABLE profiles ADD COLUMN is_banned INTEGER DEFAULT 0",
            "ALTER TABLE profiles ADD COLUMN premium_until TEXT",
            "ALTER TABLE profiles ADD COLUMN free_used_today INTEGER DEFAULT 0",
            "ALTER TABLE profiles ADD COLUMN free_used_date TEXT",
            "ALTER TABLE profiles ADD COLUMN balance REAL DEFAULT 0.0",
            "ALTER TABLE profiles ADD COLUMN referred_by INTEGER",
            "ALTER TABLE profiles ADD COLUMN referrals_count INTEGER DEFAULT 0",
            "ALTER TABLE profiles ADD COLUMN referrals_earned REAL DEFAULT 0.0",
            "ALTER TABLE profiles ADD COLUMN referral_paid INTEGER DEFAULT 0",
        ):
            try:
                await db.execute(migration)
                await db.commit()
            except Exception:
                pass

        # Дефолтные настройки
        for key, value in (
            ("referral_bonus", str(REFERRAL_BONUS)),
            ("premium_price_rub", str(PREMIUM_PRICE_RUB)),
        ):
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )
        await db.commit()

    await seed_workouts()
    logger.info("БД инициализирована")


async def seed_workouts() -> None:
    """Заполнить каталог тренировок при первом запуске."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM workout_catalog") as cur:
            count = (await cur.fetchone())[0]
        if count > 0:
            return

        workouts = [
            # ─── Девушки ───
            ("female", "glutes", "Приседания с собственным весом",
             "3 подхода по 15 раз, отдых 60 сек. Держи спину прямо, колени не выходят за носки.",
             15, 120, "🟢 Новичок"),
            ("female", "glutes", "Выпады на месте",
             "3 подхода по 12 раз на каждую ногу. Руки на поясе, спина ровная.",
             12, 100, "🟢 Новичок"),
            ("female", "glutes", "Ягодичный мостик",
             "4 подхода по 20 раз. Лёжа на спине, стопы на полу, поднимай таз вверх.",
             10, 90, "🟢 Новичок"),
            ("female", "glutes", "Махи ногой в сторону",
             "3 подхода по 15 раз на каждую ногу. Стоя на четвереньках.",
             8, 70, "🟢 Новичок"),
            ("female", "yoga", "Утренняя йога 15 минут",
             "Приветствие солнцу, поза кошки-коровы, поза ребёнка. Дыхание спокойное.",
             15, 60, "🟢 Новичок"),
            ("female", "yoga", "Растяжка всего тела",
             "Наклоны, скручивания, шпагат-подготовка. По 40 секунд на позу.",
             20, 70, "🟢 Новичок"),
            ("female", "yoga", "Йога для осанки",
             "Поза дерева, воина, кобры. Укрепляет спину и раскрывает плечи.",
             18, 65, "🟢 Новичок"),
            ("female", "cardio", "Быстрая ходьба 30 минут",
             "Темп 5–6 км/ч. Хорошо для жиросжигания и суставов.",
             30, 150, "🟢 Новичок"),
            ("female", "cardio", "Прыжки на скакалке",
             "3 подхода по 3 минуты, отдых 1 минута.",
             12, 140, "🟡 Средний"),
            ("female", "cardio", "Танцевальная разминка",
             "Любые активные движения под музыку. Просто танцуй 20 минут!",
             20, 130, "🟢 Новичок"),
            ("female", "abs", "Скручивания на пресс",
             "3 подхода по 20 раз. Руки за головой, не тяни шею.",
             10, 80, "🟢 Новичок"),
            ("female", "abs", "Планка",
             "3 подхода по 30 секунд. Держи тело прямой линией.",
             5, 50, "🟢 Новичок"),
            ("female", "abs", "Велосипед",
             "3 подхода по 30 секунд. Локтем тянись к противоположному колену.",
             8, 70, "🟡 Средний"),

            # ─── Мужчины ───
            ("male", "chest_arms", "Отжимания от пола",
             "4 подхода по 15 раз. Если тяжело — с колен.",
             12, 130, "🟢 Новичок"),
            ("male", "chest_arms", "Отжимания с узкой постановкой рук",
             "3 подхода по 12 раз. Прорабатывает трицепс.",
             10, 110, "🟡 Средний"),
            ("male", "chest_arms", "Обратные отжимания от стула",
             "3 подхода по 15 раз. Спиной к стулу, руки на краю.",
             10, 100, "🟢 Новичок"),
            ("male", "back_shoulders", "Подтягивания",
             "3 подхода по максимуму. Если не получается — с резинкой.",
             15, 140, "🔴 Сложный"),
            ("male", "back_shoulders", "Тяга в наклоне с бутылками",
             "3 подхода по 12 раз. Возьми бутылки с водой как гантели.",
             12, 110, "🟢 Новичок"),
            ("male", "back_shoulders", "Разведение рук в стороны",
             "3 подхода по 15 раз с бутылками. Прорабатывает плечи.",
             10, 80, "🟢 Новичок"),
            ("male", "back_shoulders", "Лодочка",
             "3 подхода по 20 секунд. Лёжа на животе, поднимай руки и ноги.",
             6, 55, "🟢 Новичок"),
            ("male", "cardio", "Бег на месте",
             "3 раунда по 5 минут, отдых 1 минута.",
             18, 180, "🟢 Новичок"),
            ("male", "cardio", "Прыжки на скакалке",
             "5 подходов по 3 минуты.",
             18, 200, "🟡 Средний"),
            ("male", "cardio", "Бёрпи",
             "4 подхода по 10 раз. Присед → планка → отжимание → прыжок.",
             10, 150, "🟡 Средний"),
            ("male", "legs", "Приседания",
             "4 подхода по 20 раз. Можно с рюкзаком для нагрузки.",
             15, 140, "🟢 Новичок"),
            ("male", "legs", "Выпады",
             "3 подхода по 15 раз на каждую ногу.",
             12, 120, "🟢 Новичок"),
            ("male", "legs", "Подъёмы на носки",
             "4 подхода по 25 раз. Стоя на книге или ступеньке.",
             8, 70, "🟢 Новичок"),
            ("male", "hiit", "Табата 4 минуты",
             "20 сек работы, 10 сек отдыха, 8 раундов. Упражнения: бёрпи, приседания, отжимания, прыжки.",
             10, 160, "🔴 Сложный"),
            ("male", "hiit", "Круговая тренировка",
             "5 упражнений по 45 сек с отдыхом 15 сек, 3 круга.",
             20, 250, "🟡 Средний"),
            ("male", "hiit", "HIIT для жиросжигания",
             "30 сек максимальной интенсивности, 30 сек отдыха, 10 раундов.",
             12, 180, "🟡 Средний"),
        ]

        for w in workouts:
            await db.execute(
                "INSERT INTO workout_catalog "
                "(gender, category, name, description, duration_min, calories, level) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                w,
            )
        await db.commit()
    logger.info("Каталог тренировок заполнен (%d шт.)", len(workouts))


# ─── Профили ───

async def get_profile(tg_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM profiles WHERE telegram_id = ?", (tg_id,)
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def create_profile(tg_id: int, username: str | None, full_name: str) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(
            "INSERT OR IGNORE INTO profiles (telegram_id, username, full_name, registered_at) "
            "VALUES (?, ?, ?, ?)",
            (tg_id, username, full_name, _now()),
        )
        await db.commit()
        async with db.execute(
            "SELECT * FROM profiles WHERE telegram_id = ?", (tg_id,)
        ) as cur:
            return dict(await cur.fetchone())


async def update_profile(tg_id: int, **fields) -> None:
    if not fields:
        return
    keys = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [tg_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE profiles SET {keys} WHERE telegram_id = ?", values)
        await db.commit()


def is_premium(p: dict | None) -> bool:
    if not p:
        return False
    until = p.get("premium_until")
    if not until:
        return False
    try:
        dt = datetime.fromisoformat(until)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt > datetime.now(timezone.utc)
    except (ValueError, TypeError):
        return False


async def grant_premium(tg_id: int, days: int) -> str:
    p = await get_profile(tg_id)
    now = datetime.now(timezone.utc)
    base = now
    if p and p.get("premium_until"):
        try:
            old = datetime.fromisoformat(p["premium_until"])
            if old.tzinfo is None:
                old = old.replace(tzinfo=timezone.utc)
            if old > now:
                base = old
        except (ValueError, TypeError):
            pass
    new_until = (base + timedelta(days=days)).isoformat()
    await update_profile(tg_id, premium_until=new_until)
    return new_until


async def revoke_premium(tg_id: int) -> None:
    await update_profile(tg_id, premium_until=None)


async def check_and_increment_usage(tg_id: int) -> tuple[bool, int]:
    p = await get_profile(tg_id)
    if not p:
        return False, 0
    if is_premium(p):
        return True, -1
    today = _today()
    used = p.get("free_used_today") or 0
    if p.get("free_used_date") != today:
        used = 0
        await update_profile(tg_id, free_used_today=0, free_used_date=today)
    if used >= FREE_DAILY_LIMIT:
        return False, 0
    new_used = used + 1
    await update_profile(tg_id, free_used_today=new_used, free_used_date=today)
    return True, FREE_DAILY_LIMIT - new_used


async def get_usage_today(tg_id: int) -> int:
    p = await get_profile(tg_id)
    if not p:
        return 0
    if p.get("free_used_date") != _today():
        return 0
    return p.get("free_used_today") or 0


# ─── Еда ───

async def add_meal(
    tg_id: int, meal_type: str, description: str,
    calories: float, protein: float, fat: float, carbs: float,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO meals (telegram_id, date, meal_type, description, calories, protein, fat, carbs, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (tg_id, _today(), meal_type, description, calories, protein, fat, carbs, _now()),
        )
        await db.commit()


async def get_meals_today(tg_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM meals WHERE telegram_id = ? AND date = ? ORDER BY created_at",
            (tg_id, _today()),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_meals_range(tg_id: int, days: int = 7) -> list[dict]:
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM meals WHERE telegram_id = ? AND date >= ? ORDER BY date",
            (tg_id, start),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def delete_last_meal(tg_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM meals WHERE telegram_id = ? AND date = ? "
            "ORDER BY created_at DESC LIMIT 1",
            (tg_id, _today()),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            await db.execute("DELETE FROM meals WHERE id = ?", (row["id"],))
            await db.commit()
            return dict(row)


async def add_weight(tg_id: int, weight: float) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO weights (telegram_id, weight, created_at) VALUES (?, ?, ?)",
            (tg_id, weight, _now()),
        )
        await db.commit()
    await update_profile(tg_id, weight_current=weight)


async def get_water_today(tg_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT glasses FROM water WHERE telegram_id = ? AND date = ?",
            (tg_id, _today()),
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def add_water(tg_id: int, delta: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT glasses FROM water WHERE telegram_id = ? AND date = ?",
            (tg_id, _today()),
        ) as cur:
            row = await cur.fetchone()
        current = row[0] if row else 0
        new = max(0, current + delta)
        if row:
            await db.execute(
                "UPDATE water SET glasses = ? WHERE telegram_id = ? AND date = ?",
                (new, tg_id, _today()),
            )
        else:
            await db.execute(
                "INSERT INTO water (telegram_id, date, glasses) VALUES (?, ?, ?)",
                (tg_id, _today(), new),
            )
        await db.commit()
        return new


# ─── Оплаты и статистика ───

async def create_stars_payment(tg_id: int, stars: int, charge_id: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO payments (telegram_id, amount, charge_id, status, created_at, reviewed_at) "
            "VALUES (?, ?, ?, 'paid', ?, ?)",
            (tg_id, stars, charge_id, _now(), _now()),
        )
        await db.commit()
        return cur.lastrowid


async def get_payment_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM payments WHERE status = 'paid'"
        ) as cur:
            count, total = await cur.fetchone()
    return {"count": count, "total": total or 0}


async def get_total_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM profiles") as cur:
            total = (await cur.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM profiles WHERE is_banned = 1"
        ) as cur:
            banned = (await cur.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM profiles WHERE premium_until > ?", (_now(),)
        ) as cur:
            premium = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM meals") as cur:
            meals = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM referrals") as cur:
            refs = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM workouts") as cur:
            workouts = (await cur.fetchone())[0]
    return {
        "total": total, "banned": banned, "premium": premium,
        "meals": meals, "refs": refs, "workouts": workouts,
    }


async def get_all_users(limit: int = 10, offset: int = 0) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM profiles ORDER BY registered_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_users_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM profiles") as cur:
            return (await cur.fetchone())[0]


async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT telegram_id FROM profiles WHERE is_banned = 0"
        ) as cur:
            return [row[0] for row in await cur.fetchall()]


# ─── Настройки ───

async def get_setting(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else default


async def set_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        await db.commit()


# ─── Баланс ───

async def add_balance(tg_id: int, amount: float) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE profiles SET balance = balance + ? WHERE telegram_id = ?",
            (amount, tg_id),
        )
        await db.commit()


# ─── Рефералы ───

async def set_referred_by(invited_id: int, inviter_id: int) -> bool:
    if inviter_id == invited_id:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT referred_by, onboarded FROM profiles WHERE telegram_id = ?",
            (invited_id,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return False
            referred_by, onboarded = row
            if referred_by is not None:
                return False
            if onboarded:
                return False
        await db.execute(
            "UPDATE profiles SET referred_by = ? WHERE telegram_id = ?",
            (inviter_id, invited_id),
        )
        try:
            await db.execute(
                "INSERT INTO referrals (inviter_id, invited_id, created_at) "
                "VALUES (?, ?, ?)",
                (inviter_id, invited_id, _now()),
            )
        except aiosqlite.IntegrityError:
            pass
        await db.commit()
        return True


async def pay_referral_bonus(invited_id: int) -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT referred_by, referral_paid FROM profiles WHERE telegram_id = ?",
            (invited_id,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            inviter_id = row["referred_by"]
            already_paid = row["referral_paid"]
        if not inviter_id or already_paid:
            return None
        bonus = float(await get_setting("referral_bonus", str(REFERRAL_BONUS)))
        await db.execute(
            "UPDATE profiles SET balance = balance + ?, "
            "referrals_count = referrals_count + 1, "
            "referrals_earned = referrals_earned + ? "
            "WHERE telegram_id = ?",
            (bonus, bonus, inviter_id),
        )
        await db.execute(
            "UPDATE profiles SET referral_paid = 1 WHERE telegram_id = ?",
            (invited_id,),
        )
        await db.commit()
        return inviter_id


async def get_referral_top(limit: int = 10) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT telegram_id, full_name, username, referrals_count, referrals_earned "
            "FROM profiles WHERE referrals_count > 0 "
            "ORDER BY referrals_count DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


# ─── Тренировки ───

async def get_workouts_by_gender(gender: str, category: str | None = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if category:
            async with db.execute(
                "SELECT * FROM workout_catalog "
                "WHERE gender IN (?, 'any') AND category = ? "
                "ORDER BY level, name",
                (gender, category),
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]
        async with db.execute(
            "SELECT * FROM workout_catalog "
            "WHERE gender IN (?, 'any') ORDER BY category, level",
            (gender,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_workout(workout_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM workout_catalog WHERE id = ?", (workout_id,)
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def log_workout(
    tg_id: int, workout_type: str, name: str,
    duration_min: int, calories: float,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO workouts (telegram_id, workout_type, name, duration_min, calories, completed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (tg_id, workout_type, name, duration_min, calories, _now()),
        )
        await db.commit()


async def get_workouts_range(tg_id: int, days: int = 7) -> list[dict]:
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM workouts WHERE telegram_id = ? AND DATE(completed_at) >= ? "
            "ORDER BY completed_at DESC",
            (tg_id, start),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_workouts_count_today(tg_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM workouts WHERE telegram_id = ? AND DATE(completed_at) = ?",
            (tg_id, _today()),
        ) as cur:
            return (await cur.fetchone())[0]

# ═══════════════════════════════════════════════════════════════
#  NSCALE СЕРВИС
# ═══════════════════════════════════════════════════════════════


class NscaleService:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://inference.api.nscale.com/v1"
        self.timeout = aiohttp.ClientTimeout(total=120)

    @property
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def chat(self, messages: list[dict], max_tokens: int = 2048) -> str:
        url = f"{self.base_url}/chat/completions"
        payload = {
            "model": "meta-llama/Llama-4-Scout-17B-16E-Instruct",
            "messages": messages,
            "temperature": 0.4,
            "max_tokens": max_tokens,
        }
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as s:
                async with s.post(url, headers=self._headers, json=payload) as r:
                    if r.status != 200:
                        err = await r.text()
                        logger.error("Nscale %s: %s", r.status, err)
                        raise RuntimeError(f"Nscale ({r.status}): {err[:200]}")
                    data = await r.json()
                    return data["choices"][0]["message"]["content"]
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.exception("Сеть Nscale")
            raise RuntimeError(f"Сеть Nscale: {type(e).__name__}") from e

    async def analyze_food_text(self, text: str) -> dict:
        system = (
            "Ты — нутрициолог. Проанализируй описание еды и верни ТОЛЬКО валидный JSON "
            "без markdown-обёрток со структурой:\n"
            '{"items":[{"name":"...","calories":число,"protein":число,'
            '"fat":число,"carbs":число}],'
            '"total":{"calories":число,"protein":число,"fat":число,"carbs":число},'
            '"comment":"короткий комментарий"}\n'
            "Все числа — в граммах и килокалориях для указанного количества."
        )
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": text},
        ]
        raw = await self.chat(messages, max_tokens=1024)
        return _parse_json_safe(raw)

    async def analyze_food_photo(self, image_base64: str, mime: str, caption: str) -> dict:
        system = (
            "Ты — нутрициолог. Распознай блюда на фото и верни ТОЛЬКО валидный JSON "
            "без markdown-обёрток со структурой:\n"
            '{"items":[{"name":"...","calories":число,"protein":число,'
            '"fat":число,"carbs":число}],'
            '"total":{"calories":число,"protein":число,"fat":число,"carbs":число},'
            '"comment":"короткий комментарий"}\n'
            "Оценивай стандартную порцию на фото."
        )
        text_part = caption or "Что на фото? Посчитай калории и БЖУ."
        messages = [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": text_part},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{image_base64}"},
                    },
                ],
            },
        ]
        raw = await self.chat(messages, max_tokens=1024)
        return _parse_json_safe(raw)

    async def advice(self, profile: dict, meals: list[dict], water: int, workouts: list[dict]) -> str:
        total_cal = sum(m["calories"] for m in meals)
        burned = sum(w["calories"] for w in workouts)
        goal = profile.get("calorie_goal") or 2000
        context = (
            f"Цель: {goal} ккал.\n"
            f"Съедено сегодня: {total_cal:.0f} ккал.\n"
            f"Сожжено на тренировках: {burned:.0f} ккал.\n"
            f"Вода: {water} стаканов.\n"
        )
        if meals:
            context += "Приёмы пищи:\n" + "\n".join(
                f"- {m['description']} ({m['calories']:.0f} ккал)" for m in meals
            )
        if workouts:
            context += "\nТренировки:\n" + "\n".join(
                f"- {w['name']} ({w['duration_min']} мин, {w['calories']:.0f} ккал)" for w in workouts
            )
        system = (
            "Ты — дружелюбный нутрициолог и фитнес-тренер. Дай короткий (3–5 предложений) "
            "персональный совет. Конкретные советы, без воды."
        )
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": context},
        ]
        return await self.chat(messages, max_tokens=512)


def _parse_json_safe(raw: str) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Не удалось распарсить JSON: %s", raw[:200])
        return {
            "items": [],
            "total": {"calories": 0, "protein": 0, "fat": 0, "carbs": 0},
            "comment": raw[:300],
        }


gemini = NscaleService(NSCALE_SERVICE_TOKEN)


# ═══════════════════════════════════════════════════════════════
#  РАСЧЁТЫ
# ═══════════════════════════════════════════════════════════════


def calc_calorie_goal(profile: dict) -> int:
    sex = profile.get("sex") or "male"
    age = profile.get("age") or 30
    height = profile.get("height") or 170
    weight = profile.get("weight_current") or profile.get("weight_start") or 70
    activity = profile.get("activity") or 1.375
    if sex == "male":
        bmr = 10 * weight + 6.25 * height - 5 * age + 5
    else:
        bmr = 10 * weight + 6.25 * height - 5 * age - 161
    return max(1200, int(bmr * activity * 0.85))


def calc_bmi(weight: float, height_cm: float) -> tuple[float, str]:
    if not height_cm:
        return 0.0, "—"
    bmi = weight / ((height_cm / 100) ** 2)
    if bmi < 18.5:
        cat = "Недостаточный вес"
    elif bmi < 25:
        cat = "Норма"
    elif bmi < 30:
        cat = "Избыточный вес"
    else:
        cat = "Ожирение"
    return bmi, cat


# ═══════════════════════════════════════════════════════════════
#  ФОРМАТТЕРЫ
# ═══════════════════════════════════════════════════════════════


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_main_menu(p: dict, used: int) -> str:
    if is_premium(p):
        status = f"💎 Premium до {p['premium_until'][:10]}"
    else:
        status = f"🆓 Бесплатно (осталось {max(0, FREE_DAILY_LIMIT - used)}/{FREE_DAILY_LIMIT})"
    balance = p.get("balance", 0.0) or 0.0
    return (
        f"🥗 <b>FitBot — твой помощник в похудении</b>\n"
        f"{SEP}\n"
        f"📸 Фото еды — считаю калории\n"
        f"⚖️ Вес, 💧 вода, 💡 советы\n"
        f"🏋️ Тренировки для девушек и мужчин\n"
        f"👥 Приглашай друзей — зарабатывай ₽\n"
        f"{SEP}\n"
        f"Статус: <b>{status}</b>\n"
        f"💰 Баланс: <b>{balance:.0f} ₽</b>\n"
        f"{SEP}\n"
        f"Выбери действие:"
    )


def fmt_profile(p: dict, used: int) -> str:
    weight = p.get("weight_current") or p.get("weight_start") or 0
    bmi, cat = calc_bmi(weight, p.get("height") or 0)
    target = p.get("weight_target") or 0
    start = p.get("weight_start") or 0
    delta = weight - target if target else 0

    if is_premium(p):
        sub_line = f"💎 <b>Premium</b> до <i>{p['premium_until'][:10]}</i>"
    else:
        left = max(0, FREE_DAILY_LIMIT - used)
        sub_line = f"🆓 <b>Бесплатный</b> (осталось {left}/{FREE_DAILY_LIMIT})"

    balance = p.get("balance", 0.0) or 0.0
    ref_count = p.get("referrals_count", 0) or 0
    ref_earned = p.get("referrals_earned", 0.0) or 0.0

    return (
        f"👤 <b>Твой профиль</b>\n"
        f"{SEP}\n"
        f"📛 Имя: <b>{escape_html(p.get('full_name') or '—')}</b>\n"
        f"⚖️ Вес: <b>{weight:.1f} кг</b> (старт {start:.1f}, цель {target:.1f})\n"
        f"📏 Рост: <b>{p.get('height') or '—'} см</b>\n"
        f"🎂 Возраст: <b>{p.get('age') or '—'}</b>\n"
        f"{SEP}\n"
        f"🎯 Калорий в день: <b>{p.get('calorie_goal') or '—'} ккал</b>\n"
        f"📊 ИМТ: <b>{bmi:.1f}</b> — <i>{cat}</i>\n"
        f"📉 До цели: <b>{delta:+.1f} кг</b>\n"
        f"{SEP}\n"
        f"💰 Баланс: <b>{balance:.0f} ₽</b>\n"
        f"👥 Приглашено: <b>{ref_count}</b> · заработано: <b>{ref_earned:.0f} ₽</b>\n"
        f"Подписка: {sub_line}\n"
        f"{SEP}"
    )


def fmt_today_menu(meals: list[dict], water: int, goal: int, workouts: list[dict]) -> str:
    total_cal = sum(m["calories"] for m in meals)
    total_prot = sum(m["protein"] for m in meals)
    total_fat = sum(m["fat"] for m in meals)
    total_carb = sum(m["carbs"] for m in meals)
    burned = sum(w["calories"] for w in workouts)
    effective = total_cal - burned
    left = goal - effective

    lines = ["🍽 <b>Дневник на сегодня</b>", SEP]
    if not meals:
        lines.append("Пока пусто. Отправь фото еды или напиши, что съел.")
    else:
        for m in meals:
            icon = {"breakfast": "🌅", "lunch": "☀️", "dinner": "🌙", "snack": "🍎"}.get(
                m.get("meal_type", ""), "🍽"
            )
            lines.append(
                f"{icon} {escape_html(m['description'][:60])} — <b>{m['calories']:.0f} ккал</b>"
            )
    lines.append(SEP)
    lines.append(
        f"🔥 Съедено: <b>{total_cal:.0f}</b> / {goal} ккал\n"
        f"Б: <b>{total_prot:.0f}</b> · Ж: <b>{total_fat:.0f}</b> · У: <b>{total_carb:.0f}</b>"
    )
    if burned > 0:
        lines.append(f"🏋️ Сожжено: <b>-{burned:.0f} ккал</b>")
        lines.append(f"⚖️ Итого: <b>{effective:.0f} ккал</b>")
    if left >= 0:
        lines.append(f"✅ Осталось: <b>{left:.0f} ккал</b>")
    else:
        lines.append(f"⚠️ Перебор: <b>{-left:.0f} ккал</b>")
    lines.append(f"💧 Вода: <b>{water}</b> стаканов")
    lines.append(SEP)
    return "\n".join(lines)


def fmt_food_result(data: dict) -> str:
    total = data.get("total", {})
    items = data.get("items", [])
    comment = data.get("comment", "")
    lines = ["🍽 <b>Разбор еды</b>", SEP]
    for it in items:
        lines.append(
            f"• <b>{escape_html(it.get('name', '—'))}</b> — "
            f"{it.get('calories', 0):.0f} ккал "
            f"(Б {it.get('protein', 0):.0f} / Ж {it.get('fat', 0):.0f} / У {it.get('carbs', 0):.0f})"
        )
    lines.append(SEP)
    lines.append(
        f"🔥 <b>Итого: {total.get('calories', 0):.0f} ккал</b>\n"
        f"Б: <b>{total.get('protein', 0):.0f}</b> · Ж: <b>{total.get('fat', 0):.0f}</b> · "
        f"У: <b>{total.get('carbs', 0):.0f}</b>"
    )
    if comment:
        lines.append(SEP)
        lines.append(f"💬 <i>{escape_html(comment)}</i>")
    return "\n".join(lines)


def fmt_stats(profile: dict, meals: list[dict]) -> str:
    if not meals:
        return "📊 <b>Статистика</b>\nПока нет данных."
    by_date: dict[str, list[dict]] = {}
    for m in meals:
        by_date.setdefault(m["date"], []).append(m)
    lines = [f"📊 <b>Статистика за {len(by_date)} дней</b>", SEP]
    for date in sorted(by_date.keys())[-7:]:
        total = sum(m["calories"] for m in by_date[date])
        goal = profile.get("calorie_goal") or 2000
        emoji = "✅" if total <= goal else "⚠️"
        lines.append(f"{emoji} <b>{date}</b>: {total:.0f} ккал (цель {goal})")
    lines.append(SEP)
    avg = sum(sum(m["calories"] for m in v) for v in by_date.values()) / len(by_date)
    lines.append(f"📈 Средний дневной калораж: <b>{avg:.0f} ккал</b>")
    return "\n".join(lines)


def fmt_premium_info(price_rub: int) -> str:
    return (
        f"💎 <b>Premium подписка FitBot</b>\n"
        f"{SEP}\n"
        f"Что даёт:\n"
        f"📸 <b>Безлимитные</b> анализы еды\n"
        f"🏋️ <b>Безлимитные</b> тренировки\n"
        f"💡 Неограниченные советы\n"
        f"⚡ Приоритетная обработка\n"
        f"{SEP}\n"
        f"💰 Стоимость: <b>{SUBSCRIPTION_STARS} ⭐</b> или <b>{price_rub} ₽</b>\n"
        f"📅 Срок: <b>{SUBSCRIPTION_DAYS} дней</b>\n"
        f"{SEP}\n"
        f"<i>Оплатить можно звёздами Telegram или с баланса (заработай на рефералах).</i>"
    )


def fmt_referral_page(p: dict) -> str:
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{p['telegram_id']}"
    ref_count = p.get("referrals_count", 0) or 0
    ref_earned = p.get("referrals_earned", 0.0) or 0.0
    balance = p.get("balance", 0.0) or 0.0

    return (
        f"👥 <b>Пригласи друга — заработай ₽</b>\n"
        f"{SEP}\n"
        f"За каждого друга, который перейдёт по твоей ссылке и "
        f"<b>завершит регистрацию</b>, ты получишь деньги на баланс.\n\n"
        f"Эти рубли можно потратить на <b>Premium-подписку</b>.\n"
        f"{SEP}\n"
        f"🔗 <b>Твоя ссылка:</b>\n"
        f"<code>{ref_link}</code>\n"
        f"{SEP}\n"
        f"👥 Приглашено: <b>{ref_count}</b>\n"
        f"📈 Заработано: <b>{ref_earned:.0f} ₽</b>\n"
        f"💰 Баланс: <b>{balance:.0f} ₽</b>\n"
        f"{SEP}\n"
        f"<i>Нажми на ссылку, чтобы скопировать.</i>"
    )


def fmt_referral_top(items: list[dict]) -> str:
    if not items:
        return "🏆 <b>Топ приглашающих</b>\n\nПока никого нет."
    lines = ["🏆 <b>Топ приглашающих</b>", SEP]
    medals = ["🥇", "🥈", "🥉"]
    for i, u in enumerate(items):
        medal = medals[i] if i < 3 else f"{i+1}."
        name = escape_html(u.get("full_name") or "—")
        lines.append(
            f"{medal} {name} — <b>{u['referrals_count']}</b> друзей "
            f"(<b>{u['referrals_earned']:.0f} ₽</b>)"
        )
    return "\n".join(lines)


# ─── Тренировки ───

def fmt_workouts_menu() -> str:
    return (
        f"🏋️ <b>Тренировки</b>\n"
        f"{SEP}\n"
        f"Выбери раздел:\n\n"
        f"👩 <b>Для девушек</b> — ягодицы, йога, пресс, кардио\n"
        f"👨 <b>Для мужчин</b> — грудь, спина, ноги, HIIT\n"
        f"🎯 <b>По цели</b> — подберу случайную тренировку\n"
        f"📊 <b>Мои тренировки</b> — статистика за неделю\n"
        f"{SEP}\n"
        f"🆓 Бесплатно: <b>{WORKOUT_FREE_DAILY} тренировка в день</b>\n"
        f"💎 Premium: безлимит"
    )


def fmt_workouts_list(items: list[dict], label: str) -> str:
    if not items:
        return f"🏋️ <b>{label}</b>\n\nПока пусто."
    lines = [f"🏋️ <b>{label}</b>\n{SEP}"]
    for w in items:
        lines.append(
            f"• <b>{escape_html(w['name'])}</b>\n"
            f"  {w['level']} · ⏱ {w['duration_min']} мин · 🔥 ~{w['calories']:.0f} ккал"
        )
    return "\n".join(lines)


def fmt_workout_card(w: dict) -> str:
    return (
        f"🏋️ <b>{escape_html(w['name'])}</b>\n"
        f"{SEP}\n"
        f"Уровень: {w['level']}\n"
        f"⏱ Длительность: <b>{w['duration_min']} мин</b>\n"
        f"🔥 Сожжёт: <b>~{w['calories']:.0f} ккал</b>\n"
        f"{SEP}\n"
        f"<i>{escape_html(w['description'] or '')}</i>\n"
        f"{SEP}"
    )


def fmt_workouts_stats(items: list[dict]) -> str:
    if not items:
        return (
            f"📊 <b>Мои тренировки</b>\n{SEP}\n"
            f"Пока нет записей. Начни с раздела «🏋️ Тренировки»!"
        )
    total_cal = sum(w["calories"] for w in items)
    total_min = sum(w["duration_min"] for w in items)
    lines = [f"📊 <b>Мои тренировки (7 дней)</b>\n{SEP}"]
    for w in items[:15]:
        try:
            dt = datetime.fromisoformat(w["completed_at"]).strftime("%d.%m %H:%M")
        except (ValueError, TypeError):
            dt = "—"
        lines.append(f"✅ {dt} · {escape_html(w['name'])} — {w['duration_min']} мин")
    lines.append(SEP)
    lines.append(
        f"🔥 Всего тренировок: <b>{len(items)}</b>\n"
        f"⏱ Общее время: <b>{total_min} мин</b>\n"
        f"🔥 Сожжено: <b>{total_cal:.0f} ккал</b>"
    )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════


def kb_main(p: dict | None = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🍽 Дневник на сегодня", callback_data="today")],
        [
            InlineKeyboardButton(text="📸 Как считать?", callback_data="how_to"),
            InlineKeyboardButton(text="💧 +1 вода", callback_data="add_water"),
        ],
        [
            InlineKeyboardButton(text="⚖️ Записать вес", callback_data="add_weight"),
            InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
        ],
        [InlineKeyboardButton(text="🏋️ Тренировки", callback_data="workouts")],
        [
            InlineKeyboardButton(text="💡 Совет дня", callback_data="advice"),
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
        ],
        [InlineKeyboardButton(text="👥 Пригласить друга (+₽)", callback_data="referral")],
    ]
    if p and not is_premium(p):
        rows.append([InlineKeyboardButton(
            text=f"💎 Купить Premium ({SUBSCRIPTION_STARS} ⭐)",
            callback_data="buy_premium",
        )])
    if p and is_admin(p["telegram_id"]):
        rows.append([InlineKeyboardButton(text="🛡 Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_today() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💧 +1 вода", callback_data="add_water")],
        [InlineKeyboardButton(text="🗑 Удалить последнюю еду", callback_data="del_last")],
        [InlineKeyboardButton(text="💡 Совет дня", callback_data="advice")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
    ])


def kb_back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
    ])


def kb_onboard_sex() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👨 Мужской", callback_data="ob_sex:male"),
            InlineKeyboardButton(text="👩 Женский", callback_data="ob_sex:female"),
        ],
    ])


def kb_onboard_activity() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛋 Минимум (сидячий)", callback_data="ob_act:1.2")],
        [InlineKeyboardButton(text="🚶 Лёгкая (1–3 трен/нед)", callback_data="ob_act:1.375")],
        [InlineKeyboardButton(text="🏃 Средняя (3–5 трен/нед)", callback_data="ob_act:1.55")],
        [InlineKeyboardButton(text="💪 Высокая (6–7 трен/нед)", callback_data="ob_act:1.725")],
    ])


def kb_goal_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data="ob_accept")],
        [InlineKeyboardButton(text="🔄 Заново", callback_data="ob_restart")],
    ])


def kb_premium(p: dict | None, price_rub: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(
            text=f"⭐ Оплатить {SUBSCRIPTION_STARS} звёзд",
            callback_data="pay_stars",
        )],
    ]
    if p:
        balance = p.get("balance", 0.0) or 0.0
        if balance >= price_rub:
            rows.append([InlineKeyboardButton(
                text=f"💰 Оплатить с баланса ({price_rub} ₽)",
                callback_data="pay_balance",
            )])
        else:
            need = price_rub - int(balance)
            rows.append([InlineKeyboardButton(
                text=f"💰 Не хватает {need} ₽",
                callback_data="no_money",
            )])
    rows.append([InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_referral() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Топ приглашающих", callback_data="ref_top")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
    ])


# ─── Тренировки: клавиатуры ───

def kb_workouts_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👩 Для девушек", callback_data="w_gender:female")],
        [InlineKeyboardButton(text="👨 Для мужчин", callback_data="w_gender:male")],
        [InlineKeyboardButton(text="🎯 Подобрать по цели", callback_data="w_by_goal")],
        [InlineKeyboardButton(text="📊 Мои тренировки", callback_data="w_stats")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
    ])


def kb_workout_categories(gender: str) -> InlineKeyboardMarkup:
    if gender == "female":
        rows = [
            [InlineKeyboardButton(text="🍑 Ягодицы и ноги", callback_data=f"w_cat:{gender}:glutes")],
            [InlineKeyboardButton(text="🧘 Йога и растяжка", callback_data=f"w_cat:{gender}:yoga")],
            [InlineKeyboardButton(text="🔥 Кардио", callback_data=f"w_cat:{gender}:cardio")],
            [InlineKeyboardButton(text="💪 Пресс и талия", callback_data=f"w_cat:{gender}:abs")],
        ]
    else:
        rows = [
            [InlineKeyboardButton(text="💪 Грудь и руки", callback_data=f"w_cat:{gender}:chest_arms")],
            [InlineKeyboardButton(text="🏋️ Спина и плечи", callback_data=f"w_cat:{gender}:back_shoulders")],
            [InlineKeyboardButton(text="🔥 Кардио", callback_data=f"w_cat:{gender}:cardio")],
            [InlineKeyboardButton(text="🦵 Ноги", callback_data=f"w_cat:{gender}:legs")],
            [InlineKeyboardButton(text="⚡ HIIT / кроссфит", callback_data=f"w_cat:{gender}:hiit")],
        ]
    rows.append([InlineKeyboardButton(text="📋 Все тренировки", callback_data=f"w_all:{gender}")])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="workouts")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_workout_list(items: list[dict], gender: str, category: str | None = None) -> InlineKeyboardMarkup:
    rows = []
    for w in items[:20]:
        rows.append([InlineKeyboardButton(
            text=f"{w['level'].split()[0]} {w['name'][:35]}",
            callback_data=f"w_open:{w['id']}",
        )])
    back_cb = f"w_gender:{gender}" if category else "workouts"
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_workout_card(workout_id: int, gender: str, category: str | None) -> InlineKeyboardMarkup:
    back_cb = f"w_cat:{gender}:{category}" if category else f"w_gender:{gender}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отметить выполненной", callback_data=f"w_done:{workout_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=back_cb)],
    ])


# ─── Админ ───

def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm_users:0")],
        [InlineKeyboardButton(text="🎁 Выдать Premium", callback_data="adm_grant")],
        [InlineKeyboardButton(text="💰 Начислить баланс", callback_data="adm_addbal")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="adm_settings")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
    ])


def kb_admin_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Сумма за реферала", callback_data="adm_set:referral_bonus")],
        [InlineKeyboardButton(text="💎 Цена Premium (₽)", callback_data="adm_set:premium_price_rub")],
        [InlineKeyboardButton(text="🔙 В админку", callback_data="admin")],
    ])


def kb_admin_user_card(tg_id: int, banned: bool, premium: bool) -> InlineKeyboardMarkup:
    ban_btn = (
        InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm_unban:{tg_id}")
        if banned
        else InlineKeyboardButton(text="🚫 Забанить", callback_data=f"adm_ban:{tg_id}")
    )
    prem_btn = (
        InlineKeyboardButton(text="❌ Снять Premium", callback_data=f"adm_revoke:{tg_id}")
        if premium
        else InlineKeyboardButton(text="🎁 Выдать Premium", callback_data=f"adm_prem:{tg_id}")
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [prem_btn],
        [InlineKeyboardButton(text="💰 Начислить баланс", callback_data=f"adm_bal:{tg_id}")],
        [ban_btn],
        [InlineKeyboardButton(text="🔙 К списку", callback_data="adm_users:0")],
    ])


def kb_admin_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin")],
    ])


def kb_back_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В админку", callback_data="admin")],
    ])

# ═══════════════════════════════════════════════════════════════
#  ХЕНДЛЕРЫ
# ═══════════════════════════════════════════════════════════════

router = Router()


class Onboarding(StatesGroup):
    sex = State()
    age = State()
    height = State()
    weight_start = State()
    weight_target = State()
    activity = State()
    confirm = State()


class WeightInput(StatesGroup):
    waiting_weight = State()


class AdminStates(StatesGroup):
    waiting_user_id_for_premium = State()
    waiting_days_for_premium = State()
    waiting_user_id_for_balance = State()
    waiting_amount_for_balance = State()
    waiting_broadcast_text = State()
    waiting_setting_value = State()


# ─── /start ───

@router.message(CommandStart(deep_link=True))
async def cmd_start_ref(message: Message, command: CommandObject, state: FSMContext) -> None:
    args = command.args or ""
    inviter_id: int | None = None
    if args.startswith("ref_"):
        try:
            inviter_id = int(args[4:])
        except ValueError:
            inviter_id = None

    u = message.from_user
    await state.clear()
    p = await create_profile(u.id, u.username, u.full_name)

    if inviter_id and inviter_id != u.id:
        linked = await set_referred_by(u.id, inviter_id)
        if linked:
            logger.info("Пользователь %s привязан к %s", u.id, inviter_id)

    if p.get("is_banned"):
        await message.answer("🚫 Вы забанены в этом боте.")
        return

    if not p.get("onboarded"):
        await message.answer(
            f"👋 Привет, <b>{escape_html(u.full_name)}</b>!\n"
            f"{SEP}\n"
            f"Я — <b>FitBot</b>, помогу тебе похудеть.\n\n"
            f"📸 Фото еды — считаю калории\n"
            f"🏋️ Тренировки для девушек и мужчин\n"
            f"⚖️ Вес, 💧 вода, 💡 советы\n"
            f"👥 Приглашай друзей — зарабатывай ₽\n"
            f"{SEP}\n"
            f"Ответь на 6 вопросов — 1 минута.",
            parse_mode="HTML",
        )
        await asyncio.sleep(1)
        await message.answer("1️⃣ <b>Твой пол?</b>", reply_markup=kb_onboard_sex(), parse_mode="HTML")
        await state.set_state(Onboarding.sex)
        return

    used = await get_usage_today(u.id)
    await message.answer(fmt_main_menu(p, used), reply_markup=kb_main(p), parse_mode="HTML")


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    u = message.from_user
    p = await create_profile(u.id, u.username, u.full_name)

    if p.get("is_banned"):
        await message.answer("🚫 Вы забанены в этом боте.")
        return

    if not p.get("onboarded"):
        await message.answer(
            f"👋 Привет, <b>{escape_html(u.full_name)}</b>!\n"
            f"{SEP}\n"
            f"Я — <b>FitBot</b>, помогу тебе похудеть.\n\n"
            f"📸 Фото еды — считаю калории\n"
            f"🏋️ Тренировки для девушек и мужчин\n"
            f"⚖️ Вес, 💧 вода, 💡 советы\n"
            f"👥 Приглашай друзей — зарабатывай ₽\n"
            f"{SEP}\n"
            f"Ответь на 6 вопросов — 1 минута.",
            parse_mode="HTML",
        )
        await asyncio.sleep(1)
        await message.answer("1️⃣ <b>Твой пол?</b>", reply_markup=kb_onboard_sex(), parse_mode="HTML")
        await state.set_state(Onboarding.sex)
        return

    used = await get_usage_today(u.id)
    await message.answer(fmt_main_menu(p, used), reply_markup=kb_main(p), parse_mode="HTML")


# ─── Онбординг ───

@router.callback_query(Onboarding.sex, F.data.startswith("ob_sex:"))
async def ob_sex(cb: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(sex=cb.data.split(":", 1)[1])
    await cb.message.edit_text(
        "2️⃣ <b>Сколько тебе лет?</b>\n\nНапиши числом, например: <code>25</code>",
        parse_mode="HTML",
    )
    await state.set_state(Onboarding.age)
    await cb.answer()


@router.message(Onboarding.age)
async def ob_age(message: Message, state: FSMContext) -> None:
    try:
        age = int((message.text or "").strip())
        if not (10 <= age <= 100):
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи возраст числом 10–100.")
        return
    await state.update_data(age=age)
    await message.answer("3️⃣ <b>Твой рост в см?</b>\n\nНапример: <code>175</code>", parse_mode="HTML")
    await state.set_state(Onboarding.height)


@router.message(Onboarding.height)
async def ob_height(message: Message, state: FSMContext) -> None:
    try:
        height = float((message.text or "").replace(",", ".").strip())
        if not (120 <= height <= 230):
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи рост 120–230 см.")
        return
    await state.update_data(height=height)
    await message.answer("4️⃣ <b>Текущий вес в кг?</b>\n\nНапример: <code>82.5</code>", parse_mode="HTML")
    await state.set_state(Onboarding.weight_start)


@router.message(Onboarding.weight_start)
async def ob_weight_start(message: Message, state: FSMContext) -> None:
    try:
        w = float((message.text or "").replace(",", ".").strip())
        if not (30 <= w <= 250):
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи вес 30–250 кг.")
        return
    await state.update_data(weight_start=w)
    await message.answer("5️⃣ <b>Целевой вес в кг?</b>\n\nНапример: <code>75</code>", parse_mode="HTML")
    await state.set_state(Onboarding.weight_target)


@router.message(Onboarding.weight_target)
async def ob_weight_target(message: Message, state: FSMContext) -> None:
    try:
        w = float((message.text or "").replace(",", ".").strip())
        if not (30 <= w <= 250):
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи вес 30–250 кг.")
        return
    await state.update_data(weight_target=w)
    await message.answer("6️⃣ <b>Твоя активность?</b>", reply_markup=kb_onboard_activity(), parse_mode="HTML")
    await state.set_state(Onboarding.activity)


@router.callback_query(Onboarding.activity, F.data.startswith("ob_act:"))
async def ob_activity(cb: CallbackQuery, state: FSMContext) -> None:
    act = float(cb.data.split(":", 1)[1])
    data = await state.get_data()
    data["activity"] = act
    await state.update_data(activity=act)

    goal = calc_calorie_goal({
        "sex": data["sex"], "age": data["age"], "height": data["height"],
        "weight_current": data["weight_start"], "activity": act,
    })
    await state.update_data(calorie_goal=goal)

    bmi, cat = calc_bmi(data["weight_start"], data["height"])
    delta = data["weight_start"] - data["weight_target"]

    await cb.message.edit_text(
        f"🎯 <b>Твоя норма на день</b>\n{SEP}\n"
        f"⚖️ Старт: <b>{data['weight_start']:.1f} кг</b>\n"
        f"🎯 Цель: <b>{data['weight_target']:.1f} кг</b>\n"
        f"📉 Сбросить: <b>{delta:.1f} кг</b>\n{SEP}\n"
        f"📊 ИМТ: <b>{bmi:.1f}</b> — <i>{cat}</i>\n"
        f"🔥 Калорий: <b>{goal} ккал/день</b>\n"
        f"   <i>(дефицит 15% для похудения)</i>\n{SEP}\n"
        f"Всё верно?",
        reply_markup=kb_goal_confirm(),
        parse_mode="HTML",
    )
    await state.set_state(Onboarding.confirm)
    await cb.answer()


@router.callback_query(Onboarding.confirm, F.data == "ob_accept")
async def ob_accept(cb: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await update_profile(
        cb.from_user.id,
        sex=data["sex"], age=data["age"], height=data["height"],
        weight_start=data["weight_start"], weight_current=data["weight_start"],
        weight_target=data["weight_target"], activity=data["activity"],
        calorie_goal=data["calorie_goal"], onboarded=1,
    )
    await state.clear()

    inviter_id = await pay_referral_bonus(cb.from_user.id)
    if inviter_id:
        bonus = await get_setting("referral_bonus", str(REFERRAL_BONUS))
        try:
            await cb.bot.send_message(
                inviter_id,
                f"🎉 <b>+{bonus} ₽ на баланс!</b>\n{SEP}\n"
                f"По твоей ссылке зарегистрировался новый пользователь.\n"
                f"Баланс можно потратить на Premium-подписку.\n{SEP}",
                parse_mode="HTML",
            )
        except Exception:
            pass

    p = await get_profile(cb.from_user.id)
    await cb.message.edit_text(
        f"✅ <b>Профиль готов!</b>\n{SEP}\n"
        f"Отправляй фото еды или пиши, что съел.\n"
        f"🆓 Бесплатно: <b>{FREE_DAILY_LIMIT} анализа в день</b>.\n"
        f"🏋️ Тренировки: <b>{WORKOUT_FREE_DAILY} в день</b>.\n"
        f"👥 Приглашай друзей — зарабатывай ₽ на Premium!\n{SEP}",
        reply_markup=kb_main(p),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(Onboarding.confirm, F.data == "ob_restart")
async def ob_restart(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await cb.message.edit_text("1️⃣ <b>Твой пол?</b>", reply_markup=kb_onboard_sex(), parse_mode="HTML")
    await state.set_state(Onboarding.sex)
    await cb.answer()


# ─── Меню ───

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    used = await get_usage_today(cb.from_user.id)
    await cb.message.edit_text(
        fmt_main_menu(p, used), reply_markup=kb_main(p), parse_mode="HTML"
    )
    await cb.answer()


@router.callback_query(F.data == "profile")
async def cb_profile(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    used = await get_usage_today(cb.from_user.id)
    await cb.message.edit_text(
        fmt_profile(p, used), reply_markup=kb_back_main(), parse_mode="HTML"
    )
    await cb.answer()


@router.callback_query(F.data == "how_to")
async def cb_how_to(cb: CallbackQuery) -> None:
    await cb.message.edit_text(
        f"📸 <b>Как считать калории</b>\n{SEP}\n"
        f"<b>Фото</b>: сфотографируй тарелку и отправь боту.\n"
        f"<b>Текст</b>: напиши, что съел — «2 яйца и тост».\n\n"
        f"После расчёта запись попадёт в дневник.\n{SEP}\n"
        f"🆓 Бесплатно: <b>{FREE_DAILY_LIMIT} анализа в день</b>\n"
        f"💎 Premium: безлимит за <b>{SUBSCRIPTION_STARS} ⭐</b>",
        reply_markup=kb_back_main(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "today")
async def cb_today(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    meals = await get_meals_today(cb.from_user.id)
    water = await get_water_today(cb.from_user.id)
    workouts = await get_workouts_range(cb.from_user.id, days=1)
    goal = p.get("calorie_goal") or 2000
    await cb.message.edit_text(
        fmt_today_menu(meals, water, goal, workouts),
        reply_markup=kb_today(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "add_water")
async def cb_add_water(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    glasses = await add_water(cb.from_user.id, 1)
    await cb.answer(f"💧 +1 вода — теперь {glasses}/{p.get('water_goal') or 8}")
    meals = await get_meals_today(cb.from_user.id)
    workouts = await get_workouts_range(cb.from_user.id, days=1)
    goal = p.get("calorie_goal") or 2000
    await cb.message.edit_text(
        fmt_today_menu(meals, glasses, goal, workouts),
        reply_markup=kb_today(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "del_last")
async def cb_del_last(cb: CallbackQuery) -> None:
    meal = await delete_last_meal(cb.from_user.id)
    if not meal:
        await cb.answer("Нечего удалять", show_alert=True)
        return
    await cb.answer(f"🗑 Удалено: {meal['description'][:30]}")
    p = await get_profile(cb.from_user.id)
    meals = await get_meals_today(cb.from_user.id)
    water = await get_water_today(cb.from_user.id)
    workouts = await get_workouts_range(cb.from_user.id, days=1)
    goal = (p or {}).get("calorie_goal") or 2000
    await cb.message.edit_text(
        fmt_today_menu(meals, water, goal, workouts),
        reply_markup=kb_today(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    meals = await get_meals_range(cb.from_user.id, days=7)
    await cb.message.edit_text(
        fmt_stats(p, meals), reply_markup=kb_back_main(), parse_mode="HTML"
    )
    await cb.answer()


@router.callback_query(F.data == "advice")
async def cb_advice(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    allowed, _ = await check_and_increment_usage(cb.from_user.id)
    if not allowed:
        await cb.answer(
            f"🆓 Бесплатный лимит исчерпан ({FREE_DAILY_LIMIT}/день).",
            show_alert=True,
        )
        return
    wait_msg = await cb.message.edit_text(
        f"💡 <b>Совет дня</b>\n{SEP}\n<i>Думаю…</i>", parse_mode="HTML"
    )
    meals = await get_meals_today(cb.from_user.id)
    water = await get_water_today(cb.from_user.id)
    workouts = await get_workouts_range(cb.from_user.id, days=1)
    try:
        text = await gemini.advice(p, meals, water, workouts)
    except RuntimeError as e:
        await wait_msg.edit_text(
            f"❌ <b>Ошибка</b>\n{SEP}\n<i>{escape_html(str(e))}</i>",
            parse_mode="HTML",
            reply_markup=kb_back_main(),
        )
        return
    await wait_msg.edit_text(
        f"💡 <b>Совет дня</b>\n{SEP}\n{escape_html(text)}\n{SEP}",
        parse_mode="HTML",
        reply_markup=kb_back_main(),
    )


# ─── Вес ───

@router.callback_query(F.data == "add_weight")
async def cb_add_weight(cb: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(WeightInput.waiting_weight)
    await cb.message.edit_text(
        f"⚖️ <b>Запись веса</b>\n{SEP}\n"
        f"Отправь текущий вес, например: <code>81.5</code>\n{SEP}",
        reply_markup=kb_back_main(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(WeightInput.waiting_weight)
async def msg_add_weight(message: Message, state: FSMContext) -> None:
    try:
        weight = float((message.text or "").replace(",", ".").strip())
        if not (30 <= weight <= 250):
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи вес 30–250 кг.")
        return
    await add_weight(message.from_user.id, weight)
    await state.clear()
    p = await get_profile(message.from_user.id)
    target = p.get("weight_target") or 0
    delta = weight - target if target else 0
    await message.answer(
        f"✅ <b>Вес записан</b>\n{SEP}\n"
        f"⚖️ Текущий: <b>{weight:.1f} кг</b>\n"
        f"🎯 Цель: <b>{target:.1f} кг</b>\n"
        f"📉 Осталось: <b>{delta:+.1f} кг</b>\n{SEP}",
        reply_markup=kb_main(p),
        parse_mode="HTML",
    )


# ─── Еда (фото) ───

@router.message(F.photo)
async def msg_food_photo(message: Message) -> None:
    p = await get_profile(message.from_user.id)
    if not p:
        await message.answer("Сначала нажми /start")
        return
    if not p.get("onboarded"):
        await message.answer("Сначала заверши регистрацию — нажми /start")
        return
    if p.get("is_banned"):
        await message.answer("🚫 Ты забанен.")
        return

    allowed, remaining = await check_and_increment_usage(message.from_user.id)
    if not allowed:
        price = int(await get_setting("premium_price_rub", str(PREMIUM_PRICE_RUB)))
        await message.answer(
            f"🆓 <b>Бесплатный лимит исчерпан</b>\n{SEP}\n"
            f"Ты использовал {FREE_DAILY_LIMIT} анализа сегодня.\n"
            f"Оформи Premium за <b>{SUBSCRIPTION_STARS} ⭐</b> или "
            f"<b>{price} ₽</b> (с баланса).\n{SEP}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"💎 Купить Premium ({SUBSCRIPTION_STARS} ⭐)",
                    callback_data="buy_premium",
                )],
                [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
            ]),
            parse_mode="HTML",
        )
        return

    photo = message.photo[-1]
    caption = (message.caption or "").strip()
    wait = await message.answer(
        f"🔍 <b>Анализирую фото…</b>\n{SEP}\n<i>3–10 секунд.</i>",
        parse_mode="HTML",
    )

    try:
        file = await message.bot.get_file(photo.file_id)
        file_bytes = await message.bot.download_file(file.file_path)
        image_base64 = base64.b64encode(file_bytes.read()).decode("ascii")
        ext = (file.file_path or "").rsplit(".", 1)[-1].lower()
        mime_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
                    "png": "image/png", "webp": "image/webp"}
        mime = mime_map.get(ext, "image/jpeg")
        data = await gemini.analyze_food_photo(image_base64, mime, caption)
    except RuntimeError as e:
        await wait.edit_text(
            f"❌ <b>Ошибка</b>\n{SEP}\n<i>{escape_html(str(e))}</i>",
            parse_mode="HTML",
        )
        return

    total = data.get("total", {})
    description = caption or ", ".join(
        it.get("name", "—") for it in data.get("items", [])
    ) or "Фото еды"

    await add_meal(
        message.from_user.id, "lunch", description,
        total.get("calories", 0), total.get("protein", 0),
        total.get("fat", 0), total.get("carbs", 0),
    )
    meals = await get_meals_today(message.from_user.id)
    water = await get_water_today(message.from_user.id)
    workouts = await get_workouts_range(message.from_user.id, days=1)
    goal = p.get("calorie_goal") or 2000
    total_today = sum(m["calories"] for m in meals)
    burned = sum(w["calories"] for w in workouts)
    left = goal - (total_today - burned)

    tail = f"\n{SEP}\n📊 Сегодня: <b>{total_today:.0f}</b> / {goal} ккал\n"
    if burned > 0:
        tail += f"🏋️ Сожжено: <b>-{burned:.0f} ккал</b>\n"
    tail += (f"✅ Осталось: <b>{left:.0f} ккал</b>" if left >= 0
             else f"⚠️ Перебор: <b>{-left:.0f} ккал</b>")
    tail += f"\n💧 Вода: <b>{water}</b>"
    if remaining >= 0:
        tail += f"\n🆓 Осталось бесплатных: <b>{remaining}</b>"
    else:
        tail += f"\n💎 <b>Premium</b>"

    await wait.edit_text(
        fmt_food_result(data) + tail, reply_markup=kb_main(p), parse_mode="HTML"
    )


# ─── Еда (текст) ───

@router.message(F.text & ~F.text.startswith("/"))
async def msg_food_text(message: Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is not None:
        return

    p = await get_profile(message.from_user.id)
    if not p or not p.get("onboarded"):
        await message.answer("Сначала нажми /start")
        return
    if p.get("is_banned"):
        await message.answer("🚫 Ты забанен.")
        return

    text = (message.text or "").strip()
    if len(text) < 3:
        return

    allowed, remaining = await check_and_increment_usage(message.from_user.id)
    if not allowed:
        price = int(await get_setting("premium_price_rub", str(PREMIUM_PRICE_RUB)))
        await message.answer(
            f"🆓 <b>Бесплатный лимит исчерпан</b>\n{SEP}\n"
            f"Оформи Premium за <b>{SUBSCRIPTION_STARS} ⭐</b> или "
            f"<b>{price} ₽</b> (с баланса).\n{SEP}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"💎 Купить Premium ({SUBSCRIPTION_STARS} ⭐)",
                    callback_data="buy_premium",
                )],
                [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
            ]),
            parse_mode="HTML",
        )
        return

    wait = await message.answer(
        f"🔍 <b>Считаю калории…</b>\n{SEP}\n<i>{escape_html(text[:80])}</i>",
        parse_mode="HTML",
    )

    try:
        data = await gemini.analyze_food_text(text)
    except RuntimeError as e:
        await wait.edit_text(
            f"❌ <b>Ошибка</b>\n{SEP}\n<i>{escape_html(str(e))}</i>",
            parse_mode="HTML",
        )
        return

    total = data.get("total", {})
    items = data.get("items", [])
    description = ", ".join(it.get("name", "—") for it in items) or text[:60]

    await add_meal(
        message.from_user.id, "lunch", description,
        total.get("calories", 0), total.get("protein", 0),
        total.get("fat", 0), total.get("carbs", 0),
    )
    meals = await get_meals_today(message.from_user.id)
    water = await get_water_today(message.from_user.id)
    workouts = await get_workouts_range(message.from_user.id, days=1)
    goal = p.get("calorie_goal") or 2000
    total_today = sum(m["calories"] for m in meals)
    burned = sum(w["calories"] for w in workouts)
    left = goal - (total_today - burned)

    tail = f"\n{SEP}\n📊 Сегодня: <b>{total_today:.0f}</b> / {goal} ккал\n"
    if burned > 0:
        tail += f"🏋️ Сожжено: <b>-{burned:.0f} ккал</b>\n"
    tail += (f"✅ Осталось: <b>{left:.0f} ккал</b>" if left >= 0
             else f"⚠️ Перебор: <b>{-left:.0f} ккал</b>")
    tail += f"\n💧 Вода: <b>{water}</b>"
    if remaining >= 0:
        tail += f"\n🆓 Осталось бесплатных: <b>{remaining}</b>"
    else:
        tail += f"\n💎 <b>Premium</b>"

    await wait.edit_text(
        fmt_food_result(data) + tail, reply_markup=kb_main(p), parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
#  ТРЕНИРОВКИ — ХЕНДЛЕРЫ
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "workouts")
async def cb_workouts(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    await cb.message.edit_text(
        fmt_workouts_menu(),
        reply_markup=kb_workouts_menu(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("w_gender:"))
async def cb_w_gender(cb: CallbackQuery) -> None:
    gender = cb.data.split(":", 1)[1]
    label = "Для девушек 👩" if gender == "female" else "Для мужчин 👨"
    await cb.message.edit_text(
        f"🏋️ <b>{label}</b>\n{SEP}\nВыбери категорию:",
        reply_markup=kb_workout_categories(gender),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("w_cat:"))
async def cb_w_cat(cb: CallbackQuery) -> None:
    _, gender, category = cb.data.split(":", 2)
    items = await get_workouts_by_gender(gender, category)
    label = "Для девушек 👩" if gender == "female" else "Для мужчин 👨"
    await cb.message.edit_text(
        fmt_workouts_list(items, label),
        reply_markup=kb_workout_list(items, gender, category),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("w_all:"))
async def cb_w_all(cb: CallbackQuery) -> None:
    gender = cb.data.split(":", 1)[1]
    items = await get_workouts_by_gender(gender)
    label = "Все тренировки для девушек 👩" if gender == "female" else "Все тренировки для мужчин 👨"
    await cb.message.edit_text(
        fmt_workouts_list(items, label),
        reply_markup=kb_workout_list(items, gender),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("w_open:"))
async def cb_w_open(cb: CallbackQuery) -> None:
    workout_id = int(cb.data.split(":", 1)[1])
    w = await get_workout(workout_id)
    if not w:
        await cb.answer("Тренировка не найдена", show_alert=True)
        return
    await cb.message.edit_text(
        fmt_workout_card(w),
        reply_markup=kb_workout_card(workout_id, w["gender"], w["category"]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("w_done:"))
async def cb_w_done(cb: CallbackQuery) -> None:
    workout_id = int(cb.data.split(":", 1)[1])
    w = await get_workout(workout_id)
    if not w:
        await cb.answer("Тренировка не найдена", show_alert=True)
        return

    p = await get_profile(cb.from_user.id)
    is_prem = is_premium(p)

    if not is_prem:
        done_today = await get_workouts_count_today(cb.from_user.id)
        if done_today >= WORKOUT_FREE_DAILY:
            await cb.answer(
                f"🆓 Бесплатно: {WORKOUT_FREE_DAILY} тренировка в день.\n"
                f"Оформи Premium для безлимита!",
                show_alert=True,
            )
            return

    await log_workout(
        cb.from_user.id, w["category"], w["name"],
        w["duration_min"], w["calories"],
    )
    await cb.answer("✅ Тренировка записана!")

    await cb.message.edit_text(
        f"✅ <b>Тренировка выполнена!</b>\n{SEP}\n"
        f"🏋️ {escape_html(w['name'])}\n"
        f"⏱ {w['duration_min']} мин\n"
        f"🔥 Сожжено: <b>~{w['calories']:.0f} ккал</b>\n"
        f"{SEP}\n"
        f"<i>Молодец! Продолжай в том же духе.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏋️ Ещё тренировка", callback_data="workouts")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="main_menu")],
        ]),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "w_stats")
async def cb_w_stats(cb: CallbackQuery) -> None:
    items = await get_workouts_range(cb.from_user.id, days=7)
    await cb.message.edit_text(
        fmt_workouts_stats(items),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏋️ К тренировкам", callback_data="workouts")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="main_menu")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "w_by_goal")
async def cb_w_by_goal(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    gender = p.get("sex") or "male"
    items = await get_workouts_by_gender(gender)

    import random as _r
    picks = _r.sample(items, min(3, len(items))) if items else []

    if not picks:
        await cb.answer("Нет доступных тренировок", show_alert=True)
        return

    lines = [f"🎯 <b>Подобрано под твою цель</b>\n{SEP}"]
    for w in picks:
        lines.append(
            f"🏋️ <b>{escape_html(w['name'])}</b>\n"
            f"  {w['level']} · ⏱ {w['duration_min']} мин · 🔥 ~{w['calories']:.0f} ккал"
        )
    lines.append(SEP)

    rows = []
    for w in picks:
        rows.append([InlineKeyboardButton(
            text=f"🏋️ {w['name'][:30]}",
            callback_data=f"w_open:{w['id']}",
        )])
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="workouts")])

    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML",
    )
    await cb.answer()


# ═══════════════════════════════════════════════════════════════
#  РЕФЕРАЛКА
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "referral")
async def cb_referral(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    await cb.message.edit_text(
        fmt_referral_page(p),
        reply_markup=kb_referral(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "ref_top")
async def cb_ref_top(cb: CallbackQuery) -> None:
    items = await get_referral_top(10)
    await cb.message.edit_text(
        fmt_referral_top(items),
        reply_markup=kb_referral(),
        parse_mode="HTML",
    )
    await cb.answer()


# ═══════════════════════════════════════════════════════════════
#  PREMIUM
# ═══════════════════════════════════════════════════════════════


@router.callback_query(F.data == "buy_premium")
async def cb_buy_premium(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    if is_premium(p):
        await cb.answer("💎 У тебя уже есть Premium!", show_alert=True)
        return
    price_rub = int(await get_setting("premium_price_rub", str(PREMIUM_PRICE_RUB)))
    await cb.message.edit_text(
        fmt_premium_info(price_rub),
        reply_markup=kb_premium(p, price_rub),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "pay_stars")
async def cb_pay_stars(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    if is_premium(p):
        await cb.answer("💎 У тебя уже есть Premium!", show_alert=True)
        return

    await cb.answer()
    try:
        await cb.bot.send_invoice(
            chat_id=cb.from_user.id,
            title="Premium подписка FitBot",
            description=f"Безлимитные анализы еды и тренировки на {SUBSCRIPTION_DAYS} дней",
            payload="premium_30d",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Premium", amount=SUBSCRIPTION_STARS)],
            start_parameter="premium",
        )
    except Exception as e:
        logger.exception("Ошибка отправки счёта Stars")
        await cb.message.answer(f"❌ {escape_html(str(e))}", parse_mode="HTML")


@router.callback_query(F.data == "pay_balance")
async def cb_pay_balance(cb: CallbackQuery) -> None:
    p = await get_profile(cb.from_user.id)
    if not p:
        await cb.answer("Нажми /start", show_alert=True)
        return
    if is_premium(p):
        await cb.answer("💎 У тебя уже есть Premium!", show_alert=True)
        return

    price = int(await get_setting("premium_price_rub", str(PREMIUM_PRICE_RUB)))
    balance = p.get("balance", 0.0) or 0.0

    if balance < price:
        await cb.answer(f"❌ Не хватает {price - int(balance)} ₽", show_alert=True)
        return

    await add_balance(cb.from_user.id, -price)
    until = await grant_premium(cb.from_user.id, SUBSCRIPTION_DAYS)

    await cb.answer("💰 Оплачено с баланса!")
    p = await get_profile(cb.from_user.id)
    await cb.message.edit_text(
        f"🎉 <b>Premium активирован!</b>\n{SEP}\n"
        f"💰 Списано: <b>{price} ₽</b>\n"
        f"💵 Остаток баланса: <b>{p.get('balance', 0):.0f} ₽</b>\n"
        f"💎 Действует до: <b>{until[:10]}</b>\n{SEP}",
        reply_markup=kb_main(p),
        parse_mode="HTML",
    )

    for admin_id in ADMIN_IDS:
        try:
            await cb.bot.send_message(
                admin_id,
                f"💰 <b>Premium за баланс</b>\n{SEP}\n"
                f"👤 {escape_html(cb.from_user.full_name)}\n"
                f"🆔 <code>{cb.from_user.id}</code>\n"
                f"💵 Списано: <b>{price} ₽</b>\n"
                f"💎 Premium до <b>{until[:10]}</b>\n{SEP}",
                parse_mode="HTML",
            )
        except Exception:
            pass


@router.callback_query(F.data == "no_money")
async def cb_no_money(cb: CallbackQuery) -> None:
    await cb.answer(
        "Недостаточно средств. Приглашай друзей через «👥 Пригласить друга» — "
        "за каждого получишь бонус на баланс.",
        show_alert=True,
    )


@router.pre_checkout_query()
async def process_pre_checkout(query: PreCheckoutQuery) -> None:
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def process_successful_payment(message: Message) -> None:
    payment = message.successful_payment
    if payment.invoice_payload == "premium_30d":
        until = await grant_premium(message.from_user.id, SUBSCRIPTION_DAYS)
        await create_stars_payment(
            message.from_user.id,
            payment.total_amount,
            payment.telegram_payment_charge_id,
        )
        p = await get_profile(message.from_user.id)
        await message.answer(
            f"🎉 <b>Premium активирован!</b>\n{SEP}\n"
            f"⭐ Оплачено: <b>{payment.total_amount} звёзд</b>\n"
            f"💎 Действует до: <b>{until[:10]}</b>\n{SEP}",
            reply_markup=kb_main(p),
            parse_mode="HTML",
        )


# ═══════════════════════════════════════════════════════════════
#  АДМИН-ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════════


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа.")
        return
    await message.answer(
        f"🛡 <b>Админ-панель</b>\n{SEP}\nВыбери действие:",
        reply_markup=kb_admin(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "admin")
async def cb_admin(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    await state.clear()
    await cb.message.edit_text(
        f"🛡 <b>Админ-панель</b>\n{SEP}\nВыбери действие:",
        reply_markup=kb_admin(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "adm_stats")
async def cb_adm_stats(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    s = await get_total_stats()
    ps = await get_payment_stats()
    text = (
        f"📊 <b>Статистика FitBot</b>\n{SEP}\n"
        f"👥 Пользователей: <b>{s['total']}</b>\n"
        f"💎 Premium: <b>{s['premium']}</b>\n"
        f"🚫 Забанено: <b>{s['banned']}</b>\n"
        f"🍽 Записей о еде: <b>{s['meals']}</b>\n"
        f"🏋️ Тренировок: <b>{s['workouts']}</b>\n"
        f"👥 Рефералов: <b>{s['refs']}</b>\n{SEP}\n"
        f"⭐ Оплат Stars: <b>{ps['count']}</b>\n"
        f"💰 Звёзд получено: <b>{ps['total']}</b>\n{SEP}"
    )
    await cb.message.edit_text(text, reply_markup=kb_back_admin(), parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data == "adm_settings")
async def cb_adm_settings(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    bonus = await get_setting("referral_bonus", str(REFERRAL_BONUS))
    price = await get_setting("premium_price_rub", str(PREMIUM_PRICE_RUB))
    await cb.message.edit_text(
        f"⚙️ <b>Настройки</b>\n{SEP}\n"
        f"💰 Бонус за реферала: <b>{bonus} ₽</b>\n"
        f"💎 Цена Premium: <b>{price} ₽</b>\n"
        f"{SEP}\n"
        f"Нажми на параметр, чтобы изменить его.",
        reply_markup=kb_admin_settings(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("adm_set:"))
async def cb_adm_set(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    key = cb.data.split(":", 1)[1]
    titles = {
        "referral_bonus": "💰 Сумма за реферала (₽)",
        "premium_price_rub": "💎 Цена Premium (₽)",
    }
    current = await get_setting(key, "0")
    await state.update_data(setting_key=key)
    await state.set_state(AdminStates.waiting_setting_value)
    await cb.message.edit_text(
        f"<b>{titles.get(key, key)}</b>\n{SEP}\n"
        f"Текущее значение: <b>{current}</b>\n\n"
        f"Отправь новое значение числом.",
        reply_markup=kb_admin_cancel(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.waiting_setting_value)
async def msg_adm_setting_value(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or "").replace(",", ".").strip())
        if value < 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи неотрицательное число.")
        return
    data = await state.get_data()
    key = data.get("setting_key")
    await set_setting(key, str(int(value) if value.is_integer() else value))
    await state.clear()
    await message.answer(
        f"✅ <b>Настройка сохранена</b>\n{SEP}\n"
        f"<code>{key}</code> = <b>{int(value)}</b>\n{SEP}",
        reply_markup=kb_admin(),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("adm_users:"))
async def cb_adm_users(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    offset = int(cb.data.split(":", 1)[1])
    users = await get_all_users(limit=10, offset=offset)
    total = await get_users_count()

    lines = [f"👥 <b>Пользователи</b> (стр. {offset // 10 + 1})\n{SEP}"]
    for u in users:
        ban = "🚫" if u.get("is_banned") else "✅"
        prem = "💎" if is_premium(u) else "🆓"
        bal = u.get("balance", 0) or 0
        lines.append(
            f"{ban}{prem} <code>{u['telegram_id']}</code> · "
            f"{escape_html(u.get('full_name') or '—')} · {bal:.0f}₽\n"
        )
    lines.append(SEP)

    rows: list[list[InlineKeyboardButton]] = []
    for u in users:
        prefix = f"{'🚫' if u.get('is_banned') else '✅'}{'💎' if is_premium(u) else '🆓'}"
        rows.append([InlineKeyboardButton(
            text=f"{prefix} {u.get('full_name') or u['telegram_id']}",
            callback_data=f"adm_card:{u['telegram_id']}",
        )])
    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"adm_users:{max(0, offset - 10)}"))
    if offset + 10 < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"adm_users:{offset + 10}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="🔙 В админку", callback_data="admin")])

    await cb.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("adm_card:"))
async def cb_adm_card(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    u = await get_profile(tg_id)
    if not u:
        await cb.answer("Не найден", show_alert=True)
        return

    prem_line = f"💎 до {u['premium_until'][:10]}" if is_premium(u) else "🆓 Бесплатный"
    ban_line = "🚫 Забанен" if u.get("is_banned") else "✅ Активен"
    balance = u.get("balance", 0) or 0
    ref_count = u.get("referrals_count", 0) or 0

    text = (
        f"👤 <b>Карточка</b>\n{SEP}\n"
        f"🆔 <code>{tg_id}</code>\n"
        f"📛 {escape_html(u.get('full_name') or '—')}\n"
        f"🔗 @{u.get('username') or '—'}\n{SEP}\n"
        f"💰 Баланс: <b>{balance:.0f} ₽</b>\n"
        f"👥 Приглашено: <b>{ref_count}</b>\n"
        f"Подписка: <b>{prem_line}</b>\n"
        f"Статус: <b>{ban_line}</b>\n{SEP}"
    )
    await cb.message.edit_text(
        text,
        reply_markup=kb_admin_user_card(tg_id, bool(u.get("is_banned")), is_premium(u)),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("adm_ban:"))
async def cb_adm_ban(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    await update_profile(tg_id, is_banned=1)
    await cb.answer("🚫 Забанен", show_alert=True)
    await cb_adm_card(cb)


@router.callback_query(F.data.startswith("adm_unban:"))
async def cb_adm_unban(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    await update_profile(tg_id, is_banned=0)
    await cb.answer("✅ Разбанен", show_alert=True)
    await cb_adm_card(cb)


@router.callback_query(F.data.startswith("adm_prem:"))
async def cb_adm_prem(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    until = await grant_premium(tg_id, SUBSCRIPTION_DAYS)
    await cb.answer(f"💎 до {until[:10]}", show_alert=True)
    await cb_adm_card(cb)


@router.callback_query(F.data.startswith("adm_revoke:"))
async def cb_adm_revoke(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    await revoke_premium(tg_id)
    await cb.answer("❌ Снят", show_alert=True)
    await cb_adm_card(cb)


@router.callback_query(F.data.startswith("adm_bal:"))
async def cb_adm_bal(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    await state.update_data(balance_target=tg_id)
    await state.set_state(AdminStates.waiting_amount_for_balance)
    await cb.message.edit_text(
        f"💰 <b>Начислить баланс</b>\n{SEP}\n"
        f"Пользователь: <code>{tg_id}</code>\n"
        f"Отправь сумму в ₽ (можно отрицательную).",
        reply_markup=kb_admin_cancel(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "adm_addbal")
async def cb_adm_addbal(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_user_id_for_balance)
    await cb.message.edit_text(
        f"💰 <b>Начисление баланса</b>\n{SEP}\n"
        f"Отправь <b>Telegram ID</b> пользователя.",
        reply_markup=kb_admin_cancel(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.waiting_user_id_for_balance)
async def msg_adm_bal_id(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        tg_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Некорректный ID.")
        return
    u = await get_profile(tg_id)
    if not u:
        await message.answer("❌ Не найден.", reply_markup=kb_admin_cancel())
        return
    await state.update_data(balance_target=tg_id)
    await state.set_state(AdminStates.waiting_amount_for_balance)
    await message.answer(
        f"💰 <b>Начисление</b>\n{SEP}\n"
        f"👤 {escape_html(u.get('full_name') or '—')}\n"
        f"💰 Текущий: <b>{u.get('balance', 0):.0f} ₽</b>\n{SEP}\n"
        f"Отправь сумму в ₽.",
        reply_markup=kb_admin_cancel(),
        parse_mode="HTML",
    )


@router.message(AdminStates.waiting_amount_for_balance)
async def msg_adm_bal_amount(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        amount = float((message.text or "").replace(",", ".").strip())
    except ValueError:
        await message.answer("❌ Некорректная сумма.")
        return
    data = await state.get_data()
    tg_id = data.get("balance_target")
    if not tg_id:
        await state.clear()
        return
    await add_balance(tg_id, amount)
    u = await get_profile(tg_id)
    await state.clear()
    sign = "+" if amount >= 0 else ""
    await message.answer(
        f"✅ <b>Баланс обновлён</b>\n{SEP}\n"
        f"👤 <code>{tg_id}</code>\n"
        f"Изменение: <b>{sign}{amount:.0f} ₽</b>\n"
        f"Новый баланс: <b>{u['balance']:.0f} ₽</b>\n{SEP}",
        reply_markup=kb_admin(),
        parse_mode="HTML",
    )
    try:
        await message.bot.send_message(
            tg_id,
            f"💰 <b>Баланс пополнен</b>\n{SEP}\n"
            f"Изменение: <b>{sign}{amount:.0f} ₽</b>\n"
            f"Баланс: <b>{u['balance']:.0f} ₽</b>\n{SEP}",
            parse_mode="HTML",
        )
    except Exception:
        pass


@router.callback_query(F.data == "adm_grant")
async def cb_adm_grant(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_user_id_for_premium)
    await cb.message.edit_text(
        f"🎁 <b>Выдать Premium</b>\n{SEP}\n"
        f"Отправь <b>Telegram ID</b>.",
        reply_markup=kb_admin_cancel(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.waiting_user_id_for_premium)
async def msg_adm_grant_id(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        tg_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Некорректный ID.")
        return
    u = await get_profile(tg_id)
    if not u:
        await message.answer("❌ Не найден.", reply_markup=kb_admin_cancel())
        return
    await state.update_data(target_id=tg_id)
    await message.answer(
        f"🎁 <b>Premium для</b> <code>{tg_id}</code>\n"
        f"Отправь количество дней (например, <code>{SUBSCRIPTION_DAYS}</code>).",
        reply_markup=kb_admin_cancel(),
        parse_mode="HTML",
    )
    await state.set_state(AdminStates.waiting_days_for_premium)


@router.message(AdminStates.waiting_days_for_premium)
async def msg_adm_grant_days(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        days = int((message.text or "").strip())
        if days <= 0 or days > 3650:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи число дней (1–3650).")
        return
    data = await state.get_data()
    tg_id = data.get("target_id")
    if not tg_id:
        await state.clear()
        return
    until = await grant_premium(tg_id, days)
    await state.clear()
    await message.answer(
        f"✅ <b>Premium выдан</b>\n{SEP}\n"
        f"🆔 <code>{tg_id}</code>\n"
        f"📅 На {days} дней\n"
        f"💎 До <b>{until[:10]}</b>\n{SEP}",
        reply_markup=kb_admin(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_broadcast_text)
    await cb.message.edit_text(
        f"📢 <b>Рассылка</b>\n{SEP}\n"
        f"Отправь текст — он уйдёт всем активным.",
        reply_markup=kb_admin_cancel(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(AdminStates.waiting_broadcast_text)
async def msg_adm_broadcast(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    text = message.html_text or message.text or ""
    if not text.strip():
        await message.answer("❌ Пусто.")
        return
    await state.clear()
    user_ids = await get_all_user_ids()
    status_msg = await message.answer(f"📢 Отправляю {len(user_ids)}…")
    ok, fail = 0, 0
    for uid in user_ids:
        try:
            await message.bot.send_message(
                uid,
                f"📢 <b>Сообщение</b>\n{SEP}\n{text}",
                parse_mode="HTML",
            )
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await status_msg.edit_text(
        f"✅ <b>Готово</b>\n{SEP}\n"
        f"📤 Доставлено: <b>{ok}</b>\n"
        f"❌ Ошибок: <b>{fail}</b>\n{SEP}",
        reply_markup=kb_admin(),
        parse_mode="HTML",
    )


# ═══════════════════════════════════════════════════════════════
#  ЗАПУСК (WEBHOOK)
# ═══════════════════════════════════════════════════════════════


async def on_startup(bot: Bot) -> None:
    await bot.set_webhook(
        url=WEBHOOK_URL,
        secret_token=WEBHOOK_SECRET,
        drop_pending_updates=True,
    )
    logger.info("Webhook: %s", WEBHOOK_URL)


async def healthcheck(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def main() -> None:
    await init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(on_startup)

    app = web.Application()
    handler = SimpleRequestHandler(
        dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET,
    )
    handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    app.router.add_get("/health", healthcheck)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info("FitBot запущен. Порт %d", PORT)
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановлено")