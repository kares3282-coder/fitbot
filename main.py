"""
FitBot — Telegram-бот для похудения с подпиской через Telegram Stars
и админ-панелью. Использует Nscale (Llama 4 Scout) для анализа еды.
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
from aiogram.filters import Command, CommandStart
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
from dotenv import load_dotenv

# ═══════════════════════════════════════════════════════════════
#  КОНФИГ
# ═══════════════════════════════════════════════════════════════

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
NSCALE_SERVICE_TOKEN = os.getenv("NSCALE_SERVICE_TOKEN", "")

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
}

DB_PATH = "fitbot.db"
SEP = "━━━━━━━━━━━━━━"

SUBSCRIPTION_STARS = int(os.getenv("SUBSCRIPTION_STARS", "100"))
SUBSCRIPTION_DAYS = int(os.getenv("SUBSCRIPTION_DAYS", "30"))
FREE_DAILY_LIMIT = int(os.getenv("FREE_DAILY_LIMIT", "3"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("fitbot")


def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS


# ═══════════════════════════════════════════════════════════════
#  SSL-ПАТЧ ДЛЯ ANDROID
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
                free_used_date   TEXT
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
        await db.commit()

        for migration in (
            "ALTER TABLE profiles ADD COLUMN is_banned INTEGER DEFAULT 0",
            "ALTER TABLE profiles ADD COLUMN premium_until TEXT",
            "ALTER TABLE profiles ADD COLUMN free_used_today INTEGER DEFAULT 0",
            "ALTER TABLE profiles ADD COLUMN free_used_date TEXT",
        ):
            try:
                await db.execute(migration)
                await db.commit()
            except Exception:
                pass

    logger.info("БД инициализирована")


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


# ─── Платежи Stars ───


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
    return {
        "total": total, "banned": banned, "premium": premium, "meals": meals,
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


# ═══════════════════════════════════════════════════════════════
#  NSCALE СЕРВИС (Llama 4 Scout)
# ═══════════════════════════════════════════════════════════════


class NscaleService:
    """Клиент Nscale для работы с Llama 4 Scout."""

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

    async def advice(self, profile: dict, meals: list[dict], water: int) -> str:
        total_cal = sum(m["calories"] for m in meals)
        goal = profile.get("calorie_goal") or 2000
        context = (
            f"Цель: {goal} ккал.\n"
            f"Съедено сегодня: {total_cal:.0f} ккал.\n"
            f"Вода: {water} стаканов.\n"
        )
        if meals:
            context += "Приёмы пищи:\n" + "\n".join(
                f"- {m['description']} ({m['calories']:.0f} ккал)" for m in meals
            )
        system = (
            "Ты — дружелюбный нутрициолог. Дай короткий (3–5 предложений) "
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
    return (
        f"🥗 <b>FitBot — твой помощник в похудении</b>\n"
        f"{SEP}\n"
        f"📸 Отправь фото еды — посчитаю калории\n"
        f"✍️ Или напиши, что съел — тоже посчитаю\n"
        f"⚖️ Записывай вес, 💧 считай воду, 💡 получай советы\n"
        f"{SEP}\n"
        f"Статус: <b>{status}</b>\n"
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
        f"Подписка: {sub_line}\n"
        f"{SEP}"
    )


def fmt_today_menu(meals: list[dict], water: int, goal: int) -> str:
    total_cal = sum(m["calories"] for m in meals)
    total_prot = sum(m["protein"] for m in meals)
    total_fat = sum(m["fat"] for m in meals)
    total_carb = sum(m["carbs"] for m in meals)
    left = goal - total_cal
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


def fmt_premium_info() -> str:
    return (
        f"💎 <b>Premium подписка FitBot</b>\n"
        f"{SEP}\n"
        f"Что даёт:\n"
        f"📸 <b>Безлимитные</b> анализы еды\n"
        f"💡 Неограниченные советы\n"
        f"⚡ Приоритетная обработка\n"
        f"{SEP}\n"
        f"💰 Стоимость: <b>{SUBSCRIPTION_STARS} ⭐</b>\n"
        f"📅 Срок: <b>{SUBSCRIPTION_DAYS} дней</b>\n"
        f"{SEP}\n"
        f"<i>Звёзды Telegram — внутренняя валюта, купить можно в приложении Telegram (Settings → Telegram Stars).</i>"
    )


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
        [
            InlineKeyboardButton(text="💡 Совет дня", callback_data="advice"),
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
        ],
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


def kb_premium() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"⭐ Оплатить {SUBSCRIPTION_STARS} звёзд",
            callback_data="pay_stars",
        )],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
    ])


def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm_users:0")],
        [InlineKeyboardButton(text="🎁 Выдать Premium", callback_data="adm_grant")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="main_menu")],
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
    waiting_broadcast_text = State()


# ─── /start ───

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
            f"Я — <b>FitBot</b>, помогу тебе похудеть без сложных диет.\n\n"
            f"📸 Отправляй фото еды — считаю калории\n"
            f"✍️ Пиши, что съел — тоже считаю\n"
            f"⚖️ Записывай вес и следи за прогрессом\n"
            f"💡 Получай персональные советы\n"
            f"{SEP}\n"
            f"🆓 Бесплатно: <b>{FREE_DAILY_LIMIT} анализа в день</b>\n"
            f"💎 Premium: безлимит за <b>{SUBSCRIPTION_STARS} ⭐ / {SUBSCRIPTION_DAYS} дней</b>\n"
            f"{SEP}\n"
            f"Ответь на 6 вопросов — займёт 1 минуту.",
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
    p = await get_profile(cb.from_user.id)
    await cb.message.edit_text(
        f"✅ <b>Профиль готов!</b>\n{SEP}\n"
        f"Отправляй фото еды или пиши, что съел — я всё посчитаю.\n"
        f"🆓 Бесплатно: <b>{FREE_DAILY_LIMIT} анализа в день</b>.\n"
        f"💎 Premium: безлимит за <b>{SUBSCRIPTION_STARS} ⭐ / {SUBSCRIPTION_DAYS} дней</b>.\n{SEP}",
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


# ─── Меню и разделы ───

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
    goal = p.get("calorie_goal") or 2000
    await cb.message.edit_text(
        fmt_today_menu(meals, water, goal), reply_markup=kb_today(), parse_mode="HTML"
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
    goal = p.get("calorie_goal") or 2000
    await cb.message.edit_text(
        fmt_today_menu(meals, glasses, goal), reply_markup=kb_today(), parse_mode="HTML"
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
    goal = (p or {}).get("calorie_goal") or 2000
    await cb.message.edit_text(
        fmt_today_menu(meals, water, goal), reply_markup=kb_today(), parse_mode="HTML"
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
            f"🆓 Бесплатный лимит исчерпан ({FREE_DAILY_LIMIT}/день). "
            f"Оформи Premium за {SUBSCRIPTION_STARS} ⭐.",
            show_alert=True,
        )
        return
    wait_msg = await cb.message.edit_text(
        f"💡 <b>Совет дня</b>\n{SEP}\n<i>Думаю…</i>", parse_mode="HTML"
    )
    meals = await get_meals_today(cb.from_user.id)
    water = await get_water_today(cb.from_user.id)
    try:
        text = await gemini.advice(p, meals, water)
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


# ─── Запись веса ───

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


# ─── Еда (фото + текст) ───

@router.message(F.photo)
async def msg_food_photo(message: Message, state: FSMContext) -> None:
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
        await message.answer(
            f"🆓 <b>Бесплатный лимит исчерпан</b>\n{SEP}\n"
            f"Ты использовал {FREE_DAILY_LIMIT} анализа сегодня.\n"
            f"Лимит обновится завтра, либо оформи Premium за "
            f"<b>{SUBSCRIPTION_STARS} ⭐ / {SUBSCRIPTION_DAYS} дней</b>.\n{SEP}",
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
    goal = p.get("calorie_goal") or 2000
    total_today = sum(m["calories"] for m in meals)
    left = goal - total_today

    tail = f"\n{SEP}\n📊 Сегодня: <b>{total_today:.0f}</b> / {goal} ккал\n"
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


@router.message(F.text & ~F.text.startswith("/"))
async def msg_food_text(message: Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state in (
        Onboarding.sex.state, Onboarding.age.state, Onboarding.height.state,
        Onboarding.weight_start.state, Onboarding.weight_target.state,
        Onboarding.activity.state, Onboarding.confirm.state,
        WeightInput.waiting_weight.state,
        AdminStates.waiting_user_id_for_premium.state,
        AdminStates.waiting_days_for_premium.state,
        AdminStates.waiting_broadcast_text.state,
    ):
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
        await message.answer(
            f"🆓 <b>Бесплатный лимит исчерпан</b>\n{SEP}\n"
            f"Оформи Premium за <b>{SUBSCRIPTION_STARS} ⭐ / "
            f"{SUBSCRIPTION_DAYS} дней</b> для безлимита.\n{SEP}",
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
    goal = p.get("calorie_goal") or 2000
    total_today = sum(m["calories"] for m in meals)
    left = goal - total_today

    tail = f"\n{SEP}\n📊 Сегодня: <b>{total_today:.0f}</b> / {goal} ккал\n"
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
#  ОПЛАТА ЧЕРЕЗ TELEGRAM STARS
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

    await cb.message.edit_text(
        fmt_premium_info(),
        reply_markup=kb_premium(),
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
            description=f"Безлимитные анализы еды на {SUBSCRIPTION_DAYS} дней",
            payload="premium_30d",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Premium", amount=SUBSCRIPTION_STARS)],
            start_parameter="premium",
        )
    except Exception as e:
        logger.exception("Ошибка отправки счёта Stars")
        await cb.message.answer(
            f"❌ <b>Не удалось создать счёт</b>\n{SEP}\n"
            f"<i>{escape_html(str(e))}</i>",
            parse_mode="HTML",
        )


@router.pre_checkout_query()
async def process_pre_checkout(query: PreCheckoutQuery) -> None:
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def process_successful_payment(message: Message) -> None:
    payment = message.successful_payment
    logger.info(
        "Успешный платёж: user=%s, amount=%s XTR, payload=%s, charge_id=%s",
        message.from_user.id,
        payment.total_amount,
        payment.invoice_payload,
        payment.telegram_payment_charge_id,
    )

    if payment.invoice_payload == "premium_30d":
        until = await grant_premium(message.from_user.id, SUBSCRIPTION_DAYS)
        await create_stars_payment(
            message.from_user.id,
            payment.total_amount,
            payment.telegram_payment_charge_id,
        )

        p = await get_profile(message.from_user.id)
        await message.answer(
            f"🎉 <b>Premium активирован!</b>\n"
            f"{SEP}\n"
            f"⭐ Оплачено: <b>{payment.total_amount} звёзд</b>\n"
            f"💎 Действует до: <b>{until[:10]}</b>\n"
            f"📸 Теперь у тебя безлимитные запросы.\n"
            f"{SEP}",
            reply_markup=kb_main(p),
            parse_mode="HTML",
        )

        for admin_id in ADMIN_IDS:
            try:
                await message.bot.send_message(
                    admin_id,
                    f"💰 <b>Новая оплата Stars</b>\n{SEP}\n"
                    f"👤 {escape_html(message.from_user.full_name)}\n"
                    f"🆔 <code>{message.from_user.id}</code>\n"
                    f"⭐ Сумма: <b>{payment.total_amount} звёзд</b>\n"
                    f"💎 Premium до <b>{until[:10]}</b>\n{SEP}",
                    parse_mode="HTML",
                )
            except Exception:
                pass


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
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    s = await get_total_stats()
    ps = await get_payment_stats()
    text = (
        f"📊 <b>Статистика FitBot</b>\n{SEP}\n"
        f"👥 Пользователей: <b>{s['total']}</b>\n"
        f"💎 Premium: <b>{s['premium']}</b>\n"
        f"🚫 Забанено: <b>{s['banned']}</b>\n"
        f"🍽 Записей о еде: <b>{s['meals']}</b>\n{SEP}\n"
        f"⭐ Оплат Stars: <b>{ps['count']}</b>\n"
        f"💰 Звёзд получено: <b>{ps['total']}</b>\n{SEP}"
    )
    await cb.message.edit_text(
        text, reply_markup=kb_back_admin(), parse_mode="HTML"
    )
    await cb.answer()


@router.callback_query(F.data.startswith("adm_users:"))
async def cb_adm_users(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    offset = int(cb.data.split(":", 1)[1])
    users = await get_all_users(limit=10, offset=offset)
    total = await get_users_count()

    lines = [f"👥 <b>Пользователи</b> (стр. {offset // 10 + 1})\n{SEP}"]
    for u in users:
        ban = "🚫" if u.get("is_banned") else "✅"
        prem = "💎" if is_premium(u) else "🆓"
        uname = f"@{u['username']}" if u.get("username") else "—"
        lines.append(
            f"{ban}{prem} <code>{u['telegram_id']}</code> · "
            f"{escape_html(u.get('full_name') or '—')} · {uname}\n"
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
        nav.append(InlineKeyboardButton(
            text="⬅️", callback_data=f"adm_users:{max(0, offset - 10)}"
        ))
    if offset + 10 < total:
        nav.append(InlineKeyboardButton(
            text="➡️", callback_data=f"adm_users:{offset + 10}"
        ))
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
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    u = await get_profile(tg_id)
    if not u:
        await cb.answer("Пользователь не найден.", show_alert=True)
        return

    prem_line = f"💎 до {u['premium_until'][:10]}" if is_premium(u) else "🆓 Бесплатный"
    ban_line = "🚫 Забанен" if u.get("is_banned") else "✅ Активен"

    text = (
        f"👤 <b>Карточка пользователя</b>\n{SEP}\n"
        f"🆔 <code>{tg_id}</code>\n"
        f"📛 {escape_html(u.get('full_name') or '—')}\n"
        f"🔗 @{u.get('username') or '—'}\n"
        f"📅 Регистрация: {u.get('registered_at', '')[:10]}\n{SEP}\n"
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
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    await update_profile(tg_id, is_banned=1)
    await cb.answer("🚫 Пользователь забанен", show_alert=True)
    await cb_adm_card(cb)


@router.callback_query(F.data.startswith("adm_unban:"))
async def cb_adm_unban(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    await update_profile(tg_id, is_banned=0)
    await cb.answer("✅ Разбанен", show_alert=True)
    await cb_adm_card(cb)


@router.callback_query(F.data.startswith("adm_prem:"))
async def cb_adm_prem(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    until = await grant_premium(tg_id, SUBSCRIPTION_DAYS)
    await cb.answer(f"💎 Premium до {until[:10]}", show_alert=True)
    await cb_adm_card(cb)
    try:
        await cb.bot.send_message(
            tg_id,
            f"🎉 <b>Premium активирован!</b>\n{SEP}\n"
            f"💎 Действует до <b>{until[:10]}</b>\n{SEP}",
            parse_mode="HTML",
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("adm_revoke:"))
async def cb_adm_revoke(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    tg_id = int(cb.data.split(":", 1)[1])
    await revoke_premium(tg_id)
    await cb.answer("❌ Premium снят", show_alert=True)
    await cb_adm_card(cb)


@router.callback_query(F.data == "adm_grant")
async def cb_adm_grant(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_user_id_for_premium)
    await cb.message.edit_text(
        f"🎁 <b>Выдать Premium</b>\n{SEP}\n"
        f"Отправь <b>Telegram ID</b> пользователя.\n{SEP}",
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
        await message.answer("❌ Пользователь не найден.", reply_markup=kb_admin_cancel())
        return
    await state.update_data(target_id=tg_id)
    await message.answer(
        f"🎁 <b>Premium для</b> <code>{tg_id}</code>\n{SEP}\n"
        f"Отправь количество дней (например, <code>{SUBSCRIPTION_DAYS}</code>).\n{SEP}",
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
    try:
        await message.bot.send_message(
            tg_id,
            f"🎉 <b>Premium активирован!</b>\n{SEP}\n"
            f"💎 Действует до <b>{until[:10]}</b>\n{SEP}",
            parse_mode="HTML",
        )
    except Exception:
        pass


@router.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Нет доступа.", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_broadcast_text)
    await cb.message.edit_text(
        f"📢 <b>Рассылка</b>\n{SEP}\n"
        f"Отправь текст — он уйдёт всем активным пользователям.\n"
        f"Поддерживается HTML: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>.\n{SEP}",
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
        await message.answer("❌ Пустое сообщение.")
        return
    await state.clear()
    user_ids = await get_all_user_ids()
    status_msg = await message.answer(f"📢 Отправляю {len(user_ids)} пользователям…")
    ok, fail = 0, 0
    for uid in user_ids:
        try:
            await message.bot.send_message(
                uid,
                f"📢 <b>Сообщение от админа</b>\n{SEP}\n{text}",
                parse_mode="HTML",
            )
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await status_msg.edit_text(
        f"✅ <b>Рассылка завершена</b>\n{SEP}\n"
        f"📤 Доставлено: <b>{ok}</b>\n"
        f"❌ Ошибок: <b>{fail}</b>\n{SEP}",
        reply_markup=kb_admin(),
        parse_mode="HTML",
    )


# ═══════════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════════


async def main() -> None:
    await init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info(
        "FitBot запущен. Nscale модель: meta-llama/Llama-4-Scout-17B-16E-Instruct | "
        "Premium: %d ⭐/%d дн | Бесплатно: %d/день",
        SUBSCRIPTION_STARS, SUBSCRIPTION_DAYS, FREE_DAILY_LIMIT,
    )
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановлено")