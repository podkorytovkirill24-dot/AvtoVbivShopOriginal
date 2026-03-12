# -*- coding: utf-8 -*-
import asyncio
import re
import sqlite3
import zipfile
from pathlib import Path

from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from prodazha_yadro import (
    CryptoPayClient,
    InstanceSupervisor,
    SalesConfig,
    TokenCipher,
    apply_paid_plan,
    attach_token_to_license,
    create_order,
    delete_license,
    describe_license,
    ensure_dirs,
    ensure_secret_key,
    format_expiration,
    get_admin_stats,
    get_license,
    get_order,
    get_promotion,
    get_promo_stats,
    init_db,
    is_priority_user,
    list_licenses,
    list_licenses_with_users,
    list_promotions,
    list_user_ids,
    list_priority_users,
    add_promotion,
    mark_order_status,
    now_ts,
    resolve_user_by_username_or_id,
    revoke_priority,
    set_promotion_active,
    set_license_start_date,
    sync_plan_prices,
    grant_priority,
    update_promotion,
    delete_promotion,
    upsert_user,
    verify_bot_token,
)


APP_CFG = "app_cfg"
APP_CRYPTO = "crypto_client"
APP_SUPERVISOR = "supervisor"
APP_CIPHER = "cipher"
TOKEN_RE = re.compile(r"^\d{6,}:[A-Za-z0-9_-]{20,}$")
DATE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
ADMIN_STATE_KEY = "admin_state"
PLAN_ALIASES = {
    "week": "week",
    "неделя": "week",
    "7": "week",
    "month": "month",
    "месяц": "month",
    "30": "month",
    "lifetime": "lifetime",
    "навсегда": "lifetime",
    "forever": "lifetime",
}


def set_admin_state(context: ContextTypes.DEFAULT_TYPE, name: str, data: dict | None = None) -> None:
    context.user_data[ADMIN_STATE_KEY] = {"name": name, "data": data or {}}


def get_admin_state(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return context.user_data.get(ADMIN_STATE_KEY)


def clear_admin_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(ADMIN_STATE_KEY, None)


def parse_plan_key(text: str) -> str | None:
    raw = (text or "").strip().lower()
    return PLAN_ALIASES.get(raw)


def parse_date(text: str) -> datetime | None:
    try:
        return datetime.strptime(text.strip(), "%d.%m.%Y")
    except Exception:
        return None


def format_date(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(int(ts)).strftime("%d.%m.%Y")


def format_datetime(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(int(ts)).strftime("%d.%m.%Y %H:%M")


def main_menu(cfg: SalesConfig) -> InlineKeyboardMarkup:
    week = cfg.plans["week"]
    month = cfg.plans["month"]
    lifetime = cfg.plans["lifetime"]
    rows = [
        [InlineKeyboardButton(f"🛒 Купить неделю ({week.price_usdt:.2f} USDT)", callback_data="buy:week")],
        [InlineKeyboardButton(f"🛒 Купить месяц ({month.price_usdt:.2f} USDT)", callback_data="buy:month")],
        [InlineKeyboardButton(f"🏆 Купить навсегда ({lifetime.price_usdt:.2f} USDT)", callback_data="buy:lifetime")],
        [InlineKeyboardButton("👤 Моя подписка", callback_data="my_license")],
    ]
    return InlineKeyboardMarkup(rows)


def payment_menu(pay_url: str, order_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("💳 Оплатить", url=pay_url)],
        [InlineKeyboardButton("✅ Проверить оплату", callback_data=f"check:{order_id}")],
        [InlineKeyboardButton("🏠 В меню", callback_data="to_menu")],
    ]
    return InlineKeyboardMarkup(rows)


def owner_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🛠 Админ-панель", callback_data="admin:panel")]]
    )


def admin_panel_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton("👥 Клиенты", callback_data="admin:clients")],
        [InlineKeyboardButton("🧾 Подписки", callback_data="admin:subs")],
        [InlineKeyboardButton("🎁 Акции", callback_data="admin:promos")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="admin:broadcast")],
        [InlineKeyboardButton("⭐ Приоритет", callback_data="admin:priority_list")],
        [InlineKeyboardButton("🗄 База", callback_data="admin:db")],
        [InlineKeyboardButton("🏠 В меню", callback_data="to_menu")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_subs_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🗑 Удалить подписку", callback_data="admin:subs:delete")],
        [InlineKeyboardButton("🕒 Изменить дату покупки", callback_data="admin:subs:backdate")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin:panel")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_promos_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("➕ Добавить акцию", callback_data="admin:promo:add")],
        [InlineKeyboardButton("✏️ Изменить акцию", callback_data="admin:promo:edit")],
        [InlineKeyboardButton("🗑 Удалить акцию", callback_data="admin:promo:delete")],
        [InlineKeyboardButton("🔛 Вкл/Выкл", callback_data="admin:promo:toggle")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin:promo:stats")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin:panel")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_db_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("⬇️ Скачать базу", callback_data="admin:db:download")],
        [InlineKeyboardButton("⬆️ Загрузить базу", callback_data="admin:db:upload")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin:panel")],
    ]
    return InlineKeyboardMarkup(rows)


def get_runtime(context: ContextTypes.DEFAULT_TYPE):
    cfg = context.application.bot_data[APP_CFG]
    crypto = context.application.bot_data[APP_CRYPTO]
    supervisor = context.application.bot_data[APP_SUPERVISOR]
    cipher = context.application.bot_data[APP_CIPHER]
    return cfg, crypto, supervisor, cipher


def is_owner(cfg: SalesConfig, user_id: int) -> bool:
    return int(user_id) in cfg.owner_ids


def parse_priority_command(text: str):
    raw = (text or "").strip()
    low = raw.lower()
    if low.startswith("выдать приоритет "):
        target = raw[len("выдать приоритет ") :].strip()
        return "grant", target
    if low.startswith("снять приоритет "):
        target = raw[len("снять приоритет ") :].strip()
        return "revoke", target
    return None, ""


def stats_text(cfg: SalesConfig, supervisor: InstanceSupervisor) -> str:
    stats = get_admin_stats(cfg)
    running = 0
    for row in list_licenses(cfg):
        if supervisor.is_running(int(row["user_id"])):
            running += 1
    return (
        "📊 Статистика\n\n"
        f"👥 Пользователей: {int(stats['users_total'])}\n"
        f"🧾 Заказов всего: {int(stats['orders_total'])}\n"
        f"✅ Оплачено: {int(stats['orders_paid'])}\n"
        f"⏳ В ожидании: {int(stats['orders_pending'])}\n"
        f"⌛ Истекших инвойсов: {int(stats['orders_expired'])}\n"
        f"💰 Выручка (paid): {stats['revenue_paid_usdt']:.2f} USDT\n\n"
        f"🎫 Лицензий всего: {int(stats['licenses_total'])}\n"
        f"🟢 Активных: {int(stats['licenses_active'])}\n"
        f"🔑 Ожидают токен: {int(stats['licenses_pending_token'])}\n"
        f"⛔ Истекли: {int(stats['licenses_expired'])}\n"
        f"🛑 Остановлены: {int(stats['licenses_stopped'])}\n"
        f"⚙️ Сейчас запущено инстансов: {running}\n\n"
        f"⭐ Пользователей с приоритетом: {int(stats['priority_users'])}\n\n"
        "Команды:\n"
        "выдать приоритет @username\n"
        "снять приоритет @username"
    )


def promo_list_text(rows) -> str:
    if not rows:
        return "🎁 Акции\n\nПока нет активных/созданных акций."
    lines = ["🎁 Акции", ""]
    for r in rows:
        status = "✅" if int(r["is_active"] or 0) else "❌"
        lines.append(
            f"{r['id']} | {status} {r['title']} | {r['plan_key']} | +{r['bonus_days']} дн | "
            f"{format_date(r['start_ts'])}–{format_date(r['end_ts'])}"
        )
    return "\n".join(lines)


def promo_stats_text(stats: dict) -> str:
    promo = stats["promo"]
    status = "✅" if int(promo["is_active"] or 0) else "❌"
    return (
        "📊 Статистика акции\n\n"
        f"ID: {promo['id']}\n"
        f"{status} {promo['title']}\n"
        f"План: {promo['plan_key']}\n"
        f"Бонус: +{promo['bonus_days']} дн\n"
        f"Период: {format_date(promo['start_ts'])}–{format_date(promo['end_ts'])}\n\n"
        f"Оплаченных заказов: {stats['orders_paid']}\n"
        f"Уникальных покупателей: {stats['users_paid']}\n"
        f"Выручка: {stats['revenue_usdt']:.2f} USDT"
    )


def _validate_sqlite_db(path: Path) -> tuple[bool, str]:
    try:
        with path.open("rb") as handle:
            header = handle.read(16)
        if header != b"SQLite format 3\x00":
            return False, "Файл не похож на SQLite базу."
        conn = sqlite3.connect(str(path))
        row = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        if not row or row[0] != "ok":
            return False, f"Integrity check: {row[0] if row else 'error'}"
    except Exception as exc:
        return False, f"Ошибка SQLite: {exc}"
    return True, ""


async def handle_admin_state_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    cfg: SalesConfig,
    supervisor: InstanceSupervisor,
) -> bool:
    state = get_admin_state(context)
    if not state:
        return False

    msg = update.effective_message
    text = (msg.text or msg.caption or "").strip()
    has_photo = bool(getattr(msg, "photo", None))

    if not text and not has_photo:
        return True

    if text.lower() in ("отмена", "cancel", "/cancel"):
        clear_admin_state(context)
        await update.effective_message.reply_text("Действие отменено.", reply_markup=admin_panel_menu())
        return True

    name = state.get("name")
    data = state.get("data", {})

    if name == "admin_db_upload":
        await update.effective_message.reply_text("Пришлите файл базы .db (документом).")
        return True

    if name == "admin_delete_sub":
        user_row = resolve_user_by_username_or_id(cfg, text)
        if user_row is None:
            await update.effective_message.reply_text("Пользователь не найден. Введите ID или @username.")
            return True
        deleted = delete_license(cfg, int(user_row["user_id"]))
        supervisor.sync_user(int(user_row["user_id"]))
        clear_admin_state(context)
        if deleted:
            await update.effective_message.reply_text("Подписка удалена.", reply_markup=admin_panel_menu())
        else:
            await update.effective_message.reply_text("Подписка не найдена.", reply_markup=admin_panel_menu())
        return True

    if name == "admin_backdate_sub":
        match = DATE_RE.search(text)
        if not match:
            await update.effective_message.reply_text("Укажите дату в формате ДД.ММ.ГГГГ.")
            return True
        date_str = match.group(1)
        target = text.replace(date_str, "").replace("|", " ").strip()
        if not target:
            await update.effective_message.reply_text("Укажите ID или @username пользователя.")
            return True
        user_row = resolve_user_by_username_or_id(cfg, target)
        if user_row is None:
            await update.effective_message.reply_text("Пользователь не найден. Введите ID или @username.")
            return True
        dt = parse_date(date_str)
        if not dt:
            await update.effective_message.reply_text("Неверная дата. Формат: ДД.ММ.ГГГГ.")
            return True
        start_ts = int(dt.timestamp())
        updated = set_license_start_date(cfg, int(user_row["user_id"]), start_ts)
        supervisor.sync_user(int(user_row["user_id"]))
        clear_admin_state(context)
        if not updated:
            await update.effective_message.reply_text("Подписка не найдена.", reply_markup=admin_panel_menu())
            return True
        await update.effective_message.reply_text(
            "Дата покупки изменена.\n"
            f"Старт: {format_date(start_ts)}\n"
            f"Истекает: {format_expiration(updated['expires_at'])}",
            reply_markup=admin_panel_menu(),
        )
        return True

    if name == "admin_broadcast":
        if not text and not has_photo:
            await update.effective_message.reply_text("Пришлите текст или фото для рассылки.")
            return True

        button_text = None
        button_url = None
        cleaned_lines = []
        for line in (text.splitlines() if text else []):
            low = line.strip().lower()
            if low.startswith("кнопка:") or low.startswith("button:"):
                rest = line.split(":", 1)[1].strip()
                if "|" in rest:
                    bt, bu = [p.strip() for p in rest.split("|", 1)]
                    if bt and bu and bu.startswith("http"):
                        button_text = bt
                        button_url = bu
                        continue
            cleaned_lines.append(line)
        clean_text = "\n".join(cleaned_lines).strip()
        reply_markup = None
        if button_text and button_url:
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=button_url)]])

        photo_id = msg.photo[-1].file_id if has_photo else None
        user_ids = list_user_ids(cfg)
        sent = 0
        failed = 0
        for uid in user_ids:
            try:
                if photo_id:
                    await context.bot.send_photo(
                        chat_id=uid,
                        photo=photo_id,
                        caption=clean_text if clean_text else None,
                        reply_markup=reply_markup,
                    )
                else:
                    if not clean_text:
                        failed += 1
                        continue
                    await context.bot.send_message(chat_id=uid, text=clean_text, reply_markup=reply_markup)
                sent += 1
            except Exception:
                failed += 1
        clear_admin_state(context)
        await update.effective_message.reply_text(
            f"Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}",
            reply_markup=admin_panel_menu(),
        )
        return True

    if name == "promo_add_title":
        data["title"] = text
        set_admin_state(context, "promo_add_plan", data)
        await update.effective_message.reply_text("План: week / month / lifetime")
        return True

    if name == "promo_add_plan":
        plan_key = parse_plan_key(text)
        if not plan_key:
            await update.effective_message.reply_text("Неверный план. Используйте: week / month / lifetime.")
            return True
        data["plan_key"] = plan_key
        set_admin_state(context, "promo_add_start", data)
        await update.effective_message.reply_text("Дата начала (ДД.ММ.ГГГГ):")
        return True

    if name == "promo_add_start":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("Неверная дата. Формат: ДД.ММ.ГГГГ.")
            return True
        start_ts = int(dt.timestamp())
        data["start_ts"] = start_ts
        set_admin_state(context, "promo_add_end", data)
        await update.effective_message.reply_text("Дата окончания (ДД.ММ.ГГГГ):")
        return True

    if name == "promo_add_end":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("Неверная дата. Формат: ДД.ММ.ГГГГ.")
            return True
        end_ts = int((dt + timedelta(days=1) - timedelta(seconds=1)).timestamp())
        if end_ts < int(data.get("start_ts", 0)):
            await update.effective_message.reply_text("Дата окончания меньше даты начала.")
            return True
        data["end_ts"] = end_ts
        set_admin_state(context, "promo_add_bonus", data)
        await update.effective_message.reply_text("Бонус дней (число):")
        return True

    if name == "promo_add_bonus":
        try:
            bonus_days = int(text)
        except ValueError:
            await update.effective_message.reply_text("Введите число бонусных дней.")
            return True
        promo_id = add_promotion(
            cfg,
            data.get("title", "Акция"),
            data["plan_key"],
            bonus_days,
            int(data["start_ts"]),
            int(data["end_ts"]),
            1,
        )
        clear_admin_state(context)
        await update.effective_message.reply_text(
            f"Акция добавлена. ID: {promo_id}",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_edit_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Введите числовой ID акции.")
            return True
        promo = get_promotion(cfg, promo_id)
        if promo is None:
            await update.effective_message.reply_text("Акция не найдена.")
            return True
        data = {"promo_id": promo_id}
        set_admin_state(context, "promo_edit_title", data)
        await update.effective_message.reply_text("Новое название акции:")
        return True

    if name == "promo_edit_title":
        data["title"] = text
        set_admin_state(context, "promo_edit_plan", data)
        await update.effective_message.reply_text("План: week / month / lifetime")
        return True

    if name == "promo_edit_plan":
        plan_key = parse_plan_key(text)
        if not plan_key:
            await update.effective_message.reply_text("Неверный план. Используйте: week / month / lifetime.")
            return True
        data["plan_key"] = plan_key
        set_admin_state(context, "promo_edit_start", data)
        await update.effective_message.reply_text("Дата начала (ДД.ММ.ГГГГ):")
        return True

    if name == "promo_edit_start":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("Неверная дата. Формат: ДД.ММ.ГГГГ.")
            return True
        data["start_ts"] = int(dt.timestamp())
        set_admin_state(context, "promo_edit_end", data)
        await update.effective_message.reply_text("Дата окончания (ДД.ММ.ГГГГ):")
        return True

    if name == "promo_edit_end":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("Неверная дата. Формат: ДД.ММ.ГГГГ.")
            return True
        end_ts = int((dt + timedelta(days=1) - timedelta(seconds=1)).timestamp())
        if end_ts < int(data.get("start_ts", 0)):
            await update.effective_message.reply_text("Дата окончания меньше даты начала.")
            return True
        data["end_ts"] = end_ts
        set_admin_state(context, "promo_edit_bonus", data)
        await update.effective_message.reply_text("Бонус дней (число):")
        return True

    if name == "promo_edit_bonus":
        try:
            bonus_days = int(text)
        except ValueError:
            await update.effective_message.reply_text("Введите число бонусных дней.")
            return True
        updated = update_promotion(
            cfg,
            int(data["promo_id"]),
            data.get("title", "Акция"),
            data["plan_key"],
            bonus_days,
            int(data["start_ts"]),
            int(data["end_ts"]),
            1,
        )
        clear_admin_state(context)
        await update.effective_message.reply_text(
            "Акция обновлена." if updated else "Акция не найдена.",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_delete_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Введите числовой ID акции.")
            return True
        deleted = delete_promotion(cfg, promo_id)
        clear_admin_state(context)
        await update.effective_message.reply_text(
            "Акция удалена." if deleted else "Акция не найдена.",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_toggle_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Введите числовой ID акции.")
            return True
        promo = get_promotion(cfg, promo_id)
        if promo is None:
            await update.effective_message.reply_text("Акция не найдена.")
            return True
        new_active = 0 if int(promo["is_active"] or 0) else 1
        set_promotion_active(cfg, promo_id, new_active)
        clear_admin_state(context)
        await update.effective_message.reply_text(
            f"Акция {'включена' if new_active else 'выключена'}.",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_stats_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Введите числовой ID акции.")
            return True
        stats = get_promo_stats(cfg, promo_id)
        clear_admin_state(context)
        if not stats:
            await update.effective_message.reply_text("Акция не найдена.", reply_markup=admin_promos_menu())
            return True
        await update.effective_message.reply_text(promo_stats_text(stats), reply_markup=admin_promos_menu())
        return True

    return False


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return
    msg = update.effective_message
    if not msg or not msg.document:
        return
    cfg, _, supervisor, _ = get_runtime(context)
    user_id = int(update.effective_user.id)
    if not is_owner(cfg, user_id):
        return
    state = get_admin_state(context)
    if not state or state.get("name") != "admin_db_upload":
        return

    doc = msg.document
    await msg.reply_text("⏳ Загружаю базу...")

    db_path = cfg.sales_db_path
    backup_path = None
    tmp_path = db_path.with_suffix(db_path.suffix + f".upload_{now_ts()}")
    tmp_zip = db_path.with_suffix(".upload.zip")
    try:
        file = await context.bot.get_file(doc.file_id)
        if doc.file_name and doc.file_name.lower().endswith(".zip"):
            await file.download_to_drive(custom_path=str(tmp_zip))
            with zipfile.ZipFile(tmp_zip, "r") as zf:
                db_names = [n for n in zf.namelist() if n.lower().endswith(".db")]
                if not db_names:
                    raise RuntimeError("В архиве нет .db файла.")
                with zf.open(db_names[0], "r") as src, tmp_path.open("wb") as dst:
                    dst.write(src.read())
        else:
            await file.download_to_drive(custom_path=str(tmp_path))

        ok, err = _validate_sqlite_db(tmp_path)
        if not ok:
            raise RuntimeError(err)

        supervisor.shutdown()
        if db_path.exists():
            backup_path = db_path.with_suffix(db_path.suffix + f".bak_{now_ts()}")
            db_path.replace(backup_path)
        tmp_path.replace(db_path)
        init_db(cfg)
        supervisor.start()
        clear_admin_state(context)
        backup_note = f"\nРезервная копия: {backup_path.name}" if backup_path else ""
        await msg.reply_text(f"✅ База загружена.{backup_note}", reply_markup=admin_panel_menu())
    except Exception as exc:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        try:
            if tmp_zip.exists():
                tmp_zip.unlink()
        except Exception:
            pass
        try:
            if backup_path and backup_path.exists():
                if db_path.exists():
                    db_path.unlink()
                backup_path.replace(db_path)
        except Exception:
            pass
        try:
            supervisor.start()
        except Exception:
            pass
        clear_admin_state(context)
        await msg.reply_text(f"❌ Ошибка загрузки базы: {exc}", reply_markup=admin_panel_menu())


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, _, _, _ = get_runtime(context)
    upsert_user(cfg, update.effective_user)
    text = (
        "🚀 AUTO VBIV — подписка и запуск бота\n\n"
        "1) Выберите тариф.\n"
        "2) Оплатите через Crypto Bot.\n"
        "3) Отправьте токен своего бота.\n"
        "4) Запуск происходит автоматически.\n\n"
        "ℹ️ Команда: /status"
    )
    await update.effective_message.reply_text(text, reply_markup=main_menu(cfg))

    if int(update.effective_user.id) in cfg.owner_ids:
        await update.effective_message.reply_text("🛠 Режим владельца.", reply_markup=owner_menu())


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, _, supervisor, _ = get_runtime(context)
    user_id = int(update.effective_user.id)
    row = get_license(cfg, user_id)
    text = describe_license(row)
    if row is not None:
        running = "Да" if supervisor.is_running(user_id) else "Нет"
        text += f"\nЗапущен: {running}"
    await update.effective_message.reply_text(text, reply_markup=main_menu(cfg))


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, _, supervisor, _ = get_runtime(context)
    user_id = int(update.effective_user.id)
    if not is_owner(cfg, user_id):
        await update.effective_message.reply_text("Нет доступа.")
        return
    await update.effective_message.reply_text(stats_text(cfg, supervisor), reply_markup=admin_panel_menu())


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()

    cfg, crypto, supervisor, _ = get_runtime(context)
    user_id = int(query.from_user.id)
    upsert_user(cfg, query.from_user)

    data = query.data or ""
    if data == "to_menu":
        await query.message.reply_text("Главное меню:", reply_markup=main_menu(cfg))
        return

    if data == "my_license":
        row = get_license(cfg, user_id)
        text = describe_license(row)
        if row is not None:
            text += f"\nЗапущен: {'Да' if supervisor.is_running(user_id) else 'Нет'}"
        await query.message.reply_text(text, reply_markup=main_menu(cfg))
        return

    if data == "admin:panel":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        await query.message.reply_text(stats_text(cfg, supervisor), reply_markup=admin_panel_menu())
        return

    if data == "admin:stats":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        await query.message.reply_text(stats_text(cfg, supervisor), reply_markup=admin_panel_menu())
        return

    if data == "admin:subs":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        await query.message.reply_text("🧾 Управление подписками:", reply_markup=admin_subs_menu())
        return

    if data == "admin:subs:delete":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "admin_delete_sub")
        await query.message.reply_text("Введите ID или @username пользователя для удаления подписки:")
        return

    if data == "admin:subs:backdate":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "admin_backdate_sub")
        await query.message.reply_text("Введите ID/@username и дату покупки (ДД.ММ.ГГГГ).\nПример: 123456 10.03.2026")
        return

    if data == "admin:promos":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        promos = list_promotions(cfg)
        await query.message.reply_text(promo_list_text(promos), reply_markup=admin_promos_menu())
        return

    if data == "admin:promo:add":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "promo_add_title")
        await query.message.reply_text("Введите название акции:")
        return

    if data == "admin:promo:edit":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "promo_edit_id")
        await query.message.reply_text("Введите ID акции для изменения:")
        return

    if data == "admin:promo:delete":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "promo_delete_id")
        await query.message.reply_text("Введите ID акции для удаления:")
        return

    if data == "admin:promo:toggle":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "promo_toggle_id")
        await query.message.reply_text("Введите ID акции для Вкл/Выкл:")
        return

    if data == "admin:promo:stats":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "promo_stats_id")
        await query.message.reply_text("Введите ID акции для статистики:")
        return

    if data == "admin:broadcast":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "admin_broadcast")
        await query.message.reply_text(
            "Отправьте сообщение для рассылки (текст или фото).\n"
            "Кнопка: добавьте строку вида\n"
            "Кнопка: Текст | https://example.com\n"
            "Для отмены напишите «отмена»."
        )
        return

    if data == "admin:db":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        await query.message.reply_text("🗄 Управление базой:", reply_markup=admin_db_menu())
        return

    if data == "admin:db:download":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        db_path = cfg.sales_db_path
        if not db_path.exists():
            await query.message.reply_text("Файл базы не найден.")
            return
        filename = f"sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        await query.message.reply_document(InputFile(str(db_path), filename=filename))
        return

    if data == "admin:db:upload":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        set_admin_state(context, "admin_db_upload")
        await query.message.reply_text("Отправьте файл базы (sales.db).")
        return

    if data.startswith("buy:"):
        plan_key = data.split(":", 1)[1]
        plan = cfg.plans.get(plan_key)
        if not plan:
            await query.message.reply_text("Неизвестный тариф.")
            return

        if is_priority_user(cfg, user_id):
            license_row = apply_paid_plan(cfg, user_id, plan.key)
            if license_row["token_encrypted"]:
                supervisor.sync_user(user_id)
                await query.message.reply_text(
                    "У вас приоритет: подписка выдана бесплатно.\n"
                    f"Тариф: {plan.title}\n"
                    f"Действует до: {format_expiration(license_row['expires_at'])}",
                    reply_markup=main_menu(cfg),
                )
            else:
                await query.message.reply_text(
                    "У вас приоритет: подписка выдана бесплатно.\n"
                    "Теперь отправьте токен бота в формате:\n123456:AA....",
                    reply_markup=main_menu(cfg),
                )
            return

        description = f"AUTO VBIV: {plan.title}"
        payload = f"user_{user_id}_{plan.key}_{now_ts()}"
        try:
            invoice = await asyncio.to_thread(
                crypto.create_invoice,
                plan.price_usdt,
                description,
                payload,
            )
        except Exception as exc:
            await query.message.reply_text(
                "Не удалось создать счет.\n"
                f"Ошибка: {exc}\n\n"
                "Проверьте CRYPTO_PAY_TOKEN и права приложения в Crypto Pay."
            )
            return

        invoice_id = int(invoice["invoice_id"])
        pay_url = str(invoice["pay_url"])
        order_id = create_order(
            cfg=cfg,
            user_id=user_id,
            plan_key=plan.key,
            amount_usdt=plan.price_usdt,
            invoice_id=invoice_id,
            invoice_url=pay_url,
        )
        text = (
            f"Тариф: {plan.title}\n"
            f"Сумма: {plan.price_usdt:.2f} USDT\n\n"
            "Нажмите «Оплатить», затем «Проверить оплату»."
        )
        await query.message.reply_text(text, reply_markup=payment_menu(pay_url, order_id))
        return

    if data.startswith("check:"):
        order_id_raw = data.split(":", 1)[1]
        try:
            order_id = int(order_id_raw)
        except ValueError:
            await query.message.reply_text("Некорректный ID заказа.")
            return

        order = get_order(cfg, order_id)
        if not order or int(order["user_id"]) != user_id:
            await query.message.reply_text("Заказ не найден.")
            return

        if order["status"] == "paid":
            row = get_license(cfg, user_id)
            if row and row["token_encrypted"]:
                supervisor.sync_user(user_id)
                await query.message.reply_text(
                    f"Оплата уже подтверждена.\n{describe_license(row)}",
                    reply_markup=main_menu(cfg),
                )
            else:
                await query.message.reply_text(
                    "Оплата подтверждена. Теперь отправьте токен бота сообщением.",
                    reply_markup=main_menu(cfg),
                )
            return

        try:
            invoice = await asyncio.to_thread(crypto.get_invoice, int(order["invoice_id"]))
        except Exception as exc:
            await query.message.reply_text(f"Ошибка проверки оплаты: {exc}")
            return

        if not invoice:
            await query.message.reply_text("Счет не найден в Crypto API.")
            return

        status = str(invoice.get("status", "unknown")).lower()
        if status == "paid":
            mark_order_status(cfg, order_id, "paid")
            license_row = apply_paid_plan(cfg, user_id, str(order["plan_key"]))

            if license_row["token_encrypted"]:
                supervisor.sync_user(user_id)
                text = (
                    "Оплата успешна. Подписка продлена.\n"
                    f"Действует до: {format_expiration(license_row['expires_at'])}"
                )
                await query.message.reply_text(text, reply_markup=main_menu(cfg))
            else:
                await query.message.reply_text(
                    "Оплата успешна. Теперь отправьте токен в формате:\n123456:AA....",
                    reply_markup=main_menu(cfg),
                )
            return

        if status in ("expired", "cancelled", "canceled"):
            mark_order_status(cfg, order_id, "expired")
            await query.message.reply_text("Счет неактивен. Создайте новый платеж.")
            return

        await query.message.reply_text(f"Счет еще не оплачен. Статус: {status}")
        return

    if data == "admin:clients":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return

        rows = list_licenses_with_users(cfg)
        if not rows:
            await query.message.reply_text("Клиентов пока нет.")
            return

        lines = [f"Клиентов: {len(rows)}"]
        for row in rows[:40]:
            uid = row["user_id"]
            buyer_username = row["username"] or "-"
            name_parts = [row["first_name"] or "", row["last_name"] or ""]
            buyer_name = " ".join(part for part in name_parts if part).strip() or "-"
            bot_username = row["bot_username"] or "-"
            status = row["status"]
            exp = format_expiration(row["expires_at"])
            priority_flag = "PRIORITY" if is_priority_user(cfg, int(uid)) else "-"
            lines.append(
                f"{uid} | @{buyer_username} | {buyer_name} | бот @{bot_username} | {row['plan_key']} | "
                f"{status} | до {exp} | {priority_flag}"
            )
        await query.message.reply_text("\n".join(lines), reply_markup=admin_panel_menu())
        return

    if data == "admin:priority_list":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("Нет доступа.")
            return
        rows = list_priority_users(cfg)
        if not rows:
            await query.message.reply_text("Список приоритета пуст.", reply_markup=admin_panel_menu())
            return
        lines = [f"Приоритетных пользователей: {len(rows)}"]
        for row in rows[:80]:
            uname = row["username"] or "-"
            name_parts = [row["first_name"] or "", row["last_name"] or ""]
            full_name = " ".join(part for part in name_parts if part).strip() or "-"
            lines.append(f"{row['user_id']} | @{uname} | {full_name}")
        await query.message.reply_text("\n".join(lines), reply_markup=admin_panel_menu())
        return


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return
    msg = update.effective_message
    text = (msg.text or msg.caption or "").strip()
    has_photo = bool(getattr(msg, "photo", None))
    if not text and not has_photo:
        return
    if text.startswith("/") and not has_photo:
        return

    cfg, _, supervisor, cipher = get_runtime(context)
    user_id = int(update.effective_user.id)
    upsert_user(cfg, update.effective_user)

    if is_owner(cfg, user_id):
        if await handle_admin_state_input(update, context, cfg, supervisor):
            return
        action, target = parse_priority_command(text)
        if action:
            if not target:
                await update.effective_message.reply_text(
                    "Укажите пользователя: @username или id.\n"
                    "Пример: выдать приоритет @user",
                    reply_markup=admin_panel_menu(),
                )
                return
            user_row = resolve_user_by_username_or_id(cfg, target)
            if user_row is None:
                await update.effective_message.reply_text(
                    "Пользователь не найден в базе sales-бота.\n"
                    "Пусть сначала нажмет /start в боте продаж.",
                    reply_markup=admin_panel_menu(),
                )
                return
            target_id = int(user_row["user_id"])
            target_username = (user_row["username"] or "").strip()
            if action == "grant":
                grant_priority(cfg, target_id, target_username, user_id)
                await update.effective_message.reply_text(
                    f"Приоритет выдан: {target_id} (@{target_username or '-'})",
                    reply_markup=admin_panel_menu(),
                )
            else:
                removed = revoke_priority(cfg, target_id)
                if removed:
                    await update.effective_message.reply_text(
                        f"Приоритет снят: {target_id} (@{target_username or '-'})",
                        reply_markup=admin_panel_menu(),
                    )
                else:
                    await update.effective_message.reply_text(
                        "У этого пользователя не было приоритета.",
                        reply_markup=admin_panel_menu(),
                    )
            return

        if text.strip().lower() in ("админ", "admin", "статистика", "stats"):
            await update.effective_message.reply_text(
                stats_text(cfg, supervisor),
                reply_markup=admin_panel_menu(),
            )
            return

    row = get_license(cfg, user_id)
    if row is None:
        return

    if row["status"] not in ("pending_token", "active", "expired", "stopped"):
        return

    if not TOKEN_RE.match(text):
        if row["status"] == "pending_token":
            await update.effective_message.reply_text(
                "После оплаты нужно отправить токен бота в виде:\n123456:AA...."
            )
        return

    if text == cfg.sales_bot_token:
        await update.effective_message.reply_text(
            "Этот токен принадлежит боту продаж. Его нельзя привязывать как клиентского.\n"
            "Создайте отдельного бота в @BotFather и отправьте его токен."
        )
        return

    for other in list_licenses(cfg):
        if not other["token_encrypted"]:
            continue
        if int(other["user_id"]) == user_id:
            continue
        try:
            other_token = cipher.decrypt(other["token_encrypted"])
        except Exception:
            continue
        if other_token == text:
            await update.effective_message.reply_text(
                "Этот токен уже привязан к другому пользователю. Нужен уникальный токен бота."
            )
            return

    ok, bot_info, err = await asyncio.to_thread(verify_bot_token, text)
    if not ok:
        await update.effective_message.reply_text(
            f"Токен не прошел проверку.\n{err}\n\nПовторите отправку токена."
        )
        return

    bot_id = int(bot_info.get("id"))
    bot_username = str(bot_info.get("username", "")).strip().lstrip("@")
    updated = attach_token_to_license(
        cfg=cfg,
        cipher=cipher,
        user_id=user_id,
        token_plain=text,
        bot_id=bot_id,
        bot_username=bot_username,
    )
    supervisor.sync_user(user_id)

    if updated["status"] == "expired":
        await update.effective_message.reply_text(
            "Токен сохранен, но подписка уже истекла. Оформите продление в меню.",
            reply_markup=main_menu(cfg),
        )
        return

    await asyncio.sleep(0.4)
    if not supervisor.is_running(user_id):
        await update.effective_message.reply_text(
            "Токен сохранен, но бот не запустился.\n"
            "Проверьте логи и права на хостинге. Лог: "
            f"{updated['instance_dir']}/bot_stderr.log"
        )
        return

    await update.effective_message.reply_text(
        "Токен принят. Ваш бот запущен.\n"
        f"Username: @{bot_username}\n"
        f"Действует до: {format_expiration(updated['expires_at'])}",
        reply_markup=main_menu(cfg),
    )


def main() -> None:
    project_root = Path(__file__).resolve().parent
    cfg = SalesConfig.from_env(project_root=project_root)
    ensure_dirs(cfg)
    init_db(cfg)
    sync_plan_prices(cfg)

    key = ensure_secret_key(cfg)
    cipher = TokenCipher(key)
    crypto = CryptoPayClient(cfg.crypto_pay_token, cfg.crypto_api_base, cfg.crypto_asset)
    supervisor = InstanceSupervisor(cfg, cipher)
    supervisor.start()

    app = ApplicationBuilder().token(cfg.sales_bot_token).build()
    app.bot_data[APP_CFG] = cfg
    app.bot_data[APP_CRYPTO] = crypto
    app.bot_data[APP_SUPERVISOR] = supervisor
    app.bot_data[APP_CIPHER] = cipher

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.Document.ALL, handle_document))

    try:
        app.run_polling()
    finally:
        supervisor.shutdown()


if __name__ == "__main__":
    main()
