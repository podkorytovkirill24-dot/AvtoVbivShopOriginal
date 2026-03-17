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
    "РЅРµРґРµР»СЏ": "week",
    "7": "week",
    "month": "month",
    "РјРµСЃСЏС†": "month",
    "30": "month",
    "lifetime": "lifetime",
    "РЅР°РІСЃРµРіРґР°": "lifetime",
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
        [InlineKeyboardButton(f"рџ›’ РљСѓРїРёС‚СЊ РЅРµРґРµР»СЋ ({week.price_usdt:.2f} USDT)", callback_data="buy:week")],
        [InlineKeyboardButton(f"рџ›’ РљСѓРїРёС‚СЊ РјРµСЃСЏС† ({month.price_usdt:.2f} USDT)", callback_data="buy:month")],
        [InlineKeyboardButton(f"рџЏ† РљСѓРїРёС‚СЊ РЅР°РІСЃРµРіРґР° ({lifetime.price_usdt:.2f} USDT)", callback_data="buy:lifetime")],
    ]
    return InlineKeyboardMarkup(rows)


def payment_menu(pay_url: str, order_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("рџ’і РћРїР»Р°С‚РёС‚СЊ", url=pay_url)],
        [InlineKeyboardButton("вњ… РџСЂРѕРІРµСЂРёС‚СЊ РѕРїР»Р°С‚Сѓ", callback_data=f"check:{order_id}")],
        [InlineKeyboardButton("рџЏ  Р’ РјРµРЅСЋ", callback_data="to_menu")],
    ]
    return InlineKeyboardMarkup(rows)


def owner_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("рџ›  РђРґРјРёРЅ-РїР°РЅРµР»СЊ", callback_data="admin:panel")]]
    )


def admin_panel_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°", callback_data="admin:stats")],
        [InlineKeyboardButton("рџ‘Ґ РљР»РёРµРЅС‚С‹", callback_data="admin:clients")],
        [InlineKeyboardButton("рџ§ѕ РџРѕРґРїРёСЃРєРё", callback_data="admin:subs")],
        [InlineKeyboardButton("рџЋЃ РђРєС†РёРё", callback_data="admin:promos")],
        [InlineKeyboardButton("рџ“ў Р Р°СЃСЃС‹Р»РєР°", callback_data="admin:broadcast")],
        [InlineKeyboardButton("в­ђ РџСЂРёРѕСЂРёС‚РµС‚", callback_data="admin:priority_list")],
        [InlineKeyboardButton("рџ—„ Р‘Р°Р·Р°", callback_data="admin:db")],
        [InlineKeyboardButton("рџЏ  Р’ РјРµРЅСЋ", callback_data="to_menu")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_subs_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("рџ—‘ РЈРґР°Р»РёС‚СЊ РїРѕРґРїРёСЃРєСѓ", callback_data="admin:subs:delete")],
        [InlineKeyboardButton("рџ•’ РР·РјРµРЅРёС‚СЊ РґР°С‚Сѓ РїРѕРєСѓРїРєРё", callback_data="admin:subs:backdate")],
        [InlineKeyboardButton("в¬… РќР°Р·Р°Рґ", callback_data="admin:panel")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_promos_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("вћ• Р”РѕР±Р°РІРёС‚СЊ Р°РєС†РёСЋ", callback_data="admin:promo:add")],
        [InlineKeyboardButton("вњЏпёЏ РР·РјРµРЅРёС‚СЊ Р°РєС†РёСЋ", callback_data="admin:promo:edit")],
        [InlineKeyboardButton("рџ—‘ РЈРґР°Р»РёС‚СЊ Р°РєС†РёСЋ", callback_data="admin:promo:delete")],
        [InlineKeyboardButton("рџ”› Р’РєР»/Р’С‹РєР»", callback_data="admin:promo:toggle")],
        [InlineKeyboardButton("рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°", callback_data="admin:promo:stats")],
        [InlineKeyboardButton("в¬… РќР°Р·Р°Рґ", callback_data="admin:panel")],
    ]
    return InlineKeyboardMarkup(rows)


def admin_db_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("в¬‡пёЏ РЎРєР°С‡Р°С‚СЊ Р±Р°Р·Сѓ", callback_data="admin:db:download")],
        [InlineKeyboardButton("в¬†пёЏ Р—Р°РіСЂСѓР·РёС‚СЊ Р±Р°Р·Сѓ", callback_data="admin:db:upload")],
        [InlineKeyboardButton("в¬… РќР°Р·Р°Рґ", callback_data="admin:panel")],
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
    if low.startswith("РІС‹РґР°С‚СЊ РїСЂРёРѕСЂРёС‚РµС‚ "):
        target = raw[len("РІС‹РґР°С‚СЊ РїСЂРёРѕСЂРёС‚РµС‚ ") :].strip()
        return "grant", target
    if low.startswith("СЃРЅСЏС‚СЊ РїСЂРёРѕСЂРёС‚РµС‚ "):
        target = raw[len("СЃРЅСЏС‚СЊ РїСЂРёРѕСЂРёС‚РµС‚ ") :].strip()
        return "revoke", target
    return None, ""


def stats_text(cfg: SalesConfig, supervisor: InstanceSupervisor) -> str:
    stats = get_admin_stats(cfg)
    running = 0
    for row in list_licenses(cfg):
        if supervisor.is_running(int(row["user_id"])):
            running += 1
    return (
        "рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°\n\n"
        f"рџ‘Ґ РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {int(stats['users_total'])}\n"
        f"рџ§ѕ Р—Р°РєР°Р·РѕРІ РІСЃРµРіРѕ: {int(stats['orders_total'])}\n"
        f"вњ… РћРїР»Р°С‡РµРЅРѕ: {int(stats['orders_paid'])}\n"
        f"вЏі Р’ РѕР¶РёРґР°РЅРёРё: {int(stats['orders_pending'])}\n"
        f"вЊ› РСЃС‚РµРєС€РёС… РёРЅРІРѕР№СЃРѕРІ: {int(stats['orders_expired'])}\n"
        f"рџ’° Р’С‹СЂСѓС‡РєР° (paid): {stats['revenue_paid_usdt']:.2f} USDT\n\n"
        f"рџЋ« Р›РёС†РµРЅР·РёР№ РІСЃРµРіРѕ: {int(stats['licenses_total'])}\n"
        f"рџџў РђРєС‚РёРІРЅС‹С…: {int(stats['licenses_active'])}\n"
        f"рџ”‘ РћР¶РёРґР°СЋС‚ С‚РѕРєРµРЅ: {int(stats['licenses_pending_token'])}\n"
        f"в›” РСЃС‚РµРєР»Рё: {int(stats['licenses_expired'])}\n"
        f"рџ›‘ РћСЃС‚Р°РЅРѕРІР»РµРЅС‹: {int(stats['licenses_stopped'])}\n"
        f"вљ™пёЏ РЎРµР№С‡Р°СЃ Р·Р°РїСѓС‰РµРЅРѕ РёРЅСЃС‚Р°РЅСЃРѕРІ: {running}\n\n"
        f"в­ђ РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№ СЃ РїСЂРёРѕСЂРёС‚РµС‚РѕРј: {int(stats['priority_users'])}\n\n"
        "РљРѕРјР°РЅРґС‹:\n"
        "РІС‹РґР°С‚СЊ РїСЂРёРѕСЂРёС‚РµС‚ @username\n"
        "СЃРЅСЏС‚СЊ РїСЂРёРѕСЂРёС‚РµС‚ @username"
    )


def promo_list_text(rows) -> str:
    if not rows:
        return "рџЋЃ РђРєС†РёРё\n\nРџРѕРєР° РЅРµС‚ Р°РєС‚РёРІРЅС‹С…/СЃРѕР·РґР°РЅРЅС‹С… Р°РєС†РёР№."
    lines = ["рџЋЃ РђРєС†РёРё", ""]
    for r in rows:
        status = "вњ…" if int(r["is_active"] or 0) else "вќЊ"
        lines.append(
            f"{r['id']} | {status} {r['title']} | {r['plan_key']} | +{r['bonus_days']} РґРЅ | "
            f"{format_date(r['start_ts'])}вЂ“{format_date(r['end_ts'])}"
        )
    return "\n".join(lines)


def promo_stats_text(stats: dict) -> str:
    promo = stats["promo"]
    status = "вњ…" if int(promo["is_active"] or 0) else "вќЊ"
    return (
        "рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР° Р°РєС†РёРё\n\n"
        f"ID: {promo['id']}\n"
        f"{status} {promo['title']}\n"
        f"РџР»Р°РЅ: {promo['plan_key']}\n"
        f"Р‘РѕРЅСѓСЃ: +{promo['bonus_days']} РґРЅ\n"
        f"РџРµСЂРёРѕРґ: {format_date(promo['start_ts'])}вЂ“{format_date(promo['end_ts'])}\n\n"
        f"РћРїР»Р°С‡РµРЅРЅС‹С… Р·Р°РєР°Р·РѕРІ: {stats['orders_paid']}\n"
        f"РЈРЅРёРєР°Р»СЊРЅС‹С… РїРѕРєСѓРїР°С‚РµР»РµР№: {stats['users_paid']}\n"
        f"Р’С‹СЂСѓС‡РєР°: {stats['revenue_usdt']:.2f} USDT"
    )


def _validate_sqlite_db(path: Path) -> tuple[bool, str]:
    try:
        with path.open("rb") as handle:
            header = handle.read(16)
        if header != b"SQLite format 3\x00":
            return False, "Р¤Р°Р№Р» РЅРµ РїРѕС…РѕР¶ РЅР° SQLite Р±Р°Р·Сѓ."
        conn = sqlite3.connect(str(path))
        row = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        if not row or row[0] != "ok":
            return False, f"Integrity check: {row[0] if row else 'error'}"
    except Exception as exc:
        return False, f"РћС€РёР±РєР° SQLite: {exc}"
    return True, ""


def _export_sqlite_db(src_path: Path, dst_path: Path) -> None:
    if dst_path.exists():
        try:
            dst_path.unlink()
        except Exception:
            pass
    conn = sqlite3.connect(str(src_path))
    try:
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        try:
            conn.execute("VACUUM INTO ?", (str(dst_path),))
            return
        except Exception:
            pass
        dst = sqlite3.connect(str(dst_path))
        try:
            conn.backup(dst)
        finally:
            dst.close()
    finally:
        conn.close()


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

    if text.lower() in ("РѕС‚РјРµРЅР°", "cancel", "/cancel"):
        clear_admin_state(context)
        await update.effective_message.reply_text("Р”РµР№СЃС‚РІРёРµ РѕС‚РјРµРЅРµРЅРѕ.", reply_markup=admin_panel_menu())
        return True

    name = state.get("name")
    data = state.get("data", {})

    if name == "admin_db_upload":
        await update.effective_message.reply_text("РџСЂРёС€Р»РёС‚Рµ С„Р°Р№Р» Р±Р°Р·С‹ .db (РґРѕРєСѓРјРµРЅС‚РѕРј).")
        return True

    if name == "admin_delete_sub":
        user_row = resolve_user_by_username_or_id(cfg, text)
        if user_row is None:
            await update.effective_message.reply_text("РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ. Р’РІРµРґРёС‚Рµ ID РёР»Рё @username.")
            return True
        deleted = delete_license(cfg, int(user_row["user_id"]))
        supervisor.sync_user(int(user_row["user_id"]))
        clear_admin_state(context)
        if deleted:
            await update.effective_message.reply_text("РџРѕРґРїРёСЃРєР° СѓРґР°Р»РµРЅР°.", reply_markup=admin_panel_menu())
        else:
            await update.effective_message.reply_text("РџРѕРґРїРёСЃРєР° РЅРµ РЅР°Р№РґРµРЅР°.", reply_markup=admin_panel_menu())
        return True

    if name == "admin_backdate_sub":
        match = DATE_RE.search(text)
        if not match:
            await update.effective_message.reply_text("РЈРєР°Р¶РёС‚Рµ РґР°С‚Сѓ РІ С„РѕСЂРјР°С‚Рµ Р”Р”.РњРњ.Р“Р“Р“Р“.")
            return True
        date_str = match.group(1)
        target = text.replace(date_str, "").replace("|", " ").strip()
        if not target:
            await update.effective_message.reply_text("РЈРєР°Р¶РёС‚Рµ ID РёР»Рё @username РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
            return True
        user_row = resolve_user_by_username_or_id(cfg, target)
        if user_row is None:
            await update.effective_message.reply_text("РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ. Р’РІРµРґРёС‚Рµ ID РёР»Рё @username.")
            return True
        dt = parse_date(date_str)
        if not dt:
            await update.effective_message.reply_text("РќРµРІРµСЂРЅР°СЏ РґР°С‚Р°. Р¤РѕСЂРјР°С‚: Р”Р”.РњРњ.Р“Р“Р“Р“.")
            return True
        start_ts = int(dt.timestamp())
        updated = set_license_start_date(cfg, int(user_row["user_id"]), start_ts)
        supervisor.sync_user(int(user_row["user_id"]))
        clear_admin_state(context)
        if not updated:
            await update.effective_message.reply_text("РџРѕРґРїРёСЃРєР° РЅРµ РЅР°Р№РґРµРЅР°.", reply_markup=admin_panel_menu())
            return True
        await update.effective_message.reply_text(
            "Р”Р°С‚Р° РїРѕРєСѓРїРєРё РёР·РјРµРЅРµРЅР°.\n"
            f"РЎС‚Р°СЂС‚: {format_date(start_ts)}\n"
            f"РСЃС‚РµРєР°РµС‚: {format_expiration(updated['expires_at'])}",
            reply_markup=admin_panel_menu(),
        )
        return True

    if name == "admin_broadcast":
        if not text and not has_photo:
            await update.effective_message.reply_text("РџСЂРёС€Р»РёС‚Рµ С‚РµРєСЃС‚ РёР»Рё С„РѕС‚Рѕ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё.")
            return True

        button_text = None
        button_url = None
        cleaned_lines = []
        for line in (text.splitlines() if text else []):
            low = line.strip().lower()
            if low.startswith("РєРЅРѕРїРєР°:") or low.startswith("button:"):
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
            f"Р Р°СЃСЃС‹Р»РєР° Р·Р°РІРµСЂС€РµРЅР°.\nРћС‚РїСЂР°РІР»РµРЅРѕ: {sent}\nРћС€РёР±РѕРє: {failed}",
            reply_markup=admin_panel_menu(),
        )
        return True

    if name == "promo_add_title":
        data["title"] = text
        set_admin_state(context, "promo_add_plan", data)
        await update.effective_message.reply_text("РџР»Р°РЅ: week / month / lifetime")
        return True

    if name == "promo_add_plan":
        plan_key = parse_plan_key(text)
        if not plan_key:
            await update.effective_message.reply_text("РќРµРІРµСЂРЅС‹Р№ РїР»Р°РЅ. РСЃРїРѕР»СЊР·СѓР№С‚Рµ: week / month / lifetime.")
            return True
        data["plan_key"] = plan_key
        set_admin_state(context, "promo_add_start", data)
        await update.effective_message.reply_text("Р”Р°С‚Р° РЅР°С‡Р°Р»Р° (Р”Р”.РњРњ.Р“Р“Р“Р“):")
        return True

    if name == "promo_add_start":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("РќРµРІРµСЂРЅР°СЏ РґР°С‚Р°. Р¤РѕСЂРјР°С‚: Р”Р”.РњРњ.Р“Р“Р“Р“.")
            return True
        start_ts = int(dt.timestamp())
        data["start_ts"] = start_ts
        set_admin_state(context, "promo_add_end", data)
        await update.effective_message.reply_text("Р”Р°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ (Р”Р”.РњРњ.Р“Р“Р“Р“):")
        return True

    if name == "promo_add_end":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("РќРµРІРµСЂРЅР°СЏ РґР°С‚Р°. Р¤РѕСЂРјР°С‚: Р”Р”.РњРњ.Р“Р“Р“Р“.")
            return True
        end_ts = int((dt + timedelta(days=1) - timedelta(seconds=1)).timestamp())
        if end_ts < int(data.get("start_ts", 0)):
            await update.effective_message.reply_text("Р”Р°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ РјРµРЅСЊС€Рµ РґР°С‚С‹ РЅР°С‡Р°Р»Р°.")
            return True
        data["end_ts"] = end_ts
        set_admin_state(context, "promo_add_bonus", data)
        await update.effective_message.reply_text("Р‘РѕРЅСѓСЃ РґРЅРµР№ (С‡РёСЃР»Рѕ):")
        return True

    if name == "promo_add_bonus":
        try:
            bonus_days = int(text)
        except ValueError:
            await update.effective_message.reply_text("Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ Р±РѕРЅСѓСЃРЅС‹С… РґРЅРµР№.")
            return True
        promo_id = add_promotion(
            cfg,
            data.get("title", "РђРєС†РёСЏ"),
            data["plan_key"],
            bonus_days,
            int(data["start_ts"]),
            int(data["end_ts"]),
            1,
        )
        clear_admin_state(context)
        await update.effective_message.reply_text(
            f"РђРєС†РёСЏ РґРѕР±Р°РІР»РµРЅР°. ID: {promo_id}",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_edit_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Р’РІРµРґРёС‚Рµ С‡РёСЃР»РѕРІРѕР№ ID Р°РєС†РёРё.")
            return True
        promo = get_promotion(cfg, promo_id)
        if promo is None:
            await update.effective_message.reply_text("РђРєС†РёСЏ РЅРµ РЅР°Р№РґРµРЅР°.")
            return True
        data = {"promo_id": promo_id}
        set_admin_state(context, "promo_edit_title", data)
        await update.effective_message.reply_text("РќРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ Р°РєС†РёРё:")
        return True

    if name == "promo_edit_title":
        data["title"] = text
        set_admin_state(context, "promo_edit_plan", data)
        await update.effective_message.reply_text("РџР»Р°РЅ: week / month / lifetime")
        return True

    if name == "promo_edit_plan":
        plan_key = parse_plan_key(text)
        if not plan_key:
            await update.effective_message.reply_text("РќРµРІРµСЂРЅС‹Р№ РїР»Р°РЅ. РСЃРїРѕР»СЊР·СѓР№С‚Рµ: week / month / lifetime.")
            return True
        data["plan_key"] = plan_key
        set_admin_state(context, "promo_edit_start", data)
        await update.effective_message.reply_text("Р”Р°С‚Р° РЅР°С‡Р°Р»Р° (Р”Р”.РњРњ.Р“Р“Р“Р“):")
        return True

    if name == "promo_edit_start":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("РќРµРІРµСЂРЅР°СЏ РґР°С‚Р°. Р¤РѕСЂРјР°С‚: Р”Р”.РњРњ.Р“Р“Р“Р“.")
            return True
        data["start_ts"] = int(dt.timestamp())
        set_admin_state(context, "promo_edit_end", data)
        await update.effective_message.reply_text("Р”Р°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ (Р”Р”.РњРњ.Р“Р“Р“Р“):")
        return True

    if name == "promo_edit_end":
        dt = parse_date(text)
        if not dt:
            await update.effective_message.reply_text("РќРµРІРµСЂРЅР°СЏ РґР°С‚Р°. Р¤РѕСЂРјР°С‚: Р”Р”.РњРњ.Р“Р“Р“Р“.")
            return True
        end_ts = int((dt + timedelta(days=1) - timedelta(seconds=1)).timestamp())
        if end_ts < int(data.get("start_ts", 0)):
            await update.effective_message.reply_text("Р”Р°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ РјРµРЅСЊС€Рµ РґР°С‚С‹ РЅР°С‡Р°Р»Р°.")
            return True
        data["end_ts"] = end_ts
        set_admin_state(context, "promo_edit_bonus", data)
        await update.effective_message.reply_text("Р‘РѕРЅСѓСЃ РґРЅРµР№ (С‡РёСЃР»Рѕ):")
        return True

    if name == "promo_edit_bonus":
        try:
            bonus_days = int(text)
        except ValueError:
            await update.effective_message.reply_text("Р’РІРµРґРёС‚Рµ С‡РёСЃР»Рѕ Р±РѕРЅСѓСЃРЅС‹С… РґРЅРµР№.")
            return True
        updated = update_promotion(
            cfg,
            int(data["promo_id"]),
            data.get("title", "РђРєС†РёСЏ"),
            data["plan_key"],
            bonus_days,
            int(data["start_ts"]),
            int(data["end_ts"]),
            1,
        )
        clear_admin_state(context)
        await update.effective_message.reply_text(
            "РђРєС†РёСЏ РѕР±РЅРѕРІР»РµРЅР°." if updated else "РђРєС†РёСЏ РЅРµ РЅР°Р№РґРµРЅР°.",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_delete_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Р’РІРµРґРёС‚Рµ С‡РёСЃР»РѕРІРѕР№ ID Р°РєС†РёРё.")
            return True
        deleted = delete_promotion(cfg, promo_id)
        clear_admin_state(context)
        await update.effective_message.reply_text(
            "РђРєС†РёСЏ СѓРґР°Р»РµРЅР°." if deleted else "РђРєС†РёСЏ РЅРµ РЅР°Р№РґРµРЅР°.",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_toggle_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Р’РІРµРґРёС‚Рµ С‡РёСЃР»РѕРІРѕР№ ID Р°РєС†РёРё.")
            return True
        promo = get_promotion(cfg, promo_id)
        if promo is None:
            await update.effective_message.reply_text("РђРєС†РёСЏ РЅРµ РЅР°Р№РґРµРЅР°.")
            return True
        new_active = 0 if int(promo["is_active"] or 0) else 1
        set_promotion_active(cfg, promo_id, new_active)
        clear_admin_state(context)
        await update.effective_message.reply_text(
            f"РђРєС†РёСЏ {'РІРєР»СЋС‡РµРЅР°' if new_active else 'РІС‹РєР»СЋС‡РµРЅР°'}.",
            reply_markup=admin_promos_menu(),
        )
        return True

    if name == "promo_stats_id":
        try:
            promo_id = int(text)
        except ValueError:
            await update.effective_message.reply_text("Р’РІРµРґРёС‚Рµ С‡РёСЃР»РѕРІРѕР№ ID Р°РєС†РёРё.")
            return True
        stats = get_promo_stats(cfg, promo_id)
        clear_admin_state(context)
        if not stats:
            await update.effective_message.reply_text("РђРєС†РёСЏ РЅРµ РЅР°Р№РґРµРЅР°.", reply_markup=admin_promos_menu())
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
    await msg.reply_text("вЏі Р—Р°РіСЂСѓР¶Р°СЋ Р±Р°Р·Сѓ...")

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
                    raise RuntimeError("Р’ Р°СЂС…РёРІРµ РЅРµС‚ .db С„Р°Р№Р»Р°.")
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
        backup_note = f"\nР РµР·РµСЂРІРЅР°СЏ РєРѕРїРёСЏ: {backup_path.name}" if backup_path else ""
        await msg.reply_text(f"вњ… Р‘Р°Р·Р° Р·Р°РіСЂСѓР¶РµРЅР°.{backup_note}", reply_markup=admin_panel_menu())
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
        await msg.reply_text(f"вќЊ РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё Р±Р°Р·С‹: {exc}", reply_markup=admin_panel_menu())


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, _, _, _ = get_runtime(context)
    upsert_user(cfg, update.effective_user)
    text = (
        "рџљЂ AUTO VBIV вЂ” РїРѕРґРїРёСЃРєР° Рё Р·Р°РїСѓСЃРє Р±РѕС‚Р°\n\n"
        "1) Р’С‹Р±РµСЂРёС‚Рµ С‚Р°СЂРёС„.\n"
        "2) РћРїР»Р°С‚РёС‚Рµ С‡РµСЂРµР· Crypto Bot.\n"
        "3) РћС‚РїСЂР°РІСЊС‚Рµ С‚РѕРєРµРЅ СЃРІРѕРµРіРѕ Р±РѕС‚Р°.\n"
        "4) Р—Р°РїСѓСЃРє РїСЂРѕРёСЃС…РѕРґРёС‚ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё.\n\n"
        "в„№пёЏ РљРѕРјР°РЅРґР°: /status"
    )
    await update.effective_message.reply_text(text, reply_markup=main_menu(cfg))

    if int(update.effective_user.id) in cfg.owner_ids:
        await update.effective_message.reply_text("рџ›  Р РµР¶РёРј РІР»Р°РґРµР»СЊС†Р°.", reply_markup=owner_menu())


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, _, supervisor, _ = get_runtime(context)
    user_id = int(update.effective_user.id)
    row = get_license(cfg, user_id)
    text = describe_license(row)
    if row is not None:
        running = "Р”Р°" if supervisor.is_running(user_id) else "РќРµС‚"
        text += f"\nР—Р°РїСѓС‰РµРЅ: {running}"
    await update.effective_message.reply_text(text, reply_markup=main_menu(cfg))


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, _, supervisor, _ = get_runtime(context)
    user_id = int(update.effective_user.id)
    if not is_owner(cfg, user_id):
        await update.effective_message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
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
        await query.message.reply_text("Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ:", reply_markup=main_menu(cfg))
        return

    if data == "my_license":
        await query.message.reply_text("Р Р°Р·РґРµР» РѕС‚РєР»СЋС‡РµРЅ.", reply_markup=main_menu(cfg))
        return

    if data == "admin:panel":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        await query.message.reply_text(stats_text(cfg, supervisor), reply_markup=admin_panel_menu())
        return

    if data == "admin:stats":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        await query.message.reply_text(stats_text(cfg, supervisor), reply_markup=admin_panel_menu())
        return

    if data == "admin:subs":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        await query.message.reply_text("рџ§ѕ РЈРїСЂР°РІР»РµРЅРёРµ РїРѕРґРїРёСЃРєР°РјРё:", reply_markup=admin_subs_menu())
        return

    if data == "admin:subs:delete":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "admin_delete_sub")
        await query.message.reply_text("Р’РІРµРґРёС‚Рµ ID РёР»Рё @username РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РґР»СЏ СѓРґР°Р»РµРЅРёСЏ РїРѕРґРїРёСЃРєРё:")
        return

    if data == "admin:subs:backdate":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "admin_backdate_sub")
        await query.message.reply_text("Р’РІРµРґРёС‚Рµ ID/@username Рё РґР°С‚Сѓ РїРѕРєСѓРїРєРё (Р”Р”.РњРњ.Р“Р“Р“Р“).\nРџСЂРёРјРµСЂ: 123456 10.03.2026")
        return

    if data == "admin:promos":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        promos = list_promotions(cfg)
        await query.message.reply_text(promo_list_text(promos), reply_markup=admin_promos_menu())
        return

    if data == "admin:promo:add":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "promo_add_title")
        await query.message.reply_text("Р’РІРµРґРёС‚Рµ РЅР°Р·РІР°РЅРёРµ Р°РєС†РёРё:")
        return

    if data == "admin:promo:edit":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "promo_edit_id")
        await query.message.reply_text("Р’РІРµРґРёС‚Рµ ID Р°РєС†РёРё РґР»СЏ РёР·РјРµРЅРµРЅРёСЏ:")
        return

    if data == "admin:promo:delete":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "promo_delete_id")
        await query.message.reply_text("Р’РІРµРґРёС‚Рµ ID Р°РєС†РёРё РґР»СЏ СѓРґР°Р»РµРЅРёСЏ:")
        return

    if data == "admin:promo:toggle":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "promo_toggle_id")
        await query.message.reply_text("Р’РІРµРґРёС‚Рµ ID Р°РєС†РёРё РґР»СЏ Р’РєР»/Р’С‹РєР»:")
        return

    if data == "admin:promo:stats":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "promo_stats_id")
        await query.message.reply_text("Р’РІРµРґРёС‚Рµ ID Р°РєС†РёРё РґР»СЏ СЃС‚Р°С‚РёСЃС‚РёРєРё:")
        return

    if data == "admin:broadcast":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "admin_broadcast")
        await query.message.reply_text(
            "РћС‚РїСЂР°РІСЊС‚Рµ СЃРѕРѕР±С‰РµРЅРёРµ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё (С‚РµРєСЃС‚ РёР»Рё С„РѕС‚Рѕ).\n"
            "РљРЅРѕРїРєР°: РґРѕР±Р°РІСЊС‚Рµ СЃС‚СЂРѕРєСѓ РІРёРґР°\n"
            "РљРЅРѕРїРєР°: РўРµРєСЃС‚ | https://example.com\n"
            "Р”Р»СЏ РѕС‚РјРµРЅС‹ РЅР°РїРёС€РёС‚Рµ В«РѕС‚РјРµРЅР°В»."
        )
        return

    if data == "admin:db":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        await query.message.reply_text("рџ—„ РЈРїСЂР°РІР»РµРЅРёРµ Р±Р°Р·РѕР№:", reply_markup=admin_db_menu())
        return

    if data == "admin:db:download":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        db_path = cfg.sales_db_path
        if not db_path.exists():
            await query.message.reply_text("Р¤Р°Р№Р» Р±Р°Р·С‹ РЅРµ РЅР°Р№РґРµРЅ.")
            return
        ok, err = _validate_sqlite_db(db_path)
        if not ok:
            size = db_path.stat().st_size if db_path.exists() else 0
            await query.message.reply_text(
                "Р‘Р°Р·Р° РїРѕРІСЂРµР¶РґРµРЅР° РёР»Рё РїСѓСЃС‚Р°СЏ.\n"
                f"Р Р°Р·РјРµСЂ С„Р°Р№Р»Р°: {size} Р±Р°Р№С‚\n"
                f"РџСѓС‚СЊ: {db_path}\n"
                f"РћС€РёР±РєР°: {err}"
            )
            return
        filename = f"sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        tmp_path = db_path.with_suffix(db_path.suffix + f".dump_{now_ts()}")
        try:
            _export_sqlite_db(db_path, tmp_path)
            await query.message.reply_document(InputFile(str(tmp_path), filename=filename))
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
        return

    if data == "admin:db:upload":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        set_admin_state(context, "admin_db_upload")
        await query.message.reply_text("РћС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» Р±Р°Р·С‹ (sales.db).")
        return

    if data.startswith("buy:"):
        plan_key = data.split(":", 1)[1]
        plan = cfg.plans.get(plan_key)
        if not plan:
            await query.message.reply_text("РќРµРёР·РІРµСЃС‚РЅС‹Р№ С‚Р°СЂРёС„.")
            return

        if is_priority_user(cfg, user_id):
            license_row = apply_paid_plan(cfg, user_id, plan.key)
            if license_row["token_encrypted"]:
                supervisor.sync_user(user_id)
                await query.message.reply_text(
                    "РЈ РІР°СЃ РїСЂРёРѕСЂРёС‚РµС‚: РїРѕРґРїРёСЃРєР° РІС‹РґР°РЅР° Р±РµСЃРїР»Р°С‚РЅРѕ.\n"
                    f"РўР°СЂРёС„: {plan.title}\n"
                    f"Р”РµР№СЃС‚РІСѓРµС‚ РґРѕ: {format_expiration(license_row['expires_at'])}",
                    reply_markup=main_menu(cfg),
                )
            else:
                await query.message.reply_text(
                    "РЈ РІР°СЃ РїСЂРёРѕСЂРёС‚РµС‚: РїРѕРґРїРёСЃРєР° РІС‹РґР°РЅР° Р±РµСЃРїР»Р°С‚РЅРѕ.\n"
                    "РўРµРїРµСЂСЊ РѕС‚РїСЂР°РІСЊС‚Рµ С‚РѕРєРµРЅ Р±РѕС‚Р° РІ С„РѕСЂРјР°С‚Рµ:\n123456:AA....",
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
                "РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР·РґР°С‚СЊ СЃС‡РµС‚.\n"
                f"РћС€РёР±РєР°: {exc}\n\n"
                "РџСЂРѕРІРµСЂСЊС‚Рµ CRYPTO_PAY_TOKEN Рё РїСЂР°РІР° РїСЂРёР»РѕР¶РµРЅРёСЏ РІ Crypto Pay."
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
            f"РўР°СЂРёС„: {plan.title}\n"
            f"РЎСѓРјРјР°: {plan.price_usdt:.2f} USDT\n\n"
            "РќР°Р¶РјРёС‚Рµ В«РћРїР»Р°С‚РёС‚СЊВ», Р·Р°С‚РµРј В«РџСЂРѕРІРµСЂРёС‚СЊ РѕРїР»Р°С‚СѓВ»."
        )
        await query.message.reply_text(text, reply_markup=payment_menu(pay_url, order_id))
        return

    if data.startswith("check:"):
        order_id_raw = data.split(":", 1)[1]
        try:
            order_id = int(order_id_raw)
        except ValueError:
            await query.message.reply_text("РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ ID Р·Р°РєР°Р·Р°.")
            return

        order = get_order(cfg, order_id)
        if not order or int(order["user_id"]) != user_id:
            await query.message.reply_text("Р—Р°РєР°Р· РЅРµ РЅР°Р№РґРµРЅ.")
            return

        if order["status"] == "paid":
            row = get_license(cfg, user_id)
            if row and row["token_encrypted"]:
                supervisor.sync_user(user_id)
                await query.message.reply_text(
                    f"РћРїР»Р°С‚Р° СѓР¶Рµ РїРѕРґС‚РІРµСЂР¶РґРµРЅР°.\n{describe_license(row)}",
                    reply_markup=main_menu(cfg),
                )
            else:
                await query.message.reply_text(
                    "РћРїР»Р°С‚Р° РїРѕРґС‚РІРµСЂР¶РґРµРЅР°. РўРµРїРµСЂСЊ РѕС‚РїСЂР°РІСЊС‚Рµ С‚РѕРєРµРЅ Р±РѕС‚Р° СЃРѕРѕР±С‰РµРЅРёРµРј.",
                    reply_markup=main_menu(cfg),
                )
            return

        try:
            invoice = await asyncio.to_thread(crypto.get_invoice, int(order["invoice_id"]))
        except Exception as exc:
            await query.message.reply_text(f"РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё РѕРїР»Р°С‚С‹: {exc}")
            return

        if not invoice:
            await query.message.reply_text("РЎС‡РµС‚ РЅРµ РЅР°Р№РґРµРЅ РІ Crypto API.")
            return

        status = str(invoice.get("status", "unknown")).lower()
        if status == "paid":
            mark_order_status(cfg, order_id, "paid")
            license_row = apply_paid_plan(cfg, user_id, str(order["plan_key"]))

            if license_row["token_encrypted"]:
                supervisor.sync_user(user_id)
                text = (
                    "РћРїР»Р°С‚Р° СѓСЃРїРµС€РЅР°. РџРѕРґРїРёСЃРєР° РїСЂРѕРґР»РµРЅР°.\n"
                    f"Р”РµР№СЃС‚РІСѓРµС‚ РґРѕ: {format_expiration(license_row['expires_at'])}"
                )
                await query.message.reply_text(text, reply_markup=main_menu(cfg))
            else:
                await query.message.reply_text(
                    "РћРїР»Р°С‚Р° СѓСЃРїРµС€РЅР°. РўРµРїРµСЂСЊ РѕС‚РїСЂР°РІСЊС‚Рµ С‚РѕРєРµРЅ РІ С„РѕСЂРјР°С‚Рµ:\n123456:AA....",
                    reply_markup=main_menu(cfg),
                )
            return

        if status in ("expired", "cancelled", "canceled"):
            mark_order_status(cfg, order_id, "expired")
            await query.message.reply_text("РЎС‡РµС‚ РЅРµР°РєС‚РёРІРµРЅ. РЎРѕР·РґР°Р№С‚Рµ РЅРѕРІС‹Р№ РїР»Р°С‚РµР¶.")
            return

        await query.message.reply_text(f"РЎС‡РµС‚ РµС‰Рµ РЅРµ РѕРїР»Р°С‡РµРЅ. РЎС‚Р°С‚СѓСЃ: {status}")
        return

    if data == "admin:clients":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return

        rows = list_licenses_with_users(cfg)
        if not rows:
            await query.message.reply_text("РљР»РёРµРЅС‚РѕРІ РїРѕРєР° РЅРµС‚.")
            return

        lines = [f"РљР»РёРµРЅС‚РѕРІ: {len(rows)}"]
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
                f"{uid} | @{buyer_username} | {buyer_name} | Р±РѕС‚ @{bot_username} | {row['plan_key']} | "
                f"{status} | РґРѕ {exp} | {priority_flag}"
            )
        await query.message.reply_text("\n".join(lines), reply_markup=admin_panel_menu())
        return

    if data == "admin:priority_list":
        if not is_owner(cfg, user_id):
            await query.message.reply_text("РќРµС‚ РґРѕСЃС‚СѓРїР°.")
            return
        rows = list_priority_users(cfg)
        if not rows:
            await query.message.reply_text("РЎРїРёСЃРѕРє РїСЂРёРѕСЂРёС‚РµС‚Р° РїСѓСЃС‚.", reply_markup=admin_panel_menu())
            return
        lines = [f"РџСЂРёРѕСЂРёС‚РµС‚РЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {len(rows)}"]
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
                    "РЈРєР°Р¶РёС‚Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: @username РёР»Рё id.\n"
                    "РџСЂРёРјРµСЂ: РІС‹РґР°С‚СЊ РїСЂРёРѕСЂРёС‚РµС‚ @user",
                    reply_markup=admin_panel_menu(),
                )
                return
            user_row = resolve_user_by_username_or_id(cfg, target)
            if user_row is None:
                await update.effective_message.reply_text(
                    "РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ РІ Р±Р°Р·Рµ sales-Р±РѕС‚Р°.\n"
                    "РџСѓСЃС‚СЊ СЃРЅР°С‡Р°Р»Р° РЅР°Р¶РјРµС‚ /start РІ Р±РѕС‚Рµ РїСЂРѕРґР°Р¶.",
                    reply_markup=admin_panel_menu(),
                )
                return
            target_id = int(user_row["user_id"])
            target_username = (user_row["username"] or "").strip()
            if action == "grant":
                grant_priority(cfg, target_id, target_username, user_id)
                await update.effective_message.reply_text(
                    f"РџСЂРёРѕСЂРёС‚РµС‚ РІС‹РґР°РЅ: {target_id} (@{target_username or '-'})",
                    reply_markup=admin_panel_menu(),
                )
            else:
                removed = revoke_priority(cfg, target_id)
                if removed:
                    await update.effective_message.reply_text(
                        f"РџСЂРёРѕСЂРёС‚РµС‚ СЃРЅСЏС‚: {target_id} (@{target_username or '-'})",
                        reply_markup=admin_panel_menu(),
                    )
                else:
                    await update.effective_message.reply_text(
                        "РЈ СЌС‚РѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РЅРµ Р±С‹Р»Рѕ РїСЂРёРѕСЂРёС‚РµС‚Р°.",
                        reply_markup=admin_panel_menu(),
                    )
            return

        if text.strip().lower() in ("Р°РґРјРёРЅ", "admin", "СЃС‚Р°С‚РёСЃС‚РёРєР°", "stats"):
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
                "РџРѕСЃР»Рµ РѕРїР»Р°С‚С‹ РЅСѓР¶РЅРѕ РѕС‚РїСЂР°РІРёС‚СЊ С‚РѕРєРµРЅ Р±РѕС‚Р° РІ РІРёРґРµ:\n123456:AA...."
            )
        return

    if text == cfg.sales_bot_token:
        await update.effective_message.reply_text(
            "Р­С‚РѕС‚ С‚РѕРєРµРЅ РїСЂРёРЅР°РґР»РµР¶РёС‚ Р±РѕС‚Сѓ РїСЂРѕРґР°Р¶. Р•РіРѕ РЅРµР»СЊР·СЏ РїСЂРёРІСЏР·С‹РІР°С‚СЊ РєР°Рє РєР»РёРµРЅС‚СЃРєРѕРіРѕ.\n"
            "РЎРѕР·РґР°Р№С‚Рµ РѕС‚РґРµР»СЊРЅРѕРіРѕ Р±РѕС‚Р° РІ @BotFather Рё РѕС‚РїСЂР°РІСЊС‚Рµ РµРіРѕ С‚РѕРєРµРЅ."
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
                "Р­С‚РѕС‚ С‚РѕРєРµРЅ СѓР¶Рµ РїСЂРёРІСЏР·Р°РЅ Рє РґСЂСѓРіРѕРјСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ. РќСѓР¶РµРЅ СѓРЅРёРєР°Р»СЊРЅС‹Р№ С‚РѕРєРµРЅ Р±РѕС‚Р°."
            )
            return

    try:
        ok, bot_info, err = await asyncio.wait_for(
            asyncio.to_thread(verify_bot_token, text),
            timeout=12,
        )
    except asyncio.TimeoutError:
        await update.effective_message.reply_text(
            "Проверка токена заняла слишком много времени.\n"
            "Похоже, хостинг не может достучаться до api.telegram.org.\n"
            "Попробуйте позже или проверьте доступ к Telegram API."
        )
        return
    except Exception as exc:
        await update.effective_message.reply_text(
            f"Ошибка проверки токена: {exc}\n"
            "Попробуйте еще раз чуть позже."
        )
        return
    if not ok:
        await update.effective_message.reply_text(
            f"РўРѕРєРµРЅ РЅРµ РїСЂРѕС€РµР» РїСЂРѕРІРµСЂРєСѓ.\n{err}\n\nРџРѕРІС‚РѕСЂРёС‚Рµ РѕС‚РїСЂР°РІРєСѓ С‚РѕРєРµРЅР°."
        )
        return

    try:
        bot_id = int(bot_info.get("id"))
    except Exception:
        await update.effective_message.reply_text(
            "Не удалось получить данные бота из Telegram.\n"
            "Проверьте токен и попробуйте еще раз."
        )
        return
    bot_username = str(bot_info.get("username", "")).strip().lstrip("@")
    try:
        updated = attach_token_to_license(
            cfg=cfg,
            cipher=cipher,
            user_id=user_id,
            token_plain=text,
            bot_id=bot_id,
            bot_username=bot_username,
        )
        supervisor.sync_user(user_id)
    except Exception as exc:
        await update.effective_message.reply_text(
            f"Не удалось сохранить токен/запустить бота: {exc}"
        )
        return

    if updated["status"] == "expired":
        await update.effective_message.reply_text(
            "РўРѕРєРµРЅ СЃРѕС…СЂР°РЅРµРЅ, РЅРѕ РїРѕРґРїРёСЃРєР° СѓР¶Рµ РёСЃС‚РµРєР»Р°. РћС„РѕСЂРјРёС‚Рµ РїСЂРѕРґР»РµРЅРёРµ РІ РјРµРЅСЋ.",
            reply_markup=main_menu(cfg),
        )
        return

    await asyncio.sleep(0.4)
    if not supervisor.is_running(user_id):
        await update.effective_message.reply_text(
            "РўРѕРєРµРЅ СЃРѕС…СЂР°РЅРµРЅ, РЅРѕ Р±РѕС‚ РЅРµ Р·Р°РїСѓСЃС‚РёР»СЃСЏ.\n"
            "РџСЂРѕРІРµСЂСЊС‚Рµ Р»РѕРіРё Рё РїСЂР°РІР° РЅР° С…РѕСЃС‚РёРЅРіРµ. Р›РѕРі: "
            f"{updated['instance_dir']}/bot_stderr.log"
        )
        return

    await update.effective_message.reply_text(
        "РўРѕРєРµРЅ РїСЂРёРЅСЏС‚. Р’Р°С€ Р±РѕС‚ Р·Р°РїСѓС‰РµРЅ.\n"
        f"Username: @{bot_username}\n"
        f"Р”РµР№СЃС‚РІСѓРµС‚ РґРѕ: {format_expiration(updated['expires_at'])}",
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

