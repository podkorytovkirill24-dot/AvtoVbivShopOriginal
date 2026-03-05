# -*- coding: utf-8 -*-
import asyncio
import re
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
    describe_license,
    ensure_dirs,
    ensure_secret_key,
    format_expiration,
    get_admin_stats,
    get_license,
    get_order,
    init_db,
    is_priority_user,
    list_licenses,
    list_priority_users,
    mark_order_status,
    now_ts,
    resolve_user_by_username_or_id,
    revoke_priority,
    sync_plan_prices,
    grant_priority,
    upsert_user,
    verify_bot_token,
)


APP_CFG = "app_cfg"
APP_CRYPTO = "crypto_client"
APP_SUPERVISOR = "supervisor"
APP_CIPHER = "cipher"
TOKEN_RE = re.compile(r"^\d{6,}:[A-Za-z0-9_-]{20,}$")


def main_menu(cfg: SalesConfig) -> InlineKeyboardMarkup:
    week = cfg.plans["week"]
    month = cfg.plans["month"]
    lifetime = cfg.plans["lifetime"]
    rows = [
        [InlineKeyboardButton(f"Купить неделю ({week.price_usdt:.2f} USDT)", callback_data="buy:week")],
        [InlineKeyboardButton(f"Купить месяц ({month.price_usdt:.2f} USDT)", callback_data="buy:month")],
        [InlineKeyboardButton(f"Купить навсегда ({lifetime.price_usdt:.2f} USDT)", callback_data="buy:lifetime")],
        [InlineKeyboardButton("Моя подписка", callback_data="my_license")],
    ]
    return InlineKeyboardMarkup(rows)


def payment_menu(pay_url: str, order_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Оплатить", url=pay_url)],
        [InlineKeyboardButton("Проверить оплату", callback_data=f"check:{order_id}")],
        [InlineKeyboardButton("В меню", callback_data="to_menu")],
    ]
    return InlineKeyboardMarkup(rows)


def owner_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Админ-панель", callback_data="admin:panel")]]
    )


def admin_panel_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton("Список клиентов", callback_data="admin:clients")],
        [InlineKeyboardButton("Список приоритета", callback_data="admin:priority_list")],
        [InlineKeyboardButton("В меню", callback_data="to_menu")],
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
        "Статистика\n\n"
        f"Пользователей: {int(stats['users_total'])}\n"
        f"Заказов всего: {int(stats['orders_total'])}\n"
        f"Оплачено: {int(stats['orders_paid'])}\n"
        f"В ожидании: {int(stats['orders_pending'])}\n"
        f"Истекших инвойсов: {int(stats['orders_expired'])}\n"
        f"Выручка (paid): {stats['revenue_paid_usdt']:.2f} USDT\n\n"
        f"Лицензий всего: {int(stats['licenses_total'])}\n"
        f"Активных: {int(stats['licenses_active'])}\n"
        f"Ожидают токен: {int(stats['licenses_pending_token'])}\n"
        f"Истекли: {int(stats['licenses_expired'])}\n"
        f"Остановлены: {int(stats['licenses_stopped'])}\n"
        f"Сейчас запущено инстансов: {running}\n\n"
        f"Пользователей с приоритетом: {int(stats['priority_users'])}\n\n"
        "Команды:\n"
        "выдать приоритет @username\n"
        "снять приоритет @username"
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, _, _, _ = get_runtime(context)
    upsert_user(cfg, update.effective_user)
    text = (
        "Продажа AUTO VBIV\n\n"
        "1) Выберите тариф.\n"
        "2) Оплатите через Crypto Bot.\n"
        "3) После оплаты отправьте токен своего бота.\n"
        "4) Бот запустится автоматически.\n\n"
        "Команда: /status"
    )
    await update.effective_message.reply_text(text, reply_markup=main_menu(cfg))

    if int(update.effective_user.id) in cfg.owner_ids:
        await update.effective_message.reply_text("Режим владельца.", reply_markup=owner_menu())


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

        rows = list_licenses(cfg)
        if not rows:
            await query.message.reply_text("Клиентов пока нет.")
            return

        lines = [f"Клиентов: {len(rows)}"]
        for row in rows[:40]:
            uid = row["user_id"]
            username = row["bot_username"] or "-"
            status = row["status"]
            exp = format_expiration(row["expires_at"])
            priority_flag = "PRIORITY" if is_priority_user(cfg, int(uid)) else "-"
            lines.append(f"{uid} | @{username} | {status} | до {exp} | {priority_flag}")
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
    text = (update.effective_message.text or "").strip()
    if not text:
        return
    if text.startswith("/"):
        return

    cfg, _, supervisor, cipher = get_runtime(context)
    user_id = int(update.effective_user.id)
    upsert_user(cfg, update.effective_user)

    if is_owner(cfg, user_id):
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
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_text))

    try:
        app.run_polling()
    finally:
        supervisor.shutdown()


if __name__ == "__main__":
    main()
