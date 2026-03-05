async def menu_show_profile(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    conn = get_conn()
    balance = calculate_user_balance(conn, user_id)
    stats = conn.execute(
        "SELECT "
        "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success, "
        "SUM(CASE WHEN status='slip' THEN 1 ELSE 0 END) AS slip, "
        "SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error, "
        "COUNT(*) AS total "
        "FROM queue_numbers WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    sub_until = conn.execute(
        "SELECT subscription_until FROM users WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    sub_text = "-"
    if sub_until and sub_until["subscription_until"]:
        sub_text = format_ts(sub_until["subscription_until"])
    ref_code = ensure_ref_code(conn, user_id)
    referral_enabled = get_config_bool(conn, "referral_enabled", True)
    invited = conn.execute(
        "SELECT COUNT(*) AS cnt FROM users WHERE referred_by = ?",
        (user_id,),
    ).fetchone()["cnt"]
    conn.close()
    ref_line = "Реф. ссылка: выключена в настройках"
    if referral_enabled:
        bot_username = await get_bot_username(context)
        if bot_username:
            ref_line = f"Реф. ссылка: https://t.me/{bot_username}?start={ref_code}"
        else:
            ref_line = f"Реф. ссылка: ref-код {ref_code}"
    text_profile = (
        "👤 Мой профиль\n"
        f"Баланс: ${balance:.2f}\n"
        f"Сдано: {stats['total']}\n"
        f"Встал: {stats['success']} | Слет: {stats['slip']} | Ошибка: {stats['error']}\n"
        f"Подписка до: {sub_text}\n"
        f"{ref_line}\n"
        f"Приглашено: {invited}"
    )
    rows = [[InlineKeyboardButton("💵 Запросить вывод", callback_data="user:withdraw")]]
    if MINI_APP_BASE_URL:
        rows.append([InlineKeyboardButton("✨ Мини-приложение", web_app=WebAppInfo(url=f"{MINI_APP_BASE_URL}/miniapp"))])
    rows.append([InlineKeyboardButton("🏠 Главное меню", callback_data="user:home")])
    keyboard = InlineKeyboardMarkup(rows)
    await context.bot.send_message(chat_id=chat_id, text=text_profile, reply_markup=keyboard)
