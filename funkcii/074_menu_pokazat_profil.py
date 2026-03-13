async def menu_show_profile(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message=None) -> None:
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
    ref_code = ensure_ref_code(conn, user_id)
    referral_enabled = get_config_bool(conn, "referral_enabled", True)
    invited = conn.execute(
        "SELECT COUNT(*) AS cnt FROM users WHERE referred_by = ?",
        (user_id,),
    ).fetchone()["cnt"]
    conn.close()

    ref_line = "🔗 Рефералка выключена в настройках"
    if referral_enabled:
        bot_username = await get_bot_username(context)
        if bot_username:
            ref_line = f"🔗 Реф. ссылка: https://t.me/{bot_username}?start={ref_code}"
        else:
            ref_line = f"🔗 Реф. код: {ref_code}"

    text_profile = (
        "👤 Профиль\n"
        f"ID: {user_id}\n"
        f"💰 Баланс: ${balance:.2f}\n\n"
        "📈 Активность\n"
        f"Всего: {stats['total']}\n"
        f"✅ Встал: {stats['success']} | ❌ Слёт: {stats['slip']} | ⚠️ Ошибка: {stats['error']}\n\n"
        f"{ref_line}\n"
        f"👥 Приглашено: {invited}"
    )
    rows = [[InlineKeyboardButton("💵 Запросить вывод", callback_data="user:withdraw")]]
    if MINI_APP_BASE_URL:
        rows.append([InlineKeyboardButton("✨ Мини-приложение", web_app=WebAppInfo(url=f"{MINI_APP_BASE_URL}/miniapp"))])
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="user:home")])
    keyboard = InlineKeyboardMarkup(rows)
    await send_or_update(context, chat_id, text_profile, reply_markup=keyboard, message=message)
