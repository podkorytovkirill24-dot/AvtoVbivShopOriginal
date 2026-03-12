async def menu_show_tariffs(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message=None) -> None:
    conn = get_conn()
    tariffs = conn.execute(
        "SELECT id, name, price, duration_min FROM tariffs ORDER BY id"
    ).fetchall()
    conn.close()
    if not tariffs:
        await send_or_update(
            context,
            chat_id,
            "💲 Тарифы\n\nПока не настроены.\nОбратитесь к администратору.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="user:home")]]),
            message=message,
        )
        return
    keyboard = []
    for t in tariffs:
        label = f"{t['name']} | {t['duration_min']} мин | ${t['price']}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"user:tariff:{t['id']}")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="user:home")])
    await send_or_update(
        context,
        chat_id,
        "Выберите тариф:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        message=message,
    )
