async def menu_show_tariffs(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    conn = get_conn()
    tariffs = conn.execute(
        "SELECT id, name, price, duration_min FROM tariffs ORDER BY id"
    ).fetchall()
    conn.close()
    if not tariffs:
        await context.bot.send_message(
            chat_id=chat_id,
            text="💲 Тарифы\n\nПока не настроены.\nОбратитесь к администратору.",
        )
        return
    keyboard = []
    for t in tariffs:
        label = f"{t['name']} | {t['duration_min']} мин | ${t['price']}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"user:tariff:{t['id']}")])
    await context.bot.send_message(
        chat_id=chat_id,
        text="Выберите тариф:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
