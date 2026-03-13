async def menu_show_queue(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message=None) -> None:
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, user_id, phone FROM queue_numbers "
        "WHERE status = 'queued' ORDER BY created_at, id"
    ).fetchall()
    iam_on = get_config_bool(conn, "i_am_here_on")
    conn.close()

    total = len(rows)
    user_positions = []
    for idx, r in enumerate(rows, start=1):
        if r["user_id"] == user_id:
            user_positions.append((r["phone"], idx))

    if not user_positions:
        text = (
            "📊 Очередь\n"
            f"Всего в очереди: {total}\n"
            "У вас пока нет номеров в очереди.\n\n"
            "Нажмите «Сдать номер», чтобы добавить."
        )
        keyboard_rows = []
        if iam_on:
            keyboard_rows.append([InlineKeyboardButton("👋 Я тут", callback_data="user:i_am_here")])
        keyboard_rows.append([InlineKeyboardButton("⬅ Назад", callback_data="user:home")])
        await send_or_update(
            context,
            chat_id,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
            message=message,
        )
        return

    lines = [
        "📊 Очередь",
        f"Всего в очереди: {total}",
        f"Ваших номеров: {len(user_positions)}",
        "",
        "Ваши позиции:",
    ]
    for idx, (phone, pos) in enumerate(user_positions[:20], start=1):
        lines.append(f"{idx}. {format_phone(phone)} • позиция #{pos}")
    if len(user_positions) > 20:
        lines.append("…")

    keyboard_rows = []
    if iam_on:
        keyboard_rows.append([InlineKeyboardButton("👋 Я тут", callback_data="user:i_am_here")])
    keyboard_rows.append([InlineKeyboardButton("⬅ Назад", callback_data="user:home")])
    await send_or_update(
        context,
        chat_id,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
        message=message,
    )
