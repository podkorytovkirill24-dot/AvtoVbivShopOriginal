async def menu_show_queue(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message=None) -> None:
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, user_id, phone FROM queue_numbers "
        "WHERE status = 'queued' ORDER BY created_at, id"
    ).fetchall()
    conn.close()
    total = len(rows)
    user_positions = []
    for idx, r in enumerate(rows, start=1):
        if r["user_id"] == user_id:
            user_positions.append((r["phone"], idx))
    if not user_positions:
        text = (
            "📊 Текущая очередь\n"
            f"👥 Всего в очереди: {total}\n"
            "Вы сейчас не в очереди."
        )
        await send_or_update(
            context,
            chat_id,
            text,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="user:home")]]),
            message=message,
        )
        return
    lines = [
        "📊 Текущая очередь",
        f"👥 Всего в очереди: {total}",
        f"📍 Ваши позиции ({len(user_positions)}):",
    ]
    for phone, pos in user_positions[:20]:
        lines.append(f"• {format_phone(phone)} — #{pos}")
    if len(user_positions) > 20:
        lines.append("…")
    await send_or_update(
        context,
        chat_id,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="user:home")]]),
        message=message,
    )
