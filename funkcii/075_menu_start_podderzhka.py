async def menu_start_support(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message=None) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO support_tickets (user_id, status, created_at) "
        "VALUES (?, 'open', ?) ",
        (user_id, now_ts()),
    )
    ticket_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    conn.commit()
    conn.close()
    set_state(context, "support_message", ticket_id=ticket_id)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="user:home")]])
    await send_or_update(
        context,
        chat_id,
        "Напишите сообщение для поддержки:",
        reply_markup=keyboard,
        message=message,
    )
