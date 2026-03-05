async def menu_start_support(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
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
    await context.bot.send_message(chat_id=chat_id, text="Напишите сообщение для поддержки:")
