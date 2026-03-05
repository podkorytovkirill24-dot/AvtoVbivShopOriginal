async def menu_show_archive(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    conn = get_conn()
    rows = conn.execute(
        "SELECT phone, status, created_at, completed_at FROM queue_numbers "
        "WHERE user_id = ? AND status IN ('success','slip','error','canceled') "
        "ORDER BY completed_at DESC LIMIT 30",
        (user_id,),
    ).fetchall()
    detail = get_config_bool(conn, "detail_archive")
    conn.close()
    if not rows:
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📞 Сдать номер", callback_data="menu:submit")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="user:home")],
            ]
        )
        await context.bot.send_message(chat_id=chat_id, text=ui("empty_archive"), reply_markup=keyboard)
        return
    lines = ["🗂 Архив (последние 30):"]
    for r in rows:
        status_text = status_human(r["status"])
        if detail:
            lines.append(
                f"{r['phone']} | {status_text} | "
                f"{format_ts(r['created_at'])} → {format_ts(r['completed_at'])}"
            )
        else:
            lines.append(f"{r['phone']} | {status_text}")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
