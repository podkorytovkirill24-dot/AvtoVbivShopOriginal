async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        return
    conn = get_conn()
    if not (is_admin(conn, update.effective_user.id) or await is_chat_admin(update.effective_chat.id, update.effective_user.id, context)):
        conn.close()
        await update.message.reply_text("Команда доступна только администраторам этой группы или бота.")
        return
    receptions = conn.execute(
        "SELECT r.chat_id, r.chat_title, t.name, t.duration_min, t.price "
        "FROM reception_groups r LEFT JOIN tariffs t ON r.tariff_id = t.id "
        "WHERE r.is_active = 1 ORDER BY r.chat_title"
    ).fetchall()
    conn.close()
    if not receptions:
        await update.message.reply_text("Сначала настройте приемки через /num в нужной группе.")
        return
    thread_id = update.message.message_thread_id or 0
    keyboard = []
    for r in receptions:
        title = r["chat_title"] or str(r["chat_id"])
        if r["name"]:
            tariff_label = f"{r['name']} | {r['duration_min']} мин | ${r['price']}"
            label = f"{title} • {tariff_label}"
        else:
            label = title
        keyboard.append([InlineKeyboardButton(label, callback_data=f"set_topic:{update.effective_chat.id}:{thread_id}:{r['chat_id']}")])
    await update.message.reply_text("Выберите приемку для привязки к этой теме:", reply_markup=InlineKeyboardMarkup(keyboard))
