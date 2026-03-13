async def handle_group_request_number(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        return
    if not update.message:
        return
    text = (update.message.text or "").strip().lower()
    if "номер" not in text and "nomer" not in text:
        return
    if extract_numbers(text):
        return

    conn = get_conn()
    if is_lunch_time(conn):
        conn.close()
        await update.message.reply_text("Сейчас обед. Попробуйте позже.")
        return

    thread_id = update.message.message_thread_id or 0
    issue_by = get_config_bool(conn, "issue_by_departments", False)
    reception_chat_id = None

    if issue_by:
        topic = conn.execute(
            "SELECT reception_chat_id FROM processing_topics WHERE chat_id = ? AND thread_id = ?",
            (update.effective_chat.id, thread_id),
        ).fetchone()
        if not topic or not topic["reception_chat_id"]:
            conn.close()
            await update.message.reply_text("Привязка не настроена. Напишите /set.")
            return
        reception_chat_id = topic["reception_chat_id"]
    else:
        topic = conn.execute(
            "SELECT reception_chat_id FROM processing_topics WHERE chat_id = ? AND thread_id = ?",
            (update.effective_chat.id, thread_id),
        ).fetchone()
        if not topic:
            reception = conn.execute(
                "SELECT 1 FROM reception_groups WHERE chat_id = ? AND is_active = 1",
                (update.effective_chat.id,),
            ).fetchone()
            conn.close()
            if reception:
                return

    row = fetch_next_queue(conn, [], reception_chat_id)
    if not row:
        conn.close()
        await update.message.reply_text("Очередь пуста.")
        return
    conn.execute(
        "UPDATE queue_numbers SET status = 'taken', assigned_at = ?, worker_id = ? WHERE id = ?",
        (now_ts(), update.effective_user.id, row["id"]),
    )
    conn.commit()
    conn.close()
    await send_number_to_worker(update, context, row)
