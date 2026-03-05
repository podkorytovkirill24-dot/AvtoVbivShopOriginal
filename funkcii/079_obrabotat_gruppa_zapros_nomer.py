async def handle_group_request_number(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        return
    if not update.message:
        return
    text = (update.message.text or "").strip().lower()
    if "номер" not in text:
        return
    if extract_numbers(text):
        return

    conn = get_conn()
    if is_lunch_time(conn):
        conn.close()
        await update.message.reply_text("Сейчас обед. Попробуйте позже.")
        return

    thread_id = update.message.message_thread_id or 0
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
        await update.message.reply_text("Тема не привязана к приемке. Напишите /set.")
        return

    departments = conn.execute(
        "SELECT id, name FROM departments ORDER BY id"
    ).fetchall()
    issue_by_dept = get_config_bool(conn, "issue_by_departments", False)
    if issue_by_dept and len(departments) > 1:
        keyboard = []
        for d in departments:
            keyboard.append(
                [InlineKeyboardButton(d["name"], callback_data=f"issue:{d['id']}:{topic['reception_chat_id']}")]
            )
        conn.close()
        await update.message.reply_text("Выберите отдел для выдачи:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    dept_ids = [d["id"] for d in departments] if departments else []
    row = fetch_next_queue(conn, dept_ids, topic["reception_chat_id"])
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
