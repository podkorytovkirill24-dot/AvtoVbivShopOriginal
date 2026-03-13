async def send_number_to_worker(update: Update, context: ContextTypes.DEFAULT_TYPE, row: sqlite3.Row) -> None:
    conn = get_conn()
    notify_taken = get_config_bool(conn, "notify_taken")
    reception_title = None
    reception_chat_id = row["reception_chat_id"] if "reception_chat_id" in row.keys() else None
    if reception_chat_id:
        rec = conn.execute(
            "SELECT chat_title FROM reception_groups WHERE chat_id = ?",
            (reception_chat_id,),
        ).fetchone()
        if rec:
            reception_title = rec["chat_title"]
    conn.close()

    phone_display = format_phone(row["phone"])
    title = reception_title or row["tariff_name"] or "Приемка"
    text_msg = (
        f"{title}\n"
        f"📞 Номер: {phone_display}\n"
        "Пожалуйста пришлите код по номеру."
    )

    buttons = [
        [
            InlineKeyboardButton("✅ Встал", callback_data=f"q:status:success:{row['id']}"),
            InlineKeyboardButton("❌ Ошибка", callback_data=f"q:status:error:{row['id']}"),
        ],
        [InlineKeyboardButton("✉ Сообщение владельцу", callback_data=f"q:msg:{row['id']}")],
        [InlineKeyboardButton("⏭ Скип", callback_data=f"q:skip:{row['id']}")],
    ]
    keyboard = InlineKeyboardMarkup(buttons)

    msg_src = update.message or (update.callback_query.message if update.callback_query else None)
    thread_id = msg_src.message_thread_id if msg_src else None
    reply_to_id = msg_src.message_id if update.message and msg_src else None

    sent_ok = False
    try:
        if row["photo_file_id"]:
            msg = await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                message_thread_id=thread_id,
                photo=row["photo_file_id"],
                caption=text_msg,
                reply_markup=keyboard,
                reply_to_message_id=reply_to_id,
            )
        else:
            msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                message_thread_id=thread_id,
                text=text_msg,
                reply_markup=keyboard,
                reply_to_message_id=reply_to_id,
            )
        sent_ok = True
        conn = get_conn()
        conn.execute(
            "UPDATE queue_numbers SET worker_chat_id = ?, worker_msg_id = ?, worker_thread_id = ? WHERE id = ?",
            (msg.chat_id, msg.message_id, thread_id or 0, row["id"]),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("Failed to send to worker: %s", exc)

    if not sent_ok:
        try:
            conn = get_conn()
            conn.execute(
                "UPDATE queue_numbers "
                "SET status = 'queued', assigned_at = NULL, worker_id = NULL, worker_chat_id = NULL, worker_msg_id = NULL "
                "WHERE id = ?",
                (row["id"],),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
        return

    if notify_taken:
        try:
            await context.bot.send_message(
                chat_id=row["user_id"],
                text=f"✅ Ваш номер {phone_display} взяли в работу.",
            )
        except Exception:
            pass
