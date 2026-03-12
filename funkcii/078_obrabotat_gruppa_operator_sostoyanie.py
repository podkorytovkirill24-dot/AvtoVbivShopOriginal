async def handle_group_worker_state(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        return
    if not update.message:
        return
    state = get_state(context)
    if not state or state.get("name") != "worker_message_user":
        return
    data = state.get("data", {})
    if data.get("chat_id") != update.effective_chat.id:
        return
    thread_id = data.get("thread_id")
    if thread_id is not None:
        msg_thread_id = update.message.message_thread_id or 0
        if int(thread_id) != int(msg_thread_id):
            return
    prompt_msg_id = data.get("prompt_msg_id")
    if prompt_msg_id:
        reply = update.message.reply_to_message if update.message else None
        if reply and reply.message_id != prompt_msg_id:
            return

    queue_id = data.get("queue_id")
    if not queue_id:
        clear_state(context)
        return

    text_msg = update.message.text or update.message.caption or ""
    if not text_msg and not update.message.photo:
        return

    conn = get_conn()
    row = conn.execute(
        "SELECT user_id, phone FROM queue_numbers WHERE id = ?",
        (queue_id,),
    ).fetchone()
    conn.close()
    if not row:
        clear_state(context)
        return

    phone_display = format_phone(row["phone"])
    try:
        if update.message.photo:
            photo_id = update.message.photo[-1].file_id
            caption = f"Сообщение от оператора по номеру {phone_display}"
            if text_msg:
                caption = f"{caption}\n{text_msg}"
            await context.bot.send_photo(
                chat_id=row["user_id"],
                photo=photo_id,
                caption=caption,
            )
        else:
            await context.bot.send_message(
                chat_id=row["user_id"],
                text=f"Сообщение от оператора по номеру {phone_display}:\n{text_msg}",
            )
    except Exception:
        pass

    clear_state(context)
    try:
        await update.message.reply_text("✅ Сообщение отправлено владельцу.")
    except Exception:
        pass
