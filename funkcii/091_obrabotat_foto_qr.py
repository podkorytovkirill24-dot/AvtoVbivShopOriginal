async def handle_photo_qr(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return
    if get_state(context):
        return
    if not update.message.photo:
        return
    conn = get_conn()
    row = conn.execute(
        "SELECT id, worker_chat_id, worker_msg_id FROM queue_numbers "
        "WHERE user_id = ? AND qr_requested = 1 ORDER BY created_at DESC LIMIT 1",
        (update.effective_user.id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    photo_id = update.message.photo[-1].file_id
    try:
        await context.bot.send_photo(
            chat_id=row["worker_chat_id"],
            photo=photo_id,
            caption="QR/код",
            reply_to_message_id=row["worker_msg_id"],
        )
    except Exception:
        pass
    conn.execute("UPDATE queue_numbers SET qr_requested = 2 WHERE id = ?", (row["id"],))
    conn.commit()
    conn.close()
    await update.message.reply_text("QR отправлен оператору.")
