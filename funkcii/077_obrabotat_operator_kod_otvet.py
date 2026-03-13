async def handle_worker_code_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        return
    if not update.message or not update.message.reply_to_message:
        return
    if not update.message.reply_to_message.from_user or not update.message.reply_to_message.from_user.is_bot:
        return

    reply_msg = update.message.reply_to_message
    conn = get_conn()
    row = conn.execute(
        "SELECT id, user_id, phone FROM queue_numbers WHERE worker_chat_id = ? AND worker_msg_id = ?",
        (reply_msg.chat_id, reply_msg.message_id),
    ).fetchone()
    if not row:
        conn.close()
        return

    code_text = update.message.text or update.message.caption or ""
    photo_id = None
    if update.message.photo:
        photo_id = update.message.photo[-1].file_id
        conn.execute(
            "UPDATE queue_numbers SET photo_file_id = ? WHERE id = ?",
            (photo_id, row["id"]),
        )
    conn.commit()
    conn.close()

    phone_display = format_phone(row["phone"])
    user_keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔁 Повтор кода", callback_data=f"user:repeat:{row['id']}"),
                InlineKeyboardButton("📱 Запрос QR", callback_data=f"user:qr:{row['id']}"),
            ]
        ]
    )
    try:
        if photo_id:
            caption = f"Код для номера {phone_display}"
            if code_text:
                caption = f"{caption}\n{code_text}"
            await context.bot.send_photo(
                chat_id=row["user_id"],
                photo=photo_id,
                caption=caption,
                reply_markup=user_keyboard,
            )
        elif code_text:
            await context.bot.send_message(
                chat_id=row["user_id"],
                text=f"Код для номера {phone_display}:\n{code_text}",
                reply_markup=user_keyboard,
            )
    except Exception:
        pass

    try:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Встал", callback_data=f"q:status:success:{row['id']}"),
                    InlineKeyboardButton("❌ Ошибка", callback_data=f"q:status:error:{row['id']}"),
                ],
                [InlineKeyboardButton("✉ Сообщение владельцу", callback_data=f"q:msg:{row['id']}")],
                [InlineKeyboardButton("⏭ Скип", callback_data=f"q:skip:{row['id']}")],
            ]
        )
        await context.bot.send_message(
            chat_id=reply_msg.chat_id,
            message_thread_id=reply_msg.message_thread_id,
            text=f"🖼 Код передан дропу. Подтвердите статус номера:\nНомер: {phone_display}",
            reply_markup=keyboard,
            reply_to_message_id=reply_msg.message_id,
        )
    except Exception:
        pass
