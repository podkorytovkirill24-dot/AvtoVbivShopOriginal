async def cmd_num(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type == "private":
        return
    conn = get_conn()
    if not (is_admin(conn, update.effective_user.id) or await is_chat_admin(update.effective_chat.id, update.effective_user.id, context)):
        conn.close()
        await update.message.reply_text("Команда доступна только администраторам этой группы или бота.")
        return
    tariffs = conn.execute("SELECT id, name, price, duration_min FROM tariffs ORDER BY id").fetchall()
    conn.close()
    if not tariffs:
        await update.message.reply_text("Сначала создайте тарифы в админ-меню.")
        return
    keyboard = []
    chat_id = update.effective_chat.id
    for t in tariffs:
        label = f"{t['name']} | {t['duration_min']} мин | ${t['price']}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"set_reception:{chat_id}:{t['id']}")])
    await update.message.reply_text("Выберите тариф для этой приемки:", reply_markup=InlineKeyboardMarkup(keyboard))
