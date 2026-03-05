async def send_main_menu_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> None:
    conn = get_conn()
    is_admin_user = is_admin(conn, user_id)
    text = get_config(conn, "main_menu_text", DEFAULT_CONFIG["main_menu_text"])
    photo_id = get_config(conn, "main_menu_photo_id", "")
    keyboard = build_main_menu_inline(conn, is_admin_user)
    conn.close()

    if photo_id:
        await context.bot.send_photo(chat_id=chat_id, photo=photo_id, caption=text, reply_markup=keyboard)
    else:
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
