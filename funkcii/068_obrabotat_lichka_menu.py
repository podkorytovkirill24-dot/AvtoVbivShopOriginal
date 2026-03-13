async def handle_private_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return
    if get_state(context):
        return
    conn = get_conn()
    upsert_user(conn, update.effective_user)
    conn.commit()

    if not update.message.text:
        conn.close()
        return

    text = (update.message.text or "").strip()
    submit = get_config(conn, "menu_btn_submit", DEFAULT_CONFIG["menu_btn_submit"])
    queue_btn = get_config(conn, "menu_btn_queue", DEFAULT_CONFIG["menu_btn_queue"])
    archive = get_config(conn, "menu_btn_archive", DEFAULT_CONFIG["menu_btn_archive"])
    profile = get_config(conn, "menu_btn_profile", DEFAULT_CONFIG["menu_btn_profile"])
    support = get_config(conn, "menu_btn_support", DEFAULT_CONFIG["menu_btn_support"])
    admin_btn = get_config(conn, "menu_btn_admin", DEFAULT_CONFIG["menu_btn_admin"])
    home_btn = get_config(conn, "menu_btn_home", DEFAULT_CONFIG["menu_btn_home"])

    if text == submit:
        conn.close()
        clear_state(context)
        await menu_show_tariffs(context, update.effective_chat.id)
        return

    if text == queue_btn:
        conn.close()
        await menu_show_queue(context, update.effective_chat.id, update.effective_user.id)
        return

    if text == archive:
        conn.close()
        await menu_show_archive(context, update.effective_chat.id, update.effective_user.id)
        return

    if text == profile:
        conn.close()
        await menu_show_profile(context, update.effective_chat.id, update.effective_user.id)
        return

    if text == support:
        conn.close()
        clear_state(context)
        await menu_start_support(context, update.effective_chat.id, update.effective_user.id)
        return

    if text == admin_btn:
        conn.close()
        await cmd_admin(update, context)
        return

    if text == home_btn:
        conn.close()
        await send_main_menu(update, context)
        return

    conn.close()
