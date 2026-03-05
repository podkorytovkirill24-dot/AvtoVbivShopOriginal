async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return
    conn = get_conn()
    if not is_admin(conn, update.effective_user.id):
        conn.close()
        await update.message.reply_text(ui("no_access"))
        return
    conn.close()
    await update.message.reply_text(ui("admin_panel_title"), reply_markup=build_admin_panel())
