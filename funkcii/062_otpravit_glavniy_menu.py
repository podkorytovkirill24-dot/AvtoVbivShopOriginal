async def send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat:
        return
    await send_main_menu_chat(context, update.effective_chat.id, update.effective_user.id)
