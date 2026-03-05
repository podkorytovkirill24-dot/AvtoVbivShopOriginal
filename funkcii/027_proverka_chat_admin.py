async def is_chat_admin(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in ("administrator", "creator")
