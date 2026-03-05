async def get_bot_username(context: ContextTypes.DEFAULT_TYPE) -> str:
    cached = (context.bot_data.get("bot_username") or "").strip()
    if cached:
        return cached
    try:
        me = await context.bot.get_me()
        username = (me.username or "").strip()
    except Exception:
        username = ""
    if username:
        context.bot_data["bot_username"] = username
    return username
