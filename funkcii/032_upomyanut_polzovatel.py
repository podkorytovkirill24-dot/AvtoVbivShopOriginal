def mention_user(user_id: int, name: str) -> str:
    safe_name = (name or "пользователь").replace("<", "&lt;").replace(">", "&gt;")
    return f"<a href=\"tg://user?id={user_id}\">{safe_name}</a>"
