def build_offices_menu(conn: sqlite3.Connection) -> Tuple[str, InlineKeyboardMarkup]:
    rows = conn.execute(
        "SELECT id, name, chat_id, thread_id FROM offices ORDER BY id"
    ).fetchall()
    lines = ["🏢 Офисы"]
    if not rows:
        lines.append("(привязок нет)")
    else:
        for r in rows:
            bind = f"{r['chat_id']}" if r["chat_id"] else "не привязан"
            if r["thread_id"]:
                bind += f" / тема {r['thread_id']}"
            lines.append(f"• {r['id']}. {r['name']} → {bind}")
    keyboard = [
        [InlineKeyboardButton("➕ Добавить", callback_data="adm:office:add")],
        [
            InlineKeyboardButton("✏ Редактировать", callback_data="adm:office:edit"),
            InlineKeyboardButton("🗑 Удалить", callback_data="adm:office:delete"),
        ],
        [InlineKeyboardButton("🔗 Привязать через /set", callback_data="adm:office:bind")],
        [InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)
