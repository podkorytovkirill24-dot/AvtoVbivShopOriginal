def build_departments_menu(conn: sqlite3.Connection) -> Tuple[str, InlineKeyboardMarkup]:
    rows = conn.execute(
        "SELECT d.id, d.name, o.name AS office_name "
        "FROM departments d LEFT JOIN offices o ON d.office_id = o.id "
        "ORDER BY d.id"
    ).fetchall()
    lines = ["📥 Приемки"]
    if not rows:
        lines.append("(привязок нет)")
    else:
        for r in rows:
            office = r["office_name"] or "—"
            lines.append(f"• {r['id']}. {r['name']} → {office}")
    keyboard = [
        [InlineKeyboardButton("➕ Добавить", callback_data="adm:dept:add")],
        [
            InlineKeyboardButton("✏ Редактировать", callback_data="adm:dept:edit"),
            InlineKeyboardButton("🗑 Удалить", callback_data="adm:dept:delete"),
        ],
        [InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)
