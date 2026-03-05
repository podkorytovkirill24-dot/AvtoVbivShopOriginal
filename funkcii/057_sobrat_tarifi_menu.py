def build_tariffs_menu(conn: sqlite3.Connection) -> Tuple[str, InlineKeyboardMarkup]:
    rows = conn.execute(
        "SELECT id, name, price, duration_min, priority FROM tariffs ORDER BY id"
    ).fetchall()
    lines = ["💲 Тарифы"]
    if not rows:
        lines.append("• Пока пусто")
    else:
        for t in rows:
            dur = f"{t['duration_min']} мин" if t["duration_min"] else "-"
            price = f"{t['price']}" if t["price"] else "0"
            lines.append(f"• {t['name']} — {dur} — ${price} (prio {t['priority']})")
    keyboard = [
        [InlineKeyboardButton("➕ Добавить тариф", callback_data="adm:tariff:add")],
        [
            InlineKeyboardButton("✏ Редактировать тариф", callback_data="adm:tariff:edit"),
            InlineKeyboardButton("🗑 Удалить тариф", callback_data="adm:tariff:delete"),
        ],
        [InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)
