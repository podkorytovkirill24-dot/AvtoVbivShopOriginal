async def menu_show_archive(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message=None) -> None:
    conn = get_conn()
    rows = conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.completed_at, "
        "t.name AS tariff_name, t.duration_min "
        "FROM queue_numbers q "
        "LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "WHERE q.user_id = ? AND q.status IN ('success','slip','error','canceled') "
        "ORDER BY q.completed_at DESC LIMIT 30",
        (user_id,),
    ).fetchall()
    conn.close()
    if not rows:
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📞 Сдать номер", callback_data="menu:submit")],
                [InlineKeyboardButton("⬅ Назад", callback_data="user:home")],
            ]
        )
        await send_or_update(context, chat_id, ui("empty_archive"), reply_markup=keyboard, message=message)
        return

    lines = ["🗂 Архив", "Последние 30 номеров", ""]
    for idx, r in enumerate(rows, start=1):
        start_ts = int(r["created_at"] or 0)
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts)
        duration_min = int(duration_sec // 60)
        tariff_name = r["tariff_name"] or "-"
        limit_min = int(r["duration_min"] or 0)
        if limit_min > 0:
            mark = "✅" if duration_sec >= limit_min * 60 else "❌"
        else:
            mark = "—"
        period = f"{format_ts(start_ts)} – {format_ts(end_ts)}"
        lines.append(
            f"{idx}. {format_phone(r['phone'])} | {tariff_name} | {duration_min} мин | {period} | {mark}"
        )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="user:home")]])
    await send_or_update(context, chat_id, "\n".join(lines), reply_markup=keyboard, message=message)
