async def menu_show_archive(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, message=None) -> None:
    conn = get_conn()
    rows = conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.assigned_at, q.completed_at, "
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
                [InlineKeyboardButton("рџ“ћ РЎРґР°С‚СЊ РЅРѕРјРµСЂ", callback_data="menu:submit")],
                [InlineKeyboardButton("в¬… РќР°Р·Р°Рґ", callback_data="user:home")],
            ]
        )
        await send_or_update(context, chat_id, ui("empty_archive"), reply_markup=keyboard, message=message)
        return

    lines = ["рџ—‚ РђСЂС…РёРІ", "РџРѕСЃР»РµРґРЅРёРµ 30 РЅРѕРјРµСЂРѕРІ", ""]
    for idx, r in enumerate(rows, start=1):
        start_ts = int(r["assigned_at"] or 0)
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts) if start_ts else 0
        duration_min = int(duration_sec // 60)
        tariff_name = r["tariff_name"] or "-"
        limit_min = int(r["duration_min"] or 0)
        if not start_ts:
            mark = "РІР‚вЂќ"
        elif limit_min > 0:
            mark = "вњ…" if duration_sec >= limit_min * 60 else "вќЊ"
        else:
            mark = "вЂ”"
        start_label = format_ts(start_ts) if start_ts else "-"
        period = f"{start_label} – {format_ts(end_ts)}"
        lines.append(
            f"{idx}. {format_phone(r['phone'])} | {tariff_name} | {duration_min} РјРёРЅ | {period} | {mark}"
        )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("в¬… РќР°Р·Р°Рґ", callback_data="user:home")]])
    await send_or_update(context, chat_id, "\n".join(lines), reply_markup=keyboard, message=message)
