def build_report_detailed(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.completed_at, q.user_id, u.username, "
        "t.name AS tariff, t.duration_min "
        "FROM queue_numbers q "
        "LEFT JOIN users u ON u.user_id = q.user_id "
        "LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "ORDER BY q.created_at DESC LIMIT 30"
    ).fetchall()
    lines = ["📈 Детальный отчёт", "Период: последние 30 записей", ""]
    for r in rows:
        start_ts = int(r["created_at"] or 0)
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts)
        duration_limit = r["duration_min"]
        if duration_limit is None:
            mark = "-"
        else:
            limit_sec = int(duration_limit or 0) * 60
            mark = "✅" if duration_sec >= limit_sec else "❌"
        lines.append(
            f"• {r['phone']} | {format_user_label(r['user_id'], r['username'])} | "
            f"{status_human(r['status'])} | {r['tariff']} | "
            f"встал: {format_ts(r['created_at'])} | слетел: {format_ts(r['completed_at'])} | "
            f"стоял: {format_duration(duration_sec)} {mark}"
        )
    return "\n".join(lines)
