def build_report_detailed(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.completed_at, t.name AS tariff "
        "FROM queue_numbers q LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "ORDER BY q.created_at DESC LIMIT 30"
    ).fetchall()
    lines = ["📈 Детальный отчёт", "Период: последние 30 записей", ""]
    for r in rows:
        lines.append(
            f"• {r['phone']} | {status_human(r['status'])} | {r['tariff']} | {format_ts(r['created_at'])}"
        )
    return "\n".join(lines)
