def build_csv(conn: sqlite3.Connection, period: str) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        ["id", "user_id", "phone", "status", "created_at", "assigned_at", "completed_at", "tariff_id"]
    )
    if period == "all":
        rows = conn.execute(
            "SELECT id, user_id, phone, status, created_at, assigned_at, completed_at, tariff_id "
            "FROM queue_numbers ORDER BY id"
        ).fetchall()
    else:
        start_ts, end_ts = get_period_range(period)
        rows = conn.execute(
            "SELECT id, user_id, phone, status, created_at, assigned_at, completed_at, tariff_id "
            "FROM queue_numbers WHERE created_at BETWEEN ? AND ? ORDER BY id",
            (start_ts, end_ts),
        ).fetchall()
    for r in rows:
        writer.writerow([
            r["id"],
            r["user_id"],
            r["phone"],
            r["status"],
            format_ts(r["created_at"]),
            format_ts(r["assigned_at"]),
            format_ts(r["completed_at"]),
            r["tariff_id"],
        ])
    return output.getvalue()
