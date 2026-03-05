def build_tops_csv(conn: sqlite3.Connection, metric: str, period: str) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "count"])
    start_ts, end_ts = get_period_range(period)
    if metric == "invited":
        rows = conn.execute(
            "SELECT referred_by AS user_id, COUNT(*) AS cnt "
            "FROM users WHERE referred_by IS NOT NULL GROUP BY referred_by"
        ).fetchall()
    else:
        status_filter = ""
        if metric == "success":
            status_filter = "AND status='success'"
        elif metric == "slip":
            status_filter = "AND status='slip'"
        elif metric == "error":
            status_filter = "AND status='error'"
        if period == "all":
            rows = conn.execute(
                "SELECT user_id, COUNT(*) AS cnt FROM queue_numbers WHERE 1=1 "
                f"{status_filter} GROUP BY user_id"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT user_id, COUNT(*) AS cnt FROM queue_numbers "
                "WHERE created_at BETWEEN ? AND ? "
                f"{status_filter} GROUP BY user_id",
                (start_ts, end_ts),
            ).fetchall()
    for r in rows:
        writer.writerow([r["user_id"], r["cnt"]])
    return output.getvalue()
