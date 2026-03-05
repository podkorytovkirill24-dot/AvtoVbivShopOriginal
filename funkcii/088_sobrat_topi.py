def build_tops(conn: sqlite3.Connection, metric: str, period: str) -> str:
    start_ts, end_ts = get_period_range(period)
    period_title = {
        "today": "Сегодня",
        "yesterday": "Вчера",
        "7d": "7 дней",
        "30d": "30 дней",
        "all": "Всё время",
    }.get(period, "Всё время")

    if metric == "invited":
        total_metric = conn.execute(
            "SELECT COUNT(*) AS cnt FROM users WHERE referred_by IS NOT NULL"
        ).fetchone()["cnt"]
        rows = conn.execute(
            "SELECT r.user_id, u.username, r.cnt "
            "FROM ("
            "  SELECT referred_by AS user_id, COUNT(*) AS cnt "
            "  FROM users WHERE referred_by IS NOT NULL GROUP BY referred_by"
            ") r "
            "LEFT JOIN users u ON u.user_id = r.user_id "
            "ORDER BY r.cnt DESC LIMIT 10"
        ).fetchall()
        title = "Приглашённые"
    else:
        status_filter = ""
        if metric == "success":
            status_filter = "AND q.status='success'"
        elif metric == "slip":
            status_filter = "AND q.status='slip'"
        elif metric == "error":
            status_filter = "AND q.status='error'"
        if period == "all":
            total_metric = conn.execute(
                "SELECT COUNT(*) AS cnt FROM queue_numbers q "
                f"WHERE 1=1 {status_filter}"
            ).fetchone()["cnt"]
        else:
            total_metric = conn.execute(
                "SELECT COUNT(*) AS cnt FROM queue_numbers q "
                "WHERE q.created_at BETWEEN ? AND ? "
                f"{status_filter}",
                (start_ts, end_ts),
            ).fetchone()["cnt"]
        if period == "all":
            rows = conn.execute(
                "SELECT q.user_id, u.username, COUNT(*) AS cnt "
                "FROM queue_numbers q "
                "LEFT JOIN users u ON u.user_id = q.user_id "
                f"WHERE 1=1 {status_filter} "
                "GROUP BY q.user_id, u.username ORDER BY cnt DESC LIMIT 10"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT q.user_id, u.username, COUNT(*) AS cnt "
                "FROM queue_numbers q "
                "LEFT JOIN users u ON u.user_id = q.user_id "
                "WHERE q.created_at BETWEEN ? AND ? "
                f"{status_filter} "
                "GROUP BY q.user_id, u.username ORDER BY cnt DESC LIMIT 10",
                (start_ts, end_ts),
            ).fetchall()
        title = {
            "submitted": "Сдано номеров",
            "success": "Встал",
            "slip": "Слетел",
            "error": "Ошибки",
        }.get(metric, "Сдано номеров")

    lines = [
        "🏆 Топы",
        f"Метрика: {title}",
        f"Период: {period_title}",
        f"Всего по метрике: {total_metric}",
        "",
    ]
    if not rows:
        lines.append(ui("empty_data"))
        return "\n".join(lines)
    for idx, r in enumerate(rows, start=1):
        lines.append(
            f"{idx}) {format_user_label(r['user_id'], r['username'])} — {r['cnt']} "
            f"({pct(int(r['cnt']), int(total_metric))})"
        )
    return "\n".join(lines)
