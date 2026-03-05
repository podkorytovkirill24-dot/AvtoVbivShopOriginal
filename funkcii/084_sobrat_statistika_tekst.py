def build_stats_text(conn: sqlite3.Connection, period: str) -> str:
    start_ts, end_ts = get_period_range(period)
    users_total = conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()["cnt"]
    total_numbers = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers").fetchone()["cnt"]
    queued = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'queued'").fetchone()["cnt"]
    taken = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'taken'").fetchone()["cnt"]
    done = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'success'").fetchone()["cnt"]
    canceled = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'canceled'").fetchone()["cnt"]
    slip = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'slip'").fetchone()["cnt"]
    error = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'error'").fetchone()["cnt"]

    period_filter = ""
    params: Tuple = tuple()
    if period != "all":
        period_filter = " AND created_at BETWEEN ? AND ?"
        params = (start_ts, end_ts)

    submitted = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE 1=1" + period_filter,
        params,
    ).fetchone()["cnt"]
    taken_p = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE assigned_at BETWEEN ? AND ?",
        (start_ts, end_ts),
    ).fetchone()["cnt"] if period != "all" else conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE assigned_at IS NOT NULL"
    ).fetchone()["cnt"]
    success_p = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='success' AND completed_at BETWEEN ? AND ?",
        (start_ts, end_ts),
    ).fetchone()["cnt"] if period != "all" else conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='success'"
    ).fetchone()["cnt"]
    slip_p = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='slip' AND completed_at BETWEEN ? AND ?",
        (start_ts, end_ts),
    ).fetchone()["cnt"] if period != "all" else conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='slip'"
    ).fetchone()["cnt"]
    error_p = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='error' AND completed_at BETWEEN ? AND ?",
        (start_ts, end_ts),
    ).fetchone()["cnt"] if period != "all" else conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='error'"
    ).fetchone()["cnt"]
    finished_p = success_p + slip_p + error_p

    period_title = {
        "today": "Сегодня",
        "yesterday": "Вчера",
        "7d": "7 дней",
        "30d": "30 дней",
        "all": "Всё время",
    }.get(period, "Сегодня")

    top_depts = conn.execute(
        "SELECT d.name, COUNT(*) AS cnt FROM queue_numbers q "
        "LEFT JOIN departments d ON q.department_id = d.id "
        "WHERE q.created_at BETWEEN ? AND ? "
        "GROUP BY d.name ORDER BY cnt DESC LIMIT 5",
        (start_ts, end_ts),
    ).fetchall()

    lines = [
        "📊 Статистика",
        f"Период: {period_title}",
        "",
        "👥 Пользователи",
        f"• Всего: {users_total}",
        "",
        "📦 Номера (всего в системе)",
        f"• всего: {total_numbers}",
        f"• в ожидании: {queued} | в работе: {taken}",
        f"• встал: {done} | отменен: {canceled}",
        "",
        "🧮 За период",
        f"• Сдано: {submitted}",
        f"• Выдано: {taken_p}",
        f"• Встал: {success_p} ({pct(success_p, submitted)})",
        f"• Слет: {slip_p} ({pct(slip_p, submitted)})",
        f"• Ошибка: {error_p} ({pct(error_p, submitted)})",
        f"• Completion rate: {pct(finished_p, submitted)}",
        f"• Success rate: {pct(success_p, finished_p)}",
        "",
        "🏆 Топ отделов (по сдаче)",
    ]
    if top_depts:
        for row in top_depts:
            lines.append(f"• {row['name'] or 'Без отдела'}: {row['cnt']}")
    else:
        lines.append(f"• {ui('empty_data')}")
    return "\n".join(lines)
