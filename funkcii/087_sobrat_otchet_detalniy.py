def build_report_detailed(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.completed_at, q.user_id, u.username, "
        "t.name AS tariff, t.duration_min "
        "FROM queue_numbers q "
        "LEFT JOIN users u ON u.user_id = q.user_id "
        "LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "WHERE q.completed_at IS NOT NULL AND q.status IN ('success','slip','error','canceled') "
        "ORDER BY q.completed_at DESC LIMIT 30"
    ).fetchall()
    lines = ["📈 Детальный отчёт", "Период: последние 30 записей", ""]
    for r in rows:
        start_ts = int(r["created_at"] or 0)
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts)
        duration_min = int(duration_sec // 60)
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
            f"стоял: {format_duration(duration_sec)} ({duration_min} мин) {mark}"
        )
    return "\n".join(lines)


def _iter_report_rows(conn: sqlite3.Connection, limit: int = 50):
    return conn.execute(
        "SELECT q.phone, q.status, q.created_at, q.completed_at, q.user_id, u.username, "
        "t.name AS tariff, t.duration_min "
        "FROM queue_numbers q "
        "LEFT JOIN users u ON u.user_id = q.user_id "
        "LEFT JOIN tariffs t ON q.tariff_id = t.id "
        "WHERE q.completed_at IS NOT NULL "
        "ORDER BY q.completed_at DESC LIMIT ?",
        (limit,),
    ).fetchall()


def build_report_stood(conn: sqlite3.Connection) -> str:
    rows = _iter_report_rows(conn, limit=50)
    lines = ["✅ Номера отстоявшие", "Период: последние 50 завершённых", ""]
    shown = 0
    for r in rows:
        duration_limit = r["duration_min"]
        if duration_limit is None:
            continue
        limit_sec = int(duration_limit or 0) * 60
        if limit_sec <= 0:
            continue
        start_ts = int(r["created_at"] or 0)
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts)
        if duration_sec < limit_sec:
            continue
        duration_min = int(duration_sec // 60)
        shown += 1
        lines.append(
            f"• {r['phone']} | {format_user_label(r['user_id'], r['username'])} | "
            f"{status_human(r['status'])} | {r['tariff']} | "
            f"встал: {format_ts(r['created_at'])} | слетел: {format_ts(r['completed_at'])} | "
            f"стоял: {format_duration(duration_sec)} ({duration_min} мин)"
        )
    if shown == 0:
        lines.append("Нет записей.")
    return "\n".join(lines)


def build_report_not_stood(conn: sqlite3.Connection) -> str:
    rows = _iter_report_rows(conn, limit=50)
    lines = ["❌ Номера не отстоявшие", "Период: последние 50 завершённых", ""]
    shown = 0
    for r in rows:
        duration_limit = r["duration_min"]
        if duration_limit is None:
            continue
        limit_sec = int(duration_limit or 0) * 60
        if limit_sec <= 0:
            continue
        start_ts = int(r["created_at"] or 0)
        end_ts = int(r["completed_at"] or now_ts())
        duration_sec = max(0, end_ts - start_ts)
        if duration_sec >= limit_sec:
            continue
        duration_min = int(duration_sec // 60)
        shown += 1
        lines.append(
            f"• {r['phone']} | {format_user_label(r['user_id'], r['username'])} | "
            f"{status_human(r['status'])} | {r['tariff']} | "
            f"встал: {format_ts(r['created_at'])} | слетел: {format_ts(r['completed_at'])} | "
            f"стоял: {format_duration(duration_sec)} ({duration_min} мин)"
        )
    if shown == 0:
        lines.append("Нет записей.")
    return "\n".join(lines)
