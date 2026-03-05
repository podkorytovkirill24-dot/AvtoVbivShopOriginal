def build_report_tariff(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT t.name, "
        "SUM(CASE WHEN q.status='queued' THEN 1 ELSE 0 END) AS queued, "
        "SUM(CASE WHEN q.status='taken' THEN 1 ELSE 0 END) AS taken, "
        "SUM(CASE WHEN q.status='success' THEN 1 ELSE 0 END) AS success, "
        "SUM(CASE WHEN q.status='slip' THEN 1 ELSE 0 END) AS slip, "
        "SUM(CASE WHEN q.status='error' THEN 1 ELSE 0 END) AS error "
        "FROM tariffs t LEFT JOIN queue_numbers q ON q.tariff_id = t.id "
        "GROUP BY t.id ORDER BY t.id"
    ).fetchall()
    lines = ["📈 Отчёт по тарифам", "Формат: количество и Success rate", ""]
    for r in rows:
        processed = int(r["success"] or 0) + int(r["slip"] or 0) + int(r["error"] or 0)
        lines.append(
            f"• {r['name']}: в ожидании {r['queued']} | в работе {r['taken']} | "
            f"встал {r['success']} | слет {r['slip']} | ошибка {r['error']} | "
            f"success rate {pct(int(r['success'] or 0), processed)}"
        )
    return "\n".join(lines)
