def build_report_tariff(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT t.name, "
        "SUM(CASE WHEN q.status='success' THEN 1 ELSE 0 END) AS success, "
        "SUM(CASE WHEN q.status='slip' THEN 1 ELSE 0 END) AS slip, "
        "SUM(CASE WHEN q.status='error' THEN 1 ELSE 0 END) AS error, "
        "SUM(CASE WHEN q.status='canceled' THEN 1 ELSE 0 END) AS canceled "
        "FROM tariffs t LEFT JOIN queue_numbers q ON q.tariff_id = t.id "
        "GROUP BY t.id ORDER BY t.id"
    ).fetchall()
    lines = ["📈 Отчет по тарифам", "Формат: количество и Success rate", ""]
    for r in rows:
        processed = (
            int(r["success"] or 0)
            + int(r["slip"] or 0)
            + int(r["error"] or 0)
            + int(r["canceled"] or 0)
        )
        lines.append(
            f"• {r['name']}: "
            f"встал {r['success']} | слет {r['slip']} | ошибка {r['error']} | отменен {r['canceled']} | "
            f"всего {processed} | success rate {pct(int(r['success'] or 0), processed)}"
        )
    return "\n".join(lines)
