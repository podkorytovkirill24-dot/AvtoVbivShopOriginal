def build_report_general(conn: sqlite3.Connection) -> str:
    total = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers").fetchone()["cnt"]
    success = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='success'").fetchone()["cnt"]
    slip = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='slip'").fetchone()["cnt"]
    error = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='error'").fetchone()["cnt"]
    finished = success + slip + error
    return (
        "📈 Общий отчёт\n"
        f"• Сдано всего: {total}\n"
        f"• Встал: {success} ({pct(success, total)})\n"
        f"• Слет: {slip} ({pct(slip, total)})\n"
        f"• Ошибка: {error} ({pct(error, total)})\n"
        f"• Completion rate: {pct(finished, total)}\n"
        f"• Success rate: {pct(success, finished)}"
    )
