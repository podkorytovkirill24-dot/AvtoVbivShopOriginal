def build_report_general(conn: sqlite3.Connection) -> str:
    total = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers "
        "WHERE status IN ('success','slip','error','canceled')"
    ).fetchone()["cnt"]
    success = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='success'"
    ).fetchone()["cnt"]
    slip = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='slip'"
    ).fetchone()["cnt"]
    error = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='error'"
    ).fetchone()["cnt"]
    canceled = conn.execute(
        "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='canceled'"
    ).fetchone()["cnt"]
    finished = success + slip + error
    return (
        "📈 Общий отчет\n"
        f"• Завершено: {total}\n"
        f"• Встал: {success} ({pct(success, total)})\n"
        f"• Слет: {slip} ({pct(slip, total)})\n"
        f"• Ошибка: {error} ({pct(error, total)})\n"
        f"• Отменено: {canceled} ({pct(canceled, total)})\n"
        f"• Success rate: {pct(success, finished)}"
    )
