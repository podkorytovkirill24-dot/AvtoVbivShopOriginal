def build_service_text(conn: sqlite3.Connection) -> str:
    users = conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()["cnt"]
    queued = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'queued'").fetchone()["cnt"]
    taken = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'taken'").fetchone()["cnt"]
    done = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'success'").fetchone()["cnt"]
    slip = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'slip'").fetchone()["cnt"]
    error = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'error'").fetchone()["cnt"]
    db_size = 0
    try:
        db_size = os.path.getsize(DB_PATH)
    except Exception:
        pass
    uptime = format_duration(int(time.time()) - BOT_STARTED_AT)
    return (
        "🧰 Сервис\n"
        f"• Аптайм: {uptime}\n"
        f"• Пользователи: {users}\n"
        f"• Очередь: в ожидании {queued} | в работе {taken}\n"
        f"• Итоги: встал {done} | слет {slip} | ошибка {error}\n"
        f"• База: {db_size/1024:.1f} KB"
    )
