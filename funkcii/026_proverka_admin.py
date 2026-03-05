def is_admin(conn: sqlite3.Connection, user_id: int) -> bool:
    row = conn.execute("SELECT user_id FROM admins WHERE user_id = ?", (user_id,)).fetchone()
    return row is not None
