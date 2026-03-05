def get_config(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    return row["value"]
