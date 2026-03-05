def upsert_user(conn: sqlite3.Connection, user) -> None:
    if user is None:
        return
    conn.execute(
        "INSERT INTO users (user_id, username, first_name, last_name, created_at, last_seen) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "username=excluded.username, first_name=excluded.first_name, "
        "last_name=excluded.last_name, last_seen=excluded.last_seen",
        (
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            now_ts(),
            now_ts(),
        ),
    )
