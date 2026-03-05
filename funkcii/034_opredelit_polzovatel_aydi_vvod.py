def resolve_user_id_input(conn: sqlite3.Connection, raw_value: str) -> Optional[int]:
    token = (raw_value or "").strip()
    if not token:
        return None
    if token.isdigit():
        return int(token)
    if token.startswith("@"):
        token = token[1:].strip()
    if not token:
        return None
    row = conn.execute(
        "SELECT user_id FROM users WHERE LOWER(username) = LOWER(?) ORDER BY last_seen DESC LIMIT 1",
        (token,),
    ).fetchone()
    return int(row["user_id"]) if row else None
