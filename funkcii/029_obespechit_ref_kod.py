def ensure_ref_code(conn: sqlite3.Connection, user_id: int) -> str:
    row = conn.execute("SELECT ref_code FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if row and row["ref_code"]:
        return row["ref_code"]
    code = f"ref{user_id}"
    conn.execute(
        "UPDATE users SET ref_code = ? WHERE user_id = ?",
        (code, user_id),
    )
    conn.commit()
    return code
