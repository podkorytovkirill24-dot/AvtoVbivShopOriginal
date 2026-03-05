def log_admin_action(admin_user_id: int, admin_username: Optional[str], action: str, details: str = "") -> None:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO admin_logs (admin_user_id, admin_username, action, details, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (admin_user_id, (admin_username or ""), action, details, now_ts()),
        )
        conn.commit()
    finally:
        conn.close()
