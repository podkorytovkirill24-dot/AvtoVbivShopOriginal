def create_withdraw_request_from_miniapp(tg_user: Dict, amount_value) -> Dict:
    user_id = int(tg_user["id"])
    try:
        amount = float(str(amount_value).replace(",", "."))
    except Exception:
        return {"ok": False, "error": "Введите корректную сумму."}
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO users (user_id, username, first_name, last_name, created_at, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name, last_seen=excluded.last_seen",
            (
                user_id,
                tg_user.get("username"),
                tg_user.get("first_name"),
                tg_user.get("last_name"),
                now_ts(),
                now_ts(),
            ),
        )
        balance = calculate_user_balance(conn, user_id)
        if amount <= 0:
            conn.rollback()
            return {"ok": False, "error": "Сумма должна быть больше 0."}
        if amount > balance:
            conn.rollback()
            return {"ok": False, "error": f"Недостаточно средств. Доступно: ${balance:.2f}"}
        conn.execute(
            "INSERT INTO withdrawal_requests (user_id, amount, status, created_at) VALUES (?, ?, 'pending', ?)",
            (user_id, amount, now_ts()),
        )
        conn.commit()
        request_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        return {"ok": True, "request_id": int(request_id), "amount": round(amount, 2)}
    finally:
        conn.close()
