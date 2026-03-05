def calculate_user_balance(conn: sqlite3.Connection, user_id: int) -> float:
    row = conn.execute(
        "SELECT IFNULL(SUM(t.price),0) AS total "
        "FROM queue_numbers q JOIN tariffs t ON q.tariff_id = t.id "
        "WHERE q.user_id = ? AND q.status = 'success' "
        "AND (t.duration_min <= 0 OR (q.completed_at IS NOT NULL AND q.completed_at <= ? - t.duration_min * 60))",
        (user_id, now_ts()),
    ).fetchone()
    earned = row["total"] if row else 0.0
    paid = conn.execute(
        "SELECT IFNULL(SUM(amount),0) AS total FROM withdrawal_requests "
        "WHERE user_id = ? AND status = 'paid'",
        (user_id,),
    ).fetchone()["total"]
    bonuses = conn.execute(
        "SELECT IFNULL(SUM(amount),0) AS total FROM payouts WHERE user_id = ?",
        (user_id,),
    ).fetchone()["total"]
    return float(earned + bonuses - paid)
