def build_miniapp_user_payload(user_id: int) -> Dict:
    conn = get_conn()
    try:
        is_admin_user = is_admin(conn, user_id)
        user = conn.execute(
            "SELECT username, first_name, last_name, created_at, last_seen "
            "FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        stats = conn.execute(
            "SELECT "
            "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success, "
            "SUM(CASE WHEN status='slip' THEN 1 ELSE 0 END) AS slip, "
            "SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error, "
            "SUM(CASE WHEN status='canceled' THEN 1 ELSE 0 END) AS canceled, "
            "COUNT(*) AS total "
            "FROM queue_numbers WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        balance = calculate_user_balance(conn, user_id)
        ref_code = ensure_ref_code(conn, user_id)
        invited = conn.execute(
            "SELECT COUNT(*) AS cnt FROM users WHERE referred_by = ?",
            (user_id,),
        ).fetchone()["cnt"]
        payouts = conn.execute(
            "SELECT COUNT(*) AS cnt, IFNULL(SUM(amount),0) AS total FROM payouts WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        withdrawals = conn.execute(
            "SELECT "
            "COUNT(*) AS total_cnt, "
            "SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END) AS paid_cnt, "
            "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending_cnt, "
            "IFNULL(SUM(CASE WHEN status='paid' THEN amount ELSE 0 END),0) AS paid_sum "
            "FROM withdrawal_requests WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        stood = conn.execute(
            "SELECT "
            "COUNT(*) AS cnt, "
            "IFNULL(SUM(t.price),0) AS total "
            "FROM queue_numbers q JOIN tariffs t ON q.tariff_id = t.id "
            "WHERE q.user_id = ? AND q.status = 'success' "
            "AND (t.duration_min <= 0 OR (q.completed_at IS NOT NULL AND q.completed_at <= ? - t.duration_min * 60))",
            (user_id, now_ts()),
        ).fetchone()
        recent_numbers = conn.execute(
            "SELECT phone, status, created_at, completed_at FROM queue_numbers "
            "WHERE user_id = ? ORDER BY created_at DESC LIMIT 20",
            (user_id,),
        ).fetchall()
        recent_withdrawals = conn.execute(
            "SELECT amount, status, created_at, updated_at FROM withdrawal_requests "
            "WHERE user_id = ? ORDER BY created_at DESC LIMIT 20",
            (user_id,),
        ).fetchall()
        recent_payouts = conn.execute(
            "SELECT amount, note, created_at FROM payouts "
            "WHERE user_id = ? ORDER BY created_at DESC LIMIT 20",
            (user_id,),
        ).fetchall()
        tariffs = conn.execute(
            "SELECT id, name, price, duration_min FROM tariffs ORDER BY id"
        ).fetchall()
        success = int(stats["success"] or 0)
        slip = int(stats["slip"] or 0)
        error = int(stats["error"] or 0)
        finished = success + slip + error
        ref_link = ""
        if BOT_PUBLIC_USERNAME:
            ref_link = f"https://t.me/{BOT_PUBLIC_USERNAME}?start={ref_code}"
        full_name = " ".join([x for x in [(user["first_name"] if user else ""), (user["last_name"] if user else "")] if x]).strip()
        admin_payload = {"enabled": False}
        if is_admin_user:
            admin_numbers = conn.execute(
                "SELECT q.id, q.phone, q.status, q.created_at, q.assigned_at, q.completed_at, "
                "q.user_id, u.username, t.name AS tariff_name, t.duration_min, t.price "
                "FROM queue_numbers q "
                "LEFT JOIN users u ON u.user_id = q.user_id "
                "LEFT JOIN tariffs t ON t.id = q.tariff_id "
                "ORDER BY q.created_at DESC LIMIT 250"
            ).fetchall()
            pending_withdrawals = conn.execute(
                "SELECT COUNT(*) AS cnt FROM withdrawal_requests WHERE status = 'pending'"
            ).fetchone()["cnt"]
            admin_payload = {
                "enabled": True,
                "pending_withdrawals": int(pending_withdrawals or 0),
                "numbers": [
                    {
                        "id": int(r["id"]),
                        "phone": r["phone"],
                        "status": status_human(r["status"]),
                        "tariff_name": r["tariff_name"] or "-",
                        "price": float(r["price"] or 0),
                        "duration_min": int(r["duration_min"] or 0),
                        "submitter_id": int(r["user_id"] or 0),
                        "submitter_username": r["username"] or "",
                        "created_at": format_ts(r["created_at"]),
                        "assigned_at": format_ts(r["assigned_at"]),
                        "completed_at": format_ts(r["completed_at"]),
                        "stood_min": int(max(0, ((r["completed_at"] or now_ts()) - (r["assigned_at"] or r["created_at"] or now_ts())) // 60)),
                        "eligible_paid": bool(
                            r["status"] == "success"
                            and (r["duration_min"] or 0) > 0
                            and r["completed_at"]
                            and r["completed_at"] <= now_ts() - int(r["duration_min"] or 0) * 60
                        ) or bool(r["status"] == "success" and int(r["duration_min"] or 0) <= 0),
                    }
                    for r in admin_numbers
                ],
            }
        return {
            "profile": {
                "user_id": user_id,
                "username": user["username"] if user else "",
                "full_name": full_name,
                "created_at": format_ts(user["created_at"]) if user else "-",
                "last_seen": format_ts(user["last_seen"]) if user else "-",
            },
            "finance": {
                "balance": round(float(balance), 2),
                "payouts_count": int(payouts["cnt"] or 0),
                "payouts_total": round(float(payouts["total"] or 0), 2),
                "withdrawals_total": int(withdrawals["total_cnt"] or 0),
                "withdrawals_paid": int(withdrawals["paid_cnt"] or 0),
                "withdrawals_pending": int(withdrawals["pending_cnt"] or 0),
                "withdrawals_paid_sum": round(float(withdrawals["paid_sum"] or 0), 2),
            },
            "queue": {
                "submitted": int(stats["total"] or 0),
                "success": success,
                "slip": slip,
                "error": error,
                "canceled": int(stats["canceled"] or 0),
                "success_rate": pct(success, finished),
                "stood_count": int(stood["cnt"] or 0),
                "stood_amount": round(float(stood["total"] or 0), 2),
            },
            "referrals": {
                "invited": int(invited),
                "ref_code": ref_code,
                "ref_link": ref_link,
            },
            "activity": {
                "numbers": [
                    {
                        "phone": r["phone"],
                        "status": status_human(r["status"]),
                        "created_at": format_ts(r["created_at"]),
                        "completed_at": format_ts(r["completed_at"]),
                    }
                    for r in recent_numbers
                ],
                "withdrawals": [
                    {
                        "amount": round(float(r["amount"] or 0), 2),
                        "status": status_human(r["status"]),
                        "created_at": format_ts(r["created_at"]),
                        "updated_at": format_ts(r["updated_at"]),
                    }
                    for r in recent_withdrawals
                ],
                "payouts": [
                    {
                        "amount": round(float(r["amount"] or 0), 2),
                        "note": r["note"] or "",
                        "created_at": format_ts(r["created_at"]),
                    }
                    for r in recent_payouts
                ],
            },
            "submit_options": {
                "tariffs": [
                    {
                        "id": int(t["id"]),
                        "name": t["name"],
                        "price": float(t["price"] or 0),
                        "duration_min": int(t["duration_min"] or 0),
                    }
                    for t in tariffs
                ],
            },
            "admin": admin_payload,
        }
    finally:
        conn.close()
