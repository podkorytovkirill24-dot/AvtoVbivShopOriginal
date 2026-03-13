def create_payout_from_miniapp_admin(tg_admin: Dict, target_raw: str, amount_value, note: str = "") -> Dict:
    conn = get_conn()
    try:
        admin_id = int(tg_admin["id"])
        if not is_admin(conn, admin_id):
            return {"ok": False, "error": "Нет доступа."}
        target_user_id = resolve_user_id_input(conn, (target_raw or "").strip())
        if target_user_id is None:
            return {"ok": False, "error": "Пользователь не найден. Укажите @username или ID."}
        try:
            amount = float(str(amount_value).replace(",", "."))
        except Exception:
            return {"ok": False, "error": "Введите корректную сумму."}
        if amount <= 0:
            return {"ok": False, "error": "Сумма должна быть больше 0."}
        conn.execute(
            "INSERT INTO payouts (user_id, amount, note, created_at) VALUES (?, ?, ?, ?)",
            (target_user_id, amount, (note or "").strip(), now_ts()),
        )
        conn.commit()
        notify_user_direct(int(target_user_id), f"💸 Вам начислена выплата: ${amount:.2f}")
        log_admin_action(
            admin_id,
            tg_admin.get("username"),
            "miniapp_payout",
            f"target_id={target_user_id}|amount={amount:.2f}|note={(note or '').strip()}",
        )
        return {"ok": True, "target_user_id": int(target_user_id), "amount": round(amount, 2)}
    finally:
        conn.close()
