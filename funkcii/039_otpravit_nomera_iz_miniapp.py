def submit_numbers_from_miniapp(
    tg_user: Dict,
    numbers_text: str,
    tariff_id: int,
    reception_chat_id: Optional[int] = None,
    department_id: Optional[int] = None,
) -> Dict:
    user_id = int(tg_user["id"])
    username = tg_user.get("username")
    first_name = tg_user.get("first_name")
    last_name = tg_user.get("last_name")
    numbers = filter_kz_numbers(extract_numbers(numbers_text))
    if not numbers:
        return {"ok": False, "error": "Не вижу KZ номера. Формат: 7XXXXXXXXX"}

    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO users (user_id, username, first_name, last_name, created_at, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name, last_seen=excluded.last_seen",
            (user_id, username, first_name, last_name, now_ts(), now_ts()),
        )
        if reception_chat_id is None:
            rec_row = conn.execute(
                "SELECT chat_id FROM reception_groups WHERE tariff_id = ? AND is_active = 1 ORDER BY chat_title LIMIT 1",
                (tariff_id,),
            ).fetchone()
            reception_chat_id = int(rec_row["chat_id"]) if rec_row else 0
        reception = conn.execute(
            "SELECT chat_id FROM reception_groups WHERE chat_id = ? AND tariff_id = ? AND is_active = 1",
            (reception_chat_id, tariff_id),
        ).fetchone()
        if not reception:
            conn.rollback()
            return {"ok": False, "error": "Приемка не найдена для выбранного тарифа."}

        if department_id is not None:
            dep = conn.execute("SELECT id FROM departments WHERE id = ?", (department_id,)).fetchone()
            if not dep:
                conn.rollback()
                return {"ok": False, "error": "Выбранный отдел не найден."}

        allow_repeat = get_config_bool(conn, "allow_repeat", True)
        limit_per_day = get_config_int(conn, "limit_per_day", 0)
        if get_config_bool(conn, "stop_work"):
            conn.rollback()
            return {"ok": False, "error": "⛔ STOP-WORK. Приемка временно на паузе."}

        if limit_per_day > 0:
            tz = get_kz_tz() if "get_kz_tz" in globals() else None
            now = datetime.now(tz) if tz else datetime.now()
            start_day = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            cnt = conn.execute(
                "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE user_id = ? AND created_at >= ?",
                (user_id, start_day),
            ).fetchone()["cnt"]
            if cnt + len(numbers) > limit_per_day:
                conn.rollback()
                return {"ok": False, "error": f"Лимит сдачи на сегодня: {limit_per_day}."}

        pending_before = conn.execute(
            "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'queued' AND reception_chat_id = ?",
            (reception_chat_id,),
        ).fetchone()["cnt"]

        created_at = now_ts()
        if get_config_bool(conn, "i_am_here_on"):
            conn.execute(
                "UPDATE users SET iam_here_at = CASE WHEN iam_here_at > 0 THEN iam_here_at ELSE ? END, "
                "iam_warned_at = 0 WHERE user_id = ?",
                (created_at, user_id),
            )
        accepted = []
        skipped = []
        for idx, phone in enumerate(numbers, start=1):
            if not allow_repeat:
                exists = conn.execute(
                    "SELECT id FROM queue_numbers WHERE phone = ? AND status IN ('queued','taken','success')",
                    (phone,),
                ).fetchone()
                if exists:
                    skipped.append(phone)
                    continue
            conn.execute(
                "INSERT INTO queue_numbers "
                "(reception_chat_id, user_id, username, phone, status, created_at, tariff_id, department_id, photo_file_id) "
                "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)",
                (
                    reception_chat_id,
                    user_id,
                    username,
                    phone,
                    created_at + idx,
                    tariff_id,
                    department_id,
                    None,
                ),
            )
            accepted.append(phone)
        conn.commit()
        if not accepted:
            return {"ok": False, "error": "Номера не приняты (повторные запрещены).", "skipped": skipped}
        return {
            "ok": True,
            "accepted_count": len(accepted),
            "accepted": accepted[:30],
            "skipped_count": len(skipped),
            "queue_after": pending_before + len(accepted),
        }
    finally:
        conn.close()
