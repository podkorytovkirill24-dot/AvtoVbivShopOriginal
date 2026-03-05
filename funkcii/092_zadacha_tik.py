async def job_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    conn = get_conn()
    now = now_ts()
    if get_config_bool(conn, "auto_success_on"):
        minutes = get_config_int(conn, "auto_success_minutes", 5)
        if minutes > 0:
            rows = conn.execute(
                "SELECT id, user_id, phone FROM queue_numbers "
                "WHERE status='taken' AND assigned_at <= ?",
                (now - minutes * 60,),
            ).fetchall()
            for r in rows:
                conn.execute(
                    "UPDATE queue_numbers SET status='success', completed_at = ? WHERE id = ?",
                    (now, r["id"]),
                )
                if get_config_bool(conn, "notify_success"):
                    try:
                        await context.bot.send_message(
                            chat_id=r["user_id"],
                            text=f"✅ Ваш номер {r['phone']} встал.",
                        )
                    except Exception:
                        pass
    if get_config_bool(conn, "auto_slip_on"):
        minutes = get_config_int(conn, "auto_slip_minutes", 15)
        if minutes > 0:
            rows = conn.execute(
                "SELECT id, user_id, phone FROM queue_numbers "
                "WHERE status='taken' AND assigned_at <= ?",
                (now - minutes * 60,),
            ).fetchall()
            for r in rows:
                conn.execute(
                    "UPDATE queue_numbers SET status='slip', completed_at = ? WHERE id = ?",
                    (now, r["id"]),
                )
                if get_config_bool(conn, "notify_slip"):
                    try:
                        await context.bot.send_message(
                            chat_id=r["user_id"],
                            text=f"❌ Ваш номер {r['phone']} слетел.",
                        )
                    except Exception:
                        pass
    if get_config_bool(conn, "actualization_on"):
        minutes = get_config_int(conn, "actualization_minutes", 120)
        if minutes > 0:
            rows = conn.execute(
                "SELECT id, user_id, phone FROM queue_numbers "
                "WHERE status='queued' AND created_at <= ?",
                (now - minutes * 60,),
            ).fetchall()
            for r in rows:
                conn.execute(
                    "UPDATE queue_numbers SET status='canceled', completed_at = ? WHERE id = ?",
                    (now, r["id"]),
                )
    conn.commit()
    conn.close()
