async def job_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    conn = get_conn()
    now = now_ts()

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

    if get_config_bool(conn, "i_am_here_on"):
        minutes = get_config_int(conn, "i_am_here_minutes", 10)
        if minutes > 0:
            users = conn.execute(
                "SELECT u.user_id, u.iam_here_at, u.iam_warned_at "
                "FROM users u "
                "WHERE EXISTS (SELECT 1 FROM queue_numbers q WHERE q.user_id = u.user_id AND q.status = 'queued')"
            ).fetchall()
            for u in users:
                last_mark = int(u["iam_here_at"] or 0)
                if last_mark <= 0:
                    conn.execute(
                        "UPDATE users SET iam_here_at = ?, iam_warned_at = 0 WHERE user_id = ?",
                        (now, u["user_id"]),
                    )
                    last_mark = now
                deadline = last_mark + minutes * 60
                warn_before = 5 * 60
                if now >= deadline:
                    conn.execute(
                        "UPDATE queue_numbers SET status='canceled', completed_at = ? "
                        "WHERE user_id = ? AND status = 'queued'",
                        (now, u["user_id"]),
                    )
                    conn.execute(
                        "UPDATE users SET iam_warned_at = 0 WHERE user_id = ?",
                        (u["user_id"],),
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=u["user_id"],
                            text=(
                                "⛔ Ваши номера удалены из очереди, потому что вы не нажимали «Я тут»."
                            ),
                        )
                    except Exception:
                        pass
                elif deadline - now <= warn_before:
                    warned_at = int(u["iam_warned_at"] or 0)
                    if warned_at < last_mark:
                        try:
                            await context.bot.send_message(
                                chat_id=u["user_id"],
                                text=(
                                    "⏳ Напоминание: нажмите «Я тут». "
                                    "Осталось 5 минут, иначе номера удалятся из очереди."
                                ),
                            )
                        except Exception:
                            pass
                        conn.execute(
                            "UPDATE users SET iam_warned_at = ? WHERE user_id = ?",
                            (now, u["user_id"]),
                        )

    conn.commit()
    conn.close()
