async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != "private":
        return
    conn = get_conn()
    upsert_user(conn, update.effective_user)
    conn.commit()
    if update.message and update.message.text:
        parts = update.message.text.split(maxsplit=1)
        if len(parts) > 1 and parts[1].startswith("ref"):
            if get_config_bool(conn, "referral_enabled", True):
                ref_code = parts[1].strip()
                ref_row = conn.execute(
                    "SELECT user_id FROM users WHERE ref_code = ?",
                    (ref_code,),
                ).fetchone()
                if ref_row and ref_row["user_id"] != update.effective_user.id:
                    conn.execute(
                        "UPDATE users SET referred_by = ? WHERE user_id = ? AND referred_by IS NULL",
                        (ref_row["user_id"], update.effective_user.id),
                    )
                    conn.commit()
    conn.close()
    await send_main_menu(update, context)
