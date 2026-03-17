async def handle_private_state(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = get_state(context)
    if not state:
        return
    name = state["name"]
    text = (update.message.text or update.message.caption or "").strip()
    conn = get_conn()

    if name == "submit_numbers":
        numbers = filter_kz_numbers(extract_numbers(text))
        if not numbers:
            conn.close()
            await update.message.reply_text(f"Не вижу KZ номера.\n\n{SUBMIT_RULES_TEXT}")
            return
        tariff_id = state["data"].get("tariff_id")
        dept_id = state["data"].get("department_id")
        reception_chat_id = state["data"].get("reception_chat_id")
        if not reception_chat_id:
            conn.close()
            clear_state(context)
            await update.message.reply_text("Приемка не выбрана. Откройте меню и выберите тариф заново.")
            return
        allow_repeat = get_config_bool(conn, "allow_repeat", True)
        limit_per_day = get_config_int(conn, "limit_per_day", 0)
        if get_config_bool(conn, "stop_work"):
            conn.close()
            await update.message.reply_text("⛔ STOP-WORK\nПриемка временно на паузе. Попробуйте позже.")
            clear_state(context)
            return
        if limit_per_day > 0:
            tz = get_kz_tz() if "get_kz_tz" in globals() else None
            now = datetime.now(tz) if tz else datetime.now()
            start_day = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            cnt = conn.execute(
                "SELECT COUNT(*) AS cnt FROM queue_numbers "
                "WHERE user_id = ? AND created_at >= ?",
                (update.effective_user.id, int(start_day)),
            ).fetchone()["cnt"]
            if cnt + len(numbers) > limit_per_day:
                conn.close()
                await update.message.reply_text(f"Лимит сдачи на сегодня: {limit_per_day}.")
                clear_state(context)
                return

        photo_id = None
        if update.message.photo:
            photo_id = update.message.photo[-1].file_id

        pending_before = conn.execute(
            "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'queued' AND reception_chat_id = ?",
            (reception_chat_id,),
        ).fetchone()["cnt"]
        created_at = now_ts()
        if get_config_bool(conn, "i_am_here_on"):
            conn.execute(
                "UPDATE users SET iam_here_at = CASE WHEN iam_here_at > 0 THEN iam_here_at ELSE ? END, "
                "iam_warned_at = 0 WHERE user_id = ?",
                (created_at, update.effective_user.id),
            )
        accepted = []
        for idx, phone in enumerate(numbers, start=1):
            if not allow_repeat:
                exists = conn.execute(
                    "SELECT id FROM queue_numbers WHERE phone = ? "
                    "AND status IN ('queued','taken','success')",
                    (phone,),
                ).fetchone()
                if exists:
                    continue
            conn.execute(
                "INSERT INTO queue_numbers "
                "(reception_chat_id, user_id, username, phone, status, created_at, tariff_id, department_id, photo_file_id) "
                "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)",
                (
                    reception_chat_id,
                    update.effective_user.id,
                    update.effective_user.username,
                    phone,
                    created_at + idx,
                    tariff_id,
                    dept_id,
                    photo_id,
                ),
            )
            accepted.append(phone)
        conn.commit()
        conn.close()
        clear_state(context)
        if not accepted:
            await update.message.reply_text("Номера не приняты (повторные запрещены).")
            return
        await update.message.reply_text(build_accept_text(accepted, pending_before))
        return

    if name == "admin_tariff_add_name":
        if not text:
            conn.close()
            await update.message.reply_text("Введите название тарифа.")
            return
        set_state(context, "admin_tariff_add_price", title=text)
        conn.close()
        await update.message.reply_text("Введите цену (например 8 или 8.5):")
        return

    if name == "admin_tariff_add_price":
        title = state["data"].get("title")
        if not title:
            conn.close()
            clear_state(context)
            await update.message.reply_text("Название не найдено. Начните добавление тарифа заново.")
            return
        try:
            price = float(text.replace(",", "."))
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите цену числом (например 8 или 8.5).")
            return
        set_state(context, "admin_tariff_add_duration", title=title, price=price)
        conn.close()
        await update.message.reply_text("Введите длительность в минутах:")
        return

    if name == "admin_tariff_add_duration":
        title = state["data"].get("title")
        price = float(state["data"].get("price") or 0)
        if not title:
            conn.close()
            clear_state(context)
            await update.message.reply_text("Данные тарифа потеряны. Начните добавление тарифа заново.")
            return
        try:
            duration = int(text)
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите длительность числом (в минутах).")
            return
        conn.execute(
            "INSERT INTO tariffs (name, price, duration_min, priority) VALUES (?, ?, ?, 0)",
            (title, price, duration),
        )
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Тариф добавлен.")
        return

    if name == "admin_tariff_edit":
        tariff_id = state["data"].get("tariff_id")
        title, price, duration = parse_tariff_text(text)
        if not title:
            conn.close()
            await update.message.reply_text("Формат: Название | цена | минуты")
            return
        conn.execute(
            "UPDATE tariffs SET name = ?, price = ?, duration_min = ? WHERE id = ?",
            (title, price, duration, tariff_id),
        )
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Тариф обновлен.")
        return

    if name == "admin_tariff_delete":
        try:
            tariff_id = int(text)
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите ID тарифа.")
            return
        conn.execute("DELETE FROM tariffs WHERE id = ?", (tariff_id,))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Тариф удален.")
        return

    if name == "admin_department_add":
        if not text:
            conn.close()
            await update.message.reply_text("Введите название приемки.")
            return
        conn.execute("INSERT INTO departments (name) VALUES (?)", (text,))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Приемка добавлена.")
        return

    if name == "admin_department_edit":
        dept_id = state["data"].get("department_id")
        if not text:
            conn.close()
            await update.message.reply_text("Введите новое название.")
            return
        conn.execute("UPDATE departments SET name = ? WHERE id = ?", (text, dept_id))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Приемка обновлена.")
        return

    if name == "admin_department_delete":
        try:
            dept_id = int(text)
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите ID приемки.")
            return
        conn.execute("DELETE FROM departments WHERE id = ?", (dept_id,))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Приемка удалена.")
        return

    if name == "admin_office_add":
        if not text:
            conn.close()
            await update.message.reply_text("Введите название офиса.")
            return
        conn.execute("INSERT INTO offices (name) VALUES (?)", (text,))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Офис добавлен.")
        return

    if name == "admin_office_edit":
        office_id = state["data"].get("office_id")
        if not text:
            conn.close()
            await update.message.reply_text("Введите новое название.")
            return
        conn.execute("UPDATE offices SET name = ? WHERE id = ?", (text, office_id))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Офис обновлен.")
        return

    if name == "admin_office_delete":
        try:
            office_id = int(text)
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите ID офиса.")
            return
        conn.execute("DELETE FROM offices WHERE id = ?", (office_id,))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Офис удален.")
        return

    if name == "admin_set_priority":
        tariff_id = state["data"].get("tariff_id")
        try:
            priority = int(text)
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите число.")
            return
        conn.execute("UPDATE tariffs SET priority = ? WHERE id = ?", (priority, tariff_id))
        conn.commit()
        conn.close()
        clear_state(context)
        await update.message.reply_text("Приоритет обновлен.")
        return

    if name == "admin_limit":
        try:
            limit = int(text)
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите число.")
            return
        set_config(conn, "limit_per_day", str(limit))
        conn.close()
        clear_state(context)
        await update.message.reply_text("Лимит обновлен.")
        return

    if name == "admin_i_am_here":
        try:
            minutes = int(text)
        except ValueError:
            conn.close()
            return
        set_config(conn, "i_am_here_minutes", str(minutes))
        set_config(conn, "i_am_here_on", "1" if minutes > 0 else "0")
        conn.close()
        clear_state(context)
        if minutes > 0:
            await update.message.reply_text(f"Функция «Я тут» включена. Интервал: {minutes} мин.")
        else:
            await update.message.reply_text("Функция «Я тут» выключена.")
        return



    if name == "admin_auto_slip":
        try:
            minutes = int(text)
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите число минут.")
            return
        set_config(conn, "auto_slip_minutes", str(minutes))
        set_config(conn, "auto_slip_on", "1" if minutes > 0 else "0")
        conn.close()
        clear_state(context)
        if minutes > 0:
            await update.message.reply_text(f"Авто-слёт включен. Интервал: {minutes} мин.")
        else:
            await update.message.reply_text("Авто-слёт выключен.")
        return

    if name == "admin_lunch_text":
        if not text:
            conn.close()
            return
        set_config(conn, "lunch_text", text)
        lunch_on = get_config_bool(conn, "lunch_on")
        conn.close()
        clear_state(context)
        status = "ВКЛ" if lunch_on else "ВЫКЛ"
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("✏ Редактировать текст", callback_data="adm:lunch:edit")],
                [
                    InlineKeyboardButton("✅ Включить", callback_data="adm:lunch:on"),
                    InlineKeyboardButton("⛔ Выключить", callback_data="adm:lunch:off"),
                ],
                [InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")],
            ]
        )
        await update.message.reply_text(
            f"🍽 Расписание обедов\nСтатус: {status}\n\n{text}",
            reply_markup=keyboard,
        )
        return

    if name == "admin_add_admin":
        admin_id = resolve_user_id_input(conn, text)
        if admin_id is None:
            conn.close()
            await update.message.reply_text("Введите ЮЗ (@username) или ID пользователя.")
            return
        conn.execute("INSERT INTO admins (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING", (admin_id,))
        conn.commit()
        conn.close()
        log_admin_action(update.effective_user.id, update.effective_user.username, "add_admin", f"target_id={admin_id}")
        clear_state(context)
        await update.message.reply_text("Админ добавлен.")
        return

    if name == "admin_remove_admin":
        admin_id = resolve_user_id_input(conn, text)
        if admin_id is None:
            conn.close()
            await update.message.reply_text("Введите ЮЗ (@username) или ID пользователя.")
            return
        conn.execute("DELETE FROM admins WHERE user_id = ?", (admin_id,))
        conn.commit()
        conn.close()
        log_admin_action(update.effective_user.id, update.effective_user.username, "remove_admin", f"target_id={admin_id}")
        clear_state(context)
        await update.message.reply_text("Админ удален.")
        return

    if name == "admin_search_number":
        phone = "".join(extract_numbers(text))
        if not phone:
            conn.close()
            await update.message.reply_text("Введите номер.")
            return
        rows = conn.execute(
            "SELECT q.phone, q.status, q.created_at, q.completed_at, t.name AS tariff "
            "FROM queue_numbers q LEFT JOIN tariffs t ON q.tariff_id = t.id "
            "WHERE q.phone LIKE ? ORDER BY q.created_at DESC LIMIT 20",
            (f"%{phone}%",),
        ).fetchall()
        conn.close()
        clear_state(context)
        if not rows:
            await update.message.reply_text("Ничего не найдено.")
            return
        lines = ["🔍 Результаты поиска:"]
        for r in rows:
            lines.append(
                f"{r['phone']} | {status_human(r['status'])} | {r['tariff']} | {format_ts(r['created_at'])}"
            )
        await update.message.reply_text("\n".join(lines))
        return

    if name == "admin_broadcast":
        if not text and not update.message.photo:
            conn.close()
            await update.message.reply_text("Отправьте текст или фото.")
            return
        photo_id = update.message.photo[-1].file_id if update.message.photo else None
        users = conn.execute("SELECT user_id FROM users WHERE is_blocked = 0").fetchall()
        conn.close()
        sent = 0
        for u in users:
            try:
                if photo_id:
                    await context.bot.send_photo(chat_id=u["user_id"], photo=photo_id, caption=text or "")
                else:
                    await context.bot.send_message(chat_id=u["user_id"], text=text)
                sent += 1
            except Exception:
                continue
        clear_state(context)
        await update.message.reply_text(f"Рассылка завершена. Отправлено: {sent}.")
        return

    if name == "support_message":
        ticket_id = state["data"].get("ticket_id")
        conn.execute(
            "INSERT INTO support_messages (ticket_id, sender_id, text, created_at) VALUES (?, ?, ?, ?)",
            (ticket_id, update.effective_user.id, text, now_ts()),
        )
        conn.commit()
        admins = conn.execute("SELECT user_id FROM admins").fetchall()
        conn.close()
        for admin in admins:
            try:
                await context.bot.send_message(
                    chat_id=admin["user_id"],
                    text=(
                        f"🆘 Новое сообщение в поддержке #{ticket_id} "
                        f"от {format_user_label(update.effective_user.id, update.effective_user.username)}:\n{text}"
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Ответить", callback_data=f"adm:support_reply:{ticket_id}")]]
                    ),
                )
            except Exception:
                continue
        clear_state(context)
        await update.message.reply_text("Сообщение отправлено в поддержку.")
        return

    if name == "admin_support_reply":
        ticket_id = state["data"].get("ticket_id")
        ticket = conn.execute(
            "SELECT user_id FROM support_tickets WHERE id = ?",
            (ticket_id,),
        ).fetchone()
        if not ticket:
            conn.close()
            clear_state(context)
            await update.message.reply_text("Тикет не найден.")
            return
        conn.execute(
            "INSERT INTO support_messages (ticket_id, sender_id, text, created_at) VALUES (?, ?, ?, ?)",
            (ticket_id, update.effective_user.id, text, now_ts()),
        )
        conn.commit()
        conn.close()
        try:
            await context.bot.send_message(
                chat_id=ticket["user_id"],
                text=f"Ответ поддержки #{ticket_id}:\n{text}",
            )
        except Exception:
            pass
        clear_state(context)
        await update.message.reply_text("Ответ отправлен.")
        return

    if name == "user_withdraw":
        try:
            amount = float(text.replace(",", "."))
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите сумму.")
            return
        balance = calculate_user_balance(conn, update.effective_user.id)
        if amount <= 0 or amount > balance:
            conn.close()
            await update.message.reply_text(f"Недостаточно средств. Доступно: ${balance:.2f}")
            return
        conn.execute(
            "INSERT INTO withdrawal_requests (user_id, amount, status, created_at) "
            "VALUES (?, ?, 'pending', ?)",
            (update.effective_user.id, amount, now_ts()),
        )
        req_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        conn.commit()
        admins = conn.execute("SELECT user_id FROM admins").fetchall()
        conn.close()
        for admin in admins:
            try:
                await context.bot.send_message(
                    chat_id=admin["user_id"],
                    text=(
                        "💰 Новый запрос вывода:\n"
                        f"{format_user_label(update.effective_user.id, update.effective_user.username)}\n"
                        f"Сумма: ${amount:.2f}"
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton(f"✅ Оплачено #{req_id}", callback_data=f"adm:withdraw:pay:{req_id}")],
                            [InlineKeyboardButton(f"❌ Ошибка #{req_id}", callback_data=f"adm:withdraw:error:{req_id}")],
                        ]
                    ),
                )
            except Exception:
                continue
        clear_state(context)
        await update.message.reply_text("Запрос на вывод отправлен.")
        return

    if name == "admin_payout_user":
        user_id = resolve_user_id_input(conn, text)
        if user_id is None:
            conn.close()
            await update.message.reply_text("Пользователь не найден. Введите @username или ID.")
            return
        row = conn.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)).fetchone()
        label = format_user_label(user_id, row["username"] if row else None)
        set_state(context, "admin_payout_amount", user_id=user_id)
        conn.close()
        await update.message.reply_text(f"Введите сумму выплаты для {label}:")
        return

    if name == "admin_payout_amount":
        user_id = state["data"].get("user_id")
        if not user_id:
            conn.close()
            clear_state(context)
            await update.message.reply_text("Не найден пользователь. Начните заново.")
            return
        try:
            amount = float(text.replace(",", "."))
        except ValueError:
            conn.close()
            await update.message.reply_text("Введите сумму числом (например 110 или 110.5).")
            return
        if amount <= 0:
            conn.close()
            await update.message.reply_text("Сумма должна быть больше нуля.")
            return
        row = conn.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)).fetchone()
        conn.execute(
            "INSERT INTO payouts (user_id, amount, note, created_at) VALUES (?, ?, ?, ?)",
            (user_id, amount, "", now_ts()),
        )
        conn.commit()
        conn.close()
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"💸 Вам начислена выплата: ${amount:.2f}",
            )
        except Exception:
            pass
        clear_state(context)
        label = format_user_label(user_id, row["username"] if row else None)
        await update.message.reply_text(f"Выплата отправлена: {label} на ${amount:.2f}.")
        return


    if name == "mainmenu_text":
        set_config(conn, "main_menu_text", text)
        conn.close()
        clear_state(context)
        await update.message.reply_text("Текст главного меню обновлен.")
        return

    if name == "mainmenu_photo":
        if not update.message.photo:
            conn.close()
            await update.message.reply_text("Отправьте фото.")
            return
        photo_id = update.message.photo[-1].file_id
        set_config(conn, "main_menu_photo_id", photo_id)
        conn.close()
        clear_state(context)
        await update.message.reply_text("Фото главного меню обновлено.")
        return

    if name == "mainmenu_btn":
        key = state["data"].get("key")
        if key:
            set_config(conn, key, text)
        conn.close()
        clear_state(context)
        await update.message.reply_text("Кнопка обновлена.")
        return

    if name == "admin_report_date":
        try:
            dt = datetime.strptime(text, "%d.%m.%Y")
        except ValueError:
            conn.close()
            await update.message.reply_text("Неверный формат. Пример: 04.02.2026")
            return
        tz = get_kz_tz() if "get_kz_tz" in globals() else None
        if tz:
            dt = dt.replace(tzinfo=tz)
        start_ts = int(dt.timestamp())
        end_ts = int((dt + timedelta(days=1)).timestamp())
        rows = conn.execute(
            "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE completed_at BETWEEN ? AND ? AND status IN ('success','slip','error','canceled')",
            (start_ts, end_ts),
        ).fetchone()
        success = conn.execute(
            "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='success' AND completed_at BETWEEN ? AND ?",
            (start_ts, end_ts),
        ).fetchone()
        slip = conn.execute(
            "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='slip' AND completed_at BETWEEN ? AND ?",
            (start_ts, end_ts),
        ).fetchone()
        error = conn.execute(
            "SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status='error' AND completed_at BETWEEN ? AND ?",
            (start_ts, end_ts),
        ).fetchone()
        conn.close()
        clear_state(context)
        await update.message.reply_text(
            f"Отчет за {text}\n"
            f"Сдано: {rows['cnt']}\n"
            f"Встал: {success['cnt']} | Слет: {slip['cnt']} | Ошибки: {error['cnt']}"
        )
        return

    if name == "admin_user_search":
        user_id = resolve_user_id_input(conn, text)
        if user_id is None:
            conn.close()
            await update.message.reply_text("Введите корректный ЮЗ (@username) или ID.")
            return
        user = conn.execute(
            "SELECT user_id, username, last_seen, is_approved FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        conn.close()
        clear_state(context)
        if not user:
            await update.message.reply_text("Пользователь не найден.")
            return
        await update.message.reply_text(
            f"{format_user_label(user['user_id'], user['username'])}\n"
            f"Активность: {format_ts(user['last_seen'])}\n"
            f"Одобрен: {'да' if user['is_approved'] else 'нет'}"
        )
        return

    conn.close()
