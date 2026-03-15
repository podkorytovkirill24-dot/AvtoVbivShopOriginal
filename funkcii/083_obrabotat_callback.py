def _short_title(text: str, limit: int = 26) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _build_issue_by_departments_menu(conn: sqlite3.Connection) -> Tuple[str, InlineKeyboardMarkup]:
    current = get_config_bool(conn, "issue_by_departments", False)
    rows = conn.execute(
        "SELECT p.chat_id, p.thread_id, p.chat_title, "
        "r.chat_title AS reception_title, t.name AS tariff_name "
        "FROM processing_topics p "
        "LEFT JOIN reception_groups r ON p.reception_chat_id = r.chat_id "
        "LEFT JOIN tariffs t ON r.tariff_id = t.id "
        "ORDER BY p.chat_id, p.thread_id"
    ).fetchall()
    status = "ВКЛ" if current else "ВЫКЛ"
    lines = ["🗂 Выдача по отделам", f"Статус: {status}", ""]
    keyboard: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🔁 Включить/выключить", callback_data="adm:issue_by_departments:toggle")]
    ]
    if not rows:
        lines.append("Привязок /set нет.")
        lines.append("Сначала сделайте /set в нужной группе/теме.")
    else:
        for r in rows:
            title = r["chat_title"] or str(r["chat_id"])
            topic = f"тема {r['thread_id']}" if r["thread_id"] else "без темы"
            target = r["tariff_name"] or r["reception_title"] or "не назначено"
            lines.append(f"• {title} ({topic}) → {target}")
            keyboard.append(
                [InlineKeyboardButton(f"Настроить { _short_title(title) }", callback_data=f"adm:issue_by_departments:topic:{r['chat_id']}:{r['thread_id']}")]
            )
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")])
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    parts = data.split(":")

    if data.startswith("menu:"):
        action = parts[1]
        if action == "submit":
            await menu_show_tariffs(context, query.from_user.id, message=query.message)
            await query.answer()
            return
        if action == "queue":
            await menu_show_queue(context, query.from_user.id, query.from_user.id, message=query.message)
            await query.answer()
            return
        if action == "archive":
            await menu_show_archive(context, query.from_user.id, query.from_user.id, message=query.message)
            await query.answer()
            return
        if action == "profile":
            await menu_show_profile(context, query.from_user.id, query.from_user.id, message=query.message)
            await query.answer()
            return
        if action == "support":
            clear_state(context)
            await menu_start_support(context, query.from_user.id, query.from_user.id, message=query.message)
            await query.answer()
            return
        if action == "admin":
            conn = get_conn()
            if not is_admin(conn, query.from_user.id):
                conn.close()
                await query.answer(ui("no_access"), show_alert=True)
                return
            conn.close()
            await send_or_update(
                context,
                query.from_user.id,
                ui("admin_panel_title"),
                reply_markup=build_admin_panel(),
                message=query.message,
            )
            await query.answer()
            return
        await query.answer()
        return

    if data == "adm:panel":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        conn.close()
        await query.edit_message_text(ui("admin_panel_title"), reply_markup=build_admin_panel())
        await query.answer()
        return

    if data == "adm:service":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        conn.close()
        await query.edit_message_text(ui("service_title"), reply_markup=build_service_menu())
        await query.answer()
        return

    if data == "adm:service:info":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        text_info = build_service_text(conn)
        conn.close()
        await query.edit_message_text(text_info, reply_markup=build_service_menu())
        await query.answer()
        return

    if data == "adm:service:logs":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        text_logs = build_admin_logs_text(conn)
        conn.close()
        await query.edit_message_text(text_logs, reply_markup=build_service_menu())
        await query.answer()
        return

    if data == "adm:service:export_queue":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        csv_data = build_queue_csv(conn)
        conn.close()
        filename = "queue.csv"
        await query.message.reply_document(InputFile(io.BytesIO(csv_data.encode("utf-8")), filename=filename))
        await query.answer("Готово")
        return

    if data == "adm:service:clear_queue":
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("✅ Очистить", callback_data="adm:service:clear_queue_confirm")],
                [InlineKeyboardButton("↩ Назад", callback_data="adm:service")],
            ]
        )
        await query.edit_message_text("Очистить активную очередь?", reply_markup=keyboard)
        await query.answer()
        return

    if data == "adm:service:clear_queue_confirm":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        conn.execute(
            "UPDATE queue_numbers SET status='canceled', completed_at = ? WHERE status IN ('queued','taken')",
            (now_ts(),),
        )
        conn.commit()
        conn.close()
        log_admin_action(query.from_user.id, query.from_user.username, "queue_clear", "status in queued,taken -> canceled")
        await query.edit_message_text("✅ Очередь очищена.", reply_markup=build_service_menu())
        await query.answer()
        return

    if data == "adm:settings":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        text = ui("settings_title")
        keyboard = build_settings_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data.startswith("adm:toggle:"):
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        key = parts[2]
        current = get_config_bool(conn, key)
        set_config(conn, key, "0" if current else "1")
        keyboard = build_settings_menu(conn)
        conn.close()
        log_admin_action(query.from_user.id, query.from_user.username, "toggle_setting", f"{key}={'0' if current else '1'}")
        await query.edit_message_text(ui("settings_title"), reply_markup=keyboard)
        await query.answer("Обновлено")
        return

    if data == "adm:notifications":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        keyboard = build_notifications_menu(conn)
        conn.close()
        await query.edit_message_text("🔔 Уведомления", reply_markup=keyboard)
        await query.answer()
        return

    if data == "adm:tariffs":
        clear_state(context)
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        text, keyboard = build_tariffs_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data == "adm:tariff:add":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        conn.close()
        set_state(context, "admin_tariff_add_name")
        await query.edit_message_text(
            "Введите название тарифа:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="adm:tariffs")]]),
        )
        await query.answer()
        return

    if data.startswith("user:repeat:") or data.startswith("user:qr:"):
        action = parts[1]
        queue_id = int(parts[2])
        conn = get_conn()
        row = conn.execute(
            "SELECT user_id, phone, worker_chat_id, worker_thread_id, worker_msg_id FROM queue_numbers WHERE id = ?",
            (queue_id,),
        ).fetchone()
        conn.close()
        if not row or row["user_id"] != query.from_user.id:
            await query.answer("Нет доступа", show_alert=True)
            return
        if not row["worker_chat_id"]:
            await query.answer("Оператор еще не назначен", show_alert=True)
            return
        phone_display = format_phone(row["phone"])
        action_label = "Повтор кода" if action == "repeat" else "Запрос QR"
        notify_text = (
            f"🔔 {action_label} от {format_user_label(query.from_user.id, query.from_user.username)}\n"
            f"Номер: {phone_display}"
        )
        try:
            await context.bot.send_message(
                chat_id=row["worker_chat_id"],
                message_thread_id=row["worker_thread_id"] or None,
                text=notify_text,
                reply_to_message_id=row["worker_msg_id"] or None,
            )
        except Exception:
            pass
        await query.answer("Запрос отправлен")
        return

    if data == "user:i_am_here":
        conn = get_conn()
        conn.execute(
            "UPDATE users SET iam_here_at = ?, iam_warned_at = 0 WHERE user_id = ?",
            (now_ts(), query.from_user.id),
        )
        conn.commit()
        conn.close()
        await query.answer("Отметка принята")
        return

    if data == "user:lunch":
        conn = get_conn()
        text = get_config(conn, "lunch_text", "").strip()
        conn.close()
        if not text:
            text = "Расписание обедов пока не настроено."
        await send_or_update(
            context,
            query.from_user.id,
            f"🍽 Расписание обедов\n\n{text}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="user:home")]]),
            message=query.message,
        )
        await query.answer()
        return

    if data == "adm:tariff:edit":
        conn = get_conn()
        tariffs = conn.execute("SELECT id, name FROM tariffs ORDER BY id").fetchall()
        conn.close()
        if not tariffs:
            await query.answer("Нет тарифов", show_alert=True)
            return
        keyboard = [[InlineKeyboardButton(f"{t['id']} {t['name']}", callback_data=f"adm:tariff:edit:{t['id']}")] for t in tariffs]
        keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="adm:tariffs")])
        await query.edit_message_text("Выберите тариф:", reply_markup=InlineKeyboardMarkup(keyboard))
        await query.answer()
        return

    if data.startswith("adm:tariff:edit:"):
        tariff_id = int(parts[3])
        set_state(context, "admin_tariff_edit", tariff_id=tariff_id)
        await query.edit_message_text("Введите: Название | цена | минуты")
        await query.answer()
        return

    if data == "adm:tariff:delete":
        set_state(context, "admin_tariff_delete")
        await query.edit_message_text("Введите ID тарифа для удаления:")
        await query.answer()
        return

    if data == "adm:priorities":
        conn = get_conn()
        tariffs = conn.execute("SELECT id, name, priority FROM tariffs ORDER BY id").fetchall()
        conn.close()
        if not tariffs:
            await query.edit_message_text(ui("empty_tariffs"), reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")]]
            ))
            await query.answer()
            return
        lines = ["⚡ Приоритеты тарифов:"]
        keyboard = []
        for t in tariffs:
            lines.append(f"{t['id']}. {t['name']} — {t['priority']}")
            keyboard.append([InlineKeyboardButton(f"Изменить {t['id']}", callback_data=f"adm:priority:{t['id']}")])
        keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")])
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard))
        await query.answer()
        return

    if data.startswith("adm:priority:"):
        tariff_id = int(parts[2])
        set_state(context, "admin_set_priority", tariff_id=tariff_id)
        await query.edit_message_text("Введите новый приоритет (число):")
        await query.answer()
        return

    if data == "adm:departments":
        conn = get_conn()
        text, keyboard = build_departments_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data.startswith("adm:reception:delete:"):
        chat_id = int(parts[3])
        conn = get_conn()
        conn.execute("DELETE FROM reception_groups WHERE chat_id = ?", (chat_id,))
        conn.commit()
        text, keyboard = build_departments_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer("Удалено")
        return

    if data == "adm:offices":
        conn = get_conn()
        text, keyboard = build_offices_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data.startswith("adm:topic:delete:"):
        chat_id = int(parts[3])
        thread_id = int(parts[4])
        conn = get_conn()
        conn.execute(
            "DELETE FROM processing_topics WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        )
        conn.commit()
        text, keyboard = build_offices_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer("Удалено")
        return

    if data.startswith("set_topic:"):
        chat_id = int(parts[1])
        thread_id = int(parts[2])
        reception_chat_id = int(parts[3])
        conn = get_conn()
        if not (is_admin(conn, query.from_user.id) or await is_chat_admin(chat_id, query.from_user.id, context)):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        chat_title = query.message.chat.title if query.message else None
        conn.execute(
            "INSERT INTO processing_topics (chat_id, thread_id, reception_chat_id, chat_title) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "reception_chat_id = excluded.reception_chat_id, "
            "chat_title = excluded.chat_title",
            (chat_id, thread_id, reception_chat_id, chat_title),
        )
        conn.commit()
        conn.close()
        await query.edit_message_text("✅ Тема привязана к приемке.")
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                message_thread_id=thread_id if thread_id > 0 else None,
                text="📦 Рабочая панель\n"
                "Чтобы получить номер, нажмите кнопку «Взять номер».\n\n"
                f"{WORKER_RULES_TEXT}",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("📥 Взять номер", callback_data="topic:next")]]
                ),
            )
        except Exception:
            pass
        await query.answer("Готово")
        return

    if data.startswith("set_reception:"):


        chat_id = int(parts[1])
        tariff_id = int(parts[2])
        conn = get_conn()
        if not (is_admin(conn, query.from_user.id) or await is_chat_admin(chat_id, query.from_user.id, context)):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        tariff = conn.execute(
            "SELECT name, price, duration_min FROM tariffs WHERE id = ?",
            (tariff_id,),
        ).fetchone()
        conn.execute(
            "INSERT INTO reception_groups (chat_id, chat_title, tariff_id, is_active) "
            "VALUES (?, ?, ?, 1) "
            "ON CONFLICT(chat_id) DO UPDATE SET tariff_id = excluded.tariff_id, is_active = 1",
            (
                chat_id,
                query.message.chat.title if query.message else str(chat_id),
                tariff_id,
            ),
        )
        conn.commit()
        conn.close()
        if tariff:
            hint = build_submit_hint(tariff["name"], tariff["duration_min"], tariff["price"])
            await query.edit_message_text(f"✅ Приемка настроена.\n\n{hint}")
        else:
            await query.edit_message_text("✅ Приемка настроена. Тариф привязан к этой группе.")
        await query.answer("Готово")
        return

    if data == "adm:mainmenu":
        conn = get_conn()
        text, keyboard = build_main_menu_settings(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data == "adm:mainmenu:text":
        set_state(context, "mainmenu_text")
        await query.edit_message_text("Введите новый текст главного меню:")
        await query.answer()
        return

    if data == "adm:mainmenu:photo":
        set_state(context, "mainmenu_photo")
        await query.edit_message_text("Отправьте новое фото для главного меню:")
        await query.answer()
        return

    if data.startswith("adm:mainmenu:btn:"):
        key_map = {
            "submit": "menu_btn_submit",
            "queue": "menu_btn_queue",
            "archive": "menu_btn_archive",
            "profile": "menu_btn_profile",
            "support": "menu_btn_support",
        }
        key = key_map.get(parts[3])
        if not key:
            await query.answer("Не найдено", show_alert=True)
            return
        set_state(context, "mainmenu_btn", key=key)
        await query.edit_message_text("Введите новый текст кнопки:")
        await query.answer()
        return

    if data == "adm:mainmenu:reset":
        conn = get_conn()
        for k, v in DEFAULT_CONFIG.items():
            if k.startswith("menu_btn_") or k.startswith("main_menu_"):
                set_config(conn, k, v)
        conn.close()
        await query.edit_message_text("Сброшено.", reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅ Назад", callback_data="adm:mainmenu")]]
        ))
        await query.answer()
        return

    if data == "adm:stats:today" or data.startswith("adm:stats:"):
        period = parts[2] if len(parts) > 2 else "today"
        conn = get_conn()
        text = build_stats_text(conn, period)
        conn.close()
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Сегодня", callback_data="adm:stats:today"),
                    InlineKeyboardButton("Вчера", callback_data="adm:stats:yesterday"),
                    InlineKeyboardButton("7 дней", callback_data="adm:stats:7d"),
                ],
                [
                    InlineKeyboardButton("30 дней", callback_data="adm:stats:30d"),
                    InlineKeyboardButton("Всё время", callback_data="adm:stats:all"),
                ],
                [InlineKeyboardButton("⬇ CSV за период", callback_data=f"adm:stats_csv:{period}")],
                [InlineKeyboardButton("⬇ CSV за всё время", callback_data="adm:stats_csv:all")],
                [InlineKeyboardButton("⬅ Назад", callback_data="adm:panel")],
            ]
        )
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data.startswith("adm:stats_csv:"):
        period = parts[2]
        conn = get_conn()
        csv_data = build_csv(conn, period)
        conn.close()
        filename = f"stats_{period}.csv"
        await query.message.reply_document(InputFile(io.BytesIO(csv_data.encode("utf-8")), filename=filename))
        await query.answer("Готово")
        return

    if data == "adm:reports":
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("1) Детальный отчёт", callback_data="adm:report:detailed")],
                [InlineKeyboardButton("2) Просмотр за дату", callback_data="adm:report:date")],
                [InlineKeyboardButton("3) Номера отстоявшие", callback_data="adm:report:stood")],
                [InlineKeyboardButton("4) Номера не отстоявшие", callback_data="adm:report:not_stood")],
                [InlineKeyboardButton("⬅ Назад", callback_data="adm:panel")],
            ]
        )
        await query.edit_message_text(ui("reports_menu_title"), reply_markup=keyboard)
        await query.answer()
        return

    if data.startswith("adm:report:"):
        report_type = parts[2]
        conn = get_conn()
        if report_type == "tariff":
            text = build_report_tariff(conn)
        elif report_type == "general":
            text = build_report_general(conn)
        elif report_type == "detailed":
            text = build_report_detailed(conn)
        elif report_type == "sim":
            text = build_report_general(conn)
        elif report_type == "stood":
            text = build_report_stood(conn)
        elif report_type == "not_stood":
            text = build_report_not_stood(conn)
        elif report_type == "date":
            set_state(context, "admin_report_date")
            conn.close()
            await query.edit_message_text("📅 Введите дату в формате: ДД.ММ.ГГГГ")
            await query.answer()
            return
        else:
            text = "Отчёт не найден."
        conn.close()
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅ Назад", callback_data="adm:reports")]]
        ))
        await query.answer()
        return

    if data.startswith("adm:tops:"):
        metric = parts[2]
        period = parts[3] if len(parts) > 3 else "all"
        conn = get_conn()
        text = build_tops(conn, metric, period)
        conn.close()
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Сдано номеров", callback_data="adm:tops:submitted:all"),
                    InlineKeyboardButton("Приглашенные", callback_data="adm:tops:invited:all"),
                ],
                [
                    InlineKeyboardButton("Встал", callback_data="adm:tops:success:all"),
                    InlineKeyboardButton("Слетел", callback_data="adm:tops:slip:all"),
                    InlineKeyboardButton("Ошибки", callback_data="adm:tops:error:all"),
                ],
                [
                    InlineKeyboardButton("Сегодня", callback_data=f"adm:tops:{metric}:today"),
                    InlineKeyboardButton("Вчера", callback_data=f"adm:tops:{metric}:yesterday"),
                    InlineKeyboardButton("7 дней", callback_data=f"adm:tops:{metric}:7d"),
                ],
                [
                    InlineKeyboardButton("30 дней", callback_data=f"adm:tops:{metric}:30d"),
                    InlineKeyboardButton("Всё время", callback_data=f"adm:tops:{metric}:all"),
                ],
                [InlineKeyboardButton("⬇ CSV", callback_data=f"adm:tops_csv:{metric}:{period}")],
                [InlineKeyboardButton("⬅ Назад", callback_data="adm:panel")],
            ]
        )
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data.startswith("adm:tops_csv:"):
        metric = parts[2]
        period = parts[3]
        conn = get_conn()
        csv_data = build_tops_csv(conn, metric, period)
        conn.close()
        filename = f"tops_{metric}_{period}.csv"
        await query.message.reply_document(InputFile(io.BytesIO(csv_data.encode("utf-8")), filename=filename))
        await query.answer("Готово")
        return

    if data == "adm:users":
        conn = get_conn()
        total = conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()["cnt"]
        lines = [f"👥 Пользователи: {total}"]
        rows = conn.execute(
            "SELECT user_id, username, last_seen FROM users ORDER BY last_seen DESC LIMIT 10"
        ).fetchall()
        for r in rows:
            lines.append(f"{format_user_label(r['user_id'], r['username'])} | {format_ts(r['last_seen'])}")
        conn.close()
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🔎 Поиск по ЮЗ/ID", callback_data="adm:user:search")],
                [InlineKeyboardButton("⬅ Назад", callback_data="adm:panel")],
            ]
        )
        await query.edit_message_text("\n".join(lines), reply_markup=keyboard)
        await query.answer()
        return

    if data == "adm:user:search":
        set_state(context, "admin_user_search")
        await query.edit_message_text("Введите ЮЗ (@username) или ID пользователя:")
        await query.answer()
        return

    if data == "adm:queue":
        conn = get_conn()
        queued = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'queued'").fetchone()["cnt"]
        taken = conn.execute("SELECT COUNT(*) AS cnt FROM queue_numbers WHERE status = 'taken'").fetchone()["cnt"]
        conn.close()
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("?? ???????? ???????", callback_data="adm:queue:clear")],
                [InlineKeyboardButton("? ?????", callback_data="adm:panel")],
            ]
        )
        await query.edit_message_text(
            f"""?? ???????
? ????????: {queued}
? ??????: {taken}""",
            reply_markup=keyboard,
        )
        await query.answer()
        return

    if data == "adm:payouts":
        conn = get_conn()
        if not is_admin(conn, query.from_user.id):
            conn.close()
            await query.answer(ui("no_access"), show_alert=True)
            return
        conn.close()
        set_state(context, "admin_payout_user")
        await query.edit_message_text(
            "Введите @username или ID пользователя для выплаты:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="adm:panel")]]),
        )
        await query.answer()
        return

    if data == "adm:lunch":
        conn = get_conn()
        lunch_text = get_config(conn, "lunch_text", "").strip()
        lunch_on = get_config_bool(conn, "lunch_on")
        conn.close()
        if not lunch_text:
            set_state(context, "admin_lunch_text")
            await query.edit_message_text(
                "Введите текст для расписания обедов (время или любой текст):",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")]]),
            )
            await query.answer()
            return
        status = "ВКЛ" if lunch_on else "ВЫКЛ"
        body = lunch_text or "Расписание пока не задано."
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
        await query.edit_message_text(
            f"🍽 Расписание обедов\nСтатус: {status}\n\n{body}",
            reply_markup=keyboard,
        )
        await query.answer()
        return

    if data == "adm:lunch:edit":
        set_state(context, "admin_lunch_text")
        await query.edit_message_text("Введите новый текст для расписания обедов:")
        await query.answer()
        return

    if data == "adm:lunch:on" or data == "adm:lunch:off":
        conn = get_conn()
        set_config(conn, "lunch_on", "1" if data.endswith(":on") else "0")
        lunch_text = get_config(conn, "lunch_text", "").strip()
        lunch_on = get_config_bool(conn, "lunch_on")
        conn.close()
        status = "ВКЛ" if lunch_on else "ВЫКЛ"
        body = lunch_text or "Расписание пока не задано."
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
        await query.edit_message_text(
            f"🍽 Расписание обедов\nСтатус: {status}\n\n{body}",
            reply_markup=keyboard,
        )
        await query.answer("Обновлено")
        return

    if data == "adm:issue_by_departments":
        conn = get_conn()
        text, keyboard = _build_issue_by_departments_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer()
        return

    if data == "adm:issue_by_departments:toggle":
        conn = get_conn()
        current = get_config_bool(conn, "issue_by_departments", False)
        set_config(conn, "issue_by_departments", "0" if current else "1")
        text, keyboard = _build_issue_by_departments_menu(conn)
        conn.close()
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer("?????????")
        return

    if data.startswith("adm:issue_by_departments:topic:"):
        chat_id = int(parts[3])
        thread_id = int(parts[4])
        conn = get_conn()
        tariffs = conn.execute(
            "SELECT id, name, price, duration_min FROM tariffs ORDER BY id"
        ).fetchall()
        conn.close()
        if not tariffs:
            await query.answer("??? ???????", show_alert=True)
            return
        keyboard = []
        for t in tariffs:
            label = f"{t['name']} | {t['duration_min']} ??? | ${t['price']}"
            keyboard.append(
                [InlineKeyboardButton(label, callback_data=f"adm:issue_by_departments:set:{chat_id}:{thread_id}:{t['id']}")]
            )
        keyboard.append([InlineKeyboardButton("? ?????", callback_data="adm:issue_by_departments")])
        await query.edit_message_text("???????? ????? ??? ???? ????????:", reply_markup=InlineKeyboardMarkup(keyboard))
        await query.answer()
        return

    if data.startswith("adm:issue_by_departments:set:"):
        chat_id = int(parts[3])
        thread_id = int(parts[4])
        tariff_id = int(parts[5])
        conn = get_conn()
        rec = conn.execute(
            "SELECT chat_id FROM reception_groups WHERE tariff_id = ? AND is_active = 1 ORDER BY chat_title LIMIT 1",
            (tariff_id,),
        ).fetchone()
        if not rec:
            conn.close()
            await query.answer("??? ?????? ??? ??????? (/num)", show_alert=True)
            return
        updated = conn.execute(
            "UPDATE processing_topics SET reception_chat_id = ? WHERE chat_id = ? AND thread_id = ?",
            (rec["chat_id"], chat_id, thread_id),
        ).rowcount
        conn.commit()
        text, keyboard = _build_issue_by_departments_menu(conn)
        conn.close()
        if not updated:
            await query.answer("??????? ???????? /set", show_alert=True)
            return
        await query.edit_message_text(text, reply_markup=keyboard)
        await query.answer("?????????")
        return

    if data == "adm:requests":


        conn = get_conn()
        rows = conn.execute(
            "SELECT r.id, r.user_id, u.username, r.status "
            "FROM access_requests r LEFT JOIN users u ON u.user_id = r.user_id "
            "WHERE r.status = 'pending' ORDER BY r.created_at DESC"
        ).fetchall()
        conn.close()
        if not rows:
            await query.edit_message_text(ui("empty_requests"), reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")]]
            ))
            await query.answer()
            return
        lines = ["📝 Заявки:"]
        keyboard = []
        for r in rows:
            lines.append(f"#{r['id']} | {format_user_label(r['user_id'], r['username'])}")
            keyboard.append([InlineKeyboardButton(f"✅ Одобрить #{r['id']}", callback_data=f"adm:req:approve:{r['id']}")])
        keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")])
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard))
        await query.answer()
        return

    if data.startswith("adm:req:approve:"):
        req_id = int(parts[3])
        conn = get_conn()
        row = conn.execute("SELECT user_id FROM access_requests WHERE id = ?", (req_id,)).fetchone()
        if row:
            conn.execute("UPDATE access_requests SET status = 'approved' WHERE id = ?", (req_id,))
            conn.execute("UPDATE users SET is_approved = 1 WHERE user_id = ?", (row["user_id"],))
            conn.commit()
            log_admin_action(query.from_user.id, query.from_user.username, "approve_access_request", f"request_id={req_id}|user_id={row['user_id']}")
        conn.close()
        await query.edit_message_text("Заявка одобрена.", reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅ Назад", callback_data="adm:requests")]]
        ))
        await query.answer("Готово")
        return

    if data == "adm:referral":
        conn = get_conn()
        current = get_config_bool(conn, "referral_enabled", True)
        set_config(conn, "referral_enabled", "0" if current else "1")
        keyboard = build_settings_menu(conn)
        conn.close()
        log_admin_action(
            query.from_user.id,
            query.from_user.username,
            "toggle_referral",
            f"referral_enabled={'0' if current else '1'}",
        )
        status = "включена" if not current else "выключена"
        await query.edit_message_text(f"👥 Рефералка {status}.", reply_markup=keyboard)
        await query.answer("Обновлено")
        return

    if data == "adm:support":
        conn = get_conn()
        tickets = conn.execute(
            "SELECT t.id, t.user_id, u.username "
            "FROM support_tickets t LEFT JOIN users u ON u.user_id = t.user_id "
            "WHERE t.status = 'open' ORDER BY t.created_at DESC LIMIT 10"
        ).fetchall()
        conn.close()
        if not tickets:
            await query.edit_message_text(ui("empty_support_tickets"), reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")]]
            ))
            await query.answer()
            return
        lines = ["✏ Саппорт:"]
        keyboard = []
        for t in tickets:
            lines.append(f"#{t['id']} | {format_user_label(t['user_id'], t['username'])}")
            keyboard.append([InlineKeyboardButton(f"Ответить #{t['id']}", callback_data=f"adm:support_reply:{t['id']}")])
        keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")])
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard))
        await query.answer()
        return

    if data.startswith("adm:support_reply:"):
        ticket_id = int(parts[2])
        set_state(context, "admin_support_reply", ticket_id=ticket_id)
        await query.edit_message_text("Введите ответ пользователю:")
        await query.answer()
        return

    if data == "adm:slip_all":
        conn = get_conn()
        rows = conn.execute(
            "SELECT id, user_id, phone FROM queue_numbers WHERE status = 'taken'"
        ).fetchall()
        conn.execute(
            "UPDATE queue_numbers SET status = 'slip', completed_at = ? WHERE status = 'taken'",
            (now_ts(),),
        )
        conn.commit()
        conn.close()
        for r in rows:
            try:
                await context.bot.send_message(
                    chat_id=r["user_id"],
                    text=f"❌ Ваш номер {r['phone']} слетел.",
                )
            except Exception:
                continue
        await query.edit_message_text("Все активные номера отмечены как слет.", reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")]]
        ))
        await query.answer("Готово")
        return

    if data == "adm:auto_slip":
        conn = get_conn()
        minutes = get_config_int(conn, "auto_slip_minutes", 15)
        enabled = get_config_bool(conn, "auto_slip_on")
        conn.close()
        status = "ВКЛ" if enabled else "ВЫКЛ"
        set_state(context, "admin_auto_slip")
        await query.edit_message_text(
            f"🔁 Авто-слёт\nСтатус: {status}\nТекущий интервал: {minutes} мин\n\n"
            "Введите новый интервал в минутах (0 = выключить):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")]]),
        )
        await query.answer()
        return

    if data == "adm:i_am_here":
        conn = get_conn()
        minutes = get_config_int(conn, "i_am_here_minutes", 10)
        enabled = get_config_bool(conn, "i_am_here_on")
        conn.close()
        status = "ВКЛ" if enabled else "ВЫКЛ"
        set_state(context, "admin_i_am_here")
        await query.edit_message_text(
            f"👋 Я тут\nСтатус: {status}\nТекущий интервал: {minutes} мин\n\n"
            "Введите новый интервал в минутах (0 = выключить):"
        )
        await query.answer()
        return

    if data == "adm:input_type":
        conn = get_conn()
        current = get_config_bool(conn, "use_priorities", True)
        set_config(conn, "use_priorities", "0" if current else "1")
        keyboard = build_settings_menu(conn)
        conn.close()
        mode = "FIFO (по времени)" if current else "По приоритетам тарифа"
        log_admin_action(
            query.from_user.id,
            query.from_user.username,
            "switch_issue_mode",
            "use_priorities=0 (FIFO)" if current else "use_priorities=1 (priority)",
        )
        await query.edit_message_text(f"🧩 Тип вбива: {mode}", reply_markup=keyboard)
        await query.answer("Обновлено")
        return

    if data == "adm:back_to_menu":
        await query.answer("Главное меню отправлено")
        await send_main_menu_chat(context, query.from_user.id, query.from_user.id)
        return

    if data.startswith("user:tariff:"):
        tariff_id = int(parts[2])
        conn = get_conn()
        tariff = conn.execute(
            "SELECT id, name, price, duration_min FROM tariffs WHERE id = ?",
            (tariff_id,),
        ).fetchone()
        receptions = conn.execute(
            "SELECT chat_id, chat_title FROM reception_groups WHERE is_active = 1 AND tariff_id = ? ORDER BY chat_title",
            (tariff_id,),
        ).fetchall()
        depts = conn.execute("SELECT id, name FROM departments ORDER BY id").fetchall()
        conn.close()
        if not tariff:
            await query.edit_message_text("Тариф не найден.")
            await query.answer()
            return
        if not receptions:
            await query.edit_message_text("Приемки для этого тарифа не настроены. Админ: /num в нужной приемке.")
            await query.answer()
            return
        if len(receptions) == 1:
            reception_chat_id = receptions[0]["chat_id"]
            hint = build_submit_hint(tariff["name"], tariff["duration_min"], tariff["price"])
            if not depts:
                set_state(context, "submit_numbers", tariff_id=tariff_id, department_id=None, reception_chat_id=reception_chat_id)
                await query.edit_message_text(f"{hint}\n\nОтправьте номера одним сообщением:")
                await query.answer()
                return
            keyboard = [[InlineKeyboardButton(d["name"], callback_data=f"user:dept:{tariff_id}:{d['id']}:{reception_chat_id}")] for d in depts]
            await query.edit_message_text("Выберите отдел:", reply_markup=InlineKeyboardMarkup(keyboard))
            await query.answer()
            return
        keyboard = []
        for r in receptions:
            title = r["chat_title"] or str(r["chat_id"])
            keyboard.append([InlineKeyboardButton(title, callback_data=f"user:reception:{tariff_id}:{r['chat_id']}")])
        await query.edit_message_text("Выберите приемку:", reply_markup=InlineKeyboardMarkup(keyboard))
        await query.answer()
        return

    if data.startswith("user:reception:"):
        tariff_id = int(parts[2])
        reception_chat_id = int(parts[3])
        conn = get_conn()
        tariff = conn.execute(
            "SELECT id, name, price, duration_min FROM tariffs WHERE id = ?",
            (tariff_id,),
        ).fetchone()
        depts = conn.execute("SELECT id, name FROM departments ORDER BY id").fetchall()
        conn.close()
        if not tariff:
            await query.edit_message_text("Тариф не найден.")
            await query.answer()
            return
        hint = build_submit_hint(tariff["name"], tariff["duration_min"], tariff["price"])
        if not depts:
            set_state(context, "submit_numbers", tariff_id=tariff_id, department_id=None, reception_chat_id=reception_chat_id)
            await query.edit_message_text(f"{hint}\n\nОтправьте номера одним сообщением:")
            await query.answer()
            return
        keyboard = [[InlineKeyboardButton(d["name"], callback_data=f"user:dept:{tariff_id}:{d['id']}:{reception_chat_id}")] for d in depts]
        await query.edit_message_text("Выберите отдел:", reply_markup=InlineKeyboardMarkup(keyboard))
        await query.answer()
        return

    if data.startswith("user:dept:"):
        tariff_id = int(parts[2])
        dept_id = int(parts[3])
        reception_chat_id = int(parts[4]) if len(parts) > 4 else None
        if reception_chat_id is None:
            await query.edit_message_text("Приемка не выбрана. Откройте меню и выберите тариф заново.")
            await query.answer()
            return
        conn = get_conn()
        tariff = conn.execute(
            "SELECT id, name, price, duration_min FROM tariffs WHERE id = ?",
            (tariff_id,),
        ).fetchone()
        conn.close()
        if not tariff:
            await query.edit_message_text("Тариф не найден.")
            await query.answer()
            return
        set_state(context, "submit_numbers", tariff_id=tariff_id, department_id=dept_id, reception_chat_id=reception_chat_id)
        hint = build_submit_hint(tariff["name"], tariff["duration_min"], tariff["price"])
        await query.edit_message_text(f"{hint}\n\nОтправьте номера одним сообщением:")
        await query.answer()
        return
    if data == "user:request_access":
        conn = get_conn()
        conn.execute(
            "INSERT INTO access_requests (user_id, status, created_at) VALUES (?, 'pending', ?)",
            (query.from_user.id, now_ts()),
        )
        conn.commit()
        conn.close()
        await query.edit_message_text("Заявка отправлена администратору.")
        await query.answer("Готово")
        return

    if data == "user:withdraw":
        set_state(context, "user_withdraw")
        await query.edit_message_text(
            "Введите сумму для вывода:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data="user:home")]]),
        )
        await query.answer()
        return

    if data == "user:home":
        clear_state(context)
        await query.answer("Главное меню")
        await send_main_menu_chat(context, query.from_user.id, query.from_user.id, message=query.message)
        return

    if data.startswith("issue:"):
        dept_id = int(parts[1])
        reception_chat_id = int(parts[2])
        conn = get_conn()
        row = fetch_next_queue(conn, [dept_id], reception_chat_id)
        if not row:
            conn.close()
            await query.edit_message_text("Очередь пуста.")
            await query.answer()
            return
        conn.execute(
            "UPDATE queue_numbers SET status = 'taken', assigned_at = ?, worker_id = ? WHERE id = ?",
            (now_ts(), query.from_user.id, row["id"]),
        )
        conn.commit()
        conn.close()
        await send_number_to_worker(update, context, row)
        await query.answer()
        return

    if data.startswith("q:msg:"):
        queue_id = int(parts[2])
        chat_id = query.message.chat_id if query.message else None
        thread_id = query.message.message_thread_id if query.message else None
        if chat_id:
            try:
                prompt = await context.bot.send_message(
                    chat_id=chat_id,
                    message_thread_id=thread_id if thread_id and thread_id > 0 else None,
                    text="Ответьте на это сообщение, чтобы отправить владельцу (текст или фото):",
                    reply_markup=ForceReply(selective=True),
                )
                set_state(
                    context,
                    "worker_message_user",
                    queue_id=queue_id,
                    chat_id=chat_id,
                    thread_id=thread_id or 0,
                    prompt_msg_id=prompt.message_id,
                )
            except Exception:
                set_state(context, "worker_message_user", queue_id=queue_id, chat_id=chat_id, thread_id=thread_id or 0)
        else:
            set_state(context, "worker_message_user", queue_id=queue_id, chat_id=None)
        await query.answer("Введите сообщение")
        return

    if data.startswith("q:skip:"):
        queue_id = int(parts[2])
        conn = get_conn()
        row = conn.execute("SELECT user_id, phone, status FROM queue_numbers WHERE id = ?", (queue_id,)).fetchone()
        if not row:
            conn.close()
            await query.answer("Не найдено", show_alert=True)
            return
        if row["status"] not in ("taken", "queued"):
            conn.close()
            await query.answer("Уже закрыто", show_alert=True)
            return
        conn.execute(
            "UPDATE queue_numbers SET status = 'canceled', completed_at = ? WHERE id = ?",
            (now_ts(), queue_id),
        )
        conn.commit()
        conn.close()
        try:
            if query.message:
                status_line = f"⏭ пропущен ({format_msk()})"
                if query.message.photo:
                    caption = query.message.caption or ""
                    await query.message.edit_caption(
                        caption=merge_status_text(caption, status_line),
                        reply_markup=None,
                    )
                else:
                    txt = query.message.text or ""
                    await query.message.edit_text(
                        text=merge_status_text(txt, status_line),
                        reply_markup=None,
                    )
        except Exception:
            pass
        await query.answer("Пропущено")
        return

    if data.startswith("q:status:"):
        status = parts[2]
        queue_id = int(parts[3])
        conn = get_conn()
        row = conn.execute("SELECT * FROM queue_numbers WHERE id = ?", (queue_id,)).fetchone()
        if not row:
            conn.close()
            await query.answer("Не найдено", show_alert=True)
            return

        if status == "slip":
            allowed = row["status"] in ("taken", "queued", "success")
        else:
            allowed = row["status"] in ("taken", "queued")
        if not allowed:
            conn.close()
            await query.answer("Уже закрыто", show_alert=True)
            return

        conn.execute(
            "UPDATE queue_numbers SET status = ?, completed_at = ? WHERE id = ?",
            (status, now_ts(), queue_id),
        )
        conn.commit()
        notify_key = {
            "success": "notify_success",
            "slip": "notify_slip",
            "error": "notify_error",
        }.get(status, "")
        notify_on = get_config_bool(conn, notify_key) if notify_key else False
        conn.close()

        status_text = {
            "success": "✅ встал",
            "slip": "❌ слетел",
            "error": "⚠ ошибка",
        }.get(status, status)
        status_line = f"{status_text} ({format_msk()})"
        phone_display = format_phone(row["phone"])

        if notify_on:
            try:
                await context.bot.send_message(
                    chat_id=row["user_id"],
                    text=f"Ваш номер {phone_display} {status_line}.",
                )
            except Exception:
                pass

        try:
            if query.message:
                keep_success = status == "slip"
                if status == "success":
                    keyboard = InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("⚠ Слетел", callback_data=f"q:status:slip:{row['id']}")],
                            [InlineKeyboardButton("✉ Сообщение владельцу", callback_data=f"q:msg:{row['id']}")],
                        ]
                    )
                else:
                    keyboard = None
                if query.message.photo:
                    caption = query.message.caption or ""
                    await query.message.edit_caption(
                        caption=merge_status_text(caption, status_line, keep_success=keep_success),
                        reply_markup=keyboard,
                    )
                else:
                    txt = query.message.text or ""
                    await query.message.edit_text(
                        text=merge_status_text(txt, status_line, keep_success=keep_success),
                        reply_markup=keyboard,
                    )
        except Exception:
            pass
        if status in ("slip", "error") and query.message:
            try:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    message_thread_id=query.message.message_thread_id,
                    text="Нажмите «Взять номер», чтобы получить следующий:",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("📥 Взять номер", callback_data="topic:next")]]
                    ),
                )
            except Exception:
                pass
        await query.answer("Статус обновлен")
        return

        if row["status"] not in ("taken", "queued"):
            conn.close()
            await query.answer("Уже закрыто", show_alert=True)
            return
        conn.execute(
            "UPDATE queue_numbers SET status = ?, completed_at = ? WHERE id = ?",
            (status, now_ts(), queue_id),
        )
        conn.commit()
        notify_key = {
            "success": "notify_success",
            "slip": "notify_slip",
            "error": "notify_error",
        }.get(status, "")
        notify_on = get_config_bool(conn, notify_key) if notify_key else False
        conn.close()

        status_text = {
            "success": "✅ встал",
            "slip": "❌ слетел",
            "error": "⚠ ошибка",
        }.get(status, status)

        if notify_on:
            try:
                await context.bot.send_message(
                    chat_id=row["user_id"],
                    text=f"Ваш номер {row['phone']} {status_text}.",
                )
            except Exception:
                pass
        try:
            if query.message:
                if query.message.photo:
                    caption = query.message.caption or ""
                    await query.message.edit_caption(
                        caption=f"{caption}\nСтатус: {status_text}".strip(),
                        reply_markup=None,
                    )
                else:
                    txt = query.message.text or ""
                    await query.message.edit_text(
                        text=f"{txt}\nСтатус: {status_text}".strip(),
                        reply_markup=None,
                    )
        except Exception:
            pass
        await query.answer("Статус обновлен")
        return

    if data.startswith("q:repeat:"):
        queue_id = int(parts[2])
        conn = get_conn()
        row = conn.execute("SELECT photo_file_id FROM queue_numbers WHERE id = ?", (queue_id,)).fetchone()
        conn.close()
        if not row or not row["photo_file_id"]:
            await query.answer("Фото нет", show_alert=True)
            return
        try:
            await context.bot.send_photo(
                chat_id=query.message.chat_id,
                message_thread_id=query.message.message_thread_id,
                photo=row["photo_file_id"],
                caption="Повтор кода",
            )
        except Exception:
            pass
        await query.answer("Отправлено")
        return

    if data.startswith("q:qr:"):
        queue_id = int(parts[2])
        conn = get_conn()
        row = conn.execute("SELECT user_id, phone FROM queue_numbers WHERE id = ?", (queue_id,)).fetchone()
        conn.execute("UPDATE queue_numbers SET qr_requested = 1 WHERE id = ?", (queue_id,))
        conn.commit()
        conn.close()
        if row:
            try:
                await context.bot.send_message(
                    chat_id=row["user_id"],
                    text=f"Отправьте QR/код для номера {row['phone']}.",
                )
            except Exception:
                pass
        await query.answer("Запрос отправлен")
        return

    if data == "topic:next" or data.startswith("office:next:"):
        if not query.message:
            await query.answer("??? ??????", show_alert=True)
            return
        conn = get_conn()
        thread_id = query.message.message_thread_id or 0
        topic = conn.execute(
            "SELECT reception_chat_id FROM processing_topics WHERE chat_id = ? AND thread_id = ?",
            (query.message.chat_id, thread_id),
        ).fetchone()
        if is_lunch_time(conn):
            conn.close()
            await query.answer("?????? ????", show_alert=True)
            return
        issue_by = get_config_bool(conn, "issue_by_departments", False)
        reception_chat_id = None
        if issue_by:
            if not topic or not topic["reception_chat_id"]:
                conn.close()
                await query.answer("???????? ?? ?????????. ???????? /set", show_alert=True)
                return
            reception_chat_id = topic["reception_chat_id"]
        else:
            if not topic:
                conn.close()
                await query.answer("???? ?? ?????????. ???????? /set", show_alert=True)
                return

        row = fetch_next_queue(conn, [], reception_chat_id)
        if not row:
            conn.close()
            await query.answer("??????? ?????", show_alert=True)
            return
        conn.execute(
            "UPDATE queue_numbers SET status = 'taken', assigned_at = ?, worker_id = ? WHERE id = ?",
            (now_ts(), query.from_user.id, row["id"]),
        )
        conn.commit()
        conn.close()
        await send_number_to_worker(update, context, row)
        await query.answer("??????")
        return
