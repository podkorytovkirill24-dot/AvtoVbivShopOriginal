def build_settings_menu(conn: sqlite3.Connection) -> InlineKeyboardMarkup:
    stop = "⛔ Stop-Work" + (" ✅" if get_config_bool(conn, "stop_work") else " ❌")
    block_pm = "🔒 Блок лички" + (" ✅" if get_config_bool(conn, "block_pm") else " ❌")
    repeat = "🔁 Повтор кода" + (" ✅" if get_config_bool(conn, "repeat_code") else " ❌")
    qr = "📱 Запрос QR (WA)" + (" ✅" if get_config_bool(conn, "qr_request") else " ❌")
    auto_slip = "🔁 Авто-слёт" + (" ✅" if get_config_bool(conn, "auto_slip_on") else " ❌")
    auto_success = "⬆ Авто-встал (5м)" + (" ✅" if get_config_bool(conn, "auto_success_on") else " ❌")
    repeat_submit = "🔁 Повторная сдача" + (" ✅" if get_config_bool(conn, "allow_repeat") else " ❌")
    detail_archive = "📊 Детальные отчёты в архив" + (" ✅" if get_config_bool(conn, "detail_archive") else " ❌")
    issue_dept = "🗂 Выдача по отделам" + (" ✅" if get_config_bool(conn, "issue_by_departments") else " ❌")
    lunch = "🍽 Расписания обедов" + (" ✅" if get_config_bool(conn, "lunch_on") else " ❌")
    input_type = "🧩 Тип вбива: приоритеты" if get_config_bool(conn, "use_priorities", True) else "🧩 Тип вбива: FIFO"
    referral = "👥 Рефералка" + (" ✅" if get_config_bool(conn, "referral_enabled", True) else " ❌")
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(input_type, callback_data="adm:input_type"),
                InlineKeyboardButton(stop, callback_data="adm:toggle:stop_work"),
            ],
            [
                InlineKeyboardButton("💲 Тарифы", callback_data="adm:tariffs"),
                InlineKeyboardButton("⚡ Приоритеты", callback_data="adm:priorities"),
            ],
            [
                InlineKeyboardButton("🔗 ОП (подписка)", callback_data="adm:subscription"),
                InlineKeyboardButton(block_pm, callback_data="adm:toggle:block_pm"),
            ],
            [
                InlineKeyboardButton(repeat, callback_data="adm:toggle:repeat_code"),
                InlineKeyboardButton(qr, callback_data="adm:toggle:qr_request"),
            ],
            [
                InlineKeyboardButton(auto_slip, callback_data="adm:auto_slip"),
                InlineKeyboardButton(auto_success, callback_data="adm:auto_success"),
            ],
            [
                InlineKeyboardButton(repeat_submit, callback_data="adm:toggle:allow_repeat"),
                InlineKeyboardButton(detail_archive, callback_data="adm:toggle:detail_archive"),
            ],
            [InlineKeyboardButton("👋 Я тут", callback_data="adm:i_am_here")],
            [
                InlineKeyboardButton("🔢 Лимит сдачи", callback_data="adm:limit"),
                InlineKeyboardButton("⏱ Актуальность", callback_data="adm:actuality"),
            ],
            [
                InlineKeyboardButton("📥 Приемки", callback_data="adm:departments"),
                InlineKeyboardButton("🏢 Офисы", callback_data="adm:offices"),
            ],
            [
                InlineKeyboardButton(lunch, callback_data="adm:lunch"),
                InlineKeyboardButton("📝 Заявки", callback_data="adm:requests"),
            ],
            [InlineKeyboardButton(issue_dept, callback_data="adm:toggle:issue_by_departments")],
            [
                InlineKeyboardButton(referral, callback_data="adm:referral"),
                InlineKeyboardButton("✏ Саппорт", callback_data="adm:support"),
            ],
            [InlineKeyboardButton("🔔 Уведомления", callback_data="adm:notifications")],
            [InlineKeyboardButton("⬇ Слёт всем", callback_data="adm:slip_all")],
            [InlineKeyboardButton("⬅ Назад", callback_data="adm:panel")],
        ]
    )
