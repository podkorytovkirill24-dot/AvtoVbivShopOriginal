def build_main_menu_inline(conn: sqlite3.Connection, is_admin_user: bool) -> InlineKeyboardMarkup:
    submit = get_config(conn, "menu_btn_submit", DEFAULT_CONFIG["menu_btn_submit"])
    queue_btn = get_config(conn, "menu_btn_queue", DEFAULT_CONFIG["menu_btn_queue"])
    archive = get_config(conn, "menu_btn_archive", DEFAULT_CONFIG["menu_btn_archive"])
    profile = get_config(conn, "menu_btn_profile", DEFAULT_CONFIG["menu_btn_profile"])
    support = get_config(conn, "menu_btn_support", DEFAULT_CONFIG["menu_btn_support"])
    admin = get_config(conn, "menu_btn_admin", DEFAULT_CONFIG["menu_btn_admin"])
    rows = [
        [
            InlineKeyboardButton(submit, callback_data="menu:submit"),
            InlineKeyboardButton(queue_btn, callback_data="menu:queue"),
        ],
        [
            InlineKeyboardButton(archive, callback_data="menu:archive"),
            InlineKeyboardButton(profile, callback_data="menu:profile"),
        ],
        [InlineKeyboardButton(support, callback_data="menu:support")],
    ]
    if MINI_APP_BASE_URL:
        rows.append([InlineKeyboardButton("✨ Мини-приложение", web_app=WebAppInfo(url=f"{MINI_APP_BASE_URL}/miniapp"))])
    if is_admin_user:
        rows.append([InlineKeyboardButton(admin, callback_data="menu:admin")])
    return InlineKeyboardMarkup(rows)
