def build_main_menu_settings(conn: sqlite3.Connection) -> Tuple[str, InlineKeyboardMarkup]:
    text = get_config(conn, "main_menu_text", DEFAULT_CONFIG["main_menu_text"])
    photo = get_config(conn, "main_menu_photo_id", "")
    submit = get_config(conn, "menu_btn_submit", DEFAULT_CONFIG["menu_btn_submit"])
    queue_btn = get_config(conn, "menu_btn_queue", DEFAULT_CONFIG["menu_btn_queue"])
    archive = get_config(conn, "menu_btn_archive", DEFAULT_CONFIG["menu_btn_archive"])
    profile = get_config(conn, "menu_btn_profile", DEFAULT_CONFIG["menu_btn_profile"])
    support = get_config(conn, "menu_btn_support", DEFAULT_CONFIG["menu_btn_support"])
    lines = [
        "🎨 Настройка главного меню",
        f"📝 Текст: {text}",
        f"🖼 Фото: {'Установлено' if photo else 'Нет'}",
        "",
        "Названия кнопок:",
        f"• Сдать номер: {submit}",
        f"• Очередь: {queue_btn}",
        f"• Архив: {archive}",
        f"• Профиль: {profile}",
        f"• Техподдержка: {support}",
    ]
    keyboard = [
        [InlineKeyboardButton("📝 Изменить текст", callback_data="adm:mainmenu:text")],
        [InlineKeyboardButton("🖼 Изменить фото", callback_data="adm:mainmenu:photo")],
        [
            InlineKeyboardButton("☎ Сдать номер", callback_data="adm:mainmenu:btn:submit"),
            InlineKeyboardButton("📊 Очередь", callback_data="adm:mainmenu:btn:queue"),
        ],
        [
            InlineKeyboardButton("🗂 Архив", callback_data="adm:mainmenu:btn:archive"),
            InlineKeyboardButton("👤 Профиль", callback_data="adm:mainmenu:btn:profile"),
        ],
        [InlineKeyboardButton("🛠 Техподдержка", callback_data="adm:mainmenu:btn:support")],
        [InlineKeyboardButton("♻ Сбросить всё", callback_data="adm:mainmenu:reset")],
        [InlineKeyboardButton("⬅ Назад", callback_data="adm:panel")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)
