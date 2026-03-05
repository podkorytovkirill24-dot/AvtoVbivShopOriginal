def build_notifications_menu(conn: sqlite3.Connection) -> InlineKeyboardMarkup:
    def label(key: str, title: str) -> str:
        return f"{title} — {'✅ ВКЛ' if get_config_bool(conn, key) else '❌ ВЫКЛ'}"

    keyboard = [
        [InlineKeyboardButton(label("notify_success", "Встал"), callback_data="adm:toggle:notify_success")],
        [InlineKeyboardButton(label("notify_taken", "Взяли"), callback_data="adm:toggle:notify_taken")],
        [InlineKeyboardButton(label("notify_slip", "Слетел"), callback_data="adm:toggle:notify_slip")],
        [InlineKeyboardButton(label("notify_error", "Ошибка"), callback_data="adm:toggle:notify_error")],
        [InlineKeyboardButton("⬅ Назад", callback_data="adm:settings")],
    ]
    return InlineKeyboardMarkup(keyboard)
