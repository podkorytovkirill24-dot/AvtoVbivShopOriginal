def build_service_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Инфо системы", callback_data="adm:service:info")],
            [InlineKeyboardButton("📜 Лог действий", callback_data="adm:service:logs")],
            [InlineKeyboardButton("🧹 Очистить очередь", callback_data="adm:service:clear_queue")],
            [InlineKeyboardButton("🗃 Экспорт очереди", callback_data="adm:service:export_queue")],
            [InlineKeyboardButton("↩ Назад", callback_data="adm:panel")],
        ]
    )
