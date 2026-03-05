def build_admin_panel() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("⚙ Настройки", callback_data="adm:settings"),
            InlineKeyboardButton("🛡 Админы", callback_data="adm:admins"),
        ],
        [InlineKeyboardButton("🎨 Главное меню", callback_data="adm:mainmenu")],
        [
            InlineKeyboardButton("📊 Статистика", callback_data="adm:stats:today"),
            InlineKeyboardButton("📈 Отчёты", callback_data="adm:reports"),
        ],
        [InlineKeyboardButton("🏆 Топы", callback_data="adm:tops:submitted:all")],
        [
            InlineKeyboardButton("👥 Пользователи", callback_data="adm:users"),
            InlineKeyboardButton("🧹 Очередь", callback_data="adm:queue"),
        ],
        [InlineKeyboardButton("🔍 Поиск по номеру", callback_data="adm:search")],
        [
            InlineKeyboardButton("💰 Выводы", callback_data="adm:withdrawals"),
            InlineKeyboardButton("💸 Выплаты", callback_data="adm:payouts"),
        ],
        [InlineKeyboardButton("📣 Рассылка", callback_data="adm:broadcast")],
        [InlineKeyboardButton("🧰 Сервис", callback_data="adm:service")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="adm:back_to_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)
