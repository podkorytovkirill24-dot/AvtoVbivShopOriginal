def main() -> None:
    init_db()
    start_miniapp_server()
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("admin", cmd_admin))
    application.add_handler(CommandHandler("app", cmd_app))
    application.add_handler(CommandHandler("set", cmd_set))
    application.add_handler(CommandHandler("num", cmd_num))

    application.add_handler(CallbackQueryHandler(handle_callback))

    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & (filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_private_state)
    )
    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_private_menu)
    )
    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.PHOTO, handle_photo_qr)
    )
    application.add_handler(
        MessageHandler(filters.ChatType.GROUPS & filters.REPLY & (filters.TEXT | filters.PHOTO), handle_worker_code_reply)
    )
    application.add_handler(
        MessageHandler(filters.ChatType.GROUPS & (filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_group_worker_state, block=False)
    )
    application.add_handler(
        MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, handle_group_request_number, block=False)
    )
    application.add_handler(
        MessageHandler(filters.ChatType.GROUPS & (filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_group_submission)
    )

    if application.job_queue is not None:
        application.job_queue.run_repeating(job_tick, interval=60, first=10)
    else:
        logger.warning(
            "JobQueue ne dostupen. Ustanovite python-telegram-bot[job-queue], "
            "chtoby vklyuchit fonovye avto-zadachi."
        )

    logger.info("Bot started")
    application.run_polling()
