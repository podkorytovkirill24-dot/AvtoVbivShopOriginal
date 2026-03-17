def format_msk(ts: Optional[int] = None) -> str:
    if not ts:
        ts = now_ts()
    tz = get_kz_tz() if "get_kz_tz" in globals() else timezone(timedelta(hours=6))
    label = os.getenv("BOT_TZ_LABEL", "КЗ")
    return datetime.fromtimestamp(ts, tz).strftime("%d.%m %H:%M ") + label
