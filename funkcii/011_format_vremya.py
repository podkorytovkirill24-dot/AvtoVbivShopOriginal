_KZ_TZ = None


def get_kz_tz():
    global _KZ_TZ
    if _KZ_TZ is not None:
        return _KZ_TZ
    tz_name = os.getenv("BOT_TZ", "Asia/Qyzylorda")
    try:
        from zoneinfo import ZoneInfo

        _KZ_TZ = ZoneInfo(tz_name)
        return _KZ_TZ
    except Exception:
        pass
    try:
        offset_hours = int(os.getenv("BOT_TZ_OFFSET", "6"))
    except Exception:
        offset_hours = 6
    _KZ_TZ = timezone(timedelta(hours=offset_hours))
    return _KZ_TZ


def format_ts(ts: Optional[int]) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts, get_kz_tz()).strftime("%d.%m.%Y %H:%M")
