def get_period_range(period_key: str) -> Tuple[int, int]:
    tz = get_kz_tz() if "get_kz_tz" in globals() else None
    now = datetime.now(tz) if tz else datetime.now()
    if period_key == "today":
        start = datetime(now.year, now.month, now.day, tzinfo=tz) if tz else datetime(now.year, now.month, now.day)
        end = start + timedelta(days=1)
    elif period_key == "yesterday":
        end = datetime(now.year, now.month, now.day, tzinfo=tz) if tz else datetime(now.year, now.month, now.day)
        start = end - timedelta(days=1)
    elif period_key == "7d":
        end = now
        start = now - timedelta(days=7)
    elif period_key == "30d":
        end = now
        start = now - timedelta(days=30)
    else:
        return 0, now_ts()
    return int(start.timestamp()), int(end.timestamp())
