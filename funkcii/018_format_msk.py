def format_msk(ts: Optional[int] = None) -> str:
    if not ts:
        ts = now_ts()
    msk = timezone(timedelta(hours=3))
    return datetime.fromtimestamp(ts, msk).strftime("%d.%m %H:%M МСК")
