def format_ts(ts: Optional[int]) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
