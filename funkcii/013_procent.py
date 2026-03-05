def pct(part: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(part * 100.0 / total):.1f}%"
