def format_duration(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours} ч {minutes} мин"
