def ui(key: str, **kwargs) -> str:
    text = UI_TEXTS.get(key, key)
    if kwargs:
        try:
            return text.format(**kwargs)
        except Exception:
            return text
    return text
