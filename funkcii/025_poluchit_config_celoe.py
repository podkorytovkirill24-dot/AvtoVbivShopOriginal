def get_config_int(conn: sqlite3.Connection, key: str, default: int = 0) -> int:
    value = get_config(conn, key, str(default))
    try:
        return int(value)
    except ValueError:
        return default
