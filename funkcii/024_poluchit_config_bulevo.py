def get_config_bool(conn: sqlite3.Connection, key: str, default: bool = False) -> bool:
    value = get_config(conn, key, "1" if default else "0")
    return value == "1"
