def is_lunch_time(conn: sqlite3.Connection) -> bool:
    if not get_config_bool(conn, "lunch_on", False):
        return False
    start = get_config(conn, "lunch_start", "13:00")
    end = get_config(conn, "lunch_end", "14:00")
    try:
        start_dt = datetime.strptime(start, "%H:%M").time()
        end_dt = datetime.strptime(end, "%H:%M").time()
    except ValueError:
        return False
    now_t = datetime.now().time()
    if start_dt <= end_dt:
        return start_dt <= now_t <= end_dt
    return now_t >= start_dt or now_t <= end_dt
