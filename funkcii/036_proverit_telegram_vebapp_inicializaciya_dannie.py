def verify_telegram_webapp_init_data(init_data: str) -> Optional[Dict]:
    if not init_data:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None
    hash_value = pairs.pop("hash", "")
    if not hash_value:
        return None
    data_check_string = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs.keys()))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc_hash, hash_value):
        return None
    try:
        auth_date = int(pairs.get("auth_date", "0"))
    except ValueError:
        auth_date = 0
    if auth_date and now_ts() - auth_date > 86400:
        return None
    try:
        tg_user = json.loads(pairs.get("user", "{}"))
    except Exception:
        return None
    if not isinstance(tg_user, dict) or not tg_user.get("id"):
        return None
    return tg_user
