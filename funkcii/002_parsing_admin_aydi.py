def _parse_admin_ids() -> set:
    ids = set()
    raw = os.getenv("ADMIN_IDS", "")
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids
