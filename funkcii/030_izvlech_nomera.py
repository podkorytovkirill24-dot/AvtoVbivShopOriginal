def extract_numbers(text: str) -> List[str]:
    if not text:
        return []
    raw = PHONE_RE.findall(text)
    unique = []
    seen = set()
    for item in raw:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique
