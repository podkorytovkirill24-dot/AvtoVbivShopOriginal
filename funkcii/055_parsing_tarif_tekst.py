def parse_tariff_text(text: str) -> Tuple[str, float, int]:
    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 2:
        return "", 0.0, 0
    name = parts[0]
    try:
        price = float(parts[1].replace(",", "."))
    except ValueError:
        price = 0.0
    duration = 0
    if len(parts) >= 3:
        try:
            duration = int(parts[2])
        except ValueError:
            duration = 0
    return name, price, duration
