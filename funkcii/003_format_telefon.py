def format_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) in (10, 11) and digits.startswith("7"):
        return f"+{digits}"
    return phone
