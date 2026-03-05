def strip_status_lines(text: str) -> str:
    lines = [ln for ln in (text or "").splitlines() if not ln.strip().startswith("Статус:")]
    return "\n".join(lines).strip()
