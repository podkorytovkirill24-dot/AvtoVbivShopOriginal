def merge_status_text(existing: str, status_line: str, keep_success: bool = False) -> str:
    lines = (existing or "").splitlines()
    if keep_success:
        cleaned: List[str] = []
        for ln in lines:
            if ln.strip().startswith("Статус:"):
                if "✅" in ln or "встал" in ln:
                    cleaned.append(ln)
                continue
            cleaned.append(ln)
        lines = cleaned
    else:
        lines = [ln for ln in lines if not ln.strip().startswith("Статус:")]
    lines.append(f"Статус: {status_line}")
    return "\n".join([ln for ln in lines if ln]).strip()
