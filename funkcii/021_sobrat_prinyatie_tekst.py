def build_accept_text(accepted: List[str], pending_before: int) -> str:
    lines = [
        "✅ Номера приняты",
        f"Принято: {len(accepted)}",
        f"Очередь сейчас: {pending_before + len(accepted)}",
        "",
        "Ваши позиции:",
    ]
    for i, phone in enumerate(accepted, start=1):
        lines.append(f"{i}. {format_phone(phone)} — #{pending_before + i}")
    return "\n".join(lines)
