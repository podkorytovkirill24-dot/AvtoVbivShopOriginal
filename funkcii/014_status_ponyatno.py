def status_human(status: str) -> str:
    mapping = {
        "queued": "в ожидании",
        "taken": "в работе",
        "success": "встал",
        "slip": "слет",
        "error": "ошибка",
        "canceled": "отменен",
        "pending": "ожидает",
        "paid": "оплачен",
    }
    return mapping.get(status, status)
