def notify_user_direct(chat_id: int, text: str) -> None:
    try:
        body = json.dumps({"chat_id": chat_id, "text": text}, ensure_ascii=False).encode("utf-8")
        req = Request(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urlopen(req, timeout=5):
            pass
    except Exception:
        return
