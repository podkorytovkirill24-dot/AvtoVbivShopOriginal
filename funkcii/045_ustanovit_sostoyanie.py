def set_state(context: ContextTypes.DEFAULT_TYPE, name: str, **data) -> None:
    context.user_data["state"] = {"name": name, "data": data}
