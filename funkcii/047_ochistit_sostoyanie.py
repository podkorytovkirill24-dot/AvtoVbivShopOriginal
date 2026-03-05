def clear_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("state", None)
