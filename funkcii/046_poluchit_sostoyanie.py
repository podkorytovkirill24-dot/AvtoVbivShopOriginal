def get_state(context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict]:
    return context.user_data.get("state")
