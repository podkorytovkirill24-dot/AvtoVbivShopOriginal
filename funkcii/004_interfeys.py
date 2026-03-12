def ui(key: str, **kwargs) -> str:
    text = UI_TEXTS.get(key, key)
    if kwargs:
        try:
            return text.format(**kwargs)
        except Exception:
            return text
    return text


async def send_or_update(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    message = None,
) -> None:
    if message:
        try:
            await message.edit_text(text, reply_markup=reply_markup)
            return
        except Exception:
            try:
                await message.edit_caption(caption=text, reply_markup=reply_markup)
                return
            except Exception:
                pass
        try:
            await message.delete()
        except Exception:
            pass
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
