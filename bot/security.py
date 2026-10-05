from aiogram.types import CallbackQuery
def is_admin(user_id, admin_ids): return user_id in admin_ids
async def reject_if_not_admin(event, admin_ids):
    u=event.from_user
    if u is None or not is_admin(u.id,admin_ids):
        if isinstance(event,CallbackQuery): await event.answer('Not authorized.',show_alert=True)
        else: await event.answer('This bot is private.')
        return True
    return False
