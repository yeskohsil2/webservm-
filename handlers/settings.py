import logging
import asyncio
import traceback
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from database import (
    get_user_async, get_user_settings_async, update_user_transfer_confirmation_async,
    update_user_transfer_commission_async, is_vip_user_async
)
from .common import safe_answer, format_amount

logger = logging.getLogger(__name__)

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /settings - главное меню настроек"""
    if not update.effective_user:
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    text = f"⚙️ *{user_name}*, где открыть настройки?"

    keyboard = [
        [InlineKeyboardButton("💬 В ЛС", callback_data=f"settings_private_{user_id}")],
        [InlineKeyboardButton("📢 В чате", callback_data=f"settings_chat_{user_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Отправляем новое сообщение, а не отвечаем на старое
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=text,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def settings_private_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Отправка настроек в ЛС"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
        return

    await safe_answer(query, "🔄 Открываю настройки...")

    user_name = query.from_user.full_name
    text = f"⚙️ *{user_name}*, настройки:\n\n"
    keyboard = [[InlineKeyboardButton("💸 Переводы", callback_data=f"settings_transfer_{user_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        print(f"Error sending private settings: {e}")
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="❌ Не удалось отправить настройки в ЛС. Напишите боту любое сообщение в ЛС и попробуйте снова."
        )

async def settings_chat_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Показ настроек в чате"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
        return

    user_name = query.from_user.full_name
    text = f"⚙️ *{user_name}*, настройки:\n\n"
    keyboard = [[InlineKeyboardButton("💸 Переводы", callback_data=f"settings_transfer_{user_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def settings_transfer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Настройки переводов"""
    print(f"DEBUG: settings_transfer_callback START for user {user_id}")  # ДОБАВЬ
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
        return

    try:
        print(f"DEBUG: Getting settings...")  # ДОБАВЬ
        settings = await get_user_settings_async(user_id)
        print(f"DEBUG: Settings = {settings}")  # ДОБАВЬ
        
        print(f"DEBUG: Checking VIP...")  # ДОБАВЬ
        is_vip = await is_vip_user_async(user_id)
        print(f"DEBUG: Is VIP = {is_vip}")  # ДОБАВЬ
        
        # ... остальной код ...
        settings = await get_user_settings_async(user_id)
        is_vip = await is_vip_user_async(user_id)
        
        confirmation_status = "🟢 включено" if settings.get('transfer_confirmation', 1) else "🔴 выключено"
        commission_status = "🟢 включена" if settings.get('transfer_commission', 1) else "🔴 выключена"
        
        text = (
            f"⚙️ *{query.from_user.full_name}*, настройки переводов\n\n"
            f"💸 Подтверждение переводов в ЛС — *{confirmation_status}*\n"
            f"🧾 Комиссия — *{commission_status}*\n\n"
        )
        
        if not is_vip:
            text += "🔒 *Отключение комиссии доступно только с VIP статусом*"

        keyboard = [
            [InlineKeyboardButton("🟢 Включить подтверждение в ЛС", callback_data=f"settings_transfer_confirmation_on_{user_id}")],
            [InlineKeyboardButton("🔴 Выключить подтверждение в ЛС", callback_data=f"settings_transfer_confirmation_off_{user_id}")]
        ]
        
        if is_vip:
            keyboard.append([InlineKeyboardButton("🔴 Отключить комиссию", callback_data=f"settings_transfer_commission_off_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        
    except Exception as e:
        print(f"Error in settings_transfer_callback: {e}")
        traceback.print_exc()
        await safe_answer(query, "❌ Ошибка при открытии настроек", show_alert=True)

async def settings_transfer_confirmation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, enabled: bool):
    """Включение/выключение подтверждения переводов"""
    print(f"\n=== DEBUG: settings_transfer_confirmation_callback START ===")
    print(f"DEBUG: user_id = {user_id}, enabled = {enabled}")
    print(f"DEBUG: update = {update}")
    print(f"DEBUG: context = {context}")
    
    query = update.callback_query
    print(f"DEBUG: query = {query}")
    print(f"DEBUG: query.from_user.id = {query.from_user.id}")
    
    if query.from_user.id != user_id:
        print(f"DEBUG: User mismatch! {query.from_user.id} != {user_id}")
        await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
        return

    try:
        print(f"DEBUG: Getting settings from database...")
        settings = await get_user_settings_async(user_id)
        print(f"DEBUG: Current settings = {settings}")
        
        current_value = settings.get('transfer_confirmation', 1)
        print(f"DEBUG: Current confirmation value = {current_value}")
        print(f"DEBUG: Target value = {1 if enabled else 0}")
        
        if enabled and current_value == 1:
            print(f"DEBUG: Already enabled")
            await safe_answer(query, "❌ Уже включено!", show_alert=True)
            return
        if not enabled and current_value == 0:
            print(f"DEBUG: Already disabled")
            await safe_answer(query, "❌ Уже выключено!", show_alert=True)
            return

        print(f"DEBUG: Updating confirmation to {enabled}")
        await update_user_transfer_confirmation_async(user_id, enabled)
        print(f"DEBUG: Update successful!")
        
        status = "включено" if enabled else "выключено"
        print(f"DEBUG: Sending success message: {status}")
        await safe_answer(query, f"✅ Подтверждение переводов {status}!")
        
        print(f"DEBUG: Updating settings menu...")
        # Обновляем меню настроек
        await settings_transfer_callback(update, context, user_id)
        print(f"DEBUG: Menu updated successfully!")
        
    except Exception as e:
        print(f"!!! ERROR in settings_transfer_confirmation_callback: {e} !!!")
        traceback.print_exc()
        await safe_answer(query, "❌ Произошла ошибка", show_alert=True)
    
    print(f"=== DEBUG: settings_transfer_confirmation_callback END ===\n")

async def settings_transfer_commission_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Отключение комиссии (только для VIP)"""
    print(f"\n=== DEBUG: settings_transfer_commission_callback START ===")
    print(f"DEBUG: user_id = {user_id}")
    
    query = update.callback_query
    
    if query.from_user.id != user_id:
        print(f"DEBUG: User mismatch!")
        await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
        return

    try:
        print(f"DEBUG: Checking VIP status...")
        is_vip = await is_vip_user_async(user_id)
        print(f"DEBUG: Is VIP = {is_vip}")
        
        if not is_vip:
            print(f"DEBUG: Not VIP, cannot disable commission")
            await safe_answer(query, "⚡ Для отключения комиссии вы должны быть обладателем статуса VIP.", show_alert=True)
            return

        print(f"DEBUG: Getting settings...")
        settings = await get_user_settings_async(user_id)
        print(f"DEBUG: Current commission setting = {settings.get('transfer_commission', 1)}")
        
        if settings.get('transfer_commission', 1) == 0:
            print(f"DEBUG: Commission already disabled")
            await safe_answer(query, "❌ Комиссия уже отключена!", show_alert=True)
            return

        print(f"DEBUG: Disabling commission...")
        await update_user_transfer_commission_async(user_id, False)
        print(f"DEBUG: Commission disabled successfully!")
        
        await safe_answer(query, "✅ Комиссия отключена! Теперь с вас не будет взиматься комиссия при переводах.")
        
        print(f"DEBUG: Updating settings menu...")
        await settings_transfer_callback(update, context, user_id)
        print(f"DEBUG: Menu updated!")
        
    except Exception as e:
        print(f"!!! ERROR in settings_transfer_commission_callback: {e} !!!")
        traceback.print_exc()
        await safe_answer(query, "❌ Произошла ошибка", show_alert=True)
    
    print(f"=== DEBUG: settings_transfer_commission_callback END ===\n")
