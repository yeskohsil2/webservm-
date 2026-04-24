import logging
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from database import *

from .common import safe_answer, format_amount

# Константы
BANK_INTEREST_RATES = {
    1: 3,
    3: 7,
    5: 11,
    12: 12,
    30: 21
}

BANK_PRESET_AMOUNTS = [500000, 1000000, 5000000, 10000000, 50000000]
BANK_MAX_AMOUNT = 50000000
BANK_PENALTY_PERCENT = 20

bank_creation_data = {}

async def handle_bank_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для банка"""
    query = update.callback_query
    
    if data == "bank_create":
        await bank_create_callback(update, context)
    elif data == "bank_list":
        await bank_list_callback(update, context)
    elif data == "bank_convert":
        await bank_convert_callback(update, context)
    elif data == "bank_back_to_menu":
        await bank_back_to_menu_callback(update, context)
    elif data.startswith("bank_days_"):
        await bank_days_callback(update, context)
    elif data.startswith("bank_amount_"):
        if data == "bank_amount_custom":
            await bank_amount_custom_callback(update, context)
        else:
            await bank_amount_callback(update, context)
    elif data.startswith("bank_view_"):
        await bank_view_callback(update, context)
    elif data.startswith("bank_withdraw_"):
        await bank_withdraw_callback(update, context)
    elif data.startswith("bank_confirm_withdraw_"):
        await bank_confirm_withdraw_callback(update, context)
    elif data.startswith("bank_final_confirm_"):
        await bank_final_confirm_callback(update, context)

async def bank_create_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало создания депозита - выбор дней"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    bank_creation_data[user_id] = {'step': 'days'}
    
    text = (
        f"ℹ️ Для создания депозита выберите количество дней для депозита.\n\n"
        f"Выбери:"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("1 день (3%)", callback_data="bank_days_1"),
            InlineKeyboardButton("3 дня (7%)", callback_data="bank_days_3"),
            InlineKeyboardButton("5 дней (11%)", callback_data="bank_days_5")
        ],
        [
            InlineKeyboardButton("12 дней (12%)", callback_data="bank_days_12"),
            InlineKeyboardButton("30 дней (21%)", callback_data="bank_days_30")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="bank_back_to_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def bank_days_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор дней депозита"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    days = int(query.data.replace('bank_days_', ''))
    
    if user_id not in bank_creation_data:
        bank_creation_data[user_id] = {}
    
    bank_creation_data[user_id]['days'] = days
    bank_creation_data[user_id]['rate'] = BANK_INTEREST_RATES[days]
    bank_creation_data[user_id]['step'] = 'amount'
    
    text = (
        f"🏦 Теперь выберите сумму депозита:\n\n"
        f"📅 Вы выбрали {days} дней ({BANK_INTEREST_RATES[days]}%)\n"
        f"⚠ Максимальная сумма депозита – {format_amount(BANK_MAX_AMOUNT)}ms¢.\n\n"
        f"Выберите:"
    )
    
    keyboard = []
    row = []
    for i, amount in enumerate(BANK_PRESET_AMOUNTS):
        row.append(InlineKeyboardButton(format_amount(amount), callback_data=f"bank_amount_{amount}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("💳 Другое", callback_data="bank_amount_custom")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="bank_create")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def bank_amount_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор суммы депозита"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    amount = int(query.data.replace('bank_amount_', ''))
    
    if user_id not in bank_creation_data or bank_creation_data[user_id].get('step') != 'amount':
        await safe_answer(query, "❌ Сессия создания депозита устарела", show_alert=True)
        return
    
    bank_creation_data[user_id]['amount'] = amount
    await bank_confirm_deposit(query, context, user_id)

async def bank_amount_custom_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод своей суммы депозита"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if user_id not in bank_creation_data or bank_creation_data[user_id].get('step') != 'amount':
        await safe_answer(query, "❌ Сессия создания депозита устарела", show_alert=True)
        return
    
    bank_creation_data[user_id]['step'] = 'custom_amount'
    
    await query.edit_message_text(
        f"⚠ Введите сумму:\n"
        f"Можно использовать к, кк\n"
        f"Максимум: {format_amount(BANK_MAX_AMOUNT)}ms¢"
    )
    await safe_answer(query, "")

async def bank_confirm_deposit(query, context, user_id):
    """Подтверждение создания депозита"""
    data = bank_creation_data[user_id]
    amount = data['amount']
    days = data['days']
    rate = data['rate']
    
    user = await get_user_async(user_id)
    if user['balance'] < amount:
        await query.edit_message_text(f"❌ Недостаточно средств. Баланс: {format_amount(user['balance'])}ms¢")
        del bank_creation_data[user_id]
        return
    
    success, deposit_id = await create_deposit_async(user_id, amount, days, rate)
    
    if not success:
        await query.edit_message_text(f"❌ {deposit_id}")
        del bank_creation_data[user_id]
        return
    
    await update_balance_async(user_id, -amount)
    
    text = (
        f"🏦 Вы хотите создать депозит 🆔 {deposit_id}.\n\n"
        f"💸 Сумма – {format_amount(amount)}ms¢.\n"
        f"Процентная ставка: {rate}%\n\n"
        f"Подтвердите создание:"
    )
    
    keyboard = [[
        InlineKeyboardButton("✅ Создать", callback_data=f"bank_final_confirm_{deposit_id}")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup)
    bank_creation_data[user_id]['step'] = 'final'

async def bank_final_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Финальное подтверждение создания депозита"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    deposit_id = int(query.data.replace('bank_final_confirm_', ''))
    
    deposit = await get_deposit_async(deposit_id)
    
    if not deposit:
        await safe_answer(query, "❌ Депозит не найден", show_alert=True)
        return
    
    if deposit['user_id'] != user_id:
        await safe_answer(query, "❌ Это не ваш депозит", show_alert=True)
        return
    
    if deposit['status'] != 'active':
        await safe_answer(query, "❌ Депозит уже неактивен", show_alert=True)
        return
    
    await query.edit_message_text(
        f"✅ Вы успешно создали депозит 🆔 {deposit_id}!\n\n"
        f"💸 Сумма: {format_amount(deposit['amount'])}ms¢\n"
        f"📅 Дней: {deposit['days']}\n"
        f"📊 Процент: {deposit['interest_rate']}%"
    )
    
    if user_id in bank_creation_data:
        del bank_creation_data[user_id]
    
    await safe_answer(query, "")

async def bank_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список депозитов пользователя"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    deposits = await get_user_deposits_async(user_id, 'active')
    
    if not deposits:
        text = f"🏧 *{user.full_name}*, у вас пока нет депозитов!"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="bank_back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        await safe_answer(query, "")
        return
    
    text = f"🏧 *{user.full_name}*, список ваших депозитов:\n\n"
    
    now = datetime.now()
    
    for dep in deposits:
        try:
            expires = datetime.strptime(dep['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            try:
                expires = datetime.strptime(dep['expires_at'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        
        delta = expires - now
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        seconds = delta.seconds % 60
        
        text += (
            f"🆔 {dep['deposit_id']} — {format_amount(dep['amount'])}ms¢ | {dep['interest_rate']}% — "
            f"депозит снимется через {days} дн. {hours} ч. {minutes} мин. {seconds} сек.\n\n"
        )
    
    keyboard = []
    row = []
    for i, dep in enumerate(deposits):
        row.append(InlineKeyboardButton(f"🆔 {dep['deposit_id']}", callback_data=f"bank_view_{dep['deposit_id']}"))
        if len(row) == 3 or i == len(deposits) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="bank_back_to_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def bank_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр конкретного депозита"""
    query = update.callback_query
    user = query.from_user
    deposit_id = int(query.data.replace('bank_view_', ''))
    
    deposit = await get_deposit_async(deposit_id)
    if not deposit or deposit['user_id'] != user.id:
        await safe_answer(query, "❌ Депозит не найден", show_alert=True)
        return
    
    try:
        created = datetime.strptime(deposit['created_at'], '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        try:
            created = datetime.strptime(deposit['created_at'], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            created = datetime.now()
    
    try:
        expires = datetime.strptime(deposit['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        try:
            expires = datetime.strptime(deposit['expires_at'], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            expires = datetime.now()
    
    now = datetime.now()
    delta = expires - now
    days = delta.days
    hours = delta.seconds // 3600
    
    final_amount = deposit['amount'] + (deposit['amount'] * deposit['interest_rate'] // 100)
    
    text = (
        f"Депозит 🆔 {deposit['deposit_id']}:\n\n"
        f"Депозит был создан: {created.strftime('%d.%m.%Y %H:%M')}\n\n"
        f"💸 Сумма депозита – {format_amount(deposit['amount'])}ms¢\n"
        f"После истечения {days} дн. {hours} ч. вы получите {format_amount(final_amount)}ms¢.\n\n"
        f"Хотите снять депозит прямо сейчас?"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("Снять 💸", callback_data=f"bank_withdraw_{deposit_id}"),
            InlineKeyboardButton("Назад 🔙", callback_data="bank_list")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def bank_withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение снятия депозита"""
    query = update.callback_query
    user = query.from_user
    deposit_id = int(query.data.replace('bank_withdraw_', ''))
    
    deposit = await get_deposit_async(deposit_id)
    if not deposit or deposit['user_id'] != user.id:
        await safe_answer(query, "❌ Депозит не найден", show_alert=True)
        return
    
    penalty_amount = deposit['amount'] * BANK_PENALTY_PERCENT // 100
    return_amount = deposit['amount'] - penalty_amount
    
    text = (
        f"Вы точно хотите снять депозит 🆔 {deposit_id}?\n\n"
        f"Вы получите {format_amount(return_amount)}ms¢"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("Подтверждаю ✅", callback_data=f"bank_confirm_withdraw_{deposit_id}"),
            InlineKeyboardButton("Назад 🔙", callback_data=f"bank_view_{deposit_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def bank_confirm_withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение снятия депозита"""
    query = update.callback_query
    user = query.from_user
    deposit_id = int(query.data.replace('bank_confirm_withdraw_', ''))
    
    deposit = await get_deposit_async(deposit_id)
    if not deposit or deposit['user_id'] != user.id:
        await safe_answer(query, "❌ Депозит не найден", show_alert=True)
        return
    
    success, return_amount = await close_deposit_async(deposit_id, BANK_PENALTY_PERCENT)
    
    if success:
        await update_balance_async(user.id, return_amount)
        
        await query.edit_message_text(
            f"✅ Вы успешно сняли депозит 🆔 {deposit_id}.\n"
            f"💸 Получено: {format_amount(return_amount)}ms¢"
        )
    else:
        await query.edit_message_text("❌ Ошибка при снятии депозита")
    
    await safe_answer(query, "")

async def bank_convert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Конвертация валюты (недоступно)"""
    query = update.callback_query
    await safe_answer(query, "👨‍💻 Конвертация на доработках.", show_alert=True)

async def bank_back_to_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню банка"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if user_id in bank_creation_data:
        del bank_creation_data[user_id]
    
    text = (
        f"🏦 *{user.full_name}*, добро пожаловать в \"Monst Bank\"\n\n"
        f"Здесь ты можешь создать депозит и обменять валюту (временно недоступно)\n\n"
        f"Выбери действие:"
    )
    
    keyboard = [
        [InlineKeyboardButton("➕ Создать депозит", callback_data="bank_create")],
        [InlineKeyboardButton("🏧 Список депозитов", callback_data="bank_list")],
        [InlineKeyboardButton("⏳ Конвертация", callback_data="bank_convert")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")
