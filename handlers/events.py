import logging
import re
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from database import *

from .common import safe_answer, format_amount, parse_amount

SPRING_EVENT_START = datetime(2026, 3, 7)
SPRING_EVENT_END = datetime(2026, 6, 1)
SPRING_CHANNEL = "https://t.me/monstrbotnews"

spring_question_creation = {}

async def handle_events_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для обычных ивентов"""
    if data.startswith("event_view_"):
        await event_view_callback(update, context)
    elif data.startswith("event_type_"):
        await event_type_callback(update, context)
    elif data.startswith("event_confirm_"):
        await event_confirm_callback(update, context)
    elif data.startswith("event_close_"):
        if "event_close_confirm_" in data:
            await event_close_confirm_callback(update, context)
        else:
            await event_close_select_callback(update, context)

async def handle_spring_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для весеннего ивента"""
    if data == "spring_mysteries":
        await spring_mysteries_callback(update, context)
    elif data == "spring_questions_list":
        await spring_questions_list_callback(update, context)
    elif data == "spring_collect":
        await spring_collect_callback(update, context)
    elif data == "spring_exchange":
        await spring_exchange_callback(update, context)
    elif data == "spring_castle":
        await spring_castle_callback(update, context)
    elif data == "spring_tasks":
        await spring_tasks_callback(update, context)
    elif data == "spring_back_to_menu":
        await spring_back_to_menu_callback(update, context)
    elif data.startswith("spring_prize_"):
        await spring_prize_callback(update, context)

# ===== Events =====
async def event_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр конкретного ивента"""
    query = update.callback_query
    event_id = int(query.data.replace('event_view_', ''))
    
    event = await get_event_async(event_id)
    if not event:
        await safe_answer(query, "❌ Ивент не найден", show_alert=True)
        return
    
    if event['status'] == 'scheduled':
        text = (
            f"✨ *{event['name']}* — {event['description']}\n\n"
            f"📅 Запланировано на {event['date']}"
        )
        keyboard = [[InlineKeyboardButton("🕘 Ивент пока недоступен", callback_data="noop")]]
    elif event['status'] == 'active':
        text = f"✨ *{event['name']}* — {event['description']}"
        keyboard = []
    elif event['status'] == 'closed':
        text = f"✨ *{event['name']}*"
        keyboard = [[InlineKeyboardButton("❗ Временно закрыт", callback_data="noop")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        await query.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    await safe_answer(query, "")

async def event_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор типа ивента"""
    query = update.callback_query
    user_id = query.from_user.id
    
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ У вас нет прав", show_alert=True)
        return
    
    event_type = query.data.replace('event_type_', '')
    
    context.user_data['event_creation'] = {'type': event_type, 'step': 'date'}
    
    if event_type == 'scheduled':
        await query.edit_message_text(
            "📅 Введите запланированную дату ивента:\nПример: 24.03.2026"
        )
    else:
        await query.edit_message_text(
            "Введите название ивента:"
        )
    
    await safe_answer(query, "")

async def event_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение или отклонение создания ивента"""
    query = update.callback_query
    user_id = query.from_user.id
    action = query.data.replace('event_confirm_', '')
    
    if 'event_creation' not in context.user_data:
        await safe_answer(query, "❌ Сессия создания ивента не найдена", show_alert=True)
        return
    
    event_data = context.user_data['event_creation']
    
    if action == 'no':
        del context.user_data['event_creation']
        await query.edit_message_text("❌ Создание ивента отменено. Начните заново с /setevent")
        await safe_answer(query, "")
        return
    
    if event_data['type'] == 'scheduled':
        event_id = await create_event_async(
            name=event_data['name'],
            description=event_data['description'],
            date=event_data['date'],
            status='scheduled'
        )
    else:
        event_id = await create_event_async(
            name=event_data['name'],
            description=event_data['description'],
            status='active'
        )
    
    await query.edit_message_text("✅ Ивент успешно создан!")
    await safe_answer(query, "")
    
    del context.user_data['event_creation']

async def event_close_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор ивента для закрытия"""
    query = update.callback_query
    event_id = int(query.data.replace('event_close_', ''))
    
    event = await get_event_async(event_id)
    if not event:
        await safe_answer(query, "❌ Ивент не найден", show_alert=True)
        return
    
    context.user_data['closing_event_id'] = event_id
    
    text = (
        f"ℹ️ *{event['name']}* — {event['description']}\n\n"
        f"Вы хотите закрыть данный ивент?"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Да", callback_data="event_close_confirm_yes"),
            InlineKeyboardButton("Нет ⛔", callback_data="event_close_confirm_no")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def event_close_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение закрытия ивента"""
    query = update.callback_query
    data = query.data
    
    if data == "event_close_confirm_yes":
        action = "yes"
    elif data == "event_close_confirm_no":
        action = "no"
    else:
        await safe_answer(query, "❌ Неизвестное действие", show_alert=True)
        return
    
    if action == 'no':
        await query.edit_message_text("Успешно...")
        await asyncio.sleep(1)
        await query.delete_message()
        await safe_answer(query, "")
        return
    
    event_id = context.user_data.get('closing_event_id')
    if not event_id:
        await safe_answer(query, "❌ Ошибка: ивент не найден", show_alert=True)
        return
    
    await update_event_status_async(event_id, 'closed')
    
    await query.edit_message_text("Закрываю...")
    await asyncio.sleep(1)
    await query.delete_message()
    await safe_answer(query, "✅ Ивент закрыт")

# ===== Spring Event =====
async def spring_mysteries_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Весенние тайны'"""
    query = update.callback_query
    user = query.from_user
    
    text = (
        f"🤫 *{user.full_name}*, разгадай тайны весны 2026 года, получи огромный бонус за разгаданные тайны\n\n"
        f"Загадки будут пополняться раз в неделю, первый отгадавший загадку получит бонус\n\n"
        f"🤫 Следи за пополнение загадок и всей информацией в нашем телеграмм канале"
    )
    
    keyboard = [
        [InlineKeyboardButton("📣 Канал", url=SPRING_CHANNEL)],
        [InlineKeyboardButton("🤫 Загадки", callback_data="spring_questions_list")],
        [InlineKeyboardButton("🔙 Назад", callback_data="spring_back_to_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def spring_questions_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать список доступных загадок"""
    query = update.callback_query
    user = query.from_user
    
    questions = await get_all_spring_questions_async()
    
    if not questions:
        text = f"🙈 *{user.full_name}*, загадки разобрали! Следи за пополнением в канале"
        keyboard = [
            [InlineKeyboardButton("📣 Канал", url=SPRING_CHANNEL)],
            [InlineKeyboardButton("🔙 Назад", callback_data="spring_mysteries")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        await safe_answer(query, "")
        return
    
    text = f"🍩 *{user.full_name}*, доступные вопросы:\n\n"
    
    for q in questions:
        text += f"🆔 {q['id']} - {q['question']}\n\n"
    
    text += f"ℹ️ Для ответа используйте: /answer *айди вопроса* *ответ*\n"
    text += f"Пример: /answer 92 Яблоко"
    
    keyboard = [
        [InlineKeyboardButton("📣 Канал", url=SPRING_CHANNEL)],
        [InlineKeyboardButton("🔙 Назад", callback_data="spring_mysteries")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def spring_collect_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Сбор солнышек'"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    can_collect, minutes, seconds = await can_collect_sun_async(user_id, 5400)
    
    if not can_collect:
        await safe_answer(query, f"⏳ Вы уже собирали солнышки, подождите {minutes} мин. {seconds} сек.", show_alert=True)
        return
    
    sun_amount = random.randint(1, 99)
    
    await collect_sun_async(user_id)
    await add_user_suns_async(user_id, sun_amount)
    
    total_suns = await get_user_suns_async(user_id)
    
    text = (
        f"☀️ *{user.full_name}*, вы собрали {sun_amount} солнышек!\n"
        f"📊 Всего у вас: {total_suns}☀️\n\n"
        f"⏳ Следующий сбор будет доступен через 90 минут."
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="spring_back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, f"✅ +{sun_amount}☀️")

async def spring_exchange_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Обменник'"""
    query = update.callback_query
    user = query.from_user
    
    now = datetime.now()
    if now < SPRING_EVENT_END:
        text = (
            f"💱 *{user.full_name}*, обменник еще не доступен!\n\n"
            f"📅 Откроется 1 июня 2026 года.\n"
            f"Копите солнышки до этого времени!"
        )
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="spring_back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        await safe_answer(query, "")
        return
    
    suns = await get_user_suns_async(user.id)
    
    text = (
        f"💱 *{user.full_name}*, обменник открыт!\n\n"
        f"☀️ У вас {suns} солнышек\n\n"
        f"🔄 Доступные обмены:\n"
        f"• 100☀️ → 1000ms¢\n"
        f"• 500☀️ → 6000ms¢\n"
        f"• 1000☀️ → 15000ms¢\n"
        f"• 5000☀️ → Секретный приз\n\n"
        f"ℹ️ Для обмена напишите /exchange *количество*"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="spring_back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def spring_castle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Весенний замок'"""
    query = update.callback_query
    user = query.from_user
    
    text = (
        f"🏰 *{user.full_name}*, добро пожаловать в наш замок!\n\n"
        f"Это замок бота Monstmines.\n\n"
        f"Ожидай появления и доработки в нашем телеграмм канале."
    )
    
    keyboard = [
        [InlineKeyboardButton("📣 Канал", url=SPRING_CHANNEL)],
        [InlineKeyboardButton("🔙 Назад", callback_data="spring_back_to_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def spring_tasks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Задания'"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    tasks = await get_all_spring_tasks_async()
    user_tasks_data = await get_all_user_tasks_async(user_id)
    
    if not tasks:
        text = f"⏳ *{user.full_name}*, заданий пока нет, ожидайте пополнения."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="spring_back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        await safe_answer(query, "")
        return
    
    progress_dict = {}
    for ut in user_tasks_data:
        task_id = ut['id']
        progress_dict[task_id] = {
            'progress': ut['progress'],
            'completed': ut['completed'],
            'claimed': ut['claimed']
        }
    
    text = f"📒 *{user.full_name}*, доступные задания:\n\n"
    
    for task in tasks:
        task_id = task['id']
        if task_id in progress_dict:
            progress = progress_dict[task_id]['progress']
            completed = progress_dict[task_id]['completed']
            claimed = progress_dict[task_id]['claimed']
        else:
            progress = 0
            completed = 0
            claimed = 0
        
        status = "✅" if completed == 1 and claimed == 0 else "🎁" if completed == 1 and claimed == 1 else "⏳"
        text += f"🆔 {task_id} {task['description']}\n"
        text += f"Прогресс: {status} {progress} из {task['target_count']}\n\n"
    
    text += f"ℹ️ Если выполнил задания, введи /put *айди*\n"
    text += f"⏱ Задания обновляются каждый день в 00:00 по МСК"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="spring_back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def spring_back_to_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню ивента"""
    query = update.callback_query
    user = query.from_user
    
    text = (
        f"☀️ *{user.full_name}*, добро пожаловать в ивент \"Весенний блик!\"\n\n"
        f"ℹ️ Твоя задача выполнить как можно больше заданий получив больше солнышек.\n\n"
        f"⏳ В конце ивента вы сможете обменять солнышки на очень щедрые призы!"
    )
    
    keyboard = [
        [InlineKeyboardButton("🤫 Весенние тайны", callback_data="spring_mysteries")],
        [InlineKeyboardButton("☀️ Сбор солнышек", callback_data="spring_collect")],
        [InlineKeyboardButton("💱 Обменник", callback_data="spring_exchange")],
        [InlineKeyboardButton("🏰 Весенний замок", callback_data="spring_castle")],
        [InlineKeyboardButton("🎯 Задания", callback_data="spring_tasks")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "")

async def spring_prize_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора типа награды для вопроса"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if user_id not in spring_question_creation:
        await safe_answer(query, "❌ Сессия создания вопроса не найдена", show_alert=True)
        return
    
    prize_type = query.data.replace('spring_prize_', '')
    spring_question_creation[user_id]['prize_type'] = prize_type
    spring_question_creation[user_id]['step'] = 'prize_value'
    
    if prize_type == 'coins':
        await query.edit_message_text(
            "👮‍♂️ Введите сумму ms'коинов за ответ.\n"
            "Можно использовать к, кк, ккк\n"
            "Пример: 100, 1к, 2.5кк"
        )
    elif prize_type == 'gold':
        await query.edit_message_text(
            "👮‍♂️ Введите количество ms'голдов за ответ.\n"
            "Можно использовать к, кк, ккк\n"
            "Пример: 5, 10к"
        )
    elif prize_type == 'sun':
        await query.edit_message_text(
            "👮‍♂️ Введите количество солнышек за ответ.\n"
            "Пример: 50"
        )
    elif prize_type == 'secret':
        await query.edit_message_text(
            "👮‍♂️ Введите название секретного приза\n"
            "Пример: Наклейка в Telegram"
        )
    
    await safe_answer(query, "")
