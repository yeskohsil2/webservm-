# handlers/common.py
import logging
import time
import re
import secrets
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from database import *

# Константы (будут переданы из main)
CHANNEL_USERNAME = "@monstrbotnews"
CHANNEL2_USERNAME = "@monstraction"
MAIN_ADMIN_ID = 6025818386
ADMIN_IDS = []
ITEMS_PER_PAGE = 5

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
async def safe_answer(query, text, show_alert=False):
    try:
        await query.answer(text, show_alert=show_alert)
    except Exception as e:
        # Игнорируем ошибки устаревших запросов
        if "Query is too old" in str(e) or "query id is invalid" in str(e):
            logging.debug(f"Ignoring old query: {e}")
        else:
            logging.error(f"Failed to answer callback query: {e}")

def format_amount(amount):
    """Форматирует число с разделителями тысяч"""
    if amount is None:
        return "0"
    amount_str = str(int(amount))
    result = ""
    for i, digit in enumerate(reversed(amount_str)):
        if i > 0 and i % 3 == 0:
            result = "." + result
        result = digit + result
    return result

def parse_amount(amount_str):
    """Парсит сумму с суффиксами к, м и т.д."""
    if not amount_str or len(amount_str) > 20:
        return 0
    amount_str = str(amount_str).lower().replace(' ', '').replace(',', '.')
    
    patterns = [
        (r'^(\d+(?:\.\d+)?)кккк$', 1000000000000),
        (r'^(\d+(?:\.\d+)?)ккк$', 1000000000),
        (r'^(\d+(?:\.\d+)?)кк$', 1000000),
        (r'^(\d+(?:\.\d+)?)к$', 1000),
        (r'^(\d+(?:\.\d+)?)м$', 1000000),
        (r'^(\d+(?:\.\d+)?)$', 1)
    ]

    for pattern, multiplier in patterns:
        match = re.match(pattern, amount_str)
        if match:
            try:
                from decimal import Decimal, getcontext
                getcontext().prec = 28
                value = Decimal(match.group(1))
                result = int(value * Decimal(multiplier))
                if result > 10**15:
                    return 0
                return result
            except:
                return 0
    return 0

def generate_check_code():
    """Генерирует код для чека"""
    return secrets.token_hex(8)

async def check_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Проверка подписки на каналы"""
    CHANNEL_USERNAME = context.bot_data.get('CHANNEL_USERNAME', '@monstrbotnews')
    CHANNEL2_USERNAME = context.bot_data.get('CHANNEL2_USERNAME', '@monstraction')
    
    try:
        member1 = await context.bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        member2 = await context.bot.get_chat_member(chat_id=CHANNEL2_USERNAME, user_id=user_id)
        return (member1.status in ['member', 'administrator', 'creator'] and
                member2.status in ['member', 'administrator', 'creator'])
    except Exception as e:
        logging.error(f"Subscription check error: {e}")
        return False

async def check_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка бана пользователя"""
    if not update.effective_user:
        return False
    
    user_id = update.effective_user.id
    ban_info = await is_user_banned_async(user_id)

    if ban_info:
        if ban_info['banned_until']:
            try:
                banned_until = datetime.strptime(ban_info['banned_until'], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try:
                    banned_until = datetime.strptime(ban_info['banned_until'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    banned_until = None
            
            if banned_until and banned_until < datetime.now():
                await unban_user_async(user_id)
                return False

        keyboard = [[InlineKeyboardButton("🧐 Не согласен", url="https://t.me/kleymorf")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        ban_duration = "навсегда" if not ban_info['banned_until'] else f"на {ban_info['ban_days']} дней"
        
        if update.message:
            await update.message.reply_text(
                f"🚨 {update.effective_user.full_name}, вы были заблокированы в боте {ban_duration} по причине: {ban_info['ban_reason']}\n\n"
                f"❓ Не согласны с наказанием? Нажмите кнопку ниже",
                reply_markup=reply_markup
            )
        elif update.callback_query and update.callback_query.message:
            await update.callback_query.message.reply_text(
                f"🚨 {update.effective_user.full_name}, вы были заблокированы в боте {ban_duration} по причине: {ban_info['ban_reason']}\n\n"
                f"❓ Не согласны с наказанием? Нажмите кнопку ниже",
                reply_markup=reply_markup
            )
        return True

    return False

async def show_checklist(update: Update, context: ContextTypes.DEFAULT_TYPE, page=1):
    """Показать список чеков"""
    user_id = update.effective_user.id
    checks = await get_all_checks_async()
    
    if not checks:
        await update.message.reply_text("❌ Чеки не найдены.")
        return

    items_per_page = 5
    total_pages = (len(checks) + items_per_page - 1) // items_per_page
    
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start_idx = (page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, len(checks))
    
    message = f"🧾 Список всех чеков:\n[{page}/{total_pages}]\n\n"
    
    for i in range(start_idx, end_idx):
        check = checks[i]
        message += f"{i+1}. {check['used_count']} активировано из {check['max_activations']} — 🔗 Ссылка: https://t.me/{context.bot.username}?start=chk_{check['check_code']}\n"

    keyboard = []
    nav_row = []
    
    if page > 1:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"checklist_page_{page-1}"))
    
    nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"checklist_page_{page+1}"))
    
    keyboard.append(nav_row)
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    else:
        await update.message.reply_text(message, reply_markup=reply_markup)

# ===== TOP FUNCTIONS =====
async def show_global_top(message_obj, context: ContextTypes.DEFAULT_TYPE, user):
    """Показать глобальный топ (с исключениями)"""
    top_users = await get_top_users_excluding_async(10)

    if not top_users:
        await message_obj.reply_text("🏆 Пользователей пока нет.")
        return

    message = f"🏆 {user.full_name}, топ 10 пользователей с наибольшим балансом\n\n"

    emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

    for i, row in enumerate(top_users):
        user_id, name, balance = row
        message += f"{emojis[i]} {name} — {format_amount(balance)}ms¢\n"

    rank = await get_user_rank_excluding_async(user.id)

    keyboard = [
        [
            InlineKeyboardButton(f"🙈 Вы на {rank} месте.", callback_data="noop"),
            InlineKeyboardButton("💬 Топ чата", callback_data="switch_to_chat_top")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await message_obj.reply_text(message, reply_markup=reply_markup)

async def show_chat_top(message_obj, context: ContextTypes.DEFAULT_TYPE, user):
    """Показать топ пользователей в чате (с исключениями)"""
    chat_id = message_obj.chat.id
    chat_members = []
    
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        for admin in admins:
            chat_members.append(admin.user.id)
        
        if user.id not in chat_members:
            chat_members.append(user.id)
            
    except Exception as e:
        logging.error(f"Error getting chat members: {e}")
        await message_obj.reply_text("❌ Не удалось получить список участников чата.")
        return

    all_top_users = await get_top_users_excluding_async(100)
    chat_top_users = []
    
    for row in all_top_users:
        if row[0] in chat_members:
            chat_top_users.append(row)
            if len(chat_top_users) >= 10:
                break

    if not chat_top_users:
        await message_obj.reply_text("🏆 В этом чате пока нет пользователей в топе.")
        return

    message = f"💬 {user.full_name}, топ 10 пользователей в этом чате\n\n"

    emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

    for i, row in enumerate(chat_top_users):
        user_id, name, balance = row
        message += f"{emojis[i]} {name} — {format_amount(balance)}ms¢\n"

    user_rank = 1
    for row in chat_top_users:
        if row[0] == user.id:
            break
        user_rank += 1
    else:
        user_rank = len(chat_top_users) + 1

    keyboard = [
        [
            InlineKeyboardButton(f"🙈 Вы на {user_rank} месте в чате.", callback_data="noop"),
            InlineKeyboardButton("🌍 Глобальный топ", callback_data="switch_to_global_top")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await message_obj.reply_text(message, reply_markup=reply_markup)

# ===== COMMON CALLBACK HANDLERS =====
async def handle_common_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для общих кнопок (help, top, subscription)"""
    if data.startswith("help_"):
        await handle_help_callbacks(update, context, data, user_id)
        return True
    
    elif data in ["switch_to_chat_top", "switch_to_global_top"]:
        await handle_top_switch(update, context, data, user_id)
        return True
    
    elif data == "check_subscription":
        await handle_subscription_check(update, context, data, user_id)
        return True
    
    elif data.startswith(("confirm_transfer_", "final_confirm_")):
        await handle_transfer_callbacks(update, context, data, user_id)
        return True
    
    return False

async def handle_help_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик кнопок помощи"""
    query = update.callback_query
    await safe_answer(query, "")
    
    texts = {
        "help_basic": "💡 Основные команды:\n/start - Начало работы\n/help - Помощь\n/support - Поддержка\n/balance - Баланс\n/top - Топ пользователей\n/profile - Профиль\nЕжедневный бонус — бонус\n Магазин - магазин.\n\n⚙️ Раздел в разработке",
        "help_games": "🎮 Игры:\n/mines - Мины\n/gold - Золото\n/pyramid - Пирамида\n🎰 Барабан - /slot\n\n⚙️ Раздел в разработке",
        "help_other": "🔘 Другое:\n/п - Перевод средств\n/перевод - Перевод средств\n/send - Перевод средств\n/ref - Реферальная система\n/рассылка - Создать рассылку (только для админа)\n/newcheck - Создать чек (админ)\n/checklist - Список чеков (админ)\n Магазин - магазин\n Мои акции - ваши акции\n\n⚙️ Раздел в разработке",
        "help_rules": "📕 Правила:\n1. Будьте вежливы\n2. Не спамьте\n3. Соблюдайте условия использования\n4. Играйте ответственно\n\n⚙️ Раздел в разработке"
    }
    
    try:
        await query.edit_message_text(texts.get(data, "Помощь"))
    except Exception as e:
        logging.error(f"Error editing help message: {e}")

async def handle_top_switch(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Переключение между глобальным топом и топом чата"""
    query = update.callback_query
    await safe_answer(query, "")
    
    if data == "switch_to_chat_top":
        await show_chat_top(query.message, context, query.from_user)
    elif data == "switch_to_global_top":
        await show_global_top(query.message, context, query.from_user)
    
    try:
        await query.message.delete()
    except Exception as e:
        logging.error(f"Error deleting message: {e}")

async def handle_subscription_check(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Проверка подписки"""
    query = update.callback_query
    is_subscribed = await check_subscription(update, context, user_id)
    
    if is_subscribed:
        await safe_answer(query, "✅ Успешно!")
        try:
            await query.delete_message()
        except Exception as e:
            logging.error(f"Error deleting message: {e}")
        
        CHANNEL_USERNAME = context.bot_data.get('CHANNEL_USERNAME', '@monstrbotnews')
        CHANNEL2_USERNAME = context.bot_data.get('CHANNEL2_USERNAME', '@monstraction')
        
        keyboard = [
            [InlineKeyboardButton("➕ Добавить бота в чат", url="https://t.me/monstrminesbot?startgroup=true")],
            [InlineKeyboardButton("📰 Новости [1]", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
            [InlineKeyboardButton("📰 Новости [2]", url=f"https://t.me/{CHANNEL2_USERNAME[1:]}")],
            [InlineKeyboardButton("💬 Официальный чат", url="https://t.me/gamemonstroff")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="👋*Привет*! Я – Монстр бот💣\n\n"
                "📲 Проведи свое время с удовольствием играя в нашего бота! Тут ты сможешь насладиться множественным функционалом и реально годными играми\n\n"
                "🧐 Во что будем играть? Пиши /game для получения список игр.\n\n"
                "❓ Думаю все понятно, если остались вопросы просто напиши /help.",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            if "Forbidden" in str(e) or "blocked" in str(e):
                logging.info(f"User {user_id} blocked the bot")
            else:
                logging.error(f"Error sending message: {e}")
    else:
        await safe_answer(query, "❌ Вы не подписались! Попробуйте еще раз либо подождите")

# ===== TRANSFER FUNCTIONS =====
pending_transfers = {}
transfer_confirmations = {}

async def handle_transfer_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для переводов"""
    if data.startswith("confirm_transfer_"):
        transfer_id = data.replace("confirm_transfer_", "")
        await confirm_transfer(update, context, transfer_id)
    elif data.startswith("final_confirm_"):
        confirmation_id = data.replace("final_confirm_", "")
        await final_confirm_transfer(update, context, confirmation_id)

async def confirm_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE, transfer_id):
    """Подтверждение перевода (первый шаг)"""
    query = update.callback_query
    user_id = query.from_user.id

    try:
        # Получаем pending_transfers из контекста
        pending_transfers = context.bot_data.get('pending_transfers', {})
        
        if transfer_id not in pending_transfers:
            await safe_answer(query, "❌ Перевод не найден или устарел.")
            return

        transfer = pending_transfers[transfer_id]

        if user_id != transfer['from_id']:
            await safe_answer(query, "🙈 Это не ваша кнопка!")
            return

        try:
            await query.delete_message()
        except Exception as e:
            if "message can't be deleted" not in str(e).lower():
                logging.error(f"Error deleting message in confirm_transfer: {e}")

        target_mention = f"<a href='tg://user?id={transfer['to_id']}'>{transfer['to_name']}</a>"

        confirmation_id = secrets.token_hex(8)

        # Получаем transfer_confirmations из контекста
        transfer_confirmations = context.bot_data.get('transfer_confirmations', {})

        transfer_confirmations[confirmation_id] = {
            'from_id': transfer['from_id'],
            'from_name': transfer['from_name'],
            'to_id': transfer['to_id'],
            'to_name': transfer['to_name'],
            'amount': transfer['amount'],
            'time': time.time(),
            'original_message_id': transfer['message_id'],
            'original_chat_id': transfer['chat_id']
        }
        
        # Сохраняем transfer_confirmations обратно
        context.bot_data['transfer_confirmations'] = transfer_confirmations

        keyboard = [[InlineKeyboardButton("Перейти в ЛС", url=f"https://t.me/{context.bot.username}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await context.bot.send_message(
            chat_id=transfer['chat_id'],
            text=f"✔ Почти готово! Подтвердите в личные сообщения с ботом.",
            reply_markup=reply_markup
        )

        try:
            keyboard_private = [[InlineKeyboardButton("❗ ПОДТВЕРЖДАЮ", callback_data=f"final_confirm_{confirmation_id}")]]
            reply_markup_private = InlineKeyboardMarkup(keyboard_private)

            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"☑ Последний шаг для перевода\n\n"
                    f"ℹ️ Информация для перевода:\n\n"
                    f"Вы хотите перевести {transfer['amount']}ms¢ пользователю {target_mention}.\n\n"
                    f"Hash перевода: {confirmation_id}"
                ),
                reply_markup=reply_markup_private,
                parse_mode='HTML'
            )
        except Exception as e:
            if "Forbidden" in str(e) or "blocked" in str(e):
                logging.info(f"User {user_id} blocked the bot, skipping private message")
                await context.bot.send_message(
                    chat_id=transfer['chat_id'],
                    text="❌ Вы заблокировали бота. Разблокируйте для продолжения."
                )
            else:
                logging.error(f"Failed to send private message: {e}")
                await context.bot.send_message(
                    chat_id=transfer['chat_id'],
                    text="❌ Не удалось отправить сообщение в ЛС. Возможно, вы не начинали диалог с ботом."
                )

        # Удаляем из pending_transfers
        del pending_transfers[transfer_id]
        context.bot_data['pending_transfers'] = pending_transfers
        
        await safe_answer(query, "✅ Перейдите в ЛС для подтверждения")

    except Exception as e:
        logging.error(f"Error in confirm_transfer: {e}")
        await safe_answer(query, "❌ Произошла ошибка. Попробуйте позже.")

async def final_confirm_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE, confirmation_id):
    """Финальное подтверждение перевода"""
    query = update.callback_query
    user_id = query.from_user.id

    try:
        # Получаем transfer_confirmations из контекста
        transfer_confirmations = context.bot_data.get('transfer_confirmations', {})
        
        if confirmation_id not in transfer_confirmations:
            await safe_answer(query, "❌ Перевод не найден или устарел.")
            return

        transfer = transfer_confirmations[confirmation_id]

        if user_id != transfer['from_id']:
            await safe_answer(query, "🙈 Это не ваша кнопка!")
            return

        success, message = await transfer_money_async(
            transfer['from_id'],
            transfer['to_id'],
            transfer['amount']
        )

        if not success:
            await safe_answer(query, f"❌ {message}")
            return

        await update_user_stats_async(transfer['from_id'], 0, 0)
        await update_user_stats_async(transfer['to_id'], 0, 0)

        transfer['completed'] = True

        try:
            await query.edit_message_text(
                f"💸 Вы успешно перевели {transfer['amount']}ms¢ пользователю {transfer['to_name']}."
            )
        except Exception as e:
            if "message can't be edited" not in str(e).lower():
                logging.error(f"Error editing message in final_confirm_transfer: {e}")

        try:
            new_balance = await get_user_async(transfer['to_id'])
            await context.bot.send_message(
                chat_id=transfer['to_id'],
                text=(
                    f"💰 {transfer['from_name']} перевёл вам {transfer['amount']}ms¢.\n"
                    f"Ваш новый баланс: {new_balance['balance']}ms¢"
                )
            )
        except Exception as e:
            if "Forbidden" in str(e) or "blocked" in str(e):
                logging.info(f"Recipient {transfer['to_id']} blocked the bot, skipping notification")
            else:
                logging.error(f"Failed to notify recipient: {e}")

        try:
            await context.bot.edit_message_text(
                chat_id=transfer['original_chat_id'],
                message_id=transfer['original_message_id'],
                text=f"💸 Перевод {transfer['amount']}ms¢ пользователю {transfer['to_name']} успешно завершен!"
            )
        except Exception as e:
            if "message can't be edited" not in str(e).lower() and "message to be edited not found" not in str(e).lower():
                logging.error(f"Error editing original message: {e}")

        # Удаляем из transfer_confirmations
        del transfer_confirmations[confirmation_id]
        context.bot_data['transfer_confirmations'] = transfer_confirmations

        await safe_answer(query, "✅ Перевод выполнен!")

    except Exception as e:
        logging.error(f"Error in final_confirm_transfer: {e}")
        await safe_answer(query, "❌ Произошла ошибка. Попробуйте позже.")

async def send_subscription_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправка приглашения подписаться на каналы"""
    try:
        user = update.effective_user
        CHANNEL_USERNAME = context.bot_data.get('CHANNEL_USERNAME', '@monstrbotnews')
        CHANNEL2_USERNAME = context.bot_data.get('CHANNEL2_USERNAME', '@monstraction')
        
        keyboard = [
            [InlineKeyboardButton("🔘 Подписаться [1]", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
            [InlineKeyboardButton("🔘 Подписаться [2]", url=f"https://t.me/{CHANNEL2_USERNAME[1:]}")],
            [InlineKeyboardButton("🔄 Проверить", callback_data="check_subscription")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"{user.full_name}, подпишитесь на новостные каналы для продолжения!",
            reply_markup=reply_markup
        )
    except Exception as e:
        if "Forbidden" in str(e) or "blocked" in str(e):
            logging.info(f"User {update.effective_user.id} blocked the bot")
        else:
            logging.error(f"Error in send_subscription_prompt: {e}")

def generate_math_problem():
    """Генерирует случайный математический пример и 10 вариантов ответов"""
    import random
    
    problem_types = ['add', 'sub', 'mul', 'div', 'sqrt']
    ptype = random.choice(problem_types)
    
    if ptype == 'add':
        a = random.randint(1, 50)
        b = random.randint(1, 50)
        question = f"{a} + {b}"
        correct = a + b
    elif ptype == 'sub':
        a = random.randint(10, 50)
        b = random.randint(1, a-1)
        question = f"{a} - {b}"
        correct = a - b
    elif ptype == 'mul':
        a = random.randint(2, 12)
        b = random.randint(2, 12)
        question = f"{a} × {b}"
        correct = a * b
    elif ptype == 'div':
        b = random.randint(2, 10)
        correct = random.randint(2, 10)
        a = b * correct
        question = f"{a} ÷ {b}"
    else:
        correct = random.randint(2, 9)
        a = correct * correct
        question = f"√{a}"
    
    options = [correct]
    while len(options) < 10:
        wrong = max(1, correct + random.randint(-15, 15))
        if random.random() < 0.3:
            wrong = random.randint(1, 100)
        if wrong not in options:
            options.append(wrong)
    
    random.shuffle(options)
    correct_index = options.index(correct)
    
    return question, options, correct_index
