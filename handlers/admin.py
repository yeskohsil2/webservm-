# handlers/admin.py
import logging
import time
import re
import asyncio
import random
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from database import *

from .common import safe_answer, format_amount, generate_check_code, show_checklist, generate_math_problem

# Константы
SLOT_WEIGHTS = [50, 30, 12, 5, 2, 1]
SLOT_RANGES = [
    (10, 50),
    (100, 200),
    (500, 700),
    (800, 1200),
    (1500, 2500),
    (10000, 25000)
]
SLOT_EMOJIS = ["🟢", "🔵", "🟣", "🟡", "🔴", "⚫"]
SLOT_NAMES = ["Rare", "Super Rare", "Epic", "Legendary", "Mythic", "Mystical"]
BONUS_COOLDOWN = 1800

# Глобальные переменные
mailing_data = {}

# ===== CALLBACK HANDLERS =====
async def handle_admin_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Общий обработчик для админских кнопок"""
    query = update.callback_query
    
    if data.startswith("bonus_"):
        parts = data.split('_')
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
                await bonus_command(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing bonus data: {e}")
    
    elif data.startswith("slot_spin_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                await slot_spin(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing slot spin data: {e}")
    
    elif data.startswith("slot_"):
        parts = data.split('_')
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
                await slot_command(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing slot data: {e}")
    
    elif data == "math_contest_confirm":  # <-- ДОБАВЬТЕ
        await math_contest_confirm_callback(update, context)

    elif data.startswith("math_answer_"):  # <-- ДОБАВЬТЕ
        await math_answer_callback(update, context)

    elif data == "confirm_reset_stocks":
        await reset_stocks_callback(update, context)
    
    elif data == "confirm_set_stocks_100":
        await set_stocks_callback(update, context)

async def handle_mailing_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для рассылки"""
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    
    if data == "mailing_toggle_markdown":
        await mailing_toggle_markdown(update, context)
    elif data == "mailing_toggle_inline":
        await mailing_toggle_inline(update, context)
    elif data == "mailing_confirm":
        await mailing_confirm(update, context)

async def handle_check_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для чеков"""
    query = update.callback_query
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    
    if data.startswith("confirm_user_check_"):
        await confirm_user_check_callback(update, context)
    elif data.startswith("activate_user_check_"):
        await activate_user_check_callback(update, context)
    elif data.startswith("checklist_page_"):
        if query.from_user.id != MAIN_ADMIN_ID:
            await safe_answer(query, "❌ Эта ссылка не для вас.", show_alert=True)
            return
        try:
            page = int(data.split("_")[2])
            await show_checklist(update, context, page)
            await safe_answer(query, "")
        except (ValueError, IndexError) as e:
            logging.error(f"Error parsing checklist page: {e}")

async def handle_top_exclude_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для переключения топов"""
    query = update.callback_query
    await safe_answer(query, "")
    
    if data == "switch_to_chat_top":
        from .common import show_chat_top
        await show_chat_top(query.message, context, query.from_user)
    elif data == "switch_to_global_top":
        from .common import show_global_top
        await show_global_top(query.message, context, query.from_user)
    
    try:
        await query.message.delete()
    except Exception as e:
        logging.error(f"Error deleting message: {e}")

# ===== BONUS =====
async def bonus_command(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Обработчик бонуса"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!")
        return

    from .common import check_ban
    if await check_ban(update, context):
        return

    user = query.from_user
    user_id = user.id

    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    CHANNEL_USERNAME = context.bot_data.get('CHANNEL_USERNAME')
    CHANNEL2_USERNAME = context.bot_data.get('CHANNEL2_USERNAME')

    if user_id not in ADMIN_IDS:
        from .common import check_subscription
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await safe_answer(query, "❌ Вы не подписаны на каналы!")
            return

    can_claim, minutes, seconds = await can_claim_bonus_async(user_id, BONUS_COOLDOWN)
    
    if not can_claim:
        await safe_answer(query, f"⏱ Вы уже забирали бонус! Подождите {minutes} мин. {seconds} сек.", show_alert=True)
        return

    bonus_amount = random.randint(100, 999)
    await claim_bonus_async(user_id, bonus_amount)
    await update_balance_async(user_id, bonus_amount)

    await query.edit_message_text(
        f"🎁 {user.full_name}, вы получили бонус {bonus_amount}ms¢\n"
        f"⏳ Следующий бонус будет доступен через 30 минут."
    )
    await safe_answer(query, f"✅ +{bonus_amount}ms¢")

# ===== SLOT =====
async def slot_command(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Начало игры в барабан (первый экран)"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!")
        return

    from .common import check_ban
    if await check_ban(update, context):
        return

    user = query.from_user
    user_id = user.id

    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS:
        from .common import check_subscription
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await safe_answer(query, "❌ Вы не подписаны на каналы!")
            return

    can_claim, minutes, seconds = await can_claim_slot_async(user_id, 1800)

    if not can_claim:
        await safe_answer(query, f"⏱ Барабан будет доступен через {minutes} мин. {seconds} сек.", show_alert=True)
        return

    keyboard = [[InlineKeyboardButton("🔫 Крутить", callback_data=f"slot_spin_{user_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"🎰 {user.full_name}, запустите барабан!",
        reply_markup=reply_markup
    )
    await safe_answer(query, "")

async def slot_spin(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Процесс кручения барабана"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!")
        return

    from .common import check_ban
    if await check_ban(update, context):
        return

    user = query.from_user
    user_id = user.id

    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS:
        from .common import check_subscription
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await safe_answer(query, "❌ Вы не подписаны на каналы!")
            return

    can_claim, minutes, seconds = await can_claim_slot_async(user_id, 1800)

    if not can_claim:
        await safe_answer(query, f"⏱ Барабан будет доступен через {minutes} мин. {seconds} сек.", show_alert=True)
        return

    await claim_slot_async(user_id)

    total_weight = sum(SLOT_WEIGHTS)
    rand = random.randint(1, total_weight)
    cumulative = 0
    prize_index = 0
    for i, weight in enumerate(SLOT_WEIGHTS):
        cumulative += weight
        if rand <= cumulative:
            prize_index = i
            break

    prize_range = SLOT_RANGES[prize_index]
    prize_amount = random.randint(prize_range[0], prize_range[1])
    prize_emoji = SLOT_EMOJIS[prize_index]
    prize_name = SLOT_NAMES[prize_index]

    await safe_answer(query, "🎰 Барабан запущен!")

    message = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            f"{user.full_name}, барабан запущен!\n"
            f"ℹ️ Награды:\n"
            f"🟢 Rare - 10-50ms¢\n"
            f"🔵 Super Rare - 100-200ms¢\n"
            f"🟣 Epic - 500-700ms¢\n"
            f"🟡 Legendary - 800-1200ms¢\n"
            f"🔴 Mythic - 1500-2500ms¢\n"
            f"⚫ Mystical - 10.000-25.000ms¢\n\n"
            f"⏱ Прогресс :"
        )
    )

    asyncio.create_task(slot_animation(context, message, user, prize_emoji, prize_name, prize_amount, user_id))
    
    return

async def slot_animation(context, message, user, prize_emoji, prize_name, prize_amount, user_id):
    """Анимация для конкретного пользователя"""
    try:
        for i in range(7):
            emojis = random.sample(SLOT_EMOJIS, 3)
            keyboard = [
                [
                    InlineKeyboardButton(emojis[0], callback_data="noop"),
                    InlineKeyboardButton(emojis[1], callback_data="noop"),
                    InlineKeyboardButton(emojis[2], callback_data="noop")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            try:
                await message.edit_text(
                    f"{user.full_name}, барабан запущен!\n"
                    f"ℹ️ Награды:\n"
                    f"🟢 Rare - 10-50ms¢\n"
                    f"🔵 Super Rare - 100-200ms¢\n"
                    f"🟣 Epic - 500-700ms¢\n"
                    f"🟡 Legendary - 800-1200ms¢\n"
                    f"🔴 Mythic - 1500-2500ms¢\n"
                    f"⚫ Mystical - 10.000-25.000ms¢\n\n"
                    f"⏱ Прогресс :",
                    reply_markup=reply_markup
                )
            except:
                pass
            await asyncio.sleep(1)

        emojis = random.sample(SLOT_EMOJIS, 3)
        emojis[1] = prize_emoji

        keyboard = [
            [
                InlineKeyboardButton(emojis[0], callback_data="noop"),
                InlineKeyboardButton(emojis[1], callback_data="noop"),
                InlineKeyboardButton(emojis[2], callback_data="noop")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update_balance_async(user_id, prize_amount)

        try:
            await message.edit_text(
                f"🎰 {user.full_name}, вы прокрутили барабан!\n\n"
                f"Вам выпало: {prize_emoji} {prize_name} — {format_amount(prize_amount)}ms¢",
                reply_markup=reply_markup
            )
        except:
            pass
            
    except Exception as e:
        logging.error(f"Error in slot animation for user {user_id}: {e}")

# ===== MAILING =====

async def mailing_toggle_markdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')

    if user_id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ Это не ваша кнопка!", show_alert=True)
        return

    mailing_data = context.bot_data.get('mailing_data', {})

    if user_id not in mailing_data:
        await safe_answer(query, "❌ Сессия рассылки не найдена.", show_alert=True)
        return

    mailing_data[user_id]['markdown'] = not mailing_data[user_id]['markdown']

    markdown_status = "✅" if mailing_data[user_id]['markdown'] else "❌"
    inline_status = "✅" if mailing_data[user_id]['inline'] else "❌"

    keyboard = [
        [
            InlineKeyboardButton(f"{markdown_status} Markdown text", callback_data="mailing_toggle_markdown"),
            InlineKeyboardButton(f"{inline_status} Inline-keyboard", callback_data="mailing_toggle_inline")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_reply_markup(reply_markup=reply_markup)
    context.bot_data['mailing_data'] = mailing_data
    await safe_answer(query, "✅ Настройки обновлены")

async def mailing_toggle_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')

    if user_id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ Это не ваша кнопка!", show_alert=True)
        return

    mailing_data = context.bot_data.get('mailing_data', {})

    if user_id not in mailing_data:
        await safe_answer(query, "❌ Сессия рассылки не найдена.", show_alert=True)
        return

    mailing_data[user_id]['inline'] = not mailing_data[user_id]['inline']

    markdown_status = "✅" if mailing_data[user_id]['markdown'] else "❌"
    inline_status = "✅" if mailing_data[user_id]['inline'] else "❌"

    keyboard = [
        [
            InlineKeyboardButton(f"{markdown_status} Markdown text", callback_data="mailing_toggle_markdown"),
            InlineKeyboardButton(f"{inline_status} Inline-keyboard", callback_data="mailing_toggle_inline")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_reply_markup(reply_markup=reply_markup)
    context.bot_data['mailing_data'] = mailing_data
    await safe_answer(query, "✅ Настройки обновлены")

async def mailing_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение и отправка рассылки"""
    query = update.callback_query
    user_id = query.from_user.id

    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')

    if user_id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ Это не ваша кнопка!", show_alert=True)
        return

    # Получаем данные рассылки из bot_data
    mailing_data = context.bot_data.get('mailing_data', {})
    
    if user_id not in mailing_data or mailing_data[user_id].get('step') != 'awaiting_confirm':
        await safe_answer(query, "❌ Сессия рассылки не найдена.", show_alert=True)
        return

    await safe_answer(query, "✅ Рассылка запущена!")

    progress_message = await query.message.reply_text(
        f"✅ Рассылка пошла!\nПрогресс:\nДоставлено 0 пользователям из 0\n⌛"
    )

    all_users = await get_all_users_async()
    total_users = len(all_users)
    delivered = 0

    raw_text = mailing_data[user_id]['text']

    def replace_link(match):
        text = match.group(1)
        url = match.group(2)
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return f'<a href="{url}">{text}</a>'

    processed_text = re.sub(r'\(([^|]+)\|([^)]+)\)', replace_link, raw_text)

    keyboard = []
    lines = processed_text.split('\n')
    final_text_lines = []

    for line in lines:
        if line.startswith('*inl|'):
            button_content = line.replace('*inl|', '').replace('*', '')
            link_match = re.search(r'(.+?)"([^"]+)"', button_content)

            if link_match:
                button_text = link_match.group(1).strip()
                button_url = link_match.group(2)

                if not button_url.startswith(('http://', 'https://')):
                    button_url = 'https://' + button_url

                keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
            else:
                keyboard.append([InlineKeyboardButton(button_content, callback_data="noop")])
        else:
            final_text_lines.append(line)

    final_text = '\n'.join(final_text_lines)
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None

    for user_row in all_users:
        target_id = user_row[0]
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text=final_text,
                parse_mode='HTML' if mailing_data[user_id]['markdown'] else None,
                reply_markup=reply_markup
            )

            delivered += 1

            if delivered % 5 == 0 or delivered == total_users:
                emoji = "⏳" if delivered % 2 == 0 else "⌛"
                try:
                    await progress_message.edit_text(
                        f"✅ Рассылка пошла!\nПрогресс:\nДоставлено {delivered} пользователям из {total_users}\n{emoji}"
                    )
                except:
                    pass

            await asyncio.sleep(0.05)

        except Exception as e:
            if "Forbidden" in str(e) or "blocked" in str(e):
                logging.info(f"User {target_id} blocked the bot, skipping mailing")
            else:
                logging.error(f"Failed to send mailing to {target_id}: {e}")

    await progress_message.edit_text(
        f"✅ Рассылка завершена!\nПрогресс:\nДоставлено {delivered} пользователям из {total_users}\n✅"
    )

    # Удаляем данные рассылки
    del mailing_data[user_id]
    context.bot_data['mailing_data'] = mailing_data

# ===== USER CHECK =====
async def confirm_user_check_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение создания чека"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    data = query.data
    parts = data.split('_')
    
    if len(parts) != 6:
        await safe_answer(query, "❌ Ошибка формата данных", show_alert=True)
        return
    
    try:
        creator_id = int(parts[3])
        activations = int(parts[4])
        amount = int(parts[5])
    except ValueError:
        await safe_answer(query, "❌ Неверные данные", show_alert=True)
        return
    
    if creator_id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!", show_alert=True)
        return
    
    total_cost = amount * activations
    
    db_user = await get_user_async(user_id, user.full_name, user.username)
    
    if db_user['balance'] < total_cost:
        await safe_answer(query, "❌ Недостаточно средств", show_alert=True)
        try:
            await query.message.delete()
        except:
            pass
        return
    
    await update_balance_async(user_id, -total_cost)
    
    check_code, check_number = await create_user_check_async(user_id, amount, activations)
    
    if not check_code:
        await update_balance_async(user_id, total_cost)
        await safe_answer(query, "❌ Ошибка при создании чека", show_alert=True)
        try:
            await query.message.delete()
        except:
            pass
        return
    
    check_link = f"https://t.me/{context.bot.username}?start=userchk_{check_code}"
    
    formatted_amount = format_amount(amount)
    formatted_total = format_amount(total_cost)
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🎟️ Активировать", url=check_link)
    ]])
    
    try:
        await query.edit_message_text(
            f"🧾 Чек #<b>{check_number}</b> создан!\n\n"
            f"💸 Списано: {formatted_total}ms¢\n"
            f"💰 За активацию: {formatted_amount}ms¢\n"
            f"📊 Активаций: {activations}\n\n"
            f"<blockquote>☑ Для активации нажми кнопку ниже</blockquote>",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    except Exception:
        pass
    
    await safe_answer(query, "✅ Чек создан!")


async def activate_user_check_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Активация чека"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_name = user.full_name
    
    check_code = query.data.replace('activate_user_check_', '')
    
    success, result = await use_user_check_async(check_code, user_id)
    
    if not success:
        messages = {
            "not_found": "❌ Чек не найден",
            "expired": "❌ Срок действия чека истёк",
            "no_activations": "❌ Активации закончились",
            "already_used": "❌ Ты уже активировал этот чек",
            "own_check": "❌ Нельзя активировать свой чек",
            "error": "❌ Ошибка активации"
        }
        await safe_answer(query, messages.get(result, "❌ Ошибка"), show_alert=True)
        await query.message.delete()
        return
    
    reward_amount = result
    formatted_reward = format_amount(reward_amount)
    
    # Начисляем награду активатору
    await update_balance_async(user_id, reward_amount)
    
    # Получаем информацию о чеке, чтобы узнать создателя
    check = await get_user_check_async(check_code)
    
    if check and check['creator_id'] != user_id:
        # Отправляем уведомление создателю чека
        remaining = check['max_activations'] - check['used_count']
        try:
            await context.bot.send_message(
                chat_id=check['creator_id'],
                text=(
                    f"<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji> <i>{user_name}, активировал(а) твой чек и получил(а) {formatted_reward}ms¢</i>\n"
                    f"Осталось активаций: {remaining}"
                ),
                parse_mode='HTML'
            )
        except Exception as e:
            if "Forbidden" not in str(e):
                logging.error(f"Failed to notify check creator {check['creator_id']}: {e}")
    
    await query.message.edit_text(
        f"✅ {user_name}, ты успешно активировал чек и получил {formatted_reward}ms¢!"
    )
    
    await safe_answer(query, "✅ Чек активирован!")

# ===== STOCKS ADMIN =====
async def reset_stocks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения сброса акций"""
    query = update.callback_query
    user = query.from_user

    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')

    if user.id not in ADMIN_IDS and user.id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ У вас нет прав на это действие.", show_alert=True)
        return

    if query.data == "confirm_reset_stocks":
        await safe_answer(query, "⏳ Выполняется сброс...")

        try:
            stocks = await get_all_stocks_async()
            stock_updates = [(stock['stock_id'], 0) for stock in stocks]
            await update_all_stocks_prices_async(stock_updates)

            await clear_all_portfolios_async()

            await query.edit_message_text(
                "✅ *Все акции успешно сброшены до 0!*\n\n"
                "• Курсы акций обнулены\n"
                "• Портфели пользователей очищены",
                parse_mode='Markdown'
            )

            KURS_CHANNEL = context.bot_data.get('KURS_CHANNEL')
            if KURS_CHANNEL:
                await context.bot.send_message(
                    chat_id=KURS_CHANNEL,
                    text="⚠️ *АДМИНИСТРАТОР СБРОСИЛ ВСЕ АКЦИИ ДО 0*\n\n"
                         "Все курсы обнулены. Ожидайте возобновления торгов.",
                    parse_mode='Markdown'
                )

        except Exception as e:
            logging.error(f"Error resetting stocks: {e}")
            await query.edit_message_text("❌ Произошла ошибка при сбросе акций.")

    await safe_answer(query, "")

async def set_stocks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения установки акций на 100"""
    query = update.callback_query
    user = query.from_user

    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')

    if user.id not in ADMIN_IDS and user.id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ У вас нет прав на это действие.", show_alert=True)
        return

    if query.data == "confirm_set_stocks_100":
        await safe_answer(query, "⏳ Устанавливаем цены...")

        try:
            stocks = await get_all_stocks_async()
            
            stock_updates = [(stock['stock_id'], 100) for stock in stocks]
            await update_all_stocks_prices_async(stock_updates)

            await query.edit_message_text(
                "✅ *Все акции установлены на 100ms¢!*\n\n"
                "• Bitcoin: 100ms¢\n"
                "• MonsterC: 100ms¢\n"
                "• Telegram: 100ms¢",
                parse_mode='Markdown'
            )

            KURS_CHANNEL = context.bot_data.get('KURS_CHANNEL')
            if KURS_CHANNEL:
                await context.bot.send_message(
                    chat_id=KURS_CHANNEL,
                    text="📊 *АДМИНИСТРАТОР УСТАНОВИЛ ВСЕ АКЦИИ НА 100ms¢*\n\n"
                         "Торги продолжаются с новой базовой ценой.",
                    parse_mode='Markdown'
                )

        except Exception as e:
            logging.error(f"Error setting stocks to 100: {e}")
            await query.edit_message_text("❌ Произошла ошибка при установке цен.")

    await safe_answer(query, "")

# handlers/admin.py (добавьте в конец файла)

# ===== MATH CONTEST =====
async def math_contest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для создания math-конкурса: !mt сумма или мт сумма"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return
    
    text = update.message.text.strip()
    
    if text.startswith('!mt ') or text.startswith('мт '):
        parts = text.split()
        if len(parts) < 2:
            await update.message.reply_text("❌ Использование: !mt *сумма* или мт *сумма*")
            return
        
        amount_str = parts[1]
        prize_amount = parse_amount(amount_str)
        
        if prize_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма.")
            return
        
        # Получаем math_contest_pending из контекста
        math_contest_pending = context.bot_data.get('math_contest_pending', {})
        math_contest_pending[user_id] = {'prize': prize_amount}
        context.bot_data['math_contest_pending'] = math_contest_pending
        
        formatted_prize = format_amount(prize_amount)
        
        keyboard = [[InlineKeyboardButton("✅ Подтверждаю", callback_data="math_contest_confirm")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"👨‍⚖️ Конкурс на {formatted_prize}ms¢ уже готов!\n\n"
            f"Осталось ждать подтверждения от администратора...",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text("❌ Использование: !mt *сумма* или мт *сумма*")

async def math_contest_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение запуска math-конкурса"""
    query = update.callback_query
    admin_id = query.from_user.id
    
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    
    if admin_id not in ADMIN_IDS and admin_id != MAIN_ADMIN_ID:
        await safe_answer(query, "Не наглей!🤬", show_alert=True)
        return
    
    # Получаем math_contest_pending из контекста
    math_contest_pending = context.bot_data.get('math_contest_pending', {})
    
    if admin_id not in math_contest_pending:
        await safe_answer(query, "❌ Конкурс не найден", show_alert=True)
        return
    
    await safe_answer(query, "✅ Отправляю...")
    
    await query.edit_message_text("✅ Отправляю...")
    
    prize = math_contest_pending[admin_id]['prize']
    formatted_prize = format_amount(prize)
    
    question, options, correct_index = generate_math_problem()
    
    contest_id = await create_math_contest_async(
        prize_amount=prize,
        question=question,
        correct_answer=options[correct_index],
        options=options,
        created_by=admin_id
    )
    
    keyboard = []
    row1 = []
    row2 = []
    
    for i, opt in enumerate(options):
        callback_data = f"math_answer_{contest_id}_{i}"
        button = InlineKeyboardButton(str(opt), callback_data=callback_data)
        
        if i < 5:
            row1.append(button)
        else:
            row2.append(button)
    
    keyboard.append(row1)
    keyboard.append(row2)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    sent_msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            f"🧮 Math-конкурс!\n\n"
            f"💸 Награда — {formatted_prize}ms¢.\n"
            f"📝 Пример — {question}"
        ),
        reply_markup=reply_markup
    )
    
    await start_math_contest_async(contest_id, sent_msg.message_id, update.effective_chat.id)
    
    # Удаляем из pending
    del math_contest_pending[admin_id]
    context.bot_data['math_contest_pending'] = math_contest_pending

async def math_answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ответа на math-конкурс"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    data = query.data
    parts = data.split('_')
    
    if len(parts) != 4:
        await safe_answer(query, "❌ Ошибка", show_alert=True)
        return
    
    contest_id = int(parts[2])
    option_index = int(parts[3])
    
    contest = await get_math_contest_async(contest_id)
    if not contest:
        await safe_answer(query, "❌ Конкурс не найден", show_alert=True)
        return
    
    if contest['status'] != 'active':
        await safe_answer(query, "❌ Конкурс уже завершен", show_alert=True)
        return
    
    MATH_CONTEST_COOLDOWN = 0.9
    can_attempt = await can_user_attempt_async(contest_id, user_id)
    if not can_attempt:
        last_attempt = await get_user_last_attempt_time_async(contest_id, user_id)
        if last_attempt:
            from datetime import datetime
            last_time = datetime.strptime(last_attempt, '%Y-%m-%d %H:%M:%S.%f')
            now = datetime.now()
            diff = (now - last_time).total_seconds()
            if diff < MATH_CONTEST_COOLDOWN:
                remaining = round(MATH_CONTEST_COOLDOWN - diff, 1)
                await safe_answer(query, f"Подождите {remaining} секунд", show_alert=True)
                return
        
        await safe_answer(query, "✖ Неправильный вариант", show_alert=True)
        return
    
    options = contest['options']
    selected_answer = options[option_index]
    correct_answer = contest['correct_answer']
    
    is_correct = (selected_answer == correct_answer)
    
    await add_math_attempt_async(contest_id, user_id, option_index, is_correct)
    
    if is_correct:
        success = await finish_math_contest_async(contest_id, user_id, user.full_name)
        
        if success:
            await update_balance_async(user_id, contest['prize_amount'])
            
            formatted_prize = format_amount(contest['prize_amount'])
            
            keyboard = [[InlineKeyboardButton("🏆 Конкурс завершен", callback_data="noop")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                text=(
                    f"🧮 Math-конкурс окончен!\n\n"
                    f"💸 Награда — {formatted_prize}ms¢ была доставлена пользователю {user.full_name}\n\n"
                    f"Правильный ответ был {correct_answer}."
                ),
                reply_markup=reply_markup
            )
            
            await safe_answer(query, f"✅ Вы выиграли {formatted_prize}ms¢!", show_alert=True)
        else:
            await safe_answer(query, "❌ Кто-то уже ответил раньше", show_alert=True)
    else:
        await safe_answer(query, "✖ Неправильный вариант", show_alert=True)

