# handlers/games.py
import logging
import random
import time
import asyncio
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from database import *
from telegram.error import RetryAfter
# В начале файла, после других импортов, добавьте:
from .common import check_ban, check_subscription, send_subscription_prompt
# Константы
FIELD_SIZE = 5
CELLS_TOTAL = FIELD_SIZE * FIELD_SIZE
COOLDOWN_SECONDS = 2
DICE_COOLDOWN_SECONDS = 2
CRASH_SESSIONS = {}
CRASH_COOLDOWN = 2
MINES_MULTIPLIERS = {
    1: [1.01, 1.05, 1.10, 1.15, 1.21, 1.27, 1.34, 1.41, 1.48, 1.56, 1.64, 1.72, 1.81, 1.90, 2.00, 2.10, 2.21, 2.32, 2.44, 2.56, 2.69, 2.82, 2.96, 3.11],
    2: [1.05, 1.15, 1.26, 1.39, 1.53, 1.68, 1.85, 2.04, 2.24, 2.46, 2.71, 2.98, 3.28, 3.61, 3.97, 4.37, 4.81, 5.29, 5.82, 6.40, 7.04, 7.74, 8.51, 9.36],
    3: [1.10, 1.26, 1.45, 1.68, 1.94, 2.24, 2.59, 3.00, 3.47, 4.01, 4.64, 5.37, 6.21, 7.19, 8.32, 9.63, 11.14, 12.89, 14.92, 17.26, 19.97, 23.11, 26.74, 30.94],
    4: [1.15, 1.39, 1.68, 2.04, 2.47, 3.00, 3.64, 4.41, 5.35, 6.49, 7.87, 9.55, 11.58, 14.05, 17.04, 20.67, 25.08, 30.42, 36.90, 44.76, 54.30, 65.86, 79.89, 96.91],
    5: [1.21, 1.53, 1.94, 2.47, 3.14, 3.99, 5.07, 6.45, 8.20, 10.43, 13.26, 16.86, 21.44, 27.26, 34.66, 44.07, 56.04, 71.25, 90.60, 115.20, 146.48, 186.25, 236.83, 301.13],
    6: [1.27, 1.68, 2.24, 3.00, 4.01, 5.37, 7.19, 9.63, 12.89, 17.26, 23.11, 30.94, 41.43, 55.47, 74.27, 99.44, 133.14, 178.25, 238.65, 319.54, 427.86, 572.90, 767.09, 1027.23],
    7: [1.34, 1.85, 2.59, 3.64, 5.07, 7.19, 10.14, 14.30, 20.16, 28.43, 40.09, 56.53, 79.71, 112.39, 158.47, 223.44, 315.05, 444.22, 626.35, 883.15, 1245.24, 1755.79, 2475.66, 3490.68],
    8: [1.41, 2.04, 3.00, 4.41, 6.45, 9.63, 14.30, 21.23, 31.53, 46.81, 69.51, 103.22, 153.28, 227.62, 338.01, 502.05, 745.54, 1107.13, 1644.09, 2441.48, 3625.60, 5384.02, 7995.27, 11875.97],
    9: [1.48, 2.24, 3.47, 5.35, 8.20, 12.89, 20.16, 31.53, 49.30, 77.09, 120.54, 188.50, 294.78, 460.96, 720.79, 1127.16, 1762.53, 2755.80, 4309.06, 6737.87, 10536.68, 16476.43, 25764.92, 40290.61],
    10: [1.56, 2.46, 4.01, 6.49, 10.43, 17.26, 28.43, 46.81, 77.09, 126.98, 209.16, 344.54, 567.58, 935.16, 1540.58, 2538.14, 4182.41, 6891.37, 11354.75, 18713.39, 30837.85, 50817.30, 83752.76, 138023.54],
    11: [1.64, 2.71, 4.64, 7.87, 13.26, 23.11, 40.09, 69.51, 120.54, 209.16, 362.96, 629.84, 1092.92, 1896.65, 3291.52, 5711.29, 9912.09, 17202.48, 29855.30, 51812.94, 89925.45, 156083.65, 270914.12, 470237.50],
    12: [1.72, 2.98, 5.37, 9.55, 16.86, 30.94, 56.53, 103.22, 188.50, 344.54, 629.84, 1151.42, 2104.74, 3848.21, 7036.92, 12866.51, 23523.78, 43010.65, 78633.93, 143760.30, 262847.19, 480602.90, 878753.30, 1606927.03],
    13: [1.81, 3.28, 6.21, 11.58, 21.44, 41.43, 79.71, 153.28, 294.78, 567.58, 1092.92, 2104.74, 4054.04, 7809.77, 15046.42, 28993.23, 55865.90, 107639.52, 207423.18, 399705.03, 770257.96, 1484327.39, 2860424.79, 5512050.09],
    14: [1.90, 3.61, 7.19, 14.05, 27.26, 55.47, 112.39, 227.62, 460.96, 935.16, 1896.65, 3848.21, 7809.77, 15853.83, 32183.27, 65359.00, 132747.77, 269624.96, 547694.09, 1112419.61, 2259659.93, 4591021.47, 9326665.43, 18951455.02],
    15: [2.00, 3.97, 8.32, 17.04, 34.66, 74.27, 158.47, 338.01, 720.79, 1540.58, 3291.52, 7036.92, 15046.42, 32183.27, 68867.00, 147375.38, 315383.31, 675118.64, 1445430.88, 3095160.06, 6629643.13, 14202577.85, 30428517.60, 65188602.66],
    16: [2.10, 4.37, 9.63, 20.67, 44.07, 99.44, 223.44, 502.05, 1127.16, 2538.14, 5711.29, 12866.51, 28993.23, 65359.00, 147375.38, 332466.48, 750041.10, 1692756.40, 3820902.92, 8625171.19, 19473471.68, 43970855.25, 99297221.48, 224289054.56],
    17: [2.21, 4.81, 11.14, 25.08, 56.04, 133.14, 315.05, 745.54, 1762.53, 4182.41, 9912.09, 23523.78, 55865.90, 132747.77, 315383.31, 750041.10, 1784225.89, 4246487.61, 10110660.51, 24075573.68, 57352079.83, 136637698.63, 325625670.90, 776236466.11],
    18: [2.32, 5.29, 12.89, 30.42, 71.25, 178.25, 444.22, 1107.13, 2755.80, 6891.37, 17202.48, 43010.65, 107639.52, 269624.96, 675118.64, 1692756.40, 4246487.61, 10656299.66, 26744415.88, 67155747.11, 168580757.63, 423458675.13, 1063573213.67, 2671994778.91],
    19: [2.44, 5.82, 14.92, 36.90, 90.60, 238.65, 626.35, 1644.09, 4309.06, 11354.75, 29855.30, 78633.93, 207423.18, 547694.09, 1445430.88, 3820902.92, 10110660.51, 26744415.88, 70821071.71, 187603603.76, 497068226.09, 1317328720.62, 3491413668.24, 9256314892.34],
    20: [2.56, 6.40, 17.26, 44.76, 115.20, 319.54, 883.15, 2441.48, 6737.87, 18713.39, 51812.94, 143760.30, 399705.03, 1112419.61, 3095160.06, 8625171.19, 24075573.68, 67155747.11, 187603603.76, 524566257.17, 1467951831.81, 4110694109.78, 11515005527.80, 32274850425.43],
    21: [2.69, 7.04, 19.97, 54.30, 146.48, 427.86, 1245.24, 3625.60, 10536.68, 30837.85, 89925.45, 262847.19, 770257.96, 2259659.93, 6629643.13, 19473471.68, 57352079.83, 168580757.63, 497068226.09, 1467951831.81, 4340857284.73, 12848341887.98, 38066713844.24, 112868252396.56],
    22: [2.82, 7.74, 23.11, 65.86, 186.25, 572.90, 1755.79, 5384.02, 16476.43, 50817.30, 156083.65, 480602.90, 1484327.39, 4591021.47, 14202577.85, 43970855.25, 136637698.63, 423458675.13, 1317328720.62, 4110694109.78, 12848341887.98, 40234197810.62, 126234672127.78, 396856782597.25],
    23: [2.96, 8.51, 26.74, 79.89, 236.83, 767.09, 2475.66, 7995.27, 25764.92, 83752.76, 270914.12, 878753.30, 2860424.79, 9326665.43, 30428517.60, 99297221.48, 325625670.90, 1063573213.67, 3491413668.24, 11515005527.80, 38066713844.24, 126234672127.78, 419897211577.99, 1401716350143.57],
    24: [3.11, 9.36, 30.94, 96.91, 301.13, 1027.23, 3490.68, 11875.97, 40290.61, 138023.54, 470237.50, 1606927.03, 5512050.09, 18951455.02, 65188602.66, 224289054.56, 776236466.11, 2671994778.91, 9256314892.34, 32274850425.43, 112868252396.56, 396856782597.25, 1401716350143.57, 4981605467192.89]
}

GOLD_MULTIPLIERS = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]

PYRAMID_EMOJIS = ["🚪", "🏚", "🛖", "🏠", "🏡", "🏢", "🏣", "🏤", "🏛️", "🏰", "🏯", "🕌"]
PYRAMID_MULTIPLIERS_3 = [1.31, 1.74, 2.32, 3.10, 4.13, 5.51, 7.34, 9.79, 13.05, 17.40, 23.20, 30.94]
PYRAMID_MULTIPLIERS_2 = [1.45, 2.10, 3.05, 4.42, 6.41, 9.29, 13.47, 19.53, 28.32, 41.06, 59.54, 86.33]
PYRAMID_MULTIPLIERS_1 = [1.62, 2.62, 4.24, 6.86, 11.11, 17.99, 29.14, 47.20, 76.46, 123.86, 200.65, 325.05]

TOWER_FIELD_SIZE = 5
TOWER_MAX_LEVEL = 8
TOWER_MULTIPLIERS = {
    1: [1.21, 1.52, 1.89, 2.37, 2.96, 3.70, 4.63, 5.78, 7.23],
    2: [1.62, 2.69, 4.49, 7.48, 12.47, 20.79, 34.65, 57.75, 96.25],
    3: [2.15, 4.62, 9.93, 21.35, 45.90, 98.69, 212.18, 456.19, 980.81],
    4: [2.86, 8.18, 23.39, 66.89, 191.30, 547.12, 1564.77, 4475.24, 12800.00]
}

RR_MULTIPLIERS = {
    1: 1.15,
    2: 1.45,
    3: 1.95,
    4: 2.9,
    5: 5.8
}

RR_STEP_MULTIPLIERS = {
    1: [0, 0.7, 1.8, 3.2, 5.0, 7.5],
    2: [0, 0.6, 1.4, 2.4, 3.8, 5.8],
    3: [0, 0.5, 1.1, 1.9, 3.1],
    4: [0, 0.9, 1.9, 2.7],
    5: [0, 5.8]
}

DICE_MIN_BET = 1000
DICE_MAX_BET = 50000000
DICE_MAX_GAMES_PER_CHAT = 5
DICE_TIMEOUT = 30

BOWLING_MULTIPLIERS = {
    'strike': 3.5,
    'miss': 3.2,
    1: 2.9,
    2: 2.9,
    3: 2.9,
    4: 2.9
}

BOWLING_NUMBERS = {
    1: "1️⃣",
    2: "2️⃣",
    3: "3️⃣",
    4: "4️⃣",
    5: "5️⃣"
}

BOWLING_WORDS = {
    1: "кегля",
    2: "кегли",
    3: "кегли",
    4: "кегли",
    5: "кеглей"
}

# Константы для КНБ
KHB_EMOJIS = {
    'камень': '🪨',
    'ножницы': '✂️',
    'бумага': '📃'
}

KHB_WIN_MESSAGES = [
    "Победа! 🎉",
    "Вы выиграли! ✨",
    "Удача на вашей стороне! 🌟",
    "Непобедимый! 👑"
]

KHB_LOSE_MESSAGES = [
    "В следующий раз повезёт! 🍀",
    "Проигрыш — тоже опыт 💪",
    "Попробуйте ещё раз! 🔄",
    "Удача отвернулась 😅"
]

KHB_DRAW_MESSAGES = [
    "Ничья! 🤝",
    "Дружеская ничья! 🕊️",
    "В этот раз никто не уступил ⚖️"
]

# Вспомогательные функции
async def safe_answer(query, text, show_alert=False):
    try:
        await query.answer(text, show_alert=show_alert)
    except Exception as e:
        logging.error(f"Failed to answer callback query: {e}")

def format_amount(amount):
    if amount is None:
        return "0"
    amount_str = str(int(amount))
    result = ""
    for i, digit in enumerate(reversed(amount_str)):
        if i > 0 and i % 3 == 0:
            result = "." + result
        result = digit + result
    return result

def generate_game_hash(game_data):
    import json, secrets, hashlib
    data_string = json.dumps(game_data, sort_keys=True) + str(time.time()) + secrets.token_hex(8)
    return hashlib.sha256(data_string.encode()).hexdigest()[:16]

def generate_crash_multiplier():
    """
    Генерирует случайный множитель для игры Краш с диапазоном 1.00-20.00
    - 1.00-1.99: часто (50%)
    - 2.00-3.99: средне (30%)
    - 4.00-6.99: редко (15%)
    - 7.00-20.00: очень редко (5%)
    """
    r = random.random() * 100  # число от 0 до 100

    if r < 50:  # 50% - часто (1.00-1.99)
        return round(random.uniform(1.00, 1.99), 2)
    elif r < 80:  # 30% - средне (2.00-3.99)
        return round(random.uniform(2.00, 3.99), 2)
    elif r < 95:  # 15% - редко (4.00-6.99)
        return round(random.uniform(4.00, 6.99), 2)
    else:  # 5% - очень редко (7.00-20.00)
        return round(random.uniform(7.00, 20.00), 2)

def parse_bet_amount(amount_str, user_balance=None):
    if not amount_str:
        return 0
    amount_str = str(amount_str).lower().strip()
    all_variants = ['всё', 'все', 'all']
    if amount_str in all_variants:
        if user_balance is not None:
            return user_balance
        return 0
    amount_str = amount_str.replace(',', '.')
    multipliers = {
        'кккк': 1000000000000,
        'ккк': 1000000000,
        'кк': 1000000,
        'к': 1000,
        'м': 1000000
    }
    for suffix, multiplier in multipliers.items():
        if amount_str.endswith(suffix):
            try:
                num = float(amount_str[:-len(suffix)])
                return int(num * multiplier)
            except ValueError:
                pass
    try:
        if '.' in amount_str and amount_str.count('.') > 1:
            amount_str = amount_str.replace('.', '')
        return int(float(amount_str))
    except ValueError:
        return 0

def check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS):
    current_time = time.time()
    if user_id in LAST_CLICK_TIME:
        time_diff = current_time - LAST_CLICK_TIME[user_id]
        if time_diff < COOLDOWN_SECONDS:
            return round(COOLDOWN_SECONDS - time_diff, 1)
    LAST_CLICK_TIME[user_id] = current_time
    return 0

# ==================== MINES ====================
async def mines_cell_click(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, r, c):
    query = update.callback_query
    
    # Получаем данные из контекста
    MINES_SESSIONS = context.bot_data.get('MINES_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        # Обновляем словарь в контексте
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = MINES_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Вообще-то игра завершена)")
        return

    if session['board'][r][c] != '❓':
        await safe_answer(query, "🧐 Куда жмешь! Открыто уже.")
        return

    if (r, c) in session['mines']:
        session['status'] = 'lost'
        session['board'][r][c] = '💥'

        for mr, mc in session['mines']:
            if session['board'][mr][mc] == '❓':
                session['board'][mr][mc] = '💥'

        opened = session['opened']
        mines_count = session['mines_count']

        max_opened = len(MINES_MULTIPLIERS[mines_count]) - 1
        if opened > max_opened:
            opened = max_opened

        current_multiplier = MINES_MULTIPLIERS[mines_count][opened]

        board_text = ""
        for row in session['board']:
            board_text += ''.join(row) + "\n"

        message_text = (
            f"💥<b> Мины • проигрыш!</b>\n"
            f"••••••••••\n"
            f"💣 Мин: {mines_count}\n"
            f"💸 Ставка: <code>{format_amount(session['bet'])}ms¢</code>\n\n"
            f"💎 Открыто: {opened} из {CELLS_TOTAL - mines_count}\n\n"
            f"<blockquote>✔️ Ты мог забрать {int(session['bet'] * current_multiplier)}ms¢, но ничего страшного, повезет в следующий раз.</blockquote>\n\n"
            f"👩‍💻 Hash: {session['hash']}"
        )

        await save_game_hash_async(session['hash'], user_id, 'mines', session['bet'], 'lose')
        await update_user_stats_async(user_id, 0, session['bet'])
        await update_task_progress_for_game(user_id, 'mines', 1)

        keyboard = []
        for r_idx in range(FIELD_SIZE):
            row = []
            for c_idx in range(FIELD_SIZE):
                cell = session['board'][r_idx][c_idx]
                if cell == '❓':
                    cell = 'ㅤ'
                row.append(InlineKeyboardButton(cell, callback_data=f"mines_dead_{user_id}_{r_idx}_{c_idx}"))
            keyboard.append(row)

        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except:
            pass

        await safe_answer(query, "💥 Бомба!")
        # Обновляем сессию в контексте
        context.bot_data['MINES_SESSIONS'] = MINES_SESSIONS
        return

    session['board'][r][c] = '💎'
    session['opened'] += 1
    opened = session['opened']
    mines_count = session['mines_count']

    max_opened = len(MINES_MULTIPLIERS[mines_count]) - 1
    if opened > max_opened:
        opened = max_opened

    # Обновляем сессию в контексте
    context.bot_data['MINES_SESSIONS'] = MINES_SESSIONS
    await send_mines_board(update, context, user_id)
    await safe_answer(query, f"💎 +{MINES_MULTIPLIERS[mines_count][opened]:.2f}x")

# games.py - исправленная версия mines_take_win()
async def mines_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    # Получаем данные из контекста
    MINES_SESSIONS = context.bot_data.get('MINES_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = MINES_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Вообще-то игра завершена)")
        return

    if session['opened'] == 0:
        await safe_answer(query, "⚠️ Сначала откройте хотя бы одну ячейку.")
        return

    session['status'] = 'won'
    opened = session['opened']
    mines_count = session['mines_count']

    max_opened = len(MINES_MULTIPLIERS[mines_count]) - 1
    if opened > max_opened:
        opened = max_opened

    current_multiplier = MINES_MULTIPLIERS[mines_count][opened]
    win_amount = int(session['bet'] * current_multiplier)

    await update_balance_async(user_id, win_amount)

    for mr, mc in session['mines']:
        if session['board'][mr][mc] == '❓':
            session['board'][mr][mc] = '💣'

    board_text = ""
    for row in session['board']:
        board_text += ''.join(row) + "\n"

    message_text = (
        f"🎉<b> Мины • Победа!</b> ✅\n"
        f"💣 Мин: {mines_count}\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
        f"💎 Открыто: {opened} из {CELLS_TOTAL - mines_count}\n\n"
        f"Забранный выигрыш {win_amount}ms¢ успешно пополнен на баланс.\n\n"
        f"👩‍💻 Hash: {session['hash']}"
    )

    await save_game_hash_async(session['hash'], user_id, 'mines', session['bet'], 'win')
    await update_user_stats_async(user_id, win_amount, 0)
    await update_task_progress_for_game(user_id, 'mines', 1)

    keyboard = []
    for r_idx in range(FIELD_SIZE):
        row = []
        for c_idx in range(FIELD_SIZE):
            cell = session['board'][r_idx][c_idx]
            if cell == '❓':
                cell = 'ㅤ'
            row.append(InlineKeyboardButton(cell, callback_data=f"mines_dead_{user_id}_{r_idx}_{c_idx}"))
        keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.edit_message_text(
            message_text,
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except:
        pass

    # ВАЖНО: Удаляем сессию из словаря
    del MINES_SESSIONS[user_id]
    # И СОХРАНЯЕМ обратно в context.bot_data
    context.bot_data['MINES_SESSIONS'] = MINES_SESSIONS
    
    await safe_answer(query, f"Выигрыш {win_amount}ms¢")

async def mines_cancel_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    # Получаем данные из контекста
    MINES_SESSIONS = context.bot_data.get('MINES_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = MINES_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['opened'] > 0:
        await safe_answer(query, "⚠️ Нельзя отменить игру после первого хода.")
        return

    await update_balance_async(user_id, session['bet'])

    try:
        await context.bot.delete_message(
            chat_id=session['chat_id'],
            message_id=session['message_id']
        )
    except:
        pass

    del MINES_SESSIONS[user_id]
    context.bot_data['MINES_SESSIONS'] = MINES_SESSIONS
    await safe_answer(query, "Игра отменена, средства возвращены")

async def send_mines_board(update, context, user_id):
    MINES_SESSIONS = context.bot_data.get('MINES_SESSIONS', {})
    session = MINES_SESSIONS.get(user_id)
    if not session:
        return

    opened = session['opened']
    mines_count = session['mines_count']

    max_opened = len(MINES_MULTIPLIERS[mines_count]) - 1
    if opened > max_opened:
        opened = max_opened

    current_multiplier = MINES_MULTIPLIERS[mines_count][opened]
    next_multiplier = MINES_MULTIPLIERS[mines_count][opened + 1] if opened < max_opened else current_multiplier

    board_text = ""
    for row in session['board']:
        board_text += ''.join(row) + "\n"

    message_text = (
        f"☘️ Мины — начни игру!\n"
        f"•••••••\n"
        f"💣 Мин: {mines_count}\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
        f"Текущий множитель: x{current_multiplier:.2f}.\n"
        f"📈 Следующий множитель: x{next_multiplier:.2f}.\n"
        f"{board_text}"
    )

    keyboard = []
    for r in range(FIELD_SIZE):
        row = []
        for c in range(FIELD_SIZE):
            cell = session['board'][r][c]
            if cell == '❓':
                row.append(InlineKeyboardButton("❓", callback_data=f"mines_cell_{user_id}_{r}_{c}"))
            elif cell == '💎':
                row.append(InlineKeyboardButton("💎", callback_data=f"mines_cell_{user_id}_{r}_{c}"))
            else:
                row.append(InlineKeyboardButton(cell, callback_data=f"mines_cell_{user_id}_{r}_{c}"))
        keyboard.append(row)

    if opened > 0:
        keyboard.append([
            InlineKeyboardButton("✔️ Забрать", callback_data=f"mines_take_{user_id}"),
        ])
    else:
        keyboard.append([
            InlineKeyboardButton("✔️ Забрать", callback_data=f"mines_take_{user_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"mines_cancel_{user_id}")
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    if session.get('message_id'):
        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except:
            msg = await context.bot.send_message(
                chat_id=session['chat_id'],
                text=message_text,
                reply_markup=reply_markup,
                message_thread_id=session['message_thread_id']
            )
            session['message_id'] = msg.message_id
    else:
        msg = await context.bot.send_message(
            chat_id=session['chat_id'],
            text=message_text,
            reply_markup=reply_markup,
            message_thread_id=session['message_thread_id']
        )
        session['message_id'] = msg.message_id
    
    context.bot_data['MINES_SESSIONS'] = MINES_SESSIONS

# ==================== GOLD ====================
async def gold_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, side):
    query = update.callback_query
    
    # Получаем данные из контекста
    GOLD_SESSIONS = context.bot_data.get('GOLD_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = GOLD_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Вообще-то игра завершена)")
        return

    current_level = session['opened']

    if current_level >= 12:
        await safe_answer(query, "⚠️ Вы уже прошли все уровни!")
        return

    if session['board'][current_level][0] != '❓' or session['board'][current_level][1] != '❓':
        await safe_answer(query, "🧐 Этот уровень уже открыт!")
        return

    correct_side = session['mines'][current_level]

    if side == correct_side:
        session['board'][current_level][0 if side == 'left' else 1] = '💰'
        session['board'][current_level][1 if side == 'left' else 0] = '🧨'
        session['opened'] += 1

        context.bot_data['GOLD_SESSIONS'] = GOLD_SESSIONS
        await send_gold_board(update, context, user_id)

        if session['opened'] >= 12:
            await gold_take_win(update, context, user_id)
        else:
            await safe_answer(query, "💰 +1 уровень!")
    else:
        session['status'] = 'lost'
        session['board'][current_level][0 if side == 'left' else 1] = '💥'

        for level in range(12):
            if session['board'][level][0] == '❓' and session['board'][level][1] == '❓':
                correct = session['mines'][level]
                session['board'][level][0] = '💸' if correct == 'right' else '🧨'
                session['board'][level][1] = '💸' if correct == 'left' else '🧨'

        board_lines = []
        for i in range(11, -1, -1):
            level = i
            left, right = session['board'][level]
            multiplier = GOLD_MULTIPLIERS[level]
            potential_win = int(session['bet'] * multiplier)
            board_lines.append(f"| {left} | {right} | {format_amount(potential_win)}ms¢ ({multiplier}x)")

        board_text = "\n".join(board_lines)

        message_text = (
            f"💥 Золото • Проигрыш!\n"
            f"••••••••\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
            f"⚜️ Пройдено: {session['opened']} из 12\n\n"
            f"👩‍💻 Hash: {session['hash']}"
        )

        await save_game_hash_async(session['hash'], user_id, 'gold', session['bet'], 'lose')
        await update_user_stats_async(user_id, 0, session['bet'])
        await update_task_progress_for_game(user_id, 'gold', 1)

        keyboard = []
        row = []
        row.append(InlineKeyboardButton("🧨", callback_data=f"gold_dead_{user_id}"))
        row.append(InlineKeyboardButton("🧨", callback_data=f"gold_dead_{user_id}"))
        keyboard.append(row)

        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except:
            pass

        del GOLD_SESSIONS[user_id]
        context.bot_data['GOLD_SESSIONS'] = GOLD_SESSIONS
        await safe_answer(query, "💥 Бомба!")

async def gold_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    # Получаем данные из контекста
    GOLD_SESSIONS = context.bot_data.get('GOLD_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = GOLD_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Вообще-то игра завершена)")
        return

    if session['opened'] == 0:
        await safe_answer(query, "⚠️ Сначала откройте хотя бы один уровень.")
        return

    session['status'] = 'won'
    opened = session['opened']
    current_multiplier = GOLD_MULTIPLIERS[opened - 1]
    win_amount = int(session['bet'] * current_multiplier)

    await update_balance_async(user_id, win_amount)

    for level in range(12):
        if session['board'][level][0] == '❓' and session['board'][level][1] == '❓':
            correct = session['mines'][level]
            session['board'][level][0] = '💸' if correct == 'right' else '🧨'
            session['board'][level][1] = '💸' if correct == 'left' else '🧨'

    board_lines = []
    for i in range(11, -1, -1):
        level = i
        left, right = session['board'][level]
        multiplier = GOLD_MULTIPLIERS[level]
        potential_win = int(session['bet'] * multiplier)
        board_lines.append(f"| {left} | {right} | {format_amount(potential_win)}ms¢ ({multiplier}x)")

    board_text = "\n".join(board_lines)

    message_text = (
        f"🎉 Золото • Победа!\n"
        f"••••••••\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
        f"💰 Выигрыш: {format_amount(win_amount)}ms¢\n\n"
        f"👩‍💻 Hash: {session['hash']}"
    )

    await save_game_hash_async(session['hash'], user_id, 'gold', session['bet'], 'win')
    await update_user_stats_async(user_id, win_amount, 0)
    await update_task_progress_for_game(user_id, 'gold', 1)

    keyboard = []
    row = []
    row.append(InlineKeyboardButton("💰", callback_data=f"gold_dead_{user_id}"))
    row.append(InlineKeyboardButton("💰", callback_data=f"gold_dead_{user_id}"))
    keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.edit_message_text(
            message_text,
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            reply_markup=reply_markup
        )
    except:
        pass

    del GOLD_SESSIONS[user_id]
    context.bot_data['GOLD_SESSIONS'] = GOLD_SESSIONS
    await safe_answer(query, f"✅ Выигрыш {format_amount(win_amount)}ms¢")

async def send_gold_board(update, context, user_id):
    GOLD_SESSIONS = context.bot_data.get('GOLD_SESSIONS', {})
    session = GOLD_SESSIONS.get(user_id)
    if not session:
        return

    board_lines = []
    for i in range(11, -1, -1):
        level = i
        left, right = session['board'][level]
        multiplier = GOLD_MULTIPLIERS[level]
        potential_win = int(session['bet'] * multiplier)
        board_lines.append(f"| {left} | {right} | {format_amount(potential_win)}ms¢ ({multiplier}x)")

    board_text = "\n".join(board_lines)

    if session['opened'] == 0:
        message_text = (
            f"🌻 Золото - начни игру!\n"
            f"••••••••••••••\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
            f"{board_text}"
        )
    else:
        current_multiplier = GOLD_MULTIPLIERS[session['opened'] - 1]
        current_win = int(session['bet'] * current_multiplier)
        message_text = (
            f"⚜️ Золото • игра идёт!\n"
            f"••••••••••\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
            f"💰 Выигрыш: x{current_multiplier} / {format_amount(current_win)}ms¢\n\n"
            f"{board_text}"
        )

    keyboard = []
    row = []
    row.append(InlineKeyboardButton("❓", callback_data=f"gold_left_{user_id}"))
    row.append(InlineKeyboardButton("❓", callback_data=f"gold_right_{user_id}"))
    keyboard.append(row)

    if session['opened'] > 0:
        keyboard.append([InlineKeyboardButton("✅ Забрать выигрыш", callback_data=f"gold_take_{user_id}")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    if session.get('message_id'):
        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except:
            msg = await context.bot.send_message(
                chat_id=session['chat_id'],
                text=message_text,
                reply_markup=reply_markup,
                message_thread_id=session['message_thread_id']
            )
            session['message_id'] = msg.message_id
    else:
        msg = await context.bot.send_message(
            chat_id=session['chat_id'],
            text=message_text,
            reply_markup=reply_markup,
            message_thread_id=session['message_thread_id']
        )
        session['message_id'] = msg.message_id
    
    context.bot_data['GOLD_SESSIONS'] = GOLD_SESSIONS

# ==================== PYRAMID ====================
async def pyramid_cell_click(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, level, door):
    query = update.callback_query
    
    # Получаем данные из контекста
    PYRAMID_SESSIONS = context.bot_data.get('PYRAMID_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = PYRAMID_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Вообще-то игра завершена)")
        return

    current_level = session['current_level']

    if current_level != level:
        await safe_answer(query, "🧐 Не тот уровень!")
        return

    if current_level >= 12:
        await safe_answer(query, "⚠️ Вы уже прошли все уровни!")
        return

    if (current_level, door) in session.get('opened_doors', []):
        await safe_answer(query, "🧐 Эта дверь уже открыта!")
        return

    if 'opened_doors' not in session:
        session['opened_doors'] = []

    if door in session['grave_positions'][current_level]:
        session['status'] = 'lost'

        final_board = []
        for i in range(4):
            if i in session['grave_positions'][current_level]:
                final_board.append('🪦')
            else:
                final_board.append('⭐')

        message_text = (
            f"😱 Пирамида • Проигрыш!\n"
            f"•••••••••••\n"
            f"🚪 Двери: {session['doors_count']}\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
            f"🔝 Пройдено: {current_level} из 12\n\n"
        )

        if current_level > 0:
            could_multiplier = session['multipliers'][current_level - 1]
            could_win = int(session['bet'] * could_multiplier)
            message_text += f"✔️ Мог забрать x{could_multiplier:.2f} / {could_win}ms¢\n\n"

        message_text += f"{final_board[0]}{final_board[1]}\n{final_board[2]}{final_board[3]}\n\n"
        message_text += f"👩‍💻 Hash: {session['hash']}"

        await save_game_hash_async(session['hash'], user_id, 'pyramid', session['bet'], 'lose')
        await update_user_stats_async(user_id, 0, session['bet'])
        await update_task_progress_for_game(user_id, 'pyramid', 1)

        keyboard = []
        row1 = []
        row1.append(InlineKeyboardButton(final_board[0], callback_data=f"dead_{user_id}"))
        row1.append(InlineKeyboardButton(final_board[1], callback_data=f"dead_{user_id}"))
        keyboard.append(row1)

        row2 = []
        row2.append(InlineKeyboardButton(final_board[2], callback_data=f"dead_{user_id}"))
        row2.append(InlineKeyboardButton(final_board[3], callback_data=f"dead_{user_id}"))
        keyboard.append(row2)

        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except Exception as e:
            logging.error(f"Error editing message: {e}")

        del PYRAMID_SESSIONS[user_id]
        context.bot_data['PYRAMID_SESSIONS'] = PYRAMID_SESSIONS
        await safe_answer(query, "💥 Могила!")
        return

    session['opened_doors'].append((current_level, door))
    session['current_level'] += 1

    context.bot_data['PYRAMID_SESSIONS'] = PYRAMID_SESSIONS

    if session['current_level'] >= 12:
        await pyramid_take_win(update, context, user_id)
    else:
        await send_pyramid_board(update, context, user_id)
        await safe_answer(query, f"✅ Уровень {current_level + 1} пройден!")

async def pyramid_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    # Получаем данные из контекста
    PYRAMID_SESSIONS = context.bot_data.get('PYRAMID_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = PYRAMID_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Вообще-то игра завершена)")
        return

    if session['current_level'] == 0:
        await safe_answer(query, "⚠️ Сначала откройте хотя бы один уровень.")
        return

    session['status'] = 'won'
    current_level = session['current_level']
    current_multiplier = session['multipliers'][current_level - 1]
    win_amount = int(session['bet'] * current_multiplier)

    await update_balance_async(user_id, win_amount)

    last_level = current_level - 1
    final_board = []
    for i in range(4):
        if i in session['grave_positions'][last_level]:
            final_board.append('🪦')
        else:
            final_board.append('⭐')

    message_text = (
        f"🥳 Пирамида • Победа!✅\n"
        f"•••••••••••\n"
        f"🚪 Двери: {session['doors_count']}\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"🔝 Пройдено: {current_level} из 12\n\n"
        f"💰 Выигрыш x{current_multiplier:.2f} / {win_amount}ms¢\n\n"
        f"{final_board[0]}{final_board[1]}\n{final_board[2]}{final_board[3]}\n\n"
        f"👩‍💻 Hash: {session['hash']}"
    )

    await save_game_hash_async(session['hash'], user_id, 'pyramid', session['bet'], 'win')
    await update_user_stats_async(user_id, win_amount, 0)
    await update_task_progress_for_game(user_id, 'pyramid', 1)
    
    keyboard = []
    row1 = []
    row1.append(InlineKeyboardButton(final_board[0], callback_data=f"dead_{user_id}"))
    row1.append(InlineKeyboardButton(final_board[1], callback_data=f"dead_{user_id}"))
    keyboard.append(row1)

    row2 = []
    row2.append(InlineKeyboardButton(final_board[2], callback_data=f"dead_{user_id}"))
    row2.append(InlineKeyboardButton(final_board[3], callback_data=f"dead_{user_id}"))
    keyboard.append(row2)

    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.edit_message_text(
            message_text,
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            reply_markup=reply_markup
        )
    except Exception as e:
        logging.error(f"Error editing message: {e}")

    del PYRAMID_SESSIONS[user_id]
    context.bot_data['PYRAMID_SESSIONS'] = PYRAMID_SESSIONS
    await safe_answer(query, f"✅ Выигрыш {win_amount}ms¢")

async def pyramid_cancel_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    # Получаем данные из контекста
    PYRAMID_SESSIONS = context.bot_data.get('PYRAMID_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = PYRAMID_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if session['current_level'] > 0:
        await safe_answer(query, "⚠️ Нельзя отменить игру после первого хода.")
        return

    await update_balance_async(user_id, session['bet'])

    try:
        await context.bot.delete_message(
            chat_id=session['chat_id'],
            message_id=session['message_id']
        )
    except Exception as e:
        logging.error(f"Error deleting message: {e}")

    del PYRAMID_SESSIONS[user_id]
    context.bot_data['PYRAMID_SESSIONS'] = PYRAMID_SESSIONS
    await safe_answer(query, "✅ Игра отменена, средства возвращены")

async def send_pyramid_board(update, context, user_id):
    PYRAMID_SESSIONS = context.bot_data.get('PYRAMID_SESSIONS', {})
    session = PYRAMID_SESSIONS.get(user_id)
    if not session:
        return

    current_level = session['current_level']
    doors_count = session['doors_count']
    multipliers = session['multipliers']

    current_emoji = PYRAMID_EMOJIS[current_level]
    current_multiplier = multipliers[current_level]
    current_win = int(session['bet'] * current_multiplier)

    if current_level == 0:
        message_text = (
            "🏃‍♂ Пирамида • начни путь!\n"
            f"•••••••\n"
            f"🚪 Двери: {doors_count}\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
            f"💰 Текущий множитель: x{current_multiplier:.2f} / {current_win}ms¢\n"
        )
    else:
        message_text = (
            f"🏃‍♂ Пирамида • игра идёт • уровень {current_level + 1}\n"
            f"•••••••\n"
            f"🚪 Двери: {doors_count}\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
            f"💰 Текущий множитель: x{current_multiplier:.2f} / {current_win}ms¢\n"
        )

    random_suffix = random.randint(1000, 9999)

    keyboard = []
    row1 = []
    row1.append(InlineKeyboardButton(current_emoji, callback_data=f"pyr_{user_id}_{current_level}_0_{random_suffix}"))
    row1.append(InlineKeyboardButton(current_emoji, callback_data=f"pyr_{user_id}_{current_level}_1_{random_suffix}"))
    keyboard.append(row1)

    row2 = []
    row2.append(InlineKeyboardButton(current_emoji, callback_data=f"pyr_{user_id}_{current_level}_2_{random_suffix}"))
    row2.append(InlineKeyboardButton(current_emoji, callback_data=f"pyr_{user_id}_{current_level}_3_{random_suffix}"))
    keyboard.append(row2)

    action_row = []
    if current_level > 0:
        action_row.append(InlineKeyboardButton("✅ Забрать выигрыш", callback_data=f"take_{user_id}_{random_suffix}"))
    else:
        action_row.append(InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{user_id}_{random_suffix}"))
    keyboard.append(action_row)

    reply_markup = InlineKeyboardMarkup(keyboard)

    if session.get('message_id'):
        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except Exception as e:
            logging.error(f"Error editing message: {e}")
            msg = await context.bot.send_message(
                chat_id=session['chat_id'],
                text=message_text,
                reply_markup=reply_markup,
                message_thread_id=session['message_thread_id']
            )
            session['message_id'] = msg.message_id
    else:
        msg = await context.bot.send_message(
            chat_id=session['chat_id'],
            text=message_text,
            reply_markup=reply_markup,
            message_thread_id=session['message_thread_id']
        )
        session['message_id'] = msg.message_id
    
    context.bot_data['PYRAMID_SESSIONS'] = PYRAMID_SESSIONS

# ==================== TOWER ====================
async def tower_cell_click(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, level, col):
    query = update.callback_query
    
    # Получаем данные из контекста
    TOWER_SESSIONS = context.bot_data.get('TOWER_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек.")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = TOWER_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if not session.get('message_id'):
        await update_balance_async(user_id, session['bet'])
        del TOWER_SESSIONS[user_id]
        context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
        await safe_answer(query, "⚠️ Игра не была создана. Средства возвращены.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Игра уже завершена")
        return

    if level != session['current_level']:
        await safe_answer(query, "🧐 Не тот уровень!")
        return

    session['last_activity'] = time.time()

    if session['board'][level][col] != 'ㅤ':
        await safe_answer(query, "🧐 Эта клетка уже открыта!")
        return

    if col in session['mines'][level]:
        session['board'][level][col] = '💥'
        session['status'] = 'lost'

        keyboard = []
        for lvl in range(level, -1, -1):
            row = []
            for c in range(5):
                cell = session['board'][lvl][c]
                if cell == '💎':
                    button_text = '💎'
                elif cell == '💥':
                    button_text = '💥'
                elif cell == '💣':
                    button_text = '💣'
                else:
                    if c in session['mines'][lvl]:
                        button_text = '💣'
                    else:
                        button_text = '💼'
                row.append(InlineKeyboardButton(button_text, callback_data=f"tower_dead_{user_id}"))
            keyboard.append(row)

        reply_markup = InlineKeyboardMarkup(keyboard)

        message_text = (
            f"💥 Башня • проигрыш!\n"
            f"•••••••••••\n"
            f"💣 Мин: {session['mines_count']}\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
            f"💼 Пройдено: {level} из {TOWER_MAX_LEVEL}\n\n"
            f"👩‍💻 Hash: {session['hash']}"
        )

        await save_game_hash_async(session['hash'], user_id, 'tower', session['bet'], 'lose')
        await update_user_stats_async(user_id, 0, session['bet'])

        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except:
            pass

        del TOWER_SESSIONS[user_id]
        context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
        await safe_answer(query, "💥 Бомба!")
        return

    session['board'][level][col] = '💎'

    if level + 1 >= TOWER_MAX_LEVEL:
        session['status'] = 'won'

        multipliers = TOWER_MULTIPLIERS[session['mines_count']]
        win_amount = int(session['bet'] * multipliers[level])

        await update_balance_async(user_id, win_amount)
        await save_game_hash_async(session['hash'], user_id, 'tower', session['bet'], 'win')
        await update_user_stats_async(user_id, win_amount, 0)

        keyboard = []
        for lvl in range(level, -1, -1):
            row = []
            for c in range(5):
                cell = session['board'][lvl][c]
                if cell == '💎':
                    button_text = '💎'
                else:
                    if c in session['mines'][lvl]:
                        button_text = '💣'
                    else:
                        button_text = '💼'
                row.append(InlineKeyboardButton(button_text, callback_data=f"tower_dead_{user_id}"))
            keyboard.append(row)

        reply_markup = InlineKeyboardMarkup(keyboard)

        message_text = (
            f"🎉 Башня – победа!✅\n"
            f"••••••••••••••\n"
            f"💣 Мин: {session['mines_count']}\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
            f"> Вы прошли до конца!\n\n"
            f"👩‍💻 Hash: {session['hash']}"
        )

        try:
            await context.bot.edit_message_text(
                message_text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except:
            pass

        del TOWER_SESSIONS[user_id]
        context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
        await safe_answer(query, f"🎉 Победа! +{format_amount(win_amount)}ms¢")
        return

    session['current_level'] += 1
    context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
    await update_tower_board(update, context, user_id)
    await safe_answer(query, f"Мины не оказалось.")

async def tower_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    # Получаем данные из контекста
    TOWER_SESSIONS = context.bot_data.get('TOWER_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек.")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = TOWER_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if not session.get('message_id'):
        await update_balance_async(user_id, session['bet'])
        del TOWER_SESSIONS[user_id]
        context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
        await safe_answer(query, "⚠️ Игра не была создана. Средства возвращены.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Игра уже завершена")
        return

    if session['current_level'] == 0:
        await safe_answer(query, "⚠️ Сначала откройте хотя бы один уровень.")
        return

    session['status'] = 'won'
    current_level = session['current_level']
    multipliers = TOWER_MULTIPLIERS[session['mines_count']]
    win_amount = int(session['bet'] * multipliers[current_level - 1])

    await update_balance_async(user_id, win_amount)
    await save_game_hash_async(session['hash'], user_id, 'tower', session['bet'], 'win')
    await update_user_stats_async(user_id, win_amount, 0)

    keyboard = []
    for lvl in range(current_level - 1, -1, -1):
        row = []
        for c in range(5):
            cell = session['board'][lvl][c]
            if cell == '💎':
                button_text = '💎'
            else:
                if c in session['mines'][lvl]:
                    button_text = '💣'
                else:
                    button_text = '💼'
            row.append(InlineKeyboardButton(button_text, callback_data=f"tower_dead_{user_id}"))
        keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"🎉 Башня – победа!✅\n"
        f"••••••••••••••\n"
        f"💣 Мин: {session['mines_count']}\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"> Вы забрали выигрыш!\n\n"
        f"👩‍💻 Hash: {session['hash']}"
    )

    try:
        await context.bot.edit_message_text(
            message_text,
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            reply_markup=reply_markup
        )
    except:
        pass

    del TOWER_SESSIONS[user_id]
    context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
    await safe_answer(query, f"Выигрыш {format_amount(win_amount)}ms¢")

async def tower_cancel_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    # Получаем данные из контекста
    TOWER_SESSIONS = context.bot_data.get('TOWER_SESSIONS', {})
    LAST_CLICK_TIME = context.bot_data.get('LAST_CLICK_TIME', {})
    COOLDOWN_SECONDS = context.bot_data.get('COOLDOWN_SECONDS', 2)

    cooldown = check_cooldown(user_id, LAST_CLICK_TIME, COOLDOWN_SECONDS)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек.")
        context.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
        return

    session = TOWER_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if not session.get('message_id'):
        await update_balance_async(user_id, session['bet'])
        del TOWER_SESSIONS[user_id]
        context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
        await safe_answer(query, "⚠️ Игра не была создана. Средства возвращены.")
        return

    if session['current_level'] > 0:
        await safe_answer(query, "⚠️ Нельзя отменить игру после первого хода.")
        return

    await update_balance_async(user_id, session['bet'])

    try:
        await context.bot.delete_message(
            chat_id=session['chat_id'],
            message_id=session['message_id']
        )
    except:
        pass

    del TOWER_SESSIONS[user_id]
    context.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
    await safe_answer(query, " Игра отменена, средства возвращены")

async def send_tower_start(update, context, user_id):
    TOWER_SESSIONS = context.bot_data.get('TOWER_SESSIONS', {})
    session = TOWER_SESSIONS.get(user_id)
    if not session:
        return None

    mines_count = session['mines_count']
    bet = session['bet']
    
    multipliers = TOWER_MULTIPLIERS[mines_count]
    next_multiplier = multipliers[1]
    
    message_text = (
        f"🍀 Башня • начни игру!\n"
        f"•••••••••••••••\n"
        f"💣 Мин: {mines_count}\n"
        f"💸 Ставка: {format_amount(bet)}ms¢\n\n"
        f"Следующий уровень: x{next_multiplier:.2f}.\n"
    )

    keyboard = []
    row = []
    for col in range(5):
        row.append(InlineKeyboardButton(
            "ㅤ", 
            callback_data=f"tower_cell_{user_id}_0_{col}"
        ))
    keyboard.append(row)
    keyboard.append([InlineKeyboardButton("❌ Отменить", callback_data=f"tower_cancel_{user_id}")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    msg = await context.bot.send_message(
        chat_id=session['chat_id'],
        text=message_text,
        reply_markup=reply_markup,
        message_thread_id=session['message_thread_id']
    )
    
    return msg

async def update_tower_board(update, context, user_id):
    TOWER_SESSIONS = context.bot_data.get('TOWER_SESSIONS', {})
    session = TOWER_SESSIONS.get(user_id)
    if not session:
        return

    current_level = session['current_level']
    mines_count = session['mines_count']
    bet = session['bet']

    multipliers = TOWER_MULTIPLIERS[mines_count]

    if current_level == 0:
        return

    current_multiplier = multipliers[current_level - 1]
    next_multiplier = multipliers[current_level] if current_level < TOWER_MAX_LEVEL else current_multiplier
    current_win = int(bet * current_multiplier)

    message_text = (
        f"🗼 Башня • игра идёт.\n"
        f"•••••••••••••••\n"
        f"💣 Мин: {mines_count}\n"
        f"💸 Ставка: {format_amount(bet)}ms¢\n"
        f"📊 Выигрыш: x{current_multiplier:.2f} / {format_amount(current_win)}ms¢\n\n"
        f"Следующий уровень: x{next_multiplier:.2f}.\n\n"
    )

    keyboard = []

    row = []
    for col in range(5):
        row.append(InlineKeyboardButton(
            "ㅤ",
            callback_data=f"tower_cell_{user_id}_{current_level}_{col}"
        ))
    keyboard.append(row)

    for level in range(current_level - 1, -1, -1):
        row = []
        for col in range(5):
            cell = session['board'][level][col]
            if cell == '💎':
                button_text = '💎'
            elif cell in ['💣', '💥']:
                button_text = '💣'
            else:
                button_text = "ㅤ"
            row.append(InlineKeyboardButton(button_text, callback_data=f"tower_dead_{user_id}"))
        keyboard.append(row)

    if current_level > 0:
        keyboard.append([InlineKeyboardButton("✅ Забрать награду", callback_data=f"tower_take_{user_id}")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    
    full_text = message_text

    try:
        await context.bot.edit_message_text(
            full_text,
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            reply_markup=reply_markup
        )
    except Exception as e:
        if "Message is not modified" not in str(e):
            logging.error(f"Error updating tower board: {e}")

# ==================== RUSSIAN ROULETTE ====================
async def rr_bullets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!", show_alert=True)
        return
    
    data = query.data
    parts = data.split('_')
    bet_amount = int(parts[2])
    bullets = int(parts[3])
    multiplier = RR_MULTIPLIERS[bullets]
    
    import random
    positions = [0] * 6
    bullet_indices = random.sample(range(6), bullets)
    for idx in bullet_indices:
        positions[idx] = 1
    
    game_id = await create_rr_game_async(user_id, bet_amount, bullets, multiplier, positions)
    
    keyboard = []
    for i in range(0, 6, 2):
        row = [
            InlineKeyboardButton("ㅤ", callback_data=f"rr_cell_{game_id}_{i}"),
            InlineKeyboardButton("ㅤ", callback_data=f"rr_cell_{game_id}_{i+1}")
        ]
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    bullet_emoji = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"][bullets-1]
    
    text = (
        f"🔫 Рус. рулетка • игра начата!\n"
        f"••••••••••••••••\n"
        f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
        f"🔫 Пули: {bullet_emoji}\n"
        f"📈 Коэффициент: x0"
    )
    
    sent_msg = await query.edit_message_text(text, reply_markup=reply_markup)
    
    RR_SESSIONS = context.bot_data.get('RR_SESSIONS', {})
    RR_SESSIONS[user_id] = {
        'game_id': game_id,
        'message_id': sent_msg.message_id,
        'chat_id': update.effective_chat.id,
        'start_time': time.time()
    }
    context.bot_data['RR_SESSIONS'] = RR_SESSIONS
    
    await safe_answer(query, "")

async def rr_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!", show_alert=True)
        return
    
    import re
    match = re.search(r'rr_bullets_(\d+)_', query.message.text)
    if match:
        bet_amount = int(match.group(1))
        await update_balance_async(user_id, bet_amount)
    
    await query.message.delete()
    await safe_answer(query, "❌ Игра отменена")

async def rr_cell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    data = query.data
    
    if data == "rr_dead":
        await safe_answer(query, "❌ Эта клетка уже открыта", show_alert=True)
        return
    
    if data == "rr_finished":
        await safe_answer(query, "❌ Игра уже завершена", show_alert=True)
        return
    
    parts = data.split('_')
    game_id = int(parts[2])
    cell_idx = int(parts[3])
    
    game = await get_rr_game_async(game_id)
    if not game:
        await safe_answer(query, "❌ Игра не найдена", show_alert=True)
        return
    
    if game['user_id'] != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return
    
    if game['status'] != 'active':
        await safe_answer(query, "❌ Игра уже завершена", show_alert=True)
        return
    
    if cell_idx in game['opened']:
        await safe_answer(query, "❌ Эта клетка уже открыта", show_alert=True)
        return
    
    RR_SESSIONS = context.bot_data.get('RR_SESSIONS', {})
    if user_id in RR_SESSIONS:
        RR_SESSIONS[user_id]['start_time'] = time.time()
        context.bot_data['RR_SESSIONS'] = RR_SESSIONS
    
    opened = game['opened'] + [cell_idx]
    await update_rr_game_async(game_id, opened)
    
    game = await get_rr_game_async(game_id)
    
    is_bullet = game['positions'][cell_idx] == 1
    
    bullets = game['bullets']
    opened_count = len(game['opened'])
    current_multiplier = RR_STEP_MULTIPLIERS[bullets][opened_count]
    
    bullet_emoji = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"][bullets-1]
    
    keyboard = []
    for i in range(0, 6, 2):
        row = []
        for j in range(2):
            idx = i + j
            if idx in game['opened']:
                if game['positions'][idx] == 1:
                    button = InlineKeyboardButton("💥", callback_data="rr_dead")
                else:
                    button = InlineKeyboardButton("✅", callback_data="rr_dead")
            else:
                button = InlineKeyboardButton("⬜", callback_data=f"rr_cell_{game_id}_{idx}")
            row.append(button)
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if is_bullet:
        await finish_rr_game_async(game_id, 'lost')
        win_amount = 0
        
        text = (
            f"🔫 Русская рулетка • ПРОИГРЫШ! 💥\n"
            f"••••••••••••••••\n"
            f"💸 Ставка: {format_amount(game['bet'])}ms¢\n"
            f"🔫 Пули: {bullet_emoji}\n"
            f"📈 Открыто ячеек: {opened_count - 1}\n"
            f"💔 Вы проиграли {format_amount(game['bet'])}ms¢"
        )
        
        await query.edit_message_text(text, reply_markup=reply_markup)
        await safe_answer(query, f"💥 БАХ! Вы проиграли {format_amount(game['bet'])}ms¢!", show_alert=True)
        
        await update_user_stats_async(user_id, 0, game['bet'])
        
        if user_id in RR_SESSIONS:
            del RR_SESSIONS[user_id]
            context.bot_data['RR_SESSIONS'] = RR_SESSIONS
        
    else:
        total_cells = 6
        bullets_count = game['bullets']
        empty_cells = total_cells - bullets_count
        opened_safe = len([i for i in game['opened'] if game['positions'][i] == 0])
        
        if opened_safe == empty_cells:
            await finish_rr_game_async(game_id, 'won')
            
            final_multiplier = RR_STEP_MULTIPLIERS[bullets][opened_count]
            win_amount = int(game['bet'] * final_multiplier)
            
            await update_balance_async(user_id, win_amount)
            
            text = (
                f"🔫 Русская рулетка • ПОБЕДА! ✅\n"
                f"••••••••••••••••\n"
                f"💸 Ставка: {format_amount(game['bet'])}ms¢\n"
                f"🔫 Пули: {bullet_emoji}\n"
                f"📈 Коэффициент: x{final_multiplier:.2f}\n"
                f"💰 Выигрыш: {format_amount(win_amount)}ms¢"
            )
            
            dead_keyboard = []
            for i in range(0, 6, 2):
                row = [
                    InlineKeyboardButton("⬜", callback_data="rr_finished"),
                    InlineKeyboardButton("⬜", callback_data="rr_finished")
                ]
                dead_keyboard.append(row)
            dead_reply_markup = InlineKeyboardMarkup(dead_keyboard)
            
            await query.edit_message_text(text, reply_markup=dead_reply_markup)
            await safe_answer(query, f"✅ Вы выиграли {format_amount(win_amount)}ms¢!", show_alert=True)
            
            await update_user_stats_async(user_id, win_amount, 0)
            
            if user_id in RR_SESSIONS:
                del RR_SESSIONS[user_id]
                context.bot_data['RR_SESSIONS'] = RR_SESSIONS
            
        else:
            text = (
                f"🔫 Русская рулетка • игра продолжается!\n"
                f"••••••••••••••••\n"
                f"💸 Ставка: {format_amount(game['bet'])}ms¢\n"
                f"🔫 Пули: {bullet_emoji}\n"
                f"📈 Текущий коэффициент: x{current_multiplier:.2f}\n"
                f"✅ Безопасных ячеек осталось: {empty_cells - opened_safe}"
            )
            
            await query.edit_message_text(text, reply_markup=reply_markup)
            await safe_answer(query, f"✅ Безопасно! +{current_multiplier:.2f}x")

# ==================== DICE ====================
async def dice_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_name = user.full_name
    chat_id = update.effective_chat.id
    
    dice_cooldown = context.bot_data.get('dice_cooldown', {})
    DICE_COOLDOWN_SECONDS = context.bot_data.get('DICE_COOLDOWN_SECONDS', 2)
    
    current_time = time.time()
    if user_id in dice_cooldown:
        time_passed = current_time - dice_cooldown[user_id]
        if time_passed < DICE_COOLDOWN_SECONDS:
            remaining = round(DICE_COOLDOWN_SECONDS - time_passed, 1)
            await safe_answer(query, f"⏳ Подождите {remaining} сек.", show_alert=True)
            return
    
    data = query.data
    game_number = int(data.replace('dice_join_', ''))
    
    games = await get_chat_dice_games_async(chat_id, 'waiting')
    game = next((g for g in games if g['game_number'] == game_number), None)
    
    if not game:
        await safe_answer(query, "❌ Игра не найдена", show_alert=True)
        return
    
    game_id = game['game_id']
    
    players = await get_dice_game_players_async(game_id)
    if any(p['user_id'] == user_id for p in players):
        await safe_answer(query, "🎲 Вы уже играете.", show_alert=True)
        return
    
    if len(players) >= game['max_players']:
        await safe_answer(query, "❌ Мест больше нет", show_alert=True)
        return
    
    db_user = await get_user_async(user_id)
    if db_user['balance'] < game['bet_amount']:
        await safe_answer(query, f"❌ Недостаточно средств. Нужно {format_amount(game['bet_amount'])}ms¢", show_alert=True)
        return
    
    dice_cooldown[user_id] = current_time
    context.bot_data['dice_cooldown'] = dice_cooldown
    
    await update_balance_async(user_id, -game['bet_amount'])
    
    success, player_count = await add_dice_player_async(game_id, user_id, user_name)
    
    if not success:
        await safe_answer(query, "❌ Ошибка при входе в игру", show_alert=True)
        return
    
    players = await get_dice_game_players_async(game_id)
    
    from datetime import datetime
    try:
        expires = datetime.strptime(game['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        expires = datetime.strptime(game['expires_at'], '%Y-%m-%d %H:%M:%S')
    
    now = datetime.now()
    delta = expires - now
    minutes = max(0, delta.seconds // 60)
    seconds = max(0, delta.seconds % 60)
    
    players_text = ""
    for p in players:
        players_text += f"[{p['user_name']}](tg://user?id={p['user_id']})\n"
    
    game_id_display = f"{game['game_number']}.{chat_id % 1000:03d}"
    
    text = (
        f"🎲 Игра в кости #{game_id_display}\n"
        f"💰 Ставка: {format_amount(game['bet_amount'])}ms¢\n\n"
        f"👥 Мест: [{len(players)}/{game['max_players']}]\n"
        f"✅ Игроки:\n"
        f"{players_text}"
        f"\n"
        f"⚠ До автоматической отмены игры: {minutes} мин. {seconds} сек."
    )
    
    if len(players) >= game['max_players']:
        await query.edit_message_text(text, parse_mode='Markdown')
        await safe_answer(query, "✅ Вы зашли в игру!")
        
        await start_dice_game(update, context, game_id, chat_id)
    else:
        keyboard = [
            [
                InlineKeyboardButton("🎲 Играть", callback_data=f"dice_join_{game_number}"),
                InlineKeyboardButton("❌ Отмена", callback_data=f"dice_leave_{game_number}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        await safe_answer(query, "🎲 Вы зашли в игру.")

async def dice_leave_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    chat_id = update.effective_chat.id
    
    dice_cooldown = context.bot_data.get('dice_cooldown', {})
    DICE_COOLDOWN_SECONDS = context.bot_data.get('DICE_COOLDOWN_SECONDS', 2)
    
    current_time = time.time()
    if user_id in dice_cooldown:
        time_passed = current_time - dice_cooldown[user_id]
        if time_passed < DICE_COOLDOWN_SECONDS:
            remaining = round(DICE_COOLDOWN_SECONDS - time_passed, 1)
            await safe_answer(query, f"⏳ Подождите {remaining} сек.", show_alert=True)
            return
    
    data = query.data
    game_number = int(data.replace('dice_leave_', ''))
    
    games = await get_chat_dice_games_async(chat_id, 'waiting')
    game = next((g for g in games if g['game_number'] == game_number), None)
    
    if not game:
        await safe_answer(query, "❌ Игра не найдена", show_alert=True)
        return
    
    game_id = game['game_id']
    
    players = await get_dice_game_players_async(game_id)
    player = next((p for p in players if p['user_id'] == user_id), None)
    
    if not player:
        await safe_answer(query, "🗿 Это не ваша игра!", show_alert=True)
        return
    
    dice_cooldown[user_id] = current_time
    context.bot_data['dice_cooldown'] = dice_cooldown
    
    await update_balance_async(user_id, game['bet_amount'])
    
    remaining = await remove_dice_player_async(game_id, user_id)
    
    if remaining == 0:
        await cancel_dice_game_async(game_id)
        try:
            await query.message.delete()
        except Exception as e:
            logging.error(f"Error deleting dice game message: {e}")
        await safe_answer(query, "🎲 Вы успешно вышли из игры.")
        return
    
    remaining_players = await get_dice_game_players_async(game_id)
    
    from datetime import datetime
    try:
        expires = datetime.strptime(game['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        expires = datetime.strptime(game['expires_at'], '%Y-%m-%d %H:%M:%S')
    
    now = datetime.now()
    delta = expires - now
    minutes = max(0, delta.seconds // 60)
    seconds = max(0, delta.seconds % 60)
    
    players_text = ""
    for p in remaining_players:
        players_text += f"[{p['user_name']}](tg://user?id={p['user_id']})\n"
    
    game_id_display = f"{game['game_number']}.{chat_id % 1000:03d}"
    
    text = (
        f"🎲 Игра в кости #{game_id_display}\n"
        f"💰 Ставка: {format_amount(game['bet_amount'])}ms¢\n\n"
        f"👥 Мест: [{remaining}/{game['max_players']}]\n"
        f"✅ Игроки:\n"
        f"{players_text}"
        f"\n"
        f"⚠ До автоматической отмены игры: {minutes} мин. {seconds} сек."
    )
    
    keyboard = [
        [
            InlineKeyboardButton("Играть", callback_data=f"dice_join_{game_number}"),
            InlineKeyboardButton("Отмена", callback_data=f"dice_leave_{game_number}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    await safe_answer(query, "🎲 Вы успешно вышли из игры.")

async def start_dice_game(update: Update, context: ContextTypes.DEFAULT_TYPE, game_id, chat_id):
    await start_dice_game_async(game_id)
    
    game = await get_dice_game_async(game_id)
    players = await get_dice_game_players_async(game_id)
    
    sorted_players = sorted(players, key=lambda x: x['dice_value'], reverse=True)
    
    top_value = sorted_players[0]['dice_value']
    winners = [p for p in sorted_players if p['dice_value'] == top_value]
    
    total_pot = game['bet_amount'] * len(players)
    
    if len(winners) == 1:
        winner = winners[0]
        win_amount = total_pot
        await update_balance_async(winner['user_id'], win_amount)
        
        result_text = f"💰 Победитель: [{winner['user_name']}](tg://user?id={winner['user_id']}), он забирает весь банк {format_amount(win_amount)}ms¢."
    else:
        win_amount = total_pot // len(winners)
        for w in winners:
            await update_balance_async(w['user_id'], win_amount)
        
        winners_names = ", ".join([f"[{w['user_name']}](tg://user?id={w['user_id']})" for w in winners])
        result_text = f"💰 {winners_names} делят между собой весь банк. Каждый получает: {format_amount(win_amount)}ms¢"
    
    players_text = ""
    for p in sorted_players:
        players_text += f"[{p['user_name']}](tg://user?id={p['user_id']}): {p['dice_value']}\n"
    
    game_id_display = f"{game['game_number']}.{chat_id % 1000:03d}"
    
    text = (
        f"🎲 Игра в кости #{game_id_display}\n\n"
        f"{players_text}\n"
        f"{result_text}"
    )
    
    try:
        await context.bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=game['message_id'],
            parse_mode='Markdown'
        )
    except Exception as e:
        logging.error(f"Error editing dice game message: {e}")
    
    await finish_dice_game_async(game_id, [w['user_id'] for w in winners])

async def check_expired_dice_games(context: ContextTypes.DEFAULT_TYPE):
    expired = await get_expired_dice_games_async()
    
    for game in expired:
        players = await get_dice_game_players_async(game['game_id'])
        for player in players:
            await update_balance_async(player['user_id'], game['bet_amount'])
            
            try:
                await context.bot.send_message(
                    chat_id=player['user_id'],
                    text=(
                        f"Ваша игра в кости была завершена по истечению времени!\n"
                        f"💰 Ваша ставка {format_amount(game['bet_amount'])}ms¢ была возвращена на ваш баланс!"
                    )
                )
            except Exception as e:
                if "Forbidden" not in str(e):
                    logging.error(f"Failed to notify user {player['user_id']}: {e}")
        
        await cancel_dice_game_async(game['game_id'])
        
        try:
            await context.bot.edit_message_text(
                "⌛ Игра в кости отменена по таймауту.",
                chat_id=game['chat_id'],
                message_id=game['message_id']
            )
        except:
            pass

# ==================== COINFALL ====================
async def coinfall_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_name = user.full_name
    chat_id = update.effective_chat.id
    
    game = await get_active_coinfall_async(chat_id)
    if not game:
        await safe_answer(query, "❌ Монетопад не найден", show_alert=True)
        return
    
    if game['status'] != 'waiting':
        await safe_answer(query, "❌ Монетопад уже начался", show_alert=True)
        return
    
    success, player_count = await add_coinfall_player_async(game['id'], user_id, user_name)
    
    if not success:
        await safe_answer(query, "❌ Вы уже участвуете", show_alert=True)
        return
    
    players = await get_coinfall_players_async(game['id'])
    player_names = [p['user_name'] for p in players]
    
    participants_text = "👥 Участники: " + ", ".join(player_names)
    
    formatted_prize = format_amount(game['prize'])
    
    text = (
        f"🪙 Монетопад запущен!\n\n"
        f"💸 Приз – {formatted_prize}ms¢.\n\n"
        f"{participants_text}\n\n"
    )
    
    if player_count >= game['max_players']:
        text += f"✅ Набрано максимальное количество участников! Админ может запустить монетопад."
        keyboard = [
            [InlineKeyboardButton("🥇 Участвовать", callback_data="coinfall_join_disabled")],
            [InlineKeyboardButton("🪙 Запустить", callback_data="coinfall_start")]
        ]
    else:
        text += f"Монетопад начнется тогда, когда достигнется максимальное количество участников и администратор запустит.\n"
        text += f"ℹ️ Чтобы вступить нажми кнопку ниже."
        keyboard = [[InlineKeyboardButton("🥇 Участвовать", callback_data="coinfall_join")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup)
    await safe_answer(query, f"✅ Вы стали участником монетопада!")

async def coinfall_join_disabled_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer(query, "❌ Участники уже набраны", show_alert=True)

async def coinfall_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = update.effective_chat.id
    
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ Вы не администратор.", show_alert=True)
        return
    
    game = await get_active_coinfall_async(chat_id)
    if not game or game['status'] != 'waiting':
        await safe_answer(query, "❌ Монетопад не найден или уже начат", show_alert=True)
        return
    
    players = await get_coinfall_players_async(game['id'])
    if len(players) < game['max_players']:
        await safe_answer(query, f"❌ Нужно еще {game['max_players'] - len(players)} участников", show_alert=True)
        return
    
    await start_coinfall_async(game['id'])
    
    await query.edit_message_text("🎉 Разыгрываем....")
    
    await context.bot.send_message(chat_id=chat_id, text="🪙")
    
    await asyncio.sleep(4)
    
    winner = random.choice(players)
    winner_id = winner['user_id']
    winner_name = winner['user_name']
    
    await finish_coinfall_async(game['id'], winner_id, winner_name)
    
    formatted_prize = format_amount(game['prize'])
    
    text = (
        f"🪙 Монетопад окончен!\n\n"
        f"Победитель — {winner_name}.\n\n"
        f"ℹ️ {winner_name}, чтобы забрать выигрыш нажми кнопку ниже."
    )
    
    keyboard = [[InlineKeyboardButton(f"✅ Забрать {formatted_prize}ms¢", callback_data=f"coinfall_claim_{game['id']}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    await safe_answer(query, "")

async def coinfall_claim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    game_id = int(data.replace('coinfall_claim_', ''))
    
    game = await get_coinfall_async(game_id)
    if not game or game['status'] != 'finished':
        await safe_answer(query, "❌ Монетопад не найден", show_alert=True)
        return
    
    if game['claimed'] == 1:
        await safe_answer(query, "❌ Вы уже забирали данную награду!", show_alert=True)
        return
    
    if game['winner_id'] != user_id:
        await safe_answer(query, "❌ Это не ваша кнопка!", show_alert=True)
        return
    
    success, prize = await claim_coinfall_async(game_id, user_id)
    
    if success:
        await update_balance_async(user_id, prize)
        
        formatted_prize = format_amount(prize)
        keyboard = [[InlineKeyboardButton(f"✅ Получено", callback_data="coinfall_claimed")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_reply_markup(reply_markup=reply_markup)
        await safe_answer(query, f"✅ Вы получили {formatted_prize}ms¢!", show_alert=True)
    else:
        await safe_answer(query, "❌ Ошибка при получении", show_alert=True)

async def coinfall_claimed_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer(query, "✅ Награда уже получена", show_alert=True)

# ==================== KNB ====================
KHB_GAMES = {}
KHB_DUELS = {}

async def knb_choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, game_id, choice):
    query = update.callback_query
    user_id = query.from_user.id

    game = KHB_GAMES.get(game_id)
    if not game:
        await safe_answer(query, "❌ Игра не найдена.")
        return

    if game['type'] != 'bot':
        return

    if user_id != game['user1_id']:
        await safe_answer(query, "🗿 Это не ваша игра!")
        return

    if game['status'] == 'waiting_bot':
        await safe_answer(query, "⏱ Сейчас не ваш ход.")
        return

    if game['status'] != 'waiting_user':
        await safe_answer(query, "❌ Игра уже завершена.")
        return

    if game['user1_choice'] is not None:
        await safe_answer(query, "❌ Вы уже сделали ход.")
        return

    game['user1_choice'] = choice

    if choice == game['user2_choice']:
        win_amount = game['bet']
        await update_balance_async(user_id, game['bet'])
        await update_user_stats_async(user_id, 0, 0)
        quote = random.choice(KHB_DRAW_MESSAGES)
        winner_text = "Ничья! 🤝"
    elif (choice == 'камень' and game['user2_choice'] == 'ножницы') or \
         (choice == 'ножницы' and game['user2_choice'] == 'бумага') or \
         (choice == 'бумага' and game['user2_choice'] == 'камень'):
        win_amount = game['bet'] * 2
        await update_balance_async(user_id, win_amount)
        await update_user_stats_async(user_id, win_amount, 0)
        quote = random.choice(KHB_WIN_MESSAGES)
        winner_text = f"{game['user1_name']} победил! 🎉"
    else:
        win_amount = 0
        await update_user_stats_async(user_id, 0, game['bet'])
        quote = random.choice(KHB_LOSE_MESSAGES)
        winner_text = "Бот победил! 🤖"

    game['status'] = 'finished'
    user_choice_emoji = KHB_EMOJIS[game['user1_choice']]
    bot_choice_emoji = KHB_EMOJIS[game['user2_choice']]

    total_bank = game['bet'] * 2

    try:
        await query.edit_message_text(
            f"⏱ {game['user1_name']}, игра окончена! • Камень-Ножницы-Бумага\n"
            f"•••••••••••\n"
            f"🤖 Бот — {bot_choice_emoji}\n"
            f"👤 {game['user1_name']} — {user_choice_emoji}\n\n"
            f"🏦 Банк в размере {format_amount(total_bank)}ms¢ забирает {winner_text}\n\n"
            f"{quote}"
        )
    except Exception as e:
        logging.error(f"Error in knb_choice_handler: {e}")

    await safe_answer(query, "")

async def knb_pvp_choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, game_id, choice):
    query = update.callback_query
    user_id = query.from_user.id

    game = KHB_GAMES.get(game_id)
    if not game:
        await safe_answer(query, "❌ Игра не найдена.")
        return

    if game['type'] != 'pvp':
        return

    if user_id != game['user1_id'] and user_id != game['user2_id']:
        await safe_answer(query, "🗿 Это не ваша игра!")
        return

    if game['turn'] != user_id:
        await safe_answer(query, "⏱ Сейчас не ваш ход.")
        return

    try:
        if user_id == game['user1_id']:
            if game['user1_choice'] is not None:
                await safe_answer(query, "❌ Вы уже сделали ход.")
                return
                
            game['user1_choice'] = choice
            game['turn'] = game['user2_id']

            keyboard = [
                [
                    InlineKeyboardButton(f"Камень {KHB_EMOJIS['камень']}", callback_data=f"knb:pvp:{game_id}:камень"),
                    InlineKeyboardButton(f"Ножницы {KHB_EMOJIS['ножницы']}", callback_data=f"knb:pvp:{game_id}:ножницы"),
                    InlineKeyboardButton(f"Бумага {KHB_EMOJIS['бумага']}", callback_data=f"knb:pvp:{game_id}:бумага")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"🪨 {game['user2_name']}, игра начата! • Камень-Ножницы-Бумага\n"
                f"•••••••••••\n"
                f"Сейчас ходит — {game['user2_name']}. Ожидаем ход..\n\n"
                f"👤 {game['user1_name']} — ♟️ Ход сделан\n"
                f"👤 {game['user2_name']} — *ваш ход*\n\n"
                f"💸 Ставка: {format_amount(game['bet'])}ms¢.",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )

            await safe_answer(query, "✅ Ход принят!")

        elif user_id == game['user2_id']:
            if game['user2_choice'] is not None:
                await safe_answer(query, "❌ Вы уже сделали ход.")
                return
                
            if game['user1_choice'] is None:
                await safe_answer(query, "⏱ Ожидаем ход первого игрока.")
                return
                
            game['user2_choice'] = choice

            if game['user1_choice'] == game['user2_choice']:
                await update_balance_async(game['user1_id'], game['bet'])
                await update_balance_async(game['user2_id'], game['bet'])
                await update_user_stats_async(game['user1_id'], 0, 0)
                await update_user_stats_async(game['user2_id'], 0, 0)
                quote = random.choice(KHB_DRAW_MESSAGES)
                winner_text = "Ничья! 🤝"
            elif (game['user1_choice'] == 'камень' and game['user2_choice'] == 'ножницы') or \
                 (game['user1_choice'] == 'ножницы' and game['user2_choice'] == 'бумага') or \
                 (game['user1_choice'] == 'бумага' and game['user2_choice'] == 'камень'):
                win_amount = game['bet'] * 2
                await update_balance_async(game['user1_id'], win_amount)
                await update_user_stats_async(game['user1_id'], win_amount, 0)
                await update_user_stats_async(game['user2_id'], 0, game['bet'])
                quote = random.choice(KHB_WIN_MESSAGES)
                winner_text = f"{game['user1_name']} победил! 🎉"
            else:
                win_amount = game['bet'] * 2
                await update_balance_async(game['user2_id'], win_amount)
                await update_user_stats_async(game['user2_id'], win_amount, 0)
                await update_user_stats_async(game['user1_id'], 0, game['bet'])
                quote = random.choice(KHB_LOSE_MESSAGES)
                winner_text = f"{game['user2_name']} победил! 🎉"

            game['status'] = 'finished'

            if game['user1_choice'] in KHB_EMOJIS and game['user2_choice'] in KHB_EMOJIS:
                user1_emoji = KHB_EMOJIS[game['user1_choice']]
                user2_emoji = KHB_EMOJIS[game['user2_choice']]
            else:
                user1_emoji = "❓"
                user2_emoji = "❓"

            total_bank = game['bet'] * 2

            await query.edit_message_text(
                f"⏱ {game['user1_name']} и {game['user2_name']}, игра окончена! • Камень-Ножницы-Бумага\n"
                f"•••••••••••\n"
                f"👤 {game['user1_name']} — {user1_emoji}\n"
                f"👤 {game['user2_name']} — {user2_emoji}\n\n"
                f"🏦 Банк в размере {format_amount(total_bank)}ms¢ забирает {winner_text}\n\n"
                f"{quote}"
            )
            
            await safe_answer(query, "")

    except Exception as e:
        logging.error(f"❌ Error in knb_pvp_choice_handler: {e}", exc_info=True)
        await safe_answer(query, f"❌ Ошибка: {str(e)[:50]}")

async def knb_accept_duel(update: Update, context: ContextTypes.DEFAULT_TYPE, duel_id):
    query = update.callback_query
    user_id = query.from_user.id

    duel = KHB_DUELS.get(duel_id)
    if not duel or duel['status'] != 'active':
        await safe_answer(query, "❌ Вызов уже неактивен.")
        return

    if user_id != duel['opponent_id']:
        await safe_answer(query, "🗿 Не ваш вызов!")
        return

    opponent = await get_user_async(user_id, query.from_user.full_name, query.from_user.username)
    if opponent['balance'] < duel['bet']:
        await safe_answer(query, f"❌ Недостаточно средств. Ваш баланс: {format_amount(opponent['balance'])}ms¢")
        return

    await update_balance_async(duel['opponent_id'], -duel['bet'])

    duel['status'] = 'accepted'

    game_id = f"{duel['challenger_id']}_{duel['opponent_id']}_{int(time.time())}"
    KHB_GAMES[game_id] = {
        'type': 'pvp',
        'user1_id': duel['challenger_id'],
        'user1_name': duel['challenger_name'],
        'user2_id': duel['opponent_id'],
        'user2_name': duel['opponent_name'],
        'bet': duel['bet'],
        'message_id': None,
        'chat_id': update.effective_chat.id,
        'status': 'waiting_user1',
        'user1_choice': None,
        'user2_choice': None,
        'turn': duel['challenger_id']
    }

    keyboard = [
        [
            InlineKeyboardButton(f"Камень {KHB_EMOJIS['камень']}", callback_data=f"knb:pvp:{game_id}:камень"),
            InlineKeyboardButton(f"Ножницы {KHB_EMOJIS['ножницы']}", callback_data=f"knb:pvp:{game_id}:ножницы"),
            InlineKeyboardButton(f"Бумага {KHB_EMOJIS['бумага']}", callback_data=f"knb:pvp:{game_id}:бумага")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if duel_id in KHB_DUELS:
        del KHB_DUELS[duel_id]

    try:
        await query.edit_message_text(
            f"🪨 {duel['challenger_name']}, игра начата! • Камень-Ножницы-Бумага\n"
            f"•••••••••••\n"
            f"Сейчас ходит — {duel['challenger_name']}. Ожидаем ход..\n\n"
            f"👤 {duel['challenger_name']} — *ваш ход*\n"
            f"👤 {duel['opponent_name']} — *ждёт хода*\n\n"
            f"💸 Ставка: {format_amount(duel['bet'])}ms¢.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        logging.error(f"Ошибка при создании игры: {e}")

    await safe_answer(query, "✅ Вызов принят! Ожидаем ход создателя.")

async def knb_cancel_duel(update: Update, context: ContextTypes.DEFAULT_TYPE, duel_id):
    query = update.callback_query
    user_id = query.from_user.id

    duel = KHB_DUELS.get(duel_id)
    if not duel or duel['status'] != 'active':
        await safe_answer(query, "❌ Вызов уже неактивен.")
        return

    if user_id != duel['challenger_id']:
        await safe_answer(query, "🗿 Только создатель вызова может его отменить.")
        return
        
    await update_balance_async(duel['challenger_id'], duel['bet'])

    duel['status'] = 'cancelled'

    keyboard = [[InlineKeyboardButton("❌ Вызов отменён", callback_data="noop")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(
            text=(
                f"🔫 {duel['opponent_name']}, вызов на дуэль \"КНБ\" отменён\n"
                f"Вызов от {duel['challenger_name']}\n\n"
                f"🙈 Вызов - отменён\n\n"
                f"💸 Ставка: {format_amount(duel['bet'])}ms¢.\n\n"
                f"❌ Вызов отменён создателем. Средства возвращены."
            ),
            reply_markup=reply_markup
        )
    except Exception as e:
        logging.error(f"Ошибка при отмене вызова: {e}")

    await safe_answer(query, "✅ Вызов отменён, средства возвращены")

# ==================== BOWLING ====================
async def bowling_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в боулинг"""
    if not update.effective_user:
        return

    user = update.effective_user
    user_id = user.id
    user_full_name = user.full_name

    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    if user_id not in ADMIN_IDS:
        from .common import check_subscription
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    # Проверка на активные сессии
    MINES_SESSIONS = context.bot_data.get('MINES_SESSIONS', {})
    GOLD_SESSIONS = context.bot_data.get('GOLD_SESSIONS', {})
    PYRAMID_SESSIONS = context.bot_data.get('PYRAMID_SESSIONS', {})
    BOWLING_SESSIONS = context.bot_data.get('BOWLING_SESSIONS', {})

    if user_id in MINES_SESSIONS:
        session = MINES_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del MINES_SESSIONS[user_id]
            context.bot_data['MINES_SESSIONS'] = MINES_SESSIONS
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Мины. Завершите её сначала.")
            return

    if user_id in GOLD_SESSIONS:
        session = GOLD_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del GOLD_SESSIONS[user_id]
            context.bot_data['GOLD_SESSIONS'] = GOLD_SESSIONS
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Золото. Завершите её сначала.")
            return

    if user_id in PYRAMID_SESSIONS:
        session = PYRAMID_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del PYRAMID_SESSIONS[user_id]
            context.bot_data['PYRAMID_SESSIONS'] = PYRAMID_SESSIONS
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Пирамида. Завершите её сначала.")
            return

    if user_id in BOWLING_SESSIONS:
        session = BOWLING_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del BOWLING_SESSIONS[user_id]
            context.bot_data['BOWLING_SESSIONS'] = BOWLING_SESSIONS
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Боулинг. Завершите её сначала.")
            return

    text = update.message.text.strip()
    args = []

    if text.startswith('/bowling'):
        args = context.args if context.args else []
    elif text.lower().startswith('бо') or text.lower().startswith('боулинг'):
        parts = text.lower().split()
        if len(parts) > 1:
            args = parts[1:]

    if len(args) < 2:
        await update.message.reply_text(
            "ℹ️ *Боулинг* – это игра, в которой вам нужно сбить кегли, чтобы получить максимальный множитель\n\n"
            f"🤖 {user_full_name}, чтобы начать игру, используй команду:\n\n"
            "🎳 `/bowling [кегель] [ставка]`\n\n"
            "📊 *Коэффициенты:*\n"
            "• 1-4 кегли — x2.9\n"
            "• Страйк (5 кеглей) — x3.5\n"
            "• Мимо — x3.2\n\n"
            "📝 *Примеры:*\n"
            "• `/bowling 2 100`\n"
            "• `бо страйк 100`\n"
            "• `бо мимо 100`\n"
            "• `бо 5 100` (страйк)",
            parse_mode='Markdown'
        )
        return

    pins_input = args[0].lower()
    bet_amount = parse_amount(args[1])

    if bet_amount <= 0:
        await update.message.reply_text("❌ Неверная сумма ставки.")
        return

    if pins_input in ['страйк', '5']:
        pins = 5
    elif pins_input == 'мимо':
        pins = 0
    else:
        try:
            pins = int(pins_input)
            if pins < 1 or pins > 4:
                await update.message.reply_text("❌ Количество кеглей должно быть от 1 до 4 (или 'страйк'/'мимо').")
                return
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Используйте число от 1 до 4, 'страйк' или 'мимо'.")
            return

    db_user = await get_user_async(user_id, user.full_name, user.username)
    success = await update_balance_safe_async(user_id, -bet_amount, bet_amount)
    if not success:
        await update.message.reply_text("❌ Недостаточно средств на балансе.")
        return

    game_hash = generate_game_hash({
        'user_id': user_id,
        'game': 'bowling',
        'bet': bet_amount,
        'pins': pins,
        'full_name': user_full_name
    })

    msg = await context.bot.send_dice(
        chat_id=update.effective_chat.id,
        emoji='🎳',
        message_thread_id=update.effective_message.message_thread_id
    )
    
    result_pins = msg.dice.value
    
    if result_pins == 6:
        result_pins = 5
    
    if pins == 5:
        if result_pins == 5:
            multiplier = BOWLING_MULTIPLIERS['strike']
        else:
            multiplier = BOWLING_MULTIPLIERS[result_pins] if result_pins > 0 else BOWLING_MULTIPLIERS['miss']
    elif pins == 0:
        if result_pins == 0:
            multiplier = BOWLING_MULTIPLIERS['miss']
        else:
            multiplier = BOWLING_MULTIPLIERS[result_pins] if result_pins > 0 else BOWLING_MULTIPLIERS['miss']
    else:
        if result_pins == pins:
            multiplier = BOWLING_MULTIPLIERS[pins]
        elif result_pins == 5:
            multiplier = BOWLING_MULTIPLIERS['strike']
        elif result_pins == 0:
            multiplier = BOWLING_MULTIPLIERS['miss']
        else:
            multiplier = BOWLING_MULTIPLIERS[result_pins]
    
    win_amount = int(bet_amount * multiplier)
    
    if pins == 5:
        is_win = result_pins == 5
    elif pins == 0:
        is_win = result_pins == 0
    else:
        is_win = result_pins == pins
    
    await asyncio.sleep(3.5)
    
    if is_win:
        await update_balance_async(user_id, win_amount)
        await save_game_hash_async(game_hash, user_id, 'bowling', bet_amount, 'win')
        await update_user_stats_async(user_id, win_amount, 0)
        
        if pins == 5:
            choice_display = "страйк"
        elif pins == 0:
            choice_display = "мимо"
        else:
            choice_display = f"{BOWLING_NUMBERS[pins]} {BOWLING_WORDS[pins]}"
        
        if result_pins == 5:
            result_display = "страйк"
        elif result_pins == 0:
            result_display = "мимо"
        else:
            result_display = f"{BOWLING_NUMBERS[result_pins]} {BOWLING_WORDS[result_pins]}"
        
        message_text = (
            f"*{user_full_name}*\n"
            f"🎉 Боулинг - Победа!✅\n"
            f"•••••••\n"
            f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
            f"🎲 Выбрано: {choice_display}\n"
            f"💰 Выигрыш: x{multiplier} / {format_amount(win_amount)}ms¢\n"
            f"••••••••\n"
            f"⚡️ Итог: {result_display}"
        )
    else:
        await save_game_hash_async(game_hash, user_id, 'bowling', bet_amount, 'lose')
        await update_user_stats_async(user_id, 0, bet_amount)
        
        if pins == 5:
            choice_display = "страйк"
        elif pins == 0:
            choice_display = "мимо"
        else:
            choice_display = f"{BOWLING_NUMBERS[pins]} {BOWLING_WORDS[pins]}"
        
        if result_pins == 5:
            result_display = "страйк"
        elif result_pins == 0:
            result_display = "мимо"
        else:
            result_display = f"{BOWLING_NUMBERS[result_pins]} {BOWLING_WORDS[result_pins]}"
        
        message_text = (
            f" *{user_full_name}*\n"
            f"😵‍💫 Боулинг - Проигрыш!\n"
            f"•••••••••••\n"
            f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
            f"🎲 Выбрано: {choice_display}\n"
            f"••••••\n"
            f"⚡️ Итог: {result_display}"
        )
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=message_text,
        parse_mode='Markdown',
        message_thread_id=update.effective_message.message_thread_id
    )

async def update_task_progress_for_game(user_id, game_type, increment=1):
    """Обновить прогресс заданий для конкретной игры"""
    tasks = await get_all_spring_tasks_async()
    
    for task in tasks:
        if task['game_type'] == game_type or task['game_type'] is None:
            await update_user_task_progress_async(user_id, task['id'], increment)

# ==================== CALLBACK HANDLERS (EXPORT) ====================
async def handle_mines_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    query = update.callback_query
    
    if data.startswith("mines_cell_"):
        parts = data.split('_')
        if len(parts) >= 5:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                r = int(parts[3])
                c = int(parts[4])
                await mines_cell_click(update, context, target_id, r, c)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing mines cell data: {e}")
    
    elif data.startswith("mines_take_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await mines_take_win(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing mines take data: {e}")
    
    elif data.startswith("mines_cancel_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await mines_cancel_game(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing mines cancel data: {e}")
    
    elif data.startswith("mines_dead_"):
        await safe_answer(query, "🙈 Вообще-то игра завершена)")

async def handle_gold_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    query = update.callback_query
    
    if data.startswith("gold_left_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await gold_choice(update, context, target_id, 'left')
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing gold left data: {e}")
    
    elif data.startswith("gold_right_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await gold_choice(update, context, target_id, 'right')
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing gold right data: {e}")
    
    elif data.startswith("gold_take_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await gold_take_win(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing gold take data: {e}")
    
    elif data.startswith("gold_dead_"):
        await safe_answer(query, "🙈 Вообще-то игра завершена)")

async def handle_pyramid_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    query = update.callback_query
    
    if data.startswith("pyr_"):
        parts = data.split('_')
        if len(parts) >= 5:
            try:
                target_id = int(parts[1])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                level = int(parts[2])
                door = int(parts[3])
                await pyramid_cell_click(update, context, target_id, level, door)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing pyramid data: {e}")
                await safe_answer(query, "❌ Ошибка обработки")
    
    elif data.startswith("take_"):
        parts = data.split('_')
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await pyramid_take_win(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing take data: {e}")
    
    elif data.startswith("cancel_"):
        parts = data.split('_')
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await pyramid_cancel_game(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing cancel data: {e}")

async def handle_tower_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    query = update.callback_query
    
    if data.startswith("tower_cell_"):
        parts = data.split('_')
        if len(parts) >= 5:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                level = int(parts[3])
                col = int(parts[4])
                await tower_cell_click(update, context, target_id, level, col)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing tower cell data: {e}")
    
    elif data.startswith("tower_take_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await tower_take_win(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing tower take data: {e}")
    
    elif data.startswith("tower_cancel_"):
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                target_id = int(parts[2])
                if query.from_user.id != target_id:
                    await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
                    return
                await tower_cancel_game(update, context, target_id)
            except (ValueError, IndexError) as e:
                logging.error(f"Error parsing tower cancel data: {e}")
    
    elif data.startswith("tower_dead_"):
        await safe_answer(query, "🙈 Этот ряд пройден")

async def handle_rr_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    if data.startswith("rr_bullets_"):
        await rr_bullets_callback(update, context)
    elif data == "rr_cancel":
        await rr_cancel_callback(update, context)
    elif data.startswith("rr_cell_"):
        await rr_cell_callback(update, context)

async def handle_dice_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    if data.startswith("dice_join_"):
        await dice_join_callback(update, context)
    elif data.startswith("dice_leave_"):
        await dice_leave_callback(update, context)

async def handle_coinfall_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    if data == "coinfall_join":
        await coinfall_join_callback(update, context)
    elif data == "coinfall_join_disabled":
        await coinfall_join_disabled_callback(update, context)
    elif data == "coinfall_start":
        await coinfall_start_callback(update, context)
    elif data.startswith("coinfall_claim_"):
        await coinfall_claim_callback(update, context)
    elif data == "coinfall_claimed":
        await coinfall_claimed_callback(update, context)

async def handle_knb_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    if data.startswith("knb:choice:"):
        parts = data.split(':')
        if len(parts) >= 4:
            game_id = parts[2]
            choice = parts[3]
            await knb_choice_handler(update, context, game_id, choice)
    elif data.startswith("knb:pvp:"):
        parts = data.split(':')
        if len(parts) >= 4:
            game_id = parts[2]
            choice = parts[3]
            await knb_pvp_choice_handler(update, context, game_id, choice)
    elif data.startswith("knb_accept_"):
        duel_id = data.replace("knb_accept_", "")
        await knb_accept_duel(update, context, duel_id)
    elif data.startswith("knb_cancel_"):
        duel_id = data.replace("knb_cancel_", "")
        await knb_cancel_duel(update, context, duel_id)

async def crash_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Краш"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    # Проверка подписки для обычных пользователей
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    if user_id not in ADMIN_IDS:
        from .common import check_subscription
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    # Получаем текст сообщения
    text = update.message.text.strip()
    parts = text.split()
    
    # Проверяем различные варианты команд
    cmd = parts[0].lower()
    if cmd not in ['краш', '/crash', 'к']:
        return
    
    # Если нет аргументов - показываем справку
    if len(parts) < 3:
        await update.message.reply_text(
            "<blockquote>ℹ️ Краш – это игра, в которой вам нужно выбрать множитель от x1.01 до x100.000. Бот случайным образом останавливается на значении от x1 до x100.000</blockquote>\n\n"
            f"🤖 <b>{user_name}</b>, чтобы начать игру, используй команду:\n\n"
            "📈 <b><u>/crash [ставка] [1.01-20.00]</u></b>\n\n"
            "Пример:\n"
            "<code>/crash 100 1.1</code>\n"
            "<code>/краш 100 1.1</code>",
            parse_mode='HTML'
        )
        return
    
    try:

        db_user = await get_user_async(user_id, user.full_name, user.username)
        user_balance = db_user['balance']
        # Парсим ставку и множитель
        bet_amount = parse_bet_amount(parts[1], user_balance)
        target_multiplier = float(parts[2].replace(',', '.'))
        
        # Проверяем корректность
        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return
        
        if target_multiplier < 1.01 or target_multiplier > 100000:
            await update.message.reply_text("❌ Множитель должен быть от 1.01 до 20")
            return
        
        # Проверяем баланс
        if db_user['balance'] < bet_amount:
            await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(db_user['balance'])}ms¢")
            return
        
        # Списываем ставку
        await update_balance_async(user_id, -bet_amount)
        
        # Генерируем результат
        crash_multiplier = generate_crash_multiplier()
        
        # Определяем победу или проигрыш
        is_win = crash_multiplier >= target_multiplier
        
        # Рассчитываем выигрыш (если игрок выиграл)
        if is_win:
            win_amount = int(bet_amount * target_multiplier)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)
            
            message_text = (
                f"<blockquote><b><tg-emoji emoji-id='5283080528818360566'>🚀</tg-emoji> Ракета упала на x{crash_multiplier} <tg-emoji emoji-id='5244837092042750681'>📈</tg-emoji> </b>\n\n"
                f"<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji> Ты выиграл! Твой выигрыш составил {format_amount(win_amount)}ms¢</blockquote>"
            )
        else:
            await update_user_stats_async(user_id, 0, bet_amount)

            # Формируем сообщение о проигрыше
            message_text = (
                f"<blockquote><b><tg-emoji emoji-id='5283080528818360566'>🚀</tg-emoji> Ракета упала на x{crash_multiplier} <tg-emoji emoji-id='5246762912428603768'>📉</tg-emoji> </b>\n\n"
                f"<tg-emoji emoji-id='5210952531676504517'>❌</tg-emoji> Ты проиграл {format_amount(bet_amount)}ms¢</blockquote>"
            )
        
        # Сохраняем хеш игры (опционально)
        game_hash = generate_game_hash({
            'user_id': user_id,
            'game': 'crash',
            'bet': bet_amount,
            'target': target_multiplier,
            'result': crash_multiplier,
            'win': is_win
        })
        await save_game_hash_async(game_hash, user_id, 'crash', bet_amount, 'win' if is_win else 'lose')
        
        # Отправляем результат
        await update.message.reply_text(
            message_text,
            parse_mode='HTML'
        )
        
    except ValueError:
        await update.message.reply_text("❌ Неверный формат множителя. Используйте число (например: 1.5, 2.0, 10.5)")
    except RetryAfter as e:
        retry_after = e.retry_after
        await update.message.reply_text(f"⏳ Flood Control, ожидай {retry_after} сек.")
        logging.warning(f"Flood control in crash command for user {user_id}: wait {retry_after}s")
        await asyncio.sleep(retry_after)
    except Exception as e:
        logging.error(f"Error in crash command: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")
        if 'bet_amount' in locals():
            await update_balance_async(user_id, bet_amount)

DICE_SESSIONS = {}
DICE_MULTIPLIERS = {
    'number': 5.8,
    'even': 1.94,
    'odd': 1.94,
    'big': 1.94,
    'small': 2.9,
    'equal': 5.8
}

DICE_NUMBERS = {
    1: "1️⃣",
    2: "2️⃣",
    3: "3️⃣",
    4: "4️⃣",
    5: "5️⃣",
    6: "6️⃣"
}

async def cubic_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Кубик"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    # Проверка подписки
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    text = update.message.text.strip()
    parts = text.split()
    
    # Если нет аргументов или только команда
    if len(parts) == 1 or (len(parts) == 2 and parts[1].lower() in ['куб', 'dice']):
        await update.message.reply_text(
            "<blockquote>ℹ️ Кубик – это игра, в которой нужно угадать число на кубике или сделать ставку на то, будет ли оно чётным или нечетным, а также больше 3 или меньше 3.</blockquote>\n\n"
            f"🤖 <b>{user_name}</b>, чтобы начать игру, используй команду:\n\n"
            "<tg-emoji emoji-id='5350314303352223876'>🎲</tg-emoji> /dice [ставка]\n\n"
            "Пример:\n"
            "Куб 100\n"
            "/dice 100",
            parse_mode='HTML'
        )
        return

    # Парсим ставку
    db_user = await get_user_async(user_id, user.full_name, user.username)
    bet_amount = parse_bet_amount(parts[1], db_user['balance'])
    
    if bet_amount <= 0:
        await update.message.reply_text("Неверная сумма ставки.")
        return

    # Проверяем баланс
    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f" Недостаточно средств. Баланс: {format_amount(db_user['balance'])}ms¢")
        return

    # Списываем ставку
    await update_balance_async(user_id, -bet_amount)

    # Создаем сессию
    DICE_SESSIONS = context.bot_data.get('DICE_SESSIONS', {})
    DICE_SESSIONS[user_id] = {
        'bet': bet_amount,
        'status': 'waiting',
        'choice': None,
        'choice_type': None,
        'choice_display': None,
        'multiplier': None,
        'message_id': None,
        'chat_id': update.effective_chat.id,
        'thread_id': update.effective_message.message_thread_id,
        'user_name': user_name
    }
    context.bot_data['DICE_SESSIONS'] = DICE_SESSIONS

    # Отправляем сообщение с выбором
    await send_dice_choice(update, context, user_id, user_name, bet_amount)

async def send_dice_choice(update, context, user_id, user_name, bet_amount):
    """Отправка сообщения с выбором исхода"""
    formatted_bet = format_amount(bet_amount)
    
    text = (
        f"🎲 <b>{user_name}*</b>, выберите исход:\n\n"
        f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji> Ставка: {formatted_bet}ms¢"
    )

    keyboard = [
        [
            InlineKeyboardButton("1️⃣", callback_data=f"dice_num_{user_id}_1"),
            InlineKeyboardButton("2️⃣", callback_data=f"dice_num_{user_id}_2"),
            InlineKeyboardButton("3️⃣", callback_data=f"dice_num_{user_id}_3")
        ],
        [
            InlineKeyboardButton("4️⃣", callback_data=f"dice_num_{user_id}_4"),
            InlineKeyboardButton("5️⃣", callback_data=f"dice_num_{user_id}_5"),
            InlineKeyboardButton("6️⃣", callback_data=f"dice_num_{user_id}_6")
        ],
        [
            InlineKeyboardButton("Большие", callback_data=f"dice_big_{user_id}"),
            InlineKeyboardButton("Равно (3)", callback_data=f"dice_equal_{user_id}"),
            InlineKeyboardButton("Малые", callback_data=f"dice_small_{user_id}")
        ],
        [
            InlineKeyboardButton("Чётное", callback_data=f"dice_even_{user_id}"),
            InlineKeyboardButton("Нечётное", callback_data=f"dice_odd_{user_id}")
        ],
        [InlineKeyboardButton("❌ Отменить", callback_data=f"dice_cancel_{user_id}")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    
    msg = await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')
    
    # Сохраняем ID сообщения в сессии
    DICE_SESSIONS = context.bot_data.get('DICE_SESSIONS', {})
    if user_id in DICE_SESSIONS:
        DICE_SESSIONS[user_id]['message_id'] = msg.message_id
        context.bot_data['DICE_SESSIONS'] = DICE_SESSIONS

async def dice_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, choice_type, value=None):
    """Обработчик выбора исхода"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    DICE_SESSIONS = context.bot_data.get('DICE_SESSIONS', {})
    session = DICE_SESSIONS.get(user_id)
    
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.", show_alert=True)
        return

    if session['status'] != 'waiting':
        await safe_answer(query, "🙈 Игра уже начата или завершена", show_alert=True)
        return

    # Определяем выбранный исход
    if choice_type == 'num':
        num = int(value)
        session['choice'] = num
        session['choice_type'] = 'number'
        session['choice_display'] = DICE_NUMBERS[num]
        multiplier = DICE_MULTIPLIERS['number']
    elif choice_type == 'even':
        session['choice_type'] = 'even'
        session['choice_display'] = "Чётное"
        multiplier = DICE_MULTIPLIERS['even']
    elif choice_type == 'odd':
        session['choice_type'] = 'odd'
        session['choice_display'] = "Нечётное"
        multiplier = DICE_MULTIPLIERS['odd']
    elif choice_type == 'big':
        session['choice_type'] = 'big'
        session['choice_display'] = "Большие (4-6)"
        multiplier = DICE_MULTIPLIERS['big']
    elif choice_type == 'small':
        session['choice_type'] = 'small'
        session['choice_display'] = "Малые (1-3)"
        multiplier = DICE_MULTIPLIERS['small']
    elif choice_type == 'equal':
        session['choice_type'] = 'equal'
        session['choice_display'] = "Равно 3"
        multiplier = DICE_MULTIPLIERS['equal']
    else:
        await safe_answer(query, "❌ Неизвестный выбор", show_alert=True)
        return

    session['status'] = 'rolling'
    session['multiplier'] = multiplier
    DICE_SESSIONS[user_id] = session
    context.bot_data['DICE_SESSIONS'] = DICE_SESSIONS

    # Удаляем клавиатуру
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except:
        pass

    await safe_answer(query, "🎲 Бросаем кубик...")

    # Отправляем эмодзи кубика
    dice_msg = await context.bot.send_dice(
        chat_id=session['chat_id'],
        emoji='🎲',
        message_thread_id=session.get('thread_id'),
        reply_to_message_id=session['message_id']
    )

    # Получаем результат
    dice_value = dice_msg.dice.value

    # Запускаем задачу для обработки результата через 4.5 секунды, передавая значение
    asyncio.create_task(
        process_dice_result(
            context, 
            user_id, 
            dice_msg.message_id, 
            session, 
            dice_value  # 👈 передаем значение
        )
    )


async def process_dice_result(context: ContextTypes.DEFAULT_TYPE, user_id: int, dice_msg_id: int, session: dict, dice_value: int):
    """Обработка результата игры в Кубик (запускается как задача)"""
    try:
        # Ждем 4.5 секунды (время анимации кубика)
        await asyncio.sleep(4.5)

        result = dice_value  # используем переданное значение

        # Определяем, выиграл ли игрок
        win = False
        if session['choice_type'] == 'number' and result == session['choice']:
            win = True
        elif session['choice_type'] == 'even' and result % 2 == 0:
            win = True
        elif session['choice_type'] == 'odd' and result % 2 == 1:
            win = True
        elif session['choice_type'] == 'big' and result > 3:
            win = True
        elif session['choice_type'] == 'small' and result <= 3:
            win = True
        elif session['choice_type'] == 'equal' and result == 3:
            win = True

        multiplier = session['multiplier']

        if win:
            win_amount = int(session['bet'] * multiplier)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)

            text = (
                f"🎉 <b>Кубик • Победа!</b><tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji>\n"
                f"•••••••••••••••\n"
                f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji>Ставка: {format_amount(session['bet'])}ms¢\n"
                f"<tg-emoji emoji-id='5350314303352223876'>🎲</tg-emoji>Выбрано: {session['choice_display']}\n"
                f"💰 Выигрыш: x{multiplier} / {format_amount(win_amount)}ms¢\n"
                f"•••••••••\n"
                f"<blockquote><tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji> Выпало: {DICE_NUMBERS[result]}</blockquote>"
            )
        else:
            await update_user_stats_async(user_id, 0, session['bet'])

            text = (
                f"🛑<b> Кубик • Проигрыш!</b>\n"
                f"•••••••••••\n"
                f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji>Ставка: {format_amount(session['bet'])}ms¢\n"
                f"<tg-emoji emoji-id='5350314303352223876'>🎲</tg-emoji>Выбрано: {session['choice_display']}\n"
                f"•••••••\n"
                f"<blockquote><tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji> Выпало: {DICE_NUMBERS[result]}</blockquote>"
            )

        await context.bot.send_message(
            chat_id=session['chat_id'],
            text=text,
            parse_mode='HTML',
            message_thread_id=session.get('thread_id'),
            reply_to_message_id=dice_msg_id
        )

        # Очищаем сессию
        DICE_SESSIONS = context.bot_data.get('DICE_SESSIONS', {})
        if user_id in DICE_SESSIONS:
            del DICE_SESSIONS[user_id]
            context.bot_data['DICE_SESSIONS'] = DICE_SESSIONS

    except Exception as e:
        logging.error(f"Error in process_dice_result: {e}")
        # В случае ошибки возвращаем ставку
        await update_balance_async(user_id, session['bet'])
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text="❌ Произошла ошибка. Ставка возвращена.",
            message_thread_id=session.get('thread_id')
        )

async def dice_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Отмена игры"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!", show_alert=True)
        return

    DICE_SESSIONS = context.bot_data.get('DICE_SESSIONS', {})
    session = DICE_SESSIONS.get(user_id)
    
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.", show_alert=True)
        return

    # Возвращаем ставку
    await update_balance_async(user_id, session['bet'])

    try:
        await query.message.delete()
    except:
        pass

    del DICE_SESSIONS[user_id]
    context.bot_data['DICE_SESSIONS'] = DICE_SESSIONS
    await safe_answer(query, "❌ Игра отменена, средства возвращены")

async def handle_dice_game_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для игры в Кубик"""
    if data.startswith("dice_num_"):
        parts = data.split('_')
        if len(parts) >= 4:
            target_id = int(parts[2])
            num = parts[3]
            await dice_choice_callback(update, context, target_id, 'num', num)
    elif data.startswith("dice_even_"):
        target_id = int(data.split('_')[2])
        await dice_choice_callback(update, context, target_id, 'even')
    elif data.startswith("dice_odd_"):
        target_id = int(data.split('_')[2])
        await dice_choice_callback(update, context, target_id, 'odd')
    elif data.startswith("dice_big_"):
        target_id = int(data.split('_')[2])
        await dice_choice_callback(update, context, target_id, 'big')
    elif data.startswith("dice_small_"):
        target_id = int(data.split('_')[2])
        await dice_choice_callback(update, context, target_id, 'small')
    elif data.startswith("dice_equal_"):
        target_id = int(data.split('_')[2])
        await dice_choice_callback(update, context, target_id, 'equal')
    elif data.startswith("dice_cancel_"):
        target_id = int(data.split('_')[2])
        await dice_cancel_callback(update, context, target_id)

__all__ = [
    'handle_mines_callbacks',
    'handle_gold_callbacks',
    'handle_pyramid_callbacks',
    'handle_tower_callbacks',
    'handle_rr_callbacks',
    'handle_dice_callbacks',
    'handle_coinfall_callbacks',
    'handle_knb_callbacks',
    'handle_dice_game_callbacks',  # это для callback'ов новой игры
    'cubic_command'  # добавляем новую команду
]
