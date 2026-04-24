import logging
import os
import re
import random
import aiosqlite
import math
import time
import hashlib
import json
import asyncio
import secrets
from functools import wraps
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatPermissions, InlineQueryResultArticle, InputTextMessageContent, KeyboardButtonRequestChat
from telegram.error import RetryAfter
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, InlineQueryHandler
from database import (
    init_db, get_user_async, update_balance_async, update_balance_safe_async,
    get_top_users_async, get_user_rank_async, is_user_banned_async,
    ban_user_async, unban_user_async, save_game_hash_async, get_game_hash_async,
    update_user_stats_async, get_user_stats_async, transfer_money_async,
    get_all_users_async, add_referral_async,
    get_top_referrers_async, get_referrer_id_async, create_check_async,
    get_check_async, use_check_async, get_all_checks_async, delete_check_async,
    get_user_referral_count_async, get_referral_rank_async,
    can_claim_bonus_async, claim_bonus_async, get_user_by_username_async,
    get_all_stocks_async, get_stock_async, get_stock_by_symbol_async,
    update_stock_price_async, update_all_stocks_prices_async,
    get_user_portfolio_async, get_user_portfolio_total_async,
    get_user_portfolio_count_async, buy_stock_async, sell_stock_async,
    get_user_stock_quantity_async, clear_all_portfolios_async,
    create_event_async, get_all_events_async, get_event_async,
    update_event_status_async, delete_event_async, can_claim_slot_async,
    create_spring_question_async, get_all_spring_questions_async, get_spring_question_async,
    solve_spring_question_async, get_user_suns_async, add_user_suns_async,
    can_collect_sun_async, collect_sun_async,
    create_spring_task_async, get_all_spring_tasks_async, get_spring_task_async,
    get_user_task_progress_async, update_user_task_progress_async,
    claim_task_reward_async, get_all_user_tasks_async,
    claim_slot_async, create_math_contest_async, get_math_contest_async,
    get_active_math_contest_async, start_math_contest_async, finish_math_contest_async,
    add_math_attempt_async, can_user_attempt_async, get_user_last_attempt_time_async,
    get_top_users_excluding_async, get_user_rank_excluding_async, get_top_exclude_list_async,
    add_to_top_exclude_async, remove_from_top_exclude_async, is_top_excluded_async,
    create_deposit_async, get_user_deposits_async, get_deposit_async,
    close_deposit_async, complete_deposit_async, get_expired_deposits_async,
    create_coinfall_async, get_coinfall_async, get_active_coinfall_async,
    add_coinfall_player_async, get_coinfall_players_async,
    start_coinfall_async, finish_coinfall_async, claim_coinfall_async,
    get_next_game_number_async, create_dice_game_async, get_dice_game_async,
    get_chat_dice_games_async, get_dice_game_players_async, add_dice_player_async,
    remove_dice_player_async, start_dice_game_async, finish_dice_game_async,
    cancel_dice_game_async, get_expired_dice_games_async, create_rr_game_async,
    get_rr_game_async, update_rr_game_async, finish_rr_game_async,
    create_promo_async, get_all_promos_async, get_promo_async,
    get_promo_by_id_async, use_promo_async, delete_promo_async,
    check_user_promo_async, create_user_check_async, get_user_check_async,
    get_user_checks_async, use_user_check_async, delete_user_check_async,
    get_user_msg_async, update_user_msg_async, transfer_msg_async,
    create_promotion_task ,get_active_tasks, get_total_pages,
    check_task_completion, report_task, delete_task,
    get_db, get_available_tasks, get_available_total_pages,
    get_my_tasks, get_my_tasks_total_pages, get_msg_rate,
    update_msg_rate, init_chats_db, add_bot_chat,
    init_logs_db, add_log_chat, init_cases_db,
    add_user_case, get_user_cases, remove_user_case,
    can_claim_daily, claim_daily_bonus, init_keys_db,
    add_key, get_key, init_work_conditions_db,
    get_work_conditions, set_work_conditions, init_games_db,
    save_game_session, get_game_session, update_game_session,
    delete_game_session, cleanup_expired_games, is_vip_user_async,
    get_user_settings_async, check_donate_feature, get_safe,
    has_safe_pin, update_safe_balance, clear_user_portfolio_async,
    set_balance_async, has_check_book, add_easter_keys,
    update_egg_cooldown, get_easter_keys, get_easter_top
)

from handlers import button_handler
from handlers.games import (
    crash_command, cubic_command, handle_dice_game_callbacks
)
from handlers.checks import handle_check_text_input, handle_check_activation, handle_check_callbacks
from handlers.settings import settings_command

getcontext().prec = 28

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=os.getenv('LOG_LEVEL', 'INFO')
)

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id.strip()]
MAIN_ADMIN_ID = 6025818386
CHANNEL_USERNAME = "@monstrbotnewss"
CHANNEL2_USERNAME = "@kursmsgmonstr"
KURS_CHANNEL = "@kursmsgmonstr"
INVESTMENT_ADMIN_IDS = []
ITEMS_PER_PAGE = 3
COOLDOWN_SECONDS = 2
SESSION_TIMEOUT = 150
CACHE_TTL = 300
TRANSFER_TTL = 120
BONUS_COOLDOWN = 1800
DB_PATH = os.getenv('DATABASE_PATH', 'data/bot.db')
ACTIVE_GAMES = {}
SPAM_PROTECTION = {}
MAX_CONCURRENT_GAMES = 3
SPAM_WARNING_LIMIT = 5
SPAM_BLOCK_TIME = 300
SPAM_BAN_DAYS = 10
COINFLIP_SESSIONS = {}
COINFLIP_MULTIPLIER = 1.97
COINFLIP_RESULTS = {
    'орел': 'орел 🦅',
    'решка': 'решка 🪙'
}
MINES_SESSIONS = {}
GOLD_SESSIONS = {}
PYRAMID_SESSIONS = {}
SLOT_SESSIONS = {}
LAST_CLICK_TIME = {}
subscription_cache = {}
user_cache = {}
pending_transfers = {}
transfer_confirmations = {}
ROULETTE_SESSIONS = {}
ROULETTE_COLORS = {
    0: "🟢",
    1: "🔴", 2: "⚫", 3: "🔴", 4: "⚫", 5: "🔴", 6: "⚫", 7: "🔴", 8: "⚫", 9: "🔴", 10: "⚫",
    11: "⚫", 12: "🔴", 13: "⚫", 14: "🔴", 15: "⚫", 16: "🔴", 17: "⚫", 18: "🔴",
    19: "🔴", 20: "⚫", 21: "🔴", 22: "⚫", 23: "🔴", 24: "⚫", 25: "🔴", 26: "⚫", 27: "🔴", 28: "⚫",
    29: "⚫", 30: "🔴", 31: "⚫", 32: "🔴", 33: "⚫", 34: "🔴", 35: "⚫", 36: "🔴"
}

ROULETTE_MULTIPLIERS = {
    'red': 2,
    'black': 2,
    'even': 2,
    'odd': 2,
    'high': 2,
    'low': 2,
    '1-12': 3,
    '13-24': 3,
    '25-36': 3,
    'number': 36
}

ROULETTE_BETS = {
    'к': 'red',
    'красное': 'red',
    'ч': 'black',
    'черное': 'black',
    'чет': 'even',
    'четное': 'even',
    'неч': 'odd',
    'нечетное': 'odd',
    'бол': 'high',
    'большие': 'high',
    'мал': 'low',
    'малые': 'low',
    '1-12': '1-12',
    '13-24': '13-24',
    '25-36': '25-36'
}
mailing_data = {}
pending_msg_transfers = {}
MSG_COMMISSION = 10
checklist_pages = {}
BOX_FIELD_SIZE = 4
BOX_TOTAL_CELLS = 16
BOX_SPIDERS_COUNT = 3
BOX_SAFE_CELLS = BOX_TOTAL_CELLS - BOX_SPIDERS_COUNT

TREASURES = {
    '💍': {'name': 'Кольцо', 'base_mult': 1.0, 'magic_range': (-0.5, -2.0)},
    '💎': {'name': 'Алмаз', 'base_mult': 1.2, 'magic_range': (-1.0, -2.5)},
    '💠': {'name': 'Сапфир', 'base_mult': 1.5, 'magic_range': (-1.5, -3.0)},
    '👑': {'name': 'Корона', 'base_mult': 2.0, 'magic_range': (-2.0, -3.5)},
    '🔮': {'name': 'Магический кристалл', 'base_mult': 2.5, 'magic_range': (-2.5, -4.0)}
}

BOX_MULTIPLIERS = [1.0, 1.2, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5]

BOX_SESSIONS = {}
CRASH_SESSIONS = {}
DICE_SESSIONS = {}
FIELD_SIZE = 5
CELLS_TOTAL = FIELD_SIZE * FIELD_SIZE
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
RR_SESSIONS = {}
MUTE_COMMANDS = ['мут', 'глуш']
KICK_COMMANDS = ['кик']
GOLD_MULTIPLIERS = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]

DARTS_SESSIONS = {}
DARTS_MULTIPLIERS = {
    'miss': 4.8,
    'red': 1.96,
    'center': 3.8,
    'white': 1.94
}

DARTS_BETS = {
    'м': 'miss',
    'мимо': 'miss',
    'к': 'red',
    'красное': 'red',
    'ц': 'center',
    'центр': 'center',
    'б': 'white',
    'белое': 'white'
}

DARTS_RESULTS = {
    1: ('miss', 'мимо 😯'),
    2: ('red', 'красное 🔴'),
    3: ('white', 'белое ⚪'),
    4: ('red', 'красное 🔴'),
    5: ('white', 'белое ⚪'),
    6: ('center', 'центр 🎯')
}
SPACESHIP_SESSIONS = {}

SPACESHIP_MULTIPLIERS = {
    1: 1.3,
    2: 1.7,
    3: 2.3,
    4: 2.6,
    5: 3.6,
    6: 4.2
}

SPACESHIP_POLICE_CHANCE = 30
PYRAMID_EMOJIS = ["🚪", "🏚", "🛖", "🏠", "🏡", "🏢", "🏣", "🏤", "🏛️", "🏰", "🏯", "🕌"]
PYRAMID_MULTIPLIERS_3 = [1.31, 1.74, 2.32, 3.10, 4.13, 5.51, 7.34, 9.79, 13.05, 17.40, 23.20, 30.94]
PYRAMID_MULTIPLIERS_2 = [1.45, 2.10, 3.05, 4.42, 6.41, 9.29, 13.47, 19.53, 28.32, 41.06, 59.54, 86.33]
PYRAMID_MULTIPLIERS_1 = [1.62, 2.62, 4.24, 6.86, 11.11, 17.99, 29.14, 47.20, 76.46, 123.86, 200.65, 325.05]

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

SLOT_EMOJIS = ["🟢", "🔵", "🟣", "🟡", "🔴", "⚫"]
SLOT_NAMES = ["Rare", "Super Rare", "Epic", "Legendary", "Mythic", "Mystical"]
SLOT_RANGES = [(10, 50), (100, 200), (500, 700), (800, 1200), (1500, 2500), (10000, 25000)]
SLOT_WEIGHTS = [50, 25, 15, 6, 3, 1]

FOOTBALL_WIN_QUOTES = [
    "Удача сегодня на вашей стороне!✨",
    "Оторвём частичку клевера!✨",
    "Удача — беспредельное счастье!✨",
    "Твоя удача — твое счастье!✨",
    "Удача – символ твоей жизни!✨",
    "Твоя удача — невероятна.✨"
]

FOOTBALL_LOSE_QUOTES = [
    "Даже у королей бывает плохая раздача карт — Наполеон Б.",
    "Кто не рискует, тот иногда пьет дешевый коньяк — Михаил Крут",
    "Иногда проиграть — это просто способ сбросить лишний вес — Арнольд Шварц.",
    "Даже если ты проиграл, ты все равно в игре, просто теперь ты зритель — Джокер",
    "В этот раз фортуна просто перепутала адресата — Аноним.Г"
]

BASKETBALL_WIN_QUOTES = [
    "Снайперская точность!🎯",
    "Трёхочковый в корзину!🏀",
    "Как Майкл Джордан в прайме!🔥",
    "Невозможное возможно!✨",
    "Чистое попадание!✅"
]

BASKETBALL_LOSE_QUOTES = [
    "Мяч круглый, а удача квадратная — народная мудрость",
    "Даже Леброн иногда мажет — ничего страшного",
    "В следующий раз обязательно забросишь!💪",
    "Промах — это тоже опыт",
    "Фортуна сегодня взяла тайм-аут"
]

BOT_START_TIME = time.time()
FOOTBALL_EMOJI = "⚽"
BASKETBALL_EMOJI = "🏀"

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

KHB_DUELS = {}
KHB_GAMES = {}
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

BOWLING_SESSIONS = {}
SPRING_EVENT_START = datetime(2026, 3, 7)
SPRING_EVENT_END = datetime(2026, 6, 1)
SPRING_CHANNEL = "https://t.me/monstrbotnewss"
spring_question_creation = {}
math_contest_pending = {}
math_contest_cooldown = {}
MATH_CONTEST_COOLDOWN = 0.9
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
DICE_MIN_BET = 1000
DICE_MAX_BET = 50000000
DICE_MAX_GAMES_PER_CHAT = 5
DICE_TIMEOUT = 30
FROG_SESSIONS = {}
FROG_MULTIPLIERS = {
    4: 25.25,  # 4-й уровень (верхний)
    3: 5.05,   # 3-й уровень
    2: 2.00,   # 2-й уровень
    1: 1.21,   # 1-й уровень
    0: 1.00    # стартовый уровень
}

FROG_BOMBS_COUNT = {
    1: 1,   # на уровне x1.21 - 1 бомба
    2: 2,   # на уровне x2.00 - 2 бомбы
    3: 3,   # на уровне x5.05 - 3 бомбы
    4: 4    # на уровне x25.25 - 4 бомбы
}
EVENT_COUNTER = 0
dice_cooldown = {}
DICE_COOLDOWN_SECONDS = 2
CASES_SESSIONS = {}

CASES_DATA = {
    'daily': {
        'name': '🎊 Daily',
        'emoji': '🎊',
        'min_reward': 5000,
        'max_reward': 25000,
        'empty_chance': 20,
        'cells': 9,
        'opens': 3
    },
    'empty': {
        'name': '😑 Пустышка',
        'emoji': '😑',
        'min_reward': 2000,
        'max_reward': 7000,
        'empty_chance': 40,
        'cells': 9,
        'opens': 3
    }
}

CASES_NAMES_RU = {
    'daily': 'Дэйли',
    'empty': 'Пустышка'
}


DIAMOND_SESSIONS = {}
DIAMOND_CELLS_PER_LEVEL = 3
DIAMOND_MULTIPLIERS_1 = {
    1: 1.46,
    2: 2.18,
    3: 3.27,
    4: 4.91,
    5: 7.37,
    6: 11.05,
    7: 16.57,
    8: 24.86,
    9: 37.29,
    10: 55.94,
    11: 83.91,
    12: 125.87,
    13: 188.81,
    14: 283.22,
    15: 424.83,
    16: 637.25
}
DIAMOND_MULTIPLIERS_2 = {
    1: 2.91,
    2: 8.73,
    3: 26.19,
    4: 78.57,
    5: 235.71,
    6: 707.13,
    7: 2121.39,
    8: 6364.17,
    9: 19092.51,
    10: 57277.53,
    11: 171832.59,
    12: 515497.77,
    13: 1546493.31,
    14: 4639479.93,
    15: 13918439.79,
    16: 41755319.37
}
DIAMOND_EMOJI = {
    'hidden': '❓',      # неоткрытая ячейка
    'safe': '💠',        # алмаз (безопасная ячейка)
    'bomb_hit': '💥',    # бомба на которую нарвались
    'bomb_miss': '🧨',   # бомба которая была, но на нее не нарвались
    'unopened_safe': '💰' # неоткрытая ячейка без бомбы
}
TOWER_FIELD_SIZE = 5
TOWER_MAX_LEVEL = 8
TOWER_SESSIONS = {}

TOWER_MULTIPLIERS = {
    1: [1.21, 1.52, 1.89, 2.37, 2.96, 3.70, 4.63, 5.78, 7.23],
    2: [1.62, 2.69, 4.49, 7.48, 12.47, 20.79, 34.65, 57.75, 96.25],
    3: [2.15, 4.62, 9.93, 21.35, 45.90, 98.69, 212.18, 456.19, 980.81],
    4: [2.86, 8.18, 23.39, 66.89, 191.30, 547.12, 1564.77, 4475.24, 12800.00]
}

def is_recent(update: Update) -> bool:
    """
    Проверяет, было ли сообщение/callback отправлено после запуска бота
    """
    if update.message:
        return update.message.date.timestamp() >= BOT_START_TIME
    elif update.callback_query and update.callback_query.message:
        return update.callback_query.message.date.timestamp() >= BOT_START_TIME
    elif update.inline_query:
        return update.inline_query.date.timestamp() >= BOT_START_TIME
    return True 

def generate_game_hash(game_data):
    data_string = json.dumps(game_data, sort_keys=True) + str(time.time()) + secrets.token_hex(8)
    return hashlib.sha256(data_string.encode()).hexdigest()[:16]

def generate_transfer_hash():
    return secrets.token_hex(8)

def generate_check_code():
    return secrets.token_hex(8)

# Форматтер для отображения чисел с разделителями тысяч
def format_amount(amount):
    """
    Форматирует число с разделителями тысяч.
    Пример: 200000 -> 200.000, 1000000 -> 1.000.000
    """
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
    if not amount_str or len(amount_str) > 20:
        return 0

    amount_str = str(amount_str).lower().replace(' ', '')
    amount_str = amount_str.replace(',', '.')
    
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
                value = Decimal(match.group(1))
                result = int(value * Decimal(multiplier))
                if result > 10**15:
                    return 0
                return result
            except (ValueError, OverflowError):
                return 0
    return 0


def parse_roulette_bet(bet_str: str) -> tuple:
    """
    Парсит ставку для рулетки
    Возвращает (тип_ставки, значение/диапазон, множитель)
    """
    bet_str = bet_str.lower().strip()
    
    # Проверяем на конкретное число (0-36)
    if bet_str.isdigit():
        num = int(bet_str)
        if 0 <= num <= 36:
            return ('number', num, ROULETTE_MULTIPLIERS['number'])
    
    # Проверяем на диапазон (1-12, 13-24, 25-36)
    if '-' in bet_str:
        parts = bet_str.split('-')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            start, end = int(parts[0]), int(parts[1])
            range_key = f"{start}-{end}"
            if range_key in ROULETTE_MULTIPLIERS:
                return (range_key, (start, end), ROULETTE_MULTIPLIERS[range_key])
    
    # Проверяем на текстовые команды
    if bet_str in ROULETTE_BETS:
        bet_type = ROULETTE_BETS[bet_str]
        return (bet_type, None, ROULETTE_MULTIPLIERS[bet_type])
    
    return None

def generate_math_problem():
    """Генерирует случайный математический пример и 10 вариантов ответов"""
    import random
    import math
    
    problem_types = [
        'add', 'sub', 'mul', 'div', 'sqrt'
    ]
    
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

async def check_expired_deposits(context: ContextTypes.DEFAULT_TYPE):
    """Проверка и автоматическое завершение просроченных депозитов"""
    expired = await get_expired_deposits_async()
    
    for deposit in expired:
        success, amount = await complete_deposit_async(deposit['deposit_id'])
        if success:
            await update_balance_async(deposit['user_id'], amount)
            
            try:
                await context.bot.send_message(
                    chat_id=deposit['user_id'],
                    text=f"✅ Ваш депозит 🆔 {deposit['deposit_id']} завершен!\n"
                         f"💸 Получено: {format_amount(amount)}ms¢"
                )
            except:
                pass

async def reset_stocks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сброс всех акций до 0 (только для админов)"""
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    keyboard = [[
        InlineKeyboardButton("⚠️ ПОДТВЕРДИТЬ СБРОС", callback_data="confirm_reset_stocks")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "⚠️ *ВНИМАНИЕ!*\n\n"
        "Вы собираетесь сбросить ВСЕ акции до 0:\n"
        "• Курсы всех акций станут 0ms¢\n"
        "• Все портфели пользователей будут очищены\n"
        "• Это действие необратимо\n\n"
        "Для подтверждения нажмите кнопку ниже:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def reset_stocks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения сброса акций"""
    query = update.callback_query
    user = query.from_user

    if user.id not in ADMIN_IDS and user.id != MAIN_ADMIN_ID:
        await query.answer("❌ У вас нет прав на это действие.", show_alert=True)
        return

    if query.data == "confirm_reset_stocks":
        await query.answer("⏳ Выполняется сброс...")

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

    await query.answer()

# Парсер для ставок с поддержкой "всё"
def parse_bet_amount(amount_str, user_balance=None):
    """
    Парсит ставку с поддержкой "всё/все/all" и чисел с суффиксами
    """
    if not amount_str:
        return 0

    amount_str = str(amount_str).lower().strip()
    
    all_variants = ['всё', 'все', 'all', 'вс']
    if amount_str in all_variants:
        if user_balance is not None:
            return user_balance
        return 0

    amount_str = amount_str.replace(',', '.')
    
    if amount_str.endswith('кккк'):
        num_part = amount_str[:-4]
        try:
            value = float(num_part) if '.' in num_part else int(num_part)
            return int(value * 1000000000000)
        except ValueError:
            pass
    elif amount_str.endswith('ккк'):
        num_part = amount_str[:-3]
        try:
            value = float(num_part) if '.' in num_part else int(num_part)
            return int(value * 1000000000)
        except ValueError:
            pass
    elif amount_str.endswith('кк'):
        num_part = amount_str[:-2]
        try:
            value = float(num_part) if '.' in num_part else int(num_part)
            return int(value * 1000000)
        except ValueError:
            pass
    elif amount_str.endswith('к'):
        num_part = amount_str[:-1]
        try:
            value = float(num_part) if '.' in num_part else int(num_part)
            return int(value * 1000)
        except ValueError:
            pass
    elif amount_str.endswith('м'):
        num_part = amount_str[:-1]
        try:
            value = float(num_part) if '.' in num_part else int(num_part)
            return int(value * 1000000)
        except ValueError:
            pass
    
    try:
        if '.' in amount_str and amount_str.count('.') > 1:
            amount_str = amount_str.replace('.', '')
        return int(float(amount_str))
    except ValueError:
        return 0

def simple_parse_bet(amount_str, user_balance=None):
    """
    Упрощённый парсер для игр, где важна скорость
    """
    if not amount_str:
        return 0
    
    amount_str = str(amount_str).lower().strip()
    
    if amount_str in ['всё', 'все', 'all']:
        return user_balance if user_balance is not None else 0
    
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

def check_cooldown(user_id):
    current_time = time.time()
    if user_id in LAST_CLICK_TIME:
        time_diff = current_time - LAST_CLICK_TIME[user_id]
        if time_diff < COOLDOWN_SECONDS:
            return round(COOLDOWN_SECONDS - time_diff, 1)
    LAST_CLICK_TIME[user_id] = current_time
    return 0

async def check_spam_protection(user_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверка на спам и защита от нагрузки"""
    current_time = time.time()
    
    if await check_ban(update, context):
        return False
    
    if user_id in SPAM_PROTECTION:
        spam_info = SPAM_PROTECTION[user_id]
        
        if current_time < spam_info['blocked_until']:
            remaining = int(spam_info['blocked_until'] - current_time)
            
            await update.message.reply_text(
                f"⚠ Вы временно заблокированы на {remaining} секунд из-за частых запросов.\n"
                "Пожалуйста, подождите."
            )
            return False
        else:
            del SPAM_PROTECTION[user_id]
    
    if user_id in ACTIVE_GAMES:
        active_count = ACTIVE_GAMES[user_id].get('count', 0)
        
        if active_count >= MAX_CONCURRENT_GAMES:
            await update.message.reply_text(
                f"⚠ У вас уже {active_count} активных игр. "
                f"Подождите завершения текущих игр."
            )
            return False
        
        games_in_window = ACTIVE_GAMES[user_id].get('games_in_window', [])
        games_in_window = [t for t in games_in_window if current_time - t < 60]
        ACTIVE_GAMES[user_id]['games_in_window'] = games_in_window
        
        if len(games_in_window) >= SPAM_WARNING_LIMIT:
            warnings = SPAM_PROTECTION.get(user_id, {}).get('warnings', 0) + 1
            
            SPAM_PROTECTION[user_id] = {
                'warnings': warnings,
                'blocked_until': current_time + SPAM_BLOCK_TIME,
                'banned': False
            }
            
            await update.message.reply_text(
                f"⚠ Вы делаете слишком много запросов!\n"
                f"Предупреждение {warnings}/3. При 3-х предупреждениях - бан на 10 дней."
            )
            
            if warnings >= 3:
                await ban_spammer(user_id, update, context)
            
            return False
    
    return True

import time

EGG_COOLDOWN_MINUTES = 10

async def check_egg_cooldown(user_id: int):
    """Проверяет кулдаун. Возвращает (можно_ли_кидать, секунды_до_разблокировки)"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute(
            "SELECT last_egg_time FROM easter_cooldown WHERE user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()

        if not result or result[0] is None:
            return True, 0

        last_timestamp = int(result[0])
        now_timestamp = int(time.time())
        diff = now_timestamp - last_timestamp
        
        cooldown_seconds = EGG_COOLDOWN_MINUTES * 60

        if diff >= cooldown_seconds:
            return True, 0

        remaining = cooldown_seconds - diff
        return False, remaining


async def update_egg_cooldown(user_id: int):
    """Обновляет время последнего использования яйца"""
    now_timestamp = int(time.time())
    async with aiosqlite.connect('data/bot.db') as db:
        await db.execute('''
            INSERT INTO easter_cooldown (user_id, last_egg_time)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET last_egg_time = ?
        ''', (user_id, now_timestamp, now_timestamp))
        await db.commit()

# ==================== КОНСТАНТЫ ИВЕНТОВ ====================

# ==================== ОСНОВНАЯ КОМАНДА ИВЕНТОВ ====================
async def events_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для просмотра ивентов"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    
    all_events = await get_all_events_async()
    
    available_events = [e for e in all_events if e['status'] in ['active', 'scheduled']]
    
    text = (
        f"🦩 *{user.full_name}*, добро пожаловать в категорию \"Ивенты\"\n\n"
        f"ℹ️ Тут ты сможешь увидеть ивенты, которые доступны или будут доступны в ближайшее время!"
    )
    
    keyboard = []
    if available_events:
        for event in available_events:
            if event['status'] == 'scheduled':
                button_text = f"🕘 {event['name']}"
            else:
                button_text = event['name']
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"event_view_{event['event_id']}")])
    else:
        keyboard.append([InlineKeyboardButton("⌛ Ожидаем ивенты...", callback_data="noop")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

# ==================== ПРОСМОТР ИВЕНТА ====================
async def event_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр конкретного ивента"""
    query = update.callback_query
    event_id = int(query.data.replace('event_view_', ''))
    
    event = await get_event_async(event_id)
    if not event:
        await query.answer("❌ Ивент не найден", show_alert=True)
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
    
    await query.answer()

# ==================== КОМАНДА СОЗДАНИЯ ИВЕНТА ====================
async def setevent_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для создания ивента (только для админов)"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return
    
    keyboard = [
        [
            InlineKeyboardButton("🕘 Запланированный", callback_data="event_type_scheduled"),
            InlineKeyboardButton("🚀 Готовый", callback_data="event_type_ready")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Выберите категорию ивента:",
        reply_markup=reply_markup
    )

# ==================== ТИПЫ ИВЕНТОВ ====================
async def event_type_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор типа ивента"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await query.answer("❌ У вас нет прав", show_alert=True)
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
    
    await query.answer()

async def list_donate_features(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список выданных донат-функций (только для админа)"""
    
    if update.effective_user.id != 6025818386:
        await update.message.reply_text("❌ Нет доступа.")
        return
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT user_id, username, expires_at FROM donate_features WHERE feature_type = "give" ORDER BY expires_at'
        )
        rows = cursor.fetchall()
        
        if not rows:
            await update.message.reply_text("📋 Нет активных донат-функций give.")
            return
        
        message = "📋 <b>Активные донат-функции give:</b>\n\n"
        for row in rows:
            user_id = row['user_id']
            username = row['username']
            expires_at = row['expires_at']
            expires = datetime.fromisoformat(expires_at)
            remaining = expires - datetime.now()
            days_remaining = remaining.days
            
            message += f"👤 @{username} (ID: {user_id})\n"
            message += f"⏰ Осталось: {days_remaining} дней\n"
            message += f"📅 До: {expires.strftime('%d.%m.%Y %H:%M')}\n"
            message += "─" * 20 + "\n"
        
        await update.message.reply_text(message, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in list_donate_features: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")

# ==================== ОБРАБОТКА ТЕКСТА ДЛЯ СОЗДАНИЯ ИВЕНТА ====================
async def event_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текста при создании ивента"""
    user_id = update.effective_user.id
    
    if 'event_creation' not in context.user_data:
        return
    
    event_data = context.user_data['event_creation']
    step = event_data.get('step')
    
    if event_data['type'] == 'scheduled':
        if step == 'date':
            date_pattern = r'^\d{2}\.\d{2}\.\d{4}$'
            if not re.match(date_pattern, update.message.text):
                await update.message.reply_text("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ")
                return
            
            event_data['date'] = update.message.text
            event_data['step'] = 'name'
            await update.message.reply_text("Принял! А теперь напишите название будущего ивента.")
            
        elif step == 'name':
            event_data['name'] = update.message.text
            event_data['step'] = 'description'
            await update.message.reply_text("🙈 Теперь введите описание ивента.")
            
        elif step == 'description':
            event_data['description'] = update.message.text
            event_data['step'] = 'confirm'
            
            text = (
                f"❕ Проверьте анкету перед созданием!\n\n"
                f"ℹ️ \"{event_data['name']}\" — {event_data['description']}.\n\n"
                f"Запланировано на {event_data['date']}"
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Верно", callback_data="event_confirm_yes"),
                    InlineKeyboardButton("🗿 Сомневаюсь", callback_data="event_confirm_no")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(text, reply_markup=reply_markup)
    
    else:
        if step == 'date':
            event_data['name'] = update.message.text
            event_data['step'] = 'description'
            await update.message.reply_text("Введите описание ивента:")
            
        elif step == 'description':
            event_data['description'] = update.message.text
            event_data['step'] = 'confirm'
            
            text = (
                f"❗ Перед отправкой ГОТОВОГО ивента убедитесь в правильности формата:\n\n"
                f"ℹ️ *{event_data['name']}* — {event_data['description']}"
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Готово", callback_data="event_confirm_yes"),
                    InlineKeyboardButton("🗿 Сомневаюсь", callback_data="event_confirm_no")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(text, reply_markup=reply_markup)

# ==================== ПОДТВЕРЖДЕНИЕ СОЗДАНИЯ ИВЕНТА ====================
async def event_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение или отклонение создания ивента"""
    query = update.callback_query
    user_id = query.from_user.id
    action = query.data.replace('event_confirm_', '')
    
    if 'event_creation' not in context.user_data:
        await query.answer("❌ Сессия создания ивента не найдена", show_alert=True)
        return
    
    event_data = context.user_data['event_creation']
    
    if action == 'no':
        del context.user_data['event_creation']
        await query.edit_message_text("❌ Создание ивента отменено. Начните заново с /setevent")
        await query.answer()
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
    await query.answer()
    
    del context.user_data['event_creation']

# ==================== ЗАКРЫТИЕ ИВЕНТА ====================
async def closeevent_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для закрытия ивента (только для админов)"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return
    
    all_events = await get_all_events_async()
    
    if not all_events:
        await update.message.reply_text("❌ Нет доступных ивентов для закрытия.")
        return
    
    keyboard = []
    for event in all_events:
        if event['status'] != 'closed':
            keyboard.append([InlineKeyboardButton(event['name'], callback_data=f"event_close_{event['event_id']}")])
    
    if not keyboard:
        await update.message.reply_text("❌ Нет открытых ивентов для закрытия.")
        return
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🌍 Выберите ивент для внепланового закрытия:",
        reply_markup=reply_markup
    )

async def event_close_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор ивента для закрытия"""
    query = update.callback_query
    event_id = int(query.data.replace('event_close_', ''))
    
    event = await get_event_async(event_id)
    if not event:
        await query.answer("❌ Ивент не найден", show_alert=True)
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
    await query.answer()

async def event_close_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение закрытия ивента"""
    query = update.callback_query
    data = query.data
    
    if data == "event_close_confirm_yes":
        action = "yes"
    elif data == "event_close_confirm_no":
        action = "no"
    else:
        await query.answer("❌ Неизвестное действие", show_alert=True)
        return
    
    if action == 'no':
        await query.edit_message_text("Успешно...")
        await asyncio.sleep(1)
        await query.delete_message()
        await query.answer()
        return
    
    event_id = context.user_data.get('closing_event_id')
    if not event_id:
        await query.answer("❌ Ошибка: ивент не найден", show_alert=True)
        return
    
    await update_event_status_async(event_id, 'closed')
    
    await query.edit_message_text("Закрываю...")
    await asyncio.sleep(1)
    await query.delete_message()
    await query.answer("✅ Ивент закрыт")

async def stocks_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда 'Акции' - информация об акциях"""
    user = update.effective_user
    if not user:
        return

    await get_user_async(user.id, user.full_name, user.username)

    text = (
        "<tg-emoji emoji-id='5231200819986047254'>📊</tg-emoji>Акции – инвестируй и зарабатывай!*\n\n"
        "<tg-emoji emoji-id='5224257782013769471'>💰</tg-emoji>Покупай акции, следи за курсом и продавай, когда цена растёт.\n"
        "📈 Каждую минуту курсы меняются от -30% до +30%.\n"
        "💎 Доступные акции: Bitcoin (BTC), MonsterC (MSC), Telegram (TG).\n\n"
        "📌 *Команды:*\n"
        "• <code>мои акции</code> — твой портфель\n"
        "• <code>магазин или /shop</code> — купить акции\n"
        "• <code>/buyact ID количество</code> — быстрая покупка\n"
        "• <code>/sellact ID количество</code> — продажа"
    )

    keyboard = [[
        InlineKeyboardButton("📉 Курс-канал", url="https://t.me/monstraction")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        text, 
        reply_markup=reply_markup, 
        parse_mode='HTML'
    )

async def ban_spammer(user_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Бан спамера через существующую систему банов"""
    try:
        ban_days = 10
        reason = "Чрезмерный спам в играх (автоматический бан)"
        
        await ban_user_async(user_id, ban_days, reason)
        
        SPAM_PROTECTION[user_id] = {
            'warnings': 3,
            'blocked_until': time.time() + SPAM_BLOCK_TIME,
            'banned': True,
            'reason': reason
        }
        
        user = update.effective_user
        username = f"@{user.username}" if user.username else f"ID: {user_id}"
        
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"🚫 **АВТОМАТИЧЕСКИЙ БАН**\n\n"
                         f"Пользователь: {username}\n"
                         f"Имя: {user.full_name}\n"
                         f"ID: `{user_id}`\n"
                         f"Причина: {reason}\n"
                         f"Срок: {ban_days} дней\n"
                         f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    parse_mode='Markdown'
                )
            except:
                pass
        
        try:
            keyboard = [[InlineKeyboardButton("🧐 Не согласен", url="https://t.me/kleymorf")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await context.bot.send_message(
                chat_id=user_id,
                text=f"🚨 Вы были заблокированы в боте на {ban_days} дней по причине: {reason}\n\n"
                     f"❓ Не согласны с наказанием? Нажмите кнопку ниже",
                reply_markup=reply_markup
            )
        except Exception as e:
            if "Forbidden" not in str(e):
                logging.error(f"Failed to send ban notification to {user_id}: {e}")
        
        await update.message.reply_text(
            f"Вы забанены на {ban_days} дней за чрезмерный спам!\n"
            f"Причина: {reason}\n"
            f"Обратитесь к администратору."
        )
        
    except Exception as e:
        logging.error(f"Ошибка при бане спамера {user_id}: {e}")

async def send_subscription_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
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

async def cleanup_old_sessions(app: Application):
    while True:
        try:
            await asyncio.sleep(60)
            current_time = time.time()

            for user_id in list(LAST_CLICK_TIME.keys()):
                if current_time - LAST_CLICK_TIME[user_id] > 3600:
                    del LAST_CLICK_TIME[user_id]

            for user_id in list(user_cache.keys()):
                if current_time - user_cache[user_id]['time'] > CACHE_TTL:
                    del user_cache[user_id]

            for user_id in list(subscription_cache.keys()):
                if current_time - subscription_cache[user_id][0] > CACHE_TTL:
                    del subscription_cache[user_id]

            # ==================== ОЧИСТКА ИГР С СОХРАНЕНИЕМ В БД ====================
            # Очистка MINES, GOLD, PYRAMID (через БД)
            from database import cleanup_expired_games
            expired_count = await cleanup_expired_games()
            if expired_count > 0:
                logging.info(f"Cleaned up {expired_count} expired game sessions from DB")

            # Очистка активных сессий в памяти
            for session_dict in [MINES_SESSIONS, GOLD_SESSIONS, PYRAMID_SESSIONS]:
                for user_id in list(session_dict.keys()):
                    try:
                        session = session_dict[user_id]

                        # Проверяем, есть ли запись в БД
                        db_id = session.get('db_id')
                        if db_id:
                            db_session = await get_game_session(db_id)
                            if db_session and db_session['status'] == 'expired':
                                # Если в БД игра уже завершена, удаляем из памяти
                                del session_dict[user_id]
                                continue

                        if session.get('status') == 'active':
                            if 'last_activity' in session:
                                if current_time - session['last_activity'] > 600:
                                    if 'bet' in session and session['bet'] > 0:
                                        await update_balance_async(user_id, session['bet'])
                                        if db_id:
                                            await update_game_session(db_id, status='expired')
                                        del session_dict[user_id]
                                        try:
                                            await app.bot.send_message(
                                                chat_id=user_id,
                                                text="⏰ Игра автоматически завершена по таймауту (10 минут бездействия). Средства возвращены на баланс."
                                            )
                                        except:
                                            pass
                            else:
                                if 'start_time' in session and current_time - session['start_time'] > 600:
                                    if 'bet' in session and session['bet'] > 0:
                                        await update_balance_async(user_id, session['bet'])
                                        if db_id:
                                            await update_game_session(db_id, status='expired')
                                        del session_dict[user_id]
                                        try:
                                            await app.bot.send_message(
                                                chat_id=user_id,
                                                text="⏰ Игра автоматически завершена по таймауту (10 минут). Средства возвращены на баланс."
                                            )
                                        except:
                                            pass
                            continue

                        if session.get('status') in ['lost', 'won']:
                            if 'end_time' not in session:
                                session['end_time'] = current_time
                            elif current_time - session['end_time'] > 120:
                                if db_id:
                                    await delete_game_session(db_id)
                                del session_dict[user_id]
                            continue

                        if 'start_time' in session and current_time - session['start_time'] > 300:
                            if 'bet' in session and session['bet'] > 0:
                                await update_balance_async(user_id, session['bet'])
                                if db_id:
                                    await update_game_session(db_id, status='expired')
                                try:
                                    await app.bot.send_message(
                                        chat_id=user_id,
                                        text="⏰ Игра автоматически завершена по таймауту ожидания. Средства возвращены на баланс."
                                    )
                                except:
                                    pass
                            del session_dict[user_id]

                    except Exception as e:
                        logging.error(f"Error cleaning up session for user {user_id}: {e}")

            # ==================== ОСТАЛЬНЫЕ ИГРЫ БЕЗ ИЗМЕНЕНИЙ ====================
            for user_id in list(RR_SESSIONS.keys()):
                try:
                    session = RR_SESSIONS[user_id]

                    game = await get_rr_game_async(session['game_id'])
                    if not game:
                        del RR_SESSIONS[user_id]
                        continue

                    if 'start_time' in session:
                        if current_time - session['start_time'] > 300:
                            if game['status'] == 'active':
                                await update_balance_async(user_id, game['bet'])
                                await finish_rr_game_async(session['game_id'], 'timeout')

                                try:
                                    await app.bot.send_message(
                                        chat_id=user_id,
                                        text=(
                                            f"🔫 Ваша игра в русскую рулетку была окончена по истечению времени!\n"
                                            f"Ваша ставка в размере {format_amount(game['bet'])}ms¢ была возвращена на ваш баланс!"
                                        )
                                    )
                                except Exception as e:
                                    if "Forbidden" not in str(e):
                                        logging.error(f"Failed to send rr timeout message to {user_id}: {e}")

                                logging.info(f"RR session for user {user_id} expired after 5 minutes, bet {game['bet']} returned")

                            del RR_SESSIONS[user_id]

                except Exception as e:
                    logging.error(f"Error cleaning up rr session for user {user_id}: {e}")

            for user_id in list(BOWLING_SESSIONS.keys()):
                try:
                    session = BOWLING_SESSIONS[user_id]

                    if session.get('status') in ['won', 'lost']:
                        if 'end_time' not in session:
                            session['end_time'] = current_time
                        elif current_time - session['end_time'] > 120:
                            del BOWLING_SESSIONS[user_id]
                            logging.info(f"Bowling session for user {user_id} cleaned up (completed)")

                    elif session.get('status') == 'waiting':
                        if 'start_time' in session and current_time - session['start_time'] > 300:
                            if 'bet' in session and session['bet'] > 0:
                                await update_balance_async(user_id, session['bet'])
                                logging.info(f"Bowling session timeout for user {user_id}, bet {session['bet']} returned")

                                try:
                                    await app.bot.send_message(
                                        chat_id=user_id,
                                        text="⏰ Игра в Боулинг автоматически завершена по таймауту. Средства возвращены на баланс."
                                    )
                                except Exception as e:
                                    if "Forbidden" not in str(e) and "blocked" not in str(e):
                                        logging.error(f"Failed to send bowling timeout message to {user_id}: {e}")

                            del BOWLING_SESSIONS[user_id]

                except Exception as e:
                    logging.error(f"Error cleaning up bowling session for user {user_id}: {e}")

            for user_id in list(SLOT_SESSIONS.keys()):
                try:
                    session = SLOT_SESSIONS[user_id]
                    if 'start_time' in session and current_time - session['start_time'] > 300:
                        del SLOT_SESSIONS[user_id]
                        logging.info(f"Slot session for user {user_id} expired after 5 minutes")
                    elif session.get('status') in ['finished']:
                        if 'end_time' not in session:
                            session['end_time'] = current_time
                        elif current_time - session['end_time'] > 120:
                            del SLOT_SESSIONS[user_id]
                except Exception as e:
                    logging.error(f"Error cleaning up slot session for user {user_id}: {e}")

            for user_id in list(TOWER_SESSIONS.keys()):
                try:
                    session = TOWER_SESSIONS[user_id]

                    if session.get('status') == 'active':
                        if 'last_activity' in session:
                            if current_time - session['last_activity'] > 120:
                                if 'bet' in session and session['bet'] > 0:
                                    await update_balance_async(user_id, session['bet'])
                                    del TOWER_SESSIONS[user_id]
                                    try:
                                        await app.bot.send_message(
                                            chat_id=user_id,
                                            text="⏰ Игра в Башню автоматически завершена по таймауту (2 минуты бездействия). Средства возвращены на баланс."
                                        )
                                    except Exception as e:
                                        if "Forbidden" not in str(e):
                                            logging.error(f"Failed to send tower timeout message to {user_id}: {e}")
                                    logging.info(f"Tower session for user {user_id} expired after 2 minutes, bet {session['bet']} returned")
                        else:
                            if 'start_time' in session and current_time - session['start_time'] > 120:
                                if 'bet' in session and session['bet'] > 0:
                                    await update_balance_async(user_id, session['bet'])
                                    del TOWER_SESSIONS[user_id]
                                    try:
                                        await app.bot.send_message(
                                            chat_id=user_id,
                                            text="⏰ Игра в Башню автоматически завершена по таймауту (2 минуты). Средства возвращены на баланс."
                                        )
                                    except Exception as e:
                                        if "Forbidden" not in str(e):
                                            logging.error(f"Failed to send tower timeout message to {user_id}: {e}")
                                    logging.info(f"Tower session for user {user_id} expired after 2 minutes (start_time), bet {session['bet']} returned")

                    elif session.get('status') in ['lost', 'won']:
                        if 'end_time' not in session:
                            session['end_time'] = current_time
                        elif current_time - session['end_time'] > 120:
                            del TOWER_SESSIONS[user_id]
                            logging.info(f"Tower session for user {user_id} cleaned up (completed)")

                except Exception as e:
                    logging.error(f"Error cleaning up tower session for user {user_id}: {e}")

            for duel_id in list(KHB_DUELS.keys()):
                try:
                    duel = KHB_DUELS[duel_id]
                    if duel['status'] == 'active' and current_time > duel['expire_time']:
                        await update_balance_async(duel['challenger_id'], duel['bet'])
                        logging.info(f"🧹 Дуэль {duel_id} очищена по таймауту. Возврат {duel['bet']}ms¢ пользователю {duel['challenger_id']}")

                        try:
                            keyboard = [[InlineKeyboardButton("⏱ Вызов отозван", callback_data="noop")]]
                            reply_markup = InlineKeyboardMarkup(keyboard)

                            await app.bot.edit_message_text(
                                chat_id=duel['chat_id'],
                                message_id=duel['message_id'],
                                text=(
                                    f"🔫 {duel['opponent_name']}, вас вызвали на дуэль \"КНБ\"\n"
                                    f"Вызов от {duel['challenger_name']}\n\n"
                                    f"🙈 Вызов - неактивен\n\n"
                                    f"💸 Ставка: {format_amount(duel['bet'])}ms¢.\n\n"
                                    f"⏱ Вызов был автоматически отозван. Средства возвращены."
                                ),
                                reply_markup=reply_markup
                            )
                        except:
                            pass

                        del KHB_DUELS[duel_id]

                    elif duel['status'] in ['accepted', 'cancelled', 'expired']:
                        if 'cleanup_time' not in duel:
                            duel['cleanup_time'] = current_time
                        elif current_time - duel['cleanup_time'] > 3600:
                            del KHB_DUELS[duel_id]

                except Exception as e:
                    logging.error(f"Error cleaning up KHB duel {duel_id}: {e}")

            for game_id in list(KHB_GAMES.keys()):
                try:
                    game = KHB_GAMES[game_id]
                    if game.get('status') in ['finished']:
                        if 'cleanup_time' not in game:
                            game['cleanup_time'] = current_time
                        elif current_time - game['cleanup_time'] > 3600:
                            del KHB_GAMES[game_id]
                    elif 'start_time' in game and current_time - game['start_time'] > 1800:
                        if game['type'] == 'pvp':
                            if game['user1_choice'] is None:
                                await update_balance_async(game['user1_id'], game['bet'])
                            if game['user2_choice'] is None:
                                await update_balance_async(game['user2_id'], game['bet'])
                        del KHB_GAMES[game_id]
                        logging.info(f"🧹 Игра КНБ {game_id} очищена по таймауту")
                except Exception as e:
                    logging.error(f"Error cleaning up KHB game {game_id}: {e}")

            for transfer_id in list(pending_transfers.keys()):
                if current_time - pending_transfers[transfer_id]['time'] > TRANSFER_TTL:
                    del pending_transfers[transfer_id]

            for transfer_id in list(transfer_confirmations.keys()):
                if current_time - transfer_confirmations[transfer_id]['time'] > TRANSFER_TTL:
                    del transfer_confirmations[transfer_id]

            for user_id in list(mailing_data.keys()):
                if current_time - mailing_data[user_id].get('time', 0) > 3600:
                    del mailing_data[user_id]

            for user_id in list(checklist_pages.keys()):
                if current_time - checklist_pages[user_id].get('time', 0) > 600:
                    del checklist_pages[user_id]

        except Exception as e:
            logging.error(f"Error in cleanup task: {e}")

async def safe_answer(query, text, show_alert=False):
    try:
        await query.answer(text, show_alert=show_alert)
    except Exception as e:
        logging.error(f"Failed to answer callback query: {e}")

async def update_stock_prices(context: ContextTypes.DEFAULT_TYPE):
    """Обновление цен акций каждую минуту с реалистичным поведением"""
    stocks = await get_all_stocks_async()
    updates = []
    stock_updates = []

    # Получаем или создаем словарь для хранения трендов акций
    stock_trends = context.bot_data.get('stock_trends', {})

    for stock in stocks:
        stock_id = stock['stock_id']
        name = stock['name']
        previous_price = stock['current_price']
        
        # ✅ ПРОВЕРКА: если предыдущая цена 0, устанавливаем минимальную
        if previous_price <= 0:
            previous_price = 1
            await update_stock_price_async(stock_id, 1)
            logging.warning(f"Stock {stock_id} had zero price, reset to 1")

        # Получаем текущий тренд для акции или создаем новый
        if stock_id not in stock_trends:
            stock_trends[stock_id] = {
                'trend': random.choice(['up', 'down', 'side']),
                'strength': random.uniform(0.5, 1.5),
                'duration': random.randint(3, 8),
                'counter': 0
            }

        trend_data = stock_trends[stock_id]
        trend_data['counter'] += 1

        # Если тренд закончился, меняем его
        if trend_data['counter'] >= trend_data['duration']:
            # Определяем новый тренд на основе текущего
            if trend_data['trend'] == 'up':
                # После роста может быть коррекция вниз или боковик
                new_trend = random.choices(['down', 'side'], weights=[60, 40])[0]
            elif trend_data['trend'] == 'down':
                # После падения может быть отскок вверх или боковик
                new_trend = random.choices(['up', 'side'], weights=[70, 30])[0]
            else:
                # Из боковика может быть любое движение
                new_trend = random.choices(['up', 'down', 'side'], weights=[40, 40, 20])[0]

            stock_trends[stock_id] = {
                'trend': new_trend,
                'strength': random.uniform(0.8, 2.0),
                'duration': random.randint(4, 12),
                'counter': 0
            }
            trend_data = stock_trends[stock_id]

        # Генерируем изменение на основе тренда
        if trend_data['trend'] == 'up':
            # Восходящий тренд: в основном рост, иногда небольшие падения
            if random.random() < 0.8:  # 80% рост
                change_percent = random.uniform(0.01, 0.08) * trend_data['strength']
            else:  # 20% небольшая коррекция
                change_percent = random.uniform(-0.03, -0.01) * trend_data['strength']

        elif trend_data['trend'] == 'down':
            # Нисходящий тренд: в основном падение, иногда отскоки
            if random.random() < 0.8:  # 80% падение
                change_percent = random.uniform(-0.08, -0.01) * trend_data['strength']
            else:  # 20% небольшой отскок
                change_percent = random.uniform(0.01, 0.03) * trend_data['strength']

        else:  # side
            # Боковик: случайные небольшие колебания
            change_percent = random.uniform(-0.03, 0.03)

        # Иногда случаются сильные движения (новости, события)
        if random.random() < 0.05:  # 5% шанс на сильное движение
            if random.random() < 0.5:
                # Резкий скачок вверх
                change_percent = random.uniform(0.10, 0.25)
                trend_data['trend'] = 'up'  # Меняем тренд на восходящий
                trend_data['counter'] = 0
                trend_data['duration'] = random.randint(5, 10)
            else:
                # Резкое падение
                change_percent = random.uniform(-0.25, -0.10)
                trend_data['trend'] = 'down'  # Меняем тренд на нисходящий
                trend_data['counter'] = 0
                trend_data['duration'] = random.randint(5, 10)

        # Рассчитываем изменение
        change = int(previous_price * change_percent)

        # Минимальное изменение
        if change == 0:
            change = random.choice([-1, 1]) * random.randint(1, 3)

        # Новая цена (минимум 1)
        new_price = max(1, previous_price + change)

        stock_updates.append((stock_id, new_price))

        # ✅ ИСПРАВЛЕНИЕ: безопасное вычисление процента
        if previous_price > 0:
            percent_change = ((new_price - previous_price) / previous_price) * 100
        else:
            percent_change = 0.0

        # Выбираем эмодзи в зависимости от силы движения
        if percent_change > 5:
            change_emoji = "🚀"  # Сильный рост
        elif percent_change > 1:
            change_emoji = "📈"  # Рост
        elif percent_change > 0:
            change_emoji = "↗️"  # Небольшой рост
        elif percent_change < -5:
            change_emoji = "💥"  # Сильное падение
        elif percent_change < -1:
            change_emoji = "📉"  # Падение
        elif percent_change < 0:
            change_emoji = "↘️"  # Небольшое падение
        else:
            change_emoji = "➡️"  # Без изменений

        # Добавляем информацию о тренде
        trend_emoji = {
            'up': '📈',
            'down': '📉',
            'side': '➡️'
        }.get(trend_data['trend'], '')

        updates.append(
            f"🆔 {stock_id} — {name} {new_price}ms¢ "
            f"({change_emoji}{percent_change:+.2f}%) {trend_emoji}"
        )

    # Сохраняем обновленные тренды
    context.bot_data['stock_trends'] = stock_trends

    if stock_updates:
        success = await update_all_stocks_prices_async(stock_updates)
        if not success:
            logging.error("Failed to update stock prices in database")

    if updates and KURS_CHANNEL:
        report = "📊 Ежеминутный отчёт.\n\n" + "\n".join(updates)
        try:
            await context.bot.send_message(chat_id=KURS_CHANNEL, text=report)
        except Exception as e:
            logging.error(f"Failed to send kurs update: {e}")

async def get_user_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для получения информации о пользователе (только для админов)"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    # Проверка прав админа
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    target_id = None
    target_name = None

    # Если ответ на сообщение
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.full_name
    else:
        # Если указан username или ID
        if len(context.args) < 1:
            await update.message.reply_text(
                "❌ Использование:\n"
                "• Ответьте на сообщение: /гет\n"
                "• Или укажите: /гет @username или /гет id"
            )
            return


        target = context.args[0]

        if target.startswith('@'):
            username = target[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                return
            target_id = user_data['user_id']
            target_name = user_data['full_name'] or username
        else:
            try:
                target_id = int(target)
                user_data = await get_user_async(target_id)
                target_name = user_data.get('full_name') or f"ID: {target_id}"
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    # Получаем информацию о пользователе
    db_user = await get_user_async(target_id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден.")
        return

    # Получаем баланс MSG
    msg_balance = db_user.get('msg_balance', 0)

    # Получаем депозиты
    from database import get_user_deposits_async
    deposits = await get_user_deposits_async(target_id, 'active')

    # Получаем портфель акций
    portfolio = await get_user_portfolio_async(target_id)
    portfolio_total = await get_user_portfolio_total_async(target_id)
    portfolio_count = await get_user_portfolio_count_async(target_id)

    # Формируем текст
    text = f"👤 Профиль пользователя {target_name}:\n\n"
    text += f"💸 Баланс – {format_amount(db_user['balance'])}ms¢\n"
    text += f"🍯 MSG – {format_amount(msg_balance)}\n\n"

    # Депозиты
    text += f"🏦 Депозиты:\n"
    if deposits:
        from datetime import datetime
        now = datetime.now()
        for dep in deposits:
            try:
                expires = datetime.strptime(dep['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
            except:
                try:
                    expires = datetime.strptime(dep['expires_at'], '%Y-%m-%d %H:%M:%S')
                except:
                    continue

            delta = expires - now
            days = delta.days
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60

            text += f"🆔 {dep['deposit_id']} — {format_amount(dep['amount'])}ms¢ — закончится через {days} дн. {hours} ч. {minutes} мин.\n"
    else:
        text += f"Нет активных депозитов\n"

    # Акции
    text += f"\n📊 Акции:\n"
    if portfolio:
        for item in portfolio:
            text += f"• {item['name']} — {item['quantity']} шт. x {item['current_price']}ms¢\n"
        text += f"\n💰 Общая сумма акций: {format_amount(portfolio_total)}ms¢ ({portfolio_count} шт.)"
    else:
        text += f"Нет акций"

    await update.message.reply_text(text)


async def donut_give(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выдача донат-функции give админом"""
    
    # Проверяем, что команду вызывает админ
    if update.effective_user.id != 6025818386:
        await update.message.reply_text("❌ У вас нет прав для использования этой команды.")
        return
    
    # Проверяем аргументы
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "❌ Использование:\n"
            "По username: /givedg @username количество_дней\n"
            "По ID: /givedg 123456789 количество_дней\n\n"
            "Примеры:\n"
            "/givedg @john_doe 30\n"
            "/givedg 6025818386 7"
        )
        return
    
    target = context.args[0]
    user_id = None
    username = None
    
    try:
        days = int(context.args[1])
        if days <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Количество дней должно быть положительным числом.")
        return
    
    # Определяем, это username или ID
    if target.startswith('@'):
        # Это username
        username = target[1:]
        try:
            # Пытаемся найти пользователя через Telegram API
            user = await context.bot.get_chat(username)
            user_id = user.id
            username = user.username or username
        except Exception:
            # Если не нашли через API, ищем в базе данных
            user_data = await get_user_by_username_async(username)
            if user_data:
                user_id = user_data['user_id']
            else:
                await update.message.reply_text(f"❌ Не удалось найти пользователя @{username}.")
                return
    else:
        # Это ID
        try:
            user_id = int(target)
            # Пытаемся получить username через API
            try:
                user = await context.bot.get_chat(user_id)
                username = user.username or str(user_id)
            except:
                # Если не получается, ищем в базе
                user_data = await get_user_by_id_async(user_id)
                if user_data:
                    username = user_data.get('username', str(user_id))
                else:
                    username = str(user_id)
        except ValueError:
            await update.message.reply_text("❌ Неверный формат ID или username.")
            return
    
    # Рассчитываем дату истечения
    expires_at = datetime.now() + timedelta(days=days)
    
    # Сохраняем в базу
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO donate_features (user_id, username, feature_type, expires_at, granted_by) VALUES (?, ?, ?, ?, ?)',
            (user_id, username, 'give', expires_at.isoformat(), update.effective_user.id)
        )
        conn.commit()
        
        expires_str = expires_at.strftime('%d.%m.%Y %H:%M')
        
        await update.message.reply_text(
            f"✅ Функция <b>give</b> успешно выдана!\n\n"
            f"👤 Пользователь: {username} (ID: {user_id})\n"
            f"📅 Истекает: {expires_str}\n"
            f"⏰ Дней: {days}",
            parse_mode='HTML'
        )
        
        # Уведомляем пользователя
        try:
            await context.bot.send_message(
                user_id,
                f"🎉 Вам выдана функция <b>give</b> на {days} дней!\n\n"
                f"Теперь вы можете использовать команду /give @username сумма\n"
                f"⏰ Функция будет активна до: {expires_str}",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
            
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def clean_expired_features():
    """Очищает просроченные донат-функции (запускать каждый час)"""
    
    async with db.execute(
        'DELETE FROM donate_features WHERE expires_at < ?',
        (datetime.now().isoformat(),)
    ):
        await db.commit()
    
    # Логируем очистку
    print(f"[{datetime.now()}] Cleaned expired donate features")


def check_donate_feature(user_id: int, feature_type: str) -> bool:
    """Проверяет, есть ли у пользователя активная донат-функция"""
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Сначала удаляем все просроченные записи
        cursor.execute(
            'DELETE FROM donate_features WHERE expires_at < datetime("now")'
        )
        conn.commit()
        
        # Проверяем конкретного пользователя
        cursor.execute(
            'SELECT expires_at FROM donate_features WHERE user_id = ? AND feature_type = ?',
            (user_id, feature_type)
        )
        row = cursor.fetchone()
        
        if row:
            expires_at = datetime.fromisoformat(row['expires_at'])
            if expires_at > datetime.now():
                return True
        
        return False
    except Exception as e:
        logger.error(f"Error checking donate feature: {e}")
        return False

from PIL import Image, ImageDraw, ImageFont
import io

async def generate_check_image(creator_name: str, amount: str, activations_left: int, check_number: int) -> io.BytesIO:
    """Генерирует изображение для чека на основе шаблона"""
    # Путь к шаблону (положи фото в папку assets)
    template_path = "assets/check_template.png"
    
    try:
        img = Image.open(template_path)
    except:
        # Если шаблона нет, создаём заглушку
        img = Image.new('RGB', (1024, 640), color='#1a1a2e')
    
    draw = ImageDraw.Draw(img)
    
    # Шрифты
    try:
        font_large = ImageFont.truetype("fonts/arial_bold.ttf", 48)
        font_medium = ImageFont.truetype("fonts/arial.ttf", 28)
    except:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
    
    # 1. Сумма в синем поле (центр, Y: 245-255)
    amount_text = f"{amount} ms¢"
    bbox = draw.textbbox((0, 0), amount_text, font=font_large)
    text_width = bbox[2] - bbox[0]
    x_amount = (1024 - text_width) // 2
    draw.text((x_amount, 245), amount_text, fill='#FFFFFF', font=font_large)
    
    # 2. Имя создателя (X: 120-130, Y: 510-520)
    if len(creator_name) > 20:
        creator_name = creator_name[:18] + ".."
    draw.text((130, 515), creator_name, fill='#4ecdc4', font=font_medium)
    
    # 3. Количество активаций (X: 360-370, Y: 510-520)
    draw.text((370, 515), str(activations_left), fill='#4ecdc4', font=font_medium)
    
    # Сохраняем
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    
    return buf

temp_safe_data = {}
temp_pin_data = {}

def format_number(num: int) -> str:
    """Форматирует число в удобный вид"""
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.2f}ккк".rstrip('0').rstrip('.')
    elif num >= 1_000_000:
        return f"{num/1_000_000:.2f}кк".rstrip('0').rstrip('.')
    elif num >= 1_000:
        return f"{num/1_000:.2f}к".rstrip('0').rstrip('.')
    else:
        return str(num)


async def safe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню сейфа"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    # Проверяем, что update и update.message существуют
    if not update or not update.message:
        return
    
    user = update.effective_user
    safe_data = get_safe(user.id)
    
    keyboard = [
        [InlineKeyboardButton("ℹ️ Просмотреть сейф", callback_data="safe_view")],
        [InlineKeyboardButton("📤 Внести", callback_data="safe_deposit"),
         InlineKeyboardButton("📥 Снять", callback_data="safe_withdraw")],
        [InlineKeyboardButton("🔐 Установить PIN-CODE", callback_data="safe_set_pin")]
    ]
    
    # Добавляем ID владельца в текст сообщения
    await update.message.reply_text(
        f"🧰 *{user.first_name}*, сейф\n"
        f"👤 ID: `{user.id}`\n\n"
        f"Выберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def safe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback от сейфа"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    data = query.data
    
    if data == "safe_view":
        await safe_view(query, user)
    
    elif data == "safe_deposit":
        await safe_deposit_menu(query, user)
    
    elif data == "safe_withdraw":
        await safe_withdraw_menu(query, user)
    
    elif data == "safe_set_pin":
        await safe_set_pin_start(query, user)
    
    elif data.startswith("safe_pin_digit_"):
        digit = data.split("_")[-1]
        await safe_pin_input(query, user, digit)
    
    elif data == "safe_pin_clear":
        await safe_pin_clear(query, user)
    
    elif data == "safe_pin_confirm":
        await safe_pin_confirm(query, user)
    
    elif data.startswith("safe_deposit_amount_"):
        amount = data.split("_")[-1]
        await safe_deposit_amount(query, user, amount)
    
    elif data.startswith("safe_withdraw_amount_"):
        amount = data.split("_")[-1]
        await safe_withdraw_amount(query, user, amount)
    
    elif data == "safe_deposit_custom":
        await safe_deposit_custom(query, user)
    
    elif data == "safe_withdraw_custom":
        await safe_withdraw_custom(query, user)

async def safe_view(query, user):
    """Просмотр сейфа"""
    safe_data = get_safe(user.id)
    balance = safe_data['balance']
    has_pin = has_safe_pin(user.id)
    
    pin_status = "🔴 нету" if not has_pin else "🟢 есть"
    
    await query.answer(
        f"🧰 В вашем сейфе хранится {format_number(balance)}ms¢\n\n"
        f"PIN-CODE - {pin_status}",
        show_alert=True
    )

async def safe_deposit_menu(query, user):
    """Меню внесения"""
    from database import get_balance_async
    
    balance = await get_balance_async(user.id)
    formatted_balance = format_number(balance)
    
    keyboard = [
        [InlineKeyboardButton("10кк", callback_data="safe_deposit_amount_10000000"),
         InlineKeyboardButton("500кк", callback_data="safe_deposit_amount_500000000"),
         InlineKeyboardButton("1ккк", callback_data="safe_deposit_amount_1000000000")],
        [InlineKeyboardButton(f"• Весь баланс ({formatted_balance})", callback_data=f"safe_deposit_amount_{balance}")],
        [InlineKeyboardButton("✏️ Ввести", callback_data="safe_deposit_custom")],
        [InlineKeyboardButton("◀️ Назад", callback_data="safe_back")]
    ]
    
    await query.edit_message_text(
        f"📤 *{user.first_name}*, введите или укажите сумму для взноса:\n"
        f"👤 ID: `{user.id}`",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def safe_withdraw_menu(query, user):
    """Меню снятия"""
    safe_data = get_safe(user.id)
    balance = safe_data['balance']
    formatted_balance = format_number(balance)
    
    keyboard = [
        [InlineKeyboardButton("10кк", callback_data="safe_withdraw_amount_10000000"),
         InlineKeyboardButton("500кк", callback_data="safe_withdraw_amount_500000000"),
         InlineKeyboardButton("1ккк", callback_data="safe_withdraw_amount_1000000000")],
        [InlineKeyboardButton(f"• Весь сейф ({formatted_balance})", callback_data=f"safe_withdraw_amount_{balance}")],
        [InlineKeyboardButton("✏️ Ввести", callback_data="safe_withdraw_custom")],
        [InlineKeyboardButton("◀️ Назад", callback_data="safe_back")]
    ]
    
    await query.edit_message_text(
        f"📥 *{user.first_name}*, выберите сумму для снятия:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def safe_deposit_amount(query, user, amount_str):
    """Обработка внесения суммы"""
    from database import has_safe_pin, get_balance_async, update_safe_balance
    from last14 import update_balance_async, format_number, safe_command
    
    global temp_safe_data
    
    try:
        amount = int(amount_str)
    except:
        await query.answer("❌ Неверная сумма", show_alert=True)
        return
    
    # Проверяем PIN если установлен
    if has_safe_pin(user.id):
        temp_safe_data[user.id] = {'action': 'deposit', 'amount': amount}
        await safe_set_pin_start(query, user, verify_mode=True)
        return
    
    # Если PIN нет, сразу выполняем
    user_balance = await get_balance_async(user.id)
    
    if amount > user_balance:
        await query.answer(f"❌ Недостаточно средств! У вас {format_number(user_balance)}ms¢", show_alert=True)
        return
    
    # Снимаем с баланса
    await update_balance_async(user.id, -amount)
    # Добавляем в сейф
    update_safe_balance(user.id, amount)
    
    await query.answer(f"✅ Вы успешно внесли в сейф {format_number(amount)}ms¢", show_alert=True)
    
    # Возвращаем в главное меню - создаем update объект
    class FakeUpdate:
        def __init__(self, user, message):
            self.effective_user = user
            self.message = message
    
    fake_update = FakeUpdate(user, query.message)
    await safe_command(fake_update, None)
    await query.message.delete()

async def safe_withdraw_amount(query, user, amount_str):
    """Обработка снятия суммы"""
    try:
        amount = int(amount_str)
    except:
        await query.answer("❌ Неверная сумма", show_alert=True)
        return

    safe_data = get_safe(user.id)
    if safe_data is None:
        await query.answer("❌ Ошибка: сейф не найден", show_alert=True)
        return
        
    if amount > safe_data['balance']:
        await query.answer(f"❌ В сейфе {format_number(safe_data['balance'])}ms¢", show_alert=True)
        return

    # Проверяем PIN если установлен
    if has_safe_pin(user.id):
        temp_safe_data[user.id] = {'action': 'withdraw', 'amount': amount}
        await safe_ask_pin(query, user, "снятия")
        return

    # Если PIN нет, сразу выполняем
    await execute_withdraw(query, user, amount)


async def safe_deposit_custom(query, user, context=None):
    """Ожидание ввода суммы для внесения"""
    global temp_safe_data
    temp_safe_data[user.id] = {'action': 'awaiting_deposit'}
    await query.edit_message_text(
        f"📤 *{user.first_name}*, введите сумму:\n\n"
        f"Поддерживается: число, всё, пол (половина баланса)",
        parse_mode='Markdown'
    )

async def safe_withdraw_custom(query, user, context=None):
    """Ожидание ввода суммы для снятия"""
    global temp_safe_data
    temp_safe_data[user.id] = {'action': 'awaiting_withdraw'}
    await query.edit_message_text(
        f"📥 *{user.first_name}*, введите сумму:\n\n"
        f"Поддерживается: число, всё, пол (половина сейфа)",
        parse_mode='Markdown'
    )

async def safe_ask_pin(query, user, action):
    """Запрос PIN-кода для операции"""
    global temp_safe_data
    temp_safe_data[user.id] = {'action': action, 'awaiting_pin': True}
    
    keyboard = [
        [InlineKeyboardButton("1️⃣", callback_data="safe_pin_digit_1"),
         InlineKeyboardButton("2️⃣", callback_data="safe_pin_digit_2"),
         InlineKeyboardButton("3️⃣", callback_data="safe_pin_digit_3")],
        [InlineKeyboardButton("4️⃣", callback_data="safe_pin_digit_4"),
         InlineKeyboardButton("5️⃣", callback_data="safe_pin_digit_5"),
         InlineKeyboardButton("6️⃣", callback_data="safe_pin_digit_6")],
        [InlineKeyboardButton("7️⃣", callback_data="safe_pin_digit_7"),
         InlineKeyboardButton("8️⃣", callback_data="safe_pin_digit_8"),
         InlineKeyboardButton("9️⃣", callback_data="safe_pin_digit_9")],
        [InlineKeyboardButton("0️⃣", callback_data="safe_pin_digit_0")],
        [InlineKeyboardButton("🔄 Очистить", callback_data="safe_pin_clear"),
         InlineKeyboardButton("✅ Подтвердить", callback_data="safe_pin_confirm")]
    ]
    
    await query.edit_message_text(
        f"🔐 *{user.first_name}*, введите PIN-код для {action}а:\n\n"
        f"Введите 4 цифры:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def safe_set_pin_start(query, user, verify_mode=False):
    """Начало установки PIN-кода"""
    global temp_pin_data
    
    temp_pin_data[user.id] = {'pin': '', 'verify_mode': verify_mode}
    
    keyboard = [
        [InlineKeyboardButton("1️⃣", callback_data="safe_pin_digit_1"),
         InlineKeyboardButton("2️⃣", callback_data="safe_pin_digit_2"),
         InlineKeyboardButton("3️⃣", callback_data="safe_pin_digit_3")],
        [InlineKeyboardButton("4️⃣", callback_data="safe_pin_digit_4"),
         InlineKeyboardButton("5️⃣", callback_data="safe_pin_digit_5"),
         InlineKeyboardButton("6️⃣", callback_data="safe_pin_digit_6")],
        [InlineKeyboardButton("7️⃣", callback_data="safe_pin_digit_7"),
         InlineKeyboardButton("8️⃣", callback_data="safe_pin_digit_8"),
         InlineKeyboardButton("9️⃣", callback_data="safe_pin_digit_9")],
        [InlineKeyboardButton("0️⃣", callback_data="safe_pin_digit_0")],
        [InlineKeyboardButton("🔄 Очистить", callback_data="safe_pin_clear"),
         InlineKeyboardButton("✅ Установить", callback_data="safe_pin_confirm")]
    ]
    
    message = f"🔐 *{user.first_name}*, установка PIN-COD'а.\n"
    message += f"👤 ID: `{user.id}`\n\n"
    
    if verify_mode:
        message = f"🔐 *{user.first_name}*, введите PIN-код для подтверждения:\n"
        message += f"👤 ID: `{user.id}`\n\n"
    else:
        message += "🔴 Для установки пин кода не забудьте сохранить его в надежном месте иначе ваш сейф будет заблокирован при четвертом неправильном вводе кода.\n"
        message += "Администрация не несет ответственность за данную систему.\n\n"
    
    message += f"Введите код через кнопки, вы ввели: `{temp_pin_data[user.id]['pin'] or '____'}`"
    
    await query.edit_message_text(
        message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def safe_pin_input(query, user, digit):
    """Ввод цифры PIN-кода"""
    global temp_pin_data
    
    if user.id not in temp_pin_data:
        return
    
    current_pin = temp_pin_data[user.id]['pin']
    if len(current_pin) < 4:
        temp_pin_data[user.id]['pin'] = current_pin + digit
    
    await safe_pin_update_message(query, user)

async def safe_pin_clear(query, user):
    """Очистить PIN-код"""
    global temp_pin_data
    
    if user.id in temp_pin_data:
        temp_pin_data[user.id]['pin'] = ''
    await safe_pin_update_message(query, user)

async def safe_pin_update_message(query, user):
    """Обновление сообщения с PIN-кодом"""
    global temp_pin_data
    
    if user.id not in temp_pin_data:
        return
    
    pin = temp_pin_data[user.id]['pin']
    masked_pin = '*' * len(pin) + '_' * (4 - len(pin))
    
    keyboard = [
        [InlineKeyboardButton("1️⃣", callback_data="safe_pin_digit_1"),
         InlineKeyboardButton("2️⃣", callback_data="safe_pin_digit_2"),
         InlineKeyboardButton("3️⃣", callback_data="safe_pin_digit_3")],
        [InlineKeyboardButton("4️⃣", callback_data="safe_pin_digit_4"),
         InlineKeyboardButton("5️⃣", callback_data="safe_pin_digit_5"),
         InlineKeyboardButton("6️⃣", callback_data="safe_pin_digit_6")],
        [InlineKeyboardButton("7️⃣", callback_data="safe_pin_digit_7"),
         InlineKeyboardButton("8️⃣", callback_data="safe_pin_digit_8"),
         InlineKeyboardButton("9️⃣", callback_data="safe_pin_digit_9")],
        [InlineKeyboardButton("0️⃣", callback_data="safe_pin_digit_0")],
        [InlineKeyboardButton("🔄 Очистить", callback_data="safe_pin_clear"),
         InlineKeyboardButton("✅ Установить", callback_data="safe_pin_confirm")]
    ]
    
    message = f"🔐 *{user.first_name}*, установка PIN-COD'а.\n\n"
    if temp_pin_data[user.id].get('verify_mode'):
        message = f"🔐 *{user.first_name}*, введите PIN-код для подтверждения:\n\n"
    else:
        message += "🔴 Для установки пин кода не забудьте сохранить его в надежном месте иначе ваш сейф будет заблокирован при четвертом неправильном вводе кода.\nАдминистрация не несет ответственность за данную систему.\n\n"
    
    message += f"Введите код через кнопки, вы ввели: `{masked_pin}`"
    
    try:
        # Пробуем отредактировать сообщение
        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    except Exception as e:
        # Если ошибка "Message is not modified", игнорируем
        if "Message is not modified" not in str(e):
            # Если ошибка с парсингом Markdown, пробуем без Markdown
            if "Can't parse entities" in str(e):
                await query.edit_message_text(
                    message.replace('*', '').replace('`', ''),
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                raise e

async def safe_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений для сейфа"""
    from database import get_balance_async, get_safe, update_safe_balance, has_safe_pin
    from last14 import update_balance_async, format_number, safe_command
    
    user = update.effective_user
    user_id = user.id
    if not update.effective_user:
        return
    
    if ('pending_password_check' in context.user_data or
        'pending_comment_check' in context.user_data or
        f'pending_activation_{user_id}' in context.user_data):
        from handlers.checks import handle_check_text_input
        await handle_check_text_input(update, context)
        return
    # Проверяем, ожидает ли пользователь ввод для сейфа
    if user_id not in temp_safe_data:
        return
    
    action_data = temp_safe_data.get(user_id, {})
    action = action_data.get('action')
    
    # Получаем текст сообщения
    text = update.message.text.strip()
    
    # Обработка внесения
    if action == 'awaiting_deposit':
        # Парсим сумму
        if text.lower() == 'всё':
            amount = await get_balance_async(user_id)
        elif text.lower() == 'пол':
            amount = await get_balance_async(user_id) // 2
        else:
            try:
                amount = int(text)
                if amount <= 0:
                    raise ValueError
            except:
                await update.message.reply_text("❌ Неверная сумма! Введите положительное число, 'всё' или 'пол'.")
                return
        
        # Проверяем баланс
        user_balance = await get_balance_async(user_id)
        if amount > user_balance:
            await update.message.reply_text(f"❌ Недостаточно средств! У вас {format_number(user_balance)}ms¢")
            return
        
        # Проверяем PIN если установлен
        if has_safe_pin(user_id):
            # Сохраняем сумму и запрашиваем PIN
            temp_safe_data[user_id] = {'action': 'deposit', 'amount': amount}
            # Отправляем сообщение с запросом PIN
            keyboard = [[InlineKeyboardButton("🔐 Ввести PIN-код", callback_data="safe_enter_pin")]]
            await update.message.reply_text(
                "🔐 Для внесения средств в сейф требуется PIN-код.\n"
                "Нажмите кнопку ниже для ввода PIN-кода:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        # Если PIN нет, выполняем операцию
        await update_balance_async(user_id, -amount)
        update_safe_balance(user_id, amount)
        
        await update.message.reply_text(f"✅ Вы успешно внесли в сейф {format_number(amount)}ms¢")
        
        # Очищаем временные данные
        temp_safe_data.pop(user_id, None)
        
        # Показываем меню сейфа
        await safe_command(update, context)
    
    # Обработка снятия
    elif action == 'awaiting_withdraw':
        safe_data = get_safe(user_id)
        
        # Парсим сумму
        if text.lower() == 'всё':
            amount = safe_data['balance']
        elif text.lower() == 'пол':
            amount = safe_data['balance'] // 2
        else:
            try:
                amount = int(text)
                if amount <= 0:
                    raise ValueError
            except:
                await update.message.reply_text("❌ Неверная сумма! Введите положительное число, 'всё' или 'пол'.")
                return
        
        # Проверяем баланс сейфа
        if amount > safe_data['balance']:
            await update.message.reply_text(f"❌ В сейфе {format_number(safe_data['balance'])}ms¢")
            return
        
        # Проверяем PIN если установлен
        if has_safe_pin(user_id):
            # Сохраняем сумму и запрашиваем PIN
            temp_safe_data[user_id] = {'action': 'withdraw', 'amount': amount}
            # Отправляем сообщение с запросом PIN
            keyboard = [[InlineKeyboardButton("🔐 Ввести PIN-код", callback_data="safe_enter_pin")]]
            await update.message.reply_text(
                "🔐 Для снятия средств из сейфа требуется PIN-код.\n"
                "Нажмите кнопку ниже для ввода PIN-кода:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        # Если PIN нет, выполняем операцию
        await update_balance_async(user_id, amount)
        update_safe_balance(user_id, -amount)
        
        await update.message.reply_text(f"✅ Вы успешно сняли {format_number(amount)}ms¢")
        
        # Очищаем временные данные
        temp_safe_data.pop(user_id, None)
        
        # Показываем меню сейфа
        await safe_command(update, context)
    
    else:
        # Если не в режиме ожидания, просто удаляем из временных данных
        temp_safe_data.pop(user_id, None)
        await update.message.reply_text("❌ Действие отменено")


async def safe_pin_confirm(query, user):
    """Подтверждение PIN-кода"""
    from database import set_safe_pin, check_safe_pin
    from last14 import safe_command

    global temp_pin_data, temp_safe_data

    if user.id not in temp_pin_data:
        return

    pin = temp_pin_data[user.id]['pin']
    verify_mode = temp_pin_data[user.id].get('verify_mode', False)

    if len(pin) != 4:
        await query.answer("❌ PIN-код должен состоять из 4 цифр!", show_alert=True)
        return

    if verify_mode:
        # Режим проверки PIN для операций
        if check_safe_pin(user.id, pin):
            action_data = temp_safe_data.get(user.id, {})
            action = action_data.get('action')
            amount = action_data.get('amount')

            if action == 'deposit':
                await execute_deposit(query, user, amount)
            elif action == 'withdraw':
                await execute_withdraw(query, user, amount)
            else:
                # Если нет действия, просто возвращаем в меню
                class FakeUpdate:
                    def __init__(self, user, message):
                        self.effective_user = user
                        self.message = message

                fake_update = FakeUpdate(user, query.message)
                await safe_command(fake_update, None)
                await query.message.delete()

            temp_safe_data.pop(user.id, None)
        else:
            await query.answer("❌ Неверный PIN-код!", show_alert=True)
            temp_safe_data.pop(user.id, None)
            temp_pin_data.pop(user.id, None)
            
            class FakeUpdate:
                def __init__(self, user, message):
                    self.effective_user = user
                    self.message = message

            fake_update = FakeUpdate(user, query.message)
            await safe_command(fake_update, None)
            await query.message.delete()
            return

        temp_pin_data.pop(user.id, None)
    else:
        # Режим установки PIN
        set_safe_pin(user.id, pin)
        await query.answer("✅ PIN-код успешно установлен!", show_alert=True)
        temp_pin_data.pop(user.id, None)
        
        class FakeUpdate:
            def __init__(self, user, message):
                self.effective_user = user
                self.message = message

        fake_update = FakeUpdate(user, query.message)
        await safe_command(fake_update, None)
        await query.message.delete()

async def execute_deposit(query, user, amount):
    """Выполнить внесение в сейф"""
    from database import get_balance_async, update_safe_balance
    from last14 import update_balance_async, format_number, safe_command
    
    user_balance = await get_balance_async(user.id)
    
    if amount > user_balance:
        await query.answer(f"❌ Недостаточно средств! У вас {format_number(user_balance)}ms¢", show_alert=True)
        return
    
    # Снимаем с баланса
    await update_balance_async(user.id, -amount)
    # Добавляем в сейф
    update_safe_balance(user.id, amount)
    
    await query.answer(f"✅ Вы успешно внесли в сейф {format_number(amount)}ms¢", show_alert=True)
    
    # Возвращаем в главное меню
    class FakeUpdate:
        def __init__(self, user, message):
            self.effective_user = user
            self.message = message
    
    fake_update = FakeUpdate(user, query.message)
    await safe_command(fake_update, None)
    await query.message.delete()

async def execute_withdraw(query, user, amount):
    """Выполнить снятие из сейфа"""
    from database import get_safe, update_safe_balance
    from last14 import update_balance_async, format_number, safe_command

    safe_data = get_safe(user.id)

    if safe_data is None:
        await query.answer("❌ Ошибка: сейф не найден", show_alert=True)
        return

    if amount > safe_data['balance']:
        await query.answer(f"❌ В сейфе {format_number(safe_data['balance'])}ms¢", show_alert=True)
        return

    # Добавляем на баланс
    await update_balance_async(user.id, amount)
    # Снимаем с сейфа
    update_safe_balance(user.id, -amount)

    await query.answer(f"✅ Вы успешно сняли {format_number(amount)}ms¢", show_alert=True)

    # Возвращаем в главное меню
    class FakeUpdate:
        def __init__(self, user, message):
            self.effective_user = user
            self.message = message

    fake_update = FakeUpdate(user, query.message)
    await safe_command(fake_update, None)
    await query.message.delete()

def parse_custom_amount(amount_str: str, user_id: int, balance_type: str = 'balance') -> int:
    """Парсит сумму из текста"""
    amount_str = amount_str.strip().lower()
    
    if amount_str == 'всё':
        if balance_type == 'balance':
            return asyncio.run(get_balance(user_id))
        else:
            safe_data = get_safe(user_id)
            return safe_data['balance']
    elif amount_str == 'пол':
        if balance_type == 'balance':
            return asyncio.run(get_balance(user_id)) // 2
        else:
            safe_data = get_safe(user_id)
            return safe_data['balance'] // 2
    else:
        try:
            return int(amount_str)
        except:
            return None

async def workcondit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Включить режим технических работ"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    # Проверка прав админа
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    # Включаем режим тех. работ
    await set_work_conditions(True, user_id)
    
    # Отправляем сообщение
    msg = await update.message.reply_text("Done.")
    
    # Удаляем сообщение пользователя
    try:
        await update.message.delete()
    except:
        pass
    
    # Удаляем наше сообщение через 2 секунды
    await asyncio.sleep(2)
    try:
        await msg.delete()
    except:
        pass

async def setcondit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выключить режим технических работ"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    # Проверка прав админа
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    # Выключаем режим тех. работ
    await set_work_conditions(False, user_id)
    
    # Отправляем сообщение
    msg = await update.message.reply_text("Done.")
    
    # Удаляем сообщение пользователя
    try:
        await update.message.delete()
    except:
        pass
    
    # Удаляем наше сообщение через 2 секунды
    await asyncio.sleep(2)
    try:
        await msg.delete()
    except:
        pass
# ==================== КОМАНДЫ ====================
async def myaction_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для 'мои акции' и других команд"""
    user = update.effective_user
    if not user:
        return
    
    await get_user_async(user.id, user.full_name, user.username)
    
    text = update.message.text.lower()
    
    if text == "мои акции":
        await my_stocks_handler(update, context, user)

async def my_stocks_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """Обработчик 'Мои акции'"""
    total_value = await get_user_portfolio_total_async(user.id)
    total_count = await get_user_portfolio_count_async(user.id)
    
    text = (
        f"💼 {user.full_name}, ваши акции:\n\n"
        f"ℹ️ Сумма всех ваших акций – {total_value}ms¢\n\n"
        f"📊 У вас {total_count} акций."
    )
    
    keyboard = [[InlineKeyboardButton("ℹ️ Просмотреть акции", callback_data="view_stocks")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup)

async def shop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда 'Магазин' или 'маг'"""
    user = update.effective_user
    if not user:
        return
    
    await get_user_async(user.id, user.full_name, user.username)
    
    text = "🛍️ Магазин:"
    keyboard = [[InlineKeyboardButton("📊 Акции", callback_data="shop_stocks")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup)

async def buyact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда покупки акций: buyact *id* *количество*"""
    user = update.effective_user
    if not user:
        return
    
    await get_user_async(user.id, user.full_name, user.username)
    
    if len(context.args) < 2:
        await update.message.reply_text("❌ Использование: buyact *id* *количество*")
        return
    
    try:
        stock_id = int(context.args[0])
        quantity = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Неверный формат. Используйте числа.")
        return
    
    if quantity <= 0:
        await update.message.reply_text("❌ Количество должно быть положительным.")
        return
    
    stock = await get_stock_async(stock_id)
    if not stock:
        await update.message.reply_text("❌ Акция не найдена.")
        return
    
    if stock['current_price'] == 0:
        await update.message.reply_text("❌ Эта акция пока недоступна для покупки.")
        return
    
    success, result = await buy_stock_async(user.id, stock_id, quantity)
    
    if success:
        await update.message.reply_text(
            f"✅ {user.full_name}, вы успешно купили {quantity} {stock['symbol']} "
            f"за {result}ms¢."
        )
    else:
        await update.message.reply_text(f"❌ {result}")

async def sellact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда продажи акций: sellact *id* *количество или 'все'*"""
    user = update.effective_user
    if not user:
        return

    await get_user_async(user.id, user.full_name, user.username)

    if len(context.args) < 2:
        await update.message.reply_text("❌ Использование: sellact *id* *количество или все*")
        return

    try:
        stock_id = int(context.args[0])  # ✅ stock_id как int

        if context.args[1].lower() == 'все':
            quantity = await get_user_stock_quantity_async(user.id, stock_id)
        else:
            quantity = int(context.args[1])
        
        # ✅ Принудительно преобразуем quantity в int
        quantity = int(quantity)  # <- Добавь эту строку!
        
    except ValueError:
        await update.message.reply_text("❌ Неверный формат количества.")
        return

    if quantity <= 0:
        await update.message.reply_text("❌ Количество должно быть положительным.")
        return

    stock = await get_stock_async(stock_id)
    if not stock:
        await update.message.reply_text("❌ Акция не найдена.")
        return

    available = await get_user_stock_quantity_async(user.id, stock_id)
    if available < quantity:
        await update.message.reply_text(
            f"❌ У вас только {available} {stock['symbol']}. "
            f"Недостаточно акций для продажи."
        )
        return

    total = stock['current_price'] * quantity

    # ✅ Формируем callback_data с принудительным int
    keyboard = [[
        InlineKeyboardButton(
            "✅ Подтверждаю",
            callback_data=f"confirm_sell_{int(stock_id)}_{int(quantity)}"  # <- Принудительно int
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"❓ {user.full_name}, вы уверенны что хотите продать {stock['name']} "
        f"в количестве {quantity} шт. за сумму {total}ms¢.",
        reply_markup=reply_markup
    )

# ==================== ОБРАБОТЧИКИ КНОПОК ====================
async def investment_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок инвестиций"""
    query = update.callback_query
    user = query.from_user
    
    if query.data == "view_stocks":
        if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user.id:
            await query.answer("🙈 Это не ваша кнопка.", show_alert=True)
            return
        
        portfolio = await get_user_portfolio_async(user.id)
        
        if not portfolio:
            await query.answer("У вас нет акций.🙁", show_alert=True)
            return
        
        context.user_data['portfolio'] = [dict(item) for item in portfolio]
        context.user_data['page'] = 0
        
        await show_portfolio_page(query, context, user)
    
    elif query.data.startswith("portfolio_page_"):
        action = query.data.replace("portfolio_page_", "")
        await handle_portfolio_pagination(query, context, user, action)
    
    elif query.data == "shop_stocks":
        stocks = await get_all_stocks_async()
        available_stocks = [s for s in stocks if s['current_price'] > 0]
        
        if not available_stocks:
            await query.answer("🛒 Сейчас нет доступных акций для покупки.", show_alert=True)
            return
        
        context.user_data['shop_stocks'] = [dict(s) for s in available_stocks]
        context.user_data['shop_page'] = 0
        
        await show_shop_page(query, context, user)
    
    elif query.data.startswith("shop_page_"):
        action = query.data.replace("shop_page_", "")
        await handle_shop_pagination(query, context, user, action)
    
    elif query.data.startswith("stock_info_"):
        stock_id = int(query.data.replace("stock_info_", ""))
        stock = await get_stock_async(stock_id)
        
        if not stock:
            await query.answer("❌ Акция не найдена", show_alert=True)
            return
        
        text = (
            f"ℹ️ Информация о акции {stock['name']}:\n\n"
            f"📊 Текущий курс — {stock['current_price']}ms¢\n\n"
            f"🆔 {stock['stock_id']}\n\n"
            f"— Для покупки введите buyact {stock['stock_id']} *кол-во*"
        )
        
        await query.edit_message_text(text)
        await query.answer()
    
    elif query.data.startswith("confirm_sell_"):
        parts = query.data.split("_")
        if len(parts) == 4:
            _, _, stock_id, quantity = parts

            try:
                stock_id = int(float(stock_id))  # сначала float, потом int
                quantity = int(float(quantity))
            except:
                await query.answer("❌ Ошибка формата данных", show_alert=True)
                return
            await confirm_sell(query, context, user, stock_id, quantity)

    elif query.data == "cancel_sell":
        await query.message.delete()
        await query.answer("❌ Продажа отменена")

async def show_portfolio_page(query, context, user):
    """Показать страницу портфеля"""
    portfolio = context.user_data.get('portfolio', [])
    page = context.user_data.get('page', 0)
    
    start = page * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    page_items = portfolio[start:end]
    total_pages = (len(portfolio) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    
    text = f"🏦 Список ваших акций:\n\n"
    
    for item in page_items:
        text += (
            f"— 🆔 {item['stock_id']} - {item['name']} : "
            f"{item['current_price']}ms¢ ({item['quantity']} шт.)\n"
        )
    
    text += f"\nℹ️ Чтобы продать акцию, напишите следующую команду\n— sellact *id* *сколько штук или все*"
    
    keyboard = []
    nav_buttons = []
    
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="portfolio_page_prev"))
    
    nav_buttons.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("➡️ Вперёд", callback_data="portfolio_page_next"))
    
    keyboard.append(nav_buttons)
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup)
    await query.answer()

async def handle_portfolio_pagination(query, context, user, action):
    """Обработка пагинации портфеля"""
    page = context.user_data.get('page', 0)
    portfolio = context.user_data.get('portfolio', [])
    total_pages = (len(portfolio) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    
    if action == "next":
        if page >= total_pages - 1:
            await query.answer("Вы итак на последнем уровне.", show_alert=True)
            return
        context.user_data['page'] = page + 1
    elif action == "prev":
        if page <= 0:
            await query.answer("Вы итак на минимальном уровне.", show_alert=True)
            return
        context.user_data['page'] = page - 1
    
    await show_portfolio_page(query, context, user)

async def show_shop_page(query, context, user):
    """Показать страницу магазина"""
    stocks = context.user_data.get('shop_stocks', [])
    page = context.user_data.get('shop_page', 0)
    
    start = page * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    page_items = stocks[start:end]
    total_pages = (len(stocks) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    
    text = "🛒 Доступные акции для покупки:\n\n"
    
    for item in page_items:
        text += f"🆔 {item['stock_id']} — {item['name']} курс {item['current_price']}ms¢.\n"
    
    keyboard = []
    row = []
    for i, item in enumerate(page_items):
        row.append(InlineKeyboardButton(
            f"🆔 {item['stock_id']}", 
            callback_data=f"stock_info_{item['stock_id']}"
        ))
        if len(row) == 3 or i == len(page_items) - 1:
            keyboard.append(row)
            row = []
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Обратно", callback_data="shop_page_prev"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("➡️ Следующая страница", callback_data="shop_page_next"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup)
    await query.answer()

async def handle_shop_pagination(query, context, user, action):
    """Обработка пагинации магазина"""
    page = context.user_data.get('shop_page', 0)
    stocks = context.user_data.get('shop_stocks', [])
    total_pages = (len(stocks) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    
    if action == "next":
        if page >= total_pages - 1:
            await query.answer("Это последняя страница", show_alert=True)
            return
        context.user_data['shop_page'] = page + 1
    elif action == "prev":
        if page <= 0:
            await query.answer("Это первая страница", show_alert=True)
            return
        context.user_data['shop_page'] = page - 1
    
    await show_shop_page(query, context, user)

async def confirm_sell(query, context, user, stock_id, quantity):
    """Подтверждение продажи"""
    if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user.id:
        await query.answer("🙈 Это не ваша кнопка.", show_alert=True)
        return

    await query.answer("⏳ Обрабатываем продажу...")

    try:
        stock = await get_stock_async(stock_id)
        if not stock:
            await query.message.reply_text("❌ Акция не найдена")
            return

        current_price = stock['current_price']
        logging.error(f"🔍 CURRENT PRICE from DB: {current_price}")

        success, result = await sell_stock_async(user.id, stock_id, quantity)
        
        logging.error(f"🔍 sell_stock_async returned: success={success}, result={result}, type={type(result)}")
        
        if success:
            total = current_price * quantity
            formatted = format_amount(total)
            
            text = f"✅ Продано {quantity} {stock['symbol']} за {formatted}ms¢."
            logging.error(f"🔍 SENDING: {text}")
            
            await query.message.reply_text(text)
            
            try:
                await query.message.delete()
            except Exception as e:
                logging.error(f"Error deleting message: {e}")
        else:
            error_text = str(result)[:100]
            await query.message.reply_text(f"❌ {error_text}")
            
    except Exception as e:
        logging.error(f"Error in confirm_sell: {e}", exc_info=True)
        try:
            await query.message.reply_text("❌ Произошла ошибка")
        except:
            pass

async def check_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    try:
        member1 = await context.bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        member2 = await context.bot.get_chat_member(chat_id=CHANNEL2_USERNAME, user_id=user_id)
        return (member1.status in ['member', 'administrator', 'creator'] and
                member2.status in ['member', 'administrator', 'creator'])
    except Exception as e:
        logging.error(f"Subscription check error: {e}")
        return False

async def check_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return False
    
    user_id = update.effective_user.id
    ban_info = await is_user_banned_async(user_id)

    if ban_info:
        if ban_info['banned_until']:
            try:
                banned_until = datetime.strptime(ban_info['banned_until'], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                banned_until = datetime.strptime(ban_info['banned_until'], '%Y-%m-%d %H:%M:%S')
            
            if banned_until < datetime.now():
                await unban_user_async(user_id)
                return False

        keyboard = [[InlineKeyboardButton("🧐 Не согласен", url="https://t.me/kleymorf")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        ban_duration = "навсегда" if not ban_info['banned_until'] else f"на {ban_info['ban_days']} дней"
        await update.message.reply_text(
            f"🚨 {update.effective_user.full_name}, вы были заблокированы в боте {ban_duration} по причине: {ban_info['ban_reason']}\n\n"
            f"❓ Не согласны с наказанием? Нажмите кнопку ниже",
            reply_markup=reply_markup
        )
        return True

    return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    args = context.args

    referrer_id = None
    check_code = None
    user_check_code = None

    if args and len(args) > 0:
        arg = args[0]

        if arg == 'bank':
            await bank_private_command(update, context)
            return

        elif arg.startswith('chk_'):
            parts = arg.split('_')
            if len(parts) == 3:
                creator_id = int(parts[1])
                check_number = int(parts[2])
                from handlers.checks import handle_check_activation
                await handle_check_activation(update, context, creator_id, check_number)
                return

    if args and args[0].startswith('pchk_'):
        parts = args[0].split('_')
        if len(parts) == 3:
            creator_id = int(parts[1])
            check_number = int(parts[2])
            from handlers.checks import handle_personal_check_activation

            await handle_personal_check_activation(update, context, creator_id, check_number)
            return

        elif arg.startswith('ref_'):
            try:
                referrer_id = int(arg.replace('ref_', ''))
                logging.info(f"User {user_id} came via referral from {referrer_id}")
            except:
                pass

        elif arg.startswith('listcheck_'):
            if user_id == MAIN_ADMIN_ID:
                await show_checklist(update, context, 1)
                return
            else:
                await update.message.reply_text("❌ Эта ссылка не для вас.")
                return

    # Получаем пользователя
    db_user = await get_user_async(user_id, user.full_name, user.username)
    is_new_user = db_user.get('registered_at') is None

    # Если есть чек
    if check_code:
        check_data = await get_check_async(check_code)

        if not check_data:
            await update.message.reply_text("❌ Чек не найден.")
            return

        if check_data['used_count'] >= check_data['max_activations']:
            await update.message.reply_text("❌ Активации закончились.")
            return

        claimed_list = check_data['claimed_by'].split(',') if check_data['claimed_by'] else []
        claimed_list = [uid.strip() for uid in claimed_list if uid.strip()]

        if str(user_id) in claimed_list:
            await update.message.reply_text("❌ Вы уже забирали данный чек.")
            return

        is_subscribed = await check_subscription(update, context, user_id)

        if is_subscribed:
            success = await use_check_async(check_code, user_id)

            if success:
                await update_balance_async(user_id, check_data['amount'])
                await update.message.reply_text(
                    f"☑ {user.full_name}, активирован чек на {check_data['amount']}ms¢."
                )
                await send_welcome(update, context)
            return
        else:
            await send_subscription_prompt(update, context)
            return

    if user_id in ADMIN_IDS:
        await send_welcome(update, context)
        return

    if referrer_id and referrer_id == user_id:
        await update.message.reply_text(
            "🦣 НЕ ПОЛУЧИЛОСЬ? Вы перешли по своей реферальной ссылке\n\n"
            "- Не обманешь систему)"
        )
        return

    is_subscribed = await check_subscription(update, context, user_id)

    if not is_subscribed:
        await send_subscription_prompt(update, context)
        return

    if is_new_user and referrer_id and referrer_id != user_id:
        referrer_exists = await get_user_async(referrer_id)
        if referrer_exists:
            await add_referral_async(referrer_id, user_id)

            reward = random.randint(100, 50000)
            await update_balance_async(user_id, reward)
            await update_balance_async(referrer_id, reward)

            await update.message.reply_text(
                f"🦣 За переход по реферальной ссылке вы получили {reward}ms¢"
            )

            try:
                await context.bot.send_message(
                    chat_id=referrer_id,
                    text=f"🦣 ЗАСКАМЛЕНО! Вы получили за реферала {reward}ms¢ от {user.full_name}"
                )
            except Exception as e:
                if "Forbidden" in str(e) or "blocked" in str(e):
                    logging.info(f"Referrer {referrer_id} blocked the bot, skipping notification")
                else:
                    logging.error(f"Failed to notify referrer {referrer_id}: {e}")

    elif not is_new_user and referrer_id:
        logging.info(f"Existing user {user_id} clicked referral link from {referrer_id} - no bonus")

    await send_welcome(update, context)

async def setlogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Установить текущий чат для логов переводов"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    # Проверка прав админа
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    chat_id = update.effective_chat.id
    chat_name = update.effective_chat.title or "Личный чат"

    # Добавляем чат в базу логов
    success = await add_log_chat(chat_id, user_id)

    if success:
        await update.message.reply_text(
            f"✅ Чат «{chat_name}» добавлен в список логов переводов!"
        )
    else:
        await update.message.reply_text("❌ Ошибка при добавлении чата.")

async def removelogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удалить текущий чат из логов переводов"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    chat_id = update.effective_chat.id

    success = await remove_log_chat(chat_id)

    if success:
        await update.message.reply_text("✅ Чат удален из списка логов переводов!")
    else:
        await update.message.reply_text("❌ Ошибка при удалении чата.")

async def send_transfer_log(context: ContextTypes.DEFAULT_TYPE, from_user: dict, to_user: dict, amount: int, transfer_hash: str, is_msg: bool = False):
    """Отправить лог о переводе во все логи-чаты"""
    try:
        # Всегда получаем свежий список чатов из БД
        log_chats = await get_log_chats()
        
        if not log_chats:
            logging.debug("No log chats configured")
            return

        from_name = from_user.get('full_name') or from_user.get('username') or f"ID: {from_user['user_id']}"
        to_name = to_user.get('full_name') or to_user.get('username') or f"ID: {to_user['user_id']}"
        
        if is_msg:
            currency = "MSG"
            emoji = "🍯"
        else:
            currency = "ms¢"
            emoji = "💰"

        print(f"🔥 send_transfer_log ВЫЗВАНА! is_msg={is_msg}")
        print(f"📤 from: {from_user}")
        print(f"📥 to: {to_user}")
        print(f"💰 amount: {amount}")
        print(f"🔑 hash: {transfer_hash}")
        print(f"📋 log_chats: {log_chats}")
        text = (
            f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji> Совершен новый перевод\n"
            f"Кто переводил: {from_name}\n"
            f"Кому: {to_name}\n"
            f"Сумма: {format_amount(amount)} {currency}\n"
            f"<tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji> Хэш перевода для ознакомления: <code>{transfer_hash}</code>"
        )

        for chat_id in log_chats:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='HTML'
                )
                logging.info(f"Transfer log sent to chat {chat_id}")
            except Exception as e:
                if "chat not found" in str(e).lower() or "forbidden" in str(e).lower():
                    # Если бот больше не в чате, удаляем его из БД
                    await remove_log_chat(chat_id)
                    logging.info(f"Removed chat {chat_id} from log chats (bot not found)")
                else:
                    logging.error(f"Failed to send log to chat {chat_id}: {e}")
    except Exception as e:
        logging.error(f"Error in send_transfer_log: {e}")

async def allchats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Рассылка во все чаты, где есть бот"""
    if not update.effective_user:
        return

    # Проверка прав админа
    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    text = update.message.text.strip()
    parts = text.split(maxsplit=1)

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ Использование: /allchats *текст*\n\n"
            "Для добавления кнопок:\n"
            "• inl|текст - обычная кнопка\n"
            "• inl|текст\"ссылка\" - кнопка с ссылкой\n\n"
            "Пример:\n"
            "/allchats Привет! inl|Нажми\"https://t.me/durov\""
        )
        return

    raw_text = parts[1]

    # Парсим кнопки
    keyboard = []
    lines = raw_text.split('\n')
    final_text_lines = []
    has_buttons = False

    for line in lines:
        if line.startswith('inl|'):
            has_buttons = True
            button_content = line.replace('inl|', '').strip()
            
            # Проверяем, есть ли ссылка в кавычках
            import re
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

    # Отправляем подтверждение
    progress_msg = await update.message.reply_text("⏳ Начинаю рассылку по всем чатам...")

    # Получаем все чаты
    chats = await get_all_chats()
    
    if not chats:
        await progress_msg.edit_text("❌ Бот не добавлен ни в один чат.")
        return

    successful = 0
    failed = 0
    total = len(chats)

    for chat_id, chat_title, chat_type in chats:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=final_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            successful += 1
        except Exception as e:
            failed += 1
            logging.error(f"Failed to send to chat {chat_id}: {e}")
            
            # Если бот больше не в чате - удаляем из БД
            if "chat not found" in str(e).lower() or "forbidden" in str(e).lower():
                def _remove():
                    with get_db() as conn:
                        conn.execute("DELETE FROM bot_chats WHERE chat_id = ?", (chat_id,))
                        conn.commit()
                await asyncio.to_thread(_remove)

        # Небольшая задержка, чтобы не спамить
        await asyncio.sleep(0.1)

    await progress_msg.edit_text(
        f"✅ Рассылка завершена!\n"
        f"📊 Всего чатов: {total}\n"
        f"✅ Успешно: {successful}\n"
        f"❌ Ошибок: {failed}"
    )

async def track_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отслеживание добавления бота в новые чаты"""
    if not update.message or not update.message.chat:
        return

    chat = update.message.chat
    chat_id = chat.id
    chat_title = chat.title or "Личный чат"
    chat_type = chat.type

    await add_bot_chat(chat_id, chat_title, chat_type)


async def send_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    try:
        keyboard = [
            [InlineKeyboardButton("➕ Добавить бота в чат", url="https://t.me/monstrminesbot?startgroup=true")],
            [InlineKeyboardButton("📰 Новости [1]", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
            [InlineKeyboardButton("📰 Новости [2]", url=f"https://t.me/{CHANNEL2_USERNAME[1:]}")],
            [InlineKeyboardButton("💬 Официальный чат", url="https://t.me/gamemonstroff")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "👋*Привет*! Я – Монстр бот💣\n\n"
            "📲 Проведи свое время с удовольствием играя в нашего бота! Тут ты сможешь насладиться множественным функционалом и реально годными играми\n\n"
            "🧐 Во что будем играть? Пиши /game для получения список игр.\n\n"
            "❓ Думаю все понятно, если остались вопросы просто напиши /help.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        if "Forbidden" in str(e) or "blocked" in str(e):
            logging.info(f"User {update.effective_user.id} blocked the bot")
        else:
            logging.error(f"Error in send_welcome: {e}")

async def games(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    await update.message.reply_text(
        "🎮 Список доступных игр:\n"
        "💣 Мины — /mines *мин* *ставка*\n"
        "💰 Золото — /gold *ставка*\n"
        "🎳 Боулинг – /bowling *ставка* *мимо,страйк*\n"
        "🏝️ Пирамида — /pyramid *ставка* *дверей (1-3)*\n"
        "🚀 Краш – /crash *ставка* *коэф*\n"
        "🎲 Дайс – /cubic *ставка*\n"
        "🎰 Рулетка – /roulette *диапазон* *ставка*\n"
        "🗼Башня – /tower *ставка* *мин*\n"
        "🪨 КНБ - /knb *ставка*\n"
        "💠 Алмазы – /diamond *ставка* *мин1-2\n"
        "🐸 Frog – /frog *ставка*\n"
        "🎲 Кости - кости *ставка*\n"
        "🎰 Барабан — /slot\n"
        "⚽ Футбол — /fb *ставка* *гол/мимо*\n"
        "🏀 Баскетбол — /bk *ставка* *гол/мимо*\n"
        "• Пример: /mines 5 1к\n"
        "• Пример: /gold 100\n"
        "• Пример: /pyramid 100 2"
    )

async def frog_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Frog (Квак)"""
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
    
    # Проверяем различные варианты команд
    cmd = parts[0].lower()
    if cmd not in ['/frog', 'квак', 'frog']:
        return

    # Если нет аргументов
    if len(parts) < 2:
        await update.message.reply_text(
            "<blockquote>ℹ️ Frog – это игра, в которой перед лягушкой расположены 4 ряда по 5 кувшинок в каждом. Вам нужно выбрать, на какой лист ей прыгнуть!</blockquote>\n\n"
            f"🤖 *{user_name}*, чтобы начать игру, используй команду:\n\n"
            "🐸 <u><i>/frog [ставка]</i></u>\n\n"
            "Пример:\n"
            "/frog 100\n"
            "Квак 100",
            parse_mode='HTML'
        )
        return

    try:
        # Получаем баланс пользователя
        db_user = await get_user_async(user_id, user.full_name, user.username)
        user_balance = db_user['balance']
        
        # Парсим ставку с поддержкой "всё"
        bet_amount = parse_bet_amount(parts[1], user_balance)

        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return

        if bet_amount > user_balance:
            await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(user_balance)}ms¢")
            return

        # Списываем ставку
        await update_balance_async(user_id, -bet_amount)

        # Генерируем позиции бомб для каждого уровня
        # Уровень 0 (стартовый) - нет бомб
        bombs_positions = {}
        for level in range(1, 5):  # 1-4 уровни
            bomb_count = FROG_BOMBS_COUNT.get(level, 0)
            if bomb_count > 0:
                # Генерируем случайные позиции бомб (0-4)
                positions = random.sample(range(5), bomb_count)
                bombs_positions[level] = positions
            else:
                bombs_positions[level] = []

        # Создаем сессию
        FROG_SESSIONS = context.bot_data.get('FROG_SESSIONS', {})
        FROG_SESSIONS[user_id] = {
            'bet': bet_amount,
            'current_level': 0,  # 0 - стартовый, 1-4 - уровни
            'opened_positions': [],  # список кортежей (level, position)
            'bombs_positions': bombs_positions,
            'user_name': user_name,
            'chat_id': update.effective_chat.id,
            'thread_id': update.effective_message.message_thread_id,
            'message_id': None,
            'status': 'active'
        }
        context.bot_data['FROG_SESSIONS'] = FROG_SESSIONS

        # Отправляем начальное игровое поле
        await send_frog_board(update, context, user_id)

    except Exception as e:
        logging.error(f"Error in frog command: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")
        if 'bet_amount' in locals():
            await update_balance_async(user_id, bet_amount)

async def send_frog_board(update, context, user_id):
    """Отправка/обновление игрового поля Frog"""
    FROG_SESSIONS = context.bot_data.get('FROG_SESSIONS', {})
    session = FROG_SESSIONS.get(user_id)
    
    if not session:
        return

    current_level = session['current_level']
    bet = session['bet']
    user_name = session['user_name']

    # Определяем текущий множитель и следующий
    current_multiplier = FROG_MULTIPLIERS.get(current_level, 1.00)
    next_multiplier = FROG_MULTIPLIERS.get(current_level + 1, 0)
    current_win = int(bet * current_multiplier)
    next_win = int(bet * next_multiplier) if next_multiplier > 0 else 0

    if current_level == 0:
        text = (
            f"🪰<b>Frog • начни игру!</b>\n"
            f"••••••••••\n"
            f"💸 Ставка: {format_amount(bet)}ms¢\n"
            f"<blockquote>🍀Сл. кувшин: x{next_multiplier} / {format_amount(next_win)}ms¢</blockquote>\n"
        )
    else:
        text = (
            f"🐸<b>Frog • игра идёт.</b>\n"
            f"••••••••••\n"
            f"💸 Ставка: {format_amount(bet)}ms¢\n"
            f"📊 Выигрыш: x{current_multiplier} / {format_amount(current_win)}ms¢\n"
            f"<blockquote>🍀Сл. кувшин: x{next_multiplier} / {format_amount(next_win)}ms¢</blockquote>\n"
        )

    # Строим игровое поле (сверху вниз: уровень 4 -> уровень 0)
    board_lines = []
    
    # Уровень 4 (верхний)
    level_4_line = await get_level_line(session, 4, FROG_MULTIPLIERS[4])
    board_lines.append(level_4_line)
    
    # Уровень 3
    level_3_line = await get_level_line(session, 3, FROG_MULTIPLIERS[3])
    board_lines.append(level_3_line)
    
    # Уровень 2
    level_2_line = await get_level_line(session, 2, FROG_MULTIPLIERS[2])
    board_lines.append(level_2_line)
    
    # Уровень 1
    level_1_line = await get_level_line(session, 1, FROG_MULTIPLIERS[1])
    board_lines.append(level_1_line)
    
    # Стартовый уровень
    start_line = await get_start_level_line(session)
    board_lines.append(start_line)
    
    text += "\n".join(board_lines)

    # Создаем клавиатуру
    keyboard = []
    
    if current_level < 4:
        row = []
        for pos in range(5):
            row.append(InlineKeyboardButton("🍀", callback_data=f"frog_cell_{user_id}_{pos}"))
        keyboard.append(row)
        
        if current_level == 0:
            keyboard.append([InlineKeyboardButton("❌ Отменить", callback_data=f"frog_cancel_{user_id}")])
        else:
            keyboard.append([InlineKeyboardButton("✅ Забрать награду", callback_data=f"frog_take_{user_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if session.get('message_id'):
        try:
            await context.bot.edit_message_text(
                text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except:
            msg = await context.bot.send_message(
                chat_id=session['chat_id'],
                text=text,
                reply_markup=reply_markup,
                message_thread_id=session['thread_id'],
                parse_mode='HTML'
            )
            session['message_id'] = msg.message_id
    else:
        msg = await context.bot.send_message(
            chat_id=session['chat_id'],
            text=text,
            reply_markup=reply_markup,
            message_thread_id=session['thread_id'],
            parse_mode='HTML'
        )
        session['message_id'] = msg.message_id
    
    context.bot_data['FROG_SESSIONS'] = FROG_SESSIONS


async def send_frog_final_board(update, context, user_id, is_win, win_amount=None, crash_level=None, crash_position=None):
    """Отправка финального поля после победы или проигрыша"""
    query = update.callback_query
    FROG_SESSIONS = context.bot_data.get('FROG_SESSIONS', {})
    session = FROG_SESSIONS.get(user_id)
    
    if not session:
        return

    bet = session['bet']
    current_level = session['current_level']

    if is_win:
        final_multiplier = FROG_MULTIPLIERS.get(current_level, 1.00)
        text = (
            f"🤑<b>Frog • Победа!</b><tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji>\n"
            f"••••••••••\n"
            f"💸 Ставка: {format_amount(bet)}ms¢\n"
            f"💰 Выигрыш: x{final_multiplier} / {format_amount(win_amount)}ms¢\n\n"
        )
        
        # Строим финальное поле для победы
        board_lines = []
        
        level_4_line = await get_final_level_line_win(session, 4, FROG_MULTIPLIERS[4])
        board_lines.append(level_4_line)
        
        level_3_line = await get_final_level_line_win(session, 3, FROG_MULTIPLIERS[3])
        board_lines.append(level_3_line)
        
        level_2_line = await get_final_level_line_win(session, 2, FROG_MULTIPLIERS[2])
        board_lines.append(level_2_line)
        
        level_1_line = await get_final_level_line_win(session, 1, FROG_MULTIPLIERS[1])
        board_lines.append(level_1_line)
        
        start_line = await get_final_start_level_line_win(session)
        board_lines.append(start_line)
        
        text += "\n".join(board_lines)
        
    else:
        text = (
            f"🌀<b>Frog • Проигрыш!</b>\n"
            f"••••••••••\n"
            f"💸 Ставка: {format_amount(bet)}ms¢\n"
            f"🍀 Пройдено: {current_level} из 4\n\n"
        )
        
        # Строим финальное поле для проигрыша
        board_lines = []
        
        level_4_line = await get_final_level_line_lose(session, 4, FROG_MULTIPLIERS[4], crash_level, crash_position)
        board_lines.append(level_4_line)
        
        level_3_line = await get_final_level_line_lose(session, 3, FROG_MULTIPLIERS[3], crash_level, crash_position)
        board_lines.append(level_3_line)
        
        level_2_line = await get_final_level_line_lose(session, 2, FROG_MULTIPLIERS[2], crash_level, crash_position)
        board_lines.append(level_2_line)
        
        level_1_line = await get_final_level_line_lose(session, 1, FROG_MULTIPLIERS[1], crash_level, crash_position)
        board_lines.append(level_1_line)
        
        start_line = await get_final_start_level_line_lose(session, crash_level, crash_position)
        board_lines.append(start_line)
        
        text += "\n".join(board_lines)
    
    # Создаем неактивную клавиатуру
    keyboard = [[InlineKeyboardButton("❌ Игра завершена", callback_data="noop")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

async def get_level_line(session, level, multiplier):
    """Получить строку для уровня"""
    line = []
    current_level = session['current_level']
    
    for pos in range(5):
        # Проверяем, открыта ли эта позиция на этом уровне
        opened = None
        for op in session['opened_positions']:
            if op['level'] == level and op['position'] == pos:
                opened = op
                break
        
        if opened:
            if opened.get('is_bomb', False):
                line.append("🌀")
            else:
                # Лягушка ТОЛЬКО на текущем уровне, и ТОЛЬКО если этот уровень уже открыт
                # И это не бомба
                if level == current_level and opened.get('is_bomb', False) == False:
                    line.append("🐸️")
                else:
                    line.append("🍀")
        else:
            # Неоткрытые позиции
            if level < current_level:
                # Пройденные уровни - показываем 🍀 (лягушки нет)
                line.append("🍀")
            elif level == current_level:
                # Текущий уровень - неоткрытые 🍀
                line.append("🍀")
            else:
                # Будущие уровни - все 🍀
                line.append("🍀")
    
    return f"{'|'.join(line)}| ({multiplier}x)"

async def get_start_level_line(session):
    """Получить строку стартового уровня"""
    current_level = session['current_level']
    
    # Начало игры - лягушка в центре
    if current_level == 0:
        return "◾️|◾️|🐸️|◾️|◾️| (1.00x)"
    
    # После первого хода - фиксированная строка, которая больше никогда не меняется
    return "◼️|◼️|🍀|◼️|◼️| (1.00x)"

async def get_final_level_line(session, level, multiplier):
    """Получить строку для уровня в финальном поле"""
    line = []
    
    for pos in range(5):
        # Проверяем, была ли здесь бомба
        is_bomb = pos in session['bombs_positions'].get(level, [])
        
        # Проверяем, открыта ли эта позиция
        opened = False
        for op in session['opened_positions']:
            if op['level'] == level and op['position'] == pos:
                opened = True
                break
        
        if is_bomb:
            line.append("🌀")
        else:
            line.append("🍀")
    
    return f"{'|'.join(line)}| ({multiplier}x)"

async def get_final_level_line_lose(session, level, multiplier, crash_level, crash_position):
    """Получить строку для уровня в финальном поле при проигрыше"""
    line = []
    
    for pos in range(5):
        # Проверяем, была ли здесь бомба
        is_bomb = pos in session['bombs_positions'].get(level, [])
        
        # Проверяем, это место, где лягушка упала
        is_crash = (level == crash_level and pos == crash_position)
        
        if is_crash:
            line.append("🔵")  # Синяя точка - место падения лягушки
        elif is_bomb:
            line.append("🌀")
        else:
            line.append("🍀")
    
    return f"{'|'.join(line)}| ({multiplier}x)"

async def get_final_start_level_line_win(session):
    """Получить строку стартового уровня в финальном поле при победе"""
    return "◼️|◼️|🍀|◼️|◼️| (1.00x)"


async def get_final_start_level_line_lose(session, crash_level, crash_position):
    """Получить строку стартового уровня в финальном поле при проигрыше"""
    # Стартовый уровень всегда одинаковый после первого хода
    return "◼️|◼️|🍀|◼️|◼️| (1.00x)"

async def get_final_level_line_win(session, level, multiplier):
    """Получить строку для уровня в финальном поле при победе"""
    line = []
    
    for pos in range(5):
        # Проверяем, была ли здесь бомба
        is_bomb = pos in session['bombs_positions'].get(level, [])
        
        if is_bomb:
            line.append("🌀")
        else:
            line.append("🍀")
    
    return f"{'|'.join(line)}| ({multiplier}x)"

async def get_final_start_level_line(session):
    """Получить строку стартового уровня в финальном поле"""
    # Стартовый уровень - показываем куда прыгнула лягушка
    # Находим позицию на 1-м уровне, куда прыгнула лягушка
    frog_pos = None
    for op in session['opened_positions']:
        if op['level'] == 1 and not op.get('is_bomb', False):
            frog_pos = op['position']
            break
    
    if frog_pos is None:
        frog_pos = 2
    
    line = []
    for pos in range(5):
        if pos == frog_pos:
            line.append("🐸️")
        else:
            # Проверяем, была ли здесь бомба на 1-м уровне
            is_bomb = pos in session['bombs_positions'].get(1, [])
            if is_bomb:
                line.append("🌀")
            else:
                line.append("🍀")
    
    return f"{'|'.join(line)}| (1.00x)"

async def frog_cell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, position: int):
    """Обработка нажатия на кувшинку"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    await safe_answer(query, "")

    FROG_SESSIONS = context.bot_data.get('FROG_SESSIONS', {})
    session = FROG_SESSIONS.get(user_id)
    
    if not session or session['status'] != 'active':
        await safe_answer(query, "❌ Игра не найдена или завершена", show_alert=True)
        return

    current_level = session['current_level']
    next_level = current_level + 1

    if next_level > 4:
        await safe_answer(query, "❌ Вы уже прошли все уровни!", show_alert=True)
        return

    # Проверяем, не открыта ли уже эта позиция
    for op in session['opened_positions']:
        if op['level'] == next_level and op['position'] == position:
            await safe_answer(query, "❌ Эта кувшинка уже открыта", show_alert=True)
            return

    # Проверяем, есть ли бомба на этой позиции
    is_bomb = position in session['bombs_positions'].get(next_level, [])
    
    # Записываем открытую позицию
    session['opened_positions'].append({
        'level': next_level,
        'position': position,
        'is_bomb': is_bomb
    })

    if is_bomb:
        # Проигрыш
        session['status'] = 'lost'
        await send_frog_final_board(update, context, user_id, is_win=False, 
                                     crash_level=next_level, crash_position=position)
        await update_user_stats_async(user_id, 0, session['bet'])
        
        # Удаляем сессию
        del FROG_SESSIONS[user_id]
        context.bot_data['FROG_SESSIONS'] = FROG_SESSIONS
        return
    
    # Безопасное нажатие - переходим на следующий уровень
    session['current_level'] = next_level
    
    # Проверяем победу (прошли все 4 уровня)
    if next_level >= 4:
        # Победа
        session['status'] = 'won'
        final_multiplier = FROG_MULTIPLIERS[4]
        win_amount = int(session['bet'] * final_multiplier)
        
        await update_balance_async(user_id, win_amount)
        await update_user_stats_async(user_id, win_amount, 0)
        
        await send_frog_final_board(update, context, user_id, is_win=True, win_amount=win_amount)
        
        # Удаляем сессию
        del FROG_SESSIONS[user_id]
        context.bot_data['FROG_SESSIONS'] = FROG_SESSIONS
        return
    
    # Обновляем поле для следующего уровня
    await send_frog_board(update, context, user_id)
    await safe_answer(query, f"✅ Переход на уровень {next_level}!")

async def frog_take_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Забрать награду"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    FROG_SESSIONS = context.bot_data.get('FROG_SESSIONS', {})
    session = FROG_SESSIONS.get(user_id)
    
    if not session or session['status'] != 'active':
        await safe_answer(query, "❌ Игра не найдена или завершена", show_alert=True)
        return

    current_level = session['current_level']
    if current_level == 0:
        await safe_answer(query, "❌ Сначала пройдите хотя бы один уровень!", show_alert=True)
        return

    current_multiplier = FROG_MULTIPLIERS.get(current_level, 1.00)
    win_amount = int(session['bet'] * current_multiplier)
    
    await update_balance_async(user_id, win_amount)
    await update_user_stats_async(user_id, win_amount, 0)
    
    text = (
        f"🤑<b>Frog • Победа!</b><tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji>\n"
        f"••••••••••\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"💰 Выигрыш: x{current_multiplier} / {format_amount(win_amount)}ms¢\n\n"
    )
    
    # Строим финальное поле
    board_lines = []
    
    level_4_line = await get_level_line(session, 4, FROG_MULTIPLIERS[4])
    board_lines.append(level_4_line)
    
    level_3_line = await get_level_line(session, 3, FROG_MULTIPLIERS[3])
    board_lines.append(level_3_line)
    
    level_2_line = await get_level_line(session, 2, FROG_MULTIPLIERS[2])
    board_lines.append(level_2_line)
    
    level_1_line = await get_level_line(session, 1, FROG_MULTIPLIERS[1])
    board_lines.append(level_1_line)
    
    start_line = await get_start_level_line(session)
    board_lines.append(start_line)
    
    text += "\n".join(board_lines)
    
    # Создаем неактивную клавиатуру
    keyboard = [[InlineKeyboardButton("✅ Награда получена", callback_data="noop")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
    
    # Удаляем сессию
    del FROG_SESSIONS[user_id]
    context.bot_data['FROG_SESSIONS'] = FROG_SESSIONS


async def frog_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Отмена игры"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    FROG_SESSIONS = context.bot_data.get('FROG_SESSIONS', {})
    session = FROG_SESSIONS.get(user_id)
    
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.", show_alert=True)
        return

    if session['current_level'] > 0:
        await safe_answer(query, "⚠️ Нельзя отменить игру после первого хода.", show_alert=True)
        return

    await update_balance_async(user_id, session['bet'])
    
    try:
        await query.message.delete()
    except:
        pass
    
    del FROG_SESSIONS[user_id]
    context.bot_data['FROG_SESSIONS'] = FROG_SESSIONS
    await safe_answer(query, "✅ Игра отменена, средства возвращены")

async def msg_transfer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для перевода MSG: msg @username количество"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    # Проверка подписки
    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    target_user = None
    amount = 0

    # Если ответ на сообщение
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user

        if len(context.args) >= 1:
            amount = int(context.args[0]) if context.args[0].isdigit() else 0
    else:
        # Формат: msg @username 100
        if len(context.args) < 2:
            await update.message.reply_text(
                "<blockquote>ℹ️ Для перевода MSG, следуй инструкции:</blockquote>\n\n"
                f"<b><u>{user_name}, для перевода MSG следуй данному формату –\n"
                f"msg *username* *кол-во*</u></b>\n\n"
                f"Пример: msg @durov 10",
                parse_mode='HTML'
            )
            return

        target_identifier = context.args[0]
        amount_str = context.args[1]

        if not amount_str.isdigit():
            await update.message.reply_text("❌ Неверный формат суммы.")
            return

        amount = int(amount_str)

        if target_identifier.startswith('@'):
            username = target_identifier[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь {target_identifier} не найден.")
                return
            target_id = user_data['user_id']
            target_name = user_data['full_name'] or username
        else:
            try:
                target_id = int(target_identifier)
                target_data = await get_user_async(target_id)
                target_name = target_data.get('full_name') or f"ID: {target_id}"
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    # Добавляем проверку минимальной суммы
    if amount < 10:
        await update.message.reply_text("❌ Минимальная сумма перевода: 10 MSG")
        return

    if target_user.id == user_id:
        await update.message.reply_text("❌ Нельзя перевести MSG самому себе.")
        return

    # Проверяем баланс MSG
    db_user = await get_user_async(user_id)
    msg_balance = db_user.get('msg_balance', 0)

    if msg_balance < amount:
        await update.message.reply_text(
            f"❌ Недостаточно MSG. Ваш баланс: {format_amount(msg_balance)} MSG"
        )
        return

    # ===== РАСЧЁТ КОМИССИИ (НОВЫЙ КОД) =====
    settings = await get_user_settings_async(user_id)
    is_vip = await is_vip_user_async(user_id)
    
    if is_vip:
        commission_rate = 0.01
        if settings.get('transfer_commission', 1) == 0:
            commission = 0
        else:
            commission = int(amount * commission_rate)
    else:
        commission_rate = 0.10
        commission = int(amount * commission_rate)
    
    recipient_amount = amount - commission
    # ===== КОНЕЦ РАСЧЁТА КОМИССИИ =====

    transfer_id = generate_transfer_hash()

    target_mention = f"<a href='tg://user?id={target_user.id}'>{target_user.full_name}</a>"

    pending_msg_transfers = context.bot_data.get('pending_msg_transfers', {})
    pending_msg_transfers[transfer_id] = {
        'from_id': user_id,
        'from_name': user.full_name,
        'to_id': target_user.id,
        'to_name': target_user.full_name,
        'amount': amount,
        'commission': commission,
        'recipient_amount': recipient_amount,
        'time': time.time()
    }
    context.bot_data['pending_msg_transfers'] = pending_msg_transfers

    # Твой текст, добавил только комиссию
    text = (
        f"<tg-emoji emoji-id='5402418909557053333'>🍯</tg-emoji> {user_name}, вы хотите перевести {amount} MSG пользователю {target_mention}\n\n"
        f"🧾 Комиссия {int(commission_rate*100)}% ({commission} MSG)"
    )

    keyboard = [[InlineKeyboardButton("☑ Подтверждаю", callback_data=f"confirm_msg_{transfer_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

async def confirm_msg_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE, transfer_id):
    """Подтверждение перевода MSG"""
    query = update.callback_query
    user_id = query.from_user.id

    try:
        # Получаем данные из bot_data
        pending_msg_transfers = context.bot_data.get('pending_msg_transfers', {})

        if transfer_id not in pending_msg_transfers:
            await safe_answer(query, "❌ Перевод не найден или устарел.", show_alert=True)
            return

        transfer = pending_msg_transfers[transfer_id]

        if user_id != transfer['from_id']:
            await safe_answer(query, "🙈 Это не ваша кнопка!", show_alert=True)
            return

        # Проверяем баланс еще раз
        db_user = await get_user_async(user_id)
        msg_balance = db_user.get('msg_balance', 0)

        if msg_balance < transfer['amount']:
            await safe_answer(query, "❌ Недостаточно MSG.", show_alert=True)
            return

        # Выполняем перевод (переводим сумму с учётом комиссии)
        success, result = await transfer_msg_async(
            transfer['from_id'],
            transfer['to_id'],
            transfer['recipient_amount']  # изменено
        )

        if not success:
            await safe_answer(query, f"❌ {result}", show_alert=True)
            return

        print(f"✅ Перевод MSG завершен, вызываю send_transfer_log")
        # Обновляем сообщение (твой текст, добавил комиссию)
        text = (
            f"<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji> {transfer['from_name']}, вы успешно перевели {transfer['recipient_amount']} MSG пользователю {transfer['to_name']}\n"
            f"🧾 Комиссия съела {transfer['commission']} MSG\n"
            f"👨‍💻 Hash: {transfer_id}"
        )

        await query.edit_message_text(
            text,
            parse_mode='HTML'
        )

        # Уведомляем получателя (твой текст, добавил комиссию)
        try:
            await context.bot.send_message(
                chat_id=transfer['to_id'],
                text=f"<tg-emoji emoji-id='5402418909557053333'>🍯</tg-emoji> {transfer['from_name']} перевел вам {transfer['recipient_amount']} MSG.",
                parse_mode='HTML'
            )
        except Exception as e:
            if "Forbidden" not in str(e):
                logging.error(f"Failed to notify recipient: {e}")

        # 👇 ОТПРАВЛЯЕМ ЛОГ О ПЕРЕВОДЕ MSG
        try:
            from_user = await get_user_async(transfer['from_id'])
            to_user = await get_user_async(transfer['to_id'])
            await send_transfer_log(
                context,
                from_user,
                to_user,
                transfer['amount'],
                transfer_id,
                is_msg=True,
                commission=transfer['commission']  # добавил commission
            )
        except Exception as e:
            logging.error(f"Error sending MSG transfer log: {e}")

        # Удаляем перевод
        del pending_msg_transfers[transfer_id]
        context.bot_data['pending_msg_transfers'] = pending_msg_transfers
        await safe_answer(query, "✅ Перевод выполнен!")

    except Exception as e:
        logging.error(f"Error in confirm_msg_transfer: {e}")
        await safe_answer(query, "❌ Произошла ошибка.", show_alert=True)

async def ucheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для создания персонального чека /ucheck @username сумма"""
    user = update.effective_user
    if not user:
        return

    # Если в группе - отправляем в ЛС
    if update.effective_chat.type in ["group", "supergroup"]:
        keyboard = [[InlineKeyboardButton(
            "📱 Перейти в ЛС",
            url=f"https://t.me/{context.bot.username}?start=create_check"
        )]]
        await update.message.reply_text(
            f"🧾 {user.full_name}, для продолжения перейди в ЛС.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # Проверяем аргументы
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            f"<b>⚠️ Неверный формат!</b>\n"
            f"/ucheck [username/id] [сумма]\n\n"
            f"Пример: /ucheck @durov 20кк\n"
            f"Пример: /ucheck 123456789 1000000",
            parse_mode='HTML'
        )
        return

    # Парсим получателя
    target_input = context.args[0]
    target_user_id = None
    target_username = None
    
    # Если это username (начинается с @)
    if target_input.startswith('@'):
        target_username = target_input[1:]
        # Пытаемся найти пользователя
        try:
            chat = await context.bot.get_chat(target_input)
            target_user_id = chat.id
            target_username = chat.username or target_username
        except:
            await update.message.reply_text(f"❌ Пользователь {target_input} не найден!")
            return
    else:
        # Это ID
        try:
            target_user_id = int(target_input)
        except:
            await update.message.reply_text("❌ Неверный формат ID!")
            return

    # Парсим сумму
    try:
        amount_str = context.args[1].lower()
        amount = parse_amount_with_suffix(amount_str)
        
        if amount <= 0 or amount > 100_000_000_000:
            await update.message.reply_text("Максимальная сумма чека: 100.000.000.000ms¢ 🔴")
            return
    except:
        await update.message.reply_text("❌ Неверная сумма!")
        return

    # Проверяем баланс
    db_user = await get_user_async(user.id, user.full_name, user.username)
    balance = db_user.get('balance', 0)

    if balance < amount:
        await update.message.reply_text(f"❌ Недостаточно средств! Баланс: {format_amount(balance)}ms¢")
        return

    # Списываем средства
    await update_balance_async(user.id, -amount)

    # Создаём персональный чек
    from handlers.checks import create_personal_check_async
    check_number, success = await create_personal_check_async(
        user.id, target_user_id, target_username, amount
    )

    if not success:
        await update_balance_async(user.id, amount)
        await update.message.reply_text("❌ Ошибка при создании чека!")
        return

    # Получаем информацию о получателе
    target_name = target_username or f"ID{target_user_id}"
    try:
        target_chat = await context.bot.get_chat(target_user_id)
        target_name = target_chat.first_name or target_name
    except:
        pass

    check_link = f"https://t.me/{context.bot.username}?start=pchk_{user.id}_{check_number}"
    formatted_amount = format_amount(amount)

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 Копировать ссылку", callback_data=f"copy_personal_link_{user.id}_{check_number}"),
            InlineKeyboardButton("🔑 Установить пароль", callback_data=f"set_personal_password_{user.id}_{check_number}")
        ],
        [
            InlineKeyboardButton("💭 Комментарий", callback_data=f"set_personal_comment_{user.id}_{check_number}")
        ]
    ])

    await update.message.reply_text(
        f"✅<b> ПЕРСОНАЛЬНЫЙ ЧЕК #{check_number} СОЗДАН!</b>\n"
        f"•••••••••••\n"
        f"💎 Чек на <b>{formatted_amount}</b>ms¢ для <b>{target_name}</b>\n\n"
        f"🔗 Скопируй ссылку, чтобы отправить чек:\n"
        f"<code>{check_link}</code>\n\n"
        f"<blockquote>⚠️ Этот чек может активировать только {target_name}!</blockquote>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

async def give_msg_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админская команда для выдачи MSG: !gmsg @username 100"""
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    target_id = None
    target_name = None
    amount = 0

    # Если ответ на сообщение
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.full_name
        if len(context.args) >= 1:
            amount = int(context.args[0]) if context.args[0].isdigit() else 0
    else:
        if len(context.args) < 2:
            await update.message.reply_text("❌ Использование: !gmsg *@username* *кол-во*")
            return
        
        target = context.args[0]
        amount_str = context.args[1]
        
        if not amount_str.isdigit():
            await update.message.reply_text("❌ Неверный формат суммы.")
            return
            
        amount = int(amount_str)
        
        if target.startswith('@'):
            username = target[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                return
            target_id = user_data['user_id']
            target_name = user_data['full_name'] or username
        else:
            try:
                target_id = int(target)
                user_data = await get_user_async(target_id)
                target_name = user_data.get('full_name') or f"ID: {target_id}"
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    if amount <= 0:
        await update.message.reply_text("❌ Неверная сумма.")
        return

    # Выдаем MSG
    success = await update_user_msg_async(target_id, amount)
    
    if success:
        await update.message.reply_text(
            f"<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji> Успешно выдано {amount} MSG.\n"
            f"Получатель — {target_name}.",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text("❌ Ошибка при выдаче MSG.")

async def take_msg_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админская команда для забора MSG: !tmsg @username 100"""
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    target_id = None
    target_name = None
    amount = 0

    # Если ответ на сообщение
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.full_name
        if len(context.args) >= 1:
            amount = int(context.args[0]) if context.args[0].isdigit() else 0
    else:
        if len(context.args) < 2:
            await update.message.reply_text("❌ Использование: !tmsg *@username* *кол-во*")
            return
        
        target = context.args[0]
        amount_str = context.args[1]
        
        if not amount_str.isdigit():
            await update.message.reply_text("❌ Неверный формат суммы.")
            return
            
        amount = int(amount_str)
        
        if target.startswith('@'):
            username = target[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                return
            target_id = user_data['user_id']
            target_name = user_data['full_name'] or username
        else:
            try:
                target_id = int(target)
                user_data = await get_user_async(target_id)
                target_name = user_data.get('full_name') or f"ID: {target_id}"
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    if amount <= 0:
        await update.message.reply_text("❌ Неверная сумма.")
        return

    # Забираем MSG (отрицательная сумма)
    success = await update_user_msg_async(target_id, -amount)
    
    if success:
        await update.message.reply_text(
            f"<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji> Успешно списано {amount} MSG.\n"
            f"Пользователь — {target_name}.",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text("❌ Ошибка при списании MSG (возможно недостаточно средств).")
# ==================== ПРОМОКОДЫ ====================
async def setpromo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создание промокода: /setpromo активаций награда название"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id

    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if len(context.args) < 3:
        await update.message.reply_text(
            "❌ Использование: /setpromo *активаций* *награда* *название*\n"
            "Пример: /setpromo 100 10к пятница"
        )
        return

    try:
        activations = int(context.args[0])
        reward = parse_amount(context.args[1])
        name = ' '.join(context.args[2:])

        if activations <= 0 or reward <= 0:
            await update.message.reply_text("❌ Неверные значения.")
            return

        if not name:
            await update.message.reply_text("❌ Укажите название промокода.")
            return

    except Exception as e:
        logging.error(f"Error parsing promo args: {e}")
        await update.message.reply_text("❌ Неверный формат.")
        return

    # ВАЖНО: нормализуем название перед сохранением
    normalized_name = name.strip().lower()
    logging.info(f"Creating promo: original='{name}', normalized='{normalized_name}', activations={activations}, reward={reward}")

    promo_id = await create_promo_async(activations, reward, normalized_name, user_id)

    if promo_id:
        formatted_reward = format_amount(reward)
        # В ответе показываем оригинальное название (как ввёл пользователь)
        await update.message.reply_text(
            f"📃 Промокод «<b>{name}</b>» на <b>{activations}</b> активаций был создан!\n"
            f"💰 Награда: {formatted_reward}ms¢\n"
            f"🔍 Для активации используйте: промо {name}",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text("❌ Ошибка при создании промокода. Возможно, такое название уже существует.")

async def checkprom_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр всех промокодов"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return
    
    promos = await get_all_promos_async()
    
    if not promos:
        await update.message.reply_text("📃 Промокодов пока нет.")
        return
    
    text = "📃 <b>Список промокодов:</b>\n\n"
    
    for p in promos:
        formatted_reward = format_amount(p['reward_amount'])
        text += f"🆔 <b>{p['id']}</b>: <code>{p['code']}</code> - {p['used_count']}/{p['max_activations']} акт. | {formatted_reward}ms¢\n"
    
    await update.message.reply_text(text, parse_mode='HTML')

async def delpromo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление промокода по ID"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("У вас нет прав на выполнение этой команды.")
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("Использование: /delpromo *id*")
        return
    
    try:
        promo_id = int(context.args[0])
    except:
        await update.message.reply_text("Неверный ID.")
        return
    
    promo = await get_promo_by_id_async(promo_id)
    if not promo:
        await update.message.reply_text("Промокод с таким ID не найден.")
        return
    
    success = await delete_promo_async(promo_id)
    
    if success:
        await update.message.reply_text(
            f"✅ Промокод «<b>{promo['code']}</b>» (ID: {promo_id}) удалён.",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text("Ошибка при удалении промокода.")

async def promo_activate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Активация промокода: промо название"""
    if not update.effective_user:
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    # Проверяем подписку
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    if not context.args:
        await update.message.reply_text("Использование: промо *название*")
        return

    promo_code = ' '.join(context.args).strip()
    normalized_code = promo_code.lower().strip()

    logging.info(f"Активация промокода: original='{promo_code}', normalized='{normalized_code}', пользователем {user_id}")

    # Импортируем функции из database
    from database import get_promo_async, use_promo_async, check_user_promo_async
    
    # Получаем промокод
    promo = await get_promo_async(normalized_code)

    if not promo:
        await update.message.reply_text(f"{user_name}, данного промокода не существует.")
        return

    # Проверяем, не активировал ли уже пользователь этот промокод
    already_used = await check_user_promo_async(promo['id'], user_id)
    if already_used:
        await update.message.reply_text(f"{user_name}, ты уже активировал этот промокод ☑")
        return

    # Проверяем, не закончились ли активации
    if promo['used_count'] >= promo['max_activations']:
        await update.message.reply_text(f"{user_name}, активации закончились.")
        return

    # Активируем промокод
    success, result = await use_promo_async(normalized_code, user_id)

    if not success:
        if result == "not_found":
            await update.message.reply_text(f"{user_name}, данного промокода не существует.")
        elif result == "no_activations":
            await update.message.reply_text(f"{user_name}, активации закончились.")
        elif result == "already_used":
            await update.message.reply_text(f"{user_name}, ты уже активировал этот промокод ☑")
        else:
            await update.message.reply_text(f"{user_name}, ошибка при активации промокода.")
        return

    reward_amount = result
    formatted_reward = format_amount(reward_amount)

    # НАЧИСЛЯЕМ НАГРАДУ (так как use_promo_async не начисляет автоматически)
    await update_balance_async(user_id, reward_amount)

    await update.message.reply_text(
        f"{user_name},<i> ты успешно активировал промокод «<b>{promo_code}</b>» и получил {formatted_reward}ms¢!</i>",
        parse_mode='HTML'
    )

async def mute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для мута пользователя"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("❌ Эта команда работает только в группах.")
        return

    try:
        chat_member = await context.bot.get_chat_member(chat_id, user_id)
        if chat_member.status not in ['administrator', 'creator']:
            await update.message.reply_text("❌ Эта команда только для администраторов.")
            return
    except Exception as e:
        logging.error(f"Error checking admin status: {e}")
        await update.message.reply_text("❌ Не удалось проверить права.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Ответьте на сообщение пользователя, которого хотите заглушить.")
        return

    target_user = update.message.reply_to_message.from_user
    target_id = target_user.id
    target_name = target_user.full_name

    try:
        target_member = await context.bot.get_chat_member(chat_id, target_id)
        if target_member.status in ['administrator', 'creator']:
            await update.message.reply_text("❌ Нельзя заглушить администратора.")
            return
    except Exception as e:
        logging.error(f"Error checking target status: {e}")

    args = context.args if context.args else []
    
    logging.info(f"Mute args: {args}")

    mute_time = 10

    if args:
        try:
            time_str = ' '.join(args).lower()
            logging.info(f"Time string: {time_str}")
            
            numbers = re.findall(r'\d+', time_str)
            
            if numbers:
                mute_time = int(numbers[0])
                
                if 'час' in time_str or 'часа' in time_str or 'часов' in time_str:
                    mute_time = mute_time * 60
                    
        except Exception as e:
            logging.error(f"Error parsing mute time: {e}")
            mute_time = 10

    if mute_time > 1440:
        mute_time = 1440
        
    logging.info(f"Final mute time: {mute_time} minutes")

    try:
        from datetime import datetime, timedelta
        until_date = datetime.now() + timedelta(minutes=mute_time)
        
        await context.bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=target_id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until_date
        )

        if mute_time % 10 == 1 and mute_time % 100 != 11:
            minutes_text = "минуту"
        elif 2 <= mute_time % 10 <= 4 and (mute_time % 100 < 10 or mute_time % 100 >= 20):
            minutes_text = "минуты"
        else:
            minutes_text = "минут"

        await update.message.reply_text(
            f"🔇 <b>{target_name}</b> был заглушён на <b>{mute_time}</b> {minutes_text}.",
            parse_mode='HTML'
        )

    except Exception as e:
        logging.error(f"Error muting user: {e}")
        await update.message.reply_text("❌ Не удалось заглушить пользователя. Убедитесь, что бот является администратором.")

async def keys_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать количество святых ключей"""
    user = update.effective_user
    
    keys = await get_easter_keys(user.id)
    
    await update.message.reply_text(
        f"🔑 <b>{user.first_name}</b>, у вас <b>{keys}</b> святых ключей!",
        parse_mode='HTML'
    )

from datetime import datetime

async def egg_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в яйцо"""
    user = update.effective_user

    # Проверяем кулдаун
    can_play, remaining = await check_egg_cooldown(user.id)

    if not can_play:
        minutes = remaining // 60
        seconds = remaining % 60
        await update.message.reply_text(
            f"🥚 <b>{user.first_name}</b>, вы сможете кинуть яйцо через {minutes} мин {seconds} сек!",
            parse_mode='HTML'
        )
        return

    # Проверяем, есть ли ответ на сообщение
    if not update.message.reply_to_message:
        await update.message.reply_text(
            f"🥚 <b>{user.first_name}</b>, чтобы кинуть яйцо, ответь на сообщение пользователя!",
            parse_mode='HTML'
        )
        return

    # Получаем цель
    target_user = update.message.reply_to_message.from_user

    # Нельзя кидать в себя
    if target_user.id == user.id:
        await update.message.reply_text(
            f"🥚 <b>{user.first_name}</b>, нельзя кидать яйцо в себя!",
            parse_mode='HTML'
        )
        return

    # Рандомный результат: 1 - проиграл кидающий, 2 - проиграл цель
    result = random.randint(1, 2)

    # Сообщения при проигрыше кидающего
    lose_messages = [
        f"🐣 <b>{user.first_name}</b>, ваше яйцо разбилось... И оттуда вылупился птенчик, какая милота!",
        f"🍳 <b>{user.first_name}</b>, в смятку! Ваше яйцо разбилось!",
        f"💔 <b>{user.first_name}</b>, яйцо треснуло... Не повезло!",
        f"🥚💥 <b>{user.first_name}</b>, яйцо разбилось вдребезги!"
    ]

    # Сообщения при победе
    win_messages = [
        f"🥚 <b>{user.first_name}</b>, ваше яйцо оказалось сильнее!\n\n🔑 Вы получили",
        f"🥚 <b>{user.first_name}</b>, ваше яйцо непобедимо!\n\n🔑 Вы получили",
        f"🛡️ <b>{user.first_name}</b>, яйцо-танк! Соперник разбит!\n\n🔑 Вы получили",
        f"⚡ <b>{user.first_name}</b>, критический удар! Яйцо соперника разбилось!\n\n🔑 Вы получили"
    ]

    if result == 1:
        # Проиграл тот кто кидал
        await update.message.reply_text(random.choice(lose_messages), parse_mode='HTML')
        # ✅ Обновляем кулдаун ТОЛЬКО после игры
        await update_egg_cooldown(user.id)

    else:
        # Проиграл тот в кого кидали - кидающий выиграл
        keys_won = random.randint(1, 5)
        await add_easter_keys(user.id, keys_won)

        win_text = random.choice(win_messages) + f" <b>{keys_won}</b> святых ключей!"
        await update.message.reply_text(win_text, parse_mode='HTML')
        
        # ✅ Обновляем кулдаун ТОЛЬКО после игры
        await update_egg_cooldown(user.id)

        # Записываем в историю
        async with aiosqlite.connect('data/bot.db') as db:
            await db.execute('''
                INSERT INTO easter_games (winner_id, loser_id, keys_won)
                VALUES (?, ?, ?)
            ''', (user.id, target_user.id, keys_won))
            await db.commit()

async def easter_top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ пользователей по святым ключам"""
    top_users = await get_easter_top(10)
    
    if not top_users:
        await update.message.reply_text("🥚 Пока никто не получил святые ключи!")
        return
    
    text = "🥚 <b>Топ пользователей по святым ключам:</b>\n\n"
    
    emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    
    for i, (user_id, keys) in enumerate(top_users):
        # Получаем имя пользователя
        try:
            user = await context.bot.get_chat(user_id)
            name = user.first_name
        except:
            name = f"ID{user_id}"
        
        emoji = emojis[i] if i < len(emojis) else f"{i+1}️⃣"
        text += f"{emoji} <b>{name}</b> – {keys}🔑\n"
    
    await update.message.reply_text(text, parse_mode='HTML')

async def kick_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для кика пользователя"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    try:
        chat_member = await context.bot.get_chat_member(chat_id, user_id)
        if chat_member.status not in ['administrator', 'creator']:
            await update.message.reply_text("Эта команда только для администраторов.")
            return
    except:
        await update.message.reply_text("Не удалось проверить права.")
        return

    target_id = None
    target_name = None

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.full_name
    else:
        if not context.args:
            await update.message.reply_text(
                "Использование:\n"
                "• Ответьте на сообщение: /кик\n"
                "• Или укажите: /кик *@username* или /кик *id*"
            )
            return
        
        target = context.args[0]
        
        if target.startswith('@'):
            username = target[1:]
            await update.message.reply_text("Поиск по username временно недоступен. Используйте ответ на сообщение.")
            return
        else:
            try:
                target_id = int(target)
                try:
                    chat_member = await context.bot.get_chat_member(chat_id, target_id)
                    target_name = chat_member.user.full_name
                except:
                    target_name = f"ID: {target_id}"
            except:
                await update.message.reply_text("Неверный формат.")
                return

    try:
        target_member = await context.bot.get_chat_member(chat_id, target_id)
        if target_member.status in ['administrator', 'creator']:
            await update.message.reply_text("Нельзя кикнуть администратора.")
            return
    except:
        pass

    try:
        await context.bot.ban_chat_member(chat_id, target_id)
        await context.bot.unban_chat_member(chat_id, target_id)
        
        await update.message.reply_text(
            f"<blockquote>👢 {target_name} был выпнут из беседы.</blockquote>",
            parse_mode='HTML'
        )
        
    except Exception as e:
        logging.error(f"Error kicking user: {e}")
        await update.message.reply_text("Не удалось кикнуть пользователя.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    keyboard = [
        [
            InlineKeyboardButton("💡 Основные", callback_data="help_basic"),
            InlineKeyboardButton("🎮 Игры", callback_data="help_games")
        ],
        [
            InlineKeyboardButton("🔘 Другое", callback_data="help_other"),
            InlineKeyboardButton("📕 Правила", callback_data="help_rules")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🗂 Помощь по боту:",
        reply_markup=reply_markup
    )

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    keyboard = [
        [InlineKeyboardButton("🆘 Помощь", url="https://t.me/ahpeplov")],
        [InlineKeyboardButton("🧐 Др. вопросы", url="https://t.me/kleymorf")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🛡️ Мы всегда на страже помощи!",
        reply_markup=reply_markup
    )

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    db_user = await get_user_async(user.id, user.full_name, user.username)
    msg_amount = db_user.get('msg_balance', 0)
    
    keyboard = [
        [
            InlineKeyboardButton("🎁 Бонус", callback_data=f"bonus_{user_id}"),
            InlineKeyboardButton("🎰 Барабан", callback_data=f"slot_{user_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # 👇 ТОЧНО ТАК, КАК ВЫ ХОТИТЕ
    text = (
        f"💎 {user.full_name}, ваш баланс: {format_amount(db_user['balance'])}ms¢\n"
        f"<tg-emoji emoji-id='5402418909557053333'>🍯</tg-emoji> MSG: {format_amount(msg_amount)}"
    )
    is_vip = await is_vip_user_async(user_id)
    if is_vip:
        text += "\n😎 V.I.P"

    await update.message.reply_text(
        text=text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    stats = await get_user_stats_async(user_id)
    db_user = await get_user_async(user_id, user.full_name, user.username)
    msg_amount = db_user.get('msg_balance', 0)
    referral_count = await get_user_referral_count_async(user_id)

    total_games = stats['games_played']

    keyboard = [
        [
            InlineKeyboardButton("🎁 Бонус", callback_data=f"bonus_{user_id}"),
            InlineKeyboardButton("🎰 Барабан", callback_data=f"slot_{user_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    text = (
        f"👤 {user.full_name}, ваш профиль:\n\n"
        f"🆔 {user_id}\n\n"
        f"💰 Общая сумма выигрышей: {format_amount(stats['total_win'])}ms¢\n\n"
        f"💸 Общая сумма проигрышей: {format_amount(stats['total_loss'])}ms¢\n\n"
        f"<tg-emoji emoji-id='5402418909557053333'>🍯</tg-emoji> MSG: {format_amount(msg_amount)}\n\n"
        f"Ваш баланс: {format_amount(db_user['balance'])}ms¢\n\n"
        f"За все время вы сыграли в {total_games} игр.\n\n"
        f"🦣 Приглашено мамонтов: {referral_count}"
    )

    await update.message.reply_text(
        text=text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

async def bonus_command(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!")
        return

    if await check_ban(update, context):
        return

    user = query.from_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
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

# ==================== КОНСТАНТЫ БАРАБАНА ====================
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

# ==================== КОМАНДА БАРАБАНА (вызывается из кнопки) ====================
async def slot_command(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Начало игры в барабан (первый экран)"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка!")
        return

    if await check_ban(update, context):
        return

    user = query.from_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
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

    if await check_ban(update, context):
        return

    user = query.from_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
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
    """Анимация для конкретного пользователя (выполняется в отдельной задаче)"""
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

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный топ пользователей"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    await show_global_top(update.message, context, user)

async def chat_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ пользователей в чате"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    await show_chat_top(update.message, context, user)

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

async def easter_exchange_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для открытия обменника"""
    user = update.effective_user
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Обменник", callback_data="easter_exchange_open")
    ]])
    
    await update.message.reply_text(
        f"⛪ <b>{user.first_name}</b>, ивент пасха окончен!",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


async def easter_exchange_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню обменника"""
    query = update.callback_query
    user = query.from_user
    
    await query.answer()
    
    # Получаем количество ключей пользователя
    keys = await get_easter_keys(user.id)
    
    text = (
        f"<b>{user.first_name}</b>, пасхальный обменник\n"
        f"••••••••••••\n"
        f"🔑 У вас ключей: <b>{keys}</b>\n\n"
        f"🥚 Ничего • 1 🔑\n"
        f"🍯 1 MSG • 4 🔑\n"
        f"🍯 3 MSG • 19 🔑\n"
        f"🍯 5 MSG • 29 🔑\n"
        f"🍯 10 MSG • 49 🔑\n"
        f"🥚 Кейс «Пасхальный» • 9 🔑\n"
        f"1.000.000msCoin • 19 🔑\n"
        f"5.000.000msCoin • 49 🔑\n"
        f"10.000.000msCoin • 99 🔑\n"
        f"25.000.000msCoin • <s>149</s> 119 🔑"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🥚 Ничего • 1 🔑", callback_data="exchange_nothing_1")],
        [InlineKeyboardButton("🍯 1 MSG • 4 🔑", callback_data="exchange_msg_1_4")],
        [InlineKeyboardButton("🍯 3 MSG • 19 🔑", callback_data="exchange_msg_3_19")],
        [InlineKeyboardButton("🍯 5 MSG • 29 🔑", callback_data="exchange_msg_5_29")],
        [InlineKeyboardButton("🍯 10 MSG • 49 🔑", callback_data="exchange_msg_10_49")],
        [InlineKeyboardButton("🥚 Кейс «Пасхальный» • 9 🔑", callback_data="exchange_case_9")],
        [InlineKeyboardButton("1.000.000msCoin • 19 🔑", callback_data="exchange_coin_1m_19")],
        [InlineKeyboardButton("5.000.000msCoin • 49 🔑", callback_data="exchange_coin_5m_49")],
        [InlineKeyboardButton("10.000.000msCoin • 99 🔑", callback_data="exchange_coin_10m_99")],
        [InlineKeyboardButton("25.000.000msCoin • 119 🔑", callback_data="exchange_coin_25m_119")],
        [InlineKeyboardButton("◀️ Назад", callback_data="exchange_back")]
    ])
    
    await query.edit_message_text(
        text=text,
        reply_markup=keyboard,
        parse_mode='HTML'
    )


async def handle_easter_exchange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка обмена ключей"""
    query = update.callback_query
    user = query.from_user
    data = query.data
    
    await query.answer()
    
    # Словарь с предметами обмена
    exchanges = {
        "exchange_nothing_1": {"name": "Ничего", "keys": 1, "type": "nothing", "value": 0},
        "exchange_msg_1_4": {"name": "1 MSG", "keys": 4, "type": "msg", "value": 1},
        "exchange_msg_3_19": {"name": "3 MSG", "keys": 19, "type": "msg", "value": 3},
        "exchange_msg_5_29": {"name": "5 MSG", "keys": 29, "type": "msg", "value": 5},
        "exchange_msg_10_49": {"name": "10 MSG", "keys": 49, "type": "msg", "value": 10},
        "exchange_case_9": {"name": "Кейс «Пасхальный»", "keys": 9, "type": "case", "value": "easter"},
        "exchange_coin_1m_19": {"name": "1.000.000msCoin", "keys": 19, "type": "coin", "value": 1_000_000},
        "exchange_coin_5m_49": {"name": "5.000.000msCoin", "keys": 49, "type": "coin", "value": 5_000_000},
        "exchange_coin_10m_99": {"name": "10.000.000msCoin", "keys": 99, "type": "coin", "value": 10_000_000},
        "exchange_coin_25m_119": {"name": "25.000.000msCoin", "keys": 119, "type": "coin", "value": 25_000_000},
    }
    
    if data == "exchange_back":
        await easter_exchange_menu(update, context)
        return
    
    if data in exchanges:
        exchange = exchanges[data]
        
        # Проверяем количество ключей
        user_keys = await get_easter_keys(user.id)
        
        if user_keys < exchange["keys"]:
            await query.answer(f"❌ Недостаточно ключей! Нужно {exchange['keys']} 🔑", show_alert=True)
            return
        
        # Списываем ключи
        await add_easter_keys(user.id, -exchange["keys"])
        
        # Выдаём награду
        reward_text = ""
        
        if exchange["type"] == "nothing":
            reward_text = "🤡 Ты обменял ключи на ничего!"
            
        elif exchange["type"] == "msg":
            # Добавляем MSG баланс
            async with aiosqlite.connect('data/bot.db') as db:
                await db.execute(
                    "UPDATE users SET msg_balance = msg_balance + ? WHERE user_id = ?",
                    (exchange["value"], user.id)
                )
                await db.commit()
            reward_text = f"🍯 Ты получил {exchange['value']} MSG!"
            
        elif exchange["type"] == "coin":
            await update_balance_async(user.id, exchange["value"])
            reward_text = f"💰 Ты получил {format_amount(exchange['value'])}msCoin!"
            
        elif exchange["type"] == "case":
            # Добавляем пасхальный кейс в инвентарь
            async with aiosqlite.connect('data/bot.db') as db:
                # Проверяем есть ли таблица инвентаря
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS user_cases (
                        user_id INTEGER,
                        case_type TEXT,
                        quantity INTEGER DEFAULT 0,
                        PRIMARY KEY (user_id, case_type)
                    )
                ''')
                await db.execute('''
                    INSERT INTO user_cases (user_id, case_type, quantity)
                    VALUES (?, ?, 1)
                    ON CONFLICT(user_id, case_type) DO UPDATE SET quantity = quantity + 1
                ''', (user.id, "easter"))
                await db.commit()
            reward_text = f"🥚 Ты получил Пасхальный кейс!"
        
        # Обновляем меню с новым количеством ключей
        keys_left = await get_easter_keys(user.id)
        
        # Отправляем сообщение об успехе
        await query.message.reply_text(
            f"✅ <b>{user.first_name}</b>, успешно обменяно!\n"
            f"📦 {exchange['name']} - {exchange['keys']} 🔑\n"
            f"{reward_text}\n\n"
            f"🔑 Осталось ключей: {keys_left}",
            parse_mode='HTML'
        )
        
        # Обновляем меню обменника
        await easter_exchange_menu(update, context)
        return
    
    await query.answer("❌ Неизвестная команда", show_alert=True)
    
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

async def top_ref_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    top_refs = await get_top_referrers_async(10)

    if not top_refs:
        await update.message.reply_text("🦣 Мамонтоводов пока нет.")
        return

    message = f"🦣 {user.full_name}, топ мамонтоводов:\n\n"

    emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

    for i, row in enumerate(top_refs):
        ref_id, name, count = row
        message += f"{emojis[i]} {name} — заскамил {count} чел.\n"

    rank = await get_referral_rank_async(user.id)
    
    if rank > 0 and rank <= 10:
        keyboard = [[InlineKeyboardButton(f"🙈 Вы на {rank} месте.", callback_data="noop")]]
    else:
        keyboard = [[InlineKeyboardButton(f"🥲 Вы не в топе.", callback_data="noop")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(message, reply_markup=reply_markup)

async def ref_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    ref_link = f"https://t.me/{context.bot.username}?start=ref_{user_id}"
    referral_count = await get_user_referral_count_async(user_id)

    await update.message.reply_text(
        f"🦣 {user.full_name}, система для мамонтов:\n\n"
        f"🔗 Ваша реферальная ссылка: {ref_link}\n\n"
        f"Краткая информация - вы приглашаете пользователя и получаете 100-50.000ms¢\n"
        f"❕ Важная информация, реферал зачисляется только после того, как подпишется на новостные каналы.\n\n"
        f"📊 Приглашено мамонтов: {referral_count}"
    )


async def sprevent_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главная команда ивента"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    
    now = datetime.now()
    if now < SPRING_EVENT_START:
        await update.message.reply_text("🌸 Ивент еще не начался! Ожидайте 1 марта 2026 года.")
        return
    
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
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

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
    await query.answer()

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
    await query.answer()

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
        await query.answer()
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
    await query.answer()

async def answer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для ответа на загадку: /answer id ответ"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    user_id = user.id
    
    if len(context.args) < 2:
        await update.message.reply_text("❌ Использование: /answer *айди* *ответ*")
        return
    
    try:
        question_id = int(context.args[0])
        answer = ' '.join(context.args[1:]).strip().lower()
    except:
        await update.message.reply_text("❌ Неверный формат ID вопроса.")
        return
    
    question = await get_spring_question_async(question_id)
    if not question:
        await update.message.reply_text("❌ Вопрос не найден.")
        return
    
    if question['solved_by'] is not None:
        await update.message.reply_text("❌ На этот вопрос уже ответили.")
        return
    
    correct_answer = question['answer'].strip().lower()
    if answer != correct_answer:
        await update.message.reply_text("❌ Неправильный ответ. Попробуй еще!")
        return
    
    solved = await solve_spring_question_async(question_id, user_id)
    if not solved:
        await update.message.reply_text("❌ Кто-то уже ответил быстрее!")
        return
    
    prize_text = ""
    if question['prize_type'] == 'coins':
        amount = int(question['prize_value'])
        await update_balance_async(user_id, amount)
        prize_text = f"{format_amount(amount)}ms¢"
    elif question['prize_type'] == 'gold':
        amount = int(question['prize_value'])
        prize_text = f"{amount}🍩 ms'gold"
    elif question['prize_type'] == 'sun':
        amount = int(question['prize_value'])
        await add_user_suns_async(user_id, amount)
        prize_text = f"{amount}☀️ солнышек"
    elif question['prize_type'] == 'secret':
        prize_text = f"🎁 {question['prize_value']}"
    
    await update.message.reply_text(
        f"✅ {user.full_name}, вы угадали вопрос.\n"
        f"Ваш приз: {prize_text}."
    )

async def question_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для добавления вопроса: /question вопрос|ответ"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: /question *вопрос|ответ*")
        return
    
    full_text = ' '.join(context.args)
    if '|' not in full_text:
        await update.message.reply_text("❌ Используйте формат: вопрос|ответ")
        return
    
    question_part, answer_part = full_text.split('|', 1)
    question = question_part.strip()
    answer = answer_part.strip()
    
    if not question or not answer:
        await update.message.reply_text("❌ Вопрос и ответ не могут быть пустыми.")
        return
    
    spring_question_creation[user_id] = {
        'question': question,
        'answer': answer,
        'step': 'prize_type'
    }
    
    await update.message.reply_text(
        f"👮‍♂️ Вы установили вопрос «{question}»\n"
        f"Теперь укажите награду"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("💸 ms'coin", callback_data="spring_prize_coins"),
            InlineKeyboardButton("🍩 ms'gold", callback_data="spring_prize_gold")
        ],
        [
            InlineKeyboardButton("🙊 Секретный приз", callback_data="spring_prize_secret"),
            InlineKeyboardButton("☀️ Солнышки", callback_data="spring_prize_sun")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text("Выберите тип награды:", reply_markup=reply_markup)

async def spring_prize_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора типа награды для вопроса"""
    query = update.callback_query
    user_id = query.from_user.id
    
    if user_id not in spring_question_creation:
        await query.answer("❌ Сессия создания вопроса не найдена", show_alert=True)
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
    
    await query.answer()

async def spring_prize_value_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода значения награды"""
    user_id = update.effective_user.id
    
    if user_id not in spring_question_creation:
        return
    
    data = spring_question_creation[user_id]
    if data.get('step') != 'prize_value':
        return
    
    prize_type = data['prize_type']
    value = update.message.text.strip()
    
    if prize_type in ['coins', 'gold', 'sun']:
        try:
            if prize_type in ['coins', 'gold']:
                parsed_value = parse_amount(value)
            else:
                parsed_value = int(value)
            
            if parsed_value <= 0:
                await update.message.reply_text("❌ Значение должно быть больше 0.")
                return
        except:
            await update.message.reply_text("❌ Неверный формат числа.")
            return
    else:
        parsed_value = value
    
    await create_spring_question_async(
        question=data['question'],
        answer=data['answer'],
        prize_type=prize_type,
        prize_value=str(parsed_value)
    )
    
    del spring_question_creation[user_id]
    
    await update.message.reply_text("✅ Вопрос успешно добавлен!")

async def spring_collect_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Сбор солнышек'"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    can_collect, minutes, seconds = await can_collect_sun_async(user_id, 5400)
    
    if not can_collect:
        await query.answer(f"⏳ Вы уже собирали солнышки, подождите {minutes} мин. {seconds} сек.", show_alert=True)
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
    await query.answer(f"✅ +{sun_amount}☀️")

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
    await query.answer()

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
        await query.answer()
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
    await query.answer()

async def exchange_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для обмена солнышек: /exchange количество"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    user_id = user.id
    
    now = datetime.now()
    if now < SPRING_EVENT_END:
        await update.message.reply_text("❌ Обменник откроется 1 июня 2026 года.")
        return
    
    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: /exchange *количество*")
        return
    
    try:
        amount = int(context.args[0])
    except:
        await update.message.reply_text("❌ Неверный формат числа.")
        return
    
    valid_amounts = [100, 500, 1000, 5000]
    if amount not in valid_amounts:
        await update.message.reply_text("❌ Доступные значения: 100, 500, 1000, 5000")
        return
    
    suns = await get_user_suns_async(user_id)
    if suns < amount:
        await update.message.reply_text(f"❌ У вас только {suns}☀️. Нужно {amount}☀️.")
        return
    
    if amount == 100:
        reward = 1000
        await update_balance_async(user_id, reward)
        await add_user_suns_async(user_id, -amount)
        await update.message.reply_text(f"✅ Вы обменяли 100☀️ на {reward}ms¢!")
    elif amount == 500:
        reward = 6000
        await update_balance_async(user_id, reward)
        await add_user_suns_async(user_id, -amount)
        await update.message.reply_text(f"✅ Вы обменяли 500☀️ на {reward}ms¢!")
    elif amount == 1000:
        reward = 15000
        await update_balance_async(user_id, reward)
        await add_user_suns_async(user_id, -amount)
        await update.message.reply_text(f"✅ Вы обменяли 1000☀️ на {reward}ms¢!")
    elif amount == 5000:
        await add_user_suns_async(user_id, -amount)
        await update.message.reply_text(
            f"🎁 {user.full_name}, вы получили секретный приз!\n"
            f"Свяжитесь с администратором: @ahpeplov"
        )

# ==================== ЗАДАНИЯ ====================
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
        await query.answer()
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
    await query.answer()

async def put_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для получения награды за задание: /put айди"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    user_id = user.id
    
    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: /put *айди задания*")
        return
    
    try:
        task_id = int(context.args[0])
    except:
        await update.message.reply_text("❌ Неверный формат ID.")
        return
    
    task = await get_spring_task_async(task_id)
    if not task:
        await update.message.reply_text("❌ Задание не найдено.")
        return
    
    progress_data = await get_user_task_progress_async(user_id, task_id)
    
    if progress_data['completed'] == 0:
        await update.message.reply_text("❌ Задание еще не выполнено.")
        return
    
    if progress_data['claimed'] == 1:
        await update.message.reply_text("❌ Вы уже получили награду за это задание.")
        return
    
    success, prize, suns = await claim_task_reward_async(user_id, task_id)
    
    if success:
        await update_balance_async(user_id, prize)
        await add_user_suns_async(user_id, suns)
        
        await update.message.reply_text(
            f"✅ {user.full_name}, вы выполнили задание и получили {format_amount(prize)}ms¢ и {suns}☀️"
        )
    else:
        await update.message.reply_text("❌ Ошибка при получении награды.")

async def add_task_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для добавления задания (только для админов)"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав.")
        return
    
    if len(context.args) < 1:
        await update.message.reply_text(
            "❌ Использование: /addtask *описание|цель|мин_награда|макс_награда|мин_солн|макс_солн|тип_игры*\n\n"
            "Типы игр: mines, gold, pyramid, slot, football, basketball, knb, transfer\n"
            "Пример: /addtask Сыграй в мины|5|100|500|1|5|mines"
        )
        return
    
    full_text = ' '.join(context.args)
    parts = full_text.split('|')
    
    if len(parts) != 7:
        await update.message.reply_text("❌ Должно быть 7 частей, разделенных |")
        return
    
    description = parts[0].strip()
    try:
        target = int(parts[1].strip())
        prize_min = int(parts[2].strip())
        prize_max = int(parts[3].strip())
        sun_min = int(parts[4].strip())
        sun_max = int(parts[5].strip())
    except:
        await update.message.reply_text("❌ Цель и награды должны быть числами.")
        return
    
    game_type = parts[6].strip()
    valid_types = ['mines', 'gold', 'pyramid', 'slot', 'football', 'basketball', 'knb', 'transfer', None]
    if game_type not in valid_types:
        await update.message.reply_text(f"❌ Неверный тип игры. Допустимые: {', '.join(valid_types)}")
        return
    
    task_id = await create_spring_task_async(
        description=description,
        target_count=target,
        prize_min=prize_min,
        prize_max=prize_max,
        sun_min=sun_min,
        sun_max=sun_max,
        game_type=game_type if game_type != 'None' else None
    )
    
    await update.message.reply_text(f"✅ Задание №{task_id} успешно создано!")

async def update_task_progress_for_game(user_id, game_type, increment=1):
    """Обновить прогресс заданий для конкретной игры"""
    tasks = await get_all_spring_tasks_async()
    
    for task in tasks:
        if task['game_type'] == game_type or task['game_type'] is None:
            await update_user_task_progress_async(user_id, task['id'], increment)

async def give_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    user_id = update.effective_user.id

    has_feature = check_donate_feature(user_id, 'give')  # без await

    # Если пользователь не админ и нет донат-функции - блокируем
    if user_id not in ADMIN_IDS and not has_feature:
        await update.message.reply_text(
            "❌ У вас нет доступа к функции /give\n\n"
            "Эта функция доступна только по донату.\n"
            "Для получения доступа используйте /donat или обратитесь к администратору."
        )
        return

    # Дальше идет существующий код
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target = f"ID: {target_id}"
        if len(context.args) < 1:
            await update.message.reply_text("❌ Использование при ответе на сообщение: !give *summ*")
            return
        amount_str = ' '.join(context.args[0:])
    else:
        if len(context.args) < 2:
            await update.message.reply_text("❌ Использование: !give *id or @username* *summ*")
            return

        target = context.args[0]
        amount_str = ' '.join(context.args[1:])

        if target.startswith('@'):
            username = target[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                return
            target_id = user_data['user_id']
        else:
            try:
                target_id = int(target)
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    amount = parse_amount(amount_str)

    if amount <= 0:
        await update.message.reply_text("❌ Неверная сумма.")
        return

    await update_balance_async(target_id, amount)

    # Для админов показываем обычное сообщение
    if user_id in ADMIN_IDS:
        await update.message.reply_text(f"✅ Пользователю {target} начислено {amount}ms¢.")
    else:
        # Для пользователей с донат-функцией добавляем информацию об оставшемся времени
        # Используем синхронный вызов, без async with
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT expires_at FROM donate_features WHERE user_id = ? AND feature_type = ?',
                (user_id, 'give')
            )
            row = cursor.fetchone()
            if row:
                expires_at = datetime.fromisoformat(row['expires_at'])
                remaining_days = (expires_at - datetime.now()).days
                await update.message.reply_text(
                    f"✅ Пользователю {target} начислено {amount}ms¢.\n\n"
                    f"💎 Доступ к функции give: {remaining_days} дней"
                )
            else:
                await update.message.reply_text(f"✅ Пользователю {target} начислено {amount}ms¢.")
        except Exception as e:
            logger.error(f"Error getting donate feature info: {e}")
            await update.message.reply_text(f"✅ Пользователю {target} начислено {amount}ms¢.")

async def take_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target = f"ID: {target_id}"
        if len(context.args) < 1:
            await update.message.reply_text("❌ Использование при ответе на сообщение: !take *summ*")
            return
        amount_str = ' '.join(context.args[0:])
    else:
        if len(context.args) < 2:
            await update.message.reply_text("❌ Использование: !take *id or @username* *summ*")
            return
        
        target = context.args[0]
        amount_str = ' '.join(context.args[1:])
        
        if target.startswith('@'):
            username = target[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                return
            target_id = user_data['user_id']
        else:
            try:
                target_id = int(target)
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    amount = parse_amount(amount_str)

    if amount <= 0:
        await update.message.reply_text("❌ Неверная сумма.")
        return

    success = await update_balance_safe_async(target_id, -amount, amount)
    if success:
        await update.message.reply_text(f"✅ У пользователя {target} списано {amount}ms¢.")
    else:
        await update.message.reply_text(f"❌ Недостаточно средств у пользователя {target}.")

async def checkhash_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: !checkhash *hash*")
        return

    game_hash = context.args[0]
    game_data = await get_game_hash_async(game_hash)

    if not game_data:
        await update.message.reply_text(f"❌ Хэш {game_hash} не найден.")
        return

    result_text = "выигрышем" if game_data['result'] == 'win' else "проигрышем" if game_data['result'] == 'lose' else game_data['result']

    await update.message.reply_text(
        f"ℹ️ Хэш {game_hash}.\n"
        f"Информация о данном хэше:\n"
        f"Игра: {game_data['game_type']}.\n"
        f"Ставка: {game_data['bet']}ms¢.\n"
        f"Была окончена: {result_text}."
    )

async def tcheckhash_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: !tcheckhash *hash*")
        return

    transfer_hash = context.args[0]
    
    if transfer_hash not in transfer_confirmations:
        await update.message.reply_text(f"❌ Хэш перевода {transfer_hash} не найден.")
        return
    
    transfer_data = transfer_confirmations[transfer_hash]
    
    await update.message.reply_text(
        f"ℹ️ Хэш перевода {transfer_hash}.\n"
        f"Информация о данном переводе:\n"
        f"Отправитель: {transfer_data['from_id']}\n"
        f"Получатель: {transfer_data['to_id']}\n"
        f"Сумма: {transfer_data['amount']}ms¢\n"
        f"Статус: {'подтвержден' if transfer_data.get('completed') else 'ожидает подтверждения'}"
    )



async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        if len(context.args) >= 1:
            try:
                ban_days = int(context.args[0])
            except:
                await update.message.reply_text("❌ Неверный формат дней. Используйте число (0 для перманентного бана).")
                return
            reason = ' '.join(context.args[1:]) if len(context.args) > 1 else "Не указана"
        else:
            await update.message.reply_text("❌ Использование при ответе на сообщение: /ban *сколько дней* *причина*")
            return
    else:
        if len(context.args) < 2:
            await update.message.reply_text("❌ Использование: /ban *id or @username* *сколько дней* *причина*")
            return
        
        target = context.args[0]
        try:
            ban_days = int(context.args[1])
        except:
            await update.message.reply_text("❌ Неверный формат дней. Используйте число (0 для перманентного бана).")
            return
        
        reason = ' '.join(context.args[2:]) if len(context.args) > 2 else "Не указана"
        
        if target.startswith('@'):
            username = target[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                return
            target_id = user_data['user_id']
        else:
            try:
                target_id = int(target)
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    if target_id in ADMIN_IDS:
        await update.message.reply_text("❌ Нельзя забанить администратора.")
        return

    await ban_user_async(target_id, ban_days, reason)

    ban_duration = "навсегда" if ban_days == 0 else f"на {ban_days} дней"
    await update.message.reply_text(
        f"👮‍♂️ {update.effective_user.full_name}, вы успешно забанили пользователя {ban_duration} по причине: {reason}."
    )

    try:
        keyboard = [[InlineKeyboardButton("🧐 Не согласен", url="https://t.me/kleymorf")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await context.bot.send_message(
            chat_id=target_id,
            text=f"🚨 Вы были заблокированы в боте {ban_duration} по причине: {reason}\n\n"
                 f"❓ Не согласны с наказанием? Нажмите кнопку ниже",
            reply_markup=reply_markup
        )
    except Exception as e:
        if "Forbidden" in str(e) or "blocked" in str(e):
            logging.info(f"User {target_id} blocked the bot, skipping ban notification")
        else:
            logging.error(f"Failed to send ban notification: {e}")

async def msh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка ключа: /msh ключ"""
    if not update.effective_user:
        return

    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: /msh *ключ*")
        return

    key_code = context.args[0].strip()
    
    key_info = await get_key(key_code)
    
    if not key_info:
        await update.message.reply_text(
            "⚪ Ключ не найден в базе! Будьте осторожны"
        )
        return
    
    if key_info['status'] == 'scam':
        await update.message.reply_text(
            "⚠️ Данный ключ занесён в скам базу!\n"
            "Не участвуйте в розыгрышах и др. раздачах."
        )
    elif key_info['status'] == 'verified':
        channel_name = key_info.get('channel_name', '')
        text = f"✅ Канал был верифицирован владельцем! Смело участвуйте и выигрывайте"
        if channel_name:
            text += f"\n\n📢 Канал: {channel_name}"
        await update.message.reply_text(text)
    elif key_info['status'] == 'safe':
        await update.message.reply_text(
            "✅ Ключ в безопасной базе! Можете участвовать"
        )

async def gmsh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создание ключа: /gmsh ключ статус [название_канала]"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ Использование: /gmsh *ключ* *статус* [название_канала]\n"
            "Статусы: scam, verified, safe"
        )
        return

    key_code = context.args[0].strip()
    status = context.args[1].lower().strip()
    channel_name = ' '.join(context.args[2:]) if len(context.args) > 2 else None

    if status not in ['scam', 'verified', 'safe']:
        await update.message.reply_text("❌ Статус должен быть: scam, verified, safe")
        return

    success = await add_key(key_code, status, user_id, None, channel_name)
    
    if success:
        await update.message.reply_text(
            f"✅ Ключ «{key_code}» создан со статусом «{status}»"
        )
    else:
        await update.message.reply_text("❌ Ошибка при создании ключа")

async def rmsh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактирование ключа: /rmsh ключ"""
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: /rmsh *ключ*")
        return

    key_code = context.args[0].strip()
    
    key_info = await get_key(key_code)
    
    if not key_info:
        await update.message.reply_text(f"❌ Ключ «{key_code}» не найден")
        return

    text = (
        f"⚙️ *{update.effective_user.full_name}*, редактируйте ключ *{key_code}*\n\n"
        f"📊 Текущий статус: *{key_info['status']}*"
    )

    keyboard = [
        [
            InlineKeyboardButton("⚠️ Скам", callback_data=f"key_edit_{key_code}_scam"),
            InlineKeyboardButton("✅ Верифицирован", callback_data=f"key_edit_{key_code}_verified"),
            InlineKeyboardButton("🟢 Безопасный", callback_data=f"key_edit_{key_code}_safe")
        ],
        [InlineKeyboardButton("❌ Удалить", callback_data=f"key_delete_{key_code}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def key_edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, key_code: str, status: str):
    """Обработчик редактирования ключа"""
    query = update.callback_query
    user_id = query.from_user.id
    
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ У вас нет прав", show_alert=True)
        return

    await update_key_status(key_code, status)
    
    status_names = {
        'scam': '⚠️ Скам',
        'verified': '✅ Верифицирован',
        'safe': '🟢 Безопасный'
    }
    
    await query.edit_message_text(
        f"✅ Ключ «{key_code}» обновлен. Новый статус: {status_names.get(status, status)}"
    )
    await safe_answer(query, "✅ Готово")

async def key_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, key_code: str):
    """Обработчик удаления ключа"""
    query = update.callback_query
    user_id = query.from_user.id
    
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])

    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await safe_answer(query, "❌ У вас нет прав", show_alert=True)
        return

    await delete_key(key_code)
    
    await query.edit_message_text(f"✅ Ключ «{key_code}» удален из базы")
    await safe_answer(query, "✅ Удалено")

async def diamond_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Алмазы"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    text = update.message.text.strip()
    parts = text.split()
    
    cmd = parts[0].lower()
    if cmd not in ['/diamond', 'алмазы', 'алмаз']:
        return

    if len(parts) < 2:
        await update.message.reply_text(
            "<blockquote>ℹ️ Алмазы – это игра, в которой вы должны угадать ячейки, в которых спрятан алмаз. Вам нужно открывать по одной ячейке, на каждом из 16 уровней, чтобы найти алмаз.</blockquote>\n\n"
            f"🤖 *{user_name}*, чтобы начать игру, используй команду:\n\n"
            "💠 <u><i>/diamond [ставка] [мины 1-2]</i></u>\n\n"
            "Пример:\n"
            "/diamond 100 2\n"
            "алмазы 100",
            parse_mode='HTML'
        )
        return

    try:
        db_user = await get_user_async(user_id, user.full_name, user.username)
        user_balance = db_user['balance']
        
        bet_amount = parse_bet_amount(parts[1], user_balance)
        
        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return

        if bet_amount > user_balance:
            await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(user_balance)}ms¢")
            return

        mines_count = 1
        if len(parts) >= 3:
            try:
                mines_count = int(parts[2])
                if mines_count not in [1, 2]:
                    mines_count = 1
            except:
                mines_count = 1

        await update_balance_async(user_id, -bet_amount)

        # Создаем сессию
        DIAMOND_SESSIONS = context.bot_data.get('DIAMOND_SESSIONS', {})
        
        # Генерируем позиции для каждого уровня
        # Если mines_count = 1: 1 мина + 2 алмаза
        # Если mines_count = 2: 2 мины + 1 алмаз
        safe_positions = {}  # позиции алмазов (безопасные)
        for level in range(1, 17):
            if mines_count == 1:
                # 2 алмаза - выбираем 2 случайные позиции из 3
                safe_positions[level] = random.sample(range(3), 2)
            else:
                # 1 алмаз - выбираем 1 случайную позицию из 3
                safe_positions[level] = [random.randint(0, 2)]
        
        # Выбираем множители
        if mines_count == 1:
            multipliers = DIAMOND_MULTIPLIERS_1
        else:
            multipliers = DIAMOND_MULTIPLIERS_2
        
        DIAMOND_SESSIONS[user_id] = {
            'bet': bet_amount,
            'mines_count': mines_count,
            'safe_positions': safe_positions,  # список позиций с алмазами
            'multipliers': multipliers,
            'current_level': 0,
            'opened_positions': [],  # список (level, position)
            'user_name': user_name,
            'chat_id': update.effective_chat.id,
            'thread_id': update.effective_message.message_thread_id,
            'message_id': None,
            'status': 'active'
        }
        context.bot_data['DIAMOND_SESSIONS'] = DIAMOND_SESSIONS

        await send_diamond_board(update, context, user_id)

    except Exception as e:
        logging.error(f"Error in diamond command: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")
        if 'bet_amount' in locals():
            await update_balance_async(user_id, bet_amount)

async def send_diamond_board(update, context, user_id):
    """Отправка/обновление игрового поля Алмазы (во время игры)"""
    DIAMOND_SESSIONS = context.bot_data.get('DIAMOND_SESSIONS', {})
    session = DIAMOND_SESSIONS.get(user_id)
    
    if not session:
        return

    current_level = session['current_level']
    bet = session['bet']
    mines_count = session['mines_count']
    multipliers = session['multipliers']
    user_name = session['user_name']

    if current_level == 0:
        text = (
            f"🍀<b>Алмазы • начни игру!</b>\n"
            f"••••••••••\n"
            f"🧨 Мин: {mines_count}\n"
            f"💸 Ставка: {format_amount(bet)}ms¢\n\n"
            f"<blockquote>🪜 Сл. множитель: х{multipliers[1]}</blockquote>\n"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("❓", callback_data=f"diamond_cell_{user_id}_1_0"),
                InlineKeyboardButton("❓", callback_data=f"diamond_cell_{user_id}_1_1"),
                InlineKeyboardButton("❓", callback_data=f"diamond_cell_{user_id}_1_2")
            ],
            [InlineKeyboardButton("❌ Отменить", callback_data=f"diamond_cancel_{user_id}")]
        ]
        
    else:
        current_multiplier = multipliers.get(current_level, 1.00)
        next_multiplier = multipliers.get(current_level + 1, 0)
        current_win = int(bet * current_multiplier)
        next_win = int(bet * next_multiplier) if next_multiplier > 0 else 0
        
        text = (
            f"💠<b>Алмазы • игра идёт!</b>\n"
            f"••••••••••\n"
            f"🧨 Мин: {mines_count}\n"
            f"💸 Ставка: {format_amount(bet)}ms¢\n"
            f"📊 Выигрыш: x{current_multiplier} / {format_amount(current_win)}ms¢\n\n"
            f"<blockquote>🪜 Сл. множитель: х{next_multiplier}</blockquote>\n"
        )
        
        keyboard = []
        
        # Пройденные уровни (показываем только открытые ячейки, остальные ❓)
        for level in range(1, current_level + 1):
            row = []
            for pos in range(3):
                opened = None
                for op in session['opened_positions']:
                    if op['level'] == level and op['position'] == pos:
                        opened = op
                        break
                
                if opened:
                    # Открытая ячейка - показываем 💠 (алмаз)
                    row.append(InlineKeyboardButton("💠", callback_data="diamond_dead"))
                else:
                    # Неоткрытая ячейка на пройденном уровне - ❓
                    row.append(InlineKeyboardButton("❓", callback_data="diamond_dead"))
            keyboard.append(row)
        
        # Текущий уровень (активные кнопки)
        current_row = []
        for pos in range(3):
            current_row.append(InlineKeyboardButton("❓", callback_data=f"diamond_cell_{user_id}_{current_level + 1}_{pos}"))
        keyboard.append(current_row)
        
        # Кнопка забрать награду
        keyboard.append([InlineKeyboardButton("✅ Забрать выигрыш", callback_data=f"diamond_take_{user_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if session.get('message_id'):
        try:
            await context.bot.edit_message_text(
                text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except:
            msg = await context.bot.send_message(
                chat_id=session['chat_id'],
                text=text,
                reply_markup=reply_markup,
                message_thread_id=session['thread_id'],
                parse_mode='HTML'
            )
            session['message_id'] = msg.message_id
    else:
        msg = await context.bot.send_message(
            chat_id=session['chat_id'],
            text=text,
            reply_markup=reply_markup,
            message_thread_id=session['thread_id'],
            parse_mode='HTML'
        )
        session['message_id'] = msg.message_id
    
    context.bot_data['DIAMOND_SESSIONS'] = DIAMOND_SESSIONS

async def diamond_lose_board(update, context, user_id, crash_level, crash_position):
    """Финальное поле при проигрыше"""
    query = update.callback_query
    DIAMOND_SESSIONS = context.bot_data.get('DIAMOND_SESSIONS', {})
    session = DIAMOND_SESSIONS.get(user_id)
    
    if not session:
        return

    text = (
        f"💥<b>Алмазы • Проигрыш!</b>\n"
        f"••••••••••\n"
        f"💣 Мин: {session['mines_count']}\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"🪜 Пройдено: {session['current_level']} из 16\n\n"
    )
    
    keyboard = []
    
    for lvl in range(1, crash_level + 1):
        row = []
        safe_positions = session['safe_positions'].get(lvl, [])
        for pos in range(3):
            opened = None
            for op in session['opened_positions']:
                if op['level'] == lvl and op['position'] == pos:
                    opened = op
                    break
            
            if opened and not opened.get('is_safe', True) and lvl == crash_level and pos == crash_position:
                # Место падения (мина)
                row.append(InlineKeyboardButton("💥", callback_data="diamond_dead"))
            elif opened and opened.get('is_safe', False):
                # Открытый алмаз - 💠
                row.append(InlineKeyboardButton("💠", callback_data="diamond_dead"))
            elif pos in safe_positions:
                # Неоткрытый алмаз - 💰
                row.append(InlineKeyboardButton("💰", callback_data="diamond_dead"))
            else:
                # Мина - 🧨
                row.append(InlineKeyboardButton("🧨", callback_data="diamond_dead"))
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

async def diamond_cell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, level: int, position: int):
    """Обработка нажатия на ячейку"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    await safe_answer(query, "")

    DIAMOND_SESSIONS = context.bot_data.get('DIAMOND_SESSIONS', {})
    session = DIAMOND_SESSIONS.get(user_id)
    
    if not session or session['status'] != 'active':
        await safe_answer(query, "❌ Игра не найдена или завершена", show_alert=True)
        return

    current_level = session['current_level']
    
    if level != current_level + 1:
        await safe_answer(query, "🧐 Не тот уровень!", show_alert=True)
        return

    for op in session['opened_positions']:
        if op['level'] == level and op['position'] == position:
            await safe_answer(query, "❌ Эта ячейка уже открыта", show_alert=True)
            return

    # Проверяем, алмаз это или мина
    safe_positions = session['safe_positions'].get(level, [])
    is_safe = (position in safe_positions)  # если позиция в списке алмазов - безопасно
    
    session['opened_positions'].append({
        'level': level,
        'position': position,
        'is_safe': is_safe,
        'is_crash': not is_safe
    })

    if not is_safe:
        # Проигрыш (нажали на мину)
        session['status'] = 'lost'
        await diamond_lose_board(update, context, user_id, level, position)
        await update_user_stats_async(user_id, 0, session['bet'])
        
        del DIAMOND_SESSIONS[user_id]
        context.bot_data['DIAMOND_SESSIONS'] = DIAMOND_SESSIONS
        return
    
    # Безопасное нажатие (алмаз)
    session['current_level'] = level
    
    if level >= 16:
        # Победа (прошли все 16 уровней)
        await diamond_win_board(update, context, user_id)
        return
    
    await send_diamond_board(update, context, user_id)
    await safe_answer(query, f"✅ Уровень {level} пройден!")

async def diamond_win_board(update, Update, context, user_id):
    """Финальное поле при прохождении всех 16 уровней"""
    query = update.callback_query
    DIAMOND_SESSIONS = context.bot_data.get('DIAMOND_SESSIONS', {})
    session = DIAMOND_SESSIONS.get(user_id)
    
    if not session:
        return

    final_multiplier = session['multipliers'].get(16, 1.00)
    win_amount = int(session['bet'] * final_multiplier)
    
    await update_balance_async(user_id, win_amount)
    await update_user_stats_async(user_id, win_amount, 0)
    
    text = (
        f"💎<b>Алмазы • Победа!</b><tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji>\n"
        f"••••••••••\n"
        f"💣 Мин: {session['mines_count']}\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"💰 Выигрыш: x{final_multiplier} / {format_amount(win_amount)}ms¢\n\n"
    )
    
    keyboard = []
    
    for lvl in range(1, 17):
        row = []
        safe_positions = session['safe_positions'].get(lvl, [])
        for pos in range(3):
            opened = None
            for op in session['opened_positions']:
                if op['level'] == lvl and op['position'] == pos:
                    opened = op
                    break
            
            if opened:
                # Открытая ячейка - это алмаз (💠)
                row.append(InlineKeyboardButton("💠", callback_data="diamond_dead"))
            else:
                if pos in safe_positions:
                    # Неоткрытый алмаз - 💰
                    row.append(InlineKeyboardButton("💰", callback_data="diamond_dead"))
                else:
                    # Мина - 🧨
                    row.append(InlineKeyboardButton("🧨", callback_data="diamond_dead"))
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
    
    del DIAMOND_SESSIONS[user_id]
    context.bot_data['DIAMOND_SESSIONS'] = DIAMOND_SESSIONS

async def diamond_take_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Забрать награду"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    DIAMOND_SESSIONS = context.bot_data.get('DIAMOND_SESSIONS', {})
    session = DIAMOND_SESSIONS.get(user_id)
    
    if not session or session['status'] != 'active':
        await safe_answer(query, "❌ Игра не найдена или завершена", show_alert=True)
        return

    current_level = session['current_level']
    if current_level == 0:
        await safe_answer(query, "❌ Сначала пройдите хотя бы один уровень!", show_alert=True)
        return

    current_multiplier = session['multipliers'].get(current_level, 1.00)
    win_amount = int(session['bet'] * current_multiplier)
    
    await update_balance_async(user_id, win_amount)
    await update_user_stats_async(user_id, win_amount, 0)
    
    text = (
        f"💎<b>Алмазы • Победа!</b><tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji>\n"
        f"••••••••••\n"
        f"💣 Мин: {session['mines_count']}\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"💰 Выигрыш: x{current_multiplier} / {format_amount(win_amount)}ms¢\n\n"
    )
    
    keyboard = []
    
    # Показываем пройденные уровни
    for lvl in range(1, current_level + 1):
        row = []
        safe_positions = session['safe_positions'].get(lvl, [])
        for pos in range(3):
            # Проверяем, открыта ли эта позиция
            opened = None
            for op in session['opened_positions']:
                if op['level'] == lvl and op['position'] == pos:
                    opened = op
                    break
            
            if opened:
                # Открытая ячейка - это алмаз (💠)
                row.append(InlineKeyboardButton("💠", callback_data="diamond_dead"))
            else:
                if pos in safe_positions:
                    # Неоткрытый алмаз - 💰
                    row.append(InlineKeyboardButton("💰", callback_data="diamond_dead"))
                else:
                    # Мина - 🧨
                    row.append(InlineKeyboardButton("🧨", callback_data="diamond_dead"))
        keyboard.append(row)
    
    # Добавляем следующий уровень (который не проходили)
    next_level = current_level + 1
    if next_level <= 16:
        next_row = []
        next_safe_positions = session['safe_positions'].get(next_level, [])
        for pos in range(3):
            if pos in next_safe_positions:
                # Алмаз на следующем уровне - 💰
                next_row.append(InlineKeyboardButton("💰", callback_data="diamond_dead"))
            else:
                # Мина на следующем уровне - 🧨
                next_row.append(InlineKeyboardButton("🧨", callback_data="diamond_dead"))
        keyboard.append(next_row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
    
    del DIAMOND_SESSIONS[user_id]
    context.bot_data['DIAMOND_SESSIONS'] = DIAMOND_SESSIONS

async def diamond_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Отмена игры"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    DIAMOND_SESSIONS = context.bot_data.get('DIAMOND_SESSIONS', {})
    session = DIAMOND_SESSIONS.get(user_id)
    
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.", show_alert=True)
        return

    if session['current_level'] > 0:
        await safe_answer(query, "⚠️ Нельзя отменить игру после первого хода.", show_alert=True)
        return

    await update_balance_async(user_id, session['bet'])
    
    try:
        await query.message.delete()
    except:
        pass
    
    del DIAMOND_SESSIONS[user_id]
    context.bot_data['DIAMOND_SESSIONS'] = DIAMOND_SESSIONS
    await safe_answer(query, "✅ Игра отменена, средства возвращены")

BOX_SESSIONS = {}

async def handle_box_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """
    Распределитель callback-запросов для игры 'Шкатулка'
    data приходит в формате: box_cell_USERID_R_C, box_cashout_USERID и т.д.
    """
    parts = data.split('_')
    action = parts[1]  # cell, cashout, cancel или opened

    # Проверка, что на кнопку нажал именно тот игрок, который начал игру
    # В data обычно: box_action_userid_...
    try:
        data_user_id = int(parts[2])
    except (IndexError, ValueError):
        return

    if user_id != data_user_id:
        await update.callback_query.answer("❌ Это не ваша игра!", show_alert=True)
        return

    # Логика в зависимости от действия
    if action == "cell":
        # Формат: box_cell_userid_r_c
        r, c = int(parts[3]), int(parts[4])
        await box_cell_click(update, context, user_id, r, c)

    elif action == "cashout":
        # Формат: box_cashout_userid
        await box_cashout(update, context, user_id)

    elif action == "cancel":
        # Формат: box_cancel_userid
        await box_cancel(update, context, user_id)

    elif action == "opened":
        # Если нажал на уже открытую ячейку
        await update.callback_query.answer("💎 Эта ячейка уже открыта!")

def generate_box_hash():
    """Генерирует хеш для игры"""
    import hashlib
    import secrets
    random_string = secrets.token_hex(16)
    return hashlib.sha256(random_string.encode()).hexdigest()

def create_box_board(spiders_positions, treasures):
    """Создает игровое поле"""
    board = [['ㅤ' for _ in range(BOX_FIELD_SIZE)] for _ in range(BOX_FIELD_SIZE)]
    
    # Расставляем паутины
    for r, c in spiders_positions:
        board[r][c] = '🕸'
    
    # Расставляем сокровища
    for (r, c), treasure in treasures.items():
        board[r][c] = treasure
    
    return board

def get_cell_value(r, c, spiders_positions, treasures):
    """Определяет что находится в ячейке"""
    if (r, c) in spiders_positions:
        return '🕸'
    if (r, c) in treasures:
        return treasures[(r, c)]
    return None

def calculate_multiplier(opened_count, is_magical=False, magic_mult=None):
    """Рассчитывает множитель выигрыша"""
    base_mult = BOX_MULTIPLIERS[min(opened_count, len(BOX_MULTIPLIERS)-1)]
    
    if is_magical and magic_mult:
        return base_mult * magic_mult
    return base_mult


async def cmd_box(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для начала игры в шкатулку"""
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    # Парсим ставку
    if not context.args:
        await update.message.reply_text(
            f"<blockquote>ℹ️ Магическая шкатулка – это игра, в которой вы должны найти все спрятанные магические украшения и драгоценности не попав на паутину.</blockquote>\n\n"
            f"🤖 {username}, чтобы начать игру, используй команду:\n\n"
            f"📦 /box [ставка]\n\n"
            f"Пример:\n"
            f"/box 100\n"
            f"Шкатулка все",
            parse_mode='HTML'
        )
        return

    # Получаем баланс через твою функцию
    db_user = await get_user_async(user_id, update.effective_user.full_name, update.effective_user.username)
    balance = db_user.get('balance', 0)
    
    # Парсим ставку
    bet_amount = parse_bet_amount(context.args[0], balance)
    
    if bet_amount is None or bet_amount <= 0:
        await update.message.reply_text("Неверная ставка!")
        return

    if balance < bet_amount:
        await update.message.reply_text(f"Недостаточно средств! Ваш баланс: {format_amount(balance)}ms¢")
        return

    # Списываем ставку через твою функцию
    success = await update_balance_async(user_id, -bet_amount)
    if not success:
        await update.message.reply_text("Ошибка при списании средств!")
        return

    # Генерируем позиции паутин
    all_positions = [(r, c) for r in range(BOX_FIELD_SIZE) for c in range(BOX_FIELD_SIZE)]
    spiders_positions = set(random.sample(all_positions, BOX_SPIDERS_COUNT))

    # Генерируем позиции сокровищ
    safe_positions = [p for p in all_positions if p not in spiders_positions]
    treasures = {}
    for pos in safe_positions:
        treasure_type = random.choice(list(TREASURES.keys()))
        treasures[pos] = treasure_type

    # Создаем сессию
    game_hash = generate_box_hash()
    session = {
        'user_id': user_id,
        'bet': bet_amount,
        'board': create_box_board(spiders_positions, treasures),
        'spiders': spiders_positions,
        'treasures': treasures,
        'opened': [],
        'opened_count': 0,
        'status': 'active',
        'chat_id': update.effective_chat.id,
        'message_id': None,
        'hash': game_hash,
        'current_mult': 1.0,
        'found_treasures': []
    }

    BOX_SESSIONS[user_id] = session

    # Создаем клавиатуру
    keyboard = create_box_keyboard_start(session['board'], user_id)
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Отправляем сообщение
    message_text = (
        f"💎 <b>Шкатулка • начни игру!</b>\n"
        f"••••••••••\n"
        f"💸 Ставка: <b>{format_amount(bet_amount)}</b>ms¢\n"
        f"🕸 Паутин: {BOX_SPIDERS_COUNT}\n"
        f"••••••••••"
    )
    
    message = await update.message.reply_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

    session['message_id'] = message.message_id

    # Сохраняем хеш в БД (если есть такая функция)
    await save_game_hash_async(game_hash, user_id, 'box', bet_amount, 'pending')

def create_box_keyboard(board, user_id, opened_cells):
    """Создает клавиатуру с открытыми ячейками"""
    keyboard = []
    for r in range(BOX_FIELD_SIZE):
        row = []
        for c in range(BOX_FIELD_SIZE):
            if (r, c) in opened_cells:
                # Уже открытая ячейка - показываем что там
                row.append(InlineKeyboardButton(board[r][c], callback_data=f"box_opened_{user_id}_{r}_{c}"))
            else:
                # Закрытая ячейка - кнопка выбора
                row.append(InlineKeyboardButton('❓', callback_data=f"box_cell_{user_id}_{r}_{c}"))
        keyboard.append(row)
    
    # Добавляем кнопки управления
    keyboard.append([
        InlineKeyboardButton("💰 Забрать выигрыш", callback_data=f"box_cashout_{user_id}"),
        InlineKeyboardButton("❌ Отменить", callback_data=f"box_cancel_{user_id}")
    ])
    
    return keyboard

def create_box_keyboard_start(board, user_id):
    """Создает начальную клавиатуру (все ячейки закрыты)"""
    return create_box_keyboard(board, user_id, [])

async def box_cell_click(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, r, c):
    """Обработка клика по ячейке"""
    query = update.callback_query

    session = BOX_SESSIONS.get(user_id)
    if not session or session['status'] != 'active':
        await safe_answer(query, "⚠️ Игра не активна!")
        return

    if (r, c) in session['opened']:
        await safe_answer(query, "🧐 Эта ячейка уже открыта!")
        return

    # Добавляем в открытые
    session['opened'].append((r, c))
    session['opened_count'] += 1

    # Проверяем попали ли на паутину
    if (r, c) in session['spiders']:
        session['status'] = 'lost'
        
        # Открываем все паутины
        for sr, sc in session['spiders']:
            session['board'][sr][sc] = '🕸'
        
        # Открываем все сокровища для показа
        for (tr, tc), treasure in session['treasures'].items():
            if (tr, tc) not in session['opened']:
                session['board'][tr][tc] = treasure

        board_text = "\n".join(''.join(row) for row in session['board'])
        possible_win = int(session['bet'] * session['current_mult'])

        message_text = (
            f"💎 <b>Шкатулка • вы проиграли!</b> ❌\n"
            f"••••••••••\n"
            f"💸 Ставка: <b>{format_amount(session['bet'])}</b>ms¢\n"
            f"🕸 Паутин: {BOX_SPIDERS_COUNT}\n"
            f"🔝 Пройдено: {session['opened_count']} из {BOX_SAFE_CELLS}\n"
            f"<blockquote>✔ Вы могли забрать {possible_win}ms¢</blockquote>\n"
            f"••••••••••\n"
            f"<code>{board_text}</code>\n"
            f"👩‍💻 Hash: {session['hash']}"
        )

        await context.bot.edit_message_text(
            message_text,
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            parse_mode='HTML'
        )

        await save_game_hash_async(session['hash'], user_id, 'box', session['bet'], 'lose')
        await safe_answer(query, "🕸 Вы запутались в паутине! Проигрыш!")
        return

    # Нашли сокровище
    treasure_type = session['treasures'][(r, c)]
    treasure = TREASURES[treasure_type]

    # Проверяем магическое ли сокровище (20% шанс)
    is_magical = random.random() < 0.6
    
    if is_magical:
        # Магическое сокровище - изменяет множитель (может быть как плюс, так и минус)
        min_mult, max_mult = treasure['magic_range']
        # magic_mult может быть как отрицательным, так и положительным
        magic_change = random.uniform(min_mult, max_mult) / 10
        session['current_mult'] *= (1 + magic_change)
        session['current_mult'] = max(0.3, min(10.0, session['current_mult']))  # Ограничиваем
        treasure_icon = '✨' + treasure_type
    else:
        # Обычное сокровище - увеличивает множитель по таблице
        mult_index = min(session['opened_count'], len(BOX_MULTIPLIERS)-1)
        session['current_mult'] = BOX_MULTIPLIERS[mult_index]
        treasure_icon = treasure_type

    session['board'][r][c] = treasure_icon
    session['found_treasures'].append(treasure_type)

    # Проверяем победу
    if session['opened_count'] >= BOX_SAFE_CELLS:
        session['status'] = 'won'
        win_amount = int(session['bet'] * session['current_mult'])
        
        # Открываем все оставшиеся сокровища для показа
        for (tr, tc), trea in session['treasures'].items():
            if (tr, tc) not in session['opened']:
                if (tr, tc) in session['spiders']:
                    session['board'][tr][tc] = '🕸'
                else:
                    session['board'][tr][tc] = trea
        
        board_text = "\n".join(''.join(row) for row in session['board'])
        
        await update_balance_async(user_id, win_amount)
        
        message_text = (
            f"💎 Шкатулка • вы победили! ✅\n"
            f"••••••••••\n"
            f"💸 Ставка: <b>{format_amount(session['bet'])}</b>ms¢\n"
            f"🕸 Паутин: {BOX_SPIDERS_COUNT}\n"
            f"📊 Выигрыш: x{session['current_mult']:.2f} / <b>{win_amount}</b>ms¢\n"
            f"••••••••••\n"
            f"<code>{board_text}</code>\n"
            f"👩‍💻 Hash: {session['hash']}"
        )
        
        await context.bot.edit_message_text(
            message_text,
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            parse_mode='HTML'
        )
        
        await save_game_hash_async(session['hash'], user_id, 'box', session['bet'], 'win')
        await update_user_stats_async(user_id, win_amount, 0)
        await safe_answer(query, f"🎉 Победа! Вы нашли все сокровища!")
        return

    # Игра продолжается
    current_win = int(session['bet'] * session['current_mult'])
    
    message_text = (
        f"💎 <b>Шкатулка • игра продолжается!</b>\n"
        f"••••••••••\n"
        f"💸 Ставка: <b>{format_amount(session['bet'])}</b>ms¢\n"
        f"🕸 Паутин: {BOX_SPIDERS_COUNT}\n"
        f"📊 Множитель: x{session['current_mult']:.2f} / <b>{current_win}</b>ms¢\n"
        f"📦 Найдено: {session['opened_count']} из {BOX_SAFE_CELLS}\n"
        f"••••••••••"
    )

    # Обновляем клавиатуру
    keyboard = create_box_keyboard(session['board'], user_id, session['opened'])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.edit_message_text(
        message_text,
        chat_id=session['chat_id'],
        message_id=session['message_id'],
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

    await safe_answer(query, f"✅ Вы нашли {treasure['name']}!")

async def box_cashout(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Забрать текущий выигрыш"""
    query = update.callback_query
    
    session = BOX_SESSIONS.get(user_id)
    if not session or session['status'] != 'active':
        await safe_answer(query, "⚠️ Нет активной игры!")
        return
    
    if len(session['opened']) == 0:
        await safe_answer(query, "❌ Сначала сделайте хотя бы один ход!")
        return
    
    win_amount = int(session['bet'] * session['current_mult'])
    
    # Открываем все ячейки для показа
    for (tr, tc), trea in session['treasures'].items():
        if (tr, tc) in session['spiders']:
            session['board'][tr][tc] = '🕸'
        elif (tr, tc) not in session['opened']:
            session['board'][tr][tc] = trea
    
    for sr, sc in session['spiders']:
        session['board'][sr][sc] = '🕸'
    
    board_text = "\n".join(''.join(row) for row in session['board'])
    
    await update_balance_async(user_id, win_amount)
    session['status'] = 'cashed_out'
    
    message_text = (
        f"💎<b> Шкатулка • вы забрали выигрыш!</b> ✅\n"
        f"••••••••••\n"
        f"💸 Ставка: <b>{format_amount(session['bet'])}</b>ms¢\n"
        f"💰 Выигрыш: x{session['current_mult']:.2f} / <b>{win_amount}</b>ms¢\n"
        f"📦 Найдено: {session['opened_count']} из {BOX_SAFE_CELLS}\n"
        f"••••••••••\n"
        f"<code>{board_text}</code>\n"
        f"👩‍💻 Hash: {session['hash']}"
    )
    
    await context.bot.edit_message_text(
        message_text,
        chat_id=session['chat_id'],
        message_id=session['message_id'],
        parse_mode='HTML'
    )
    
    await save_game_hash_async(session['hash'], user_id, 'box', session['bet'], 'win')
    await update_user_stats_async(user_id, win_amount, 0)
    await safe_answer(query, f"💰 Вы забрали {win_amount}ms¢!")
    
    # Удаляем сессию
    del BOX_SESSIONS[user_id]

async def box_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Отмена игры и возврат ставки"""
    query = update.callback_query
    
    session = BOX_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игра не найдена!")
        return
    
    if session['status'] != 'active' or len(session['opened']) > 0:
        await safe_answer(query, "❌ Нельзя отменить игру после первого хода!")
        return
    
    # Возвращаем ставку
    await update_balance_async(user_id, session['bet'])
    session['status'] = 'cancelled'
    
    # Удаляем сообщение с игрой
    try:
        await context.bot.delete_message(
            chat_id=session['chat_id'],
            message_id=session['message_id']
        )
    except:
        pass
    
    # Отправляем новое сообщение об отмене
    await query.answer("✅ Игра отменена")
    await context.bot.send_message(
        chat_id=session['chat_id'],
        text=f"❌ Игра отменена!\n💸 Ставка {format_amount(session['bet'])}ms¢ возвращена.",
        parse_mode='HTML'
    )
    
    del BOX_SESSIONS[user_id]

async def actionn_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для очистки портфеля акций: /actionn @username причина"""
    if not update.effective_user:
        return

    # Проверка прав администратора
    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    target_id = None
    target_name = None
    reason = None

    # Проверяем, ответил ли админ на сообщение
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.full_name
        
        # Причина из аргументов
        if context.args:
            reason = ' '.join(context.args)
        else:
            reason = "Не указана"

        # Очищаем портфель
        success = await clear_user_portfolio_async(target_id)
        
        if success:
            await update.message.reply_text(
                f"✅ Портфель акций пользователя {target_name} (ID: {target_id}) очищен.\n"
                f"📝 Причина: {reason}"
            )
        else:
            await update.message.reply_text(f"❌ Ошибка при очистке портфеля пользователя {target_name}")
        return

    # Если нет ответа, проверяем аргументы
    if len(context.args) < 2:
        await update.message.reply_text(
            "📝 Использование:\n"
            "/actionn *@username* *причина*\n"
            "/actionn *id* *причина*\n"
            "Или ответь на сообщение пользователя командой /actionn *причина*"
        )
        return

    target = context.args[0]
    reason = ' '.join(context.args[1:])

    # Поиск по username
    if target.startswith('@'):
        username = target[1:]
        user_data = await get_user_by_username_async(username)
        if not user_data:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
            return
        # ✅ Исправляем: используем индексы вместо .get()
        target_id = user_data['user_id']
        target_name = user_data.get('full_name') if isinstance(user_data, dict) else user_data['full_name'] or username
    else:
        # Поиск по ID
        try:
            target_id = int(target)
            user_data = await get_user_async(target_id)
            # ✅ Исправляем: преобразуем Row в dict если нужно
            if user_data:
                if isinstance(user_data, dict):
                    target_name = user_data.get('full_name') or user_data.get('username') or str(target_id)
                else:
                    # Это sqlite3.Row, используем индексы
                    target_name = user_data['full_name'] or user_data['username'] or str(target_id)
            else:
                target_name = str(target_id)
        except Exception as e:
            await update.message.reply_text(f"❌ Неверный формат ID. Ошибка: {e}")
            return

    # Очищаем портфель
    success = await clear_user_portfolio_async(target_id)
    
    if success:
        await update.message.reply_text(
            f"✅ Портфель акций пользователя {target_name} (ID: {target_id}) очищен.\n"
            f"📝 Причина: {reason}"
        )
    else:
        await update.message.reply_text(f"❌ Ошибка при очистке портфеля пользователя {target_name}")

async def set_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для установки точного баланса: /set @username сумма"""
    if not update.effective_user:
        return

    # Проверка прав администратора
    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    target_id = None
    target_name = None
    new_balance = None

    # Проверяем, ответил ли админ на сообщение
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.full_name
        
        if len(context.args) < 1:
            await update.message.reply_text("❌ Укажите сумму для установки баланса.")
            return
        
        try:
            new_balance = int(context.args[0])
            if new_balance < 0:
                await update.message.reply_text("❌ Баланс не может быть отрицательным.")
                return
        except ValueError:
            await update.message.reply_text("❌ Неверный формат суммы.")
            return

        # Устанавливаем баланс
        success = await set_balance_async(target_id, new_balance)
        
        if success:
            await update.message.reply_text(
                f"✅ Баланс пользователя {target_name} (ID: {target_id}) установлен на {format_amount(new_balance)}ms¢"
            )
        else:
            await update.message.reply_text(f"❌ Ошибка при установке баланса пользователя {target_name}")
        return

    # Если нет ответа, проверяем аргументы
    if len(context.args) < 2:
        await update.message.reply_text(
            "📝 Использование:\n"
            "/set *@username* *сумма*\n"
            "/set *id* *сумма*\n"
            "Или ответь на сообщение пользователя командой /set *сумма*"
        )
        return

    target = context.args[0]
    
    try:
        new_balance = int(context.args[1])
        if new_balance < 0:
            await update.message.reply_text("❌ Баланс не может быть отрицательным.")
            return
    except ValueError:
        await update.message.reply_text("❌ Неверный формат суммы.")
        return
    except IndexError:
        await update.message.reply_text("❌ Укажите сумму для установки баланса.")
        return

    # Поиск по username
    if target.startswith('@'):
        username = target[1:]
        user_data = await get_user_by_username_async(username)
        if not user_data:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
            return
        # ✅ Исправляем
        if isinstance(user_data, dict):
            target_id = user_data.get('user_id')
            target_name = user_data.get('full_name') or username
        else:
            target_id = user_data['user_id']
            target_name = user_data['full_name'] or username
    else:
        # Поиск по ID
        try:
            target_id = int(target)
            user_data = await get_user_async(target_id)
            if user_data:
                if isinstance(user_data, dict):
                    target_name = user_data.get('full_name') or user_data.get('username') or str(target_id)
                else:
                    target_name = user_data['full_name'] or user_data['username'] or str(target_id)
            else:
                target_name = str(target_id)
        except Exception as e:
            await update.message.reply_text(f"❌ Неверный формат ID. Ошибка: {e}")
            return

    # Устанавливаем баланс
    success = await set_balance_async(target_id, new_balance)
    
    if success:
        await update.message.reply_text(
            f"✅ Баланс пользователя {target_name} (ID: {target_id}) установлен на {format_amount(new_balance)}ms¢"
        )
    else:
        await update.message.reply_text(f"❌ Ошибка при установке баланса пользователя {target_name}")

async def spaceship_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Космолёт"""
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
    
    # Проверяем различные варианты команд
    cmd = parts[0].lower()
    if cmd not in ['/spaceship', 'космолёт', 'космо', '/space']:
        return

    # Если нет аргументов
    if len(parts) < 2:
        await update.message.reply_text(
            "<blockquote>ℹ️ Космолёт – игра в которой, вы — опасный контрабандист, за вами гонятся рейнджеры космоса чтобы забрать ваш товар, ваша задача облетать базы чужаков и сбежать от рейнджеров.</blockquote>\n\n"
            f"🤖 *{user_name}*, чтобы начать игру, используй команду:\n\n"
            "🛸 <b><i>/space [ставка]</i></b>\n\n"
            "Пример:\n"
            "/space 100\n"
            "космо все",
            parse_mode='HTML'
        )
        return

    try:
        # Получаем баланс пользователя
        db_user = await get_user_async(user_id, user.full_name, user.username)
        user_balance = db_user['balance']
        
        # Парсим ставку с поддержкой "всё"
        bet_amount = parse_bet_amount(parts[1], user_balance)

        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return

        if bet_amount > user_balance:
            await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(user_balance)}ms¢")
            return

        # Списываем ставку
        await update_balance_async(user_id, -bet_amount)

        # Создаем сессию с сеткой 6x3
        grid = []
        for level in range(6):
            row = ['ㅤ', 'ㅤ', 'ㅤ']
            # В каждом уровне прячем полицейского в случайной позиции
            police_pos = random.randint(0, 2)
            row[police_pos] = '👮'
            grid.append(row)

        SPACESHIP_SESSIONS = context.bot_data.get('SPACESHIP_SESSIONS', {})
        SPACESHIP_SESSIONS[user_id] = {
            'bet': bet_amount,
            'level': 0,
            'grid': grid,
            'opened': [],
            'user_name': user_name,
            'chat_id': update.effective_chat.id,
            'thread_id': update.effective_message.message_thread_id,
            'message_id': None,
            'status': 'active'
        }
        context.bot_data['SPACESHIP_SESSIONS'] = SPACESHIP_SESSIONS

        # Отправляем начальное сообщение
        await send_spaceship_board(update, context, user_id)

    except RetryAfter as e:
        await handle_flood(update, context, e)
    except Exception as e:
        logging.error(f"Error in spaceship command: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")
        if 'bet_amount' in locals():
            await update_balance_async(user_id, bet_amount)

async def send_spaceship_board(update, context, user_id):
    """Отправка начального игрового поля Космолёта"""
    SPACESHIP_SESSIONS = context.bot_data.get('SPACESHIP_SESSIONS', {})
    session = SPACESHIP_SESSIONS.get(user_id)
    
    if not session:
        return

    text = (
        f"🛸 {session['user_name']}, вы начали погоню!\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"📊 Выигрыш: x0.0 / 0"
    )
    
    # Только один ряд активных кнопок для первого уровня
    keyboard = [
        [InlineKeyboardButton("ㅤ", callback_data=f"spaceship_cell_{user_id}_0_0"),
         InlineKeyboardButton("ㅤ", callback_data=f"spaceship_cell_{user_id}_0_1"),
         InlineKeyboardButton("ㅤ", callback_data=f"spaceship_cell_{user_id}_0_2")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if session.get('message_id'):
        try:
            await context.bot.edit_message_text(
                text,
                chat_id=session['chat_id'],
                message_id=session['message_id'],
                reply_markup=reply_markup
            )
        except:
            msg = await context.bot.send_message(
                chat_id=session['chat_id'],
                text=text,
                reply_markup=reply_markup,
                message_thread_id=session['thread_id']
            )
            session['message_id']= msg.message_id
    else:
        msg = await context.bot.send_message(
            chat_id=session['chat_id'],
            text=text,
            reply_markup=reply_markup,
            message_thread_id=session['thread_id']
        )
        session['message_id'] = msg.message_id
    
    context.bot_data['SPACESHIP_SESSIONS'] = SPACESHIP_SESSIONS

async def spaceship_cell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, level: int, position: int):
    """Обработка нажатия на ячейку в Космолёте"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    # Сразу отвечаем на callback, чтобы кнопка перестала грузиться
    await safe_answer(query, "")

    SPACESHIP_SESSIONS = context.bot_data.get('SPACESHIP_SESSIONS', {})
    session = SPACESHIP_SESSIONS.get(user_id)
    
    if not session:
        await safe_answer(query, "❌ Игра не найдена!", show_alert=True)
        return

    if session['status'] != 'active':
        await safe_answer(query, "❌ Игра уже завершена", show_alert=True)
        return

    # Проверяем, что нажимают на текущий уровень
    if level != session['level']:
        await safe_answer(query, "🧐 Не тот уровень!", show_alert=True)
        return

    # Проверяем, не открыта ли уже эта клетка
    for opened in session['opened']:
        if opened['level'] == level and opened['position'] == position:
            await safe_answer(query, "❌ Эта клетка уже открыта", show_alert=True)
            return

    import random
    
    # Проверяем, не полицейский ли (30% шанс)
    is_police = random.randint(1, 100) <= 30
    
    # Отмечаем открытую клетку
    session['opened'].append({
        'level': level,
        'position': position,
        'is_police': is_police
    })
    
    if is_police:
        # Проигрыш
        session['status'] = 'lost'
        
        text = (
            f"🛸 {query.from_user.full_name}, вы проиграли, вас поймали!\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
            f"🔝 Пройдено: {session['level']} из 6\n"
            f"Вы попались рейнджерам..."
        )
        
        # Строим клавиатуру
        keyboard = []
        for lvl in range(5, -1, -1):
            row = []
            for pos in range(3):
                opened_cell = None
                for op in session['opened']:
                    if op['level'] == lvl and op['position'] == pos:
                        opened_cell = op
                        break
                
                if opened_cell:
                    if opened_cell['is_police']:
                        row.append(InlineKeyboardButton("👮‍♂️", callback_data="spaceship_dead"))
                    else:
                        row.append(InlineKeyboardButton("✅", callback_data="spaceship_dead"))
                else:
                    row.append(InlineKeyboardButton("ㅤ", callback_data="spaceship_dead"))
            keyboard.append(row)
        
        await update_user_stats_async(user_id, 0, session['bet'])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        
        # Удаляем сессию после проигрыша
        del SPACESHIP_SESSIONS[user_id]
        context.bot_data['SPACESHIP_SESSIONS'] = SPACESHIP_SESSIONS
        return
        
    # Безопасное нажатие - переходим на следующий уровень
    session['level'] += 1
    
    # Проверяем победу (все 6 уровней пройдены)
    if session['level'] >= 6:
        final_multiplier = SPACESHIP_MULTIPLIERS[6]
        win_amount = int(session['bet'] * final_multiplier)
        await update_balance_async(user_id, win_amount)
        await update_user_stats_async(user_id, win_amount, 0)
        
        text = (
            f"🛸 {query.from_user.full_name}, погоня окончена, вы сбежали!\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
            f"📊 Выигрыш: x{final_multiplier} / {format_amount(win_amount)}ms¢"
        )
        
        # Показываем все открытые клетки
        keyboard = []
        for lvl in range(5, -1, -1):
            row = []
            for pos in range(3):
                opened_cell = None
                for op in session['opened']:
                    if op['level'] == lvl and op['position'] == pos:
                        opened_cell = op
                        break
                
                if opened_cell:
                    row.append(InlineKeyboardButton("✅", callback_data="spaceship_dead"))
                else:
                    row.append(InlineKeyboardButton("ㅤ", callback_data="spaceship_dead"))
            keyboard.append(row)
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        
        del SPACESHIP_SESSIONS[user_id]
        context.bot_data['SPACESHIP_SESSIONS'] = SPACESHIP_SESSIONS
        return
    
    # Продолжаем игру
    current_multiplier = SPACESHIP_MULTIPLIERS[session['level']]
    current_win = int(session['bet'] * current_multiplier)
    
    text = (
        f"🛸 {query.from_user.full_name}, погоня продолжается!\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"📊 Выигрыш: x{current_multiplier} / {format_amount(current_win)}ms¢"
    )
    
    # Строим клавиатуру
    keyboard = []
    
    # Сначала ряд с новым уровнем (активные кнопки)
    new_row = []
    for pos in range(3):
        new_row.append(InlineKeyboardButton("ㅤ", callback_data=f"spaceship_cell_{user_id}_{session['level']}_{pos}"))
    keyboard.append(new_row)
    
    # Потом все пройденные уровни (снизу вверх)
    for lvl in range(session['level'] - 1, -1, -1):
        row = []
        for pos in range(3):
            opened_cell = None
            for op in session['opened']:
                if op['level'] == lvl and op['position'] == pos:
                    opened_cell = op
                    break
            
            if opened_cell:
                row.append(InlineKeyboardButton("✅", callback_data="spaceship_dead"))
            else:
                row.append(InlineKeyboardButton("ㅤ", callback_data="spaceship_dead"))
        keyboard.append(row)
    
    # Кнопка забрать награду
    keyboard.append([InlineKeyboardButton("✅ Забрать награду", callback_data=f"spaceship_take_{user_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup)
    
    # Сохраняем обновленную сессию
    context.bot_data['SPACESHIP_SESSIONS'] = SPACESHIP_SESSIONS

async def spaceship_take_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Забрать награду в Космолёте"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!", show_alert=True)
        return

    SPACESHIP_SESSIONS = context.bot_data.get('SPACESHIP_SESSIONS', {})
    session = SPACESHIP_SESSIONS.get(user_id)
    
    if not session or session['status'] != 'active':
        await safe_answer(query, "❌ Игра уже завершена", show_alert=True)
        return

    if session['level'] == 0:
        await safe_answer(query, "❌ Сначала пройдите хотя бы один уровень", show_alert=True)
        return

    current_multiplier = SPACESHIP_MULTIPLIERS[session['level']]
    win_amount = int(session['bet'] * current_multiplier)
    
    await update_balance_async(user_id, win_amount)
    await update_user_stats_async(user_id, win_amount, 0)
    
    text = (
        f"🛸 {query.from_user.full_name}, погоня окончена, вы забрали выигрыш!\n"
        f"💸 Ставка: {format_amount(session['bet'])}ms¢\n"
        f"📊 Выигрыш: x{current_multiplier} / {format_amount(win_amount)}ms¢\n"
        f"🔝 Пройдено: {session['level']} из 6"
    )
    
    await query.edit_message_text(text)
    
    del SPACESHIP_SESSIONS[user_id]
    context.bot_data['SPACESHIP_SESSIONS'] = SPACESHIP_SESSIONS
async def handle_flood(update: Update, context: ContextTypes.DEFAULT_TYPE, error: RetryAfter):
    """Единый обработчик Flood Control"""
    retry_after = error.retry_after
    
    # Если это callback query
    if update.callback_query:
        await safe_answer(
            update.callback_query, 
            f"⏳ Flood Control, ожидай {retry_after} сек.", 
            show_alert=True
        )
    # Если это обычное сообщение
    elif update.message:
        await update.message.reply_text(
            f"⏳ Слишком много запросов. Подождите {retry_after} секунд."
        )
    
    # Логируем
    logging.warning(f"Flood control for user {update.effective_user.id}: wait {retry_after}s")
    
    # Ждем указанное время
    await asyncio.sleep(retry_after)

async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    else:
        if len(context.args) < 1:
            await update.message.reply_text("❌ Использование: /unban *id or @username*")
            return
        
        target = context.args[0]
        
        if target.startswith('@'):
            username = target[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                return
            target_id = user_data['user_id']
        else:
            try:
                target_id = int(target)
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    ban_info = await is_user_banned_async(target_id)
    if not ban_info:
        await update.message.reply_text(f"❌ Пользователь не находится в бане.")
        return

    await unban_user_async(target_id)
    await update.message.reply_text(f"✅ Пользователь успешно разбанен.")

    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=f"✅ Ваша блокировка в боте снята."
        )
    except Exception as e:
        if "Forbidden" in str(e) or "blocked" in str(e):
            logging.info(f"User {target_id} blocked the bot, skipping unban notification")
        else:
            logging.error(f"Failed to send unban notification: {e}")

async def daily_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ежедневный бонус"""
    if not update.effective_user:
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    can_claim, streak, hours, minutes = await can_claim_daily(user_id)

    if not can_claim:
        await update.message.reply_text(
            f"😯 {user_name}, вы уже забирали сегодня бонус! Приходите через {hours} ч. {minutes} мин.",
            parse_mode='HTML'
        )
        return

    text = (
        f"🎁 {user_name}, чтобы получить ежедневный бонус, следуйте инструкции:\n\n"
        f"1️⃣ — Добавьте юзер @monstrminesbot в описании профиля (раздел \"О себе\")\n"
        f"••••••••••••••••\n"
        f"2️⃣ — Написать любую команду бота @monstrminesbot\n"
        f"••••••••••••••••\n"
        f"3️⃣ — Забрать бонус."
    )

    keyboard = [[InlineKeyboardButton("✅ Сделано", callback_data="daily_claim")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(text, reply_markup=reply_markup)

async def daily_claim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка и получение ежедневного бонуса"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_name = user.full_name

    await safe_answer(query, "⏳ Проверяю...")

    try:
        chat_member = await context.bot.get_chat(user_id)
        bio = getattr(chat_member, 'bio', '') or ''
        
        if '@monstrminesbot' not in bio:
            await query.edit_message_text("❌ Вы не выполнили все условия! Добавьте @monstrminesbot в описание профиля.")
            return
    except Exception as e:
        logging.error(f"Error checking bio: {e}")
        await query.edit_message_text("❌ Не удалось проверить профиль. Убедитесь, что у вас есть описание.")
        return

    can_claim, streak, hours, minutes = await can_claim_daily(user_id)
    
    if not can_claim:
        await query.edit_message_text(
            f"😯 {user_name}, вы уже забирали сегодня бонус! Приходите через {hours} ч. {minutes} мин."
        )
        return

    import random
    prize_type = random.choices(['money', 'case_daily', 'case_empty'], weights=[70, 15, 15])[0]
    
    if prize_type == 'money':
        amount = random.randint(10000, 100000)
        await update_balance_async(user_id, amount)
        await query.edit_message_text(f"✅ Вы получили {format_amount(amount)}ms¢!")
        
    elif prize_type == 'case_daily':
        await add_user_case(user_id, 'daily', 1)
        await query.edit_message_text(f"✅ Вы получили 🎊 Кейс «Daily»!")
        
    else:
        await add_user_case(user_id, 'empty', 1)
        await query.edit_message_text(f"✅ Вы получили 😑 Кейс «Пустышка»!")

    await claim_daily_bonus(user_id)

async def cases_command(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Просмотр кейсов пользователя"""
    if not update.effective_user:
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    # Получаем кейсы пользователя
    user_cases = await get_user_cases(user_id)
    
    if not user_cases:
        text = f"😯 {user_name}, похоже у тебя нет кейсов!"
        keyboard = [[InlineKeyboardButton("🔄 Обновить", callback_data="cases_refresh")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
        return

    # Сортируем кейсы по названию
    user_cases.sort()
    
    # Пагинация (по 6 кейсов на страницу)
    items_per_page = 6
    total_pages = (len(user_cases) + items_per_page - 1) // items_per_page
    start_idx = (page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, len(user_cases))
    
    text = f"💼 {user_name}, вот твои кейсы:\n\n"
    
    for i in range(start_idx, end_idx):
        case_type, quantity = user_cases[i]
        case_info = CASES_DATA.get(case_type, {'name': case_type, 'emoji': '📦'})
        text += f"{case_info['emoji']} {case_info['name']} — {quantity}шт.\n"
        text += f"•••••••••••••\n"
    
    # Создаем кнопки для кейсов (3 в ряд)
    keyboard = []
    row = []
    for i in range(start_idx, end_idx):
        case_type, quantity = user_cases[i]
        case_info = CASES_DATA.get(case_type, {'name': case_type, 'emoji': '📦'})
        button_text = f"{case_info['emoji']} {case_info['name']}"
        row.append(InlineKeyboardButton(button_text, callback_data=f"case_open_{case_type}"))
        
        if len(row) == 3 or i == end_idx - 1:
            keyboard.append(row)
            row = []
    
    # Кнопки навигации
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"cases_page_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("➡️", callback_data=f"cases_page_{page+1}"))
    keyboard.append(nav_row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def case_open_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, case_type: str):
    """Начало открытия кейса"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_name = user.full_name

    # Проверяем, есть ли у пользователя такой кейс
    user_cases = await get_user_cases(user_id)
    case_exists = False
    for ct, qty in user_cases:
        if ct == case_type and qty > 0:
            case_exists = True
            break
    
    if not case_exists:
        await safe_answer(query, "❌ У вас нет такого кейса!", show_alert=True)
        return

    case_data = CASES_DATA.get(case_type, CASES_DATA['empty'])
    
    # Создаем сессию открытия кейса
    CASES_SESSIONS = context.bot_data.get('CASES_SESSIONS', {})
    
    # Генерируем сетку 3x3 с наградами
    import random
    grid = []
    total_reward = 0
    
    for i in range(9):
        # Определяем, будет ячейка пустой или с деньгами
        if random.randint(1, 100) <= case_data['empty_chance']:
            grid.append({'type': 'empty', 'value': 0})
        else:
            reward = random.randint(case_data['min_reward'], case_data['max_reward'])
            grid.append({'type': 'money', 'value': reward})
            total_reward += reward
    
    CASES_SESSIONS[user_id] = {
        'case_type': case_type,
        'grid': grid,
        'opened': [],
        'opens_left': case_data['opens'],
        'total_reward': total_reward,
        'message_id': None,
        'chat_id': update.effective_chat.id,
        'thread_id': update.effective_message.message_thread_id,
        'status': 'active'
    }
    context.bot_data['CASES_SESSIONS'] = CASES_SESSIONS

    text = f"🔑 {user_name}, открывай ячейки!\n\n"
    
    # Создаем клавиатуру 3x3
    keyboard = []
    for i in range(0, 9, 3):
        row = []
        for j in range(3):
            idx = i + j
            row.append(InlineKeyboardButton("ㅤ", callback_data=f"case_cell_{user_id}_{idx}"))
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup)

async def case_cell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, cell_idx: int):
    """Обработка открытия ячейки кейса"""
    query = update.callback_query
    
    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваш кейс!", show_alert=True)
        return

    CASES_SESSIONS = context.bot_data.get('CASES_SESSIONS', {})
    session = CASES_SESSIONS.get(user_id)
    
    if not session or session['status'] != 'active':
        await safe_answer(query, "❌ Сессия открытия кейса не найдена или завершена", show_alert=True)
        return

    if cell_idx in session['opened']:
        await safe_answer(query, "❌ Эта ячейка уже открыта", show_alert=True)
        return

    if session['opens_left'] <= 0:
        await safe_answer(query, "❌ У вас больше нет попыток", show_alert=True)
        return

    # Открываем ячейку
    session['opened'].append(cell_idx)
    session['opens_left'] -= 1
    cell = session['grid'][cell_idx]
    
    opened_count = len(session['opened'])
    
    # Формируем текст с результатами открытых ячеек
    text = f"🔑 {query.from_user.full_name}, открывай ячейки!\n\n"
    
    for i, idx in enumerate(session['opened'], 1):
        cell_data = session['grid'][idx]
        if cell_data['type'] == 'money':
            text += f"{i}️⃣ — {format_amount(cell_data['value'])}ms¢\n"
        else:
            text += f"{i}️⃣ — пусто.\n"
    
    # Создаем обновленную клавиатуру
    keyboard = []
    for i in range(0, 9, 3):
        row = []
        for j in range(3):
            idx = i + j
            if idx in session['opened']:
                if session['grid'][idx]['type'] == 'money':
                    row.append(InlineKeyboardButton("☑️", callback_data=f"case_dead_{user_id}"))
                else:
                    row.append(InlineKeyboardButton("⚪", callback_data=f"case_dead_{user_id}"))
            else:
                row.append(InlineKeyboardButton("ㅤ", callback_data=f"case_cell_{user_id}_{idx}"))
        keyboard.append(row)
    
    # Проверяем, все ли попытки использованы
    if session['opens_left'] == 0:
        session['status'] = 'finished'
        
        # Подсчитываем общий выигрыш
        total_win = 0
        for idx in session['opened']:
            if session['grid'][idx]['type'] == 'money':
                total_win += session['grid'][idx]['value']
        
        # Удаляем кейс у пользователя
        await remove_user_case(user_id, session['case_type'], 1)
        
        # Начисляем выигрыш
        if total_win > 0:
            await update_balance_async(user_id, total_win)
            text = f"🎊 {query.from_user.full_name}, кейс открыт!\n\n"
            
            for i, idx in enumerate(session['opened'], 1):
                cell_data = session['grid'][idx]
                if cell_data['type'] == 'money':
                    text += f"{i}️⃣ — {format_amount(cell_data['value'])}ms¢\n"
                else:
                    text += f"{i}️⃣ — пусто.\n"
            
            text += f"\n💸 Общая сумма выигрыша {format_amount(total_win)}ms¢"
        else:
            text = f"😯 {query.from_user.full_name}, открытые ячейки были пусты."
        
        # Обновляем клавиатуру (все ячейки открыты)
        for i in range(0, 9, 3):
            for j in range(3):
                idx = i + j
                if session['grid'][idx]['type'] == 'money':
                    row[j] = InlineKeyboardButton("☑️", callback_data=f"case_finished_{user_id}")
                else:
                    row[j] = InlineKeyboardButton("⚪", callback_data=f"case_finished_{user_id}")
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup)
    
    if session['opens_left'] == 0:
        # Очищаем сессию
        del CASES_SESSIONS[user_id]
        context.bot_data['CASES_SESSIONS'] = CASES_SESSIONS
    else:
        context.bot_data['CASES_SESSIONS'] = CASES_SESSIONS


async def transfer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    target_user = None
    amount = 0

    if update.message.reply_to_message:
        reply_user = update.message.reply_to_message.from_user
        # Получаем реальное имя из БД
        target_data = await get_user_async(reply_user.id)
        if target_data:
            # Создаём объект с правильными данными
            class TempUser:
                def __init__(self, user_id, full_name, username):
                    self.id = user_id
                    self.full_name = full_name
                    self.username = username
            target_user = TempUser(
                reply_user.id,
                target_data.get('full_name', reply_user.full_name),
                target_data.get('username', reply_user.username)
            )
        else:
            class TempUser:
                def __init__(self, user_id, full_name, username):
                    self.id = user_id
                    self.full_name = full_name
                    self.username = username
            target_user = TempUser(
                reply_user.id,
                reply_user.full_name,
                reply_user.username
            )

        if len(context.args) >= 1:
            amount = parse_amount(context.args[0])
    else:
        if len(context.args) < 2:
            await update.message.reply_text(
                "❌ Использование:\n"
                "• п *юзернейм* *сумма*\n"
                "• перевод *юзернейм* *сумма*\n"
                "• /send *юзернейм* *сумма*\n"
                "• Или ответьте на сообщение пользователя: п *сумма*"
            )
            return

        target_identifier = context.args[0]
        amount_str = ' '.join(context.args[1:])
        amount = parse_amount(amount_str)

        class TempUser:
            def __init__(self, user_id, full_name, username):
                self.id = user_id
                self.full_name = full_name
                self.username = username

        if target_identifier.startswith('@'):
            username = target_identifier[1:]
            user_data = await get_user_by_username_async(username)
            if not user_data:
                await update.message.reply_text(f"❌ Пользователь {target_identifier} не найден.")
                return
            target_user = TempUser(
                user_data['user_id'],
                user_data.get('full_name') or username,
                username
            )
        else:
            try:
                target_id = int(target_identifier)
                target_data = await get_user_async(target_id)
                if not target_data:
                    await update.message.reply_text(f"❌ Пользователь с ID {target_id} не найден.")
                    return
                target_user = TempUser(
                    target_id,
                    target_data.get('full_name') or f"User {target_id}",
                    target_data.get('username')
                )
            except:
                await update.message.reply_text("❌ Неверный формат ID.")
                return

    if amount <= 0:
        await update.message.reply_text("❌ Неверная сумма.")
        return

    if target_user.id == user_id:
        await update.message.reply_text("❌ Нельзя перевести средства самому себе.")
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)

    if db_user['balance'] < amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Ваш баланс: {db_user['balance']}ms¢")
        return

    # ===== РАСЧЁТ КОМИССИИ =====
    settings = await get_user_settings_async(user_id)
    is_vip = await is_vip_user_async(user_id)
    
    if is_vip:
        commission_rate = 0.01
        if settings.get('transfer_commission', 1) == 0:
            commission = 0
        else:
            commission = int(amount * commission_rate)
    else:
        commission_rate = 0.10
        commission = int(amount * commission_rate)
    
    recipient_amount = amount - commission
    # ===== КОНЕЦ РАСЧЁТА КОМИССИИ =====

    transfer_id = generate_transfer_hash()
    target_mention = f"<a href='tg://user?id={target_user.id}'>{target_user.full_name}</a>"

    pending_transfers = context.bot_data.get('pending_transfers', {})
    transfer_confirmation = settings.get('transfer_confirmation', 1)

    if transfer_confirmation == 0:
        success, result = await transfer_money_async(user_id, target_user.id, amount, commission_rate, commission == 0)
        
        if not success:
            await update.message.reply_text(f"❌ {result}")
            return
        
        await update.message.reply_text(
            f"💸 Вы успешно перевели {recipient_amount}ms¢ пользователю {target_mention}.\n"
            f"🧾 Комиссия: {commission}ms¢",
            parse_mode='HTML'
        )
        
        try:
            await context.bot.send_message(
                chat_id=target_user.id,
                text=f"💰 {user.full_name} перевёл вам {recipient_amount}ms¢."
            )
        except:
            pass
        
        await update_user_stats_async(user_id, 0, 0)
        await update_user_stats_async(target_user.id, 0, 0)
        return

    pending_transfers[transfer_id] = {
        'from_id': user_id,
        'from_name': user.full_name,
        'to_id': target_user.id,
        'to_name': target_user.full_name,
        'amount': amount,
        'commission': commission,
        'recipient_amount': recipient_amount,
        'commission_rate': commission_rate,
        'time': time.time(),
        'message_id': update.message.message_id,
        'chat_id': update.effective_chat.id
    }

    context.bot_data['pending_transfers'] = pending_transfers

    keyboard = [[InlineKeyboardButton("☑ Подтверждаю", callback_data=f"confirm_transfer_{transfer_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"❓ {user.full_name}, вы хотите перевести {amount}ms¢ пользователю {target_mention}.\n\n"
        f"🧾 Комиссия: {commission}ms¢",
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

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
        confirmation_id = generate_transfer_hash()

        # Получаем transfer_confirmations из контекста
        transfer_confirmations = context.bot_data.get('transfer_confirmations', {})

        transfer_confirmations[confirmation_id] = {
            'from_id': transfer['from_id'],
            'from_name': transfer['from_name'],
            'to_id': transfer['to_id'],
            'to_name': transfer['to_name'],
            'amount': transfer['amount'],
            'commission': transfer['commission'],
            'recipient_amount': transfer['recipient_amount'],
            'commission_rate': transfer.get('commission_rate', 0.10),
            'time': time.time(),
            'original_message_id': transfer['message_id'],
            'original_chat_id': transfer['chat_id']
        }

        # Сохраняем transfer_confirmations
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
                    f"Вы хотите перевести {transfer['amount']}ms¢ пользователю {target_mention}.\n"
                    f"🧾 Комиссия: {transfer['commission']}ms¢\n"
                    f"📥 Получит: {transfer['recipient_amount']}ms¢\n\n"
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

        print(f"✅ Перевод ms¢ завершен, вызываю send_transfer_log")
        # Удаляем из pending_transfers
        del pending_transfers[transfer_id]
        context.bot_data['pending_transfers'] = pending_transfers

        await safe_answer(query, "✅ Перейдите в ЛС для подтверждения")

    except Exception as e:
        logging.error(f"Error in confirm_transfer: {e}")
        await safe_answer(query, "❌ Произошла ошибка. Попробуйте позже.")


async def final_confirm_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE, confirmation_id):
    """Финальное подтверждение перевода в ЛС"""
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

        # Проверяем баланс еще раз
        db_user = await get_user_async(user_id)
        if db_user['balance'] < transfer['amount']:
            await safe_answer(query, f"❌ Недостаточно средств. Ваш баланс: {format_amount(db_user['balance'])}ms¢", show_alert=True)
            return

        # Выполняем перевод (переводим полную сумму, функция сама вычтет комиссию)
        success, result = await transfer_money_async(
            transfer['from_id'],
            transfer['to_id'],
            transfer['amount'],
            commission_rate=transfer.get('commission_rate', 0.10),
            skip_commission=False
        )

        if not success:
            await safe_answer(query, f"❌ {result}", show_alert=True)
            return

        await update_user_stats_async(transfer['from_id'], 0, 0)
        await update_user_stats_async(transfer['to_id'], 0, 0)

        transfer['completed'] = True

        try:
            await query.edit_message_text(
                f"💸 Вы успешно перевели {result}ms¢ пользователю {transfer['to_name']}.\n"
                f"🧾 Комиссия: {transfer['commission']}ms¢"
            )
        except Exception as e:
            if "message can't be edited" not in str(e).lower():
                logging.error(f"Error editing message in final_confirm_transfer: {e}")

        try:
            new_balance = await get_user_async(transfer['to_id'])
            await context.bot.send_message(
                chat_id=transfer['to_id'],
                text=(
                    f"💰 {transfer['from_name']} перевёл вам {result}ms¢.\n"
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
                text=f"💸 Перевод {transfer['amount']}ms¢ пользователю {transfer['to_name']} успешно завершен!\n🧾 Комиссия: {transfer['commission']}ms¢"
            )
        except Exception as e:
            if "message can't be edited" not in str(e).lower() and "message to be edited not found" not in str(e).lower():
                logging.error(f"Error editing original message: {e}")

        # Отправляем лог о переводе
        try:
            from_user = {'user_id': transfer['from_id'], 'full_name': transfer['from_name']}
            to_user = {'user_id': transfer['to_id'], 'full_name': transfer['to_name']}
            await send_transfer_log(
                context,
                from_user,
                to_user,
                transfer['amount'],
                confirmation_id,
                is_msg=False,
                commission=transfer['commission']
            )
        except Exception as e:
            logging.error(f"Error sending transfer log: {e}")

        # Удаляем из transfer_confirmations
        del transfer_confirmations[confirmation_id]
        context.bot_data['transfer_confirmations'] = transfer_confirmations

        await update_task_progress_for_game(user_id, 'transfer', 1)
        await safe_answer(query, "✅ Перевод выполнен!")

    except Exception as e:
        logging.error(f"Error in final_confirm_transfer: {e}")
        await safe_answer(query, "❌ Произошла ошибка. Попробуйте позже.")

async def final_confirm_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE, confirmation_id):
    query = update.callback_query
    user_id = query.from_user.id

    try:
        transfer_confirmations = context.bot_data.get('transfer_confirmations', {})

        if confirmation_id not in transfer_confirmations:
            await safe_answer(query, "❌ Перевод не найден или устарел.")
            return

        transfer = transfer_confirmations[confirmation_id]

        if user_id != transfer['from_id']:
            await safe_answer(query, "🙈 Это не ваша кнопка!")
            return

        # Выполняем перевод (переводим сумму с учётом комиссии)
        success, message = await transfer_money_async(
            transfer['from_id'],
            transfer['to_id'],
            transfer['recipient_amount']  # изменено
        )

        if not success:
            await safe_answer(query, f"❌ {message}")
            return

        await update_user_stats_async(transfer['from_id'], 0, 0)
        await update_user_stats_async(transfer['to_id'], 0, 0)

        transfer['completed'] = True

        # Твой текст, добавил комиссию
        try:
            await query.edit_message_text(
                f"💸 Вы успешно перевели {transfer['recipient_amount']}ms¢ пользователю {transfer['to_name']}.\n"
                f"🧾 Комиссия: {transfer['commission']}ms¢"
            )
        except Exception as e:
            if "message can't be edited" not in str(e).lower():
                logging.error(f"Error editing message in final_confirm_transfer: {e}")

        try:
            new_balance = await get_user_async(transfer['to_id'])
            await context.bot.send_message(
                chat_id=transfer['to_id'],
                text=(
                    f"💰 {transfer['from_name']} перевёл вам {transfer['recipient_amount']}ms¢.\n"
                    f"Ваш новый баланс: {new_balance['balance']}ms¢"
                )
            )
        except Exception as e:
            if "Forbidden" in str(e) or "blocked" in str(e):
                logging.info(f"Recipient {transfer['to_id']} blocked the bot, skipping notification")
            else:
                logging.error(f"Failed to notify recipient: {e}")

        # Твой текст, добавил комиссию
        try:
            await context.bot.edit_message_text(
                chat_id=transfer['original_chat_id'],
                message_id=transfer['original_message_id'],
                text=f"💸 Перевод {transfer['amount']}ms¢ пользователю {transfer['to_name']} успешно завершен!\n🧾 Комиссия: {transfer['commission']}ms¢"
            )
        except Exception as e:
            if "message can't be edited" not in str(e).lower() and "message to be edited not found" not in str(e).lower():
                logging.error(f"Error editing original message: {e}")

        # 👇 ОТПРАВЛЯЕМ ЛОГ О ПЕРЕВОДЕ
        try:
            from_user = {'user_id': transfer['from_id'], 'full_name': transfer['from_name']}
            to_user = {'user_id': transfer['to_id'], 'full_name': transfer['to_name']}
            await send_transfer_log(
                context,
                from_user,
                to_user,
                transfer['amount'],
                confirmation_id,
                is_msg=False,
                commission=transfer['commission']
            )
        except Exception as e:
            logging.error(f"Error sending transfer log: {e}")

        del transfer_confirmations[confirmation_id]
        context.bot_data['transfer_confirmations'] = transfer_confirmations

        await update_task_progress_for_game(user_id, 'transfer', 1)
        await safe_answer(query, "✅ Перевод выполнен!")

    except Exception as e:
        logging.error(f"Error in final_confirm_transfer: {e}")
        await safe_answer(query, "❌ Произошла ошибка. Попробуйте позже.")

async def newcheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if len(context.args) < 2:
        await update.message.reply_text("❌ Использование: /newcheck *активаций* *сумма*")
        return

    try:
        activations = int(context.args[0])
        amount = parse_amount(context.args[1])
    except:
        await update.message.reply_text("❌ Неверный формат.")
        return

    if activations <= 0 or amount <= 0:
        await update.message.reply_text("❌ Неверные значения.")
        return

    check_code = generate_check_code()
    print(f"📝 Создаю чек: code={check_code}, activations={activations}, amount={amount}")
    
    await create_check_async(check_code, activations, amount)
    
    check_verify = await get_check_async(check_code)
    print(f"✅ Проверка создания: {check_verify}")

    check_link = f"https://t.me/{context.bot.username}?start=chk_{check_code}"

    await update.message.reply_text(
        f"🧾 {update.effective_user.full_name}, вы успешно создали чек на {activations} активаций суммой {amount}ms¢\n\n"
        f"🔗 Ссылка для чека: {check_link}"
    )

async def checklist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    list_link = f"https://t.me/{context.bot.username}?start=listcheck_{update.effective_user.id}"
    
    keyboard = [[InlineKeyboardButton("✅ Просмотреть чеки", url=list_link)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "☑ Нажмите ниже для просмотра чеков",
        reply_markup=reply_markup
    )

async def show_checklist(update: Update, context: ContextTypes.DEFAULT_TYPE, page=1):
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

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для создания чеков"""
    user = update.effective_user
    if not user:
        return
    
    # Если в группе - отправляем в ЛС
    if update.effective_chat.type in ["group", "supergroup"]:
        keyboard = [[InlineKeyboardButton(
            "📱 Перейти в ЛС",
            url=f"https://t.me/{context.bot.username}?start=create_check"
        )]]
        await update.message.reply_text(
            f"🧾 {user.full_name}, для продолжения перейди в ЛС.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    # Если нет аргументов - показываем меню
    if not context.args:
        # Показываем кнопку "Мои чеки" и подсказку
        keyboard = [[
            InlineKeyboardButton("🔰 Мои чеки", callback_data="my_checks")
        ]]
        await update.message.reply_text(
            f"<b>⚠️ {user.full_name}, неверный формат.</b>\n"
            f"/check [сумма] [активации]\n\n"
            f"Пример: /check 1000 1",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        return
    
    # Парсим аргументы: /check сумма активации
    if len(context.args) < 2:
        keyboard = [[
            InlineKeyboardButton("🔰 Мои чеки", callback_data="my_checks")
        ]]
        await update.message.reply_text(
            f"<b>⚠️ {user.full_name}, неверный формат.</b>\n"
            f"/check [сумма] [активации]\n\n"
            f"Пример: /check 1000 1",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        return
    
    try:
        # Парсим сумму с поддержкой к, кк
        amount_str = context.args[0].lower()
        amount = parse_amount_with_suffix(amount_str)
        
        activations = int(context.args[1])
        
        if activations <= 0 or activations > 100:
            await update.message.reply_text("Максимум активаций: 100 🔴")
            return
        
        if amount <= 0 or amount > 100_000_000_000:
            await update.message.reply_text("Максимальная сумма чека: 100.000.000.000ms¢ 🔴")
            return
        
        total_cost = amount * activations
        
        # Проверяем баланс
        db_user = await get_user_async(user.id, user.full_name, user.username)
        balance = db_user.get('balance', 0)
        
        if balance < total_cost:
            await update.message.reply_text("Недостаточно средств! 🔴")
            return
        
        # ✅ Проверяем наличие чековой книжки
        from handlers.checks import has_check_book
        has_book = await has_check_book(user.id)
        
        if not has_book:
            # Предлагаем купить чековую книжку
            await offer_check_book(update, context, user, amount, activations, total_cost)
            return
        
        # ✅ Если книжка есть - создаём чек
        from handlers.checks import create_check_async
        
        # Списываем средства
        await update_balance_async(user.id, -total_cost)
        
        # Создаём чек
        check_number, success = await create_check_async(user.id, amount, activations)
        
        if not success:
            await update_balance_async(user.id, total_cost)
            await update.message.reply_text("❌ Ошибка при создании чека!")
            return
        
        check_link = f"https://t.me/{context.bot.username}?start=chk_{user.id}_{check_number}"
        formatted_amount = format_amount(amount)
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Скопировать ссылку", callback_data=f"copy_link_{user.id}_{check_number}"),
                InlineKeyboardButton("🔑 Установить пароль", callback_data=f"set_password_{user.id}_{check_number}")
            ],
            [
                InlineKeyboardButton("💭 Комментарий", callback_data=f"set_comment_{user.id}_{check_number}")
            ]
        ])
        
        await update.message.reply_text(
            f"✅<b> ЧЕК #{check_number} СОЗДАН!</b>\n"
            f"•••••••••••\n"
            f"🔗 Скопируй ссылку, чтобы поделиться чеком:\n"
            f"<tg-spoiler><code>{check_link}</code></tg-spoiler>\n\n"
            f"👤 Пользователь, перешедший по ссылке, получит <b>{formatted_amount}</b>msCoin\n\n"
            f"<blockquote>⚠️ Чек может активировать любой пользователь! Не пересылайте чек в иные чаты.</blockquote>",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
    except ValueError as e:
        logging.error(f"Error in check_command: {e}")
        keyboard = [[
            InlineKeyboardButton("🔰 Мои чеки", callback_data="my_checks")
        ]]
        await update.message.reply_text(
            f"<b>⚠️ {user.full_name}, неверный формат.</b>\n"
            f"/check [сумма] [активации]\n\n"
            f"Пример: /check 1000 1",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

def parse_amount_with_suffix(amount_str: str) -> int:
    """Парсит сумму с суффиксами к, кк, м"""
    amount_str = amount_str.lower().replace('ms¢', '').strip()
    
    multipliers = {
        'кк': 1_000_000_000,
        'к': 1_000,
        'м': 1_000_000
    }
    
    for suffix, multiplier in multipliers.items():
        if amount_str.endswith(suffix):
            num = int(amount_str[:-len(suffix)])
            return num * multiplier
    
    return int(amount_str)


async def show_check_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user):
    """Показать меню чеков"""
    keyboard = [[
        InlineKeyboardButton("🔰 Мои чеки", callback_data="my_checks")
    ]]
    
    await update.message.reply_text(
        f"<b>⚠️ {user.full_name}, неверный формат.</b> /check [сумма] [активации]\n\n"
        f"Пример: /check 1000 1",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )


async def show_my_checks_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать кнопку Мои чеки"""
    keyboard = [[
        InlineKeyboardButton("🔰 Мои чеки", callback_data="my_checks")
    ]]
    await update.message.reply_text(
        "🔰 Нажмите кнопку, чтобы посмотреть свои чеки",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def offer_check_book(update: Update, context: ContextTypes.DEFAULT_TYPE, user, amount: int, activations: int, total_cost: int):
    """Предложение купить чековую книжку"""
    commission = 1_000_000
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("💳 Заплатить", callback_data=f"buy_check_book_{amount}_{activations}_{total_cost}")
    ]])
    
    await update.message.reply_text(
        f"💸 <b>ОПЛАТА КОМИССИ</b>\n"
        f"•••••••••••••\n"
        f"🗳️ <b>{user.full_name}</b>, для получения доступа к чековой книжке необходимо оплатить комиссию в размере: {format_amount(commission)}ms¢.\n\n"
        f"<i>ℹ️ После оплаты вы сможете создавать чеки бесплатно и в неограниченном количестве. Обратите внимание, что при нажатии на кнопку покупки, транзакция осуществляется без подтверждения, при условии наличия msCoin!</i>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

async def create_check_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, user, amount: int, activations: int, total_cost: int):
    """Создание чека (основной поток)"""
    # Сохраняем данные в context.user_data
    context.user_data['check_amount'] = amount
    context.user_data['check_activations'] = activations
    context.user_data['check_total'] = total_cost
    
    # Списываем средства
    await update_balance_async(user.id, -total_cost)
    
    # Создаём чек
    check_number, success = await create_check_async(user.id, amount, activations)
    
    if not success:
        await update_balance_async(user.id, total_cost)
        await update.message.reply_text("❌ Ошибка при создании чека!")
        return
    
    check_link = f"https://t.me/{context.bot.username}?start=chk_{user.id}_{check_number}"
    formatted_amount = format_amount(amount)
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 Скопировать ссылку", callback_data=f"copy_link_{user.id}_{check_number}"),
            InlineKeyboardButton("🔑 Установить пароль", callback_data=f"set_password_{user.id}_{check_number}")
        ],
        [
            InlineKeyboardButton("💭 Комментарий", callback_data=f"set_comment_{user.id}_{check_number}")
        ]
    ])
    
    await update.message.reply_text(
        f"✅<b> ЧЕК #{check_number} СОЗДАН!</b>\n"
        f"•••••••••••\n"
        f"🔗 Скопируй ссылку, чтобы поделиться чеком:\n"
        f"<tg-spoiler><code>{check_link}</code></tg-spoiler>\n\n"
        f"👤 Пользователь, перешедший по ссылке, получит <b>{formatted_amount}</b>msCoin\n\n"
        f"<blockquote>⚠️ Чек может активировать любой пользователь! Не пересылайте чек в иные чаты.</blockquote>",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


async def my_checks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Показать список чеков пользователя"""
    query = update.callback_query
    user = query.from_user
    
    checks = await get_user_checks_async(user.id, limit=6, offset=(page-1)*6)
    total_checks = await get_user_checks_count_async(user.id)
    total_pages = (total_checks + 5) // 6
    
    if not checks:
        await query.message.edit_text("🔴 У вас нет активных чеков!")
        return
    
    text = f"🟢 Активные чеки\n••••••••\n📄 Страница: {page}/{total_pages}\n\n"
    
    for check in checks:
        amount = format_amount(check['amount'])
        remaining = check['max_activations'] - check['used_count']
        check_link = f"https://t.me/{context.bot.username}?start=chk_{user.id}_{check['check_number']}"
        text += f"🎫 {amount}ms¢ • {remaining} активаций (#{check['check_number']})\n🔗 <code>{check_link}</code>\n\n"
    
    keyboard = []
    if page > 1:
        keyboard.append(InlineKeyboardButton("◀️ Назад", callback_data=f"my_checks_page_{page-1}"))
    if page < total_pages:
        keyboard.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"my_checks_page_{page+1}"))
    
    reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

async def mailing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    user_id = update.effective_user.id
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')

    if user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    mailing_data = context.bot_data.get('mailing_data', {})
    mailing_data[user_id] = {
        'step': 'awaiting_text',
        'markdown': False,
        'inline': False,
        'text': None,
        'time': time.time()
    }
    context.bot_data['mailing_data'] = mailing_data

    keyboard = [
        [
            InlineKeyboardButton("❌ Markdown text", callback_data="mailing_toggle_markdown"),
            InlineKeyboardButton("❌ Inline-keyboard", callback_data="mailing_toggle_inline")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "📃 Введите текст рассылки.\n\n"
        "Если вы хотите использовать кликабельный текст (текст с ссылкой) или инлайн кнопки поставьте галочки ниже\n\n"
        "Формат для текста с ссылкой: (*текст*|*ссылка*)\n"
        "Пример: Привет! (User|https://t.me/durov)\n\n"
        "Формат для кнопок:\n"
        "• Обычная кнопка: *inl|текст*\n"
        "• Кнопка-ссылка: *inl|текст\"ссылка\"*\n"
        "Пример: *inl|Нажми\"https://t.me/durov\"*",
        reply_markup=reply_markup
    )


async def mailing_handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    mailing_data = context.bot_data.get('mailing_data', {})

    if user_id not in mailing_data or mailing_data[user_id].get('step') != 'awaiting_text':
        return

    mailing_data[user_id]['text'] = update.message.text
    mailing_data[user_id]['step'] = 'awaiting_confirm'
    mailing_data[user_id]['message_id'] = update.message.message_id
    mailing_data[user_id]['chat_id'] = update.effective_chat.id
    context.bot_data['mailing_data'] = mailing_data

    keyboard = [[InlineKeyboardButton("☑ Подтверждаю", callback_data="mailing_confirm")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    preview_text = update.message.text

    if mailing_data[user_id]['markdown']:
        preview_text = "🔹 Markdown включен\n\n" + preview_text
    if mailing_data[user_id]['inline']:
        preview_text = "🔹 Inline-кнопки включены\n\n" + preview_text

    await update.message.reply_text(
        f"☑ Подтвердите рассылку\n\n{preview_text}",
        reply_markup=reply_markup
    )

async def darts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Дартс"""
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
    
    # Проверяем различные варианты команд
    cmd = parts[0].lower()
    if cmd not in ['дартс', '/darts', 'дс']:
        return

    # Если нет аргументов или их меньше 3
    if len(parts) < 3:
        await update.message.reply_text(
            "<blockquote>ℹ️ Дартс – это игра, в которой нужно попасть в центр мишени, чтобы получить максимальный множитель</blockquote>\n\n"
            f"🤖 *{user_name}*, чтобы начать игру, используй команду:\n\n"
            "🎯 /darts [ставка] [исход]\n\n"
            "<b>Пример:</b>\n"
            "/darts 100 ц\n"
            "дартс 100 мимо",
            parse_mode='HTML'
        )
        return

    try:
        # Получаем баланс пользователя
        db_user = await get_user_async(user_id, user.full_name, user.username)
        user_balance = db_user['balance']
        
        # Парсим ставку с поддержкой "всё"
        bet_amount = parse_bet_amount(parts[1], user_balance)
        
        # Парсим исход
        bet_choice = parts[2].lower()
        if bet_choice not in DARTS_BETS:
            await update.message.reply_text("❌ Неверный исход. Доступные: м/мимо, к/красное, ц/центр, б/белое")
            return
        
        bet_type = DARTS_BETS[bet_choice]

        # Проверяем корректность
        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return

        if bet_amount > user_balance:
            await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(user_balance)}ms¢")
            return

        # Списываем ставку
        await update_balance_async(user_id, -bet_amount)

        # Создаем сессию
        DARTS_SESSIONS = context.bot_data.get('DARTS_SESSIONS', {})
        DARTS_SESSIONS[user_id] = {
            'bet': bet_amount,
            'bet_type': bet_type,
            'bet_choice': bet_choice,
            'user_name': user_name,
            'chat_id': update.effective_chat.id,
            'thread_id': update.effective_message.message_thread_id
        }
        context.bot_data['DARTS_SESSIONS'] = DARTS_SESSIONS

        # Отправляем анимированный дротик
        darts_msg = await context.bot.send_dice(
            chat_id=update.effective_chat.id,
            emoji='🎯',
            message_thread_id=update.effective_message.message_thread_id
        )

        # Получаем результат сразу (значение от 1 до 6)
        dice_value = darts_msg.dice.value

        # ЗАПУСКАЕМ ОТДЕЛЬНУЮ ЗАДАЧУ - НИКАКОГО SLEEP В ОСНОВНОМ ПОТОКЕ!
        asyncio.create_task(
            process_darts_result(
                context,
                user_id,
                darts_msg.message_id,
                dice_value,
                DARTS_SESSIONS[user_id]
            )
        )

    except Exception as e:
        logging.error(f"Error in darts command: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")

async def process_darts_result(context: ContextTypes.DEFAULT_TYPE, user_id: int, darts_msg_id: int, dice_value: int, session: dict):
    """Обработка результата игры в Дартс (в отдельной задаче)"""
    try:
        # Ждем 4.5 секунды (только в этой задаче, не блокируя бота)
        await asyncio.sleep(4.5)
        
        # Определяем результат по значению кубика
        result_type, result_display = DARTS_RESULTS.get(dice_value, ('miss', 'мимо 😯'))
        
        # Определяем, выиграл ли игрок
        bet_type = session['bet_type']
        is_win = (result_type == bet_type)
        
        multiplier = DARTS_MULTIPLIERS.get(bet_type, 1.0)
        bet_amount = session['bet']
        user_name = session['user_name']

        if is_win:
            win_amount = int(bet_amount * multiplier)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)
            
            # Выбираем эмодзи для выбранного исхода
            if bet_type == 'center':
                choice_display = "центр 🎯"
            elif bet_type == 'red':
                choice_display = "красное 🔴"
            elif bet_type == 'white':
                choice_display = "белое ⚪"
            else:
                choice_display = "мимо 😯"
            
            text = (
                f"🎊 <b>Дартс • Победа! <tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji></b>\n"
                f"•••••••••••\n"
                f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji> Ставка: {format_amount(bet_amount)}ms¢\n"
                f"🎲 Выбрано: {choice_display}\n"
                f"💰 Выигрыш: x{multiplier} / {format_amount(win_amount)}ms¢\n"
                f"••••••••\n"
                f"<blockquote><b><tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji> Выпало:</b> {result_display}</blockquote>"
            )
        else:
            await update_user_stats_async(user_id, 0, bet_amount)
            
            # Выбираем эмодзи для выбранного исхода
            if bet_type == 'center':
                choice_display = "центр 🎯"
            elif bet_type == 'red':
                choice_display = "красное 🔴"
            elif bet_type == 'white':
                choice_display = "белое ⚪"
            else:
                choice_display = "мимо 😯"
            
            text = (
                f"😣<b> Дартс • Проигрыш!</b>\n"
                f"••••••••••••\n"
                f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji> Ставка: {format_amount(bet_amount)}ms¢\n"
                f"🎲 Выбрано: {choice_display}\n"
                f"•••••••••\n"
                f"<blockquote><b><tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji> Выпало:</b> {result_display}</blockquote>"
            )

        print(f"🎯 Результат дартса: dice_value={dice_value}")
        print(f"📊 Определено как: {DARTS_RESULTS.get(dice_value, ('unknown', 'неизвестно'))}")
        # Отправляем результат
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text=text,
            parse_mode='HTML',
            message_thread_id=session.get('thread_id'),
            reply_to_message_id=darts_msg_id
        )

        # Очищаем сессию
        DARTS_SESSIONS = context.bot_data.get('DARTS_SESSIONS', {})
        if user_id in DARTS_SESSIONS:
            del DARTS_SESSIONS[user_id]
            context.bot_data['DARTS_SESSIONS'] = DARTS_SESSIONS

    except Exception as e:
        logging.error(f"Error in process_darts_result: {e}")
        # Возвращаем ставку при ошибке
        await update_balance_async(user_id, session['bet'])
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text="❌ Произошла ошибка. Ставка возвращена.",
            message_thread_id=session.get('thread_id')
        )

async def bowling_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в боулинг"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    user_full_name = user.full_name

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    if user_id in MINES_SESSIONS:
        session = MINES_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del MINES_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Мины. Завершите её сначала.")
            return

    if user_id in GOLD_SESSIONS:
        session = GOLD_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del GOLD_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Золото. Завершите её сначала.")
            return

    if user_id in PYRAMID_SESSIONS:
        session = PYRAMID_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del PYRAMID_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Пирамида. Завершите её сначала.")
            return

    if user_id in BOWLING_SESSIONS:
        session = BOWLING_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del BOWLING_SESSIONS[user_id]
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
            "<blockquote>ℹ️ *Боулинг – это игра, в которой вам нужно сбить кегли, чтобы получить максимальный множитель</blockquote>\n\n"
            f"🤖 {user_full_name}, чтобы начать игру, используй команду:\n\n"
            "🎳 <code>/bowling [кегель] [ставка]</code>\n\n"
            "Пример:\n"
            "• <code>/bowling 2 100</code>\n"
            "• <code>бо страйк 100</code>\n"
            "• <code>бо мимо 100</code>\n"
            "• <code>бо 5 100</code> (страйк)",
            parse_mode='HTML'
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
            f"<b>{user_full_name}</b>\n"
            f"🎉 Боулинг - Победа!✅\n"
            f"•••••••\n"
            f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
            f"🎲 Выбрано: {choice_display}\n"
            f"💰 Выигрыш: x{multiplier} / {format_amount(win_amount)}ms¢\n"
            f"••••••••\n"
            f"<blockquote>⚡️ Итог: {result_display}</blockquote>"
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
            f"<b>{user_full_name}</b>\n"
            f"😵‍💫 Боулинг - Проигрыш!\n"
            f"•••••••••••\n"
            f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
            f"🎲 Выбрано: {choice_display}\n"
            f"••••••\n"
            f"<blockquote>⚡️ Итог: {result_display}</blockquote>"
        )
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=message_text,
        parse_mode='HTML',
        message_thread_id=update.effective_message.message_thread_id
    )

async def roulette_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в рулетку"""
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
    
    # Если нет аргументов
    if len(parts) == 1 or (len(parts) == 2 and parts[1].lower() in ['рул', 'roulette']):
        await update.message.reply_text(
            "<blockquote>ℹ️ Рулетка – это популярная игра, в которой вы должны угадать итог выпавшего значения</blockquote>\n\n"
            f"🤖 *{user_name}*, чтобы начать игру, используй команду:\n\n"
            "🎰 <i><u>/roulette [диапазон] [ставка]</u></i>\n\n"
            "<b>Доступные ставки и множители:</b>\n"
            "• красное (к) - x2\n"
            "• черное (ч) - x2\n"
            "• четное - x2\n"
            "• нечётное - x2\n"
            "• большие (19-36) - x2\n"
            "• малые (1-18) - x2\n"
            "• 1-12 - x3\n"
            "• 13-24 - x3\n"
            "• 25-36 - x3\n"
            "• число (0-36) - x36\n\n"
            "<b>Примеры:</b>\n"
            "/roulette к 10к\n"
            "рул 13-24 100к\n"
            "рул 7 1кк",
            parse_mode='HTML'
        )
        return

    if len(parts) < 3:
        await update.message.reply_text("❌ Неправильный формат. Используйте: рул [диапазон] [ставка]")
        return

    bet_range = parts[1]
    bet_amount_str = parts[2]

    # Парсим ставку
    db_user = await get_user_async(user_id, user.full_name, user.username)
    bet_amount = parse_bet_amount(bet_amount_str, db_user['balance'])
    
    if bet_amount <= 0:
        await update.message.reply_text("❌ Неверная сумма ставки.")
        return

    # Парсим диапазон
    bet_info = parse_roulette_bet(bet_range)
    if not bet_info:
        await update.message.reply_text("❌ Неверный диапазон. Проверьте список доступных ставок.")
        return

    bet_type, bet_value, multiplier = bet_info

    # Проверяем баланс
    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(db_user['balance'])}ms¢")
        return

    # Списываем ставку
    await update_balance_async(user_id, -bet_amount)

    # Создаем сессию
    ROULETTE_SESSIONS = context.bot_data.get('ROULETTE_SESSIONS', {})
    ROULETTE_SESSIONS[user_id] = {
        'bet': bet_amount,
        'bet_type': bet_type,
        'bet_value': bet_value,
        'multiplier': multiplier,
        'user_name': user_name,
        'chat_id': update.effective_chat.id,
        'thread_id': update.effective_message.message_thread_id
    }
    context.bot_data['ROULETTE_SESSIONS'] = ROULETTE_SESSIONS

    # Отправляем конкретный эмодзи рулетки с указанным ID
    roulette_text = "<tg-emoji emoji-id='5416126611214846609'>🎰</tg-emoji>"

    roulette_msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=roulette_text,
        parse_mode='HTML',
        message_thread_id=update.effective_message.message_thread_id
    )

    # Запускаем обработку результата
    asyncio.create_task(
        process_roulette_result(
            context,
            user_id,
            roulette_msg.message_id,
            ROULETTE_SESSIONS[user_id]
        )
    )

async def process_roulette_result(context: ContextTypes.DEFAULT_TYPE, user_id: int, roulette_msg_id: int, session: dict):
    """Обработка результата игры в рулетку"""
    try:
        # Ждем 3 секунды (время анимации)
        await asyncio.sleep(3)

        # Генерируем случайное число от 0 до 36
        import random
        result = random.randint(0, 36)
        
        # Определяем цвет
        color = ROULETTE_COLORS.get(result, "⚫")
        
        # Определяем, выиграл ли игрок
        win = False
        bet_type = session['bet_type']
        bet_value = session['bet_value']
        
        if bet_type == 'red':
            win = color == "🔴"
        elif bet_type == 'black':
            win = color == "⚫"
        elif bet_type == 'even':
            win = result != 0 and result % 2 == 0
        elif bet_type == 'odd':
            win = result != 0 and result % 2 == 1
        elif bet_type == 'high':
            win = 19 <= result <= 36
        elif bet_type == 'low':
            win = 1 <= result <= 18
        elif bet_type == '1-12':
            win = 1 <= result <= 12
        elif bet_type == '13-24':
            win = 13 <= result <= 24
        elif bet_type == '25-36':
            win = 25 <= result <= 36
        elif bet_type == 'number':
            win = result == bet_value

        multiplier = session['multiplier']
        bet_amount = session['bet']

        if win:
            win_amount = int(bet_amount * multiplier)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)
            
            text = (
                f"<blockquote><tg-emoji emoji-id='5235989279024373566'>🎰</tg-emoji> Итоги игры «Рулетка»</blockquote>\n\n"
                f"<b><tg-emoji emoji-id='5415683280395585071'>🎰</tg-emoji> Выпало: {result} {color}</b>\n\n"
                f"<tg-emoji emoji-id='5415594207068822547'>🤑</tg-emoji> Выигрыш: x{multiplier} / {format_amount(win_amount)}ms¢"
            )
        else:
            await update_user_stats_async(user_id, 0, bet_amount)
            
            text = (
                f"<blockquote><tg-emoji emoji-id='5235989279024373566'>🎰</tg-emoji> Итоги игры «Рулетка»</blockquote>\n\n"
                f"<b><tg-emoji emoji-id='5415683280395585071'>🎰</tg-emoji> Выпало: {result} {color}</b>\n\n"
                f"<tg-emoji emoji-id='5373272140499918095'>😕</tg-emoji> Вы проиграли {format_amount(bet_amount)}ms¢!"
            )

        await context.bot.send_message(
            chat_id=session['chat_id'],
            text=text,
            parse_mode='HTML',
            message_thread_id=session.get('thread_id'),
            reply_to_message_id=roulette_msg_id
        )

        # Очищаем сессию
        ROULETTE_SESSIONS = context.bot_data.get('ROULETTE_SESSIONS', {})
        if user_id in ROULETTE_SESSIONS:
            del ROULETTE_SESSIONS[user_id]
            context.bot_data['ROULETTE_SESSIONS'] = ROULETTE_SESSIONS

    except Exception as e:
        logging.error(f"Error in process_roulette_result: {e}")
        await update_balance_async(user_id, session['bet'])
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text="❌ Произошла ошибка. Ставка возвращена.",
            message_thread_id=session.get('thread_id')
        )


async def set_msg_rate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админская команда для установки курса: !setmsgrate 50000"""
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("❌ Использование: !setmsgrate *курс*")
        return

    try:
        new_rate = int(context.args[0])
        if new_rate < 1000:
            await update.message.reply_text("❌ Курс не может быть меньше 1000")
            return
        
        await update_msg_rate(new_rate)
        await update.message.reply_text(f"✅ Курс MSG установлен: {format_amount(new_rate)} ms¢")
        
        # Отправляем в канал
        KURS_CHANNEL = context.bot_data.get('KURS_MSG_CHANNEL', '@kursmsgmonstr')
        text = (
            f"<tg-emoji emoji-id='5375338737028841420'>🔄</tg-emoji> MSG: 📊 {format_amount(new_rate)} ms¢ (установлено администратором)"
        )
        try:
            await context.bot.send_message(
                chat_id=KURS_CHANNEL,
                text=text,
                parse_mode='HTML'
            )
        except:
            pass
            
    except ValueError:
        await update.message.reply_text("❌ Неверный формат числа")


async def donat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для доната и конвертации"""
    if not update.effective_user:
        return

    # Проверяем, в чате ли вызвана команда
    if update.effective_chat.type in ["group", "supergroup"]:
        user_name = update.effective_user.full_name
        keyboard = [[InlineKeyboardButton(
            "📱 Перейти в ЛС",
            url=f"https://t.me/{context.bot.username}?start=donat"
        )]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            f"⚙️ {user_name}, донат доступен только в ЛС!",
            reply_markup=reply_markup
        )
        return

    user = update.effective_user
    user_name = user.full_name

    # Получаем баланс MSG
    db_user = await get_user_async(user.id, user.full_name, user.username)
    msg_balance = db_user.get('msg_balance', 0)
    current_rate = await get_msg_rate()

    # Текст с кастомными эмодзи через HTML
    text = (
        f"<tg-emoji emoji-id='5373351094883719887'>🍩</tg-emoji> {user_name}, донат:\n\n"
        f"<tg-emoji emoji-id='5402418909557053333'>🍯</tg-emoji> Ваш баланс: {format_amount(msg_balance)} MSG\n"
        f"<tg-emoji emoji-id='5402186569006210455'>💱</tg-emoji> Текущий курс: 1 MSG = {format_amount(current_rate)} ms¢"
    )

    # Кнопки с обычными эмодзи
    keyboard = [
        [InlineKeyboardButton("💱 MSG to ms¢", callback_data="donat_exchange")],
        [InlineKeyboardButton("🛒 Донат", url="https://t.me/kleymorf")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        text=text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )


async def donat_exchange_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать форму для обмена MSG"""
    query = update.callback_query
    user = query.from_user
    user_name = user.full_name

    # Получаем баланс MSG и текущий курс
    db_user = await get_user_async(user.id)
    msg_balance = db_user.get('msg_balance', 0)
    current_rate = await get_msg_rate()

    text = (
        f"<tg-emoji emoji-id='5402186569006210455'>💱</tg-emoji> {user_name}, текущий курс за 1 MSG = {format_amount(current_rate)} ms¢\n"
        f"<tg-emoji emoji-id='5402418909557053333'>🍯</tg-emoji> У вас {format_amount(msg_balance)} MSG, введите количество которое вы хотите обменять."
    )

    keyboard = [
        [
            InlineKeyboardButton("🍯 10 MSG", callback_data="donat_amount_10"),
            InlineKeyboardButton("🍯 50 MSG", callback_data="donat_amount_50"),
            InlineKeyboardButton("🍯 100 MSG", callback_data="donat_amount_100")
        ],
        [InlineKeyboardButton(f"🍯 Макс: {format_amount(msg_balance)} MSG", callback_data="donat_amount_max")],
        [InlineKeyboardButton("◀️ Назад", callback_data="donat_back")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    context.user_data['donat_step'] = 'waiting_amount'

    try:
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except Exception as e:
        if "Message is not modified" not in str(e):
            logging.error(f"Error in donat_exchange_callback: {e}")


async def donat_amount_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: int):
    """Обработка выбранной суммы"""
    query = update.callback_query
    user = query.from_user
    user_name = user.full_name

    # Получаем баланс MSG и текущий курс
    db_user = await get_user_async(user.id)
    msg_balance = db_user.get('msg_balance', 0)
    current_rate = await get_msg_rate()

    if amount > msg_balance:
        await safe_answer(query, f"❌ У вас только {format_amount(msg_balance)} MSG", show_alert=True)
        return

    # Рассчитываем получаемые ms¢
    msc_amount = amount * current_rate
    
    # Сохраняем в контексте
    context.user_data['donat_amount'] = amount
    context.user_data['donat_msc'] = msc_amount
    if 'donat_step' in context.user_data:
        del context.user_data['donat_step']

    text = (
        f"<tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji> {user_name}, вы уверены что хотите обменять {format_amount(amount)} MSG на {format_amount(msc_amount)} ms¢?"
    )

    keyboard = [[
        InlineKeyboardButton("✅ Подтверждаю", callback_data="donat_confirm")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except Exception as e:
        if "Message is not modified" not in str(e):
            logging.error(f"Error in donat_amount_callback: {e}")


async def donat_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение обмена MSG на ms¢"""
    query = update.callback_query
    user = query.from_user
    user_name = user.full_name

    amount = context.user_data.get('donat_amount')
    msc_amount = context.user_data.get('donat_msc')

    if not amount or not msc_amount:
        await safe_answer(query, "❌ Ошибка: данные не найдены", show_alert=True)
        context.user_data.clear()
        return

    # Проверяем баланс еще раз
    db_user = await get_user_async(user.id)
    msg_balance = db_user.get('msg_balance', 0)

    if msg_balance < amount:
        await safe_answer(query, f"❌ Недостаточно MSG. Баланс: {format_amount(msg_balance)}", show_alert=True)
        context.user_data.clear()
        return

    # Списываем MSG и начисляем ms¢
    from database import update_user_msg_async, update_balance_async
    await update_user_msg_async(user.id, -amount)
    await update_balance_async(user.id, msc_amount)

    text = (
        f"<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji> Вы успешно обменяли {format_amount(amount)} MSG на {format_amount(msc_amount)} ms¢."
    )

    keyboard = [[InlineKeyboardButton("◀️ В меню доната", callback_data="donat_back")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except Exception as e:
        if "Message is not modified" not in str(e):
            logging.error(f"Error in donat_confirm_callback: {e}")
    
    context.user_data.clear()


async def donat_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вернуться в меню доната"""
    query = update.callback_query
    user = query.from_user
    user_name = user.full_name

    context.user_data.clear()

    db_user = await get_user_async(user.id)
    msg_balance = db_user.get('msg_balance', 0)
    current_rate = await get_msg_rate()

    text = (
        f"<tg-emoji emoji-id='5373351094883719887'>🍩</tg-emoji> {user_name}, донат:\n\n"
        f"<tg-emoji emoji-id='5402418909557053333'>🍯</tg-emoji> Ваш баланс: {format_amount(msg_balance)} MSG\n"
        f"<tg-emoji emoji-id='5402186569006210455'>💱</tg-emoji> Текущий курс: 1 MSG = {format_amount(current_rate)} ms¢"
    )

    keyboard = [
        [InlineKeyboardButton("💱 MSG to ms¢", callback_data="donat_exchange")],
        [InlineKeyboardButton("🛒 Донат", url="https://t.me/kleymorf")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except Exception as e:
        if "Message is not modified" not in str(e):
            logging.error(f"Error in donat_back_callback: {e}")


async def donat_handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстового ввода суммы для обмена"""
    if 'donat_step' not in context.user_data or context.user_data['donat_step'] != 'waiting_amount':
        return

    user = update.effective_user
    text = update.message.text.strip()

    try:
        amount = int(text)
    except ValueError:
        await update.message.reply_text("❌ Пожалуйста, введите число")
        return

    # Получаем баланс и курс
    db_user = await get_user_async(user.id)
    msg_balance = db_user.get('msg_balance', 0)
    current_rate = await get_msg_rate()

    if amount <= 0:
        await update.message.reply_text("❌ Сумма должна быть больше 0")
        return

    if amount > msg_balance:
        await update.message.reply_text(f"❌ У вас только {format_amount(msg_balance)} MSG")
        return

    # Рассчитываем получаемые ms¢
    msc_amount = amount * current_rate

    # Сохраняем в контексте
    context.user_data['donat_amount'] = amount
    context.user_data['donat_msc'] = msc_amount
    context.user_data['donat_step'] = 'confirm'

    response_text = (
        f"<tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji> {user.full_name}, вы уверены что хотите обменять {format_amount(amount)} MSG на {format_amount(msc_amount)} ms¢?"
    )

    keyboard = [[
        InlineKeyboardButton("✅ Подтверждаю", callback_data="donat_confirm")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        text=response_text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

async def coinflip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Монетку"""
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
    
    # Проверяем различные варианты команд
    cmd = parts[0].lower()
    if cmd not in ['монетка', '/coinflip', 'мон']:
        return

    # Если нет аргументов или их меньше 3
    if len(parts) < 3:
        await update.message.reply_text(
            "<blockquote>ℹ️ Монетка – это игра в которой вы должны угадать исход упавшей монетки. Что будет орел или решка.</blockquote>\n\n"
            f"🤖 <b>{user_name}</b>, чтобы начать игру, используй команду:\n\n"
            "🪙 /coinflip [исход] [ставка]\n\n"
            "Пример:\n"
            "/coinflip орел 100\n"
            "монетка решка все",
            parse_mode='HTML'
        )
        return

    try:
        # Получаем исход (орел/решка)
        bet_choice = parts[1].lower()
        if bet_choice not in ['орел', 'решка']:
            await update.message.reply_text("❌ Неверный исход. Доступные: орел, решка")
            return

        # Получаем баланс пользователя
        db_user = await get_user_async(user_id, user.full_name, user.username)
        user_balance = db_user['balance']
        
        # Парсим ставку с поддержкой "всё"
        bet_amount = parse_bet_amount(parts[2], user_balance)

        # Проверяем корректность
        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return

        if bet_amount > user_balance:
            await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(user_balance)}ms¢")
            return

        # Списываем ставку
        await update_balance_async(user_id, -bet_amount)

        # Создаем сессию
        COINFLIP_SESSIONS = context.bot_data.get('COINFLIP_SESSIONS', {})
        COINFLIP_SESSIONS[user_id] = {
            'bet': bet_amount,
            'choice': bet_choice,
            'user_name': user_name,
            'chat_id': update.effective_chat.id,
            'thread_id': update.effective_message.message_thread_id
        }
        context.bot_data['COINFLIP_SESSIONS'] = COINFLIP_SESSIONS

        # Отправляем сообщение о подбрасывании
        await update.message.reply_text("Подбрасываю монетку..")

        # Отправляем эмодзи монетки
        coin_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="🪙",
            message_thread_id=update.effective_message.message_thread_id
        )

        # Запускаем обработку результата
        asyncio.create_task(
            process_coinflip_result(
                context,
                user_id,
                coin_msg.message_id,
                COINFLIP_SESSIONS[user_id]
            )
        )

    except RetryAfter as e:
        retry_after = e.retry_after
        await update.message.reply_text(f"⏳ Flood Control, ожидай {retry_after} сек.")
        logging.warning(f"Flood control in coinflip command: wait {retry_after}s")
        await asyncio.sleep(retry_after)
    except Exception as e:
        logging.error(f"Error in coinflip command: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")

async def process_coinflip_result(context: ContextTypes.DEFAULT_TYPE, user_id: int, coin_msg_id: int, session: dict):
    """Обработка результата игры в Монетку"""
    try:
        # Ждем немного для анимации
        await asyncio.sleep(2)

        # Рандомно выбираем результат
        import random
        result = random.choice(['орел', 'решка'])
        result_display = COINFLIP_RESULTS[result]

        # Определяем, выиграл ли игрок
        bet_choice = session['choice']
        is_win = (result == bet_choice)

        bet_amount = session['bet']
        user_name = session['user_name']

        if is_win:
            win_amount = int(bet_amount * COINFLIP_MULTIPLIER)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)

            choice_display = COINFLIP_RESULTS[bet_choice]

            text = (
                f"🎊<b>Монета • Победа!<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji></b>\n"
                f"•••••••••\n"
                f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji>Ставка: {format_amount(bet_amount)}ms¢\n"
                f"🎲 Выбрано: {choice_display}\n"
                f"📊 Выигрыш: x{COINFLIP_MULTIPLIER} / {format_amount(win_amount)}ms¢\n"
                f"•••••••••\n"
                f"<blockquote><tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji>Выпало: {result_display}</blockquote>"
            )
        else:
            await update_user_stats_async(user_id, 0, bet_amount)

            choice_display = COINFLIP_RESULTS[bet_choice]

            text = (
                f"😣<b> Монета • Проигрыш!</b>\n"
                f"••••••••••\n"
                f"<tg-emoji emoji-id='5472030678633684592'>💸</tg-emoji>Ставка: {format_amount(bet_amount)}ms¢\n"
                f"🎲 Выбрано: {choice_display}\n"
                f"•••••••••\n"
                f"<blockquote><tg-emoji emoji-id='5258203794772085854'>⚡️</tg-emoji>Выпало: {result_display}</blockquote>"
            )

        # Отправляем результат
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text=text,
            parse_mode='HTML',
            message_thread_id=session.get('thread_id'),
            reply_to_message_id=coin_msg_id
        )

        # Очищаем сессию
        COINFLIP_SESSIONS = context.bot_data.get('COINFLIP_SESSIONS', {})
        if user_id in COINFLIP_SESSIONS:
            del COINFLIP_SESSIONS[user_id]
            context.bot_data['COINFLIP_SESSIONS'] = COINFLIP_SESSIONS

    except Exception as e:
        logging.error(f"Error in process_coinflip_result: {e}")
        await update_balance_async(user_id, session['bet'])
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text="❌ Произошла ошибка. Ставка возвращена.",
            message_thread_id=session.get('thread_id')
        )
async def update_msg_rate_job(context: ContextTypes.DEFAULT_TYPE):
    """Обновление курса MSG каждые 10 минут"""
    current_rate = await get_msg_rate()

    # Рандомно выбираем, повысится курс или понизится (50/50)
    import random
    
    # Если курс вышел за пределы, корректируем направление
    if current_rate >= 48000:
        # Слишком высокий - заставляем понижаться
        force_down = True
        force_up = False
    elif current_rate <= 27000:
        # Слишком низкий - заставляем повышаться
        force_down = False
        force_up = True
    else:
        # В норме - случайное направление
        force_down = False
        force_up = False
    
    if force_up or (not force_down and random.choice([True, False])):
        # Повышение: 500 — 2.000
        increase = random.randint(500, 2000)
        new_rate = current_rate + increase
        change_emoji = "📈"
    else:
        # Понижение: 300 — 1.500
        decrease = random.randint(300, 1500)
        new_rate = current_rate - decrease
        change_emoji = "📉"
    
    # Жестко ограничиваем диапазон 25.000 - 50.000
    new_rate = max(25000, min(50000, new_rate))

    # Сохраняем новый курс
    await update_msg_rate(new_rate)

    # Отправляем в канал
    KURS_CHANNEL = context.bot_data.get('KURS_MSG_CHANNEL', '@kursmsgmonstr')

    text = (
        f"<tg-emoji emoji-id='5375338737028841420'>🔄</tg-emoji> MSG: {change_emoji} {format_amount(new_rate)} ms¢"
    )

    try:
        await context.bot.send_message(
            chat_id=KURS_CHANNEL,
            text=text,
            parse_mode='HTML'
        )
        print(f"📊 Курс MSG обновлен: {current_rate} -> {new_rate} ({change_emoji})")
    except Exception as e:
        logging.error(f"Failed to send kurs update: {e}")


async def give_vip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для выдачи VIP статуса: /gvip @username 10 часов 60 минут"""
    if not update.effective_user:
        return

    user = update.effective_user
    user_id = user.id

    # Проверка на админа
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return

    # Проверка аргументов
    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ Использование:\n"
            "`/gvip @username 10 часов`\n"
            "`/gvip @username 60 минут`\n"
            "`/gvip @username 1 день 5 часов`\n"
            "`/gvip @username 30 секунд`",
            parse_mode='Markdown'
        )
        return

    # Определяем пользователя
    target_identifier = context.args[0]
    target_id = None
    target_name = None
    
    # Ищем пользователя
    if target_identifier.startswith('@'):
        username = target_identifier[1:]
        user_data = await get_user_by_username_async(username)
        if not user_data:
            await update.message.reply_text(f"❌ Пользователь {target_identifier} не найден.")
            return
        # sqlite3.Row - используем индексы или dict()
        target_id = user_data['user_id']
        target_name = user_data['full_name'] or username
    else:
        try:
            target_id = int(target_identifier)
            target_data = await get_user_async(target_id)
            if not target_data:
                await update.message.reply_text(f"❌ Пользователь с ID {target_id} не найден.")
                return
            target_name = target_data.get('full_name') or f"User {target_id}"
        except:
            await update.message.reply_text("❌ Неверный формат. Укажите @username или ID.")
            return

    # Парсим время
    time_string = ' '.join(context.args[1:])
    days, hours, minutes, seconds = parse_time_string(time_string)
    
    if days == 0 and hours == 0 and minutes == 0 and seconds == 0:
        await update.message.reply_text("❌ Неверный формат времени. Пример: `10 часов` или `1 день 5 часов`", parse_mode='Markdown')
        return

    # Вычисляем дату окончания VIP
    vip_until = datetime.now() + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    vip_until_str = vip_until.strftime('%Y-%m-%d %H:%M:%S')

    # Обновляем статус VIP в БД
    with get_db() as conn:
        conn.execute('''
            UPDATE users 
            SET vip_status = 1, vip_until = ? 
            WHERE user_id = ?
        ''', (vip_until_str, target_id))
        conn.commit()

    # Формируем сообщение о выдаче
    time_parts = []
    if days > 0:
        if days == 1:
            time_parts.append(f"{days} день")
        elif days in [2, 3, 4]:
            time_parts.append(f"{days} дня")
        else:
            time_parts.append(f"{days} дней")
    if hours > 0:
        if hours == 1:
            time_parts.append(f"{hours} час")
        elif hours in [2, 3, 4]:
            time_parts.append(f"{hours} часа")
        else:
            time_parts.append(f"{hours} часов")
    if minutes > 0:
        if minutes == 1:
            time_parts.append(f"{minutes} минуту")
        elif minutes in [2, 3, 4]:
            time_parts.append(f"{minutes} минуты")
        else:
            time_parts.append(f"{minutes} минут")
    if seconds > 0:
        if seconds == 1:
            time_parts.append(f"{seconds} секунду")
        elif seconds in [2, 3, 4]:
            time_parts.append(f"{seconds} секунды")
        else:
            time_parts.append(f"{seconds} секунд")
    
    time_text = ' '.join(time_parts)

    # Уведомление админу
    await update.message.reply_text(
        f"✅ VIP статус выдан пользователю {target_name}\n"
        f"⏰ На: {time_text}\n"
        f"📅 Действует до: {vip_until.strftime('%d.%m.%Y %H:%M:%S')}"
    )

    # Уведомление пользователю
    target_mention = f"<a href='tg://user?id={target_id}'>{target_name}</a>"
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"😎 Вам был выдан VIP статус на *{time_text}*.\n\n"
                f"📅 Действует до: {vip_until.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                f"🎁 Привилегии VIP:\n"
                f"• Комиссия при переводах: 1% (можно отключить)\n"
                f"• Отключение комиссии в настройках\n"
                f"• И другие бонусы!"
            ),
            parse_mode='HTML'
        )
    except Exception as e:
        print(f"Failed to notify user {target_id}: {e}")


def parse_time_string(time_string: str) -> tuple:
    """Парсит строку времени и возвращает (дни, часы, минуты, секунды)"""
    days = hours = minutes = seconds = 0
    
    # Паттерны для поиска
    patterns = {
        'days': r'(\d+)\s*(?:день|дня|дней|д|day|days)',
        'hours': r'(\d+)\s*(?:час|часа|часов|ч|hour|hours)',
        'minutes': r'(\d+)\s*(?:минуту|минуты|минут|мин|м|minute|minutes|min)',
        'seconds': r'(\d+)\s*(?:секунду|секунды|секунд|сек|с|second|seconds|sec)'
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, time_string, re.IGNORECASE)
        if match:
            value = int(match.group(1))
            if key == 'days':
                days = value
            elif key == 'hours':
                hours = value
            elif key == 'minutes':
                minutes = value
            elif key == 'seconds':
                seconds = value
    
    return days, hours, minutes, seconds

def parse_time_string(time_string: str) -> tuple:
    """Парсит строку времени и возвращает (дни, часы, минуты, секунды)"""
    days = hours = minutes = seconds = 0
    
    # Паттерны для поиска
    patterns = {
        'days': r'(\d+)\s*(?:день|дня|дней|д|day|days)',
        'hours': r'(\d+)\s*(?:час|часа|часов|ч|hour|hours)',
        'minutes': r'(\d+)\s*(?:минуту|минуты|минут|мин|м|minute|minutes|min)',
        'seconds': r'(\d+)\s*(?:секунду|секунды|секунд|сек|с|second|seconds|sec)'
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, time_string, re.IGNORECASE)
        if match:
            value = int(match.group(1))
            if key == 'days':
                days = value
            elif key == 'hours':
                hours = value
            elif key == 'minutes':
                minutes = value
            elif key == 'seconds':
                seconds = value
    
    return days, hours, minutes, seconds

async def promotion_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для продвижения"""
    if not update.effective_user:
        return

    user = update.effective_user
    user_id = user.id
    user_name = user.full_name

    # Если в группе/чате
    if update.effective_chat.type in ["group", "supergroup"]:
        keyboard = [[InlineKeyboardButton(
            "📱 Перейти в ЛС", 
            url=f"https://t.me/{context.bot.username}?start=promotion"
        )]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"⚙️ {user_name}, продвижение доступно только в ЛС!",
            reply_markup=reply_markup
        )
        return

    # Если в ЛС
    db_user = await get_user_async(user_id, user.full_name, user.username)
    msg_balance = db_user.get('msg_balance', 0)

    text = (
        f"⚙️ {user_name}, что ты хочешь рекламировать?\n\n"
        f"⚠️ Продвигая канал/группу или же чат вы автоматически принимаете правила продвижения!\n\n"
        f"🍯 Баланс: {format_amount(msg_balance)} MSG"
    )

    keyboard = [
        [InlineKeyboardButton("📢 Продвигать канал", callback_data="promo_channel")],
        [InlineKeyboardButton("💬 Продвигать чат/группу", callback_data="promo_chat")],
        [InlineKeyboardButton("🎯 Активные задания", callback_data="promo_my_tasks")],
        [InlineKeyboardButton("📕 Правила", callback_data="promo_rules")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(text, reply_markup=reply_markup)

async def promo_rules_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать правила продвижения"""
    query = update.callback_query
    user_name = query.from_user.full_name

    text = (
        f"👨‍⚖️ *{user_name}*, общие положения*\n\n"
        "1. Нажимая /promotion в боте @monstrminesbot, вы соглашаетесь с этими правилами и правилами Telegram.\n\n"
        "2. Незнание правил не освобождает от ответственности.\n\n"
        "3. Вы несёте личную ответственность за контент своих каналов и чатов.\n\n"
        "❓ *Что запрещено продвигать*\n"
        "Если ваш канал или чат содержит что-либо из списка ниже он не будет принят, и вы можете лишиться доступа к продвижению:\n\n"
        "1. Порнография и эротика\n"
        "2. Политика и войны\n"
        "3. Запрещённая торговля\n"
        "4. Насилие, травля, жестокость\n"
        "5. Дезинформация и фейки\n"
        "6. Хакерство и доступы\n"
        "7. Спам и навязчивая реклама\n"
        "8. Нарушение приватности\n\n"
        "✅ *Что разрешено продвигать?*\n"
        "Почти всё кроме указано выше\n\n"
        "⚖️ *Наказание за нарушение*\n"
        "Ваше задание будет удалено без возвращения MSG. Продвижение для вас может стать недоступным навсегда. При грубых нарушениях — бан аккаунта.\n\n"
        "⚠️ *Важно*\n"
        "• Не пытайтесь обходить правила через звёздочки (с*кс) цензуру, временные замены контента\n"
        "• Если ваш канал выглядит нормально, но позже контент меняется на запрещённый — это также считается нарушением\n"
        "• Жалобы обрабатываются в течении 24-72 часов после жалобы. Пожаловаться на канал/чат можно используя кнопку пожаловаться в выполнении задания"
    )

    keyboard = [[InlineKeyboardButton("◀ Назад", callback_data="promo_back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"Error in promo_rules_callback: {e}")
        # Если Markdown не работает, отправляем без форматирования
        await query.edit_message_text(text.replace('*', ''), reply_markup=reply_markup)

async def promo_price_input(update: Update, context: ContextTypes.DEFAULT_TYPE, promo_type: str):
    """Запрос цены за подписчика"""
    query = update.callback_query
    user_name = query.from_user.full_name
    user_id = query.from_user.id

    # Сохраняем тип продвижения
    context.user_data['promo_type'] = promo_type
    context.user_data['promo_step'] = 'price'

    text = (
        f"👤 {user_name}, напишите цену за 1 подписчика!\n\n"
        f"⚠️ Минимальная цена за 1 подписчика — 1 MSG!\n"
        f"<blockquote>🔝 Чем выше цена за подписчика – тем выше будет в списке твое задание!</blockquote>"
    )

    keyboard = [
        [
            InlineKeyboardButton("1 MSG", callback_data="promo_price_1"),
            InlineKeyboardButton("3 MSG", callback_data="promo_price_3"),
            InlineKeyboardButton("5 MSG", callback_data="promo_price_5")
        ],
        [InlineKeyboardButton("◀ Назад", callback_data="promo_back_to_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

async def promo_users_count(update: Update, context: ContextTypes.DEFAULT_TYPE, price: int):
    """Запрос количества подписчиков"""
    query = update.callback_query
    user_name = query.from_user.full_name
    user_id = query.from_user.id

    # Получаем баланс пользователя
    db_user = await get_user_async(user_id)
    msg_balance = db_user.get('msg_balance', 0)
    
    # Рассчитываем максимальное количество подписчиков
    max_users = msg_balance // price
    
    context.user_data['promo_price'] = price
    context.user_data['promo_step'] = 'users'
    context.user_data['promo_max_users'] = max_users

    text = (
        f"🔶 {user_name}, введите количество подписчиков:\n\n"
        f"🔘 Максимум: {max_users} чел."
    )

    keyboard = [
        [
            InlineKeyboardButton("1 чел.", callback_data="promo_users_1"),
            InlineKeyboardButton("5 чел.", callback_data="promo_users_5"),
            InlineKeyboardButton("10 чел.", callback_data="promo_users_10")
        ],
        [InlineKeyboardButton(f"Макс • {max_users} чел.", callback_data=f"promo_users_max")],
        [InlineKeyboardButton("◀ Назад", callback_data="promo_back_to_price")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup)


async def promo_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, users_count: int):
    """Подтверждение и запрос канала/чата"""
    query = update.callback_query
    user_name = query.from_user.full_name
    user_id = query.from_user.id

    price = context.user_data.get('promo_price')
    promo_type = context.user_data.get('promo_type')
    
    total_cost = price * users_count
    
    context.user_data['promo_users'] = users_count
    context.user_data['promo_total'] = total_cost
    context.user_data['promo_step'] = 'waiting_chat'

    text = (
        f"❗ *Важно:*\n"
        f"Канал должен быть публичным и без заявок на вступление\n"
        f"Канал не должен содержать список нарушений указанных в правилах\n"
        f"Соблюдайте правила! Мы не несём ответственность за ваш канал! В ином случае вы будете забанены навсегда!\n\n"
        f"📝 *Инструкция по добавлению:*\n\n"
        f"⬇️ Нажмите кнопку ниже и выберите канал/чат"
    )

    await query.edit_message_text(text, parse_mode='Markdown')
    
    # Создаем кнопку для выбора канала с правами администратора
    from telegram import KeyboardButton, ReplyKeyboardMarkup, KeyboardButtonRequestChat, ChatAdministratorRights
    
    # Права для бота (минимальные - только постить)
    bot_rights = ChatAdministratorRights(
        is_anonymous=False,
        can_post_messages=False,  # Может постить
        can_edit_messages=False,
        can_delete_messages=False,
        can_invite_users=True,
        can_restrict_members=False,
        can_pin_messages=False,
        can_promote_members=True,
        can_change_info=False,
        can_manage_chat=False,
        can_manage_video_chats=False,
        can_post_stories=False,
        can_edit_stories=False,
        can_delete_stories=False
    )
    
    # Права для пользователя (максимальные - все)
    user_rights = ChatAdministratorRights(
        is_anonymous=False,
        can_change_info=True,           # изменять информацию о канале
        can_post_messages=True,          # публиковать сообщения
        can_edit_messages=True,          # редактировать сообщения
        can_delete_messages=True,        # удалять сообщения
        can_invite_users=True,           # приглашать пользователей по ссылке
        can_restrict_members=True,       # блокировать пользователей
        can_pin_messages=True,           # закреплять сообщения
        can_promote_members=True,        # добавлять администраторов
        can_manage_chat=True,             # управлять чатом
        can_manage_video_chats=True,      # управлять трансляциями
        can_post_stories=True,            # публиковать истории
        can_edit_stories=True,            # редактировать истории
        can_delete_stories=True           # удалять истории
    )
    
    if promo_type == 'channel':
        button_text = "📢 Выбрать канал"
        request = KeyboardButtonRequestChat(
            request_id=1,
            chat_is_channel=True,
            bot_administrator_rights=bot_rights,
            user_administrator_rights=user_rights
        )
    else:
        button_text = "💬 Выбрать чат/группу"
        request = KeyboardButtonRequestChat(
            request_id=1,
            chat_is_channel=False,
            bot_administrator_rights=bot_rights,
            user_administrator_rights=user_rights
        )
    
    button = KeyboardButton(
        text=button_text,
        request_chat=request
    )
    
    # Кнопка отмены
    cancel_button = KeyboardButton("❌ Отменить")
    
    reply_markup = ReplyKeyboardMarkup(
        [[button], [cancel_button]], 
        one_time_keyboard=True, 
        resize_keyboard=True
    )
    
    await context.bot.send_message(
        chat_id=user_id,
        text="Выберите канал/чат для продвижения:",
        reply_markup=reply_markup
    )

async def promo_handle_chat_shared(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка полученного чата/канала через кнопку выбора"""
    print("🔍 promo_handle_chat_shared вызвана!")
    logging.info(f"chat_shared получен: {update.message.chat_shared}")
    
    # Проверяем, есть ли активный шаг продвижения
    if 'promo_step' not in context.user_data:
        print("❌ promo_step не найден в context.user_data")
        print(f"context.user_data: {context.user_data}")
        return
    
    if context.user_data['promo_step'] != 'waiting_chat':
        print(f"❌ promo_step = {context.user_data['promo_step']}, ожидалось 'waiting_chat'")
        return

    user = update.effective_user
    if not user:
        print("❌ Нет effective_user")
        return
        
    user_id = user.id
    user_name = user.full_name
    
    print(f"👤 Пользователь: {user_name} ({user_id})")
    
    # Получаем информацию о выбранном чате
    chat_shared = update.message.chat_shared
    if not chat_shared:
        print("❌ chat_shared отсутствует в сообщении")
        return
    
    print(f"📢 chat_shared данные: request_id={chat_shared.request_id}, chat_id={chat_shared.chat_id}")
    
    if chat_shared.request_id != 1:
        print(f"❌ request_id = {chat_shared.request_id}, ожидалось 1")
        return
    
    chat_id = chat_shared.chat_id
    chat_title = getattr(chat_shared, 'title', 'Unknown')
    
    print(f"✅ Выбран чат: {chat_title} (ID: {chat_id})")
    
    # Получаем данные из контекста
    promo_type = context.user_data.get('promo_type')
    price = context.user_data.get('promo_price')
    users_count = context.user_data.get('promo_users')
    total_cost = context.user_data.get('promo_total')
    
    print(f"📊 Данные продвижения: type={promo_type}, price={price}, users={users_count}, total={total_cost}")
    
    # Проверяем наличие всех данных
    if not all([promo_type, price, users_count, total_cost]):
        print("❌ Не все данные продвижения найдены в context.user_data")
        await update.message.reply_text("❌ Ошибка: данные продвижения не найдены. Начните заново.")
        context.user_data.clear()
        return
    
    # Проверяем баланс еще раз
    from database import get_user_async
    db_user = await get_user_async(user_id)
    msg_balance = db_user.get('msg_balance', 0)
    
    print(f"💰 Баланс пользователя: {msg_balance}, нужно: {total_cost}")
    
    if msg_balance < total_cost:
        from telegram import ReplyKeyboardRemove
        await update.message.reply_text(
            "❌ Недостаточно средств на балансе.",
            reply_markup=ReplyKeyboardRemove()
        )
        print("❌ Недостаточно средств")
        context.user_data.clear()
        return
    
    # СОЗДАЕМ ПРИГЛАСИТЕЛЬНУЮ ССЫЛКУ
    from telegram import ReplyKeyboardRemove
    try:
        if promo_type == 'channel':
            # Для канала
            invite_link = await context.bot.create_chat_invite_link(
                chat_id=chat_id,
                member_limit=users_count  # Максимум подписчиков
            )
            link = invite_link.invite_link
            print(f"🔗 Создана пригласительная ссылка для канала: {link}")
        else:
            # Для чата/группы
            invite_link = await context.bot.create_chat_invite_link(
                chat_id=chat_id,
                member_limit=users_count,
                creates_join_request=False  # Сразу вступают без заявки
            )
            link = invite_link.invite_link
            print(f"🔗 Создана пригласительная ссылка для чата: {link}")
        
    except Exception as e:
        print(f"❌ Ошибка создания пригласительной ссылки: {e}")
        await update.message.reply_text(
            "❌ Не удалось создать пригласительную ссылку. Убедитесь, что бот имеет права.",
            reply_markup=ReplyKeyboardRemove()
        )
        context.user_data.clear()
        return
    
    # Создаем задание в БД
    from database import create_promotion_task
    task_id = await create_promotion_task(
        creator_id=user_id,
        task_type='channel' if promo_type == 'channel' else 'chat',
        link=link,  # Сохраняем пригласительную ссылку
        price_per_user=price,
        max_users=users_count,
        chat_id=chat_id
    )
    
    print(f"📝 Результат создания задания: task_id={task_id}")
    
    if task_id:
        # Выбираем правильное сообщение в зависимости от типа
        if promo_type == 'channel':
            success_text = f"✅ {user_name}, твой канал успешно добавлен!\n🆔 ID задания: {task_id}"
        else:
            success_text = f"✅ {user_name}, твой чат/группа успешно добавлен!\n🆔 ID задания: {task_id}"
        
        await update.message.reply_text(
            success_text,
            reply_markup=ReplyKeyboardRemove()
        )
        
        print("✅ Сообщение об успехе отправлено")
        context.user_data.clear()
    else:
        await update.message.reply_text(
            "❌ Ошибка при создании задания.",
            reply_markup=ReplyKeyboardRemove()
        )
        print("❌ Ошибка создания задания")
        context.user_data.clear()

async def promo_tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Показать активные задания"""
    # Определяем, откуда вызвана функция
    if update.callback_query:
        # Вызвано из callback
        query = update.callback_query
        user_name = query.from_user.full_name
        chat_id = query.message.chat.id
        message = query.message
    else:
        # Вызвано из текстовой команды
        user_name = update.effective_user.full_name
        chat_id = update.effective_chat.id
        message = update.message

    tasks = await get_active_tasks(page)
    total_pages = await get_total_pages()

    if not tasks:
        text = f"📝 {user_name}, активных заданий пока нет!"
        keyboard = [[InlineKeyboardButton("◀ Назад", callback_data="promo_back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await message.reply_text(text, reply_markup=reply_markup)
        return

    text = f"📝 {user_name}, активные задания:\n\n"
    
    for task in tasks:
        task_id, task_type, link, price, max_users, current = task
        action = "Вступить в группу" if task_type == 'chat' else "Подписаться на канал"
        text += f"🆔 {task_id} — {action}\n"

    # Кнопки с ID заданий
    keyboard = []
    row = []
    for i, task in enumerate(tasks[:3]):
        row.append(InlineKeyboardButton(f"🆔 {task[0]}", callback_data=f"promo_task_{task[0]}"))
    keyboard.append(row)
    
    row = []
    for i, task in enumerate(tasks[3:5]):
        row.append(InlineKeyboardButton(f"🆔 {task[0]}", callback_data=f"promo_task_{task[0]}"))
    keyboard.append(row)

    # Кнопки навигации
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"promo_tasks_page_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"promo_tasks_page_{page+1}"))
    keyboard.append(nav_row)

    keyboard.append([InlineKeyboardButton("◀ В меню", callback_data="promo_back_to_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await message.reply_text(text, reply_markup=reply_markup)

async def promo_task_view(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    """Показать конкретное задание"""
    query = update.callback_query
    
    def _get_task():
        with get_db() as conn:
            return conn.execute('''
                SELECT task_type, link, price_per_user FROM promotion_tasks 
                WHERE task_id = ? AND status = 'active'
            ''', (task_id,)).fetchone()
    
    task = await asyncio.to_thread(_get_task)
    
    if not task:
        await safe_answer(query, "❌ Задание не найдено или уже завершено", show_alert=True)
        return

    task_type, link, price = task
    action = "вступить в группу" if task_type == 'chat' else "подписаться на канал"

    text = (
        f"🎯 Задание №{task_id} — {action}\n\n"
        f"⬇️ Выберите действие:"
    )

    keyboard = [
        [
            InlineKeyboardButton("Подписаться", url=link),
            InlineKeyboardButton("🔄 Проверить", callback_data=f"promo_check_{task_id}")
        ],
        [InlineKeyboardButton("⚠️ Пожаловаться", callback_data=f"promo_report_{task_id}")],
        [InlineKeyboardButton("◀ Назад", callback_data="promo_tasks")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup)

async def promo_check_task(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    """Проверить выполнение задания"""
    query = update.callback_query
    user_id = query.from_user.id
    user_name = query.from_user.full_name
    
    await safe_answer(query, "⏳ Проверяю подписку...")
    
    # Получаем информацию о задании
    def _get_task():
        from database import get_db
        with get_db() as conn:
            return conn.execute('''
                SELECT task_type, link, price_per_user, creator_id 
                FROM promotion_tasks WHERE task_id = ? AND status = 'active'
            ''', (task_id,)).fetchone()
    
    task = await asyncio.to_thread(_get_task)
    
    if not task:
        await query.edit_message_text("❌ Задание не найдено или уже завершено")
        return
    
    task_type, link, price, creator_id = task
    
    # Извлекаем chat_id из пригласительной ссылки
    # Пригласительная ссылка вида: https://t.me/+abc123 или https://t.me/joinchat/abc123
    import re
    import logging
    
    # Пытаемся извлечь идентификатор из ссылки
    if '/joinchat/' in link:
        invite_hash = link.split('/joinchat/')[-1]
    elif '/+' in link:
        invite_hash = link.split('/+')[-1]
    else:
        # Если не удалось распарсить, пробуем использовать всю ссылку
        invite_hash = link.split('/')[-1]
    
    print(f"🔍 Проверка подписки для ссылки: {link}, хеш: {invite_hash}")
    
    try:
        # Пытаемся получить chat_id по пригласительной ссылке
        # Для этого нужно сначала получить информацию о чате
        chat = None
        
        # Пробуем разные способы получить chat_id
        if invite_hash.startswith('+'):
            # Это пригласительная ссылка вида t.me/+abc123
            try:
                # Пытаемся получить информацию о чате
                chat_info = await context.bot.get_chat(f"@{invite_hash[1:]}")
                chat_id = chat_info.id
            except:
                # Если не получается, пробуем другой способ
                chat_id = invite_hash
        else:
            chat_id = invite_hash
        
        # Проверяем, подписан ли пользователь
        try:
            chat_member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            is_member = chat_member.status in ['member', 'administrator', 'creator']
            print(f"📊 Статус подписки: {chat_member.status}, is_member={is_member}")
        except Exception as e:
            print(f"❌ Ошибка получения chat_member: {e}")
            # Если не удалось получить информацию, возможно это не ID а хеш
            # Пробуем другой способ - используем invite_hash как username
            try:
                chat_member = await context.bot.get_chat_member(chat_id=f"@{invite_hash}", user_id=user_id)
                is_member = chat_member.status in ['member', 'administrator', 'creator']
                print(f"📊 Статус подписки (через @): {chat_member.status}, is_member={is_member}")
            except:
                # Если все способы не сработали, считаем что пользователь не подписан
                is_member = False
                print("❌ Не удалось проверить подписку ни одним способом")
        
        if is_member:
            # Пользователь подписан, начисляем награду
            from database import check_task_completion
            success, reward = await check_task_completion(task_id, user_id)
            
            if success:
                await query.edit_message_text(
                    f"✅ {user_name}, ваша подписка найдена! Вам начислено {reward} MSG."
                )
                
                # Проверяем, не выполнено ли задание полностью
                def _check_completion():
                    from database import get_db
                    with get_db() as conn:
                        task_data = conn.execute('''
                            SELECT current_users, max_users FROM promotion_tasks 
                            WHERE task_id = ?
                        ''', (task_id,)).fetchone()
                        return task_data if task_data and task_data[0] >= task_data[1] else None
                
                completed_task = await asyncio.to_thread(_check_completion)
                if completed_task:
                    # Уведомляем создателя
                    try:
                        await context.bot.send_message(
                            chat_id=creator_id,
                            text=f"✅ Ваше задание №{task_id} выполнено!"
                        )
                    except Exception as e:
                        print(f"❌ Ошибка уведомления создателя: {e}")
            else:
                await query.edit_message_text("❌ Ошибка при начислении награды")
        else:
            await query.edit_message_text(
                f"❌ {user_name}, вы не подписаны. Подпишитесь и нажмите кнопку еще раз."
            )
            
    except Exception as e:
        print(f"❌ Ошибка проверки подписки: {e}")
        logging.error(f"Error checking subscription: {e}")
        await query.edit_message_text(
            "❌ Не удалось проверить подписку. Возможно, бот не является администратором."
        )

async def work_report_task(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    """Пожаловаться на задание из меню /work"""
    query = update.callback_query

    text = "Выберите причину:"
    keyboard = [
        [InlineKeyboardButton("🔞 Онанизм", callback_data=f"work_report_reason_{task_id}_porn")],
        [InlineKeyboardButton("💀 Расчленение", callback_data=f"work_report_reason_{task_id}_violence")],
        [InlineKeyboardButton("📝 Другое", callback_data=f"work_report_reason_{task_id}_other")],
        [InlineKeyboardButton("◀ Назад", callback_data=f"work_task_{task_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup)

async def work_report_submit(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int, reason: str):
    """Отправить жалобу из меню /work"""
    query = update.callback_query
    user_id = query.from_user.id
    user_name = query.from_user.full_name

    await report_task(task_id, user_id, reason)
    await query.edit_message_text("✅ Жалоба отправлена!")

    # Уведомляем админов
    def _get_task_creator():
        with get_db() as conn:
            return conn.execute('''
                SELECT creator_id, link FROM promotion_tasks WHERE task_id = ?
            ''', (task_id,)).fetchone()
    
    task_info = await asyncio.to_thread(_get_task_creator)
    if task_info:
        creator_id, link = task_info
        creator = await get_user_async(creator_id)
        creator_username = creator.get('username') or f"ID: {creator_id}"
        
        admin_text = (
            f"✔ Жалоба на №{task_id}\n"
            f"Создатель: @{creator_username}\n"
            f"Канал/чат: {link}\n"
            f"Пожаловался: {user_name} (ID: {user_id})\n"
            f"Причина: {reason}"
        )
        
        admin_keyboard = [[
            InlineKeyboardButton("Удалить задание", callback_data=f"promo_admin_delete_{task_id}"),
            InlineKeyboardButton("Оставить задание", callback_data=f"promo_admin_keep_{task_id}")
        ]]
        admin_reply_markup = InlineKeyboardMarkup(admin_keyboard)
        
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=admin_text,
                    reply_markup=admin_reply_markup
                )
            except:
                pass

# 2.1 Команда /work (заработать)
async def work_command(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Команда для заработка (доступна только в ЛС)"""
    if not update.effective_user:
        return

    # Проверяем, в чате ли вызвана команда
    if update.effective_chat.type in ["group", "supergroup"]:
        user_name = update.effective_user.full_name
        keyboard = [[InlineKeyboardButton(
            "📱 Перейти в ЛС", 
            url=f"https://t.me/{context.bot.username}?start=work"
        )]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"⚙️ {user_name}, заработок доступен только в ЛС!",
            reply_markup=reply_markup
        )
        return

    # Если в ЛС - показываем задания
    if update.callback_query:
        query = update.callback_query
        user = query.from_user
        message = query.message
    else:
        user = update.effective_user
        message = update.message

    user_id = user.id
    user_name = user.full_name

    # Проверка подписки (если нужно)
    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    tasks = await get_available_tasks(user_id, page)
    total_pages = await get_available_total_pages(user_id)

    if not tasks:
        text = f"📝 {user_name}, сейчас нет доступных заданий!"
        keyboard = [[InlineKeyboardButton("🔄 Обновить", callback_data="work_refresh")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await message.reply_text(text, reply_markup=reply_markup)
        return

    text = f"📝 {user_name}, доступные задания:\n\n"
    
    for task in tasks:
        task_id, task_type, link, price, max_users, current = task
        action = "Вступить в группу" if task_type == 'chat' else "Подписаться на канал"
        text += f"🆔 {task_id} — {action} | +{price} MSG\n"

    # Кнопки с ID заданий
    keyboard = []
    row = []
    for i, task in enumerate(tasks[:3]):
        row.append(InlineKeyboardButton(f"🆔 {task[0]}", callback_data=f"work_task_{task[0]}"))
    if row:
        keyboard.append(row)
    
    row = []
    for i, task in enumerate(tasks[3:5]):
        row.append(InlineKeyboardButton(f"🆔 {task[0]}", callback_data=f"work_task_{task[0]}"))
    if row:
        keyboard.append(row)

    # Кнопки навигации
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"work_page_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"work_page_{page+1}"))
    keyboard.append(nav_row)

    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await message.reply_text(text, reply_markup=reply_markup)

# 2.2 Просмотр задания для выполнения
async def work_task_view(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    """Показать конкретное задание для выполнения"""
    query = update.callback_query
    user_id = query.from_user.id
    
    def _get_task():
        from database import get_db
        with get_db() as conn:
            return conn.execute('''
                SELECT task_type, link, price_per_user, creator_id 
                FROM promotion_tasks 
                WHERE task_id = ? AND status = 'active'
            ''', (task_id,)).fetchone()
    
    task = await asyncio.to_thread(_get_task)
    
    if not task:
        await safe_answer(query, "❌ Задание не найдено или уже завершено", show_alert=True)
        return

    task_type, link, price, creator_id = task
    
    # Проверяем, не свое ли это задание
    if creator_id == user_id:
        await safe_answer(query, "❌ Нельзя выполнять свое задание!", show_alert=True)
        return
    
    # Проверяем, не выполнял ли уже
    def _check_completed():
        with get_db() as conn:
            return conn.execute('''
                SELECT id FROM completed_tasks WHERE task_id = ? AND user_id = ?
            ''', (task_id, user_id)).fetchone()
    
    completed = await asyncio.to_thread(_check_completed)
    if completed:
        await safe_answer(query, "❌ Вы уже выполнили это задание", show_alert=True)
        return

    action = "вступить в группу" if task_type == 'chat' else "подписаться на канал"

    text = (
        f"🎯 Задание №{task_id} — {action}\n"
        f"💰 Награда: {price} MSG\n\n"
        f"⬇️ Выберите действие:"
    )

    keyboard = [
        [
            InlineKeyboardButton("🔗 Перейти", url=link),
            InlineKeyboardButton("🔄 Проверить", callback_data=f"work_check_{task_id}")
        ],
        [InlineKeyboardButton("⚠️ Пожаловаться", callback_data=f"work_report_{task_id}")],
        [InlineKeyboardButton("◀ Назад", callback_data="work_refresh")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup)

# 2.3 Проверка выполнения задания
async def work_check_task(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    """Проверить выполнение задания"""
    query = update.callback_query
    user_id = query.from_user.id
    user_name = query.from_user.full_name
    
    await safe_answer(query, "⏳ Проверяю...")
    
    # Получаем информацию о задании
    def _get_task():
        from database import get_db
        with get_db() as conn:
            return conn.execute('''
                SELECT task_type, chat_id, price_per_user, creator_id, max_users, current_users
                FROM promotion_tasks WHERE task_id = ? AND status = 'active'
            ''', (task_id,)).fetchone()
    
    task = await asyncio.to_thread(_get_task)
    
    if not task:
        await query.edit_message_text("❌ Задание не найдено или уже завершено")
        return
    
    task_type, chat_id, price, creator_id, max_users, current_users = task
    
    # Проверяем, не свое ли задание
    if creator_id == user_id:
        await query.edit_message_text("❌ Нельзя выполнять свое задание!")
        return
    
    if not chat_id:
        await query.edit_message_text("❌ Ошибка: ID чата не найден")
        return
    
    try:
        # Проверяем подписку
        chat_member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        is_member = chat_member.status in ['member', 'administrator', 'creator']
        
        if is_member:
            # Пользователь подписан, начисляем награду
            from database import check_task_completion
            success, reward = await check_task_completion(task_id, user_id)
            
            if success:
                await query.edit_message_text(
                    f"✅ {user_name}, вы выполнили задание! Получено {reward} MSG."
                )
                
                # Проверяем, не выполнено ли задание полностью
                def _check_completion():
                    with get_db() as conn:
                        task_data = conn.execute('''
                            SELECT current_users, max_users, creator_id 
                            FROM promotion_tasks 
                            WHERE task_id = ?
                        ''', (task_id,)).fetchone()
                        return task_data if task_data else None
                
                completed_task = await asyncio.to_thread(_check_completion)
                if completed_task and completed_task[0] >= completed_task[1]:
                    # Задание выполнено полностью
                    creator_id = completed_task[2]
                    
                    # Получаем имя владельца
                    creator = await get_user_async(creator_id)
                    creator_name = creator.get('full_name') or f"ID: {creator_id}"
                    
                    # Уведомляем владельца
                    try:
                        await context.bot.send_message(
                            chat_id=creator_id,
                            text=f"✅ {creator_name}, задание №{task_id} выполнено!"
                        )
                        print(f"📨 Уведомление отправлено создателю задания {task_id}")
                    except Exception as e:
                        print(f"❌ Ошибка уведомления создателя: {e}")
            else:
                await query.edit_message_text("❌ Ошибка при начислении награды")
        else:
            await query.edit_message_text(
                f"❌ {user_name}, вы не подписались. Подпишитесь и нажмите кнопку еще раз."
            )
            
    except Exception as e:
        print(f"❌ Ошибка проверки подписки: {e}")
        logging.error(f"Error checking subscription: {e}")
        await query.edit_message_text(
            "❌ Не удалось проверить подписку. Возможно, бот не является администратором."
        )

# 2.4 Мои задания (созданные пользователем)
async def my_tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Показать задания, созданные пользователем"""
    query = update.callback_query
    user_id = query.from_user.id
    user_name = query.from_user.full_name

    tasks = await get_my_tasks(user_id, page)
    total_pages = await get_my_tasks_total_pages(user_id)

    if not tasks:
        text = f"📝 {user_name}, у вас пока нет созданных заданий!"
        keyboard = [[InlineKeyboardButton("◀ Назад", callback_data="promo_back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup)
        return

    text = f"📝 {user_name}, ваши активные задания:\n\n"
    
    for task in tasks:
        task_id, task_type, link, price, max_users, current, status = task
        action = "Группа" if task_type == 'chat' else "Канал"
        status_emoji = "✅" if status == 'completed' else "🔄" if current >= max_users else "⏳"
        text += f"{status_emoji} 🆔 {task_id} — {action} | {current}/{max_users} | {price} MSG\n"

    keyboard = []
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"my_tasks_page_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"my_tasks_page_{page+1}"))
    keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("◀ В меню", callback_data="promo_back_to_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup)

async def mines_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    db_user = await get_user_async(user_id, user.full_name, user.username)

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    if user_id in MINES_SESSIONS:
        session = MINES_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del MINES_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Мины. Завершите её сначала.")
            return

    if user_id in GOLD_SESSIONS:
        session = GOLD_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del GOLD_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Золото. Завершите её сначала.")
            return

    if user_id in PYRAMID_SESSIONS:
        session = PYRAMID_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del PYRAMID_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Пирамида. Завершите её сначала.")
            return

    text = update.message.text.strip()

    mines_count = 1
    bet_amount = 0

    if text.startswith('/mines'):
        args = context.args
        if len(args) == 1:
            bet_amount = parse_bet_amount(args[0], db_user['balance'])
        elif len(args) >= 2:
            bet_amount = parse_bet_amount(args[0], db_user['balance'])
            try:
                mines_count = int(args[1])
                if mines_count < 1 or mines_count > 24:
                    mines_count = 1
            except:
                mines_count = 1
    elif text.lower().startswith('мины '):
        parts = text.lower().split()
        if len(parts) == 2:
            bet_amount = parse_bet_amount(parts[1], db_user['balance'])
        elif len(parts) >= 3:
            bet_amount = parse_bet_amount(parts[1], db_user['balance'])
            try:
                mines_count = int(parts[2])
                if mines_count < 1 or mines_count > 24:
                    mines_count = 1
            except:
                mines_count = 1

    if bet_amount <= 0:
        await update.message.reply_text(
            "<blockquote>ℹ️ Мины - это игра, в которой вам нужно угадывать пустые ячейки, чем больше откроете, тем больше будет множитель!</blockquote>\n\n"
            f"🤖 {user.full_name}, чтобы начать игру используй команду:\n\n"
            "💣 /mines [*ставка] [мины (1-24)]\n\n"
            "Примеры:\n"
            "- /mines 100 6\n"
            "- Мины 100",
            parse_mode='HTML'
        )
        return

    if mines_count > 24:
        mines_count = 24

    if db_user['balance'] < bet_amount:
        await update.message.reply_text("Недостаточно средств на балансе.")
        return

    success = await update_balance_safe_async(user_id, -bet_amount, bet_amount)
    if not success:
        await update.message.reply_text("Ошибка при списании средств.")
        return

    game_board = [['❓' for _ in range(FIELD_SIZE)] for _ in range(FIELD_SIZE)]

    mine_positions = []
    cells = [(r, c) for r in range(FIELD_SIZE) for c in range(FIELD_SIZE)]
    mine_positions = random.sample(cells, mines_count)

    game_hash = generate_game_hash({
        'user_id': user_id,
        'game': 'mines',
        'bet': bet_amount,
        'mines': mines_count,
        'positions': [[r, c] for r, c in mine_positions]
    })

    MINES_SESSIONS[user_id] = {
        'board': game_board,
        'mines': mine_positions,
        'mines_count': mines_count,
        'bet': bet_amount,
        'opened': 0,
        'message_id': None,
        'chat_id': update.effective_chat.id,
        'message_thread_id': update.effective_message.message_thread_id,
        'status': 'active',
        'start_multiplier': MINES_MULTIPLIERS[mines_count][0],
        'hash': game_hash,
        'start_time': time.time()
    }

    await send_mines_board(update, context, user_id)


async def send_mines_board(update, context, user_id):
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
        f"📈 Следующий множитель: x{next_multiplier:.2f}."
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

async def mines_cell_click(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, r, c):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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
            f"💥 Мины — проигрыш!\n"
            f"••••••••••\n"
            f"💣 Мин: {mines_count}\n"
            f"💸 Ставка: {format_amount(session['bet'])}ms¢\n\n"
            f"💎 Открыто: {opened} из {CELLS_TOTAL - mines_count}\n\n"
            f"✔️ Ты мог забрать {int(session['bet'] * current_multiplier)}ms¢, но ничего страшного, повезет в следующий раз.\n\n"
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
                reply_markup=reply_markup
            )
        except:
            pass

        await safe_answer(query, "💥 Бомба!")
        return

    session['board'][r][c] = '💎'
    session['opened'] += 1
    opened = session['opened']
    mines_count = session['mines_count']

    max_opened = len(MINES_MULTIPLIERS[mines_count]) - 1
    if opened > max_opened:
        opened = max_opened

    await send_mines_board(update, context, user_id)
    await safe_answer(query, f"💎 +{MINES_MULTIPLIERS[mines_count][opened]:.2f}x")

async def mines_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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
        f"🎉 Мины — Победа! ✅\n"
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
            reply_markup=reply_markup
        )
    except:
        pass

    await safe_answer(query, f"Выигрыш {win_amount}ms¢")

async def mines_cancel_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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
    await safe_answer(query, "Игра отменена, средства возвращены")

async def gold_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    if user_id in GOLD_SESSIONS:
        session = GOLD_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del GOLD_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Золото. Завершите её сначала.")
            return

    if user_id in MINES_SESSIONS:
        session = MINES_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del MINES_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Мины. Завершите её сначала.")
            return

    if user_id in PYRAMID_SESSIONS:
        session = PYRAMID_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del PYRAMID_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Пирамида. Завершите её сначала.")
            return

    text = update.message.text.strip()

    bet_amount = 0

    if text.startswith('/gold'):
        if len(context.args) >= 1:
            bet_amount = parse_amount(context.args[0])
    elif text.lower().startswith('золото '):
        parts = text.lower().split()
        if len(parts) >= 2:
            bet_amount = parse_amount(parts[1])

    if bet_amount <= 0:
        await update.message.reply_text(
            "<blockquote>ℹ️ Золото — это игра, в которой необходимо угадать, где спрятано золото. Вам нужно открыть по одной ячейке на каждом уровне.</blockquote>\n\n"
            f"🤖 {user.full_name}, чтобы начать игру, используй команду:\n\n"
            "💰 /gold [ставка]\n\n"
            "Примеры:\n"
            "— /gold 100\n"
            "— золото 100",
            parse_mode='HTML'
        )
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)

    success = await update_balance_safe_async(user_id, -bet_amount, bet_amount)
    if not success:
        await update.message.reply_text("❌ Недостаточно средств на балансе.")
        return

    mine_positions = []
    for level in range(12):
        mine_positions.append(random.choice(['left', 'right']))

    board = [['❓', '❓'] for _ in range(12)]

    game_hash = generate_game_hash({
        'user_id': user_id,
        'game': 'gold',
        'bet': bet_amount,
        'mines': mine_positions
    })

    GOLD_SESSIONS[user_id] = {
        'board': board,
        'mines': mine_positions,
        'bet': bet_amount,
        'opened': 0,
        'message_id': None,
        'chat_id': update.effective_chat.id,
        'message_thread_id': update.effective_message.message_thread_id,
        'status': 'active',
        'hash': game_hash,
        'start_time': time.time()
    }

    await send_gold_board(update, context, user_id)

async def send_gold_board(update, context, user_id):
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

async def gold_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, side):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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

        await safe_answer(query, "💥 Бомба!")

async def gold_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, side):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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

        await safe_answer(query, "💥 Бомба!")

async def gold_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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

    await safe_answer(query, f"✅ Выигрыш {format_amount(win_amount)}ms¢")

async def tower_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в Башню"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    user_full_name = user.full_name

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    if user_id in TOWER_SESSIONS:
        session = TOWER_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del TOWER_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Башню. Завершите её сначала.")
            return

    text = update.message.text.strip()
    args = []

    if text.startswith('/tower'):
        args = context.args if context.args else []
    elif text.lower().startswith('башня '):
        parts = text.lower().split()
        if len(parts) > 1:
            args = parts[1:]

    if len(args) == 0:
        multipliers_text = []
        for i in range(8):
            multipliers_text.append(f"{i+1} • x{TOWER_MULTIPLIERS[1][i]:.2f}")
        
        await update.message.reply_text(
            f"<blockquote>ℹ️ Башня – это игра, в которой нужно избежать мин и добраться до вершины.</blockquote>\n\n"
            f"🤖 <b>{user_full_name}</b>, чтобы начать игру, используй команду:\n\n"
            f"🗼 <code>/tower [ставка] [мины 1-4]</code>\n\n"
            f"<b>Пример:</b>\n"
            f"• <code>/tower 100</code>\n"
            f"• <code>башня 100 3</code>",
            parse_mode='HTML'
        )
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)
    
    if len(args) == 1:
        bet_amount = parse_bet_amount(args[0], db_user['balance'])
        mines_count = 1
    else:
        bet_amount = parse_bet_amount(args[0], db_user['balance'])
        try:
            mines_count = int(args[1])
            if mines_count < 1 or mines_count > 4:
                mines_count = 1
        except:
            mines_count = 1

    if bet_amount <= 0:
        await update.message.reply_text("❌ Неверная сумма ставки.")
        return

    if db_user['balance'] < bet_amount:
        await update.message.reply_text("❌ Недостаточно средств на балансе.")
        return

    success = await update_balance_safe_async(user_id, -bet_amount, bet_amount)
    if not success:
        await update.message.reply_text("❌ Ошибка при списании средств.")
        return

    mine_positions = []
    for level in range(TOWER_MAX_LEVEL):
        positions = random.sample(range(TOWER_FIELD_SIZE), mines_count)
        mine_positions.append(positions)

    game_board = [['ㅤ' for _ in range(TOWER_FIELD_SIZE)] for _ in range(TOWER_MAX_LEVEL)]

    game_hash = generate_game_hash({
        'user_id': user_id,
        'game': 'tower',
        'bet': bet_amount,
        'mines': mines_count
    })

    TOWER_SESSIONS[user_id] = {
        'board': game_board,
        'mines': mine_positions,
        'mines_count': mines_count,
        'bet': bet_amount,
        'current_level': 0,
        'message_id': None,
        'chat_id': update.effective_chat.id,
        'message_thread_id': update.effective_message.message_thread_id,
        'status': 'active',
        'hash': game_hash,
        'start_time': time.time(),
        'last_activity': time.time()
    }

    try:
        msg = await send_tower_start(update, context, user_id)
        if msg:
            TOWER_SESSIONS[user_id]['message_id'] = msg.message_id
        else:
            del TOWER_SESSIONS[user_id]
            await update_balance_async(user_id, bet_amount)
            await update.message.reply_text("❌ Ошибка при создании игры. Попробуйте позже.")
    except Exception as e:
        logging.error(f"Error creating tower game: {e}")
        if user_id in TOWER_SESSIONS:
            del TOWER_SESSIONS[user_id]
        await update_balance_async(user_id, bet_amount)
        await update.message.reply_text("❌ Ошибка при создании игры. Попробуйте позже.")


async def send_tower_start(update, context, user_id):
    """Отправка начального сообщения Башни"""
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
    """Обновление доски Башни после хода"""
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

async def tower_cell_click(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, level, col):
    """Обработка нажатия на ячейку"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    session = TOWER_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if not session.get('message_id'):
        await update_balance_async(user_id, session['bet'])
        del TOWER_SESSIONS[user_id]
        await safe_answer(query, "⚠️ Игра не была создана. Средства возвращены.")
        return

    if session['status'] != 'active':
        await safe_answer(query, "🙈 Игра уже завершена")
        return

    if level != session['current_level']:
        await safe_answer(query, "🧐 Не тот уровень!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек.")
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

        await safe_answer(query, f"🎉 Победа! +{format_amount(win_amount)}ms¢")
        return

    session['current_level'] += 1
    await update_tower_board(update, context, user_id)
    await safe_answer(query, f"Мины не оказалось.")

async def tower_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Забрать выигрыш"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    session = TOWER_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if not session.get('message_id'):
        await update_balance_async(user_id, session['bet'])
        del TOWER_SESSIONS[user_id]
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

    await safe_answer(query, f"Выигрыш {format_amount(win_amount)}ms¢")

async def tower_cancel_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    """Отмена игры"""
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    session = TOWER_SESSIONS.get(user_id)
    if not session:
        await safe_answer(query, "⚠️ Игровая сессия не найдена.")
        return

    if not session.get('message_id'):
        await update_balance_async(user_id, session['bet'])
        del TOWER_SESSIONS[user_id]
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
    await safe_answer(query, " Игра отменена, средства возвращены")

async def rr_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для русской рулетки"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    user_id = user.id
    
    if await check_ban(update, context):
        return
    
    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return
    
    text = update.message.text.strip()
    parts = text.split()
    
    if len(parts) == 1:
        await update.message.reply_text(
            f"<blockquote>ℹ️ Русская рулетка – это игра, в которой игрок использует револьвер с одним или пятью патронами, помещая его в барабан и вращая его.</blockquote>\n\n"
            f"🤖 <b>{user.full_name}</b>, чтобы начать игру, используй команду:\n\n"
            f"🔫 /buckshot [ставка]\n\n"
            f"Пример:\n"
            f"/buckshot 100\n"
            f"рр 100",
            parse_mode='HTML'
        )
        return
    
    bet_amount = parse_amount(parts[1])
    
    if bet_amount < 1000:
        await update.message.reply_text("❌ Минимальная ставка: 1.000ms¢")
        return
    
    db_user = await get_user_async(user_id)
    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(db_user['balance'])}ms¢")
        return
    
    await update_balance_async(user_id, -bet_amount)
    
    text = (
        f"🔫 Рус. рулетка • начни игру!\n"
        f"••••••••••••••••\n"
        f"💸 Ставка: {format_amount(bet_amount)}ms¢"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("1️⃣ пуля x1.15", callback_data=f"rr_bullets_{bet_amount}_1"),
            InlineKeyboardButton("2️⃣ пули x1.45", callback_data=f"rr_bullets_{bet_amount}_2")
        ],
        [
            InlineKeyboardButton("3️⃣ пули x1.95", callback_data=f"rr_bullets_{bet_amount}_3"),
            InlineKeyboardButton("4️⃣ пули x2.9", callback_data=f"rr_bullets_{bet_amount}_4")
        ],
        [
            InlineKeyboardButton("5️⃣ пуль x5.8", callback_data=f"rr_bullets_{bet_amount}_5"),
            InlineKeyboardButton("❌ Отменить", callback_data="rr_cancel")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup)

async def rr_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена создания игры"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user_id:
        await query.answer("🙈 Это не ваша кнопка!", show_alert=True)
        return
    
    import re
    match = re.search(r'rr_bullets_(\d+)_', query.message.text)
    if match:
        bet_amount = int(match.group(1))
        await update_balance_async(user_id, bet_amount)
    
    await query.message.delete()
    await query.answer("❌ Игра отменена")

async def rr_bullets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор количества пуль"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user_id:
        await query.answer("🙈 Это не ваша кнопка!", show_alert=True)
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
    
    RR_SESSIONS[user_id] = {
        'game_id': game_id,
        'message_id': sent_msg.message_id,
        'chat_id': update.effective_chat.id,
        'start_time': time.time()
    }
    
    await query.answer()

async def rr_cell_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия на ячейку"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    data = query.data
    
    if data == "rr_dead":
        await query.answer("❌ Эта клетка уже открыта", show_alert=True)
        return
    
    if data == "rr_finished":
        await query.answer("❌ Игра уже завершена", show_alert=True)
        return
    
    parts = data.split('_')
    game_id = int(parts[2])
    cell_idx = int(parts[3])
    
    game = await get_rr_game_async(game_id)
    if not game:
        await query.answer("❌ Игра не найдена", show_alert=True)
        return
    
    if game['user_id'] != user_id:
        await query.answer("🙈 Это не ваша игра!", show_alert=True)
        return
    
    if game['status'] != 'active':
        await query.answer("❌ Игра уже завершена", show_alert=True)
        return
    
    if cell_idx in game['opened']:
        await query.answer("❌ Эта клетка уже открыта", show_alert=True)
        return
    
    if user_id in RR_SESSIONS:
        RR_SESSIONS[user_id]['start_time'] = time.time()
    
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
        await query.answer(f"💥 БАХ! Вы проиграли {format_amount(game['bet'])}ms¢!", show_alert=True)
        
        await update_user_stats_async(user_id, 0, game['bet'])
        
        if user_id in RR_SESSIONS:
            del RR_SESSIONS[user_id]
        
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
            await query.answer(f"✅ Вы выиграли {format_amount(win_amount)}ms¢!", show_alert=True)
            
            await update_user_stats_async(user_id, win_amount, 0)
            
            if user_id in RR_SESSIONS:
                del RR_SESSIONS[user_id]
            
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
            await query.answer(f"✅ Безопасно! +{current_multiplier:.2f}x")

async def dice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в кости"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    user_id = user.id
    user_name = user.full_name
    chat_id = update.effective_chat.id
    
    if ('pending_password_check' in context.user_data or
        'pending_comment_check' in context.user_data or
        f'pending_activation_{user_id}' in context.user_data):
        from handlers.checks import handle_check_text_input
        await handle_check_text_input(update, context)
        return

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return
    
    text = update.message.text.strip()
    parts = text.split()
    
    if len(parts) == 1:
        await show_dice_games(update, context)
        return
    
    if len(parts) == 2:
        max_players = 2
        bet_amount = parse_amount(parts[1])
    elif len(parts) == 3:
        try:
            max_players = int(parts[1])
            if max_players < 2 or max_players > 10:
                await update.message.reply_text("Количество игроков должно быть от 2 до 10.")
                return
        except:
            await update.message.reply_text("Неверный формат количества игроков.")
            return
        bet_amount = parse_amount(parts[2])
    else:
        await update.message.reply_text(
            "Использование:\n"
            "• `кости` - показать активные игры\n"
            "• `кости 1к` - игра на 2 игроков со ставкой 1к\n"
            "• `кости 5 1к` - игра на 5 игроков со ставкой 1к"
        )
        return
    
    if bet_amount < DICE_MIN_BET:
        await update.message.reply_text(f"Минимальная ставка: {format_amount(DICE_MIN_BET)}ms¢")
        return
    
    if bet_amount > DICE_MAX_BET:
        await update.message.reply_text(f"Максимальная ставка: {format_amount(DICE_MAX_BET)}ms¢")
        return
    
    db_user = await get_user_async(user_id)
    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f"Недостаточно средств. Баланс: {format_amount(db_user['balance'])}ms¢")
        return
    
    active_games = await get_chat_dice_games_async(chat_id, 'waiting')
    if len(active_games) >= DICE_MAX_GAMES_PER_CHAT:
        await update.message.reply_text(f"В чате максимум {DICE_MAX_GAMES_PER_CHAT} активных игр.")
        return
    
    await update_balance_async(user_id, -bet_amount)
    
    game_number = await get_next_game_number_async(chat_id)
    game_id_display = f"{game_number}.{chat_id % 1000:03d}"
    
    from datetime import datetime, timedelta
    expires_at = (datetime.now() + timedelta(minutes=DICE_TIMEOUT)).strftime('%Y-%m-%d %H:%M:%S')
    
    text = (
        f"🎲 Игра в кости #{game_id_display}\n"
        f"💰 Ставка: {format_amount(bet_amount)}ms¢\n\n"
        f"👥 Мест: [1/{max_players}]\n"
        f"✅ Игроки:\n"
        f"[{user_name}](tg://user?id={user_id})\n"
        f"\n"
        f"⚠ До автоматической отмены игры: {DICE_TIMEOUT} мин. 0 сек."
    )
    
    keyboard = [
        [
            InlineKeyboardButton("Играть", callback_data=f"dice_join_{game_number}"),
            InlineKeyboardButton("Отмена", callback_data=f"dice_leave_{game_number}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    sent_msg = await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    await create_dice_game_async(
        chat_id=chat_id,
        game_number=game_number,
        creator_id=user_id,
        creator_name=user_name,
        max_players=max_players,
        bet_amount=bet_amount,
        message_id=sent_msg.message_id
    )

async def show_dice_games(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать активные игры в кости (каждую отдельным сообщением)"""
    chat_id = update.effective_chat.id
    
    games = await get_chat_dice_games_async(chat_id, 'waiting')
    
    if not games:
        await update.message.reply_text("🎲 Сейчас нет ставок.")
        return
    
    for game in games:
        players = await get_dice_game_players_async(game['game_id'])
        
        from datetime import datetime
        try:
            expires = datetime.strptime(game['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            expires = datetime.strptime(game['expires_at'], '%Y-%m-%d %H:%M:%S')
        
        now = datetime.now()
        delta = expires - now
        
        minutes = delta.seconds // 60
        seconds = delta.seconds % 60
        
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
        
        keyboard = [
            [
                InlineKeyboardButton("Играть", callback_data=f"dice_join_{game['game_number']}"),
                InlineKeyboardButton("Отмена", callback_data=f"dice_leave_{game['game_number']}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def dice_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Присоединение к игре"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_name = user.full_name
    chat_id = update.effective_chat.id
    
    current_time = time.time()
    if user_id in dice_cooldown:
        time_passed = current_time - dice_cooldown[user_id]
        if time_passed < DICE_COOLDOWN_SECONDS:
            remaining = round(DICE_COOLDOWN_SECONDS - time_passed, 1)
            await query.answer(f"⏳ Подождите {remaining} сек.", show_alert=True)
            return
    
    data = query.data
    game_number = int(data.replace('dice_join_', ''))
    
    games = await get_chat_dice_games_async(chat_id, 'waiting')
    game = next((g for g in games if g['game_number'] == game_number), None)
    
    if not game:
        await query.answer("❌ Игра не найдена", show_alert=True)
        return
    
    game_id = game['game_id']
    
    players = await get_dice_game_players_async(game_id)
    if any(p['user_id'] == user_id for p in players):
        await query.answer("🎲 Вы уже играете.", show_alert=True)
        return
    
    if len(players) >= game['max_players']:
        await query.answer("❌ Мест больше нет", show_alert=True)
        return
    
    db_user = await get_user_async(user_id)
    if db_user['balance'] < game['bet_amount']:
        await query.answer(f"❌ Недостаточно средств. Нужно {format_amount(game['bet_amount'])}ms¢", show_alert=True)
        return
    
    dice_cooldown[user_id] = current_time
    
    await update_balance_async(user_id, -game['bet_amount'])
    
    success, player_count = await add_dice_player_async(game_id, user_id, user_name)
    
    if not success:
        await query.answer("❌ Ошибка при входе в игру", show_alert=True)
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
        await query.answer("✅ Вы зашли в игру!")
        
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
        await query.answer("🎲 Вы зашли в игру.")

async def dice_leave_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выход из игры"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    chat_id = update.effective_chat.id
    
    current_time = time.time()
    if user_id in dice_cooldown:
        time_passed = current_time - dice_cooldown[user_id]
        if time_passed < DICE_COOLDOWN_SECONDS:
            remaining = round(DICE_COOLDOWN_SECONDS - time_passed, 1)
            await query.answer(f"⏳ Подождите {remaining} сек.", show_alert=True)
            return
    
    data = query.data
    game_number = int(data.replace('dice_leave_', ''))
    
    games = await get_chat_dice_games_async(chat_id, 'waiting')
    game = next((g for g in games if g['game_number'] == game_number), None)
    
    if not game:
        await query.answer("❌ Игра не найдена", show_alert=True)
        return
    
    game_id = game['game_id']
    
    players = await get_dice_game_players_async(game_id)
    player = next((p for p in players if p['user_id'] == user_id), None)
    
    if not player:
        await query.answer("🗿 Это не ваша игра!", show_alert=True)
        return
    
    dice_cooldown[user_id] = current_time
    
    await update_balance_async(user_id, game['bet_amount'])
    
    remaining = await remove_dice_player_async(game_id, user_id)
    
    if remaining == 0:
        await cancel_dice_game_async(game_id)
        try:
            await query.message.delete()
        except Exception as e:
            logging.error(f"Error deleting dice game message: {e}")
        await query.answer("🎲 Вы успешно вышли из игры.")
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
    await query.answer("🎲 Вы успешно вышли из игры.")

async def start_dice_game(update: Update, context: ContextTypes.DEFAULT_TYPE, game_id, chat_id):
    """Запуск игры и определение победителя"""
    
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
        f"\n"
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
    """Проверка просроченных игр в кости"""
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

async def coinfall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создание монетопада: /coinfall участников сумма или кф участников сумма"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return
    
    text = update.message.text.strip()
    
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text("❌ Использование: /coinfall *участников* *сумма* или кф *участников* *сумма*")
        return
    
    try:
        max_players = int(parts[1])
        if max_players < 2 or max_players > 20:
            await update.message.reply_text("❌ Количество участников должно быть от 2 до 20.")
            return
    except:
        await update.message.reply_text("❌ Неверный формат количества участников.")
        return
    
    prize = parse_amount(parts[2])
    if prize <= 0:
        await update.message.reply_text("❌ Неверная сумма.")
        return

    formatted_prize = format_amount(prize)
    
    text = (
        f"🪙 Монетопад запущен!\n\n"
        f"💸 Приз – {formatted_prize}ms¢.\n\n"
        f"👥 Участники: \n\n"
        f"Монетопад начнется тогда, когда достигнется максимальное количество участников и администратор запустит.\n"
        f"ℹ️ Чтобы вступить нажми кнопку ниже."
    )
    
    keyboard = [[InlineKeyboardButton("🥇 Участвовать", callback_data="coinfall_join")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    sent_msg = await update.message.reply_text(text, reply_markup=reply_markup)
    
    game_id = await create_coinfall_async(
        prize=prize,
        max_players=max_players,
        created_by=user_id,
        chat_id=update.effective_chat.id,
        message_id=sent_msg.message_id
    )
    
    if 'coinfall_games' not in context.chat_data:
        context.chat_data['coinfall_games'] = {}
    context.chat_data['coinfall_games'][update.effective_chat.id] = game_id

async def coinfall_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки участия в монетопаде"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    user_name = user.full_name
    chat_id = update.effective_chat.id
    
    game = await get_active_coinfall_async(chat_id)
    if not game:
        await query.answer("❌ Монетопад не найден", show_alert=True)
        return
    
    if game['status'] != 'waiting':
        await query.answer("❌ Монетопад уже начался", show_alert=True)
        return
    
    success, player_count = await add_coinfall_player_async(game['id'], user_id, user_name)
    
    if not success:
        await query.answer("❌ Вы уже участвуете", show_alert=True)
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
    await query.answer(f"✅ Вы стали участником монетопада!")

async def coinfall_start_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запуск монетопада администратором"""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = update.effective_chat.id
    
    if user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await query.answer("❌ Вы не администратор.", show_alert=True)
        return
    
    game = await get_active_coinfall_async(chat_id)
    if not game or game['status'] != 'waiting':
        await query.answer("❌ Монетопад не найден или уже начат", show_alert=True)
        return
    
    players = await get_coinfall_players_async(game['id'])
    if len(players) < game['max_players']:
        await query.answer(f"❌ Нужно еще {game['max_players'] - len(players)} участников", show_alert=True)
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
    await query.answer()

async def coinfall_claim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получение выигрыша победителем"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    game_id = int(data.replace('coinfall_claim_', ''))
    
    game = await get_coinfall_async(game_id)
    if not game or game['status'] != 'finished':
        await query.answer("❌ Монетопад не найден", show_alert=True)
        return
    
    if game['claimed'] == 1:
        await query.answer("❌ Вы уже забирали данную награду!", show_alert=True)
        return
    
    if game['winner_id'] != user_id:
        await query.answer("❌ Это не ваша кнопка!", show_alert=True)
        return
    
    success, prize = await claim_coinfall_async(game_id, user_id)
    
    if success:
        await update_balance_async(user_id, prize)
        
        formatted_prize = format_amount(prize)
        keyboard = [[InlineKeyboardButton(f"✅ Получено", callback_data="coinfall_claimed")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_reply_markup(reply_markup=reply_markup)
        await query.answer(f"✅ Вы получили {formatted_prize}ms¢!", show_alert=True)
    else:
        await query.answer("❌ Ошибка при получении", show_alert=True)

async def coinfall_join_disabled_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Заглушка для неактивной кнопки участия"""
    query = update.callback_query
    await query.answer("❌ Участники уже набраны", show_alert=True)

async def coinfall_claimed_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Заглушка для полученного приза"""
    query = update.callback_query
    await query.answer("✅ Награда уже получена", show_alert=True)


async def bank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для банка (в беседе)"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    
    if update.effective_chat.type == "private":
        await bank_private_command(update, context)
        return
    
    text = (
        f"🏦 *{user.full_name}*, банк доступен только в ЛС!"
    )
    
    keyboard = [[InlineKeyboardButton("📱 Перейти в ЛС", url=f"https://t.me/{context.bot.username}?start=bank")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def bank_private_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Банк в личных сообщениях"""
    if not update.effective_user:
        return
    
    user = update.effective_user
    user_id = user.id
    
    if context.args and context.args[0] == 'bank':
        await update.message.reply_text("🔄 Перенаправляю в банк...")
    
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
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def bank_convert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Конвертация валюты (недоступно)"""
    query = update.callback_query
    await query.answer("👨‍💻 Конвертация на доработках.", show_alert=True)

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
        await query.answer()
        return
    
    text = f"🏧 *{user.full_name}*, список ваших депозитов:\n\n"
    
    from datetime import datetime
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
    await query.answer()

async def bank_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр конкретного депозита"""
    query = update.callback_query
    user = query.from_user
    deposit_id = int(query.data.replace('bank_view_', ''))
    
    deposit = await get_deposit_async(deposit_id)
    if not deposit or deposit['user_id'] != user.id:
        await query.answer("❌ Депозит не найден", show_alert=True)
        return
    
    from datetime import datetime
    
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
    await query.answer()

async def bank_withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение снятия депозита"""
    query = update.callback_query
    user = query.from_user
    deposit_id = int(query.data.replace('bank_withdraw_', ''))
    
    deposit = await get_deposit_async(deposit_id)
    if not deposit or deposit['user_id'] != user.id:
        await query.answer("❌ Депозит не найден", show_alert=True)
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
    await query.answer()

async def bank_confirm_withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение снятия депозита"""
    query = update.callback_query
    user = query.from_user
    deposit_id = int(query.data.replace('bank_confirm_withdraw_', ''))
    
    deposit = await get_deposit_async(deposit_id)
    if not deposit or deposit['user_id'] != user.id:
        await query.answer("❌ Депозит не найден", show_alert=True)
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
    
    await query.answer()

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
    await query.answer()

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
    await query.answer()

async def bank_amount_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор суммы депозита"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    amount = int(query.data.replace('bank_amount_', ''))
    
    if user_id not in bank_creation_data or bank_creation_data[user_id].get('step') != 'amount':
        await query.answer("❌ Сессия создания депозита устарела", show_alert=True)
        return
    
    bank_creation_data[user_id]['amount'] = amount
    await bank_confirm_deposit(query, context, user_id)

async def bank_amount_custom_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод своей суммы депозита"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if user_id not in bank_creation_data or bank_creation_data[user_id].get('step') != 'amount':
        await query.answer("❌ Сессия создания депозита устарела", show_alert=True)
        return
    
    bank_creation_data[user_id]['step'] = 'custom_amount'
    
    await query.edit_message_text(
        f"⚠ Введите сумму:\n"
        f"Можно использовать к, кк\n"
        f"Максимум: {format_amount(BANK_MAX_AMOUNT)}ms¢"
    )
    await query.answer()

async def bank_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений в ЛС для банка"""
    if not update.effective_user:
        return
    user_id = update.effective_user.id
    
    if ('pending_password_check' in context.user_data or
        'pending_comment_check' in context.user_data or
        f'pending_activation_{user_id}' in context.user_data):
        from handlers.checks import handle_check_text_input
        await handle_check_text_input(update, context)
        return
    
    if user_id not in bank_creation_data or bank_creation_data[user_id].get('step') != 'custom_amount':
        return
    
    text = update.message.text.strip()
    
    import re
    if not re.match(r'^[\d\.,\s]*[kKкКmMмМ]?[\d\.,\s]*$', text):
        await update.message.reply_text("❌ Пожалуйста, введите число (например: 1000, 1к, 2.5кк)")
        return
    
    amount = parse_amount(text)
    
    if amount <= 0:
        await update.message.reply_text("❌ Неверная сумма.")
        return
    
    if amount > BANK_MAX_AMOUNT:
        await update.message.reply_text(f"❌ Максимальная сумма {format_amount(BANK_MAX_AMOUNT)}ms¢")
        return
    
    user = await get_user_async(user_id)
    if user['balance'] < amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Баланс: {format_amount(user['balance'])}ms¢")
        return
    
    bank_creation_data[user_id]['amount'] = amount
    bank_creation_data[user_id]['step'] = 'confirm'
    
    success, deposit_id = await create_deposit_async(
        user_id, 
        amount, 
        bank_creation_data[user_id]['days'], 
        bank_creation_data[user_id]['rate']
    )
    
    if not success:
        await update.message.reply_text(f"❌ {deposit_id}")
        del bank_creation_data[user_id]
        return
    
    bank_creation_data[user_id]['deposit_id'] = deposit_id
    
    text = (
        f"🏦 Вы хотите создать депозит 🆔 {deposit_id}.\n\n"
        f"💸 Сумма – {format_amount(amount)}ms¢.\n"
        f"Процентная ставка: {bank_creation_data[user_id]['rate']}%\n\n"
        f"Подтвердите создание:"
    )
    
    keyboard = [[
        InlineKeyboardButton("✅ Создать", callback_data=f"bank_final_confirm_{deposit_id}")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup)

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
        await query.answer("❌ Депозит не найден", show_alert=True)
        return
    
    if deposit['user_id'] != user_id:
        await query.answer("❌ Это не ваш депозит", show_alert=True)
        return
    
    if deposit['status'] != 'active':
        await query.answer("❌ Депозит уже неактивен", show_alert=True)
        return
    
    await query.edit_message_text(
        f"✅ Вы успешно создали депозит 🆔 {deposit_id}!\n\n"
        f"💸 Сумма: {format_amount(deposit['amount'])}ms¢\n"
        f"📅 Дней: {deposit['days']}\n"
        f"📊 Процент: {deposit['interest_rate']}%"
    )
    
    if user_id in bank_creation_data:
        del bank_creation_data[user_id]
    
    await query.answer()

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
    await query.answer()

async def untop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для исключения пользователя из топа: антоп @username или в ответ на сообщение"""
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("Ахуел?")
        return

    target_id = None
    target_name = None

    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
        target_name = update.message.reply_to_message.from_user.full_name
        
        await add_to_top_exclude_async(target_id)
        
        await update.message.reply_text(
            f"Пользователю {target_name} был наложен ТБАН.\n"
            f"ID: {target_id}"
        )
        return

    if len(context.args) < 1:
        await update.message.reply_text(
            "Использование: /антоп *@username* или /антоп *id*\n"
            "Или ответь на сообщение пользователя командой /антоп"
        )
        return

    target = context.args[0]

    if target.startswith('@'):
        username = target[1:]
        user_data = await get_user_by_username_async(username)
        if not user_data:
            await update.message.reply_text(f"Пользователь @{username} не найден.")
            return
        target_id = user_data['user_id']
        target_name = user_data['full_name'] or username
    else:
        try:
            target_id = int(target)
            user_data = await get_user_async(target_id)
            target_name = user_data.get('full_name') or user_data.get('username') or str(target_id)
        except:
            await update.message.reply_text("Неверный ID.")
            return

    await add_to_top_exclude_async(target_id)
    
    await update.message.reply_text(
        f"Пользователю {target_name} был наложен ТБАН.\n"
        f"ID: {target_id}"
    )

async def return_top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для возвращения пользователя в топ: вернитоп @username"""
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("У вас нет прав на выполнение этой команды.")
        return

    if not context.args or len(context.args) < 1:
        await update.message.reply_text("Использование: /втоп *@username* или /втоп *id*")
        return

    target = context.args[0]
    target_id = None
    target_name = None

    if target.startswith('@'):
        username = target[1:]
        user_data = await get_user_by_username_async(username)
        if not user_data:
            await update.message.reply_text(f"Пользователь @{username} не найден.")
            return
        target_id = user_data['user_id']
        target_name = user_data['full_name'] or username
    else:
        try:
            target_id = int(target)
            user_data = await get_user_async(target_id)
            target_name = user_data.get('full_name') or user_data.get('username') or str(target_id)
        except:
            await update.message.reply_text("Неверный формат ID.")
            return

    await remove_from_top_exclude_async(target_id)
    
    await update.message.reply_text(
        f"Пользователь {target_name} возвращён в топ.\n"
        f"ID: {target_id}"
    )

async def math_contest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для создания math-конкурса: !mt сумма или мт сумма"""
    if not update.effective_user:
        return
    
    user_id = update.effective_user.id
    
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
        
        math_contest_pending[user_id] = {'prize': prize_amount}
        
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
    
    if admin_id not in ADMIN_IDS and admin_id != MAIN_ADMIN_ID:
        await query.answer("Не наглей!🤬", show_alert=True)
        return
    
    if admin_id not in math_contest_pending:
        await query.answer("❌ Конкурс не найден", show_alert=True)
        return
    
    await query.answer("✅ Отправляю...")
    
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
    
    del math_contest_pending[admin_id]

async def math_answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ответа на math-конкурс"""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    data = query.data
    parts = data.split('_')
    
    if len(parts) != 4:
        await query.answer("❌ Ошибка", show_alert=True)
        return
    
    contest_id = int(parts[2])
    option_index = int(parts[3])
    
    contest = await get_math_contest_async(contest_id)
    if not contest:
        await query.answer("❌ Конкурс не найден", show_alert=True)
        return
    
    if contest['status'] != 'active':
        await query.answer("❌ Конкурс уже завершен", show_alert=True)
        return
    
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
                await query.answer(f"Подождите {remaining} секунд", show_alert=True)
                return
        
        await query.answer("✖ Неправильный вариант", show_alert=True)
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
            
            await query.answer(f"✅ Вы выиграли {formatted_prize}ms¢!", show_alert=True)
        else:
            await query.answer("❌ Кто-то уже ответил раньше", show_alert=True)
    else:
        await query.answer("✖ Неправильный вариант", show_alert=True)



async def pyramid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
        
    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    if user_id in PYRAMID_SESSIONS:
        session = PYRAMID_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del PYRAMID_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Пирамида. Завершите её сначала.")
            return

    if user_id in MINES_SESSIONS:
        session = MINES_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del MINES_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Мины. Завершите её сначала.")
            return

    if user_id in GOLD_SESSIONS:
        session = GOLD_SESSIONS[user_id]
        if session.get('status') in ['lost', 'won']:
            del GOLD_SESSIONS[user_id]
        else:
            await update.message.reply_text("⚠️ У вас уже есть активная игра в Золото. Завершите её сначала.")
            return

    text = update.message.text.strip()

    doors_count = 3
    bet_amount = 0

    db_user = await get_user_async(user_id, user.full_name, user.username)

    if text.startswith('/pyramid'):
        args = context.args
        if len(args) == 1:
            bet_amount = parse_bet_amount(args[0], db_user['balance'])
            doors_count = 3
        elif len(args) >= 2:
            bet_amount = parse_bet_amount(args[0], db_user['balance'])
            try:
                doors_count = int(args[1])
                if doors_count < 1 or doors_count > 4:
                    doors_count = 3
            except:
                doors_count = 3
    elif text.lower().startswith('пирамида '):
        parts = text.lower().split()
        if len(parts) == 2:
            bet_amount = parse_bet_amount(parts[1], db_user['balance'])
            doors_count = 3
        elif len(parts) >= 3:
            bet_amount = parse_bet_amount(parts[1], db_user['balance'])
            try:
                doors_count = int(parts[2])
                if doors_count < 1 or doors_count > 4:
                    doors_count = 3
            except:
                doors_count = 3

    if bet_amount <= 0:
        await update.message.reply_text(
            "<blockquote>ℹ️ Пирамида - это игра, где в каждом раунде перед вами 4 двери. Ваша задача выбрать одну из них и подниматься все выше: от заброшенной развалюхи до священной вершины.</blockquote>\n"
            "Дойдите до вершины и заберите максимальный выигрыш. Чем меньше дверей, тем выше выигрыш\n\n"
            f"🤖 {user.full_name}, чтобы начать игру, используйте команду:\n\n"
            "🏝️ /pyramid [ставка] [1-3]\n\n"
            "Примеры:\n"
            "– Пирамида 100\n"
            "– /pyramid 100 1",
            parse_mode='HTML'
        )
        return

    if doors_count > 3:
        doors_count = 3

    db_user = await get_user_async(user_id, user.full_name, user.username)

    success = await update_balance_safe_async(user_id, -bet_amount, bet_amount)
    if not success:
        await update.message.reply_text("❌ Недостаточно средств на балансе.")
        return

    multipliers = []
    if doors_count == 1:
        multipliers = PYRAMID_MULTIPLIERS_1
    elif doors_count == 2:
        multipliers = PYRAMID_MULTIPLIERS_2
    else:
        multipliers = PYRAMID_MULTIPLIERS_3

    grave_positions = []
    bombs_count = 4 - doors_count
    for level in range(12):
        positions = [0, 1, 2, 3]
        bombs = random.sample(positions, bombs_count)
        grave_positions.append(bombs)

    game_hash = generate_game_hash({
        'user_id': user_id,
        'game': 'pyramid',
        'bet': bet_amount,
        'doors': doors_count,
        'grave_positions': grave_positions
    })

    PYRAMID_SESSIONS[user_id] = {
        'grave_positions': grave_positions,
        'doors_count': doors_count,
        'multipliers': multipliers,
        'bet': bet_amount,
        'current_level': 0,
        'message_id': None,
        'chat_id': update.effective_chat.id,
        'message_thread_id': update.effective_message.message_thread_id,
        'status': 'active',
        'hash': game_hash,
        'opened_doors': [],
        'start_time': time.time()
    }

    await send_pyramid_board(update, context, user_id)

async def send_pyramid_board(update, context, user_id):
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

async def pyramid_cell_click(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id, level, door):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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

        await safe_answer(query, "💥 Могила!")
        return

    session['opened_doors'].append((current_level, door))
    session['current_level'] += 1

    if session['current_level'] >= 12:
        await pyramid_take_win(update, context, user_id)
    else:
        await send_pyramid_board(update, context, user_id)
        await safe_answer(query, f"✅ Уровень {current_level + 1} пройден!")

async def pyramid_take_win(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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

    await safe_answer(query, f"✅ Выигрыш {win_amount}ms¢")

async def pyramid_cancel_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id):
    query = update.callback_query

    if query.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша игра!")
        return

    cooldown = check_cooldown(user_id)
    if cooldown > 0:
        await safe_answer(query, f"⏳ Подождите {cooldown} сек. перед след. нажатием")
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
    await safe_answer(query, "✅ Игра отменена, средства возвращены")



async def basketball_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if not is_recent(update):
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) == 1 or (len(parts) == 2 and parts[1].lower() in ['бк', 'bk']):
        await update.message.reply_text(
            "<blockquote>ℹ️ Баскетбол – игра в которой вы должны забросить мяч в корзину!</blockquote>\n\n"
            f"🤖 {user.full_name}, используйте команду:\n\n"
            "Пример: бк 10000 гол\n"
            "Пример: бк 10000 мимо\n\n"
            "Коэффициенты:\n"
            "• Гол ✅ — x1.7\n"
            "• Мимо ❌ — x0.3",
            parse_mode='HTML'
        )
        return

    if len(parts) < 3:
        await update.message.reply_text(
            "❌ Неправильный формат. Используйте:\n"
            "бк *ставка* *итог*\n"
            "Пример: бк 10000 гол"
        )
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)
    bet_amount = parse_bet_amount(parts[1], db_user['balance'])
    user_choice = parts[2].lower()

    if user_choice not in ['гол', 'мимо']:
        await update.message.reply_text("❌ Итог должен быть 'гол' или 'мимо'")
        return

    if bet_amount <= 0:
        await update.message.reply_text("❌ Неверная сумма ставки.")
        return


    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Ваш баланс: {format_amount(db_user['balance'])}ms¢")
        return

    await update_balance_async(user_id, -bet_amount)

    await update.message.reply_text("🏀 Бросаю мяч....")

    asyncio.create_task(process_basketball_game(update, context, user_id, bet_amount, user_choice))


async def process_basketball_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, bet_amount: int, user_choice: str):
    """Обработка игры в баскетбол в фоновом режиме"""
    try:
        dice_message = await update.message.reply_dice(emoji="🏀")

        await asyncio.sleep(3)

        dice_value = dice_message.dice.value

        actual_result = "гол" if dice_value >= 4 else "мимо"
        user_won = (user_choice == actual_result)

        user = update.effective_user

        if user_won:
            multiplier = 1.7
            win_amount = int(bet_amount * multiplier)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)
            await update_task_progress_for_game(user_id, 'basketball', 1)

            quote = random.choice(BASKETBALL_WIN_QUOTES)
            result_text = "гол ✅" if actual_result == 'гол' else "мимо ❌"

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"🏀 Игра \"Баскетбол\" окончена!\n\n"
                    f"✔️ Итог – {result_text}\n\n"
                    f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
                    f"💰 Выигрыш: {format_amount(win_amount)}ms¢ (x1.7)\n\n"
                    f"{quote}"
                ),
                reply_to_message_id=dice_message.message_id
            )
        else:
            multiplier = 0.3
            win_amount = int(bet_amount * multiplier)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)
            await update_task_progress_for_game(user_id, 'basketball', 1)

            quote = random.choice(BASKETBALL_LOSE_QUOTES)
            result_text = "гол ✅" if actual_result == 'гол' else "мимо ❌"

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"🏀 Игра \"Баскетбол\" окончена!\n\n"
                    f"✔️ Итог – {result_text}\n\n"
                    f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
                    f"💰 Выигрыш: {format_amount(win_amount)}ms¢ (x0.3)\n\n"
                    f"{quote}"
                ),
                reply_to_message_id=dice_message.message_id
            )
    except Exception as e:
        logging.error(f"Ошибка в process_basketball_game: {e}")
        await update_balance_async(user_id, bet_amount)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Произошла ошибка во время игры. Ставка возвращена."
        )

async def football_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if not is_recent(update):
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id

    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) == 1 or (len(parts) == 2 and parts[1].lower() in ['фб', 'fb']):
        await update.message.reply_text(
            "<blockquote>ℹ️ Футбол – игра в которой вы должны попасть в ворота!</blockquote>\n\n"
            f"🤖 {user.full_name}, используйте команду:\n\n"
            "Пример: фб 10000 гол\n"
            "Пример: фб 10000 мимо\n\n"
            "Коэффициенты:\n"
            "• Гол ✅ — x1.7\n"
            "• Мимо ❌ — x0.3",
            parse_mode='HTML'
        )
        return

    if len(parts) < 3:
        await update.message.reply_text(
            "❌ Неправильный формат. Используйте:\n"
            "фб *ставка* *итог*\n"
            "Пример: фб 10000 гол"
        )
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)

    bet_amount = parse_bet_amount(parts[1], db_user['balance'])
    user_choice = parts[2].lower()

    if user_choice not in ['гол', 'мимо']:
        await update.message.reply_text("❌ Итог должен быть 'гол' или 'мимо'")
        return

    if bet_amount <= 0:
        await update.message.reply_text("❌ Неверная сумма ставки.")
        return


    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Ваш баланс: {format_amount(db_user['balance'])}ms¢")
        return

    await update_balance_async(user_id, -bet_amount)

    await update.message.reply_text("⚽ Пинаю мяч....")

    asyncio.create_task(process_football_game(update, context, user_id, bet_amount, user_choice))


async def process_football_game(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, bet_amount: int, user_choice: str):
    """Обработка игры в футбол в фоновом режиме"""
    try:
        dice_message = await update.message.reply_dice(emoji="⚽")

        await asyncio.sleep(4)

        dice_value = dice_message.dice.value
        
        if dice_value in [3, 4, 5]:
            actual_result = "гол"
        else:
            actual_result = "мимо"

        user_won = (user_choice == actual_result)

        if user_won:
            multiplier = 1.7 if user_choice == 'гол' else 0.3
            win_amount = int(bet_amount * multiplier)
            await update_balance_async(user_id, win_amount)
            await update_user_stats_async(user_id, win_amount, 0)
            await update_task_progress_for_game(user_id, 'football', 1)

            quote = random.choice(FOOTBALL_WIN_QUOTES)
            result_text = "гол ✅" if actual_result == 'гол' else "мимо ❌"

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"⚽ Игра \"Футбол\" окончена!\n\n"
                    f"☑ Итог — {result_text}\n\n"
                    f"💸 Ставка: {format_amount(bet_amount)}ms¢\n"
                    f"💰 Выигрыш: {format_amount(win_amount)}ms¢\n\n"
                    f"{quote}"
                ),
                reply_to_message_id=dice_message.message_id
            )
        else:
            await update_user_stats_async(user_id, 0, bet_amount)
            await update_task_progress_for_game(user_id, 'football', 1)

            quote = random.choice(FOOTBALL_LOSE_QUOTES)
            result_text = "гол ✅" if actual_result == 'гол' else "мимо ❌"

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"⚽ Игра \"Футбол\" окончена!\n\n"
                    f"☑ Итог — {result_text}\n\n"
                    f"💸 Ставка: {format_amount(bet_amount)}ms¢\n\n"
                    f"{quote}"
                ),
                reply_to_message_id=dice_message.message_id
            )
    except Exception as e:
        logging.error(f"Ошибка в process_football_game: {e}")
        await update_balance_async(user_id, bet_amount)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Произошла ошибка во время игры. Ставка возвращена."
        )


BASKETBALL_WIN_QUOTES = [
    "🏀 С победой!",
    "🏀 Отличный бросок!",
    "🏀 Точное попадание!",
    "🏀 Мяч в корзине!"
]

BASKETBALL_LOSE_QUOTES = [
    "🏀 Мимо... Повезёт в следующий раз!",
    "🏀 Немного не повезло.",
    "🏀 Бросок не достиг цели.",
    "🏀 В следующий раз получится!"
]

FOOTBALL_WIN_QUOTES = [
    "⚽ ГОЛ!",
    "⚽ Отличный удар!",
    "⚽ Мяч в сетке!",
    "⚽ Победа!"
]

FOOTBALL_LOSE_QUOTES = [
    "⚽ Мимо ворот...",
    "⚽ Штанга!",
    "⚽ Немного не повезло.",
    "⚽ В следующий раз забьёшь!"
]


async def set_stocks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Установить все акции на 100 (только для админов)"""
    if not update.effective_user:
        return

    if update.effective_user.id not in ADMIN_IDS and update.effective_user.id != MAIN_ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав на выполнение этой команды.")
        return

    keyboard = [[
        InlineKeyboardButton("✅ ПОДТВЕРДИТЬ", callback_data="confirm_set_stocks_100")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "⚠️ *Подтверждение*\n\n"
        "Вы собираетесь установить ВСЕ акции на 100ms¢.\n"
        "Цены будут изменены, но портфели пользователей останутся без изменений.\n\n"
        "Продолжить?",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def set_stocks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения установки акций на 100"""
    query = update.callback_query
    user = query.from_user

    if user.id not in ADMIN_IDS and user.id != MAIN_ADMIN_ID:
        await query.answer("❌ У вас нет прав на это действие.", show_alert=True)
        return

    if query.data == "confirm_set_stocks_100":
        await query.answer("⏳ Устанавливаем цены...")

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

    await query.answer()

async def knb_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для игры в КНБ (Камень-Ножницы-Бумага)"""
    if not update.effective_user:
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    parts = text.split()

    if len(parts) == 1 or (len(parts) == 2 and parts[1].lower() in ['кнб', 'knb']):
        await update.message.reply_text(
            "<blockquote>ℹ️ КНБ (Камень-Ножницы-Бумага) — популярная игра в которой вы должны поставить нужный знак под другой знак. Например вы ставите ножницы, а бот ставит бумагу и т.д.</blockquote>\n\n"
            f"🤖 {user.full_name}, чтобы начать игру используйте команду:\n\n"
            "🤖 /knb [ставка] — игра с ботом\n"
            "🤝 /knb *юзернейм* [ставка] — игра с другим пользователем\n\n"
            "Пример: /knb 1к\n"
            "Пример: /knb @durov 100",
            parse_mode='HTML'
        )
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)
    if user_id not in ADMIN_IDS:
        is_subscribed = await check_subscription(update, context, user_id)
        if not is_subscribed:
            await send_subscription_prompt(update, context)
            return

    if len(parts) == 2:
        bet_amount = parse_bet_amount(parts[1], db_user['balance'])
        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return

        await knb_vs_bot(update, context, user, bet_amount)

    elif len(parts) >= 3:
        target = parts[1]
        bet_amount = parse_bet_amount(parts[2])

        if bet_amount <= 0:
            await update.message.reply_text("❌ Неверная сумма ставки.")
            return

        if target.startswith('@'):
            username = target[1:]
            result = await get_user_by_username_async(username)
            if not result:
                await update.message.reply_text(f"❌ Пользователь {target} не найден.")
                return
            target_id, target_name = result
        else:
            await update.message.reply_text("❌ Укажите корректный юзернейм (например @durov)")
            return

        if target_id == user_id:
            await update.message.reply_text("❌ Нельзя вызвать на дуэль самого себя.")
            return

        await knb_challenge(update, context, user, target_id, target_name, bet_amount)


async def knb_vs_bot(update: Update, context: ContextTypes.DEFAULT_TYPE, user, bet_amount):
    """Игра против бота"""
    user_id = user.id

    if await check_ban(update, context):
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)
    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Ваш баланс: {format_amount(db_user['balance'])}ms¢")
        return

    await update_balance_async(user_id, -bet_amount)

    game_msg = await update.message.reply_text(
        f"🗿 {user.full_name}, игра начата! • Камень-Ножницы-Бумага\n"
        f"•••••••••••\n"
        f"🤖 Бот — *ждёт хода*\n"
        f"👤 {user.full_name} — *ждёт хода*\n\n"
        f"💸 Ставка: {format_amount(bet_amount)}ms¢.",
        parse_mode='Markdown'
    )

    game_id = f"{user_id}_{int(time.time() * 1000)}"
    
    # Получаем KHB_GAMES из контекста
    KHB_GAMES = context.bot_data.get('KHB_GAMES', {})
    
    KHB_GAMES[game_id] = {
        'type': 'bot',
        'user1_id': user_id,
        'user1_name': user.full_name,
        'user2_id': None,
        'user2_name': 'Бот',
        'bet': bet_amount,
        'message_id': game_msg.message_id,
        'chat_id': update.effective_chat.id,
        'status': 'waiting_bot',
        'user1_choice': None,
        'user2_choice': None,
        'bot_move_time': time.time() + 4.2,
        'turn': None
    }
    
    # Сохраняем обратно
    context.bot_data['KHB_GAMES'] = KHB_GAMES
    
    print(f"✅ Создана игра с ID: {game_id}")

    keyboard = [
        [
            InlineKeyboardButton(f"Камень {KHB_EMOJIS['камень']}", callback_data=f"knb:choice:{game_id}:камень"),
            InlineKeyboardButton(f"Ножницы {KHB_EMOJIS['ножницы']}", callback_data=f"knb:choice:{game_id}:ножницы"),
            InlineKeyboardButton(f"Бумага {KHB_EMOJIS['бумага']}", callback_data=f"knb:choice:{game_id}:бумага")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.edit_message_text(
        chat_id=game_msg.chat_id,
        message_id=game_msg.message_id,
        text=(
            f"🗿 {user.full_name}, игра начата! • Камень-Ножницы-Бумага\n"
            f"•••••••••••\n"
            f"🤖 Бот — *ждёт хода*\n"
            f"👤 {user.full_name} — *ждёт хода*\n\n"
            f"💸 Ставка: {format_amount(bet_amount)}ms¢."
        ),
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    asyncio.create_task(knb_bot_move(context, game_id))

async def knb_bot_move(context: ContextTypes.DEFAULT_TYPE, game_id):
    """Обработчик хода бота"""
    await asyncio.sleep(4.2)

    # Получаем KHB_GAMES из контекста
    KHB_GAMES = context.bot_data.get('KHB_GAMES', {})
    
    game = KHB_GAMES.get(game_id)
    if not game or game['status'] != 'waiting_bot':
        return

    choices = ['камень', 'ножницы', 'бумага']
    bot_choice = random.choice(choices)
    game['user2_choice'] = bot_choice
    game['status'] = 'waiting_user'
    game['turn'] = game['user1_id']
    
    # Сохраняем изменения
    context.bot_data['KHB_GAMES'] = KHB_GAMES

    keyboard = [
        [
            InlineKeyboardButton(f"Камень {KHB_EMOJIS['камень']}", callback_data=f"knb:choice:{game_id}:камень"),
            InlineKeyboardButton(f"Ножницы {KHB_EMOJIS['ножницы']}", callback_data=f"knb:choice:{game_id}:ножницы"),
            InlineKeyboardButton(f"Бумага {KHB_EMOJIS['бумага']}", callback_data=f"knb:choice:{game_id}:бумага")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.edit_message_text(
            chat_id=game['chat_id'],
            message_id=game['message_id'],
            text=(
                f"🗿 {game['user1_name']}, игра начата! • Камень-Ножницы-Бумага\n"
                f"•••••••••••\n"
                f"🤖 Бот — ♟️ Ход сделан\n"
                f"👤 {game['user1_name']} — *ваш ход*\n\n"
                f"💸 Ставка: {format_amount(game['bet'])}ms¢."
            ),
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        logging.error(f"Error in knb_bot_move: {e}")

async def knb_choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, game_id, choice):
    """Обработчик выбора в игре с ботом"""
    query = update.callback_query
    user_id = query.from_user.id

    # Получаем KHB_GAMES из контекста
    KHB_GAMES = context.bot_data.get('KHB_GAMES', {})
    
    print(f"🔍 knb_choice_handler: game_id={game_id}, choice={choice}, user_id={user_id}")
    print(f"🔍 KHB_GAMES keys: {list(KHB_GAMES.keys())}")

    game = KHB_GAMES.get(game_id)
    if not game:
        print(f"❌ Игра {game_id} не найдена в KHB_GAMES")
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
    
    # Сохраняем изменения
    context.bot_data['KHB_GAMES'] = KHB_GAMES

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

async def knb_challenge(update: Update, context: ContextTypes.DEFAULT_TYPE, user, target_id, target_name, bet_amount):
    """Создание вызова на дуэль"""
    user_id = user.id

    if await check_ban(update, context):
        return

    db_user = await get_user_async(user_id, user.full_name, user.username)
    if db_user['balance'] < bet_amount:
        await update.message.reply_text(f"❌ Недостаточно средств. Ваш баланс: {format_amount(db_user['balance'])}ms¢")
        return

    await update_balance_async(user_id, -bet_amount)

    # Получаем KHB_DUELS из контекста
    KHB_DUELS = context.bot_data.get('KHB_DUELS', {})

    for duel_id, duel in list(KHB_DUELS.items()):
        if duel['status'] == 'active' and (duel['challenger_id'] == user_id or duel['opponent_id'] == user_id):
            await update_balance_async(duel['challenger_id'], duel['bet'])
            del KHB_DUELS[duel_id]

    duel_id = f"{user_id}_{target_id}_{int(time.time())}"
    expire_time = time.time() + 300

    keyboard = [
        [InlineKeyboardButton("✅ Принять вызов", callback_data=f"knb_accept_{duel_id}")],
        [InlineKeyboardButton("❌ Отменить вызов", callback_data=f"knb_cancel_{duel_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = await update.message.reply_text(
        f"🔫 {target_name}, вас вызвали на дуэль \"КНБ\"\n"
        f"Вызов от {user.full_name}\n\n"
        f"🙈 Вызов - активен\n\n"
        f"💸 Ставка: {format_amount(bet_amount)}ms¢.\n\n"
        f"⏱ Вызов будет автоматически отозван через 5 минут.",
        reply_markup=reply_markup
    )

    KHB_DUELS[duel_id] = {
        'challenger_id': user_id,
        'challenger_name': user.full_name,
        'opponent_id': target_id,
        'opponent_name': target_name,
        'bet': bet_amount,
        'message_id': message.message_id,
        'chat_id': update.effective_chat.id,
        'status': 'active',
        'expire_time': expire_time
    }
    
    # Сохраняем изменения
    context.bot_data['KHB_DUELS'] = KHB_DUELS

    asyncio.create_task(knb_duel_expire(context, duel_id))

async def knb_duel_expire(context: ContextTypes.DEFAULT_TYPE, duel_id):
    """Таймер для автоматической отмены вызова с возвратом денег"""
    await asyncio.sleep(300)

    # Получаем KHB_DUELS из контекста
    KHB_DUELS = context.bot_data.get('KHB_DUELS', {})

    duel = KHB_DUELS.get(duel_id)
    if not duel or duel['status'] != 'active':
        return

    await update_balance_async(duel['challenger_id'], duel['bet'])

    logging.info(f"⚙️ Дуэль {duel_id} автоматически отменена. Возврат {duel['bet']}ms¢ пользователю {duel['challenger_id']}")

    duel['status'] = 'expired'
    
    # Сохраняем изменения
    context.bot_data['KHB_DUELS'] = KHB_DUELS

    keyboard = [[InlineKeyboardButton("⏱ Вызов отозван", callback_data="noop")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.edit_message_text(
            chat_id=duel['chat_id'],
            message_id=duel['message_id'],
            text=(
                f"🔫 {duel['opponent_name']}, вас вызвали на дуэль \"КНБ\"\n"
                f"Вызов от {duel['challenger_name']}\n\n"
                f"🙈 Вызов - неактивен\n\n"
                f"💸 Ставка: {format_amount(duel['bet'])}ms¢.\n\n"
                f"⏱ Вызов был автоматически отозван. Средства возвращены."
            ),
            reply_markup=reply_markup
        )
    except Exception as e:
        logging.error(f"Ошибка при редактировании сообщения об истечении: {e}")

async def knb_cancel_duel(update: Update, context: ContextTypes.DEFAULT_TYPE, duel_id):
    """Ручная отмена вызова вызывающим игроком с возвратом денег"""
    query = update.callback_query
    user_id = query.from_user.id

    # Получаем KHB_DUELS из контекста
    KHB_DUELS = context.bot_data.get('KHB_DUELS', {})

    duel = KHB_DUELS.get(duel_id)
    if not duel or duel['status'] != 'active':
        await safe_answer(query, "❌ Вызов уже неактивен.")
        return

    if user_id != duel['challenger_id']:
        await safe_answer(query, "🗿 Только создатель вызова может его отменить.")
        return

    await update_balance_async(duel['challenger_id'], duel['bet'])

    duel['status'] = 'cancelled'
    
    # Сохраняем изменения
    context.bot_data['KHB_DUELS'] = KHB_DUELS

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

async def knb_pvp_choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, game_id, choice):
    """Обработчик выбора в игре между пользователями"""
    query = update.callback_query
    user_id = query.from_user.id

    # Получаем KHB_GAMES из контекста
    KHB_GAMES = context.bot_data.get('KHB_GAMES', {})

    logging.info(f"👥 INSIDE knb_pvp_choice_handler: game={game_id}, choice={choice}, user={user_id}")

    game = KHB_GAMES.get(game_id)
    if not game:
        logging.error(f"❌ Игра {game_id} не найдена")
        await safe_answer(query, "❌ Игра не найдена.")
        return

    if game['type'] != 'pvp':
        logging.error(f"❌ Неправильный тип игры: {game['type']}")
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
            
            # Сохраняем изменения
            context.bot_data['KHB_GAMES'] = KHB_GAMES

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
            
            # Сохраняем изменения
            context.bot_data['KHB_GAMES'] = KHB_GAMES

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
    """Принятие вызова на дуэль"""
    query = update.callback_query
    user_id = query.from_user.id

    logging.info(f"✅ INSIDE knb_accept_duel: duel={duel_id}, user={user_id}")

    # Получаем KHB_DUELS и KHB_GAMES из контекста
    KHB_DUELS = context.bot_data.get('KHB_DUELS', {})
    KHB_GAMES = context.bot_data.get('KHB_GAMES', {})

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

    # Сохраняем изменения
    context.bot_data['KHB_GAMES'] = KHB_GAMES
    context.bot_data['KHB_DUELS'] = KHB_DUELS

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


async def promo_report_task(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int):
    query = update.callback_query
    await query.edit_message_text(
        "Выберите причину:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔞 Запрещенный контент", callback_data=f"promo_report_reason_{task_id}_forbidden"),
             InlineKeyboardButton("💀 Насилие", callback_data=f"promo_report_reason_{task_id}_violence"),
             InlineKeyboardButton("📝 Другое", callback_data=f"promo_report_reason_{task_id}_other")],
            [InlineKeyboardButton("◀ Назад", callback_data=f"promo_task_{task_id}")]
        ])
    )

async def promo_report_submit(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: int, reason: str):
    query = update.callback_query
    await report_task(task_id, query.from_user.id, reason)
    await query.edit_message_text("✅ Жалоба отправлена!")
    def _get_task_creator():
        with get_db() as conn:
            return conn.execute('SELECT creator_id, link FROM promotion_tasks WHERE task_id = %s', (task_id,)).fetchone()
    info = await asyncio.to_thread(_get_task_creator)
    if info:
        creator_id, link = info
        creator = await get_user_async(creator_id)
        creator_username = creator.get('username') or f"ID: {creator_id}"
        admin_text = f"✔ Жалоба на №{task_id}\nСоздатель: @{creator_username}\nКанал/чат: {link}\nПожаловался: {query.from_user.full_name} (ID: {query.from_user.id})\nПричина: {reason}"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=aid,
                    text=admin_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Удалить задание", callback_data=f"promo_admin_delete_{task_id}"),
                         InlineKeyboardButton("Оставить задание", callback_data=f"promo_admin_keep_{task_id}")]
                    ])
                )
            except:
                pass

async def check_and_notify_vip_expired(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Проверка VIP и уведомление при истечении"""
    with get_db() as conn:
        user = conn.execute('SELECT vip_status, vip_until, full_name FROM users WHERE user_id = ?', (user_id,)).fetchone()
        if user and user[0] == 1 and user[1]:
            try:
                vip_until = datetime.strptime(user[1], '%Y-%m-%d %H:%M:%S')
                if vip_until < datetime.now():
                    # VIP истёк
                    conn.execute('UPDATE users SET vip_status = 0, vip_until = NULL WHERE user_id = ?', (user_id,))
                    conn.commit()
                    
                    # Уведомляем пользователя
                    try:
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=(
                                f"⏰ *Ваш VIP статус истёк!*\n\n"
                                f"📅 Действовал до: {vip_until.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                                f"🔓 Теперь при переводах будет взиматься комиссия 10%."
                            ),
                            parse_mode='Markdown'
                        )
                    except:
                        pass
                    return False
            except:
                pass
    return True

async def box_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общий callback для игры"""
    query = update.callback_query
    data = query.data.split('_')
    
    action = data[1]
    user_id = int(data[2])
    
    if query.from_user.id != user_id:
        await query.answer("❌ Это не ваша игра!", show_alert=True)
        return
    
    if action == 'cell':
        r, c = int(data[3]), int(data[4])
        await box_cell_click(update, context, user_id, r, c)
    elif action == 'cashout':
        await box_cashout(update, context, user_id)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    if not is_recent(update):
        return

    if await check_ban(update, context):
        return

    user = update.effective_user
    user_id = update.effective_user.id

    if update.message.chat.type == 'private':
        # Проверяем есть ли ожидания по чекам
        has_pending_check = (
            'pending_password_check' in context.user_data or
            'pending_comment_check' in context.user_data or
            f'pending_activation_{user_id}' in context.user_data
        )

        if has_pending_check:
            from handlers.checks import handle_check_text_input
            await handle_check_text_input(update, context)
            return  # ВАЖНО: возвращаемся, не обрабатываем другие команды!

    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    username = user.username or f"ID{user_id}"
    ADMIN_IDS = context.bot_data.get('ADMIN_IDS', [])
    first_name = user.first_name or ""
    
    message_text = update.message.text or ""
    
    # Показываем в консоли или логируем
    print(f"📨 Сообщение от @{username} ({user_id}) - {first_name}: {message_text[:100]}")

    # Проверяем статус тех. работ
    is_work_conditions = await get_work_conditions()
    
    # Если тех. работы активны и пользователь не админ
    if is_work_conditions and user_id not in ADMIN_IDS and user_id != MAIN_ADMIN_ID:
        await update.message.reply_text(
            "<tg-emoji emoji-id='5420323339723881652'>⚠️</tg-emoji> Активны технические работы!",
        parse_mode='HTML'
        )
        return

    # 👇 ПОЛУЧАЕМ ДАННЫЕ ИЗ КОНТЕКСТА
    MAIN_ADMIN_ID = context.bot_data.get('MAIN_ADMIN_ID')
    mailing_data = context.bot_data.get('mailing_data', {})

    # 👇 ВАЖНО: проверяем, есть ли активная сессия рассылки
    user_id = update.effective_user.id
    if user_id in mailing_data and mailing_data[user_id].get('step') == 'awaiting_text':
        await mailing_handle_text(update, context)
        return

    # Если это админ и он ввел "рассылка" - обрабатываем команду
    if user_id == MAIN_ADMIN_ID and update.message.text and update.message.text.strip().lower() == 'рассылка':
        await mailing_command(update, context)
        return

    if update.message.chat_shared:
        await promo_handle_chat_shared(update, context)
        return

    if not update.message.text:
        return


    text = update.message.text.strip().lower()

    if text == 'рассылка' and update.effective_user.id == MAIN_ADMIN_ID:
        await mailing_command(update, context)
        return

    if text in ['б', 'бал', 'баланс']:
        await balance(update, context)
    elif text in ['проф', 'profile', '/profile']:
        await profile_command(update, context)
    elif text in ['реф', '/ref', 'ref']:
        await ref_command(update, context)
    elif text in ['топ реф', 'топ рефы', 'топ рефов', 'topref']:
        await top_ref_command(update, context)
    elif text == 'топ':
        await top(update, context)
    if text in ['/promotion', 'продвижение', '/work', 'заработать']:
        if text in ['/work', 'заработать']:
            await work_command(update, context)
        else:
            await promotion_command(update, context)
        return
    elif text == "/topchat" or text == "топ ч":
        await chat_top(update, context)
    elif text.startswith('мины '):
        await mines_command(update, context)
    elif text.startswith('фб ') or text.startswith('/fb '):
        parts = text.split()
        if len(parts) >= 3:
            context.args = parts[1:]
            await football_command(update, context)
    elif text.startswith('бк ') or text.startswith('/bk '):
        parts = text.split()
        if len(parts) >= 3:
            context.args = parts[1:]
            await basketball_command(update, context)
    elif text.startswith('краш ') or text.startswith('/crash ') or text.startswith('к '):
        await crash_command(update, context)
        return
    elif text.startswith('золото '):
        await gold_command(update, context)
    elif text.startswith('кнб ') or text.startswith('/knb '):
        parts = text.split()
        context.args = parts[1:]
        await knb_command(update, context)
    elif text == "яйцо" or text == "egg":
        await egg_command(update, context)
        return
    elif text.startswith('шкатулка '):
        args = text.split()[1:]
        context.args = args if args else []  # ✅ если нет аргументов, пустой список
        await cmd_box(update, context)
    elif text == 'шкатулка':
        context.args = []  # ✅ без аргументов
        await cmd_box(update, context)
    elif text.startswith('/box '):
        args = text.split()[1:]
        context.args = args
        await cmd_box(update, context)
    elif text == '/box':
        context.args = []
        await cmd_box(update, context)
    if text.startswith('дартс ') or text.startswith('дс ') or text == 'дартс' or text == 'дс':
        await darts_command(update, context)
        return
    if text.startswith('рр ') or text.startswith('Рр ') or text.startswith('rr '):
        await rr_command(update, context)
        return
    if text.startswith('куб ') or text.startswith('кубик ') or text == 'куб' or text == 'кубик':
        await cubic_command(update, context)
        return
    if text.startswith('рул ') or text.startswith('рулетка ') or text == 'рул' or text == 'рулетка':
        await roulette_command(update, context)
        return
    if text == "кости" or text.startswith("кости "):
        await dice_command(update, context)
        return
    if text.startswith('квак ') or text == 'квак' or text.startswith('/frog '):
        await frog_command(update, context)
        return
    elif text.startswith('бо ') or text.startswith('Бо ') or text.startswith('боулинг ') or text.startswith('Боулинг '):
        await bowling_command(update, context)
    elif text.startswith('башня ') or text.startswith('/tower '):
        parts = text.split()
        if len(parts) >= 2:
            context.args = parts[1:]
            await tower_command(update, context)
        else:
            await tower_command(update, context)
        return
    if text.startswith('/sprevent'):
        await sprevent_command(update, context)
        return
    if text.startswith('/msh '):
        await msh_command(update, context)
        return
    if text.startswith('алмазы ') or text == 'алмазы' or text.startswith('/diamond '):
        await diamond_command(update, context)
        return
    if text.startswith('космо ') or text.startswith('космолёт ') or text == 'космо' or text == 'космолёт':
        await spaceship_command(update, context)
        return
    if text.startswith('msg ') or text.startswith('мсг ') or text.startswith('мг '):
        parts = text.split()
        context.args = parts[1:]
        await msg_transfer_command(update, context)
        return
    if text.startswith('монетка ') or text.startswith('мон ') or text == 'монетка' or text == 'мон':
        await coinflip_command(update, context)
        return
    if text == "ивент" or text == "весна":
        await sprevent_command(update, context)
        return
    if user_id in spring_question_creation:
        await spring_prize_value_handler(update, context)
        return
    if text.startswith('/check'):
        await check_command(update, context)
        return
    if text.startswith('!mt ') or text.startswith('мт '):
        await math_contest_command(update, context)
        return
    elif text in ['кнб', 'knb']:
        await knb_command(update, context)
    if text == "банк" or text == "Банк":
        if update.effective_chat.type == "private":
            await bank_private_command(update, context)
        else:
            await bank_command(update, context)
        return
    elif text == "акции" or text == "Акции":
        await stocks_info_command(update, context)
    if update.message.chat_shared:
        await promo_handle_chat_shared(update, context)
        return
    if 'donat_step' in context.user_data and context.user_data['donat_step'] == 'waiting_amount':
        await donat_handle_text(update, context)
        return
    if text in ['/donat', '/donate', '/conversion', 'донат', 'конвертация', 'обменник']:
        await donat_command(update, context)
        return
    if 'promo_step' in context.user_data and context.user_data['promo_step'] == 'waiting_link':
        await promo_handle_link(update, context)
        return
    elif text == "ключи" or text == "keys":
        await keys_command(update, context)
        return
    elif text == "мои акции" or text == "Мои акции":
        await myaction_command(update, context)
    elif text == "магазин" or text == "Магазин" or text == "маг" or text == "Маг":
        await shop_command(update, context)
    elif text.startswith('buyact '):
        args = text.replace('buyact ', '').split()
        context.args = args
        await buyact_command(update, context)
    elif text.startswith('sellact '):
        args = text.replace('sellact ', '').split()
        context.args = args
        await sellact_command(update, context)
    if text in ['/daily', 'бонус']:
        await daily_command(update, context)
        return
    if text in ['/cases', 'кейсы', 'кейсы']:
        await cases_command(update, context)
        return
    elif text.startswith('пирамида '):
        await pyramid_command(update, context)
    elif text.startswith('п ') or text.startswith('перевод ') or text.startswith('/send ') or text.startswith('дать '):
        parts = text.split()
        if len(parts) >= 2:
            context.args = parts[1:]
            await transfer_command(update, context)
    if text.startswith('/антоп') or text.startswith('/untop'):
        parts = text.split()
        context.args = parts[1:] if len(parts) > 1 else []
        await untop_command(update, context)
        return
    if text.startswith('/втоп') or text.startswith('/returntop'):
        parts = text.split()
        context.args = parts[1:] if len(parts) > 1 else []
        await return_top_command(update, context)
        return
    if text.startswith('/coinfall ') or text.startswith('кф '):
        await coinfall_command(update, context)
        return
    if text.startswith('/actionn ') or text.startswith('/actionn'):
        parts = text.split()
        context.args = parts[1:] if len(parts) > 1 else []
        await actionn_command(update, context)
        return
    if text.startswith('/set ') or text.startswith('/set'):
        parts = text.split()
        context.args = parts[1:] if len(parts) > 1 else []
        await set_balance_command(update, context)
        return
    if text.startswith('гет ') or text == 'гет' or text.startswith('/get '):
        await get_user_info_command(update, context)
        return
    elif text.startswith('мут ') or text.startswith('глуш '):
        parts = text.split()
        if len(parts) > 1:
            context.args = parts[1:]
        else:
            context.args = []
        await mute_command(update, context)
        return
    elif text == 'мут' or text == 'глуш':
        context.args = []
        await mute_command(update, context)
        return
    elif text.startswith('кик '):
        parts = text.split()
        if len(parts) > 1:
            context.args = parts[1:]
        else:
            context.args = []
        await kick_command(update, context)
        return
    elif text == 'кик':
        context.args = []
        await kick_command(update, context)
        return
    elif text.startswith('!give '):
        args = text.replace('!give ', '').split()
        context.args = args
        await give_command(update, context)
    elif text.startswith('!take '):
        args = text.replace('!take ', '').split()
        context.args = args
        await take_command(update, context)
    elif text.startswith('промо '):
        promo_code = text[6:].strip()
        if promo_code:
            context.args = [promo_code]
            await promo_activate_command(update, context)
        else:
            await update.message.reply_text("Укажите название промокода.")
        return
    elif text.startswith('!checkhash '):
        args = text.replace('!checkhash ', '').split()
        context.args = args
        await checkhash_command(update, context)
    if text == "ивенты" or text == "Ивенты":
        await events_command(update, context)
        return
    if 'event_creation' in context.user_data:
        await event_text_handler(update, context)
        return
    elif text.startswith('!tcheckhash '):
        args = text.replace('!tcheckhash ', '').split()
        context.args = args
        await tcheckhash_command(update, context)
    elif text.startswith('/newcheck ') or text.startswith('+чек '):
        parts = text.split()
        if len(parts) >= 3:
            context.args = parts[1:]
            await newcheck_command(update, context)
    elif text == '/checklist':
        await checklist_command(update, context)

# ==================== BUTTON HANDLER ====================
# Теперь используется импортированный из handlers
async def post_init(application: Application):
    """Функция, вызываемая после инициализации бота"""
    asyncio.create_task(cleanup_old_sessions(application))
    asyncio.create_task(load_chat_stats(application))

async def load_chat_stats(application: Application):
    """Загрузка статистики чатов при запуске"""
    try:
        chats_to_monitor = []
        logging.info(f"Loading chat stats for {len(chats_to_monitor)} chats")
        
        for chat_id in chats_to_monitor:
            try:
                admins = await application.bot.get_chat_administrators(chat_id)
                logging.info(f"Chat {chat_id}: loaded {len(admins)} admins")
            except Exception as e:
                logging.error(f"Failed to load chat {chat_id}: {e}")
                
    except Exception as e:
        logging.error(f"Error loading chat stats: {e}")
async def get_all_chats() -> list:
    """Получить список всех чатов, где есть бот"""
    # Это нужно будет реализовать через БД
    # Создайте таблицу для хранения чатов
    def _get():
        with get_db() as conn:
            cursor = conn.execute("SELECT chat_id, chat_title, chat_type FROM bot_chats")
            return cursor.fetchall()
    return await asyncio.to_thread(_get)
# ==================== MAIN ====================
def main():
    global BOT_START_TIME, application
    BOT_START_TIME = time.time()
    init_db()
    from database import init_promotion_db, init_currency_db, init_chats_db, init_cases_db, init_keys_db, init_logs_db, init_work_conditions_db, init_games_db, cleanup_expired_games, init_easter_db
    init_promotion_db()
    init_currency_db()
    init_chats_db()
    init_cases_db()
    init_easter_db()
    init_keys_db()
    init_logs_db()
    init_work_conditions_db()
    init_games_db()

    from handlers.checks import init_checks_db
    init_checks_db()

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    async def run_cleanup():
        return await cleanup_expired_games()

    try:

        loop = asyncio.get_running_loop()
        asyncio.create_task(run_cleanup())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        expired_count = loop.run_until_complete(run_cleanup())
        if expired_count > 0:
            logging.info(f"Returned {expired_count} expired game bets")
    # Передаем константы в bot_data
    application.bot_data['ADMIN_IDS'] = ADMIN_IDS
    application.bot_data['MAIN_ADMIN_ID'] = MAIN_ADMIN_ID
    application.bot_data['CHANNEL_USERNAME'] = CHANNEL_USERNAME
    application.bot_data['CHANNEL2_USERNAME'] = CHANNEL2_USERNAME
    application.bot_data['KURS_CHANNEL'] = KURS_CHANNEL
    application.bot_data['COOLDOWN_SECONDS'] = COOLDOWN_SECONDS
    application.bot_data['KURS_MSG_CHANNEL'] = '@kursmsgmonstr'

    application.bot_data['MINES_SESSIONS'] = MINES_SESSIONS
    application.bot_data['GOLD_SESSIONS'] = GOLD_SESSIONS
    application.bot_data['PYRAMID_SESSIONS'] = PYRAMID_SESSIONS
    application.bot_data['FROG_SESSIONS'] = FROG_SESSIONS
    application.bot_data['DIAMOND_SESSIONS'] = DIAMOND_SESSIONS
    application.bot_data['TOWER_SESSIONS'] = TOWER_SESSIONS
    application.bot_data['RR_SESSIONS'] = RR_SESSIONS
    application.bot_data['BOWLING_SESSIONS'] = BOWLING_SESSIONS
    application.bot_data['dice_cooldown'] = dice_cooldown
    application.bot_data['LAST_CLICK_TIME'] = LAST_CLICK_TIME
    application.bot_data['KHB_GAMES'] = {}
    application.bot_data['KHB_DUELS'] = {}
    application.bot_data['COINFLIP_SESSIONS'] = COINFLIP_SESSIONS
    application.bot_data['CASES_SESSIONS'] = CASES_SESSIONS
    application.bot_data['SPACESHIP_SESSIONS'] = SPACESHIP_SESSIONS

    application.bot_data['CRASH_SESSIONS'] = CRASH_SESSIONS
    application.bot_data['math_contest_pending'] = math_contest_pending
    application.bot_data['pending_transfers'] = pending_transfers
    application.bot_data['transfer_confirmations'] = transfer_confirmations
    application.bot_data['mailing_data'] = {}
    application.bot_data['DICE_SESSIONS'] = DICE_SESSIONS
    application.bot_data['pending_msg_transfers'] = {}
    application.bot_data['ROULETTE_SESSIONS'] = ROULETTE_SESSIONS
    application.bot_data['DARTS_SESSIONS'] = DARTS_SESSIONS


    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("game", games))
    application.add_handler(CommandHandler("games", games))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("support", support))
    application.add_handler(CommandHandler("mines", mines_command))
    application.add_handler(CommandHandler("gold", gold_command))
    application.add_handler(CommandHandler("promotion", promotion_command))
    application.add_handler(CommandHandler("work", work_command))
    application.add_handler(CommandHandler("cubic", cubic_command))
    application.add_handler(CommandHandler("pyramid", pyramid_command))
    application.add_handler(CommandHandler("slot", slot_command))
    application.add_handler(CommandHandler("football", football_command))
    application.add_handler(CommandHandler("fb", football_command))
    application.add_handler(CommandHandler("knb", knb_command))
    application.add_handler(CommandHandler("roulette", roulette_command))
    application.add_handler(CommandHandler("diamond", diamond_command))
    application.add_handler(CommandHandler("basketball", basketball_command))
    application.add_handler(CommandHandler("space", spaceship_command))
    application.add_handler(CommandHandler("safe", safe_command))
    application.add_handler(CommandHandler("egg", egg_command))
    application.add_handler(CommandHandler("eastertop", easter_top_command))
    application.add_handler(CommandHandler("check", check_command))
    application.add_handler(CommandHandler("box", cmd_box))
    application.add_handler(CommandHandler("frog", frog_command))
    application.add_handler(CommandHandler("spaceship", spaceship_command))
    application.add_handler(CommandHandler("keys", keys_command))
    
    application.add_handler(CommandHandler("easter", easter_exchange_command))
    application.add_handler(CommandHandler("bk", basketball_command))
    application.add_handler(CommandHandler("darts", darts_command))
    application.add_handler(CommandHandler("daily", daily_command))
    application.add_handler(CommandHandler("cases", cases_command))
    application.add_handler(CommandHandler("tower", tower_command))
    application.add_handler(CommandHandler("bowling", bowling_command))
    application.add_handler(CommandHandler("crash", crash_command))
    application.add_handler(CommandHandler("ucheck", ucheck_command))
    application.add_handler(CommandHandler("balance", balance))
    application.add_handler(CommandHandler("top", top))
    application.add_handler(CommandHandler("topchat", chat_top))
    application.add_handler(CommandHandler("donat", donat_command))
    application.add_handler(CommandHandler("donate", donat_command))
    application.add_handler(CommandHandler("conversion", donat_command))
    application.add_handler(CommandHandler("profile", profile_command))
    application.add_handler(CommandHandler("coinflip", coinflip_command))
    application.add_handler(CommandHandler("ref", ref_command))
    application.add_handler(CommandHandler("topref", top_ref_command))
    application.add_handler(CommandHandler("msg", msg_transfer_command))
    application.add_handler(CommandHandler("gmsg", give_msg_command))
    application.add_handler(CommandHandler("tmsg", take_msg_command))
    application.add_handler(CommandHandler("givemoney", give_command))
    application.add_handler(CommandHandler("ban", ban_command))
    application.add_handler(CommandHandler("events", events_command))
    application.add_handler(CommandHandler("sprevent", sprevent_command))
    application.add_handler(CommandHandler("answer", answer_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("question", question_command))
    application.add_handler(CommandHandler("setlogs", setlogs_command))
    application.add_handler(CommandHandler("msh", msh_command))
    application.add_handler(CommandHandler("gmsh", gmsh_command))
    application.add_handler(CommandHandler("rmsh", rmsh_command))
    application.add_handler(CommandHandler("removelogs", removelogs_command))
    application.add_handler(CommandHandler("put", put_command))
    application.add_handler(CommandHandler("exchange", exchange_command))
    application.add_handler(CommandHandler("allchats", allchats_command))
    application.add_handler(CommandHandler("addtask", add_task_command))
    application.add_handler(CommandHandler("setevent", setevent_command))
    application.add_handler(CommandHandler("closeevent", closeevent_command))
    application.add_handler(CommandHandler("resetstocks", reset_stocks_command))
    application.add_handler(CommandHandler("setmsgrate", set_msg_rate_command))
    application.add_handler(CommandHandler("workcondit", workcondit_command))
    application.add_handler(CommandHandler("setcondit", setcondit_command))
    application.add_handler(CommandHandler("setstocks", set_stocks_command))
    application.add_handler(CommandHandler("unban", unban_command))
    application.add_handler(CommandHandler("get", get_user_info_command))
    application.add_handler(CommandHandler("coinfall", coinfall_command))
    application.add_handler(CommandHandler("send", transfer_command))
    application.add_handler(CommandHandler("kmmute", mute_command))
    application.add_handler(CommandHandler("kmkick", kick_command))
    application.add_handler(CommandHandler("setpromo", setpromo_command))
    application.add_handler(CommandHandler("givedg", donut_give))
    application.add_handler(CommandHandler("checkprom", checkprom_command))
    application.add_handler(CommandHandler("delpromo", delpromo_command))
    application.add_handler(CommandHandler("newcheck", newcheck_command))
    application.add_handler(CommandHandler("untop", untop_command))
    application.add_handler(CommandHandler("returntop", return_top_command))
    application.add_handler(CommandHandler("mt", math_contest_command))
    application.add_handler(CommandHandler("checklist", checklist_command))
    application.add_handler(CommandHandler("gvip", give_vip_command))
    application.add_handler(CommandHandler("mailing", mailing_command))
    application.add_handler(CommandHandler("listgive", list_donate_features))
    application.add_handler(CommandHandler("buyact", buyact_command))
    application.add_handler(CommandHandler("sellact", sellact_command))
    application.add_handler(CommandHandler("shop", shop_command))
    application.add_handler(CommandHandler("stocks", stocks_info_command))

    application.add_handler(MessageHandler(
        filters.Regex(r'^кости(\s|$)'), dice_command
    ))

    application.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        bank_text_handler
    ))

    application.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        safe_text_handler
    ))

    application.add_handler(CallbackQueryHandler(button_handler))

    application.add_handler(MessageHandler(filters.ALL, message_handler))

    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(update_stock_prices, interval=60, first=10)
        job_queue.run_repeating(check_expired_deposits, interval=60, first=30)
        job_queue.run_repeating(check_expired_dice_games, interval=60, first=30)
        job_queue.run_repeating(update_msg_rate_job, interval=600, first=5)
    webhook_url = os.getenv('WEBHOOK_URL')
    webhook_port = int(os.getenv('WEBHOOK_PORT', 8443))
    webhook_path = os.getenv('WEBHOOK_PATH', '/webhook')
    webhook_listen = os.getenv('WEBHOOK_LISTEN', '0.0.0.0')

    if not webhook_url:
        logging.error("WEBHOOK_URL не указан в .env")
        logging.info("Запуск в polling режиме...")
        application.run_polling()
        return

    async def init_webhook():
        try:
            await asyncio.wait_for(
                application.bot.set_webhook(url=f"{webhook_url}{webhook_path}"),
                timeout=30.0
            )
            logging.info(f"Webhook установлен на {webhook_url}{webhook_path}")
            return True
        except asyncio.TimeoutError:
            logging.error("Таймаут при установке вебхука (30 сек)")
            return False
        except Exception as e:
            logging.error(f"Ошибка при установке вебхука: {e}")
            return False

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        webhook_set = loop.run_until_complete(init_webhook())

        if not webhook_set:
            logging.error("Не удалось установить вебхук. Запуск в polling режиме...")
            application.run_polling()
        else:
            logging.info(f"Запуск вебхука на {webhook_listen}:{webhook_port}")
            application.run_webhook(
                listen=webhook_listen,
                port=webhook_port,
                url_path=webhook_path,
                webhook_url=f"{webhook_url}{webhook_path}",
                secret_token=None,
                cert=None
            )
    except KeyboardInterrupt:
        logging.info("Бот остановлен")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        logging.info("Пробуем запустить в polling режиме...")
        application.run_polling()
    finally:
        if loop:
            loop.close()

if __name__ == '__main__':
    main()
