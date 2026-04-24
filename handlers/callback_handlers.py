import logging
import asyncio
import aiosqlite
import traceback
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

from .games import (
    handle_mines_callbacks, handle_gold_callbacks, handle_pyramid_callbacks,
    handle_tower_callbacks, handle_rr_callbacks, handle_dice_callbacks,
    handle_coinfall_callbacks, handle_knb_callbacks, handle_dice_game_callbacks
)
from .bank import handle_bank_callbacks
from .investments import handle_investment_callbacks
from .admin import (
    handle_admin_callbacks, handle_mailing_callbacks, handle_check_callbacks,
    handle_top_exclude_callbacks, math_contest_confirm_callback, math_answer_callback
)
from .events import (
    handle_events_callbacks, handle_spring_callbacks
)
from .common import (
    handle_common_callbacks, handle_subscription_check, handle_help_callbacks,
    handle_transfer_callbacks, safe_answer, format_amount, get_user_async
)

from .settings import (
    settings_private_callback, settings_chat_callback, settings_transfer_callback,
    settings_transfer_confirmation_callback, settings_transfer_commission_callback
)

from .safe import handle_safe_callbacks

from database import get_easter_keys, add_easter_keys
#from main import easter_exchange_menu

from .checks import (
    handle_check_callbacks, handle_check_activation, activate_check_callback,
    handle_check_text_input, my_checks_callback
)
# ===== ВСПОМОГАТЕЛЬНЫЙ ИМПОРТ ДЛЯ ОСТАЛЬНЫХ ФУНКЦИЙ =====
# Эти функции из last14 импортируем через ленивый импорт, чтобы избежать цикла
_last14 = None

def _get_last14():
    global _last14
    if _last14 is None:
        from last14 import (
            confirm_msg_transfer, promo_rules_callback, promo_price_input,
            promo_tasks_command, promo_users_count, promo_confirm,
            promo_task_view, promo_check_task, promo_report_submit,
            promo_report_task, work_command, work_task_view,
            work_check_task, work_report_submit, work_report_task,
            my_tasks_command, donat_exchange_callback, donat_amount_callback,
            donat_confirm_callback, donat_back_callback, daily_claim_callback,
            cases_command, case_open_callback, case_cell_callback,
            spaceship_cell_callback, spaceship_take_callback,
            key_edit_callback, key_delete_callback, delete_task,
            my_tasks_command

        )
        _last14 = locals()
    return _last14

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

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Основной роутер для всех callback запросов"""
    if not update.callback_query:
        return

    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    print(f"🔴 CALLBACK DATA: {data}")
    logging.debug(f"Callback received: {data} from user {user_id}")

    # ===== ЗАГЛУШКИ =====
    if 'pending_activation' in context.user_data:
        # Это не callback, а текстовый ввод, обрабатывается в message_handler
        pass

    if data == "noop":
        await safe_answer(query, "")
        return

    if data.startswith("dead_") or "dead_" in data:
        await safe_answer(query, "🙈 Игра завершена")
        return

    if data.startswith("safe_"):
        handled = await handle_safe_callbacks(update, context, data, user_id)
        if handled:
            return

    if data == "safe_enter_pin":
        await safe_enter_pin_handler(update, context)
        return

    
    # ===== НАСТРОЙКИ (СНАЧАЛА БОЛЕЕ КОНКРЕТНЫЕ) =====
    if data.startswith("settings_transfer_confirmation_on_"):
        try:
            print(f"DEBUG: Processing confirmation_on callback")
            target_id = int(data.split('_')[4])
            print(f"DEBUG: target_id = {target_id}, user_id = {user_id}")
            if target_id != user_id:
                await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
                return
            from .settings import settings_transfer_confirmation_callback
            print(f"DEBUG: Import successful, calling callback")
            await settings_transfer_confirmation_callback(update, context, target_id, True)
            print(f"DEBUG: Callback finished")
        except Exception as e:
            print(f"Error in settings_transfer_confirmation_on_: {e}")
            traceback.print_exc()
            await safe_answer(query, "❌ Ошибка", show_alert=True)
        return

    if data.startswith("settings_transfer_confirmation_off_"):
        try:
            print(f"DEBUG: Processing confirmation_off callback")
            target_id = int(data.split('_')[4])
            print(f"DEBUG: target_id = {target_id}, user_id = {user_id}")
            if target_id != user_id:
                await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
                return
            from .settings import settings_transfer_confirmation_callback
            print(f"DEBUG: Import successful, calling callback")
            await settings_transfer_confirmation_callback(update, context, target_id, False)
            print(f"DEBUG: Callback finished")
        except Exception as e:
            print(f"Error in settings_transfer_confirmation_off_: {e}")
            traceback.print_exc()
            await safe_answer(query, "❌ Ошибка", show_alert=True)
        return

    if data.startswith("settings_transfer_commission_off_"):
        try:
            target_id = int(data.split('_')[4])
            if target_id != user_id:
                await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
                return
            from .settings import settings_transfer_commission_callback
            await settings_transfer_commission_callback(update, context, target_id)
        except Exception as e:
            print(f"Error in settings_transfer_commission_off_: {e}")
            traceback.print_exc()
            await safe_answer(query, "❌ Ошибка", show_alert=True)
        return

    if data.startswith("settings_transfer_"):
        try:
            target_id = int(data.split('_')[2])
            if target_id != user_id:
                await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
                return
            from .settings import settings_transfer_callback
            await settings_transfer_callback(update, context, target_id)
        except Exception as e:
            print(f"Error in settings_transfer_: {e}")
            traceback.print_exc()
            await safe_answer(query, "❌ Ошибка", show_alert=True)
        return

    if data.startswith("settings_private_"):
        try:
            target_id = int(data.split('_')[2])
            if target_id != user_id:
                await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
                return
            from .settings import settings_private_callback
            await settings_private_callback(update, context, target_id)
        except Exception as e:
            print(f"Error in settings_private_: {e}")
            traceback.print_exc()
            await safe_answer(query, "❌ Ошибка", show_alert=True)
        return

    if data.startswith("settings_chat_"):
        try:
            target_id = int(data.split('_')[2])
            if target_id != user_id:
                await safe_answer(query, "🙈 Это не ваши настройки!", show_alert=True)
                return
            from .settings import settings_chat_callback
            await settings_chat_callback(update, context, target_id)
        except Exception as e:
            print(f"Error in settings_chat_: {e}")
            traceback.print_exc()
            await safe_answer(query, "❌ Ошибка", show_alert=True)
        return
	    # ===== ОБЩИЕ КНОПКИ =====
    if await handle_common_callbacks(update, context, data, user_id):
        return
    
    if data.startswith(("mines_cell_", "mines_take_", "mines_cancel_", "mines_dead_")):
        await handle_mines_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("dice_num_", "dice_even_", "dice_odd_", "dice_big_", "dice_small_", "dice_equal_", "dice_cancel_")):
        await handle_dice_game_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("gold_left_", "gold_right_", "gold_take_", "gold_dead_")):
        await handle_gold_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("pyr_", "take_", "cancel_")):
        await handle_pyramid_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("box_cell_", "box_cashout_", "box_cancel_", "box_opened_", "box_dead_", "box_finished_")):
        from last14 import handle_box_callbacks
        await handle_box_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("tower_cell_", "tower_take_", "tower_cancel_", "tower_dead_")):
        await handle_tower_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("rr_bullets_", "rr_cell_", "rr_cancel")):
        await handle_rr_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("dice_join_", "dice_leave_")):
        await handle_dice_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("coinfall_join", "coinfall_start", "coinfall_claim_", "coinfall_claimed", "coinfall_join_disabled")):
        await handle_coinfall_callbacks(update, context, data, user_id)
        return
    
    if data.startswith(("knb:choice:", "knb:pvp:", "knb_accept_", "knb_cancel_")):
        await handle_knb_callbacks(update, context, data, user_id)
        return
    
# В функции button_handler добавьте:
# ==================== ПАСХАЛЬНЫЙ ОБМЕННИК ====================
    if data == "easter_exchange_open":
        await easter_exchange_menu(update, context)
        return

    if data == "exchange_back":
        await easter_exchange_menu(update, context)
        return

    if data.startswith("exchange_"):
        await handle_easter_exchange(update, context)
        return

 #===== ИНВЕСТИЦИИ =====
    if data.startswith(("view_stocks", "portfolio_page_", "shop_stocks", "shop_page_", 
                        "stock_info_", "confirm_sell_", "cancel_sell")):
        await handle_investment_callbacks(update, context, data, user_id)
        return
    
    # ===== БАНК =====
    if data.startswith(("bank_", "bank_create", "bank_list", "bank_convert", 
                        "bank_back_to_menu", "bank_days_", "bank_amount_", 
                        "bank_view_", "bank_withdraw_", "bank_confirm_withdraw_", 
                        "bank_final_confirm_")):
        await handle_bank_callbacks(update, context, data, user_id)
        return
    
    # ===== ИВЕНТЫ =====
    if data.startswith(("spring_", "spring_mysteries", "spring_questions_list", 
                        "spring_collect", "spring_exchange", "spring_castle", 
                        "spring_tasks", "spring_back_to_menu", "spring_prize_")):
        await handle_spring_callbacks(update, context, data, user_id)
        return
    
    # ===== MATH КОНКУРС (АДМИН) =====
    if data.startswith(("math_contest_confirm", "math_answer_")):
        if data == "math_contest_confirm":
            await math_contest_confirm_callback(update, context)
        elif data.startswith("math_answer_"):
            await math_answer_callback(update, context)
        return

    if data.startswith("frog_cell_"):
        from last14 import frog_cell_callback
        parts = data.split('_')
        target_id = int(parts[2])
        position = int(parts[3])
        await frog_cell_callback(update, context, target_id, position)
        return

    if data.startswith("frog_take_"):
        from last14 import frog_take_callback
        target_id = int(data.split('_')[2])
        await frog_take_callback(update, context, target_id)
        return

    if data.startswith("frog_cancel_"):
        from last14 import frog_cancel_callback
        target_id = int(data.split('_')[2])
        await frog_cancel_callback(update, context, target_id)
        return

    if data.startswith("frog_dead"):
        await safe_answer(query, "🙈 Эта игра завершена")
        return
    # ===== ОБЫЧНЫЕ ИВЕНТЫ =====
    if data.startswith(("event_view_", "event_type_", "event_confirm_", "event_close_", "event_close_confirm_")):
        await handle_events_callbacks(update, context, data, user_id)
        return
    
# ===== АДМИНКА =====
    if data.startswith(("bonus_", "slot_", "slot_spin_")):
        await handle_admin_callbacks(update, context, data, user_id)
        return

    if data.startswith(("mailing_toggle_markdown", "mailing_toggle_inline", "mailing_confirm")):
        await handle_mailing_callbacks(update, context, data, user_id)
        return

    if data.startswith(("confirm_reset_stocks", "confirm_set_stocks_100")):
        await handle_admin_callbacks(update, context, data, user_id)
        return

    # ===== ЧЕКИ (РАСПОЛОЖЕНО ПРАВИЛЬНО - ДО ВОЗВРАТА В АДМИНКЕ) =====
    if data.startswith(('buy_check_book_', 'my_checks', 'my_checks_page_',
                        'set_password_', 'set_comment_', 'copy_link_',
                        'activate_check_')):
        from .checks import handle_check_callbacks
        await handle_check_callbacks(update, context)
        return
    
    if data.startswith(('copy_personal_link_', 'set_personal_password_', 'set_personal_comment_', 'activate_personal_check_')):
        from .checks import handle_personal_check_callbacks
        await handle_personal_check_callbacks(update, context)
        return

    if data.startswith(("switch_to_chat_top", "switch_to_global_top")):
        await handle_top_exclude_callbacks(update, context, data, user_id)
        return
    
    if data == "check_subscription":
        await handle_subscription_check(update, context, data, user_id)
        return
    
    if data.startswith("help_"):
        await handle_help_callbacks(update, context, data, user_id)
        return
    
    # ===== ПЕРЕВОДЫ =====
    if data.startswith("confirm_msg_"):
        transfer_id = data.replace("confirm_msg_", "")
        last14 = _get_last14()
        await last14['confirm_msg_transfer'](update, context, transfer_id)
        return
    
    if data.startswith(("confirm_transfer_", "final_confirm_")):
        await handle_transfer_callbacks(update, context, data, user_id)
        return
    
    # ===== ПРОДВИЖЕНИЕ =====
    last14 = _get_last14()
    
    if data == "promo_rules":
        await last14['promo_rules_callback'](update, context)
        return
    
    if data == "promo_channel":
        await last14['promo_price_input'](update, context, 'channel')
        return
    
    if data == "promo_chat":
        await last14['promo_price_input'](update, context, 'chat')
        return
    
    if data== "promo_my_tasks":
        await last14['my_tasks_command'](update, context, 1)
        return

    if data == "promo_tasks":
        await last14['promo_tasks_command'](update, context, 1)
        return
    
    if data.startswith("promo_price_"):
        price = int(data.replace("promo_price_", ""))
        await last14['promo_users_count'](update, context, price)
        return
    
    if data.startswith("promo_users_"):
        if data == "promo_users_max":
            users = context.user_data.get('promo_max_users', 1)
        else:
            users = int(data.replace("promo_users_", ""))
        await last14['promo_confirm'](update, context, users)
        return
    
    if data == "promo_back_to_menu":
        user = query.from_user
        db_user = await get_user_async(user.id)
        msg_balance = db_user.get('msg_balance', 0)
        
        text = (
            f"⚙️ {user.full_name}, что ты хочешь рекламировать?\n\n"
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
        
        await query.edit_message_text(text, reply_markup=reply_markup)
        return
    
    if data == "promo_back_to_price":
        promo_type = context.user_data.get('promo_type', 'channel')
        await last14['promo_price_input'](update, context, promo_type)
        return
    
    if data == "promo_cancel":
        await query.edit_message_text("❌ Действие отменено")
        context.user_data.clear()
        return
    
    if data.startswith("promo_task_"):
        task_id = int(data.replace("promo_task_", ""))
        await last14['promo_task_view'](update, context, task_id)
        return
    
    if data.startswith("my_tasks_page_"):
        page = int(data.replace("my_tasks_page", ""))
        await last14['my_tasks_command'](update, context, page)
        return

    if data.startswith("promo_check_"):
        task_id = int(data.replace("promo_check_", ""))
        await last14['promo_check_task'](update, context, task_id)
        return
    
    if data.startswith("promo_report_"):
        if "promo_report_reason_" in data:
            parts = data.split('_')
            task_id = int(parts[3])
            reason = parts[4]
            await last14['promo_report_submit'](update, context, task_id, reason)
        else:
            task_id = int(data.replace("promo_report_", ""))
            await last14['promo_report_task'](update, context, task_id)
        return
    
    if data.startswith("promo_tasks_page_"):
        page = int(data.replace("promo_tasks_page_", ""))
        await last14['promo_tasks_command'](update, context, page)
        return
    
    if data.startswith("promo_admin_delete_"):
        task_id = int(data.replace("promo_admin_delete_", ""))
        await last14['delete_task'](task_id)
        await safe_answer(query, "✅ Задание удалено")
        await query.edit_message_text("✅ Задание удалено")
        return
    
    if data.startswith("promo_admin_keep_"):
        await safe_answer(query, "✅ Задание оставлено")
        await query.edit_message_text("✅ Задание оставлено")
        return
    
    if data == "work_refresh":
        await last14['work_command'](update, context, 1)
        return
    
    if data.startswith("work_page_"):
        page = int(data.replace("work_page_", ""))
        await last14['work_command'](update, context, page)
        return
    
    if data.startswith("work_task_"):
        task_id = int(data.replace("work_task_", ""))
        await last14['work_task_view'](update, context, task_id)
        return
    
    if data.startswith("work_check_"):
        task_id = int(data.replace("work_check_", ""))
        await last14['work_check_task'](update, context, task_id)
        return
    
    if data.startswith("work_report_reason_"):
        parts = data.split('_')
        task_id = int(parts[3])
        reason = parts[4]
        await last14['work_report_submit'](update, context, task_id, reason)
        return
    
    if data.startswith("work_report_"):
        task_id = int(data.replace("work_report_", ""))
        await last14['work_report_task'](update, context, task_id)
        return
    
    if data == "promo_my_tasks":
        await last14['my_tasks_command'](update, context, 1)
        return
    
    if data.startswith("my_tasks_page_"):
        page = int(data.replace("my_tasks_page_", ""))
        await last14['my_tasks_command'](update, context, page)
        return
    
    # ===== DONAT =====
    if data == "donat_exchange":
        await last14['donat_exchange_callback'](update, context)
        return
    
    if data.startswith("donat_amount_"):
        if data == "donat_amount_max":
            db_user = await get_user_async(query.from_user.id)
            amount = db_user.get('msg_balance', 0)
        else:
            amount = int(data.replace("donat_amount_", ""))
        await last14['donat_amount_callback'](update, context, amount)
        return
    
    if data == "donat_confirm":
        await last14['donat_confirm_callback'](update, context)
        return
    
    if data == "donat_back":
        await last14['donat_back_callback'](update, context)
        return
    
    # ===== DAILY =====
    if data == "daily_claim":
        await last14['daily_claim_callback'](update, context)
        return
    
    # ===== CASES =====
    if data == "cases_refresh":
        await last14['cases_command'](update, context, 1)
        return
    
    if data.startswith("cases_page_"):
        page = int(data.replace("cases_page_", ""))
        await last14['cases_command'](update, context, page)
        return
    
    if data.startswith("case_open_"):
        case_type = data.replace("case_open_", "")
        await last14['case_open_callback'](update, context, case_type)
        return
    
    if data.startswith("case_cell_"):
        parts = data.split('_')
        target_id = int(parts[2])
        cell_idx = int(parts[3])
        await last14['case_cell_callback'](update, context, target_id, cell_idx)
        return
    
    if data.startswith(("case_dead_", "case_finished_")):
        await safe_answer(query, "🙈 Эта ячейка уже открыта")
        return
    
    # ===== SPACESHIP =====
    if data.startswith("spaceship_cell_"):
        parts = data.split('_')
        target_id = int(parts[2])
        level = int(parts[3])
        position = int(parts[4])
        await last14['spaceship_cell_callback'](update, context, target_id, level, position)
        return
    
    if data.startswith("spaceship_take_"):
        target_id = int(data.split('_')[2])
        await last14['spaceship_take_callback'](update, context, target_id)
        return
    
    if data.startswith("spaceship_dead"):
        await safe_answer(query, "🙈 Эта игра завершена")
        return
    
    # ===== КЛЮЧИ =====
    if data.startswith("key_edit_"):
        parts = data.split('_')
        key_code = parts[2]
        status = parts[3]
        await last14['key_edit_callback'](update, context, key_code, status)
        return
    
    if data.startswith("key_delete_"):
        key_code = data.replace("key_delete_", "")
        await last14['key_delete_callback'](update, context, key_code)
        return

    if data.startswith("diamond_cell_"):
        from last14 import diamond_cell_callback
        parts = data.split('_')
        target_id = int(parts[2])
        level = int(parts[3])
        position = int(parts[4])
        await diamond_cell_callback(update, context, target_id, level, position)
        return

    if data.startswith("diamond_take_"):
        from last14 import diamond_take_callback
        target_id = int(data.split('_')[2])
        await diamond_take_callback(update, context, target_id)
        return

    if data.startswith("diamond_cancel_"):
        from last14 import diamond_cancel_callback
        target_id = int(data.split('_')[2])
        await diamond_cancel_callback(update, context, target_id)
        return

    if data.startswith("diamond_dead"):
        await safe_answer(query, "🙈 Эта игра завершена")
        return

   # Если ничего не подошло
    logging.warning(f"Unhandled callback: {data}")
    await safe_answer(query, "❌ Неизвестная команда")

