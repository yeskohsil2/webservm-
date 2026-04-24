import logging
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from database import get_db

logger = logging.getLogger(__name__)

# Временное хранилище для вводимых PIN-кодов (в реальном проекте лучше использовать context.user_data)
temp_pins = {}

async def safe_enter_pin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для ввода PIN-кода - создает клавиатуру с цифрами"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Инициализируем временный PIN для пользователя
    temp_pins[user_id] = ""
    
    # Создаем клавиатуру с цифрами
    keyboard = []
    
    # Первый ряд: 1 2 3
    row = []
    for i in range(1, 4):
        row.append(InlineKeyboardButton(str(i), callback_data=f"safe_pin_digit_{i}"))
    keyboard.append(row)
    
    # Второй ряд: 4 5 6
    row = []
    for i in range(4, 7):
        row.append(InlineKeyboardButton(str(i), callback_data=f"safe_pin_digit_{i}"))
    keyboard.append(row)
    
    # Третий ряд: 7 8 9
    row = []
    for i in range(7, 10):
        row.append(InlineKeyboardButton(str(i), callback_data=f"safe_pin_digit_{i}"))
    keyboard.append(row)
    
    # Четвертый ряд: 0 Очистить Подтвердить
    keyboard.append([
        InlineKeyboardButton("0", callback_data="safe_pin_digit_0"),
        InlineKeyboardButton("🗑 Очистить", callback_data="safe_pin_clear"),
        InlineKeyboardButton("✅ Подтвердить", callback_data="safe_pin_confirm")
    ])
    
    # Пятый ряд: Назад
    keyboard.append([
        InlineKeyboardButton("◀️ Назад", callback_data="safe_back")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Показываем текущий ввод
    current_pin = temp_pins.get(user_id, "")
    display_pin = "*" * len(current_pin) if current_pin else "____"
    
    await query.edit_message_text(
        f"🔐 Введите PIN-код для сейфа\n\n"
        f"Код: {display_pin}\n\n"
        f"Используйте кнопки ниже для ввода цифр",
        reply_markup=reply_markup
    )

async def handle_safe_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик callback для сейфа"""
    
    query = update.callback_query
    
    # Проверяем владельца сейфа по тексту сообщения
    message = query.message
    owner_id = None
    
    if message and message.text:
        match = re.search(r'ID: (\d+)', message.text)
        if match:
            owner_id = int(match.group(1))
    
    if owner_id and owner_id != user_id:
        await query.answer("❌ Это не ваш сейф!", show_alert=True)
        return True
    
    # Импортируем функции из last14
    try:
        from last14 import (
            safe_view, safe_deposit_menu, safe_withdraw_menu,
            safe_set_pin_start, safe_deposit_amount, safe_withdraw_amount,
            safe_deposit_custom, safe_withdraw_custom, safe_command
        )
    except ImportError:
        # Если нет last14, используем заглушки
        async def safe_view(q, u): await q.edit_message_text("📦 Сейф\n💰 Баланс: 0 MSG")
        async def safe_deposit_menu(q, u): await q.edit_message_text("💰 Внесение средств\nВыберите сумму:")
        async def safe_withdraw_menu(q, u): await q.edit_message_text("💸 Снятие средств\nВыберите сумму:")
        async def safe_set_pin_start(q, u): await safe_enter_pin_handler(update, context)
        async def safe_deposit_custom(q, u, c): await q.edit_message_text("Введите сумму:")
        async def safe_withdraw_custom(q, u, c): await q.edit_message_text("Введите сумму:")
        async def safe_command(u, c): pass
    
    # ===== ОБРАБОТКА ЦИФР ДЛЯ PIN-КОДА =====
    if data.startswith("safe_pin_digit_"):
        digit = data.replace("safe_pin_digit_", "")
        
        # Получаем текущий PIN
        current_pin = temp_pins.get(user_id, "")
        
        # Добавляем цифру (максимум 4 цифры)
        if len(current_pin) < 4:
            temp_pins[user_id] = current_pin + digit
        
        # Обновляем клавиатуру
        keyboard = []
        
        # Первый ряд: 1 2 3
        keyboard.append([
            InlineKeyboardButton("1", callback_data="safe_pin_digit_1"),
            InlineKeyboardButton("2", callback_data="safe_pin_digit_2"),
            InlineKeyboardButton("3", callback_data="safe_pin_digit_3")
        ])
        
        # Второй ряд: 4 5 6
        keyboard.append([
            InlineKeyboardButton("4", callback_data="safe_pin_digit_4"),
            InlineKeyboardButton("5", callback_data="safe_pin_digit_5"),
            InlineKeyboardButton("6", callback_data="safe_pin_digit_6")
        ])
        
        # Третий ряд: 7 8 9
        keyboard.append([
            InlineKeyboardButton("7", callback_data="safe_pin_digit_7"),
            InlineKeyboardButton("8", callback_data="safe_pin_digit_8"),
            InlineKeyboardButton("9", callback_data="safe_pin_digit_9")
        ])
        
        # Четвертый ряд: 0 Очистить Подтвердить
        keyboard.append([
            InlineKeyboardButton("0", callback_data="safe_pin_digit_0"),
            InlineKeyboardButton("🗑 Очистить", callback_data="safe_pin_clear"),
            InlineKeyboardButton("✅ Подтвердить", callback_data="safe_pin_confirm")
        ])
        
        # Пятый ряд: Назад
        keyboard.append([
            InlineKeyboardButton("◀️ Назад", callback_data="safe_back")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Показываем текущий ввод
        new_pin = temp_pins.get(user_id, "")
        display_pin = "*" * len(new_pin) if new_pin else "____"
        
        await query.edit_message_text(
            f"🔐 Введите PIN-код для сейфа\n\n"
            f"Код: {display_pin}\n\n"
            f"Используйте кнопки ниже для ввода цифр",
            reply_markup=reply_markup
        )
        await query.answer()
        return True
    
    # Очистка PIN
    elif data == "safe_pin_clear":
        temp_pins[user_id] = ""
        
        # Обновляем сообщение с пустым PIN
        keyboard = []
        keyboard.append([InlineKeyboardButton("1", callback_data="safe_pin_digit_1"), InlineKeyboardButton("2", callback_data="safe_pin_digit_2"), InlineKeyboardButton("3", callback_data="safe_pin_digit_3")])
        keyboard.append([InlineKeyboardButton("4", callback_data="safe_pin_digit_4"), InlineKeyboardButton("5", callback_data="safe_pin_digit_5"), InlineKeyboardButton("6", callback_data="safe_pin_digit_6")])
        keyboard.append([InlineKeyboardButton("7", callback_data="safe_pin_digit_7"), InlineKeyboardButton("8", callback_data="safe_pin_digit_8"), InlineKeyboardButton("9", callback_data="safe_pin_digit_9")])
        keyboard.append([InlineKeyboardButton("0", callback_data="safe_pin_digit_0"), InlineKeyboardButton("🗑 Очистить", callback_data="safe_pin_clear"), InlineKeyboardButton("✅ Подтвердить", callback_data="safe_pin_confirm")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="safe_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"🔐 Введите PIN-код для сейфа\n\n"
            f"Код: ____\n\n"
            f"Используйте кнопки ниже для ввода цифр",
            reply_markup=reply_markup
        )
        await query.answer()
        return True
    
    # Подтверждение PIN
    elif data == "safe_pin_confirm":
        entered_pin = temp_pins.get(user_id, "")
        
        if len(entered_pin) != 4:
            await query.answer("❌ PIN-код должен состоять из 4 цифр!", show_alert=True)
            return True
        
        # Здесь проверяем PIN из базы данных
        with get_db() as conn:
            cursor = conn.execute(
                "SELECT safe_pin FROM users WHERE id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            correct_pin = row['safe_pin'] if row else None
        
        if correct_pin and entered_pin == correct_pin:
            # PIN правильный
            temp_pins.pop(user_id, None)
            await query.answer("✅ PIN-код принят!", show_alert=True)
            
            # Показываем меню сейфа
            from last14 import safe_command
            fake_update = type('obj', (object,), {'effective_user': query.from_user, 'message': query.message})
            await safe_command(fake_update, context)
            await query.message.delete()
        else:
            # PIN неправильный
            temp_pins[user_id] = ""
            await query.answer("❌ Неверный PIN-код! Попробуйте снова.", show_alert=True)
            
            # Сбрасываем ввод
            keyboard = []
            keyboard.append([InlineKeyboardButton("1", callback_data="safe_pin_digit_1"), InlineKeyboardButton("2", callback_data="safe_pin_digit_2"), InlineKeyboardButton("3", callback_data="safe_pin_digit_3")])
            keyboard.append([InlineKeyboardButton("4", callback_data="safe_pin_digit_4"), InlineKeyboardButton("5", callback_data="safe_pin_digit_5"), InlineKeyboardButton("6", callback_data="safe_pin_digit_6")])
            keyboard.append([InlineKeyboardButton("7", callback_data="safe_pin_digit_7"), InlineKeyboardButton("8", callback_data="safe_pin_digit_8"), InlineKeyboardButton("9", callback_data="safe_pin_digit_9")])
            keyboard.append([InlineKeyboardButton("0", callback_data="safe_pin_digit_0"), InlineKeyboardButton("🗑 Очистить", callback_data="safe_pin_clear"), InlineKeyboardButton("✅ Подтвердить", callback_data="safe_pin_confirm")])
            keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="safe_back")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"🔐 Введите PIN-код для сейфа\n\n"
                f"Код: ____\n\n"
                f"❌ Неверный PIN! Попробуйте снова.",
                reply_markup=reply_markup
            )
        
        return True
    
    # Просмотр сейфа
    elif data == "safe_view":
        await safe_view(query, update.effective_user)
        return True
    
    # Меню внесения
    elif data == "safe_deposit":
        await safe_deposit_menu(query, update.effective_user)
        return True
    
    # Меню снятия
    elif data == "safe_withdraw":
        await safe_withdraw_menu(query, update.effective_user)
        return True
    
    # Установка PIN
    elif data == "safe_set_pin":
        await safe_set_pin_start(query, update.effective_user)
        return True
    
    # Внесение суммы
    elif data.startswith("safe_deposit_amount_"):
        amount_str = data.replace("safe_deposit_amount_", "")
        await safe_deposit_amount(query, update.effective_user, amount_str)
        return True
    
    # Снятие суммы
    elif data.startswith("safe_withdraw_amount_"):
        amount_str = data.replace("safe_withdraw_amount_", "")
        await safe_withdraw_amount(query, update.effective_user, amount_str)
        return True
    
    elif data == "safe_deposit_custom":
        await safe_deposit_custom(query, update.effective_user, context)
        return True
    
    elif data == "safe_withdraw_custom":
        await safe_withdraw_custom(query, update.effective_user, context)
        return True
    
    # Назад в главное меню
    elif data == "safe_back":
        class FakeUpdate:
            def __init__(self, user, message):
                self.effective_user = user
                self.message = message
        
        fake_update = FakeUpdate(update.effective_user, query.message)
        await safe_command(fake_update, context)
        await query.message.delete()
        return True
    
    return False
