"""
Модуль для работы с чеками
"""
import logging
import io
import os
import sqlite3
import asyncio
import aiosqlite
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import BadRequest

from .common import safe_answer, format_amount, get_user_async, update_balance_async

# Попытка импорта PIL
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
    logging.info("✅ PIL imported successfully")
except ImportError as e:
    PIL_AVAILABLE = False
    logging.warning(f"❌ PIL import failed: {e}")

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# ============ КЭШ ДЛЯ ФОТО ============
photo_cache = {}
personal_photo_cache = {}

# ============ ИНИЦИАЛИЗАЦИЯ БД ============

def init_checks_db():
    """Инициализация таблиц для чеков"""
    os.makedirs('data', exist_ok=True)

    with sqlite3.connect('data/bot.db') as conn:
        # Таблица чековых книжек
        conn.execute('''
            CREATE TABLE IF NOT EXISTS check_books (
                user_id INTEGER PRIMARY KEY,
                has_book INTEGER DEFAULT 0,
                purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')

        # Таблица для чеков
        conn.execute('''
            CREATE TABLE IF NOT EXISTS checks_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_number INTEGER NOT NULL,
                creator_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                max_activations INTEGER NOT NULL,
                used_count INTEGER DEFAULT 0,
                password TEXT DEFAULT NULL,
                comment TEXT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(creator_id, check_number)
            )
        ''')

        # Таблица активаций чеков
        conn.execute('''
            CREATE TABLE IF NOT EXISTS user_check_activations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(check_id, user_id)
            )
        ''')

        # Индексы
        conn.execute('CREATE INDEX IF NOT EXISTS idx_checks_new_creator ON checks_new(creator_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_checks_new_id ON checks_new(id)')

        conn.commit()

    logging.info("✅ Checks database initialized")


# ============ ФУНКЦИИ БД ============

async def has_check_book(user_id: int) -> bool:
    """Проверяет, есть ли у пользователя чековая книжка"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute(
            "SELECT has_book FROM check_books WHERE user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return result[0] == 1 if result else False


async def purchase_check_book(user_id: int) -> bool:
    """Покупка чековой книжки"""
    async with aiosqlite.connect('data/bot.db') as db:
        await db.execute('''
            INSERT OR REPLACE INTO check_books (user_id, has_book, purchased_at)
            VALUES (?, 1, CURRENT_TIMESTAMP)
        ''', (user_id,))
        await db.commit()
        return True


async def get_next_check_number(user_id: int) -> int:
    """Получить следующий номер чека для пользователя"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute(
            "SELECT MAX(check_number) FROM checks_new WHERE creator_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return (result[0] or 0) + 1


async def create_check_async(user_id: int, amount: int, max_activations: int, password: str = None, comment: str = None) -> tuple:
    """Создать новый чек"""
    try:
        async with aiosqlite.connect('data/bot.db') as db:
            check_number = await get_next_check_number(user_id)

            await db.execute('''
                INSERT INTO checks_new (check_number, creator_id, amount, max_activations, password, comment)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (check_number, user_id, amount, max_activations, password, comment))
            await db.commit()
            return check_number, True
    except Exception as e:
        logging.error(f"Error creating check: {e}")
        return None, False


async def get_check_by_number_async(check_number: int, creator_id: int):
    """Получить чек по номеру и создателю"""
    async with aiosqlite.connect('data/bot.db') as db:
        db.row_factory = sqlite3.Row
        cursor = await db.execute(
            "SELECT * FROM checks_new WHERE check_number = ? AND creator_id = ?",
            (check_number, creator_id)
        )
        result = await cursor.fetchone()
        return dict(result) if result else None


async def get_user_checks_async(user_id: int, limit: int = 6, offset: int = 0):
    """Получить все чеки пользователя с пагинацией"""
    async with aiosqlite.connect('data/bot.db') as db:
        db.row_factory = sqlite3.Row
        cursor = await db.execute('''
            SELECT * FROM checks_new
            WHERE creator_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        ''', (user_id, limit, offset))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_user_checks_count_async(user_id: int) -> int:
    """Получить количество чеков пользователя"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM checks_new WHERE creator_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return result[0] or 0


async def get_check_by_id_async(check_id: int):
    """Получить чек по ID"""
    async with aiosqlite.connect('data/bot.db') as db:
        db.row_factory = sqlite3.Row
        cursor = await db.execute(
            "SELECT * FROM checks_new WHERE id = ?",
            (check_id,)
        )
        result = await cursor.fetchone()
        return dict(result) if result else None


# ============ ФУНКЦИИ ДЛЯ ПЕРСОНАЛЬНЫХ ЧЕКОВ ============

async def get_next_personal_check_number(user_id: int) -> int:
    """Получить следующий номер персонального чека для пользователя"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute(
            "SELECT MAX(check_number) FROM personal_checks WHERE creator_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return (result[0] or 0) + 1


async def create_personal_check_async(user_id: int, target_user_id: int, target_username: str, amount: int, password: str = None, comment: str = None) -> tuple:
    """Создать персональный чек для конкретного пользователя"""
    try:
        async with aiosqlite.connect('data/bot.db') as db:
            check_number = await get_next_personal_check_number(user_id)

            await db.execute('''
                INSERT INTO personal_checks (check_number, creator_id, target_user_id, target_username, amount, password, comment)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (check_number, user_id, target_user_id, target_username, amount, password, comment))
            await db.commit()
            return check_number, True
    except Exception as e:
        logging.error(f"Error creating personal check: {e}")
        return None, False


async def get_personal_check_by_number_async(check_number: int, creator_id: int):
    """Получить персональный чек по номеру и создателю"""
    async with aiosqlite.connect('data/bot.db') as db:
        db.row_factory = sqlite3.Row
        cursor = await db.execute(
            "SELECT * FROM personal_checks WHERE check_number = ? AND creator_id = ?",
            (check_number, creator_id)
        )
        result = await cursor.fetchone()
        return dict(result) if result else None


async def get_personal_check_by_id_async(check_id: int):
    """Получить персональный чек по ID"""
    async with aiosqlite.connect('data/bot.db') as db:
        db.row_factory = sqlite3.Row
        cursor = await db.execute(
            "SELECT * FROM personal_checks WHERE id = ?",
            (check_id,)
        )
        result = await cursor.fetchone()
        return dict(result) if result else None


async def activate_personal_check_async(check_id: int, user_id: int, password: str = None) -> tuple:
    """Активация персонального чека"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute(
            "SELECT * FROM personal_checks WHERE id = ?",
            (check_id,)
        )
        check = await cursor.fetchone()

        if not check:
            return False, "not_found"

        if check[6] == 1:  # used
            return False, "already_used"

        if check[7] and check[7] != password:
            return False, "wrong_password"

        # Проверяем, тот ли пользователь активирует
        if check[3] != user_id:
            return False, "wrong_user"

        # Обновляем used
        await db.execute(
            "UPDATE personal_checks SET used = 1 WHERE id = ?",
            (check_id,)
        )

        await db.commit()
        return True, check[5]  # amount


async def activate_check_async(check_id: int, user_id: int, password: str = None) -> tuple:
    """Активация чека"""
    async with aiosqlite.connect('data/bot.db') as db:
        # Получаем чек
        cursor = await db.execute(
            "SELECT * FROM checks_new WHERE id = ?",
            (check_id,)
        )
        check = await cursor.fetchone()

        if not check:
            return False, "not_found"

        if check[5] >= check[4]:
            return False, "no_activations"
        if check[6] and check[6] != password:
            return False, "wrong_password"

        # Проверяем, не активировал ли уже пользователь
        cursor = await db.execute(
            "SELECT * FROM user_check_activations WHERE check_id = ? AND user_id = ?",
            (check_id, user_id)
        )
        if await cursor.fetchone():
            return False, "already_used"

        # Обновляем used_count
        await db.execute(
            "UPDATE checks_new SET used_count = used_count + 1 WHERE id = ?",
            (check_id,)
        )

        # Вставляем запись об активации
        await db.execute('''
            INSERT OR IGNORE INTO user_check_activations (check_id, user_id, activated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (check_id, user_id))

        await db.commit()
        return True, check[3]


# ============ ГЕНЕРАЦИЯ ФОТО С КЭШИРОВАНИЕМ ============

def generate_check_image_sync(creator_name: str, amount: str, activations_left: int):
    """Синхронная генерация изображения для чека"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(current_dir)

    template_path = os.path.join(project_dir, 'assets', 'check_template.png')
    font_path = os.path.join(project_dir, 'fonts', 'arial.ttf')

    if os.path.exists(template_path):
        img = Image.open(template_path)
        if img.size != (1296, 831):
            img = img.resize((1296, 831), Image.Resampling.LANCZOS)
    else:
        img = Image.new('RGB', (1296, 831), color='#0a0a1a')

    draw = ImageDraw.Draw(img)

    try:
        font_big = ImageFont.truetype(font_path, 90)
        font_small = ImageFont.truetype(font_path, 45)
    except:
        font_big = font_small = ImageFont.load_default()

    # Сумма
    amount_full = f"{amount} ms¢"
    draw.text((648, 410), amount_full, fill='#FFFFFF', font=font_big, anchor='mm')

    # Имя создателя
    if len(creator_name) > 17:
        creator_name = creator_name[:15] + ".."
    draw.text((215, 667), creator_name, fill='#FFFFFF', font=font_small, anchor='mm')

    # Активации
    draw.text((505, 667), str(activations_left), fill='#FF0000', font=font_small, anchor='mm')

    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=30, optimize=True)
    buf.seek(0)
    return buf

async def generate_check_image(creator_name: str, amount: str, activations_left: int, check_number: int):
    """Генерирует изображение для чека с кэшированием"""
    global photo_cache
    
    if not PIL_AVAILABLE:
        return None

    cache_key = f"{creator_name}_{amount}_{activations_left}"
    
    if cache_key in photo_cache:
        photo_cache[cache_key].seek(0)
        return photo_cache[cache_key]

    loop = asyncio.get_event_loop()
    buf = await loop.run_in_executor(None, generate_check_image_sync, creator_name, amount, activations_left)
    
    if buf:
        photo_cache[cache_key] = buf
        buf.seek(0)
    
    logging.info(f"✅ Image generated for check #{check_number}")
    return buf


def generate_personal_check_image_sync(creator_name: str, target_name: str, amount: str):
    """Синхронная генерация изображения для персонального чека"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(current_dir)

    template_path = os.path.join(project_dir, 'assets', 'check_template.png')
    font_path = os.path.join(project_dir, 'fonts', 'arial.ttf')

    if os.path.exists(template_path):
        img = Image.open(template_path)
        if img.size != (1296, 831):
            img = img.resize((1296, 831), Image.Resampling.LANCZOS)
    else:
        img = Image.new('RGB', (1296, 831), color='#0a0a1a')

    draw = ImageDraw.Draw(img)

    try:
        font_big = ImageFont.truetype(font_path, 90)
        font_small = ImageFont.truetype(font_path, 45)
    except:
        font_big = font_small = ImageFont.load_default()

    # Сумма
    amount_full = f"{amount} ms¢"
    draw.text((648, 410), amount_full, fill='#FFFFFF', font=font_big, anchor='mm')

    # Имя создателя
    if len(creator_name) > 17:
        creator_name = creator_name[:15] + ".."
    draw.text((215, 667), creator_name, fill='#FFFFFF', font=font_small, anchor='mm')

    # Для кого чек
    target_text = f"Для: {target_name}"
    if len(target_text) > 20:
        target_text = target_text[:18] + ".."
    draw.text((505, 667), target_text, fill='#FFD700', font=font_small, anchor='mm')

    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=30, optimize=True)
    buf.seek(0)
    return buf

async def generate_personal_check_image(creator_name: str, target_name: str, amount: str, check_number: int):
    """Генерирует изображение для персонального чека с кэшированием"""
    global personal_photo_cache
    
    if not PIL_AVAILABLE:
        return None

    cache_key = f"{creator_name}_{target_name}_{amount}"
    
    if cache_key in personal_photo_cache:
        personal_photo_cache[cache_key].seek(0)
        return personal_photo_cache[cache_key]

    loop = asyncio.get_event_loop()
    buf = await loop.run_in_executor(None, generate_personal_check_image_sync, creator_name, target_name, amount)
    
    if buf:
        personal_photo_cache[cache_key] = buf
        buf.seek(0)
    
    logging.info(f"✅ Personal check image generated for #{check_number}")
    return buf


# ============ ОСНОВНЫЕ ОБРАБОТЧИКИ ============

async def my_checks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Показать список чеков пользователя"""
    query = update.callback_query
    user = query.from_user

    checks = await get_user_checks_async(user.id, limit=6, offset=(page-1)*6)
    total_checks = await get_user_checks_count_async(user.id)
    total_pages = (total_checks + 5) // 6 if total_checks > 0 else 1

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
    row = []
    if page > 1:
        row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"my_checks_page_{page-1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"my_checks_page_{page+1}"))
    if row:
        keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    await query.message.edit_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def handle_check_activation(update: Update, context: ContextTypes.DEFAULT_TYPE, creator_id: int, check_number: int):
    """Обработка активации чека по ссылке"""
    user = update.effective_user

    check = await get_check_by_number_async(check_number, creator_id)

    if not check:
        await update.message.reply_text("❌ Чек не найден или недоступен.")
        return

    if check['used_count'] >= check['max_activations']:
        await update.message.reply_text("❌ Активации закончились! Чек недоступен.")
        return

    creator = await get_user_async(creator_id)
    creator_name = creator.get('full_name', 'Неизвестно') if creator else 'Неизвестно'
    amount = format_amount(check['amount'])
    remaining = check['max_activations'] - check['used_count']
    check_link = f"https://t.me/{context.bot.username}?start=chk_{creator_id}_{check_number}"

    # Генерация фото с кэшем (быстро!)
    photo = await generate_check_image(creator_name, amount, remaining, check_number)

    # Формируем текст
    text = f"💎 <a href='{check_link}'>Чек</a> на <b>{amount}</b>ms¢"

    if check.get('comment'):
        text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("💰 Получить", callback_data=f"activate_check_{check['id']}")
    ]])

    if photo:
        await update.message.reply_photo(photo=photo, caption=text, reply_markup=keyboard, parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode='HTML')


async def activate_check_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Активация чека по кнопке"""
    query = update.callback_query
    user = query.from_user

    try:
        await query.answer()
    except BadRequest:
        await update.effective_chat.send_message("⏰ Кнопка устарела. Перейдите по ссылке заново.")
        return

    try:
        check_id = int(query.data.split('_')[2])
    except:
        await query.answer("❌ Ошибка", show_alert=True)
        return

    check = await get_check_by_id_async(check_id)

    if not check:
        await query.answer("❌ Чек не найден", show_alert=True)
        return

    if check['used_count'] >= check['max_activations']:
        await query.answer("❌ Активации закончились!", show_alert=True)
        return

    # Если есть пароль
    if check.get('password'):
        context.user_data[f'pending_activation_{user.id}'] = {
            'check_id': check_id,
            'message_id': query.message.message_id,
            'chat_id': query.message.chat_id,
            'is_photo': query.message.photo is not None
        }

        amount = format_amount(check['amount'])
        check_link = f"https://t.me/{context.bot.username}?start=chk_{check['creator_id']}_{check['check_number']}"

        text = f"💎 <a href='{check_link}'>Чек</a> на <b>{amount}</b>ms¢"

        if check.get('comment'):
            text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"

        text += f"\n\n<b>🔐 Введите пароль:</b>"

        await query.edit_message_caption(
            caption=text,
            parse_mode='HTML',
            reply_markup=None
        )
        await query.answer()
        return

    # Активация без пароля
    success, result = await activate_check_async(check_id, user.id)

    if success:
        await update_balance_async(user.id, result)

        check = await get_check_by_id_async(check_id)
        remaining_activations = check['max_activations'] - check['used_count']

        # Уведомление создателю
        try:
            reward_formatted = format_amount(result)
            notify_text = (
                f"<i>✅ {user.first_name}, активировал(а) твой чек и получил(а) {reward_formatted}ms¢</i>\n\n"
                f"Осталось активаций: <b>{remaining_activations}</b>"
            )
            await context.bot.send_message(
                chat_id=check['creator_id'],
                text=notify_text,
                parse_mode='HTML'
            )
        except Exception as e:
            logging.error(f"Failed to notify: {e}")

        amount = format_amount(result)
        result_text = f"📥 <b>Ты получил {amount}msCoin!</b>"

        if check and check.get('comment'):
            result_text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"

        await query.edit_message_caption(
            caption=result_text,
            parse_mode='HTML'
        )
        await query.answer("✅ Чек активирован!")
    else:
        await query.answer("❌ Ошибка активации", show_alert=True)


async def activate_personal_check_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Активация персонального чека по кнопке"""
    query = update.callback_query
    user = query.from_user

    try:
        await query.answer()
    except BadRequest:
        await update.effective_chat.send_message("⏰ Кнопка устарела. Перейдите по ссылке заново.")
        return

    try:
        check_id = int(query.data.split('_')[3])
    except:
        await query.answer("❌ Ошибка", show_alert=True)
        return

    check = await get_personal_check_by_id_async(check_id)

    if not check:
        await query.answer("❌ Чек не найден", show_alert=True)
        return

    if check['used'] == 1:
        await query.answer("❌ Чек уже использован!", show_alert=True)
        return

    if check['target_user_id'] != user.id:
        await query.answer("❌ Этот чек создан не для вас!", show_alert=True)
        return

    # Если есть пароль
    if check.get('password'):
        context.user_data[f'pending_personal_activation_{user.id}'] = {
            'check_id': check_id,
            'message_id': query.message.message_id,
            'chat_id': query.message.chat_id,
            'is_photo': query.message.photo is not None
        }

        amount = format_amount(check['amount'])
        check_link = f"https://t.me/{context.bot.username}?start=pchk_{check['creator_id']}_{check['check_number']}"

        text = f"💎 <a href='{check_link}'>Чек</a> на <b>{amount}</b>ms¢ для <b>{user.first_name}</b>"

        if check.get('comment'):
            text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"
        text += f"\n\n<b>🔐 Введите пароль:</b>"

        await query.edit_message_caption(
            caption=text,
            parse_mode='HTML',
            reply_markup=None
        )
        await query.answer()
        return

    # Активация без пароля
    success, result = await activate_personal_check_async(check_id, user.id)

    if success:
        await update_balance_async(user.id, result)

        amount = format_amount(result)
        result_text = f"📥 <b>Ты получил {amount}msCoin!</b>"

        if check.get('comment'):
            result_text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"

        await query.edit_message_caption(
            caption=result_text,
            parse_mode='HTML'
        )
        await query.answer("✅ Чек активирован!")

        try:
            await context.bot.send_message(
                chat_id=check['creator_id'],
                text=f"<i>✅ {user.first_name} активировал(а) твой персональный чек и получил(а) {amount}ms¢</i>",
                parse_mode='HTML'
            )
        except:
            pass
    else:
        await query.answer("❌ Ошибка активации", show_alert=True)


async def handle_personal_check_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-кнопок для персональных чеков"""
    query = update.callback_query
    user = query.from_user
    data = query.data

    if data.startswith("set_personal_password_"):
        parts = data.split('_')
        creator_id = int(parts[3])
        check_number = int(parts[4])

        if creator_id != user.id:
            await query.answer("🙈 Это не ваш чек!", show_alert=True)
            return

        context.user_data['pending_personal_password_check'] = {
            'creator_id': creator_id,
            'check_number': check_number
        }

        await query.message.reply_text("🔐 Введите пароль для чека (макс. 128 символов):")
        await query.answer()
        return

    elif data.startswith("set_personal_comment_"):
        parts = data.split('_')
        creator_id = int(parts[3])
        check_number = int(parts[4])

        if creator_id != user.id:
            await query.answer("🙈 Это не ваш чек!", show_alert=True)
            return

        context.user_data['pending_personal_comment_check'] = {
            'creator_id': creator_id,
            'check_number': check_number
        }

        await query.message.reply_text(f"📝 Введите комментарий для чека #{check_number}:")
        await query.answer()
        return

    elif data.startswith("copy_personal_link_"):
        parts = data.split('_')
        creator_id = int(parts[3])
        check_number = int(parts[4])

        check_link = f"https://t.me/{context.bot.username}?start=pchk_{creator_id}_{check_number}"
        await query.message.reply_text(
            f"🔗 Ссылка на персональный чек #{check_number}:\n<code>{check_link}</code>",
            parse_mode='HTML'
        )
        await query.answer()
        return

    elif data.startswith("activate_personal_check_"):
        await activate_personal_check_callback(update, context)
        return


async def handle_personal_check_activation(update: Update, context: ContextTypes.DEFAULT_TYPE, creator_id: int, check_number: int):
    """Обработка активации персонального чека по ссылке"""
    user = update.effective_user

    check = await get_personal_check_by_number_async(check_number, creator_id)

    if not check:
        await update.message.reply_text("❌ Чек не найден или недоступен.")
        return

    if check['used'] == 1:
        await update.message.reply_text("❌ Чек уже использован!")
        return

    if check['target_user_id'] != user.id:
        await update.message.reply_text("❌ Этот чек создан не для вас!")
        return

    creator = await get_user_async(creator_id)
    creator_name = creator.get('full_name', 'Неизвестно') if creator else 'Неизвестно'
    target_name = user.first_name
    amount = format_amount(check['amount'])
    check_link = f"https://t.me/{context.bot.username}?start=pchk_{creator_id}_{check_number}"

    photo = await generate_personal_check_image(creator_name, target_name, amount, check_number)

    text = f"💎 <a href='{check_link}'>Чек</a> на <b>{amount}</b>ms¢ для <b>{target_name}</b>"

    if check.get('comment'):
        text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("💰 Получить", callback_data=f"activate_personal_check_{check['id']}")
    ]])

    if photo:
        await update.message.reply_photo(photo=photo, caption=text, reply_markup=keyboard, parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode='HTML')


async def handle_check_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстового ввода для пароля/комментария"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if update.message.chat.type != 'private':
        context.user_data.pop('pending_password_check', None)
        context.user_data.pop('pending_comment_check', None)
        context.user_data.pop('pending_personal_password_check', None)
        context.user_data.pop('pending_personal_comment_check', None)
        context.user_data.pop(f'pending_activation_{user_id}', None)
        context.user_data.pop(f'pending_personal_activation_{user_id}', None)
        return

    if text.startswith('/'):
        return

    # Установка пароля (обычный чек)
    if 'pending_password_check' in context.user_data:
        data = context.user_data.pop('pending_password_check')

        if not text:
            await update.message.reply_text("❌ Пароль не может быть пустым!")
            return

        if len(text) > 128:
            await update.message.reply_text("❌ Пароль слишком длинный (макс. 128 символов)")
            return

        async with aiosqlite.connect('data/bot.db') as db:
            await db.execute(
                "UPDATE checks_new SET password = ? WHERE creator_id = ? AND check_number = ?",
                (text, data['creator_id'], data['check_number'])
            )
            await db.commit()

        await update.message.delete()
        await update_check_message(update, context, data['creator_id'], data['check_number'])
        return

    # Установка комментария (обычный чек)
    if 'pending_comment_check' in context.user_data:
        data = context.user_data.pop('pending_comment_check')

        if len(text) > 500:
            await update.message.reply_text("❌ Комментарий слишком длинный (макс. 500 символов)")
            return

        async with aiosqlite.connect('data/bot.db') as db:
            await db.execute(
                "UPDATE checks_new SET comment = ? WHERE creator_id = ? AND check_number = ?",
                (text, data['creator_id'], data['check_number'])
            )
            await db.commit()

        await update.message.reply_text(f"✅ Комментарий добавлен!")
        await update_check_message(update, context, data['creator_id'], data['check_number'])
        return

    # Установка пароля (персональный чек)
    if 'pending_personal_password_check' in context.user_data:
        data = context.user_data.pop('pending_personal_password_check')

        if not text:
            await update.message.reply_text("❌ Пароль не может быть пустым!")
            return

        if len(text) > 128:
            await update.message.reply_text("❌ Пароль слишком длинный (макс. 128 символов)")
            return

        async with aiosqlite.connect('data/bot.db') as db:
            await db.execute(
                "UPDATE personal_checks SET password = ? WHERE creator_id = ? AND check_number = ?",
                (text, data['creator_id'], data['check_number'])
            )
            await db.commit()

        await update.message.delete()
        await update_personal_check_message(update, context, data['creator_id'], data['check_number'])
        return

    # Установка комментария (персональный чек)
    if 'pending_personal_comment_check' in context.user_data:
        data = context.user_data.pop('pending_personal_comment_check')

        if len(text) > 500:
            await update.message.reply_text("❌ Комментарий слишком длинный (макс. 500 символов)")
            return

        async with aiosqlite.connect('data/bot.db') as db:
            await db.execute(
                "UPDATE personal_checks SET comment = ? WHERE creator_id = ? AND check_number = ?",
                (text, data['creator_id'], data['check_number'])
            )
            await db.commit()

        await update.message.reply_text(f"✅ Комментарий добавлен!")
        await update_personal_check_message(update, context, data['creator_id'], data['check_number'])
        return

    # Активация обычного чека с паролем
    pending_key = f'pending_activation_{user_id}'
    if pending_key in context.user_data:
        pending_data = context.user_data.pop(pending_key)
        check_id = pending_data['check_id']
        check = await get_check_by_id_async(check_id)

        if not check:
            await update.message.reply_text("❌ Чек не найден")
            await update.message.delete()
            return

        if check.get('password') != text:
            await update.message.reply_text("❌ Неверный пароль!")
            await update.message.delete()
            return

        if check['used_count'] >= check['max_activations']:
            await update.message.reply_text("❌ Активации закончились!")
            await update.message.delete()
            return

        success, result = await activate_check_async(check_id, user_id, text)
        await update.message.delete()

        if success:
            await update_balance_async(user_id, result)
            check = await get_check_by_id_async(check_id)
            remaining_activations = check['max_activations'] - check['used_count']

            try:
                reward_formatted = format_amount(result)
                notify_text = (
                    f"<i>✅ {user.first_name}, активировал(а) твой чек и получил(а) {reward_formatted}ms¢</i>\n\n"
                    f"Осталось активаций: <b>{remaining_activations}</b>"
                )
                await context.bot.send_message(
                    chat_id=check['creator_id'],
                    text=notify_text,
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"Failed to notify: {e}")

            amount = format_amount(result)
            result_text = f"📥 <b>Ты получил {amount}msCoin!</b>"

            if check and check.get('comment'):
                result_text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"

            try:
                await context.bot.edit_message_caption(
                    chat_id=pending_data['chat_id'],
                    message_id=pending_data['message_id'],
                    caption=result_text,
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"Failed to edit message: {e}")
                await update.message.reply_text(f"✅ Успех! Ты получил {amount}msCoin!", parse_mode='HTML')
        else:
            await update.message.reply_text("❌ Ошибка активации!")
        return

    # Активация персонального чека с паролем
    pending_personal_key = f'pending_personal_activation_{user_id}'
    if pending_personal_key in context.user_data:
        pending_data = context.user_data.pop(pending_personal_key)
        check_id = pending_data['check_id']
        check = await get_personal_check_by_id_async(check_id)

        if not check:
            await update.message.reply_text("❌ Чек не найден")
            await update.message.delete()
            return

        if check.get('password') != text:
            await update.message.reply_text("❌ Неверный пароль!")
            await update.message.delete()
            return

        if check['used'] == 1:
            await update.message.reply_text("❌ Чек уже использован!")
            await update.message.delete()
            return

        if check['target_user_id'] != user_id:
            await update.message.reply_text("❌ Этот чек создан не для вас!")
            await update.message.delete()
            return

        success, result = await activate_personal_check_async(check_id, user_id, text)
        await update.message.delete()

        if success:
            await update_balance_async(user_id, result)

            amount = format_amount(result)
            result_text = f"📥 <b>Ты получил {amount}msCoin!</b>"

            if check.get('comment'):
                result_text += f"\n\n<blockquote>💬 {check['comment']}</blockquote>"

            try:
                await context.bot.edit_message_caption(
                    chat_id=pending_data['chat_id'],
                    message_id=pending_data['message_id'],
                    caption=result_text,
                    parse_mode='HTML'
                )
            except Exception as e:
                logging.error(f"Failed to edit message: {e}")
                await update.message.reply_text(f"✅ Успех! Ты получил {amount}msCoin!")

            try:
                await context.bot.send_message(
                    chat_id=check['creator_id'],
                    text=f"<i>✅ {user.first_name} активировал(а) твой персональный чек и получил(а) {amount}ms¢</i>",
                    parse_mode='HTML'
                )
            except:
                pass
        else:
            await update.message.reply_text("❌ Ошибка активации!")
        return


async def update_personal_check_message(update: Update, context: ContextTypes.DEFAULT_TYPE, creator_id: int, check_number: int):
    """Обновляет сообщение с персональным чеком после установки пароля/комментария"""
    check = await get_personal_check_by_number_async(check_number, creator_id)
    if not check:
        return

    amount = format_amount(check['amount'])
    check_link = f"https://t.me/{context.bot.username}?start=pchk_{creator_id}_{check_number}"

    target_name = check.get('target_username') or f"ID{check['target_user_id']}"
    try:
        target_chat = await context.bot.get_chat(check['target_user_id'])
        target_name = target_chat.first_name or target_name
    except:
        pass

    warning = ""
    if check['password']:
        warning = "\n\n<blockquote>⚠️ Персональный чек защищен паролем! Не сообщайте пароль посторонним.</blockquote>"

    comment_text = ""
    if check.get('comment'):
        comment_text = f"\n\n💬 <i>Комментарий:</i> {check['comment']}"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 Копировать ссылку", callback_data=f"copy_personal_link_{creator_id}_{check_number}"),
            InlineKeyboardButton("🔑 Установить пароль", callback_data=f"set_personal_password_{creator_id}_{check_number}")
        ],
        [
            InlineKeyboardButton("💭 Комментарий", callback_data=f"set_personal_comment_{creator_id}_{check_number}")
        ]
    ])

    await update.message.reply_text(
        f"✅<b> ПЕРСОНАЛЬНЫЙ ЧЕК #{check_number} СОЗДАН!</b>\n"
        f"•••••••••••\n"
        f"💎 Чек на <b>{amount}</b>ms¢ для <b>{target_name}</b>\n\n"
        f"🔗 Скопируй ссылку, чтобы отправить чек:\n"
        f"<code>{check_link}</code>\n\n"
        f"<blockquote>⚠️ Этот чек может активировать только {target_name}!</blockquote>{warning}{comment_text}",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


async def update_check_message(update: Update, context: ContextTypes.DEFAULT_TYPE, creator_id: int, check_number: int):
    """Обновляет сообщение с чеком после установки пароля/комментария"""
    check = await get_check_by_number_async(check_number, creator_id)
    if not check:
        return

    amount = format_amount(check['amount'])
    check_link = f"https://t.me/{context.bot.username}?start=chk_{creator_id}_{check_number}"

    warning = ""
    if check['password']:
        warning = "\n\n<blockquote>⚠️ Чек может активировать любой пользователь! Не доверяйте никому пароль от данного чека и не пересылайте пароль в иные чаты.</blockquote>"

    comment_text = ""
    if check.get('comment'):
        comment_text = f"\n\n💬 <i>Комментарий:</i> {check['comment']}"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 Копировать ссылку", callback_data=f"copy_link_{creator_id}_{check_number}"),
            InlineKeyboardButton("🔑 Установить пароль", callback_data=f"set_password_{creator_id}_{check_number}")
        ],
        [
            InlineKeyboardButton("💭 Комментарий", callback_data=f"set_comment_{creator_id}_{check_number}")
        ]
    ])

    await update.message.reply_text(
        f"✅<b> ЧЕК #{check_number} СОЗДАН!</b>\n"
        f"•••••••••••\n"
        f"🔗 Скопируй ссылку, чтобы поделиться чеком:\n"
        f"<tg-spoiler><code>{check_link}</code></tg-spoiler>\n\n"
        f"👤 Пользователь, перешедший по ссылке, получит <b>{amount}</b>msCoin{warning}{comment_text}",
        reply_markup=keyboard,
        parse_mode='HTML'
    )


async def handle_check_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-кнопок для чеков"""
    query = update.callback_query
    user = query.from_user
    data = query.data

    if data == "my_checks":
        await my_checks_callback(update, context, 1)
        return

    if data.startswith("my_checks_page_"):
        page = int(data.split("_")[3])
        await my_checks_callback(update, context, page)
        return

    if data.startswith('buy_check_book_'):
        parts = data.split('_')
        amount = int(parts[3])
        activations = int(parts[4])
        total_cost = int(parts[5]) if len(parts) > 5 else amount * activations

        db_user = await get_user_async(user.id, user.full_name, user.username)

        if db_user.get('balance', 0) < 1_000_000:
            await query.answer("🔴 У тебя недостаточно msCoin!", show_alert=True)
            return

        await update_balance_async(user.id, -1_000_000)
        await purchase_check_book(user.id)

        await query.answer("🟢 Вы успешно приобрели чековую книжку!", show_alert=True)

        await update_balance_async(user.id, -total_cost)

        check_number, success = await create_check_async(user.id, amount, activations)

        if not success:
            await update_balance_async(user.id, total_cost)
            await query.message.reply_text("❌ Ошибка при создании чека!")
            return

        check_link = f"https://t.me/{context.bot.username}?start=chk_{user.id}_{check_number}"
        formatted_amount = format_amount(amount)

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📋 Копировать ссылку", callback_data=f"copy_link_{user.id}_{check_number}"),
                InlineKeyboardButton("🔑 Установить пароль", callback_data=f"set_password_{user.id}_{check_number}")
            ],
            [
                InlineKeyboardButton("💭 Комментарий", callback_data=f"set_comment_{user.id}_{check_number}")
            ]
        ])

        await query.message.edit_text(
            f"✅<b> ЧЕК #{check_number} СОЗДАН!</b>\n"
            f"•••••••••••\n"
            f"🔗 Скопируй ссылку, чтобы поделиться чеком:\n"
            f"<tg-spoiler><code>{check_link}</code></tg-spoiler>\n\n"
            f"👤 Пользователь, перешедший по ссылке, получит <b>{formatted_amount}</b>msCoin\n\n"
            f"<blockquote>⚠️ Чек может активировать любой пользователь! Не пересылайте чек в иные чаты.</blockquote>",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        return

    if data.startswith("set_password_"):
        parts = data.split('_')
        if len(parts) >= 4:
            creator_id = int(parts[2])
            check_number = int(parts[3])
        else:
            await query.answer("❌ Ошибка формата", show_alert=True)
            return

        if creator_id != user.id:
            await query.answer("🙈 Это не ваш чек!", show_alert=True)
            return

        context.user_data['pending_password_check'] = {
            'creator_id': creator_id,
            'check_number': check_number
        }

        await query.message.reply_text(
            "🔐 <u>Введите пароль для активации чека</u>\n🔻 Макс. 128 символов:",
            parse_mode='HTML'
        )
        await query.answer()
        return

    if data.startswith("set_comment_"):
        parts = data.split('_')
        if len(parts) >= 4:
            creator_id = int(parts[2])
            check_number = int(parts[3])
        else:
            await query.answer("❌ Ошибка формата", show_alert=True)
            return

        if creator_id != user.id:
            await query.answer("🙈 Это не ваш чек!", show_alert=True)
            return

        context.user_data['pending_comment_check'] = {
            'creator_id': creator_id,
            'check_number': check_number
        }

        await query.message.reply_text(
            f"📝 {user.full_name}, введите комментарий для текущего чека (#{check_number})"
        )
        await query.answer()
        return

    if data.startswith("copy_link_"):
        parts = data.split('_')
        if len(parts) >= 4:
            creator_id = int(parts[2])
            check_number = int(parts[3])
        else:
            await query.answer("❌ Ошибка формата", show_alert=True)
            return

        check_link = f"https://t.me/{context.bot.username}?start=chk_{creator_id}_{check_number}"

        await query.message.reply_text(
            f"🔗 <b>Ссылка на чек #{check_number}:</b>\n<code>{check_link}</code>\n\n<i>Нажмите на код, чтобы скопировать</i>",
            parse_mode='HTML'
        )
        await query.answer("✅ Ссылка отправлена в чат!")
        return

    if data.startswith("activate_check_"):
        await activate_check_callback(update, context)
        return
