import logging
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from database import *

from .common import safe_answer, format_amount

ITEMS_PER_PAGE = 3

async def handle_investment_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str, user_id: int):
    """Обработчик для инвестиций (акции)"""
    query = update.callback_query
    
    if data == "view_stocks":
        if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user_id:
            await safe_answer(query, "🙈 Это не ваша кнопка.", show_alert=True)
            return
        
        portfolio = await get_user_portfolio_async(user_id)
        
        if not portfolio:
            await safe_answer(query, "У вас нет акций.🙁", show_alert=True)
            return
        
        context.user_data['portfolio'] = [dict(item) for item in portfolio]
        context.user_data['page'] = 0
        
        await show_portfolio_page(query, context, user_id)
    
    elif data.startswith("portfolio_page_"):
        action = data.replace("portfolio_page_", "")
        await handle_portfolio_pagination(query, context, user_id, action)
    
    elif data == "shop_stocks":
        stocks = await get_all_stocks_async()
        available_stocks = [s for s in stocks if s['current_price'] > 0]
        
        if not available_stocks:
            await safe_answer(query, "🛒 Сейчас нет доступных акций для покупки.", show_alert=True)
            return
        
        context.user_data['shop_stocks'] = [dict(s) for s in available_stocks]
        context.user_data['shop_page'] = 0
        
        await show_shop_page(query, context, user_id)
    
    elif data.startswith("shop_page_"):
        action = data.replace("shop_page_", "")
        await handle_shop_pagination(query, context, user_id, action)
    
    elif data.startswith("stock_info_"):
        stock_id = int(data.replace("stock_info_", ""))
        stock = await get_stock_async(stock_id)
        
        if not stock:
            await safe_answer(query, "❌ Акция не найдена", show_alert=True)
            return
        
        text = (
            f"ℹ️ Информация о акции {stock['name']}:\n\n"
            f"📊 Текущий курс — {stock['current_price']}ms¢\n\n"
            f"🆔 {stock['stock_id']}\n\n"
            f"— Для покупки введите buyact {stock['stock_id']} *кол-во*"
        )
        
        await query.edit_message_text(text)
        await safe_answer(query, "")
    
    elif data.startswith("confirm_sell_"):
        parts = data.split("_")
        if len(parts) == 4:
            _, _, stock_id, quantity = parts
            await confirm_sell(query, context, user_id, int(stock_id), int(quantity))
    
    elif data == "cancel_sell":
        await query.message.delete()
        await safe_answer(query, "❌ Продажа отменена")

async def show_portfolio_page(query, context, user_id):
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
    await safe_answer(query, "")

async def handle_portfolio_pagination(query, context, user_id, action):
    """Обработка пагинации портфеля"""
    page = context.user_data.get('page', 0)
    portfolio = context.user_data.get('portfolio', [])
    total_pages = (len(portfolio) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    
    if action == "next":
        if page >= total_pages - 1:
            await safe_answer(query, "Вы итак на последнем уровне.", show_alert=True)
            return
        context.user_data['page'] = page + 1
    elif action == "prev":
        if page <= 0:
            await safe_answer(query, "Вы итак на минимальном уровне.", show_alert=True)
            return
        context.user_data['page'] = page - 1
    
    await show_portfolio_page(query, context, user_id)

async def show_shop_page(query, context, user_id):
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
    await safe_answer(query, "")

async def handle_shop_pagination(query, context, user_id, action):
    """Обработка пагинации магазина"""
    page = context.user_data.get('shop_page', 0)
    stocks = context.user_data.get('shop_stocks', [])
    total_pages = (len(stocks) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    
    if action == "next":
        if page >= total_pages - 1:
            await safe_answer(query, "Это последняя страница", show_alert=True)
            return
        context.user_data['shop_page'] = page + 1
    elif action == "prev":
        if page <= 0:
            await safe_answer(query, "Это первая страница", show_alert=True)
            return
        context.user_data['shop_page'] = page - 1
    
    await show_shop_page(query, context, user_id)

async def confirm_sell(query, context, user_id, stock_id, quantity):
    """Подтверждение продажи"""
    
    # ✅ Безопасное преобразование чисел
    try:
        # Если пришли как строка с научной нотацией
        if isinstance(stock_id, str) and 'e' in stock_id:
            stock_id = int(float(stock_id))
        else:
            stock_id = int(stock_id)
            
        if isinstance(quantity, str) and 'e' in quantity:
            quantity = int(float(quantity))
        else:
            quantity = int(quantity)
    except (ValueError, TypeError) as e:
        logging.error(f"Error converting values: stock_id={stock_id}, quantity={quantity}, error={e}")
        await safe_answer(query, "❌ Ошибка: неверный формат данных", show_alert=True)
        return
    
    if query.message.reply_to_message and query.message.reply_to_message.from_user.id != user_id:
        await safe_answer(query, "🙈 Это не ваша кнопка.", show_alert=True)
        return

    await safe_answer(query, "⏳ Обрабатываем продажу...")

    try:
        stock = await get_stock_async(stock_id)
        if not stock:
            await query.message.reply_text("❌ Акция не найдена")
            return

        current_price = stock['current_price']

        success, result = await sell_stock_async(user_id, stock_id, quantity)

        if success:
            total = current_price * quantity
            formatted = format_amount(total)

            text = f"✅ Продано {quantity} {stock['symbol']} за {formatted}ms¢."

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
