import os
import sqlite3
import json
import aiosqlite
import time
import random
import secrets
import logging
import time
import threading
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any
from contextlib import contextmanager

DATABASE_PATH = os.getenv('DATABASE_PATH', 'data/bot.db')
_db_connections = {}

def get_db():
    thread_id = threading.get_ident()
    if thread_id not in _db_connections:
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _db_connections[thread_id] = conn
    return _db_connections[thread_id]

from contextlib import contextmanager

@contextmanager
def transaction(conn):
    """Контекстный менеджер для безопасных транзакций"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.commit()
    except Exception as e:
        conn.execute("ROLLBACK")
        raise e

def table_exists(table_name):
    """Проверка существования таблицы"""
    with get_db() as conn:
        result = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        ).fetchone()
        return result is not None

def parse_datetime(date_str):
    """Универсальный парсинг дат из БД"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            return None

def init_db():
    with get_db() as conn:
        cursor = conn.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in cursor.fetchall()]

        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                balance INTEGER DEFAULT 1000,
                msg_balance INTEGER DEFAULT 0,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        if 'total_win' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN total_win INTEGER DEFAULT 0')
        if 'total_loss' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN total_loss INTEGER DEFAULT 0')
        if 'games_played' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN games_played INTEGER DEFAULT 0')
        if 'referred_by' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN referred_by INTEGER DEFAULT NULL')
        if 'last_bonus' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN last_bonus TIMESTAMP DEFAULT NULL')
        if 'last_slot' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN last_slot TIMESTAMP DEFAULT NULL')
        if 'transfer_confirmation' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN transfer_confirmation INTEGER DEFAULT 1')  # 1 = включено, 0 = выключено
        if 'transfer_commission' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN transfer_commission INTEGER DEFAULT 1')   # 1 = включена, 0 = выключена (только для VIP)
        if 'vip_status' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN vip_status INTEGER DEFAULT 0')            # 1 = VIP, 0 = нет
        if 'vip_until' not in columns:
            conn.execute('ALTER TABLE users ADD COLUMN vip_until TIMESTAMP DEFAULT NULL')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS bans (
                user_id INTEGER PRIMARY KEY,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                banned_until TIMESTAMP,
                ban_days INTEGER,
                ban_reason TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS donate_features (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                feature_type TEXT,
                expires_at TIMESTAMP,
                granted_by INTEGER
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS safes (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                pin_code TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS games_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                game_type TEXT,
                bet INTEGER,
                win_amount INTEGER DEFAULT 0,
                doors_count INTEGER DEFAULT 0,
                opened_levels INTEGER DEFAULT 0,
                multiplier REAL DEFAULT 0,
                result TEXT,
                game_hash TEXT UNIQUE,
                played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE SET NULL
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referral_id INTEGER UNIQUE,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (referral_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_code TEXT UNIQUE,
                max_activations INTEGER,
                used_count INTEGER DEFAULT 0,
                amount INTEGER,
                claimed_by TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')


        conn.execute('''
            CREATE TABLE IF NOT EXISTS personal_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_number INTEGER NOT NULL,
                creator_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                target_username TEXT,
                amount INTEGER NOT NULL,
                used INTEGER DEFAULT 0,
                password TEXT DEFAULT NULL,
                comment TEXT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(creator_id, check_number)
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                max_activations INTEGER NOT NULL,
                used_count INTEGER DEFAULT 0,
                reward_amount INTEGER NOT NULL,
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS promo_activations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                promo_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (promo_id) REFERENCES promocodes(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(promo_id, user_id)
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS user_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_code TEXT UNIQUE NOT NULL,
                creator_id INTEGER NOT NULL,
                check_number INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                max_activations INTEGER NOT NULL,
                used_count INTEGER DEFAULT 0,
                total_amount INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (creator_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(creator_id, check_number)
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS user_check_activations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                activated_at TEXT NOT NULL,
                FOREIGN KEY (check_id) REFERENCES user_checks(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(check_id, user_id)
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS promotion_tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                creator_id INTEGER NOT NULL,
                task_type TEXT NOT NULL,  -- 'channel', 'chat', 'group'
                title TEXT,
                link TEXT NOT NULL,
                price_per_user INTEGER NOT NULL,  -- цена за 1 подписчика в MSG
                max_users INTEGER NOT NULL,  -- максимальное количество подписчиков
                current_users INTEGER DEFAULT 0,  -- текущее количество
                total_cost INTEGER NOT NULL,  -- общая стоимость (price_per_user * max_users)
                status TEXT DEFAULT 'active',  -- 'active', 'completed', 'banned'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (creator_id) REFERENCES users (user_id)
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS completed_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reward INTEGER NOT NULL,
                UNIQUE(task_id, user_id),
                FOREIGN KEY (task_id) REFERENCES promotion_tasks (task_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS check_books (
                user_id INTEGER PRIMARY KEY,
                has_book INTEGER DEFAULT 0,
                purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')

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

        conn.execute('''
            CREATE TABLE IF NOT EXISTS task_reports (
                report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT DEFAULT 'pending',  -- 'pending', 'resolved', 'rejected'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES promotion_tasks (task_id),
                FOREIGN KEY (reporter_id) REFERENCES users (user_id)
            )
        ''')
        conn.commit()


        conn.execute('CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_history_user ON games_history(user_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_history_played ON games_history(played_at)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_history_hash ON games_history(game_hash)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_bans_until ON bans(banned_until)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_referrals_referral ON referrals(referral_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_checks_new_creator ON checks_new(creator_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_checks_new_id ON checks_new(id)')

        conn.execute('CREATE INDEX IF NOT EXISTS idx_promocodes_code ON promocodes(code)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_promo_activations_user ON promo_activations(user_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_promo_activations_promo ON promo_activations(promo_id)')

def execute_query(query, params=(), fetchone=False, fetchall=False):
    try:
        with get_db() as conn:
            cursor = conn.execute(query, params)
            if fetchone:
                return cursor.fetchone()
            if fetchall:
                return cursor.fetchall()
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"SQL Error: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        return None if (fetchone or fetchall) else 0

async def execute_query_async(query, params=(), fetchone=False, fetchall=False):
    return await asyncio.to_thread(execute_query, query, params, fetchone, fetchall)

def get_user(user_id, full_name=None, username=None):
    with get_db() as conn:
        user = conn.execute(
            'SELECT * FROM users WHERE user_id = ?',
            (user_id,)
        ).fetchone()
        
        if not user:
            conn.execute(
                'INSERT INTO users (user_id, username, full_name, balance, msg_balance,  total_win, total_loss, games_played, referred_by, last_bonus) VALUES (?, ?, ?, 1000, 0, 0,  0, 0, NULL, NULL)',
                (user_id, username, full_name)
            )
            conn.commit()
            return {
                'user_id': user_id,
                'username': username,
                'full_name': full_name,
                'balance': 1000,
                'msg_balance': 0,
                'total_win': 0,
                'total_loss': 0,
                'games_played': 0,
                'referred_by': None,
                'last_bonus': None
            }
        
        if full_name is not None or username is not None:
            updates = []
            params = []
    
            if full_name is not None:
                updates.append("full_name = ?")
                params.append(full_name)
    
            if username is not None:
                updates.append("username = ?")
                params.append(username)
    
            if updates:
                query = f"UPDATE users SET {', '.join(updates)}, last_active = CURRENT_TIMESTAMP WHERE user_id = ?"
                params.append(user_id)
                conn.execute(query, params)
                conn.commit()

        return dict(user)

async def get_user_async(user_id, full_name=None, username=None):
    return await asyncio.to_thread(get_user, user_id, full_name, username)

def get_all_users():
    with get_db() as conn:
        return conn.execute('SELECT user_id FROM users').fetchall()

async def get_all_users_async():
    return await asyncio.to_thread(get_all_users)

def get_balance(user_id):
    """Получить баланс пользователя (синхронно)"""
    with get_db() as conn:
        result = conn.execute(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,)
        ).fetchone()
        return result[0] if result else 0

async def get_balance_async(user_id):
    """Получить баланс пользователя (асинхронно)"""
    return await asyncio.to_thread(get_balance, user_id)

def update_balance(user_id, amount):
    with get_db() as conn:
        # Проверяем, что баланс не станет отрицательным
        if amount < 0:
            current = conn.execute(
                'SELECT balance FROM users WHERE user_id = ?',
                (user_id,)
            ).fetchone()
            if not current or current[0] + amount < 0:
                return False

        conn.execute(
            'UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
            (amount, user_id)
        )
        conn.commit()
        return True

async def update_balance_async(user_id, amount):
    return await asyncio.to_thread(update_balance, user_id, amount)

def update_balance_safe(user_id, amount, required_balance=None):
    with get_db() as conn:
        if required_balance is not None:
            # При списании нужна проверка, что баланс >= сумме списания
            if amount < 0:
                result = conn.execute(
                    'UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ? AND balance >= ?',
                    (amount, user_id, -amount)  # required_balance игнорируем при списании
                )
            else:
                result = conn.execute(
                    'UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ? AND balance >= ?',
                    (amount, user_id, required_balance)
                )
            affected = result.rowcount
            conn.commit()
            return affected > 0
        else:
            conn.execute(
                'UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
                (amount, user_id)
            )
            conn.commit()
            return True

async def update_balance_safe_async(user_id, amount, required_balance=None):
    return await asyncio.to_thread(update_balance_safe, user_id, amount, required_balance)

async def transfer_msg_async(from_id: int, to_id: int, amount: int, commission_rate: float = 0.10, skip_commission: bool = False) -> tuple[bool, str]:
    """Перевод MSG между пользователями с комиссией"""
    def _transfer():
        with get_db() as conn:
            # Проверяем баланс отправителя
            sender = conn.execute(
                "SELECT msg_balance FROM users WHERE user_id = ?",
                (from_id,)
            ).fetchone()

            if not sender or sender[0] < amount:
                return False, "Недостаточно MSG"

            # Рассчитываем комиссию
            if skip_commission:
                commission = 0
                recipient_amount = amount
            else:
                commission = int(amount * commission_rate)
                recipient_amount = amount - commission

            # Списываем у отправителя
            conn.execute(
                "UPDATE users SET msg_balance = msg_balance - ? WHERE user_id = ?",
                (amount, from_id)
            )

            # Начисляем получателю (за вычетом комиссии)
            conn.execute(
                "UPDATE users SET msg_balance = msg_balance + ? WHERE user_id = ?",
                (recipient_amount, to_id)
            )

            conn.commit()
            return True, recipient_amount
    return await asyncio.to_thread(_transfer)


def transfer_money(from_id, to_id, amount, commission_rate: float = 0.10, skip_commission: bool = False):
    with get_db() as conn:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            # Проверяем существование отправителя
            from_user = conn.execute(
                'SELECT balance FROM users WHERE user_id = ?',
                (from_id,)
            ).fetchone()

            if not from_user:
                conn.execute("ROLLBACK")
                return False, "Отправитель не найден"

            # Проверяем существование получателя
            to_user = conn.execute(
                'SELECT user_id FROM users WHERE user_id = ?',
                (to_id,)
            ).fetchone()

            if not to_user:
                conn.execute("ROLLBACK")
                return False, "Получатель не найден"

            # Рассчитываем комиссию
            if skip_commission:
                commission = 0
                recipient_amount = amount
            else:
                commission = int(amount * commission_rate)
                recipient_amount = amount - commission

            if from_user[0] < amount:
                conn.execute("ROLLBACK")
                return False, "Недостаточно средств"

            # Списываем полную сумму
            conn.execute(
                'UPDATE users SET balance = balance - ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
                (amount, from_id)
            )

            # Начисляем получателю сумму после вычета комиссии
            conn.execute(
                'UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
                (recipient_amount, to_id)
            )

            conn.execute("COMMIT")
            return True, recipient_amount

        except Exception as e:
            conn.execute("ROLLBACK")
            return False, str(e)


async def transfer_money_async(from_id, to_id, amount, commission_rate: float = 0.10, skip_commission: bool = False):
    return await asyncio.to_thread(transfer_money, from_id, to_id, amount, commission_rate, skip_commission)

def update_user_stats(user_id, win_amount, loss_amount):
    with get_db() as conn:
        conn.execute('''
            UPDATE users 
            SET total_win = total_win + ?, 
                total_loss = total_loss + ?,
                games_played = games_played + 1,
                last_active = CURRENT_TIMESTAMP 
            WHERE user_id = ?
        ''', (win_amount, loss_amount, user_id))
        conn.commit()

async def update_user_stats_async(user_id, win_amount, loss_amount):
    return await asyncio.to_thread(update_user_stats, user_id, win_amount, loss_amount)

def get_user_stats(user_id):
    with get_db() as conn:
        result = conn.execute(
            'SELECT total_win, total_loss, games_played FROM users WHERE user_id = ?',
            (user_id,)
        ).fetchone()
        
        if result:
            return {
                'total_win': result[0] or 0,
                'total_loss': result[1] or 0,
                'games_played': result[2] or 0
            }
        return {'total_win': 0, 'total_loss': 0, 'games_played': 0}

async def get_user_stats_async(user_id):
    return await asyncio.to_thread(get_user_stats, user_id)

def get_top_users(limit=10):
    with get_db() as conn:
        return conn.execute(
            'SELECT user_id, COALESCE(full_name, username, "Пользователь") as name, balance FROM users ORDER BY balance DESC LIMIT ?',
            (limit,)
        ).fetchall()

async def get_top_users_async(limit=10):
    return await asyncio.to_thread(get_top_users, limit)

def get_user_rank(user_id):
    with get_db() as conn:
        result = conn.execute('''
            SELECT COUNT(*) + 1 FROM users WHERE balance > (SELECT balance FROM users WHERE user_id = ?)
        ''', (user_id,)).fetchone()
        return result[0] if result else 1

async def get_user_rank_async(user_id):
    return await asyncio.to_thread(get_user_rank, user_id)

def save_game_hash(game_hash, user_id, game_type, bet, result):
    with get_db() as conn:
        conn.execute('''
            INSERT INTO games_history (game_hash, user_id, game_type, bet, result)
            VALUES (?, ?, ?, ?, ?)
        ''', (game_hash, user_id, game_type, bet, result))
        conn.commit()

async def save_game_hash_async(game_hash, user_id, game_type, bet, result):
    return await asyncio.to_thread(save_game_hash, game_hash, user_id, game_type, bet, result)

def get_game_hash(game_hash):
    with get_db() as conn:
        result = conn.execute('''
            SELECT game_hash, user_id, game_type, bet, result FROM games_history WHERE game_hash = ?
        ''', (game_hash,)).fetchone()
        
        if result:
            return {
                'game_hash': result[0],
                'user_id': result[1],
                'game_type': result[2],
                'bet': result[3],
                'result': result[4]
            }
        return None

async def get_game_hash_async(game_hash):
    return await asyncio.to_thread(get_game_hash, game_hash)

def ban_user(user_id, days, reason):
    banned_until = None
    if days > 0:
        banned_until = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S.%f')
    
    with get_db() as conn:
        conn.execute('''
            INSERT OR REPLACE INTO bans (user_id, banned_until, ban_days, ban_reason)
            VALUES (?, ?, ?, ?)
        ''', (user_id, banned_until, days, reason))
        conn.commit()

async def ban_user_async(user_id, days, reason):
    return await asyncio.to_thread(ban_user, user_id, days, reason)

def unban_user(user_id):
    with get_db() as conn:
        conn.execute('DELETE FROM bans WHERE user_id = ?', (user_id,))
        conn.commit()
    return True

async def unban_user_async(user_id):
    return await asyncio.to_thread(unban_user, user_id)

def is_user_banned(user_id):
    with get_db() as conn:
        ban = conn.execute('''
            SELECT banned_until, ban_days, ban_reason FROM bans WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if ban:
            return {
                'banned_until': ban[0],
                'ban_days': ban[1],
                'ban_reason': ban[2]
            }
        return None

async def is_user_banned_async(user_id):
    return await asyncio.to_thread(is_user_banned, user_id)

def add_referral(referrer_id, referral_id):
    with get_db() as conn:
        conn.execute('''
            INSERT OR IGNORE INTO referrals (referrer_id, referral_id)
            VALUES (?, ?)
        ''', (referrer_id, referral_id))
        
        conn.execute('''
            UPDATE users SET referred_by = ? WHERE user_id = ?
        ''', (referrer_id, referral_id))
        
        conn.commit()

async def add_referral_async(referrer_id, referral_id):
    return await asyncio.to_thread(add_referral, referrer_id, referral_id)

def get_referrer_id(referral_id):
    with get_db() as conn:
        result = conn.execute('''
            SELECT referred_by FROM users WHERE user_id = ?
        ''', (referral_id,)).fetchone()
        return result[0] if result else None

async def get_referrer_id_async(referral_id):
    return await asyncio.to_thread(get_referrer_id, referral_id)

def get_user_referral_count(user_id):
    with get_db() as conn:
        result = conn.execute('''
            SELECT COUNT(*) FROM referrals WHERE referrer_id = ?
        ''', (user_id,)).fetchone()
        return result[0] if result else 0

async def get_user_referral_count_async(user_id):
    return await asyncio.to_thread(get_user_referral_count, user_id)

def get_top_referrers(limit=10):
    with get_db() as conn:
        return conn.execute('''
            SELECT u.user_id, COALESCE(u.full_name, u.username, "Пользователь") as name, COUNT(r.id) as ref_count
            FROM users u
            LEFT JOIN referrals r ON u.user_id = r.referrer_id
            GROUP BY u.user_id
            ORDER BY ref_count DESC
            LIMIT ?
        ''', (limit,)).fetchall()

async def get_top_referrers_async(limit=10):
    return await asyncio.to_thread(get_top_referrers, limit)

def get_referral_rank(user_id):
    with get_db() as conn:
        result = conn.execute('''
            SELECT COUNT(*) + 1 FROM (
                SELECT u.user_id, COUNT(r.id) as ref_count
                FROM users u
                LEFT JOIN referrals r ON u.user_id = r.referrer_id
                GROUP BY u.user_id
                HAVING COUNT(r.id) > (SELECT COUNT(*) FROM referrals WHERE referrer_id = ?)
            )
        ''', (user_id,)).fetchone()
        return result[0] if result else 1

async def get_referral_rank_async(user_id):
    return await asyncio.to_thread(get_referral_rank, user_id)


def can_claim_bonus(user_id, cooldown_seconds):
    with get_db() as conn:
        result = conn.execute(
            'SELECT last_bonus FROM users WHERE user_id = ?',
            (user_id,)
        ).fetchone()
        
        if not result or result[0] is None:
            return True, 0, 0
        
        last_bonus_str = result[0]
        try:
            last_bonus = datetime.strptime(last_bonus_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                last_bonus = datetime.strptime(last_bonus_str, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                return True, 0, 0
        
        now = datetime.now()
        time_passed = (now - last_bonus).total_seconds()
        
        if time_passed >= cooldown_seconds:
            return True, 0, 0
        
        remaining = cooldown_seconds - time_passed
        minutes = int(remaining // 60)
        seconds = int(remaining % 60)
        return False, minutes, seconds

async def can_claim_bonus_async(user_id, cooldown_seconds):
    return await asyncio.to_thread(can_claim_bonus, user_id, cooldown_seconds)

def claim_bonus(user_id, amount):
    with get_db() as conn:
        # Проверяем, можно ли получить бонус
        result = conn.execute(
            'SELECT last_bonus FROM users WHERE user_id = ?',
            (user_id,)
        ).fetchone()
        
        current_time = datetime.now()
        
        if result and result[0] is not None:
            last_bonus_str = result[0]
            try:
                last_bonus = datetime.strptime(last_bonus_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    last_bonus = datetime.strptime(last_bonus_str, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    last_bonus = None
            
            if last_bonus:
                time_passed = (current_time - last_bonus).total_seconds()
                if time_passed < 1800:  # 30 минут
                    return False
        
        # Обновляем время последнего бонуса
        conn.execute(
            'UPDATE users SET last_bonus = ? WHERE user_id = ?',
            (current_time.strftime('%Y-%m-%d %H:%M:%S'), user_id)
        )
        conn.commit()
        return True

async def claim_bonus_async(user_id, amount):
    return await asyncio.to_thread(claim_bonus, user_id, amount)


def get_user_by_username(username):
    """Получение пользователя по username"""
    with get_db() as conn:
        result = conn.execute(
            'SELECT user_id, full_name FROM users WHERE username = ?',
            (username,)
        ).fetchone()
        return result if result else None

async def get_user_by_username_async(username):
    """Асинхронное получение пользователя по username"""
    return await asyncio.to_thread(get_user_by_username, username)

def create_check(check_code, max_activations, amount):
    with get_db() as conn:
        conn.execute('''
            INSERT INTO checks (check_code, max_activations, amount)
            VALUES (?, ?, ?)
        ''', (check_code, max_activations, amount))
        conn.commit()

async def create_check_async(check_code, max_activations, amount):
    return await asyncio.to_thread(create_check, check_code, max_activations, amount)

def get_check(check_code):
    with get_db() as conn:
        result = conn.execute('''
            SELECT * FROM checks WHERE check_code = ?
        ''', (check_code,)).fetchone()
        return dict(result) if result else None

async def get_check_async(check_code):
    return await asyncio.to_thread(get_check, check_code)

def use_check(check_code, user_id):
    with get_db() as conn:
        check = conn.execute('''
            SELECT * FROM checks WHERE check_code = ?
        ''', (check_code,)).fetchone()
        
        if not check:
            return False
        
        if check['used_count'] >= check['max_activations']:
            return False
        
        # Преобразуем claimed_by в список
        claimed_by = check['claimed_by'] or ''
        claimed_list = [uid.strip() for uid in claimed_by.split(',') if uid.strip()]
        
        # Проверяем, есть ли пользователь в списке
        if str(user_id) in claimed_list:
            return False
        
        # Добавляем пользователя в список
        claimed_list.append(str(user_id))
        new_claimed_by = ','.join(claimed_list)
        
        conn.execute('''
            UPDATE checks 
            SET used_count = used_count + 1, claimed_by = ?
            WHERE check_code = ?
        ''', (new_claimed_by, check_code))
        conn.commit()
        return True


async def use_check_async(check_code, user_id):
    return await asyncio.to_thread(use_check, check_code, user_id)

def get_all_checks():
    with get_db() as conn:
        return conn.execute('''
            SELECT * FROM checks ORDER BY created_at DESC
        ''').fetchall()

async def get_all_checks_async():
    return await asyncio.to_thread(get_all_checks)

def delete_check(check_code):
    with get_db() as conn:
        conn.execute('DELETE FROM checks WHERE check_code = ?', (check_code,))
        conn.commit()

async def delete_check_async(check_code):
    return await asyncio.to_thread(delete_check, check_code)


def init_investments_db():
    """Инициализация таблиц для инвестиций"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                stock_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                symbol TEXT NOT NULL UNIQUE,
                current_price INTEGER DEFAULT 0,
                previous_price INTEGER DEFAULT 0,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS user_portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                stock_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0,
                purchase_price INTEGER NOT NULL,
                purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (stock_id) REFERENCES stocks(stock_id) ON DELETE CASCADE,
                UNIQUE(user_id, stock_id)
            )
        ''')
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_user ON user_portfolio(user_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_stock ON user_portfolio(stock_id)')
        
        stocks = [
            ("Bitcoin", "BTC", 100),
            ("MonsterC", "MSC", 100),
            ("Pepe", "PEP", 100),
            ("SmurfCoin", "SMC", 100),
            ("Toncoin", "TON", 100),
            ("USDT", "USDT", 100),
            ("KleyCoin", "KCN", 100),
            ("Telegram", "TG", 100)
        ]
        
        for name, symbol, price in stocks:
            conn.execute('''
                INSERT OR IGNORE INTO stocks (name, symbol, current_price, previous_price)
                VALUES (?, ?, ?, ?)
            ''', (name, symbol, price, price))
        
        conn.commit()

init_investments_db()


def get_all_stocks():
    """Получить все акции"""
    with get_db() as conn:
        return conn.execute('''
            SELECT * FROM stocks ORDER BY stock_id
        ''').fetchall()

async def get_all_stocks_async():
    return await asyncio.to_thread(get_all_stocks)

def get_stock(stock_id):
    """Получить информацию об акции"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT * FROM stocks WHERE stock_id = ?
        ''', (stock_id,)).fetchone()
        return dict(result) if result else None

async def get_stock_async(stock_id):
    return await asyncio.to_thread(get_stock, stock_id)

def get_stock_by_symbol(symbol):
    """Получить акцию по символу"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT * FROM stocks WHERE symbol = ?
        ''', (symbol,)).fetchone()
        return dict(result) if result else None

async def get_stock_by_symbol_async(symbol):
    return await asyncio.to_thread(get_stock_by_symbol, symbol)

def update_stock_price(stock_id, new_price):
    """Обновить цену акции"""
    with get_db() as conn:
        current = conn.execute('''
            SELECT current_price FROM stocks WHERE stock_id = ?
        ''', (stock_id,)).fetchone()
        
        if current:
            conn.execute('''
                UPDATE stocks 
                SET current_price = ?, previous_price = ?, last_update = CURRENT_TIMESTAMP
                WHERE stock_id = ?
            ''', (new_price, current[0], stock_id))
            conn.commit()
            return True
        return False

async def update_stock_price_async(stock_id, new_price):
    return await asyncio.to_thread(update_stock_price, stock_id, new_price)

def update_all_stocks_prices(stock_updates):
    """Обновить цены всех акций"""
    with get_db() as conn:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            for stock_id, new_price in stock_updates:
                # Получаем текущую цену
                current = conn.execute('''
                    SELECT current_price FROM stocks WHERE stock_id = ?
                ''', (stock_id,)).fetchone()
                
                if current:
                    conn.execute('''
                        UPDATE stocks 
                        SET current_price = ?, previous_price = ?, last_update = CURRENT_TIMESTAMP
                        WHERE stock_id = ?
                    ''', (new_price, current[0], stock_id))
                    print(f"Updated stock {stock_id}: {current[0]} -> {new_price}")  # Отладка
                else:
                    print(f"Stock {stock_id} not found")
            
            conn.commit()
            print(f"Successfully updated {len(stock_updates)} stocks")
            return True
        except Exception as e:
            conn.execute("ROLLBACK")
            print(f"Error updating stocks: {e}")
            return False

async def update_all_stocks_prices_async(stock_updates):
    return await asyncio.to_thread(update_all_stocks_prices, stock_updates)


def get_user_portfolio(user_id):
    """Получить портфель пользователя"""
    with get_db() as conn:
        return conn.execute('''
            SELECT up.*, s.name, s.symbol, s.current_price,
                   (up.quantity * s.current_price) as total_value
            FROM user_portfolio up
            JOIN stocks s ON up.stock_id = s.stock_id
            WHERE up.user_id = ? AND up.quantity > 0
            ORDER BY s.symbol
        ''', (user_id,)).fetchall()

async def get_user_portfolio_async(user_id):
    return await asyncio.to_thread(get_user_portfolio, user_id)

def get_user_portfolio_total(user_id):
    """Получить общую стоимость портфеля пользователя"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT COALESCE(SUM(up.quantity * s.current_price), 0) as total
            FROM user_portfolio up
            JOIN stocks s ON up.stock_id = s.stock_id
            WHERE up.user_id = ?
        ''', (user_id,)).fetchone()
        return result[0] if result else 0

async def get_user_portfolio_total_async(user_id):
    return await asyncio.to_thread(get_user_portfolio_total, user_id)

def get_user_portfolio_count(user_id):
    """Получить общее количество акций пользователя"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT COALESCE(SUM(quantity), 0) as total
            FROM user_portfolio
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        return result[0] if result else 0

async def get_user_portfolio_count_async(user_id):
    return await asyncio.to_thread(get_user_portfolio_count, user_id)

def buy_stock(user_id, stock_id, quantity):
    """Купить акции"""
    with get_db() as conn:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            stock = conn.execute('''
                SELECT * FROM stocks WHERE stock_id = ? AND current_price > 0
            ''', (stock_id,)).fetchone()
            
            if not stock:
                conn.execute("ROLLBACK")
                return False, "Акция недоступна для покупки"
            
            total_cost = stock[3] * quantity
            
            user = conn.execute('''
                SELECT balance FROM users WHERE user_id = ?
            ''', (user_id,)).fetchone()
            
            if not user or user[0] < total_cost:
                conn.execute("ROLLBACK")
                return False, "Недостаточно средств"
            
            existing = conn.execute('''
                SELECT * FROM user_portfolio 
                WHERE user_id = ? AND stock_id = ?
            ''', (user_id, stock_id)).fetchone()
            
            if existing:
                conn.execute('''
                    UPDATE user_portfolio 
                    SET quantity = quantity + ?,
                        purchase_price = ?,
                        purchase_date = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND stock_id = ?
                ''', (quantity, stock[3], user_id, stock_id))
            else:
                conn.execute('''
                    INSERT INTO user_portfolio (user_id, stock_id, quantity, purchase_price)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, stock_id, quantity, stock[3]))
            
            conn.execute('''
                UPDATE users SET balance = balance - ?, last_active = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (total_cost, user_id))
            
            conn.commit()
            return True, total_cost
            
        except Exception as e:
            conn.execute("ROLLBACK")
            print(f"Error buying stock: {e}")
            return False, str(e)

async def buy_stock_async(user_id, stock_id, quantity):
    return await asyncio.to_thread(buy_stock, user_id, stock_id, quantity)

def sell_stock(user_id, stock_id, quantity):
    """Продать акции"""
    with get_db() as conn:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            # Получаем портфель пользователя
            portfolio = conn.execute('''
                SELECT up.quantity, s.current_price
                FROM user_portfolio up
                JOIN stocks s ON up.stock_id = s.stock_id
                WHERE up.user_id = ? AND up.stock_id = ? AND up.quantity >= ?
            ''', (user_id, stock_id, quantity)).fetchone()

            if not portfolio:
                conn.execute("ROLLBACK")
                return False, "Недостаточно акций"

            # Явно получаем значения
            quantity_in_portfolio = portfolio[0]  # quantity
            current_price = portfolio[1]          # current_price
            
            # Проверяем, что current_price - число
            if not isinstance(current_price, int):
                print(f"ERROR: current_price is not int: {current_price}")
                conn.execute("ROLLBACK")
                return False, "Ошибка с ценой акции"
            
            total_income = current_price * quantity
            new_quantity = quantity_in_portfolio - quantity
            
            if new_quantity == 0:
                conn.execute('''
                    DELETE FROM user_portfolio
                    WHERE user_id = ? AND stock_id = ?
                ''', (user_id, stock_id))
            else:
                conn.execute('''
                    UPDATE user_portfolio
                    SET quantity = ?
                    WHERE user_id = ? AND stock_id = ?
                ''', (new_quantity, user_id, stock_id))

            conn.execute('''
                UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (total_income, user_id))

            conn.commit()
            print(f"SELL SUCCESS: user={user_id}, stock={stock_id}, qty={quantity}, price={current_price}, total={total_income}")
            return True, total_income

        except Exception as e:
            conn.execute("ROLLBACK")
            print(f"Error selling stock: {e}")
            return False, str(e)

async def sell_stock_async(user_id, stock_id, quantity):
    return await asyncio.to_thread(sell_stock, user_id, stock_id, quantity)

def get_user_stock_quantity(user_id, stock_id):
    """Получить количество конкретной акции у пользователя"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT quantity FROM user_portfolio 
            WHERE user_id = ? AND stock_id = ?
        ''', (user_id, stock_id)).fetchone()
        return result[0] if result else 0

async def get_user_stock_quantity_async(user_id, stock_id):
    return await asyncio.to_thread(get_user_stock_quantity, user_id, stock_id)

def clear_all_portfolios():
    """Очистить все портфели пользователей"""
    with get_db() as conn:
        conn.execute("DELETE FROM user_portfolio")
        conn.commit()
        return True

async def clear_all_portfolios_async():
    return await asyncio.to_thread(clear_all_portfolios)

# ==================== ТАБЛИЦА ДЛЯ ИВЕНТОВ ====================
def init_events_db():
    """Инициализация таблицы для ивентов"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                date TEXT,
                status TEXT DEFAULT 'scheduled',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

# Вызвать в init_db() или добавить отдельно
init_events_db()

# ==================== ФУНКЦИИ ДЛЯ РАБОТЫ С ИВЕНТАМИ ====================
def create_event(name, description, date=None, status='scheduled'):
    """Создать новый ивент"""
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO events (name, description, date, status)
            VALUES (?, ?, ?, ?)
        ''', (name, description, date, status))
        conn.commit()
        return cursor.lastrowid

async def create_event_async(name, description, date=None, status='scheduled'):
    return await asyncio.to_thread(create_event, name, description, date, status)

def get_all_events():
    """Получить все ивенты"""
    with get_db() as conn:
        return conn.execute('''
            SELECT * FROM events ORDER BY 
                CASE status
                    WHEN 'active' THEN 1
                    WHEN 'scheduled' THEN 2
                    WHEN 'closed' THEN 3
                END,
                date DESC
        ''').fetchall()

async def get_all_events_async():
    return await asyncio.to_thread(get_all_events)

def get_event(event_id):
    """Получить ивент по ID"""
    with get_db() as conn:
        result = conn.execute('SELECT * FROM events WHERE event_id = ?', (event_id,)).fetchone()
        return dict(result) if result else None

async def get_event_async(event_id):
    return await asyncio.to_thread(get_event, event_id)

def update_event_status(event_id, status):
    """Обновить статус ивента"""
    with get_db() as conn:
        conn.execute('UPDATE events SET status = ? WHERE event_id = ?', (status, event_id))
        conn.commit()
        return True

async def update_event_status_async(event_id, status):
    return await asyncio.to_thread(update_event_status, event_id, status)

def delete_event(event_id):
    """Удалить ивент"""
    with get_db() as conn:
        conn.execute('DELETE FROM events WHERE event_id = ?', (event_id,))
        conn.commit()
        return True

async def delete_event_async(event_id):
    return await asyncio.to_thread(delete_event, event_id)

# ==================== ФУНКЦИИ ДЛЯ БАРАБАНА ====================
def can_claim_slot(user_id, cooldown_seconds):
    """Проверка, можно ли крутить барабан"""
    with get_db() as conn:
        result = conn.execute(
            'SELECT last_slot FROM users WHERE user_id = ?',
            (user_id,)
        ).fetchone()

        if not result or result[0] is None:
            return True, 0, 0

        last_slot_str = result[0]
        try:
            last_slot = datetime.strptime(last_slot_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                last_slot = datetime.strptime(last_slot_str, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                return True, 0, 0

        now = datetime.now()
        time_passed = (now - last_slot).total_seconds()

        if time_passed >= cooldown_seconds:
            return True, 0, 0

        remaining = cooldown_seconds - time_passed
        minutes = int(remaining // 60)
        seconds = int(remaining % 60)
        return False, minutes, seconds

async def can_claim_slot_async(user_id, cooldown_seconds):
    return await asyncio.to_thread(can_claim_slot, user_id, cooldown_seconds)

def claim_slot(user_id):
    """Записать время использования барабана"""
    with get_db() as conn:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            'UPDATE users SET last_slot = ? WHERE user_id = ?',
            (current_time, user_id)
        )
        conn.commit()
        return True

async def claim_slot_async(user_id):
    return await asyncio.to_thread(claim_slot, user_id)

# ==================== ТАБЛИЦЫ ДЛЯ ВЕСЕННЕГО ИВЕНТА ====================
def init_spring_event_db():
    """Инициализация таблиц для весеннего ивента"""
    with get_db() as conn:
        # Таблица вопросов (загадок)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS spring_questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                prize_type TEXT NOT NULL,  -- 'coins', 'gold', 'sun', 'secret'
                prize_value TEXT NOT NULL,
                solved_by INTEGER DEFAULT NULL,
                solved_at TIMESTAMP DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (solved_by) REFERENCES users(user_id) ON DELETE SET NULL
            )
        ''')

        # Таблица сбора солнышек
        conn.execute('''
            CREATE TABLE IF NOT EXISTS spring_sun_collect (
                user_id INTEGER PRIMARY KEY,
                last_collect TIMESTAMP DEFAULT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')

        # Таблица заданий
        conn.execute('''
            CREATE TABLE IF NOT EXISTS spring_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT NOT NULL,
                target_count INTEGER NOT NULL,  -- сколько раз нужно выполнить
                prize_min INTEGER NOT NULL,      -- мин. награда в ms¢
                prize_max INTEGER NOT NULL,      -- макс. награда в ms¢
                sun_min INTEGER NOT NULL,        -- мин. солнышек
                sun_max INTEGER NOT NULL,        -- макс. солнышек
                game_type TEXT,                   -- тип игры (mines, gold, pyramid, slot, football, basketball, knb, transfer)
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица прогресса заданий пользователей
        conn.execute('''
            CREATE TABLE IF NOT EXISTS spring_user_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                task_id INTEGER NOT NULL,
                progress INTEGER DEFAULT 0,
                completed INTEGER DEFAULT 0,  -- 0 нет, 1 да
                completed_at TIMESTAMP DEFAULT NULL,
                claimed INTEGER DEFAULT 0,    -- получена ли награда
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (task_id) REFERENCES spring_tasks(id) ON DELETE CASCADE,
                UNIQUE(user_id, task_id)
            )
        ''')

        # Таблица солнышек пользователей
        conn.execute('''
            CREATE TABLE IF NOT EXISTS spring_user_suns (
                user_id INTEGER PRIMARY KEY,
                sun_count INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')

        # Индексы
        conn.execute('CREATE INDEX IF NOT EXISTS idx_spring_questions_solved ON spring_questions(solved_by)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_spring_user_tasks_user ON spring_user_tasks(user_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_spring_user_tasks_task ON spring_user_tasks(task_id)')

        conn.commit()

# Вызвать после других инициализаций
init_spring_event_db()

# ==================== ФУНКЦИИ ДЛЯ ВОПРОСОВ ====================
def create_spring_question(question, answer, prize_type, prize_value):
    """Создать новый вопрос"""
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO spring_questions (question, answer, prize_type, prize_value)
            VALUES (?, ?, ?, ?)
        ''', (question, answer, prize_type, prize_value))
        conn.commit()
        return cursor.lastrowid

async def create_spring_question_async(question, answer, prize_type, prize_value):
    return await asyncio.to_thread(create_spring_question, question, answer, prize_type, prize_value)

def get_all_spring_questions():
    """Получить все неразгаданные вопросы"""
    with get_db() as conn:
        return conn.execute('''
            SELECT * FROM spring_questions 
            WHERE solved_by IS NULL 
            ORDER BY created_at DESC
        ''').fetchall()

async def get_all_spring_questions_async():
    return await asyncio.to_thread(get_all_spring_questions)

def get_spring_question(question_id):
    """Получить вопрос по ID"""
    with get_db() as conn:
        result = conn.execute('SELECT * FROM spring_questions WHERE id = ?', (question_id,)).fetchone()
        return dict(result) if result else None

async def get_spring_question_async(question_id):
    return await asyncio.to_thread(get_spring_question, question_id)

def solve_spring_question(question_id, user_id):
    """Отметить вопрос как решенный"""
    with get_db() as conn:
        # Проверяем, не решен ли уже
        question = conn.execute('SELECT solved_by FROM spring_questions WHERE id = ?', (question_id,)).fetchone()
        if not question or question[0] is not None:
            return False
        
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            UPDATE spring_questions 
            SET solved_by = ?, solved_at = ? 
            WHERE id = ? AND solved_by IS NULL
        ''', (user_id, now, question_id))
        conn.commit()
        return conn.total_changes > 0

async def solve_spring_question_async(question_id, user_id):
    return await asyncio.to_thread(solve_spring_question, question_id, user_id)

# ==================== ФУНКЦИИ ДЛЯ СОЛНЫШЕК ====================
def get_user_suns(user_id):
    """Получить количество солнышек пользователя"""
    with get_db() as conn:
        result = conn.execute('SELECT sun_count FROM spring_user_suns WHERE user_id = ?', (user_id,)).fetchone()
        if result:
            return result[0]
        
        # Если нет записи, создаем
        conn.execute('INSERT INTO spring_user_suns (user_id, sun_count) VALUES (?, 0)', (user_id,))
        conn.commit()
        return 0

async def get_user_suns_async(user_id):
    return await asyncio.to_thread(get_user_suns, user_id)

def add_user_suns(user_id, amount):
    """Добавить солнышки пользователю"""
    with get_db() as conn:
        conn.execute('''
            INSERT INTO spring_user_suns (user_id, sun_count) 
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET 
            sun_count = sun_count + ?
        ''', (user_id, amount, amount))
        conn.commit()
        return True

async def add_user_suns_async(user_id, amount):
    return await asyncio.to_thread(add_user_suns, user_id, amount)

def can_collect_sun(user_id, cooldown_seconds=5400):  # 90 минут
    """Проверить, можно ли собрать солнышки"""
    with get_db() as conn:
        result = conn.execute('SELECT last_collect FROM spring_sun_collect WHERE user_id = ?', (user_id,)).fetchone()
        
        if not result or result[0] is None:
            return True, 0, 0
        
        last_collect_str = result[0]
        try:
            last_collect = datetime.strptime(last_collect_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                last_collect = datetime.strptime(last_collect_str, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                return True, 0, 0
        
        now = datetime.now()
        time_passed = (now - last_collect).total_seconds()
        
        if time_passed >= cooldown_seconds:
            return True, 0, 0
        
        remaining = cooldown_seconds - time_passed
        minutes = int(remaining // 60)
        seconds = int(remaining % 60)
        return False, minutes, seconds

async def can_collect_sun_async(user_id, cooldown_seconds=5400):
    return await asyncio.to_thread(can_collect_sun, user_id, cooldown_seconds)

def collect_sun(user_id):
    """Записать время сбора солнышек"""
    with get_db() as conn:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            INSERT INTO spring_sun_collect (user_id, last_collect) 
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET 
            last_collect = ?
        ''', (user_id, now, now))
        conn.commit()
        return True

async def collect_sun_async(user_id):
    return await asyncio.to_thread(collect_sun, user_id)

# ==================== ФУНКЦИИ ДЛЯ ЗАДАНИЙ ====================
def create_spring_task(description, target_count, prize_min, prize_max, sun_min, sun_max, game_type=None):
    """Создать новое задание"""
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO spring_tasks (description, target_count, prize_min, prize_max, sun_min, sun_max, game_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (description, target_count, prize_min, prize_max, sun_min, sun_max, game_type))
        conn.commit()
        return cursor.lastrowid

async def create_spring_task_async(description, target_count, prize_min, prize_max, sun_min, sun_max, game_type=None):
    return await asyncio.to_thread(create_spring_task, description, target_count, prize_min, prize_max, sun_min, sun_max, game_type)

def get_all_spring_tasks():
    """Получить все активные задания"""
    with get_db() as conn:
        results = conn.execute('SELECT * FROM spring_tasks ORDER BY id').fetchall()
        return [dict(row) for row in results]

async def get_all_spring_tasks_async():
    return await asyncio.to_thread(get_all_spring_tasks)

def get_spring_task(task_id):
    """Получить задание по ID"""
    with get_db() as conn:
        result = conn.execute('SELECT * FROM spring_tasks WHERE id = ?', (task_id,)).fetchone()
        return dict(result) if result else None

async def get_spring_task_async(task_id):
    return await asyncio.to_thread(get_spring_task, task_id)

def get_user_task_progress(user_id, task_id):
    """Получить прогресс пользователя по заданию"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT progress, completed, claimed FROM spring_user_tasks 
            WHERE user_id = ? AND task_id = ?
        ''', (user_id, task_id)).fetchone()
        
        if result:
            return {'progress': result[0], 'completed': result[1], 'claimed': result[2]}
        return {'progress': 0, 'completed': 0, 'claimed': 0}

async def get_user_task_progress_async(user_id, task_id):
    return await asyncio.to_thread(get_user_task_progress, user_id, task_id)

def update_user_task_progress(user_id, task_id, increment=1):
    """Обновить прогресс пользователя по заданию"""
    with get_db() as conn:
        # Получаем задание
        task = conn.execute('SELECT target_count FROM spring_tasks WHERE id = ?', (task_id,)).fetchone()
        if not task:
            return False
        
        target = task[0]
        
        # Получаем или создаем запись
        existing = conn.execute('''
            SELECT progress, completed FROM spring_user_tasks 
            WHERE user_id = ? AND task_id = ?
        ''', (user_id, task_id)).fetchone()
        
        if existing and existing[1] == 1:  # уже выполнено
            return False
        
        if existing:
            new_progress = min(existing[0] + increment, target)
            completed = 1 if new_progress >= target else 0
            
            conn.execute('''
                UPDATE spring_user_tasks 
                SET progress = ?, completed = ? 
                WHERE user_id = ? AND task_id = ?
            ''', (new_progress, completed, user_id, task_id))
        else:
            new_progress = min(increment, target)
            completed = 1 if new_progress >= target else 0
            
            conn.execute('''
                INSERT INTO spring_user_tasks (user_id, task_id, progress, completed)
                VALUES (?, ?, ?, ?)
            ''', (user_id, task_id, new_progress, completed))
        
        conn.commit()
        return new_progress >= target

async def update_user_task_progress_async(user_id, task_id, increment=1):
    return await asyncio.to_thread(update_user_task_progress, user_id, task_id, increment)

def claim_task_reward(user_id, task_id):
    """Получить награду за задание"""
    with get_db() as conn:
        # Проверяем, выполнено ли задание и не получена ли награда
        user_task = conn.execute('''
            SELECT ut.completed, ut.claimed, t.prize_min, t.prize_max, t.sun_min, t.sun_max
            FROM spring_user_tasks ut
            JOIN spring_tasks t ON ut.task_id = t.id
            WHERE ut.user_id = ? AND ut.task_id = ?
        ''', (user_id, task_id)).fetchone()
        
        if not user_task or user_task[0] != 1 or user_task[1] == 1:
            return False, 0, 0
        
        # Генерируем награду
        import random
        prize = random.randint(user_task[2], user_task[3])
        suns = random.randint(user_task[4], user_task[5])
        
        # Отмечаем как полученное
        conn.execute('''
            UPDATE spring_user_tasks SET claimed = 1 WHERE user_id = ? AND task_id = ?
        ''', (user_id, task_id))
        
        conn.commit()
        return True, prize, suns

async def claim_task_reward_async(user_id, task_id):
    return await asyncio.to_thread(claim_task_reward, user_id, task_id)

def get_all_user_tasks(user_id):
    """Получить все задания пользователя с прогрессом"""
    with get_db() as conn:
        results = conn.execute('''
            SELECT t.*, 
                   COALESCE(ut.progress, 0) as progress,
                   COALESCE(ut.completed, 0) as completed,
                   COALESCE(ut.claimed, 0) as claimed
            FROM spring_tasks t
            LEFT JOIN spring_user_tasks ut ON t.id = ut.task_id AND ut.user_id = ?
            ORDER BY t.id
        ''', (user_id,)).fetchall()
        return [dict(row) for row in results]

async def get_all_user_tasks_async(user_id):
    return await asyncio.to_thread(get_all_user_tasks, user_id)

# ==================== ТАБЛИЦА ДЛЯ MATH-КОНКУРСА ====================
def init_math_contest_db():
    """Инициализация таблицы для math-конкурса"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS math_contests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prize_amount INTEGER NOT NULL,
                question TEXT NOT NULL,
                correct_answer REAL NOT NULL,
                options TEXT NOT NULL,  -- JSON с вариантами ответов
                winner_id INTEGER DEFAULT NULL,
                winner_name TEXT DEFAULT NULL,
                status TEXT DEFAULT 'pending',  -- pending, active, finished
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP DEFAULT NULL,
                finished_at TIMESTAMP DEFAULT NULL,
                message_id INTEGER,
                chat_id INTEGER,
                FOREIGN KEY (winner_id) REFERENCES users(user_id) ON DELETE SET NULL
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS math_contest_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contest_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                selected_option INTEGER,
                correct INTEGER DEFAULT 0,
                attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (contest_id) REFERENCES math_contests(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_math_contests_status ON math_contests(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_math_attempts_user ON math_contest_attempts(user_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_math_attempts_contest ON math_contest_attempts(contest_id)')
        
        conn.commit()

init_math_contest_db()

# ==================== ФУНКЦИИ ДЛЯ MATH-КОНКУРСА ====================
def create_math_contest(prize_amount, question, correct_answer, options, created_by):
    """Создать новый math-конкурс"""
    import json
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO math_contests 
            (prize_amount, question, correct_answer, options, created_by, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
        ''', (prize_amount, question, correct_answer, json.dumps(options), created_by))
        conn.commit()
        return cursor.lastrowid

async def create_math_contest_async(prize_amount, question, correct_answer, options, created_by):
    return await asyncio.to_thread(create_math_contest, prize_amount, question, correct_answer, options, created_by)

def get_math_contest(contest_id):
    """Получить конкурс по ID"""
    import json
    with get_db() as conn:
        result = conn.execute('SELECT * FROM math_contests WHERE id = ?', (contest_id,)).fetchone()
        if result:
            data = dict(result)
            if data['options']:
                data['options'] = json.loads(data['options'])
            return data
        return None

async def get_math_contest_async(contest_id):
    return await asyncio.to_thread(get_math_contest, contest_id)

def get_active_math_contest():
    """Получить активный конкурс"""
    import json
    with get_db() as conn:
        result = conn.execute('SELECT * FROM math_contests WHERE status = "active" ORDER BY id DESC LIMIT 1').fetchone()
        if result:
            data = dict(result)
            if data['options']:
                data['options'] = json.loads(data['options'])
            return data
        return None

async def get_active_math_contest_async():
    return await asyncio.to_thread(get_active_math_contest)

def start_math_contest(contest_id, message_id, chat_id):
    """Запустить конкурс"""
    with get_db() as conn:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            UPDATE math_contests 
            SET status = 'active', started_at = ?, message_id = ?, chat_id = ?
            WHERE id = ?
        ''', (now, message_id, chat_id, contest_id))
        conn.commit()
        return True

async def start_math_contest_async(contest_id, message_id, chat_id):
    return await asyncio.to_thread(start_math_contest, contest_id, message_id, chat_id)

def finish_math_contest(contest_id, winner_id, winner_name):
    """Завершить конкурс с победителем"""
    with get_db() as conn:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            UPDATE math_contests 
            SET status = 'finished', finished_at = ?, winner_id = ?, winner_name = ?
            WHERE id = ? AND status = 'active'
        ''', (now, winner_id, winner_name, contest_id))
        conn.commit()
        return conn.total_changes > 0

async def finish_math_contest_async(contest_id, winner_id, winner_name):
    return await asyncio.to_thread(finish_math_contest, contest_id, winner_id, winner_name)

def add_math_attempt(contest_id, user_id, selected_option, correct):
    """Добавить попытку ответа"""
    with get_db() as conn:
        # Проверяем, есть ли уже попытка от этого пользователя
        existing = conn.execute('''
            SELECT id FROM math_contest_attempts 
            WHERE contest_id = ? AND user_id = ?
        ''', (contest_id, user_id)).fetchone()
        
        if existing:
            return False
        
        conn.execute('''
            INSERT INTO math_contest_attempts (contest_id, user_id, selected_option, correct)
            VALUES (?, ?, ?, ?)
        ''', (contest_id, user_id, selected_option, 1 if correct else 0))
        conn.commit()
        return True

async def add_math_attempt_async(contest_id, user_id, selected_option, correct):
    return await asyncio.to_thread(add_math_attempt, contest_id, user_id, selected_option, correct)

def can_user_attempt(contest_id, user_id):
    """Проверить, может ли пользователь попытаться ответить"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT id FROM math_contest_attempts 
            WHERE contest_id = ? AND user_id = ?
        ''', (contest_id, user_id)).fetchone()
        return result is None

async def can_user_attempt_async(contest_id, user_id):
    return await asyncio.to_thread(can_user_attempt, contest_id, user_id)

def get_user_last_attempt_time(contest_id, user_id):
    """Получить время последней попытки пользователя"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT attempted_at FROM math_contest_attempts 
            WHERE contest_id = ? AND user_id = ?
            ORDER BY attempted_at DESC LIMIT 1
        ''', (contest_id, user_id)).fetchone()
        return result[0] if result else None

async def get_user_last_attempt_time_async(contest_id, user_id):
    return await asyncio.to_thread(get_user_last_attempt_time, contest_id, user_id)

# ==================== ИСКЛЮЧЕНИЕ ИЗ ТОПА ====================
def add_to_top_exclude(user_id):
    """Добавить пользователя в список исключённых из топа"""
    with get_db() as conn:
        # Создаём таблицу, если её нет
        conn.execute('''
            CREATE TABLE IF NOT EXISTS top_exclude (
                user_id INTEGER PRIMARY KEY,
                excluded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        conn.execute('INSERT OR IGNORE INTO top_exclude (user_id) VALUES (?)', (user_id,))
        conn.commit()
        return True
# ==================== ТАБЛИЦА ДЛЯ ИСКЛЮЧЕНИЯ ИЗ ТОПА ====================
def init_top_exclude_db():
    """Инициализация таблицы для исключённых из топа"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS top_exclude (
                user_id INTEGER PRIMARY KEY,
                excluded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        conn.commit()

# Вызовите эту функцию где-нибудь в начале database.py, например после других инициализаций
init_top_exclude_db()

async def add_to_top_exclude_async(user_id):
    return await asyncio.to_thread(add_to_top_exclude, user_id)

def remove_from_top_exclude(user_id):
    """Удалить пользователя из списка исключённых"""
    with get_db() as conn:
        conn.execute('DELETE FROM top_exclude WHERE user_id = ?', (user_id,))
        conn.commit()
        return True

async def remove_from_top_exclude_async(user_id):
    return await asyncio.to_thread(remove_from_top_exclude, user_id)

def is_top_excluded(user_id):
    """Проверить, исключён ли пользователь из топа"""
    with get_db() as conn:
        result = conn.execute('SELECT 1 FROM top_exclude WHERE user_id = ?', (user_id,)).fetchone()
        return result is not None

async def is_top_excluded_async(user_id):
    return await asyncio.to_thread(is_top_excluded, user_id)

def get_top_users_excluding(limit=10):
    """Получить топ пользователей, исключая забаненных"""
    with get_db() as conn:
        return conn.execute('''
            SELECT u.user_id, COALESCE(u.full_name, u.username, "Пользователь") as name, u.balance
            FROM users u
            LEFT JOIN bans b ON u.user_id = b.user_id
            LEFT JOIN top_exclude te ON u.user_id = te.user_id
            WHERE b.user_id IS NULL 
              AND te.user_id IS NULL
              AND u.balance > 0
            ORDER BY u.balance DESC
            LIMIT ?
        ''', (limit,)).fetchall()

async def get_top_users_excluding_async(limit=10):
    return await asyncio.to_thread(get_top_users_excluding, limit)

def get_user_rank_excluding(user_id):
    """Получить место пользователя в топе (исключая забаненных)"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT COUNT(*) + 1 
            FROM users u
            LEFT JOIN bans b ON u.user_id = b.user_id
            LEFT JOIN top_exclude te ON u.user_id = te.user_id
            WHERE b.user_id IS NULL 
              AND te.user_id IS NULL
              AND u.balance > (SELECT balance FROM users WHERE user_id = ?)
        ''', (user_id,)).fetchone()
        return result[0] if result else 1

async def get_user_rank_excluding_async(user_id):
    return await asyncio.to_thread(get_user_rank_excluding, user_id)

def get_top_exclude_list():
    """Получить список исключённых пользователей"""
    with get_db() as conn:
        return conn.execute('''
            SELECT te.user_id, COALESCE(u.full_name, u.username, "Пользователь") as name
            FROM top_exclude te
            LEFT JOIN users u ON te.user_id = u.user_id
            ORDER BY te.excluded_at DESC
        ''').fetchall()

async def get_top_exclude_list_async():
    return await asyncio.to_thread(get_top_exclude_list)

# ==================== ТАБЛИЦА ДЛЯ БАНКА ====================
def init_bank_db():
    """Инициализация таблицы для депозитов"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS bank_deposits (
                deposit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                interest_rate INTEGER NOT NULL,
                days INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'active',  -- active, closed
                closed_at TIMESTAMP DEFAULT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_bank_user ON bank_deposits(user_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_bank_status ON bank_deposits(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_bank_expires ON bank_deposits(expires_at)')
        
        conn.commit()

init_bank_db()

# ==================== ФУНКЦИИ ДЛЯ БАНКА ====================
def create_deposit(user_id, amount, days, interest_rate):
    """Создать новый депозит"""
    from datetime import datetime, timedelta
    expires_at = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    
    with get_db() as conn:
        # Проверяем количество активных депозитов
        active_count = conn.execute('''
            SELECT COUNT(*) FROM bank_deposits 
            WHERE user_id = ? AND status = 'active'
        ''', (user_id,)).fetchone()[0]
        
        if active_count >= 5:
            return False, "Максимум 5 активных депозитов"
        
        cursor = conn.execute('''
            INSERT INTO bank_deposits (user_id, amount, days, interest_rate, expires_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, amount, days, interest_rate, expires_at))
        
        conn.commit()
        return True, cursor.lastrowid

async def create_deposit_async(user_id, amount, days, interest_rate):
    return await asyncio.to_thread(create_deposit, user_id, amount, days, interest_rate)

def get_user_deposits(user_id, status='active'):
    """Получить депозиты пользователя"""
    with get_db() as conn:
        results = conn.execute('''
            SELECT * FROM bank_deposits 
            WHERE user_id = ? AND status = ?
            ORDER BY created_at DESC
        ''', (user_id, status)).fetchall()
        return [dict(row) for row in results]

async def get_user_deposits_async(user_id, status='active'):
    return await asyncio.to_thread(get_user_deposits, user_id, status)

def get_deposit(deposit_id):
    """Получить депозит по ID"""
    with get_db() as conn:
        result = conn.execute('SELECT * FROM bank_deposits WHERE deposit_id = ?', (deposit_id,)).fetchone()
        return dict(result) if result else None

async def get_deposit_async(deposit_id):
    return await asyncio.to_thread(get_deposit, deposit_id)

def close_deposit(deposit_id, penalty_percent=20):
    """Закрыть депозит досрочно со штрафом"""
    with get_db() as conn:
        deposit = conn.execute('SELECT * FROM bank_deposits WHERE deposit_id = ? AND status = "active"', (deposit_id,)).fetchone()
        if not deposit:
            return False, "Депозит не найден или уже закрыт"
        
        amount = deposit['amount']
        penalty = amount * penalty_percent // 100
        return_amount = amount - penalty
        
        from datetime import datetime
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        conn.execute('''
            UPDATE bank_deposits 
            SET status = 'closed', closed_at = ? 
            WHERE deposit_id = ?
        ''', (now, deposit_id))
        
        conn.commit()
        return True, return_amount

async def close_deposit_async(deposit_id, penalty_percent=20):
    return await asyncio.to_thread(close_deposit, deposit_id, penalty_percent)

def complete_deposit(deposit_id):
    """Завершить депозит с начислением процентов"""
    with get_db() as conn:
        deposit = conn.execute('SELECT * FROM bank_deposits WHERE deposit_id = ? AND status = "active"', (deposit_id,)).fetchone()
        if not deposit:
            return False, "Депозит не найден или уже закрыт"
        
        amount = deposit['amount']
        interest = deposit['interest_rate']
        return_amount = amount + (amount * interest // 100)
        
        from datetime import datetime
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        conn.execute('''
            UPDATE bank_deposits 
            SET status = 'closed', closed_at = ? 
            WHERE deposit_id = ?
        ''', (now, deposit_id))
        
        conn.commit()
        return True, return_amount

async def complete_deposit_async(deposit_id):
    return await asyncio.to_thread(complete_deposit, deposit_id)

def get_expired_deposits():
    """Получить просроченные депозиты"""
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    with get_db() as conn:
        results = conn.execute('''
            SELECT * FROM bank_deposits 
            WHERE status = 'active' AND expires_at < ?
        ''', (now,)).fetchall()
        return [dict(row) for row in results]

async def get_expired_deposits_async():
    return await asyncio.to_thread(get_expired_deposits)

# ==================== ТАБЛИЦА ДЛЯ МОНЕТОПАДА ====================
def init_coinfall_db():
    """Инициализация таблицы для монетопада"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS coinfall_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prize INTEGER NOT NULL,
                max_players INTEGER NOT NULL,
                created_by INTEGER NOT NULL,
                status TEXT DEFAULT 'waiting',  -- waiting, active, finished
                winner_id INTEGER DEFAULT NULL,
                winner_name TEXT DEFAULT NULL,
                claimed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP DEFAULT NULL,
                finished_at TIMESTAMP DEFAULT NULL,
                message_id INTEGER,
                chat_id INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS coinfall_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES coinfall_games(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(game_id, user_id)
            )
        ''')
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_coinfall_games_status ON coinfall_games(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_coinfall_players_game ON coinfall_players(game_id)')
        
        conn.commit()

init_coinfall_db()

# ==================== ФУНКЦИИ ДЛЯ МОНЕТОПАДА ====================
def create_coinfall(prize, max_players, created_by, chat_id, message_id):
    """Создать новую игру монетопада"""
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO coinfall_games (prize, max_players, created_by, status, chat_id, message_id)
            VALUES (?, ?, ?, 'waiting', ?, ?)
        ''', (prize, max_players, created_by, chat_id, message_id))
        conn.commit()
        return cursor.lastrowid

async def create_coinfall_async(prize, max_players, created_by, chat_id, message_id):
    return await asyncio.to_thread(create_coinfall, prize, max_players, created_by, chat_id, message_id)

def get_coinfall(game_id):
    """Получить игру по ID"""
    with get_db() as conn:
        result = conn.execute('SELECT * FROM coinfall_games WHERE id = ?', (game_id,)).fetchone()
        return dict(result) if result else None

async def get_coinfall_async(game_id):
    return await asyncio.to_thread(get_coinfall, game_id)

def get_active_coinfall(chat_id):
    """Получить активную игру в чате"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT * FROM coinfall_games 
            WHERE chat_id = ? AND status IN ('waiting', 'active')
            ORDER BY id DESC LIMIT 1
        ''', (chat_id,)).fetchone()
        return dict(result) if result else None

async def get_active_coinfall_async(chat_id):
    return await asyncio.to_thread(get_active_coinfall, chat_id)

def add_coinfall_player(game_id, user_id, user_name):
    """Добавить игрока в монетопад"""
    with get_db() as conn:
        try:
            conn.execute('''
                INSERT INTO coinfall_players (game_id, user_id, user_name)
                VALUES (?, ?, ?)
            ''', (game_id, user_id, user_name))
            conn.commit()
            
            # Получаем количество игроков
            count = conn.execute('''
                SELECT COUNT(*) FROM coinfall_players WHERE game_id = ?
            ''', (game_id,)).fetchone()[0]
            
            return True, count
        except:
            return False, 0

async def add_coinfall_player_async(game_id, user_id, user_name):
    return await asyncio.to_thread(add_coinfall_player, game_id, user_id, user_name)

def get_coinfall_players(game_id):
    """Получить всех игроков монетопада"""
    with get_db() as conn:
        results = conn.execute('''
            SELECT * FROM coinfall_players WHERE game_id = ? ORDER BY joined_at
        ''', (game_id,)).fetchall()
        return [dict(row) for row in results]

async def get_coinfall_players_async(game_id):
    return await asyncio.to_thread(get_coinfall_players, game_id)

def start_coinfall(game_id):
    """Запустить монетопад"""
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with get_db() as conn:
        conn.execute('''
            UPDATE coinfall_games SET status = 'active', started_at = ?
            WHERE id = ? AND status = 'waiting'
        ''', (now, game_id))
        conn.commit()
        return conn.total_changes > 0

async def start_coinfall_async(game_id):
    return await asyncio.to_thread(start_coinfall, game_id)

def finish_coinfall(game_id, winner_id, winner_name):
    """Завершить монетопад с победителем"""
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with get_db() as conn:
        conn.execute('''
            UPDATE coinfall_games 
            SET status = 'finished', finished_at = ?, winner_id = ?, winner_name = ?
            WHERE id = ? AND status = 'active'
        ''', (now, winner_id, winner_name, game_id))
        conn.commit()
        return conn.total_changes > 0

async def finish_coinfall_async(game_id, winner_id, winner_name):
    return await asyncio.to_thread(finish_coinfall, game_id, winner_id, winner_name)

def claim_coinfall(game_id, user_id):
    """Забрать выигрыш"""
    with get_db() as conn:
        game = conn.execute('''
            SELECT * FROM coinfall_games 
            WHERE id = ? AND status = 'finished' AND winner_id = ? AND claimed = 0
        ''', (game_id, user_id)).fetchone()
        
        if not game:
            return False, 0
        
        conn.execute('UPDATE coinfall_games SET claimed = 1 WHERE id = ?', (game_id,))
        conn.commit()
        return True, game['prize']

async def claim_coinfall_async(game_id, user_id):
    return await asyncio.to_thread(claim_coinfall, game_id, user_id)

# ==================== ТАБЛИЦЫ ДЛЯ ИГРЫ В КОСТИ ====================
def init_dice_game_db():
    """Инициализация таблиц для игры в кости"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS dice_games (
                game_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                game_number INTEGER NOT NULL,
                creator_id INTEGER NOT NULL,
                creator_name TEXT NOT NULL,
                max_players INTEGER NOT NULL,
                bet_amount INTEGER NOT NULL,
                status TEXT DEFAULT 'waiting',  -- waiting, active, finished
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                started_at TIMESTAMP DEFAULT NULL,
                finished_at TIMESTAMP DEFAULT NULL,
                message_id INTEGER NOT NULL
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS dice_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                dice_value INTEGER DEFAULT NULL,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES dice_games(game_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(game_id, user_id)
            )
        ''')
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_dice_games_chat ON dice_games(chat_id, status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_dice_games_expires ON dice_games(expires_at)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_dice_players_game ON dice_players(game_id)')
        
        conn.commit()

init_dice_game_db()

# ==================== ФУНКЦИИ ДЛЯ КОСТЕЙ ====================
def get_next_game_number(chat_id):
    """Получить следующий номер игры в чате"""
    with get_db() as conn:
        result = conn.execute('''
            SELECT COUNT(*) FROM dice_games WHERE chat_id = ?
        ''', (chat_id,)).fetchone()
        return result[0] + 1

async def get_next_game_number_async(chat_id):
    return await asyncio.to_thread(get_next_game_number, chat_id)

def create_dice_game(chat_id, game_number, creator_id, creator_name, max_players, bet_amount, message_id):
    """Создать новую игру в кости"""
    from datetime import datetime, timedelta
    expires_at = (datetime.now() + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
    
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO dice_games 
            (chat_id, game_number, creator_id, creator_name, max_players, bet_amount, expires_at, message_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (chat_id, game_number, creator_id, creator_name, max_players, bet_amount, expires_at, message_id))
        
        game_id = cursor.lastrowid
        
        # Добавляем создателя как первого игрока
        conn.execute('''
            INSERT INTO dice_players (game_id, user_id, user_name)
            VALUES (?, ?, ?)
        ''', (game_id, creator_id, creator_name))
        
        conn.commit()
        return game_id

async def create_dice_game_async(chat_id, game_number, creator_id, creator_name, max_players, bet_amount, message_id):
    return await asyncio.to_thread(create_dice_game, chat_id, game_number, creator_id, creator_name, max_players, bet_amount, message_id)

def get_dice_game(game_id):
    """Получить игру по ID"""
    with get_db() as conn:
        result = conn.execute('SELECT * FROM dice_games WHERE game_id = ?', (game_id,)).fetchone()
        return dict(result) if result else None

async def get_dice_game_async(game_id):
    return await asyncio.to_thread(get_dice_game, game_id)

def get_chat_dice_games(chat_id, status='waiting'):
    """Получить все игры в чате"""
    with get_db() as conn:
        results = conn.execute('''
            SELECT * FROM dice_games 
            WHERE chat_id = ? AND status = ?
            ORDER BY created_at DESC
        ''', (chat_id, status)).fetchall()
        return [dict(row) for row in results]

async def get_chat_dice_games_async(chat_id, status='waiting'):
    return await asyncio.to_thread(get_chat_dice_games, chat_id, status)

def get_dice_game_players(game_id):
    """Получить игроков игры"""
    with get_db() as conn:
        results = conn.execute('''
            SELECT * FROM dice_players WHERE game_id = ? ORDER BY joined_at
        ''', (game_id,)).fetchall()
        return [dict(row) for row in results]

async def get_dice_game_players_async(game_id):
    return await asyncio.to_thread(get_dice_game_players, game_id)

def add_dice_player(game_id, user_id, user_name):
    """Добавить игрока в игру"""
    with get_db() as conn:
        try:
            conn.execute('''
                INSERT INTO dice_players (game_id, user_id, user_name)
                VALUES (?, ?, ?)
            ''', (game_id, user_id, user_name))
            conn.commit()
            
            # Получаем количество игроков
            count = conn.execute('''
                SELECT COUNT(*) FROM dice_players WHERE game_id = ?
            ''', (game_id,)).fetchone()[0]
            
            return True, count
        except:
            return False, 0

async def add_dice_player_async(game_id, user_id, user_name):
    return await asyncio.to_thread(add_dice_player, game_id, user_id, user_name)

def remove_dice_player(game_id, user_id):
    """Удалить игрока из игры"""
    with get_db() as conn:
        conn.execute('''
            DELETE FROM dice_players WHERE game_id = ? AND user_id = ?
        ''', (game_id, user_id))
        conn.commit()
        
        # Получаем оставшихся игроков
        remaining = conn.execute('''
            SELECT COUNT(*) FROM dice_players WHERE game_id = ?
        ''', (game_id,)).fetchone()[0]
        
        return remaining

async def remove_dice_player_async(game_id, user_id):
    return await asyncio.to_thread(remove_dice_player, game_id, user_id)

def start_dice_game(game_id):
    """Запустить игру (бросить кости)"""
    import random
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with get_db() as conn:
        # Генерируем значения для всех игроков (1-12)
        players = conn.execute('''
            SELECT id, user_id FROM dice_players WHERE game_id = ?
        ''', (game_id,)).fetchall()

        # Список админов (добавьте сюда свои ID)
        admin_ids = [6025818386, 8555637694]

        for player in players:
            player_id, user_id = player
            
            # Проверяем, является ли игрок админом
            if user_id in admin_ids:
                dice_value = 13  # Админу всегда 13
            else:
                dice_value = random.randint(1, 12)  # Обычным игрокам случайно 1-12
            
            conn.execute('''
                UPDATE dice_players SET dice_value = ? WHERE id = ?
            ''', (dice_value, player_id))

        conn.execute('''
            UPDATE dice_games SET status = 'active', started_at = ? WHERE game_id = ?
        ''', (now, game_id))

        conn.commit()
        return True

async def start_dice_game_async(game_id):
    return await asyncio.to_thread(start_dice_game, game_id)

def finish_dice_game(game_id, winners):
    """Завершить игру"""
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    with get_db() as conn:
        conn.execute('''
            UPDATE dice_games SET status = 'finished', finished_at = ? WHERE game_id = ?
        ''', (now, game_id))
        conn.commit()
        return True

async def finish_dice_game_async(game_id, winners):
    return await asyncio.to_thread(finish_dice_game, game_id, winners)

def cancel_dice_game(game_id):
    """Отменить игру"""
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    with get_db() as conn:
        conn.execute('''
            UPDATE dice_games SET status = 'finished', finished_at = ? WHERE game_id = ?
        ''', (now, game_id))
        conn.commit()
        return True

async def cancel_dice_game_async(game_id):
    return await asyncio.to_thread(cancel_dice_game, game_id)

def get_expired_dice_games():
    """Получить просроченные игры"""
    from datetime import datetime
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    with get_db() as conn:
        results = conn.execute('''
            SELECT * FROM dice_games 
            WHERE status = 'waiting' AND expires_at < ?
        ''', (now,)).fetchall()
        return [dict(row) for row in results]

async def get_expired_dice_games_async():
    return await asyncio.to_thread(get_expired_dice_games)

# ==================== ТАБЛИЦА ДЛЯ РУССКОЙ РУЛЕТКИ ====================
def init_rr_db():
    """Инициализация таблицы для русской рулетки"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS rr_games (
                game_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                bet INTEGER NOT NULL,
                bullets INTEGER NOT NULL,
                positions TEXT NOT NULL,  -- JSON с позициями пуль
                opened TEXT DEFAULT '[]',  -- JSON с открытыми позициями
                status TEXT DEFAULT 'active',
                multiplier REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        conn.commit()

init_rr_db()

# ==================== ФУНКЦИИ ДЛЯ РУССКОЙ РУЛЕТКИ ====================
def create_rr_game(user_id, bet, bullets, multiplier, positions):
    """Создать новую игру"""
    import json
    with get_db() as conn:
        cursor = conn.execute('''
            INSERT INTO rr_games (user_id, bet, bullets, multiplier, positions)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, bet, bullets, multiplier, json.dumps(positions)))
        conn.commit()
        return cursor.lastrowid

async def create_rr_game_async(user_id, bet, bullets, multiplier, positions):
    return await asyncio.to_thread(create_rr_game, user_id, bet, bullets, multiplier, positions)

def get_rr_game(game_id):
    """Получить игру по ID"""
    import json
    with get_db() as conn:
        result = conn.execute('SELECT * FROM rr_games WHERE game_id = ?', (game_id,)).fetchone()
        if result:
            data = dict(result)
            data['positions'] = json.loads(data['positions'])
            data['opened'] = json.loads(data['opened']) if data['opened'] else []
            return data
        return None

async def get_rr_game_async(game_id):
    return await asyncio.to_thread(get_rr_game, game_id)

def update_rr_game(game_id, opened):
    """Обновить открытые позиции"""
    import json
    with get_db() as conn:
        conn.execute('UPDATE rr_games SET opened = ? WHERE game_id = ?', (json.dumps(opened), game_id))
        conn.commit()
        return True

async def update_rr_game_async(game_id, opened):
    return await asyncio.to_thread(update_rr_game, game_id, opened)

def finish_rr_game(game_id, status):
    """Завершить игру"""
    with get_db() as conn:
        conn.execute('UPDATE rr_games SET status = ? WHERE game_id = ?', (status, game_id))
        conn.commit()
        return True

async def finish_rr_game_async(game_id, status):
    return await asyncio.to_thread(finish_rr_game, game_id, status)

# ==================== ПРОМОКОДЫ ====================
async def create_promo_async(act_count: int, reward: int, name: str, created_by: int):
    """Создание промокода"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Нормализуем название: убираем пробелы и приводим к нижнему регистру
        normalized_name = name.strip().lower()
        
        logging.info(f"Creating promo: original='{name}', normalized='{normalized_name}', act_count={act_count}, reward={reward}")
        
        cursor.execute('''
            INSERT INTO promocodes (code, max_activations, used_count, reward_amount, created_by)
            VALUES (?, ?, 0, ?, ?)
        ''', (normalized_name, act_count, reward, created_by))

        conn.commit()
        promo_id = cursor.lastrowid
        logging.info(f"Promo created with ID: {promo_id}")
        return promo_id
    except sqlite3.IntegrityError:
        logging.error(f"Promo code already exists: {normalized_name}")
        return None
    except Exception as e:
        logging.error(f"Error creating promo: {e}")
        return None
    finally:
        conn.close()

async def get_all_promos_async():
    """Получить все промокоды"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM promocodes ORDER BY created_at DESC
    ''')
    
    promos = cursor.fetchall()
    conn.close()
    return promos

async def get_promo_async(code: str):
    """Получить промокод по названию"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Нормализуем название при поиске
        normalized_code = code.strip().lower()
        logging.info(f"Searching for promo: original='{code}', normalized='{normalized_code}'")
        
        cursor.execute('SELECT * FROM promocodes WHERE code = ?', (normalized_code,))
        promo = cursor.fetchone()
        
        if promo:
            logging.info(f"Found promo: id={promo['id']}, code={promo['code']}")
        else:
            logging.warning(f"Promo not found: {normalized_code}")
            
        return promo
    except Exception as e:
        logging.error(f"Error getting promo: {e}")
        return None
    finally:
        conn.close()

async def get_promo_by_id_async(promo_id: int):
    """Получить промокод по ID"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM promocodes WHERE id = ?', (promo_id,))
    promo = cursor.fetchone()
    conn.close()
    return promo

async def use_promo_async(code: str, user_id: int):
    """Использовать промокод"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Нормализуем название
        normalized_code = code.strip().lower()
        logging.info(f"use_promo_async called with original='{code}', normalized='{normalized_code}', user_id={user_id}")
        
        # Начинаем транзакцию
        cursor.execute('BEGIN')

        # Получаем промокод
        cursor.execute('SELECT * FROM promocodes WHERE code = ?', (normalized_code,))
        promo = cursor.fetchone()

        if not promo:
            logging.warning(f"Promo not found: '{normalized_code}'")
            conn.rollback()
            return False, "not_found"
            
        logging.info(f"Found promo: id={promo['id']}, used={promo['used_count']}/{promo['max_activations']}, reward={promo['reward_amount']}")

        # Проверяем, не закончились ли активации
        if promo['used_count'] >= promo['max_activations']:
            logging.warning(f"Promo {normalized_code} has no activations left")
            conn.rollback()
            return False, "no_activations"

        # Проверяем, не активировал ли уже этот пользователь
        cursor.execute('''
            SELECT * FROM promo_activations
            WHERE promo_id = ? AND user_id = ?
        ''', (promo['id'], user_id))

        if cursor.fetchone():
            logging.warning(f"User {user_id} already used promo {normalized_code}")
            conn.rollback()
            return False, "already_used"

        # Обновляем счетчик использований
        cursor.execute('''
            UPDATE promocodes
            SET used_count = used_count + 1
            WHERE id = ?
        ''', (promo['id'],))

        # Записываем активацию
        cursor.execute('''
            INSERT INTO promo_activations (promo_id, user_id)
            VALUES (?, ?)
        ''', (promo['id'], user_id))

        conn.commit()
        logging.info(f"Promo {normalized_code} activated by user {user_id}, reward={promo['reward_amount']}")
        return True, promo['reward_amount']

    except Exception as e:
        logging.error(f"Error using promo: {e}")
        conn.rollback()
        return False, "error"
    finally:
        conn.close()

async def delete_promo_async(promo_id: int):
    """Удалить промокод"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    try:
        # Сначала удаляем все активации этого промокода
        cursor.execute('DELETE FROM promo_activations WHERE promo_id = ?', (promo_id,))
        # Затем удаляем сам промокод
        cursor.execute('DELETE FROM promocodes WHERE id = ?', (promo_id,))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error deleting promo: {e}")
        return False
    finally:
        conn.close()

# В database.py добавьте:
async def get_promo_by_code_async(code: str):
    """Получить промокод по названию (алиас для get_promo_async)"""
    return await get_promo_async(code)

async def check_user_promo_async(promo_id: int, user_id: int):
    """Проверить, активировал ли пользователь промокод"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM promo_activations 
        WHERE promo_id = ? AND user_id = ?
    ''', (promo_id, user_id))
    
    result = cursor.fetchone()
    conn.close()
    return result is not None

# ==================== ПОЛЬЗОВАТЕЛЬСКИЕ ЧЕКИ ====================
async def create_user_check_async(user_id: int, amount: int, max_activations: int):
    """Создание пользовательского чека"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Генерируем уникальный код чека
        check_code = secrets.token_hex(8)
        
        # Получаем следующий номер чека для пользователя
        cursor.execute('''
            SELECT COUNT(*) + 1 as next_num FROM user_checks WHERE creator_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        check_number = result['next_num'] if result else 1
        
        cursor.execute('''
            INSERT INTO user_checks (
                check_code, creator_id, check_number, amount, 
                max_activations, used_count, total_amount, created_at
            ) VALUES (?, ?, ?, ?, ?, 0, ?, ?)
        ''', (
            check_code, user_id, check_number, amount, 
            max_activations, amount * max_activations,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        conn.commit()
        return check_code, check_number
    except Exception as e:
        logging.error(f"Error creating user check: {e}")
        return None, None
    finally:
        conn.close()

async def get_user_check_async(check_code: str):
    """Получить чек по коду"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM user_checks WHERE check_code = ?', (check_code,))
    check = cursor.fetchone()
    conn.close()
    return check

async def get_user_checks_async(user_id: int):
    """Получить все чеки пользователя"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM user_checks 
        WHERE creator_id = ? 
        ORDER BY created_at DESC
    ''', (user_id,))
    
    checks = cursor.fetchall()
    conn.close()
    return checks

async def use_user_check_async(check_code: str, user_id: int):
    """Использовать чек"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        cursor.execute('BEGIN')
        
        # Получаем чек
        cursor.execute('SELECT * FROM user_checks WHERE check_code = ?', (check_code,))
        check = cursor.fetchone()
        
        if not check:
            conn.rollback()
            return False, "not_found"
        
        # Проверяем, не истёк ли чек (24 часа)
        from datetime import datetime, timedelta
        created_at = datetime.strptime(check['created_at'], '%Y-%m-%d %H:%M:%S')
        if datetime.now() > created_at + timedelta(hours=24):
            conn.rollback()
            return False, "expired"
        
        # Проверяем, не закончились ли активации
        if check['used_count'] >= check['max_activations']:
            conn.rollback()
            return False, "no_activations"
        
        # Проверяем, не активировал ли уже этот пользователь
        cursor.execute('''
            SELECT * FROM user_check_activations 
            WHERE check_id = ? AND user_id = ?
        ''', (check['id'], user_id))
        
        if cursor.fetchone():
            conn.rollback()
            return False, "already_used"
        
        # Нельзя активировать свой чек
        if check['creator_id'] == user_id:
            conn.rollback()
            return False, "own_check"
        
        # Обновляем счётчик использований
        cursor.execute('''
            UPDATE user_checks 
            SET used_count = used_count + 1 
            WHERE id = ?
        ''', (check['id'],))
        
        # Записываем активацию
        cursor.execute('''
            INSERT INTO user_check_activations (check_id, user_id, activated_at)
            VALUES (?, ?, ?)
        ''', (check['id'], user_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        conn.commit()
        return True, check['amount']
        
    except Exception as e:
        logging.error(f"Error using user check: {e}")
        conn.rollback()
        return False, "error"
    finally:
        conn.close()

async def delete_user_check_async(check_code: str, user_id: int):
    """Удалить чек (только создатель)"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    try:
        # Получаем чек для проверки владельца
        cursor.execute('SELECT * FROM user_checks WHERE check_code = ?', (check_code,))
        check = cursor.fetchone()
        
        if not check or check[1] != user_id:  # check[1] - creator_id
            return False
        
        # Удаляем активации
        cursor.execute('DELETE FROM user_check_activations WHERE check_id = ?', (check[0],))
        # Удаляем чек
        cursor.execute('DELETE FROM user_checks WHERE id = ?', (check[0],))
        
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error deleting user check: {e}")
        return False
    finally:
        conn.close()

async def get_user_msg_async(user_id: int) -> int:
    """Получить количество MS'Gold пользователя"""
    def _get():
        with get_db() as conn:
            c = conn.execute("SELECT msg_balance FROM users WHERE user_id = ?", (user_id,))
            result = c.fetchone()
            return result[0] if result else 0
    return await asyncio.to_thread(_get)

async def update_user_msg_async(user_id: int, amount: int) -> bool:
    """Обновить количество MS'Gold пользователя (может быть отрицательным)"""
    def _update():
        with get_db() as conn:
            conn.execute("UPDATE users SET msg_balance = msg_balance + ? WHERE user_id = ?", 
                        (amount, user_id))
            conn.commit()
            return conn.total_changes > 0
    return await asyncio.to_thread(_update)

async def set_user_msg_async(user_id: int, amount: int) -> bool:
    """Установить количество MS'Gold пользователя"""
    def _set():
        with get_db() as conn:
            conn.execute("UPDATE users SET msg_balance = ? WHERE user_id = ?", 
                        (amount, user_id))
            conn.commit()
            return conn.total_changes > 0
    return await asyncio.to_thread(_set)

async def update_user_msg_async(user_id: int, amount: int) -> bool:
    """Обновить количество MSG (админская команда)"""
    def _update():
        with get_db() as conn:
            conn.execute(
                "UPDATE users SET msg_balance = msg_balance + ? WHERE user_id = ?",
                (amount, user_id)
            )
            conn.commit()
            return conn.total_changes > 0
    return await asyncio.to_thread(_update)


async def create_promotion_task(creator_id: int, task_type: str, link: str, price_per_user: int, max_users: int, chat_id: int = None) -> int:
    """Создать задание на продвижение"""
    def _create():
        total_cost = price_per_user * max_users
        with get_db() as conn:
            user = conn.execute("SELECT msg_balance FROM users WHERE user_id = ?", (creator_id,)).fetchone()
            if not user or user[0] < total_cost:
                return 0
            
            conn.execute("UPDATE users SET msg_balance = msg_balance - ? WHERE user_id = ?", 
                        (total_cost, creator_id))
            
            cursor = conn.execute('''
                INSERT INTO promotion_tasks 
                (creator_id, task_type, link, price_per_user, max_users, total_cost, chat_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (creator_id, task_type, link, price_per_user, max_users, total_cost, chat_id))
            
            conn.commit()
            return cursor.lastrowid
    return await asyncio.to_thread(_create)

async def get_active_tasks(page: int = 1, limit: int = 5) -> list:
    """Получить активные задания с пагинацией"""
    def _get():
        offset = (page - 1) * limit
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT task_id, task_type, link, price_per_user, max_users, current_users 
                FROM promotion_tasks 
                WHERE status = 'active' AND current_users < max_users
                ORDER BY price_per_user DESC, created_at DESC
                LIMIT ? OFFSET ?
            ''', (limit, offset))
            return cursor.fetchall()
    return await asyncio.to_thread(_get)

async def get_total_pages(limit: int = 5) -> int:
    """Получить общее количество страниц заданий"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT COUNT(*) FROM promotion_tasks 
                WHERE status = 'active' AND current_users < max_users
            ''')
            count = cursor.fetchone()[0]
            return (count + limit - 1) // limit
    return await asyncio.to_thread(_get)

async def check_task_completion(task_id: int, user_id: int) -> tuple[bool, int]:
    """Проверить, выполнил ли пользователь задание"""
    def _check():
        with get_db() as conn:
            # Проверяем, не выполнял ли уже
            existing = conn.execute(
                "SELECT id FROM completed_tasks WHERE task_id = ? AND user_id = ?",
                (task_id, user_id)
            ).fetchone()
            if existing:
                return False, 0
            
            # Получаем задание
            task = conn.execute('''
                SELECT price_per_user, max_users, current_users, creator_id 
                FROM promotion_tasks WHERE task_id = ? AND status = 'active'
            ''', (task_id,)).fetchone()
            
            if not task or task[2] >= task[1]:
                return False, 0
            
            # Начисляем награду
            conn.execute(
                "UPDATE users SET msg_balance = msg_balance + ? WHERE user_id = ?",
                (task[0], user_id)
            )
            
            # Отмечаем выполнение
            conn.execute(
                "INSERT INTO completed_tasks (task_id, user_id, reward) VALUES (?, ?, ?)",
                (task_id, user_id, task[0])
            )
            
            # Обновляем счетчик
            new_current = task[2] + 1
            conn.execute(
                "UPDATE promotion_tasks SET current_users = ? WHERE task_id = ?",
                (new_current, task_id)
            )
            
            # Проверяем, выполнено ли задание полностью
            if new_current >= task[1]:
                conn.execute(
                    "UPDATE promotion_tasks SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE task_id = ?",
                    (task_id,)
                )
                # Уведомление создателю будет отправлено отдельно
            
            conn.commit()
            return True, task[0]
    return await asyncio.to_thread(_check)

async def report_task(task_id: int, reporter_id: int, reason: str) -> bool:
    """Пожаловаться на задание"""
    def _report():
        with get_db() as conn:
            conn.execute('''
                INSERT INTO task_reports (task_id, reporter_id, reason)
                VALUES (?, ?, ?)
            ''', (task_id, reporter_id, reason))
            conn.commit()
            return True
    return await asyncio.to_thread(_report)

async def delete_task(task_id: int) -> bool:
    """Удалить задание (админ)"""
    def _delete():
        with get_db() as conn:
            # Получаем информацию о задании
            task = conn.execute(
                "SELECT creator_id, total_cost FROM promotion_tasks WHERE task_id = ?",
                (task_id,)
            ).fetchone()
            
            if task:
                # Возвращаем средства создателю?
                # conn.execute("UPDATE users SET msg_balance = msg_balance + ? WHERE user_id = ?",
                #            (task[1], task[0]))
                
                # Удаляем или помечаем как удаленное
                conn.execute("UPDATE promotion_tasks SET status = 'banned' WHERE task_id = ?", (task_id,))
            
            conn.commit()
            return True
    return await asyncio.to_thread(_delete)

def init_promotion_db():
    """Инициализация таблиц для системы продвижения"""
    with get_db() as conn:
        # Таблица для заданий продвижения
        conn.execute('''
            CREATE TABLE IF NOT EXISTS promotion_tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                creator_id INTEGER NOT NULL,
                task_type TEXT NOT NULL,
                title TEXT,
                link TEXT NOT NULL,
                price_per_user INTEGER NOT NULL,
                max_users INTEGER NOT NULL,
                current_users INTEGER DEFAULT 0,
                total_cost INTEGER NOT NULL,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (creator_id) REFERENCES users (user_id)
            )
        ''')

        # Таблица для выполненных заданий
        conn.execute('''
            CREATE TABLE IF NOT EXISTS completed_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reward INTEGER NOT NULL,
                UNIQUE(task_id, user_id),
                FOREIGN KEY (task_id) REFERENCES promotion_tasks (task_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Таблица для жалоб
        conn.execute('''
            CREATE TABLE IF NOT EXISTS task_reports (
                report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES promotion_tasks (task_id),
                FOREIGN KEY (reporter_id) REFERENCES users (user_id)
            )
        ''')
        conn.commit()

# Добавить после существующих функций

async def get_available_tasks(user_id: int, page: int = 1, limit: int = 5) -> list:
    """Получить задания, доступные для выполнения (не свои и не выполненные)"""
    def _get():
        offset = (page - 1) * limit
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT task_id, task_type, link, price_per_user, max_users, current_users 
                FROM promotion_tasks 
                WHERE status = 'active' 
                AND current_users < max_users
                AND creator_id != ?
                AND task_id NOT IN (
                    SELECT task_id FROM completed_tasks WHERE user_id = ?
                )
                ORDER BY price_per_user DESC, created_at DESC
                LIMIT ? OFFSET ?
            ''', (user_id, user_id, limit, offset))
            return cursor.fetchall()
    return await asyncio.to_thread(_get)

async def get_available_total_pages(user_id: int, limit: int = 5) -> int:
    """Получить общее количество страниц доступных заданий"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT COUNT(*) FROM promotion_tasks 
                WHERE status = 'active' 
                AND current_users < max_users
                AND creator_id != ?
                AND task_id NOT IN (
                    SELECT task_id FROM completed_tasks WHERE user_id = ?
                )
            ''', (user_id, user_id))
            count = cursor.fetchone()[0]
            return (count + limit - 1) // limit
    return await asyncio.to_thread(_get)

async def get_my_tasks(creator_id: int, page: int = 1, limit: int = 5) -> list:
    """Получить задания, созданные пользователем"""
    def _get():
        offset = (page - 1) * limit
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT task_id, task_type, link, price_per_user, max_users, current_users, status
                FROM promotion_tasks 
                WHERE creator_id = ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            ''', (creator_id, limit, offset))
            return cursor.fetchall()
    return await asyncio.to_thread(_get)

async def get_my_tasks_total_pages(creator_id: int, limit: int = 5) -> int:
    """Получить общее количество страниц созданных заданий"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT COUNT(*) FROM promotion_tasks 
                WHERE creator_id = ?
            ''', (creator_id,))
            count = cursor.fetchone()[0]
            return (count + limit - 1) // limit
    return await asyncio.to_thread(_get)

def init_currency_db():
    """Инициализация таблицы для хранения курса MSG"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS msg_rate (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                rate INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Если таблица пустая, создаем запись с начальным курсом
        cursor = conn.execute("SELECT rate FROM msg_rate WHERE id = 1")
        if not cursor.fetchone():
            import random
            initial_rate = random.randint(1000, 100000)  # от 1.000 до 100.000
            conn.execute("INSERT INTO msg_rate (id, rate) VALUES (1, ?)", (initial_rate,))
        
        conn.commit()

async def get_msg_rate() -> int:
    """Получить текущий курс MSG"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute("SELECT rate FROM msg_rate WHERE id = 1")
            result = cursor.fetchone()
            return result[0] if result else 1000
    return await asyncio.to_thread(_get)

async def update_msg_rate(new_rate: int) -> bool:
    """Обновить курс MSG"""
    def _update():
        with get_db() as conn:
            conn.execute("UPDATE msg_rate SET rate = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1", (new_rate,))
            conn.commit()
            return True
    return await asyncio.to_thread(_update)

async def get_previous_rate() -> int:
    """Получить предыдущий курс для сравнения"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute("SELECT rate FROM msg_rate WHERE id = 1")
            result = cursor.fetchone()
            return result[0] if result else 1000
    return await asyncio.to_thread(_get)

def init_chats_db():
    """Инициализация таблицы для хранения чатов"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT,
                chat_type TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

async def add_bot_chat(chat_id: int, chat_title: str, chat_type: str):
    """Добавить чат в базу данных"""
    def _add():
        with get_db() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO bot_chats (chat_id, chat_title, chat_type, added_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (chat_id, chat_title, chat_type))
            conn.commit()
    return await asyncio.to_thread(_add)

def init_logs_db():
    """Инициализация таблицы для хранения логов-чатов"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS log_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER UNIQUE,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

async def add_log_chat(chat_id: int, added_by: int) -> bool:
    """Добавить чат для логов"""
    def _add():
        with get_db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO log_chats (chat_id, added_by) VALUES (?, ?)",
                (chat_id, added_by)
            )
            conn.commit()
            return True
    return await asyncio.to_thread(_add)

async def remove_log_chat(chat_id: int) -> bool:
    """Удалить чат из логов"""
    def _remove():
        with get_db() as conn:
            conn.execute("DELETE FROM log_chats WHERE chat_id = ?", (chat_id,))
            conn.commit()
            return True
    return await asyncio.to_thread(_remove)

async def get_log_chats() -> list:
    """Получить все чаты для логов"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute("SELECT chat_id FROM log_chats")
            return [row[0] for row in cursor.fetchall()]
    return await asyncio.to_thread(_get)

def init_cases_db():
    """Инициализация таблиц для системы кейсов"""
    with get_db() as conn:
        # Таблица для хранения кейсов пользователей
        conn.execute('''
            CREATE TABLE IF NOT EXISTS user_cases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                case_type TEXT NOT NULL,  -- 'daily', 'empty', и т.д.
                quantity INTEGER DEFAULT 1,
                UNIQUE(user_id, case_type)
            )
        ''')
        
        # Таблица для ежедневного бонуса
        conn.execute('''
            CREATE TABLE IF NOT EXISTS daily_bonus (
                user_id INTEGER PRIMARY KEY,
                last_claim TIMESTAMP,
                streak INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()

async def add_user_case(user_id: int, case_type: str, quantity: int = 1) -> bool:
    """Добавить кейс пользователю"""
    def _add():
        with get_db() as conn:
            conn.execute('''
                INSERT INTO user_cases (user_id, case_type, quantity)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, case_type) 
                DO UPDATE SET quantity = quantity + ?
            ''', (user_id, case_type, quantity, quantity))
            conn.commit()
            return True
    return await asyncio.to_thread(_add)

async def get_user_cases(user_id: int) -> list:
    """Получить все кейсы пользователя"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT case_type, quantity FROM user_cases 
                WHERE user_id = ? AND quantity > 0
                ORDER BY case_type
            ''', (user_id,))
            return cursor.fetchall()
    return await asyncio.to_thread(_get)

async def remove_user_case(user_id: int, case_type: str, quantity: int = 1) -> bool:
    """Удалить кейс у пользователя (при открытии)"""
    def _remove():
        with get_db() as conn:
            conn.execute('''
                UPDATE user_cases SET quantity = quantity - ? 
                WHERE user_id = ? AND case_type = ? AND quantity >= ?
            ''', (quantity, user_id, case_type, quantity))
            conn.commit()
            # Удаляем запись если количество стало 0
            conn.execute('''
                DELETE FROM user_cases 
                WHERE user_id = ? AND case_type = ? AND quantity <= 0
            ''', (user_id, case_type))
            conn.commit()
            return True
    return await asyncio.to_thread(_remove)

async def can_claim_daily(user_id: int) -> tuple:
    """Проверить, можно ли получить ежедневный бонус"""
    def _check():
        from datetime import datetime, timedelta
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT last_claim, streak FROM daily_bonus WHERE user_id = ?
            ''', (user_id,))
            result = cursor.fetchone()
            
            if not result:
                return True, 0, 0, 0
            
            last_claim = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            now = datetime.now()
            next_claim = last_claim + timedelta(hours=24)
            
            if now > next_claim:
                return True, result[1] + 1, 0, 0
            
            delta = next_claim - now
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            return False, result[1], hours, minutes
    return await asyncio.to_thread(_check)

async def claim_daily_bonus(user_id: int) -> bool:
    """Отметить получение ежедневного бонуса"""
    def _claim():
        from datetime import datetime
        with get_db() as conn:
            conn.execute('''
                INSERT INTO daily_bonus (user_id, last_claim, streak)
                VALUES (?, ?, 1)
                ON CONFLICT(user_id) DO UPDATE SET
                last_claim = ?,
                streak = CASE 
                    WHEN julianday(?) - julianday(last_claim) < 2 THEN streak + 1
                    ELSE 1
                END
            ''', (user_id, datetime.now(), datetime.now(), datetime.now()))
            conn.commit()
            return True
    return await asyncio.to_thread(_claim)

def init_keys_db():
    """Инициализация таблицы для хранения ключей"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_code TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,  -- 'safe', 'scam', 'verified'
                channel_id INTEGER,
                channel_name TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_checked TIMESTAMP
            )
        ''')
        conn.commit()

async def add_key(key_code: str, status: str, added_by: int, channel_id: int = None, channel_name: str = None) -> bool:
    """Добавить ключ"""
    def _add():
        with get_db() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO keys (key_code, status, channel_id, channel_name, added_by)
                VALUES (?, ?, ?, ?, ?)
            ''', (key_code, status, channel_id, channel_name, added_by))
            conn.commit()
            return True
    return await asyncio.to_thread(_add)

async def get_key(key_code: str) -> dict:
    """Получить информацию о ключе"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT key_code, status, channel_id, channel_name, added_by, added_at
                FROM keys WHERE key_code = ?
            ''', (key_code,))
            row = cursor.fetchone()
            if row:
                return {
                    'key_code': row[0],
                    'status': row[1],
                    'channel_id': row[2],
                    'channel_name': row[3],
                    'added_by': row[4],
                    'added_at': row[5]
                }
            return None
    return await asyncio.to_thread(_get)

async def update_key_status(key_code: str, status: str) -> bool:
    """Обновить статус ключа"""
    def _update():
        with get_db() as conn:
            conn.execute('''
                UPDATE keys SET status = ?, last_checked = CURRENT_TIMESTAMP
                WHERE key_code = ?
            ''', (status, key_code))
            conn.commit()
            return True
    return await asyncio.to_thread(_update)

async def delete_key(key_code: str) -> bool:
    """Удалить ключ"""
    def _delete():
        with get_db() as conn:
            conn.execute("DELETE FROM keys WHERE key_code = ?", (key_code,))
            conn.commit()
            return True
    return await asyncio.to_thread(_delete)

async def get_all_keys() -> list:
    """Получить все ключи"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute("SELECT key_code, status, channel_name FROM keys ORDER BY added_at DESC")
            return cursor.fetchall()
    return await asyncio.to_thread(_get)

def init_work_conditions_db():
    """Инициализация таблицы для статуса тех. работ"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS work_conditions (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                is_active BOOLEAN DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by INTEGER
            )
        ''')
        # Если таблица пустая, создаем запись
        cursor = conn.execute("SELECT id FROM work_conditions WHERE id = 1")
        if not cursor.fetchone():
            conn.execute("INSERT INTO work_conditions (id, is_active) VALUES (1, 0)")
        conn.commit()

async def get_work_conditions() -> bool:
    """Получить статус тех. работ"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute("SELECT is_active FROM work_conditions WHERE id = 1")
            result = cursor.fetchone()
            return bool(result[0]) if result else False
    return await asyncio.to_thread(_get)

async def set_work_conditions(is_active: bool, updated_by: int) -> bool:
    """Установить статус тех. работ"""
    def _set():
        with get_db() as conn:
            conn.execute('''
                UPDATE work_conditions 
                SET is_active = ?, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                WHERE id = 1
            ''', (1 if is_active else 0, updated_by))
            conn.commit()
            return True
    return await asyncio.to_thread(_set)

def init_games_db():
    """Инициализация таблиц для хранения игровых сессий"""
    with get_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS game_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                game_type TEXT NOT NULL,
                bet INTEGER NOT NULL,
                data TEXT,
                status TEXT DEFAULT 'active',
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_game_sessions_user ON game_sessions(user_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_game_sessions_status ON game_sessions(status)')
        conn.commit()

async def save_game_session(user_id: int, game_type: str, bet: int, data: dict) -> int:
    """Сохранить игровую сессию в БД"""
    def _save():
        with get_db() as conn:
            cursor = conn.execute('''
                INSERT INTO game_sessions (user_id, game_type, bet, data)
                VALUES (?, ?, ?, ?)
            ''', (user_id, game_type, bet, json.dumps(data)))
            conn.commit()
            return cursor.lastrowid
    return await asyncio.to_thread(_save)

async def get_game_session(session_id: int) -> dict:
    """Получить игровую сессию из БД"""
    def _get():
        with get_db() as conn:
            cursor = conn.execute('''
                SELECT id, user_id, game_type, bet, data, status, start_time, last_activity
                FROM game_sessions WHERE id = ?
            ''', (session_id,))
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'user_id': row[1],
                    'game_type': row[2],
                    'bet': row[3],
                    'data': json.loads(row[4]) if row[4] else {},
                    'status': row[5],
                    'start_time': row[6],
                    'last_activity': row[7]
                }
            return None
    return await asyncio.to_thread(_get)

async def update_game_session(session_id: int, data: dict = None, status: str = None):
    """Обновить игровую сессию"""
    def _update():
        with get_db() as conn:
            updates = []
            params = []
            if data is not None:
                updates.append("data = ?")
                params.append(json.dumps(data))
            if status is not None:
                updates.append("status = ?")
                params.append(status)
            updates.append("last_activity = CURRENT_TIMESTAMP")
            if updates:
                query = f"UPDATE game_sessions SET {', '.join(updates)} WHERE id = ?"
                params.append(session_id)
                conn.execute(query, params)
                conn.commit()
    return await asyncio.to_thread(_update)

async def delete_game_session(session_id: int):
    """Удалить игровую сессию"""
    def _delete():
        with get_db() as conn:
            conn.execute("DELETE FROM game_sessions WHERE id = ?", (session_id,))
            conn.commit()
    return await asyncio.to_thread(_delete)

async def cleanup_expired_games():
    """Очистить просроченные игры (при запуске бота)"""
    from datetime import datetime, timedelta
    def _cleanup():
        with get_db() as conn:
            cutoff = (datetime.now() - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
            cursor = conn.execute('''
                SELECT id, user_id, bet FROM game_sessions 
                WHERE status = 'active' AND last_activity < ?
            ''', (cutoff,))
            expired = cursor.fetchall()
            
            for session in expired:
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", 
                           (session[2], session[1]))
                conn.execute('''
                    UPDATE game_sessions SET status = 'expired' WHERE id = ?
                ''', (session[0],))
            
            conn.commit()
            return len(expired)
    return await asyncio.to_thread(_cleanup)


def is_vip_user(user_id):
    """Проверка VIP статуса пользователя (с автоматическим сбросом при истечении)"""
    with get_db() as conn:
        user = conn.execute('SELECT vip_status, vip_until, full_name FROM users WHERE user_id = ?', (user_id,)).fetchone()
        if not user:
            return False
        if user[0] == 1:
            if user[1]:
                try:
                    vip_until = datetime.strptime(user[1], '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    try:
                        vip_until = datetime.strptime(user[1], '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        return True
                if vip_until > datetime.now():
                    return True
                else:
                    # VIP истёк, обновляем статус
                    conn.execute('UPDATE users SET vip_status = 0, vip_until = NULL WHERE user_id = ?', (user_id,))
                    conn.commit()
                    return False
            return True
        return False

async def is_vip_user_async(user_id):
    return await asyncio.to_thread(is_vip_user, user_id)

def get_user_settings(user_id):
    """Получить настройки пользователя"""
    with get_db() as conn:
        user = conn.execute('SELECT transfer_confirmation, transfer_commission, vip_status FROM users WHERE user_id = ?', (user_id,)).fetchone()
        if user:
            return {
                'transfer_confirmation': user[0] if user[0] is not None else 1,
                'transfer_commission': user[1] if user[1] is not None else 1,
                'vip_status': user[2] if user[2] is not None else 0
            }
        return {'transfer_confirmation': 1, 'transfer_commission': 1, 'vip_status': 0}

async def get_user_settings_async(user_id):
    return await asyncio.to_thread(get_user_settings, user_id)

def update_user_transfer_confirmation(user_id, enabled):
    """Обновить настройку подтверждения переводов"""
    with get_db() as conn:
        conn.execute('UPDATE users SET transfer_confirmation = ? WHERE user_id = ?', (1 if enabled else 0, user_id))
        conn.commit()

async def update_user_transfer_confirmation_async(user_id, enabled):
    return await asyncio.to_thread(update_user_transfer_confirmation, user_id, enabled)

def update_user_transfer_commission(user_id, enabled):
    """Обновить настройку комиссии (только для VIP)"""
    with get_db() as conn:
        conn.execute('UPDATE users SET transfer_commission = ? WHERE user_id = ?', (1 if enabled else 0, user_id))
        conn.commit()

async def update_user_transfer_commission_async(user_id, enabled):
    return await asyncio.to_thread(update_user_transfer_commission, user_id, enabled)

def give_vip(user_id, days=0, hours=0, minutes=0, seconds=0):
    """Выдача VIP статуса"""
    with get_db() as conn:
        vip_until = datetime.now() + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
        conn.execute('''
            UPDATE users 
            SET vip_status = 1, vip_until = ? 
            WHERE user_id = ?
        ''', (vip_until.strftime('%Y-%m-%d %H:%M:%S'), user_id))
        conn.commit()
        return True

async def give_vip_async(user_id, days=0, hours=0, minutes=0, seconds=0):
    return await asyncio.to_thread(give_vip, user_id, days, hours, minutes, seconds)

def check_expired_vips():
    """Проверка истекших VIP статусов и уведомление пользователей"""
    with get_db() as conn:
        # Находим пользователей с истекшим VIP
        expired = conn.execute('''
            SELECT user_id, full_name, username, vip_until 
            FROM users 
            WHERE vip_status = 1 AND vip_until IS NOT NULL AND vip_until < datetime('now')
        ''').fetchall()
        
        if expired:
            for user in expired:
                # Сбрасываем VIP статус
                conn.execute('''
                    UPDATE users 
                    SET vip_status = 0, vip_until = NULL 
                    WHERE user_id = ?
                ''', (user['user_id'],))
            
            conn.commit()
            
        return [dict(user) for user in expired]

async def check_expired_vips_async():
    return await asyncio.to_thread(check_expired_vips)

# database.py
async def check_donate_feature(user_id: int, feature_type: str) -> bool:
    """Проверяет, есть ли у пользователя активная донат-функция"""
    
    try:
        async with get_db() as db:
            cursor = await db.execute(
                'SELECT expires_at FROM donate_features WHERE user_id = ? AND feature_type = ?',
                (user_id, feature_type)
            )
            row = await cursor.fetchone()
            
            if row:
                expires_at = datetime.fromisoformat(row[0])
                if expires_at > datetime.now():
                    return True
                else:
                    # Если срок истек, удаляем запись
                    await db.execute(
                        'DELETE FROM donate_features WHERE user_id = ? AND feature_type = ?',
                        (user_id, feature_type)
                    )
                    await db.commit()
            
            return False
    except Exception as e:
        logger.error(f"Error checking donate feature: {e}")
        return False

def get_safe(user_id: int):
    """Получить данные сейфа пользователя"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM safes WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if not row:
            # Создаем сейф если нет
            cursor.execute(
                'INSERT INTO safes (user_id, balance, pin_code) VALUES (?, ?, ?)',
                (user_id, 0, None)
            )
            conn.commit()
            return {'user_id': user_id, 'balance': 0, 'pin_code': None}
        
        return dict(row)
    except Exception as e:
        logger.error(f"Error getting safe: {e}")
        return {'user_id': user_id, 'balance': 0, 'pin_code': None}  # Возвращаем словарь по умолчанию

def update_safe_balance(user_id: int, amount: int):
    """Обновить баланс сейфа"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE safes SET balance = balance + ? WHERE user_id = ?',
            (amount, user_id)
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating safe balance: {e}")
        return False

def set_safe_pin(user_id: int, pin_code: str):
    """Установить PIN-код сейфа"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE safes SET pin_code = ? WHERE user_id = ?',
            (pin_code, user_id)
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error setting safe pin: {e}")
        return False

def check_safe_pin(user_id: int, pin_code: str) -> bool:
    """Проверить PIN-код сейфа"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT pin_code FROM safes WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        
        if row and row['pin_code']:
            return row['pin_code'] == pin_code
        return False
    except Exception as e:
        logger.error(f"Error checking safe pin: {e}")
        return False

def has_safe_pin(user_id: int) -> bool:
    """Проверить установлен ли PIN-код"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT pin_code FROM safes WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        return row and row['pin_code'] is not None
    except Exception as e:
        logger.error(f"Error checking safe pin exists: {e}")
        return False

async def clear_user_portfolio_async(user_id: int) -> bool:
    """Очищает портфель пользователя"""
    try:
        import aiosqlite
        
        # ✅ Используем правильный путь к БД
        db_path = 'data/bot.db'  # как в database.py
        
        async with aiosqlite.connect(db_path) as db:
            # Создаём таблицу если её нет
            await db.execute('''
                CREATE TABLE IF NOT EXISTS user_portfolio (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    stock_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL DEFAULT 0,
                    purchase_price INTEGER NOT NULL,
                    purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, stock_id)
                )
            ''')
            
            await db.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_user ON user_portfolio(user_id)')
            await db.commit()
            
            await db.execute("DELETE FROM user_portfolio WHERE user_id = ?", (user_id,))
            await db.commit()
            
            return True
            
    except Exception as e:
        logging.error(f"Error clearing portfolio: {e}")
        return False


async def set_balance_async(user_id: int, new_balance: int) -> bool:
    """Устанавливает точный баланс пользователя"""
    try:
        import aiosqlite
        
        # ✅ Используем правильный путь к БД
        db_path = 'data/bot.db'
        
        MAX_BALANCE = 9_000_000_000_000_000_000
        if new_balance > MAX_BALANCE:
            new_balance = MAX_BALANCE
        
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            exists = await cursor.fetchone()
            
            if exists:
                await db.execute(
                    "UPDATE users SET balance = ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?",
                    (new_balance, user_id)
                )
            else:
                await db.execute(
                    "INSERT INTO users (user_id, balance, last_active) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (user_id, new_balance)
                )
            
            await db.commit()
            return True
            
    except Exception as e:
        logging.error(f"Error setting balance for user {user_id}: {e}")
        return False

# ============ ЧЕКОВАЯ КНИЖКА ============

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
        
        # Проверяем активации
        if check[5] >= check[4]:  # used_count >= max_activations
            return False, "no_activations"
        
        # Проверяем пароль
        if check[6] and check[6] != password:
            return False, "wrong_password"
        
        # Проверяем, не активировал ли уже пользователь этот чек
        cursor = await db.execute(
            "SELECT * FROM user_check_activations WHERE check_id = ? AND user_id = ?",
            (check_id, user_id)
        )
        if await cursor.fetchone():
            return False, "already_used"
        
        # Записываем активацию
        await db.execute(
            "INSERT INTO user_check_activations (check_id, user_id, activated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (check_id, user_id)
        )
        
        # Обновляем used_count
        await db.execute(
            "UPDATE checks_new SET used_count = used_count + 1 WHERE id = ?",
            (check_id,)
        )
        
        await db.commit()
        return True, check[3]  # amount


def init_easter_db():
    """Инициализация таблиц для пасхального ивента"""
    with sqlite3.connect('data/bot.db') as conn:
        # Таблица для ключей пользователей
        conn.execute('''
            CREATE TABLE IF NOT EXISTS easter_keys (
                user_id INTEGER PRIMARY KEY,
                keys INTEGER DEFAULT 0
            )
        ''')
        
        # Таблица для кулдауна игры
        conn.execute('''
            CREATE TABLE IF NOT EXISTS easter_cooldown (
                user_id INTEGER PRIMARY KEY,
                last_egg_time TEXT DEFAULT (datetime('now'))
            )
        ''')
        
        # Таблица для истории игр (опционально)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS easter_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                winner_id INTEGER,
                loser_id INTEGER,
                keys_won INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
    logging.info("✅ Easter database initialized")

# ============ ПАСХАЛЬНЫЙ ИВЕНТ ============

EGG_COOLDOWN_MINUTES = 10


async def update_egg_cooldown(user_id: int):
    """Обновляет время последнего использования яйца"""
    async with aiosqlite.connect('data/bot.db') as db:
        await db.execute('''
            INSERT INTO easter_cooldown (user_id, last_egg_time) 
            VALUES (?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET last_egg_time = CURRENT_TIMESTAMP
        ''', (user_id,))
        await db.commit()


async def get_easter_keys(user_id: int) -> int:
    """Получить количество святых ключей пользователя"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute(
            "SELECT keys FROM easter_keys WHERE user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return result[0] if result else 0


async def add_easter_keys(user_id: int, amount: int):
    """Добавить святые ключи пользователю"""
    async with aiosqlite.connect('data/bot.db') as db:
        await db.execute('''
            INSERT INTO easter_keys (user_id, keys) 
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET keys = keys + ?
        ''', (user_id, amount, amount))
        await db.commit()


async def get_easter_top(limit: int = 10) -> list:
    """Получить топ пользователей по ключам"""
    async with aiosqlite.connect('data/bot.db') as db:
        cursor = await db.execute('''
            SELECT user_id, keys FROM easter_keys 
            WHERE keys > 0 
            ORDER BY keys DESC 
            LIMIT ?
        ''', (limit,))
        return await cursor.fetchall()
