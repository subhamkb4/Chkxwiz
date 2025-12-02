import aiosqlite
import os
import json
from datetime import datetime

# === Default constants ===
DEFAULT_FREE_CREDITS = 200
DEFAULT_PLAN = "Free"
DEFAULT_STATUS = "Free"
DEFAULT_PLAN_EXPIRY = "N/A"
DEFAULT_KEYS_REDEEMED = 0

# === Database file path ===
DB_FILE = "bot_data.db"

# === Initialize DB ===
async def init_db():
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                credits INTEGER DEFAULT {DEFAULT_FREE_CREDITS},
                plan TEXT DEFAULT '{DEFAULT_PLAN}',
                status TEXT DEFAULT '{DEFAULT_STATUS}',
                plan_expiry TEXT DEFAULT '{DEFAULT_PLAN_EXPIRY}',
                keys_redeemed INTEGER DEFAULT {DEFAULT_KEYS_REDEEMED},
                registered_at TEXT,
                custom_urls TEXT DEFAULT '[]',
                serp_key TEXT UNIQUE
            )
        """)
        await conn.commit()

# === Normalize JSON fields ===
def normalize_json_field(value):
    if value is None:
        return []
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return []
    if isinstance(value, list):
        return value
    return []

# === Get or create user ===
async def get_user(user_id):
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        
        if row:
            user_data = dict(row)
            user_data["custom_urls"] = normalize_json_field(user_data.get("custom_urls"))
            return user_data
        else:
            now = datetime.now().strftime('%d-%m-%Y')
            await conn.execute(
                """
                INSERT INTO users (
                    id, credits, plan, status, plan_expiry, keys_redeemed, registered_at, custom_urls
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, DEFAULT_FREE_CREDITS, DEFAULT_PLAN, DEFAULT_STATUS, 
                 DEFAULT_PLAN_EXPIRY, DEFAULT_KEYS_REDEEMED, now, json.dumps([]))
            )
            await conn.commit()
            
            async with conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
            user_data = dict(row)
            user_data["custom_urls"] = []
            return user_data

# === Update user fields ===
async def update_user(user_id, **kwargs):
    if not kwargs:
        return
    
    async with aiosqlite.connect(DB_FILE) as conn:
        sets = []
        values = []
        
        for k, v in kwargs.items():
            if k == "custom_urls":
                sets.append(f"{k} = ?")
                values.append(json.dumps(v))
            else:
                sets.append(f"{k} = ?")
                values.append(v)
        
        values.append(user_id)
        query = f"UPDATE users SET {', '.join(sets)} WHERE id = ?"
        await conn.execute(query, values)
        await conn.commit()

# === Get all users ===
async def get_all_users():
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id, plan, custom_urls, serp_key FROM users") as cursor:
            rows = await cursor.fetchall()
        
        result = []
        for row in rows:
            r = dict(row)
            r["custom_urls"] = normalize_json_field(r.get("custom_urls"))
            result.append(r)
        return result

# === Get total user count ===
async def get_user_count():
    async with aiosqlite.connect(DB_FILE) as conn:
        async with conn.execute("SELECT COUNT(*) FROM users") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

# === SERP key functions ===
async def set_serp_key(user_id: int, serp_key: str) -> bool:
    """
    Save a SERP key for a user.
    Returns True if success, False if the key already belongs to another user.
    """
    async with aiosqlite.connect(DB_FILE) as conn:
        try:
            async with conn.execute("SELECT id FROM users WHERE id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
            
            if not row:
                now = datetime.now().strftime('%d-%m-%Y')
                await conn.execute(
                    """
                    INSERT INTO users (
                        id, credits, plan, status, plan_expiry, keys_redeemed, registered_at, custom_urls, serp_key
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, DEFAULT_FREE_CREDITS, DEFAULT_PLAN, DEFAULT_STATUS,
                     DEFAULT_PLAN_EXPIRY, DEFAULT_KEYS_REDEEMED, now, json.dumps([]), serp_key)
                )
            else:
                await conn.execute("UPDATE users SET serp_key = ? WHERE id = ?", (serp_key, user_id))
            
            await conn.commit()
            return True
        except Exception as e:
            if "unique" in str(e).lower():
                return False
            raise

async def get_serp_key(user_id: int):
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT serp_key FROM users WHERE id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        return row["serp_key"] if row and row["serp_key"] else None

async def delete_serp_key(user_id: int) -> bool:
    """
    Remove a user's serp_key.
    Returns True if a key was deleted, False if no key existed.
    """
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT serp_key FROM users WHERE id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        
        if not row or not row["serp_key"]:
            return False
        
        await conn.execute("UPDATE users SET serp_key = NULL WHERE id = ?", (user_id,))
        await conn.commit()
        return True

# Alias (optional, for compatibility with your older clear_serp_key usage)
clear_serp_key = delete_serp_key

async def serp_key_exists(serp_key: str, exclude_user: int = None) -> bool:
    async with aiosqlite.connect(DB_FILE) as conn:
        if exclude_user:
            async with conn.execute(
                "SELECT id FROM users WHERE serp_key = ? AND id <> ?",
                (serp_key, exclude_user)
            ) as cursor:
                row = await cursor.fetchone()
        else:
            async with conn.execute("SELECT id FROM users WHERE serp_key = ?", (serp_key,)) as cursor:
                row = await cursor.fetchone()
        
        return bool(row)
