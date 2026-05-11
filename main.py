
import asyncio
import random
import string
import psutil
from datetime import datetime, timedelta
import pyrogram
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.raw.functions.channels import CreateForumTopic
from pyrogram.raw.types import InputChannel
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram.errors import FloodWait, PeerIdInvalid

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_ID = 20137104
API_HASH = "1209338eedc55ab701dd2e9d353c05ad"
BOT_TOKEN = "" # Master Engine Token
MONGO_URI = "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies"
SPECIAL_GROUP_ID = -1003667939361
ADMIN_ID = 5050578106  

# ==========================================
# 🗄️ INITIALIZATION & DATABASE
# ==========================================
app = Client("master_engine_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

db_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=20000, maxIdleTimeMS=50000)
db = db_client["telegram_file_bot"]
connections_db = db["channel_connections"]
stats_db = db["bot_stats"] 
viewer_stats_db = db["viewer_stats"] 
users_db = db["all_users"] 
links_db = db["short_links"] 
sudo_db = db["sudo_users"] 
daily_access_db = db["daily_access_tracker"]
hosted_bots_db = db["hosted_bots"] 

message_queue = asyncio.Queue()
TOPIC_LOCKS = {} 
active_clients = {} 
USER_STATES = {} # Interactive UI ke liye track karega

# ==========================================
# 🛡️ SUDO SYSTEM & PEER FIXER
# ==========================================
async def is_sudo(user_id):
    if user_id == ADMIN_ID: return True
    user = await sudo_db.find_one({"user_id": user_id})
    if not user: return False
    if user.get("expiry_date") and user["expiry_date"] < datetime.utcnow(): return False
    return True

async def cache_channels(client):
    try:
        cursor = connections_db.find({})
        async for conn in cursor:
            try:
                await client.get_chat(conn["private_channel_id"])
                await client.get_chat(conn["public_channel_id"])
            except Exception: pass
            await asyncio.sleep(0.5)
    except Exception: pass


# ==========================================
# 1️⃣ USER COMMANDS & DM BOT LOGIC
# ==========================================
async def start_handler(client, message):
    text = message.text
    viewer_id = message.from_user.id
    viewer_name = message.from_user.first_name or "Unknown User"
    
    await users_db.update_one({"user_id": viewer_id}, {"$set": {"name": viewer_name}}, upsert=True)
    
    # 📌 VIDEO FETCH LOGIC (Untouched, Limit check with daily counter)
    if len(text.split()) > 1:
        short_code = text.split()[1]
        try:
            link_data = await links_db.find_one({"short_code": short_code})
            if not link_data:
                return await message.reply_text("❌ <b>Sorry, this link has expired or does not exist.</b>", parse_mode=ParseMode.HTML)
            p_chat_id = link_data["chat_id"]
            msg_id = link_data["msg_id"]

            connection = await connections_db.find_one({"private_channel_id": p_chat_id})
            if connection:
                owner_id = connection["user_id"]
                channel_name = connection.get("channel_name", "Unknown Channel")
                
                if viewer_id != owner_id and viewer_id != ADMIN_ID:
                    active_limit = -1 
                    if "custom_limit" in connection: active_limit = connection["custom_limit"] 
                    else:
                        owner_data = await sudo_db.find_one({"user_id": owner_id})
                        if owner_data and "global_daily_limit" in owner_data: active_limit = owner_data["global_daily_limit"]
                    
                    if active_limit > 0:
                        today_date = datetime.utcnow().strftime("%Y-%m-%d")
                        access_record = await daily_access_db.find_one({"viewer_id": viewer_id, "channel_id": p_chat_id, "date": today_date})
                        current_count = access_record.get("count", 0) if access_record else 0

                        if current_count >= active_limit:
                            return await message.reply_text(f"🚫 <b>Daily Limit Reached!</b>\n\nYou have watched your maximum limit of <b>{active_limit} videos</b> for today from <b>{channel_name}</b>.\n⏳ <i>Please come back tomorrow!</i>", parse_mode=ParseMode.HTML)
                        await daily_access_db.update_one({"viewer_id": viewer_id, "channel_id": p_chat_id, "date": today_date}, {"$inc": {"count": 1}}, upsert=True)
                
                await viewer_stats_db.update_one({"owner_id": owner_id, "viewer_id": viewer_id, "channel_name": channel_name}, {"$inc": {"view_count": 1}, "$set": {"viewer_name": viewer_name}}, upsert=True)
            
            await client.copy_message(chat_id=message.chat.id, from_chat_id=p_chat_id, message_id=msg_id, protect_content=True)
            await stats_db.update_one({"type": "global"}, {"$inc": {"total_video_views": 1}}, upsert=True)
            await stats_db.update_one({"user_id": viewer_id}, {"$inc": {"videos_accessed": 1}}, upsert=True)
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await message.reply_text("⏳ <b>Server is busy. Please try again.</b>", parse_mode=ParseMode.HTML)
        except Exception as e:
            await message.reply_text("❌ <b>Sorry, a technical error occurred.</b>", parse_mode=ParseMode.HTML)
    else:
        # 🎨 NEW BUTTON DASHBOARD (Sab commands ke exact buttons)
        if await is_sudo(viewer_id):
            buttons = [
                [InlineKeyboardButton("➕ Connect Channels", callback_data="btn_connect")],
                [InlineKeyboardButton("⚙️ Global Daily Limit", callback_data="btn_global_limit"), InlineKeyboardButton("⚙️ Channel Limit", callback_data="btn_channel_limit")],
                [InlineKeyboardButton("👥 Video Access", callback_data="btn_videoaccess_0"), InlineKeyboardButton("📊 Status", callback_data="btn_status")],
                [InlineKeyboardButton("🧹 Clean Messages", callback_data="btn_deleteall")]
            ]
            if viewer_id == ADMIN_ID:
                buttons.append([InlineKeyboardButton("🤖 Engine Dashboard", callback_data="btn_engine"), InlineKeyboardButton("👑 Sudo List", callback_data="btn_sudolist")])
            await message.reply_text(f"🚀 <b>Welcome {viewer_name}!</b>\n\n🎛 <b><u>Main Control Panel:</u></b>", reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("⚙️ <b><u>How to Setup:</u></b>\n\n<b>1.</b> Add me as an <b>Admin</b> in both your Private & Public channels.\n<b>2.</b> You need an active Sudo Subscription.", parse_mode=ParseMode.HTML)

# 📄 PAGINATED VIDEO ACCESS
async def send_paginated_videoaccess(client, message, owner_id, page=0, is_edit=False):
    ITEMS_PER_PAGE = 10
    stats = await viewer_stats_db.find({"owner_id": owner_id}).sort("view_count", -1).to_list(length=None)
    if not stats:
        text = "📉 <b>No views recorded yet.</b>"
        return await message.edit_text(text, parse_mode=ParseMode.HTML) if is_edit else await message.reply_text(text, parse_mode=ParseMode.HTML)

    channel_data = {}
    for stat in stats:
        c_name = stat.get("channel_name", "Unknown Channel")
        if c_name not in channel_data: channel_data[c_name] = []
        channel_data[c_name].append(stat)

    all_lines = []
    c_idx = 1
    for c_name, viewers in channel_data.items():
        all_lines.append(f"<b>{c_idx}. 📢 Channel:</b> <code>{c_name}</code>")
        for v_idx, v in enumerate(viewers, 1):
            all_lines.append(f"   ├ <b>{v_idx}.</b> {v.get('viewer_name', 'Unknown')} - <b>{v.get('view_count', 0)} Videos</b>")
        all_lines.append("━━━━━━━━━━━━━━━━━━━━")
        c_idx += 1

    total_pages = max(1, (len(all_lines) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    page_lines = all_lines[page * ITEMS_PER_PAGE : (page + 1) * ITEMS_PER_PAGE]

    text = "👥 <b><u>Viewer Data:</u></b>\n\n" + "\n".join(page_lines) + f"\n\n💡 <i>Page {page+1} of {total_pages} (Total Unique Viewers: {len(stats)})</i>"
    buttons = []
    if page > 0: buttons.append(InlineKeyboardButton("⬅️ Back", callback_data=f"btn_videoaccess_{page-1}"))
    if page < total_pages - 1: buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"btn_videoaccess_{page+1}"))
    
    markup = InlineKeyboardMarkup([buttons]) if buttons else None
    if is_edit: await message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else: await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
# ==========================================
# 🎛 BUTTON CLICKS & INTERACTIVE STATE MACHINE
# ==========================================
async def handle_callbacks(client, callback_query):
    data = callback_query.data
    user_id = callback_query.from_user.id
    msg = callback_query.message

    if data.startswith("vid_"):
        bot_info = await client.get_me()
        return await callback_query.answer(url=f"https://t.me/{bot_info.username}?start={data.replace('vid_', '')}")

    if not await is_sudo(user_id): return await callback_query.answer("❌ Sudo Required!", show_alert=True)

    # 1. Connect Channels Button
    if data == "btn_connect":
        USER_STATES[user_id] = {"state": "connect_source"}
        await msg.reply_text("📡 <b>STEP 1/2: Connect Channels</b>\n\nPlease send me your <b>Private / Source Channel ID</b> (e.g. -100123456789).", parse_mode=ParseMode.HTML)
        await callback_query.answer()

    # 2. Global Limit Button
    elif data == "btn_global_limit":
        USER_STATES[user_id] = {"state": "global_limit"}
        await msg.reply_text("⚙️ <b>Set Global Daily Limit</b>\n\nPlease send me a number (e.g. <code>5</code>). Send <code>0</code> for Unlimited.", parse_mode=ParseMode.HTML)
        await callback_query.answer()

    # 3. Channel Limit Button
    elif data == "btn_channel_limit":
        USER_STATES[user_id] = {"state": "channel_limit"}
        await msg.reply_text("⚙️ <b>Set Specific Channel Limit</b>\n\nPlease send the Channel ID and Limit together.\nExample: <code>-100123456789 5</code>\n(Or send <code>-100123456789 default</code> to remove).", parse_mode=ParseMode.HTML)
        await callback_query.answer()

    # 4. Clean Channel Button
    elif data == "btn_deleteall":
        USER_STATES[user_id] = {"state": "clean_channel"}
        await msg.reply_text("🧹 <b>Clean Channel Messages</b>\n\nPlease send the <b>Channel ID</b> you want to clean.", parse_mode=ParseMode.HTML)
        await callback_query.answer()

    # 5. Video Access List
    elif data.startswith("btn_videoaccess_"):
        page = int(data.split("_")[-1])
        await send_paginated_videoaccess(client, msg, user_id, page, is_edit=True)
        await callback_query.answer()

    # 6. Status & Dashboard
    elif data == "btn_status":
        await status_handler(client, msg)
        await callback_query.answer()
    elif data == "btn_engine":
        await engine_dashboard(client, msg)
        await callback_query.answer()
    elif data == "btn_sudolist":
        await sudolist_handler(client, msg, is_callback=True)
        await callback_query.answer()

    # Engine Toggle On/Off
    elif data.startswith("toggle_"):
        if user_id != ADMIN_ID: return
        bot_id = data.replace("toggle_", "")
        bot_data = await hosted_bots_db.find_one({"bot_id": bot_id})
        if bot_data["status"] == "on":
            await hosted_bots_db.update_one({"bot_id": bot_id}, {"$set": {"status": "off"}})
            if bot_id in active_clients:
                await active_clients[bot_id].stop()
                del active_clients[bot_id]
        else:
            await hosted_bots_db.update_one({"bot_id": bot_id}, {"$set": {"status": "on"}})
            await start_slave_client(bot_data["bot_token"])
        await engine_dashboard(client, msg, is_edit=True)


# 🧠 TEXT LISTENER FOR BUTTONS (The Brain)
async def text_state_handler(client, message):
    user_id = message.from_user.id
    if user_id in USER_STATES:
        state_data = USER_STATES[user_id]
        state = state_data["state"]

        # Handle Connect
        if state == "connect_source":
            try:
                USER_STATES[user_id] = {"state": "connect_target", "source": int(message.text)}
                await message.reply_text("✅ <b>Source ID Saved!</b>\n\n📡 <b>STEP 2/2:</b> Now send your <b>Public / Target Channel ID</b>.", parse_mode=ParseMode.HTML)
            except ValueError: await message.reply_text("❌ Please send a valid numeric ID.")
        elif state == "connect_target":
            try:
                target_id, source_id = int(message.text), state_data["source"]
                del USER_STATES[user_id]
                try:
                    priv_info = await client.get_chat(source_id)
                    await client.get_chat(target_id)
                except Exception: return await message.reply_text("❌ Error! Bot must be Admin in BOTH channels.")
                await connections_db.update_one({"private_channel_id": source_id}, {"$set": {"user_id": user_id, "public_channel_id": target_id, "channel_name": priv_info.title}}, upsert=True)
                await message.reply_text("✅ <b>Channels Successfully Connected!</b>", parse_mode=ParseMode.HTML)
            except ValueError: await message.reply_text("❌ Please send a valid numeric ID.")

        # Handle Global Limit
        elif state == "global_limit":
            try:
                limit = int(message.text)
                if limit < 0: raise ValueError
                del USER_STATES[user_id]
                await sudo_db.update_one({"user_id": user_id}, {"$set": {"global_daily_limit": limit}}, upsert=True)
                msg_txt = f"✅ Global Limit Set to {limit} videos/day." if limit > 0 else "✅ Global Limit Removed (Unlimited)."
                await message.reply_text(msg_txt)
            except ValueError: await message.reply_text("❌ Invalid Number.")

        # Handle Channel Limit
        elif state == "channel_limit":
            args = message.text.split()
            if len(args) != 2: return await message.reply_text("❌ Format: `-100123456789 5`")
            try:
                channel_id, limit_str = int(args[0]), args[1].lower()
                conn = await connections_db.find_one({"private_channel_id": channel_id, "user_id": user_id})
                if not conn: return await message.reply_text("❌ Channel not found or not yours.")
                del USER_STATES[user_id]
                if limit_str == "default":
                    await connections_db.update_one({"private_channel_id": channel_id}, {"$unset": {"custom_limit": ""}})
                    await message.reply_text("✅ Custom channel limit removed.")
                else:
                    await connections_db.update_one({"private_channel_id": channel_id}, {"$set": {"custom_limit": int(limit_str)}})
                    await message.reply_text(f"✅ Limit set to {limit_str} for this channel.")
            except ValueError: await message.reply_text("❌ Invalid format.")

        # Handle Clean Channel
        elif state == "clean_channel":
            try:
                channel_id = int(message.text)
                del USER_STATES[user_id]
                await deleteall_handler(client, message, override_id=channel_id)
            except ValueError: await message.reply_text("❌ Invalid Channel ID.")

# ==========================================
# 🛑 TEXT COMMANDS FALLBACK & ADMIN DATA
# ==========================================
async def status_handler(client, message):
    user_id = message.from_user.id
    cpu_usage, ram_usage = psutil.cpu_percent(interval=0.5), psutil.virtual_memory().percent
    global_data = await stats_db.find_one({"type": "global"}) or {}
    total_views, total_files = global_data.get("total_video_views", 0), global_data.get("total_files_processed", 0)

    stats_msg = f"📊 <b><u>BOT STATUS</u></b> 📊\n🖥 <b>CPU:</b> `{cpu_usage}%` | <b>RAM:</b> `{ram_usage}%`\n🌐 <b>Total Files:</b> `{total_files}` | <b>Total Views:</b> `{total_views}`\n\n"
    if user_id == ADMIN_ID:
        connections = await connections_db.find({}).to_list(length=None)
        user_channels = {}
        for conn in connections:
            uid = conn["user_id"]
            if uid not in user_channels: user_channels[uid] = []
            user_channels[uid].append(conn.get("channel_name", "Unknown Channel"))
        for uid, channels in user_channels.items():
            stats_msg += f"👤 <b>Owner:</b> `{uid}` | 📢 <b>Channels:</b> {len(channels)}\n"
    else:
        u_channels = await connections_db.count_documents({"user_id": user_id})
        sudo_u = await sudo_db.find_one({"user_id": user_id})
        expiry_txt = "Unknown"
        if sudo_u and sudo_u.get("expiry_date"): expiry_txt = f"{(sudo_u['expiry_date'] - datetime.utcnow()).days} Days"
        stats_msg += f"👤 <b>Your Channels:</b> `{u_channels}`\n⏳ <b>Subscription:</b> `{expiry_txt}`"

    if len(stats_msg) > 4000: stats_msg = stats_msg[:4000] + "..."
    await message.reply_text(stats_msg, parse_mode=ParseMode.HTML)

async def deleteall_handler(client, message, override_id=None):
    owner_id = message.from_user.id
    channel_id = override_id
    if not channel_id:
        args = message.text.split()
        if len(args) < 2: return await message.reply_text("❌ `/deleteall -100ID`")
        try: channel_id = int(args[1])
        except: return await message.reply_text("❌ Invalid ID.")

    status_msg = await message.reply_text("⏳ <b>Scanning channel...</b>", parse_mode=ParseMode.HTML)
    try:
        dummy_msg = await client.send_message(channel_id, "<i>Cleaning in progress...</i>")
        latest_msg_id = dummy_msg.id
        await status_msg.edit_text(f"⏳ <b>Cleaning started...</b> IDs: {latest_msg_id}")
        for i in range(latest_msg_id, 0, -100):
            message_ids = list(range(i, max(0, i - 100), -1))
            try:
                await client.delete_messages(channel_id, message_ids)
                await asyncio.sleep(2.5)
            except FloodWait as e: await asyncio.sleep(e.value)
            except Exception: pass
        await status_msg.edit_text("✅ <b>Mission Successful!</b> Channel clean.")
    except Exception: await status_msg.edit_text("❌ Error! Cache issue.")

async def engine_dashboard(client, message, is_edit=False):
    bots = await hosted_bots_db.find().to_list(length=None)
    if not bots: return await message.reply_text("🛠 No bots. Use `/addnewbot <token>`")
    text = "🛠 <b><u>Engine Dashboard:</u></b>\n\n"
    buttons = []
    for b in bots:
        b_id, status = b["bot_id"], "🟢" if b["status"] == "on" else "🔴"
        text += f"🤖 ID: `{b_id}` - {status}\n"
        buttons.append([InlineKeyboardButton(f"Turn {'OFF' if status=='🟢' else 'ON'} {b_id}", callback_data=f"toggle_{b_id}")])
    markup = InlineKeyboardMarkup(buttons)
    if is_edit: await message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else: await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

# Master Add/Remove Sudo
async def addsudo_handler(client, message):
    if message.from_user.id != ADMIN_ID: return
    args = message.text.split()
    if len(args) < 2: return await message.reply_text("❌ `/addsudo UserID [Days]`")
    try:
        target_id, days = int(args[1]), int(args[2]) if len(args) > 2 else 30
        expiry_date = datetime.utcnow() + timedelta(days=days)
        await sudo_db.update_one({"user_id": target_id}, {"$set": {"expiry_date": expiry_date, "last_notified": None}}, upsert=True)
        await message.reply_text(f"✅ <b>Sudo Added!</b> `{target_id}` for `{days}` days.")
    except: pass

async def sudolist_handler(client, message, is_callback=False):
    users = await sudo_db.find({}).to_list(length=None)
    if not users: text = "📋 No Sudo Users."
    else:
        text = "👑 <b><u>SUDO USERS</u></b>\n\n"
        for c, u in enumerate(users, 1):
            exp = u.get("expiry_date")
            status = f"🟢 {(exp - datetime.utcnow()).days} Days" if exp and (exp - datetime.utcnow()).days >= 0 else ("🔴 Expired" if exp else "♾️ Lifetime")
            text += f"<b>{c}. User:</b> `{u['user_id']}`\n   └ {status}\n\n"
    if is_callback: await message.edit_text(text, parse_mode=ParseMode.HTML)
    else: await message.reply_text(text, parse_mode=ParseMode.HTML)



# ==========================================
# 2️⃣ MESSAGE CATCHER (For Forum Topic)
# ==========================================
async def enqueue_message(client, message):
    chat_id = message.chat.id
    connection = await connections_db.find_one({"private_channel_id": chat_id})
    if connection:
        owner_id = connection.get("user_id")
        if not await is_sudo(owner_id): return 
        topic_id = connection.get("topic_id")
        if not topic_id:
            if chat_id in TOPIC_LOCKS:
                await asyncio.sleep(3)
                recheck = await connections_db.find_one({"private_channel_id": chat_id})
                topic_id = recheck.get("topic_id") if recheck else None
            if not topic_id:
                TOPIC_LOCKS[chat_id] = True 
                try:
                    peer = await client.resolve_peer(SPECIAL_GROUP_ID)
                    raw_result = await client.invoke(CreateForumTopic(channel=InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash), title=connection.get("channel_name", f"Ch {chat_id}")[:128], random_id=random.randint(100000, 999999999)))
                    if hasattr(raw_result, 'updates'):
                        for upd in raw_result.updates:
                            if hasattr(upd, 'message') and hasattr(upd.message, 'id'): topic_id = upd.message.id; break
                            elif hasattr(upd, 'id'): topic_id = upd.id; break
                    if topic_id: await connections_db.update_one({"private_channel_id": chat_id}, {"$set": {"topic_id": topic_id}})
                except Exception: pass
                finally: TOPIC_LOCKS.pop(chat_id, None)

        await message_queue.put({"client": client, "message": message, "public_id": connection["public_channel_id"], "topic_id": topic_id})

# ==========================================
# 3️⃣ BACKGROUND WORKERS & ALERTS
# ==========================================
async def process_queue():
    while True:
        if message_queue.empty():
            await asyncio.sleep(1)
            continue
        await asyncio.sleep(2.5) 
        batch_items = []
        while not message_queue.empty(): batch_items.append(await message_queue.get())
        batch_items.sort(key=lambda x: x["message"].id)
        
        for item in batch_items:
            bot_client, message, public_channel_id, topic_id = item["client"], item["message"], item["public_id"], item["topic_id"]
            try:
                msg_id, chat_id = message.id, message.chat.id
                try:
                    if topic_id: await bot_client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id, reply_to_message_id=int(topic_id))
                    else: await bot_client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    await bot_client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except Exception: pass

                is_video = bool(message.video or (message.document and message.document.file_name and message.document.file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm'))))

                try:
                    if is_video:
                        caption = message.caption if message.caption else "🎬 <b>New Video Uploaded!</b>\n\n<i>Click below to watch.</i>"
                        short_code = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                        await links_db.insert_one({"short_code": short_code, "chat_id": chat_id, "msg_id": msg_id})
                        btn = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Video", callback_data=f"vid_{short_code}")]])
                        await bot_client.send_message(chat_id=public_channel_id, text=caption, reply_markup=btn, parse_mode=ParseMode.HTML, protect_content=True)
                    else: await bot_client.copy_message(chat_id=public_channel_id, from_chat_id=chat_id, message_id=msg_id)
                except FloodWait as e: await asyncio.sleep(e.value)
                await stats_db.update_one({"type": "global"}, {"$inc": {"total_files_processed": 1}}, upsert=True)
            except: pass
            finally: await asyncio.sleep(2) 

async def check_expirations():
    while True:
        try:
            now = datetime.utcnow()
            expiring_users = await sudo_db.find({"expiry_date": {"$lte": now + timedelta(days=3), "$gt": now}}).to_list(length=None)
            for user in expiring_users:
                try:
                    user_id = user["user_id"]
                    if not user.get("last_notified") or (now - user.get("last_notified")).total_seconds() > 82800:
                        days_left, hours_left = (user["expiry_date"] - now).days, int((user["expiry_date"] - now).seconds / 3600)
                        txt = f"<b>{days_left} days and {hours_left} hours</b>" if days_left > 0 else f"<b>{hours_left} hours</b>"
                        msg = await app.send_message(user_id, f"⚠️ <b><u>SUBSCRIPTION ALERT</u></b> ⚠️\n\nSudo access expiring in less than {txt}.\n⏳ <b>Please renew!</b>", parse_mode=ParseMode.HTML)
                        try: await msg.pin(both_sides=True)
                        except Exception: pass
                        await sudo_db.update_one({"user_id": user_id}, {"$set": {"last_notified": now}})
                except Exception: pass
        except Exception: pass
        await asyncio.sleep(21600) 

# ==========================================
# 🚀 BINDING & RUNNER
# ==========================================
def bind_bot_handlers(bot_client):
    # Registering all Handlers
    bot_client.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
    bot_client.add_handler(MessageHandler(status_handler, filters.command("status") & filters.private))
    bot_client.add_handler(MessageHandler(deleteall_handler, filters.command("deleteall") & filters.private))
    bot_client.add_handler(MessageHandler(addsudo_handler, filters.command("addsudo") & filters.private))
    bot_client.add_handler(MessageHandler(sudolist_handler, filters.command("sudolist") & filters.private))
    
    # Core Button Interactive Text Catcher
    bot_client.add_handler(MessageHandler(text_state_handler, filters.text & filters.private & ~filters.command(["start", "status", "deleteall", "addsudo", "sudolist", "addnewbot"])))
    
    bot_client.add_handler(CallbackQueryHandler(handle_callbacks, filters.regex(r"^(vid_|btn_|toggle_)")))
    bot_client.add_handler(MessageHandler(enqueue_message, filters.channel))

async def start_slave_client(token):
    bot_id = token.split(":")[0]
    slave_client = Client(f"session_{bot_id}", api_id=API_ID, api_hash=API_HASH, bot_token=token)
    bind_bot_handlers(slave_client)
    await slave_client.start()
    active_clients[bot_id] = slave_client
    print(f"🟢 Started Engine Slave: {bot_id}")
    asyncio.create_task(cache_channels(slave_client)) 

@app.on_message(filters.command("addnewbot") & filters.private)
async def addnewbot_cmd(client, message):
    if message.from_user.id != ADMIN_ID: return
    try:
        token = message.text.split()[1]
        bot_id = token.split(":")[0]
        await hosted_bots_db.update_one({"bot_id": bot_id}, {"$set": {"bot_token": token, "status": "on"}}, upsert=True)
        await message.reply_text(f"⏳ Starting {bot_id}...")
        await start_slave_client(token)
        await message.reply_text("✅ Bot Hosted Successfully!")
    except Exception as e: await message.reply_text(f"❌ Error: {e}")

async def main():
    print("🚀 Auto File Store ENGINE BOT is starting...")
    bind_bot_handlers(app) 
    app.add_handler(MessageHandler(addnewbot_cmd, filters.command("addnewbot") & filters.private))
    await app.start()
    asyncio.create_task(cache_channels(app)) 
    
    cursor = hosted_bots_db.find({"status": "on"})
    bots_list = await cursor.to_list(length=None)
    for b in bots_list:
        try: await start_slave_client(b["bot_token"])
        except Exception: pass

    asyncio.create_task(process_queue())
    asyncio.create_task(check_expirations())
    await pyrogram.idle()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

