
import asyncio
import random
import string
import psutil
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.raw.functions.channels import CreateForumTopic
from pyrogram.raw.types import InputChannel
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram.errors import FloodWait, PeerIdInvalid, UsernameNotOccupied, ChatAdminRequired

# ⚙️ CONFIGURATION
API_ID = 20137104
API_HASH = "1209338eedc55ab701dd2e9d353c05ad"
BOT_TOKEN = "8704548125:AAHXgiG0OHUffQkqrlz4qoSDjJOWXUp5PkE"
MONGO_URI = "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies"
SPECIAL_GROUP_ID = -1003667939361
ADMIN_ID = 5050578106

app = Client("multi_user_store_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
db_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=20000, maxIdleTimeMS=50000)
db = db_client["telegram_file_bot"]
connections_db = db["channel_connections"]
stats_db = db["bot_stats"] 
viewer_stats_db = db["viewer_stats"] 
users_db = db["all_users"] 
links_db = db["short_links"] 
sudo_db = db["sudo_users"] 
daily_access_db = db["daily_access_tracker"]

message_queue = asyncio.Queue()
BOT_USERNAME = None
TOPIC_LOCKS = {} 
bot_data = {}  # For multi-bot engine

# Cache for channel peers (Fix PeerIdInvalid)
channel_peer_cache = {}

async def get_cached_peer(chat_id):
    """Get cached peer to fix PeerIdInvalid"""
    if chat_id not in channel_peer_cache:
        try:
            peer = await app.resolve_peer(chat_id)
            channel_peer_cache[chat_id] = peer
        except:
            pass
    return channel_peer_cache.get(chat_id)

# 🛡️ SUDO VERIFICATION SYSTEM
async def is_sudo(user_id):
    if user_id == ADMIN_ID:
        return True
    user = await sudo_db.find_one({"user_id": user_id})
    if not user:
        return False
    if user.get("expiry_date") and user["expiry_date"] < datetime.utcnow():
        return False
    return True

# ==========================================
# 🆕 NEW COMMANDS
# ==========================================

@app.on_message(filters.command("viwesudolist") & filters.private)
async def viewsudo_list(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    
    sudo_users = await sudo_db.find({}).sort("expiry_date", -1).to_list(length=None)
    
    if not sudo_users:
        return await message.reply_text("📋 <b>No Sudo Users Found!</b>", parse_mode=ParseMode.HTML)
    
    text = "👑 <b>SUDO USERS LIST</b>\n\n"
    idx = 1
    buttons = []
    
    for user in sudo_users:
        uid = user["user_id"]
        expiry = user.get("expiry_date", datetime.utcnow())
        days_left = max(0, (expiry - datetime.utcnow()).days)
        
        status = "✅ Active" if expiry > datetime.utcnow() else "❌ Expired"
        text += f"{idx}. <code>{uid}</code> - <b>{days_left} days</b> {status}\n"
        idx += 1
        
        if idx % 10 == 0:  # Pagination every 10 users
            buttons.append([InlineKeyboardButton(f"Page {len(buttons)+1}", callback_data=f"sudolist_{len(buttons)}")])
    
    keyboard = InlineKeyboardMarkup(buttons)
    await message.reply_text(text[:4000], reply_markup=keyboard, parse_mode=ParseMode.HTML)

# ==========================================
# 1️⃣ ENHANCED START COMMAND WITH BUTTONS
# ==========================================
@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    global BOT_USERNAME
    if BOT_USERNAME is None:
        bot_info = await app.get_me()
        BOT_USERNAME = bot_info.username

    text = message.text
    viewer_id = message.from_user.id
    viewer_name = message.from_user.first_name or "Unknown User"
    
    await users_db.update_one({"user_id": viewer_id}, {"$set": {"name": viewer_name}}, upsert=True)
    
    # 📌 VIDEO FETCH LOGIC (UNCHANGED)
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
                    if "custom_limit" in connection:
                        active_limit = connection["custom_limit"]
                    else:
                        owner_data = await sudo_db.find_one({"user_id": owner_id})
                        if owner_data and "global_daily_limit" in owner_data:
                            active_limit = owner_data["global_daily_limit"]
                    
                    if active_limit > 0:
                        today_date = datetime.utcnow().strftime("%Y-%m-%d")
                        access_record = await daily_access_db.find_one({
                            "viewer_id": viewer_id,
                            "channel_id": p_chat_id, 
                            "date": today_date
                        })
                        current_count = access_record.get("count", 0) if access_record else 0
                        if current_count >= active_limit:
                            return await message.reply_text(
                                f"🚫 <b>Daily Limit Reached!</b>\n\nYou have watched your maximum limit of <b>{active_limit} videos</b> for today from <b>{channel_name}</b>.\n⏳ <i>Please come back tomorrow!</i>",
                                parse_mode=ParseMode.HTML
                            )
                        await daily_access_db.update_one(
                            {"viewer_id": viewer_id, "channel_id": p_chat_id, "date": today_date},
                            {"$inc": {"count": 1}},
                            upsert=True
                        )
                
                await viewer_stats_db.update_one(
                    {"owner_id": owner_id, "viewer_id": viewer_id, "channel_name": channel_name},
                    {"$inc": {"view_count": 1}, "$set": {"viewer_name": viewer_name}},
                    upsert=True
                )
            
            await client.copy_message(
                chat_id=message.chat.id, 
                from_chat_id=p_chat_id, 
                message_id=msg_id,
                protect_content=True 
            )
            
            await stats_db.update_one({"type": "global"}, {"$inc": {"total_video_views": 1}}, upsert=True)
            await stats_db.update_one({"user_id": viewer_id}, {"$inc": {"videos_accessed": 1}}, upsert=True)
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await message.reply_text("⏳ <b>Server is busy. Please try again in a few seconds.</b>", parse_mode=ParseMode.HTML)
        except Exception as e:
            await message.reply_text("❌ <b>Sorry, a technical error occurred.</b>", parse_mode=ParseMode.HTML)
    else:
        # 🆕 ENHANCED INTERACTIVE UI
        if await is_sudo(viewer_id):
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Connect Channels", callback_data="connect_menu")],
                [InlineKeyboardButton("📊 My Stats", callback_data="my_stats")],
                [InlineKeyboardButton("👥 Video Access", callback_data="video_access")],
                [InlineKeyboardButton("⚙️ Daily Limit", callback_data="daily_limit")],
                [InlineKeyboardButton("🧹 Delete All", callback_data="delete_all")],
                [InlineKeyboardButton("📈 Channel Limit", callback_data="channel_limit")]
            ])
        else:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Me To Channel", url=f"https://t.me/{BOT_USERNAME}?startchannel=admin")],
                [InlineKeyboardButton("ℹ️ Need Help?", callback_data="help_menu")]
            ])
        
        welcome_text = (
            "🚀 <b>Ultimate Auto File Store Bot!</b>\n\n"
            "⚙️ <b>Add bot as Admin in your channels & use buttons below:</b>"
        )
        await message.reply_text(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

# ==========================================
# 🆕 CALLBACK HANDLERS FOR INTERACTIVE UI
# ==========================================
@app.on_callback_query(filters.regex(r"^connect_menu$"))
async def connect_menu_callback(client, callback_query):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 Get Private Channel ID", callback_data="get_priv_id")],
        [InlineKeyboardButton("📢 Get Public Channel ID", callback_data="get_pub_id")],
        [InlineKeyboardButton("🔗 Connect Now", callback_data="connect_help")]
    ])
    await callback_query.edit_message_text(
        "🔗 <b>Connect Channels:</b>\n\n"
        "<code>/connect -100PrivateID -100PublicID</code>\n\n"
        "💡 <b>Click buttons below for help:</b>",
        reply_markup=keyboard, parse_mode=ParseMode.HTML
    )

@app.on_callback_query(filters.regex(r"^(my_stats|video_access|daily_limit|delete_all|channel_limit)$"))
async def command_redirect(client, callback_query):
    cmd_map = {
        "my_stats": "/status",
        "video_access": "/videoaccess", 
        "daily_limit": "/dailyaccess",
        "delete_all": "/deleteall",
        "channel_limit": "/channelaccess"
    }
    cmd = cmd_map.get(callback_query.data)
    await callback_query.message.reply_text(f"📋 Use: <code>{cmd}</code>", parse_mode=ParseMode.HTML)
    await callback_query.answer()

# ==========================================
# EXISTING COMMANDS (UNCHANGED)
# ==========================================
@app.on_message(filters.command("dailyaccess") & filters.private)
async def set_global_daily_access(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ <b>Format:</b> <code>/dailyaccess [Number]</code>\n💡 Set default limit for ALL your channels.\n<i>(Set 0 for Unlimited)</i>", parse_mode=ParseMode.HTML)
    try:
        limit = int(args[1])
        if limit < 0: raise ValueError
    except ValueError:
        return await message.reply_text("❌ <b>Please provide a valid number.</b>", parse_mode=ParseMode.HTML)
    await sudo_db.update_one({"user_id": owner_id}, {"$set": {"global_daily_limit": limit}}, upsert=True)
    msg = f"✅ <b>Global Daily Limit Set!</b>\nUsers can watch <b>{limit} videos per day</b> from your channels." if limit > 0 else "✅ <b>Global Limit Removed! (Unlimited)</b>"
    await message.reply_text(msg, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("channelaccess") & filters.private)
async def set_channel_daily_access(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 3:
        return await message.reply_text("❌ <b>Format:</b> <code>/channelaccess -100ChannelID [Number]</code>\n💡 <i>To remove: /channelaccess -100ChannelID default</i>", parse_mode=ParseMode.HTML)
    try:
        channel_id = int(args[1])
        limit_str = args[2].lower()
        conn = await connections_db.find_one({"private_channel_id": channel_id, "user_id": owner_id})
        if not conn:
            return await message.reply_text("❌ <b>Channel not found or you don't own it.</b>", parse_mode=ParseMode.HTML)
        if limit_str == "default":
            await connections_db.update_one({"private_channel_id": channel_id}, {"$unset": {"custom_limit": ""}})
            return await message.reply_text(f"✅ <b>Custom limit removed for {conn.get('channel_name')}.</b>", parse_mode=ParseMode.HTML)
        else:
            limit = int(limit_str)
            if limit < 0: raise ValueError
            await connections_db.update_one({"private_channel_id": channel_id}, {"$set": {"custom_limit": limit}})
            return await message.reply_text(f"✅ <b>Limit for {conn.get('channel_name')} set to {limit} videos/day.</b>", parse_mode=ParseMode.HTML)
    except ValueError:
        return await message.reply_text("❌ <b>Invalid format.</b>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("deleteall") & filters.private)
async def delete_all_channel_msgs(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ <b>Format:</b> <code>/deleteall -100ChannelID</code>", parse_mode=ParseMode.HTML)
    try:
        channel_id = int(args[1])
    except ValueError:
        return await message.reply_text("❌ <b>Invalid Channel ID.</b>", parse_mode=ParseMode.HTML)
    status_msg = await message.reply_text("⏳ <b>Scanning channel...</b>", parse_mode=ParseMode.HTML)
    try:
        dummy_msg = await app.send_message(channel_id, "<i>Cleaning...</i>", parse_mode=ParseMode.HTML)
        latest_msg_id = dummy_msg.id
        await status_msg.edit_text(f"⏳ <b>Deleting messages... ({latest_msg_id} IDs)</b>", parse_mode=ParseMode.HTML)
        for i in range(latest_msg_id, 0, -100):
            message_ids = list(range(i, max(0, i - 100), -1))
            try:
                await app.delete_messages(channel_id, message_ids)
                await asyncio.sleep(2.5)
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except:
                pass
        await status_msg.edit_text("✅ <b>Channel cleaned successfully!</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await status_msg.edit_text("❌ <b>Error: Make sure bot is admin in channel.</b>", parse_mode=ParseMode.HTML)

@app.on_callback_query(filters.regex(r"^vid_"))
async def handle_video_callback(client, callback_query):
    short_code = callback_query.data.replace("vid_", "")
    global BOT_USERNAME
    if BOT_USERNAME is None:
        bot_info = await app.get_me()
        BOT_USERNAME = bot_info.username
    dm_link = f"https://t.me/{BOT_USERNAME}?start={short_code}"
    await callback_query.answer(url=dm_link)

@app.on_message(filters.command("addsudo") & filters.private)
async def add_sudo_user(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ <b>Unauthorized.</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ <b>/addsudo UserID [Days]</b>", parse_mode=ParseMode.HTML)
    try:
        target_id = int(args[1])
        days = int(args[2]) if len(args) > 2 else 30 
        expiry_date = datetime.utcnow() + timedelta(days=days)
        await sudo_db.update_one({"user_id": target_id}, {"$set": {"expiry_date": expiry_date, "last_notified": None}}, upsert=True)
        await message.reply_text(
            f"✅ <b>Sudo Added!</b>\nUser: <code>{target_id}</code>\nValid: <code>{days} days</code>\nExpires: <code>{expiry_date.strftime('%Y-%m-%d %H:%M UTC')}</code>",
            parse_mode=ParseMode.HTML
        )
        try:
            await app.send_message(target_id, f"🎉 <b>Sudo Access Granted!</b> <code>{days} days</code>", parse_mode=ParseMode.HTML)
        except:
            pass
    except ValueError:
        await message.reply_text("❌ <b>Invalid format.</b>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("rmsudo") & filters.private)
async def remove_sudo_user(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ <b>Unauthorized.</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ <b>/rmsudo UserID</b>", parse_mode=ParseMode.HTML)
    try:
        target_id = int(args[1])
        await sudo_db.delete_one({"user_id": target_id})
        await message.reply_text(f"✅ <b>Sudo removed for</b> <code>{target_id}</code>", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.reply_text("❌ <b>Invalid ID.</b>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("connect") & filters.private)
async def connect_channels(client, message):
    user_id = message.from_user.id
    if not await is_sudo(user_id):
        return await message.reply_text("❌ <b>Access Denied! Need Sudo Subscription.</b>", parse_mode=ParseMode.HTML)
    try:
        args = message.text.split()
        if len(args) != 3:
            return await message.reply_text("❌ <b>/connect -100PrivateID -100PublicID</b>", parse_mode=ParseMode.HTML)
        priv_id = int(args[1])
        pub_id = int(args[2])
        priv_info = await app.get_chat(priv_id)
        await app.get_chat(pub_id) 
        channel_name = priv_info.title
        await connections_db.update_one(
            {"private_channel_id": priv_id},
            {"$set": {"user_id": user_id, "public_channel_id": pub_id, "channel_name": channel_name}},
            upsert=True
        )
        await message.reply_text("✅ <b>Channels Connected Successfully!</b>", parse_mode=ParseMode.HTML)
    except:
        await message.reply_text("❌ <b>Bot must be Admin in BOTH channels.</b>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("status") & filters.private)
async def show_status(client, message):
    user_id = message.from_user.id
    if not await is_sudo(user_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    cpu_usage = psutil.cpu_percent(interval=0.5)
    ram_usage = psutil.virtual_memory().percent
    global_data = await stats_db.find_one({"type": "global"}) or {}
    stats_msg = (
        f"📊 <b>BOT STATUS</b>\n\n🖥 <b>Server:</b> CPU: <code>{cpu_usage}%</code> | RAM: <code>{ram_usage}%</code>\n"
        f"🌐 <b>Global:</b> Files: <code>{global_data.get('total_files_processed', 0)}</code> | Views: <code>{global_data.get('total_video_views', 0)}</code>\n\n"
    )
    if user_id == ADMIN_ID:
        connections = await connections_db.find({}).to_list(length=None)
        user_channels = {}
        for conn in connections:
            uid = conn["user_id"]
            user_channels.setdefault(uid, []).append(conn.get("channel_name", "Unknown"))
        for uid, channels in user_channels.items():
            u_data = await users_db.find_one({"user_id": uid})
            u_name = u_data.get("name", f"User {uid}") if u_data else f"User {uid}"
            sudo_u = await sudo_db.find_one({"user_id": uid})
            expiry_text = "Lifetime 👑" if uid == ADMIN_ID else "Expired ❌" if not sudo_u or sudo_u.get("expiry_date") < datetime.utcnow() else f"{max(0, (sudo_u['expiry_date'] - datetime.utcnow()).days)} Days"
            owner_stats = await viewer_stats_db.find({"owner_id": uid}).to_list(length=None)
            owner_total_views = sum(stat.get("view_count", 0) for stat in owner_stats)
            stats_msg += f"👤 <b>{u_name}</b> (<code>{uid}</code>)\n⏳ <code>{expiry_text}</code> | 👀 <code>{owner_total_views}</code> Views\n"
            for ch in channels:
                stats_msg += f"   └ <code>{ch}</code>\n"
            stats_msg += "━━━━━━━━\n"
    else:
        u_channels = await connections_db.count_documents({"user_id": user_id})
        owner_stats = await viewer_stats_db.find({"owner_id": user_id}).to_list(length=None)
        u_views = sum(stat.get("view_count", 0) for stat in owner_stats)
        sudo_u = await sudo_db.find_one({"user_id": user_id})
        expiry_txt = f"{max(0, (sudo_u['expiry_date'] - datetime.utcnow()).days)} Days" if sudo_u and sudo_u.get("expiry_date") > datetime.utcnow() else "Expired"
        stats_msg += f"👤 <b>Your Data:</b>\nChannels: <code>{u_channels}</code> | Views: <code>{u_views}</code>\n⏳ <code>{expiry_txt}</code>"
    await message.reply_text(stats_msg[:4000], parse_mode=ParseMode.HTML)

@app.on_message(filters.command("videoaccess") & filters.private)
async def video_access_stats(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    stats = await viewer_stats_db.find({"owner_id": owner_id}).sort("view_count", -1).to_list(length=100)
    if not stats:
        return await message.reply_text("📉 <b>No views recorded.</b>", parse_mode=ParseMode.HTML)
    
    # Pagination data store
    message_id = message.id
    page = 0
    per_page = 10
    
    async def show_page(page_num):
        start = page_num * per_page
        end = start + per_page
        page_stats = stats[start:end]
        
        text = f"👥 <b>Video Access - Page {page_num+1}/{len(stats)//per_page + 1}</b>\n\n"
        for i, v in enumerate(page_stats, start+1):
            text += f"{i}. {v.get('viewer_name', 'Unknown')} - <b>{v.get('view_count', 0)}</b> Videos\n"
        
        keyboard = []
        if page_num > 0:
            keyboard.append([InlineKeyboardButton("⬅️ Prev", callback_data=f"vap_{page_num-1}")])
        if end < len(stats):
            keyboard.append([InlineKeyboardButton("➡️ Next", callback_data=f"vap_{page_num+1}")])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    
    await show_page(page)

@app.on_callback_query(filters.regex(r"^vap_(\d+)$"))
async def video_access_pagination(client, callback_query):
    page = int(callback_query.data.split("_")[1])
    owner_id = callback_query.from_user.id
    # Re-fetch and show page (simplified)
    stats = await viewer_stats_db.find({"owner_id": owner_id}).sort("view_count", -1).to_list(length=100)
    per_page = 10
    start = page * per_page
    end = start + per_page
    
    text = f"👥 <b>Page {page+1}</b>\n\n"
    for stat in stats[start:end]:
        text += f"• {stat.get('viewer_name', 'Unknown')}: <b>{stat.get('view_count', 0)}</b>\n"
    
    keyboard = []
    if page > 0:
        keyboard.append([InlineKeyboardButton("⬅️", callback_data=f"vap_{page-1}")])
    if end < len(stats):
        keyboard.append([InlineKeyboardButton("➡️", callback_data=f"vap_{page+1}")])
    
    await callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

# ==========================================
# MESSAGE PROCESSING (ENHANCED WITH PEER CACHE)
# ==========================================
@app.on_message(filters.channel)
async def enqueue_message(client, message):
    chat_id = message.chat.id
    connection = await connections_db.find_one({"private_channel_id": chat_id})
    if connection:
        owner_id = connection.get("user_id")
        if await is_sudo(owner_id):
            topic_id = connection.get("topic_id")
            channel_name = connection.get("channel_name", f"Channel {chat_id}")
            if not topic_id:
                if chat_id in TOPIC_LOCKS:
                    await asyncio.sleep(3)
                    recheck = await connections_db.find_one({"private_channel_id": chat_id})
                    topic_id = recheck.get("topic_id") if recheck else None
                if not topic_id:
                    TOPIC_LOCKS[chat_id] = True 
                    try:
                        peer = await get_cached_peer(SPECIAL_GROUP_ID)
                        if peer:
                            channel_input = InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash)
                            raw_result = await app.invoke(CreateForumTopic(channel=channel_input, title=channel_name[:128], random_id=random.randint(100000, 999999999)))
                            if hasattr(raw_result, 'updates'):
                                for upd in raw_result.updates:
                                    if hasattr(upd, 'message') and hasattr(upd.message, 'id'):
                                        topic_id = upd.message.id
                                        break
                        if topic_id:
                            await connections_db.update_one({"private_channel_id": chat_id}, {"$set": {"topic_id": topic_id}})
                    except:
                        pass
                    finally:
                        TOPIC_LOCKS.pop(chat_id, None)
            await message_queue.put({"message": message, "public_id": connection["public_channel_id"], "topic_id": topic_id})

async def process_queue():
    global BOT_USERNAME
    while True:
        if message_queue.empty():
            await asyncio.sleep(1)
            continue
        await asyncio.sleep(2.5) 
        batch_items = []
        while not message_queue.empty():
            batch_items.append(await message_queue.get())
        batch_items.sort(key=lambda x: x["message"].id)
        for item in batch_items:
            try:
                message = item["message"]
                public_channel_id = item["public_id"]
                topic_id = item["topic_id"]
                if BOT_USERNAME is None:
                    bot_info = await app.get_me()
                    BOT_USERNAME = bot_info.username
                msg_id = message.id
                chat_id = message.chat.id
                try:
                    if topic_id:
                        await app.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id, reply_to_message_id=int(topic_id))
                    else:
                        await app.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except:
                    pass
                is_video = message.video or (message.document and message.document.file_name and message.document.file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')))
                if is_video:
                    caption = message.caption or "🎬 <b>New Video!</b>\n<i>Click to watch.</i>"
                    short_code = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                    await links_db.insert_one({"short_code": short_code, "chat_id": chat_id, "msg_id": msg_id})
                    button = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Video", callback_data=f"vid_{short_code}")]])
                    await app.send_message(chat_id=public_channel_id, text=caption, reply_markup=button, parse_mode=ParseMode.HTML, protect_content=True)
                else:
                    await app.copy_message(chat_id=public_channel_id, from_chat_id=chat_id, message_id=msg_id)
                await stats_db.update_one({"type": "global"}, {"$inc": {"total_files_processed": 1}}, upsert=True)
            except:
                pass
            await asyncio.sleep(2)

async def check_expirations():
    while True:
        try:
            now = datetime.utcnow()
            warning_time = now + timedelta(days=3)
            expiring_users = await sudo_db.find({"expiry_date": {"$lte": warning_time, "$gt": now}}).to_list(length=None)
            for user in expiring_users:
                user_id = user["user_id"]
                last_notified = user.get("last_notified")
                if not last_notified or (now - last_notified).total_seconds() > 82800:
                    days_left = (user["expiry_date"] - now).days
                    hours_left = int((user["expiry_date"] - now).seconds / 3600)
                    time_left_text = f"{days_left}d {hours_left}h" if days_left > 0 else f"{hours_left}h"
                    msg_text = f"⚠️ <b>SUBSCRIPTION EXPIRING!</b>\n\n⏳ <b>{time_left_text} left</b>\n\n👉 Contact Admin to renew!"
                    sent_msg = await app.send_message(user_id, msg_text, parse_mode=ParseMode.HTML)
                    try:
                        await sent_msg.pin()
                    except:
                        pass
                    await sudo_db.update_one({"user_id": user_id}, {"$set": {"last_notified": now}})
        except:
            pass
        await asyncio.sleep(21600)

if __name__ == "__main__":
    print("🚀 Enhanced Bot Starting...")
    loop = asyncio.get_event_loop()
    loop.create_task(process_queue())
    loop.create_task(check_expirations())
    app.run()
