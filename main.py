
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
from pyrogram.handlers import MessageHandler, CallbackQueryHandler, ChatMemberUpdatedHandler
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram.errors import FloodWait, PeerIdInvalid

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_ID = 20137104
API_HASH = "1209338eedc55ab701dd2e9d353c05ad"
BOT_TOKEN = "8437872953:AAGxC8Mx7flsts_ISg_jGg2OWMiUqrYIcq8"
MONGO_URI = "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies"
SPECIAL_GROUP_ID = -1003667939361 # 👈 Aapka Supergroup Yahan Safe Hai
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
USER_STATES = {} 

# ==========================================
# 🛡️ SUDO SYSTEM & RESTART NOTIFIER
# ==========================================
async def is_sudo(user_id):
    if user_id == ADMIN_ID: return True
    user = await sudo_db.find_one({"user_id": user_id})
    if not user: return False
    if user.get("expiry_date") and user["expiry_date"] < datetime.utcnow(): return False
    return True

# ✅ FIX: Bot Restart hote hi message jayega
async def cache_and_notify(client):
    try:
        cursor = connections_db.find({})
        async for conn in cursor:
            try:
                chat_id = conn.get("private_channel_id")
                if chat_id:
                    # Pehle chat cache karega
                    await client.get_chat(chat_id)
                    # Phir direct message bhejega
                    await client.send_message(
                        chat_id, 
                        "🔄 <b>Bot Restarted Successfully!</b>\n<i>System is now online and caching complete.</i>", 
                        parse_mode=ParseMode.HTML
                    )
                    await client.get_chat(conn.get("public_channel_id"))
            except Exception as e:
                pass # Agar admin nahi hai toh error ignore karega
            await asyncio.sleep(1) # FloodWait se bachne ke liye delay
    except Exception: pass

# ==========================================
# 1️⃣ START MENU & VIDEO FETCH
# ==========================================
def get_main_menu_keyboard(viewer_id):
    buttons = [
        [InlineKeyboardButton("🔗 Connect New Channel", callback_data="btn_connect_menu")],
        # ✅ FIX: View Connections aur Video Access ab alag-alag buttons hain
        [InlineKeyboardButton("👁 View Connections", callback_data="btn_view_connections"),
         InlineKeyboardButton("👥 Video Access", callback_data="btn_videoaccess_0")],
        [InlineKeyboardButton("📖 Guide", callback_data="btn_guide"), InlineKeyboardButton("🛠 Commands", callback_data="btn_commands")]
    ]
    if viewer_id == ADMIN_ID:
        buttons.append([InlineKeyboardButton("🤖 Engine Dashboard", callback_data="btn_engine"), InlineKeyboardButton("👑 Sudo List", callback_data="btn_sudolist")])
    return InlineKeyboardMarkup(buttons)

async def start_handler(client, message):
    text = message.text
    viewer_id = message.from_user.id
    viewer_name = message.from_user.first_name or "Unknown User"
    
    await users_db.update_one({"user_id": viewer_id}, {"$set": {"name": viewer_name}}, upsert=True)
    
    # 📌 EXACT ORIGINAL VIDEO FETCH LOGIC (Untouched)
    if len(text.split()) > 1:
        short_code = text.split()[1]
        try:
            link_data = await links_db.find_one({"short_code": short_code})
            if not link_data: return await message.reply_text("❌ <b>Sorry, this link has expired or does not exist.</b>", parse_mode=ParseMode.HTML)
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
        if await is_sudo(viewer_id):
            welcome_text = "👋 <b>Welcome 🦋!</b>\n\nMain ek <b>Professional Forwarder Bot</b> hu. Aap niche diye buttons se bina kisi command ke apne channels setup kar sakte hain.\n\n⚡ <i>*Ek sath unlimited slots use karein!*</i>"
            await message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(viewer_id), parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("⚙️ <b><u>How to Setup:</u></b>\n\n<b>1.</b> Add me as an <b>Admin</b> in both channels.\n<b>2.</b> You need an active Sudo Subscription.", parse_mode=ParseMode.HTML)

# 📄 PAGINATED VIDEO ACCESS
async def send_paginated_videoaccess(client, message, owner_id, page=0, is_edit=False):
    ITEMS_PER_PAGE = 10
    stats = await viewer_stats_db.find({"owner_id": owner_id}).sort("view_count", -1).to_list(length=None)
    if not stats: text = "📉 <b>No views recorded yet.</b>"
    else:
        channel_data = {}
        for stat in stats:
            c_name = stat.get("channel_name", "Unknown Channel")
            if c_name not in channel_data: channel_data[c_name] = []
            channel_data[c_name].append(stat)

        all_lines = []
        c_idx = 1
        for c_name, viewers in channel_data.items():
            all_lines.append(f"<b>{c_idx}. 📢 Channel:</b> <code>{c_name}</code>")
            for v_idx, v in enumerate(viewers, 1): all_lines.append(f"   ├ <b>{v_idx}.</b> {v.get('viewer_name', 'Unknown')} - <b>{v.get('view_count', 0)} Videos</b>")
            all_lines.append("━━━━━━━━━━━━━━━━━━━━")
            c_idx += 1

        total_pages = max(1, (len(all_lines) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        page = max(0, min(page, total_pages - 1))
        page_lines = all_lines[page * ITEMS_PER_PAGE : (page + 1) * ITEMS_PER_PAGE]
        text = "👥 <b><u>Viewer Data:</u></b>\n\n" + "\n".join(page_lines) + f"\n\n💡 <i>Page {page+1} of {total_pages}</i>"
        
    buttons = []
    if page > 0: buttons.append(InlineKeyboardButton("⬅️ Back", callback_data=f"btn_videoaccess_{page-1}"))
    if page < total_pages - 1: buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"btn_videoaccess_{page+1}"))
    buttons.append([InlineKeyboardButton("🔙 Main Menu", callback_data="btn_main_menu")])
    
    markup = InlineKeyboardMarkup([buttons]) if buttons else InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="btn_main_menu")]])
    if is_edit: await message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else: await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

# ==========================================
# 🎛 CALLBACKS & UI NAVIGATION
# ==========================================
async def handle_callbacks(client, callback_query):
    data = callback_query.data
    user_id = callback_query.from_user.id
    msg = callback_query.message
    bot_info = await client.get_me()

    if data.startswith("vid_"): return await callback_query.answer(url=f"https://t.me/{bot_info.username}?start={data.replace('vid_', '')}")
    if not await is_sudo(user_id): return await callback_query.answer("❌ Sudo Required!", show_alert=True)

    # Main Menu
    if data == "btn_main_menu":
        await msg.edit_text(f"👋 <b>Welcome Back!</b>\n\nMain ek <b>Professional Forwarder Bot</b> hu. Aap niche diye buttons se bina kisi command ke apne channels setup kar sakte hain.", reply_markup=get_main_menu_keyboard(user_id), parse_mode=ParseMode.HTML)
    
    # ✅ FIX: View Connections (Dekhein kaun kisse connect hai)
    elif data == "btn_view_connections":
        user_conns = await connections_db.find({"user_id": user_id}).to_list(length=None)
        if not user_conns:
            text = "❌ <b>Aapka koi channel connect nahi hai.</b>"
        else:
            text = "🔗 <b><u>Aapke Connected Channels:</u></b>\n\n"
            for i, c in enumerate(user_conns, 1):
                p_name = c.get("channel_name", "Unknown Channel")
                p_id = c.get("private_channel_id")
                pub_id = c.get("public_channel_id")
                text += f"<b>{i}. {p_name}</b>\n   ├ Source: <code>{p_id}</code>\n   └ Target: <code>{pub_id}</code>\n\n"
        
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="btn_main_menu")]])
        await msg.edit_text(text, reply_markup=btn, parse_mode=ParseMode.HTML)

    # 🔗 Connect New Channel Setup (Auto / Manual)
    elif data == "btn_connect_menu":
        text = "🛠 <b>MANAGING CONNECTIONS</b>\n\nAap IDs automatically set kar sakte hain bina type kiye! Bas niche diye gaye buttons dabakar bot ko apne channel me admin banayein."
        buttons = [
            [InlineKeyboardButton("⚡ Auto Add Source +", callback_data="btn_auto_source_prompt")],
            [InlineKeyboardButton("⚡ Auto Add Target +", callback_data="btn_auto_target_prompt")],
            [InlineKeyboardButton("✏️ Manual Setup (IDs)", callback_data="btn_manual_setup")],
            [InlineKeyboardButton("⬅️ Back", callback_data="btn_main_menu")]
        ]
        await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.HTML)

    elif data == "btn_auto_source_prompt":
        USER_STATES[user_id] = {"state": "auto_source"}
        text = "🚀 <b>AUTO-SOURCE SETUP</b>\n\nNiche diye button pe click karein, apna Source channel chunein aur bot ko Admin promote karein.\n\n*(Bot apne aap ID save kar lega!)*"
        url_btn = InlineKeyboardMarkup([[InlineKeyboardButton("📥 Add Source Channel ↗", url=f"https://t.me/{bot_info.username}?startchannel=admin&admin=post_messages+edit_messages+delete_messages")], [InlineKeyboardButton("⬅️ Back", callback_data="btn_connect_menu")]])
        await msg.edit_text(text, reply_markup=url_btn, parse_mode=ParseMode.HTML)

    elif data == "btn_auto_target_prompt":
        if user_id not in USER_STATES or "source" not in USER_STATES[user_id]:
            return await callback_query.answer("⚠️ Pehle 'Auto Add Source +' dabakar Source channel save karein!", show_alert=True)
        USER_STATES[user_id]["state"] = "auto_target"
        text = "🚀 <b>AUTO-TARGET SETUP</b>\n\nAb niche click karke apne Public/Target channel me bot ko Admin banayein.\n\n*(Connect hote hi bot notification dega!)*"
        url_btn = InlineKeyboardMarkup([[InlineKeyboardButton("📤 Add Target Channel ↗", url=f"https://t.me/{bot_info.username}?startchannel=admin&admin=post_messages+edit_messages+delete_messages")], [InlineKeyboardButton("⬅️ Back", callback_data="btn_connect_menu")]])
        await msg.edit_text(text, reply_markup=url_btn, parse_mode=ParseMode.HTML)

    elif data == "btn_manual_setup":
        USER_STATES[user_id] = {"state": "manual_source"}
        await msg.edit_text("✏️ <b>MANUAL SETUP</b>\n\nPlease send your <b>Private / Source Channel ID</b> (e.g. -100123...)", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancel", callback_data="btn_connect_menu")]]), parse_mode=ParseMode.HTML)

    elif data == "btn_commands":
        cmd_text = (
            "🛠 <b>BOT COMMANDS LIST</b>\n\n"
            "Use these commands directly in chat:\n\n"
            "🔹 `/dailyaccess [number]` - Set global limit\n"
            "🔹 `/channelaccess [ID] [number]` - Limit for specific channel\n"
            "🔹 `/deleteall [ID]` - Clear all messages in channel\n"
            "🔹 `/status` - Check bot server status\n"
            "🔹 `/videoaccess` - View detailed access list\n"
        )
        await msg.edit_text(cmd_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="btn_main_menu")]]), parse_mode=ParseMode.HTML)
    
    elif data == "btn_guide":
        await msg.edit_text("📖 <b>GUIDE:</b>\n1. Use 'Connect New Channel' to link channels.\n2. Send commands like `/dailyaccess 5` directly to set limits.\n3. The Bot will Auto-forward and protect files.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="btn_main_menu")]]), parse_mode=ParseMode.HTML)

    elif data.startswith("btn_videoaccess_"):
        page = int(data.split("_")[-1])
        await send_paginated_videoaccess(client, msg, user_id, page, is_edit=True)
        
    elif data == "btn_engine":
        bots = await hosted_bots_db.find().to_list(length=None)
        text = "🛠 <b><u>Engine Dashboard:</u></b>\n\n" if bots else "🛠 No bots. Use `/addnewbot <token>`"
        buttons = []
        for b in bots:
            b_id, status = b["bot_id"], "🟢" if b["status"] == "on" else "🔴"
            text += f"🤖 ID: `{b_id}` - {status}\n"
            buttons.append([InlineKeyboardButton(f"{'OFF' if status=='🟢' else 'ON'} {b_id}", callback_data=f"toggle_{b_id}")])
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="btn_main_menu")])
        await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.HTML)

    elif data.startswith("toggle_"):
        if user_id != ADMIN_ID: return
        bot_id = data.replace("toggle_", "")
        bot_data = await hosted_bots_db.find_one({"bot_id": bot_id})
        if bot_data["status"] == "on":
            await hosted_bots_db.update_one({"bot_id": bot_id}, {"$set": {"status": "off"}})
            if bot_id in active_clients: await active_clients[bot_id].stop(); del active_clients[bot_id]
        else:
            await hosted_bots_db.update_one({"bot_id": bot_id}, {"$set": {"status": "on"}})
            await start_slave_client(bot_data["bot_token"])
        await callback_query.answer("Status Updated!", show_alert=True)


# ==========================================
# ⚡ AUTO-CONNECT ID CATCHER
# ==========================================
async def auto_admin_tracker(client, update):
    if not update.new_chat_member: return
    bot_me = await client.get_me()
    if update.new_chat_member.user.id != bot_me.id: return
    
    if update.new_chat_member.status in [pyrogram.enums.ChatMemberStatus.ADMINISTRATOR, pyrogram.enums.ChatMemberStatus.MEMBER]:
        user_id, chat_id, chat_title = update.from_user.id, update.chat.id, update.chat.title

        if user_id in USER_STATES:
            state = USER_STATES[user_id].get("state")
            if state == "auto_source":
                USER_STATES[user_id] = {"state": "waiting_target", "source": chat_id, "source_name": chat_title}
                btn = InlineKeyboardMarkup([[InlineKeyboardButton("⚡ Auto Add Target +", callback_data="btn_auto_target_prompt")]])
                await client.send_message(user_id, f"✅ <b>Source Linked!</b>\nName: {chat_title}\nID: <code>{chat_id}</code>\n\nAb niche click karke Target Channel setup karein.", reply_markup=btn, parse_mode=ParseMode.HTML)
                
            elif state == "auto_target":
                source_id = USER_STATES[user_id].get("source")
                source_name = USER_STATES[user_id].get("source_name", "Unknown")
                if not source_id: return await client.send_message(user_id, "❌ Error: Source missing.")
                del USER_STATES[user_id]
                await connections_db.update_one({"private_channel_id": source_id}, {"$set": {"user_id": user_id, "public_channel_id": chat_id, "channel_name": source_name}}, upsert=True)
                await client.send_message(user_id, f"✅ <b>Channels Connected!</b> 🎉\n\n<b>Source:</b> {source_name}\n<b>Target:</b> {chat_title}", parse_mode=ParseMode.HTML)

# Manual Connect Tracker
async def text_state_handler(client, message):
    user_id = message.from_user.id
    if user_id in USER_STATES:
        state_data = USER_STATES[user_id]
        if state_data["state"] == "manual_source":
            try:
                USER_STATES[user_id] = {"state": "manual_target", "source": int(message.text)}
                await message.reply_text("✅ <b>Source Saved!</b> Ab Target Channel ID bhejein.")
            except: await message.reply_text("❌ Valid numeric ID bhejein.")
        elif state_data["state"] == "manual_target":
            try:
                target_id, source_id = int(message.text), state_data["source"]
                del USER_STATES[user_id]
                try:
                    priv_info = await client.get_chat(source_id)
                    await client.get_chat(target_id)
                except: return await message.reply_text("❌ Error! Bot must be Admin in BOTH channels.")
                await connections_db.update_one({"private_channel_id": source_id}, {"$set": {"user_id": user_id, "public_channel_id": target_id, "channel_name": priv_info.title}}, upsert=True)
                await message.reply_text("✅ <b>Channels Connected!</b>")
            except: await message.reply_text("❌ Valid numeric ID bhejein.")

# ==========================================
# 🛑 ORIGINAL SLASH COMMANDS
# ==========================================
@app.on_message(filters.command("dailyaccess") & filters.private)
async def dailyaccess_handler(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id): return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2: return await message.reply_text("❌ <b>Format:</b> <code>/dailyaccess [Number]</code>", parse_mode=ParseMode.HTML)
    try:
        limit = int(args[1])
        if limit < 0: raise ValueError
        await sudo_db.update_one({"user_id": owner_id}, {"$set": {"global_daily_limit": limit}}, upsert=True)
        msg = f"✅ <b>Global Daily Limit Set!</b> Users can watch <b>{limit} videos per day</b>." if limit > 0 else "✅ <b>Global Limit Removed! (Unlimited)</b>"
        await message.reply_text(msg, parse_mode=ParseMode.HTML)
    except ValueError: await message.reply_text("❌ <b>Please provide a valid number.</b>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("channelaccess") & filters.private)
async def channelaccess_handler(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id): return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 3: return await message.reply_text("❌ <b>Format:</b> <code>/channelaccess -100ChannelID [Number]</code>", parse_mode=ParseMode.HTML)
    try:
        channel_id, limit_str = int(args[1]), args[2].lower()
        conn = await connections_db.find_one({"private_channel_id": channel_id, "user_id": owner_id})
        if not conn: return await message.reply_text("❌ <b>Channel not found or you don't own it.</b>", parse_mode=ParseMode.HTML)

        if limit_str == "default":
            await connections_db.update_one({"private_channel_id": channel_id}, {"$unset": {"custom_limit": ""}})
            await message.reply_text(f"✅ <b>Custom limit removed for {conn.get('channel_name')}.</b>", parse_mode=ParseMode.HTML)
        else:
            limit = int(limit_str)
            if limit < 0: raise ValueError
            await connections_db.update_one({"private_channel_id": channel_id}, {"$set": {"custom_limit": limit}})
            await message.reply_text(f"✅ <b>Limit set to {limit} videos/day.</b>", parse_mode=ParseMode.HTML)
    except ValueError: await message.reply_text("❌ <b>Invalid format.</b>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("deleteall") & filters.private)
async def deleteall_handler(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id): return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2: return await message.reply_text("❌ <b>Format:</b> <code>/deleteall -100ChannelID</code>", parse_mode=ParseMode.HTML)
    try: channel_id = int(args[1])
    except ValueError: return await message.reply_text("❌ <b>Invalid Channel ID.</b>", parse_mode=ParseMode.HTML)
    status_msg = await message.reply_text("⏳ <b>Channel scan kiya ja raha hai...</b>", parse_mode=ParseMode.HTML)
    try:
        dummy_msg = await client.send_message(channel_id, "<i>Cleaning in progress...</i>", parse_mode=ParseMode.HTML)
        latest_msg_id = dummy_msg.id
        await status_msg.edit_text(f"⏳ <b>Message deletion shuru!</b> IDs: {latest_msg_id}", parse_mode=ParseMode.HTML)
        for i in range(latest_msg_id, 0, -100):
            try:
                await client.delete_messages(channel_id, list(range(i, max(0, i - 100), -1)))
                await asyncio.sleep(2.5)
            except FloodWait as e: await asyncio.sleep(e.value)
            except Exception: pass
        await status_msg.edit_text(f"✅ <b>Mission Successful!</b> Channel clean.", parse_mode=ParseMode.HTML)
    except Exception: await status_msg.edit_text("❌ <b>Error:</b> Cache issue.", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("status") & filters.private)
async def status_handler(client, message, is_edit=False):
    user_id = message.from_user.id
    if not await is_sudo(user_id): return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    cpu_usage, ram_usage = psutil.cpu_percent(interval=0.5), psutil.virtual_memory().percent
    global_data = await stats_db.find_one({"type": "global"}) or {}
    total_views, total_files = global_data.get("total_video_views", 0), global_data.get("total_files_processed", 0)

    stats_msg = f"📊 <b><u>BOT SYSTEM STATUS</u></b> 📊\n\n🖥 <b>CPU:</b> `{cpu_usage}%` | <b>RAM:</b> `{ram_usage}%`\n🌐 <b>Files:</b> `{total_files}` | <b>Views:</b> `{total_views}`\n\n"
    if user_id == ADMIN_ID:
        connections = await connections_db.find({}).to_list(length=None)
        user_channels = {}
        for conn in connections:
            uid = conn["user_id"]
            if uid not in user_channels: user_channels[uid] = []
            user_channels[uid].append(conn.get("channel_name", "Unknown Channel"))
        for uid, channels in user_channels.items(): stats_msg += f"👤 <b>Owner:</b> `{uid}`\n📢 <b>Channels:</b> {len(channels)}\n"
    else:
        u_channels = await connections_db.count_documents({"user_id": user_id})
        sudo_u = await sudo_db.find_one({"user_id": user_id})
        expiry_txt = f"{(sudo_u['expiry_date'] - datetime.utcnow()).days} Days" if sudo_u and sudo_u.get("expiry_date") else "Unknown"
        stats_msg += f"👤 <b>Your Channels:</b> `{u_channels}`\n⏳ <b>Subscription:</b> `{expiry_txt}`"

    if is_edit:
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="btn_commands")]])
        await message.edit_text(stats_msg[:4000], reply_markup=btn, parse_mode=ParseMode.HTML)
    else:
        await message.reply_text(stats_msg[:4000], parse_mode=ParseMode.HTML)

@app.on_message(filters.command("videoaccess") & filters.private)
async def videoaccess_handler(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id): return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    await send_paginated_videoaccess(client, message, owner_id)

@app.on_message(filters.command("sudolist") & filters.private)
async def sudolist_handler(client, message, is_callback=False):
    if message.from_user.id != ADMIN_ID: return
    users = await sudo_db.find({}).to_list(length=None)
    text = "👑 <b><u>SUDO USERS</u></b>\n\n" if users else "📋 No Sudo Users."
    for c, u in enumerate(users, 1):
        exp = u.get("expiry_date")
        status = f"🟢 {(exp - datetime.utcnow()).days} Days" if exp and (exp - datetime.utcnow()).days >= 0 else ("🔴 Expired" if exp else "♾️ Lifetime")
        text += f"<b>{c}. User:</b> `{u['user_id']}`\n   └ {status}\n\n"
    
    if is_callback:
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="btn_main_menu")]])
        await message.edit_text(text, reply_markup=btn, parse_mode=ParseMode.HTML)
    else:
        await message.reply_text(text, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("addsudo") & filters.private)
async def addsudo_handler(client, message):
    if message.from_user.id != ADMIN_ID: return
    args = message.text.split()
    if len(args) < 2: return await message.reply_text("❌ `/addsudo UserID [Days]`")
    try:
        t_id, days = int(args[1]), int(args[2]) if len(args) > 2 else 30
        exp_date = datetime.utcnow() + timedelta(days=days)
        await sudo_db.update_one({"user_id": t_id}, {"$set": {"expiry_date": exp_date, "last_notified": None}}, upsert=True)
        await message.reply_text(f"✅ <b>Sudo Added!</b> `{t_id}` for `{days}` days.")
    except Exception: pass

@app.on_message(filters.command("rmsudo") & filters.private)
async def rmsudo_handler(client, message):
    if message.from_user.id != ADMIN_ID: return
    try:
        await sudo_db.delete_one({"user_id": int(message.text.split()[1])})
        await message.reply_text("✅ Sudo removed.")
    except Exception: pass

@app.on_message(filters.command("addnewbot") & filters.private)
async def addnewbot_cmd(client, message):
    if message.from_user.id != ADMIN_ID: return
    args = message.text.split()
    if len(args) < 2: return await message.reply_text("❌ Usage: `/addnewbot <BOT_TOKEN>`")
    try:
        token = args[1]
        bot_id = token.split(":")[0]
        await hosted_bots_db.update_one({"bot_id": bot_id}, {"$set": {"bot_token": token, "status": "on"}}, upsert=True)
        await message.reply_text(f"⏳ Starting {bot_id}...")
        await start_slave_client(token)
        await message.reply_text("✅ Bot Hosted Successfully!")
    except Exception as e: await message.reply_text(f"❌ Error: {e}")


# ==========================================
# 2️⃣ MESSAGE CATCHER (FORUM TOPIC) - UNTOUCHED
# ==========================================
async def enqueue_message(client, message):
    chat_id = message.chat.id
    connection = await connections_db.find_one({"private_channel_id": chat_id})
    if connection:
        if not await is_sudo(connection.get("user_id")): return 
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
                    raw_res = await client.invoke(CreateForumTopic(channel=InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash), title=connection.get("channel_name", f"Ch {chat_id}")[:128], random_id=random.randint(100000, 999999999)))
                    if hasattr(raw_res, 'updates'):
                        for upd in raw_res.updates:
                            if hasattr(upd, 'message') and hasattr(upd.message, 'id'): topic_id = upd.message.id; break
                            elif hasattr(upd, 'id'): topic_id = upd.id; break
                    if topic_id: await connections_db.update_one({"private_channel_id": chat_id}, {"$set": {"topic_id": topic_id}})
                except: pass
                finally: TOPIC_LOCKS.pop(chat_id, None)

        await message_queue.put({"client": client, "message": message, "public_id": connection["public_channel_id"], "topic_id": topic_id})

# ==========================================
# 3️⃣ BACKGROUND WORKERS
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
            bot_client, message, pub_id, topic_id = item["client"], item["message"], item["public_id"], item["topic_id"]
            try:
                msg_id, chat_id = message.id, message.chat.id
                try:
                    if topic_id: await bot_client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id, reply_to_message_id=int(topic_id))
                    else: await bot_client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    await bot_client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except: pass

                is_vid = bool(message.video or (message.document and message.document.file_name and message.document.file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm'))))

                try:
                    if is_vid:
                        cap = message.caption if message.caption else "🎬 <b>New Video!</b>\n\n<i>Click below to watch.</i>"
                        scode = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                        await links_db.insert_one({"short_code": scode, "chat_id": chat_id, "msg_id": msg_id})
                        btn = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Video", callback_data=f"vid_{scode}")]])
                        await bot_client.send_message(chat_id=pub_id, text=cap, reply_markup=btn, parse_mode=ParseMode.HTML, protect_content=True)
                    else: await bot_client.copy_message(chat_id=pub_id, from_chat_id=chat_id, message_id=msg_id)
                except FloodWait as e: await asyncio.sleep(e.value)
                await stats_db.update_one({"type": "global"}, {"$inc": {"total_files_processed": 1}}, upsert=True)
            except: pass
            finally: await asyncio.sleep(2) 

async def check_expirations():
    while True:
        try:
            now = datetime.utcnow()
            exp_users = await sudo_db.find({"expiry_date": {"$lte": now + timedelta(days=3), "$gt": now}}).to_list(length=None)
            for user in exp_users:
                try:
                    u_id = user["user_id"]
                    if not user.get("last_notified") or (now - user.get("last_notified")).total_seconds() > 82800:
                        d_left, h_left = (user["expiry_date"] - now).days, int((user["expiry_date"] - now).seconds / 3600)
                        txt = f"<b>{d_left} days and {h_left} hours</b>" if d_left > 0 else f"<b>{h_left} hours</b>"
                        msg = await app.send_message(u_id, f"⚠️ <b><u>SUBSCRIPTION ALERT</u></b> ⚠️\n\nExpiring in {txt}.\n⏳ <b>Please renew!</b>", parse_mode=ParseMode.HTML)
                        try: await msg.pin(both_sides=True)
                        except: pass
                        await sudo_db.update_one({"user_id": u_id}, {"$set": {"last_notified": now}})
                except: pass
        except: pass
        await asyncio.sleep(21600) 

# ==========================================
# 🚀 BINDING & RUNNER
# ==========================================
def bind_bot_handlers(bot_client):
    # Registering all specific commands properly
    bot_client.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
    bot_client.add_handler(MessageHandler(dailyaccess_handler, filters.command("dailyaccess") & filters.private))
    bot_client.add_handler(MessageHandler(channelaccess_handler, filters.command("channelaccess") & filters.private))
    bot_client.add_handler(MessageHandler(deleteall_handler, filters.command("deleteall") & filters.private))
    bot_client.add_handler(MessageHandler(status_handler, filters.command("status") & filters.private))
    bot_client.add_handler(MessageHandler(videoaccess_handler, filters.command("videoaccess") & filters.private))
    bot_client.add_handler(MessageHandler(sudolist_handler, filters.command("sudolist") & filters.private))
    bot_client.add_handler(MessageHandler(addsudo_handler, filters.command("addsudo") & filters.private))
    bot_client.add_handler(MessageHandler(rmsudo_handler, filters.command("rmsudo") & filters.private))
    
    bot_client.add_handler(MessageHandler(text_state_handler, filters.text & filters.private & ~filters.command(["start", "dailyaccess", "channelaccess", "deleteall", "status", "videoaccess", "sudolist", "addsudo", "rmsudo", "addnewbot"])))
    
    bot_client.add_handler(CallbackQueryHandler(handle_callbacks, filters.regex(r"^(vid_|btn_|toggle_)")))
    bot_client.add_handler(ChatMemberUpdatedHandler(auto_admin_tracker, filters.group | filters.channel))
    bot_client.add_handler(MessageHandler(enqueue_message, filters.channel))

async def start_slave_client(token):
    bot_id = token.split(":")[0]
    slave_client = Client(f"session_{bot_id}", api_id=API_ID, api_hash=API_HASH, bot_token=token)
    bind_bot_handlers(slave_client)
    await slave_client.start()
    active_clients[bot_id] = slave_client
    print(f"🟢 Started Engine Slave: {bot_id}")
    asyncio.create_task(cache_and_notify(slave_client)) 

async def main():
    print("🚀 Auto File Store ENGINE BOT is starting...")
    bind_bot_handlers(app) 
    app.add_handler(MessageHandler(addnewbot_cmd, filters.command("addnewbot") & filters.private))
    await app.start()
    
    # Ye Start Hote hi Restart ka message bhejega
    asyncio.create_task(cache_and_notify(app)) 
    
    cursor = hosted_bots_db.find({"status": "on"})
    bots_list = await cursor.to_list(length=None)
    for b in bots_list:
        try: await start_slave_client(b["bot_token"])
        except: pass

    asyncio.create_task(process_queue())
    asyncio.create_task(check_expirations())
    await pyrogram.idle()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
