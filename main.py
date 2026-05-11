
import math
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
import asyncio
import random
import string
import psutil
#from datetime import datetime
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.raw.functions.channels import CreateForumTopic
from pyrogram.raw.types import InputChannel
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram.errors import FloodWait, PeerIdInvalid


# ⚙️ CONFIGURATION (Apni Details Daalein)
# ==========================================
API_ID = 20137104
API_HASH = "1209338eedc55ab701dd2e9d353c05ad"
BOT_TOKEN = "8698945941:AAGyFS8zEeNjOCP9z34HMN9bsRujHO4kxhg"

MONGO_URI = "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies"
SPECIAL_GROUP_ID = -1003667939361

ADMIN_ID = 5050578106  # 👈 Apni Telegram ID daalein

# ==========================================
# INITIALIZATION
# ==========================================
app = Client("srcjabghssbslser_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

#db_client = AsyncIOMotorClient(MONGO_URI)
db_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=20000, maxIdleTimeMS=50000)
db = db_client["telegram_file_bot"]
connections_db = db["channel_connections"]
stats_db = db["bot_stats"] 
viewer_stats_db = db["viewer_stats"] 
users_db = db["all_users"] 
links_db = db["short_links"] 
sudo_db = db["sudo_users"] 
daily_access_db = db["daily_access_tracker"]
WAITING_FOR_LIMIT = {}  # Daily access limit input track karne ke liye
PENDING_SOURCES = {}    # Auto channel connect status track karne ke liye

# 🆕 Daily limit track karne ke liye


message_queue = asyncio.Queue()
BOT_USERNAME = None
TOPIC_LOCKS = {} 

# ==========================================
# 🛡️ SUDO VERIFICATION SYSTEM
# ==========================================
async def is_sudo(user_id):
    if user_id == ADMIN_ID:
        return True
    user = await sudo_db.find_one({"user_id": user_id})
    if not user:
        return False
    # Agar current time expiry date se aage nikal gaya toh False (Auto Expire)
    if user.get("expiry_date") and user["expiry_date"] < datetime.utcnow():
        return False
    return True


# ==========================================
# 1️⃣ USER COMMANDS & DM BOT LOGIC
# ==========================================
# ==========================================
# 1️⃣ USER COMMANDS & DM BOT LOGIC (/start)
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
    
    # 📌 VIDEO FETCH LOGIC (Same as your original code)
    if len(text.split()) > 1:
        short_code = text.split()[1]
        try:
            link_data = await links_db.find_one({"short_code": short_code})
            
            if not link_data:
                return await message.reply_text("❌ <b>Sorry, this link has expired or does not exist.</b>", parse_mode=ParseMode.HTML)
                
            p_chat_id = link_data["chat_id"]
            msg_id = link_data["msg_id"]

            # --- 🛑 DAILY LIMIT CHECKER ---
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
        # 🎨 NEW PREMIUM MAIN MENU UI
        welcome_text = (
            "🚀 <b>Welcome to the SRC Bot</b>\n\n"
            "⚙️ <b><u>Main Menu:</u></b>\n"
            "Select an option below to easily connect and manage your channels.\n\n"
            "⚠️ <i><b>Note:</b> An active Sudo Subscription is required to manage channels.</i>"
        )
        
        # 🆕 Updated Buttons for Auto-Connect
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("Auto Add Source +", url=f"https://t.me/{BOT_USERNAME}?startchannel=admin&admin=post_messages+edit_messages+delete_messages"),
             InlineKeyboardButton("Auto Add Target +", url=f"https://t.me/{BOT_USERNAME}?startchannel=admin&admin=post_messages+edit_messages+delete_messages")],
            [InlineKeyboardButton("🔗 Connected Channels", callback_data="cmd_connected")],
            [InlineKeyboardButton("📊 Video Access", callback_data="cmd_videoaccess_1"),
             InlineKeyboardButton("⏳ Daily Access", callback_data="cmd_dailyaccess")]
        ])
        
        await message.reply_text(welcome_text, reply_markup=buttons, parse_mode=ParseMode.HTML, protect_content=True)


# ==========================================
# 🎨 INTERACTIVE UI BUTTON HANDLERS
# ==========================================
@app.on_callback_query(filters.regex(r"^cmd_"))
async def handle_main_menu_callbacks(client, callback_query):
    data = callback_query.data
    user_id = callback_query.from_user.id

    if not await is_sudo(user_id):
        return await callback_query.answer("❌ Access Denied! An active Sudo Subscription is required.", show_alert=True)

    # 📊 Video Access Logic (With Pagination)
    if data.startswith("cmd_videoaccess"):
        page = int(data.split("_")[2]) if len(data.split("_")) > 2 else 1
        limit_per_page = 15 # Ek page me kitne dikhane hain
        
        stats = await viewer_stats_db.find({"owner_id": user_id}).sort("view_count", -1).to_list(length=None)
        
        if not stats:
            return await callback_query.answer("📉 No views recorded yet.", show_alert=True)
            
        total_pages = math.ceil(len(stats) / limit_per_page)
        start_idx = (page - 1) * limit_per_page
        end_idx = start_idx + limit_per_page
        current_stats = stats[start_idx:end_idx]
        
        text = f"👥 <b><u>Top Viewers (Page {page}/{total_pages}):</u></b>\n\n"
        for idx, v in enumerate(current_stats, start=start_idx + 1):
            c_name = v.get("channel_name", "Unknown")
            v_name = v.get("viewer_name", "Unknown")
            v_count = v.get("view_count", 0)
            text += f"<b>{idx}.</b> {v_name} - <b>{v_count} Views</b> (<i>{c_name}</i>)\n"
            
        # Pagination Buttons Generate karna
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"cmd_videoaccess_{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"cmd_videoaccess_{page+1}"))
            
        keyboard = InlineKeyboardMarkup([nav_buttons]) if nav_buttons else None
        
        try:
            await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await callback_query.answer()

    # 🔗 Connected Channels Button Logic
    elif data == "cmd_connected":
        connections = await connections_db.find({"user_id": user_id}).to_list(length=None)
        if not connections:
            return await callback_query.answer("❌ No channels connected yet.", show_alert=True)

        text = "🔗 <b><u>Your Connected Channels:</u></b>\n\n"
        for c in connections:
            s_name = c.get("channel_name", "Unknown Name")
            s_id = c.get("private_channel_id", "Unknown")
            t_id = c.get("public_channel_id", "Unknown Target")
            
            text += f"📁 <b>{s_name}</b>\n"
            text += f"   ├ <b>Source ID:</b> <code>{s_id}</code>\n"
            text += f"   └ <b>Target ID:</b> <code>{t_id}</code>\n"
            text += "━━━━━━━━━━━━━━━━━━━━\n"

        if len(text) > 4000:
            text = text[:4000] + "...\n<i>Message too long.</i>"
            
        await callback_query.message.edit_text(text, parse_mode=ParseMode.HTML)
        await callback_query.answer()

    # ⏳ Daily Access Input Button
    elif data == "cmd_dailyaccess":
        WAITING_FOR_LIMIT[user_id] = True # Status ON kar diya user ke liye
        await callback_query.message.reply_text(
            "📝 <b><u>Set Global Daily Access Limit</u></b>\n\n"
            "Please send the number of videos a user can watch per day (e.g., <code>5</code>).\n\n"
            "<i>(Type <code>0</code> for Unlimited Access)</i>",
            reply_markup=ForceReply(selective=True),
            parse_mode=ParseMode.HTML
        )
        await callback_query.answer()


# ==========================================
# ⌨️ DYNAMIC TEXT HANDLER (Daily Access Number)
# ==========================================
@app.on_message(filters.private & filters.text & ~filters.regex(r"^/"))
async def handle_dynamic_inputs(client, message):
    user_id = message.from_user.id
    
    # Agar user Daily Access set karne wala tha...
    if WAITING_FOR_LIMIT.get(user_id):
        try:
            limit = int(message.text.strip())
            if limit < 0: raise ValueError
        except ValueError:
            return await message.reply_text("❌ <b>Please send a valid positive number.</b>", parse_mode=ParseMode.HTML)

        await sudo_db.update_one(
            {"user_id": user_id},
            {"$set": {"global_daily_limit": limit}},
            upsert=True
        )
        WAITING_FOR_LIMIT.pop(user_id, None) # Status OFF kar diya

        msg = f"✅ <b>Global Daily Limit Set!</b>\nUsers can now watch <b>{limit} videos per day</b> from your channels." if limit > 0 else "✅ <b>Global Limit Removed! (Unlimited)</b>"
        await message.reply_text(msg, parse_mode=ParseMode.HTML)


# ==========================================
# 🤖 AUTO DETECT & CONNECT MAGIC WAND
# ==========================================
@app.on_chat_member_updated(filters.channel)
async def on_bot_added_to_channel(client, update):
    bot_id = (await app.get_me()).id
    
    # Check karein agar bot ko kisi channel me Add ya Promote kiya gaya hai
    if update.new_chat_member and update.new_chat_member.user.id == bot_id:
        if update.new_chat_member.status in [ChatMemberStatus.ADMINISTRATOR]:
            user_id = update.from_user.id
            if not await is_sudo(user_id):
                return

            chat_id = update.chat.id
            chat_title = update.chat.title

            pending_source = PENDING_SOURCES.get(user_id)

            if not pending_source:
                # 1️⃣ PEHLI BAAR ADD KIYA (Auto Source Detection)
                try:
                    # Auto Create Topic in Master Group
                    peer = await app.resolve_peer(SPECIAL_GROUP_ID)
                    channel_input = InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash)
                    raw_result = await app.invoke(
                        CreateForumTopic(channel=channel_input, title=chat_title[:128], random_id=random.randint(100000, 999999999))
                    )
                    
                    topic_id = None
                    if hasattr(raw_result, 'updates'):
                        for upd in raw_result.updates:
                            if hasattr(upd, 'message') and hasattr(upd.message, 'id'):
                                topic_id = upd.message.id; break
                            elif hasattr(upd, 'id'):
                                topic_id = upd.id; break

                    # Pending Source memory me save kar lo
                    PENDING_SOURCES[user_id] = {
                        "chat_id": chat_id,
                        "chat_title": chat_title,
                        "topic_id": topic_id
                    }

                    await app.send_message(
                        user_id,
                        f"✅ <b>SOURCE DETECTED!</b>\n\n"
                        f"📁 <b>Channel:</b> {chat_title}\n"
                        f"🗂️ <b>Logging Topic Created.</b>\n\n"
                        f"⚡ <b>Next Step:</b> Ab <b>Auto Add Target +</b> button dabayein aur public channel connect karein.",
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    print(f"Auto Topic Error: {e}")
                    await app.send_message(user_id, f"⚠️ Source Detected, lekin Auto- connect nahi ho raha hai /connect es commd ka use kar ke connect kare ")

            else:
                # 2️⃣ DUSRI BAAR ADD KIYA (Auto Target & Connect!)
                source_id = pending_source["chat_id"]
                source_title = pending_source["chat_title"]
                topic_id = pending_source["topic_id"]

                # Dono channels ko automatically Database me connect kar do
                await connections_db.update_one(
                    {"private_channel_id": source_id},
                    {"$set": {
                        "user_id": user_id,
                        "public_channel_id": chat_id,
                        "channel_name": source_title,
                        "topic_id": topic_id
                    }},
                    upsert=True
                )

                # Pending list se hata do
                PENDING_SOURCES.pop(user_id, None)

                await app.send_message(
                    user_id,
                    f"🎯 <b>TARGET DETECTED & AUTOMATICALLY CONNECTED!</b>\n\n"
                    f"✅ <b>Source:</b> {source_title}\n"
                    f"✅ <b>Target:</b> {chat_title}\n\n"
                    f"🎉 <i>Bina ID copy-paste kiye, aapke dono channels successfully connect ho gaye hain!</i>",
                    parse_mode=ParseMode.HTML
                )



# ==========================================
# 🛑 1. GLOBAL DAILY ACCESS LIMIT COMMAND
# ==========================================
@app.on_message(filters.command("dailyaccess") & filters.private)
async def set_global_daily_access(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)

    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ <b>Format:</b> <code>/dailyaccess [Number]</code>\n💡 Set default limit for ALL your channels.\n<i>(Set 0 for Unlimited)</i>", parse_mode=ParseMode.HTML, protect_content=True)

    try:
        limit = int(args[1])
        if limit < 0: raise ValueError
    except ValueError:
        return await message.reply_text("❌ <b>Please provide a valid number.</b>", parse_mode=ParseMode.HTML)

    await sudo_db.update_one(
        {"user_id": owner_id},
        {"$set": {"global_daily_limit": limit}},
        upsert=True
    )

    msg = f"✅ <b>Global Daily Limit Set!</b>\nUsers can watch <b>{limit} videos per day</b> from your channels." if limit > 0 else "✅ <b>Global Limit Removed! (Unlimited)</b>"
    await message.reply_text(msg, parse_mode=ParseMode.HTML)


# ==========================================
# 🛑 2. SPECIFIC CHANNEL ACCESS LIMIT
# ==========================================
@app.on_message(filters.command("channelaccess") & filters.private)
async def set_channel_daily_access(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)

    args = message.text.split()
    if len(args) < 3:
        return await message.reply_text("❌ <b>Format:</b> <code>/channelaccess -100ChannelID [Number]</code>\n💡 <i>To remove channel limit and use Global limit, type:</i>\n<code>/channelaccess -100ChannelID default</code>", parse_mode=ParseMode.HTML)

    try:
        channel_id = int(args[1])
        limit_str = args[2].lower()

        # Check if the channel belongs to this owner
        conn = await connections_db.find_one({"private_channel_id": channel_id, "user_id": owner_id})
        if not conn:
            return await message.reply_text("❌ <b>Channel not found or you don't own it.</b>", parse_mode=ParseMode.HTML)

        if limit_str == "default":
            await connections_db.update_one({"private_channel_id": channel_id}, {"$unset": {"custom_limit": ""}})
            return await message.reply_text(f"✅ <b>Custom limit removed for {conn.get('channel_name')}.</b>\nNow global limit will apply.", parse_mode=ParseMode.HTML)
        else:
            limit = int(limit_str)
            if limit < 0: raise ValueError
            await connections_db.update_one({"private_channel_id": channel_id}, {"$set": {"custom_limit": limit}})
            return await message.reply_text(f"✅ <b>Limit for {conn.get('channel_name')} set to {limit} videos/day.</b>", parse_mode=ParseMode.HTML)

    except ValueError:
        return await message.reply_text("❌ <b>Invalid Channel ID or Number format.</b>", parse_mode=ParseMode.HTML)


# ==========================================
# 🗑️ SMART DELETE ALL MESSAGES COMMAND
# ==========================================
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

    status_msg = await message.reply_text("⏳ <b>Channel scan kiya ja raha hai...</b>", parse_mode=ParseMode.HTML)
    
    try:
        # TRICK: Ek dummy message bhej kar latest Message ID nikalna aur Peer ID fix karna
        dummy_msg = await app.send_message(channel_id, "<i>Cleaning in progress...</i>", parse_mode=ParseMode.HTML)
        latest_msg_id = dummy_msg.id
        
        await status_msg.edit_text(f"⏳ <b>Message deletion shuru ho gaya hai!</b>\n<i>Total IDs to scan: {latest_msg_id}</i>\n(Bade channels me thoda time lag sakta hai...)", parse_mode=ParseMode.HTML)
        
        # Ulti ginti me messages delete karna (100 ke bundle me)
        for i in range(latest_msg_id, 0, -100):
            # 100 IDs ki list banana
            message_ids = list(range(i, max(0, i - 100), -1))
            try:
                await app.delete_messages(channel_id, message_ids)
                await asyncio.sleep(2.5) # Telegram Ban (FloodWait) se bachne ke liye aaram
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass # Ignore error agar koi ID already delete ho chuki ho
                
        await status_msg.edit_text(f"✅ <b>Mission Successful!</b> 🧹\n\nChannel ekdam clean ho gaya hai.", parse_mode=ParseMode.HTML)
        
    except Exception as e:
        print(f"Delete Error: {e}")
        await status_msg.edit_text("❌ <b>Error:</b> Bot ko channel nahi mil raha. Kripya channel me ek 'Hi' ka message bhejein taaki bot channel ko cache kar sake, aur fir try karein.", parse_mode=ParseMode.HTML)


# ==========================================
# 🚀 THE MAGIC: CALLBACK TO DM REDIRECT
# ==========================================
@app.on_callback_query(filters.regex(r"^vid_"))
async def handle_video_callback(client, callback_query):
    short_code = callback_query.data.replace("vid_", "")
    global BOT_USERNAME
    if BOT_USERNAME is None:
        bot_info = await app.get_me()
        BOT_USERNAME = bot_info.username
        
    dm_link = f"https://t.me/{BOT_USERNAME}?start={short_code}"
    await callback_query.answer(url=dm_link)

# ==========================================
# 👑 ADMIN COMMANDS (Add/Remove Sudo)
# ==========================================
@app.on_message(filters.command("addsudo") & filters.private)
async def add_sudo_user(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ <b>You do not have permission to use this command.</b>", parse_mode=ParseMode.HTML)
    
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ <b>Format:</b> <code>/addsudo UserID [Days]</code>\nExample: <code>/addsudo 123456789 30</code>", parse_mode=ParseMode.HTML)
    
    try:
        target_id = int(args[1])
        days = int(args[2]) if len(args) > 2 else 30 
        
        expiry_date = datetime.utcnow() + timedelta(days=days)
        
        # Reset last_notified so daily reminders work correctly for renewed users
        await sudo_db.update_one(
            {"user_id": target_id},
            {"$set": {"expiry_date": expiry_date, "last_notified": None}},
            upsert=True
        )
        
        success_msg = (
            "✅ <b>Sudo Added Successfully!</b>\n\n"
            f"👤 <b>User:</b> <code>{target_id}</code>\n"
            f"⏳ <b>Validity:</b> <code>{days} days</code>\n"
            f"📅 <b>Expires on:</b> <code>{expiry_date.strftime('%Y-%m-%d %H:%M UTC')}</code>"
        )
        await message.reply_text(success_msg, parse_mode=ParseMode.HTML)
        
        try:
            await app.send_message(target_id, f"🎉 <b>Congratulations!</b>\nYou have been granted Sudo Access for <code>{days} days</code>.\nYou can now connect your channels.", parse_mode=ParseMode.HTML)
        except:
            pass
            
    except ValueError:
        await message.reply_text("❌ <b>Invalid ID or Days format.</b>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("rmsudo") & filters.private)
async def remove_sudo_user(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ <b>Unauthorized.</b>", parse_mode=ParseMode.HTML)
    
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ <b>Format:</b> <code>/rmsudo UserID</code>", parse_mode=ParseMode.HTML)
    
    try:
        target_id = int(args[1])
        await sudo_db.delete_one({"user_id": target_id})
        await message.reply_text(f"✅ <b>Sudo access removed for user</b> <code>{target_id}</code>.", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.reply_text("❌ <b>Invalid ID format.</b>", parse_mode=ParseMode.HTML)

# ==========================================
# COMMANDS (Connect, Status, Broadcast)
# ==========================================
@app.on_message(filters.command("connect") & filters.private)
async def connect_channels(client, message):
    user_id = message.from_user.id
    
    if not await is_sudo(user_id):
        return await message.reply_text("❌ <b>Access Denied!</b>\nYou need an active Sudo Subscription to use this bot.", parse_mode=ParseMode.HTML)

    try:
        args = message.text.split()
        if len(args) != 3:
            return await message.reply_text("❌ <b>Invalid format!</b>\nUse: <code>/connect -100PrivateID -100PublicID</code>", parse_mode=ParseMode.HTML)
        
        priv_id = int(args[1])
        pub_id = int(args[2])

        try:
            priv_info = await app.get_chat(priv_id)
            await app.get_chat(pub_id) 
            channel_name = priv_info.title
        except Exception:
            return await message.reply_text("❌ <b>Error!</b> Ensure the bot is an Admin in BOTH channels.", parse_mode=ParseMode.HTML)

        existing_connection = await connections_db.find_one({"private_channel_id": priv_id})
        
        await connections_db.update_one(
            {"private_channel_id": priv_id},
            {"$set": {
                "user_id": user_id, 
                "public_channel_id": pub_id,
                "channel_name": channel_name
            }},
            upsert=True
        )
        
        if existing_connection and existing_connection.get("topic_id"):
            success_text = "✅ <b>Channel Connection Updated!</b>"
        else:
            success_text = "✅ <b>Channels Successfully Connected!</b>"
            
        await message.reply_text(success_text, parse_mode=ParseMode.HTML)
    except ValueError:
        await message.reply_text("❌ <b>IDs must be numeric.</b>", parse_mode=ParseMode.HTML)


@app.on_message(filters.command("status") & filters.private)
async def show_status(client, message):
    user_id = message.from_user.id
    
    if not await is_sudo(user_id):
        return await message.reply_text("❌ <b>Access Denied!</b>\nYour subscription has expired.", parse_mode=ParseMode.HTML)

    # 🖥 Bot Par Load (CPU & RAM)
    cpu_usage = psutil.cpu_percent(interval=0.5)
    ram_usage = psutil.virtual_memory().percent

    global_data = await stats_db.find_one({"type": "global"}) or {}
    total_views = global_data.get("total_video_views", 0)
    total_files = global_data.get("total_files_processed", 0)

    stats_msg = (
        "📊 <b><u>BOT SYSTEM STATUS</u></b> 📊\n\n"
        "🖥 <b><u>Server Load:</u></b>\n"
        f"▪️ <b>CPU Usage:</b> <code>{cpu_usage}%</code>\n"
        f"▪️ <b>RAM Usage:</b> <code>{ram_usage}%</code>\n\n"
        "🌐 <b><u>Global Bot Stats:</u></b>\n"
        f"▪️ Total Files Processed: <code>{total_files}</code>\n"
        f"▪️ Total Video Views: <code>{total_views}</code>\n\n"
    )

    # 👑 Agar Admin command use kare toh SABKA DATA dikhega
    if user_id == ADMIN_ID:
        stats_msg += "👥 <b><u>All Connected Users & Channels:</u></b>\n\n"
        
        connections = await connections_db.find({}).to_list(length=None)
        
        # Data ko user ke hisaab se group karna
        user_channels = {}
        for conn in connections:
            uid = conn["user_id"]
            if uid not in user_channels:
                user_channels[uid] = []
            user_channels[uid].append(conn.get("channel_name", "Unknown Channel"))
            
        if not user_channels:
            stats_msg += "<i>No channels connected yet.</i>\n"
        else:
            for uid, channels in user_channels.items():
                # Owner ka naam nikalna
                u_data = await users_db.find_one({"user_id": uid})
                u_name = u_data.get("name", f"User {uid}") if u_data else f"User {uid}"
                
                # Expiry nikalna
                sudo_u = await sudo_db.find_one({"user_id": uid})
                if uid == ADMIN_ID:
                    expiry_text = "Lifetime Access 👑"
                elif sudo_u and sudo_u.get("expiry_date"):
                    days_left = (sudo_u["expiry_date"] - datetime.utcnow()).days
                    if days_left < 0:
                        expiry_text = "Expired ❌"
                    else:
                        expiry_text = f"{days_left} Days remaining"
                else:
                    expiry_text = "No Sudo ❌"

                # Us owner ke sabhi channels ke total views nikalna
                owner_stats = await viewer_stats_db.find({"owner_id": uid}).to_list(length=None)
                owner_total_views = sum(stat.get("view_count", 0) for stat in owner_stats)

                # Format karna
                stats_msg += f"👤 <b>Owner:</b> {u_name} (<code>{uid}</code>)\n"
                stats_msg += f"⏳ <b>Sudo Expires In:</b> <code>{expiry_text}</code>\n"
                stats_msg += f"👀 <b>Total Watch/Access:</b> <code>{owner_total_views} Views</code>\n"
                stats_msg += f"📢 <b>Connected Channels:</b>\n"
                for ch in channels:
                    stats_msg += f"   ├ <code>{ch}</code>\n"
                stats_msg += "━━━━━━━━━━━━━━━━━━━━\n"
                
    # 👤 Agar Sudo User (Non-Admin) use kare toh sirf APNA DATA dikhega
    else:
        u_channels = await connections_db.count_documents({"user_id": user_id})
        owner_stats = await viewer_stats_db.find({"owner_id": user_id}).to_list(length=None)
        u_views = sum(stat.get("view_count", 0) for stat in owner_stats)
        
        sudo_u = await sudo_db.find_one({"user_id": user_id})
        expiry_txt = "Unknown"
        if sudo_u and sudo_u.get("expiry_date"):
            days_left = (sudo_u["expiry_date"] - datetime.utcnow()).days
            expiry_txt = f"{days_left} Days remaining"

        stats_msg += (
            "👤 <b><u>Your Data:</u></b>\n"
            f"▪️ Your Channels: <code>{u_channels}</code>\n"
            f"▪️ Your Total Views: <code>{u_views}</code>\n"
            f"⏳ Subscription: <code>{expiry_txt}</code>\n\n"
            "👁️ Type <code>/videoaccess</code> to check your viewers in detail."
        )

    # Message limit check
    if len(stats_msg) > 4000:
        stats_msg = stats_msg[:4000] + "...\n\n⚠️ <i>Data is too long to show fully.</i>"

    await message.reply_text(stats_msg, parse_mode=ParseMode.HTML)
    

@app.on_message(filters.command("videoaccess") & filters.private)
async def video_access_stats(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b> Active subscription required.", parse_mode=ParseMode.HTML)

    # Database se top 100 views uthayenge, descending order me (sabse zyada views wale pehle)
    stats = await viewer_stats_db.find({"owner_id": owner_id}).sort("view_count", -1).to_list(length=100)
    
    if not stats:
        return await message.reply_text("📉 <b>No views recorded yet.</b>", parse_mode=ParseMode.HTML)
        
    # Data ko channel ke hisaab se group karne ka logic
    channel_data = {}
    for stat in stats:
        c_name = stat.get("channel_name", "Unknown Channel")
        if c_name not in channel_data:
            channel_data[c_name] = []
        channel_data[c_name].append(stat)
        
    text = "👥 <b><u>Channel-Wise Top Viewer Data:</u></b>\n\n"
    
    # Text format banana
    c_idx = 1
    for c_name, viewers in channel_data.items():
        text += f"<b>{c_idx}. 📢 Channel:</b> <code>{c_name}</code>\n"
        
        v_idx = 1
        for v in viewers:
            v_name = v.get("viewer_name", "Unknown")
            v_count = v.get("view_count", 0)
            text += f"   ├ <b>{v_idx}.</b> {v_name} - <b>{v_count} Videos</b>\n"
            v_idx += 1
            
        text += "━━━━━━━━━━━━━━━━━━━━\n"
        c_idx += 1
        
    text += f"\n💡 <i>Total Unique Viewers (Top 100): {len(stats)}</i>"
    
    # Telegram ki 4096 character limit cross na ho, isliye safety check
    if len(text) > 4000:
        text = text[:4000] + "...\n\n⚠️ <i>Message is too long, showing top results only.</i>"
        
    await message.reply_text(text, parse_mode=ParseMode.HTML)


# ==========================================
# 🔌 DISCONNECT CHANNEL COMMAND
# ==========================================
@app.on_message(filters.command("disconnect") & filters.private)
async def disconnect_channels(client, message):
    user_id = message.from_user.id
    
    if not await is_sudo(user_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
    
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text(
            "❌ <b>Format:</b> <code>/disconnect -100PrivateChannelID</code>\n\n"
            "💡 <i>Apne channels ki ID dekhne ke liye 'Connected Channels' button dabayein.</i>", 
            parse_mode=ParseMode.HTML
        )
    
    try:
        channel_id = int(args[1])
        # Database se connection delete karna
        result = await connections_db.delete_one({"private_channel_id": channel_id, "user_id": user_id})
        
        if result.deleted_count > 0:
            await message.reply_text("✅ <b>Channel Successfully Disconnected!</b>\nAb is channel se videos forward/process nahi hongi.", parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("❌ <b>Channel nahi mila ya aap iske owner nahi hain.</b>", parse_mode=ParseMode.HTML)
            
    except ValueError:
        await message.reply_text("❌ <b>Invalid Channel ID.</b> Kripya sahi ID daalein (jaise: -100123456789).", parse_mode=ParseMode.HTML)



# ==========================================
# 📢 BROADCAST TO PRIVATE CHANNELS COMMAND
# ==========================================
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_to_private_channels(client, message):
    user_id = message.from_user.id
    
    # 1. Verify Sudo Access
    if not await is_sudo(user_id):
        return await message.reply_text("❌ <b>Access Denied!</b> Active subscription required.", parse_mode=ParseMode.HTML)

    # 2. Check if there is a message to broadcast (supports replies and text)
    reply_msg = message.reply_to_message
    command_text = message.text.split(None, 1)

    if not reply_msg and len(command_text) < 2:
        return await message.reply_text(
            "❌ <b>Format Error!</b>\n\n"
            "Please use the command like this:\n"
            "👉 <code>/broadcast Your message here</code>\n\n"
            "Or reply to any message/media with <code>/broadcast</code>.", 
            parse_mode=ParseMode.HTML
        )

    # 3. Let the user know the process has started
    status_msg = await message.reply_text("⏳ <b>Fetching your connected private channels...</b>", parse_mode=ParseMode.HTML)

    # 4. Find all private channels connected by this specific user
    user_connections = await connections_db.find({"user_id": user_id}).to_list(length=None)
    
    if not user_connections:
        return await status_msg.edit_text("❌ <b>No channels found!</b> You haven't connected any private channels yet.", parse_mode=ParseMode.HTML)

    await status_msg.edit_text(f"🚀 <b>Broadcasting to {len(user_connections)} channels...</b>\n<i>Please wait...</i>", parse_mode=ParseMode.HTML)

    successful = 0
    failed = 0

    # 5. Loop through channels and safely send the message
    for conn in user_connections:
        target_chat_id = conn.get("private_channel_id")
        if not target_chat_id:
            continue

        try:
            if reply_msg:
                # If they replied to a message, copy it (this supports media, buttons, etc.)
                await reply_msg.copy(target_chat_id)
            else:
                # If they just typed text, send the text
                text_to_send = command_text[1]
                await app.send_message(target_chat_id, text_to_send, parse_mode=ParseMode.HTML)
            
            successful += 1
            await asyncio.sleep(1.5) # Sleep to avoid Telegram's spam filters (FloodWait)
            
        except FloodWait as e:
            # If we send too fast, wait the required time and try again
            await asyncio.sleep(e.value)
            try:
                if reply_msg:
                    await reply_msg.copy(target_chat_id)
                else:
                    await app.send_message(target_chat_id, command_text[1], parse_mode=ParseMode.HTML)
                successful += 1
            except:
                failed += 1
        except Exception as e:
            print(f"Broadcast failed for channel {target_chat_id}: {e}")
            failed += 1

    # 6. Final Report to the user
    report = (
        "✅ <b>Broadcast Completed!</b>\n\n"
        f"📢 <b>Target Channels:</b> {len(user_connections)}\n"
        f"✅ <b>Successfully Sent:</b> {successful}\n"
        f"❌ <b>Failed:</b> {failed}"
    )
    await status_msg.edit_text(report, parse_mode=ParseMode.HTML)



# ==========================================
# 2️⃣ MESSAGE CATCHER 
# ==========================================
@app.on_message(filters.channel)
async def enqueue_message(client, message):
    chat_id = message.chat.id
    connection = await connections_db.find_one({"private_channel_id": chat_id})
    
    if connection:
        owner_id = connection.get("user_id")
        if not await is_sudo(owner_id):
            return 

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
                    await app.get_chat(SPECIAL_GROUP_ID)
                    peer = await app.resolve_peer(SPECIAL_GROUP_ID)
                    channel_input = InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash)

                    raw_result = await app.invoke(
                        CreateForumTopic(
                            channel=channel_input,
                            title=channel_name[:128],
                            random_id=random.randint(100000, 999999999) 
                        )
                    )
                    
                    if hasattr(raw_result, 'updates'):
                        for upd in raw_result.updates:
                            if hasattr(upd, 'message') and hasattr(upd.message, 'id'):
                                topic_id = upd.message.id
                                break
                            elif hasattr(upd, 'id'):
                                topic_id = upd.id
                                break
                    
                    if topic_id:
                        await connections_db.update_one(
                            {"private_channel_id": chat_id},
                            {"$set": {"topic_id": topic_id}}
                        )
                except Exception as e:
                    print(f"Auto Topic Error: {e}")
                finally:
                    TOPIC_LOCKS.pop(chat_id, None)

        await message_queue.put({
            "message": message, 
            "public_id": connection["public_channel_id"],
            "topic_id": topic_id
        })

# ==========================================
# 3️⃣ BACKGROUND WORKERS
# ==========================================
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
            message = item["message"]
            public_channel_id = item["public_id"]
            topic_id = item["topic_id"]
            
            try:
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
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    await app.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except Exception as e:
                    print(f"⚠️ Supergroup backup error: {e}")

                is_video = False
                if message.video:
                    is_video = True
                elif message.document and message.document.file_name:
                    ext = message.document.file_name.lower()
                    if ext.endswith(('.mp4', '.mkv', '.avi', '.webm')):
                        is_video = True

                try:
                    if is_video:
                        caption = message.caption if message.caption else "🎬 <b>New Video Uploaded!</b>\n\n<i>Click below to watch.</i>"
                        short_code = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                        
                        await links_db.insert_one({
                            "short_code": short_code,
                            "chat_id": chat_id,
                            "msg_id": msg_id
                        })
                        
                        button = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Video", callback_data=f"vid_{short_code}")]])
                        await app.send_message(chat_id=public_channel_id, text=caption, reply_markup=button, parse_mode=ParseMode.HTML, protect_content=True)
                    else:
                        await app.copy_message(chat_id=public_channel_id, from_chat_id=chat_id, message_id=msg_id)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    if is_video:
                        await app.send_message(chat_id=public_channel_id, text=caption, reply_markup=button, parse_mode=ParseMode.HTML, protect_content=True)
                    else:
                        await app.copy_message(chat_id=public_channel_id, from_chat_id=chat_id, message_id=msg_id)

                await stats_db.update_one({"type": "global"}, {"$inc": {"total_files_processed": 1}}, upsert=True)

            except Exception as e:
                print(f"❌ Error processing message: {e}")
                
            finally:
                await asyncio.sleep(2) 

# 🔔 🆕 Expiration Reminder & Pin Background Task
async def check_expirations():
    while True:
        try:
            now = datetime.utcnow()
            warning_time = now + timedelta(days=3) 
            
            # Un users ko dhundo jinka time 3 din se kam reh gaya hai aur jo expire nahi huye hain
            expiring_users = await sudo_db.find({
                "expiry_date": {"$lte": warning_time, "$gt": now}
            }).to_list(length=None)

            for user in expiring_users:
                try:
                    user_id = user["user_id"]
                    last_notified = user.get("last_notified")
                    
                    # Agar pehle kabhi notify nahi kiya ya 23 ghante se zyada ho gaye (Daily reminder logic)
                    if not last_notified or (now - last_notified).total_seconds() > 82800:
                        days_left = (user["expiry_date"] - now).days
                        hours_left = int((user["expiry_date"] - now).seconds / 3600)
                        
                        time_left_text = f"<b>{days_left} days and {hours_left} hours</b>" if days_left > 0 else f"<b>{hours_left} hours</b>"

                        msg_text = (
                            "⚠️ <b><u>URGENT SUBSCRIPTION ALERT!</u></b> ⚠️\n\n"
                            f"Your Sudo access to the Auto File Store Bot is expiring in less than {time_left_text}.\n\n"
                            "⏳ <b>If you do not renew, the bot will automatically stop forwarding messages from your channels.</b>\n\n"
                            "👉 <i>Please contact the Administrator immediately to renew your subscription.</i>"
                        )
                        
                        # Message Bhejna aur Pin karna
                        sent_msg = await app.send_message(user_id, msg_text, parse_mode=ParseMode.HTML, protect_content=True)
                        try:
                            await sent_msg.pin(both_sides=True) # Dono taraf pin ho jayega
                        except Exception as pin_err:
                            print(f"Pin failed for {user_id}: {pin_err}")
                        
                        # Database me update karna ki aaj message bhej diya gaya hai
                        await sudo_db.update_one({"user_id": user_id}, {"$set": {"last_notified": now}})
                except Exception as e:
                    print(f"Could not send daily reminder to {user.get('user_id')}: {e}")
                    
        except Exception as e:
            print(f"Error in expiry checker: {e}")
            
        # Loop ko har 6 ghante me ek baar chalayenge (lekin message sirf 23 hours baad hi jayega if condition ki wajah se)
        await asyncio.sleep(21600) 

# ==========================================
# 🚀 RUN THE BOT
# ==========================================
if __name__ == "__main__":
    print("🚀 Premium UI Bot with Daily Pin Alerts is starting up...")
    loop = asyncio.get_event_loop()
    loop.create_task(process_queue())
    loop.create_task(check_expirations()) 
    app.run()


