
import asyncio
import random
import string
import psutil
import subprocess
import sys
from datetime import datetime, timedelta
from pyrogram import Client, idle, filters
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated
)
# ForumTopic raw imports removed as we use built-in method now
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram.errors import FloodWait, PeerIdInvalid


# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_ID = 20137104
API_HASH = "1209338eedc55ab701dd2e9d353c05ad"
#BOT_TOKEN = "8797515244:AAEP7za-JSFuqLuSV1IHtE1lXFn2nmwVLeY"

BOT_TOKEN = "8718799133:AAFR1Fzduqgf9m-h-KOSy5cBHyesSQWFPfU"
ADMIN_ID = 5050578106


MONGO_URI = "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies"
SPECIAL_GROUP_ID = -1003667939361
ADMIN_ID = 5050578106

# ==========================================
# INITIALIZATION
# ==========================================
app = Client(
    "multi_user_store_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

db_client = AsyncIOMotorClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=20000,
    maxIdleTimeMS=50000
)
db = db_client["telegram_file_bot"]
connections_db   = db["channel_connections"]
stats_db         = db["bot_stats"]
viewer_stats_db  = db["viewer_stats"]
users_db         = db["all_users"]
links_db         = db["short_links"]
sudo_db          = db["sudo_users"]
daily_access_db  = db["daily_access_tracker"]
engine_bots_db   = db["engine_bots"]          # 🆕 Engine Bot storage
pending_connect_db = db["pending_connect"]    # 🆕 Pending channel-connect state

message_queue = asyncio.Queue()
BOT_USERNAME  = None
TOPIC_LOCKS   = {}

# Slave bot processes {bot_token: subprocess}
slave_processes: dict[str, subprocess.Popen] = {}


# ==========================================
# 🛡️ SUDO VERIFICATION
# ==========================================
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
# 🔧 HELPER: Build Main Dashboard Keyboard
# ==========================================
async def get_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    is_admin = (user_id == ADMIN_ID)
    sudo     = await is_sudo(user_id)

    rows = []

    if sudo:
        rows.append([
            InlineKeyboardButton("🔗 Connect New Channel", callback_data="menu_connect"),
            InlineKeyboardButton("👁 View Connections",    callback_data="menu_view_connections"),
        ])
        rows.append([
            InlineKeyboardButton("📊 My Stats",           callback_data="menu_status"),
            InlineKeyboardButton("👥 Video Access",       callback_data="va_page_0"),
        ])
        rows.append([
            InlineKeyboardButton("⚙️ Daily Limit (Global)", callback_data="menu_dailyaccess"),
        ])

    if is_admin:
        rows.append([
            InlineKeyboardButton("🚀 Engine Dashboard",   callback_data="engine_dashboard"),
            InlineKeyboardButton("👑 Admin Stats",        callback_data="admin_stats"),
        ])

    if not sudo and not is_admin:
        rows.append([InlineKeyboardButton("ℹ️ About",     callback_data="menu_about")])

    return InlineKeyboardMarkup(rows)


# ==========================================
# 1️⃣ /start  — Interactive Dashboard
# ==========================================
@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    global BOT_USERNAME
    if BOT_USERNAME is None:
        bot_info = await client.get_me()
        BOT_USERNAME = bot_info.username

    text    = message.text
    user_id = message.from_user.id
    name    = message.from_user.first_name or "User"

    await users_db.update_one({"user_id": user_id}, {"$set": {"name": name}}, upsert=True)

    # Deep-link: video fetch
    if len(text.split()) > 1:
        short_code = text.split()[1]
        # ── skip menu codes ──
        if not short_code.startswith("connect_"):
            await _deliver_video(client, message, short_code, user_id, name)
            return

    sudo = await is_sudo(user_id)
    welcome = (
        f"👋 <b>Hello, {name}!</b>\n\n"
        "🤖 <b>Auto File Store Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Use the buttons below to manage your channels.\n\n"
        "⚠️ <i>An active Sudo Subscription is required to connect channels.</i>"
    ) if sudo else (
        f"👋 <b>Welcome, {name}!</b>\n\n"
        "🤖 <b>Auto File Store Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <b>You do not have an active subscription.</b>\n"
        "Please contact the admin to get Sudo access."
    )

    kb = await get_main_keyboard(user_id)
    await message.reply_text(welcome, reply_markup=kb, parse_mode=ParseMode.HTML)


# ── Video delivery helper ──────────────────────────────────────────────────────
async def _deliver_video(client, message, short_code, viewer_id, viewer_name):
    try:
        link_data = await links_db.find_one({"short_code": short_code})
        if not link_data:
            return await message.reply_text(
                "❌ <b>Sorry, this link has expired or does not exist.</b>",
                parse_mode=ParseMode.HTML
            )

        p_chat_id = link_data["chat_id"]
        msg_id    = link_data["msg_id"]

        connection = await connections_db.find_one({"private_channel_id": p_chat_id})
        if connection:
            owner_id     = connection["user_id"]
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
                    today_date    = datetime.utcnow().strftime("%Y-%m-%d")
                    access_record = await daily_access_db.find_one({
                        "viewer_id": viewer_id, "channel_id": p_chat_id, "date": today_date
                    })
                    current_count = access_record.get("count", 0) if access_record else 0
                    if current_count >= active_limit:
                        return await message.reply_text(
                            f"🚫 <b>Daily Limit Reached!</b>\n\nMax <b>{active_limit} videos/day</b> from <b>{channel_name}</b>.\n⏳ Come back tomorrow!",
                            parse_mode=ParseMode.HTML
                        )
                    await daily_access_db.update_one(
                        {"viewer_id": viewer_id, "channel_id": p_chat_id, "date": today_date},
                        {"$inc": {"count": 1}}, upsert=True
                    )

            await viewer_stats_db.update_one(
                {"owner_id": owner_id, "viewer_id": viewer_id, "channel_name": channel_name},
                {"$inc": {"view_count": 1}, "$set": {"viewer_name": viewer_name}}, upsert=True
            )

        await client.copy_message(
            chat_id=message.chat.id, from_chat_id=p_chat_id,
            message_id=msg_id, protect_content=True
        )
        await stats_db.update_one({"type": "global"}, {"$inc": {"total_video_views": 1}}, upsert=True)

    except FloodWait as e:
        await asyncio.sleep(e.value)
        await message.reply_text("⏳ Server busy. Try again in a few seconds.", parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"Video delivery error: {e}")
        await message.reply_text("❌ <b>A technical error occurred.</b>", parse_mode=ParseMode.HTML)


# ==========================================
# 🔗 CONNECT FLOW — Button-Driven
# ==========================================

# ── Step 1: Show connect sub-menu ──────────────────────────────────────────────
@app.on_callback_query(filters.regex("^menu_connect$"))
async def cb_menu_connect(client, cq):
    user_id = cq.from_user.id
    if not await is_sudo(user_id):
        return await cq.answer("❌ Access Denied!", show_alert=True)

    global BOT_USERNAME
    if BOT_USERNAME is None:
        bot_info = await client.get_me()
        BOT_USERNAME = bot_info.username

    add_url = f"https://t.me/{BOT_USERNAME}?startchannel=admin"

    text = (
        "🔗 <b>Connect New Channel</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>Step 1:</b> Click <b>⚡ Auto Add Source +</b> and add the bot as admin to your <b>Private/Source</b> channel.\n\n"
        "<b>Step 2:</b> Click <b>⚡ Auto Add Target +</b> and add the bot as admin to your <b>Public/Target</b> channel.\n\n"
        "<b>Step 3:</b> After both are done, click <b>✅ Link Channels</b>.\n\n"
        "ℹ️ <i>The bot will auto-detect channels when you promote it to admin.</i>"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Auto Add Source +", url=add_url)],
        [InlineKeyboardButton("⚡ Auto Add Target +", url=add_url)],
        [InlineKeyboardButton("✅ Link Channels",     callback_data=f"connect_link_{user_id}")],
        [InlineKeyboardButton("🔙 Back",              callback_data="menu_back")],
    ])
    await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


# ── Auto-catch when bot is promoted to admin in ANY channel ────────────────────
@app.on_chat_member_updated()
async def on_bot_promoted(client, update: ChatMemberUpdated):
    if update.new_chat_member is None:
        return
    me = await client.get_me()
    if update.new_chat_member.user.id != me.id:
        return

    new_status = update.new_chat_member.status
    is_admin_now = new_status in (
        ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER
    )
    if not is_admin_now:
        return

    chat    = update.chat
    chat_id = chat.id
    title   = chat.title or f"Channel {chat_id}"

    promoter_id = None
    if update.from_user:
        promoter_id = update.from_user.id

    if promoter_id is None:
        return

    pending = await pending_connect_db.find_one({"user_id": promoter_id}) or {}

    if "source_id" not in pending:
        await pending_connect_db.update_one(
            {"user_id": promoter_id},
            {"$set": {"source_id": chat_id, "source_name": title}},
            upsert=True
        )
        try:
            await client.send_message(
                promoter_id,
                f"✅ <b>Source channel detected!</b>\n📢 <b>{title}</b>\n\nNow add the bot as admin to your <b>Target/Public</b> channel.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    elif "target_id" not in pending:
        await pending_connect_db.update_one(
            {"user_id": promoter_id},
            {"$set": {"target_id": chat_id, "target_name": title}},
            upsert=True
        )
        try:
            await client.send_message(
                promoter_id,
                f"✅ <b>Target channel detected!</b>\n📢 <b>{title}</b>\n\nBoth channels are ready! Click <b>✅ Link Channels</b> in the menu to complete the connection.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass


# ── Step 3: Link the two saved channels ───────────────────────────────────────
@app.on_callback_query(filters.regex(r"^connect_link_(\d+)$"))
async def cb_connect_link(client, cq):
    user_id = cq.from_user.id
    if not await is_sudo(user_id):
        return await cq.answer("❌ Access Denied!", show_alert=True)

    pending = await pending_connect_db.find_one({"user_id": user_id})

    if not pending or "source_id" not in pending or "target_id" not in pending:
        return await cq.answer(
            "⚠️ Both channels not detected yet.\nAdd the bot as admin to Source AND Target channels first.",
            show_alert=True
        )

    source_id   = pending["source_id"]
    target_id   = pending["target_id"]
    source_name = pending.get("source_name", f"Channel {source_id}")

    await connections_db.update_one(
        {"private_channel_id": source_id},
        {"$set": {
            "user_id": user_id,
            "public_channel_id": target_id,
            "channel_name": source_name
        }},
        upsert=True
    )

    await pending_connect_db.delete_one({"user_id": user_id})

    kb = await get_main_keyboard(user_id)
    await cq.message.edit_text(
        f"✅ <b>Channels Successfully Linked!</b>\n\n"
        f"📥 Source: <code>{source_name}</code>\n"
        f"📤 Target ID: <code>{target_id}</code>",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )
    await cq.answer("✅ Done!")

# ==========================================
# 👁 VIEW CONNECTIONS
# ==========================================
@app.on_callback_query(filters.regex("^menu_view_connections$"))
async def cb_view_connections(client, cq):
    user_id = cq.from_user.id
    if not await is_sudo(user_id):
        return await cq.answer("❌ Access Denied!", show_alert=True)

    conns = await connections_db.find({"user_id": user_id}).to_list(length=None)

    if not conns:
        return await cq.answer("No channels connected yet.", show_alert=True)

    text = "👁 <b>Your Connected Channels:</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
    for i, c in enumerate(conns, 1):
        limit = c.get("custom_limit", "Global")
        text += (
            f"<b>{i}. {c.get('channel_name', 'Unknown')}</b>\n"
            f"   📥 Source: <code>{c['private_channel_id']}</code>\n"
            f"   📤 Target: <code>{c['public_channel_id']}</code>\n"
            f"   ⏱ Daily Limit: <code>{limit}</code>\n\n"
        )

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]])
    await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


# ==========================================
# 📊 STATUS (My Stats)
# ==========================================
@app.on_callback_query(filters.regex("^menu_status$"))
async def cb_status(client, cq):
    user_id = cq.from_user.id
    if not await is_sudo(user_id):
        return await cq.answer("❌ Access Denied!", show_alert=True)

    u_channels  = await connections_db.count_documents({"user_id": user_id})
    owner_stats = await viewer_stats_db.find({"owner_id": user_id}).to_list(length=None)
    u_views     = sum(s.get("view_count", 0) for s in owner_stats)
    sudo_u      = await sudo_db.find_one({"user_id": user_id})

    expiry_txt = "Unknown"
    if user_id == ADMIN_ID:
        expiry_txt = "Lifetime 👑"
    elif sudo_u and sudo_u.get("expiry_date"):
        days_left  = (sudo_u["expiry_date"] - datetime.utcnow()).days
        expiry_txt = f"{days_left} days remaining"

    text = (
        "📊 <b>Your Stats:</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📢 Connected Channels: <code>{u_channels}</code>\n"
        f"👀 Total Video Views: <code>{u_views}</code>\n"
        f"⏳ Subscription: <code>{expiry_txt}</code>"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]])
    await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


# ==========================================
# ⚙️ Daily Limit Prompt
# ==========================================
@app.on_callback_query(filters.regex("^menu_dailyaccess$"))
async def cb_daily_access_prompt(client, cq):
    user_id = cq.from_user.id
    if not await is_sudo(user_id):
        return await cq.answer("❌ Access Denied!", show_alert=True)

    text = (
        "⚙️ <b>Set Global Daily Limit</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Send a command in chat:\n"
        "➠ <code>/dailyaccess [Number]</code>\n"
        "Example: <code>/dailyaccess 5</code>\n\n"
        "For per-channel limit:\n"
        "➠ <code>/channelaccess -100ChannelID [Number]</code>"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]])
    await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


# ── Back button ────────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex("^menu_back$"))
async def cb_back(client, cq):
    user_id = cq.from_user.id
    name    = cq.from_user.first_name or "User"
    sudo    = await is_sudo(user_id)

    welcome = (
        f"👋 <b>Hello, {name}!</b>\n\n"
        "🤖 <b>Auto File Store Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Use the buttons below to manage your channels."
    ) if sudo else (
        f"👋 <b>Welcome, {name}!</b>\n\n"
        "⚠️ <b>You do not have an active subscription.</b>"
    )
    kb = await get_main_keyboard(user_id)
    await cq.message.edit_text(welcome, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


@app.on_callback_query(filters.regex("^menu_about$"))
async def cb_about(client, cq):
    text = (
        "ℹ️ <b>About This Bot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "This bot automatically stores and forwards files from a private source channel to a public target channel.\n\n"
        "🔑 <b>Need access?</b> Contact the administrator."
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]])
    await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


# ==========================================
# 👥 VIDEO ACCESS — Paginated (10 per page)
# ==========================================
PAGE_SIZE = 10

@app.on_callback_query(filters.regex(r"^va_page_(\d+)$"))
async def cb_video_access_page(client, cq):
    owner_id = cq.from_user.id
    if not await is_sudo(owner_id):
        return await cq.answer("❌ Access Denied!", show_alert=True)

    page = int(cq.data.split("_")[-1])

    all_stats = await viewer_stats_db.find({"owner_id": owner_id}).sort("view_count", -1).to_list(length=None)
    total     = len(all_stats)

    if not all_stats:
        return await cq.answer("No views recorded yet.", show_alert=True)

    start = page * PAGE_SIZE
    end   = start + PAGE_SIZE
    slice_= all_stats[start:end]

    text = f"👥 <b>Video Access Stats</b> (Page {page+1}/{(total-1)//PAGE_SIZE+1})\n━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, s in enumerate(slice_, start=start+1):
        v_name  = s.get("viewer_name", "Unknown")
        v_count = s.get("view_count", 0)
        c_name  = s.get("channel_name", "Unknown Channel")
        text   += f"<b>{i}.</b> {v_name} — <b>{v_count} videos</b>\n   📢 <i>{c_name}</i>\n"

    text += f"\n💡 <i>Total unique viewers: {total}</i>"

    if len(text) > 4000:
        text = text[:4000] + "..."

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Back", callback_data=f"va_page_{page-1}"))
    if end < total:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"va_page_{page+1}"))

    rows = []
    if nav_buttons:
        rows.append(nav_buttons)
    rows.append([InlineKeyboardButton("🔙 Main Menu", callback_data="menu_back")])

    kb = InlineKeyboardMarkup(rows)

    try:
        await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception:
        await cq.answer("Already on this page.", show_alert=False)
    await cq.answer()


@app.on_message(filters.command("videoaccess") & filters.private)
async def video_access_stats(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ <b>Access Denied!</b>", parse_mode=ParseMode.HTML)

    all_stats = await viewer_stats_db.find({"owner_id": owner_id}).sort("view_count", -1).to_list(length=None)
    if not all_stats:
        return await message.reply_text("📉 <b>No views recorded yet.</b>", parse_mode=ParseMode.HTML)

    total  = len(all_stats)
    slice_ = all_stats[:PAGE_SIZE]
    text   = f"👥 <b>Video Access Stats</b> (Page 1/{(total-1)//PAGE_SIZE+1})\n━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, s in enumerate(slice_, 1):
        v_name  = s.get("viewer_name", "Unknown")
        v_count = s.get("view_count", 0)
        c_name  = s.get("channel_name", "Unknown Channel")
        text   += f"<b>{i}.</b> {v_name} — <b>{v_count} videos</b>\n   📢 <i>{c_name}</i>\n"

    text += f"\n💡 <i>Total unique viewers: {total}</i>"
    if len(text) > 4000:
        text = text[:4000] + "..."

    nav = []
    if total > PAGE_SIZE:
        nav.append([InlineKeyboardButton("Next ➡️", callback_data="va_page_1")])
    nav.append([InlineKeyboardButton("🔙 Main Menu", callback_data="menu_back")])

    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(nav), parse_mode=ParseMode.HTML)


# ==========================================
# 👑 ADMIN STATS (from button)
# ==========================================
@app.on_callback_query(filters.regex("^admin_stats$"))
async def cb_admin_stats(client, cq):
    if cq.from_user.id != ADMIN_ID:
        return await cq.answer("❌ Unauthorized!", show_alert=True)

    cpu_usage = psutil.cpu_percent(interval=0.5)
    ram_usage = psutil.virtual_memory().percent
    gd        = await stats_db.find_one({"type": "global"}) or {}
    total_v   = gd.get("total_video_views", 0)
    total_f   = gd.get("total_files_processed", 0)

    connections = await connections_db.find({}).to_list(length=None)
    user_map = {}
    for c in connections:
        uid = c["user_id"]
        user_map.setdefault(uid, []).append(c.get("channel_name", "Unknown"))

    msg = (
        "📊 <b>Admin System Status</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🖥 CPU: <code>{cpu_usage}%</code>  |  RAM: <code>{ram_usage}%</code>\n"
        f"📁 Files Processed: <code>{total_f}</code>\n"
        f"👀 Total Views: <code>{total_v}</code>\n\n"
        "👥 <b>Users & Channels:</b>\n"
    )

    for uid, channels in user_map.items():
        u_data   = await users_db.find_one({"user_id": uid})
        u_name   = u_data.get("name", f"User {uid}") if u_data else f"User {uid}"
        sudo_u   = await sudo_db.find_one({"user_id": uid})
        if uid == ADMIN_ID:
            exp = "Lifetime 👑"
        elif sudo_u and sudo_u.get("expiry_date"):
            dl  = (sudo_u["expiry_date"] - datetime.utcnow()).days
            exp = f"Expired ❌" if dl < 0 else f"{dl} days"
        else:
            exp = "No Sudo ❌"

        o_stats = await viewer_stats_db.find({"owner_id": uid}).to_list(length=None)
        o_views = sum(s.get("view_count", 0) for s in o_stats)

        msg += f"\n👤 <b>{u_name}</b> (<code>{uid}</code>)\n"
        msg += f"   ⏳ {exp}  |  👀 {o_views} views\n"
        for ch in channels:
            msg += f"   ├ {ch}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"

    if len(msg) > 4000:
        msg = msg[:4000] + "..."

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]])
    await cq.message.edit_text(msg, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


# ==========================================
# 🚀 VIDEO CALLBACK (DM redirect)
# ==========================================
@app.on_callback_query(filters.regex(r"^vid_"))
async def handle_video_callback(client, cq):
    short_code = cq.data.replace("vid_", "")
    global BOT_USERNAME
    if BOT_USERNAME is None:
        bot_info = await client.get_me()
        BOT_USERNAME = bot_info.username
    await cq.answer(url=f"https://t.me/{BOT_USERNAME}?start={short_code}")


# ==========================================
# 👑 ADMIN COMMANDS (/addsudo, /rmsudo)
# ==========================================
@app.on_message(filters.command("addsudo") & filters.private)
async def add_sudo_user(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ Unauthorized.", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ Format: <code>/addsudo UserID [Days]</code>", parse_mode=ParseMode.HTML)
    try:
        target_id   = int(args[1])
        days        = int(args[2]) if len(args) > 2 else 30
        expiry_date = datetime.utcnow() + timedelta(days=days)
        await sudo_db.update_one(
            {"user_id": target_id},
            {"$set": {"expiry_date": expiry_date, "last_notified": None}},
            upsert=True
        )
        await message.reply_text(
            f"✅ <b>Sudo Added!</b>\n👤 <code>{target_id}</code>\n⏳ {days} days\n📅 Expires: <code>{expiry_date.strftime('%Y-%m-%d %H:%M UTC')}</code>",
            parse_mode=ParseMode.HTML
        )
        try:
            await app.send_message(target_id, f"🎉 <b>Sudo Access granted for {days} days!</b>", parse_mode=ParseMode.HTML)
        except Exception:
            pass
    except ValueError:
        await message.reply_text("❌ Invalid format.", parse_mode=ParseMode.HTML)


@app.on_message(filters.command("rmsudo") & filters.private)
async def remove_sudo_user(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ Unauthorized.", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ Format: <code>/rmsudo UserID</code>", parse_mode=ParseMode.HTML)
    try:
        target_id = int(args[1])
        await sudo_db.delete_one({"user_id": target_id})
        await message.reply_text(f"✅ Sudo removed for <code>{target_id}</code>.", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.reply_text("❌ Invalid ID.", parse_mode=ParseMode.HTML)


# ==========================================
# ⚙️ DAILY/CHANNEL LIMIT COMMANDS
# ==========================================
@app.on_message(filters.command("dailyaccess") & filters.private)
async def set_global_daily_access(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ Access Denied!", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ Format: <code>/dailyaccess [Number]</code>", parse_mode=ParseMode.HTML)
    try:
        limit = int(args[1])
        if limit < 0: raise ValueError
    except ValueError:
        return await message.reply_text("❌ Invalid number.", parse_mode=ParseMode.HTML)
    await sudo_db.update_one({"user_id": owner_id}, {"$set": {"global_daily_limit": limit}}, upsert=True)
    msg = f"✅ Global daily limit set to <b>{limit} videos/day</b>." if limit > 0 else "✅ Global limit removed (Unlimited)."
    await message.reply_text(msg, parse_mode=ParseMode.HTML)


@app.on_message(filters.command("channelaccess") & filters.private)
async def set_channel_daily_access(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ Access Denied!", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 3:
        return await message.reply_text("❌ Format: <code>/channelaccess -100ChannelID [Number|default]</code>", parse_mode=ParseMode.HTML)
    try:
        channel_id = int(args[1])
        limit_str  = args[2].lower()
        conn = await connections_db.find_one({"private_channel_id": channel_id, "user_id": owner_id})
        if not conn:
            return await message.reply_text("❌ Channel not found or you don't own it.", parse_mode=ParseMode.HTML)
        if limit_str == "default":
            await connections_db.update_one({"private_channel_id": channel_id}, {"$unset": {"custom_limit": ""}})
            return await message.reply_text(f"✅ Custom limit removed. Global limit now applies.", parse_mode=ParseMode.HTML)
        limit = int(limit_str)
        if limit < 0: raise ValueError
        await connections_db.update_one({"private_channel_id": channel_id}, {"$set": {"custom_limit": limit}})
        await message.reply_text(f"✅ Limit for <b>{conn.get('channel_name')}</b> set to <b>{limit}/day</b>.", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.reply_text("❌ Invalid format.", parse_mode=ParseMode.HTML)


# ==========================================
# 🗑️ DELETE ALL MESSAGES
# ==========================================
@app.on_message(filters.command("deleteall") & filters.private)
async def delete_all_channel_msgs(client, message):
    owner_id = message.from_user.id
    if not await is_sudo(owner_id):
        return await message.reply_text("❌ Access Denied!", parse_mode=ParseMode.HTML)
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text("❌ Format: <code>/deleteall -100ChannelID</code>", parse_mode=ParseMode.HTML)
    try:
        channel_id = int(args[1])
    except ValueError:
        return await message.reply_text("❌ Invalid Channel ID.", parse_mode=ParseMode.HTML)

    status_msg = await message.reply_text("⏳ Scanning channel...", parse_mode=ParseMode.HTML)
    try:
        dummy_msg      = await client.send_message(channel_id, "<i>Cleaning in progress...</i>", parse_mode=ParseMode.HTML)
        latest_msg_id  = dummy_msg.id
        await status_msg.edit_text(f"⏳ Deleting {latest_msg_id} messages...", parse_mode=ParseMode.HTML)
        for i in range(latest_msg_id, 0, -100):
            ids = list(range(i, max(0, i - 100), -1))
            try:
                await client.delete_messages(channel_id, ids)
                await asyncio.sleep(2.5)
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass
        await status_msg.edit_text("✅ Channel cleaned successfully!", parse_mode=ParseMode.HTML)
    except Exception as e:
        await status_msg.edit_text("❌ Error. Make sure the bot is admin in the channel.", parse_mode=ParseMode.HTML)


# ==========================================
# 🚀 ENGINE BOT FEATURE (Multi-Bot Hosting)
# ==========================================

@app.on_message(filters.command("addnewbot") & filters.private)
async def add_new_bot(client, message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply_text("❌ Unauthorized.", parse_mode=ParseMode.HTML)

    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text(
            "❌ Format: <code>/addnewbot BOT_TOKEN</code>",
            parse_mode=ParseMode.HTML
        )

    token = args[1].strip()

    if ":" not in token or len(token) < 30:
        return await message.reply_text("❌ Invalid bot token format.", parse_mode=ParseMode.HTML)

    existing = await engine_bots_db.find_one({"token": token})
    if existing:
        return await message.reply_text("⚠️ This bot token is already registered.", parse_mode=ParseMode.HTML)

    try:
        test_client = Client(
            f"slave_{token[:8]}",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=token,
            in_memory=True
        )
        await test_client.start()
        bot_info = await test_client.get_me()
        bot_name = bot_info.username
        await test_client.stop()
    except Exception as e:
        return await message.reply_text(f"❌ Could not validate token: {e}", parse_mode=ParseMode.HTML)

    await engine_bots_db.insert_one({
        "token": token,
        "username": bot_name,
        "status": "on",
        "added_at": datetime.utcnow()
    })

    _start_slave_process(token)

    await message.reply_text(
        f"✅ <b>Slave Bot Added!</b>\n🤖 @{bot_name}\n\nBot is now running.",
        parse_mode=ParseMode.HTML
    )


def _start_slave_process(token: str):
    if token in slave_processes:
        proc = slave_processes[token]
        if proc.poll() is None: 
            return
    try:
        proc = subprocess.Popen(
            [sys.executable, __file__, "--slave-token", token],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        slave_processes[token] = proc
        print(f"✅ Slave bot started: {token[:8]}... PID={proc.pid}")
    except Exception as e:
        print(f"❌ Failed to start slave: {e}")


@app.on_callback_query(filters.regex("^engine_dashboard$"))
async def cb_engine_dashboard(client, cq):
    if cq.from_user.id != ADMIN_ID:
        return await cq.answer("❌ Unauthorized!", show_alert=True)

    bots = await engine_bots_db.find({}).to_list(length=None)

    if not bots:
        text = "🚀 <b>Engine Dashboard</b>\n━━━━━━━━━━━━━━━━━━━━\n\n<i>No slave bots added yet.</i>\n\nUse <code>/addnewbot BOT_TOKEN</code> to add one."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]])
        return await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

    text = "🚀 <b>Engine Dashboard</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
    rows = []
    for bot in bots:
        username = bot.get("username", "Unknown")
        status   = bot.get("status", "off")
        icon     = "🟢" if status == "on" else "🔴"
        text += f"{icon} @{username}\n"
        toggle_label = "🔴 Turn OFF" if status == "on" else "🟢 Turn ON"
        rows.append([
            InlineKeyboardButton(
                f"{icon} @{username} — {toggle_label}",
                callback_data=f"engine_toggle_{str(bot['_id'])}"
            )
        ])

    rows.append([InlineKeyboardButton("🔙 Back", callback_data="menu_back")])
    kb = InlineKeyboardMarkup(rows)

    await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer()


@app.on_callback_query(filters.regex(r"^engine_toggle_(.+)$"))
async def cb_engine_toggle(client, cq):
    if cq.from_user.id != ADMIN_ID:
        return await cq.answer("❌ Unauthorized!", show_alert=True)

    from bson import ObjectId
    bot_id = cq.data.split("_", 2)[-1]
    try:
        bot = await engine_bots_db.find_one({"_id": ObjectId(bot_id)})
    except Exception:
        return await cq.answer("❌ Bot not found.", show_alert=True)

    if not bot:
        return await cq.answer("❌ Bot not found.", show_alert=True)

    token      = bot["token"]
    new_status = "off" if bot.get("status") == "on" else "on"

    await engine_bots_db.update_one({"_id": bot["_id"]}, {"$set": {"status": new_status}})

    if new_status == "on":
        _start_slave_process(token)
        await cq.answer(f"✅ @{bot.get('username')} turned ON", show_alert=True)
    else:
        proc = slave_processes.get(token)
        if proc and proc.poll() is None:
            proc.terminate()
            await cq.answer(f"🔴 @{bot.get('username')} turned OFF", show_alert=True)
        else:
            await cq.answer("ℹ️ Bot was not running.", show_alert=True)

    await cb_engine_dashboard(client, cq)

# ==========================================
# 2️⃣ MESSAGE CATCHER
# ==========================================
@app.on_message(filters.channel)
async def enqueue_message(client, message):
    chat_id    = message.chat.id
    connection = await connections_db.find_one({"private_channel_id": chat_id})
    if not connection:
        return

    owner_id = connection.get("user_id")
    if not await is_sudo(owner_id):
        return

    topic_id     = connection.get("topic_id")
    channel_name = connection.get("channel_name", f"Channel {chat_id}")

    # FIX 1: Using built-in create_forum_topic instead of raw Invoke
    if not topic_id:
        if chat_id not in TOPIC_LOCKS:
            TOPIC_LOCKS[chat_id] = True
            try:
                topic = await client.create_forum_topic(
                    chat_id=SPECIAL_GROUP_ID,
                    title=channel_name[:128]
                )
                topic_id = topic.id
                if topic_id:
                    await connections_db.update_one(
                        {"private_channel_id": chat_id},
                        {"$set": {"topic_id": topic_id}}
                    )
            except Exception as e:
                print(f"Auto Topic Error: {e}")
            finally:
                TOPIC_LOCKS.pop(chat_id, None)
        else:
            await asyncio.sleep(3)
            recheck  = await connections_db.find_one({"private_channel_id": chat_id})
            topic_id = recheck.get("topic_id") if recheck else None

    # FIX 2: Passing specific client so slave bots don't crash the queue
    await message_queue.put({
        "client": client,
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
            client           = item["client"] # using the passed client
            message          = item["message"]
            public_channel_id = item["public_id"]
            topic_id         = item["topic_id"]

            try:
                if BOT_USERNAME is None:
                    bot_info     = await client.get_me()
                    BOT_USERNAME = bot_info.username

                msg_id  = message.id
                chat_id = message.chat.id

                # Backup to super group
                try:
                    if topic_id:
                        await client.copy_message(
                            chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id,
                            message_id=msg_id, reply_to_message_id=int(topic_id)
                        )
                    else:
                        await client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    await client.copy_message(chat_id=SPECIAL_GROUP_ID, from_chat_id=chat_id, message_id=msg_id)
                except Exception as e:
                    print(f"⚠️ Supergroup backup error: {e}")

                is_video = False
                if message.video:
                    is_video = True
                elif message.document and message.document.file_name:
                    if message.document.file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
                        is_video = True

                try:
                    if is_video:
                        caption    = message.caption if message.caption else "🎬 <b>New Video!</b>\n\n<i>Click below to watch.</i>"
                        short_code = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                        await links_db.insert_one({"short_code": short_code, "chat_id": chat_id, "msg_id": msg_id})
                        button = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Video", callback_data=f"vid_{short_code}")]])
                        await client.send_message(
                            chat_id=public_channel_id, text=caption,
                            reply_markup=button, parse_mode=ParseMode.HTML, protect_content=True
                        )
                    else:
                        await client.copy_message(chat_id=public_channel_id, from_chat_id=chat_id, message_id=msg_id)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                    if is_video:
                        await client.send_message(
                            chat_id=public_channel_id, text=caption,
                            reply_markup=button, parse_mode=ParseMode.HTML, protect_content=True
                        )
                    else:
                        await client.copy_message(chat_id=public_channel_id, from_chat_id=chat_id, message_id=msg_id)

                await stats_db.update_one({"type": "global"}, {"$inc": {"total_files_processed": 1}}, upsert=True)

            except Exception as e:
                print(f"❌ Queue processing error: {e}")
            finally:
                await asyncio.sleep(2)


# ==========================================
# 🔄 STARTUP CHANNEL CACHING
# ==========================================
async def cache_channels_on_startup():
    print("🔄 Caching channels from database...")
    connections = await connections_db.find({}).to_list(length=None)

    for conn in connections:
        priv_id  = conn.get("private_channel_id")
        pub_id   = conn.get("public_channel_id")
        sp_id    = SPECIAL_GROUP_ID

        for cid in [priv_id, pub_id, sp_id]:
            if cid:
                try:
                    await app.get_chat(cid)
                    await asyncio.sleep(0.3)
                except Exception as e:
                    pass

        if priv_id:
            try:
                await app.send_message(
                    priv_id,
                    "🔄 <b>Bot Restarted Successfully!</b>\nAll systems are online.",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                pass

    try:
        await app.get_chat(SPECIAL_GROUP_ID)
    except Exception:
        pass

    print(f"✅ Cached {len(connections)} channel connections.")


# ==========================================
# 🔔 EXPIRY REMINDER BACKGROUND TASK
# ==========================================
async def check_expirations():
    while True:
        try:
            now          = datetime.utcnow()
            warning_time = now + timedelta(days=3)

            expiring_users = await sudo_db.find({
                "expiry_date": {"$lte": warning_time, "$gt": now}
            }).to_list(length=None)

            for user in expiring_users:
                try:
                    user_id      = user["user_id"]
                    last_notified = user.get("last_notified")
                    if not last_notified or (now - last_notified).total_seconds() > 82800:
                        days_left   = (user["expiry_date"] - now).days
                        hours_left  = int((user["expiry_date"] - now).seconds / 3600)
                        time_txt    = f"<b>{days_left} days and {hours_left} hours</b>" if days_left > 0 else f"<b>{hours_left} hours</b>"
                        msg_text    = (
                            "⚠️ <b>SUBSCRIPTION ALERT!</b>\n\n"
                            f"Your Sudo access expires in {time_txt}.\n\n"
                            "⏳ <b>Renew now to avoid interruption.</b>\n"
                            "👉 Contact the Administrator."
                        )
                        sent_msg = await app.send_message(user_id, msg_text, parse_mode=ParseMode.HTML, protect_content=True)
                        try:
                            await sent_msg.pin(both_sides=True)
                        except Exception:
                            pass
                        await sudo_db.update_one({"user_id": user_id}, {"$set": {"last_notified": now}})
                except Exception as e:
                    pass
        except Exception as e:
            pass
        await asyncio.sleep(21600)


# ==========================================
# 🤖 SLAVE BOT RUNNER (subprocess mode)
# ==========================================
def run_as_slave(slave_token: str):
    slave = Client(
        f"slave_{slave_token[:8]}",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=slave_token
    )

    @slave.on_message(filters.channel)
    async def slave_enqueue(client, message):
        chat_id    = message.chat.id
        connection = await connections_db.find_one({"private_channel_id": chat_id})
        if not connection:
            return
        owner_id = connection.get("user_id")
        if not await is_sudo(owner_id):
            return
        
        # FIX: Pass client to queue here as well
        await message_queue.put({
            "client": client,
            "message": message,
            "public_id": connection["public_channel_id"],
            "topic_id": connection.get("topic_id")
        })

    async def start_slave():
        await slave.start()
        asyncio.create_task(process_queue())
        await idle()  # FIX 3: Keep-alive
        await slave.stop()

    asyncio.run(start_slave())


# ==========================================
# 🚀 MAIN ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    if "--slave-token" in sys.argv:
        idx   = sys.argv.index("--slave-token")
        token = sys.argv[idx + 1]
        print(f"🤖 Running as slave bot: {token[:8]}...")
        run_as_slave(token)
    else:
        print("🚀 Master Bot starting...")

        async def main():
            await app.start()
            await cache_channels_on_startup()

            active_slaves = await engine_bots_db.find({"status": "on"}).to_list(length=None)
            for bot_doc in active_slaves:
                _start_slave_process(bot_doc["token"])
                print(f"▶️ Re-launched slave: @{bot_doc.get('username')}")

            asyncio.create_task(process_queue())
            asyncio.create_task(check_expirations())

            print("✅ All systems online. Bot is running.")
            await idle()  # FIX 3: Replaced the freezing event wait with idle
            await app.stop()

        asyncio.run(main())
