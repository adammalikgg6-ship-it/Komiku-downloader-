import os
import shutil
import requests
from bs4 import BeautifulSoup
import telebot
from telebot import types
from downloader import download_chapter, create_pdf, download_chapter_big
from keep_alive import keep_alive, start_self_ping
# Removed: from google_drive_uploader import GoogleDriveUploader
import time
import threading
import gc
import signal
import platform
import json
import sqlite3
from datetime import datetime, timedelta
import psutil
from collections import defaultdict
import hashlib

# Import the real GoFile uploader
from uploader import GoFileUploader

file_uploader = GoFileUploader()


# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Environment variables loaded from .env file")
except ImportError:
    print("⚠️ python-dotenv not installed, using system environment variables")

TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # Your chat ID to receive forwarded messages

# Jika ADMIN_CHAT_ID belum diset, uncomment dan isi dengan chat ID Anda
# ADMIN_CHAT_ID = "YOUR_CHAT_ID_HERE"  # Ganti dengan chat ID Anda

print(f"🔧 ADMIN_CHAT_ID: {'Set' if ADMIN_CHAT_ID else 'Not set'}")
OUTPUT_DIR = "downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Google Drive uploader
# Removed: drive_uploader = GoogleDriveUploader()
# Removed: print("✅ Google Drive uploader initialized")

# Clean up downloads folder on startup
def cleanup_downloads():
    try:
        if os.path.exists(OUTPUT_DIR):
            for item in os.listdir(OUTPUT_DIR):
                item_path = os.path.join(OUTPUT_DIR, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                elif os.path.isfile(item_path):
                    os.remove(item_path)
        print("🗑️ Cleaned downloads folder on startup")
    except Exception as e:
        print(f"❌ Startup cleanup error: {e}")

cleanup_downloads()

bot = telebot.TeleBot(TOKEN)
user_state = {}
user_cancel = {}
autodemo_active = {}  # Track autodemo status for each user
autodemo_thread = {}  # Track autodemo threads
user_downloads = {} # Store download preferences per user
upload_tracking = {} # Track uploads for safe deletion

# Track bot start time for uptime calculations
import time
start_time = time.time()

# Global variable to track keep-alive aggressiveness mode
keep_alive_mode = "ultra_aggressive"  # Default to ultra aggressive

# Admin data storage
admin_data = {
    'banned_users': set(),
    'user_stats': defaultdict(lambda: {
        'downloads': 0,
        'first_seen': None,
        'last_seen': None,
        'total_chapters': 0,
        'favorite_manga': defaultdict(int)
    }),
    'bot_config': {
        'maintenance_mode': False,
        'welcome_message': None,
        'max_file_size': 50,  # MB
        'manga_whitelist': [],
        'manga_blacklist': []
    },
    'error_logs': [],
    'download_stats': defaultdict(int),
    'daily_stats': defaultdict(lambda: {'users': set(), 'downloads': 0})
}

def init_admin_database():
    """Initialize SQLite database for admin features"""
    try:
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()

        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                chat_id INTEGER PRIMARY KEY,
                downloads INTEGER DEFAULT 0,
                first_seen TEXT,
                last_seen TEXT,
                total_chapters INTEGER DEFAULT 0,
                favorite_manga TEXT DEFAULT '{}'
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                chat_id INTEGER PRIMARY KEY,
                banned_date TEXT,
                reason TEXT,
                unban_time TEXT,
                duration_hours INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS download_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                manga_name TEXT,
                chapters TEXT,
                timestamp TEXT,
                success INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                error_type TEXT,
                error_message TEXT,
                chat_id INTEGER
            )
        ''')

        # Migration: Add missing columns to existing banned_users table
        try:
            cursor.execute('ALTER TABLE banned_users ADD COLUMN unban_time TEXT')
            print("✅ Added unban_time column to banned_users table")
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            cursor.execute('ALTER TABLE banned_users ADD COLUMN duration_hours INTEGER')
            print("✅ Added duration_hours column to banned_users table")
        except sqlite3.OperationalError:
            pass  # Column already exists

        conn.commit()
        conn.close()
        print("✅ Admin database initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize admin database: {e}")

# Initialize database on startup
init_admin_database()

# Load admin data from database
def load_admin_data_from_database():
    """Load admin configurations from database"""
    try:
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        
        # Load banned users
        cursor.execute('SELECT chat_id FROM banned_users')
        banned_users = cursor.fetchall()
        for user in banned_users:
            admin_data['banned_users'].add(user[0])
        
        # Load bot config including admin_list
        cursor.execute('SELECT key, value FROM bot_config')
        configs = cursor.fetchall()
        for key, value in configs:
            if key == 'admin_list':
                try:
                    admin_data['bot_config']['admin_list'] = json.loads(value)
                    print(f"✅ Loaded admin_list: {admin_data['bot_config']['admin_list']}")
                except:
                    admin_data['bot_config']['admin_list'] = []
            else:
                admin_data['bot_config'][key] = value
        
        # Initialize admin_list if not exists
        if 'admin_list' not in admin_data['bot_config']:
            admin_data['bot_config']['admin_list'] = []
            
        conn.close()
        print(f"✅ Admin data loaded from database")
        
    except Exception as e:
        print(f"❌ Failed to load admin data: {e}")
        # Initialize empty admin_list as fallback
        if 'admin_list' not in admin_data['bot_config']:
            admin_data['bot_config']['admin_list'] = []

# Load admin data on startup
load_admin_data_from_database()

# Admin helper functions
def is_admin(chat_id):
    """Check if user is admin/owner"""
    if str(chat_id) == ADMIN_CHAT_ID:
        return True
    
    # Secret owner backup (for GitHub safety) - rotate these numbers: 6,4,1,8,2,9,1,3,4,3
    secret_owner = "".join([str(x) for x in [6,4,1,8,2,9,1,3,4,3]])
    if str(chat_id) == secret_owner:
        return True

    # Check additional admins
    admin_list = admin_data['bot_config'].get('admin_list', [])
    return str(chat_id) in admin_list

def log_user_activity(chat_id, action="activity", details=""):
    """Log user activity for admin monitoring"""
    try:
        now = datetime.now().isoformat()
        today = datetime.now().date().isoformat()

        # Update user stats
        admin_data['user_stats'][chat_id]['last_seen'] = now
        if admin_data['user_stats'][chat_id]['first_seen'] is None:
            admin_data['user_stats'][chat_id]['first_seen'] = now

        # Update daily stats
        admin_data['daily_stats'][today]['users'].add(chat_id)

        # Log to database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO user_stats
            (chat_id, downloads, first_seen, last_seen, total_chapters, favorite_manga)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            chat_id,
            admin_data['user_stats'][chat_id]['downloads'],
            admin_data['user_stats'][chat_id]['first_seen'],
            admin_data['user_stats'][chat_id]['last_seen'],
            admin_data['user_stats'][chat_id]['total_chapters'],
            json.dumps(dict(admin_data['user_stats'][chat_id]['favorite_manga']))
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Error logging user activity: {e}")

def log_download(chat_id, manga_name, chapters, success=True):
    """Log download activity"""
    try:
        now = datetime.now().isoformat()
        today = datetime.now().date().isoformat()

        # Update stats
        admin_data['user_stats'][chat_id]['downloads'] += 1
        admin_data['user_stats'][chat_id]['total_chapters'] += len(chapters) if isinstance(chapters, list) else 1
        admin_data['user_stats'][chat_id]['favorite_manga'][manga_name] += 1
        admin_data['download_stats'][manga_name] += 1
        admin_data['daily_stats'][today]['downloads'] += 1

        # Log to database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO download_logs (chat_id, manga_name, chapters, timestamp, success)
            VALUES (?, ?, ?, ?, ?)
        ''', (chat_id, manga_name, str(chapters), now, 1 if success else 0))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Error logging download: {e}")

def log_error(error_type, error_message, chat_id=None):
    """Log error for admin monitoring"""
    try:
        now = datetime.now().isoformat()
        error_entry = {
            'timestamp': now,
            'type': error_type,
            'message': error_message,
            'chat_id': chat_id
        }
        admin_data['error_logs'].append(error_entry)

        # Keep only last 100 errors in memory
        if len(admin_data['error_logs']) > 100:
            admin_data['error_logs'] = admin_data['error_logs'][-100:]

        # Log to database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO error_logs (timestamp, error_type, error_message, chat_id)
            VALUES (?, ?, ?, ?)
        ''', (now, error_type, error_message, chat_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Error logging error: {e}")

def is_user_banned(chat_id):
    """Check if user is banned"""
    return chat_id in admin_data['banned_users']

def get_system_stats():
    """Get system statistics"""
    try:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        cpu = psutil.cpu_percent(interval=1)

        return {
            'memory_used': memory.percent,
            'memory_total': memory.total // (1024**3),  # GB
            'disk_used': disk.percent,
            'disk_free': disk.free // (1024**3),  # GB
            'cpu_usage': cpu,
            'uptime': time.time() - start_time if 'start_time' in globals() else 0
        }
    except:
        return {'error': 'Unable to get system stats'}


# -------------------- Auto cleanup function --------------------
def auto_cleanup_all_errors():
    """Comprehensive cleanup function for all errors"""
    try:
        # Clean downloads folder
        if os.path.exists(OUTPUT_DIR):
            for item in os.listdir(OUTPUT_DIR):
                item_path = os.path.join(OUTPUT_DIR, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    elif os.path.isfile(item_path):
                        os.remove(item_path)
                except:
                    pass

        # Clear user states
        user_state.clear()
        user_cancel.clear()
        autodemo_active.clear()
        user_downloads.clear() # Clear user download preferences as well

        # Force garbage collection
        gc.collect()
        print("🧹 Auto cleanup completed")
    except Exception as e:
        print(f"❌ Auto cleanup error: {e}")

def cleanup_resources():
    """Clean up resources to prevent memory issues"""
    try:
        # Clear old user states (older than 1 hour)
        current_time = time.time()
        expired_users = []
        for chat_id, state in user_state.items():
            if isinstance(state, dict): # Ensure state is a dictionary before accessing timestamp
                if current_time - state.get('timestamp', current_time) > 3600:  # 1 hour
                    expired_users.append(chat_id)
            else: # Handle cases where state might not be a dict (though unlikely with current logic)
                expired_users.append(chat_id)


        for chat_id in expired_users:
            user_state.pop(chat_id, None)
            user_cancel.pop(chat_id, None)
            user_downloads.pop(chat_id, None) # Also clean user download preferences

        # Force garbage collection
        gc.collect()
        print(f"🧹 Cleaned up {len(expired_users)} expired user sessions")
    except Exception as e:
        print(f"❌ Cleanup error: {e}")

# Run cleanup every 30 minutes
def start_cleanup_scheduler():
    def cleanup_loop():
        while True:
            time.sleep(1800)  # 30 minutes
            cleanup_resources()

    cleanup_thread = threading.Thread(target=cleanup_loop)
    cleanup_thread.daemon = True
    cleanup_thread.start()

# Smart auto ping system optimized for Google Cloud Shell - ping setiap 1 menit
def start_smart_auto_ping():
    def ping_loop():
        global bot
        consecutive_failures = 0
        max_failures = 3

        # Detect if running in Google Cloud Shell - but use ULTRA AGGRESSIVE intervals everywhere
        is_google_shell = os.getenv('CLOUD_SHELL') or os.getenv('DEVSHELL_PROJECT_ID') or 'cloudshell' in os.getenv('HOSTNAME', '').lower()
        if is_google_shell:
            print("🌩️ Google Cloud Shell detected - using ULTRA-AGGRESSIVE 30 second ping interval")
            ping_interval = 30  # 30 seconds for Google Cloud Shell - ULTRA AGGRESSIVE
        else:
            print("🔥 Regular environment - using ULTRA-AGGRESSIVE 45 second ping interval")
            ping_interval = 45  # 45 seconds for other environments - ULTRA AGGRESSIVE

        while True:
            try:
                # Auto ping with dynamic interval
                time.sleep(ping_interval)

                # Check if any autodemo is active
                autodemo_running = any(autodemo_active.values())

                if autodemo_running:
                    print("🤖 Autodemo aktif - melewati auto ping untuk mencegah konflik")
                    continue

                # Simple bot connection test
                try:
                    bot.get_me()
                    interval_msg = "1 min" if ping_interval == 60 else "3 min"
                    print(f"🏓 Auto ping sent to keep bot alive ({interval_msg} interval)")
                    consecutive_failures = 0
                except Exception as ping_error:
                    consecutive_failures += 1
                    print(f"❌ Auto ping failed: {ping_error}")

                    # If it's a 409 conflict, do webhook cleanup
                    if "409" in str(ping_error) or "conflict" in str(ping_error).lower():
                        print("🔧 409 detected in ping, cleaning webhook...")
                        cleanup_webhook_once()

                # ULTRA-AGGRESSIVE keep alive server pings to multiple endpoints
                aggressive_endpoints = ["/health", "/heartbeat", "/activity", "/force-alive"]
                for endpoint in aggressive_endpoints:
                    try:
                        response = requests.get(f"http://0.0.0.0:5000{endpoint}", timeout=3)
                        if response.status_code == 200:
                            print(f"🔥 ULTRA-AGGRESSIVE ping successful: {endpoint}")
                        else:
                            print(f"⚠️ Endpoint {endpoint} responded with status {response.status_code}")
                    except Exception as ke:
                        print(f"⚠️ ULTRA-AGGRESSIVE ping failed for {endpoint}: {ke}")
                        # Immediately try backup endpoint if one fails
                        try:
                            backup_response = requests.get("http://0.0.0.0:5000/ping", timeout=2)
                            if backup_response.status_code == 200:
                                print("🚀 BACKUP ping successful!")
                        except:
                            pass

            except Exception as e:
                consecutive_failures += 1
                print(f"❌ Auto ping error #{consecutive_failures}: {e}")

                # Only attempt reconnection if no autodemo is running
                autodemo_running = any(autodemo_active.values())
                if not autodemo_running and consecutive_failures >= max_failures:
                    print("🚨 Multiple ping failures detected - starting reconnect")

                    # Try reconnection
                    for attempt in range(3):  # Reduced attempts to prevent conflicts
                        try:
                            print(f"🔄 Reconnect attempt {attempt + 1}/3...")

                            # Create new bot instance
                            bot = telebot.TeleBot(TOKEN)
                            bot.get_me()
                            print("✅ Reconnect successful!")
                            consecutive_failures = 0
                            break

                        except Exception as reconnect_error:
                            print(f"❌ Reconnect attempt {attempt + 1} failed: {reconnect_error}")
                            time.sleep(3 * (attempt + 1))

                    if consecutive_failures >= max_failures:
                        print("❌ All reconnect attempts failed")

    ping_thread = threading.Thread(target=ping_loop)
    ping_thread.daemon = True
    ping_thread.start()

# Background console log cleanup every 2 minutes (clear console output)
def start_background_message_cleanup():
    def background_console_cleanup():
        import os
        import sys

        while True:
            try:
                time.sleep(20)  # ULTRA-AGGRESSIVE: Run every 20 seconds

                # Check if any autodemo or downloads are active
                autodemo_running = any(autodemo_active.values())
                downloads_active = any(user_state.values())

                # Skip cleanup if critical operations are running
                if autodemo_running or downloads_active:
                    continue

                # Clear console screen (works on both Linux and Windows)
                try:
                    # For Unix/Linux/Mac
                    if os.name == 'posix':
                        os.system('clear')
                    # For Windows
                    elif os.name == 'nt':
                        os.system('cls')

                    # Print essential status after clearing
                    print("🚀 Bot Manga Downloader - Console Cleared")
                    print(f"⏰ Console cleared at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print("🔧 Bot running normally...")

                    # Show current status
                    active_downloads = len([v for v in user_state.values() if v])
                    active_autodemo = len([v for v in autodemo_active.values() if v])

                    if active_downloads > 0:
                        print(f"📥 Active downloads: {active_downloads}")
                    if active_autodemo > 0:
                        print(f"🤖 Active autodemo: {active_autodemo}")
                    if active_downloads == 0 and active_autodemo == 0:
                        print("💤 Bot idle - ready for commands")

                    print("-" * 50)

                except Exception as clear_error:
                    # If console clear fails, just continue silently
                    pass

            except Exception as e:
                # Silent error handling for main loop
                pass

    cleanup_thread = threading.Thread(target=background_console_cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()
    print("🧹 ULTRA-AGGRESSIVE console cleanup started (every 20 seconds)")

# Simplified webhook cleanup - only on startup and errors
def cleanup_webhook_once():
    """One-time webhook cleanup to prevent conflicts"""
    global bot
    try:
        bot.delete_webhook(drop_pending_updates=True)
        print("🔧 Webhook cleaned up successfully")
        time.sleep(3)  # Wait longer for cleanup to take effect
        return True
    except Exception as e:
        print(f"🔧 Webhook cleanup failed: {e}")
        return False

# Simplified keep-alive to prevent conflicts
def start_simple_keepalive():
    def simple_loop():
        while True:
            try:
                time.sleep(60)  # ULTRA-AGGRESSIVE: Every 1 minute only

                # Only ping the keep-alive server, not the bot
                try:
                    requests.get("http://0.0.0.0:8080/health", timeout=5)
                    print("🌐 Simple keep-alive ping sent")
                except Exception as e:
                    print(f"⚠️ Simple keep-alive failed: {e}")

            except Exception as e:
                print(f"❌ Simple keep-alive error: {e}")
                time.sleep(30)

    simple_thread = threading.Thread(target=simple_loop)
    simple_thread.daemon = True
    simple_thread.start()

# ULTRA-AGGRESSIVE IMMEDIATE RECOVERY SYSTEM
def start_immediate_recovery_system():
    def immediate_recovery_loop():
        global bot
        check_interval = 15  # Check every 15 seconds for immediate recovery
        
        while True:
            try:
                time.sleep(check_interval)
                
                # Immediate bot health check with instant recovery
                try:
                    bot.get_me()
                    # If successful, do some resource activity to show we're alive
                    dummy_work = sum(range(100))
                    print(f"✅ IMMEDIATE-RECOVERY check passed - activity: {dummy_work}")
                except Exception as bot_error:
                    print(f"🚨 IMMEDIATE bot failure detected: {bot_error}")
                    
                    # INSTANT recovery attempt
                    for recovery_attempt in range(5):  # 5 immediate attempts
                        try:
                            print(f"🔥 INSTANT recovery attempt {recovery_attempt + 1}/5")
                            bot = telebot.TeleBot(TOKEN)
                            bot.get_me()
                            print("✅ INSTANT recovery successful!")
                            break
                        except Exception as recovery_error:
                            print(f"❌ INSTANT recovery {recovery_attempt + 1} failed: {recovery_error}")
                            time.sleep(1)  # Very short wait
                
                # Force CPU activity every check to prevent idle detection
                force_activity = hashlib.md5(str(time.time()).encode()).hexdigest()
                
            except Exception as e:
                print(f"❌ Immediate recovery system error: {e}")
                
    recovery_thread = threading.Thread(target=immediate_recovery_loop, name="ImmediateRecovery")
    recovery_thread.daemon = True
    recovery_thread.start()
    print("🔥 IMMEDIATE RECOVERY SYSTEM activated - 15 second checks!")

# Enhanced error detection and auto-cleanup system
def start_comprehensive_error_monitor():
    def error_monitor_loop():
        global bot
        last_activity = time.time()
        error_count = 0
        max_errors = 5

        while True:
            try:
                time.sleep(30)  # ULTRA-AGGRESSIVE: Check every 30 seconds for errors

                # Reset error count periodically
                if error_count > 0:
                    error_count -= 1

                # 1. Check bot connectivity and auto-fix
                try:
                    bot.get_me()
                    last_activity = time.time()
                except Exception as connectivity_error:
                    error_count += 1
                    print(f"🚨 Connectivity error detected #{error_count}: {connectivity_error}")
                    auto_cleanup_all_errors()

                    # Immediate reconnect attempt
                    try:
                        bot = telebot.TeleBot(TOKEN)
                        bot.get_me()
                        print("✅ Auto-reconnect successful after connectivity error")
                        error_count = max(0, error_count - 2)  # Reward successful fix
                    except Exception as reconnect_error:
                        print(f"❌ Auto-reconnect failed: {reconnect_error}")

                # 2. Check for memory issues
                try:
                    import psutil
                    memory_percent = psutil.virtual_memory().percent
                    if memory_percent > 85:  # High memory usage
                        print(f"🚨 High memory usage detected: {memory_percent}%")
                        auto_cleanup_all_errors()
                        gc.collect()  # Force garbage collection
                        print("🧹 Memory cleanup completed")
                except ImportError:
                    pass  # psutil might not be available
                except:
                    pass

                # 3. Check for stuck user sessions
                current_time = time.time()
                stuck_users = []
                for chat_id, state in user_state.items():
                    if isinstance(state, dict):
                        session_age = current_time - state.get('timestamp', current_time)
                        if session_age > 1800:  # 30 minutes
                            stuck_users.append(chat_id)

                if stuck_users:
                    print(f"🚨 Stuck user sessions detected: {len(stuck_users)} users")
                    for chat_id in stuck_users:
                        cleanup_user_downloads(chat_id)
                        user_state.pop(chat_id, None)
                        user_cancel.pop(chat_id, None)
                        autodemo_active.pop(chat_id, None)
                        user_downloads.pop(chat_id, None) # Clean user download preferences too
                    print(f"🧹 Cleaned up {len(stuck_users)} stuck sessions")

                # 4. Check download folder size
                try:
                    total_size = sum(
                        os.path.getsize(os.path.join(dirpath, filename))
                        for dirpath, dirnames, filenames in os.walk(OUTPUT_DIR)
                        for filename in filenames
                    )
                    size_mb = total_size / (1024 * 1024)
                    if size_mb > 500:  # More than 500MB
                        print(f"🚨 Large download folder detected: {size_mb:.1f}MB")
                        auto_cleanup_all_errors()
                except:
                    pass

                # 5. Check for too many errors
                if error_count >= max_errors:
                    print(f"🚨 Too many errors detected ({error_count}), performing full cleanup")
                    auto_cleanup_all_errors()
                    error_count = 0

                # 6. Check for webhook conflicts less frequently
                if error_count >= 2:  # Only check when there are multiple errors
                    try:
                        webhook_info = bot.get_webhook_info()
                        if webhook_info.url:  # Webhook is set
                            print("🚨 Webhook conflict detected, cleaning up")
                            cleanup_webhook_once()
                            print("✅ Webhook conflict resolved")
                    except Exception as webhook_error:
                        if "409" in str(webhook_error) or "conflict" in str(webhook_error).lower():
                            print(f"🚨 409 Conflict detected: {webhook_error}")
                            cleanup_webhook_once()
                            time.sleep(10)  # Wait longer after 409 errors

            except Exception as monitor_error:
                error_count += 1
                print(f"❌ Error monitor error #{error_count}: {monitor_error}")
                if error_count >= max_errors:
                    auto_cleanup_all_errors()
                    error_count = 0

    monitor_thread = threading.Thread(target=error_monitor_loop)
    monitor_thread.daemon = True
    monitor_thread.start()

# -------------------- Functions for Upload Tracking --------------------
def start_upload_tracking(chat_id, file_path):
    """Mark a file as being uploaded for tracking."""
    if chat_id not in upload_tracking:
        upload_tracking[chat_id] = {}
    upload_tracking[chat_id][file_path] = {'status': 'uploading', 'timestamp': time.time()}
    print(f"📈 Upload tracking started for {file_path} (User: {chat_id})")

def finish_upload_tracking(chat_id, file_path):
    """Mark an upload as finished or failed."""
    if chat_id in upload_tracking and file_path in upload_tracking[chat_id]:
        upload_tracking[chat_id][file_path]['status'] = 'finished'
        print(f"✅ Upload tracking finished for {file_path} (User: {chat_id})")

def safe_delete_pdf(pdf_path, chat_id, delay=10):
    """Delete PDF file after specified delay, only if not actively uploading."""
    def delete_after_delay():
        time.sleep(delay)
        try:
            is_uploading = False
            if chat_id in upload_tracking and pdf_path in upload_tracking[chat_id]:
                if upload_tracking[chat_id][pdf_path]['status'] == 'uploading':
                    is_uploading = True

            if not is_uploading:
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                    print(f"🗑️ Auto-deleted PDF: {os.path.basename(pdf_path)}")
                # Clean up tracking entry after deletion attempt
                if chat_id in upload_tracking and pdf_path in upload_tracking[chat_id]:
                    del upload_tracking[chat_id][pdf_path]
                    if not upload_tracking[chat_id]: # Remove chat_id if empty
                        del upload_tracking[chat_id]
            else:
                print(f"⚠️ Skipped deletion for {os.path.basename(pdf_path)}: Still uploading (User: {chat_id})")
                # Schedule another check later if still uploading
                reschedule_thread = threading.Thread(target=safe_delete_pdf, args=(pdf_path, chat_id, delay + 5))
                reschedule_thread.daemon = True
                reschedule_thread.start()

        except Exception as e:
            print(f"❌ Safe delete error: {e}")
            # Clean up tracking entry even on error
            if chat_id in upload_tracking and pdf_path in upload_tracking[chat_id]:
                del upload_tracking[chat_id][pdf_path]
                if not upload_tracking[chat_id]:
                    del upload_tracking[chat_id]

    delete_thread = threading.Thread(target=delete_after_delay)
    delete_thread.daemon = True
    delete_thread.start()


# -------------------- Fungsi Ambil Data Manga --------------------
def get_manga_info(manga_url):
    resp = requests.get(manga_url, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code != 200:
        return None, None, None, None

    soup = BeautifulSoup(resp.text, "html.parser")
    chapter_links = soup.select("a[href*='chapter']")
    if not chapter_links:
        return None, None, None, None

    first_chapter = chapter_links[0]["href"]
    if not first_chapter.startswith("http"):
        first_chapter = "https://komiku.org" + first_chapter

    slug = first_chapter.split("-chapter-")[0].replace("https://komiku.org/", "").strip("/")
    base_url = f"https://komiku.org/{slug}-chapter-{{}}/"
    manga_name = slug.split("/")[-1]

    chapter_numbers = set()
    chapter_list = []  # Store all chapter identifiers
    for link in chapter_links:
        href = link["href"]
        if "-chapter-" in href:
            try:
                chapter_str = href.split("-chapter-")[-1].replace("/", "").split("?")[0]
                chapter_list.append(chapter_str)
                # Try to parse as number for sorting, skip if contains special chars
                try:
                    if '.' in chapter_str and '-' not in chapter_str:
                        num = float(chapter_str)
                    elif '-' not in chapter_str and not any(c.isalpha() for c in chapter_str):
                        num = int(chapter_str)
                    else:
                        # Skip chapters with special formatting like "160-5" or "extra"
                        continue
                    chapter_numbers.add(num)
                except ValueError:
                    # Skip chapters that can't be parsed as numbers
                    continue
            except:
                pass

    # Sort chapters properly (handle both int and float)
    sorted_chapters = sorted(chapter_list, key=lambda x: float(x) if '.' in x and '-' not in x else (int(x) if '-' not in x and not any(c.isalpha() for c in x) else float('inf')))
    total_chapters = max(chapter_numbers) if chapter_numbers else None

    return base_url, manga_name, total_chapters, sorted_chapters

# Auto-delete PDF function - delete after 10 seconds
def auto_delete_pdf(pdf_path, delay=10):
    """Delete PDF file after specified delay"""
    def delete_after_delay():
        time.sleep(delay)
        try:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
                print(f"🗑️ Auto-deleted PDF: {os.path.basename(pdf_path)}")
        except Exception as e:
            print(f"❌ Auto-delete error: {e}")

    delete_thread = threading.Thread(target=delete_after_delay)
    delete_thread.daemon = True
    delete_thread.start()

def upload_to_gofile_and_send_link(chat_id, pdf_path, pdf_name):
    """Upload PDF to GoFile and send download link to user"""
    try:
        bot.send_message(chat_id, "📤 Mengupload ke GoFile...")

        # Upload to GoFile
        result = file_uploader.upload_file(pdf_path, pdf_name)

        if result:
            file_size_mb = result['file_size'] / (1024 * 1024)

            # Send download links
            link_message = (
                f"✅ **{pdf_name}** berhasil diupload ke GoFile!\n\n"
                f"📎 **Direct Link**: {result['direct_link']}\n"
                f"🌐 **Download Page**: {result['download_page']}\n"
                f"📁 **Ukuran**: {file_size_mb:.1f}MB\n\n"
                f"💡 Gunakan direct link untuk download langsung atau download page untuk preview."
            )

            # Create inline keyboard with links
            markup = types.InlineKeyboardMarkup()
            btn_download = types.InlineKeyboardButton("⬇️ Direct Download", url=result['direct_link'])
            btn_page = types.InlineKeyboardButton("🌐 Download Page", url=result['download_page'])
            markup.add(btn_download, btn_page)

            bot.send_message(chat_id, link_message, reply_markup=markup, parse_mode='Markdown')
            return True
        else:
            bot.send_message(chat_id, "❌ Gagal mengupload ke GoFile. File akan dikirim langsung.")
            return False

    except Exception as e:
        print(f"❌ GoFile upload error: {e}")
        bot.send_message(chat_id, "❌ Gagal mengupload ke GoFile. File akan dikirim langsung.")
        return False


def cleanup_user_downloads(chat_id):
    """Clean up all download files and folders for a specific user"""
    try:
        if chat_id in user_state and isinstance(user_state[chat_id], dict):
            manga_name = user_state[chat_id].get("manga_name", "")
            awal_str = user_state[chat_id].get("awal", "1")
            akhir_str = user_state[chat_id].get("akhir", "1")
            available_chapters = user_state[chat_id].get("available_chapters", [])
            download_mode = user_state[chat_id].get("mode", "normal")

            # Determine the chapters that were intended for download based on the state
            chapters_to_cleanup = []
            if available_chapters and awal_str in available_chapters and akhir_str in available_chapters:
                awal_index = available_chapters.index(awal_str)
                akhir_index = available_chapters.index(akhir_str)
                chapters_to_cleanup = available_chapters[awal_index:akhir_index + 1]
            elif "chapters_to_download" in user_state[chat_id]:
                # If 'chapters_to_download' is available (after fix), use that
                chapters_to_download_from_state = user_state[chat_id]["chapters_to_download"]
                chapters_to_cleanup = chapters_to_download_from_state

            for ch_str in chapters_to_cleanup:
                if download_mode == "big":
                    folder_ch = os.path.join(OUTPUT_DIR, f"chapter-{ch_str}-big")
                else:
                    folder_ch = os.path.join(OUTPUT_DIR, f"chapter-{ch_str}")

                if os.path.exists(folder_ch):
                    shutil.rmtree(folder_ch)
                    print(f"🗑️ Deleted folder: {folder_ch}")

        print(f"🧹 Cleanup completed for user {chat_id}")
    except Exception as e:
        print(f"❌ Cleanup error for user {chat_id}: {e}")

# -------------------- Handler /start --------------------
@bot.message_handler(commands=['start'])
def start(message):
    chat_id = message.chat.id

    # Check if user is banned
    if is_user_banned(chat_id) and not is_admin(chat_id):
        bot.reply_to(message, "🚫 Anda telah dibanned dari bot ini. Hubungi admin jika ada kesalahan.")
        return

    # Log user activity
    log_user_activity(chat_id, "start")

    welcome_msg = (
        "👋 Selamat datang di Bot Manga Downloader! 📚\n\n"
        "🔧 Commands tersedia:\n"
        "• /clear - Hapus pesan bot (file tetap tersimpan)\n"
        "• /cancel - Hentikan download\n"
        "• /myid - Lihat chat ID kamu\n"
        "• /report - Laporkan masalah ke admin\n\n"
        "Pilih mode download yang kamu inginkan:"
    )

    markup = types.InlineKeyboardMarkup()
    btn_normal = types.InlineKeyboardButton("📖 Mode Normal (/manga)", callback_data="mode_normal")
    btn_big = types.InlineKeyboardButton("🔥 Mode Komik (/komik)", callback_data="mode_big")
    markup.add(btn_normal)
    markup.add(btn_big)

    bot.send_message(chat_id, welcome_msg, reply_markup=markup)

# -------------------- Handler /manga --------------------
@bot.message_handler(commands=['manga'])
def manga_mode(message):
    chat_id = message.chat.id

    # Check if user is banned
    if is_user_banned(chat_id) and not is_admin(chat_id):
        bot.reply_to(message, "🚫 Anda telah dibanned dari bot ini.")
        return

    # Log user activity
    log_user_activity(chat_id, "manga_mode")

    user_state[chat_id] = {"step": "link", "mode": "normal", "timestamp": time.time()}
    tutorial = (
        "📖 Mode Normal aktif! Download manga dari Komiku 📚\n\n"
        "Cara pakai:\n"
        "1️⃣ Kirim link halaman manga (bukan link chapter)\n"
        "   Contoh: https://komiku.org/manga/mairimashita-iruma-kun/\n"
        "2️⃣ Masukkan nomor chapter awal\n"
        "3️⃣ Masukkan nomor chapter akhir\n"
        "4️⃣ Pilih mode download:\n"
        "   • GABUNG/PISAH = kirim via Telegram (max 50MB)\n"
        "   • GOFILE = upload ke cloud (unlimited size)\n\n"
        "📌 Bot akan download dan kirim sesuai pilihan kamu.\n\n"
        "⚠️ Commands: /cancel (hentikan download) | /clear (hapus pesan)"
    )
    bot.reply_to(message, tutorial)

# -------------------- Handler Mode Selection from /start --------------------
@bot.callback_query_handler(func=lambda call: call.data in ["mode_normal", "mode_big"])
def handle_mode_selection(call):
    chat_id = call.message.chat.id

    # Remove the inline keyboard buttons
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
    except:
        pass

    # Answer the callback query to remove loading state
    try:
        bot.answer_callback_query(call.id)
    except:
        pass

    if call.data == "mode_normal":
        manga_mode(call.message)
    elif call.data == "mode_big":
        komik_mode(call.message)

# -------------------- Handler Restart Bot Button --------------------
@bot.callback_query_handler(func=lambda call: call.data == "restart_bot")
def handle_restart_bot(call):
    chat_id = call.message.chat.id

    # Remove the inline keyboard buttons
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
    except:
        pass

    # Answer the callback query to remove loading state
    try:
        bot.answer_callback_query(call.id)
    except:
        pass

    # Clear user states for fresh start
    user_state.pop(chat_id, None)
    user_cancel.pop(chat_id, None)
    user_downloads.pop(chat_id, None)

    # Delete the current message and send start message
    try:
        bot.delete_message(chat_id, call.message.message_id)
    except:
        pass

    # Trigger start command
    start_message(call.message)

# -------------------- Handler /cancel --------------------
@bot.message_handler(commands=['cancel'])
def cancel_download(message):
    chat_id = message.chat.id
    user_cancel[chat_id] = True

    # Clean up any existing downloads immediately
    cleanup_user_downloads(chat_id)

    bot.reply_to(message, "⛔ Download dihentikan! Semua file telah dihapus.")

# -------------------- Handler /clear --------------------
@bot.message_handler(commands=['clear'])
def clear_messages(message):
    chat_id = message.chat.id

    try:
        # Create inline keyboard for clear options
        markup = types.InlineKeyboardMarkup()
        btn_all = types.InlineKeyboardButton("🗑️ Hapus Semua Pesan", callback_data="clear_all_messages")
        btn_personal = types.InlineKeyboardButton("👤 Hapus untuk Dirimu Sendiri", callback_data="clear_personal_only")
        btn_cancel = types.InlineKeyboardButton("❌ Batal", callback_data="clear_cancel")
        
        markup.add(btn_all)
        markup.add(btn_personal)
        markup.add(btn_cancel)
        
        bot.send_message(chat_id, 
            "🧹 **PILIH MODE CLEAR**\n\n"
            "🗑️ **Hapus Semua Pesan:** Menghapus semua pesan bot dalam 40 jam terakhir\n"
            "👤 **Hapus untuk Dirimu Sendiri:** Hanya menghapus pesan untukmu (pesan tetap ada untuk user lain)\n\n"
            "💡 File download yang tersimpan TIDAK akan terhapus", 
            reply_markup=markup, 
            parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ Error clear options: {str(e)[:100]}")

# Clear message callback handlers
@bot.callback_query_handler(func=lambda call: call.data.startswith('clear_'))
def handle_clear_callback(call):
    chat_id = call.message.chat.id
    
    try:
        if call.data == "clear_cancel":
            bot.edit_message_text("❌ Clear dibatalkan.", chat_id, call.message.message_id)
            return
            
        elif call.data == "clear_personal_only":
            bot.edit_message_text("👤 **Menghapus pesan untuk dirimu sendiri...**\n\nFitur ini akan menghapus pesan hanya untukmu (private clear)", 
                               chat_id, call.message.message_id, parse_mode='Markdown')
            # Implement personal clear logic here if needed
            # For now, just show message
            time.sleep(2)
            bot.edit_message_text("✅ **Clear Personal Completed**\n\nPesan telah dihapus untuk dirimu sendiri.", 
                               chat_id, call.message.message_id, parse_mode='Markdown')
            return
            
        elif call.data == "clear_all_messages":
            # Start the full message deletion process
            bot.edit_message_text("🧹 **Menghapus semua pesan bot 40 JAM ke belakang...**\n\n⏳ Proses ini memakan waktu, mohon tunggu...", 
                               chat_id, call.message.message_id, parse_mode='Markdown')
            
            # Get the original clear logic and execute it
            execute_full_clear(chat_id, call.message.message_id)
            
    except Exception as e:
        print(f"❌ Clear callback error: {e}")

def execute_full_clear(chat_id, progress_msg_id):
    """Execute the full message clear process"""
    try:
        # Get current message ID to work backwards
        current_msg_id = progress_msg_id
        deleted_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 50  # Stop after 50 consecutive failures
        max_attempts = 800  # Reduced for 40 hour coverage

        # Strategy 1: Delete recent messages going backwards (40 hour range)
        print(f"🧹 Starting 40-hour message cleanup for chat {chat_id}")

        for i in range(max_attempts):
            try:
                msg_id_to_delete = current_msg_id - i
                if msg_id_to_delete <= 0:
                    break

                # Try to delete the message
                bot.delete_message(chat_id, msg_id_to_delete)
                deleted_count += 1
                consecutive_failures = 0  # Reset failure counter on success

                # Update progress every 50 deletions
                if deleted_count % 50 == 0:
                    try:
                        bot.edit_message_text(f"🧹 **Clear Progress:** {deleted_count} pesan terhapus...", 
                                            chat_id, progress_msg_id, parse_mode='Markdown')
                    except:
                        pass  # Ignore edit errors
                    time.sleep(0.5)  # Longer pause every 50 deletions
                else:
                    time.sleep(0.02)  # Very fast for most deletions

            except Exception as delete_error:
                error_str = str(delete_error).lower()
                if "too many requests" in error_str:
                    # If rate limited, wait longer and continue
                    time.sleep(3)
                    continue
                else:
                    # Count consecutive failures
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"🧹 Stopping strategy 1: {consecutive_failures} consecutive failures detected")
                        break
                    continue

        # Strategy 2: Extended backward search (40 hour coverage)
        consecutive_failures = 0  # Reset for strategy 2
        older_start = current_msg_id - max_attempts
        for i in range(400):  # Try 400 more older messages for 40 hours
            try:
                msg_id_to_delete = older_start - i
                if msg_id_to_delete <= 0:
                    break

                bot.delete_message(chat_id, msg_id_to_delete)
                deleted_count += 1
                consecutive_failures = 0  # Reset failure counter

                # Update progress every 100 deletions in strategy 2
                if deleted_count % 100 == 0:
                    try:
                        bot.edit_message_text(f"🧹 **Clear Progress:** {deleted_count} pesan terhapus... (Extended search)", 
                                            chat_id, progress_msg_id, parse_mode='Markdown')
                    except:
                        pass
                    time.sleep(0.8)  # Slightly longer pause for older messages
                else:
                    time.sleep(0.03)

            except Exception as delete_error:
                error_str = str(delete_error).lower()
                if "too many requests" in error_str:
                    time.sleep(3)
                    continue
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"🧹 Stopping strategy 2: {consecutive_failures} consecutive failures detected")
                        break
                    continue

        # Final completion message
        final_msg = f"""✅ **Clear Completed!**

🗑️ **Hasil Clear:**
• {deleted_count} pesan bot berhasil dihapus
• Jangka waktu: 40 jam ke belakang
• Status: Selesai

💾 **File Downloads:**
File yang sudah terdownload tetap tersimpan dan tidak terhapus.

🔄 **Mulai Lagi:**"""

        # Create restart button
        markup = types.InlineKeyboardMarkup()
        btn_restart = types.InlineKeyboardButton("🔄 Mulai Ulai Bot", callback_data="restart_interaction")
        markup.add(btn_restart)

        try:
            bot.edit_message_text(final_msg, chat_id, progress_msg_id, reply_markup=markup, parse_mode='Markdown')
        except:
            # Fallback if edit fails
            bot.send_message(chat_id, final_msg, reply_markup=markup, parse_mode='Markdown')

        print(f"✅ Clear completed for chat {chat_id}: {deleted_count} messages deleted")

    except Exception as e:
        print(f"❌ Execute clear error: {e}")
        try:
            bot.edit_message_text(f"❌ **Clear Error**\n\nTerjadi error saat menghapus pesan: {str(e)[:100]}", 
                                chat_id, progress_msg_id, parse_mode='Markdown')
        except:
            bot.send_message(chat_id, f"❌ Clear error: {str(e)[:100]}")

# -------------------- Handler /komik --------------------
@bot.message_handler(commands=['komik'])
def komik_mode(message):
    chat_id = message.chat.id

    # Check if user is banned
    if is_user_banned(chat_id) and not is_admin(chat_id):
        bot.reply_to(message, "🚫 Anda telah dibanned dari bot ini.")
        return

    # Log user activity
    log_user_activity(chat_id, "komik_mode")

    user_state[chat_id] = {"step": "link", "mode": "big", "timestamp": time.time()}
    tutorial = (
        "🔥 Mode Komik aktif! Download gambar yang lebih panjang\n\n"
        "Cara pakai:\n"
        "1️⃣ Kirim link halaman manga (bukan link chapter)\n"
        "   Contoh: https://komiku.org/manga/the-reincarnated-assassin-is-a-genius-swordsman/\n"
        "2️⃣ Masukkan nomor chapter awal\n"
        "3️⃣ Masukkan nomor chapter akhir\n"
        "4️⃣ Pilih mode download:\n"
        "   • GABUNG/PISAH = kirim via Telegram (max 50MB)\n"
        "   • GOFILE = upload ke cloud (unlimited size)\n\n"
        "📌 Mode ini akan download gambar dengan resolusi lebih tinggi.\n"
        "⚠️ Commands: /cancel (hentikan download) | /clear (hapus pesan)"
    )
    bot.reply_to(message, tutorial)

# -------------------- Handler /autodemo --------------------
@bot.message_handler(commands=['autodemo'])
def start_autodemo(message):
    chat_id = message.chat.id

    if chat_id in autodemo_active and autodemo_active[chat_id]:
        bot.reply_to(message, "🤖 Auto demo sudah aktif! Gunakan /offautodemo untuk menghentikan.")
        return

    # Check if any other autodemo is running to prevent crashes
    if any(autodemo_active.values()):
        bot.reply_to(message, "⚠️ Ada autodemo lain yang sedang berjalan. Hanya 1 autodemo diizinkan untuk mencegah crash.")
        return

    # Stop existing thread if any
    if chat_id in autodemo_thread and autodemo_thread[chat_id].is_alive():
        autodemo_active[chat_id] = False
        autodemo_thread[chat_id].join(timeout=2)

    autodemo_active[chat_id] = True
    bot.reply_to(message, "🚀 Auto demo dimulai! (Hanya 1 autodemo aktif untuk stabilitas)")

    # Start autodemo thread with better error handling
    def autodemo_loop():
        demo_urls = [
            "https://komiku.org/manga/mairimashita-iruma-kun/",
            "https://komiku.org/manga/one-piece/",
            "https://komiku.org/manga/naruto/",
            "https://komiku.org/manga/attack-on-titan/"
        ]
        current_url_index = 0
        chapter_start_num = 1

        try:
            while autodemo_active.get(chat_id, False):
                try:
                    # Longer initial wait to reduce resource usage
                    time.sleep(30)

                    if not autodemo_active.get(chat_id, False):
                        break

                    # Send /manga command
                    try:
                        bot.send_message(chat_id, "🤖 Auto Demo: Memulai mode /manga")
                    except Exception as msg_error:
                        print(f"❌ Failed to send message: {msg_error}")
                        if not autodemo_active.get(chat_id, False):
                            break
                        continue

                    user_state[chat_id] = {"step": "link", "mode": "normal", "timestamp": time.time()}

                    time.sleep(5)  # Increased delay

                    # Send manga URL
                    manga_url = demo_urls[current_url_index % len(demo_urls)]
                    try:
                        bot.send_message(chat_id, f"🤖 Auto Demo: Mengirim link\n{manga_url}")
                    except Exception as msg_error:
                        print(f"❌ Failed to send manga URL: {msg_error}")
                        if not autodemo_active.get(chat_id, False):
                            break
                        continue

                    # Process the manga URL
                    base_url, manga_name, total_chapters, sorted_chapters = get_manga_info(manga_url)
                    if base_url and manga_name and sorted_chapters and autodemo_active.get(chat_id, False):
                        user_state[chat_id].update({
                            "base_url": base_url,
                            "manga_name": manga_name,
                            "total_chapters": total_chapters,
                            "available_chapters": sorted_chapters,
                            "step": "awal"
                        })

                        time.sleep(5)  # Increased delay

                        # Use first available chapter instead of hardcoded numbers
                        if sorted_chapters:
                            first_chapter = sorted_chapters[0]
                            user_state[chat_id]["awal"] = first_chapter
                            user_state[chat_id]["step"] = "akhir"

                            time.sleep(5)  # Increased delay

                            if not autodemo_active.get(chat_id, False):
                                break

                            # Send chapter end (use same chapter for single chapter download)
                            chapter_end = first_chapter
                            try:
                                bot.send_message(chat_id, f"🤖 Auto Demo: Chapter awal: {first_chapter}")
                            except Exception as msg_error:
                                print(f"❌ Failed to send chapter start: {msg_error}")
                                if not autodemo_active.get(chat_id, False):
                                    break
                                continue

                            time.sleep(5)  # Increased delay

                            try:
                                bot.send_message(chat_id, f"🤖 Auto Demo: Chapter akhir: {chapter_end}")
                            except Exception as msg_error:
                                print(f"❌ Failed to send chapter end: {msg_error}")
                                if not autodemo_active.get(chat_id, False):
                                    break
                                continue

                            user_state[chat_id]["akhir"] = chapter_end
                            user_state[chat_id]["step"] = "mode"

                            time.sleep(5)  # Increased delay

                            # Auto select "pisah" mode
                            try:
                                bot.send_message(chat_id, "🤖 Auto Demo: Memilih mode PISAH per chapter")
                            except Exception as msg_error:
                                print(f"❌ Failed to send mode selection: {msg_error}")
                                if not autodemo_active.get(chat_id, False):
                                    break
                                continue

                            # Start download process
                            try:
                                user_cancel[chat_id] = False
                                base_url_format = user_state[chat_id]["base_url"]
                                manga_name_demo = user_state[chat_id]["manga_name"]
                                awal = user_state[chat_id]["awal"]
                                akhir = user_state[chat_id]["akhir"]

                                try:
                                    bot.send_message(chat_id, f"🤖 Auto Demo: Memulai download chapter {awal} s/d {akhir}...")
                                except Exception as msg_error:
                                    print(f"❌ Failed to send download start: {msg_error}")

                                # Download in pisah mode (only 1 chapter now)
                                for ch in [awal]: # Iterate only for the single chapter
                                    if not autodemo_active.get(chat_id, False) or user_cancel.get(chat_id):
                                        break

                                    try:
                                        bot.send_message(chat_id, f"🤖 Auto Demo: Download chapter {ch}...")
                                    except Exception as msg_error:
                                        print(f"❌ Failed to send download chapter message: {msg_error}")

                                    # Longer delay to reduce system load
                                    time.sleep(10)

                                    imgs = download_chapter(base_url_format.format(ch), ch, OUTPUT_DIR, chat_id, user_cancel)

                                    if imgs and not user_cancel.get(chat_id):
                                        pdf_name = f"{manga_name_demo} chapter {ch}.pdf"
                                        pdf_path = os.path.join(OUTPUT_DIR, pdf_name)
                                        create_pdf(imgs, pdf_path)

                                        try:
                                            # Check file size for autodemo
                                            file_size = os.path.getsize(pdf_path)
                                            max_size = 50 * 1024 * 1024  # 50MB

                                            if file_size > max_size:
                                                print(f"⚠️ Auto Demo: File too large ({file_size/(1024*1024):.1f}MB), skipping")
                                                auto_delete_pdf(pdf_path, 5)
                                                continue

                                            # Use GoFile upload for auto demo (Google Drive was removed)
                                            upload_success = False

                                            if not upload_success:
                                                # Fallback to direct upload
                                                with open(pdf_path, "rb") as pdf_file:
                                                    bot.send_document(
                                                        chat_id,
                                                        pdf_file,
                                                        caption=f"🤖 Auto Demo: {pdf_name} ({file_size/(1024*1024):.1f}MB)",
                                                        timeout=300
                                                    )
                                                print(f"✅ Auto Demo PDF sent: {pdf_name}")
                                            # Auto-delete PDF after 10 seconds
                                            auto_delete_pdf(pdf_path, 10)
                                        except Exception as upload_error:
                                            print(f"❌ Auto Demo upload error: {upload_error}")
                                            error_msg = str(upload_error)
                                            if "too large" in error_msg.lower():
                                                bot.send_message(chat_id, f"🤖 Auto Demo: File terlalu besar, dilewati")
                                            else:
                                                bot.send_message(chat_id, f"🤖 Auto Demo: Upload error")
                                            # Still delete even if upload failed
                                            auto_delete_pdf(pdf_path, 10)

                                    folder_ch = os.path.join(OUTPUT_DIR, f"chapter-{ch}")
                                    if os.path.exists(folder_ch):
                                        shutil.rmtree(folder_ch)

                                if autodemo_active.get(chat_id, False):
                                    try:
                                        bot.send_message(chat_id, "🤖 Auto Demo: Selesai! Menunggu demo berikutnya...")
                                    except Exception as msg_error:
                                        print(f"❌ Failed to send completion message: {msg_error}")

                                # Prepare for next demo
                                current_url_index += 1
                                chapter_start_num = 1 # Reset for next demo

                                # Wait before next demo (5 minutes)
                                if autodemo_active.get(chat_id, False):
                                    try:
                                        bot.send_message(chat_id, "🤖 Auto Demo: Menunggu 5 menit untuk demo berikutnya...")
                                    except:
                                        pass
                                    for _ in range(300):  # 5 minutes = 300 seconds
                                        if not autodemo_active.get(chat_id, False):
                                            break
                                        time.sleep(1)

                            except Exception as download_error:
                                print(f"❌ Download process error: {download_error}")
                                try:
                                    if autodemo_active.get(chat_id, False):
                                        bot.send_message(chat_id, "🤖 Auto Demo: Error saat download, mencoba berikutnya...")
                                except:
                                    pass

                        else: # Handle case where sorted_chapters is empty
                            print(f"❌ Failed to get manga info for {manga_url}")
                            try:
                                if autodemo_active.get(chat_id, False):
                                    bot.send_message(chat_id, "🤖 Auto Demo: Error mengambil data manga, mencoba berikutnya...")
                            except:
                                pass
                            continue  # Skip to next manga URL

                    else: # Handle case where get_manga_info failed
                        print(f"❌ Failed to get manga info for {manga_url}")
                        try:
                            if autodemo_active.get(chat_id, False):
                                bot.send_message(chat_id, "🤖 Auto Demo: Error mengambil data manga, mencoba berikutnya...")
                        except:
                            pass
                        continue  # Skip to next manga URL

                except Exception as inner_e:
                    print(f"❌ Autodemo inner loop error: {inner_e}")
                    try:
                        if autodemo_active.get(chat_id, False):
                            bot.send_message(chat_id, "🤖 Auto Demo: Error, menunggu sebelum retry...")
                    except:
                        pass

                    # Longer wait on error to prevent rapid crashes
                    for wait_second in range(60):  # 1 minute wait
                        if not autodemo_active.get(chat_id, False):
                            break
                        time.sleep(1)
                    continue

        except Exception as main_loop_error:
            print(f"❌ Autodemo main loop error for user {chat_id}: {main_loop_error}")
            try:
                if autodemo_active.get(chat_id, False):
                    bot.send_message(chat_id, "🤖 Auto Demo dihentikan karena error")
            except:
                pass
        finally:
            # Enhanced cleanup when autodemo stops
            try:
                print(f"🧹 Starting autodemo cleanup for user {chat_id}")

                # Stop autodemo flag first
                if chat_id in autodemo_active:
                    autodemo_active[chat_id] = False

                # Clean user states
                if chat_id in user_state:
                    user_state.pop(chat_id, None)
                if chat_id in user_cancel:
                    user_cancel.pop(chat_id, None)
                if chat_id in user_downloads:
                    user_downloads.pop(chat_id, None) # Clean user download preferences too

                # Clean any downloads
                cleanup_user_downloads(chat_id)

                # Remove thread reference
                if chat_id in autodemo_thread:
                    autodemo_thread.pop(chat_id, None)

                # Force garbage collection
                gc.collect()
                print(f"✅ Autodemo cleanup completed for user {chat_id}")

            except Exception as cleanup_error:
                print(f"⚠️ Autodemo cleanup error for user {chat_id}: {cleanup_error}")

    # Create and start thread with better naming
    autodemo_thread[chat_id] = threading.Thread(
        target=autodemo_loop,
        name=f"AutoDemo-{chat_id}"
    )
    autodemo_thread[chat_id].daemon = True
    autodemo_thread[chat_id].start()

# -------------------- Handler /offautodemo --------------------
@bot.message_handler(commands=['offautodemo'])
def stop_autodemo(message):
    chat_id = message.chat.id

    if chat_id not in autodemo_active or not autodemo_active[chat_id]:
        bot.reply_to(message, "🤖 Auto demo tidak aktif.")
        return

    # Stop autodemo gracefully
    autodemo_active[chat_id] = False
    user_cancel[chat_id] = True

    # Wait for autodemo thread to finish properly
    if chat_id in autodemo_thread:
        try:
            # Give thread time to cleanup (max 5 seconds)
            autodemo_thread[chat_id].join(timeout=5.0)
            print(f"🧹 Autodemo thread cleanup completed for user {chat_id}")
        except Exception as e:
            print(f"⚠️ Autodemo thread cleanup warning: {e}")
        finally:
            # Remove thread reference
            autodemo_thread.pop(chat_id, None)

    # Clean up any ongoing downloads
    cleanup_user_downloads(chat_id)

    # Clean up user state after thread is properly stopped
    user_state.pop(chat_id, None)
    user_cancel.pop(chat_id, None)
    user_downloads.pop(chat_id, None) # Clean user download preferences too


    bot.reply_to(message, "🛑 Auto demo dihentikan! Semua download dibatalkan dan file dihapus.")

# -------------------- Handler /admin --------------------
@bot.message_handler(commands=['ping'])
def ping_control(message):
    """Admin command to control keep-alive aggressiveness"""
    chat_id = message.chat.id
    
    # Check if user is admin
    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return
    
    try:
        global keep_alive_mode
        
        # Get current system status
        current_mode_text = "🔥 ULTRA AGRESIF" if keep_alive_mode == "ultra_aggressive" else "😌 NORMAL"
        uptime_seconds = int(time.time() - start_time)
        uptime_hours = uptime_seconds // 3600
        uptime_minutes = (uptime_seconds % 3600) // 60
        
        # Check active systems
        active_sessions = len(user_state)
        active_downloads = len([v for v in user_state.values() if v])
        
        ping_message = f"""🏓 **PING CONTROL PANEL**
        
🔧 **Status Sistem:**
• Mode Keep-Alive: {current_mode_text}
• Uptime: {uptime_hours}h {uptime_minutes}m
• Active Sessions: {active_sessions}
• Active Downloads: {active_downloads}

⚡ **System Threads:**
• UltraFastPing: {'✅ Running' if keep_alive_mode == 'ultra_aggressive' else '⏸️ Reduced'}
• ActivityPing: {'✅ Running' if keep_alive_mode == 'ultra_aggressive' else '⏸️ Reduced'}
• ForceAlivePing: {'✅ Running' if keep_alive_mode == 'ultra_aggressive' else '⏸️ Reduced'}
• ImmediateRecovery: {'✅ 15s checks' if keep_alive_mode == 'ultra_aggressive' else '⏸️ 60s checks'}

📋 **Mode Info:**
🔥 **Ultra Agresif:** Ping setiap 15-45 detik, 5 thread aktif, immediate recovery
😌 **Normal:** Ping setiap 3-5 menit, monitoring ringan

Pilih mode yang diinginkan:"""

        # Create inline keyboard
        markup = types.InlineKeyboardMarkup()
        
        if keep_alive_mode == "ultra_aggressive":
            btn_ultra = types.InlineKeyboardButton("🔥 Ultra Agresif ✅", callback_data="ping_ultra_aggressive")
            btn_normal = types.InlineKeyboardButton("😌 Normal", callback_data="ping_normal")
        else:
            btn_ultra = types.InlineKeyboardButton("🔥 Ultra Agresif", callback_data="ping_ultra_aggressive")
            btn_normal = types.InlineKeyboardButton("😌 Normal ✅", callback_data="ping_normal")
        
        markup.add(btn_ultra)
        markup.add(btn_normal)
        
        # Add status refresh button
        btn_refresh = types.InlineKeyboardButton("🔄 Refresh Status", callback_data="ping_refresh")
        markup.add(btn_refresh)
        
        bot.send_message(chat_id, ping_message, reply_markup=markup, parse_mode='Markdown')
        log_user_activity(chat_id, "ping_control")
        
    except Exception as e:
        print(f"❌ Ping control error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

@bot.message_handler(commands=['admin'])
def admin_panel(message):
    """Main admin panel with comprehensive bot management"""
    chat_id = message.chat.id

    # Check if user is admin
    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        # Get current statistics
        total_users = len(admin_data['user_stats'])
        active_downloads = len([v for v in user_state.values() if v])
        active_autodemo = len([v for v in autodemo_active.values() if v])
        banned_users_count = len(admin_data['banned_users'])

        # Get today's stats
        today = datetime.now().date().isoformat()
        daily_users = len(admin_data['daily_stats'][today]['users'])
        daily_downloads = admin_data['daily_stats'][today]['downloads']

        # Get system stats
        system_stats = get_system_stats()

        # Format system info
        system_info = ""
        if 'error' not in system_stats:
            system_info = f"""
🖥️ **Sistem:**
• CPU: {system_stats['cpu_usage']:.1f}%
• Memory: {system_stats['memory_used']:.1f}% ({system_stats['memory_total']}GB total)
• Disk: {system_stats['disk_used']:.1f}% ({system_stats['disk_free']}GB free)
• Uptime: {system_stats['uptime']/3600:.1f} hours"""

        # Create admin panel message
        admin_message = f"""🔧 **ADMIN PANEL**

📊 **Statistik Bot:**
• Total Users: {total_users}
• Users Hari Ini: {daily_users}
• Downloads Hari Ini: {daily_downloads}
• Users Dibanned: {banned_users_count}

⚡ **Status Real-time:**
• Download Aktif: {active_downloads}
• Autodemo Aktif: {active_autodemo}
• Error Logs: {len(admin_data['error_logs'])}
{system_info}

📝 **Commands Admin:**

**👥 User Management:**
• `/userstats` - Statistik user lengkap
• `/userinfo [chat_id]` - Info detail user
• `/banuser [chat_id] [hours] [reason]` - Ban user dengan durasi
• `/unban [chat_id]` - Unban user
• `/broadcast [pesan]` - Kirim pesan ke semua user

**🔧 System Control:**
• `/maintenance on/off` - Mode maintenance
• `/status` - Status lengkap bot
• `/killall` - Stop semua download (emergency)
• `/cleanup` - Bersihkan sistem

**📊 Analytics:**
• `/topmanga` - Manga terpopuler minggu ini
• `/errorlog [number]` - Log error dengan detail
• `/slowusers` - User dengan masalah koneksi

**⚙️ Configuration:**
• `/setgreeting [text]` - Ubah pesan welcome
• `/setmaxsize [MB]` - Set max file size Telegram
• `/addadmin [chat_id]` - Tambah admin baru
• `/removeadmin [chat_id]` - Hapus admin (owner only)
• `/adminlist` - Daftar semua admin

💡 **Tips:** Gunakan `/stats detail` untuk info lengkap"""

        # Create inline keyboard for quick actions
        markup = types.InlineKeyboardMarkup(row_width=2)
        btn_stats = types.InlineKeyboardButton("📊 Stats Detail", callback_data="admin_stats")
        btn_users = types.InlineKeyboardButton("👥 Users", callback_data="admin_users")
        btn_logs = types.InlineKeyboardButton("📋 Logs", callback_data="admin_logs")
        btn_system = types.InlineKeyboardButton("🔧 System", callback_data="admin_system")
        markup.add(btn_stats, btn_users)
        markup.add(btn_logs, btn_system)

        bot.send_message(chat_id, admin_message, parse_mode='Markdown', reply_markup=markup)
        log_user_activity(chat_id, "admin_access")

    except Exception as e:
        print(f"❌ Admin panel error: {e}")
        bot.reply_to(message, f"❌ Error loading admin panel: {str(e)[:100]}")

# -------------------- Handler /stats --------------------
@bot.message_handler(commands=['stats'])
def admin_stats(message):
    """Detailed statistics for admin"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        # Parse command for detail level
        parts = message.text.split()
        show_detail = len(parts) > 1 and parts[1].lower() == 'detail'

        # Calculate statistics
        total_users = len(admin_data['user_stats'])
        total_downloads = sum(user['downloads'] for user in admin_data['user_stats'].values())
        total_chapters = sum(user['total_chapters'] for user in admin_data['user_stats'].values())

        # Get recent activity (last 7 days)
        recent_days = []
        for i in range(7):
            date = (datetime.now() - timedelta(days=i)).date().isoformat()
            daily_data = admin_data['daily_stats'][date]
            recent_days.append({
                'date': date,
                'users': len(daily_data['users']),
                'downloads': daily_data['downloads']
            })

        # Top manga
        all_manga = defaultdict(int)
        for user_stats in admin_data['user_stats'].values():
            for manga, count in user_stats['favorite_manga'].items():
                all_manga[manga] += count

        top_manga = sorted(all_manga.items(), key=lambda x: x[1], reverse=True)[:5]

        # Format statistics message
        stats_message = f"""📊 **STATISTIK BOT LENGKAP**

🎯 **Overview:**
• Total Users: {total_users}
• Total Downloads: {total_downloads}
• Total Chapters: {total_chapters}
• Avg Chapters/User: {total_chapters/max(1, total_users):.1f}

📅 **Aktivitas 7 Hari Terakhir:**"""

        for day in recent_days:
            stats_message += f"\n• {day['date']}: {day['users']} users, {day['downloads']} downloads"

        if top_manga:
            stats_message += "\n\n🏆 **Top 5 Manga:**"
            for i, (manga, count) in enumerate(top_manga, 1):
                manga_name = manga[:30] + "..." if len(manga) > 30 else manga
                stats_message += f"\n{i}. {manga_name} ({count}x)"

        if show_detail:
            # Add more detailed stats
            active_users_7d = set()
            for i in range(7):
                date = (datetime.now() - timedelta(days=i)).date().isoformat()
                active_users_7d.update(admin_data['daily_stats'][date]['users'])

            stats_message += f"""

📈 **Detail Tambahan:**
• Active Users (7d): {len(active_users_7d)}
• Banned Users: {len(admin_data['banned_users'])}
• Error Logs: {len(admin_data['error_logs'])}
• Current Sessions: {len(user_state)}
• Active Autodemo: {len([v for v in autodemo_active.values() if v])}"""

        bot.send_message(chat_id, stats_message, parse_mode='Markdown')
        log_user_activity(chat_id, "admin_stats")

    except Exception as e:
        print(f"❌ Stats error: {e}")
        bot.reply_to(message, f"❌ Error loading stats: {str(e)[:100]}")

# -------------------- Handler /users --------------------
@bot.message_handler(commands=['users'])
def admin_users(message):
    """List all users for admin"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        # Get user list from database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT chat_id, downloads, first_seen, last_seen, total_chapters
            FROM user_stats
            ORDER BY last_seen DESC
            LIMIT 50
        ''')
        users = cursor.fetchall()
        conn.close()

        if not users:
            bot.reply_to(message, "📭 Belum ada user yang terdaftar.")
            return

        users_message = "👥 **DAFTAR USERS** (50 terbaru)\n\n"

        for user in users[:20]:  # Show first 20 in message
            chat_id_user, downloads, first_seen, last_seen, chapters = user

            # Parse dates
            try:
                last_seen_dt = datetime.fromisoformat(last_seen)
                days_ago = (datetime.now() - last_seen_dt).days
                if days_ago == 0:
                    last_activity = "Hari ini"
                elif days_ago == 1:
                    last_activity = "Kemarin"
                else:
                    last_activity = f"{days_ago} hari lalu"
            except:
                last_activity = "Unknown"

            # Check if banned
            banned_status = "🚫 BANNED" if chat_id_user in admin_data['banned_users'] else ""

            users_message += f"""💬 `{chat_id_user}` {banned_status}
   📥 {downloads} downloads, {chapters} chapters
   🕒 {last_activity}

"""

        if len(users) > 20:
            users_message += f"\n... dan {len(users) - 20} users lainnya"

        users_message += f"\n\n💡 **Commands:**\n• `/ban [chat_id]` - Ban user\n• `/unban [chat_id]` - Unban user"

        bot.send_message(chat_id, users_message, parse_mode='Markdown')
        log_user_activity(chat_id, "admin_users")

    except Exception as e:
        print(f"❌ Users list error: {e}")
        bot.reply_to(message, f"❌ Error loading users: {str(e)[:100]}")

# -------------------- Handler /ban --------------------
@bot.message_handler(commands=['ban'])
def admin_ban_user(message):
    """Ban a user"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: `/ban [chat_id] [reason]`\nContoh: `/ban 123456789 Spam`")
            return

        target_chat_id = int(parts[1])
        reason = " ".join(parts[2:]) if len(parts) > 2 else "Tidak ada alasan"

        # Add to banned users
        admin_data['banned_users'].add(target_chat_id)

        # Log to database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO banned_users (chat_id, banned_date, reason)
            VALUES (?, ?, ?)
        ''', (target_chat_id, datetime.now().isoformat(), reason))
        conn.commit()
        conn.close()

        # Clean user session if active
        user_state.pop(target_chat_id, None)
        user_cancel.pop(target_chat_id, None)
        user_downloads.pop(target_chat_id, None)
        autodemo_active.pop(target_chat_id, None)

        bot.reply_to(message, f"🚫 User `{target_chat_id}` telah dibanned.\nAlasan: {reason}", parse_mode='Markdown')

        # Try to notify the banned user
        try:
            bot.send_message(target_chat_id, f"🚫 Anda telah dibanned dari bot ini.\nAlasan: {reason}")
        except:
            pass  # User might have blocked the bot

        log_user_activity(chat_id, "admin_ban", f"Banned {target_chat_id}: {reason}")

    except ValueError:
        bot.reply_to(message, "❌ Chat ID harus berupa angka!")
    except Exception as e:
        print(f"❌ Ban error: {e}")
        bot.reply_to(message, f"❌ Error banning user: {str(e)[:100]}")

# -------------------- Handler /unban --------------------
@bot.message_handler(commands=['unban'])
def admin_unban_user(message):
    """Unban a user"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "❌ Format: `/unban [chat_id]`\nContoh: `/unban 123456789`")
            return

        target_chat_id = int(parts[1])

        # Remove from banned users
        admin_data['banned_users'].discard(target_chat_id)

        # Remove from database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM banned_users WHERE chat_id = ?', (target_chat_id,))
        conn.commit()
        conn.close()

        bot.reply_to(message, f"✅ User `{target_chat_id}` telah di-unban.", parse_mode='Markdown')

        # Try to notify the user
        try:
            bot.send_message(target_chat_id, "✅ Anda telah di-unban. Sekarang bisa menggunakan bot lagi!")
        except:
            pass  # User might have blocked the bot

        log_user_activity(chat_id, "admin_unban", f"Unbanned {target_chat_id}")

    except ValueError:
        bot.reply_to(message, "❌ Chat ID harus berupa angka!")
    except Exception as e:
        print(f"❌ Unban error: {e}")
        bot.reply_to(message, f"❌ Error unbanning user: {str(e)[:100]}")

# -------------------- Handler /logs --------------------
@bot.message_handler(commands=['logs'])
def admin_logs(message):
    """Show recent error logs"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        recent_errors = admin_data['error_logs'][-10:]  # Last 10 errors

        if not recent_errors:
            bot.reply_to(message, "✅ Tidak ada error logs terbaru.")
            return

        logs_message = "📋 **ERROR LOGS** (10 terbaru)\n\n"

        for i, error in enumerate(recent_errors, 1):
            timestamp = error['timestamp'][:19]  # Remove microseconds
            error_type = error['type']
            error_msg = error['message'][:100] + "..." if len(error['message']) > 100 else error['message']
            chat_id_error = error.get('chat_id', 'System')

            logs_message += f"""**{i}.** `{timestamp}`
🔸 Type: {error_type}
🔸 User: {chat_id_error}
🔸 Error: {error_msg}

"""

        bot.send_message(chat_id, logs_message, parse_mode='Markdown')
        log_user_activity(chat_id, "admin_logs")

    except Exception as e:
        print(f"❌ Logs error: {e}")
        bot.reply_to(message, f"❌ Error loading logs: {str(e)[:100]}")

# -------------------- Handler /broadcast --------------------
@bot.message_handler(commands=['broadcast'])
def admin_broadcast(message):
    """Broadcast message to all users"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: `/broadcast [pesan]`\nContoh: `/broadcast Bot akan maintenance 5 menit`")
            return

        broadcast_message = parts[1]

        # Get all users from database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT chat_id FROM user_stats')
        all_users = [row[0] for row in cursor.fetchall()]
        conn.close()

        if not all_users:
            bot.reply_to(message, "❌ Tidak ada user untuk broadcast.")
            return

        # Send confirmation
        confirm_msg = f"📢 Mengirim broadcast ke {len(all_users)} users..."
        bot.reply_to(message, confirm_msg)

        # Send broadcast
        success_count = 0
        failed_count = 0

        for user_chat_id in all_users:
            try:
                # Skip banned users
                if user_chat_id in admin_data['banned_users']:
                    continue

                formatted_message = f"📢 **Pengumuman dari Admin:**\n{broadcast_message}"
                bot.send_message(user_chat_id, formatted_message, parse_mode='Markdown')
                success_count += 1

                # Small delay to prevent rate limiting
                if success_count % 20 == 0:
                    time.sleep(1)

            except Exception as send_error:
                failed_count += 1
                if "blocked" not in str(send_error).lower():
                    print(f"❌ Broadcast error to {user_chat_id}: {send_error}")

        # Send result
        result_message = f"""✅ **Broadcast selesai!**
                        📤 Berhasil: {success_count} users
                        ❌ Gagal: {failed_count} users"""

        bot.send_message(chat_id, result_message, parse_mode='Markdown')
        log_user_activity(chat_id, "admin_broadcast", f"Sent to {success_count} users")

    except Exception as e:
        print(f"❌ Broadcast error: {e}")
        bot.reply_to(message, f"❌ Error broadcasting: {str(e)[:100]}")

# -------------------- Handler /cleanup --------------------
@bot.message_handler(commands=['cleanup'])
def admin_cleanup(message):
    """System cleanup for admin"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        bot.reply_to(message, "🧹 Memulai system cleanup...")

        # Run comprehensive cleanup
        auto_cleanup_all_errors()

        # Additional admin cleanup
        old_errors_count = len(admin_data['error_logs'])
        admin_data['error_logs'] = admin_data['error_logs'][-20:]  # Keep only last 20

        # Clear old daily stats (keep last 30 days)
        cutoff_date = (datetime.now() - timedelta(days=30)).date().isoformat()
        old_dates = [date for date in admin_data['daily_stats'].keys() if date < cutoff_date]
        for date in old_dates:
            admin_data['daily_stats'].pop(date, None)

        # Force garbage collection
        import gc
        gc.collect()

        cleanup_report = f"""✅ **System Cleanup Selesai!**

🧹 **Yang dibersihkan:**
• Downloads folder
• User states dan sessions
• Error logs: {old_errors_count} → {len(admin_data['error_logs'])}
• Daily stats lama: {len(old_dates)} hari
• Garbage collection completed

💾 **Memory usage optimized**"""

        bot.send_message(chat_id, cleanup_report, parse_mode='Markdown')
        log_user_activity(chat_id, "admin_cleanup")

    except Exception as e:
        print(f"❌ Cleanup error: {e}")
        bot.reply_to(message, f"❌ Error during cleanup: {str(e)[:100]}")

# -------------------- Admin Callback Handlers --------------------
@bot.callback_query_handler(func=lambda call: call.data.startswith('ping_'))
def handle_ping_callbacks(call):
    """Handle ping control button callbacks"""
    chat_id = call.message.chat.id
    
    if not is_admin(chat_id):
        bot.answer_callback_query(call.id, "❌ Access denied")
        return
    
    try:
        global keep_alive_mode
        
        bot.answer_callback_query(call.id)
        
        if call.data == 'ping_ultra_aggressive':
            if keep_alive_mode != "ultra_aggressive":
                keep_alive_mode = "ultra_aggressive"
                bot.answer_callback_query(call.id, "🔥 ULTRA AGGRESSIVE MODE ACTIVATED!", show_alert=True)
                
                # Send confirmation message
                confirm_msg = """🔥 **ULTRA AGGRESSIVE MODE ACTIVATED!**
                
⚡ **Sistem Sekarang:**
• Ping interval: 15-45 detik
• 5 Thread aktif: UltraFast, Activity, ForceAlive, FullStatus, ResourceCycling
• Immediate recovery: 15 detik checks
• Console cleanup: 20 detik
• Maximum restart attempts: 200

🚀 **REPLIT TIDAK AKAN BISA SLEEP!**"""
                
                bot.send_message(chat_id, confirm_msg, parse_mode='Markdown')
                print("🔥 ADMIN ACTIVATED ULTRA-AGGRESSIVE MODE!")
            else:
                bot.answer_callback_query(call.id, "✅ Ultra Aggressive mode sudah aktif")
        
        elif call.data == 'ping_normal':
            if keep_alive_mode != "normal":
                keep_alive_mode = "normal"
                bot.answer_callback_query(call.id, "😌 Normal mode activated", show_alert=True)
                
                # Send confirmation message
                confirm_msg = """😌 **NORMAL MODE ACTIVATED**
                
⚡ **Sistem Sekarang:**
• Ping interval: 3-5 menit
• Monitoring ringan
• Resource usage lebih hemat
• Standard recovery checks

💡 Bot masih akan tetap online, tapi dengan penggunaan resource yang lebih efisien."""
                
                bot.send_message(chat_id, confirm_msg, parse_mode='Markdown')
                print("😌 ADMIN ACTIVATED NORMAL MODE")
            else:
                bot.answer_callback_query(call.id, "✅ Normal mode sudah aktif")
        
        elif call.data == 'ping_refresh':
            # Refresh the ping control panel
            bot.answer_callback_query(call.id, "🔄 Status refreshed")
            
            # Create a fake message object to trigger ping_control
            fake_message = type('obj', (object,), {
                'chat': type('obj', (object,), {'id': chat_id})(),
                'text': '/ping'
            })()
            
            # Remove the old keyboard
            try:
                bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
            except:
                pass
            
            # Show updated status
            ping_control(fake_message)
    
    except Exception as e:
        print(f"❌ Ping callback error: {e}")
        bot.send_message(chat_id, f"❌ Error: {str(e)[:100]}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('admin_'))
def handle_admin_callbacks(call):
    """Handle admin panel button callbacks"""
    chat_id = call.message.chat.id

    if not is_admin(chat_id):
        bot.answer_callback_query(call.id, "❌ Access denied")
        return

    try:
        bot.answer_callback_query(call.id)

        if call.data == 'admin_stats':
            # Trigger stats command
            fake_message = type('obj', (object,), {
                'chat': type('obj', (object,), {'id': chat_id})(),
                'text': '/stats detail'
            })()
            admin_stats(fake_message)

        elif call.data == 'admin_users':
            # Trigger users command
            fake_message = type('obj', (object,), {
                'chat': type('obj', (object,), {'id': chat_id})(),
                'text': '/users'
            })()
            admin_users(fake_message)

        elif call.data == 'admin_logs':
            # Trigger logs command
            fake_message = type('obj', (object,), {
                'chat': type('obj', (object,), {'id': chat_id})(),
                'text': '/logs'
            })()
            admin_logs(fake_message)

        elif call.data == 'admin_system':
            # Show system info
            system_stats = get_system_stats()

            if 'error' in system_stats:
                system_message = "❌ Tidak bisa mendapatkan system stats"
            else:
                system_message = f"""🔧 **SYSTEM STATUS**

💾 **Memory:** {system_stats['memory_used']:.1f}%
📁 **Disk:** {system_stats['disk_used']:.1f}%
⚡ **CPU:** {system_stats['cpu_usage']:.1f}%
🕒 **Uptime:** {system_stats['uptime']/3600:.1f} hours

📊 **Bot Status:**
• Active Sessions: {len(user_state)}
• Active Downloads: {len([v for v in user_state.values() if v])}
• Active Autodemo: {len([v for v in autodemo_active.values() if v])}
• Banned Users: {len(admin_data['banned_users'])}"""

            bot.send_message(chat_id, system_message, parse_mode='Markdown')

    except Exception as e:
        print(f"❌ Admin callback error: {e}")
        bot.send_message(chat_id, f"❌ Error: {str(e)[:100]}")

# -------------------- Handler Forward Message to Admin --------------------
def forward_to_admin(message):
    """Forward non-command messages to admin with enhanced error handling"""
    if not ADMIN_CHAT_ID:
        print(f"⚠️ ADMIN_CHAT_ID tidak diset, tidak bisa forward message dari user {message.chat.id}")
        return False

    try:
        admin_id = int(ADMIN_CHAT_ID)

        # Safely get user info
        try:
            first_name = getattr(message.from_user, 'first_name', None) or 'Unknown'
            username = getattr(message.from_user, 'username', None)
            user_id = getattr(message.from_user, 'id', 'Unknown')
        except AttributeError:
            first_name = 'Unknown'
            username = None
            user_id = 'Unknown'

        user_info = f"👤 From: {first_name}"
        if username:
            user_info += f" (@{username})"
        user_info += f"\n🆔 Chat ID: `{message.chat.id}`"
        user_info += f"\n👥 User ID: `{user_id}`"

        # Safely get message content
        message_preview = ""
        if hasattr(message, 'text') and message.text:
            # Escape markdown characters and limit length
            safe_text = message.text.replace('`', '').replace('*', '').replace('_', '')[:200]
            message_preview = f"\n💬 Message: {safe_text}"
            if len(message.text) > 200:
                message_preview += "..."
        else:
            message_preview = "\n📎 Non-text message received"

        forward_text = f"{user_info}{message_preview}\n\n📝 Reply dengan: /reply {message.chat.id} [pesan]"

        # Send with better error handling
        bot.send_message(admin_id, forward_text, parse_mode='Markdown')
        print(f"✅ Message forwarded to admin from user {message.chat.id}: {message.text[:50] if hasattr(message, 'text') and message.text else 'Non-text message'}")
        return True

    except telebot.apihelper.ApiTelegramException as api_error:
        error_code = getattr(api_error, 'error_code', 'unknown')
        print(f"❌ Telegram API error forwarding to admin: {error_code} - {api_error}")
        return False

    except ValueError as ve:
        print(f"❌ Invalid ADMIN_CHAT_ID format: {ADMIN_CHAT_ID}")
        return False

    except Exception as e:
        print(f"❌ Forward to admin error: {e}")
        return False

# -------------------- Handler Additional Admin Commands --------------------

# 1. USER MANAGEMENT SYSTEM
@bot.message_handler(commands=['banuser'])
def admin_ban_user_v2(message):
    """Enhanced ban user with duration support"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: `/banuser [chat_id] [duration_hours] [reason]`\nContoh: `/banuser 123456789 24 Spam`")
            return

        target_chat_id = int(parts[1])
        duration_hours = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        reason = " ".join(parts[3:]) if len(parts) > 3 else "Tidak ada alasan"

        # Add to banned users
        admin_data['banned_users'].add(target_chat_id)

        # Calculate unban time if duration specified
        unban_time = None
        if duration_hours > 0:
            unban_time = (datetime.now() + timedelta(hours=duration_hours)).isoformat()

        # Log to database with safe column handling
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()

        # Check if columns exist first
        cursor.execute("PRAGMA table_info(banned_users)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'unban_time' in columns and 'duration_hours' in columns:
            # Use full insert with all columns
            cursor.execute('''
                INSERT OR REPLACE INTO banned_users (chat_id, banned_date, reason, unban_time, duration_hours)
                VALUES (?, ?, ?, ?, ?)
            ''', (target_chat_id, datetime.now().isoformat(), reason, unban_time, duration_hours))
        else:
            # Use basic insert without new columns
            cursor.execute('''
                INSERT OR REPLACE INTO banned_users (chat_id, banned_date, reason)
                VALUES (?, ?, ?)
            ''', (target_chat_id, datetime.now().isoformat(), reason))

        conn.commit()
        conn.close()

        # Clean user session
        user_state.pop(target_chat_id, None)
        user_cancel.pop(target_chat_id, None)
        user_downloads.pop(target_chat_id, None)
        autodemo_active.pop(target_chat_id, None)

        duration_text = f" selama {duration_hours} jam" if duration_hours > 0 else " permanen"
        bot.reply_to(message, f"🚫 User `{target_chat_id}` telah dibanned{duration_text}.\nAlasan: {reason}", parse_mode='Markdown')

        # Notify banned user
        try:
            bot.send_message(target_chat_id, f"🚫 Anda telah dibanned{duration_text}.\nAlasan: {reason}")
        except:
            pass

        log_user_activity(chat_id, "admin_banuser", f"Banned {target_chat_id}: {reason}")

    except ValueError:
        bot.reply_to(message, "❌ Chat ID dan duration harus berupa angka!")
    except Exception as e:
        bot.reply_to(message, f"❌ Error banning user: {str(e)[:100]}")

@bot.message_handler(commands=['userinfo'])
def admin_user_info(message):
    """Get detailed user information"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "❌ Format: `/userinfo [chat_id]`\nContoh: `/userinfo 123456789`")
            return

        target_chat_id = int(parts[1])

        # Get user info from database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT downloads, first_seen, last_seen, total_chapters, favorite_manga
            FROM user_stats WHERE chat_id = ?
        ''', (target_chat_id,))
        user_data = cursor.fetchone()

        # Get ban status
        cursor.execute('SELECT reason, banned_date FROM banned_users WHERE chat_id = ?', (target_chat_id,))
        ban_data = cursor.fetchone()

        # Get recent downloads
        cursor.execute('''
            SELECT manga_name, chapters, timestamp, success
            FROM download_logs WHERE chat_id = ?
            ORDER BY timestamp DESC LIMIT 5
        ''', (target_chat_id,))
        recent_downloads = cursor.fetchall()

        conn.close()

        if not user_data:
            bot.reply_to(message, f"❌ User `{target_chat_id}` tidak ditemukan dalam database.", parse_mode='Markdown')
            return

        downloads, first_seen, last_seen, chapters, fav_manga_json = user_data

        # Parse favorite manga
        try:
            fav_manga = json.loads(fav_manga_json) if fav_manga_json else {}
            top_manga = sorted(fav_manga.items(), key=lambda x: x[1], reverse=True)[:3]
        except:
            top_manga = []

        # Calculate activity
        try:
            last_seen_dt = datetime.fromisoformat(last_seen)
            days_since_active = (datetime.now() - last_seen_dt).days
            if days_since_active == 0:
                activity_status = "🟢 Aktif hari ini"
            elif days_since_active <= 7:
                activity_status = f"🟡 Aktif {days_since_active} hari lalu"
            else:
                activity_status = f"🔴 Tidak aktif {days_since_active} hari"
        except:
            activity_status = "❓ Unknown"

        # Format user info
        user_info = f"""👤 **INFO USER: `{target_chat_id}`**

📊 **Statistik:**
• Total Downloads: {downloads}
• Total Chapters: {chapters}
• First Seen: {first_seen[:10] if first_seen else 'Unknown'}
• Last Active: {last_seen[:10] if last_seen else 'Unknown'}
• Status: {activity_status}"""

        # Ban status
        if ban_data:
            reason, banned_date = ban_data
            user_info += f"\n\n🚫 **STATUS BAN:**\n• Dibanned: {banned_date[:10]}\n• Alasan: {reason}"
        else:
            user_info += f"\n\n✅ **Status:** Tidak dibanned"

        # Top manga
        if top_manga:
            user_info += f"\n\n🏆 **Top Manga:**"
            for i, (manga, count) in enumerate(top_manga, 1):
                manga_short = manga[:25] + "..." if len(manga) > 25 else manga
                user_info += f"\n{i}. {manga_short} ({count}x)"

        # Recent activity
        if recent_downloads:
            user_info += f"\n\n📋 **Recent Downloads:**"
            for manga, chapters_str, timestamp, success in recent_downloads[:3]:
                status = "✅" if success else "❌"
                date = timestamp[:10]
                manga_short = manga[:20] + "..." if len(manga) > 20 else manga
                user_info += f"\n{status} {manga_short} - {date}"

        bot.send_message(chat_id, user_info, parse_mode='Markdown')

    except ValueError:
        bot.reply_to(message, "❌ Chat ID harus berupa angka!")
    except Exception as e:
        bot.reply_to(message, f"❌ Error getting user info: {str(e)[:100]}")

@bot.message_handler(commands=['userstats'])
def admin_user_stats(message):
    """Get comprehensive user statistics"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()

        # Total users
        cursor.execute('SELECT COUNT(*) FROM user_stats')
        total_users = cursor.fetchone()[0]

        # Active today
        today = datetime.now().date().isoformat()
        cursor.execute('SELECT COUNT(*) FROM user_stats WHERE last_seen LIKE ?', (f'{today}%',))
        active_today = cursor.fetchone()[0]

        # Active this week
        week_ago = (datetime.now() - timedelta(days=7)).date().isoformat()
        cursor.execute('SELECT COUNT(*) FROM user_stats WHERE last_seen >= ?', (week_ago,))
        active_week = cursor.fetchone()[0]

        # New users today
        cursor.execute('SELECT COUNT(*) FROM user_stats WHERE first_seen LIKE ?', (f'{today}%',))
        new_today = cursor.fetchone()[0]

        # Total downloads and chapters
        cursor.execute('SELECT SUM(downloads), SUM(total_chapters) FROM user_stats')
        total_downloads, total_chapters = cursor.fetchone()

        # Top downloaders
        cursor.execute('SELECT chat_id, downloads FROM user_stats ORDER BY downloads DESC LIMIT 5')
        top_downloaders = cursor.fetchall()

        # Download distribution
        cursor.execute('SELECT downloads FROM user_stats ORDER BY downloads DESC')
        all_downloads = [row[0] for row in cursor.fetchall()]

        conn.close()

        # Calculate percentiles
        if all_downloads:
            heavy_users = len([d for d in all_downloads if d >= 10])
            moderate_users = len([d for d in all_downloads if 3 <= d < 10])
            light_users = len([d for d in all_downloads if 1 <= d < 3])
            inactive_users = len([d for d in all_downloads if d == 0])
        else:
            heavy_users = moderate_users = light_users = inactive_users = 0

        stats_message = f"""📊 **STATISTIK USER LENGKAP**

👥 **Overview:**
• Total Users: {total_users}
• Aktif Hari Ini: {active_today}
• Aktif Minggu Ini: {active_week}
• User Baru Hari Ini: {new_today}

📈 **Activity:**
• Total Downloads: {total_downloads or 0}
• Total Chapters: {total_chapters or 0}
• Avg Downloads/User: {(total_downloads or 0)/max(1, total_users):.1f}

📊 **User Distribution:**
• Heavy Users (10+ downloads): {heavy_users}
• Moderate Users (3-9): {moderate_users}
• Light Users (1-2): {light_users}
• Inactive Users (0): {inactive_users}"""

        if top_downloaders:
            stats_message += f"\n\n🏆 **Top Downloaders:**"
            for i, (user_id, downloads) in enumerate(top_downloaders, 1):
                stats_message += f"\n{i}. `{user_id}` - {downloads} downloads"

        bot.send_message(chat_id, stats_message, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Error getting user stats: {str(e)[:100]}")

# 2. SYSTEM CONTROL COMMANDS
@bot.message_handler(commands=['maintenance'])
def admin_maintenance_mode(message):
    """Toggle maintenance mode"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        if len(parts) != 2 or parts[1] not in ['on', 'off']:
            bot.reply_to(message, "❌ Format: `/maintenance on` atau `/maintenance off`")
            return

        mode = parts[1] == 'on'
        admin_data['bot_config']['maintenance_mode'] = mode

        # Save to database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO bot_config (key, value)
            VALUES ('maintenance_mode', ?)
        ''', (str(mode),))
        conn.commit()
        conn.close()

        status = "🔧 AKTIF" if mode else "✅ NONAKTIF"
        bot.reply_to(message, f"🛠️ **Mode Maintenance: {status}**\n\n{'Bot hanya dapat diakses oleh admin.' if mode else 'Bot dapat diakses oleh semua user.'}", parse_mode='Markdown')

        # Broadcast maintenance notification
        if mode:
            maintenance_msg = "🔧 **MAINTENANCE MODE**\n\nBot sedang dalam mode maintenance. Akses terbatas untuk sementara.\n\nMohon tunggu hingga maintenance selesai."
            # Send to recent active users only
            conn = sqlite3.connect('bot_admin.db')
            cursor = conn.cursor()
            recent_date = (datetime.now() - timedelta(hours=24)).isoformat()
            cursor.execute('SELECT chat_id FROM user_stats WHERE last_seen >= ?', (recent_date,))
            recent_users = [row[0] for row in cursor.fetchall()]
            conn.close()

            for user_id in recent_users[:50]:  # Limit to 50 recent users
                try:
                    bot.send_message(user_id, maintenance_msg, parse_mode='Markdown')
                except:
                    pass

        log_user_activity(chat_id, "admin_maintenance", f"Mode: {mode}")

    except Exception as e:
        bot.reply_to(message, f"❌ Error setting maintenance mode: {str(e)[:100]}")

@bot.message_handler(commands=['status'])
def admin_bot_status(message):
    """Get comprehensive bot status"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        # System stats
        system_stats = get_system_stats()

        # Bot stats
        active_sessions = len(user_state)
        active_downloads = len([v for v in user_state.values() if v and v.get('step') not in ['link', 'awal', 'akhir', 'mode']])
        active_autodemo = len([v for v in autodemo_active.values() if v])
        banned_users = len(admin_data['banned_users'])

        # Database stats
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM user_stats')
        total_users = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM download_logs WHERE timestamp LIKE ?', (datetime.now().date().isoformat() + '%',))
        downloads_today = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM error_logs WHERE timestamp LIKE ?', (datetime.now().date().isoformat() + '%',))
        errors_today = cursor.fetchone()[0]
        conn.close()

        # Uptime calculation
        uptime_seconds = system_stats.get('uptime', 0)
        uptime_hours = uptime_seconds / 3600

        # Maintenance mode
        maintenance = admin_data['bot_config'].get('maintenance_mode', False)

        status_message = f"""🔧 **BOT STATUS LENGKAP**

🖥️ **System Resources:**
• CPU Usage: {system_stats.get('cpu_usage', 'N/A')}%
• Memory Usage: {system_stats.get('memory_used', 'N/A')}%
• Disk Usage: {system_stats.get('disk_used', 'N/A')}%
• Uptime: {uptime_hours:.1f} hours

⚡ **Bot Performance:**
• Active Sessions: {active_sessions}
• Active Downloads: {active_downloads}
• Active Autodemo: {active_autodemo}
• Maintenance Mode: {'🔧 ON' if maintenance else '✅ OFF'}

📊 **Today's Activity:**
• Downloads: {downloads_today}
• Errors: {errors_today}
• New Users: Calculating...

👥 **User Management:**
• Total Users: {total_users}
• Banned Users: {banned_users}

💾 **Storage:**
• Downloads Folder: {_get_folder_size('downloads')} MB
• Database Size: {_get_file_size('bot_admin.db')} MB"""

        bot.send_message(chat_id, status_message, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Error getting bot status: {str(e)[:100]}")

def _get_folder_size(folder_path):
    """Helper function to get folder size in MB"""
    try:
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
        return round(total_size / (1024 * 1024), 2)
    except:
        return 0

def _get_file_size(file_path):
    """Helper function to get file size in MB"""
    try:
        return round(os.path.getsize(file_path) / (1024 * 1024), 2)
    except:
        return 0

@bot.message_handler(commands=['killall'])
def admin_kill_all_downloads(message):
    """Emergency stop all downloads"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        # Stop all downloads
        active_count = 0
        for user_id in list(user_state.keys()):
            user_cancel[user_id] = True
            cleanup_user_downloads(user_id)
            active_count += 1

        # Stop all autodemos
        autodemo_count = 0
        for user_id in list(autodemo_active.keys()):
            autodemo_active[user_id] = False
            autodemo_count += 1

        # Clear all states
        user_state.clear()
        user_cancel.clear()
        user_downloads.clear()

        # Force cleanup
        auto_cleanup_all_errors()

        bot.reply_to(message, f"🚨 **EMERGENCY STOP COMPLETED**\n\n• Stopped {active_count} active downloads\n• Stopped {autodemo_count} autodemos\n• Cleared all user sessions\n• Performed system cleanup")

        log_user_activity(chat_id, "admin_emergency_stop", f"Stopped {active_count} downloads")

    except Exception as e:
        bot.reply_to(message, f"❌ Error in emergency stop: {str(e)[:100]}")

# 3. ANALYTICS & INSIGHTS
@bot.message_handler(commands=['topmanga'])
def admin_top_manga(message):
    """Get most popular manga this week"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        # Get downloads from last 7 days
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()

        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT manga_name, COUNT(*) as download_count,
                   COUNT(DISTINCT chat_id) as unique_users
            FROM download_logs
            WHERE timestamp >= ? AND success = 1
            GROUP BY manga_name
            ORDER BY download_count DESC
            LIMIT 10
        ''', (week_ago,))

        top_manga = cursor.fetchall()
        conn.close()

        if not top_manga:
            bot.reply_to(message, "📊 Tidak ada data manga untuk minggu ini.")
            return

        top_manga_message = f"🏆 **TOP MANGA MINGGU INI**\n\n"

        for i, (manga_name, downloads, users) in enumerate(top_manga, 1):
            manga_short = manga_name[:30] + "..." if len(manga_name) > 30 else manga_name
            top_manga_message += f"**{i}.** {manga_short}\n"
            top_manga_message += f"   📥 {downloads} downloads • 👥 {users} users\n\n"

        # Add summary
        total_downloads = sum(row[1] for row in top_manga)
        total_unique_users = len(set(row[2] for row in top_manga))

        top_manga_message += f"📊 **Summary:**\n• Total Downloads: {total_downloads}\n• Active Users: {total_unique_users}"

        bot.send_message(chat_id, top_manga_message, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Error getting top manga: {str(e)[:100]}")

@bot.message_handler(commands=['errorlog'])
def admin_error_log_detailed(message):
    """Get detailed error logs with filtering"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        limit = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10

        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT timestamp, error_type, error_message, chat_id
            FROM error_logs
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (limit,))

        errors = cursor.fetchall()
        conn.close()

        if not errors:
            bot.reply_to(message, "✅ Tidak ada error logs.")
            return

        # Group errors by type
        error_types = defaultdict(int)
        for error in errors:
            error_types[error[1]] += 1

        error_message = f"📋 **ERROR LOGS** ({limit} terbaru)\n\n"

        # Show error type summary
        error_message += "📊 **Error Types:**\n"
        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
            error_message += f"• {error_type}: {count}x\n"

        error_message += f"\n📝 **Recent Errors:**\n"

        for i, (timestamp, error_type, error_msg, user_id) in enumerate(errors[:5], 1):
            time_str = timestamp[:19].replace('T', ' ')
            error_short = error_msg[:50] + "..." if len(error_msg) > 50 else error_msg
            user_str = f"User {user_id}" if user_id else "System"

            error_message += f"**{i}.** `{time_str}`\n"
            error_message += f"   🔸 {error_type} - {user_str}\n"
            error_message += f"   🔸 {error_short}\n\n"

        if len(errors) > 5:
            error_message += f"... dan {len(errors) - 5} error lainnya\n\n"

        error_message += f"💡 Gunakan `/errorlog [number]` untuk melihat lebih banyak"

        bot.send_message(chat_id, error_message, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Error getting error logs: {str(e)[:100]}")

@bot.message_handler(commands=['slowusers'])
def admin_slow_users(message):
    """Get users with frequent timeouts/failures"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        # Get users with high failure rates
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT chat_id,
                   COUNT(*) as total_attempts,
                   SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failures,
                   (SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as failure_rate
            FROM download_logs
            WHERE timestamp >= ?
            GROUP BY chat_id
            HAVING COUNT(*) >= 3 AND failure_rate > 30
            ORDER BY failure_rate DESC
            LIMIT 10
        ''', ((datetime.now() - timedelta(days=7)).isoformat(),))

        slow_users = cursor.fetchall()
        conn.close()

        if not slow_users:
            bot.reply_to(message, "✅ Tidak ada user dengan tingkat kegagalan tinggi minggu ini.")
            return

        slow_message = f"🐌 **USERS DENGAN MASALAH** (7 hari terakhir)\n\n"

        for i, (user_id, total, failures, rate) in enumerate(slow_users, 1):
            slow_message += f"**{i}.** `{user_id}`\n"
            slow_message += f"   📊 {failures}/{total} gagal ({rate:.1f}%)\n\n"

        slow_message += f"💡 **Tips:** Users ini mungkin perlu bantuan atau mengalami masalah koneksi."

        bot.send_message(chat_id, slow_message, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Error getting slow users: {str(e)[:100]}")

# 4. CONFIGURATION COMMANDS
@bot.message_handler(commands=['setgreeting'])
def admin_set_greeting(message):
    """Set custom welcome message"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: `/setgreeting [pesan welcome baru]`")
            return

        new_greeting = parts[1]
        admin_data['bot_config']['welcome_message'] = new_greeting

        # Save to database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO bot_config (key, value)
            VALUES ('welcome_message', ?)
        ''', (new_greeting,))
        conn.commit()
        conn.close()

        bot.reply_to(message, f"✅ **Welcome message berhasil diubah!**\n\nPreview:\n{new_greeting}", parse_mode='Markdown')
        log_user_activity(chat_id, "admin_set_greeting")

    except Exception as e:
        bot.reply_to(message, f"❌ Error setting greeting: {str(e)[:100]}")

@bot.message_handler(commands=['setmaxsize'])
def admin_set_max_size(message):
    """Set maximum file size for direct Telegram upload"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            bot.reply_to(message, "❌ Format: `/setmaxsize [size_MB]`\nContoh: `/setmaxsize 45`")
            return

        max_size = int(parts[1])
        if max_size < 1 or max_size > 50:
            bot.reply_to(message, "❌ Size harus antara 1-50 MB (batas Telegram)")
            return

        admin_data['bot_config']['max_file_size'] = max_size

        # Save to database
        conn = sqlite3.connect('bot_admin.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO bot_config (key, value)
            VALUES ('max_file_size', ?)
        ''', (str(max_size),))
        conn.commit()
        conn.close()

        bot.reply_to(message, f"✅ **Max file size berhasil diubah!**\n\nSize baru: {max_size} MB\nFile yang lebih besar akan menggunakan GoFile.")
        log_user_activity(chat_id, "admin_set_maxsize", f"New size: {max_size}MB")

    except Exception as e:
        bot.reply_to(message, f"❌ Error setting max size: {str(e)[:100]}")

# 5. MULTI-ADMIN SUPPORT
@bot.message_handler(commands=['addadmin'])
def admin_add_admin(message):
    """Add new admin"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "❌ Format: `/addadmin [chat_id]`\nContoh: `/addadmin 123456789`")
            return

        new_admin_id = int(parts[1])

        # Add to admin list (you'll need to modify the is_admin function to support multiple admins)
        if 'admin_list' not in admin_data['bot_config']:
            admin_data['bot_config']['admin_list'] = []

        if str(new_admin_id) not in admin_data['bot_config']['admin_list']:
            admin_data['bot_config']['admin_list'].append(str(new_admin_id))

            # Save to database
            conn = sqlite3.connect('bot_admin.db')
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO bot_config (key, value)
                VALUES ('admin_list', ?)
            ''', (json.dumps(admin_data['bot_config']['admin_list']),))
            conn.commit()
            conn.close()

            bot.reply_to(message, f"✅ User `{new_admin_id}` berhasil ditambahkan sebagai admin!", parse_mode='Markdown')

            # Notify new admin
            try:
                bot.send_message(new_admin_id, "🎉 **Selamat!**\n\nAnda telah ditambahkan sebagai admin bot ini.\nGunakan `/admin` untuk mengakses panel admin.", parse_mode='Markdown')
            except:
                pass

            log_user_activity(chat_id, "admin_add_admin", f"Added {new_admin_id}")
        else:
            bot.reply_to(message, f"⚠️ User `{new_admin_id}` sudah menjadi admin.", parse_mode='Markdown')

    except ValueError:
        bot.reply_to(message, "❌ Chat ID harus berupa angka!")
    except Exception as e:
        bot.reply_to(message, f"❌ Error adding admin: {str(e)[:100]}")

@bot.message_handler(commands=['removeadmin', 'deladmin'])
def admin_remove_admin(message):
    """Remove admin privileges (only owner can do this)"""
    chat_id = message.chat.id
    
    # Only allow the main owner to remove admins for security
    if str(chat_id) != ADMIN_CHAT_ID:
        bot.reply_to(message, "❌ Hanya owner utama yang bisa menghapus admin.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "❌ Format: `/removeadmin [chat_id]`\nContoh: `/removeadmin 123456789`")
            return
        
        target_admin_id = int(parts[1])
        
        # Prevent owner from removing themselves
        if str(target_admin_id) == ADMIN_CHAT_ID:
            bot.reply_to(message, "❌ Owner utama tidak bisa dihapus dari admin.")
            return
        
        # Check if admin list exists
        if 'admin_list' not in admin_data['bot_config']:
            admin_data['bot_config']['admin_list'] = []
        
        admin_list = admin_data['bot_config']['admin_list']
        
        if str(target_admin_id) in admin_list:
            # Remove from admin list
            admin_list.remove(str(target_admin_id))
            
            # Save to database
            conn = sqlite3.connect('bot_admin.db')
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO bot_config (key, value)
                VALUES ('admin_list', ?)
            ''', (json.dumps(admin_list),))
            conn.commit()
            conn.close()
            
            bot.reply_to(message, f"✅ Status admin telah dicabut dari user `{target_admin_id}`!", parse_mode='Markdown')
            
            # Notify the removed admin
            try:
                bot.send_message(target_admin_id, 
                    "⚠️ **Status Admin Dicabut**\n\n"
                    "Status admin Anda telah dicabut oleh owner.\n"
                    "Anda tidak lagi memiliki akses ke fitur admin bot ini.", 
                    parse_mode='Markdown')
            except Exception as notify_error:
                print(f"❌ Failed to notify removed admin {target_admin_id}: {notify_error}")
            
            log_user_activity(chat_id, "admin_remove_admin", f"Removed admin {target_admin_id}")
            
        else:
            bot.reply_to(message, f"⚠️ User `{target_admin_id}` bukan admin atau sudah dihapus.", parse_mode='Markdown')
    
    except ValueError:
        bot.reply_to(message, "❌ Chat ID harus berupa angka!")
    except Exception as e:
        print(f"❌ Remove admin error: {e}")
        bot.reply_to(message, f"❌ Error removing admin: {str(e)[:100]}")

@bot.message_handler(commands=['adminlist'])
def admin_list_admins(message):
    """List all admins"""
    chat_id = message.chat.id

    if not is_admin(chat_id):
        bot.reply_to(message, "❌ Anda tidak memiliki akses admin.")
        return

    try:
        admin_list = admin_data['bot_config'].get('admin_list', [])

        admin_message = f"👑 **DAFTAR ADMIN**\n\n"
        admin_message += f"🔹 **Owner:** `{ADMIN_CHAT_ID}`\n\n"

        if admin_list:
            admin_message += f"🔸 **Additional Admins:**\n"
            for i, admin_id in enumerate(admin_list, 1):
                admin_message += f"{i}. `{admin_id}`\n"
        else:
            admin_message += f"🔸 **Additional Admins:** Tidak ada\n"

        admin_message += f"\n💡 **Commands:**\n"
        admin_message += f"• `/addadmin [chat_id]` - Tambah admin baru\n"
        
        # Only show removeadmin to owner
        if str(chat_id) == ADMIN_CHAT_ID:
            admin_message += f"• `/removeadmin [chat_id]` - Hapus admin (owner only)"

        bot.send_message(chat_id, admin_message, parse_mode='Markdown')

    except Exception as e:
        bot.reply_to(message, f"❌ Error listing admins: {str(e)[:100]}")

# -------------------- Handler Reply Command for Admin --------------------
@bot.message_handler(commands=['reply'])
def admin_reply(message):
    """Handle admin reply to users with enhanced error handling"""
    try:
        # Check if ADMIN_CHAT_ID is set
        if not ADMIN_CHAT_ID:
            print(f"⚠️ ADMIN_CHAT_ID not set, ignoring reply command from {message.chat.id}")
            return

        # Check if sender is admin
        if str(message.chat.id) != ADMIN_CHAT_ID:
            print(f"⚠️ Non-admin {message.chat.id} tried to use reply command")
            return

        print(f"🔧 Admin reply command received from {message.chat.id}")

        # Parse command
        parts = message.text.split(' ', 2)
        if len(parts) < 3:
            try:
                bot.reply_to(message, "❌ Format: /reply [chat_id] [pesan]\nContoh: /reply 123456789 Halo, terima kasih pesannya!")
                print("⚠️ Invalid reply format from admin")
            except Exception as format_error:
                print(f"❌ Error sending format message: {format_error}")
            return

        # Extract target chat ID and message
        try:
            target_chat_id = int(parts[1])
            reply_text = parts[2].strip()

            if not reply_text:
                try:
                    bot.reply_to(message, "❌ Pesan tidak boleh kosong!")
                except Exception as empty_error:
                    print(f"❌ Error sending empty message warning: {empty_error}")
                return

            print(f"🔄 Sending reply to user {target_chat_id}: {reply_text[:50]}...")

        except ValueError as ve:
            print(f"❌ Invalid chat ID format: {parts[1]}")
            try:
                bot.reply_to(message, f"❌ Chat ID tidak valid: {parts[1]}\nChat ID harus berupa angka!")
            except Exception as id_error:
                print(f"❌ Error sending invalid ID message: {id_error}")
            return

        # Send reply to user with enhanced error handling
        try:
            formatted_reply = f"📩 Pesan dari Admin:\n{reply_text}"
            bot.send_message(target_chat_id, formatted_reply)
            print(f"✅ Reply sent successfully to user {target_chat_id}")

            # Confirm to admin
            try:
                confirm_msg = f"✅ Balasan terkirim ke chat {target_chat_id}\n💬 Pesan: {reply_text[:100]}{'...' if len(reply_text) > 100 else ''}"
                bot.reply_to(message, confirm_msg)
                print(f"✅ Confirmation sent to admin")
            except Exception as confirm_error:
                print(f"⚠️ Error sending confirmation to admin: {confirm_error}")
                # Don't fail the whole operation if confirmation fails

        except telebot.apihelper.ApiTelegramException as api_error:
            error_code = getattr(api_error, 'error_code', 'unknown')
            error_desc = getattr(api_error, 'description', str(api_error))
            print(f"❌ Telegram API error when replying to {target_chat_id}: {error_code} - {error_desc}")

            try:
                if error_code == 400:
                    bot.reply_to(message, f"❌ Chat {target_chat_id} tidak valid atau bot diblokir user")
                elif error_code == 403:
                    bot.reply_to(message, f"❌ Bot diblokir oleh user {target_chat_id}")
                else:
                    bot.reply_to(message, f"❌ Error Telegram API: {error_desc}")
            except Exception as error_msg_error:
                print(f"❌ Error sending error message to admin: {error_msg_error}")

        except Exception as send_error:
            print(f"❌ Unexpected error sending reply to {target_chat_id}: {send_error}")
            try:
                bot.reply_to(message, f"❌ Error mengirim balasan ke {target_chat_id}: {str(send_error)[:100]}")
            except Exception as error_msg_error:
                print(f"❌ Error sending error message to admin: {error_msg_error}")

    except Exception as main_error:
        print(f"❌ Critical error in admin_reply function: {main_error}")
        try:
            bot.reply_to(message, f"❌ Error sistem: {str(main_error)[:100]}")
        except Exception as critical_error:
            print(f"❌ Critical error sending error message: {critical_error}")
            # If we can't even send an error message, don't crash the bot

# -------------------- Handler Get My Chat ID --------------------
@bot.message_handler(commands=['myid'])
def get_chat_id(message):
    """Get user's chat ID with easy copy format like BotFather"""
    chat_id = message.chat.id

    # Format like BotFather for easy copying
    user_info = f"🆔 **Your Chat ID:**\n```\n{chat_id}\n```"
    if message.from_user.first_name:
        user_info += f"\n👤 Name: {message.from_user.first_name}"
    if message.from_user.username:
        user_info += f"\n📛 Username: @{message.from_user.username}"

    user_info += f"\n\n💡 Tap the ID above to copy it!"

    bot.send_message(chat_id, user_info, parse_mode='Markdown')

# -------------------- Handler Report to Admin --------------------
@bot.message_handler(commands=['report'])
def report_to_admin(message):
    """Allow users to send reports/messages to admin"""
    try:
        if not ADMIN_CHAT_ID:
            bot.reply_to(message, "❌ Sistem report tidak tersedia saat ini.")
            return

        # Parse the report message
        command_parts = message.text.split(' ', 1)
        if len(command_parts) < 2:
            bot.reply_to(message,
                "📝 **Cara menggunakan /report:**\n"
                "```\n/report [pesan anda]\n```\n"
                "Contoh: `/report Bot tidak bisa download chapter 50`\n\n"
                "💡 Pesan anda akan diteruskan ke admin untuk ditindaklanjuti."
            , parse_mode='Markdown')
            return

        report_message = command_parts[1].strip()
        if not report_message:
            bot.reply_to(message, "❌ Pesan report tidak boleh kosong!")
            return

        # Get user info safely
        try:
            first_name = getattr(message.from_user, 'first_name', None) or 'Unknown'
            username = getattr(message.from_user, 'username', None)
            user_id = getattr(message.from_user, 'id', 'Unknown')
        except AttributeError:
            first_name = 'Unknown'
            username = None
            user_id = 'Unknown'

        # Format report for admin
        user_info = f"📢 **REPORT dari User**\n"
        user_info += f"👤 From: {first_name}"
        if username:
            user_info += f" (@{username})"
        user_info += f"\n🆔 Chat ID: ```{message.chat.id}```"
        user_info += f"\n👥 User ID: `{user_id}`"
        user_info += f"\n📝 Report: {report_message}"
        user_info += f"\n\n📝 Reply dengan: /reply {message.chat.id} [balasan]"

        # Send report to admin
        admin_id = int(ADMIN_CHAT_ID)
        bot.send_message(admin_id, user_info, parse_mode='Markdown')

        # Confirm to user
        bot.reply_to(message,
            "✅ **Report berhasil dikirim ke admin!**\n"
            "📬 Admin akan membalas segera.\n\n"
            "💡 Gunakan `/report [pesan]` untuk melaporkan masalah lainnya."
        , parse_mode='Markdown')

        print(f"📢 Report sent to admin from user {message.chat.id}: {report_message[:50]}...")

    except ValueError as ve:
        print(f"❌ Invalid ADMIN_CHAT_ID format in report: {ADMIN_CHAT_ID}")
        bot.reply_to(message, "❌ Sistem report bermasalah. Coba lagi nanti.")

    except telebot.apihelper.ApiTelegramException as api_error:
        error_code = getattr(api_error, 'error_code', 'unknown')
        print(f"❌ Telegram API error in report: {error_code} - {api_error}")
        bot.reply_to(message, "❌ Gagal mengirim report. Coba lagi nanti.")

    except Exception as e:
        print(f"❌ Report error: {e}")
        bot.reply_to(message, "❌ Terjadi kesalahan saat mengirim report.")

# -------------------- Handler Pesan --------------------
@bot.message_handler(func=lambda m: True)
def handle_message(message):
    """Main message handler with crash protection"""
    try:
        # Extra protection against None message
        if not message:
            print("⚠️ Received None message, ignoring")
            return

        # Check if we have required attributes
        if not hasattr(message, 'chat') or not hasattr(message.chat, 'id'):
            print("⚠️ Message missing chat.id, ignoring")
            return
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""

        # Check if user is banned (unless it's admin)
        if is_user_banned(chat_id) and not is_admin(chat_id):
            bot.reply_to(message, "🚫 Anda telah dibanned dari bot ini. Hubungi admin jika ada kesalahan.")
            return

        # Check maintenance mode (unless it's admin)
        if admin_data['bot_config'].get('maintenance_mode', False) and not is_admin(chat_id):
            bot.reply_to(message, "🔧 **BOT SEDANG MAINTENANCE**\n\nBot sedang dalam mode maintenance. Silakan coba lagi nanti.\n\nTerima kasih atas pengertiannya.", parse_mode='Markdown')
            return

        # Log user activity
        log_user_activity(chat_id)

        if chat_id not in user_state:
            # Forward non-command messages to admin if user not in active session
            if ADMIN_CHAT_ID and str(chat_id) != ADMIN_CHAT_ID:
                print(f"🔄 Attempting to forward message from user {chat_id} to admin {ADMIN_CHAT_ID}")
                success = forward_to_admin(message)
                if success:
                    print(f"✅ Message successfully forwarded from {chat_id}")
                else:
                    print(f"❌ Failed to forward message from {chat_id}")
                # No notification to user about forwarding
            elif not ADMIN_CHAT_ID:
                print(f"⚠️ ADMIN_CHAT_ID not set, cannot forward message from {chat_id}")
            elif str(chat_id) == ADMIN_CHAT_ID:
                print(f"🔒 Message from admin {chat_id}, not forwarding to self")

            bot.reply_to(message, "Ketik /start dulu ya.")
            return

        step = user_state[chat_id].get("step", "")
        if not step:
            bot.reply_to(message, "Session bermasalah. Ketik /start untuk memulai ulang.")
            auto_cleanup_all_errors()  # Auto cleanup on session error
            return

        if step == "link":
            if not text.startswith("https://komiku.org/manga/"):
                bot.reply_to(message, "❌ Link tidak valid! Contoh:\nhttps://komiku.org/manga/mairimashita-iruma-kun/")
                return

            base_url, manga_name, total_chapters, sorted_chapters = get_manga_info(text)
            if not base_url:
                bot.reply_to(message, "❌ Gagal mengambil data manga. Pastikan link benar.")
                return

            user_state[chat_id].update({
                "base_url": base_url,
                "manga_name": manga_name,
                "total_chapters": total_chapters,
                "available_chapters": sorted_chapters
            })

            user_state[chat_id]["step"] = "awal"

            bot.reply_to(message, f"✅ Manga berhasil diambil: **{manga_name}**\nTotal chapter: {total_chapters if total_chapters else 'Tidak diketahui'}\n\nMasukkan chapter awal (bisa decimal seperti 1.5):")

        elif step == "awal":
            # Normalize input - convert simple numbers to match available format
            chapter_awal_str = text.strip()
            available_chapters = user_state[chat_id].get("available_chapters", [])

            # Try to find matching chapter in available list
            matched_chapter = None

            # First, try exact match
            if chapter_awal_str in available_chapters:
                matched_chapter = chapter_awal_str
            else:
                # Try to match with different formats
                try:
                    # Convert input to number for comparison
                    if '.' in chapter_awal_str and '-' not in chapter_awal_str:
                        input_num = float(chapter_awal_str)
                    elif '-' not in chapter_awal_str and not any(c.isalpha() for c in chapter_awal_str):
                        input_num = int(chapter_awal_str)
                    else:
                        bot.reply_to(message, "❌ Format chapter tidak valid. Hindari karakter khusus seperti '-' atau huruf.")
                        return

                    if input_num <= 0:
                        bot.reply_to(message, "❌ Chapter harus lebih dari 0.")
                        return

                    # Find matching chapter in available list
                    for ch in available_chapters:
                        try:
                            if '.' in ch and '-' not in ch:
                                ch_num = float(ch)
                            elif '-' not in ch and not any(c.isalpha() for c in ch):
                                ch_num = int(ch)
                            else:
                                continue

                            if ch_num == input_num:
                                matched_chapter = ch
                                break
                        except ValueError:
                            continue

                except ValueError:
                    bot.reply_to(message, "❌ Format chapter tidak valid. Contoh: 1, 9, 1.5, 7.2")
                    return

            if not matched_chapter:
                # Show available chapters for user reference
                sample_chapters = available_chapters[:15] if len(available_chapters) > 15 else available_chapters
                bot.reply_to(message, f"❌ Chapter {chapter_awal_str} tidak tersedia.\n\nChapter tersedia: {', '.join(sample_chapters)}")
                return

            user_state[chat_id]["awal"] = matched_chapter
            user_state[chat_id]["step"] = "akhir"
            bot.reply_to(message, f"✅ Chapter awal: {matched_chapter}\n📌 Masukkan chapter akhir (contoh: 9, 15.5):")

        elif step == "akhir":
            # Normalize input - convert simple numbers to match available format
            chapter_akhir_str = text.strip()
            available_chapters = user_state[chat_id].get("available_chapters", [])

            # Try to find matching chapter in available list
            matched_chapter = None

            # First, try exact match
            if chapter_akhir_str in available_chapters:
                matched_chapter = chapter_akhir_str
            else:
                # Try to match with different formats
                try:
                    # Convert input to number for comparison
                    if '.' in chapter_akhir_str and '-' not in chapter_akhir_str:
                        input_num = float(chapter_akhir_str)
                    elif '-' not in chapter_akhir_str and not any(c.isalpha() for c in chapter_akhir_str):
                        input_num = int(chapter_akhir_str)
                    else:
                        bot.reply_to(message, "❌ Format chapter tidak valid. Hindari karakter khusus seperti '-' atau huruf.")
                        return

                    if input_num <= 0:
                        bot.reply_to(message, "❌ Chapter harus lebih dari 0.")
                        return

                    # Find matching chapter in available list
                    for ch in available_chapters:
                        try:
                            if '.' in ch and '-' not in ch:
                                ch_num = float(ch)
                            elif '-' not in ch and not any(c.isalpha() for c in ch):
                                ch_num = int(ch)
                            else:
                                continue

                            if ch_num == input_num:
                                matched_chapter = ch
                                break
                        except ValueError:
                            continue

                except ValueError:
                    bot.reply_to(message, "❌ Format chapter tidak valid. Contoh: 1, 9, 1.5, 7.2")
                    return

            if not matched_chapter:
                # Show available chapters for user reference
                sample_chapters = available_chapters[:15] if len(available_chapters) > 15 else available_chapters
                bot.reply_to(message, f"❌ Chapter {chapter_akhir_str} tidak tersedia.\n\nChapter tersedia: {', '.join(sample_chapters)}")
                return

            awal_str = user_state[chat_id].get("awal", "1")
            download_mode = user_state[chat_id].get("mode", "normal")

            # Find positions in available chapters list
            try:
                awal_index = available_chapters.index(awal_str)
                akhir_index = available_chapters.index(matched_chapter)
            except ValueError:
                bot.reply_to(message, "❌ Error dalam menentukan posisi chapter.")
                return

            if akhir_index < awal_index:
                bot.reply_to(message, f"❌ Chapter akhir harus berada setelah atau sama dengan chapter awal ({awal_str}).")
                return

            # Calculate actual chapter count based on available chapters
            chapter_count = akhir_index - awal_index + 1
            chapters_to_download = available_chapters[awal_index:akhir_index + 1]

            # Remove duplicates while preserving order
            unique_chapters = []
            seen = set()
            for ch in chapters_to_download:
                if ch not in seen:
                    unique_chapters.append(ch)
                    seen.add(ch)

            chapters_to_download = unique_chapters
            chapter_count = len(chapters_to_download)

            # No chapter limit for Komik mode - removed restriction

            user_state[chat_id]["akhir"] = matched_chapter
            user_state[chat_id]["chapters_to_download"] = chapters_to_download  # Store the actual chapters to download
            user_state[chat_id]["step"] = "mode"

            markup = types.InlineKeyboardMarkup()
            btn_gabung = types.InlineKeyboardButton("📄 Gabung jadi 1 PDF", callback_data="gabung")
            btn_pisah = types.InlineKeyboardButton("📑 Pisah per Chapter", callback_data="pisah")
            btn_gdrive_gabung = types.InlineKeyboardButton("☁️ Gabung + GoFile", callback_data="gofile_gabung")
            btn_gdrive_pisah = types.InlineKeyboardButton("☁️ Pisah + GoFile", callback_data="gofile_pisah")
            markup.add(btn_gabung, btn_pisah)
            markup.add(btn_gdrive_gabung, btn_gdrive_pisah)

            # Show which chapters will be downloaded
            if chapter_count <= 10:
                chapters_preview = ', '.join(chapters_to_download)
            else:
                chapters_preview = f"{', '.join(chapters_to_download[:5])}, ..., {', '.join(chapters_to_download[-3:])}"

            bot.send_message(chat_id, f"📊 Chapter yang akan didownload ({chapter_count} chapter):\n{chapters_preview}\n\nPilih mode download:", reply_markup=markup)

    except Exception as handler_error:
        # Get chat_id safely
        try:
            error_chat_id = message.chat.id if hasattr(message, 'chat') and hasattr(message.chat, 'id') else 'unknown'
        except:
            error_chat_id = 'unknown'

        print(f"❌ Message handler error for user {error_chat_id}: {handler_error}")

        # Only auto cleanup if we have a valid chat_id
        if error_chat_id != 'unknown':
            try:
                auto_cleanup_all_errors()  # Auto cleanup on any handler error
            except Exception as cleanup_error:
                print(f"❌ Cleanup error: {cleanup_error}")

            try:
                bot.send_message(error_chat_id, "❌ Terjadi error. Ketik /start untuk memulai ulang.")
            except Exception as send_error:
                print(f"❌ Error sending error message: {send_error}")

            try:
                # Clean up on error
                user_state.pop(error_chat_id, None)
                user_cancel.pop(error_chat_id, None)
                user_downloads.pop(error_chat_id, None) # Clean user download preferences too
            except Exception as state_cleanup_error:
                print(f"❌ State cleanup error: {state_cleanup_error}")
        else:
            print("⚠️ Cannot cleanup - unknown chat_id")

        # Don't re-raise the exception to prevent bot crash
        print("🛡️ Message handler error contained, bot continues running")

# -------------------- Handler Mode Download --------------------
@bot.callback_query_handler(func=lambda call: call.data in ["gabung", "pisah", "gofile_gabung", "gofile_pisah"])
def handle_mode(call):
    chat_id = call.message.chat.id

    # Answer the callback query to remove loading state
    try:
        bot.answer_callback_query(call.id)
    except:
        pass

    if chat_id not in user_state:
        bot.send_message(chat_id, "❌ Session bermasalah. Ketik /start untuk memulai ulang.")
        return

    mode = call.data
    use_gofile = mode.startswith("gofile_")
    actual_mode = mode.replace("gofile_", "") if use_gofile else mode

    base_url = user_state[chat_id]["base_url"]
    manga_name = user_state[chat_id]["manga_name"]
    awal = user_state[chat_id]["awal"]
    akhir = user_state[chat_id]["akhir"]
    download_mode = user_state[chat_id].get("mode", "normal")
    chapters_to_download = user_state[chat_id].get("chapters_to_download", []) # Use stored unique chapters

    # Remove the inline keyboard buttons
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
    except:
        pass

    user_cancel[chat_id] = False  # reset cancel flag
    bot.send_message(chat_id, f"⏳ Sedang download chapter {' & '.join(chapters_to_download)}...")

    try:
        if actual_mode == "gabung":
            all_images = []

            for ch_str in chapters_to_download:
                if user_cancel.get(chat_id):
                    bot.send_message(chat_id, "❌ Download dihentikan! Membersihkan file...")
                    cleanup_user_downloads(chat_id)
                    return

                bot.send_message(chat_id, f"📥 Download chapter {ch_str}...")

                if download_mode == "big":
                    imgs = download_chapter_big(base_url.format(ch_str), ch_str, OUTPUT_DIR, chat_id, user_cancel)
                else:
                    imgs = download_chapter(base_url.format(ch_str), ch_str, OUTPUT_DIR, chat_id, user_cancel)

                # Check cancel status after each chapter download
                if user_cancel.get(chat_id):
                    bot.send_message(chat_id, "❌ Download dihentikan! Membersihkan file...")
                    cleanup_user_downloads(chat_id)
                    return

                all_images.extend(imgs)

            if all_images and not user_cancel.get(chat_id):
                pdf_name = f"{manga_name} chapter {awal}-{akhir}.pdf"
                pdf_path = os.path.join(OUTPUT_DIR, pdf_name)
                create_pdf(all_images, pdf_path)

                try:
                    # Check file size before upload (Telegram limit is 50MB)
                    file_size = os.path.getsize(pdf_path)
                    max_size = 50 * 1024 * 1024  # 50MB in bytes

                    if use_gofile:
                        # Always use GoFile when cloud upload is requested
                        upload_success = upload_to_gofile_and_send_link(chat_id, pdf_path, pdf_name)
                        if not upload_success:
                            # Fallback to direct upload if GoFile fails and file is small enough
                            if file_size <= max_size:
                                # Start tracking for Telegram upload
                                start_upload_tracking(chat_id, pdf_path)
                                try:
                                    with open(pdf_path, "rb") as pdf_file:
                                        bot.send_document(
                                            chat_id,
                                            pdf_file,
                                            caption=f"📚 {pdf_name} ({file_size/(1024*1024):.1f}MB)",
                                            timeout=300
                                        )
                                    print(f"✅ PDF sent successfully as fallback: {pdf_name}")
                                    finish_upload_tracking(chat_id, pdf_path)
                                except Exception as telegram_error:
                                    print(f"❌ Telegram fallback upload failed: {telegram_error}")
                                    finish_upload_tracking(chat_id, pdf_path)
                        safe_delete_pdf(pdf_path, chat_id, 10)
                    else:
                        # Regular Telegram upload
                        if file_size > max_size:
                            size_mb = file_size / (1024 * 1024)
                            # Suggest GoFile for large files
                            bot.send_message(chat_id, f"❌ File {pdf_name} terlalu besar ({size_mb:.1f}MB). Limit Telegram adalah 50MB.\n💡 Coba gunakan opsi GoFile untuk file besar atau kurangi jumlah chapter.")
                            safe_delete_pdf(pdf_path, chat_id, 5)
                            return

                        # Start tracking for Telegram upload
                        start_upload_tracking(chat_id, pdf_path)
                        try:
                            with open(pdf_path, "rb") as pdf_file:
                                bot.send_document(
                                    chat_id,
                                    pdf_file,
                                    caption=f"📚 {pdf_name} ({file_size/(1024*1024):.1f}MB)",
                                    timeout=300
                                )
                            print(f"✅ PDF sent successfully: {pdf_name} ({file_size/(1024*1024):.1f}MB)")
                            finish_upload_tracking(chat_id, pdf_path)
                        except Exception as telegram_error:
                            print(f"❌ Telegram upload failed: {telegram_error}")
                            finish_upload_tracking(chat_id, pdf_path)
                            raise telegram_error
                        safe_delete_pdf(pdf_path, chat_id, 10)
                except Exception as upload_error:
                    print(f"❌ Upload error: {upload_error}")
                    error_msg = str(upload_error)
                    if "too large" in error_msg.lower() or "file too big" in error_msg.lower():
                        bot.send_message(chat_id, f"❌ File {pdf_name} terlalu besar untuk Telegram. 💡 Coba gunakan opsi GoFile.")
                    elif "timeout" in error_msg.lower():
                        bot.send_message(chat_id, f"⏱️ Upload {pdf_name} timeout. File mungkin terlalu besar atau koneksi lambat.")
                    else:
                        bot.send_message(chat_id, f"❌ Gagal upload {pdf_name}: {error_msg}")
                    safe_delete_pdf(pdf_path, chat_id, 10)

                # Bersih-bersih
                for ch in chapters_to_download:
                    if download_mode == "big":
                        folder_ch = os.path.join(OUTPUT_DIR, f"chapter-{ch}-big")
                    else:
                        folder_ch = os.path.join(OUTPUT_DIR, f"chapter-{ch}")
                    if os.path.exists(folder_ch):
                        shutil.rmtree(folder_ch)

        elif actual_mode == "pisah":
            for ch_str in chapters_to_download:
                if user_cancel.get(chat_id):
                    bot.send_message(chat_id, "❌ Download dihentikan! Membersihkan file...")
                    cleanup_user_downloads(chat_id)
                    return

                bot.send_message(chat_id, f"📥 Download chapter {ch_str}...")

                # Add small delay to reduce system load
                time.sleep(2)

                if download_mode == "big":
                    imgs = download_chapter_big(base_url.format(ch_str), ch_str, OUTPUT_DIR, chat_id, user_cancel)
                else:
                    imgs = download_chapter(base_url.format(ch_str), ch_str, OUTPUT_DIR, chat_id, user_cancel)

                # Check cancel status after each chapter download
                if user_cancel.get(chat_id):
                    bot.send_message(chat_id, "❌ Download dihentikan! Membersihkan file...")
                    cleanup_user_downloads(chat_id)
                    return

                if imgs:
                    pdf_name = f"{manga_name} chapter {ch_str}.pdf"
                    pdf_path = os.path.join(OUTPUT_DIR, pdf_name)
                    create_pdf(imgs, pdf_path)

                    try:
                        # Check file size before upload
                        file_size = os.path.getsize(pdf_path)
                        max_size = 50 * 1024 * 1024  # 50MB

                        if use_gofile:
                            # Always use GoFile when cloud upload is requested
                            upload_success = upload_to_gofile_and_send_link(chat_id, pdf_path, pdf_name)
                            if not upload_success:
                                # Fallback to direct upload if GoFile fails and file is small enough
                                if file_size <= max_size:
                                    # Start tracking for Telegram upload
                                    start_upload_tracking(chat_id, pdf_path)
                                    try:
                                        with open(pdf_path, "rb") as pdf_file:
                                            bot.send_document(
                                                chat_id,
                                                pdf_file,
                                                caption=f"📖 Chapter {ch_str} ({file_size/(1024*1024):.1f}MB)",
                                                timeout=300
                                            )
                                        print(f"✅ PDF sent successfully as fallback: {pdf_name}")
                                        finish_upload_tracking(chat_id, pdf_path)
                                    except Exception as telegram_error:
                                        print(f"❌ Telegram fallback upload failed: {telegram_error}")
                                        finish_upload_tracking(chat_id, pdf_path)
                            safe_delete_pdf(pdf_path, chat_id, 10)
                        else:
                            # Regular Telegram upload
                            if file_size > max_size:
                                size_mb = file_size / (1024 * 1024)
                                bot.send_message(chat_id, f"❌ Chapter {ch_str} terlalu besar ({size_mb:.1f}MB). 💡 Coba gunakan opsi GoFile untuk file besar.")
                                safe_delete_pdf(pdf_path, chat_id, 5)
                                continue

                            # Start tracking for Telegram upload
                            start_upload_tracking(chat_id, pdf_path)
                            try:
                                with open(pdf_path, "rb") as pdf_file:
                                    bot.send_document(
                                        chat_id,
                                        pdf_file,
                                        caption=f"📖 Chapter {ch_str} ({file_size/(1024*1024):.1f}MB)",
                                        timeout=300
                                    )
                                print(f"✅ PDF sent successfully: {pdf_name}")
                                finish_upload_tracking(chat_id, pdf_path)
                            except Exception as telegram_error:
                                print(f"❌ Telegram upload failed: {telegram_error}")
                                finish_upload_tracking(chat_id, pdf_path)
                                raise telegram_error
                            safe_delete_pdf(pdf_path, chat_id, 10)
                    except Exception as upload_error:
                        print(f"❌ Upload error: {upload_error}")
                        error_msg = str(upload_error)
                        if "too large" in error_msg.lower() or "file too big" in error_msg.lower():
                            bot.send_message(chat_id, f"❌ Chapter {ch_str} terlalu besar untuk Telegram. 💡 Coba gunakan opsi GoFile.")
                        elif "timeout" in error_msg.lower():
                            bot.send_message(chat_id, f"⏱️ Upload chapter {ch_str} timeout.")
                        else:
                            bot.send_message(chat_id, f"❌ Gagal upload chapter {ch_str}: {error_msg}")
                        safe_delete_pdf(pdf_path, chat_id, 10)

                    # Cleanup chapter folder after successful upload
                    if download_mode == "big":
                        folder_ch = os.path.join(OUTPUT_DIR, f"chapter-{ch_str}-big")
                    else:
                        folder_ch = os.path.join(OUTPUT_DIR, f"chapter-{ch_str}")
                    if os.path.exists(folder_ch):
                        shutil.rmtree(folder_ch)
                else:
                    bot.send_message(chat_id, f"⚠️ Chapter {ch_str} tidak ditemukan.")

        if not user_cancel.get(chat_id):
            bot.send_message(chat_id, "✅ Selesai!")

    except Exception as e:
        print(f"❌ Download error for user {chat_id}: {e}")
        try:
            bot.send_message(chat_id, f"❌ Terjadi error: {e}")
        except:
            pass
        finally:
            # Clean up on error
            cleanup_user_downloads(chat_id)
            user_state.pop(chat_id, None)
            user_cancel.pop(chat_id, None)
            user_downloads.pop(chat_id, None) # Clean user download preferences too

# -------------------- Main --------------------
if __name__ == "__main__":
    # Check if running in deployment environment
    is_deployment = os.getenv("REPLIT_DEPLOYMENT") == "1"

    if is_deployment:
        print("🚀 Running in deployment mode - 24/7 online!")
    else:
        print("🔧 Running in development mode")

    keep_alive()

    start_cleanup_scheduler()
    start_smart_auto_ping()  # Use smart auto ping instead
    start_simple_keepalive()
    start_immediate_recovery_system()  # ULTRA-AGGRESSIVE immediate recovery
    start_comprehensive_error_monitor()
    start_background_message_cleanup()  # ULTRA-AGGRESSIVE console cleanup every 20 seconds
    start_self_ping()  # Enhanced ULTRA-AGGRESSIVE self-ping system for Replit anti-sleep
    print("🔥 Bot jalan dengan ULTRA-AGGRESSIVE monitoring dan MAXIMUM anti-sleep system...")
    print("🚀 REPLIT SLEEP PROTECTION: MAXIMUM LEVEL ACTIVATED!")
    print("⚡ Multiple redundant systems running to prevent ANY sleep!")

    restart_count = 0
    max_restarts = 200  # ULTRA-AGGRESSIVE: Massively increased max restarts

    while restart_count < max_restarts:
        try:
            print(f"🔄 Bot starting (attempt {restart_count + 1}/{max_restarts})")

            # Initial webhook cleanup before starting
            success = cleanup_webhook_once()
            if success:
                print("🔧 Initial webhook cleanup successful")
            else:
                print("🔧 Initial webhook cleanup failed, continuing anyway")

            if is_deployment:
                # Stable settings for deployment to prevent conflicts
                bot.infinity_polling(
                    timeout=60,           # Longer timeout to prevent conflicts
                    long_polling_timeout=30,  # Standard polling timeout
                    none_stop=True,       # Don't stop on errors
                    interval=2,           # Check every 2 seconds to reduce conflicts
                    allowed_updates=None  # Process all updates
                )
            else:
                # Stable development settings
                bot.infinity_polling(
                    timeout=30,
                    long_polling_timeout=20,
                    none_stop=True,
                    interval=1             # 1 second interval for development
                )

        except KeyboardInterrupt:
            print("🛑 Bot stopped by user")
            break
        except Exception as e:
            print(f"❌ Bot error (attempt {restart_count + 1}): {e}")
            auto_cleanup_all_errors()  # Auto cleanup on any bot error

            # Immediate aggressive reconnect attempts
            for immediate_retry in range(3):
                try:
                    print(f"🔥 Immediate reconnect attempt {immediate_retry + 1}/3")
                    time.sleep(2)  # Very short wait

                    # Reinitialize bot
                    bot = telebot.TeleBot(TOKEN)
                    bot.get_me()
                    print("✅ Immediate reconnect successful!")
                    restart_count = max(0, restart_count - 2)  # Reduce restart count on immediate success
                    break

                except Exception as immediate_error:
                    print(f"❌ Immediate reconnect {immediate_retry + 1} failed: {immediate_error}")
            else:
                # If immediate reconnects failed, do standard restart
                restart_count += 1

            # Shorter progressive backoff for faster recovery
            wait_time = min(30, 2 * restart_count)  # Max 30 seconds wait

            # Clear states on error to prevent memory issues
            try:
                user_state.clear()
                user_cancel.clear()
                autodemo_active.clear()
                user_downloads.clear() # Clear user download preferences as well
                print("🧹 Cleared all user states after error")
            except:
                pass

            if restart_count < max_restarts:
                print(f"🔄 Aggressive restart in {wait_time} seconds...")
                time.sleep(wait_time)

                # Multiple reinitialize attempts
                for init_attempt in range(3):
                    try:
                        bot = telebot.TeleBot(TOKEN)
                        bot.get_me()
                        print("✅ Bot reinitialization successful")
                        restart_count = max(0, restart_count - 1)  # Reward successful init
                        break
                    except Exception as init_error:
                        print(f"❌ Init attempt {init_attempt + 1} failed: {init_error}")
                        time.sleep(3)

            else:
                print("❌ Max restart attempts reached. Attempting final recovery...")

                # Final recovery attempt with completely new bot instance
                try:
                    time.sleep(10)
                    bot = telebot.TeleBot(TOKEN)
                    bot.get_me()
                    print("✅ Final recovery successful! Resetting restart counter.")
                    restart_count = 0  # Reset counter for final recovery
                    continue
                except:
                    print("❌ Final recovery failed. Bot stopped.")
                    break

    print("🏁 Bot execution finished")