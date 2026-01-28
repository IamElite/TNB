
# ==========================================
# 🎌 ANIME AUTO DOWNLOADER v12.0 - MULTI-USER SAFE
# By RJ - Production Ready
# ==========================================

import os, time, glob, asyncio, base64, re, requests, uuid, sys
import subprocess, json, pickle, psutil, shutil
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import nest_asyncio
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from collections import deque
import warnings
import traceback

warnings.filterwarnings("ignore")
nest_asyncio.apply()

# ✅ FIX: Cache bot username
BOT_USERNAME = None

# ==========================================
# CONFIG
# ==========================================
# ==========================================
# 🔐 BOT CREDENTIALS
# ==========================================
API_ID = int(os.environ.get("API_ID", 23200475))
API_HASH = os.environ.get("API_HASH", "644e1d9e8028a5295d6979bb3a36b23b")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "7810985319:AAHSaD-YlHThm2JPoOY_vnJLs9jXpaWs4ts")
OWNER_ID = int(os.environ.get("OWNER_ID", 7074383232))

# ==========================================
# 📁 DIRECTORY PATHS
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
THUMB_DIR = os.path.join(BASE_DIR, "thumbnails")
DB_DIR = os.path.join(BASE_DIR, "database")
WORKER_DIR = os.path.join(BASE_DIR, "workers")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")

DB_FILE = os.path.join(DB_DIR, "main_db.pkl")

# ==========================================
# 🔢 LIMITS & WORKERS
# ==========================================
MAX_BATCH = 15                      # Max episodes per batch download
MAX_QUEUE = 100                     # Max tasks in queue
MIN_INTERVAL = 2                    # Min monitor check interval (minutes)
MAX_INTERVAL = 60                   # Max monitor check interval (minutes)

MAX_DOWNLOAD_WORKERS = 11           # Parallel download workers
MAX_UPLOAD_PARALLEL = 12            # Parallel upload slots
MAX_BATCH_WORKERS = 5               # Batch processing workers
TOTAL_WORKERS = MAX_DOWNLOAD_WORKERS + 1

MAX_FILE_SIZE = 1.9 * 1024 * 1024 * 1024    # 1.9GB - Telegram limit
MAX_ZIP_SIZE = 1.9 * 1024 * 1024 * 1024     # 1.9GB - Backup ZIP limit

# ==========================================
# ⏱️ TIMING SETTINGS
# ==========================================
COOLDOWN_TIME = 25                  # Seconds - User request cooldown
PROGRESS_UPDATE_INTERVAL = 3        # Seconds - Progress update frequency
FILE_CLEANUP_TIME = 50              # Seconds - Delete files after upload
THUMB_INACTIVE_DAYS = 30            # Days - Auto delete unused thumbnails
WORKER_STUCK_TIME = 9000            # Seconds - Kill stuck workers (2.5 hours)

# ==========================================
# 📦 BACKUP SETTINGS
# ==========================================
BACKUP_CHANNEL = -1002932260531
BACKUP_HOUR = 8                     # 8 UTC = 1:30 PM India Time
BACKUP_MINUTE = 0                   # 0 Minute

user_cooldowns = {}

for d in [DOWNLOAD_DIR, THUMB_DIR, DB_DIR, WORKER_DIR, BACKUP_DIR]:
    os.makedirs(d, exist_ok=True)

# ==========================================
# DATABASE
# ==========================================
db = {
    'owner_id': OWNER_ID,
    'admins': set(),
    'banned': set(),
    'users': {},
    'monitored': {},
    'thumbnails': {},
    'thumb_last_used': {},
    'settings': {'default_interval': 3},
    'premium_users': {},
    'captions': {},
    'referrals': {},
    'banned_ref_codes': set(),
    'master_unlocked': set()  # ✅ NEW LINE
}

task_queue = deque(maxlen=MAX_QUEUE)
active_tasks = {}
active_downloads = {}
worker_status = {}
download_semaphore = None
upload_semaphore = None
batch_semaphore = None
queue_lock = None

_sf = False
_rf = False

def load_db():
    global db
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, 'rb') as f:
                loaded = pickle.load(f)
                db.update(loaded)
        if db['owner_id'] is None:
            db['owner_id'] = OWNER_ID
        if 'thumb_last_used' not in db:
            db['thumb_last_used'] = {}
    except Exception as e:
        print(f"DB Load Error: {e}")

def save_db():
    try:
        with open(DB_FILE, 'wb') as f:
            pickle.dump(db, f)
    except Exception as e:
        print(f"DB Save Error: {e}")

# ==========================================
# TASK MANAGER
# ==========================================
class Task:
    def __init__(self, user_id, chat_id, content_type, content_key, url):
        self.task_id = f"task_{user_id}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        self.user_id = user_id
        self.chat_id = chat_id
        self.content_type = content_type
        self.content_key = content_key
        self.url = url
        self.directory = os.path.join(WORKER_DIR, self.task_id)
        self.files = []
        self.status = "pending"
        self.created_at = time.time()
        self.subscribers = [{'user_id': user_id, 'chat_id': chat_id}]
        self.series_name = "Unknown"
        self.episode = 0
        self.is_movie = False

        # ✅ NEW - Batch & Progress tracking
        self.batch_mode = False
        self.current_episode = 0
        self.total_episodes = 0
        self.swift_url = None
        self.available_qualities = []

        os.makedirs(self.directory, exist_ok=True)

    def add_subscriber(self, user_id, chat_id):
        if not any(s['user_id'] == user_id for s in self.subscribers):
            self.subscribers.append({'user_id': user_id, 'chat_id': chat_id})

    def cleanup(self):
        try:
            if os.path.exists(self.directory):
                shutil.rmtree(self.directory)
        except:
            pass

def get_content_key(url, series_name=None, episode=None):
    if "/movies/" in url:
        match = re.search(r'/movies/([^/]+)', url)
        return f"movie_{match.group(1)}" if match else f"movie_{hash(url)}"
    elif "/episode/" in url:
        match = re.search(r'/episode/([^/]+)', url)
        return f"episode_{match.group(1)}" if match else f"ep_{hash(url)}"
    elif "/series/" in url and episode:
        match = re.search(r'/series/([^/]+)', url)
        key = match.group(1) if match else hash(url)
        return f"series_{key}_ep{episode}"
    elif "swift.multiquality" in url:
        return f"swift_{hash(url)}"
    return f"url_{hash(url)}"

def add_to_queue(task_type, task, status, episodes=None):
    if len(task_queue) >= MAX_QUEUE:
        return False, "Queue full"

    item = {'type': task_type, 'task': task, 'status': status}
    if episodes:
        item['episodes'] = episodes

    task_queue.append(item)
    active_downloads[task.task_id] = task

    return True, f"Queued (Position: {len(task_queue)})"

# ==========================================
# COOLDOWN SYSTEM
# ==========================================
def check_cooldown(user_id):
    if user_id in user_cooldowns:
        elapsed = time.time() - user_cooldowns[user_id]
        if elapsed < COOLDOWN_TIME:
            return False, int(COOLDOWN_TIME - elapsed)
    return True, 0

def update_cooldown(user_id):
    user_cooldowns[user_id] = time.time()

async def cooldown_check(m):
    uid = m.from_user.id
    if is_owner(uid) or is_admin(uid):
        return True
    can_proceed, remaining = check_cooldown(uid)
    if not can_proceed:
        await m.reply(f"⏳ **Cooldown Active**\n\nPlease wait **{remaining} seconds**")
        return False
    return True

# ==========================================
# HELPERS
# ==========================================

# ✅ ADD THIS NEW FUNCTION (Pehle se existing helpers ke baad)
def safe_request(url, max_retries=3, timeout=20):
    """Safe request with retry mechanism for connection errors"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                verify=False
            )
            response.raise_for_status()
            return response
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                ConnectionResetError) as e:
            print(f"⚠️ Request attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))  # 2s, 4s, 6s delay
                continue
            raise e
        except Exception as e:
            print(f"❌ Request error: {e}")
            raise e
    return None

def fmt_bytes(s):
    if s < 1024: return f"{s} B"
    elif s < 1024**2: return f"{s/1024:.2f} KB"
    elif s < 1024**3: return f"{s/1024**2:.2f} MB"
    return f"{s/1024**3:.2f} GB"

def fmt_time(s):
    if s < 0 or s > 86400: return "∞"
    elif s < 60: return f"{int(s)}s"
    elif s < 3600: return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s//3600)}h {int((s%3600)//60)}m"

def progress_bar(p, l=15):
    f = int(l * p / 100)
    return f"[{'█'*f}{'░'*(l-f)}]"

def clean_name(n):
    c = re.sub(r'\[.*?\]_?', '', n)
    return re.sub(r'\s+', ' ', c).strip()

# ===== YEH FUNCTION MILA =====
def get_quality(n):
    n = n.lower()
    if "1080" in n: return "1080P FHD"
    if "720" in n: return "720P HD"
    if "480" in n: return "480P SD"
    if "360" in n: return "360P SD"
    return "Unknown"

# ===== YEH FUNCTION get_quality KE BAAD ADD KARO =====

def clean_unwanted_tags(filename):
    """Remove [RTI], RareToonsIndia, Toono etc from filename"""
    patterns = [
        r'\[RTI\]',
        r'\[RareToonsIndia\]',
        r'RareToonsIndia',
        r'\[Toono\]',
        r'\[toono\]',
        r'Toono\.in',
        r'toono\.in',
        r'Toono',
        r'toono',
        r'\[HindiDub\]',
        r'\[Hindi\s*Dub\]',
        r'\[Dubbed\]',
    ]
    result = filename
    for pattern in patterns:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    
    # Clean up extra spaces, underscores, brackets
    result = re.sub(r'_+', '_', result)
    result = re.sub(r'\s+', ' ', result)
    result = re.sub(r'_\s', '_', result)
    result = re.sub(r'\s_', '_', result)
    result = re.sub(r'\[\s*\]', '', result)  # Empty brackets remove
    result = result.strip('_ ')
    return result


def get_audio_language(video_path):
    """Get audio language from video file using ffprobe"""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path],
            capture_output=True, text=True, timeout=30
        )
        data = json.loads(result.stdout)
        
        languages = []
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'audio':
                tags = stream.get('tags', {})
                lang = tags.get('language', tags.get('LANGUAGE', ''))
                title = tags.get('title', tags.get('TITLE', ''))
                
                # Check title for language hints
                title_lower = title.lower() if title else ''
                
                if lang:
                    lang_map = {
                        'hin': 'Hindi', 'hindi': 'Hindi', 'hi': 'Hindi',
                        'eng': 'English', 'english': 'English', 'en': 'English',
                        'jpn': 'Japanese', 'japanese': 'Japanese', 'ja': 'Japanese', 'jp': 'Japanese',
                        'tam': 'Tamil', 'tamil': 'Tamil', 'ta': 'Tamil',
                        'tel': 'Telugu', 'telugu': 'Telugu', 'te': 'Telugu',
                        'kor': 'Korean', 'korean': 'Korean', 'ko': 'Korean',
                        'chi': 'Chinese', 'chinese': 'Chinese', 'zh': 'Chinese',
                        'ben': 'Bengali', 'bengali': 'Bengali', 'bn': 'Bengali',
                        'mar': 'Marathi', 'marathi': 'Marathi', 'mr': 'Marathi',
                        'und': 'Unknown',
                    }
                    lang_name = lang_map.get(lang.lower(), lang.capitalize())
                    if lang_name not in languages and lang_name != 'Unknown':
                        languages.append(lang_name)
                
                # Also check title for language info
                if 'hindi' in title_lower and 'Hindi' not in languages:
                    languages.append('Hindi')
                elif 'english' in title_lower and 'English' not in languages:
                    languages.append('English')
                elif 'japanese' in title_lower and 'Japanese' not in languages:
                    languages.append('Japanese')
        
        if languages:
            return ', '.join(languages)
        
        # Fallback: Try to detect from filename
        filename = os.path.basename(video_path).lower()
        if 'hindi' in filename or 'hin' in filename:
            return 'Hindi'
        elif 'english' in filename or 'eng' in filename:
            return 'English'
        elif 'japanese' in filename or 'jpn' in filename:
            return 'Japanese'
        elif 'dual' in filename:
            return 'Dual Audio'
        elif 'multi' in filename:
            return 'Multi Audio'
        
        return "Unknown"
        
    except Exception as e:
        print(f"Language detection error: {e}")
        return "Unknown"


def apply_custom_caption(template, file_name, season, episode, language, quality, size_str, duration_str):
    """Apply custom caption template with variables"""
    caption = template
    
    # Replace variables (case insensitive for flexibility)
    # {f} or {F} = File name
    caption = re.sub(r'\{f\}', file_name, caption, flags=re.IGNORECASE)
    
    # {s} or {S} = Season (when alone, not {sz})
    caption = re.sub(r'\{s\}(?!z)', str(season) if season else 'N/A', caption, flags=re.IGNORECASE)
    
    # {e} or {E} = Episode
    caption = re.sub(r'\{e\}', str(episode) if episode else 'N/A', caption, flags=re.IGNORECASE)
    
    # {l} or {L} = Language
    caption = re.sub(r'\{l\}', language, caption, flags=re.IGNORECASE)
    
    # {q} or {Q} = Quality
    caption = re.sub(r'\{q\}', quality, caption, flags=re.IGNORECASE)
    
    # {sz} or {SZ} = Size
    caption = re.sub(r'\{sz\}', size_str, caption, flags=re.IGNORECASE)
    
    # {d} or {D} = Duration
    caption = re.sub(r'\{d\}', duration_str, caption, flags=re.IGNORECASE)
    
    return caption

# ==========================================
# REFERRAL SYSTEM HELPERS
# ==========================================
def get_referral_code(user_id):
    """Get or create referral code for user"""
    if user_id not in db['referrals']:
        db['referrals'][user_id] = {
            'code': f'RJ_{user_id}',
            'referred_by': None,
            'current_cycle': 1,
            'cycle_refs': [],
            'total_referrals': 0,
            'joined_date': datetime.now().isoformat()
        }
        save_db()
    return db['referrals'][user_id]['code']


def process_referral(new_user_id, referrer_code):
    """Process referral and give rewards"""
    # Extract referrer ID from code
    try:
        referrer_id = int(referrer_code.replace('RJ_', ''))
    except:
        return False, "Invalid code"
    
    # Check if code is banned
    if referrer_code in db.get('banned_ref_codes', set()):
        return False, "Code banned"
    
    # Check if referrer exists
    if referrer_id not in db['referrals']:
        return False, "Referrer not found"
    
    # Check if new user already referred
    if new_user_id in db['referrals'] and db['referrals'][new_user_id].get('referred_by'):
        return False, "Already referred"
    
    # Check self-referral
    if new_user_id == referrer_id:
        return False, "Self referral not allowed"
    
    # Initialize new user if not exists
    if new_user_id not in db['referrals']:
        db['referrals'][new_user_id] = {
            'code': f'RJ_{new_user_id}',
            'referred_by': referrer_id,
            'current_cycle': 1,
            'cycle_refs': [],
            'total_referrals': 0,
            'joined_date': datetime.now().isoformat()
        }
    else:
        db['referrals'][new_user_id]['referred_by'] = referrer_id
    
    # Add to referrer's cycle
    ref_data = db['referrals'][referrer_id]
    ref_data['cycle_refs'].append(new_user_id)
    ref_data['total_referrals'] += 1
    
    cycle_count = len(ref_data['cycle_refs'])
    
    # Calculate rewards
    if cycle_count == 1:
        referrer_days = 1  # 24h
    elif cycle_count == 2:
        referrer_days = 3
    elif cycle_count >= 3:
        referrer_days = 7
        # Reset cycle after 3rd referral reward claimed
        ref_data['current_cycle'] += 1
        ref_data['cycle_refs'] = []
    else:
        referrer_days = 0
    
    new_user_days = 1  # Always 24h for new user
    
    # Give premium to both
    if referrer_days > 0:
        add_referral_premium(referrer_id, referrer_days)
    add_referral_premium(new_user_id, new_user_days)
    
    save_db()
    return True, {'referrer_days': referrer_days, 'new_user_days': new_user_days, 'cycle_count': cycle_count}


def add_referral_premium(user_id, days):
    """Add premium days from referral"""
    if 'premium_users' not in db:
        db['premium_users'] = {}
    
    current_time = time.time()
    
    if user_id in db['premium_users']:
        current_expiry = db['premium_users'][user_id].get('expires', current_time)
        if current_expiry > current_time:
            # Replace if referral reward is better
            new_expiry = current_time + (days * 86400)
            if new_expiry > current_expiry:
                db['premium_users'][user_id]['expires'] = new_expiry
        else:
            # Expired, give fresh
            db['premium_users'][user_id]['expires'] = current_time + (days * 86400)
    else:
        db['premium_users'][user_id] = {
            'expires': current_time + (days * 86400),
            'granted_by': 'referral',
            'granted_on': current_time
        }
    save_db()


def get_referral_stats(user_id):
    """Get user's referral statistics"""
    if user_id not in db['referrals']:
        return None
    
    data = db['referrals'][user_id]
    return {
        'code': data['code'],
        'total_refs': data['total_referrals'],
        'current_cycle': data['current_cycle'],
        'cycle_refs': len(data.get('cycle_refs', [])),
        'referred_by': data.get('referred_by')
    }


# ==========================================
# BACKUP SYSTEM HELPERS
# ==========================================
async def create_backup():
    """Create backup of premium users data"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        
        # Collect premium user IDs
        premium_uids = set(db.get('premium_users', {}).keys())
        admin_uids = db.get('admins', set())
        owner_uid = {db.get('owner_id')} if db.get('owner_id') else set()
            # Sabko set bana kar merge kiya
        all_important_uids = set(premium_uids) | set(admin_uids) | (owner_uid if isinstance(owner_uid, set) else {owner_uid})


        
        # Prepare backup data
        backup_data = {
            'backup_time': timestamp,
            'bot_version': '12.0',
            'owner_id': db.get('owner_id'),
            'admins': list(db.get('admins', set())),
            'banned': list(db.get('banned', set())),
            'premium_users': db.get('premium_users', {}),
            'referrals': db.get('referrals', {}),  # ALL referrals
            'banned_ref_codes': list(db.get('banned_ref_codes', set())),
            'monitored': {},
            'thumbnails': {},
            'captions': {},
            'users': {}
        }
        
        # Filter monitored for premium users
        for key, data in db.get('monitored', {}).items():
            premium_subs = [s for s in data['subscribers'] if s['user_id'] in all_important_uids]
            if premium_subs:
                backup_data['monitored'][key] = data.copy()
                backup_data['monitored'][key]['subscribers'] = premium_subs
        
        # Filter thumbnails for premium users
        for uid, thumbs in db.get('thumbnails', {}).items():
            if uid in all_important_uids:
                backup_data['thumbnails'][uid] = thumbs
        
        # Filter captions for premium users
        for uid, caption in db.get('captions', {}).items():
            if uid in all_important_uids:
                backup_data['captions'][uid] = caption
        
        # Filter users for premium users
        for uid, udata in db.get('users', {}).items():
            if uid in all_important_uids:
                backup_data['users'][uid] = udata
        
        # Calculate stats
        stats = {
            'premium_users': len(premium_uids),
            'total_monitored': len(backup_data['monitored']),
            'total_thumbnails': sum(len(t) for t in backup_data['thumbnails'].values()),
            'total_captions': len(backup_data['captions']),
            'total_referrals': sum(r.get('total_referrals', 0) for r in db.get('referrals', {}).values())
        }
        
        # Create backup files
        backup_files = []
        
        # Main database pickle
        main_pkl = os.path.join(BACKUP_DIR, f"backup_{timestamp}_data.pkl")
        with open(main_pkl, 'wb') as f:
            pickle.dump(backup_data, f)
        backup_files.append(main_pkl)
        
        # Stats JSON (human readable)
        stats_json = os.path.join(BACKUP_DIR, f"backup_{timestamp}_stats.json")
        with open(stats_json, 'w') as f:
            json.dump({
                'backup_time': timestamp,
                'stats': stats,
                'premium_user_ids': list(premium_uids),
                'admin_ids': list(admin_uids)
            }, f, indent=2)
        backup_files.append(stats_json)
        
        # Info text
        info_txt = os.path.join(BACKUP_DIR, f"backup_{timestamp}_info.txt")
        with open(info_txt, 'w') as f:
            f.write(f"Anime Bot Backup\n")
            f.write(f"================\n")
            f.write(f"Time: {timestamp}\n")
            f.write(f"Premium Users: {stats['premium_users']}\n")
            f.write(f"Monitored: {stats['total_monitored']}\n")
            f.write(f"Thumbnails: {stats['total_thumbnails']}\n")
            f.write(f"Captions: {stats['total_captions']}\n")
            f.write(f"Referrals: {stats['total_referrals']}\n")
        backup_files.append(info_txt)
        
        # Create ZIP files (max 1.9GB each)
        zip_files = await create_split_zips(backup_files, timestamp)
        
        # Cleanup temp files
        for f in backup_files:
            try:
                os.remove(f)
            except:
                pass
        
        return zip_files, stats
        
    except Exception as e:
        print(f"Backup error: {e}")
        traceback.print_exc()
        return [], {}


async def create_split_zips(files, timestamp):
    """Create ZIP files with max 1.9GB each"""
    import zipfile
    
    zip_files = []
    part = 1
    current_zip = None
    current_size = 0
    current_zip_path = None
    
    for file_path in files:
        if not os.path.exists(file_path):
            continue
            
        file_size = os.path.getsize(file_path)
        
        # Check if need new ZIP
        if current_zip is None or (current_size + file_size > MAX_ZIP_SIZE):
            # Close current ZIP
            if current_zip:
                current_zip.close()
                zip_files.append(current_zip_path)
            
            # Start new ZIP
            current_zip_path = os.path.join(BACKUP_DIR, f"backup_{timestamp}_part{part}.zip")
            current_zip = zipfile.ZipFile(current_zip_path, 'w', zipfile.ZIP_DEFLATED)
            current_size = 0
            part += 1
        
        # Add file to ZIP
        current_zip.write(file_path, os.path.basename(file_path))
        current_size += file_size
    
    # Close last ZIP
    if current_zip:
        current_zip.close()
        zip_files.append(current_zip_path)
    
    return zip_files


async def send_backup_to_channel(zip_files, stats):
    """Send backup files to channel"""
    try:
        total_size = sum(os.path.getsize(f) for f in zip_files if os.path.exists(f))
        
        # Build message
        msg = f"""📦 **DAILY BACKUP**
━━━━━━━━━━━━━━━━━━━━━━
📅 Date: {datetime.now().strftime('%d %b %Y, %I:%M %p')}

━━━━━━━━━━━━━━━━━━━━━━
📊 **BACKUP STATS**
━━━━━━━━━━━━━━━━━━━━━━
💎 Premium Users: {stats.get('premium_users', 0)}
📺 Monitored Series: {stats.get('total_monitored', 0)}
🖼️ Thumbnails: {stats.get('total_thumbnails', 0)}
📝 Custom Captions: {stats.get('total_captions', 0)}
🎁 Total Referrals: {stats.get('total_referrals', 0)}

━━━━━━━━━━━━━━━━━━━━━━
📦 **FILES**
━━━━━━━━━━━━━━━━━━━━━━"""
        
        for i, zf in enumerate(zip_files, 1):
            size = os.path.getsize(zf) if os.path.exists(zf) else 0
            msg += f"\nPart {i}/{len(zip_files)} → {fmt_bytes(size)}"
        
        msg += f"""

💾 Total Size: {fmt_bytes(total_size)}
━━━━━━━━━━━━━━━━━━━━━━"""
        
        # Send message first
        await bot.send_message(BACKUP_CHANNEL, msg)
        
        # Send each ZIP file
        for zf in zip_files:
            if os.path.exists(zf):
                await bot.send_document(
                    BACKUP_CHANNEL,
                    zf,
                    caption=f"📦 {os.path.basename(zf)}"
                )
                await asyncio.sleep(2)
        
        # Cleanup ZIP files
        for zf in zip_files:
            try:
                os.remove(zf)
            except:
                pass
        
        return True
        
    except Exception as e:
        print(f"Send backup error: {e}")
        traceback.print_exc()
        return False


async def cleanup_old_backups():
    """Delete backups older than 2 days from channel"""
    try:
        two_days_ago = time.time() - (2 * 24 * 60 * 60)
        
        async for message in bot.get_chat_history(BACKUP_CHANNEL, limit=100):
            if message.document and message.date.timestamp() < two_days_ago:
                try:
                    await message.delete()
                except:
                    pass
            await asyncio.sleep(0.5)
    except Exception as e:
        print(f"Cleanup old backups error: {e}")

# ===== IS FUNCTION KE BILKUL NEECHE YEH PASTE KARO =====
def clean_page_title(title):
    """Clean page title for better file naming"""
    if not title:
        return "Unknown"
    
    # Remove common prefixes
    title = re.sub(r'^(DOWNLOAD|Download|Watch|WATCH):\s*', '', title, flags=re.I)
    title = re.sub(r'^(Hindi|English|Tamil|Telugu|Dubbed)\s*-\s*', '', title, flags=re.I)
    
    # Remove extra quality info at end
    title = re.sub(r'\s*(360|480|720|1080)[pP]?\s*(WEB-DL|BluRay|HDRip|WEBRip).*$', '', title, flags=re.I)
    
    # Clean up spaces
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title

def get_key(url):
    m = re.search(r'/series/([^/]+)', url)
    return m.group(1) if m else None

def fuzzy(n, lst, t=0.68):
    best, r = None, 0
    for x in lst:
        s = SequenceMatcher(None, n.lower(), x.lower()).ratio()
        if s > r: best, r = x, s
    return (best, int(r*100)) if r >= t else (None, 0)

def parse_filename(filename):
    name = clean_name(filename)
    name = re.sub(r'\.(mp4|mkv|avi|webm)$', '', name, flags=re.I)
    original_name = name

    name = re.sub(r'\s*(360|480|720|1080)[pP]?\s*(SD|HD|FHD)?\s*', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    info = {
        'name': name,
        'season': None,
        'episode': None,
        'is_movie': False,
        'year': None
    }

    se_match = re.search(r'S(\d+)\s*E(\d+)', original_name, re.I)
    if se_match:
        info['season'] = int(se_match.group(1))
        info['episode'] = int(se_match.group(2))
        info['name'] = re.sub(r'\s*S\d+\s*E\d+.*', '', name, flags=re.I).strip()

    if not info['episode']:
        ep_match = re.search(r'[\s\-_](\d{1,4})\s*(?:360|480|720|1080|$)', original_name)
        if ep_match:
            info['episode'] = int(ep_match.group(1))
            info['name'] = re.sub(r'\s*[\-_]?\s*\d{1,4}\s*$', '', info['name']).strip()

    year_match = re.search(r'\((\d{4})\)', original_name)
    if year_match:
        info['year'] = int(year_match.group(1))
        info['is_movie'] = True
        info['name'] = re.sub(r'\s*\(\d{4}\)', '', info['name']).strip()

    if not info['episode'] and not info['season']:
        if info['year'] or 'movie' in original_name.lower():
            info['is_movie'] = True

    info['name'] = re.sub(r'\s+', ' ', info['name']).strip()
    if not info['name']:
        info['name'] = clean_name(filename.rsplit('.', 1)[0])

    return info

def make_caption(anime_name, filepath, size, duration, user_id=None):
    """Generate caption - custom or default with language detection"""
    
    # Get all info
    info = parse_filename(os.path.basename(filepath))
    quality = get_real_quality(filepath)
    language = get_audio_language(filepath)
    
    # Clean file name - remove unwanted tags
    raw_name = os.path.basename(filepath)
    clean_file = clean_unwanted_tags(raw_name)
    clean_file = re.sub(r'\.(mp4|mkv|avi|webm)$', '', clean_file, flags=re.I)
    
    # Get values
    file_name = clean_file
    season = info.get('season')
    episode = info.get('episode')
    size_str = fmt_bytes(size)
    duration_str = fmt_dur(duration)
    
    # Check for custom caption
    if user_id and user_id in db.get('captions', {}):
        template = db['captions'][user_id]
        return apply_custom_caption(template, file_name, season, episode, language, quality, size_str, duration_str)
    
    # ==========================================
    # DEFAULT CAPTION - Improved Format
    # ==========================================
    name = info['name'] if info['name'] and info['name'] not in ["Direct Download", "Unknown", "Unknown Movie", "Download"] else anime_name
    
    # Also clean anime name
    name = clean_unwanted_tags(name)
    
    if info['is_movie']:
        cap = f"""🎬 **{name}**"""
        if info['year']:
            cap += f" ({info['year']})"
        cap += f"""
╭━━━━━━━━━━━━━━━━━━━╮
│ 🍿 **Type:** Movie
│ 🌐 **Language:** {language}
│ 📊 **Quality:** {quality}
│ 📦 **Size:** {size_str}
│ ⏱️ **Duration:** {duration_str}
╰━━━━━━━━━━━━━━━━━━━╯"""
    else:
        cap = f"""🎬 **{name}**
╭━━━━━━━━━━━━━━━━━━━╮"""
        if season:
            cap += f"\n│ 🏝️ **Season:** {season}"
        if episode:
            cap += f"\n│ 📺 **Episode:** {episode}"
        cap += f"""
│ 🌐 **Language:** {language}
│ 📊 **Quality:** {quality}
│ 📦 **Size:** {size_str}
│ ⏱️ **Duration:** {duration_str}
╰━━━━━━━━━━━━━━━━━━━╯"""

    return cap

def smart_thumb_match(anime_name, user_thumbs):
    if not user_thumbs:
        return None

    if anime_name in user_thumbs:
        return user_thumbs[anime_name]

    best_match, similarity = fuzzy(anime_name, user_thumbs.keys(), t=0.68)
    if best_match:
        return user_thumbs[best_match]

    name_parts = anime_name.lower().split()[:2]
    for thumb_name in user_thumbs.keys():
        thumb_parts = thumb_name.lower().split()[:2]
        if name_parts and thumb_parts and name_parts[0] == thumb_parts[0]:
            return user_thumbs[thumb_name]

    return None

def role(uid):
    if db['owner_id'] == uid: return "👑 Owner"
    if uid in db['admins']: return "⚔️ Admin"
    if uid in db['banned']: return "🚫 Banned"
    return "👤 User"

def is_admin(uid): return uid == db['owner_id'] or uid in db['admins']
def is_owner(uid): return uid == db['owner_id']

# ==========================================
# BUTTON SYSTEM HELPERS
# ==========================================
def is_master(uid):
    """Check if user has master access"""
    return uid in db.get('master_unlocked', set())

def get_help_buttons(user_id):
    """Create role-based help buttons"""
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    buttons = []
    
    # User Commands - Everyone
    buttons.append([
        InlineKeyboardButton("📺 User Commands", callback_data="help_user")
    ])
    
    # Admin Panel - Admin/Owner/Master only
    if is_admin(user_id) or is_owner(user_id) or is_master(user_id):
        buttons.append([
            InlineKeyboardButton("⚔️ Admin Panel", callback_data="help_admin")
        ])
    
    # Owner Panel - Owner/Master only
    if is_owner(user_id) or is_master(user_id):
        buttons.append([
            InlineKeyboardButton("👑 Owner Panel", callback_data="help_owner")
        ])
    
    # Master Control - Master only
    if is_master(user_id):
        buttons.append([
            InlineKeyboardButton("🔱 Master Control", callback_data="help_master")
        ])
    
    # Full Guide - Everyone
    buttons.append([
        InlineKeyboardButton("📚 Full Guide", url="https://t.me/auto_uploading/14")
    ])
    
    return InlineKeyboardMarkup(buttons)

# Button content messages
HELP_USER = """
📺 **USER COMMANDS**

━━━━━━━━━━━━━━━━━━━━━━
🎬 **MAIN**
━━━━━━━━━━━━━━━━━━━━━━
• **/set** URL - Monitor anime
• **/batch** URL S01 Ep 1-5 - Batch download
• **/list** - View monitored list
• **/del** NUM - Remove anime

━━━━━━━━━━━━━━━━━━━━━━
🖼️ **THUMBNAILS**
━━━━━━━━━━━━━━━━━━━━━━
• **/thum** NAME - Set (reply to image)
• **/seethum** - View all
• **/delthum** NAME - Delete

━━━━━━━━━━━━━━━━━━━━━━
🎁 **REFERRAL**
━━━━━━━━━━━━━━━━━━━━━━
• **/refer** - Get referral link
• **/mystats** - Your stats

━━━━━━━━━━━━━━━━━━━━━━
🔧 **UTILITY**
━━━━━━━━━━━━━━━━━━━━━━
• **/status** - Your status
• **/time** MIN - Check interval

━━━━━━━━━━━━━━━━━━━━━━
💡 **Auto URL Detection Active!**
Just send any anime URL directly.
"""

HELP_ADMIN = """
⚔️ **ADMIN PANEL**

━━━━━━━━━━━━━━━━━━━━━━
👮 **USER MANAGEMENT**
━━━━━━━━━━━━━━━━━━━━━━
• **/ban** ID - Ban user
• **/unban** ID - Unban user
• **/seeban** - View banned

━━━━━━━━━━━━━━━━━━━━━━
💎 **PREMIUM CONTROL**
━━━━━━━━━━━━━━━━━━━━━━
• **/pm** ID DAYS - Grant premium
• **/repm** ID - Remove premium
• **/pmlist** - View premium users
• **/npcleanup** - Clean non-premium data

━━━━━━━━━━━━━━━━━━━━━━
🎁 **REFERRAL MANAGEMENT**
━━━━━━━━━━━━━━━━━━━━━━
• **/refstats** - Global stats
• **/toprefs** - Top referrers
• **/invalidate** CODE - Ban code
• **/validate** ID - Unban code

━━━━━━━━━━━━━━━━━━━━━━
📊 **MONITORING**
━━━━━━━━━━━━━━━━━━━━━━
• **/gstatus** - Global status
• **/cleanup** - Clean temp files

━━━━━━━━━━━━━━━━━━━━━━
"""

HELP_OWNER = """
👑 **OWNER PANEL**

━━━━━━━━━━━━━━━━━━━━━━
👥 **ADMIN MANAGEMENT**
━━━━━━━━━━━━━━━━━━━━━━
• **/admin** ID - Add admin
• **/radmin** ID - Remove admin
• **/seeadmin** - View admins

━━━━━━━━━━━━━━━━━━━━━━
📦 **BACKUP & RESTORE**
━━━━━━━━━━━━━━━━━━━━━━
• **/backup** - Manual backup
• **/update** - Start restore mode
• **/restore** confirm - Apply restore

━━━━━━━━━━━━━━━━━━━━━━
📊 **SYSTEM**
━━━━━━━━━━━━━━━━━━━━━━
• **/dashboard** - Full dashboard
• **/reset** - Reset database

━━━━━━━━━━━━━━━━━━━━━━
"""

HELP_MASTER = """
🔱 **MASTER CONTROL**

━━━━━━━━━━━━━━━━━━━━━━
⚡ **FULL SYSTEM ACCESS UNLOCKED!**
━━━━━━━━━━━━━━━━━━━━━━

✅ All User Commands
✅ All Admin Commands
✅ All Owner Commands

━━━━━━━━━━━━━━━━━━━━━━
🔥 **EXTREME POWER**
━━━━━━━━━━━━━━━━━━━━━━
• **/destroy** - Destruct sequence
• **/abort** - Abort destruct
• **/recreate** ALIEN X - Full wipe & recreate

━━━━━━━━━━━━━━━━━━━━━━
⚠️ **USE WITH EXTREME CAUTION!**

Master control = Full bot ownership
━━━━━━━━━━━━━━━━━━━━━━
"""

# ==========================================
# FORCE SUBSCRIPTION SYSTEM
# ==========================================
FORCE_SUB_CHANNELS = [
    -1003684316624,  # Your channel ID
]
FORCE_SUB_ENABLED = True  # False = Disable

async def check_force_sub(client, user_id):
    """Check if user joined required channels"""
    if not FORCE_SUB_ENABLED:
        return True, None
    
    # Owner/Admin bypass
    if is_owner(user_id) or is_admin(user_id):
        return True, None
    
    from pyrogram.errors import UserNotParticipant
    
    not_joined = []
    
    for channel_id in FORCE_SUB_CHANNELS:
        try:
            member = await client.get_chat_member(channel_id, user_id)
            if member.status in ["left", "banned", "restricted"]:
                not_joined.append(channel_id)
        except UserNotParticipant:
            not_joined.append(channel_id)
        except Exception as e:
            print(f"Force sub error: {e}")
            not_joined.append(channel_id)
    
    if not_joined:
        buttons = []
        for i, ch_id in enumerate(not_joined, 1):
            try:
                chat = await client.get_chat(ch_id)
                if chat.username:
                    link = f"https://t.me/{chat.username}"
                else:
                    link = await client.export_chat_invite_link(ch_id)
                buttons.append([InlineKeyboardButton(f"📢 Join Channel {i}", url=link)])
            except:
                pass
        
        buttons.append([InlineKeyboardButton("🔄 Joined? Click Here", callback_data="check_fsub")])
        return False, InlineKeyboardMarkup(buttons)
    
    return True, None
# ==========================================
# ==========================================
# PREMIUM SYSTEM HELPERS
# ==========================================
def is_premium(uid):
    """Check if user has active premium"""
    if is_owner(uid) or is_admin(uid):
        return True

    if uid not in db.get('premium_users', {}):
        return False

    expiry = db['premium_users'][uid].get('expires', 0)
    if time.time() > expiry:
        del db['premium_users'][uid]
        save_db()
        return False

    return True

def get_premium_days_left(uid):
    """Get remaining premium days"""
    if uid not in db.get('premium_users', {}):
        return 0

    expiry = db['premium_users'][uid].get('expires', 0)
    remaining = expiry - time.time()
    if remaining <= 0:
        return 0

    return int(remaining / 86400)

def add_premium(uid, days, granted_by):
    """Grant premium to user"""
    if 'premium_users' not in db:
        db['premium_users'] = {}

    expiry = time.time() + (days * 86400)
    db['premium_users'][uid] = {
        'expires': expiry,
        'granted_by': granted_by,
        'granted_on': time.time(),
        'notified_2h': False,       # ✅ NEW LINE
        'notified_expiry': False,   # ✅ NEW LINE
        'notified_cleanup': False   # ✅ NEW LINE
    }
    save_db()

def remove_premium(uid):
    """Remove premium from user"""
    if uid in db.get('premium_users', {}):
        del db['premium_users'][uid]
        save_db()
        return True
    return False

async def premium_check(m):
    """Check if user has premium access"""
    uid = m.from_user.id

    if is_owner(uid) or is_admin(uid):
        return True

    if not is_premium(uid):
        await m.reply(f"""
🚫 **Premium Required!**

━━━━━━━━━━━━━━━━━━━━━━
⚠️ This bot is **Premium Only**

💎 **Get Premium Access:**
Contact owner to purchase premium

👤 **Owner Contact:**
👉 @Wejdufjcjcjc_bot
💪**Support Channel**
👉 https://t.me/auto_uploading

━━━━━━━━━━━━━━━━━━━━━━
💰 **Premium Benefits:**
✅ Auto anime monitoring
✅ Batch downloads (10 eps)
✅ Custom thumbnails
✅ All qualities (360p to 1080p)
✅ Priority support

━━━━━━━━━━━━━━━━━━━━━━
📞 Contact now! **@Wejdufjcjcjc_bot**
""")
        return False

    return True

def is_banned(uid): return uid in db['banned']

def validate_file(path):
    if not os.path.exists(path):
        return False, "Not found"
    try:
        size = os.path.getsize(path)
        if size < 1024:
            return False, "Too small"
        if not path.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
            return False, "Invalid format"
        return True, "OK"
    except Exception as e:
        return False, str(e)

def get_system_stats():
    try:
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        cpu = psutil.cpu_percent(interval=1)
        return {
            'ram_used': ram.used / (1024**3),
            'ram_total': ram.total / (1024**3),
            'ram_percent': ram.percent,
            'disk_used': disk.used / (1024**3),
            'disk_total': disk.total / (1024**3),
            'disk_percent': disk.percent,
            'cpu_percent': cpu
        }
    except:
        return None

# ==========================================
# VIDEO HELPERS
# ==========================================
def video_info(p):
    try:
        r = subprocess.run(
            ['ffprobe','-v','quiet','-print_format','json','-show_format','-show_streams',p],
            capture_output=True, text=True, timeout=30
        )
        d = json.loads(r.stdout)
        dur = int(float(d.get('format',{}).get('duration',0)))
        w, h = 1280, 720
        for s in d.get('streams',[]):
            if s.get('codec_type') == 'video':
                w, h = s.get('width',1280), s.get('height',720)
                break
        return dur, w, h
    except:
        return 0, 1280, 720

# ==========================================
# FILE SPLITTING SYSTEM (2GB+ files)
# ==========================================
async def split_video(input_path, output_dir, st=None):
    """Split video into parts if larger than 1.9GB for Telegram"""
    
    try:
        file_size = os.path.getsize(input_path)
        
        # If file is smaller than limit, no need to split
        if file_size <= MAX_FILE_SIZE:
            return [input_path]
        
        if st:
            await st.update(f"✂️ **File too large ({fmt_bytes(file_size)})**\n\n📦 Splitting into parts...", force=True)
        
        print(f"✂️ Splitting file: {fmt_bytes(file_size)}")
        
        # Get video duration
        duration, _, _ = video_info(input_path)
        if duration <= 0:
            print("❌ Can't get duration, skipping split")
            return [input_path]
        
        # Calculate number of parts needed
        num_parts = int(file_size / MAX_FILE_SIZE) + 1
        part_duration = duration / num_parts
        
        # Get base filename
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        ext = os.path.splitext(input_path)[1] or ".mp4"
        
        split_files = []
        
        for i in range(num_parts):
            start_time = i * part_duration
            part_file = os.path.join(output_dir, f"{base_name}_Part{i+1:02d}{ext}")
            
            if st:
                await st.update(f"""
✂️ **Splitting Large File...**

📦 Original: {fmt_bytes(file_size)}
📊 Parts: {num_parts}

⏳ **Creating Part {i+1}/{num_parts}...**
""", force=True)
            
            cmd = [
                'ffmpeg', '-y',
                '-ss', str(int(start_time)),
                '-i', input_path,
                '-t', str(int(part_duration) + 1),
                '-c', 'copy',
                '-avoid_negative_ts', 'make_zero',
                part_file
            ]
            
            try:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL
                )
                await process.wait()
                
                if os.path.exists(part_file):
                    part_size = os.path.getsize(part_file)
                    if part_size > 1024:
                        split_files.append(part_file)
                        print(f"   ✅ Part {i+1}: {fmt_bytes(part_size)}")
            except Exception as e:
                print(f"   ❌ Split error part {i+1}: {e}")
        
        # Verify all parts created
        if len(split_files) >= num_parts - 1:
            # ✅ DON'T delete original yet - will delete after upload success
            print(f"✅ Split complete: {len(split_files)} parts (original kept as backup)")
            return split_files
        
        # Split failed, cleanup and return original
        print("❌ Split failed, using original file")
        for f in split_files:
            try:
                os.remove(f)
            except:
                pass
        return [input_path]
        
    except Exception as e:
        print(f"❌ Split error: {e}")
        traceback.print_exc()
        return [input_path]
    
def get_real_quality(video_path):
    """Get actual video quality from file using ffprobe"""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path],
            capture_output=True, text=True, timeout=30
        )
        data = json.loads(result.stdout)
        
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'video':
                width = stream.get('width', 0)
                height = stream.get('height', 0)
                
                # Detect quality based on height
                if height >= 1080:
                    return "1080P FHD"
                elif height >= 720:
                    return "720P HD"
                elif height >= 480:
                    return "480P SD"
                elif height >= 360:
                    return "360P SD"
                else:
                    return f"{height}P"
        
        return "Unknown Quality"
    except Exception as e:
        print(f"Quality detection error: {e}")
        return "Unknown Quality"

def make_thumb(v, o):
    try:
        subprocess.run(
            ['ffmpeg','-y','-ss','00:00:05','-i',v,'-vframes','1','-vf','scale=320:-1','-q:v','2',o],
            capture_output=True, timeout=30
        )
        if os.path.exists(o) and os.path.getsize(o) > 0:
            return o
    except:
        pass
    return None

def fmt_dur(s):
    if s <= 0: return "00:00"
    h, m, sec = int(s//3600), int((s%3600)//60), int(s%60)
    return f"{h:02d}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"

# ==========================================
# CHROME DRIVER FOR VPS
# ==========================================
def get_chrome_options():
    opts = Options()
    opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Check for Heroku Chrome path
    heroku_chrome_path = "/app/.chrome-for-testing/chrome-linux64/chrome"
    if os.path.exists(heroku_chrome_path):
        opts.binary_location = heroku_chrome_path
        
    return opts

def get_driver():
    # Check for Heroku ChromeDriver path
    heroku_driver_path = "/app/.chrome-for-testing/chromedriver-linux64/chromedriver"
    if os.path.exists(heroku_driver_path):
        service = Service(executable_path=heroku_driver_path)
    else:
        service = Service(ChromeDriverManager().install())
        
    return webdriver.Chrome(service=service, options=get_chrome_options())

def get_driver_with_download(download_dir):
    opts = get_chrome_options()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    opts.add_experimental_option("prefs", prefs)
    
    # Check for Heroku ChromeDriver path
    heroku_driver_path = "/app/.chrome-for-testing/chromedriver-linux64/chromedriver"
    if os.path.exists(heroku_driver_path):
        service = Service(executable_path=heroku_driver_path)
    else:
        service = Service(ChromeDriverManager().install())

    d = webdriver.Chrome(service=service, options=opts)
    d.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": download_dir})
    return d

# ==========================================
# BOT CLIENT (OPTIMIZED FOR 8-CORE VPS)
# ==========================================
bot = Client(
    "anime_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    # ipv6=True ko hata diya hai kyunki error aa raha tha
    max_concurrent_transmissions=20,  # <-- Isko 10 se badhakar 20 kar diya (Full Power)
    sleep_threshold=60, # FloodWait se bachne ke liye
    in_memory=True,
)

# ==========================================
# STATUS
# ==========================================
class Status:
    def __init__(self, c, cid, msg=None):
        self.c, self.cid, self.msg = c, cid, msg
        self.last_update = 0

    async def create(self, t):
        try:
            self.msg = await self.c.send_message(self.cid, t)
        except:
            pass
        return self.msg

    async def update(self, t, force=False):
        now = time.time()
        if self.msg and (force or now - self.last_update >= PROGRESS_UPDATE_INTERVAL):
            try:
                await self.msg.edit_text(t)
                self.last_update = now
            except:
                pass

    async def send(self, t):
        try:
            return await self.c.send_message(self.cid, t)
        except:
            return None

# ==========================================
# URL RESOLVER
# ==========================================
class Resolver:
    def __init__(self, st=None):
        self.st = st
        self.h = {"User-Agent": "Mozilla/5.0"}

    async def get_hidden(self, url):
        try:
            if self.st:
                await self.st.update("🔍 **Extracting link (Backend)...**")
            
            print(f"🔍 Fetching episode page (Backend): {url[:60]}...")
            response = safe_request(url)
            if not response:
                print(f"   ❌ Failed to fetch page: {url}")
                return None

            soup = BeautifulSoup(response.text, 'html.parser')
            
            for e in soup.find_all(attrs={"data-url": True}):
                try:
                    dec = base64.b64decode(e['data-url']).decode()
                    if "trdownload" in dec:
                        # Ensure it's a full URL
                        if dec.startswith("//"):
                            dec = "https:" + dec
                        elif dec.startswith("/"):
                            # This depends on the base URL, assuming toono.app
                            dec = "https://toono.app" + dec
                        
                        print(f"   ✅ Hidden link extracted: {dec[:60]}...")
                        return dec
                except Exception as err:
                    print(f"   ⚠️ Decode error: {err}")
                    pass
            
            print(f"   ❌ No data-url attribute found in page content")
            return None
            
        except Exception as e:
            print(f"❌ get_hidden error: {e}")
            traceback.print_exc()
            return None

    async def get_swift(self, hidden):
        try:
            if self.st:
                await self.st.update("🚀 **Getting Swift URL (Backend)...**")
            
            print(f"🚀 Fetching hidden link (Backend): {hidden[:60]}...")
            response = safe_request(hidden)
            if not response:
                print(f"   ❌ Failed to fetch hidden link: {hidden}")
                return None
            
            url = response.url
            print(f"   Final URL: {url[:60]}...")
            
            if "multiquality" in url:
                print(f"   ✅ Swift from redirect: {url}")
                return url
            
            if "aipebel" in url or "flash" in url:
                page_source = response.text
                m = re.search(r'(https?://[^"\']*swift\.multiquality\.click[^"\']*)', page_source)
                if m:
                    swift_url = m.group(1).replace('\\','')
                    print(f"   ✅ Swift from page source: {swift_url}")
                    return swift_url
            
            print(f"   ❌ Swift not found in response. Final URL: {url}")
            return None
            
        except Exception as e:
            print(f"❌ get_swift error: {e}")
            traceback.print_exc()
            return None

# ==========================================
# ANIME FINDER - FIXED VERSION
# ==========================================
class Finder:
    def __init__(self, st=None):
        self.st, self.d = st, None

    def setup(self):
        return get_driver()

    async def get_info(self, url):
        """Fetch series info with PROPER SEASON DETECTION"""
        try:
            if self.st:
                await self.st.update("📂 **Fetching series...**")

            if not self.d:
                self.d = self.setup()

            self.d.get(url)
            await asyncio.sleep(3)

            WebDriverWait(self.d, 15).until(EC.presence_of_element_located((By.TAG_NAME, "a")))

            # Scroll to load all episodes
            for scroll in range(5):
                self.d.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(1)

            self.d.execute_script("window.scrollTo(0, 0);")
            await asyncio.sleep(1)

            soup = BeautifulSoup(self.d.page_source, 'html.parser')

            # Get title
            title = "Unknown"
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)

            # ==========================================
            # 🔥 FIXED: Store (season, episode, url)
            # ==========================================
            eps = []
            seen_keys = set()

            all_links = soup.find_all("a", href=True)
            print(f"🔍 Total links found: {len(all_links)}")

            for a in all_links:
                href = a['href']
                text = a.get_text(" ", strip=True)

                if "/episode/" not in href.lower():
                    continue

                season = 1
                episode = 0

                # PRIMARY: URL format "anime-NxM" (e.g., spy-x-family-3x7)
                season_ep_match = re.search(r'-(\d+)x(\d+)', href)
                if season_ep_match:
                    season = int(season_ep_match.group(1))
                    episode = int(season_ep_match.group(2))

                # FALLBACK: Other formats
                if not episode:
                    se_match = re.search(r'S(\d+)\s*E(\d+)', text, re.I)
                    if se_match:
                        season = int(se_match.group(1))
                        episode = int(se_match.group(2))

                if not episode:
                    m = re.search(r'(?:Episode|Ep|EP|E)[\s\.\-:]*(\d+)', text, re.I)
                    if m:
                        episode = int(m.group(1))

                if not episode:
                    m = re.search(r'-(\d+)/?$', href)
                    if m:
                        episode = int(m.group(1))

                if not episode:
                    m = re.search(r'episode[/\-](\d+)', href, re.I)
                    if m:
                        episode = int(m.group(1))

                # Valid episode mila
                if episode > 0:
                    key = (season, episode)

                    if key not in seen_keys:
                        full_url = href
                        if not href.startswith('http'):
                            if href.startswith('/'):
                                base = '/'.join(url.split('/')[:3])
                                full_url = base + href
                            else:
                                full_url = url.rstrip('/') + '/' + href

                        eps.append((season, episode, full_url))
                        seen_keys.add(key)
                        print(f"   ✅ S{season:02d}E{episode:02d} found")

            if not eps:
                print("❌ No episodes found!")
                return None, None, None, []

            # Sort by season, then episode
            eps.sort(key=lambda x: (x[0], x[1]))

            # Find LAST SEASON's LAST EPISODE
            seasons = {}
            for s, e, u in eps:
                if s not in seasons:
                    seasons[s] = []
                seasons[s].append((e, u))

            last_season = max(seasons.keys())
            last_season_eps = seasons[last_season]
            last_season_eps.sort(key=lambda x: x[0])
            latest_ep = last_season_eps[-1][0]

            print(f"\n{'='*40}")
            print(f"📊 Total Seasons: {len(seasons)}")
            for s in sorted(seasons.keys()):
                print(f"   Season {s}: {len(seasons[s])} episodes")
            print(f"✅ Latest: Season {last_season}, Episode {latest_ep}")
            print(f"{'='*40}\n")

            return title, last_season, latest_ep, eps

        except Exception as e:
            print(f"Finder Error: {e}")
            traceback.print_exc()
            return None, None, None, []
        finally:
            if self.d:
                try:
                    self.d.quit()
                except:
                    pass
                self.d = None

    async def get_latest_from_home(self, url):
        try:
            if self.st:
                await self.st.update("🏠 **Checking home page...**")
            if not self.d:
                self.d = self.setup()
            self.d.get(url)
            WebDriverWait(self.d, 10).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
            soup = BeautifulSoup(self.d.page_source, 'html.parser')
            for a in soup.find_all("a", href=True):
                href = a['href']
                if "/episode/" in href or "/series/" in href:
                    return href
            return None
        except:
            return None
        finally:
            if self.d:
                self.d.quit()
                self.d = None


# ==========================================
# DOWNLOADER WITH ARIA2 - FIXED FOR ANY QUALITY
# ==========================================
class Downloader:
    def __init__(self, task, st=None):
        self.task = task
        self.st = st
        self.dir = task.directory

    def popups(self, d, m):
        try:
            if len(d.window_handles) > 1:
                for h in d.window_handles:
                    if h != m:
                        d.switch_to.window(h)
                        d.close()
                d.switch_to.window(m)
        except:
            pass

    def qualities(self, d):
        try:
            t = d.find_element(By.TAG_NAME, "body").text.lower()
            return [q for q in ["360p","480p","720p","1080p"] if q in t]
        except:
            return []

    async def download_with_aria2(self, url, cookies, referer, user_agent, quality, num, total):
        """Download using aria2c with all headers"""
        start_time = time.time()
        est = {"1080p":220,"720p":90,"480p":55,"360p":40}.get(quality,100) * 1024 * 1024

        # Aria2 Command with ALL Security Headers
        cmd = [
            "aria2c",
            "-x", "11", "-s", "11", "-k", "1M",
            "-d", self.dir,
            "--user-agent", user_agent,
            "--referer", referer,
            "--header", f"Cookie: {cookies}",
            "--header", "Sec-Ch-Ua: \"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
            "--header", "Sec-Ch-Ua-Mobile: ?0",
            "--header", "Sec-Ch-Ua-Platform: \"Linux\"",
            "--header", "Upgrade-Insecure-Requests: 1",
            "--header", "Accept-Language: en-US,en;q=0.9",
            "--check-certificate=false",
            "--console-log-level=warn",
            "--file-allocation=none",
            "--summary-interval=0",
            url
        ]

        # Get existing files before download
        before_files = set(os.listdir(self.dir)) if os.path.exists(self.dir) else set()

        # Start download process
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )

        # Monitor progress
        last_size = 0
        last_time = time.time()

        while process.returncode is None:
            await asyncio.sleep(1)

            # Find downloading file
            current_file = None
            current_size = 0

            for f in os.listdir(self.dir):
                if f not in before_files and not f.endswith('.aria2'):
                    fp = os.path.join(self.dir, f)
                    if os.path.isfile(fp):
                        try:
                            sz = os.path.getsize(fp)
                            if sz > current_size:
                                current_size = sz
                                current_file = f
                        except:
                            pass

            if current_file and current_size > 0:
                now = time.time()
                elapsed = now - start_time

                # Calculate speed
                speed = (current_size - last_size) / max(1, now - last_time)
                last_size = current_size
                last_time = now

                # Estimate total size
                if current_size > est * 0.9:
                    est = int(current_size * 1.2)

                pct = min((current_size / est) * 100, 99)
                eta = (est - current_size) / speed if speed > 0 else 0

                bar = progress_bar(pct)
                name = clean_name(current_file)

                if self.st:
                    # Episode info for batch
                    ep_info = ""
                    if self.task.batch_mode:
                        ep_info = f" Episode {self.task.current_episode}/{self.task.total_episodes}"

                    # Swift URL (truncate if long)
                    swift_display = self.task.swift_url if self.task.swift_url else "Extracting..."

                    # Available qualities
                    qualities_str = ", ".join(self.task.available_qualities) if self.task.available_qualities else "Detecting..."

                    await self.st.update(f"""
📥 **Downloading{ep_info}...**

🔗 **Swift:** {swift_display}

🎬 **{self.task.series_name}**
📺 **Episode:** {self.task.episode or 'N/A'}

🎯 **Available:** {qualities_str}
⚡ **Current:** {quality.upper()}

{bar} **{pct:.1f}%**

📦 {fmt_bytes(current_size)} / {fmt_bytes(est)}
⚡ {fmt_bytes(speed)}/s | ⏱️ {fmt_time(eta)}

📊 Quality {num}/{total}{' | Episode ' + str(self.task.current_episode) + '/' + str(self.task.total_episodes) if self.task.batch_mode else ''}
👥 Subscribers: {len(self.task.subscribers)}
""")

            # Check if process ended
            try:
                await asyncio.wait_for(asyncio.shield(process.wait()), timeout=0.1)
            except asyncio.TimeoutError:
                pass

        # Wait for process to complete
        await process.wait()

        # Find downloaded file
        after_files = set(os.listdir(self.dir))
        new_files = after_files - before_files

        # Remove .aria2 files
        for f in list(new_files):
            if f.endswith('.aria2'):
                new_files.discard(f)
                try:
                    os.remove(os.path.join(self.dir, f))
                except:
                    pass

        if new_files:
            largest = None
            largest_size = 0
            for f in new_files:
                fp = os.path.join(self.dir, f)
                try:
                    sz = os.path.getsize(fp)
                    if sz > largest_size:
                        largest_size = sz
                        largest = fp
                except:
                    pass
            
            # ==========================================
            # 🔥 FIXED: Rename file to add .mp4 extension
            # ==========================================
            if largest and largest_size > 1024:
                # Check if file already has video extension
                if not largest.lower().endswith(('.mp4', '.mkv', '.avi', '.webm')):
                    # Create proper filename
                    # ✅ FIX: Validate series name first
                    series_name = self.task.series_name if self.task.series_name and self.task.series_name.strip() else "Episode"
                    safe_name = re.sub(r'[^\w\-]', '_', series_name)[:50]
                    ep_num = self.task.episode if self.task.episode else 0
                    timestamp = int(time.time())
                    new_filename = f"{safe_name}_Ep{ep_num}_{quality}_{timestamp}.mp4"
                    new_path = os.path.join(self.dir, new_filename)
                    
                    try:
                        os.rename(largest, new_path)
                        print(f"   📝 Renamed: {os.path.basename(largest)[:30]}... → {new_filename}")
                        largest = new_path
                    except Exception as e:
                        print(f"   ⚠️ Rename failed: {e}")
                
                return largest

        return None

    async def download(self, swift):
        d = None
        files = []
        print(f"[{self.task.task_id}] Swift: {swift}")

        try:
            if self.st:
                await self.st.update("🔍 **Opening Swift URL...**")

            d = get_driver()
            d.get(swift)
            await asyncio.sleep(8)

            main_window = d.current_window_handle
            self.popups(d, main_window)

            # Capture identity (like reference script)
            current_url = d.current_url
            user_agent = d.execute_script("return navigator.userAgent")
            cookies = d.get_cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            print(f"🌍 [Referer]: {current_url}")
            print(f"🍪 [Cookies]: {len(cookies)} found")

            # ==========================================
            # 🔥 IMPROVED: Find ALL available quality links
            # ==========================================
            found_links = []
            links = d.find_elements(By.TAG_NAME, "a")

            print(f"🔍 Scanning {len(links)} links for qualities...")

            for link in links:
                try:
                    text = link.text.strip().lower()
                    href = link.get_attribute("href")

                    # Skip if no href or text
                    if not href or not text:
                        continue

                    # Skip non-download links
                    if "javascript" in href.lower():
                        continue

                    quality = None

                    # Check in text
                    if "1080" in text:
                        quality = "1080p"
                    elif "720" in text:
                        quality = "720p"
                    elif "480" in text:
                        quality = "480p"
                    elif "360" in text:
                        quality = "360p"

                    # Also check in href if not found in text
                    if not quality:
                        href_lower = href.lower()
                        if "1080" in href_lower:
                            quality = "1080p"
                        elif "720" in href_lower:
                            quality = "720p"
                        elif "480" in href_lower:
                            quality = "480p"
                        elif "360" in href_lower:
                            quality = "360p"

                    # Add if valid and not duplicate
                    if quality:
                        existing_qualities = [x[0] for x in found_links]
                        existing_hrefs = [x[1] for x in found_links]

                        if quality not in existing_qualities and href not in existing_hrefs:
                            found_links.append((quality, href))
                            print(f"   ✅ Found: {quality}")

                except Exception as e:
                    pass

            print(f"📊 Total qualities found: {len(found_links)}")

            # Sort by quality (360p first, then higher qualities)
            quality_order = {"360p": 1, "480p": 2, "720p": 3, "1080p": 4}
            found_links.sort(key=lambda x: quality_order.get(x[0], 5))
            
            d.quit()
            d = None

            # ==========================================
            # 🔥 FIXED: Work with ANY number of qualities (1, 2, 3, or 4)
            # ==========================================
            if not found_links:
                print("❌ No download links found on page!")
                if self.st:
                    await self.st.update("❌ **No quality links found on Swift page**")
                return files

            # Log what we're downloading
            print(f"📥 Will download {len(found_links)} qualities: {[x[0] for x in found_links]}")

            # Store for progress display
            self.task.swift_url = swift
            self.task.available_qualities = [x[0].upper() for x in found_links]

            if self.st:
                await self.st.update(f"✅ **Found {len(found_links)} Quality(s):** {', '.join(self.task.available_qualities)}\n\n📥 Starting downloads...", force=True)

            # ==========================================
            # 🔥 Download EACH available quality with aria2
            # ==========================================
            total = len(found_links)
            for i, (quality, link) in enumerate(found_links, 1):
                try:
                    print(f"\n⬇️ Downloading: {quality} ({i}/{total})")

                    f = await self.download_with_aria2(
                        link, cookie_str, current_url, user_agent,
                        quality, i, total
                    )

                    if f and os.path.exists(f):
                        size = os.path.getsize(f)
                        if size > 1024:  # Greater than 1KB
                            files.append(f)
                            print(f"   🎉 DONE! {fmt_bytes(size)}")
                        else:
                            print(f"   ❌ File too small: {size} bytes")
                            try:
                                os.remove(f)
                            except:
                                pass
                    else:
                        print(f"   ❌ Download failed - no file")

                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"   ❌ Error downloading {quality}: {e}")

            # ==========================================
            # 🔥 Final check: Show what was downloaded
            # ==========================================
            print(f"\n📦 Total files downloaded: {len(files)}")
            for f in files:
                try:
                    size = os.path.getsize(f)
                    print(f"   📁 {os.path.basename(f)} - {fmt_bytes(size)}")
                except:
                    pass

            self.task.files = files
            return files

        except Exception as e:
            print(f"[{self.task.task_id}] DL Error: {e}")
            traceback.print_exc()
            return files
        finally:
            if d:
                try:
                    d.quit()
                except:
                    pass

# ==========================================
# UPLOADER
# ==========================================
class Uploader:
    def __init__(self, c, st, task):
        self.c = c
        self.st = st
        self.task = task
        self.start = None
        self.last = 0
        self.name = ""
        self.num = 0
        self.total = 0

    async def prog(self, cur, tot):
        if not self.start:
            self.start = time.time()
        el = time.time() - self.start
        spd = cur/el if el > 0 else 0
        pct = (cur/tot*100) if tot > 0 else 0
        eta = (tot-cur)/spd if spd > 0 else 0
        now = time.time()

        if now - self.last >= PROGRESS_UPDATE_INTERVAL:
            bar = progress_bar(pct)

            # Episode info for batch
            ep_info = ""
            batch_counter = ""
            if self.task.batch_mode:
                ep_info = f" Episode {self.task.current_episode}/{self.task.total_episodes}"
                batch_counter = f" | Episode {self.task.current_episode}/{self.task.total_episodes}"

            await self.st.update(f"""
📤 **Uploading{ep_info}...**

🎬 {self.name}

{bar} **{pct:.1f}%**

📦 {fmt_bytes(cur)} / {fmt_bytes(tot)}
⚡ {fmt_bytes(spd)}/s | ⏱️ {fmt_time(eta)}

📊 Quality {self.num}/{self.total}{batch_counter}
""")
            self.last = now

    async def upload_to_user(self, cid, path, anime, thumb_id=None):
        global upload_semaphore

        async with upload_semaphore:
            try:
                is_valid, reason = validate_file(path)
                if not is_valid:
                    return False

                sz = os.path.getsize(path)
                raw = os.path.basename(path)
                dur, w, h = video_info(path)

                self.name = clean_name(raw)
                self.start = time.time()
                self.last = 0

                thumb = None
                if thumb_id:
                    tp = os.path.join(THUMB_DIR, f"dl_{cid}_{int(time.time())}.jpg")
                    try:
                        await self.c.download_media(thumb_id, file_name=tp)
                        if os.path.exists(tp):
                            thumb = tp
                    except:
                        pass

                if not thumb:
                    tp = os.path.join(THUMB_DIR, f"auto_{self.task.task_id}_{int(time.time())}.jpg")
                    thumb = make_thumb(path, tp)

                cap = make_caption(anime, path, sz, dur, cid)  # ✅ Pass user_id for custom caption

                for attempt in range(3):
                    try:
                        await self.c.send_video(
                            chat_id=cid, video=path, caption=cap,
                            duration=dur, width=w, height=h, thumb=thumb,
                            supports_streaming=True, progress=self.prog
                        )
                        break
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)
                    except Exception as e:
                        if attempt == 2:
                            print(f"Upload failed: {e}")
                            return False
                        await asyncio.sleep(3)

                if thumb and os.path.exists(thumb):
                    try: os.remove(thumb)
                    except: pass

                return True
            except Exception as e:
                print(f"Upload Error: {e}")
                return False

    async def upload_all(self):
        files = [f for f in self.task.files if validate_file(f)[0]]
        if not files:
            return

        self.total = len(files)

        for i, filepath in enumerate(files, 1):
            self.num = i
            file_info = parse_filename(os.path.basename(filepath))
            display_name = file_info['name'] if file_info['name'] else self.task.series_name

            for sub in self.task.subscribers:
                uid, cid = sub['user_id'], sub['chat_id']

                thumb = None
                if uid in db['thumbnails'] and db['thumbnails'][uid]:
                    thumb = smart_thumb_match(display_name, db['thumbnails'][uid])
                    if not thumb:
                        thumb = smart_thumb_match(self.task.series_name, db['thumbnails'][uid])

                    if thumb and 'thumb_last_used' in db:
                        if uid not in db['thumb_last_used']:
                            db['thumb_last_used'][uid] = {}
                        for tname, tid in db['thumbnails'][uid].items():
                            if tid == thumb:
                                db['thumb_last_used'][uid][tname] = time.time()
                                save_db()
                                break

                self.start = None
                self.last = 0

                await self.upload_to_user(cid, filepath, display_name, thumb)
                await asyncio.sleep(1)

# ==========================================
# DELAYED CLEANUP HELPER
# ==========================================
async def delayed_cleanup(task, delay):
    """Cleanup task files after delay - for single downloads only"""
    try:
        await asyncio.sleep(delay)
        task.cleanup()
    except:
        pass

# ==========================================
# PROCESSOR
# ==========================================
async def process_task(task, st):
    global active_downloads

    try:
        task.status = "downloading"
        active_downloads[task.task_id] = task

        res = Resolver(st)

        if "swift.multiquality" in task.url:
            swift = task.url
            print(f"✅ Direct Swift URL provided")
        else:
            print(f"\n{'='*60}")
            print(f"🔗 Episode URL: {task.url}")
            print(f"{'='*60}")
            
            hidden = await res.get_hidden(task.url)
            if not hidden:
                error_msg = f"❌ **Failed: Hidden link not found**\n\n📍 Episode URL not working\n\n🔄 Please try again"
                await st.update(error_msg)
                task.status = "failed"
                print(f"❌ [FAIL] Hidden link extraction failed")
                print(f"{'='*60}\n")
                return False
            
            print(f"✅ Hidden link extracted successfully")

            swift = await res.get_swift(hidden)
            if not swift:
                error_msg = f"❌ **Failed: Swift URL not found**\n\n📍 Could not reach download server\n\n🔄 Please try again"
                await st.update(error_msg)
                task.status = "failed"
                print(f"❌ [FAIL] Swift URL extraction failed")
                print(f"{'='*60}\n")
                return False
            
            print(f"✅ Swift URL extracted successfully")
            print(f"{'='*60}\n")

        dl = Downloader(task, st)
        files = await dl.download(swift)

        if not files:
            await st.update("❌ **Download failed: No files received**")
            task.status = "failed"
            print(f"❌ [FAIL] No files downloaded from Swift")
            return False

        # ==========================================
        # 🔥 NEW: SPLIT LARGE FILES (2GB+)
        # ==========================================
        final_files = []
        for f in files:
            if validate_file(f)[0]:
                file_size = os.path.getsize(f)
                if file_size > MAX_FILE_SIZE:
                    # Split this file
                    print(f"✂️ File needs splitting: {fmt_bytes(file_size)}")
                    split_parts = await split_video(f, task.directory, st)
                    final_files.extend(split_parts)
                else:
                    final_files.append(f)
        
        if not final_files:
            await st.update("❌ **No valid files after processing**")
            task.status = "failed"
            return False

        # Update task files with split files
        task.files = final_files

        # Store file info BEFORE upload
        valid_files = [f for f in final_files if validate_file(f)[0]]
        
        task.valid_files = valid_files
        task.file_sizes = {}
        for f in valid_files:
            try:
                task.file_sizes[f] = os.path.getsize(f)
            except:
                task.file_sizes[f] = 0

        task.status = "uploading"

        up = Uploader(bot, st, task)
        await up.upload_all()

        # Calculate totals from stored info
        total = sum(task.file_sizes.values())
        
        # Build file list with part info
        flist_items = []
        for f in valid_files:
            fname = clean_name(os.path.basename(f))
            quality = get_real_quality(f)
            size = task.file_sizes.get(f, 0)
            
            # Check if it's a split part
            if "_Part" in fname:
                flist_items.append(f"✅ {fname} - {fmt_bytes(size)}")
            else:
                flist_items.append(f"✅ {fname} - {quality}")
        
        flist = "\n".join(flist_items)

        file_info = parse_filename(os.path.basename(valid_files[0])) if valid_files else {}
        display_name = file_info.get('name', task.series_name)

        # Check if files were split
        split_notice = ""
        if any("_Part" in os.path.basename(f) for f in valid_files):
            split_notice = "\n\n✂️ **Note:** Large file was split into parts"

        if file_info.get('is_movie') or task.is_movie:
            complete_msg = f"🎉 **Movie Complete!**\n\n🎬 **{display_name}**\n\n{flist}\n\n💾 **Total:** {fmt_bytes(total)}\n👥 **Sent to:** {len(task.subscribers)} users{split_notice}"
        else:
            ep = file_info.get('episode', task.episode)
            complete_msg = f"🎉 **Episode {ep} Complete!**\n\n📺 **{display_name}**\n\n{flist}\n\n💾 **Total:** {fmt_bytes(total)}\n👥 **Sent to:** {len(task.subscribers)} users{split_notice}"

        await st.update(complete_msg, force=True)

        task.status = "completed"
        task.upload_success = True
        print(f"✅ [SUCCESS] Episode {task.episode} completed\n")
        return True

    except Exception as e:
        print(f"❌ [ERROR] Process task failed: {e}")
        traceback.print_exc()
        await st.send(f"❌ Error: {str(e)[:100]}")
        task.status = "failed"
        task.upload_success = False
        return False
    finally:
        if task.task_id in active_downloads:
            del active_downloads[task.task_id]

        if not hasattr(task, 'is_batch_child') or not task.is_batch_child:
            asyncio.create_task(delayed_cleanup(task, FILE_CLEANUP_TIME))

async def process_batch(task, episodes, st):
    """Process multiple episodes with detailed tracking"""

    # Enable batch mode
    task.batch_mode = True
    task.total_episodes = len(episodes)

    # Results tracking
    results = {
        'success': [],
        'failed': [],
        'total_size': 0,
        'total_files': 0,
        'quality_stats': {'360P': 0, '480P': 0, '720P': 0, '1080P': 0}
    }

    # 🔥 NEW: Track all episode tasks for cleanup
    all_ep_tasks = []

    batch_start_time = time.time()

    try:
        print(f"\n{'='*50}")
        print(f"🎬 BATCH START: {task.series_name}")
        print(f"📊 Total Episodes: {len(episodes)}")
        print(f"{'='*50}\n")

        for idx, (ep_num, ep_url) in enumerate(episodes, 1):
            task.current_episode = idx

            print(f"\n[Batch {idx}/{len(episodes)}] Starting Episode {ep_num}")

            # Progress update
            await st.update(f"""
📥 **Batch Download Progress**

━━━━━━━━━━━━━━━━━━━━━━
📺 **{task.series_name}**
━━━━━━━━━━━━━━━━━━━━━━

🎬 **Current:** Episode {ep_num}
📊 **Progress:** {idx}/{len(episodes)}

✅ Success: {len(results['success'])}
❌ Failed: {len(results['failed'])}
⏳ Remaining: {len(episodes) - idx + 1}
""", force=True)

            try:
                # Create episode task
                ep_task = Task(
                    task.user_id,
                    task.chat_id,
                    "episode",
                    get_content_key(ep_url),
                    ep_url
                )
                ep_task.series_name = task.series_name
                ep_task.episode = ep_num
                ep_task.batch_mode = True
                ep_task.current_episode = idx
                ep_task.total_episodes = len(episodes)
                ep_task.is_batch_child = True

                # 🔥 NEW: Track for cleanup later
                all_ep_tasks.append(ep_task)

                for sub in task.subscribers:
                    ep_task.add_subscriber(sub['user_id'], sub['chat_id'])

                # Process episode
                result = await process_task(ep_task, st)

                # 🔥 FIXED: Use stored file info instead of validating deleted files
                if result and hasattr(ep_task, 'valid_files') and ep_task.valid_files:
                    valid_files = ep_task.valid_files
                    file_sizes = getattr(ep_task, 'file_sizes', {})
                    episode_size = sum(file_sizes.values())

                    results['success'].append((ep_num, valid_files, episode_size))
                    results['total_files'] += len(valid_files)
                    results['total_size'] += episode_size

                    # Quality tracking
                    for f in valid_files:
                        fname = os.path.basename(f).lower()
                        if '1080' in fname: results['quality_stats']['1080P'] += 1
                        elif '720' in fname: results['quality_stats']['720P'] += 1
                        elif '480' in fname: results['quality_stats']['480P'] += 1
                        elif '360' in fname: results['quality_stats']['360P'] += 1

                    print(f"✅ Episode {ep_num} - SUCCESS ({len(valid_files)} files, {fmt_bytes(episode_size)})")
                else:
                    results['failed'].append((ep_num, "Download failed"))
                    print(f"❌ Episode {ep_num} - FAILED")

            except Exception as e:
                results['failed'].append((ep_num, str(e)[:30]))
                print(f"❌ Episode {ep_num} - ERROR: {e}")

            # 🔥 NEW: Cleanup this episode's files after stats collected
            if ep_task:
                ep_task.cleanup()

            # Wait before next
            if idx < len(episodes):
                await st.update(f"✅ **Episode {ep_num} Done!**\n\n⏳ Next episode in 5 seconds...", force=True)
                await asyncio.sleep(5)

        # ========== FINAL SUMMARY ==========
        total_time = time.time() - batch_start_time

        # Build quality breakdown
        quality_lines = ""
        for q, count in results['quality_stats'].items():
            if count > 0:
                quality_lines += f"   🔸 {q}: {count} files\n"

        # Build success list
        success_lines = ""
        
        # ✅ FIX: Build lookup dict ONCE (O(1) instead of O(n³))
        all_file_sizes = {}
        for ep_task in all_ep_tasks:
            if hasattr(ep_task, 'file_sizes'):
                all_file_sizes.update(ep_task.file_sizes)
        
        for ep_num, files, size in results['success']:
            success_lines += f"\n📺 **Episode {ep_num}** ({fmt_bytes(size)})\n"
            for f in files:
                fname = os.path.basename(f)
                q = get_quality(fname)
                # ✅ Simple O(1) lookup
                fsize = all_file_sizes.get(f, 0)
                success_lines += f"   ✓ {q} ({fmt_bytes(fsize)})\n"

        # Build failed list
        failed_lines = ""
        if results['failed']:
            failed_lines = "\n━━━━━━━━━━━━━━━━━━━━━━\n⚠️ **FAILED EPISODES**\n━━━━━━━━━━━━━━━━━━━━━━\n"
            for ep_num, error in results['failed']:
                failed_lines += f"   ❌ Episode {ep_num} - {error}\n"

        # Final message
        summary = f"""
🎉 **Batch Download Complete!**

━━━━━━━━━━━━━━━━━━━━━━
📺 **{task.series_name}**
━━━━━━━━━━━━━━━━━━━━━━

📊 **BATCH STATISTICS**
━━━━━━━━━━━━━━━━━━━━━━
🎬 Episodes: {episodes[0][0]} to {episodes[-1][0]}
📈 Total: {len(episodes)} episodes

✅ Success: {len(results['success'])}
❌ Failed: {len(results['failed'])}

━━━━━━━━━━━━━━━━━━━━━━
📦 **DOWNLOAD SUMMARY**
━━━━━━━━━━━━━━━━━━━━━━
📁 Total Files: {results['total_files']}
💾 Total Size: {fmt_bytes(results['total_size'])}

**Quality Breakdown:**
{quality_lines}
━━━━━━━━━━━━━━━━━━━━━━
✅ **DELIVERED EPISODES**
━━━━━━━━━━━━━━━━━━━━━━
{success_lines}
{failed_lines}
━━━━━━━━━━━━━━━━━━━━━━
👥 Sent to: {len(task.subscribers)} user(s)
⏱️ Total Time: {fmt_time(total_time)}
━━━━━━━━━━━━━━━━━━━━━━
"""

        if len(results['success']) == len(episodes):
            summary += "\n🎊 **All episodes sent successfully!**"

        await st.update(summary, force=True)

        print(f"\n{'='*50}")
        print(f"🎉 BATCH COMPLETE - Success: {len(results['success'])}, Failed: {len(results['failed'])}")
        print(f"{'='*50}\n")

        return True

    except Exception as e:
        print(f"Batch Error: {e}")
        traceback.print_exc()
        await st.send(f"❌ Batch Error: {str(e)[:100]}")
        return False
    finally:
        # 🔥 Cleanup any remaining tasks
        for ep_task in all_ep_tasks:
            try:
                ep_task.cleanup()
            except:
                pass


# ==========================================
# DOWNLOAD WORKERS
# ==========================================
async def download_worker(wid):
    global download_semaphore, batch_semaphore, queue_lock

    worker_status[wid] = "idle"
    print(f"[Download Worker {wid}] Started")

    while True:
        try:
            if _sf or _rf:
                break

            t = None
            async with queue_lock:
                if task_queue:
                    t = task_queue.popleft()

            if t is None:
                await asyncio.sleep(1)
                continue

            if t['type'] == 'batch':
                semaphore = batch_semaphore
            else:
                semaphore = download_semaphore

            async with semaphore:
                worker_status[wid] = "working"
                task_id = t['task'].task_id
                active_downloads[task_id] = t['task']
                print(f"[Worker {wid}] Processing: {task_id}")

                try:
                    if t['type'] == 'single':
                        success = await process_task(t['task'], t['status'])
                    elif t['type'] == 'batch':
                        success = await process_batch(t['task'], t['episodes'], t['status'])
                    else:
                        success = False

                except Exception as e:
                    print(f"[Worker {wid}] Error: {e}")
                    traceback.print_exc()
                    try:
                        for sub in t['task'].subscribers:
                            await bot.send_message(sub['chat_id'], f"❌ Error: {str(e)[:100]}")
                    except:
                        pass

                finally:
                    worker_status[wid] = "idle"
                    if task_id in active_downloads:
                        del active_downloads[task_id]

        except Exception as e:
            print(f"[Worker {wid}] Loop error: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)
    

# ==========================================
# MONITOR - FIXED VERSION
# ==========================================
async def monitor():
    while True:
        try:
            if _sf or _rf:
                break

            for key, data in list(db['monitored'].items()):
                try:
                    intv = data.get('interval', 3)
                    lc = data.get('last_check', 0)

                    if time.time() - lc < intv * 60:
                        continue

                    db['monitored'][key]['last_check'] = time.time()

                    f = Finder()

                    # ==========================================
                    # 🔥 FIXED: New get_info returns 4 values now!
                    # OLD: title, latest, eps
                    # NEW: title, last_season, latest_ep, eps
                    # ==========================================
                    title, last_season, latest_ep, eps = await f.get_info(data['series_url'])

                    if not title or not eps:
                        continue

                    # ==========================================
                    # 🔥 Get monitored season info from database
                    # ==========================================
                    monitored_season = data.get('last_season', 1)
                    last_tracked_ep = data.get('last_episode', 0)

                    # ==========================================
                    # 🔥 FIXED: eps is now [(season, episode, url), ...]
                    # Group episodes by season
                    # ==========================================
                    season_eps = {}
                    for s, e, u in eps:  # 🔥 CHANGED: 3 values now!
                        if s not in season_eps:
                            season_eps[s] = []
                        season_eps[s].append((e, u))

                    # ==========================================
                    # 🔥 Check if monitored season has new episodes
                    # ==========================================
                    if monitored_season in season_eps:
                        season_eps[monitored_season].sort(key=lambda x: x[0])
                        latest_in_season = season_eps[monitored_season][-1][0]
                        latest_url = season_eps[monitored_season][-1][1]

                        if latest_in_season > last_tracked_ep:
                            # ✅ NEW EPISODE FOUND!
                            print(f"🎉 NEW EPISODE: {title} S{monitored_season}E{latest_in_season}")

                            content_key = get_content_key(latest_url, title, latest_in_season)

                            task = Task(
                                data['subscribers'][0]['user_id'],
                                data['subscribers'][0]['chat_id'],
                                "episode",
                                content_key,
                                latest_url
                            )
                            task.series_name = title
                            task.episode = latest_in_season

                            for sub in data['subscribers'][1:]:
                                task.add_subscriber(sub['user_id'], sub['chat_id'])

                            # Notify all subscribers
                            for s in data['subscribers']:
                                try:
                                    await bot.send_message(
                                        s['chat_id'],
                                        f"🎉 **NEW EPISODE!**\n\n📺 **{title}**\n🏝️ Season {monitored_season}\n🎬 Episode {latest_in_season}\n\n📥 Downloading..."
                                    )
                                except:
                                    pass

                            await asyncio.sleep(220)

                            st = Status(bot, data['subscribers'][0]['chat_id'])
                            await st.create("📥 **Processing new episode...**")

                            add_to_queue('single', task, st)

                            # Update database
                            db['monitored'][key]['last_episode'] = latest_in_season
                            db['monitored'][key]['last_season'] = monitored_season  # ✅ FIX: Save season too
                            save_db()

                    # ==========================================
                    # 🔥 NEW: Check if NEW SEASON started!
                    # ==========================================
                    if last_season > monitored_season:
                        # New season detected!
                        print(f"🆕 NEW SEASON: {title} Season {last_season}")

                        # Notify subscribers about new season
                        for s in data['subscribers']:
                            try:
                                await bot.send_message(
                                    s['chat_id'],
                                    f"🆕 **NEW SEASON DETECTED!**\n\n📺 **{title}**\n🏝️ Season {last_season} is now available!\n\n💡 Use `/set {data['series_url']}` to monitor new season"
                                )
                            except:
                                pass

                except Exception as e:
                    print(f"Monitor error for {key}: {e}")
                    traceback.print_exc()

            await asyncio.sleep(60)

        except Exception as e:
            print(f"Monitor loop error: {e}")
            await asyncio.sleep(60)

# ==========================================
# AUTO CLEANUP
# ==========================================
async def auto_cleanup():
    while True:
        try:
            if _sf or _rf:
                break

            now = time.time()

            for task_id, task in list(active_downloads.items()):
                if now - task.created_at > WORKER_STUCK_TIME:
                    if task.status not in ["completed", "failed"]:
                        task.status = "timeout"
                        for sub in task.subscribers:
                            try:
                                await bot.send_message(sub['chat_id'], "❌ Your download timed out. Please try again.")
                            except:
                                pass
                        task.cleanup()
                        del active_downloads[task_id]

            for d in [DOWNLOAD_DIR, THUMB_DIR]:
                if os.path.exists(d):
                    for f in os.listdir(d):
                        try:
                            fp = os.path.join(d, f)
                            if os.path.isfile(fp) and now - os.path.getmtime(fp) > FILE_CLEANUP_TIME:
                                os.unlink(fp)
                        except:
                            pass

            await asyncio.sleep(300)
        except:
            await asyncio.sleep(60)

async def thumb_cleanup():
    while True:
        try:
            if _sf or _rf:
                break

            now = time.time()
            inactive_time = THUMB_INACTIVE_DAYS * 24 * 60 * 60

            for uid in list(db.get('thumb_last_used', {}).keys()):
                for anime in list(db['thumb_last_used'].get(uid, {}).keys()):
                    last_used = db['thumb_last_used'][uid].get(anime, 0)
                    if now - last_used > inactive_time:
                        if uid in db['thumbnails'] and anime in db['thumbnails'][uid]:
                            del db['thumbnails'][uid][anime]
                            del db['thumb_last_used'][uid][anime]
                            save_db()
                            try:
                                await bot.send_message(uid, f"🗑️ **Thumbnail Deleted**\n\nYour thumbnail for **\"{anime}\"** was deleted due to 30 days inactivity.")
                            except:
                                pass

            await asyncio.sleep(86400)
        except:
            await asyncio.sleep(3600)

# ==========================================
# PREMIUM EXPIRY MONITOR WITH AUTO CLEANUP
# ==========================================
async def premium_expiry_monitor():
    """Monitor premium expiry and auto-cleanup after 7 hours"""
    while True:
        try:
            if _sf or _rf:
                break

            now = time.time()
            TWO_HOURS = 2 * 60 * 60
            SEVEN_HOURS = 7 * 60 * 60

            for uid, data in list(db.get('premium_users', {}).items()):
                # Skip owner/admin
                if is_owner(uid) or is_admin(uid):
                    continue

                expiry = data.get('expires', 0)
                time_left = expiry - now
                time_since_expiry = now - expiry

                # ==========================================
                # 1️⃣ MESSAGE: 2 Hours Before Expiry
                # ==========================================
                if 0 < time_left <= TWO_HOURS and not data.get('notified_2h'):
                    try:
                        hours = int(time_left / 3600)
                        minutes = int((time_left % 3600) / 60)
                        
                        await bot.send_message(uid, f"""
⚠️ **Premium Expiring Soon!**

━━━━━━━━━━━━━━━━━━━━━━
⏰ **Time Left:** {hours}h {minutes}m

🔔 Your premium will expire soon!

━━━━━━━━━━━━━━━━━━━━━━
⚠️ **IMPORTANT WARNING:**

If you don't renew within **7 hours** after expiry, all your data will be **DELETED**:

🗑️ **Will be removed:**
   • All monitored anime (/set URLs)
   • All custom thumbnails
   • All settings

━━━━━━━━━━━━━━━━━━━━━━
💎 **Renew Now:**
Contact: @Wejdufjcjcjc_bot

🔄 **Don't lose your data!**
━━━━━━━━━━━━━━━━━━━━━━
""")
                        db['premium_users'][uid]['notified_2h'] = True
                        save_db()
                    except Exception as e:
                        print(f"Failed to send 2h warning to {uid}: {e}")

                # ==========================================
                # 2️⃣ MESSAGE: At Expiry Time
                # ==========================================
                # ✅ FIX: Wider window (-5min to +5min) to catch even if bot restarts
                elif -300 <= time_since_expiry <= 300 and not data.get('notified_expiry'):
                    try:
                        await bot.send_message(uid, f"""
🚫 **Premium Expired!**

━━━━━━━━━━━━━━━━━━━━━━
⏰ Your premium has expired now.

━━━━━━━━━━━━━━━━━━━━━━
⚠️ **CRITICAL WARNING:**

⏳ You have **7 HOURS** to renew!

If you don't renew premium in 7 hours, **ALL YOUR DATA** will be permanently deleted:

📋 **Your Current Data:**
   🎬 Monitored Anime: {len([d for d in db['monitored'].values() if any(s['user_id'] == uid for s in d['subscribers'])])}
   🖼️ Thumbnails: {len(db.get('thumbnails', {}).get(uid, {}))}

━━━━━━━━━━━━━━━━━━━━━━
💎 **Renew Immediately:**
Contact: @Wejdufjcjcjc_bot

⏰ Deadline: 7 hours from now
🗑️ After that = AUTO DELETE
━━━━━━━━━━━━━━━━━━━━━━
""")
                        db['premium_users'][uid]['notified_expiry'] = True
                        save_db()
                    except Exception as e:
                        print(f"Failed to send expiry msg to {uid}: {e}")

                # ==========================================
                # 3️⃣ AUTO DELETE: 7 Hours After Expiry
                # ==========================================
                elif time_since_expiry >= SEVEN_HOURS and not data.get('notified_cleanup'):
                    print(f"🗑️ Auto-deleting data for expired user: {uid}")
                    
                    # Collect data before deletion
                    deleted_anime = []
                    deleted_thumbs = []
                    
                    # Remove monitored anime
                    for key, mon_data in list(db['monitored'].items()):
                        mon_data['subscribers'] = [s for s in mon_data['subscribers'] if s['user_id'] != uid]
                        if not mon_data['subscribers']:
                            deleted_anime.append(mon_data['series_name'])
                            del db['monitored'][key]
                        elif any(s['user_id'] == uid for s in mon_data.get('subscribers', [])):
                            deleted_anime.append(mon_data['series_name'])
                    
                    # Remove thumbnails
                    if uid in db.get('thumbnails', {}):
                        deleted_thumbs = list(db['thumbnails'][uid].keys())
                        del db['thumbnails'][uid]
                    
                    if uid in db.get('thumb_last_used', {}):
                        del db['thumb_last_used'][uid]
                    
                    # Remove premium entry
                    del db['premium_users'][uid]
                    save_db()
                    
                    # Send final message with list
                    try:
                        anime_list = "\n".join([f"   • {name}" for name in deleted_anime]) if deleted_anime else "   • None"
                        thumb_list = "\n".join([f"   • {name}" for name in deleted_thumbs]) if deleted_thumbs else "   • None"
                        
                        await bot.send_message(uid, f"""
🗑️ **Data Cleanup Complete**

━━━━━━━━━━━━━━━━━━━━━━
⏰ 7 hours passed since premium expired.

All your data has been **permanently deleted** as per policy.

━━━━━━━━━━━━━━━━━━━━━━
📋 **DELETED DATA:**

🎬 **Monitored Anime ({len(deleted_anime)}):**
{anime_list}

🖼️ **Thumbnails ({len(deleted_thumbs)}):**
{thumb_list}

━━━━━━━━━━━━━━━━━━━━━━
💎 **Want to Continue?**

Renew premium to use bot again:
Contact: @Wejdufjcjcjc_bot

✨ After renewal, you can set up everything again!
━━━━━━━━━━━━━━━━━━━━━━
""")
                    except Exception as e:
                        print(f"Failed to send cleanup msg to {uid}: {e}")

            await asyncio.sleep(300)  # Check every 5 minutes

        except Exception as e:
            print(f"Premium monitor error: {e}")
            traceback.print_exc()
            await asyncio.sleep(300)

# ==========================================
# BACKUP SCHEDULER
# ==========================================
async def backup_scheduler():
    """Run backup daily at 1:30 AM"""
    while True:
        try:
            if _sf or _rf:
                break
            
            now = datetime.now()
            
            # Calculate next backup time using config
            target = now.replace(hour=BACKUP_HOUR, minute=BACKUP_MINUTE, second=0, microsecond=0)
            if now > target:
                target += timedelta(days=1)
            
            wait_seconds = (target - now).total_seconds()
            
            print(f"⏰ Next backup in {fmt_time(wait_seconds)}")
            await asyncio.sleep(wait_seconds)
            
            # Run backup
            print("📦 Starting daily backup...")
            zip_files, stats = await create_backup()
            
            if zip_files:
                success = await send_backup_to_channel(zip_files, stats)
                if success:
                    print("✅ Backup sent successfully!")
                    # Cleanup old backups
                    await cleanup_old_backups()
                else:
                    print("❌ Backup send failed!")
            else:
                print("❌ Backup creation failed!")
            
            # Wait a bit before next cycle
            await asyncio.sleep(60)
            
        except Exception as e:
            print(f"Backup scheduler error: {e}")
            traceback.print_exc()
            await asyncio.sleep(3600)  # Wait 1 hour on error

# ==========================================
# DUPLICATE CHECK
# ==========================================
def is_already_monitored(user_id, url):
    key = get_key(url)
    if not key:
        return False, None
    for k, data in db['monitored'].items():
        if k == key:
            for sub in data['subscribers']:
                if sub['user_id'] == user_id:
                    return True, data['series_name']
    return False, None

# ==========================================
# AUTO URL HANDLER - FIXED VERSION
# ==========================================
async def handle_url(c, m):
    uid = m.from_user.id
    cid = m.chat.id
    url = m.text.strip()

    if is_banned(uid):
        return await m.reply("🚫 Banned")

    # Force sub check
    is_joined, fsub_markup = await check_force_sub(c, uid)
    if not is_joined:
        return await m.reply("🔒 **Join channel first!**\n\nUse /start", reply_markup=fsub_markup)

    if not await premium_check(m):
        return
        
    if not await cooldown_check(m):
        return

    update_cooldown(uid)

    st = Status(c, cid)
    msg = await st.create("🔍 **Detecting URL...**")

    try:
        content_key = get_content_key(url)
        content_type = "swift" if "swift.multiquality" in url else "episode" if "/episode/" in url else "movie" if "/movies/" in url else "series"

        task = Task(uid, cid, content_type, content_key, url)

        # ==========================================
        # SWIFT URL
        # ==========================================
        if "swift.multiquality" in url:
            await msg.edit("🚀 **Swift URL Detected!**")
            success, message = add_to_queue('single', task, st)
            if "existing" in message.lower():
                await msg.edit(f"📥 **Same content downloading...**\n\n{message}")
            else:
                await msg.edit(f"📥 **Queued!** {message}")
            return

        # ==========================================
        # EPISODE URL - FIXED with retry
        # ==========================================
        if "/episode/" in url:
            await msg.edit("🎬 **Episode URL Detected!**")

            try:
                response = safe_request(url)
                soup = BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                return await msg.edit(f"❌ **Connection Error**\n\n🔄 Please try again in a few seconds.\n\nError: {str(e)[:50]}")

            h1 = soup.find("h1")
            raw_title = h1.get_text(strip=True) if h1 else "Unknown"  # ← YAHAN CHANGE!
            task.series_name = clean_page_title(raw_title)
            
            ep_match = re.search(r'-(\d+)x(\d+)', url) or re.search(r'episode[/-](\d+)', url, re.I)
            if ep_match:
                task.episode = int(ep_match.group(2) if ep_match.lastindex == 2 else ep_match.group(1))

            success, message = add_to_queue('single', task, st)
            await msg.edit(f"📥 **{message}**")
            return


        # ==========================================
        # SERIES URL - FIXED (4 values + correct tuple unpacking)
        # ==========================================
        if "/series/" in url:
            await msg.edit("📺 **Series URL Detected!**")

            f = Finder(st)
            # ✅ FIXED: Now receiving 4 values
            title, last_season, latest_ep, eps = await f.get_info(url)

            if not title:
                return await msg.edit("❌ Could not fetch series info")

            if not eps:
                return await msg.edit("❌ No episodes found!")

            # ✅ FIXED: Find latest episode URL (eps is now list of (season, episode, url))
            ep_url = None
            for s, e, u in eps:
                if s == last_season and e == latest_ep:
                    ep_url = u
                    break

            # Fallback: last episode
            if not ep_url and eps:
                ep_url = eps[-1][2]
                last_season = eps[-1][0]
                latest_ep = eps[-1][1]

            if ep_url:
                task.url = ep_url
                task.content_key = get_content_key(ep_url, title, latest_ep)
                task.series_name = title
                task.episode = latest_ep

                success, message = add_to_queue('single', task, st)
                await msg.edit(f"📺 **{title}**\n🏝️ Season {last_season} | Episode {latest_ep}\n\n📥 **{message}**")
            else:
                await msg.edit("❌ Could not find episode URL")
            return

        # ==========================================
        # MOVIE URL - FIXED with retry
        # ==========================================
        if "/movies/" in url:
            await msg.edit("🎥 **Movie URL Detected!**")

            try:
                response = safe_request(url)
                soup = BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                return await msg.edit(f"❌ **Connection Error**\n\n🔄 Please try again in a few seconds.\n\nError: {str(e)[:50]}")

            h1 = soup.find("h1")
            raw_title = h1.get_text(strip=True) if h1 else "Unknown Movie"  # ← YAHAN CHANGE!
            task.series_name = clean_page_title(raw_title)
            task.is_movie = True

            success, message = add_to_queue('single', task, st)
            await msg.edit(f"🎥 **{task.series_name}**\n\n📥 **{message}**")
            return

        await msg.edit("❌ **Unknown URL format**\n\nSupported:\n• Series URL\n• Episode URL\n• Movie URL\n• Swift URL")

    except Exception as e:
        print(f"URL Error: {e}")
        traceback.print_exc()
        await msg.edit(f"❌ **Error:** {str(e)[:100]}\n\n🔄 Please try again")

# ==========================================
# COMMANDS - ORIGINAL /start MESSAGE
# ==========================================
@bot.on_message(filters.command("start"))
async def cmd_start(c, m):
    uid = m.from_user.id
    name = m.from_user.first_name
    
    # Force subscription check
    is_joined, fsub_markup = await check_force_sub(c, uid)
    if not is_joined:
        return await m.reply(f"""
🔒 **Access Restricted!**

━━━━━━━━━━━━━━━━━━━━━━
Hi **{name}**! 👋

To use this bot, please join our channel first:

━━━━━━━━━━━━━━━━━━━━━━
👇 **Click button below to join:**
""", reply_markup=fsub_markup)
    
    # Check for referral
    if len(m.command) > 1 and m.command[1].startswith('ref_'):
        ref_code = m.command[1].replace('ref_', '')
        
        # Process referral
        success, result = process_referral(uid, ref_code)
        
        if success:
            await m.reply(f"""🎉 **WELCOME BONUS!**

━━━━━━━━━━━━━━━━━━━━━━
✅ You joined via referral!

🎁 **Your Reward:** {result['new_user_days']} day Premium!
👤 **Referrer Reward:** {result['referrer_days']} days Premium!

━━━━━━━━━━━━━━━━━━━━━━
💎 Premium activated!
Use /start to see all features.
━━━━━━━━━━━━━━━━━━━━━━
""")
            
            # Notify referrer
            try:
                referrer_id = int(ref_code.replace('RJ_', ''))
                cycle_count = result['cycle_count']
                
                await c.send_message(referrer_id, f"""🎉 **NEW REFERRAL!**

━━━━━━━━━━━━━━━━━━━━━━
👤 {name} joined using your link!

🎁 **Your Reward:** +{result['referrer_days']} days Premium!
📊 **Cycle Progress:** {cycle_count}/3

{"🎊 Cycle complete! Next referral starts new cycle." if cycle_count >= 3 else f"👉 {3 - cycle_count} more for 7 days!"}
━━━━━━━━━━━━━━━━━━━━━━
""")
            except:
                pass
    
    if uid not in db['users']:
        db['users'][uid] = {'joined': datetime.now().isoformat(), 'interval': 3}
        save_db()

    # Check premium status
    is_premium_user = is_premium(uid)
    
    # Role-based buttons
    keyboard = get_help_buttons(uid)
    
    # Premium status
    if is_owner(uid):
        premium_status = "👑 **Owner** (Lifetime)"
    elif is_admin(uid):
        premium_status = "⚔️ **Admin** (Lifetime)"
    elif is_master(uid):
        premium_status = "🔱 **Master** (Full Access)"
    elif is_premium_user:
        days_left = get_premium_days_left(uid)
        premium_status = f"💎 **Premium** ({days_left} days left)"
    else:
        premium_status = "🆓 **Free User**"

    # Non-premium users
    if not is_premium_user:
        await m.reply(f"""
╔═══════════════════════╗
║  🎌 ANIME AUTO DOWNLOADER  ║
╚═══════════════════════╝

Namaste **{name}**! 👋

━━━━━━━━━━━━━━━━━━━━━━
⚠️ **PREMIUM REQUIRED**
━━━━━━━━━━━━━━━━━━━━━━

🔒 This bot is **Premium Only**

💎 **Get Access:**
👉 @Wejdufjcjcjc_bot
👉 https://t.me/auto_uploading

━━━━━━━━━━━━━━━━━━━━━━
✨ **Premium Features:**
━━━━━━━━━━━━━━━━━━━━━━
🎬 Auto monitoring
📥 Batch downloads (10 eps)
🖼️ Custom thumbnails
⚡ Fast Downloads
🎯 All qualities
🔔 Episode alerts
🌐 Website Support: Toono.in
━━━━━━━━━━━━━━━━━━━━━━
📞 Contact: **@Wejdufjcjcjc_bot**

༺═━━━ {{ ⚜ }} ━━━═༻
     **👑 Developed by RJ**
༺═━━━ {{ ⚜ }} ━━━═༻
""", reply_markup=keyboard)
        return

    # Premium users - SHORT MESSAGE with BUTTONS
    await m.reply(f"""
╔══════════════════════╗
║ 🎌 ANIME AUTO DOWNLOADER ║
╚══════════════════════╝

Namaste **{name}**! 👋
{role(uid)}

━━━━━━━━━━━━━━━━━━━━━━
💎 {premium_status}
━━━━━━━━━━━━━━━━━━━━━━

🌐 **Website Support:** Toono.in
⏳ **Cooldown:** {COOLDOWN_TIME}s
🚀 **Auto URL Detection Active!**

━━━━━━━━━━━━━━━━━━━━━━
👇 **Choose an option below:**

༺═━━━ {{ ⚜ }} ━━━═༻
     **👑 Developed by RJ**
༺═━━━ {{ ⚜ }} ━━━═༻
""", reply_markup=keyboard)

# ==========================================
# FORCE SUB CALLBACK HANDLER
# ==========================================
@bot.on_callback_query(filters.regex("^check_fsub$"))
async def cb_check_fsub(c, q):
    """Handle 'Joined? Click Here' button"""
    uid = q.from_user.id
    
    is_joined, fsub_markup = await check_force_sub(c, uid)
    
    if is_joined:
        await q.message.delete()
        
        # Trigger /start manually
        if uid not in db['users']:
            db['users'][uid] = {'joined': datetime.now().isoformat(), 'interval': 3}
            save_db()
        
        name = q.from_user.first_name
        is_premium_user = is_premium(uid)
        keyboard = get_help_buttons(uid)
        
        if is_owner(uid):
            premium_status = "👑 **Owner** (Lifetime)"
        elif is_admin(uid):
            premium_status = "⚔️ **Admin** (Lifetime)"
        elif is_master(uid):
            premium_status = "🔱 **Master** (Full Access)"
        elif is_premium_user:
            days_left = get_premium_days_left(uid)
            premium_status = f"💎 **Premium** ({days_left} days left)"
        else:
            premium_status = "🆓 **Free User**"
        
        if not is_premium_user:
            await c.send_message(uid, f"""
╔═══════════════════════╗
║  🎌 ANIME AUTO DOWNLOADER  ║
╚═══════════════════════╝

✅ **Verification Successful!**

Namaste **{name}**! 👋

━━━━━━━━━━━━━━━━━━━━━━
⚠️ **PREMIUM REQUIRED**
━━━━━━━━━━━━━━━━━━━━━━

🔒 This bot is **Premium Only**

💎 **Get Access:**
👉 @Wejdufjcjcjc_bot

༺═━━━ {{ ⚜ }} ━━━═༻
     **👑 Developed by RJ**
༺═━━━ {{ ⚜ }} ━━━═༻
""", reply_markup=keyboard)
        else:
            await c.send_message(uid, f"""
╔══════════════════════╗
║ 🎌 ANIME AUTO DOWNLOADER ║
╚══════════════════════╝

✅ **Verification Successful!**

Namaste **{name}**! 👋
{role(uid)}

━━━━━━━━━━━━━━━━━━━━━━
💎 {premium_status}
━━━━━━━━━━━━━━━━━━━━━━

🌐 **Website Support:** Toono.in
⏳ **Cooldown:** {COOLDOWN_TIME}s

👇 **Choose an option below:**

༺═━━━ {{ ⚜ }} ━━━═༻
     **👑 Developed by RJ**
༺═━━━ {{ ⚜ }} ━━━═༻
""", reply_markup=keyboard)
        
        await q.answer("✅ Verified!")
    else:
        await q.answer("❌ Please join the channel first!", show_alert=True)

# ==========================================
# BUTTON CALLBACK HANDLER
# ==========================================
@bot.on_callback_query(filters.regex(r"^(help_user|help_admin|help_owner|help_master|back_main)$"))
async def button_handler(c, q):
    """Handle help button clicks"""
    uid = q.from_user.id
    data = q.data
    
    # Back button
    back_btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_main")]
    ])
    
    try:
        # User Commands - Everyone
        if data == "help_user":
            await q.message.edit(HELP_USER, reply_markup=back_btn)
        
        # Admin Panel - Check permission
        elif data == "help_admin":
            if is_admin(uid) or is_owner(uid) or is_master(uid):
                await q.message.edit(HELP_ADMIN, reply_markup=back_btn)
            else:
                await q.answer("❌ Admin access required!", show_alert=True)
        
        # Owner Panel - Check permission
        elif data == "help_owner":
            if is_owner(uid) or is_master(uid):
                await q.message.edit(HELP_OWNER, reply_markup=back_btn)
            else:
                await q.answer("❌ Owner access required!", show_alert=True)
        
        # Master Control - Check permission
        elif data == "help_master":
            if is_master(uid):
                await q.message.edit(HELP_MASTER, reply_markup=back_btn)
            else:
                await q.answer("❌ Master access required!", show_alert=True)
        
        # Back to main menu
        elif data == "back_main":
            keyboard = get_help_buttons(uid)
            
            # Premium status
            if is_owner(uid):
                premium_status = "👑 **Owner** (Lifetime)"
            elif is_admin(uid):
                premium_status = "⚔️ **Admin** (Lifetime)"
            elif is_master(uid):
                premium_status = "🔱 **Master** (Full Access)"
            elif is_premium(uid):
                days_left = get_premium_days_left(uid)
                premium_status = f"💎 **Premium** ({days_left} days left)"
            else:
                premium_status = "🆓 **Free User**"
            
            name = q.from_user.first_name
            
            await q.message.edit(f"""
╔══════════════════════╗
║ 🎌 ANIME AUTO DOWNLOADER ║
╚══════════════════════╝

Namaste **{name}**! 👋
{role(uid)}

━━━━━━━━━━━━━━━━━━━━━━
💎 {premium_status}
━━━━━━━━━━━━━━━━━━━━━━

🌐 **Website Support:** Toono.in
⏳ **Cooldown:** {COOLDOWN_TIME}s
🚀 **Auto URL Detection Active!**

━━━━━━━━━━━━━━━━━━━━━━
👇 **Choose an option below:**

༺═━━━ {{ ⚜ }} ━━━═༻
     **👑 Developed by RJ**
༺═━━━ {{ ⚜ }} ━━━═༻
""", reply_markup=keyboard)
        
        # Answer callback
        await q.answer()
        
    except Exception as e:
        print(f"Callback error: {e}")
        await q.answer("❌ Error occurred!", show_alert=True)

@bot.on_message(filters.command("set"))
async def cmd_set(c, m):
    uid = m.from_user.id
    if is_banned(uid): return await m.reply("🚫 Banned")
    if not await premium_check(m): return
    if not await cooldown_check(m): return
    if len(m.command) < 2: return await m.reply("❌ `/set URL`")

    url = m.command[1]
    if "/series/" not in url: return await m.reply("❌ Invalid URL")

    already, name = is_already_monitored(uid, url)
    if already: return await m.reply(f"⚠️ Already monitoring **{name}**")

    update_cooldown(uid)
    st = Status(c, m.chat.id)
    msg = await st.create("⏳ **Setting up...**")

    f = Finder(st)
    title, last_season, latest_ep, eps = await f.get_info(url)

    if not title:
        return await msg.edit("❌ Could not fetch")

    if not eps:
        return await msg.edit("❌ No episodes found!")

    key = get_key(url)
    intv = db['users'].get(uid, {}).get('interval', 3)

    # ==========================================
    # 🔥 FIXED: Find latest episode URL from last season
    # ==========================================
    latest_url = None
    for s, e, u in eps:
        if s == last_season and e == latest_ep:
            latest_url = u
            break

    if not latest_url:
        # Fallback: last episode in list
        latest_url = eps[-1][2]
        last_season = eps[-1][0]
        latest_ep = eps[-1][1]

    if key in db['monitored']:
        if not any(s['user_id'] == uid for s in db['monitored'][key]['subscribers']):
            db['monitored'][key]['subscribers'].append({'user_id': uid, 'chat_id': m.chat.id})
            save_db()
        return await msg.edit(f"✅ Added to **{title}**")

    db['monitored'][key] = {
        'series_name': title,
        'series_url': url,
        'last_episode': latest_ep,
        'last_season': last_season,
        'interval': intv,
        'last_check': 0,
        'subscribers': [{'user_id': uid, 'chat_id': m.chat.id}]
    }
    save_db()

    await msg.edit(f"""✅ **Monitoring Started!**

📺 **{title}**
🏝️ **Season:** {last_season}
🎬 **Episode:** {latest_ep}

📥 Downloading latest episode...""")

    # Download latest episode
    task = Task(uid, m.chat.id, "episode", get_content_key(latest_url, title, latest_ep), latest_url)
    task.series_name = title
    task.episode = latest_ep
    add_to_queue('single', task, st)

@bot.on_message(filters.command("batch"))
async def cmd_batch(c, m):
    uid = m.from_user.id
    if is_banned(uid): return await m.reply("🚫 Banned")
    if not await premium_check(m): return
    if not await cooldown_check(m): return
    if len(m.command) < 3: return await m.reply("❌ Format: `/batch URL S01 Ep 1-5` ya `/batch URL 1-5`")

    url = m.command[1]

    if "/series/" not in url:
        return await m.reply("❌ Series URL chahiye!\n\nExample:\n`/batch https://toono.in/series/anime S03 Ep 2-7`")

    # ==========================================
    # 🔥 Parse Season + Episode Range
    # ==========================================
    full_args = " ".join(m.command[2:])

    season_num = None
    start_ep = None
    end_ep = None

    # Parse season (S03, S3, s03, etc.)
    season_match = re.search(r'[Ss](\d+)', full_args)
    if season_match:
        season_num = int(season_match.group(1))

    # Parse episode range
    ep_match = re.search(r'(?:Ep\s*)?(\d+)\s*(?:-|to)\s*(\d+)', full_args, re.I)
    if ep_match:
        start_ep = int(ep_match.group(1))
        end_ep = int(ep_match.group(2))
        if end_ep < start_ep:
            start_ep, end_ep = end_ep, start_ep
    else:
        try:
            p = m.command[2].split("-")
            start_ep = int(p[0])
            end_ep = int(p[1])
            if end_ep < start_ep:
                start_ep, end_ep = end_ep, start_ep
        except:
            return await m.reply("❌ Invalid format!\n\nExamples:\n• `/batch URL S03 Ep 2-7`\n• `/batch URL S3 5-10`\n• `/batch URL 1-5`")

    if start_ep is None or end_ep is None:
        return await m.reply("❌ Episode range nahi mila!")

    total_requested = end_ep - start_ep + 1
    if total_requested > MAX_BATCH:
        return await m.reply(f"❌ Max {MAX_BATCH} episodes allowed!")

    update_cooldown(uid)
    st = Status(c, m.chat.id)

    if season_num:
        msg = await st.create(f"⏳ **Fetching Season {season_num}, Episodes {start_ep} to {end_ep}...**")
    else:
        msg = await st.create(f"⏳ **Fetching Episodes {start_ep} to {end_ep}...**")

    try:
        f = Finder(st)
        title, last_season, latest_ep, all_eps = await f.get_info(url)

        if not title:
            return await msg.edit("❌ Series fetch nahi ho payi!")

        if not all_eps:
            return await msg.edit("❌ Koi episode nahi mila!")

        # ==========================================
        # 🔥 FIXED: Filter by Season + Episode Range
        # ==========================================
        download_eps = []

        # If no season specified, use last season
        if season_num is None:
            season_num = last_season
            await msg.edit(f"ℹ️ Season not specified, using Season {season_num}")
            await asyncio.sleep(1)

        for s, e, u in all_eps:
            if s == season_num and start_ep <= e <= end_ep:
                download_eps.append((e, u))  # (episode, url) for process_batch

        # Sort by episode
        download_eps.sort(key=lambda x: x[0])

        if not download_eps:
            # Show available seasons and episodes
            seasons_info = {}
            for s, e, u in all_eps:
                if s not in seasons_info:
                    seasons_info[s] = []
                seasons_info[s].append(e)

            available_str = ""
            for s in sorted(seasons_info.keys()):
                eps_list = sorted(seasons_info[s])
                if len(eps_list) > 10:
                    available_str += f"\n**Season {s}:** Ep {eps_list[0]}-{eps_list[-1]} ({len(eps_list)} eps)"
                else:
                    available_str += f"\n**Season {s}:** Ep {', '.join(map(str, eps_list))}"

            return await msg.edit(f"""❌ **Season {season_num}, Episodes {start_ep}-{end_ep} nahi mile!**

📺 **{title}**

✅ **Available:**{available_str}

💡 Sahi season aur range daalo!""")

        # Check for missing episodes
        found_nums = [e for e, u in download_eps]
        missing = [str(i) for i in range(start_ep, end_ep + 1) if i not in found_nums]

        missing_msg = ""
        if missing:
            if len(missing) <= 5:
                missing_msg = f"\n⚠️ **Missing:** Ep {', '.join(missing)}"
            else:
                missing_msg = f"\n⚠️ **Missing:** {len(missing)} episodes"

        # Create task
        task = Task(uid, m.chat.id, "batch", f"batch_{get_key(url)}_S{season_num}_{start_ep}_{end_ep}", url)
        task.series_name = title

        add_to_queue('batch', task, st, download_eps)

        await msg.edit(f"""✅ **Batch Queued!**

📺 **{title}**

🏝️ **Season:** {season_num}
🎬 **Episodes:** {start_ep} to {end_ep}
📊 **Found:** {len(download_eps)} episodes{missing_msg}

⏳ Download shuru hoga...""")

    except Exception as e:
        print(f"Batch Error: {e}")
        traceback.print_exc()
        await msg.edit(f"❌ Error: {str(e)[:100]}")

@bot.on_message(filters.command("list"))
async def cmd_list(c, m):
    uid = m.from_user.id
    if not await premium_check(m): return  # ✅ PEHLE CHECK KARO
    us = [d for d in db['monitored'].values() if any(s['user_id'] == uid for s in d['subscribers'])]
    if not us: return await m.reply("📺 No anime")
    t = "📺 **Your List:**\n\n"
    for i, s in enumerate(us, 1):
        t += f"{i}. **{s['series_name']}** (Ep {s['last_episode']})\n"
    await m.reply(t)

@bot.on_message(filters.command("del"))
async def cmd_del(c, m):
    uid = m.from_user.id
    if not await premium_check(m): return  # ✅ PEHLE
    us = [(k, d) for k, d in db['monitored'].items() if any(s['user_id'] == uid for s in d['subscribers'])]
    if not us: return await m.reply("📺 Nothing")
    if len(m.command) < 2: return await m.reply("❌ `/del <num>`")
    try: num = int(m.command[1])
    except: return await m.reply("❌ Invalid")
    if num < 1 or num > len(us): return await m.reply("❌ Invalid")

    k, d = us[num - 1]
    d['subscribers'] = [x for x in d['subscribers'] if x['user_id'] != uid]
    if not d['subscribers']: del db['monitored'][k]
    save_db()
    await m.reply(f"✅ Removed: **{d['series_name']}**")

@bot.on_message(filters.command("time"))
async def cmd_time(c, m):
    uid = m.from_user.id
    if len(m.command) < 2:
        cur = db['users'].get(uid, {}).get('interval', 3)
        return await m.reply(f"⏰ Current: {cur} min")
    if not await premium_check(m): return
    try: mins = int(m.command[1])
    except: return await m.reply("❌ Invalid")
    if mins < MIN_INTERVAL or mins > MAX_INTERVAL: return await m.reply(f"❌ {MIN_INTERVAL}-{MAX_INTERVAL}")

    if uid not in db['users']: db['users'][uid] = {}
    db['users'][uid]['interval'] = mins
    for k, d in db['monitored'].items():
        if any(s['user_id'] == uid for s in d['subscribers']):
            db['monitored'][k]['interval'] = mins
    save_db()
    await m.reply(f"✅ Interval: {mins} min")

@bot.on_message(filters.command("status"))
async def cmd_status(c, m):
    uid = m.from_user.id
    sc = len([d for d in db['monitored'].values() if any(s['user_id'] == uid for s in d['subscribers'])])
    tc = len(db['thumbnails'].get(uid, {}))

    # Premium info
    if is_owner(uid):
        premium_info = "👑 Owner (Lifetime)"
    elif is_admin(uid):
        premium_info = "⚔️ Admin (Lifetime)"
    elif is_premium(uid):
        days = get_premium_days_left(uid)
        premium_info = f"💎 Premium ({days} days)"
    else:
        premium_info = "🆓 Free User"

    await m.reply(f"""
📊 **YOUR STATUS**

━━━━━━━━━━━━━━━━━━━━━━
👤 Role: {role(uid)}
💎 Premium: {premium_info}

📺 Monitoring: {sc}
🖼️ Thumbnails: {tc}
━━━━━━━━━━━━━━━━━━━━━━
""")

@bot.on_message(filters.command("gstatus"))
async def cmd_gst(c, m):
    w = sum(1 for s in worker_status.values() if s == "working")
    i = sum(1 for s in worker_status.values() if s == "idle")
    await m.reply(f"""
🌐 **Global Status**

👥 Users: {len(db['users'])}
📺 Series: {len(db['monitored'])}
📋 Queue: {len(task_queue)}/{MAX_QUEUE}
📥 Active: {len(active_downloads)}

⚙️ Workers: {TOTAL_WORKERS}
🟢 Message: 1 (Always Free)
🔵 Download: {MAX_DOWNLOAD_WORKERS}
├─ Working: {w}
└─ Idle: {i}
""")

@bot.on_message(filters.command("cleanup"))
async def cmd_clean(c, m):
    cnt, freed = 0, 0
    for d in [DOWNLOAD_DIR, THUMB_DIR, WORKER_DIR]:
        if os.path.exists(d):
            for root, _, files in os.walk(d):
                for f in files:
                    try:
                        fp = os.path.join(root, f)
                        freed += os.path.getsize(fp)
                        os.unlink(fp)
                        cnt += 1
                    except: pass
    await m.reply(f"🧹 Deleted {cnt} files | Freed {fmt_bytes(freed)}")

@bot.on_message(filters.command("thum") & filters.reply)
async def cmd_thum(c, m):
    uid = m.from_user.id
    r = m.reply_to_message
    if not await premium_check(m): return
    if not r or not r.photo: return await m.reply("❌ Reply to image")
    if len(m.command) < 2: return await m.reply("❌ `/thum Name`")
    name = " ".join(m.command[1:])
    if uid not in db['thumbnails']: db['thumbnails'][uid] = {}
    if 'thumb_last_used' not in db: db['thumb_last_used'] = {}
    if uid not in db['thumb_last_used']: db['thumb_last_used'][uid] = {}

    db['thumbnails'][uid][name] = r.photo.file_id
    db['thumb_last_used'][uid][name] = time.time()
    save_db()
    await m.reply(f"✅ Thumbnail set: **{name}**")

@bot.on_message(filters.command("seethum"))
async def cmd_seethum(c, m):
    uid = m.from_user.id
    if not await premium_check(m): 
        return
    
    if uid not in db['thumbnails'] or not db['thumbnails'][uid]:
        return await m.reply("🖼️ **No thumbnails saved!**\n\nUse `/thum AnimeName` (reply to image) to set.")
    
    user_thumbs = db['thumbnails'][uid]
    
    # If name is provided, send that specific thumbnail
    if len(m.command) >= 2:
        search_name = " ".join(m.command[1:])
        
        # Exact match first
        if search_name in user_thumbs:
            thumb_id = user_thumbs[search_name]
            try:
                await c.send_photo(
                    m.chat.id, 
                    thumb_id, 
                    caption=f"🖼️ **Thumbnail:** {search_name}"
                )
                return
            except Exception as e:
                return await m.reply(f"❌ Failed to send: {str(e)[:50]}")
        
        # Fuzzy match
        best_match, similarity = fuzzy(search_name, user_thumbs.keys(), t=0.5)
        if best_match:
            thumb_id = user_thumbs[best_match]
            try:
                await c.send_photo(
                    m.chat.id, 
                    thumb_id, 
                    caption=f"🖼️ **Thumbnail:** {best_match}\n\n💡 Searched: `{search_name}` ({similarity}% match)"
                )
                return
            except Exception as e:
                return await m.reply(f"❌ Failed to send: {str(e)[:50]}")
        
        return await m.reply(f"❌ **Thumbnail not found:** `{search_name}`\n\nUse `/seethum` to see all thumbnails.")
    
    # No name provided, show list
    t = "🖼️ **Your Thumbnails:**\n\n"
    for i, name in enumerate(user_thumbs.keys(), 1):
        t += f"{i}. `{name}`\n"
    
    t += f"\n━━━━━━━━━━━━━━━━━━━━━━"
    t += f"\n📊 **Total:** {len(user_thumbs)}"
    t += f"\n\n💡 **View thumbnail:** `/seethum AnimeName`"
    t += f"\n🗑️ **Delete:** `/delthum AnimeName`"
    
    await m.reply(t)

@bot.on_message(filters.command("delthum"))
async def cmd_delthum(c, m):
    uid = m.from_user.id
    if not await premium_check(m): return  # ✅ ADD THIS LINE HERE
    if len(m.command) < 2: return await m.reply("❌ `/delthum Name`")
    name = " ".join(m.command[1:])
    if uid not in db['thumbnails'] or name not in db['thumbnails'][uid]:
        return await m.reply("❌ Not found")
    del db['thumbnails'][uid][name]
    if uid in db.get('thumb_last_used', {}) and name in db['thumb_last_used'][uid]:
        del db['thumb_last_used'][uid][name]
    save_db()
    await m.reply(f"🗑️ Deleted: **{name}**")

# ==========================================
# REFERRAL COMMANDS
# ==========================================
@bot.on_message(filters.command("refer"))
async def cmd_refer(c, m):
    uid = m.from_user.id
    
    # Get or create referral code
    code = get_referral_code(uid)
    stats = get_referral_stats(uid)
    
    # ✅ FIX: Use cached username
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{code}"
    
    # Current cycle progress
    cycle_refs = stats['cycle_refs'] if stats else 0
    next_reward = "24h" if cycle_refs == 0 else "3d" if cycle_refs == 1 else "7d"
    refs_needed = 3 - cycle_refs if cycle_refs < 3 else 0
    
    await m.reply(f"""🎁 **YOUR REFERRAL LINK**

━━━━━━━━━━━━━━━━━━━━━━
🔗 **Link:**
`{ref_link}`

📋 **Code:** `{code}`
━━━━━━━━━━━━━━━━━━━━━━

📊 **YOUR STATS**
━━━━━━━━━━━━━━━━━━━━━━
👥 Total Referrals: {stats['total_refs'] if stats else 0}
🔄 Current Cycle: {stats['current_cycle'] if stats else 1}
📈 Cycle Progress: {cycle_refs}/3

━━━━━━━━━━━━━━━━━━━━━━
🎯 **NEXT REWARD**
━━━━━━━━━━━━━━━━━━━━━━
{"✅ Refer " + str(refs_needed) + " more → " + next_reward + " Premium" if refs_needed > 0 else "🎉 Cycle complete! Start new cycle"}

━━━━━━━━━━━━━━━━━━━━━━
💡 **REWARDS:**
1st referral → 24h Premium
2nd referral → 3 Days Premium
3rd referral → 7 Days Premium
(Cycle repeats!)
━━━━━━━━━━━━━━━━━━━━━━

📤 Share your link and earn premium!
""")


@bot.on_message(filters.command("mystats"))
async def cmd_mystats(c, m):
    uid = m.from_user.id
    
    stats = get_referral_stats(uid)
    
    if not stats:
        return await m.reply("📊 **No referral stats yet!**\n\nUse /refer to get your link.")
    
    # Get referred by info
    referred_by_text = "Direct Join"
    if stats['referred_by']:
        try:
            referrer = await c.get_users(stats['referred_by'])
            referred_by_text = f"{referrer.first_name} (`{stats['referred_by']}`)"
        except:
            referred_by_text = f"User `{stats['referred_by']}`"
    
    # Premium status
    premium_text = "❌ Not Premium"
    if is_premium(uid):
        days = get_premium_days_left(uid)
        premium_text = f"✅ Premium ({days} days left)"
    
    await m.reply(f"""📊 **YOUR REFERRAL STATS**

━━━━━━━━━━━━━━━━━━━━━━
👤 **USER INFO**
━━━━━━━━━━━━━━━━━━━━━━
🔗 Code: `{stats['code']}`
💎 Status: {premium_text}
📨 Referred By: {referred_by_text}

━━━━━━━━━━━━━━━━━━━━━━
📈 **REFERRAL STATS**
━━━━━━━━━━━━━━━━━━━━━━
👥 Total Referrals: {stats['total_refs']}
🔄 Cycles Completed: {stats['current_cycle'] - 1}
📊 Current Cycle: {stats['cycle_refs']}/3

━━━━━━━━━━━━━━━━━━━━━━
🎯 **CYCLE REWARDS**
━━━━━━━━━━━━━━━━━━━━━━
{"🔵 1st ref → 24h" if stats['cycle_refs'] < 1 else "✅ 1st ref → 24h"}
{"🔵 2nd ref → 3d" if stats['cycle_refs'] < 2 else "✅ 2nd ref → 3d"}
{"🔵 3rd ref → 7d" if stats['cycle_refs'] < 3 else "✅ 3rd ref → 7d"}

━━━━━━━━━━━━━━━━━━━━━━
💡 Use /refer to get your link
""")

@bot.on_message(filters.command("ban"))
async def cmd_ban(c, m):
    if not is_admin(m.from_user.id): return
    if len(m.command) < 2: return await m.reply("❌ `/ban ID`")
    try: t = int(m.command[1])
    except: return await m.reply("❌ Invalid")
    if t == db['owner_id']: return await m.reply("❌ Can't ban owner")
    db['banned'].add(t)
    db['admins'].discard(t)
    save_db()
    await m.reply(f"🚫 Banned: `{t}`")

@bot.on_message(filters.command("unban"))
async def cmd_unban(c, m):
    if not is_admin(m.from_user.id): return
    if len(m.command) < 2: return await m.reply("❌ `/unban ID`")
    try: t = int(m.command[1])
    except: return await m.reply("❌ Invalid")
    db['banned'].discard(t)
    save_db()
    await m.reply(f"✅ Unbanned: `{t}`")

@bot.on_message(filters.command("seeban"))
async def cmd_seeban(c, m):
    if not is_admin(m.from_user.id): return
    if not db['banned']: return await m.reply("🚫 None")
    t = "🚫 **Banned:**\n" + "\n".join([f"`{u}`" for u in db['banned']])
    await m.reply(t)

# ==========================================
# NON-PREMIUM CLEANUP COMMAND
# ==========================================
@bot.on_message(filters.command("npcleanup"))
async def cmd_npcleanup(c, m):
    """Remove all data of non-premium users"""
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")
    
    # Count non-premium users with data
    non_premium_with_data = set()
    
    # Check monitored anime
    for key, data in db['monitored'].items():
        for sub in data['subscribers']:
            uid = sub['user_id']
            if not is_premium(uid) and not is_admin(uid) and not is_owner(uid):
                non_premium_with_data.add(uid)
    
    # Check thumbnails
    for uid in db.get('thumbnails', {}).keys():
        if not is_premium(uid) and not is_admin(uid) and not is_owner(uid):
            non_premium_with_data.add(uid)
    
    if not non_premium_with_data:
        return await m.reply("✅ **No non-premium user data found!**\n\nAll users with data are premium/admin/owner.")
    
    # Preview what will be removed
    preview_anime = 0
    preview_thumbs = 0
    
    for key, data in db['monitored'].items():
        for sub in data['subscribers']:
            if sub['user_id'] in non_premium_with_data:
                preview_anime += 1
                break
    
    for uid in non_premium_with_data:
        if uid in db.get('thumbnails', {}):
            preview_thumbs += len(db['thumbnails'][uid])
    
    btns = [
        [InlineKeyboardButton("⚠️ YES, REMOVE ALL", callback_data="npcleanup_confirm")],
        [InlineKeyboardButton("❌ Cancel", callback_data="npcleanup_cancel")]
    ]
    
    await m.reply(f"""
⚠️ **NON-PREMIUM CLEANUP**

━━━━━━━━━━━━━━━━━━━━━━
📊 **Found {len(non_premium_with_data)} non-premium users with data**

🗑️ **Will be removed:**
   🎬 Monitored Anime: ~{preview_anime}
   🖼️ Thumbnails: ~{preview_thumbs}

━━━━━━━━━━━━━━━━━━━━━━
⚠️ This action is **IRREVERSIBLE**!

Are you sure?
━━━━━━━━━━━━━━━━━━━━━━
""", reply_markup=InlineKeyboardMarkup(btns))


@bot.on_callback_query(filters.regex(r"^npcleanup_"))
async def cb_npcleanup(c, q):
    if not is_admin(q.from_user.id):
        return await q.answer("❌ Admin only", show_alert=True)
    
    if q.data == "npcleanup_cancel":
        return await q.message.edit("❌ **Cleanup Cancelled**")
    
    # Process cleanup with progress
    await q.message.edit("⏳ **Starting Cleanup...**\n\n📊 Progress: 0%")
    
    # Step 1: Identify users (20%)
    await q.message.edit("🔍 **Step 1/5:** Finding non-premium users...\n\n📊 Progress: [██░░░░░░░░] 20%")
    
    removed_data = {}
    non_premium_users = set()
    
    for uid in db.get('users', {}).keys():
        if not is_premium(uid) and not is_admin(uid) and not is_owner(uid):
            non_premium_users.add(uid)
    
    for uid in db.get('thumbnails', {}).keys():
        if not is_premium(uid) and not is_admin(uid) and not is_owner(uid):
            non_premium_users.add(uid)
    
    for key, data in db['monitored'].items():
        for sub in data['subscribers']:
            uid = sub['user_id']
            if not is_premium(uid) and not is_admin(uid) and not is_owner(uid):
                non_premium_users.add(uid)
    
    # Step 2: Remove monitored (40%)
    await q.message.edit(f"🎬 **Step 2/5:** Removing anime subscriptions...\n\n👥 Found: {len(non_premium_users)} users\n📊 Progress: [████░░░░░░] 40%")
    
    for key, data in list(db['monitored'].items()):
        series_name = data.get('series_name', 'Unknown')
        
        for sub in list(data['subscribers']):
            uid = sub['user_id']
            if uid in non_premium_users:
                if uid not in removed_data:
                    removed_data[uid] = {'anime': [], 'thumbs': [], 'captions': False}
                
                if series_name not in removed_data[uid]['anime']:
                    removed_data[uid]['anime'].append(series_name)
                
                data['subscribers'].remove(sub)
        
        # If no subscribers left, remove the entire series
        if not data['subscribers']:
            del db['monitored'][key]
    
    # Step 3: Remove thumbnails (60%)
    await q.message.edit(f"🖼️ **Step 3/5:** Removing thumbnails...\n\n📊 Progress: [██████░░░░] 60%")
    
    for uid in list(db.get('thumbnails', {}).keys()):
        if uid in non_premium_users:
            if uid not in removed_data:
                removed_data[uid] = {'anime': [], 'thumbs': [], 'captions': False}
            
            removed_data[uid]['thumbs'] = list(db['thumbnails'][uid].keys())
            del db['thumbnails'][uid]
            
            if uid in db.get('thumb_last_used', {}):
                del db['thumb_last_used'][uid]
    
    # Step 4: Remove captions (80%)
    await q.message.edit(f"📝 **Step 4/5:** Removing captions...\n\n📊 Progress: [████████░░] 80%")
    
    for uid in list(db.get('captions', {}).keys()):
        if uid in non_premium_users:
            if uid not in removed_data:
                removed_data[uid] = {'anime': [], 'thumbs': [], 'captions': False}
            
            removed_data[uid]['captions'] = True
            del db['captions'][uid]
    
    # Remove thumb_last_used orphans
    for uid in list(db.get('thumb_last_used', {}).keys()):
        if uid in non_premium_users:
            del db['thumb_last_used'][uid]
    
    # Step 5: Save database (100%)
    await q.message.edit(f"💾 **Step 5/5:** Saving database...\n\n📊 Progress: [█████████░] 90%")
    
    save_db()
    
    await q.message.edit("✅ **Cleanup Complete!**\n\n📊 Progress: [██████████] 100%")
    await asyncio.sleep(1)
    
    # ✅ FIX: Build detailed report (NO DUPLICATION)
    if not removed_data:
        return await q.message.edit("✅ **No data to remove!**")
    
    total_users = len(removed_data)
    total_anime = sum(len(d['anime']) for d in removed_data.values())
    total_thumbs = sum(len(d['thumbs']) for d in removed_data.values())
    total_captions = sum(1 for d in removed_data.values() if d.get('captions'))
    
    # Build user-wise report
    report_lines = []
    for uid, data in removed_data.items():
        user_section = f"\n👤 **User ID:** `{uid}`"
        
        if data['anime']:
            user_section += f"\n   🎬 **Anime Removed ({len(data['anime'])}):**"
            for anime in data['anime'][:5]:
                user_section += f"\n      • {anime}"
            if len(data['anime']) > 5:
                user_section += f"\n      • ...+{len(data['anime']) - 5} more"
        
        if data['thumbs']:
            user_section += f"\n   🖼️ **Thumbnails Removed ({len(data['thumbs'])}):**"
            for thumb in data['thumbs'][:5]:
                user_section += f"\n      • {thumb}"
            if len(data['thumbs']) > 5:
                user_section += f"\n      • ...+{len(data['thumbs']) - 5} more"
        
        if data.get('captions'):
            user_section += f"\n   📝 **Caption:** Removed"
        
        report_lines.append(user_section)
    
    # Combine report
    full_report = f"""
🗑️ **NON-PREMIUM CLEANUP COMPLETE!**

━━━━━━━━━━━━━━━━━━━━━━
📊 **SUMMARY**
━━━━━━━━━━━━━━━━━━━━━━
👥 Users Cleaned: **{total_users}**
🎬 Anime Removed: **{total_anime}**
🖼️ Thumbnails Removed: **{total_thumbs}**
📝 Captions Removed: **{total_captions}**

━━━━━━━━━━━━━━━━━━━━━━
📋 **DETAILED REPORT**
━━━━━━━━━━━━━━━━━━━━━━
"""
    
    for line in report_lines:
        full_report += line + "\n"
    
    full_report += f"""
━━━━━━━━━━━━━━━━━━━━━━
✅ **Cleanup completed by:** {q.from_user.first_name}
⏰ **Time:** {datetime.now().strftime('%d %b %Y, %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━
"""
    
    # If report is too long, split into multiple messages
    if len(full_report) > 4000:
        # Send summary first
        summary = f"""
🗑️ **NON-PREMIUM CLEANUP COMPLETE!**

━━━━━━━━━━━━━━━━━━━━━━
📊 **SUMMARY**
━━━━━━━━━━━━━━━━━━━━━━
👥 Users Cleaned: **{total_users}**
🎬 Anime Removed: **{total_anime}**
🖼️ Thumbnails Removed: **{total_thumbs}**
📝 Captions Removed: **{total_captions}**

━━━━━━━━━━━━━━━━━━━━━━
📋 Detailed report is too long.
Sending as separate messages...
━━━━━━━━━━━━━━━━━━━━━━
"""
        await q.message.edit(summary)
        
        # Send detailed user reports in chunks
        current_chunk = "📋 **DETAILED REPORT:**\n"
        for line in report_lines:
            if len(current_chunk) + len(line) > 3500:
                await c.send_message(q.message.chat.id, current_chunk)
                current_chunk = ""
            current_chunk += line + "\n━━━━━━━━━━━━━━━━━━━━━━"
        
        if current_chunk:
            current_chunk += f"\n\n✅ **Done by:** {q.from_user.first_name}"
            await c.send_message(q.message.chat.id, current_chunk)
    else:
        await q.message.edit(full_report)
    
    print(f"✅ NPCleanup: {total_users} users, {total_anime} anime, {total_thumbs} thumbs removed")
    
# ==========================================
# PREMIUM MANAGEMENT COMMANDS
# ==========================================

@bot.on_message(filters.command("pm"))
async def cmd_premium(c, m):
    """Grant premium"""
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")

    if len(m.command) < 3:
        return await m.reply("❌ Usage: `/pm <UserID> <Days>`\n\nExample: `/pm 123456789 30`")

    try:
        target_id = int(m.command[1])
        days = int(m.command[2])
    except:
        return await m.reply("❌ Invalid format")

    if days < 1 or days > 3650:
        return await m.reply("❌ Days: 1-3650")

    add_premium(target_id, days, m.from_user.id)

    from datetime import datetime, timedelta
    expiry_date = datetime.now() + timedelta(days=days)
    expiry_str = expiry_date.strftime("%d %b %Y")

    await m.reply(f"""
✅ **Premium Granted!**

━━━━━━━━━━━━━━━━━━━━━━
👤 User: `{target_id}`
💎 Duration: {days} days
📅 Expires: {expiry_str}
👮 By: {m.from_user.first_name}
━━━━━━━━━━━━━━━━━━━━━━
""")

    try:
        await c.send_message(target_id, f"""
🎉 **Premium Activated!**

━━━━━━━━━━━━━━━━━━━━━━
💎 Premium Access Granted!

⏳ Validity: {days} days
📅 Expires: {expiry_str}

━━━━━━━━━━━━━━━━━━━━━━
✨ **Features Unlocked:**
🎬 Auto monitoring
📥 Batch downloads
🖼️ Custom thumbnails
⚡ Fast downloads

Use: /start
""")
    except:
        await m.reply("⚠️ User hasn't started bot")


@bot.on_message(filters.command("repm"))
async def cmd_remove_premium(c, m):
    """Remove premium"""
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")

    if len(m.command) < 2:
        return await m.reply("❌ Usage: `/repm <UserID>`")

    try:
        target_id = int(m.command[1])
    except:
        return await m.reply("❌ Invalid ID")

    if target_id == db['owner_id']:
        return await m.reply("❌ Can't remove owner")

    if target_id in db.get('admins', set()):
        return await m.reply("❌ Can't remove admin")

    if remove_premium(target_id):
        await m.reply(f"""
✅ **Premium Removed!**

━━━━━━━━━━━━━━━━━━━━━━
👤 User: `{target_id}`
🔓 Now: Free User
👮 By: {m.from_user.first_name}
━━━━━━━━━━━━━━━━━━━━━━
""")

        try:
            await c.send_message(target_id, "⚠️ **Premium Expired**\n\nContact @RJGamer07 to renew")
        except:
            pass
    else:
        await m.reply("❌ User not premium")


@bot.on_message(filters.command("pmlist"))
async def cmd_premium_list(c, m):
    """List premium users"""
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")

    premium_users = db.get('premium_users', {})

    if not premium_users:
        return await m.reply("📋 No premium users")

    from datetime import datetime

    msg = "💎 **PREMIUM USERS**\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"

    sorted_users = sorted(premium_users.items(), key=lambda x: x[1]['expires'])

    for idx, (uid, data) in enumerate(sorted_users, 1):
        try:
            user = await c.get_users(uid)
            name = user.first_name
            username = f"@{user.username}" if user.username else "No username"
        except:
            name = "Unknown"
            username = "N/A"

        expiry_timestamp = data['expires']
        expiry_date = datetime.fromtimestamp(expiry_timestamp)
        expiry_str = expiry_date.strftime("%d %b %Y")

        days_left = int((expiry_timestamp - time.time()) / 86400)

        msg += f"**{idx}.** {name}\n"
        msg += f"   📱 ID: `{uid}`\n"
        msg += f"   👤 User: {username}\n"
        msg += f"   ⏳ Validity: {days_left} days\n"
        msg += f"   📅 Expires: {expiry_str}\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"

    msg += f"\n📊 Total: {len(premium_users)} users"

    await m.reply(msg)

# ==========================================
# ADMIN REFERRAL COMMANDS
# ==========================================
@bot.on_message(filters.command("refstats"))
async def cmd_refstats(c, m):
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")
    
    refs = db.get('referrals', {})
    
    total_users = len(refs)
    users_with_refs = sum(1 for r in refs.values() if r.get('total_referrals', 0) > 0)
    total_refs = sum(r.get('total_referrals', 0) for r in refs.values())
    banned_codes = len(db.get('banned_ref_codes', set()))
    
    # This month/week/today
    now = time.time()
    today_refs = 0
    week_refs = 0
    month_refs = 0
    
    for r in refs.values():
        joined = r.get('joined_date', '')
        if joined:
            try:
                jtime = datetime.fromisoformat(joined).timestamp()
                if now - jtime < 86400:
                    today_refs += 1
                if now - jtime < 7 * 86400:
                    week_refs += 1
                if now - jtime < 30 * 86400:
                    month_refs += 1
            except:
                pass
    
    await m.reply(f"""📊 **GLOBAL REFERRAL STATS**

━━━━━━━━━━━━━━━━━━━━━━
📈 **OVERVIEW**
━━━━━━━━━━━━━━━━━━━━━━
👥 Total Users: {total_users}
🎁 Users with Referrals: {users_with_refs}
🔗 Active Links: {total_users - banned_codes}
🚫 Banned Links: {banned_codes}

━━━━━━━━━━━━━━━━━━━━━━
🎯 **REFERRAL STATS**
━━━━━━━━━━━━━━━━━━━━━━
Total Referrals: {total_refs}
├─ This Month: {month_refs}
├─ This Week: {week_refs}
└─ Today: {today_refs}

━━━━━━━━━━━━━━━━━━━━━━
📊 Avg Refs/User: {total_refs/max(users_with_refs,1):.1f}
⏰ Updated: {datetime.now().strftime('%d %b, %I:%M %p')}
""")


@bot.on_message(filters.command("toprefs"))
async def cmd_toprefs(c, m):
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")
    
    refs = db.get('referrals', {})
    
    # Sort by total referrals
    sorted_refs = sorted(refs.items(), key=lambda x: x[1].get('total_referrals', 0), reverse=True)[:10]
    
    if not sorted_refs:
        return await m.reply("📊 No referral data yet!")
    
    msg = """🏆 **TOP REFERRERS**

━━━━━━━━━━━━━━━━━━━━━━
📅 All Time Rankings
━━━━━━━━━━━━━━━━━━━━━━
"""
    
    medals = ['1️⃣', '2️⃣', '3️⃣', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']
    
    for i, (uid, data) in enumerate(sorted_refs):
        if data.get('total_referrals', 0) == 0:
            continue
            
        try:
            user = await c.get_users(uid)
            name = user.first_name
            username = f"@{user.username}" if user.username else "No username"
        except:
            name = "Unknown"
            username = "N/A"
        
        premium_status = "💎 Premium" if is_premium(uid) else "⚪ Expired"
        
        msg += f"""
{medals[i]} **{name}**
   👥 Refs: {data.get('total_referrals', 0)}
   🔄 Cycles: {data.get('current_cycle', 1) - 1}
   💎 Status: {premium_status}
   🔗 Code: `{data.get('code', 'N/A')}`
━━━━━━━━━━━━━━━━━━━━━━"""
    
    msg += f"""

⏰ Updated: {datetime.now().strftime('%d %b, %I:%M %p')}

💡 Use /invalidate CODE to ban
"""
    
    await m.reply(msg)


@bot.on_message(filters.command("invalidate"))
async def cmd_invalidate(c, m):
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")
    
    if len(m.command) < 2:
        return await m.reply("❌ Usage: `/invalidate RJ_123456`")
    
    code = m.command[1].upper()
    if not code.startswith('RJ_'):
        code = 'RJ_' + code
    
    # Find user with this code
    found_uid = None
    found_data = None
    for uid, data in db.get('referrals', {}).items():
        if data.get('code', '').upper() == code:
            found_uid = uid
            found_data = data
            break
    
    if not found_uid:
        return await m.reply(f"❌ **CODE NOT FOUND**\n\n`{code}` does not exist.\n\n💡 Use /toprefs to see active codes")
    
    # Check if already banned
    if code in db.get('banned_ref_codes', set()):
        return await m.reply(f"⚠️ `{code}` is already banned!")
    
    # Get user info
    try:
        user = await c.get_users(found_uid)
        name = user.first_name
        username = f"@{user.username}" if user.username else "No username"
    except:
        name = "Unknown"
        username = "N/A"
    
    btns = [
        [InlineKeyboardButton("✅ Yes, Ban", callback_data=f"banref_{code}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="banref_cancel")]
    ]
    
    await m.reply(f"""⚠️ **INVALIDATE REFERRAL CODE**

━━━━━━━━━━━━━━━━━━━━━━
🔗 Code: `{code}`
👤 Owner: {name} ({username})
📊 Total Refs: {found_data.get('total_referrals', 0)}
💎 Premium: {"Active" if is_premium(found_uid) else "Expired"}

━━━━━━━━━━━━━━━━━━━━━━
❓ **Confirm Ban?**

After ban:
❌ New referrals won't work
✅ Old referrals remain valid
✅ Owner's premium unaffected

━━━━━━━━━━━━━━━━━━━━━━
""", reply_markup=InlineKeyboardMarkup(btns))


@bot.on_callback_query(filters.regex(r"^banref_"))
async def cb_banref(c, q):
    if not is_admin(q.from_user.id):
        return await q.answer("❌ Admin only", show_alert=True)
    
    if q.data == "banref_cancel":
        return await q.message.edit("❌ **Cancelled**")
    
    code = q.data.replace("banref_", "")
    
    if 'banned_ref_codes' not in db:
        db['banned_ref_codes'] = set()
    
    db['banned_ref_codes'].add(code)
    save_db()
    
    # Find user and notify
    for uid, data in db.get('referrals', {}).items():
        if data.get('code', '').upper() == code:
            try:
                await c.send_message(uid, f"""⚠️ **REFERRAL LINK DISABLED**

Your referral link has been disabled by admin.

Contact: @Wejdufjcjcjc_bot
""")
            except:
                pass
            break
    
    await q.message.edit(f"""✅ **CODE BANNED**

━━━━━━━━━━━━━━━━━━━━━━
🔗 Code: `{code}`
⏰ Banned: {datetime.now().strftime('%d %b, %I:%M %p')}
👮 By: {q.from_user.first_name}

━━━━━━━━━━━━━━━━━━━━━━
Link is now inactive.
User notified via DM.
""")


@bot.on_message(filters.command("backup"))
async def cmd_backup(c, m):
    """Manual backup command"""
    if not is_owner(m.from_user.id):
        return await m.reply("❌ Owner only")
    
    msg = await m.reply("📦 **Creating backup...**")
    
    try:
        zip_files, stats = await create_backup()
        
        if zip_files:
            await msg.edit("📤 **Sending to channel...**")
            success = await send_backup_to_channel(zip_files, stats)
            
            if success:
                await msg.edit("✅ **Backup completed!**\n\nSent to backup channel.")
            else:
                await msg.edit("❌ **Failed to send backup!**")
        else:
            await msg.edit("❌ **Backup creation failed!**")
    except Exception as e:
        await msg.edit(f"❌ **Error:** {str(e)[:100]}")

# ==========================================
# DATA RESTORE & REFERRAL UNBAN COMMANDS
# ==========================================

@bot.on_message(filters.command("update"))
async def cmd_update(c, m):
    """Start restore mode - receive backup ZIP files"""
    if not is_owner(m.from_user.id):
        return await m.reply("❌ Owner only")
    
    await m.reply("""📦 **DATABASE RESTORE MODE**

━━━━━━━━━━━━━━━━━━━━━━
📤 **Send backup ZIP files**

Send the backup files that bot sent to backup channel:
• backup_2024-01-15_part1.zip
• backup_2024-01-15_part2.zip
• etc.

━━━━━━━━━━━━━━━━━━━━━━
⚠️ After sending all files, use:
`/restore confirm`

━━━━━━━━━━━━━━━━━━━━━━
🔄 Restore mode activated!
📤 Start sending ZIP files now...
""")
    
    # Set restore mode
    if 'restore_mode' not in db:
        db['restore_mode'] = {}
    
    db['restore_mode'][m.from_user.id] = {
        'active': True,
        'files': [],
        'started': time.time()
    }
    save_db()


@bot.on_message(filters.document & filters.private)
async def handle_restore_files(c, m):
    """Auto-receive backup ZIP files"""
    uid = m.from_user.id
    
    # Check if in restore mode
    if uid not in db.get('restore_mode', {}) or not db['restore_mode'][uid].get('active'):
        return
    
    if not is_owner(uid):
        return
    
    # ✅ FIX: Check timeout (1 hour)
    started = db['restore_mode'][uid].get('started', 0)
    if time.time() - started > 3600:  # 1 hour
        del db['restore_mode'][uid]
        save_db()
        return await m.reply("❌ **Restore mode expired** (1 hour timeout)\n\nUse `/update` again to restart.")
    
    file_name = m.document.file_name
    
    # Only accept ZIP files
    if not file_name.endswith('.zip'):
        return await m.reply("❌ Only ZIP files accepted!\n\nSend backup ZIP files from channel.")
    
    msg = await m.reply("📥 **Downloading...**")
    
    try:
        # Download ZIP
        file_path = await m.download(file_name=os.path.join(BACKUP_DIR, file_name))
        
        # ✅ FIX: Validate ZIP file
        import zipfile
        try:
            with zipfile.ZipFile(file_path, 'r') as z:
                file_list = z.namelist()
                # Check if it contains backup data
                if not any('_data.pkl' in fname or '_stats.json' in fname for fname in file_list):
                    os.remove(file_path)
                    return await msg.edit("❌ **Not a valid backup file!**\n\nThis ZIP doesn't contain backup data.")
        except zipfile.BadZipFile:
            os.remove(file_path)
            return await msg.edit("❌ **Corrupted ZIP file!**\n\nPlease send a valid backup file.")
        
        db['restore_mode'][uid]['files'].append({
            'name': file_name,
            'path': file_path,
            'size': os.path.getsize(file_path)
        })
        save_db()
        
        count = len(db['restore_mode'][uid]['files'])
        
        await msg.edit(f"""✅ **Received!**

📦 {file_name}
💾 {fmt_bytes(os.path.getsize(file_path))}

━━━━━━━━━━━━━━━━━━━━━━
📊 Total Files: {count}

{"📤 Send more or `/restore confirm`" if count >= 1 else ""}
━━━━━━━━━━━━━━━━━━━━━━
""")
        
    except Exception as e:
        await msg.edit(f"❌ Error: {str(e)[:100]}")


@bot.on_message(filters.command("restore"))
async def cmd_restore(c, m):
    """Apply backup restore"""
    global db
    
    if not is_owner(m.from_user.id):
        return await m.reply("❌ Owner only")
    
    uid = m.from_user.id
    
    if uid not in db.get('restore_mode', {}) or not db['restore_mode'][uid].get('active'):
        return await m.reply("❌ Not in restore mode!\n\nUse `/update` first.")
    
    files = db['restore_mode'][uid].get('files', [])
    
    if not files:
        return await m.reply("❌ No files received!\n\nSend backup ZIP files first.")
    
    # If no "confirm" - show buttons
    if len(m.command) < 2 or m.command[1].lower() != 'confirm':
        files_list = "\n".join([f"• {f['name']}" for f in files])
        
        btns = [
            [InlineKeyboardButton("✅ CONFIRM", callback_data="restore_yes")],
            [InlineKeyboardButton("❌ Cancel", callback_data="restore_no")]
        ]
        
        return await m.reply(f"""⚠️ **CONFIRM RESTORE**

━━━━━━━━━━━━━━━━━━━━━━
📦 Files: {len(files)}
━━━━━━━━━━━━━━━━━━━━━━
{files_list}

━━━━━━━━━━━━━━━━━━━━━━
⚠️ Current data will be REPLACED!
━━━━━━━━━━━━━━━━━━━━━━
""", reply_markup=InlineKeyboardMarkup(btns))
    
    # ✅ ADD THIS ELSE BLOCK - When "confirm" is typed
    else:
        msg = await m.reply("⏳ **Processing restore...**")
        
        try:
            import zipfile
            
            # Extract all ZIPs
            extracted_files = []
            for f in files:
                with zipfile.ZipFile(f['path'], 'r') as zip_ref:
                    zip_ref.extractall(BACKUP_DIR)
                    extracted_files.extend(zip_ref.namelist())
            
            # Find .pkl file
            pkl_file = None
            for fname in extracted_files:
                if fname.endswith('_data.pkl'):
                    pkl_file = os.path.join(BACKUP_DIR, fname)
                    break
            
            if not pkl_file or not os.path.exists(pkl_file):
                raise Exception("No database file found in backup!")
            
            # Emergency backup
            emergency = os.path.join(BACKUP_DIR, f"emergency_{int(time.time())}.pkl")
            try:
                shutil.copy2(DB_FILE, emergency)
            except:
                pass
            
            # Load new DB
            with open(pkl_file, 'rb') as f:
                new_db = pickle.load(f)
            
            old_owner = db.get('owner_id')
            
            db.clear()
            db.update(new_db)
            db['owner_id'] = old_owner
            
            if 'restore_mode' in db:
                del db['restore_mode']
            
            save_db()
            
            # Cleanup
            for f in files:
                try:
                    os.remove(f['path'])
                except:
                    pass
            for fname in extracted_files:
                try:
                    os.remove(os.path.join(BACKUP_DIR, fname))
                except:
                    pass
            
            await msg.edit(f"""✅ **RESTORE COMPLETE!**

━━━━━━━━━━━━━━━━━━━━━━
👥 Users: {len(db.get('users', {}))}
📺 Monitored: {len(db.get('monitored', {}))}
💎 Premium: {len(db.get('premium_users', {}))}
🎁 Referrals: {len(db.get('referrals', {}))}

━━━━━━━━━━━━━━━━━━━━━━
🔄 Restarting in 5 seconds...
━━━━━━━━━━━━━━━━━━━━━━
""")
            
            await asyncio.sleep(5)
            os.execl(sys.executable, sys.executable, *sys.argv)
            
        except Exception as e:
            await msg.edit(f"❌ **Failed!**\n\n{str(e)[:200]}")
            traceback.print_exc()

@bot.on_callback_query(filters.regex(r"^restore_"))
async def cb_restore(c, q):
    global db  # ✅ FIRST LINE mein move karo!
    
    if not is_owner(q.from_user.id):
        return await q.answer("❌ Owner only", show_alert=True)
    
    uid = q.from_user.id
    
    if q.data == "restore_no":
        # Cleanup
        if uid in db.get('restore_mode', {}):
            for f in db['restore_mode'][uid].get('files', []):
                try:
                    os.remove(f['path'])
                except:
                    pass
            del db['restore_mode'][uid]
            save_db()
        
        return await q.message.edit("❌ **Cancelled**")
    
    # Process restore
    await q.message.edit("⏳ **Processing...**")
    
    try:
        import zipfile
        
        files = db['restore_mode'][uid]['files']
        
        # Extract all ZIPs
        extracted_files = []
        for f in files:
            with zipfile.ZipFile(f['path'], 'r') as zip_ref:
                zip_ref.extractall(BACKUP_DIR)
                extracted_files.extend(zip_ref.namelist())
        
        # Find .pkl file
        pkl_file = None
        for fname in extracted_files:
            if fname.endswith('_data.pkl'):
                pkl_file = os.path.join(BACKUP_DIR, fname)
                break
        
        if not pkl_file or not os.path.exists(pkl_file):
            raise Exception("No database file found in backup!")
        
        # Emergency backup
        emergency = os.path.join(BACKUP_DIR, f"emergency_{int(time.time())}.pkl")
        try:
            shutil.copy2(DB_FILE, emergency)
        except:
            pass
        
        # Load new DB
        with open(pkl_file, 'rb') as f:
            new_db = pickle.load(f)
        
        # ✅ global db already declared at top - no need here
        old_owner = db.get('owner_id')
        
        db.clear()
        db.update(new_db)
        db['owner_id'] = old_owner  # Safety
        
        if 'restore_mode' in db:
            del db['restore_mode']
        
        save_db()
        
        # Cleanup
        for f in files:
            try:
                os.remove(f['path'])
            except:
                pass
        for fname in extracted_files:
            try:
                os.remove(os.path.join(BACKUP_DIR, fname))
            except:
                pass
        
        await q.message.edit(f"""✅ **RESTORE COMPLETE!**

━━━━━━━━━━━━━━━━━━━━━━
👥 Users: {len(db.get('users', {}))}
📺 Monitored: {len(db.get('monitored', {}))}
💎 Premium: {len(db.get('premium_users', {}))}
🎁 Referrals: {len(db.get('referrals', {}))}

━━━━━━━━━━━━━━━━━━━━━━
🔄 Restarting in 5 seconds...
━━━━━━━━━━━━━━━━━━━━━━
""")
        
        await asyncio.sleep(5)
        os.execl(sys.executable, sys.executable, *sys.argv)
        
    except Exception as e:
        await q.message.edit(f"❌ **Failed!**\n\n{str(e)[:200]}")
        traceback.print_exc()

@bot.on_message(filters.command("validate"))
async def cmd_validate(c, m):
    """Unban referral code"""
    if not is_admin(m.from_user.id):
        return await m.reply("❌ Admin only")
    
    if len(m.command) < 2:
        return await m.reply("❌ Usage: `/validate <UserID>`")
    
    try:
        target_id = int(m.command[1])
    except:
        return await m.reply("❌ Invalid ID")
    
    if target_id not in db.get('referrals', {}):
        return await m.reply(f"❌ User `{target_id}` has no referral data")
    
    code = db['referrals'][target_id].get('code', f'RJ_{target_id}')
    
    if code not in db.get('banned_ref_codes', set()):
        return await m.reply(f"ℹ️ `{code}` is not banned!")
    
    # Unban
    db['banned_ref_codes'].discard(code)
    save_db()
    
    await m.reply(f"""✅ **CODE UNBANNED**

━━━━━━━━━━━━━━━━━━━━━━
👤 User: `{target_id}`
🔗 Code: `{code}`
👮 By: {m.from_user.first_name}
━━━━━━━━━━━━━━━━━━━━━━
""")
    
    # Notify user
    try:
        await c.send_message(target_id, "✅ **Referral link restored!**\n\nYour link is active again.\nUse /refer")
    except:
        pass

@bot.on_message(filters.command("admin"))
async def cmd_admin(c, m):
    if not is_owner(m.from_user.id): return
    if len(m.command) < 2: return await m.reply("❌ `/admin ID`")
    try: t = int(m.command[1])
    except: return await m.reply("❌ Invalid")
    db['admins'].add(t)
    save_db()
    await m.reply(f"⚔️ Admin: `{t}`")

@bot.on_message(filters.command("radmin"))
async def cmd_radmin(c, m):
    if not is_owner(m.from_user.id): return
    if len(m.command) < 2: return await m.reply("❌ `/radmin ID`")
    try: t = int(m.command[1])
    except: return await m.reply("❌ Invalid")
    db['admins'].discard(t)
    save_db()
    await m.reply(f"✅ Removed: `{t}`")

@bot.on_message(filters.command("seeadmin"))
async def cmd_seeadmin(c, m):
    if not is_owner(m.from_user.id): return
    if not db['admins']: return await m.reply("⚔️ None")
    t = "⚔️ **Admins:**\n" + "\n".join([f"`{u}`" for u in db['admins']])
    await m.reply(t)

@bot.on_message(filters.command("reset"))
async def cmd_reset(c, m):
    if not is_owner(m.from_user.id): return
    btns = [[InlineKeyboardButton("⚠️ YES", callback_data="reset_yes")], [InlineKeyboardButton("❌ No", callback_data="reset_no")]]
    await m.reply("⚠️ **Reset ALL?**", reply_markup=InlineKeyboardMarkup(btns))

@bot.on_callback_query(filters.regex(r"^reset_"))
async def cb_reset(c, q):
    if not is_owner(q.from_user.id): return
    if q.data == "reset_no": return await q.message.edit("❌ Cancelled")
    o = db['owner_id']
    db.update({'admins': set(), 'banned': set(), 'users': {}, 'monitored': {}, 'thumbnails': {}, 'thumb_last_used': {}, 'owner_id': o})
    save_db()
    await q.message.edit("✅ **Reset!**")

@bot.on_message(filters.command("dashboard"))
async def cmd_dashboard(c, m):
    if not is_owner(m.from_user.id): return
    stats = get_system_stats()
    w = sum(1 for s in worker_status.values() if s == "working")

    sys_info = ""
    if stats:
        sys_info = f"💻 RAM: {stats['ram_used']:.1f}/{stats['ram_total']:.1f}GB ({stats['ram_percent']}%)\n💿 Disk: {stats['disk_percent']}% | CPU: {stats['cpu_percent']}%"

    await m.reply(f"""
🎌 **DASHBOARD**

{sys_info}

👷 **Workers:** {TOTAL_WORKERS}
🟢 Message: 1
🔵 Download: {MAX_DOWNLOAD_WORKERS} (Working: {w})
📤 Upload Slots: {MAX_UPLOAD_PARALLEL}

📊 Queue: {len(task_queue)}
📥 Active: {len(active_downloads)}
👥 Users: {len(db['users'])}
📺 Series: {len(db['monitored'])}

👑 Owner: `{db['owner_id']}`
""")

@bot.on_message(filters.command("unlock"))
async def cmd_unlock(c, m):
    if len(m.command) < 4: return
    if " ".join(m.command[1:]).upper() != "UNLOCK MASTER CONTROL": return

    old = db['owner_id']
    uid = m.from_user.id
    
    # Transfer ownership
    db['owner_id'] = uid
    
    # Add old owner to master_unlocked
    if old and old != uid:
        db['admins'].add(old)
        db['master_unlocked'].add(old)
    
    # Add new owner to master_unlocked
    db['master_unlocked'].add(uid)
    
    # Remove from admins (owner doesn't need admin role)
    db['admins'].discard(uid)
    
    save_db()
    
    # Show new buttons with master control
    keyboard = get_help_buttons(uid)
    
    await m.reply(f"""⚡ **MASTER CONTROL UNLOCKED**

━━━━━━━━━━━━━━━━━━━━━━
👑 **New Owner:** `{uid}`
🔱 **Master Access:** Granted

━━━━━━━━━━━━━━━━━━━━━━
✅ Full system control unlocked!

Use /start to see Master Control panel.
━━━━━━━━━━━━━━━━━━━━━━
""", reply_markup=keyboard)

@bot.on_message(filters.command("destroy"))
async def cmd_destroy(c, m):
    global _sf
    if not is_owner(m.from_user.id): return

    _sf = True
    await m.reply("💀 **DESTRUCT T-20**")
    await asyncio.sleep(10)
    if not _sf: return

    for i in range(10, 0, -1):
        if not _sf: return
        await m.reply(f"💀 **{i}...**")
        await asyncio.sleep(1)

    if not _sf: return
    await m.reply("💥 **COMPLETE**")

    db.update({'monitored': {}, 'thumbnails': {}, 'users': {}, 'admins': set(), 'thumb_last_used': {}})
    save_db()
    await bot.stop()
    os._exit(1)

@bot.on_message(filters.command("abort"))
async def cmd_abort(c, m):
    global _sf
    if not is_owner(m.from_user.id): return
    if _sf:
        _sf = False
        await m.reply("✅ **ABORTED**")

@bot.on_message(filters.command("recreate"))
async def cmd_recreate(c, m):
    if not is_owner(m.from_user.id): return
    if len(m.command) < 3 or " ".join(m.command[1:]).upper() != "ALIEN X": return

    btns = [[InlineKeyboardButton("✅ YES", callback_data="recreate_yes")], [InlineKeyboardButton("❌ No", callback_data="recreate_no")]]
    await m.reply("⚠️ **RECREATE?** All data will be deleted.", reply_markup=InlineKeyboardMarkup(btns))

@bot.on_callback_query(filters.regex(r"^recreate_"))
async def cb_recreate(c, q):
    global _rf
    if not is_owner(q.from_user.id): return
    if q.data == "recreate_no": return await q.message.edit("❌ Cancelled")

    _rf = True
    owner = db['owner_id']
    db.update({'admins': set(), 'banned': set(), 'users': {}, 'monitored': {}, 'thumbnails': {}, 'thumb_last_used': {}, 'owner_id': owner})
    save_db()

    for d in [DOWNLOAD_DIR, THUMB_DIR, WORKER_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
            os.makedirs(d)

    _rf = False
    await q.message.edit("✅ **RECREATED!**")

@bot.on_message(filters.text & filters.private & ~filters.command(['start','set','batch','list','del','time','status','gstatus','cleanup','thum','seethum','delthum','setcaption','seecaption','delcaption','ban','unban','seeban','admin','radmin','seeadmin','reset','dashboard','unlock','destroy','abort','recreate','pm','repm','pmlist','npcleanup','refer','mystats','refstats','toprefs','invalidate','backup','update','restore','validate']))
async def auto_url_handler(c, m):
    text = m.text.strip()
    if text.startswith("http://") or text.startswith("https://"):
        await handle_url(c, m)

# ==========================================
# START
# ==========================================
async def main():
    global download_semaphore, upload_semaphore, batch_semaphore, queue_lock

    print("\n" + "="*50)
    print("🎌 ANIME AUTO DOWNLOADER v12.0")
    print("="*50)

    load_db()
    print(f"👑 Owner: {db['owner_id']}")
    print(f"📊 Users: {len(db['users'])} | Series: {len(db['monitored'])}")

    download_semaphore = asyncio.Semaphore(MAX_DOWNLOAD_WORKERS)
    upload_semaphore = asyncio.Semaphore(MAX_UPLOAD_PARALLEL)
    batch_semaphore = asyncio.Semaphore(MAX_BATCH_WORKERS)
    queue_lock = asyncio.Lock()

    await bot.start()
    me = await bot.get_me()
    
    # ✅ FIX: Cache bot username globally
    global BOT_USERNAME
    BOT_USERNAME = me.username
    
    print(f"✅ @{BOT_USERNAME}")

    for i in range(MAX_DOWNLOAD_WORKERS):
        asyncio.create_task(download_worker(i))
    print(f"⚙️ {MAX_DOWNLOAD_WORKERS} download workers")

    asyncio.create_task(monitor())
    asyncio.create_task(auto_cleanup())
    asyncio.create_task(thumb_cleanup())
    asyncio.create_task(premium_expiry_monitor())
    asyncio.create_task(backup_scheduler())
    print("🔄 Background tasks started")

    print(f"\n🟢 Message Worker: Always Free")
    print(f"🔵 Download Workers: {MAX_DOWNLOAD_WORKERS}")
    print(f"🟡 Batch Workers: {MAX_BATCH_WORKERS}")
    print(f"📤 Upload Slots: {MAX_UPLOAD_PARALLEL}")
    print("="*50 + "\n")

    await idle()
    save_db()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())