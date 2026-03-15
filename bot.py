import os
import io
import sys
import time
import math
import asyncio
import json
import re
from curl_cffi import requests as currequests
from contextlib import redirect_stdout
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import MessageNotModified, FloodWait
import logging
import shutil
from flask import Flask
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AnimeBot")

# Download Directory Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# Global PATH fix for FFmpeg/FFprobe based on environment (Docker or Heroku)
for p in ["/app/.apt/usr/bin", "/app/vendor/ffmpeg/bin", "/usr/bin"]:
    if os.path.exists(p) and p not in os.environ["PATH"]:
        os.environ["PATH"] += os.pathsep + p

# Silence noisy libraries
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.ERROR) # For Flask

# Import your custom modules
# Helper for safe message edits with throttling
EDIT_STATES = {} # {message_id: {"time": stamp, "text": content}}

async def safe_edit(message: Message, text: str, force: bool = False, **kwargs):
    now = time.time()
    mid = message.id
    
    # Initialize state if not present
    if mid not in EDIT_STATES:
        EDIT_STATES[mid] = {"time": 0, "text": ""}
        
    # Throttling logic
    if not force:
        # Skip if text is same
        if text == EDIT_STATES[mid]["text"]:
            return
        # Skip if too frequent (min 3 seconds)
        if now - EDIT_STATES[mid]["time"] < 3.0:
            return

    try:
        await message.edit(text, **kwargs)
        EDIT_STATES[mid] = {"time": now, "text": text}
    except MessageNotModified:
        EDIT_STATES[mid]["text"] = text
    except FloodWait as e:
        logger.warning(f"FloodWait hit: Waiting {e.value} seconds...")
        await asyncio.sleep(e.value)
        try:
            # Retry once with force
            await message.edit(text, **kwargs)
            EDIT_STATES[mid] = {"time": time.time(), "text": text}
        except Exception: pass
    except Exception as e:
        logger.error(f"Safe edit failed: {e}")

from HindiAnimeZone import HindiAnimeZone
from RareAnimes import RareAnimes

API_ID = int(os.environ.get("API_ID", 23200475))
API_HASH = os.environ.get("API_HASH", "644e1d9e8028a5295d6979bb3a36b23b")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "7810985319:AAHSaD-YlHThm2JPoOY_vnJLs9jXpaWs4ts")
OWNER_ID = int(os.environ.get("OWNER_ID", 7074383232))
AUTH_CHAT = int(os.environ.get("AUTH_CHAT", -1003192464251))

app = Client(
    "anime_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

def is_authorized(message: Message) -> bool:
    user_id = message.from_user.id if message.from_user else 0
    chat_id = message.chat.id if message.chat else 0
    
    if user_id == OWNER_ID:
        return True
    if chat_id == AUTH_CHAT:
        return True
    return False

def humanbytes(size):
    if not size: return ""
    power = 2**10
    n = 0
    Dic_powerN = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + Dic_powerN[n] + 'B'

def time_formatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = ((str(days) + "d, ") if days else "") + \
          ((str(hours) + "h, ") if hours else "") + \
          ((str(minutes) + "m, ") if minutes else "") + \
          ((str(seconds) + "s, ") if seconds else "")
    return tmp[:-2] if tmp else "0s"
async def progress_for_pyrogram(current, total, ud_type, message, start):
    now = time.time()
    diff = now - start
    if diff < 1.0: return # Avoid division by zero and too early updates
    
    is_done = (current == total)
    percentage = current * 100 / total if total else 0
    speed = current / diff if diff > 0 else 0
    elapsed_time = round(diff) * 1000
    time_to_completion = round((total - current) / speed) * 1000 if speed > 0 else 0
    estimated_total_time = elapsed_time + time_to_completion

    elapsed_time_str = time_formatter(elapsed_time)
    eta_str = time_formatter(time_to_completion)

    progress = "[{0}{1}] \n**Progress**: {2}%\n".format(
        ''.join(["█" for i in range(math.floor(percentage / 10))]),
        ''.join(["░" for i in range(10 - math.floor(percentage / 10))]),
        round(percentage, 2))

    tmp = progress + "**Processed**: {0} of {1}\n**Speed:** {2}/s\n**ETA:** {3}\n".format(
        humanbytes(current),
        humanbytes(total),
        humanbytes(speed),
        eta_str
    )
    
    await safe_edit(message, f"{ud_type}\n{tmp}", force=is_done)

def clean_unwanted_tags(text):
    """Removes common distributor tags and unwanted strings."""
    if not text: return ""
    # Remove things like @ChannelName, [Distributor], websites, etc.
    text = re.sub(r'@[\w_]+', '', text)
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\(.*?\)', '', text)
    text = re.sub(r'www\.\S+', '', text)
    # Remove common quality/format tags that might be left over
    text = re.sub(r'Dubbed|Hindi|Dual|Audio|Multi|UNCUT|Eng|Sub|Soft|Hard|ESub|HSub|SD|HD|FHD|4K|Ultra|Web-DL|BluRay|x264|x265|HEVC', '', text, flags=re.I)
    # Remove leftover separators
    text = re.sub(r'[\_\.\-]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_filename(basename):
    """Extracts Season, Episode, Year and Name from filename or title string."""
    data = {"name": "Unknown", "season": None, "episode": None, "year": None, "is_movie": False}
    
    # Try to find year (4 digits in brackets or standalone 19xx/20xx)
    yr_match = re.search(r'\(?(19|20)\d{2}\)?', basename)
    if yr_match:
        data["year"] = yr_match.group(0).strip('()')
        
    # Detect if Movie
    if any(x in basename.lower() for x in ["movie", "film"]):
        data["is_movie"] = True
        
    # Season patterns: S2, S02, Season 2, 2nd Season
    s_match = re.search(r'Season\s*(\d+)', basename, re.I) or \
              re.search(r'S(\d+)', basename, re.I) or \
              re.search(r'(\d+)(?:st|nd|rd|th)\s*Season', basename, re.I)
    if s_match:
        data["season"] = s_match.group(1).zfill(2)
        
    # Episode patterns: E01, Episode 01, Ep 01, Ep.01
    ep_match = re.search(r'E(\d+)', basename, re.I) or \
               re.search(r'Episode\s*(\d+)', basename, re.I) or \
               re.search(r'Ep[\.\s]*(\d+)', basename, re.I)
    
    if ep_match:
        data["episode"] = ep_match.group(1).zfill(2)
    else:
        # Standalone number check: "Solo Leveling 01" -> 01
        nums = re.findall(r'(?:\s|^)(\d{1,4})(?:\b|(?=[^\dp]))', basename)
        for n in nums:
            if n != data["year"] and int(n) < 2000:
                data["episode"] = n.zfill(2)
                break
        
    # Clean Name extraction
    name_clean = basename
    # Remove extension
    name_clean = re.sub(r'\.(mp4|mkv|avi|webm)$', '', name_clean, flags=re.I)
    
    # Remove metadata components from name
    patterns_to_remove = [
        r'S\d+|Season\s*\d+|(\d+)(?:st|nd|rd|th)\s*Season',
        r'E\d+|Episode\s*\d+|Ep[\.\s]*\d+',
        r'\d{3,4}p',
        r'\(?(19|20)\d{2}\)?'
    ]
    # Also if we found an episode number, specifically remove it if it's standalone
    if data["episode"]:
        ep_no = data["episode"].lstrip('0') or "0"
        patterns_to_remove.append(rf'(?:\s|^){ep_no}(?:\b|(?=[^\dp]))')
        patterns_to_remove.append(rf'(?:\s|^){data["episode"]}(?:\b|(?=[^\dp]))')

    for p in patterns_to_remove:
        name_clean = re.sub(p, '', name_clean, flags=re.I)

    name_clean = clean_unwanted_tags(name_clean)
    
    if name_clean:
        # Remove trailing/leading hyphens/dots/spaces
        name_clean = re.sub(r'^[ \.\_\-]+|[ \.\_\-]+$', '', name_clean)
        # Collapse multiple hyphens/dots/spaces
        name_clean = re.sub(r'[\.\-\_]{2,}', '-', name_clean)
        name_clean = re.sub(r'\s+', ' ', name_clean)
        data["name"] = name_clean.strip()
        
    return data

def get_real_quality(filepath):
    """Extracts quality string from filename."""
    basename = os.path.basename(filepath)
    match = re.search(r'(\d{3,4}p)', basename, re.I)
    if match:
        return match.group(1)
    return "720p" # Default fallback

async def get_audio_language(filepath):
    """Best effort audio language detection using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "a",
            "-show_entries", "stream_tags=language", "-of", "json", filepath
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        data = json.loads(stdout)
        
        langs = []
        for stream in data.get("streams", []):
            lang = stream.get("tags", {}).get("language")
            if lang:
                # Map codes to Names
                if lang == "hin": langs.append("Hindi")
                elif lang == "eng": langs.append("English")
                elif lang == "jpn": langs.append("Japanese")
                else: langs.append(lang.capitalize())
        
        if langs:
            # Avoid duplicates and join
            return " / ".join(sorted(list(set(langs))))
    except: pass
    return "Hindi" # Fallback for your use case

def make_caption(filepath, size, duration, series_info=None):
    """Generate professional Telegram caption using the real filename as primary source."""
    basename = os.path.basename(filepath)
    # 1. Try to parse metadata from the REAL filename
    info = parse_filename(basename)
    
    # 2. Extract series info from page title as a strong fallback for Name
    series_name = None
    if series_info:
        series_name = parse_filename(series_info)['name']
        series_name = clean_unwanted_tags(series_name)
    
    # 3. Final cleanup and merge
    # Prefer series_name from page if filename name is messy or too short
    file_name_parsed = clean_unwanted_tags(info['name'])
    
    if series_name and (len(series_name) > len(file_name_parsed) or file_name_parsed == "Unknown"):
        display_name = series_name
    else:
        display_name = file_name_parsed or series_name or "Unknown Anime"
    
    # Ensure season/year fallbacks from page title
    if series_info:
        info_alt = parse_filename(series_info)
        if not info['season']: info['season'] = info_alt['season']
        if not info['year']: info['year'] = info_alt['year']
        if not info['is_movie']: info['is_movie'] = info_alt['is_movie']
    
    # Quality extraction from filename
    quality = get_real_quality(filepath)
    
    # Formatting
    season = info.get('season')
    episode = info.get('episode')
    
    size_str = humanbytes(size)
    duration_str = time_formatter(duration * 1000)
    
    if info['is_movie']:
        cap = f"🎬 **{display_name}**"
        if info['year']: cap += f" ({info['year']})"
        cap += f"""
╭━━━━━━━━━━━━━━━━━━━╮
│ 🍿 **Type:** Movie
│ 🌐 **Language:** Hindi
│ 📊 **Quality:** {quality.upper()}
│ 📦 **Size:** {size_str}
│ ⏱️ **Duration:** {duration_str}
╰━━━━━━━━━━━━━━━━━━━╯"""
    else:
        # Professional format: Anime Name (Year) \n ╭ \n │ ✨ season : 01 \n │ 📺 Episode: 01 \n ...
        cap = f"🎬 **{display_name}**"
        if info['year']: cap += f" ({info['year']})"
        cap += f"\n╭━━━━━━━━━━━━━━━━━━━╮"
        if season:
            cap += f"\n│ ✨ **season :** {season}"
        cap += f"\n│ 📺 **Episode:** {episode if episode else 'N/A'}" 
        cap += f"""
│ 🌐 **Language:** Hindi
│ 📊 **Quality:** {quality.upper()}
│ 📦 **Size:** {size_str}
│ ⏱️ **Duration:** {duration_str}
╰━━━━━━━━━━━━━━━━━━━╯"""
    return cap

async def get_video_metadata(filepath):
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration",
            "-of", "json", filepath
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        data = json.loads(stdout)
        
        stream = data.get("streams", [{}])[0]
        width = int(stream.get("width", 0)) if stream.get("width") else 0
        height = int(stream.get("height", 0)) if stream.get("height") else 0
        duration_str = stream.get("duration")
        duration = int(float(duration_str)) if duration_str else 0
        return width, height, duration
    except Exception as e:
        logger.error(f"Metadata error: {e}")
        return 0, 0, 0

async def generate_thumbnail(filepath, duration):
    if duration <= 0: duration = 5
    thumb_path = f"{filepath}.jpg"
    time_mark = max(int(duration * 0.15), min(10, duration // 2))
    
    try:
        cmd = [
            "ffmpeg", "-v", "error", "-ss", str(time_mark),
            "-i", filepath, "-vframes", "1", "-q:v", "2", thumb_path, "-y"
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        if os.path.exists(thumb_path):
            return thumb_path
    except Exception as e:
        logger.error(f"Thumbnail error: {e}")
    return None

async def download_file(url, file_name, status_msg, referer=None, cookies=None, user_agent=None):
    logger.info(f"Step 5: File download started -> {file_name}")
    start_t = time.time()
    
    # Attempt high-speed download with aria2c first
    try:
        # Optimization: use 16 connections for max speed
        cmd = [
            "aria2c", "-x", "16", "-s", "16", "-k", "1M",
            "--user-agent", user_agent or USER_AGENT,
            "--out", file_name,
            "--file-allocation=none",
            "--summary-interval=1",
            "--continue=true",
            url
        ]
        if referer: cmd.extend(["--referer", referer])
        if cookies:
            cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
            cmd.extend(["--header", f"Cookie: {cookie_str}"])

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )

        while True:
            line = await process.stdout.readline()
            if not line: break
            line = line.decode('utf-8', 'ignore').strip()
            
            # aria2c output example: [#2089b0 1.2MiB/3.4MiB(35%) CN:1 SPD:648KiB ETA:3s]
            match = re.search(r'([\d\.]+[KMG]i?B)/([\d\.]+[KMG]i?B)\((\d+)%\).*?SPD:([\d\.]+[KMG]i?B).*?ETA:(\d+s|[\d\w]+)', line)
            if match:
                curr_size = match.group(1).replace('i', '')
                total_size = match.group(2).replace('i', '')
                perc_val = int(match.group(3))
                speed = match.group(4).replace('i', '') + '/s'
                eta = match.group(5)
                
                # Progress bar logic
                filled = "█" * (perc_val // 10)
                empty = "░" * (10 - (perc_val // 10))
                bar = f"[{filled}{empty}]"
                
                await safe_edit(
                    status_msg, 
                    f"🚀 **Downloading (High Speed)**\n {bar} {perc_val}%\n"
                    f"**Size**: {curr_size} / {total_size}\n"
                    f"**Speed**: {speed}\n"
                    f"**ETA**: {eta}",
                    force=False
                )
        
        await process.wait()
        if process.returncode == 0 and os.path.exists(file_name):
            logger.info(f"aria2c download complete -> {file_name}")
            return True
        else:
            logger.warning(f"aria2c failed (code {process.returncode}). Falling back to curl_cffi...")
    except Exception as e:
        logger.warning(f"aria2c setup error: {e}. Falling back to curl_cffi...")

    # Fallback to curl_cffi
    try:
        state = {"downloaded": 0, "total": 0, "done": False, "error": None}
        
        def sync_download():
            try:
                # Using curl_cffi for robust impersonation
                with currequests.Session(impersonate="chrome110") as session:
                    session.headers.update({
                        "User-Agent": user_agent or USER_AGENT,
                        "Accept": "*/*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Connection": "keep-alive"
                    })
                    if referer: session.headers['Referer'] = referer
                    if cookies: session.cookies.update(cookies)
                        
                    r = session.get(url, stream=True, timeout=60, allow_redirects=True)
                    if r.status_code != 200:
                        state["error"] = f"HTTP {r.status_code}"; return
                    
                    state["total"] = int(r.headers.get('content-length', 0))
                    with open(file_name, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                                state["downloaded"] += len(chunk)
            except Exception as e: state["error"] = str(e)
            finally: state["done"] = True

        loop = asyncio.get_running_loop()
        task = loop.run_in_executor(None, sync_download)
        
        while not state["done"]:
            await asyncio.sleep(2.5)
            if state["total"] > 0:
                now = time.time()
                diff = now - start_t
                perc = state["downloaded"] * 100 / state["total"]
                speed = state["downloaded"] / diff if diff > 0 else 0
                eta_sec = (state["total"] - state["downloaded"]) / speed if speed > 0 else 0
                
                # Progress bar for fallback
                filled = "█" * (int(perc) // 10)
                empty = "░" * (10 - (int(perc) // 10))
                bar = f"[{filled}{empty}]"
                
                await safe_edit(
                    status_msg,
                    f"🐢 **Downloading (Low Speed)**\n {bar} {round(perc, 2)}%\n"
                    f"**Size**: {humanbytes(state['downloaded'])} / {humanbytes(state['total'])}\n"
                    f"**Speed**: {humanbytes(speed)}/s\n"
                    f"**ETA**: {time_formatter(eta_sec*1000)}",
                    force=False
                )
                
        await task
        if state["error"]:
            await safe_edit(status_msg, f"❌ Download failed: {state['error']}", force=True)
            return False
            
        logger.info(f"Step 5: File download complete -> {file_name}")
        return True
    except Exception as e:
        logger.error(f"Download error: {str(e)}", exc_info=True)
        await safe_edit(status_msg, f"❌ Download failed: {str(e)}", force=True)
        return False

def parse_selection(args):
    """Parses selection strings like '1', '1-5', '1 5' into a list of indices or (start, end)."""
    if not args:
        return None
    
    # Handle '1 5' format
    if len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        s, e = int(args[0]), int(args[1])
        return list(range(min(s, e), max(s, e) + 1))
    
    selection_str = "".join(args)
    indices = set()
    for part in selection_str.split(','):
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                indices.update(range(min(start, end), max(start, end) + 1))
            except ValueError: continue
        elif part.isdigit():
            indices.add(int(part))
    
    return sorted(list(indices)) if indices else None

@app.on_message(filters.command("start") & filters.incoming)
async def start_cmd(client, message):
    if not is_authorized(message):
        return await message.reply("⛔ Aap authorized nahi hain is bot ko use karne ke liye.")
    await message.reply("✅ Bot is alive and authorized!\n\n**Usage:**\n`/get <url> [selection]`\nSelection examples: `1`, `1-5`, `1 5`, `1,3,5-7`")

@app.on_message(filters.command("get") & filters.incoming)
async def get_cmd(client, message):
    if not is_authorized(message):
        return await message.reply("⛔ Unauthorized")
        
    if len(message.command) < 2:
        return await message.reply("Usage: `/get <url> [selection]`\nExample: `/get https://... 1-5`")
        
    url = message.command[1]
    selection_args = message.command[2:]
    selection = parse_selection(selection_args)
    
    logger.info(f"Step 1: Command received | URL: {url} | Selection: {selection}")
    status_msg = await message.reply("🔄 Analyzing link...")
    
    try:
        # Site Detection
        logger.info("Step 2: URL validation & Site detection")
        if "hindianimezone.com" in url:
            logger.info("Detected site: HindiAnimeZone")
            await handle_hindianime(client, message, url, selection, status_msg)
        elif any(x in url for x in ["rareanimes.app", "codedew.com"]):
            logger.info("Detected site: RareAnimes")
            await handle_rareanime(client, message, url, selection, status_msg)
        else:
            logger.warning("Unsupported site detected.")
            await status_msg.edit("❌ Unsupported site. Sirf HindiAnimeZone aur RareAnimes supported hain.")
            
    except Exception as e:
        logger.error(f"Error in get_cmd: {e}", exc_info=True)
        await status_msg.edit(f"❌ Error occurred: {str(e)}")

def parse_quality(label):
    """Extracts numeric quality from label for sorting."""
    match = re.search(r'(\d+)', str(label))
    if match:
        return int(match.group(1))
    return 0

def sort_qualities(downloads):
    """Sorts download objects from low to high quality."""
    return sorted(downloads, key=lambda x: parse_quality(x.get('label', '')))

async def process_episode_batch(client, message, episode_data, status_msg, total_eps, current_idx, series_info=None):
    """Processes all qualities for a single episode together."""
    ep_name = episode_data.get("episode", "Unknown Episode")
    downloads = sort_qualities(episode_data.get("downloads", []))
    
    if not downloads:
        logger.warning(f"No downloads for episode: {ep_name}")
        return
    
    q_labels = [d.get('label', 'N/A') for d in downloads]
    logger.info(f"Processing Ep {current_idx}/{total_eps} with qualities: {q_labels}")
    
    for q_idx, dl_obj in enumerate(downloads, 1):
        dl_url = dl_obj.get('link')
        quality_label = dl_obj.get('label', f'Q{q_idx}')
        
        # Clean up URL if needed
        if isinstance(dl_url, str) and ': http' in dl_url:
            dl_url = 'http' + dl_url.split(': http')[1].strip()
            
        if not dl_url: continue
        
        # Create a specific directory for this request/message
        req_id = f"{message.id}"
        req_dir = os.path.join(DOWNLOAD_DIR, req_id)
        if not os.path.exists(req_dir):
            os.makedirs(req_dir)
            
        # Get metadata for the download
        metadata = dl_obj.get('metadata', {})
        ref = metadata.get("referer") or episode_data.get("referer")
        cookies = metadata.get("cookies") or episode_data.get("cookies")
        ua = metadata.get("user_agent") or episode_data.get("user_agent")
        
        # Resolve real filename
        await safe_edit(status_msg, f"🔍 **Resolving real filename for {quality_label}...**", force=True)
        rare = RareAnimes()
        real_name = await asyncio.to_thread(rare.resolve_filename, dl_url, referer=ref, cookies=cookies)
        
        if real_name:
            # Sanitize filename: remove restricted characters but keep extension
            real_name = re.sub(r'[\\/*?:"<>|]', "", real_name)
            # Ensure it's not too long and has no double spaces
            real_name = re.sub(r'\s+', ' ', real_name).strip()
            file_base = real_name
            logger.info(f"Resolved real filename: {file_base}")
        else:
            file_base = f"download_{message.id}_{current_idx}_{quality_label.replace(' ', '_')}.mkv"
            logger.info(f"Failed to resolve filename, using fallback: {file_base}")

        file_path = os.path.join(req_dir, file_base)

        success = await download_file(dl_url, file_path, status_msg, referer=ref, cookies=cookies, user_agent=ua)
        if not success:
            await safe_edit(status_msg, f"❌ Failed to download: {ep_name} [{quality_label}]", force=True)
            continue
            
        await safe_edit(status_msg, f"⚙️ **Extracting Metadata Ep {current_idx}/{total_eps} [{quality_label}]...**", force=True)
        width, height, duration = await get_video_metadata(file_path)
        thumb_path = await generate_thumbnail(file_path, duration)
        
        await safe_edit(status_msg, f"📤 **Uploading Ep {current_idx}/{total_eps} [{quality_label}]...**", force=True)
        start_time = time.time()
        
        file_size = os.path.getsize(file_path)
        
        # Detect audio language async
        lang = await get_audio_language(file_path)
        
        # Generate professional caption
        cap = make_caption(file_path, file_size, duration, series_info=series_info)
        # Force language in caption if detected
        if lang:
            cap = cap.replace("│ 🌐 **Language:** Hindi", f"│ 🌐 **Language:** {lang}")

        try:
            await client.send_video(
                chat_id=message.chat.id,
                video=file_path,
                caption=cap,
                duration=duration,
                width=width,
                height=height,
                thumb=thumb_path,
                progress=progress_for_pyrogram,
                progress_args=(f"📤 **Uploading Ep {current_idx} [{quality_label}]...**", status_msg, start_time)
            )
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            await message.reply(f"❌ Upload failed for {ep_name} [{quality_label}]: {str(e)}")
            
        # Cleanup folder after each episode processing or entirely after loop
        # For simplicity and robust cleanup per request, we can keep the folder until done
        # but the request asks to do it "leech bot jaisa" which usually means cleaning up after upload.
        try:
            if os.path.exists(req_dir):
                shutil.rmtree(req_dir)
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

async def handle_hindianime(client, message, url, selection, status_msg):
    try:
        logger.info(f"Step 3: Starting HindiAnimeZone bypass for URL: {url}")
        bypasser = HindiAnimeZone()
        episodes = await asyncio.to_thread(bypasser.pro_main_bypass, url, selection=selection)
            
        if not episodes:
            logger.warning("Step 4 Failed: No download links found.")
            return await status_msg.edit("❌ Koi download links nahi mile.")
            
        total = len(episodes)
        series_info = episodes[0].get("series_info") if episodes else None # HindiAnimeZone might not have it yet
        logger.info(f"Step 4: Extracted {total} episodes with structured qualities.")
        await status_msg.edit(f"✅ Found {total} episodes. Processing qualities...")
        
        for idx, ep in enumerate(episodes, 1):
            await process_episode_batch(client, message, ep, status_msg, total, idx, series_info=series_info)
            
        await status_msg.delete()
        logger.info("Process completed successfully for HindiAnimeZone.")
            
    except Exception as e:
        logger.error(f"HindiAnimeZone Error: {e}", exc_info=True)
        await status_msg.edit(f"❌ HindiAnimeZone Error: {str(e)}")

async def handle_rareanime(client, message, url, selection, status_msg):
    try:
        logger.info(f"Step 3: Starting RareAnimes bypass for URL: {url}")
        ep_start, ep_end = None, None
        if selection:
            ep_start = min(selection)
            ep_end = max(selection)
            
        def bypass_func():
            bypasser = RareAnimes()
            return bypasser.get_links(url, ep_start=ep_start, ep_end=ep_end, verbose=True)
            
        res = await asyncio.to_thread(bypass_func)
        
        if "error" in res and not res.get("episodes"):
            logger.error(f"Step 3 Failed: RareAnimes API Error: {res.get('error')}")
            return await safe_edit(status_msg, f"❌ API fail: {res.get('error')}")
            
        episodes = res.get("episodes", [])
        if not episodes:
            logger.warning("Step 4 Failed: No episodes returned.")
            return await safe_edit(status_msg, "❌ Koi episodes nahi mile.")
            
        total = len(episodes)
        series_info = res.get("series_info")
        logger.info(f"Step 4: Found {total} episodes to process.")
        await safe_edit(status_msg, f"✅ Found {total} episodes. Processing qualities...")
        
        for idx, ep in enumerate(episodes, 1):
            await process_episode_batch(client, message, ep, status_msg, total, idx, series_info=series_info)
            
        await status_msg.delete()
        logger.info("Process completed successfully for RareAnimes.")
            
    except Exception as e:
        logger.error(f"RareAnimes Error: {e}", exc_info=True)
        await status_msg.edit(f"❌ RareAnimes Error: {str(e)}")
            
        await status_msg.delete()
        logger.info("Process completed successfully for RareAnimes.")
            
    except Exception as e:
        logger.error(f"RareAnimes Error: {e}", exc_info=True)
        await status_msg.edit(f"❌ RareAnimes Error: {str(e)}")


# Flask app for health check
flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health_check():
    return "Bot is running!", 200

def run_flask():
    port = int(os.environ.get("PORT", 8000))
    print(f"Flask health check server starting on port {port}...")
    flask_app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    print("-" * 30)
    print("🚀 ANIME BOT STARTING...")
    print("-" * 30)
    Thread(target=run_flask, daemon=True).start()
    print("✅ Health Check Server: OK")
    print("✅ Telegram Bot: STARTING...")
    print("-" * 30)
    app.run()
