"""Desi49 Bot PRO v5.7 - HyperUL Stability Edition
✅ Multi-Session Parallel Upload | ✅ Stabilized Connections | ✅ No Info Logs"""
import os, re, time, base64, asyncio, logging, psutil, uuid, struct, math, types, random
from math import ceil
from random import randint
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass
try:
    import tgcrypto
    HAS_TGCRYPTO = True
except ImportError:
    HAS_TGCRYPTO = False
from urllib.parse import unquote
import aiohttp, aiofiles
from pyrogram import Client, filters, enums, raw
from pyrogram.types import Message
from pyrogram.session import Session

# --- LOGGING SETUP (Quiet Mode) ---
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.WARNING,
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log")]
)
log = logging.getLogger("desi49bot")

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID = int(os.environ.get("OWNER_ID", 0))
DUMP_CHANNEL = int(os.environ.get("DUMP_CHANNEL", 0))

DOWNLOAD_DIR = "downloads"
MAX_TG_SIZE = 2 * 1024 * 1024 * 1024
PROGRESS_BAR_LEN = 15
MAX_CONCURRENT_TASKS = 1

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://desii49.com/",
}

app = Client(
    "desi49bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    workers=50,
    max_concurrent_transmissions=20
)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

class TaskManager:
    ACTIVE_TASKS = {}
    PENDING_QUEUE = asyncio.Queue()
    LOCK = asyncio.Lock()

    @classmethod
    def generate_task_id(cls, label: str, ep_num: int = 1) -> str:
        return uuid.uuid4().hex[:7]

    @classmethod
    async def add_to_queue(cls, user_id: int, url: str, message: Message, status_msg: Message, task_id: str):
        await cls.PENDING_QUEUE.put({"user_id": user_id, "url": url, "message": message, "status_msg": status_msg, "task_id": task_id})

    @classmethod
    def is_cancelled(cls, task_id: str) -> bool:
        return cls.ACTIVE_TASKS.get(task_id, {}).get("cancelled", False)

    @classmethod
    def register_task(cls, task_id: str, group_id: str = None):
        cls.ACTIVE_TASKS[task_id] = {"proc": None, "cancelled": False, "group": group_id}
        if group_id:
            if group_id not in cls.ACTIVE_TASKS:
                cls.ACTIVE_TASKS[group_id] = {"tasks": [], "type": "group"}
            cls.ACTIVE_TASKS[group_id]["tasks"].append(task_id)

    @classmethod
    def unregister_task(cls, task_id: str):
        cls.ACTIVE_TASKS.pop(task_id, None)

    @classmethod
    async def cancel_task(cls, task_id: str):
        if task_id in cls.ACTIVE_TASKS:
            cls.ACTIVE_TASKS[task_id]["cancelled"] = True
            proc = cls.ACTIVE_TASKS[task_id].get("proc")
            if proc and proc.returncode is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
            return True
        return False

class HyperTGUpload:
    """Stabilized Parallel session uploader"""
    GLOBAL_SEMAPHORE = asyncio.Semaphore(12) # Limit total parallel sessions

    def __init__(self, client, workers=5):
        self.client = client
        self.workers = workers # Reduced default workers
        self._processed = 0
        self._sessions = []
        self._cancelled = False

    async def _start_session(self):
        async with self.GLOBAL_SEMAPHORE:
            dc_id = await self.client.storage.dc_id()
            auth_key = await self.client.storage.auth_key()
            test_mode = await self.client.storage.test_mode()
            session = Session(self.client, dc_id, auth_key, test_mode, is_media=True)
            await session.start()
            self._sessions.append(session)
            return session

    async def _worker(self, queue, file_path, file_id, is_big, total_parts, chunk_size):
        session = await self._start_session()
        try:
            async with aiofiles.open(file_path, "rb") as f:
                while not self._cancelled:
                    part_no = await queue.get()
                    if part_no is None:
                        queue.task_done()
                        break
                    
                    await f.seek(part_no * chunk_size)
                    data = await f.read(chunk_size)
                    rpc = raw.functions.upload.SaveBigFilePart(file_id=file_id, file_part=part_no, file_total_parts=total_parts, bytes=data) if is_big else \
                          raw.functions.upload.SaveFilePart(file_id=file_id, file_part=part_no, bytes=data)
                    
                    # Retry logic with timeout
                    for attempt in range(3):
                        try:
                            await asyncio.wait_for(session.invoke(rpc), timeout=45)
                            break
                        except Exception as e:
                            if attempt == 2:
                                log.error(f"HyperUL worker error on part {part_no}: {e}")
                                self._cancelled = True
                            await asyncio.sleep(2 * (attempt + 1))
                    
                    self._processed += len(data)
                    queue.task_done()
        except Exception as e:
            log.error(f"HyperUL worker CRITICAL error: {e}")
        finally:
            if session:
                try: await session.stop()
                except: pass

    async def save_file(self, path, progress=None, progress_args=()):
        size = os.path.getsize(path)
        chunk_size = 512 * 1024
        total_parts = ceil(size / chunk_size)
        if total_parts > 4000:
            chunk_size = 1024 * 1024
            total_parts = ceil(size / chunk_size)
        
        file_id = randint(0, 2**63 - 1)
        is_big = size > 10 * 1024 * 1024
        
        queue = asyncio.Queue(maxsize=self.workers * 2)
        # Put parts in queue
        for p in range(total_parts): await queue.put(p)
        for _ in range(self.workers): await queue.put(None)
        
        tasks = [asyncio.create_task(self._worker(queue, path, file_id, is_big, total_parts, chunk_size)) for i in range(self.workers)]
        
        if progress:
            async def report():
                while not all(t.done() for t in tasks):
                    await progress(self._processed, size, *progress_args)
                    await asyncio.sleep(4)
            asyncio.create_task(report())

        await asyncio.gather(*tasks)
        if self._cancelled: return None
        if is_big: return raw.types.InputFileBig(id=file_id, parts=total_parts, name=os.path.basename(path))
        return raw.types.InputFile(id=file_id, parts=total_parts, name=os.path.basename(path), md5_checksum="")

def get_system_stats(task_start: float = None):
    cpu = psutil.cpu_percent(interval=0.1)
    ram = psutil.virtual_memory()
    try: disk = psutil.disk_usage(os.getcwd())
    except: disk = psutil.disk_usage("/")

    net_mbps = 0.0
    if task_start and time.time() - task_start > 0.5:
        try:
            net = psutil.net_io_counters()
            elapsed = time.time() - task_start
            net_mbps = ((net.bytes_recv + net.bytes_sent) / elapsed) * 8 / 1_000_000
        except: pass

    return {
        'cpu': round(cpu, 1),
        'ram_used': round(ram.used / (1024**3), 1),
        'ram_total': round(ram.total / (1024**3), 1),
        'disk_free': round(disk.free / (1024**3), 1),
        'net_mbps': round(net_mbps, 1)
    }

def progress_bar(p: float, length: int = PROGRESS_BAR_LEN) -> str:
    f = int(length * p / 100)
    return f"[{'█'*f}{'░'*(length-f)}]"

def format_size(b: float) -> str:
    if not b: return "0 B"
    for u in ['B','KB','MB','GB','TB']:
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} PB"

def format_eta(sec: int) -> str:
    sec = max(0, int(sec))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def clean_title(raw: str) -> str:
    if not raw: return "Video"
    raw = re.sub(r'\.(mp4|mkv|avi|webm)$', '', raw, flags=re.I)
    raw = re.sub(r'[_\-]?[0-9a-f]{20,}', '', raw)
    raw = re.sub(r'[A-Za-z0-9]{15,}[_\-]?', '', raw)
    cleaned = re.sub(r'[^\w\s]', ' ', raw)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned[:80] if len(cleaned) > 5 else raw[:80]

async def get_video_metadata(filepath: str):
    duration, width, height = 0, 0, 0
    try:
        with open(filepath, "rb") as f:
            while True:
                data = f.read(8)
                if len(data) < 8: break
                size, tag = struct.unpack(">I4s", data)
                if size == 1:
                    size = struct.unpack(">Q", f.read(8))[0]
                    data_size = size - 16
                else: data_size = size - 8

                if tag == b'moov':
                    moov_data = f.read(data_size)
                    mvhd_idx = moov_data.find(b'mvhd')
                    if mvhd_idx != -1:
                        version = moov_data[mvhd_idx + 4]
                        if version == 0: timescale, dur = struct.unpack(">II", moov_data[mvhd_idx+16:mvhd_idx+24])
                        else: timescale, dur = struct.unpack(">IQ", moov_data[mvhd_idx+24:mvhd_idx+36])
                        if timescale > 0: duration = int(dur / timescale)
                    
                    tkhd_idx = moov_data.find(b'tkhd')
                    if tkhd_idx != -1:
                        version = moov_data[tkhd_idx + 4]
                        offset = 84 if version == 0 else 96
                        w, h = struct.unpack(">II", moov_data[tkhd_idx+offset:tkhd_idx+offset+8])
                        width, height = w >> 16, h >> 16
                    break
                else: f.seek(data_size, 1)
    except: pass
    return duration, width, height

async def extract_video_info(page_url: str) -> dict:
    if page_url.lower().endswith(('.mp4', '.mkv', '.webm')):
        return {"video_url": page_url, "title": clean_title(unquote(page_url.split("/")[-1]))}
    
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession(headers=HEADERS) as session:
                async with session.get(page_url, timeout=20) as resp:
                    html = (await resp.read()).decode('utf-8', errors='ignore')
                    result = {"video_url": None, "title": None}
                    for pat in [r'<meta\s+itemprop=["\']contentURL["\']\s+content=["\']([^"\']+)["\']', r'player-x\.php\?q=([A-Za-z0-9+/=]+)', r'<source\s+src=["\']([^"\']+)["\']', r'(https?://[^\s"<>]+\.mp4[^\s"<>]*)']:
                        m = re.search(pat, html)
                        if m:
                            url = m.group(1)
                            if 'player-x' in pat:
                                try:
                                    url = unquote(base64.b64decode(url).decode())
                                    src = re.search(r'src=["\']?([^"\'>\s]+\.mp4)', url); url = src.group(1) if src else url
                                except: continue
                            result["video_url"] = url; break
                    for pat in [r'<meta\s+itemprop=["\']name["\']\s+content=["\']([^"\']+)["\']', r'<title>([^<]+)</title>', r'<h1[^>]*>([^<]+)</h1>']:
                        m = re.search(pat, html, re.I)
                        if m: result["title"] = clean_title(m.group(1).strip()); break
                    if not result["title"]: result["title"] = clean_title(page_url.split("/")[-1])
                    return result
        except:
            if attempt == 1: return {"video_url": None, "title": None}
            await asyncio.sleep(1.5)

async def aria2_download(video_url: str, display_title: str, status_msg, task_id: str, page_url: str = "") -> str | None:
    safe_name = re.sub(r'[^\w\-_.]', '_', display_title)[:50]
    filename = f"{safe_name}_{int(time.time())}.mp4"
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    cmd = ["aria2c", video_url, "--dir", DOWNLOAD_DIR, "--out", filename, "--max-connection-per-server=16", "--split=16", "--min-split-size=4M", "--max-tries=3", "--timeout=300", "--continue=true", "--allow-overwrite=true", "--auto-file-renaming=false", "--summary-interval=0", "--console-log-level=error", f"--header=User-Agent: {HEADERS['User-Agent']}", f"--header=Referer: {page_url or HEADERS['Referer']}"]
    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    TaskManager.ACTIVE_TASKS[task_id]["proc"] = process

    start, last_upd, total = time.time(), 0, None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.head(video_url, headers=HEADERS, timeout=10) as r:
                if r.headers.get('Content-Length'): total = int(r.headers['Content-Length'])
    except: pass

    while process.returncode is None:
        if TaskManager.is_cancelled(task_id):
            process.terminate(); return None
        await asyncio.sleep(2)
        now = time.time()
        if now - last_upd < 4.0 or not os.path.exists(filepath): continue
        last_upd = now
        curr = os.path.getsize(filepath)
        speed = curr / (now - start) if (now - start) > 0 else 0
        pct = (curr / total * 100) if total else 0
        eta = (total - curr) / speed if speed > 0 and total else 0
        stats = get_system_stats(task_start=start)
        bar = progress_bar(pct)

        res = f"🚀 `{display_title}`\n\n┌ Status : 🟢 **Downloading** 🟢\n├ {bar} **{pct:.1f}%**\n├ ⚡ **Speed** : `{format_size(speed)}/s`\n├ 📦 **Size** : `{format_size(curr)} / {format_size(total) if total else '???'}`\n├ ⏱ **ETA** : `{format_eta(int(eta))}`\n└ /c_{task_id}"
        try: await status_msg.edit_text(res, parse_mode=enums.ParseMode.MARKDOWN)
        except: pass

    await process.wait()
    return filepath if process.returncode == 0 and os.path.exists(filepath) else None

_prog_lock = {}
async def upload_progress(current, total, status_msg, title, start_t, task_id: str = None, is_hyper=False):
    key = status_msg.chat.id
    now = time.time()
    if _prog_lock.get(key, 0) > now - 4.0: return
    _prog_lock[key] = now
    if TaskManager.is_cancelled(task_id): raise asyncio.CancelledError()

    speed = current / (now - start_t) if (now - start_t) > 0 else 0
    pct = current / total * 100 if total else 0
    eta = (total - current) / speed if speed > 0 else 0
    stats = get_system_stats(task_start=start_t)
    bar = progress_bar(pct)
    tg_status = "ON | HYPER-WZML" if HAS_TGCRYPTO else "OFF (Slow)"

    res = f"🚀 `{title}`\n\n┌ Status : {'⚡' if is_hyper else '🔵'} **{'HyperUL Ultra-Fast' if is_hyper else 'Uploading'}**\n├ {bar} **{pct:.1f}%**\n├ ⚡ **Speed** : `{format_size(speed)}/s` `[{tg_status}]`\n├ 📦 **Size** : `{format_size(current)} / {format_size(total)}`\n├ ⏱ **ETA** : `{format_eta(int(eta))}`\n└ /c_{task_id if task_id else ''}"
    try: await status_msg.edit_text(res, parse_mode=enums.ParseMode.MARKDOWN)
    except: pass

@app.on_message(filters.regex(r"^/c_([a-zA-Z0-9]+)"))
async def cancel_task_handler(client: Client, message: Message):
    task_id = message.matches[0].group(1)
    if await TaskManager.cancel_task(task_id): await message.reply(f"🚫 **Task cancel ho gaya**: `{task_id}`")
    else: await message.reply("❌ Invalid ya completed task.")

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(_, m: Message):
    await m.reply_text("🎬 **Desi49 Bot PRO v5.7**\n\n📥 URL bhejiye → 📤 Ultra-Fast Video mil jayega\n🔹 `/c_<task_id>` | 🔹 `/queue` | ⚡ Stable HyperUL")

@app.on_message(filters.command("queue") & filters.private)
async def queue_status(_, m: Message):
    p, a = TaskManager.PENDING_QUEUE.qsize(), len([t for t in TaskManager.ACTIVE_TASKS if not TaskManager.ACTIVE_TASKS[t].get("cancelled")])
    await m.reply_text(f"📊 **Queue Status**\n🔄 Active: `{a}` | ⏳ Pending: `{p}`")

@app.on_message(filters.regex(r"https?://[^\s]+") & filters.private)
async def url_handler(client: Client, message: Message):
    url = message.matches[0].group(0)
    task_id = TaskManager.generate_task_id(url, 1)
    TaskManager.register_task(task_id)
    q_size = TaskManager.PENDING_QUEUE.qsize()
    status_msg = await message.reply_text(f"⏳ Queue position: {q_size + 1}\n└ /c_{task_id}" if q_size > 0 else f"🔍 Processing...\n└ /c_{task_id}")
    await TaskManager.add_to_queue(message.from_user.id, url, message, status_msg, task_id)

async def process_request(client: Client, message: Message, url: str, status: Message, task_id: str):
    filepath = None
    try:
        if TaskManager.is_cancelled(task_id): return
        info = await extract_video_info(url)
        if TaskManager.is_cancelled(task_id) or not info["video_url"]:
            if not info["video_url"]: await status.edit_text("❌ Video link nahi mila!")
            return

        video_url, title = info["video_url"], info["title"]
        await status.edit_text(f"🎬 `{title}`\n\n📥 Downloading...\n`/c_{task_id}`")
        filepath = await aria2_download(video_url, title, status, task_id, page_url=url)
        if not filepath: return

        size = os.path.getsize(filepath)
        if size > MAX_TG_SIZE:
            await status.edit_text(f"❌ {format_size(size)} > 2GB!"); return

        dur, w, h = await get_video_metadata(filepath)
        caption = f"🎬 **{title}**\n📦 `{format_size(size)}`"
        
        await status.edit_text(f"🎬 `{title}`\n\n⚡ **HyperUL Uploading**...")
        uploader = HyperTGUpload(client, workers=5)
        input_file = await uploader.save_file(filepath, progress=upload_progress, progress_args=(status, title, time.time(), task_id, True))
        if TaskManager.is_cancelled(task_id) or not input_file: return

        # Patching
        original_save_file = client.save_file
        async def patched_save_file(path, *args, **kwargs):
            if os.path.abspath(path) == os.path.abspath(filepath): return input_file
            return await original_save_file(path, *args, **kwargs)
        client.save_file = patched_save_file

        try:
            vid_kwargs = {"video": filepath, "caption": caption, "supports_streaming": True, "parse_mode": enums.ParseMode.MARKDOWN, "duration": dur, "disable_notification": True}
            if w > 0: vid_kwargs["width"] = w
            if h > 0: vid_kwargs["height"] = h
            
            if DUMP_CHANNEL:
                kw_dump = vid_kwargs.copy(); kw_dump["caption"] += f"\nUID: `{message.from_user.id}`"
                dump_msg = await client.send_video(chat_id=DUMP_CHANNEL, **kw_dump)
                if dump_msg and dump_msg.video:
                    await message.reply_video(video=dump_msg.video.file_id, caption=caption, supports_streaming=True, parse_mode=enums.ParseMode.MARKDOWN)
                else: await message.reply_video(**vid_kwargs)
            else: await message.reply_video(**vid_kwargs)
        finally: client.save_file = original_save_file

    except Exception as e:
        log.error(f"❌ Task {task_id} CRITICAL Error: {e}", exc_info=True)
        try: await status.edit_text(f"❌ Error: {str(e)[:50]}")
        except: pass
    finally:
        TaskManager.unregister_task(task_id)
        try: await status.delete()
        except: pass
        if filepath and os.path.exists(filepath):
            try: os.remove(filepath)
            except: pass

async def worker():
    while True:
        try:
            task = await TaskManager.PENDING_QUEUE.get()
            if not TaskManager.is_cancelled(task["task_id"]):
                await process_request(app, task["message"], task["url"], task["status_msg"], task["task_id"])
            TaskManager.PENDING_QUEUE.task_done()
        except Exception as e:
            log.error(f"👷 Worker loop error: {e}", exc_info=True); await asyncio.sleep(2)

async def main():
    await app.start()
    log.warning("✅ Desi49 Bot PRO v5.7 Started (Quiet Mode)!")
    asyncio.create_task(worker())
    from pyrogram import idle
    await idle(); await app.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())