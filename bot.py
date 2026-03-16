import os
import io
import sys
import time
import math
import asyncio
import json
import re
import logging
import shutil
from typing import List, Dict, Optional, Any, Tuple, Union
from threading import Thread
from contextlib import redirect_stdout
from urllib.parse import urlparse, unquote

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import MessageNotModified, FloodWait
from flask import Flask
from curl_cffi import requests as currequests

from HindiAnimeZone import HindiAnimeZone
from RareAnimes import RareAnimes

# --- CONFIGURATION ---
class Config:
    API_ID = int(os.environ.get("API_ID", 23200475))
    API_HASH = os.environ.get("API_HASH", "644e1d9e8028a5295d6979bb3a36b23b")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "7810985319:AAHSaD-YlHThm2JPoOY_vnJLs9jXpaWs4ts")
    OWNER_ID = int(os.environ.get("OWNER_ID", 7074383232))
    AUTH_CHAT = int(os.environ.get("AUTH_CHAT", -1003192464251))
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
    
    DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AnimeBot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.ERROR)

# --- UTILITIES ---
class Utils:
    EDIT_STATES: Dict[int, Dict[str, Any]] = {}

    @staticmethod
    def human_bytes(size: float) -> str:
        if not size: return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"

    @staticmethod
    def time_formatter(milliseconds: int) -> str:
        seconds, milliseconds = divmod(int(milliseconds), 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        tmp = ((f"{days}d, ") if days else "") + \
              ((f"{hours}h, ") if hours else "") + \
              ((f"{minutes}m, ") if minutes else "") + \
              ((f"{seconds}s") if seconds else "0s")
        return tmp if not tmp.endswith(", ") else tmp[:-2]

    @classmethod
    async def safe_edit(cls, message: Message, text: str, force: bool = False, **kwargs):
        now = time.time()
        mid = message.id
        state = cls.EDIT_STATES.setdefault(mid, {"time": 0, "text": ""})
        
        if not force and (text == state["text"] or now - state["time"] < 3.0):
            return

        try:
            await message.edit(text, **kwargs)
            state.update({"time": now, "text": text})
        except MessageNotModified:
            state["text"] = text
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await cls.safe_edit(message, text, force=True, **kwargs)
        except Exception as e:
            logger.error(f"Edit failed: {e}")

# --- CORE ENGINE ---
class AnimeBot:
    def __init__(self):
        self.app = Client("anime_bot", Config.API_ID, Config.API_HASH, bot_token=Config.BOT_TOKEN)
        self._setup_handlers()
        if not os.path.exists(Config.DOWNLOAD_DIR):
            os.makedirs(Config.DOWNLOAD_DIR)

    def _setup_handlers(self):
        @self.app.on_message(filters.command("start") & filters.incoming)
        async def start_handler(client, message):
            if not self._is_auth(message): return
            await message.reply("✅ **Anime Bot Pro** is active.\nUsage: `/get <url> [selection]`")

        @self.app.on_message(filters.command("get") & filters.incoming)
        async def get_handler(client, message):
            if not self._is_auth(message): return
            if len(message.command) < 2:
                return await message.reply("Usage: `/get <url> [selection]`")
            
            url = message.command[1]
            selection = self._parse_selection(message.command[2:])
            status = await message.reply("🔍 **Analyzing...**")
            
            try:
                if "rareanimes.app" in url or "codedew.com" in url:
                    await self._handle_rareanimes(message, url, selection, status)
                elif "hindianimezone.com" in url:
                    await self._handle_hindianime(message, url, selection, status)
                else:
                    await status.edit("❌ Unsupported site.")
            except Exception as e:
                logger.error(f"Cmd Error: {e}", exc_info=True)
                await Utils.safe_edit(status, f"❌ Error: {str(e)}", force=True)

    def _is_auth(self, message: Message) -> bool:
        uid = message.from_user.id if message.from_user else 0
        cid = message.chat.id if message.chat else 0
        return uid == Config.OWNER_ID or cid == Config.AUTH_CHAT

    def _parse_selection(self, args: List[str]) -> Optional[List[int]]:
        if not args: return None
        if len(args) == 2 and all(a.isdigit() for a in args):
            s, e = map(int, args)
            return list(range(min(s, e), max(s, e) + 1))
        
        sel_str = "".join(args)
        indices = set()
        for part in sel_str.split(','):
            if '-' in part:
                try:
                    s, e = map(int, part.split('-'))
                    indices.update(range(min(s, e), max(s, e) + 1))
                except: continue
            elif part.isdigit():
                indices.add(int(part))
        return sorted(list(indices)) if indices else None

    async def _handle_rareanimes(self, message: Message, url: str, selection: Optional[List[int]], status: Message):
        ep_start = min(selection) if selection else None
        ep_end = max(selection) if selection else None
        
        bypasser = RareAnimes()
        res = await asyncio.to_thread(bypasser.get_links, url, ep_start, ep_end)
        
        if "error" in res:
            return await Utils.safe_edit(status, f"❌ Error: {res['error']}", force=True)
            
        episodes = res.get("episodes", [])
        if not episodes:
            return await Utils.safe_edit(status, "❌ No episodes found.", force=True)
            
        await Utils.safe_edit(status, f"✅ Found {len(episodes)} episodes. Starting batch processing...")
        
        for i, ep in enumerate(episodes, 1):
            await self._process_episode(message, ep, status, len(episodes), i, res.get("series_info"))
        
        await status.delete()

    async def _handle_hindianime(self, message: Message, url: str, selection: Optional[List[int]], status: Message):
        bypasser = HindiAnimeZone()
        episodes = await asyncio.to_thread(bypasser.pro_main_bypass, url, selection=selection)
        
        if not episodes:
            return await Utils.safe_edit(status, "❌ No episodes found.", force=True)
            
        await Utils.safe_edit(status, f"✅ Found {len(episodes)} episodes. Starting batch processing...")
        
        for i, ep in enumerate(episodes, 1):
            await self._process_episode(message, ep, status, len(episodes), i, ep.get("series_info"))
        
        await status.delete()

    async def _process_episode(self, message: Message, ep_data: Dict, status: Message, total: int, current: int, series_info: Optional[str]):
        downloads = sorted(ep_data.get("downloads", []), key=lambda x: self._q_val(x.get('label')))
        if not downloads: return

        req_dir = os.path.join(Config.DOWNLOAD_DIR, str(message.id))
        if not os.path.exists(req_dir): os.makedirs(req_dir)

        for dl in downloads:
            url = dl['link']
            meta = dl.get('metadata', {})
            label = dl.get('label', 'N/A')
            
            # Resolve filename
            await Utils.safe_edit(status, f"🔍 **Resolving {label}...**", force=True)
            rare = RareAnimes()
            fname = await asyncio.to_thread(rare.resolve_filename, url, meta.get('referer'), meta.get('cookies'))
            
            if fname:
                fname = re.sub(r'[\\/*?:"<>|]', "", fname).strip()
            
            if not fname or fname.startswith("download_") or len(fname) < 5:
                # Smart reconstruction based on page info and metadata
                # Pattern: [Anime Name] - [Episode] [Quality].mp4
                clean_series = self._clean_noise(series_info or "Anime")
                # Extract number from ep_data['episode'] or current index
                ep_num = re.search(r'(\d+)', ep_data.get('episode', ''))
                ep_str = f"E{ep_num.group(1).zfill(2)}" if ep_num else f"E{str(current).zfill(2)}"
                
                clean_label = label.upper().replace(" ", "")
                fname = f"{clean_series} - {ep_str} [{clean_label}].mp4"
                logger.info(f"[*] Smart Reconstruction: {fname}")
                
            fpath = os.path.join(req_dir, fname)
            
            # Download
            logger.info(f"[*] Starting download: {fname}")
            await Utils.safe_edit(status, f"🚀 **Downloading {label}**...", force=True)
            success = await self._download_manager(url, fpath, status, meta)
            if not success:
                logger.error(f"[!] Download failed for: {fname}")
                await Utils.safe_edit(status, f"❌ **Download Failed** [{label}]", force=True)
                continue
            
            # Metadata & Upload
            logger.info(f"[*] Extracting meta for: {fname}")
            await Utils.safe_edit(status, f"⚙️ **Processing Meta {current}/{total}...**", force=True)
            w, h, dur = await self._get_video_meta(fpath)
            thumb = await self._make_thumb(fpath, dur)
            cap = self._make_caption(fpath, os.path.getsize(fpath), dur, series_info)
            
            start_t = time.time()
            try:
                await self.app.send_video(
                    chat_id=message.chat.id,
                    video=fpath,
                    caption=cap,
                    duration=dur, width=w, height=h,
                    thumb=thumb,
                    progress=self._upload_progress,
                    progress_args=(f"📤 **Uploading {current}/{total} [{label}]**", status, start_t)
                )
            except Exception as e:
                logger.error(f"Upload fail: {e}")
            
            # Cleanup per quality
            if os.path.exists(fpath): os.remove(fpath)
            if thumb and os.path.exists(thumb): os.remove(thumb)
            
        # Cleanup dir
        if os.path.exists(req_dir) and not os.listdir(req_dir):
            shutil.rmtree(req_dir)

    async def _download_manager(self, url: str, path: str, status: Message, meta: Dict) -> bool:
        try:
            cmd = [
                "aria2c", "-x", "16", "-s", "16", "-k", "1M",
                "--out", path, "--file-allocation=none", "--continue=true",
                "--user-agent", Config.DEFAULT_UA,
                "--header", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "--header", "Connection: keep-alive",
                url
            ]
            if meta.get('referer'): cmd.extend(["--referer", meta['referer']])
            if meta.get('cookies'):
                c_str = "; ".join([f"{k}={v}" for k, v in meta['cookies'].items()])
                cmd.extend(["--header", f"Cookie: {c_str}"])

            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            
            while True:
                line = await proc.stdout.readline()
                if not line: break
                line = line.decode('utf-8', 'ignore').strip()
                match = re.search(r'\((\d+)%\).*?SPD:([\d\.]+[KMG]i?B).*?ETA:([\d\w]+)', line)
                if match:
                    p, s, e = match.groups()
                    bar = "█" * (int(p)//10) + "░" * (10 - int(p)//10)
                    await Utils.safe_edit(status, f"🚀 **Downloading** [{p}%]\n`{bar}`\n**Speed**: {s}/s | **ETA**: {e}")
            
            await proc.wait()
            if proc.returncode == 0 and os.path.exists(path): return True
        except Exception as e:
            logger.warning(f"Aria2c fail: {e}")

        try:
            # Sync session with bypasser for maximum compatibility
            with currequests.Session(impersonate="chrome124") as s:
                if meta.get('cookies'): s.cookies.update(meta['cookies'])
                logger.info(f"[*] Fallback download starting (Chrome 124): {url[:60]}...")
                
                # Professional headers
                headers = {
                    "Referer": meta.get('referer', ''),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                }
                # Inject XSRF token if found in cookies (common for Laravel MQ API)
                xsrf_token = s.cookies.get("XSRF-TOKEN")
                if xsrf_token: headers["X-XSRF-TOKEN"] = unquote(xsrf_token)

                r = s.get(url, stream=True, headers=headers, timeout=60, allow_redirects=True)
                
                if r.status_code != 200:
                    logger.error(f"[*] Fallback failed (Status {r.status_code}). Body: {r.text[:100]}...")
                    return False
                    
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                last_update = 0
                
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(1024*1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            # Progress update every 5MB or 3s
                            if total_size and time.time() - last_update > 3:
                                p = int(downloaded * 100 / total_size)
                                bar = "█" * (p//10) + "░" * (10 - p//10)
                                await Utils.safe_edit(status, f"🚀 **Downloading (Fallback)** [{p}%]\n`{bar}`")
                                last_update = time.time()
                return os.path.exists(path)
        except Exception as e:
            logger.error(f"[*] Fallback error: {e}")
            return False

    def _make_caption(self, path: str, size: int, dur: int, series_info: Optional[str]) -> str:
        basename = os.path.basename(path)
        info = self._parse_filename(basename)
        
        series_name = None
        if series_info:
            # If it's already a clean Full Name from the scraper, use it as is
            # but try to remove common noise just in case.
            # Only split if it looks like a typical messy page title (has | or - and is long)
            if ("|" in series_info or "-" in series_info) and len(series_info) > 30:
                series_name_dirty = series_info.split("|")[0].split("-")[0].strip()
            else:
                series_name_dirty = series_info.strip()
            
            series_name = self._clean_noise(self._parse_filename(series_name_dirty)['name'])
            
        fname_clean = self._clean_noise(info['name'])
        
        if series_name and (len(series_name) > len(fname_clean) or fname_clean in ["Unknown", "Download"]):
            display_name = series_name
        else:
            display_name = fname_clean or series_name or "Unknown Anime"
            
        if len(display_name) < 4 or display_name.isdigit():
            display_name = series_name or display_name
            
        display_name = self._clean_noise(display_name)
        
        quality = re.search(r'(\d{3,4}p)', basename, re.I)
        q_str = quality.group(1).upper() if quality else "720P"
        
        cap = f"🎬 **{display_name}**"
        if info['year']: cap += f" ({info['year']})"
        cap += "\n╭━━━━━━━━━━━━━━━━━━━╮"
        if info['season']: cap += f"\n│ ✨ **season :** {info['season']}"
        cap += f"\n│ 📺 **Episode:** {info['episode'] or 'N/A'}"
        cap += f"\n│ 🌐 **Language:** Hindi\n│ 📊 **Quality:** {q_str}\n│ 📦 **Size:** {Utils.human_bytes(size)}\n│ ⏱️ **Duration:** {Utils.time_formatter(dur*1000)}\n╰━━━━━━━━━━━━━━━━━━━╯"
        return cap

    def _parse_filename(self, text: str) -> Dict:
        data = {"name": "Unknown", "season": None, "episode": None, "year": None, "is_movie": False}
        yr = re.search(r'\(?(19|20)\d{2}\)?', text)
        if yr: data["year"] = yr.group(0).strip('()')
        s = re.search(r'Season\s*(\d+)|S(\d+)', text, re.I)
        if s: data["season"] = (s.group(1) or s.group(2)).zfill(2)
        e = re.search(r'E(\d+)|Episode\s*(\d+)|Ep[\.\s]*(\d+)', text, re.I)
        if e: data["episode"] = (e.group(1) or e.group(2) or e.group(3)).zfill(2)
        
        name = re.sub(r'\.(mp4|mkv|avi|webm)$', '', text, flags=re.I)
        for p in [r'S\d+|Season\s*\d+', r'E\d+|Episode\s*\d+|Ep[\.\s]*\d+', r'\d{3,4}p', r'\(?(19|20)\d{2}\)?']:
            name = re.sub(p, '', name, flags=re.I)
        data["name"] = self._clean_noise(name)
        return data

    def _clean_noise(self, text: str) -> str:
        if not text: return ""
        noise = r'Dubbed|Hindi|Dual|Audio|Multi|UNCUT|Eng|Sub|Soft|Hard|ESub|HSub|SD|HD|FHD|4K|Ultra|Web-DL|BluRay|x264|x265|HEVC|Episodes?|Downloads?|Full|Series|Zon-E'
        text = re.sub(noise, '', text, flags=re.I)
        text = re.sub(r'[@\[\(].*?[\]\)]|www\.\S+', '', text)
        text = re.sub(r'[\_\.\-]+', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def _q_val(self, label: Any) -> int:
        m = re.search(r'(\d+)', str(label))
        return int(m.group(1)) if m else 0

    async def _get_video_meta(self, path: str) -> Tuple[int, int, int]:
        try:
            cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,duration", "-of", "json", path]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, _ = await proc.communicate()
            d = json.loads(out)["streams"][0]
            return int(d.get("width", 0)), int(d.get("height", 0)), int(float(d.get("duration", 0)))
        except: return 0, 0, 0

    async def _make_thumb(self, path: str, dur: int) -> Optional[str]:
        t_path = f"{path}.jpg"
        try:
            cmd = ["ffmpeg", "-v", "error", "-ss", str(dur//5), "-i", path, "-vframes", "1", "-q:v", "2", t_path, "-y"]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
            return t_path if os.path.exists(t_path) else None
        except: return None

    async def _upload_progress(self, current, total, ud_type, message, start):
        perc = current * 100 / total if total else 0
        bar = "█" * (int(perc)//10) + "░" * (10 - int(perc)//10)
        tmp = f"{ud_type}\n[{perc:.2f}%] `{bar}`\n**Speed**: {Utils.human_bytes(current/(time.time()-start))}/s"
        await Utils.safe_edit(message, tmp)

    def run(self):
        print("🚀 AnimeBot Pro Starting...")
        Thread(target=self._run_health_check, daemon=True).start()
        self.app.run()

    def _run_health_check(self):
        flask_app = Flask(__name__)
        @flask_app.route('/')
        def home(): return "AnimeBot Pro is Live", 200
        flask_app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8000)))

if __name__ == "__main__":
    AnimeBot().run()
