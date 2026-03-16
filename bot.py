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
from urllib.parse import urlparse, unquote, urljoin

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import MessageNotModified, FloodWait
from flask import Flask
from curl_cffi import requests as currequests
from bs4 import BeautifulSoup

from HindiAnimeZone import HindiAnimeZone

# --- CONFIGURATION ---
class Config:
    API_ID = int(os.environ.get("API_ID", 23200475))
    API_HASH = os.environ.get("API_HASH", "644e1d9e8028a5295d6979bb3a36b23b")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "7810985319:AAHSaD-YlHThm2JPoOY_vnJLs9jXpaWs4ts")
    OWNER_ID = int(os.environ.get("OWNER_ID", 7074383232))
    AUTH_CHAT = int(os.environ.get("AUTH_CHAT", -1003192464251))
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
    
    DEFAULT_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    MAX_FILE_SIZE = 2000 * 1024 * 1024 # 2GB for Telegram

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AnimeBot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.ERROR)

# --- PRO BYPASSER ENGINE ---
class RareAnimes:
    """
    Professional Bypasser for RareAnimes and associated multi-quality links.
    Features browser-grade impersonation and robust metadata extraction.
    """
    
    ROOT_URL = "https://codedew.com/"
    MQ_BASE_URL = "https://swift.multiquality.click/"
    UA = Config.DEFAULT_UA
    
    # Regex patterns for metadata extraction
    EP_REGEX = re.compile(r"(Episode\s*\d+|Ep\s*\d+|S\d+\s*E\d+|Movie|Special|OVA)", re.I)
    QUALITY_REGEX = re.compile(r'(\d{3,4}p|SD|HD|FHD|4K|Ultra\s*HD)', re.I)

    def __init__(self):
        self.session: currequests.Session = self._init_session()
        self.initialized: bool = False
        self.last_mq_referer: Optional[str] = None

    def _init_session(self) -> currequests.Session:
        """Initialize a high-fidelity browser session with professional headers."""
        session = currequests.Session(impersonate="chrome124")
        session.headers.update({
            "User-Agent": self.UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
            "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Linux"'
        })
        return session

    def init_session(self) -> None:
        """Warms up the session by visiting entry points to establish cookies."""
        if self.initialized: return
        try:
            logger.info("[*] Warming up session...")
            self.session.get("https://rareanimes.app/", timeout=15)
            time.sleep(1.0)
            self.session.get(self.ROOT_URL, timeout=15, headers={"Referer": "https://rareanimes.app/"})
            self.session.get(self.MQ_BASE_URL, timeout=15, headers={"Referer": self.ROOT_URL})
            self.initialized = True
        except Exception as e:
            logger.warning(f"[!] Session warmup failed: {e}")
            self.initialized = True

    def get_links(self, url: str, ep_start: Optional[int] = None, ep_end: Optional[int] = None) -> Dict[str, Any]:
        """Main entry point to extract direct links from a series page."""
        try:
            self.init_session()
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200:
                return {"error": f"Failed to load page (Status {resp.status_code})", "episodes": []}
                
            soup = BeautifulSoup(resp.text, "html.parser")
            series_info = self._scrape_series_metadata(soup, url)
            raw_eps = self._extract_episodes(resp.text, url)
            
            if not raw_eps: return {"error": "No episodes found on page", "episodes": []}

            # Range-based episode selection
            selected_eps = self._filter_episodes(raw_eps, ep_start, ep_end)
            
            results = []
            for ep in selected_eps:
                links = self._try_mirrors(ep["mirrors"])
                entry = {
                    "episode": ep["label"],
                    "downloads": links if links else [],
                    "metadata": {"cookies": self.session.cookies.get_dict(), "user_agent": self.UA}
                }
                results.append(entry)

            return {"episodes": results, "series_info": series_info}
        except Exception as e:
            logger.error(f"Error in get_links: {e}", exc_info=True)
            return {"error": str(e), "episodes": []}

    def _scrape_series_metadata(self, soup: BeautifulSoup, url: str) -> str:
        h1 = soup.find("h1")
        if h1: return h1.get_text(strip=True)
        return url.rstrip("/").split("/")[-1].replace("-", " ").title()

    def _extract_episodes(self, html: str, referer: str) -> List[Dict[str, Any]]:
        """Parses HTML to find and group episode links."""
        soup = BeautifulSoup(html, "html.parser")
        potential_links = []
        seen_urls = set()
        fallback_counter = 1

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href in seen_urls: continue
            
            is_zipper = "codedew.com/zipper/" in href
            is_hub = "store.animetoonhindi.com" in href or "/multiquality/" in href
            
            if not (is_zipper or is_hub): continue
            seen_urls.add(href)
            
            label = self._get_label_from_tag(a)
            if label == "Episode/Download":
                label = f"Episode {fallback_counter}"
                fallback_counter += 1
                
            potential_links.append({"url": href, "label": label, "is_hub": is_hub})

        final_links = []
        for item in potential_links:
            if item["is_hub"]:
                hub_eps = self._extract_hub_links(item["url"])
                if hub_eps: final_links.extend(hub_eps)
                else: final_links.append(item)
            else:
                final_links.append(item)
                
        grouped: Dict[str, List[Dict[str, str]]] = {}
        for link in final_links:
            grouped.setdefault(link["label"], []).append({"url": link["url"], "referer": referer})
            
        return [{"label": k, "mirrors": v} for k, v in grouped.items()]

    def _extract_hub_links(self, hub_url: str) -> List[Dict[str, Any]]:
        try:
            r = self.session.get(hub_url, timeout=15)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            eps = []
            f_count = 1
            for a in soup.find_all("a", href=True):
                if "codedew.com/zipper/" in a["href"]:
                    text = a.get_text(strip=True)
                    m = self.EP_REGEX.search(text)
                    label = m.group(1).title() if m else f"Episode {f_count}"
                    if not m: f_count += 1
                    eps.append({"url": a["href"], "label": label, "is_hub": False})
            return eps
        except: return []

    def _get_label_from_tag(self, tag: Any) -> str:
        text = tag.get_text(" ", strip=True)
        m = self.EP_REGEX.search(text)
        if m: return m.group(1).title()
        return "Episode/Download"

    def _filter_episodes(self, episodes: List[Dict], start: Optional[int], end: Optional[int]) -> List[Dict]:
        def get_num(ep):
            m = re.search(r'(\d+)', ep["label"])
            return int(m.group(1)) if m else 0
        filtered = [ep for ep in episodes if (not start or get_num(ep) >= start) and (not end or get_num(ep) <= end)]
        return sorted(filtered, key=get_num)

    def _try_mirrors(self, mirrors: List[Dict[str, str]]) -> Optional[List[Dict]]:
        total = len(mirrors)
        for i, mirror in enumerate(mirrors, 1):
            logger.info(f"[*] Trying mirror {i}/{total}...")
            links = self.process_zipper(mirror["url"], mirror["referer"])
            if links: return links
        return None

    def process_zipper(self, url: str, referer: str) -> Optional[List[Dict]]:
        try:
            curr_url, curr_ref = url, referer
            logger.info(f"[*] Bypassing zipper gate for: {url}")
            for step in range(1, 10):
                resp = self.session.get(curr_url, headers={"Referer": curr_ref}, timeout=15)
                if resp.status_code != 200: return None
                
                token_match = re.search(r'name="rtiwatch"\s+value="([^"]+)"', resp.text)
                if token_match and token_match.group(1) != "notranslate":
                    return self.process_multiquality(f"{self.MQ_BASE_URL}downlead/{token_match.group(1)}/")
                
                soup = BeautifulSoup(resp.text, "html.parser")
                next_url = self._find_next_button(soup)
                if not next_url: return None
                curr_ref, curr_url = curr_url, next_url
                time.sleep(0.8)
        except: pass
        return None

    def _find_next_button(self, soup: BeautifulSoup) -> Optional[str]:
        for selector in ["a#goBtn", "a#mainActionBtn", "a.btn-main"]:
            btn = soup.select_one(selector)
            if btn and btn.get("href"): return urljoin(self.ROOT_URL, btn["href"])
        return None

    def process_multiquality(self, url: str) -> Optional[List[Dict]]:
        try:
            logger.info(f"[*] Fetching final links from MQ page: {url}")
            resp = self.session.get(url, headers={"Referer": self.ROOT_URL}, timeout=15)
            if resp.status_code != 200:
                logger.error(f"[!] MQ Page Error: {resp.status_code}")
                return None
                
            # Robust JuicyData extraction using brace balancing
            try:
                start_marker = "window.juicyData ="
                start_idx = resp.text.find(start_marker)
                if start_idx == -1:
                    logger.error("[!] window.juicyData not found on MQ page")
                    return None
                
                # Find the first '{'
                actual_start = resp.text.find("{", start_idx)
                if actual_start == -1: return None
                
                # Balance braces
                brace_count = 0
                in_str = False
                escape = False
                jd_str = None
                
                for i in range(actual_start, len(resp.text)):
                    char = resp.text[i]
                    if char == '"' and not escape:
                        in_str = not in_str
                    if not in_str:
                        if char == '{': brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                jd_str = resp.text[actual_start:i+1]
                                break
                    escape = (char == '\\' and not escape)
                
                if not jd_str:
                    logger.error("[!] Could not find balanced JSON object for JuicyData")
                    return None
                    
                jd = json.loads(jd_str)["data"]
                # Detailed route logging
                route = jd.get("routes", {}).get("links", "MISSING")
                logger.info(f"[*] MQ Route: {route} | Token: {jd.get('token', 'MISSING')[:10]}...")
            except Exception as e:
                logger.error(f"[!] JuicyData parse error: {e}")
                return None
            
            # Link generation wait
            time.sleep(4.0)
            
            api_url = urljoin(self.MQ_BASE_URL, jd["routes"]["links"])
            headers = {
                "Host": urlparse(self.MQ_BASE_URL).netloc,
                "Origin": self.MQ_BASE_URL.rstrip("/"),
                "Referer": url,
                "User-Agent": self.UA,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "X-XSRF-TOKEN": unquote(self.session.cookies.get("XSRF-TOKEN", "")),
                "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "Sec-CH-UA-Mobile": "?0",
                "Sec-CH-UA-Platform": '"Linux"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "DNT": "1"
            }
            payload = {"captcha": None, "_token": jd["token"]}
            
            for attempt in range(1, 4):
                try:
                    logger.info(f"[*] Fetching links (Attempt {attempt}/3)...")
                    api_resp = self.session.post(api_url, headers=headers, json=payload, timeout=20)
                    
                    if not api_resp.text:
                        logger.warning(f"[!] Empty response from MQ API (Attempt {attempt})")
                        time.sleep(2 * attempt)
                        continue
                    
                    if api_resp.status_code != 200:
                        logger.warning(f"[!] MQ API Status {api_resp.status_code} (Attempt {attempt})")
                        time.sleep(2 * attempt)
                        continue
                        
                    data = api_resp.json()
                    if data.get("success"):
                        qualities = data.get("qualities", [])
                        if not qualities:
                            logger.warning("[!] Success but 0 qualities. Retrying...")
                            time.sleep(2 * attempt)
                            continue
                            
                        return [{
                            "label": q["label"], "link": q["link"],
                            "metadata": {"referer": url, "cookies": self.session.cookies.get_dict(), "user_agent": self.UA}
                        } for q in qualities]
                    else:
                        logger.warning(f"[!] MQ API Logic Error: {data.get('message', 'No message')}")
                        time.sleep(2 * attempt)
                except Exception as e:
                    logger.error(f"[!] MQ Attempt {attempt} error: {e}")
                    time.sleep(2 * attempt)
            
            logger.error("[!] Failed to get links after 3 attempts.")
        except Exception as e:
            logger.error(f"[!] Critical error in process_multiquality: {e}")
        return None

    def resolve_filename(self, url: str, referer: Optional[str] = None, cookies: Optional[Dict] = None) -> Optional[str]:
        if any(x in url.lower() for x in ["swift", "multiquality", "monster", "leech"]): return None
        try:
            with currequests.Session(impersonate="chrome124") as s:
                if cookies: s.cookies.update(cookies)
                res = s.head(url, headers={"Referer": referer or self.ROOT_URL, "User-Agent": self.UA}, allow_redirects=True, timeout=10)
                cd = res.headers.get("Content-Disposition", "")
                m = re.search(r'filename\*=utf-8\'\'(.+)|filename="(.+)"|filename=(.+)', cd, re.I)
                if m: return unquote(m.group(1) or m.group(2) or m.group(3))
                return os.path.basename(urlparse(res.url).path)
        except: return None

# --- UTILITIES ---
class Utils:
    EDIT_STATES: Dict[int, Dict[str, Any]] = {}

    @staticmethod
    def human_bytes(size: float) -> str:
        if not size: return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0: return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"

    @staticmethod
    def time_formatter(ms: int) -> str:
        s, ms = divmod(int(ms), 1000)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return (f"{h}h " if h else "") + (f"{m}m " if m else "") + f"{s}s"

    @classmethod
    async def safe_edit(cls, message: Message, text: str, force: bool = False, **kwargs):
        now = time.time()
        state = cls.EDIT_STATES.setdefault(message.id, {"time": 0, "text": ""})
        if not force and (text == state["text"] or now - state["time"] < 3.0): return
        try:
            await message.edit(text, **kwargs)
            state.update({"time": now, "text": text})
        except MessageNotModified: state["text"] = text
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await cls.safe_edit(message, text, force=True, **kwargs)
        except Exception: pass

# --- CORE ENGINE ---
class AnimeBot:
    def __init__(self):
        self.app = Client("anime_bot", Config.API_ID, Config.API_HASH, bot_token=Config.BOT_TOKEN)
        self._setup_handlers()
        if not os.path.exists(Config.DOWNLOAD_DIR): os.makedirs(Config.DOWNLOAD_DIR)

    def _setup_handlers(self):
        @self.app.on_message(filters.command("start") & filters.incoming)
        async def start_handler(c, m): await m.reply("✅ **Anime Bot Pro** is active.\nUsage: `/get <url> [selection]`")

        @self.app.on_message(filters.command("get") & filters.incoming)
        async def get_handler(c, m):
            if not self._is_auth(m): return
            if len(m.command) < 2: return await m.reply("Usage: `/get <url> [selection]`")
            url, selection = m.command[1], self._parse_selection(m.command[2:])
            status = await m.reply("🔍 **Analyzing...**")
            try:
                if "rareanimes.app" in url or "codedew.com" in url: await self._handle_rareanimes(m, url, selection, status)
                elif "hindianimezone.com" in url: await self._handle_hindianime(m, url, selection, status)
                else: await status.edit("❌ Unsupported site.")
            except Exception as e: await Utils.safe_edit(status, f"❌ Error: {str(e)}", force=True)

    def _is_auth(self, m: Message) -> bool:
        return (m.from_user.id if m.from_user else 0) == Config.OWNER_ID or m.chat.id == Config.AUTH_CHAT

    def _parse_selection(self, args: List[str]) -> Optional[List[int]]:
        if not args: return None
        nums = set()
        for a in "".join(args).split(','):
            if '-' in a:
                try:
                    s, e = map(int, a.split('-'))
                    nums.update(range(min(s, e), max(s, e) + 1))
                except: continue
            elif a.isdigit(): nums.add(int(a))
        return sorted(list(nums)) if nums else None

    async def _handle_rareanimes(self, m, url, selection, status):
        ep_start = min(selection) if selection else None
        ep_end = max(selection) if selection else None
        
        bypasser = RareAnimes()
        res = await asyncio.to_thread(bypasser.get_links, url, ep_start, ep_end)
        
        if "error" in res:
            return await Utils.safe_edit(status, f"❌ Error: {res['error']}", force=True)
            
        episodes = res.get("episodes", [])
        if not episodes:
            return await Utils.safe_edit(status, "❌ No episodes found. (Bypass failed or No Links)", force=True)
            
        await Utils.safe_edit(status, f"✅ Found {len(episodes)} episodes. Starting batch processing...")
        
        for i, ep in enumerate(episodes, 1):
            await self._process_episode(m, ep, status, len(episodes), i, res.get("series_info"))
        
        await status.delete()

    async def _handle_hindianime(self, m, url, selection, status):
        eps = await asyncio.to_thread(HindiAnimeZone().pro_main_bypass, url, selection=selection)
        if not eps: return await Utils.safe_edit(status, "❌ No episodes found.", force=True)
        for i, ep in enumerate(eps, 1):
            await self._process_episode(m, ep, status, len(eps), i, ep.get("series_info"))
        await status.delete()

    async def _process_episode(self, m, ep, status, total, current, series_info):
        downloads = sorted(ep.get("downloads", []), key=lambda x: self._q_val(x.get('label')))
        if not downloads: return
        req_dir = os.path.join(Config.DOWNLOAD_DIR, str(m.id))
        if not os.path.exists(req_dir): os.makedirs(req_dir)

        for dl in downloads:
            url, meta, label = dl['link'], dl.get('metadata', {}), dl.get('label', 'N/A')
            await Utils.safe_edit(status, f"🔍 **Resolving {label}...**", force=True)
            fname = await asyncio.to_thread(RareAnimes().resolve_filename, url, meta.get('referer'), meta.get('cookies'))
            
            if not fname or len(fname) < 5:
                ep_num = re.search(r'(\d+)', ep.get('episode', ''))
                ep_str = f"E{ep_num.group(1).zfill(2)}" if ep_num else f"E{str(current).zfill(2)}"
                fname = f"{self._clean_noise(series_info or 'Anime')} - {ep_str} [{label.upper()}].mp4"
            
            fpath = os.path.join(req_dir, re.sub(r'[\\/*?:"<>|]', "", fname).strip())
            await Utils.safe_edit(status, f"🚀 **Downloading {label}**...", force=True)
            if await self._download_manager(url, fpath, status, meta):
                # Split if necessary
                paths = await self._split_video(fpath)
                for p in paths:
                    await self._upload_file(m, p, status, current, total, label, series_info)
                    if os.path.exists(p): os.remove(p)
            if os.path.exists(fpath): os.remove(fpath)

    async def _download_manager(self, url, path, status, meta):
        ua, cookies = meta.get('user_agent', Config.DEFAULT_UA), meta.get('cookies', {})
        is_sensitive = any(x in url.lower() for x in ["monster", "swift", "multiquality", "downlead"])
        if is_sensitive: await self._warmup_mirror(f"https://{urlparse(url).netloc}/", ua, cookies)
        
        conn = "1" if is_sensitive else "16"
        cmd = ["aria2c", "-x", conn, "-s", conn, "--out", path, "--user-agent", ua, "--check-certificate=false",
               "--header", "Sec-CH-UA-Platform: \"Linux\"", "--header", "Sec-CH-UA: \"Chromium\";v=\"124\"", url]
        if meta.get('referer'): cmd.extend(["--referer", meta['referer']])
        if cookies: cmd.extend(["--header", f"Cookie: {'; '.join([f'{k}={v}' for k, v in cookies.items()])}"])
        
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        while True:
            line = await proc.stdout.readline()
            if not line: break
            m = re.search(r'\((\d+)%\).*?SPD:([\d\.]+[KMG]i?B)', line.decode('utf-8', 'ignore'))
            if m: await Utils.safe_edit(status, f"🚀 **Downloading** [{m.group(1)}%]\n**Speed**: {m.group(2)}/s")
        await proc.wait()
        return proc.returncode == 0 and os.path.exists(path)

    async def _upload_file(self, m, fpath, status, current, total, label, series_info):
        w, h, dur = await self._get_video_meta(fpath)
        thumb = await self._make_thumb(fpath, dur)
        cap = self._make_caption(fpath, os.path.getsize(fpath), dur, series_info)
        start = time.time()
        try:
            await self.app.send_video(m.chat.id, fpath, caption=cap, duration=dur, width=w, height=h, thumb=thumb,
                                    progress=self._upload_progress, progress_args=(f"📤 **Uploading {current}/{total}**", status, start))
        except Exception as e: logger.error(f"Upload fail: {e}")
        if thumb and os.path.exists(thumb): os.remove(thumb)

    async def _split_video(self, path):
        size = os.path.getsize(path)
        if size <= Config.MAX_FILE_SIZE: return [path]
        
        logger.info(f"[*] Splitting {path}...")
        duration = (await self._get_video_meta(path))[2]
        parts = math.ceil(size / Config.MAX_FILE_SIZE)
        part_dur = duration / parts
        split_files = []
        for i in range(parts):
            out = f"{path}.part{i+1}.mp4"
            cmd = ["ffmpeg", "-i", path, "-ss", str(i*part_dur), "-t", str(part_dur), "-c", "copy", out, "-y"]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
            if os.path.exists(out): split_files.append(out)
        return split_files

    def _make_caption(self, path, size, dur, series_info):
        info = self._parse_filename(os.path.basename(path))
        name = self._clean_noise(series_info or info['name'])
        q = re.search(r'(\d{3,4}p)', path, re.I)
        q_str = q.group(1).upper() if q else "720P"
        return f"🎬 **{name}**\n╭━━━━━━━━━━━━━━━━━━━╮\n│ 📺 **Episode:** {info['episode'] or 'N/A'}\n│ 🌐 **Language:** Hindi\n│ 📊 **Quality:** {q_str}\n│ 📦 **Size:** {Utils.human_bytes(size)}\n│ ⏱️ **Duration:** {Utils.time_formatter(dur*1000)}\n╰━━━━━━━━━━━━━━━━━━━╯"

    def _parse_filename(self, text):
        data = {"name": "Unknown", "season": None, "episode": None, "year": None}
        yr = re.search(r'\((19|20)\d{2}\)', text)
        if yr: data["year"] = yr.group(0).strip('()')
        e = re.search(r'E(\d+)|Episode\s*(\d+)', text, re.I)
        if e: data["episode"] = (e.group(1) or e.group(2)).zfill(2)
        data["name"] = self._clean_noise(re.sub(r'\(.*?\)|\[.*?\]', '', text.split('.')[0]))
        return data

    def _clean_noise(self, text):
        noise = r'Dubbed|Hindi|Dual|Audio|Multi|Episodes?|Downloads?|Full|Series|Zon-E'
        return re.sub(r'\s+', ' ', re.sub(noise, '', text, flags=re.I).replace('.', ' ').replace('_', ' ')).strip()

    def _q_val(self, label):
        m = re.search(r'(\d+)', str(label))
        return int(m.group(1)) if m else 0

    async def _get_video_meta(self, path):
        try:
            cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,duration", "-of", "json", path]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)
            d = json.loads((await proc.communicate())[0])["streams"][0]
            return int(d.get("width", 0)), int(d.get("height", 0)), int(float(d.get("duration", 0)))
        except: return 0, 0, 0

    async def _make_thumb(self, path, dur):
        t = f"{path}.jpg"
        cmd = ["ffmpeg", "-ss", str(dur//5), "-i", path, "-vframes", "1", "-q:v", "2", t, "-y"]
        await (await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.DEVNULL)).wait()
        return t if os.path.exists(t) else None

    async def _upload_progress(self, cur, tot, ud, msg, start):
        p = cur*100/tot if tot else 0
        if time.time() - getattr(self, '_last_up', 0) > 4:
            await Utils.safe_edit(msg, f"{ud}\n[{p:.1f}%] `{'█'*(int(p)//10)}`")
            self._last_up = time.time()

    async def _warmup_mirror(self, url, ua, cookies):
        try:
            with currequests.Session(impersonate="chrome124") as s:
                if cookies: s.cookies.update(cookies)
                s.get(url, headers={"User-Agent": ua}, timeout=10)
                cookies.update(s.cookies.get_dict())
        except: pass

    def run(self):
        print("🚀 AnimeBot Pro Starting...")
        Thread(target=lambda: Flask(__name__).run(host='0.0.0.0', port=int(os.environ.get("PORT", 8000)), debug=False), daemon=True).start()
        self.app.run()

if __name__ == "__main__":
    AnimeBot().run()
