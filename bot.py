import os, io, sys, time, math, asyncio, json, re, logging, shutil
import urllib3, traceback, psutil, uuid

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import requests as py_requests
from typing import List, Dict, Optional, Any, Tuple, Union
from threading import Thread
from contextlib import redirect_stdout
from urllib.parse import urlparse, unquote, urljoin
from concurrent.futures import ThreadPoolExecutor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    THUMB_DIR = os.path.join(BASE_DIR, "thumbnails")
    
    DEFAULT_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    MAX_FILE_SIZE = 2000 * 1024 * 1024 # 2GB for Telegram
    
    # Speed Optimization Constants
    MAX_DOWNLOAD_WORKERS = 15
    MAX_UPLOAD_PARALLEL = 15
    PROGRESS_UPDATE_INTERVAL = 5
    
    # Concurrency Constants
    HUB_MAX_WORKERS = 5
    MIRROR_MAX_WORKERS = 8

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
            if not selected_eps:
                return {"episodes": [], "series_info": series_info}
            
            def process_ep(ep):
                links = self._try_mirrors(ep["mirrors"])
                return {
                    "episode": ep["label"],
                    "downloads": links if links else [],
                    "metadata": {"cookies": self.session.cookies.get_dict(), "user_agent": self.UA}
                }
            
            logger.info(f"[*] Bypassing mirrors for {len(selected_eps)} episodes concurrently...")
            with ThreadPoolExecutor(max_workers=Config.MIRROR_MAX_WORKERS) as executor:
                for res in executor.map(process_ep, selected_eps):
                    results.append(res)

            return {"episodes": results, "series_info": series_info}
        except Exception as e:
            logger.error(f"Error in get_links: {e}", exc_info=True)
            return {"error": str(e), "episodes": []}

    def _scrape_series_metadata(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extracts comprehensive metadata: Name, Season, and expected Qualities."""
        metadata = {
            "name": "Unknown",
            "season": None,
            "expected_qualities": []
        }
        
        main_content = soup.select_one(".entry-content, article, #main") or soup
        content_text = main_content.get_text(" ", strip=True)

        # 1. Extract Full Name
        for p in main_content.find_all("p"):
            p_text = p.get_text(" ", strip=True)
            if "Full Name:" in p_text:
                span = p.find("span")
                name = span.get_text(strip=True) if span else p_text.split("Full Name:")[-1].strip()
                metadata["name"] = re.sub(r'[🌐📰🎬🏝️📊⏱️]', '', name).strip()
                break
        
        if metadata["name"] == "Unknown":
            h1 = soup.find("h1")
            if h1:
                h1_text = h1.get_text(strip=True)
                h1_text = re.sub(r'(Season\s*\d+|Hindi\s*Dubbed.*|Episodes.*|Download.*|HD|720p|480p|1080p)', '', h1_text, flags=re.I).strip()
                metadata["name"] = h1_text
            else:
                metadata["name"] = url.rstrip("/").split("/")[-1].replace("-", " ").title()

        # 2. Extract Season
        season_match = re.search(r'Season:\s*(\d+)', content_text, re.I)
        if season_match:
            metadata["season"] = season_match.group(1).zfill(2)
        else:
            # Fallback: check the name or URL for season
            s_match = re.search(r'Season\s*(\d+)|S(\d+)', metadata["name"] + " " + url, re.I)
            if s_match:
                metadata["season"] = (s_match.group(1) or s_match.group(2)).zfill(2)

        # 3. Extract Expected Qualities
        quality_match = re.search(r'Quality:\s*\((.*?)\)', content_text, re.I)
        if quality_match:
            q_str = quality_match.group(1)
            metadata["expected_qualities"] = [q.strip().lower() for q in q_str.split(",")]
        
        logger.info(f"[*] Extracted Series Metadata: {metadata}")
        return metadata

    def _extract_episodes(self, html: str, referer: str) -> List[Dict[str, Any]]:
        """Parses HTML to find and group episode links using contextual headings."""
        soup = BeautifulSoup(html, "html.parser")
        main_content = soup.select_one(".entry-content, article, #main")
        if not main_content:
            logger.warning("[!] Main content container not found, falling back to full soup.")
            main_content = soup

        potential_links = []
        seen_urls = set()
        last_found_label = None
        fallback_counter = 1

        # Walk through all elements that might provide labels or links within main_content
        for element in main_content.find_all(["p", "div", "h1", "h2", "h3", "h4", "span", "a"]):
            if element.name == "a" and element.get("href"):
                href = element["href"]
                tag_text = element.get_text(strip=True)
                # Detect hub or download link
                is_valid = any(x in href for x in ["codedew.com", "multiquality", "store.animetoonhindi.com", "mega.nz", "drive.google.com"])
                # Also treat as hub if text suggests it
                is_hub_text = any(x in tag_text.lower() for x in ["multi", "watch", "mega", "g-drive", "batch"])
                
                if is_valid or is_hub_text:
                    if href in seen_urls: continue
                    seen_urls.add(href)
                    
                    # Try to get label from tag text
                    tag_label = self.EP_REGEX.search(tag_text)
                    
                    current_label = tag_label.group(1).title() if tag_label else last_found_label
                    if not current_label:
                        current_label = f"Episode {fallback_counter}"
                        fallback_counter += 1
                        
                    is_mirror = any(x in href for x in ["codedew.com", "multiquality.click"])
                    is_hub = (not is_mirror) and (any(x in href for x in ["store.animetoonhindi.com", "/multiquality/"]) or is_hub_text)
                    potential_links.append({"url": href, "label": current_label, "is_hub": is_hub})
            else:
                # Update last seen label from text nodes
                text = element.get_text(" ", strip=True)
                if len(text) < 100: # Ignore very long blocks
                    m = self.EP_REGEX.search(text)
                    if m: 
                        last_found_label = m.group(1).title()
                        logger.debug(f"[*] Found Heading Label: {last_found_label}")

        # Process hubs and group by episode number
        final_links = []
        hub_items = [item for item in potential_links if item["is_hub"]]
        non_hub_items = [item for item in potential_links if not item["is_hub"]]
        final_links.extend(non_hub_items)
        
        if hub_items:
            logger.info(f"[*] Extracting from {len(hub_items)} hubs concurrently...")
            with ThreadPoolExecutor(max_workers=Config.HUB_MAX_WORKERS) as executor:
                future_to_item = {executor.submit(self._extract_hub_links, item["url"]): item for item in hub_items}
                for future in future_to_item:
                    item = future_to_item[future]
                    try:
                        hub_eps = future.result()
                        if hub_eps: 
                            # If hub links don't have good labels, use the hub's label
                            for h in hub_eps:
                                if "Episode" not in h["label"]: h["label"] = item["label"]
                            final_links.extend(hub_eps)
                        else: 
                            final_links.append(item)
                    except Exception as e:
                        logger.error(f"[!] Hub extraction failed for {item['url']}: {e}")
                        final_links.append(item)
                

        grouped: Dict[str, List[Dict[str, str]]] = {}
        for link in final_links:
            ep_num = self._get_ep_num(link["label"])
            key = f"Episode {ep_num}" if ep_num else link["label"]
            grouped.setdefault(key, []).append({"url": link["url"], "referer": referer})
            
        return [{"label": k, "mirrors": v} for k, v in sorted(grouped.items(), key=lambda x: self._get_ep_num(x[0]) or 0)]

    def _get_ep_num(self, label: str) -> Optional[int]:
        m = re.search(r'(\d+)', label)
        return int(m.group(1)) if m else None

    def _extract_hub_links(self, hub_url: str) -> List[Dict[str, Any]]:
        try:
            r = self.session.get(hub_url, timeout=15)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            main_content = soup.select_one(".entry-content, article, #main, .download-links, .download-box") or soup
            eps = []
            seen = set()
            f_count = 1
            for a in main_content.find_all("a", href=True):
                href = a["href"]
                if any(x in href for x in ["codedew.com/zipper/", "multiquality.click/downlead/"]):
                    if href in seen: continue
                    seen.add(href)
                    text = a.get_text(" ", strip=True)
                    m = self.EP_REGEX.search(text)
                    label = m.group(1).title() if m else f"Episode {f_count}"
                    if not m: f_count += 1
                    eps.append({"url": href, "label": label, "is_hub": False})
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
            for step in range(1, 12):
                resp = self.session.get(curr_url, headers={"Referer": curr_ref}, timeout=15)
                if resp.status_code != 200: return None
                
                # Plan A: Search for direct tokens or MQ links in HTML
                # Restricted search area to avoid sidebar/menu noise
                soup = BeautifulSoup(resp.text, "html.parser")
                main_zone = soup.select_one(".entry-content, article, #main, .download-links, .download-box") or soup
                main_html = str(main_zone)
                
                # Look for rtiwatch (Old but still used sometimes)
                token_match = re.search(r'name="rtiwatch"\s+value="([^"]+)"', main_html)
                if token_match and token_match.group(1) not in ["notranslate", ""]:
                    return self.process_multiquality(f"{self.MQ_BASE_URL}downlead/{token_match.group(1)}/")
                
                # Look for multiquality/downlead links
                mq_link = re.search(r'https?://(?:swift\.)?multiquality\.click/downlead/([^/\"\'\s>]+)', main_html)
                if mq_link:
                    return self.process_multiquality(f"{self.MQ_BASE_URL}downlead/{mq_link.group(1)}/")
                
                # Look for ziptron, liptron or multiquality internal links
                # Handle both ?url=TOKEN and ?TOKEN formats
                token_link = re.search(r'/(?:ziptron\.php|liptron\.php|multiquality)/\?(?:(?:url|id)=)?([^/\"\'\s>&\#]+)', main_html)
                if token_link:
                    token = token_link.group(1)
                    if 10 < len(token) < 50:
                        logger.info(f"[+] Found Redirect Token: {token}")
                        return self.process_multiquality(f"{self.MQ_BASE_URL}downlead/{token}/")
                
                # Look for tokens in JS variables (Token Hunter)
                # We focus on script tags within or near the main content
                scripts = re.findall(r'<script.*?>\s*(.*?)\s*</script>', resp.text, re.DOTALL | re.I)
                potential_tokens = []
                
                # Regex for variables: fid, fileId, video_id, token, id (if in script)
                # Refined to avoid descriptive strings with multiple hyphens/underscores
                for script in scripts:
                    found = re.findall(r'(?:fid|fileId|video_id|token|["\']id["\'])\s*[:=]\s*["\']([a-zA-Z0-9]{14,40})["\']', script, re.I)
                    potential_tokens.extend(found)
                
                # Also check for liptron/ziptron links anywhere in main content
                url_tokens = re.findall(r'/(?:ziptron\.php|liptron\.php|multiquality)/\?(?:url|id)=([a-zA-Z0-9_\-]{13,40})', main_html)
                potential_tokens.extend(url_tokens)

                for token in potential_tokens:
                    # Filter out common false positives
                    # 1. Block known keywords
                    if any(x in token.lower() for x in ["wrapper", "container", "loader", "progress", "button", "player", "header", "footer"]):
                        continue
                    # 2. Block all-lowercase tokens (likely CSS classes/IDs) if they are short or lack numbers
                    if token.islower() and not any(c.isdigit() for c in token) and len(token) < 16:
                        continue
                    # 3. Block tokens with too many repeating generic patterns
                    if "xxxx" in token or "aaaa" in token:
                        continue
                    
                    logger.info(f"[+] Token Hunter found potential token: {token}")
                    links = self.process_multiquality(f"{self.MQ_BASE_URL}downlead/{token}/")
                    if links: return links

                # Plan B: Follow the "Next" link chain
                soup = BeautifulSoup(resp.text, "html.parser")
                next_url = self._find_next_button(soup, curr_url)
                if not next_url:
                    # Final attempt: just look for ANY gate-like link that isn't the current one (restricted to main zone)
                    for a in main_zone.find_all("a", href=True):
                        href = urljoin(curr_url, a["href"])
                        # Basic URL validation
                        if not href.startswith("http"): continue
                        try:
                            parsed = urlparse(href)
                            if ":" in parsed.netloc: # Port check
                                port_part = parsed.netloc.split(":")[-1]
                                if port_part and not port_part.isdigit(): continue
                        except: continue

                        is_gate = any(x in href for x in ["/zipper/", "/watchbeta/", "/watch/", "/multiquality/"])
                        if is_gate and href != curr_url and not href.startswith("javascript:"):
                            next_url = href
                            break
                
                if not next_url: return None
                logger.info(f"[*] Step {step}: Jumping to {next_url[:60]}...")
                curr_ref, curr_url = curr_url, next_url
                time.sleep(1.0)
        except Exception as e:
            logger.error(f"[!] Zipper bypass error: {e}")
        return None

    def _find_next_button(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        # High-priority button selectors
        for selector in ["a#goBtn", "a#mainActionBtn", "a.btn-main", "a.btn-success", "a.btn-primary"]:
            btn = soup.select_one(selector)
            if btn and btn.get("href"):
                url = urljoin(self.ROOT_URL, btn["href"])
                if url != current_url and not url.startswith("javascript:"):
                    return url
        return None

    def process_multiquality(self, url: str) -> Optional[List[Dict[str, str]]]:
        """Final stage: Hits the MultQuality API to get CDN download links."""
        try:
            logger.info(f"[*] Fetching final links from MQ page: {url}")
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                logger.error(f"[!] MQ Page Error: {resp.status_code} for {url}")
                return None

            if "window.juicyData =" not in resp.text:
                snippet = resp.text[:200].replace('\n', ' ')
                logger.error(f"[!] window.juicyData not found on MQ page. Status: {resp.status_code}. Body Snippet: {snippet}...")
                return None
            
            # Find the start of the JSON object
            start_marker = "window.juicyData ="
            start_index = resp.text.find(start_marker)
            if start_index == -1: return None
            
            # Slice from the first '{' for robust JSON extraction
            json_start = resp.text.find("{", start_index)
            if json_start == -1: return None
            
            # Use raw_decode to parse the JSON and ignore trailing garbage (like semi-colons)
            try:
                jd, index = json.JSONDecoder().raw_decode(resp.text[json_start:])
                jd_data = jd.get("data", jd)
                logger.info("[+] JuicyData parsed successfully using raw_decode.")
            except Exception as e:
                logger.error(f"[!] JSON Decoder error: {e}")
                return None
            
            # Small delay to mimic human/browser behavior
            time.sleep(1.0)
            
            api_url = urljoin(self.MQ_BASE_URL, jd_data["routes"]["links"])
            headers = {
                "Origin": self.MQ_BASE_URL.rstrip("/"),
                "Referer": url,
                "User-Agent": self.UA,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "X-XSRF-TOKEN": unquote(self.session.cookies.get("XSRF-TOKEN", "")),
                "Sec-CH-UA-Mobile": "?0",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin"
            }
            payload = {"captcha": None, "_token": jd_data["token"]}
            
            for attempt in range(1, 4):
                try:
                    logger.info(f"[*] Fetching links (Attempt {attempt}/3) to: {api_url[:60]}...")
                    # Primary try using curl_cffi session
                    try:
                        api_resp = self.session.post(api_url, headers=headers, json=payload, timeout=12)
                        st_code = api_resp.status_code
                        resp_text = api_resp.text
                    except Exception as ce:
                        logger.warning(f"[!] curl_cffi failed on MQ API: {ce}. Falling back to standard requests...")
                        # Fallback to standard requests (stable on Linux)
                        import requests as py_requests
                        fall_resp = py_requests.post(api_url, headers=headers, json=payload, timeout=15, verify=False)
                        st_code = fall_resp.status_code
                        resp_text = fall_resp.text

                    logger.info(f"[*] MQ API Response (Attempt {attempt}): Status {st_code}")
                    if not resp_text:
                        logger.warning(f"[!] Empty response from MQ API (Attempt {attempt})")
                        time.sleep(2 * attempt)
                        continue
                    
                    if st_code != 200:
                        logger.warning(f"[!] MQ API Status {st_code} (Attempt {attempt})")
                        time.sleep(2 * attempt)
                        continue
                        
                    data = json.loads(resp_text)
                    if data.get("success"):
                        qualities = data.get("qualities", [])
                        if not qualities:
                            logger.warning(f"[!] Success but 0 qualities (Attempt {attempt}). Body: {resp_text[:100]}")
                            time.sleep(2 * attempt)
                            continue
                        
                        logger.info(f"[+] MQ Success: Found {len(qualities)} qualities!")
                        return [{
                            "label": q["label"], "link": q["link"],
                            "metadata": {"referer": url, "cookies": self.session.cookies.get_dict(), "user_agent": self.UA}
                        } for q in qualities]
                    else:
                        logger.warning(f"[!] MQ API Logic Error (Attempt {attempt}): {data.get('message', 'No message')}")
                        time.sleep(2 * attempt)
                except Exception as e:
                    logger.error(f"[!] MQ Attempt {attempt} error: {e}")
                    time.sleep(2 * attempt)
            
            logger.error("[!] Failed to get links after 3 attempts.")
        except Exception as e:
            logger.error(f"[!] Critical error in process_multiquality: {e}")
        return None

    def resolve_filename(self, url: str, referer: Optional[str] = None, cookies: Optional[Dict] = None) -> Optional[str]:
        if any(x in url.lower() for x in ["swift", "multiquality", "leech"]): return None
        try:
            headers = {"Referer": referer or self.ROOT_URL, "User-Agent": self.UA}
            res = py_requests.head(url, headers=headers, cookies=cookies, allow_redirects=True, timeout=10, verify=False)
            cd = res.headers.get("Content-Disposition", "")
            m = re.search(r'filename\*=utf-8\'\'(.+)|filename="(.+)"|filename=(.+)', cd, re.I)
            if m: return unquote(m.group(1) or m.group(2) or m.group(3))
            return os.path.basename(urlparse(res.url).path)
        except: return None

# --- UTILITIES ---
class Utils:
    EDIT_STATES: Dict[int, Dict[str, Any]] = {}
    _NET_IO = None
    _NET_TIME = None

    @staticmethod
    def progress_bar(p, l=15):
        f = int(l * p / 100)
        return f"[{'█'*f}{'░'*(l-f)}]"

    @staticmethod
    def get_system_stats():
        try:
            cpu = psutil.cpu_percent()
            ram = psutil.virtual_memory().used / (1024 * 1024)
            
            # Net Speed Logic
            now = time.time()
            io = psutil.net_io_counters()
            
            if Utils._NET_IO is None:
                Utils._NET_IO = io
                Utils._NET_TIME = now
                net_mbps = 0.0
                net_mib = 0.0
            else:
                dt = max(now - Utils._NET_TIME, 0.1)
                t_bytes = (io.bytes_sent - Utils._NET_IO.bytes_sent) + (io.bytes_recv - Utils._NET_IO.bytes_recv)
                net_mbps = (t_bytes * 8) / (1024 * 1024 * dt)
                net_mib = (io.bytes_recv - Utils._NET_IO.bytes_recv) / (1024 * 1024 * dt)
                
                Utils._NET_IO = io
                Utils._NET_TIME = now
                
            return cpu, ram, net_mbps, net_mib
        except Exception as e:
            logger.error(f"Error in get_system_stats: {e}")
            return 0, 0, 0, 0

    @staticmethod
    def get_eta(current, total, speed):
        if not speed or speed <= 0: return "00:00"
        remaining = (total - current) / speed
        return Utils.time_formatter(remaining * 1000)

    @staticmethod
    def format_progress(filename, status_icon, status_text, p, speed, curr_size, tot_size, start_time):
        cpu, ram, net_mbps, net_mib = Utils.get_system_stats()
        eta = Utils.get_eta(curr_size, tot_size, speed)
        bar = Utils.progress_bar(p, l=10)
        
        # Format metrics
        spd_str = f"{Utils.human_bytes(speed)}/s"
        sz_curr = Utils.human_bytes(curr_size)
        sz_tot = Utils.human_bytes(tot_size) if tot_size else "???"
        
        # Box drawing UI
        res = f"🚀 `{filename}`\n\n"
        res += f"┌ Status  : {status_icon} **{status_text}** {status_icon}\n"
        res += f"├ {bar} **{p:.1f}%**\n"
        res += f"├ ⚡ **Speed**   : `{spd_str}`\n"
        res += f"├ 📦 **Size**    : `{sz_curr} / {sz_tot}`\n"
        res += f"└ ⏱ **ETA**     : `{eta}`\n\n"
        res += f"🖥 **CPU**: `{cpu}%` | 💾 **RAM**: `{int(ram)}MB`"
        res += f"\n🌐 **Net**: `{net_mbps:.1f} Mbps` | ⬇ `{net_mib:.1f} MiB/s`"
        
        return res

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

    @staticmethod
    def is_faststart(filepath: str) -> bool:
        try:
            with open(filepath, "rb") as f:
                while True:
                    header = f.read(8)
                    if len(header) < 8: break
                    size = int.from_bytes(header[:4], 'big')
                    atom_type = header[4:8]
                    
                    if atom_type == b'moov': return True
                    if atom_type == b'mdat': return False
                    
                    if size == 1:
                        size = int.from_bytes(f.read(8), 'big')
                        f.seek(size - 16, 1)
                    elif size == 0:
                        break
                    else:
                        f.seek(size - 8, 1)
            return False
        except Exception as e:
            logger.error(f"Error checking faststart: {e}")
            return False

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
        self.app = Client(
            "anime_bot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            max_concurrent_transmissions=100,
            sleep_threshold=60,
            in_memory=True,
        )
        self._setup_handlers()
        if not os.path.exists(Config.DOWNLOAD_DIR): os.makedirs(Config.DOWNLOAD_DIR)
        if not os.path.exists(Config.THUMB_DIR): os.makedirs(Config.THUMB_DIR)

    def _setup_handlers(self):
        @self.app.on_message(filters.command("start") & filters.incoming)
        async def start_handler(c, m): await m.reply("✅ **Anime Bot Pro** is active.\nUsage: `/grab <url> [selection]`")

        @self.app.on_message(filters.command("grab") & filters.incoming)
        async def get_handler(c, m):
            if not self._is_auth(m): return
            if len(m.command) < 2: return await m.reply("Usage: `/grab <url> [selection]`")
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
            logger.info(f"[*] Dispatching Episode {i}/{len(episodes)}: {ep.get('episode')}")
            await self._process_episode(m, ep, bypasser, status, len(episodes), i, res.get("series_info"))
        
        await status.delete()

    async def _handle_hindianime(self, m, url, selection, status):
        eps = await asyncio.to_thread(HindiAnimeZone().pro_main_bypass, url, selection=selection)
        if not eps: return await Utils.safe_edit(status, "❌ No episodes found.", force=True)
        # Create a dummy bypasser for consistency if needed, though HindiAnimeZone handles its own session
        bypasser = RareAnimes() 
        for i, ep in enumerate(eps, 1):
            await self._process_episode(m, ep, bypasser, status, len(eps), i, ep.get("series_info"))
        await status.delete()

    async def _process_episode(self, m, ep, bypasser, status, total, current, series_info):
        try:
            downloads = sorted(ep.get("downloads", []), key=lambda x: self._q_val(x.get('label')))
            logger.info(f"[*] Episode {current}/{total} has {len(downloads)} download links.")
            if not downloads:
                logger.warning(f"[!] No valid download links for episode: {ep.get('episode') or 'Unknown'}. Skipping.")
                return
            req_dir = os.path.join(Config.DOWNLOAD_DIR, str(m.id))
            if not os.path.exists(req_dir): os.makedirs(req_dir)

            for dl in downloads:
                url, meta, label = dl['link'], dl.get('metadata', {}), dl.get('label', 'N/A')
                logger.info(f"[*] Processing Quality: {label} | URL: {url[:60]}...")
                
                await Utils.safe_edit(status, f"🔍 **Resolving {label}...**", force=True)
                fname = await asyncio.to_thread(bypasser.resolve_filename, url, meta.get('referer'), meta.get('cookies'))
                
                if not fname or len(fname) < 5:
                    # Detect season for filename
                    if isinstance(series_info, dict):
                        season_val = series_info.get('season')
                        se_str = f"S{str(season_val).zfill(2)}" if season_val else ""
                        base_name = series_info.get('name', 'Anime')
                    else:
                        s = re.search(r'S(\d+)|Season\s*(\d+)', series_info or "", re.I)
                        se_str = f"S{(s.group(1) or s.group(2)).zfill(2)}" if s else ""
                        base_name = series_info or 'Anime'
                    
                    ep_num = re.search(r'(\d+)', ep.get('episode', ''))
                    ep_str = f"E{ep_num.group(1).zfill(2)}" if ep_num else f"E{str(current).zfill(2)}"
                    
                    cleaned_name = self._clean_noise(base_name)
                    # If se_str is already in cleaned_name, don't repeat
                    if se_str and (se_str in cleaned_name.upper() or f"SEASON {int(se_str[1:])}" in cleaned_name.upper()):
                        se_str = ""
                        
                    fname = f"{cleaned_name} {se_str} {ep_str} [{label.upper()}].mp4"
                    fname = re.sub(r'\s+', ' ', fname).strip()
                
                fpath = os.path.join(req_dir, re.sub(r'[\\/*?:"<>|]', "", fname).strip())
                logger.info(f"[*] Target File: {fpath}")
                
                await Utils.safe_edit(status, f"🚀 **Downloading {label}**...", force=True)
                if await self._download_manager(url, fpath, status, meta):
                    # Faststart & Split if necessary
                    paths = await self._split_video(fpath, status)
                    for p in paths:
                        await self._upload_file(m, p, status, current, total, label, series_info)
                        if os.path.exists(p): os.remove(p)
                else:
                    logger.error(f"[!] Download failed for {label}")
                
                if os.path.exists(fpath): os.remove(fpath)
        except Exception as e:
            logger.error(f"[CRITICAL] Crash in _process_episode: {e}")
            logger.error(traceback.format_exc())
            await Utils.safe_edit(status, f"❌ Error processing episode: {str(e)}", force=True)

    async def _download_manager(self, url, path, status, meta):
        ua, cookies = meta.get('user_agent', Config.DEFAULT_UA), meta.get('cookies', {})
        referer = meta.get('referer', "https://swift.multiquality.click/")
        is_sensitive = any(x in url.lower() for x in ["monster", "swift", "multiquality", "downlead"])
        
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        }
        if referer: headers["Referer"] = referer

        if is_sensitive:
            logger.info(f"[*] Warming up mirror for sensitive link: {urlparse(url).netloc}")
            await self._warmup_mirror(f"https://{urlparse(url).netloc}/", ua, cookies)
        
        conn = "16" # Force 16 connections to maximize speed
        directory, filename = os.path.split(path)
        
        cmd = ["aria2c", "-x", conn, "-s", conn, "-j", conn, "--min-split-size=1M", "-d", directory, "-o", filename, "--user-agent", ua, "--check-certificate=false"]
        for k, v in headers.items():
            if k.lower() not in ["user-agent", "cookie"]:
                cmd.extend(["--header", f"{k}: {v}"])
        if cookies: cmd.extend(["--header", f"Cookie: {'; '.join([f'{k}={v}' for k, v in cookies.items()])}"])
        
        logger.info(f"[*] Starting aria2c: {' '.join(cmd[:10])}...")
        try:
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            while True:
                line = await proc.stdout.readline()
                if not line: break
                line_str = line.decode('utf-8', 'ignore')
                # Improved regex for aria2c: [#2089ad 4.0MiB/3.8GiB(0%) CN:1 SPD:6.2MiB/s ETA:10m31s]
                m = re.search(r'([\d\.]+[KMG]i?B)/([\d\.]+[KMG]i?B)\((\d+)%\).*?SPD:([\d\.]+[KMG]i?B).*?ETA:([\w\d]+)', line_str)
                if m:
                    curr_sz_str, tot_sz_str, p_str, spd_str, eta_str = m.groups()
                    p = int(p_str)
                    fname = os.path.basename(path)
                    
                    # Compute system stats and net
                    cpu, ram, net_mbps, net_mib = Utils.get_system_stats()
                    bar = Utils.progress_bar(p, l=10)
                    
                    msg = f"🚀 `{fname}`\n\n"
                    msg += f"┌ Status  : ⏬ **Downloading** ⏬\n"
                    msg += f"├ {bar} **{p}%**\n"
                    msg += f"├ ⚡ **Speed**   : `{spd_str}/s`\n"
                    msg += f"├ 📦 **Size**    : `{curr_sz_str} / {tot_sz_str}`\n"
                    msg += f"└ ⏱ **ETA**     : `{eta_str}`\n\n"
                    msg += f"🖥 **CPU**: `{cpu}%` | 💾 **RAM**: `{int(ram)}MB`"
                    msg += f"\n🌐 **Net**: `{net_mbps:.1f} Mbps` | ⬇ `{net_mib:.1f} MiB/s`"
                    
                    await Utils.safe_edit(status, msg)
                elif "ERROR" in line_str.upper():
                    logger.error(f"[!] aria2c error: {line_str.strip()}")
            await proc.wait()
            
            if proc.returncode == 0 and os.path.exists(path):
                logger.info(f"[*] aria2c success: {path}")
                return True
            
            if proc.returncode == 22 or any(x in url.lower() for x in ["monster", "swift"]):
                logger.warning(f"[!] aria2c failed (Code {proc.returncode}). Trying stable requests fallback...")
                return await self._download_requests(url, path, status, headers, cookies)
                
            return False
        except Exception as e:
            logger.error(f"[!] aria2c start failed: {e}. Trying fallback...")
            return await self._download_requests(url, path, status, headers, cookies)

    async def _download_requests(self, url, path, status, headers, cookies):
        try:
            logger.info(f"[*] Starting chunked requests download: {url[:60]}...")
            with py_requests.get(url, headers=headers, cookies=cookies, stream=True, timeout=30, verify=False) as r:
                r.raise_for_status()
                total = int(r.headers.get('content-length', 0))
                curr = 0
                last_up = 0
                start_t = time.time()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            curr += len(chunk)
                            if time.time() - last_up > Config.PROGRESS_UPDATE_INTERVAL:
                                p = curr * 100 / total if total else 0
                                fname = os.path.basename(path)
                                speed = curr / (time.time() - start_t)
                                
                                # Advanced Box UI
                                msg = Utils.format_progress(fname, "⏬", "Downloading (Fallback)", p, speed, curr, total, start_t)
                                await Utils.safe_edit(status, msg)
                                last_up = time.time()
            return os.path.exists(path) and os.path.getsize(path) > 1000
        except Exception as e:
            logger.error(f"[!] Requests fallback failed: {e}")
            return False

    async def _upload_file(self, m, fpath, status, current, total, label, series_info):
        w, h, dur = await self._get_video_meta(fpath)
        cap = self._make_caption(fpath, os.path.getsize(fpath), dur, series_info)
        start = time.time()
        thumb = None
        try:
            thumb = await self._make_thumb(fpath, dur)
            fname = os.path.basename(fpath)
            await self.app.send_video(m.chat.id, fpath, caption=cap, duration=dur, width=w, height=h, thumb=thumb,
                                    supports_streaming=True,
                                    progress=self._upload_progress, progress_args=(f"**Uploading {current}/{total}**", status, start, fname))
        except Exception as e: logger.error(f"Upload fail: {e}")
        finally:
            if thumb and os.path.exists(thumb):
                try: os.remove(thumb)
                except: pass

    async def _split_video(self, path, status=None):
        size = os.path.getsize(path)
        if size <= Config.MAX_FILE_SIZE:
            if Utils.is_faststart(path):
                logger.info(f"[*] {path} is already faststart optimized. Skipping ffmpeg.")
                return [path]
                
            out = f"{path}.fs.mp4"
            if status: await Utils.safe_edit(status, "⚡ **Optimizing video for Telegram Stream...**", force=True)
            cmd = ["ffmpeg", "-v", "error", "-i", path, "-c", "copy", "-movflags", "+faststart", out, "-y"]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
            if os.path.exists(out) and os.path.getsize(out) > 0:
                os.remove(path)
                os.rename(out, path)
            return [path]
        
        logger.info(f"[*] Splitting {path}...")
        if status: await Utils.safe_edit(status, "✂️ **Splitting large video...**", force=True)
        duration = (await self._get_video_meta(path))[2]
        parts = math.ceil(size / Config.MAX_FILE_SIZE)
        part_dur = duration / parts
        split_files = []
        for i in range(parts):
            out = f"{path}.part{i+1}.mp4"
            cmd = ["ffmpeg", "-v", "error", "-i", path, "-ss", str(i*part_dur), "-t", str(part_dur), "-c", "copy", "-movflags", "+faststart", out, "-y"]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
            if os.path.exists(out): split_files.append(out)
        return split_files

    def _make_caption(self, path, size, dur, series_info):
        info = self._parse_filename(os.path.basename(path))
        
        # Priority: parse from series_info dict, then path
        season = None
        if isinstance(series_info, dict):
            season = series_info.get('season')
            name = series_info.get('name', 'Unknown')
        else:
            name = series_info or info['name']

        if not season:
            s_match = re.search(r'S(\d+)|Season\s*(\d+)', name + " " + (series_info if isinstance(series_info, str) else ""), re.I)
            if s_match: season = (s_match.group(1) or s_match.group(2)).zfill(2)
            
        # Clean title for caption (Remove Season info and unwanted suffixes)
        name = self._clean_title(name)
        
        q = re.search(r'(\d{3,4}p)', path, re.I)
        q_str = q.group(1).upper() if q else "720P"
        
        cap = f"🎬 **{name}**\n╭━━━━━━━━━━━━━━━━━━━╮\n"
        if season:
            cap += f"│ 🏝️ **Season**    : {season}\n"
        cap += f"│ 📺 **Episode**   : {info['episode'] or 'N/A'}\n"
        cap += f"│ 🌐 **Language**  : Hindi\n"
        cap += f"│ 📊 **Quality**   : {q_str}\n"
        cap += f"│ 📦 **Size**      : {Utils.human_bytes(size)}\n"
        cap += f"│ ⏱️ **Duration**  : {Utils.time_formatter(dur*1000)}\n"
        cap += f"╰━━━━━━━━━━━━━━━━━━━╯"
        return cap

    def _parse_filename(self, text):
        data = {"name": "Unknown", "season": None, "episode": None, "year": None}
        yr = re.search(r'\((19|20)\d{2}\)', text)
        if yr: data["year"] = yr.group(0).strip('()')
        
        # Season detection
        s = re.search(r'S(\d+)|Season\s*(\d+)', text, re.I)
        if s: data["season"] = (s.group(1) or s.group(2)).zfill(2)
        
        e = re.search(r'E(\d+)|Episode\s*(\d+)', text, re.I)
        if e: data["episode"] = (e.group(1) or e.group(2)).zfill(2)
        data["name"] = self._clean_noise(re.sub(r'\(.*?\)|\[.*?\]', '', text.split('.')[0]))
        return data

    def _clean_title(self, text):
        # Strips EVERYTHING including Seasons for the 🎬 Title line
        noise = r'Dubbed|Hindi|Dual|Audio|Multi|Episodes?|Downloads?|Full|Series|Zon-E|HD|BluRay|FHD|SD|Season\s*\d+|S\d+'
        cleaned = re.sub(noise, '', text, flags=re.I).replace('.', ' ').replace('_', ' ')
        return re.sub(r'\s+', ' ', cleaned).strip()

    def _clean_noise(self, text):
        # Preserves Season so it stays in Filenames
        noise = r'Dubbed|Hindi|Dual|Audio|Multi|Episodes?|Downloads?|Full|Series|Zon-E|HD|BluRay|FHD|SD'
        cleaned = re.sub(noise, '', text, flags=re.I).replace('.', ' ').replace('_', ' ')
        return re.sub(r'\s+', ' ', cleaned).strip()

    def _q_val(self, label):
        m = re.search(r'(\d+)', str(label))
        return int(m.group(1)) if m else 0

    async def _get_video_meta(self, path):
        try:
            cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,duration", "-of", "json", path]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.warning(f"[!] ffprobe failed for {path}: {stderr.decode()}")
                return 0, 0, 0
            
            data = json.loads(stdout)
            if "streams" not in data or not data["streams"]:
                return 0, 0, 0
                
            d = data["streams"][0]
            width = int(d.get("width", 0))
            height = int(d.get("height", 0))
            duration = int(float(d.get("duration", 0)))
            return width, height, duration
        except Exception as e:
            logger.error(f"[!] Error extracting video meta: {e}")
            return 0, 0, 0

    async def _make_thumb(self, path, dur):
        thumb_name = f"thumb_{uuid.uuid4().hex}.jpg"
        t = os.path.join(Config.THUMB_DIR, thumb_name)
        # Using user's preferred ffmpeg arguments: seek 5s, scale 320:-1
        cmd = [
            "ffmpeg", "-y", "-ss", "00:00:05", "-i", path, 
            "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "2", t
        ]
        try:
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await proc.wait()
            if os.path.exists(t) and os.path.getsize(t) > 0:
                return t
            return None
        except Exception as e:
            logger.error(f"Thumbnail error: {e}")
            return None

    async def _upload_progress(self, cur, tot, ud, msg, start, fname):
        p = cur*100/tot if tot else 0
        if time.time() - getattr(self, '_last_up', 0) > Config.PROGRESS_UPDATE_INTERVAL:
            speed = cur / (time.time() - start) if (time.time() - start) > 0 else 0
            # Advanced Box UI
            msg_text = Utils.format_progress(fname, "⏫", ud, p, speed, cur, tot, start)
            await Utils.safe_edit(msg, msg_text)
            self._last_up = time.time()

    async def _warmup_mirror(self, url, ua, cookies):
        try:
            py_requests.get(url, headers={"User-Agent": ua}, cookies=cookies, timeout=10, verify=False)
        except: pass

    def run(self):
        print("🚀 AnimeBot Pro Starting...")
        Thread(target=lambda: Flask(__name__).run(host='0.0.0.0', port=int(os.environ.get("PORT", 8000)), debug=False), daemon=True).start()
        self.app.run()

if __name__ == "__main__":
    AnimeBot().run()
