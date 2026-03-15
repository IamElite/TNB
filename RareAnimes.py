import os
import re
import io
import sys
import time
import json
import logging
from typing import List, Dict, Optional, Any, Union
from urllib.parse import urlparse, urljoin, unquote

from curl_cffi import requests as currequests
from bs4 import BeautifulSoup

# Configure character encoding for Windows console if needed
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

logger = logging.getLogger("RareAnimes")

class RareAnimesError(Exception):
    """Base exception for RareAnimes bypasser."""
    pass

class RareAnimes:
    """
    Professional Bypasser for RareAnimes and associated multi-quality links.
    Features browser-grade impersonation and robust metadata extraction.
    """
    
    ROOT_URL = "https://codedew.com/"
    MQ_BASE_URL = "https://swift.multiquality.click/"
    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    
    # Regex patterns for metadata extraction
    EP_REGEX = re.compile(r"(Episode\s*\d+|Ep\s*\d+|S\d+\s*E\d+|Movie|Special|OVA)", re.I)
    QUALITY_REGEX = re.compile(r'(\d{3,4}p|SD|HD|FHD|4K|Ultra\s*HD)', re.I)
    CD_REGEX_UTF8 = re.compile(r"filename\*=utf-8''([^;]+)", re.I)
    CD_REGEX_QUOTED = re.compile(r'filename="([^"]+)"', re.I)
    CD_REGEX_RAW = re.compile(r"filename=([^;]+)", re.I)

    def __init__(self):
        self.session: currequests.Session = self._init_session()
        self.initialized: bool = False
        self.last_mq_referer: Optional[str] = None
        self.metadata: Dict[str, Any] = {}

    def _init_session(self) -> currequests.Session:
        """Initialize a high-fidelity browser session."""
        session = currequests.Session(impersonate="chrome124")
        session.headers.update({
            "User-Agent": self.UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        return session

    def init_session(self) -> None:
        """Warms up the session by visiting entry points."""
        if self.initialized:
            return
        try:
            logger.info("[*] Warming up session...")
            self.session.get("https://rareanimes.app/", timeout=15)
            # Ensure ROOT_URL is visited to establish domain-level cookies
            self.session.get(self.ROOT_URL, timeout=15, headers={"Referer": "https://rareanimes.app/"})
            self.initialized = True
        except Exception as e:
            logger.warning(f"[!] Session warmup partially failed: {e}")
            self.initialized = True

    def get_links(self, url: str, ep_start: Optional[int] = None, ep_end: Optional[int] = None) -> Dict[str, Any]:
        """Main entry point to extract direct links from a series page."""
        logger.info(f"[*] Analyzing Series: {url}")
        try:
            self.init_session()
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200:
                return {"error": f"Failed to load page (Status {resp.status_code})", "episodes": []}
                
            soup = BeautifulSoup(resp.text, "html.parser")
            series_info = self._scrape_series_metadata(soup, url)
            
            raw_eps = self._extract_episodes(resp.text, url)
            if not raw_eps:
                return {"error": "No episodes found on page", "episodes": []}

            # Range-based episode selection
            selected_eps = self._filter_episodes(raw_eps, ep_start, ep_end)
            
            results = []
            for i, ep in enumerate(selected_eps, 1):
                logger.info(f"[*] Processing Episode {i}/{len(selected_eps)}: {ep['label']}")
                self.last_mq_referer = None
                links = self._try_mirrors(ep["mirrors"])
                
                entry = {
                    "episode": ep["label"],
                    "url": ep["mirrors"][0]["url"],
                    "referer": self.last_mq_referer,
                    "cookies": self.session.cookies.get_dict(),
                    "user_agent": self.UA,
                    "downloads": links if links else []
                }
                if not links:
                    entry["error"] = "Failed to bypass any mirror"
                results.append(entry)

            return {"episodes": results, "series_info": series_info}
        except Exception as e:
            logger.error(f"[!] get_links error: {e}", exc_info=True)
            return {"error": str(e), "episodes": [], "series_info": ""}

    def _scrape_series_metadata(self, soup: BeautifulSoup, url: str) -> str:
        """Intelligently extracts the most accurate series name/info."""
        # 1. Look for Full Name in the Series Info section
        info_section = soup.find(string=lambda t: t and 'Anime Series Info' in t)
        if info_section:
            parent = info_section.find_parent(["h5", "h4", "p", "div"])
            if parent:
                # Scan next few siblings for "Full Name:"
                curr = parent
                for _ in range(12):
                    curr = curr.next_sibling
                    if not curr: break
                    text = curr.get_text(strip=True) if hasattr(curr, "get_text") else str(curr).strip()
                    if "Full Name:" in text:
                        return text.split("Full Name:")[1].strip()

        # 2. Fallback to H1 or Title tag
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)
            
        title = soup.find("title")
        if title:
            return title.get_text(strip=True).split("|")[0].split("-")[0].strip()
            
        # 3. Final fallback: URL slug
        return url.rstrip("/").split("/")[-1].replace("-", " ").title()

    def _extract_episodes(self, html: str, referer: str) -> List[Dict[str, Any]]:
        """Parses HTML to find and group episode links."""
        soup = BeautifulSoup(html, "html.parser")
        potential_links = []
        seen_urls = set()
        
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href in seen_urls: continue
            
            is_zipper = "codedew.com/zipper/" in href
            is_hub = "store.animetoonhindi.com" in href or "/multiquality/" in href
            
            if not (is_zipper or is_hub): continue
            seen_urls.add(href)
            
            label = self._get_label_from_tag(a)
            potential_links.append({"url": href, "label": label, "is_hub": is_hub})

        # Process hubs and group
        final_links = []
        for item in potential_links:
            if item["is_hub"]:
                final_links.extend(self._extract_hub_links(item["url"]))
            else:
                final_links.append(item)
                
        # Group by label
        grouped: Dict[str, List[Dict[str, str]]] = {}
        for link in final_links:
            grouped.setdefault(link["label"], []).append({"url": link["url"], "referer": referer})
            
        # Return structured list
        return [{"label": k, "mirrors": v} for k, v in grouped.items()]

    def _extract_hub_links(self, hub_url: str) -> List[Dict[str, Any]]:
        """Extracts individual zipper links from a multi-quality hub page."""
        try:
            r = self.session.get(hub_url, timeout=15)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            eps = []
            for a in soup.find_all("a", href=True):
                if "codedew.com/zipper/" in a["href"]:
                    label = a.get_text(strip=True)
                    m = self.EP_REGEX.search(label)
                    eps.append({
                        "url": a["href"], 
                        "label": m.group(1).title() if m else label, 
                        "is_hub": False
                    })
            return eps
        except Exception:
            return []

    def _get_label_from_tag(self, tag: Any) -> str:
        """Extracts a descriptive episode label by looking at the tag and its surroundings."""
        # 1. Link text itself
        text = tag.get_text(strip=True)
        if len(text) > 3:
            m = self.EP_REGEX.search(text)
            if m: return m.group(1).title()
            
        # 2. Parent container text
        parent = tag.find_parent(["p", "div", "li"])
        if parent:
            p_text = parent.get_text(" ", strip=True)
            m = self.EP_REGEX.search(p_text)
            if m and len(p_text) < 150: return m.group(1).title()
            
        return "Episode/Download"

    def _filter_episodes(self, episodes: List[Dict], start: Optional[int], end: Optional[int]) -> List[Dict]:
        """Filters episodes based on numeric range."""
        # Identification function
        def get_num(ep):
            m = re.search(r'(\d+)', ep["label"])
            return int(m.group(1)) if m else 0
            
        # Sort first
        episodes.sort(key=get_num)
        
        if start is None:
            return episodes
            
        s_idx = max(0, start - 1)
        e_idx = len(episodes) if end is None else min(len(episodes), end)
        return episodes[s_idx:e_idx]

    def _try_mirrors(self, mirrors: List[Dict[str, str]]) -> Optional[List[Dict]]:
        """Attempts to bypass mirrors until successful."""
        for mirror in mirrors:
            links = self.process_zipper(mirror["url"], mirror["referer"])
            if links: return links
        return None

    def process_zipper(self, url: str, referer: str) -> Optional[List[Dict]]:
        """Bypasses the zipper multi-step redirect to reach the multi-quality page."""
        try:
            curr_url, curr_ref = url, referer
            for step in range(1, 6):
                headers = {
                    "Referer": curr_ref,
                    "Sec-Fetch-Site": "cross-site" if "codedew.com" in curr_url else "same-origin",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Dest": "document"
                }
                
                resp = self.session.get(curr_url, headers=headers, timeout=15)
                if resp.status_code != 200: return None
                
                # Check for the multi-quality token
                token_match = re.search(r'name="rtiwatch"\s+value="([^"]+)"', resp.text)
                if token_match and token_match.group(1) != "notranslate":
                    mq_url = f"{self.MQ_BASE_URL}downlead/{token_match.group(1)}/"
                    self.last_mq_referer = mq_url
                    return self.process_multiquality(mq_url)
                
                # Find the 'Continue' button for next step
                soup = BeautifulSoup(resp.text, "html.parser")
                next_url = self._find_next_button(soup)
                if not next_url: return None
                
                curr_ref, curr_url = curr_url, next_url
                time.sleep(1.0)
        except Exception as e:
            logger.debug(f"Zipper error: {e}")
        return None

    def _find_next_button(self, soup: BeautifulSoup) -> Optional[str]:
        """Locates the 'Continue' or 'Next' button URL."""
        # Priority 1: Specific IDs
        for bid in ["goBtn", "mainActionBtn", "btn-main"]:
            btn = soup.find("a", id=bid)
            if btn and btn.get("href"):
                return urljoin(self.ROOT_URL, btn["href"])
        
        # Priority 2: Text matching
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            if any(kw in text for kw in ["continue", "next", "get link", "skip"]):
                return urljoin(self.ROOT_URL, a["href"])
        return None

    def process_multiquality(self, url: str) -> Optional[List[Dict]]:
        """Handles the multi-quality page API to get direct links."""
        try:
            resp = self.session.get(url, headers={"Referer": self.ROOT_URL}, timeout=15)
            # Extract JuicyData JSON
            jd = self._extract_juicy_data(resp.text)
            if not jd or not jd.get("token"): return None
            
            # API interaction
            token = jd["token"]
            links_api = urljoin(self.MQ_BASE_URL, jd["routes"]["links"])
            
            # Optional ping
            if jd.get("routes", {}).get("ping"):
                ping_url = urljoin(self.MQ_BASE_URL, jd["routes"]["ping"])
                self.session.post(ping_url, headers={"Referer": url}, timeout=5)
            
            time.sleep(1.2)
            api_resp = self.session.post(
                links_api,
                headers={
                    "Referer": url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/json"
                },
                json={"captcha": None, "_token": token},
                timeout=15
            )
            
            data = api_resp.json()
            if data.get("success") and data.get("qualities"):
                results = []
                for q in data["qualities"]:
                    label = q.get("label") or q.get("quality") or "N/A"
                    results.append({
                        "label": label,
                        "size": q.get("size"),
                        "link": q["link"],
                        "metadata": {
                            "cookies": self.session.cookies.get_dict(),
                            "referer": url,
                            "user_agent": self.UA
                        }
                    })
                return results
        except Exception as e:
            logger.debug(f"MQ API error: {e}")
        return None

    def _extract_juicy_data(self, html: str) -> Optional[Dict]:
        """Parses the window.juicyData object from script tags."""
        match = re.search(r"window\.juicyData\s*=\s*(\{)", html)
        if not match: return None
        
        start = match.start(1)
        # Use simple brace counting to find end of object
        depth, end = 0, -1
        for i in range(start, len(html)):
            if html[i] == "{": depth += 1
            elif html[i] == "}": depth -= 1
            if depth == 0:
                end = i + 1
                break
        
        if end == -1: return None
        try:
            obj = json.loads(html[start:end])
            return obj.get("data", obj)
        except:
            return None

    def get_filename_from_cd(self, cd: Optional[str]) -> Optional[str]:
        """Extracts filename from Content-Disposition header with fallback patterns."""
        if not cd: return None
        
        # 1. UTF-8 encoded pattern
        res = self.CD_REGEX_UTF8.findall(cd)
        if res: return unquote(res[0])
        
        # 2. Quoted pattern
        res = self.CD_REGEX_QUOTED.findall(cd)
        if res: return res[0]
        
        # 3. Raw pattern
        res = self.CD_REGEX_RAW.findall(cd)
        if res: return res[0].strip().strip('"')
        
        return None

    def resolve_filename(self, url: str, referer: Optional[str] = None, cookies: Optional[Dict] = None) -> Optional[str]:
        """
        High-fidelity filename resolution using professional browser headers.
        Tries HEAD first for speed, then GET with stream=True for accuracy.
        """
        headers = {
            "Referer": referer or self.ROOT_URL,
            "User-Agent": self.UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"'
        }
        
        try:
            with currequests.Session(impersonate="chrome124") as session:
                if cookies: session.cookies.update(cookies)
                
                # HEAD request check
                try:
                    res = session.head(url, headers=headers, allow_redirects=True, timeout=12)
                    filename = self.get_filename_from_cd(res.headers.get("Content-Disposition"))
                    if filename: return filename
                except: pass

                # GET stream check
                res = session.get(url, headers=headers, stream=True, allow_redirects=True, timeout=20)
                filename = self.get_filename_from_cd(res.headers.get("Content-Disposition"))
                if filename: return filename
                
                # URL path basename check (avoid generic bypass paths)
                path = urlparse(res.url).path
                basename = os.path.basename(path)
                if "." in basename and not any(x in basename.lower() for x in ["zipper", "leech", "downlead"]):
                    return unquote(basename)
                    
        except Exception as e:
            logger.debug(f"Resolution failed: {e}")
        return None
