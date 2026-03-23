import re
import time
import logging
import asyncio
import random
from typing import List, Dict, Optional, Any, Set, Tuple
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
import requests
from requests import Session
try:
    from curl_cffi import requests as currequests
except ImportError:
    currequests = None

# --- LOGGING ---
logger = logging.getLogger("HindiAnimeZone")

class HindiAnimeZone:
    """
    Professional Bypasser for HindiAnimeZone.com.
    Handles multi-quality episodes and direct gate links.
    Optimized for asynchronous parallel analysis.
    """
    
    BASE_URL = "https://hindianimezone.com/"
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ]

    def __init__(self, async_session=None):
        self.session = async_session
        self._own_session = False
        if not self.session:
            if currequests:
                self.session = currequests.AsyncSession(impersonate="chrome124")
                self._own_session = True
            else:
                self.session = Session() # Fallback for non-async usage if needed
                self.session.headers.update({'User-Agent': random.choice(self.USER_AGENTS)})
                
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': self.BASE_URL
        })
        self.GATE_PATTERN = re.compile(r'code=[a-z0-9]{4,}', re.I)

    async def close(self):
        if self._own_session and hasattr(self.session, 'close'):
            await self.session.close()

    async def get_episode_list(self, url: str, selection: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """
        Quickly parses the page to find episodes and their gate links.
        Does NOT resolve final download links yet (JIT optimization).
        """
        logger.info(f"Parsing HindiAnimeZone episodes: {url}")
        if self.GATE_PATTERN.search(url):
            return await self._handle_direct_gate(url)

        try:
            r = await self.session.get(url, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            page_title = soup.find('h1').get_text(strip=True) if soup.find('h1') else "Anime Content"

            all_ep = soup.select('div.episode')
            episodes = [ep for ep in all_ep if not ep.find('div', class_='episode')]
            
            if not episodes:
                # For legacy pages, we still resolve immediately as they are usually single-episode
                return await self._process_legacy_page(soup)

            if selection:
                episodes = [episodes[i-1] for i in selection if 0 < i <= len(episodes)]

            ep_data_list = []
            seen_hrefs: Set[str] = set()
            for ep_div in episodes:
                ep_name = self._extract_ep_name(ep_div, page_title)
                if 'how to download' in ep_name.lower(): continue
                
                lang_tag = ep_div.select_one('.language')
                lang_text = lang_tag.get_text(strip=True).lower() if lang_tag else ""
                if not any(x in lang_text for x in ['hindi', 'multi', 'dual']):
                    if not any(x in ep_name.lower() or x in page_title.lower() for x in ['hindi', 'multi', 'dual']):
                        continue
                    
                quality_map = self._get_quality_links(ep_div, seen_hrefs)
                if not quality_map: continue
                
                ep_data_list.append({
                    "episode": ep_name.split('|')[0].strip(),
                    "quality_map": quality_map,
                    "series_info": page_title,
                    "resolved": False # Mark as needs resolution
                })
            
            return ep_data_list
        except Exception as e:
            logger.error(f"HindiAnimeZone Parse Error: {e}")
            return []

    async def resolve_episode(self, ep_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolves gate links for a single episode."""
        if ep_data.get("resolved") or not ep_data.get("quality_map"):
            return ep_data

        tasks = []
        for q, data in ep_data['quality_map'].items():
            tasks.append(self._process_quality(q, data))
        
        res = await asyncio.gather(*tasks)
        ep_data["downloads"] = []
        for dl_list in res:
            if dl_list: ep_data["downloads"].extend(dl_list)
        
        ep_data["resolved"] = True
        return ep_data

    async def pro_main_bypass(self, url: str, selection: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Legacy wrapper that resolves all links at once."""
        ep_list = await self.get_episode_list(url, selection)
        tasks = [self.resolve_episode(ep) for ep in ep_list]
        return await asyncio.gather(*tasks)

    def resolve_filename(self, url: str, referer: Optional[str] = None, cookies: Optional[Dict] = None) -> Optional[str]:
        """Placeholder to satisfy the bypasser interface in bot.py."""
        return None

    def _extract_ep_name(self, ep_div: BeautifulSoup, fallback: str) -> str:
        tag = ep_div.select_one('.episode-title')
        if tag: return tag.get_text(strip=True)
        prev = ep_div.find_previous(['h3', 'h2', 'h1'])
        return prev.get_text(strip=True) if prev else fallback

    def _get_quality_links(self, container: BeautifulSoup, seen: Set[str]) -> Dict[str, Dict[str, str]]:
        q_map = {}
        for a in container.find_all('a', href=True):
            href, text = a['href'], a.get_text(strip=True).lower()
            if 't.me' in href or 'telegram' in text or 'watch' in text or href in seen:
                continue
            
            # Allow links with code or links from like.hindianimezone.com
            is_valid_gate = self.GATE_PATTERN.search(href) or "like.hindianimezone.com" in href
            if not is_valid_gate:
                continue
            
            seen.add(href)
            q_key = None
            q_match = re.search(r'q=([^&]+)', href)
            # Use q parameter or link text as quality label
            q_val = q_match.group(1).lower().replace('+', ' ') if q_match else text
            
            if '480p' in q_val: q_key = '480p'
            elif '720p' in q_val: q_key = '720p'
            elif '1080p hq' in q_val or '1080phq' in q_val: q_key = '1080p_HQ'
            elif '1080p' in q_val: q_key = '1080p'

            if q_key:
                is_better = any(x in text for x in ['x265', '10bit', 'hevc'])
                if q_key not in q_map or is_better:
                    label = text.upper() if len(text) > 3 else q_key.upper()
                    if 'X265' not in label.upper() and 'X265' in q_val.upper(): label += " x265"
                    q_map[q_key] = {'label': label, 'url': href}
        return q_map

    async def _process_quality(self, q, data):
        """Helper to process a single quality concurrently."""
        server_links = await self.bypass_gate(data['url'])
        if not server_links: return []
        
        dl_list = []
        # Support parallel resolution of multiple servers if needed, but usually 1-2 major ones exist
        for srv, srv_url in server_links.items():
            final_link = await self.get_final_link(srv, srv_url)
            if final_link:
                dl_list.append({
                    "label": data['label'],
                    "link": final_link,
                    "server": srv,
                    "metadata": self._get_metadata()
                })
        return dl_list

    async def _handle_direct_gate(self, url: str) -> List[Dict[str, Any]]:
        links = await self.bypass_gate(url)
        downloads = []
        if links:
            tasks = [self.get_final_link(s, u) for s, u in links.items()]
            finals = await asyncio.gather(*tasks)
            names = list(links.keys())
            for i, f in enumerate(finals):
                if f: downloads.append({"label": "Direct Link", "link": f, "server": names[i], "metadata": self._get_metadata()})
        
        return [{"episode": "Direct Download", "downloads": downloads}] if downloads else []

    async def bypass_gate(self, gate_url: str) -> Optional[Dict[str, str]]:
        """Simulates verification flow to reveal server links (Async)."""
        try:
            r = await self.session.get(gate_url, timeout=15)
            final_url = str(r.url)
            
            def extract(html: str) -> Dict[str, str]:
                s = BeautifulSoup(html, 'html.parser')
                l = {a['data-label']: a['href'] for a in s.find_all('a', attrs={'data-label': True})}
                if not l:
                    for a in s.find_all('a', href=True):
                        t = a.get_text(strip=True).upper()
                        for srv in ['MEGA', 'GDSHARE', 'FILEPRESS', 'GDFLIX', 'DEADDRIVE']:
                            if srv in t: l[srv] = a['href']
                return l

            for _ in range(2): # Reduced retries for speed
                links = extract(r.text)
                if links: return links
                await asyncio.sleep(1.2)
                await self.session.post(final_url, data={"verify": "1"}, headers={'Referer': final_url}, timeout=15)
                r = await self.session.get(final_url, headers={'Referer': final_url}, timeout=15)
            return extract(r.text) or None
        except: return None

    async def get_final_link(self, srv_name: str, url: str) -> Optional[str]:
        """Routes to specific server bypassers (Async)."""
        srv = srv_name.upper()
        res = None
        if 'GDSHARE' in srv: res = await self._bypass_gdshare(url)
        elif 'GDFLIX' in srv: res = await self._bypass_gdflix(url)
        elif 'FILEPRESS' in srv: res = await self._bypass_filepress(url)
        
        if res: logger.info(f"Final DL ({srv_name}): {res}")
        return res

    def _get_metadata(self) -> Dict[str, Any]:
        return {
            "cookies": self.session.cookies.get_dict(),
            "user_agent": self.session.headers.get('User-Agent')
        }

    async def _bypass_gdshare(self, url: str) -> Optional[str]:
        try:
            r = await self.session.get(url, timeout=15)
            csrf_match = re.search(r"CSRF_TOKEN\s*=\s*['\"]([^'\"]+)", r.text)
            if not csrf_match: return None
            csrf = csrf_match.group(1)
            
            h = {"X-Requested-With": "XMLHttpRequest", "X-CSRF-Token": csrf, "Referer": str(r.url)}
            resp = await self.session.get(str(r.url), headers=h, timeout=10)
            data = resp.json()
            token = data["data"]["access_token"]
            
            resp2 = await self.session.get(f"{r.url}&get_secure_links=1&access_token={token}", headers=h, timeout=10)
            j = resp2.json()
            inst = f"https://gcloud.sbs/instant/{j['gphotos_id']}/{j['gp_id']}"
            
            r2 = await self.session.get(inst, headers=h)
            csrf2_match = re.search(r"CSRF_TOKEN\s*=\s*['\"]([^'\"]+)", r2.text)
            if csrf2_match:
                h["X-CSRF-Token"] = csrf2_match.group(1)
            
            res_final = await self.session.get(f"{inst}?ajax=1", headers=h)
            return res_final.json().get("download_url")
        except: return None

    async def _bypass_filepress(self, url: str) -> Optional[str]:
        try:
            fid = re.search(r'/file/([a-f0-9]+)', url).group(1)
            domain = re.search(r'(https?://[^/]+)', url).group(1)
            api = f"{domain}/api/file"
            h = {"User-Agent": "Mozilla/5.0", "Referer": f"{domain}/file/{fid}"}

            resp = await self.session.get(f"{api}/get/{fid}", headers=h)
            info = resp.json()
            name = info["data"]["name"]
            
            resp_token = await self.session.post(f"{api}/downlaod/", 
                json={"id": fid, "method": "indexDownlaod", "captchaValue": ""}, 
                headers=h)
            token_res = resp_token.json()
            token = token_res["data"]

            h["Referer"] = f"{domain}/download/{name}"
            resp_final = await self.session.post(f"{api}/downlaod2/", 
                json={"id": token, "method": "indexDownlaod", "captchaValue": None}, 
                headers=h)
            final = resp_final.json()
            return final["data"][0] if final.get("status") else None
        except: return None

    async def _bypass_gdflix(self, url: str) -> Optional[str]:
        try:
            h = {"User-Agent": self.USER_AGENTS[0]}
            r = await self.session.get(url, headers=h, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            domain = re.search(r'https?://([^/]+)', url).group(1)

            for target in ["INSTANT DL", "DIRECT SERVER"]:
                for a in soup.find_all("a", href=True):
                    if target in a.get_text(strip=True).upper() and "login" not in a["href"].lower():
                        link = a["href"] if a["href"].startswith("http") else f"https://{domain}{a['href']}"
                        try:
                            # head/get works better with Chrome impersonation
                            resp = await self.session.get(link, headers=h, allow_redirects=True, timeout=10)
                            final_head = str(resp.url)
                            if "url=" in final_head:
                                return re.search(r'url=(https?://[^\s&]+)', final_head).group(1)
                            return final_head
                        except: return link
        except: return None

    async def get_episode_info(self, url: str) -> Dict[str, Any]:
        """Scans page for Hindi/Multi count."""
        try:
            r = await self.session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            max_ep, has_h = 0, False
            for ep in soup.select('div.episode'):
                lang = ep.select_one('.language').get_text(strip=True).lower() if ep.select_one('.language') else ""
                title = ep.select_one('.episode-title').get_text(strip=True) if ep.select_one('.episode-title') else ""
                is_hin = any(x in lang for x in ['multi', 'hindi', 'hin'])
                num = re.search(r'Episode\s*(\d+)', title, re.I)
                if num and is_hin:
                    max_ep = max(max_ep, int(num.group(1)))
                    has_h = True
            return {'count': max_ep if has_h else "N/A", 'status': "HINDI/MULTI" if has_h else "ENG/JAP"}
        except: return {'count': "N/A", 'status': "UNKNOWN"}

    async def _process_legacy_page(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        seen: Set[str] = set()
        q_map = self._get_quality_links(soup, seen)
        
        tasks = []
        for q, data in q_map.items():
            tasks.append(self._process_quality(q, data))
        
        results = await asyncio.gather(*tasks)
        downloads = []
        for dl_list in results:
            if dl_list: downloads.extend(dl_list)
        
        return [{"episode": "Content", "downloads": downloads}] if downloads else []
