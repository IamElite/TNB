import re
import time
import logging
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
    """
    
    BASE_URL = "https://hindianimezone.com/"
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ]

    def __init__(self):
        if currequests:
            self.session = currequests.Session(impersonate="chrome124")
        else:
            self.session = Session()
            self.session.headers.update({'User-Agent': random.choice(self.USER_AGENTS)})
            
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': self.BASE_URL
        })
        self.GATE_PATTERN = re.compile(r'code=[a-z0-9]{4,}', re.I)

    def pro_main_bypass(self, url: str, selection: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """
        Main entry point for bypassing HindiAnimeZone links.
        """
        logger.info(f"Analyzing HindiAnimeZone URL: {url}")
        
        if self.GATE_PATTERN.search(url):
            return self._handle_direct_gate(url)

        try:
            r = self.session.get(url, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            page_title = soup.find('h1').get_text(strip=True) if soup.find('h1') else "Anime Content"

            # Primary content detection
            all_ep = soup.select('div.episode')
            episodes = [ep for ep in all_ep if not ep.find('div', class_='episode')]
            
            if not episodes:
                return self._process_legacy_page(soup)

            # Selection filtering
            if selection:
                episodes = [episodes[i-1] for i in selection if 0 < i <= len(episodes)]

            results = []
            seen_hrefs: Set[str] = set()
            
            for ep_div in episodes:
                ep_name = self._extract_ep_name(ep_div, page_title)
                if 'how to download' in ep_name.lower(): continue
                
                # Language filtering: only Hindi or Multi-audio
                lang_tag = ep_div.select_one('.language')
                lang_text = lang_tag.get_text(strip=True).lower() if lang_tag else ""
                if not any(x in lang_text for x in ['hindi', 'multi', 'dual']):
                    # Fallback check on ep_name or page_title
                    if not any(x in ep_name.lower() or x in page_title.lower() for x in ['hindi', 'multi', 'dual']):
                        continue
                    
                quality_map = self._get_quality_links(ep_div, seen_hrefs)
                if not quality_map: continue
                    
                episode_entry = {
                    "episode": ep_name.split('|')[0].strip(),
                    "downloads": [],
                    "series_info": page_title
                }
                
                logger.info(f"Processing Episode: {episode_entry['episode']}")
                for q, data in quality_map.items():
                    server_links = self.bypass_gate(data['url'])
                    if not server_links: continue
                    
                    for srv, srv_url in server_links.items(): 
                        final_link = self.get_final_link(srv, srv_url)
                        if final_link:
                            episode_entry["downloads"].append({
                                "label": data['label'],
                                "link": final_link,
                                "server": srv,
                                "metadata": self._get_metadata()
                            })
                
                if episode_entry["downloads"]:
                    results.append(episode_entry)
            
            return results
        except Exception as e:
            logger.error(f"HindiAnimeZone Error: {e}", exc_info=True)
            return []

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

    def _handle_direct_gate(self, url: str) -> List[Dict[str, Any]]:
        links = self.bypass_gate(url)
        downloads = []
        if links:
            for sname, surl in links.items(): 
                final = self.get_final_link(sname, surl)
                if final:
                    downloads.append({"label": "Direct Link", "link": final, "server": sname, "metadata": self._get_metadata()})
        
        return [{"episode": "Direct Download", "downloads": downloads}] if downloads else []

    def bypass_gate(self, gate_url: str) -> Optional[Dict[str, str]]:
        """Simulates verification flow to reveal server links."""
        try:
            r = self.session.get(gate_url, timeout=15)
            final_url = r.url
            
            def extract(html: str) -> Dict[str, str]:
                s = BeautifulSoup(html, 'html.parser')
                l = {a['data-label']: a['href'] for a in s.find_all('a', attrs={'data-label': True})}
                if not l:
                    for a in s.find_all('a', href=True):
                        t = a.get_text(strip=True).upper()
                        for srv in ['MEGA', 'GDSHARE', 'FILEPRESS', 'GDFLIX', 'DEADDRIVE']:
                            if srv in t: l[srv] = a['href']
                return l

            for _ in range(3):
                links = extract(r.text)
                if links: return links
                time.sleep(1.5)
                # Attempt verification triggers
                self.session.post(final_url, data={"verify": "1"}, headers={'Referer': final_url}, timeout=15)
                r = self.session.get(final_url, headers={'Referer': final_url}, timeout=15)
            return extract(r.text) or None
        except: return None

    def get_final_link(self, srv_name: str, url: str) -> Optional[str]:
        """Routes to specific server bypassers."""
        srv = srv_name.upper()
        res = None
        if 'GDSHARE' in srv: res = self._bypass_gdshare(url)
        elif 'GDFLIX' in srv: res = self._bypass_gdflix(url)
        elif 'FILEPRESS' in srv: res = self._bypass_filepress(url)
        
        if res: logger.info(f"Final DL ({srv_name}): {res}")
        return res

    def _get_metadata(self) -> Dict[str, Any]:
        return {
            "cookies": self.session.cookies.get_dict(),
            "user_agent": self.session.headers.get('User-Agent')
        }

    def _bypass_gdshare(self, url: str) -> Optional[str]:
        try:
            # Using main session for unified tokens
            r = self.session.get(url, timeout=15)
            csrf_match = re.search(r"CSRF_TOKEN\s*=\s*['\"]([^'\"]+)", r.text)
            if not csrf_match: return None
            csrf = csrf_match.group(1)
            
            h = {"X-Requested-With": "XMLHttpRequest", "X-CSRF-Token": csrf, "Referer": r.url}
            data = self.session.get(r.url, headers=h, timeout=10).json()
            token = data["data"]["access_token"]
            
            j = self.session.get(f"{r.url}&get_secure_links=1&access_token={token}", headers=h, timeout=10).json()
            inst = f"https://gcloud.sbs/instant/{j['gphotos_id']}/{j['gp_id']}"
            
            r2 = self.session.get(inst, headers=h)
            csrf2_match = re.search(r"CSRF_TOKEN\s*=\s*['\"]([^'\"]+)", r2.text)
            if csrf2_match:
                h["X-CSRF-Token"] = csrf2_match.group(1)
            
            return self.session.get(f"{inst}?ajax=1", headers=h).json().get("download_url")
        except: return None

    def _bypass_filepress(self, url: str) -> Optional[str]:
        try:
            fid = re.search(r'/file/([a-f0-9]+)', url).group(1)
            domain = re.search(r'(https?://[^/]+)', url).group(1)
            api = f"{domain}/api/file"
            h = {"User-Agent": "Mozilla/5.0", "Referer": f"{domain}/file/{fid}"}

            info = requests.get(f"{api}/get/{fid}", headers=h).json()
            name = info["data"]["name"]
            
            token_res = requests.post(f"{api}/downlaod/", 
                json={"id": fid, "method": "indexDownlaod", "captchaValue": ""}, 
                headers=h).json()
            token = token_res["data"]

            h["Referer"] = f"{domain}/download/{name}"
            final = requests.post(f"{api}/downlaod2/", 
                json={"id": token, "method": "indexDownlaod", "captchaValue": None}, 
                headers=h).json()
            return final["data"][0] if final.get("status") else None
        except: return None

    def _bypass_gdflix(self, url: str) -> Optional[str]:
        try:
            h = {"User-Agent": self.USER_AGENTS[0]}
            r = requests.get(url, headers=h, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            domain = re.search(r'https?://([^/]+)', url).group(1)

            for target in ["INSTANT DL", "DIRECT SERVER"]:
                for a in soup.find_all("a", href=True):
                    if target in a.get_text(strip=True).upper() and "login" not in a["href"].lower():
                        link = a["href"] if a["href"].startswith("http") else f"https://{domain}{a['href']}"
                        try:
                            final_head = requests.head(link, headers=h, allow_redirects=True, timeout=10).url
                            if "url=" in final_head:
                                return re.search(r'url=(https?://[^\s&]+)', final_head).group(1)
                            return final_head
                        except: return link
        except: return None

    def get_episode_info(self, url: str) -> Dict[str, Any]:
        """Scans page for Hindi/Multi count."""
        try:
            r = self.session.get(url, timeout=15)
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

    def _process_legacy_page(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        seen: Set[str] = set()
        q_map = self._get_quality_links(soup, seen)
        downloads = []
        for q, data in q_map.items():
            links = self.bypass_gate(data['url'])
            if not links: continue
            for sname, surl in links.items(): 
                final = self.get_final_link(sname, surl)
                if final:
                    downloads.append({"label": data['label'], "link": final, "server": sname, "metadata": self._get_metadata()})
        
        return [{"episode": "Content", "downloads": downloads}] if downloads else []
