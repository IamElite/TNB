import requests
import re
import random
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone

class HindiAnimeZone:
    def __init__(self):
        self.GATE_PATTERN = re.compile(r'download[a-z0-9]*\.(?:php|html|htm|py)?\?code=|(?:\?|&)code=[a-z0-9]+', re.IGNORECASE)
        self.BASE_URL = "https://hindianimezone.com/"
        self.USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1"
        ]
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': self.BASE_URL
        })

    def pro_main_bypass(self, url):
        """Main entry point to bypass anime pages or direct gate links."""
        print(f"[*] ANALYZING: {url}")
        
        if self.GATE_PATTERN.search(url):
            self._handle_direct_gate(url)
            return

        try:
            r = self.session.get(url, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            page_title = soup.find('h1').text.strip() if soup.find('h1') else "Anime Content"

            # Filter for leaf episode blocks
            all_ep = soup.select('div.episode')
            episodes = [ep for ep in all_ep if not ep.find('div', class_='episode')]
            
            if not episodes:
                self.process_legacy_page(soup)
                return

            print(f"\n[+] FOUND {len(episodes)} CONTENT BLOCK(S)")
            seen_hrefs = set()
            
            for ep_div in episodes:
                ep_name = self._extract_ep_name(ep_div, page_title)
                if 'how to download' in ep_name.lower(): continue
                    
                quality_map = self._get_quality_links(ep_div, seen_hrefs)
                if not quality_map: continue
                    
                print(f"\n{'='*40}\n[*] PROCESSING: {ep_name.split('|')[0].strip()}")
                for q, data in quality_map.items():
                    print(f"\n    [*] QUALITY: {data['label']}")
                    server_links = self.bypass_gate(data['url'])
                    if server_links:
                        for s, l in server_links.items(): 
                            print(f"        {s}: {l}")
                            self.get_final_link(s, l)
                    else:
                        print(f"        [!] Failed to extract server links")
        except Exception as e:
            print(f"[!] Error: {e}")

    def _extract_ep_name(self, ep_div, fallback):
        tag = ep_div.select_one('.episode-title')
        if tag: return tag.text.strip()
        prev = ep_div.find_previous(['h3', 'h2', 'h1'])
        return prev.text.strip() if prev else fallback

    def _get_quality_links(self, ep_div, seen):
        q_map = {}
        for a in ep_div.find_all('a', href=True):
            href, text = a['href'], a.text.strip().lower()
            # Bug Fix: Properly exclude telegram buttons and irrelevant links
            if 't.me' in href or 'telegram' in text or 'watch' in text or not self.GATE_PATTERN.search(href) or href in seen:
                continue
            
            seen.add(href)
            q_key = None
            q_match = re.search(r'q=([^&]+)', href)
            q_val = q_match.group(1).lower().replace('+', ' ') if q_match else text
            
            if '480p' in q_val: q_key = '480p'
            elif '720p' in q_val: q_key = '720p'
            elif '1080p hq' in q_val or '1080phq' in q_val: q_key = '1080p_HQ'
            elif '1080p' in q_val: q_key = '1080p'

            if q_key:
                is_better = any(x in text for x in ['x265', '10bit', 'hevc'])
                if q_key not in q_map or is_better:
                    q_map[q_key] = {'label': text.upper() or q_key.upper(), 'url': href}
        return q_map

    def _handle_direct_gate(self, url):
        links = self.bypass_gate(url)
        if links:
            print(f"[+] SERVERS EXTRACTED:")
            for n, l in links.items(): 
                print(f"    - {n}: {l}")
                self.get_final_link(n, l)


    def bypass_gate(self, gate_url):
        """Bypasses download gates using verification simulation."""
        try:
            r = self.session.get(gate_url, timeout=15)
            final_url = r.url
            
            def get_links(html):
                s = BeautifulSoup(html, 'html.parser')
                l = {a['data-label']: a['href'] for a in s.find_all('a', attrs={'data-label': True})}
                if not l:
                    for a in s.find_all('a', href=True):
                        t = a.text.strip().upper()
                        for srv in ['MEGA', 'GDSHARE', 'FILEPRESS', 'GDFLIX', 'DEADDRIVE']:
                            if srv in t: l[srv] = a['href']
                return l

            for _ in range(3):
                links = get_links(r.text)
                if links: return links
                time.sleep(1.5)
                r = self.session.post(final_url, data={"verify": "1"}, headers={'Referer': final_url}, timeout=15)
                r = self.session.get(final_url, headers={'Referer': final_url}, timeout=15)
            return get_links(r.text) or None
        except: return None

    # V2 Bypass methods
    def _get_gdshare_link(self, url):
        try:
            s = requests.Session()
            h = {"User-Agent": "Mozilla/5.0"}
            r = s.get(url, headers=h)
            csrf = re.search(r"CSRF_TOKEN\s*=\s*['\"]([^'\"]+)", r.text).group(1)
            h.update({"X-Requested-With": "XMLHttpRequest", "X-CSRF-Token": csrf, "Referer": r.url})

            token = s.get(r.url, headers=h, timeout=15).json()["data"]["access_token"]
            j = s.get(f"{r.url}&get_secure_links=1&access_token={token}", headers=h, timeout=15).json()

            inst = f"https://gcloud.sbs/instant/{j['gphotos_id']}/{j['gp_id']}"
            csrf2 = re.search(r"CSRF_TOKEN\s*=\s*['\"]([^'\"]+)", s.get(inst, headers=h, timeout=15).text).group(1)
            h["X-CSRF-Token"] = csrf2

            return s.get(f"{inst}?ajax=1", headers=h, timeout=15).json().get("download_url")
        except Exception:
            return None

    def _get_filepress_link(self, url):
        try:
            fid = re.search(r'/file/([a-f0-9]+)', url).group(1)
            domain = re.search(r'(https?://[^/]+)', url).group(1)
            api = f"{domain}/api/file"
            h = {"User-Agent": "Mozilla/5.0", "Referer": f"{domain}/file/{fid}"}

            name = requests.get(f"{api}/get/{fid}", headers=h, timeout=15).json()["data"]["name"]
            token = requests.post(f"{api}/downlaod/", json={"id": fid, "method": "indexDownlaod", "captchaValue": ""}, headers=h, timeout=15).json()["data"]

            h["Referer"] = f"{domain}/download/{name}"
            r = requests.post(f"{api}/downlaod2/", json={"id": token, "method": "indexDownlaod", "captchaValue": None}, headers=h, timeout=15).json()

            return r["data"][0] if r.get("status") else None
        except Exception:
            return None

    def _get_gdflix_link(self, url):
        try:
            h = {"User-Agent": "Mozilla/5.0"}
            soup = BeautifulSoup(requests.get(url, headers=h, timeout=20).text, 'html.parser')
            domain = re.search(r'https?://([^/]+)', url).group(1)

            for target in ["INSTANT DL", "DIRECT SERVER"]:
                for a in soup.find_all("a", href=True):
                    if target in a.text.upper() and "login" not in a["href"].lower():
                        link = a["href"] if a["href"].startswith("http") else f"https://{domain}{a['href']}"
                        try:
                            final = requests.head(link, headers=h, allow_redirects=True, timeout=10).url
                            if "url=" in final:
                                final = re.search(r'url=(https?://[^\s&]+)', final).group(1)
                            return final
                        except:
                            return link
        except Exception:
            pass
        return None

    def get_final_link(self, srv_name, url):
        """Routes url to the appropriate bypasser and prints the final link."""
        srv = srv_name.upper()
        if 'GDSHARE' in srv:
            dl = self._get_gdshare_link(url)
            if dl: print(f"        [PRO] FINAL DL (GDShare): {dl}")
        elif 'GDFLIX' in srv:
            dl = self._get_gdflix_link(url)
            if dl: print(f"        [PRO] FINAL DL (GDFlix): {dl}")
        elif 'FILEPRESS' in srv:
            dl = self._get_filepress_link(url)
            if dl: print(f"        [PRO] FINAL DL (FilePress): {dl}")

    def get_episode_info(self, url):
        """Deep scans post page for the latest Hindi/Multi episode."""
        try:
            r = self.session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            max_ep, has_h = 0, False
            for ep in soup.select('div.episode'):
                lang = ep.select_one('.language').text.lower() if ep.select_one('.language') else ""
                title = ep.select_one('.episode-title').text if ep.select_one('.episode-title') else ""
                # Preserving user's expanded logic
                is_multi = any(x in lang for x in ['multi', 'hindi', 'hin-', 'hin', 'hi']) or lang == 'hin'
                num = re.search(r'Episode\s*(\d+)', title, re.IGNORECASE)
                if num and is_multi:
                    max_ep = max(max_ep, int(num.group(1)))
                    has_h = True
            return {'count': max_ep if has_h else "N/A", 'status': "HINDI/MULTI" if has_h else "ENG/JAP"}
        except: return {'count': "N/A", 'status': "UNKNOWN"}

    def get_latest_updates(self):
        """Fetches updates from the last 12 hours with deep scanning."""
        print("[*] FETCHING LATEST UPDATES (LAST 12 HOURS)...")
        try:
            r = self.session.get(self.BASE_URL, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            updates, now = [], datetime.now(timezone.utc)
            threshold = now - timedelta(hours=12)

            for tag in soup.find_all('time', datetime=True):
                try:
                    dt = datetime.fromisoformat(tag['datetime']).replace(tzinfo=timezone.utc)
                    if dt < threshold: continue
                    parent = tag.find_parent(['div', 'article', 'li'])
                    while parent and not parent.find('a', href=True): parent = parent.parent
                    if not parent: continue
                    
                    for link_tag in parent.find_all('a', href=True):
                        url, title = link_tag['href'], link_tag.text.strip()
                        if any(x in url for x in ['/category/', '/tag/', '/author/']) or len(title) < 5: continue
                        if self.BASE_URL in url and not any(u['url'] == url for u in updates):
                            print(f"    [>] Scanning: {title[:40]}...")
                            info = self.get_episode_info(url)
                            updates.append({'title': title, 'url': url, 'time': dt.strftime('%H:%M'), 'episode': info['count'], 'language': info['status']})
                            break
                except: continue
                if len(updates) >= 20: break
            return updates
        except Exception as e:
            print(f"[!] Error: {e}"); return []

    def process_legacy_page(self, soup):
        seen = set()
        q_map = self._get_quality_links(soup, seen)
        for q, data in q_map.items():
            print(f"\n[*] QUALITY: {data['label']}")
            links = self.bypass_gate(data['url'])
            if links:
                for s, l in links.items(): 
                    print(f"    {s}: {l}")
                    self.get_final_link(s, l)
