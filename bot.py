import os
import io
import sys
import time
import math
import asyncio
import aiohttp
from contextlib import redirect_stdout
from pyrogram import Client, filters
from pyrogram.types import Message
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-5s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AnimeBot")

# Import your custom modules
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
    if round(diff % 5.00) == 0 or current == total:
        percentage = current * 100 / total if total else 0
        speed = current / diff if diff > 0 else 0
        elapsed_time = round(diff) * 1000
        time_to_completion = round((total - current) / speed) * 1000 if speed > 0 else 0
        estimated_total_time = elapsed_time + time_to_completion

        elapsed_time = time_formatter(elapsed_time)
        estimated_total_time = time_formatter(estimated_total_time)

        progress = "[{0}{1}] \n**Progress**: {2}%\n".format(
            ''.join(["█" for i in range(math.floor(percentage / 10))]),
            ''.join(["░" for i in range(10 - math.floor(percentage / 10))]),
            round(percentage, 2))

        tmp = progress + "{0} of {1}\n**Speed:** {2}/s\n**ETA:** {3}\n".format(
            humanbytes(current),
            humanbytes(total),
            humanbytes(speed),
            estimated_total_time if estimated_total_time != '' else "0 s"
        )
        try:
            await message.edit(f"{ud_type}\n{tmp}")
        except Exception:
            pass

async def download_file(url, file_name, status_msg, referer=None, cookies=None, user_agent=None):
    start_t = time.time()
    logger.info(f"Step 5: File download started -> {file_name}")
    headers = {"User-Agent": user_agent or USER_AGENT}
    if referer:
        headers["Referer"] = referer
        
    try:
        state = {"downloaded": 0, "total": 0, "done": False, "error": None}
        
        def sync_download():
            try:
                import cloudscraper
                scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
                if cookies:
                    scraper.cookies.update(cookies)
                with scraper.get(url, headers=headers, stream=True) as r:
                    if r.status_code != 200:
                        state["error"] = f"HTTP {r.status_code}"
                        return
                    
                    state["total"] = int(r.headers.get('content-length', 0))
                    with open(file_name, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                                state["downloaded"] += len(chunk)
            except Exception as e:
                state["error"] = str(e)
            finally:
                state["done"] = True

        loop = asyncio.get_running_loop()
        task = loop.run_in_executor(None, sync_download)
        
        last_update = time.time()
        while not state["done"]:
            await asyncio.sleep(1)
            now = time.time()
            if now - last_update > 5 and state["total"] > 0:
                last_update = now
                percentage = state["downloaded"] * 100 / state["total"] if state["total"] else 0
                try:
                    await status_msg.edit(
                        f"📥 **Downloading...** {round(percentage, 2)}%\n"
                        f"{humanbytes(state['downloaded'])} / {humanbytes(state['total'])}"
                    )
                except Exception: pass
                
        await task  # Wait for thread to finish cleanly
        
        if state["error"]:
            logger.error(f"Download failed with error: {state['error']}")
            await status_msg.edit(f"❌ Download failed: {state['error']}")
            return False
            
        logger.info(f"Step 5: File download complete -> {file_name}")
        return True
    except Exception as e:
        logger.error(f"Step 5: File download failed -> {str(e)}", exc_info=True)
        await status_msg.edit(f"❌ Download failed: {str(e)}")
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

async def handle_hindianime(client, message, url, selection, status_msg):
    try:
        logger.info(f"Step 3: Starting HindiAnimeZone bypass for URL: {url}")
        def bypass_func():
            f = io.StringIO()
            with redirect_stdout(f):
                bypasser = HindiAnimeZone()
                bypasser.pro_main_bypass(url, selection=selection)
            return f.getvalue()
            
        output = await asyncio.to_thread(bypass_func)
        logger.info("HindiAnimeZone bypass output:\n" + output)
        
        links = []
        for line in output.split('\n'):
            if '[PRO] FINAL DL' in line:
                links.append(line.split('): ')[1].strip() if '): ' in line else line.split(': ')[1].strip())
            
        if not links:
            logger.warning("Step 4 Failed: No download links found in bypass output.")
            return await status_msg.edit("❌ Koi download links nahi mile.")
            
        logger.info(f"Step 4: Extracted {len(links)} download links.")
        await status_msg.edit(f"✅ Found {len(links)} links. Downloading and uploading...")
        
        for idx, dl_url in enumerate(links, 1):
            file_name = f"hindianime_{message.id}_{idx}.mkv"
            await status_msg.edit(f"📥 **Downloading Part {idx}/{len(links)}...**")
            
            # For HindiAnimeZone, referer can be the original page or the gate url
            success = await download_file(dl_url, file_name, status_msg, referer=url)
            if not success: continue
            
            logger.info(f"Step 6: Upload to Telegram started -> {file_name}")
            await status_msg.edit(f"📤 **Uploading Part {idx}/{len(links)}...**")
            start_time = time.time()
            
            await client.send_document(
                chat_id=message.chat.id,
                document=file_name,
                caption=f"Part {idx} via @HindiAnimeZone",
                progress=progress_for_pyrogram,
                progress_args=(f"📤 **Uploading Part {idx}...**", status_msg, start_time)
            )
            os.remove(file_name)
            logger.info(f"Step 7: Upload complete and file deleted -> {file_name}")
            
        await status_msg.delete()
        logger.info("Process completed successfully for HindiAnimeZone.")
            
    except Exception as e:
        logger.error(f"HindiAnimeZone Error: {e}", exc_info=True)
        await status_msg.edit(f"❌ HindiAnimeZone Error: {str(e)}")

async def handle_rareanime(client, message, url, selection, status_msg):
    try:
        logger.info(f"Step 3: Starting RareAnimes bypass for URL: {url}")
        # RareAnimes selection needs start/end
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
            return await status_msg.edit(f"❌ API fail: {res.get('error')}")
            
        episodes = res.get("episodes", [])
        if not episodes:
            logger.warning("Step 4 Failed: No episodes returned.")
            return await status_msg.edit("❌ Koi episodes nahi mile.")
            
        total = len(episodes)
        logger.info(f"Step 4: Found {total} episodes to process.")
        await status_msg.edit(f"✅ Found {total} episodes. Downloading and uploading...")
        
        for idx, ep in enumerate(episodes, 1):
            downloads = ep.get("downloads", [])
            if not downloads: 
                logger.warning(f"No download link for episode: {ep.get('episode', 'Unknown')}")
                continue
            
            # Loop through all available qualities
            for q_idx, dl_obj in enumerate(downloads, 1):
                dl_url = dl_obj.get('link') if isinstance(dl_obj, dict) else dl_obj
                quality_label = dl_obj.get('label', f'Q{q_idx}') if isinstance(dl_obj, dict) else f'Q{q_idx}'
                
                # Clean up the URL if needed
                if isinstance(dl_url, str) and ': http' in dl_url:
                    dl_url = 'http' + dl_url.split(': http')[1].strip()
                
                if not dl_url:
                    logger.warning(f"Empty download URL for episode: {ep.get('episode')} - Quality: {quality_label}")
                    continue
                    
                file_name = f"rareanime_{message.id}_{idx}_{quality_label}.mkv"
                logger.info(f"Step 4: Episode {idx} ({quality_label}) link extracted successfully.")
                await status_msg.edit(f"📥 **Downloading Ep {idx}/{total} [{quality_label}]...**\n{ep['episode']}")
                
                # Use metadata from RareAnimes result
                ref = ep.get("referer")
                cookies = ep.get("cookies")
                user_agent = ep.get("user_agent")
                
                success = await download_file(dl_url, file_name, status_msg, referer=ref, cookies=cookies, user_agent=user_agent)
                if not success: 
                    await status_msg.edit(f"❌ Failed to download: {ep['episode']} [{quality_label}]")
                    continue
                
                logger.info(f"Step 6: Upload to Telegram started -> {file_name}")
                await status_msg.edit(f"📤 **Uploading Ep {idx}/{total} [{quality_label}]...**")
                start_time = time.time()
                await client.send_video(
                    chat_id=message.chat.id,
                    video=file_name,
                    caption=f"{ep['episode']} [{quality_label}] via RareAnimes",
                    progress=progress_for_pyrogram,
                    progress_args=(f"📤 **Uploading Ep {idx} [{quality_label}]...**", status_msg, start_time)
                )
                
                try:
                    os.remove(file_name)
                    logger.info(f"Step 7: Upload complete and file deleted -> {file_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete file {file_name}: {e}")
            
        await status_msg.delete()
        logger.info("Process completed successfully for RareAnimes.")
            
    except Exception as e:
        logger.error(f"RareAnimes Error: {e}", exc_info=True)
        await status_msg.edit(f"❌ RareAnimes Error: {str(e)}")

if __name__ == "__main__":
    print("Starting bot...")
    app.run()
