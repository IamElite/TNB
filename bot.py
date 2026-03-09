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

async def download_file(url, file_name, status_msg):
    start_t = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as response:
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                last_update = time.time()
                
                with open(file_name, 'wb') as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        now = time.time()
                        if now - last_update > 5:
                            last_update = now
                            percentage = downloaded * 100 / total_size if total_size else 0
                            try:
                                await status_msg.edit(
                                    f"📥 **Downloading...** {round(percentage, 2)}%\n"
                                    f"{humanbytes(downloaded)} / {humanbytes(total_size)}"
                                )
                            except Exception: pass
        return True
    except Exception as e:
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
    
    status_msg = await message.reply("🔄 Analyzing link...")
    
    try:
        # Site Detection
        if "hindianimezone.com" in url:
            await handle_hindianime(client, message, url, selection, status_msg)
        elif any(x in url for x in ["rareanimes.app", "codedew.com"]):
            await handle_rareanime(client, message, url, selection, status_msg)
        else:
            await status_msg.edit("❌ Unsupported site. Sirf HindiAnimeZone aur RareAnimes supported hain.")
            
    except Exception as e:
        await status_msg.edit(f"❌ Error occurred: {str(e)}")

async def handle_hindianime(client, message, url, selection, status_msg):
    try:
        def bypass_func():
            f = io.StringIO()
            with redirect_stdout(f):
                bypasser = HindiAnimeZone()
                bypasser.pro_main_bypass(url, selection=selection)
            return f.getvalue()
            
        output = await asyncio.to_thread(bypass_func)
        
        links = []
        for line in output.split('\n'):
            if '[PRO] FINAL DL' in line:
                links.append(line.split('): ')[1].strip() if '): ' in line else line.split(': ')[1].strip())
            
        if not links:
            return await status_msg.edit("❌ Koi download links nahi mile.")
            
        await status_msg.edit(f"✅ Found {len(links)} links. Downloading and uploading...")
        
        for idx, dl_url in enumerate(links, 1):
            file_name = f"hindianime_{message.id}_{idx}.mkv"
            await status_msg.edit(f"📥 **Downloading Part {idx}/{len(links)}...**")
            
            success = await download_file(dl_url, file_name, status_msg)
            if not success: continue
            
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
            
        await status_msg.delete()
            
    except Exception as e:
        await status_msg.edit(f"❌ HindiAnimeZone Error: {str(e)}")

async def handle_rareanime(client, message, url, selection, status_msg):
    try:
        # RareAnimes selection needs start/end
        ep_start, ep_end = None, None
        if selection:
            ep_start = min(selection)
            ep_end = max(selection)
            
        def bypass_func():
            bypasser = RareAnimes()
            return bypasser.get_links(url, ep_start=ep_start, ep_end=ep_end, verbose=False)
            
        res = await asyncio.to_thread(bypass_func)
        
        if "error" in res and not res.get("episodes"):
            return await status_msg.edit(f"❌ API fail: {res.get('error')}")
            
        episodes = res.get("episodes", [])
        if not episodes:
            return await status_msg.edit("❌ Koi episodes nahi mile.")
            
        total = len(episodes)
        await status_msg.edit(f"✅ Found {total} episodes. Downloading and uploading...")
        
        for idx, ep in enumerate(episodes, 1):
            downloads = ep.get("downloads", [])
            if not downloads: continue
            
            dl_url = downloads[0]
            if isinstance(dl_url, str) and ': http' in dl_url:
                dl_url = 'http' + dl_url.split(': http')[1].strip()
            elif isinstance(dl_url, dict) and 'link' in dl_url:
                dl_url = dl_url['link']
            elif isinstance(dl_url, dict) and 'url' in dl_url:
                dl_url = dl_url['url']
                
            file_name = f"rareanime_{message.id}_{idx}.mkv"
            await status_msg.edit(f"📥 **Downloading Ep {idx}/{total}...**\n{ep['episode']}")
            
            success = await download_file(dl_url, file_name, status_msg)
            if not success: continue
            
            await status_msg.edit(f"📤 **Uploading Ep {idx}/{total}...**")
            start_time = time.time()
            
            await client.send_document(
                chat_id=message.chat.id,
                document=file_name,
                caption=f"{ep['episode']} via RareAnimes",
                progress=progress_for_pyrogram,
                progress_args=(f"📤 **Uploading Ep {idx}...**", status_msg, start_time)
            )
            os.remove(file_name)
            
        await status_msg.delete()
            
    except Exception as e:
        await status_msg.edit(f"❌ RareAnimes Error: {str(e)}")

if __name__ == "__main__":
    print("Starting bot...")
    app.run()
