# bot.py - ULTIMATE GOD LEVEL FINAL VERSION
# ============================================

import os
import re
import asyncio
import logging
import time
import signal
import sys
from datetime import datetime
from typing import Optional, Tuple, List

from pyrogram import Client as PyroClient
from pyrogram.types import Message as PyroMessage
from pyrogram.errors import (
    FloodWait, ChannelPrivate,
    PeerIdInvalid, UserNotParticipant,
)
from pyrogram.enums import MessageMediaType

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    CallbackQueryHandler
)
from telegram.constants import ParseMode

# ============================================
# ⚙️ CONFIG - APNI VALUES YAHAN PASTE KARO
# ============================================

API_ID       = 31110304
API_HASH     = "8945c9b99de5dcc82fa8520c077f5303"
SESSION_STRING = "BQHatKAAovmdkLvCXvIQn81VNKw6tYJvSIrGUV2PdxalekOdDYBc68DWBVVwvZJdn6HHDvFengpN0qr9gQZvCvlQ0PhmPWN8YRthYtaWJIFPuMUPBf-nBZnVxfoe0xVBoRVlVspQVcCBQlFGrXIGjl8VTCk1RHGkulQxM4getei-bOvKih_UyOXTVu5H3aL8A0cORKZLjVwNWxoTKWTD15vQZxa-6w1vIXErevA3t4AGDpeLd4BmrOMBLuQ19m5TTnXJs9ErUhoUPNqmwLAhxXdu_-DK8ibcoqd53aJB7RMrNxr5bAmUcLI4tuicnYEjkkkDP5ASKZONTsjxCXZVn4t2Mxz-uQAAAAF84uDeAA"
BOT_TOKEN    = "8731944731:AAGHNn-jvpHNK40V65PMqJp6aMu6A65amQE"
OWNER_ID     = 6390210782
AUTHORIZED_USERS = {OWNER_ID}

# ============================================
# LOGGER
# ============================================

logging.basicConfig(
    format   = "%(asctime)s | %(levelname)s | %(message)s",
    level    = logging.INFO,
    handlers = [logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SaveBot")

# ============================================
# PYROGRAM CLIENT
# ============================================

pyro: Optional[PyroClient] = None
pyro_started: bool = False
peer_cache: dict = {}


def make_client() -> PyroClient:
    return PyroClient(
        name           = "userbot",
        api_id         = API_ID,
        api_hash       = API_HASH,
        session_string = SESSION_STRING,
        no_updates     = True,
        in_memory      = True,
    )


async def start_pyro() -> bool:
    global pyro, pyro_started
    if pyro_started and pyro and pyro.is_connected:
        return True
    try:
        if pyro is None:
            pyro = make_client()
        await pyro.start()
        pyro_started = True
        me = await pyro.get_me()
        logger.info(f"✅ Userbot → {me.first_name} (@{me.username}) [ID:{me.id}]")
        return True
    except Exception as e:
        logger.error(f"❌ Start failed: {e}")
        pyro_started = False
        return False


async def stop_pyro():
    global pyro, pyro_started
    if not pyro_started or pyro is None:
        return
    try:
        if pyro.is_connected:
            await pyro.stop()
    except Exception as e:
        logger.warning(f"stop_pyro: {e}")
    finally:
        pyro_started = False


def is_ready() -> bool:
    return pyro_started and pyro is not None and pyro.is_connected

# ============================================
# DATABASE
# ============================================

class DB:
    def __init__(self):
        self.users:            dict = {}
        self.forward_channels: dict = {}
        self.stats = {
            "total_saved":   0,
            "total_batches": 0,
            "start_time":    datetime.now(),
        }

    def add_user(self, uid: int, uname: str = None):
        if uid not in self.users:
            self.users[uid] = {
                "username": uname,
                "joined":   datetime.now(),
                "saved":    0,
            }

    def set_forward(self, uid: int, cid: int):
        self.forward_channels[uid] = cid

    def get_forward(self, uid: int) -> Optional[int]:
        return self.forward_channels.get(uid)

    def inc(self, uid: int, n: int = 1):
        if uid in self.users:
            self.users[uid]["saved"] += n
        self.stats["total_saved"] += n


db = DB()

# ============================================
# PEER RESOLVER - UNIVERSAL
# ============================================

async def extract_bare(arg: str) -> Optional[str]:
    """
    Kisi bhi format se bare channel id ya username nikalo.

    Returns:
        str of digits  → numeric channel id (bare, no -100)
        str username   → @less username
        None           → parse nahi hua
    """
    arg = arg.strip()

    # @username
    if arg.startswith("@"):
        return arg[1:]

    # t.me/c/DIGITS → private
    m = re.search(r"t\.me/c/(\d+)", arg)
    if m:
        return m.group(1)

    # t.me/USERNAME → public
    m = re.search(r"t\.me/([a-zA-Z][a-zA-Z0-9_]{2,})", arg)
    if m:
        return m.group(1)

    # Raw numeric: -1001234567890 / -1234567890 / 1234567890
    if re.match(r"^-?\d+$", arg):
        s = arg.strip()
        if s.startswith("-100"):
            return s[4:]    # strip -100 → bare digits
        elif s.startswith("-"):
            return s[1:]    # strip - → bare digits
        else:
            return s        # already bare digits

    return None


async def get_input_peer(bare: str):
    """
    bare = raw digits string e.g. "3332521148"
    OR username string e.g. "mychannel"

    Returns resolved peer id (int) or username (str).
    Uses 4-layer fallback.
    """
    # Username → return as-is
    if not bare.isdigit():
        try:
            chat = await pyro.get_chat(bare)
            peer_cache[bare] = chat.id
            return chat.id
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            return await get_input_peer(bare)
        except Exception:
            return bare

    # Check cache
    if bare in peer_cache:
        logger.info(f"Cache hit: {bare} → {peer_cache[bare]}")
        return peer_cache[bare]

    # Build candidates
    candidates = [
        int(f"-100{bare}"),   # -1003332521148
        int(bare),            # 3332521148
        -int(bare),           # -3332521148
    ]

    # Layer 1: resolve_peer (session internal db)
    for cid in candidates:
        try:
            await pyro.resolve_peer(cid)
            peer_cache[bare] = cid
            logger.info(f"Layer1 resolve_peer: {cid}")
            return cid
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception:
            pass

    # Layer 2: get_chat
    for cid in candidates:
        try:
            chat = await pyro.get_chat(cid)
            real = chat.id
            peer_cache[bare] = real
            logger.info(f"Layer2 get_chat: {cid} → {real} [{chat.title}]")
            return real
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            return await get_input_peer(bare)
        except Exception as e:
            logger.debug(f"  get_chat({cid}): {e}")

    # Layer 3: search dialogs
    try:
        target = int(f"-100{bare}")
        async for dialog in pyro.get_dialogs():
            if dialog.chat and dialog.chat.id == target:
                peer_cache[bare] = target
                logger.info(f"Layer3 dialogs: {target}")
                return target
    except Exception as e:
        logger.warning(f"Layer3 dialogs: {e}")

    # Layer 4: fallback
    fallback = int(f"-100{bare}")
    peer_cache[bare] = fallback
    logger.warning(f"Layer4 fallback: {fallback}")
    return fallback


async def resolve_any(arg: str):
    """
    Kisi bhi format ka arg leke peer return karo.
    t.me link, @username, raw id, bare id sab handle.
    """
    bare = await extract_bare(arg)
    if bare is None:
        return None
    return await get_input_peer(bare)

# ============================================
# MESSAGE FETCHER
# ============================================

async def fetch_msg(peer, msg_id: int) -> Optional[PyroMessage]:
    """Single message fetch with 3-layer fallback."""
    # Layer 1
    try:
        msg = await pyro.get_messages(peer, msg_id)
        if msg and not msg.empty:
            return msg
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return await fetch_msg(peer, msg_id)
    except Exception as e:
        logger.warning(f"fetch_msg layer1: {e}")

    # Layer 2: list form
    try:
        msgs = await pyro.get_messages(peer, [msg_id])
        if isinstance(msgs, list):
            valid = [m for m in msgs if m and not m.empty]
            if valid:
                return valid[0]
    except Exception as e:
        logger.warning(f"fetch_msg layer2: {e}")

    # Layer 3: chat history
    try:
        async for m in pyro.get_chat_history(
            peer, limit=1, offset_id=msg_id + 1
        ):
            if m.id == msg_id:
                return m
    except Exception as e:
        logger.warning(f"fetch_msg layer3: {e}")

    return None


async def fetch_batch(peer, msg_ids: List[int]) -> List[PyroMessage]:
    """Batch message fetch."""
    try:
        result = await pyro.get_messages(peer, msg_ids)
        if isinstance(result, list):
            return [m for m in result if m and not m.empty]
        if result and not result.empty:
            return [result]
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return await fetch_batch(peer, msg_ids)
    except Exception as e:
        logger.error(f"fetch_batch: {e}")
    return []

# ============================================
# LINK PARSER
# ============================================

def parse_link(link: str) -> Optional[Tuple]:
    """
    Returns (bare_or_username, msg_id, is_private) or None
    """
    link = link.strip()
    PATTERNS = [
        (r"https?://(?:www\.)?t\.me/c/(\d+)/(\d+)",                            True),
        (r"https?://(?:www\.)?t\.me/([a-zA-Z][a-zA-Z0-9_]{2,})/(\d+)",         False),
        (r"https?://(?:www\.)?telegram\.me/c/(\d+)/(\d+)",                      True),
        (r"https?://(?:www\.)?telegram\.me/([a-zA-Z][a-zA-Z0-9_]{2,})/(\d+)",  False),
        (r"https?://(?:www\.)?telegram\.dog/c/(\d+)/(\d+)",                     True),
        (r"https?://(?:www\.)?telegram\.dog/([a-zA-Z][a-zA-Z0-9_]{2,})/(\d+)", False),
    ]
    for pat, private in PATTERNS:
        m = re.match(pat, link)
        if m:
            raw    = m.group(1)
            msg_id = int(m.group(2))
            logger.info(f"Parsed link: raw={raw!r} msg_id={msg_id} private={private}")
            return raw, msg_id, private
    return None


def validate_batch(sl: str, el: str) -> Optional[Tuple]:
    s = parse_link(sl)
    e = parse_link(el)
    if not s or not e:
        return None
    if s[0] != e[0] or s[2] != e[2]:
        return None
    s_id, e_id = s[1], e[1]
    if s_id > e_id:
        s_id, e_id = e_id, s_id
    return s[0], s[2], s_id, e_id

# ============================================
# FILE SIZE HELPER
# ============================================

def get_file_size(msg: PyroMessage) -> int:
    mt = msg.media
    try:
        if   mt == MessageMediaType.PHOTO:      return getattr(msg.photo,      "file_size", 0) or 0
        elif mt == MessageMediaType.VIDEO:      return getattr(msg.video,      "file_size", 0) or 0
        elif mt == MessageMediaType.DOCUMENT:   return getattr(msg.document,   "file_size", 0) or 0
        elif mt == MessageMediaType.AUDIO:      return getattr(msg.audio,      "file_size", 0) or 0
        elif mt == MessageMediaType.VOICE:      return getattr(msg.voice,      "file_size", 0) or 0
        elif mt == MessageMediaType.VIDEO_NOTE: return getattr(msg.video_note, "file_size", 0) or 0
        elif mt == MessageMediaType.STICKER:    return getattr(msg.sticker,    "file_size", 0) or 0
        elif mt == MessageMediaType.ANIMATION:  return getattr(msg.animation,  "file_size", 0) or 0
    except Exception:
        pass
    return 0


def fmt_size(b: int) -> str:
    if b <= 0:        return "unknown"
    if b < 1024:      return f"{b} B"
    if b < 1024**2:   return f"{b/1024:.1f} KB"
    if b < 1024**3:   return f"{b/1024**2:.1f} MB"
    return f"{b/1024**3:.2f} GB"

# ============================================
# SEND ENGINE
# All files sent via USERBOT (Pyrogram MTProto)
# → No 50MB limit (up to 2GB)
# → No "Forwarded from" tag
# → Forward set → channel | Not set → DM
# ============================================

_SEM = asyncio.Semaphore(2)


async def process_and_send(
    msg:         PyroMessage,
    bot_chat_id: int,
    fwd_channel: int = None,
) -> dict:
    """
    Target logic:
    - fwd_channel set → send to fwd_channel via userbot
    - fwd_channel not set → send to bot_chat_id via userbot
    """
    res = {
        "success": False,
        "type":    None,
        "error":   None,
        "caption": None,
        "msg_id":  getattr(msg, "id", None),
        "size":    "—",
    }

    if not msg:
        res["error"] = "Empty message"
        return res

    target = fwd_channel if fwd_channel else bot_chat_id

    async with _SEM:
        try:
            caption = msg.caption or msg.text or ""

            # ── Text ──────────────────────────────────────────────
            if not msg.media and msg.text:
                await pyro.send_message(
                    chat_id = target,
                    text    = msg.text,
                )
                res.update(
                    success = True,
                    type    = "text",
                    caption = msg.text[:200],
                    size    = f"{len(msg.text)} chars",
                )
                return res

            # ── Media ─────────────────────────────────────────────
            if msg.media:
                res = await _handle_media(msg, target, caption, res)
                return res

            res["error"] = "Unsupported message type"

        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            return await process_and_send(msg, bot_chat_id, fwd_channel)
        except Exception as e:
            res["error"] = str(e)
            logger.error(f"process_and_send [msg={getattr(msg,'id','?')}]: {e}")

    return res


async def _handle_media(msg, target, caption, res):
    os.makedirs("downloads", exist_ok=True)
    fp = tp = None

    try:
        mt = msg.media
        declared = get_file_size(msg)
        logger.info(
            f"Downloading msg {msg.id} | "
            f"type={mt} | size={fmt_size(declared)}"
        )

        # ── Download via userbot ──────────────────────────────────
        fp = await pyro.download_media(
            msg,
            file_name = f"downloads/{msg.id}_"
        )
        if not fp or not os.path.exists(fp):
            res["error"] = "Download failed - file not found"
            return res

        actual = os.path.getsize(fp)
        res["size"] = fmt_size(actual)
        logger.info(f"Downloaded: {os.path.basename(fp)} ({fmt_size(actual)})")

        # ── Thumbnail for video ───────────────────────────────────
        if mt == MessageMediaType.VIDEO:
            if msg.video and getattr(msg.video, "thumbs", None):
                try:
                    tp = await pyro.download_media(
                        msg.video.thumbs[0].file_id,
                        file_name = f"downloads/th_{msg.id}_"
                    )
                except Exception:
                    tp = None

        # ── Send via userbot (no size limit, no forward tag) ──────
        await _userbot_send(target, fp, tp, mt, msg, caption)

        mt_name = mt.name.lower() if mt else "unknown"
        res.update(
            success = True,
            type    = mt_name,
            caption = caption[:200] if caption else "—",
        )

    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        raise
    except Exception as e:
        res["error"] = str(e)
        logger.error(f"_handle_media: {e}")
    finally:
        for p in (fp, tp):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    return res


async def _userbot_send(chat_id, fp, tp, mt, msg, caption):
    """
    Pyrogram MTProto se send karo.
    ✅ No size limit (up to 2GB)
    ✅ No 'Forwarded from' tag
    ✅ Clean messages
    """
    cap = caption if caption else None
    kw  = dict(
        chat_id          = chat_id,
        caption          = cap,
        caption_entities = getattr(msg, "caption_entities", None),
    )

    if mt == MessageMediaType.PHOTO:
        await pyro.send_photo(photo=fp, **kw)

    elif mt == MessageMediaType.VIDEO:
        await pyro.send_video(
            video              = fp,
            thumb              = tp,
            duration           = getattr(msg.video,  "duration", 0) if msg.video  else 0,
            width              = getattr(msg.video,  "width",    0) if msg.video  else 0,
            height             = getattr(msg.video,  "height",   0) if msg.video  else 0,
            supports_streaming = True,
            **kw,
        )

    elif mt == MessageMediaType.DOCUMENT:
        await pyro.send_document(
            document  = fp,
            file_name = getattr(msg.document, "file_name", None) if msg.document else None,
            **kw,
        )

    elif mt == MessageMediaType.AUDIO:
        await pyro.send_audio(
            audio     = fp,
            duration  = getattr(msg.audio, "duration",  0)   if msg.audio else 0,
            performer = getattr(msg.audio, "performer", None) if msg.audio else None,
            title     = getattr(msg.audio, "title",     None) if msg.audio else None,
            **kw,
        )

    elif mt == MessageMediaType.VOICE:
        await pyro.send_voice(
            voice    = fp,
            duration = getattr(msg.voice, "duration", 0) if msg.voice else 0,
            **kw,
        )

    elif mt == MessageMediaType.VIDEO_NOTE:
        await pyro.send_video_note(
            chat_id    = chat_id,
            video_note = fp,
            duration   = getattr(msg.video_note, "duration", 0) if msg.video_note else 0,
            length     = getattr(msg.video_note, "length",   0) if msg.video_note else 0,
        )

    elif mt == MessageMediaType.STICKER:
        await pyro.send_sticker(
            chat_id = chat_id,
            sticker = fp,
        )

    elif mt == MessageMediaType.ANIMATION:
        await pyro.send_animation(animation=fp, **kw)

    else:
        # Fallback: send as document
        await pyro.send_document(document=fp, **kw)

# ============================================
# HELPERS
# ============================================

def auth(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in AUTHORIZED_USERS:
            await update.message.reply_text(
                f"⛔ *Unauthorized*\nID: `{update.effective_user.id}`",
                parse_mode = ParseMode.MARKDOWN,
            )
            return
        return await func(update, context)
    return wrapper


def pbar(cur: int, tot: int, w: int = 20) -> str:
    if tot == 0: return "░" * w
    f = int(w * cur / tot)
    return f"[{'█'*f}{'░'*(w-f)}] {cur/tot*100:.1f}%"


async def sedit(msg, text: str):
    try:
        await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        if "not modified" not in str(e).lower():
            logger.warning(f"sedit: {e}")

# ============================================
# /start
# ============================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.add_user(user.id, user.username)
    ub  = "🟢 Online" if is_ready() else "🔴 Offline"
    fwd = db.get_forward(user.id)
    kb  = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Help",     callback_data="help"),
            InlineKeyboardButton("📊 Stats",    callback_data="stats"),
        ],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
    ])
    await update.message.reply_text(
        f"🔥 *Restricted Content Saver*\n\n"
        f"Userbot : {ub}\n"
        f"Forward : {f'`{fwd}`' if fwd else 'Not set → DM mein'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📎 `/test <link>`\n"
        f"📦 `/batch <start> <end>`\n"
        f"📢 `/forward <channel>`\n"
        f"🔄 `/reconnect`\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ No size limit (2GB tak)\n"
        f"✅ No 'Forwarded from' tag\n"
        f"✅ Private channels supported\n"
        f"✅ All media types\n\n"
        f"🆔 ID: `{user.id}`",
        parse_mode   = ParseMode.MARKDOWN,
        reply_markup = kb,
    )

# ============================================
# /help
# ============================================

@auth
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Commands*\n\n"
        "📎 `/test <link>` – Single message save\n"
        "📦 `/batch <start> <end>` – Range save\n"
        "📢 `/forward <channel>` – Forward channel set\n"
        "🔄 `/reconnect` – Userbot restart\n"
        "📊 `/stats` – Statistics\n\n"
        "*Kahan jayega content:*\n"
        "• Forward set → channel mein bheja\n"
        "• Forward nahi → Bot DM mein\n\n"
        "*Forward channel formats:*\n"
        "• `/forward @username`\n"
        "• `/forward -1001234567890`\n"
        "• `/forward https://t.me/channel`\n"
        "• `/forward https://t.me/c/123/1`\n\n"
        "*Source link formats:*\n"
        "• `https://t.me/c/CHATID/MSGID`\n"
        "• `https://t.me/username/MSGID`\n\n"
        "*Note:*\n"
        "Session account channel ka member hona chahiye.",
        parse_mode = ParseMode.MARKDOWN,
    )

# ============================================
# /stats
# ============================================

@auth
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    up   = datetime.now() - db.stats["start_time"]
    h, r = divmod(int(up.total_seconds()), 3600)
    m, s = divmod(r, 60)
    ub   = "🟢 Online" if is_ready() else "🔴 Offline"
    uid  = update.effective_user.id
    fwd  = db.get_forward(uid)
    await update.message.reply_text(
        f"📊 *Statistics*\n\n"
        f"Userbot      : {ub}\n"
        f"Forward      : {f'`{fwd}`' if fwd else 'Not set'}\n"
        f"Cached peers : `{len(peer_cache)}`\n"
        f"👥 Users     : `{len(db.users)}`\n"
        f"💾 Saved     : `{db.stats['total_saved']}`\n"
        f"📦 Batches   : `{db.stats['total_batches']}`\n"
        f"⏱ Uptime    : `{h}h {m}m {s}s`",
        parse_mode = ParseMode.MARKDOWN,
    )

# ============================================
# /reconnect
# ============================================

@auth
async def cmd_reconnect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global pyro, peer_cache
    msg = await update.message.reply_text("🔄 Reconnect ho raha hai …")
    await stop_pyro()
    peer_cache = {}
    pyro       = make_client()
    ok         = await start_pyro()
    if ok:
        await sedit(msg, "✅ *Userbot reconnect ho gaya!*")
    else:
        await sedit(
            msg,
            "❌ *Reconnect fail.*\n\n"
            "`SESSION_STRING` check karo `bot.py` mein.",
        )

# ============================================
# /forward - FULLY FIXED
# ============================================

@auth
async def cmd_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not context.args:
        cur = db.get_forward(uid)
        await update.message.reply_text(
            f"📢 *Forward Channel Set Karo*\n\n"
            f"Userbot ko channel mein member/admin hona chahiye.\n\n"
            f"*Supported formats:*\n"
            f"• `/forward @username`\n"
            f"• `/forward -1001234567890`\n"
            f"• `/forward 1234567890`\n"
            f"• `/forward https://t.me/channel`\n"
            f"• `/forward https://t.me/c/1234567890/1`\n"
            f"• `/forward off` – hatao\n\n"
            f"Abhi: {f'`{cur}`' if cur else 'Not set (DM mein jayega)'}",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    arg = context.args[0].strip()

    if arg.lower() in ("off", "remove", "none", "clear", "0"):
        db.forward_channels.pop(uid, None)
        await update.message.reply_text(
            "✅ Forward hataya.\nAb sab bot DM mein jayega."
        )
        return

    if not is_ready():
        await update.message.reply_text(
            "❌ Userbot offline. /reconnect karo pehle."
        )
        return

    st = await update.message.reply_text(
        f"🔄 Resolving `{arg}` …",
        parse_mode = ParseMode.MARKDOWN,
    )

    try:
        # Universal resolve - handles ALL formats
        resolved = await resolve_any(arg)

        if resolved is None:
            await sedit(
                st,
                "❌ *Format samajh nahi aaya.*\n\n"
                "Try:\n"
                "• `/forward @username`\n"
                "• `/forward -1001234567890`\n"
                "• `/forward https://t.me/c/123/1`",
            )
            return

        await sedit(st, f"🔄 Resolved → `{resolved}`\nVerify ho raha hai …")

        # Get chat info
        chat = await pyro.get_chat(resolved)

        # Test send karo verify karne ke liye
        test_msg = await pyro.send_message(
            resolved,
            "✅ Userbot verified! Deleting …"
        )
        await test_msg.delete()

        db.set_forward(uid, resolved)

        await sedit(
            st,
            f"✅ *Forward channel set!*\n\n"
            f"📢 {chat.title}\n"
            f"🆔 `{resolved}`\n\n"
            f"Ab saari files yahan jayengi.\n"
            f"✅ No 'Forwarded from' tag\n"
            f"✅ No size limit",
        )

    except PeerIdInvalid:
        await sedit(
            st,
            "❌ *Peer ID Invalid*\n\n"
            "Userbot is channel ka member nahi hai.\n\n"
            "Fix:\n"
            "1. Session account se channel open karo\n"
            "2. /reconnect karo\n"
            "3. Dobara try karo",
        )
    except Exception as e:
        err = str(e)
        if "CHAT_WRITE_FORBIDDEN" in err or "right" in err.lower():
            await sedit(
                st,
                "❌ *Write permission nahi hai.*\n\n"
                "Userbot ko channel mein post karne ka\n"
                "permission/admin rights chahiye.",
            )
        elif "not found" in err.lower() or "USERNAME_NOT_OCCUPIED" in err:
            await sedit(
                st,
                "❌ *Channel nahi mila.*\n\n"
                "Userbot channel ka member hona chahiye.",
            )
        else:
            await sedit(st, f"❌ Error: `{e}`")

# ============================================
# /test
# ============================================

@auth
async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    bot_chat = update.effective_chat.id

    if not is_ready():
        await update.message.reply_text(
            "❌ Userbot offline. /reconnect karo."
        )
        return

    if not context.args:
        await update.message.reply_text(
            "📎 *Single Message Save*\n\n"
            "Usage: `/test <link>`\n\n"
            "*Private:*\n"
            "`/test https://t.me/c/3124399297/8`\n\n"
            "*Public:*\n"
            "`/test https://t.me/username/8`",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    parsed = parse_link(context.args[0])
    if not parsed:
        await update.message.reply_text(
            "❌ *Invalid link.*\n\n"
            "Format:\n"
            "• `https://t.me/c/CHATID/MSGID`\n"
            "• `https://t.me/username/MSGID`",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    raw, msg_id, is_private = parsed
    fwd         = db.get_forward(uid)
    target_name = f"Channel `{fwd}`" if fwd else "Bot DM"

    st = await update.message.reply_text(
        f"🔄 *Processing …*\n\n"
        f"Raw   : `{raw}`\n"
        f"Msg   : `{msg_id}`\n"
        f"Target: `{target_name}`",
        parse_mode = ParseMode.MARKDOWN,
    )

    try:
        # Resolve peer
        peer = await get_input_peer(raw) if is_private else raw
        await sedit(
            st,
            f"🔄 *Peer resolved* → `{peer}`\n"
            f"Message fetch ho raha hai …"
        )

        # Fetch message
        msg = await fetch_msg(peer, msg_id)
        if not msg:
            await sedit(
                st,
                "❌ *Message nahi mila.*\n\n"
                "• Session account channel ka member hona chahiye\n"
                "• Message delete hua hoga\n\n"
                "Agar member nahi: session account se\n"
                "channel open karo, phir /reconnect karo.",
            )
            return

        declared = get_file_size(msg)
        mt       = msg.media.name if msg.media else "text"

        await sedit(
            st,
            f"⬇️ *Downloading …*\n\n"
            f"Type  : `{mt}`\n"
            f"Size  : `{fmt_size(declared)}`\n"
            f"Target: `{target_name}`"
        )

        # Process and send
        res = await process_and_send(msg, bot_chat, fwd)

        if res["success"]:
            db.inc(uid)
            await sedit(
                st,
                f"✅ *Saved!*\n\n"
                f"Type    : `{res['type']}`\n"
                f"Size    : `{res['size']}`\n"
                f"Msg ID  : `{msg_id}`\n"
                f"Sent to : `{target_name}`\n"
                f"Caption : {res['caption'] or '—'}",
            )
        else:
            await sedit(st, f"❌ *Fail hua*\n\n`{res['error']}`")

    except PeerIdInvalid:
        await sedit(
            st,
            "❌ *Peer ID Invalid*\n\n"
            "Session account se channel open karo,\n"
            "/reconnect karo, phir retry karo.",
        )
    except UserNotParticipant:
        await sedit(st, "❌ Session account channel mein join karo.")
    except ChannelPrivate:
        await sedit(st, "❌ Private channel. Session account se join karo.")
    except Exception as e:
        await sedit(st, f"❌ `{e}`")

# ============================================
# /batch
# ============================================

@auth
async def cmd_batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not is_ready():
        await update.message.reply_text(
            "❌ Userbot offline. /reconnect karo."
        )
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "📦 *Batch Save*\n\n"
            "Usage: `/batch <start_link> <end_link>`\n\n"
            "*Example:*\n"
            "`/batch https://t.me/c/123/1 https://t.me/c/123/50`\n\n"
            "⚠️ Max 1000 msgs per batch\n"
            "⚠️ Dono links same channel ke hon",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    info = validate_batch(context.args[0], context.args[1])
    if not info:
        await update.message.reply_text(
            "❌ *Invalid links.*\n\n"
            "• Dono same channel ke hone chahiye\n"
            "• Valid Telegram message links hon",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    raw, is_private, s_id, e_id = info
    total       = e_id - s_id + 1
    fwd         = db.get_forward(uid)
    target_name = f"Channel `{fwd}`" if fwd else "Bot DM"

    if total > 1000:
        await update.message.reply_text(
            f"❌ *{total} messages* – max 1000 per batch."
        )
        return

    bot_chat = update.effective_chat.id
    token    = f"{uid}_{int(time.time())}"

    context.bot_data[token] = {
        "raw":        raw,
        "is_private": is_private,
        "s_id":       s_id,
        "e_id":       e_id,
        "bot_chat":   bot_chat,
        "fwd":        fwd,
    }

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Start Karo", callback_data=f"BATCH:{token}"),
        InlineKeyboardButton("❌ Cancel",     callback_data="CANCEL"),
    ]])

    await update.message.reply_text(
        f"📦 *Batch Confirm Karo*\n\n"
        f"Channel : `{raw}`\n"
        f"Range   : `{s_id}` → `{e_id}`\n"
        f"Total   : `{total}` messages\n"
        f"Target  : `{target_name}`\n"
        f"Est.    : ~{total * 2}s\n\n"
        f"✅ Large files bhi handle honge",
        parse_mode   = ParseMode.MARKDOWN,
        reply_markup = kb,
    )

# ============================================
# BATCH RUNNER
# ============================================

async def run_batch(
    q, context, uid,
    raw, is_private,
    s_id, e_id,
    bot_chat, fwd
):
    if not is_ready():
        await q.edit_message_text("❌ Userbot offline. /reconnect karo.")
        return

    total = e_id - s_id + 1

    # Peer resolve karo pehle
    await sedit(q.message, "🔄 *Channel peer resolve ho raha hai …*")

    peer = await get_input_peer(raw) if is_private else raw

    fwd_txt = f"Channel `{fwd}`" if fwd else "Bot DM"

    await q.edit_message_text(
        f"🚀 *Batch Shuru!*\n\n"
        f"Peer   : `{peer}`\n"
        f"Total  : `{total}`\n"
        f"Target : `{fwd_txt}`\n"
        f"{pbar(0, total)}\n"
        f"✅ 0  ❌ 0  ⏭ 0",
        parse_mode = ParseMode.MARKDOWN,
    )

    saved     = 0
    failed    = 0
    skipped   = 0
    t0        = time.time()
    last_edit = 0.0
    CHUNK     = 10

    for cs in range(s_id, e_id + 1, CHUNK):
        ce   = min(cs + CHUNK - 1, e_id)
        ids  = list(range(cs, ce + 1))

        try:
            msgs = await fetch_batch(peer, ids)
        except Exception as e:
            logger.error(f"Chunk {cs}-{ce}: {e}")
            failed += len(ids)
            continue

        fetched  = {m.id for m in msgs}
        skipped += sum(1 for i in ids if i not in fetched)

        for msg in msgs:
            try:
                r = await process_and_send(msg, bot_chat, fwd)
                if r["success"]:
                    saved += 1
                else:
                    failed += 1
                    logger.warning(f"Msg {msg.id} fail: {r['error']}")
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    r = await process_and_send(msg, bot_chat, fwd)
                    if r["success"]: saved  += 1
                    else:            failed += 1
                except Exception:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"Msg {msg.id}: {e}")

            await asyncio.sleep(1.5)

        # Progress update (har 5 second)
        now = time.time()
        if now - last_edit >= 5:
            last_edit = now
            done    = saved + failed + skipped
            elapsed = now - t0
            speed   = done / elapsed if elapsed else 0
            eta     = (total - done) / speed if speed else 0
            try:
                await q.edit_message_text(
                    f"🔄 *Process ho raha hai …*\n\n"
                    f"{pbar(done, total)}\n\n"
                    f"✅ {saved}  ❌ {failed}  ⏭ {skipped}\n\n"
                    f"⚡ {speed:.1f} msg/s  ⏱ ETA {int(eta)}s",
                    parse_mode = ParseMode.MARKDOWN,
                )
            except Exception:
                pass

    elapsed = max(time.time() - t0, 0.1)
    db.inc(uid, saved)
    db.stats["total_batches"] += 1

    try:
        await q.edit_message_text(
            f"🎉 *Batch Complete!*\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Saved   : *{saved}*\n"
            f"❌ Failed  : *{failed}*\n"
            f"⏭ Skipped : *{skipped}*\n"
            f"📦 Total   : *{total}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ {int(elapsed)}s  "
            f"⚡ {total/elapsed:.1f} msg/s",
            parse_mode = ParseMode.MARKDOWN,
        )
    except Exception:
        pass

# ============================================
# CALLBACK HANDLER
# ============================================

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    data = q.data
    await q.answer()

    BACK = [[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]]

    # ── Cancel ────────────────────────────────────────────────────
    if data == "CANCEL":
        await q.edit_message_text("❌ Cancel ho gaya.")
        return

    # ── Batch start ───────────────────────────────────────────────
    if data.startswith("BATCH:"):
        token  = data[6:]
        params = context.bot_data.pop(token, None)
        if not params:
            await q.edit_message_text(
                "❌ Batch expire ho gaya.\n"
                "/batch dobara bhejo."
            )
            return
        await run_batch(
            q, context,
            q.from_user.id,
            params["raw"],
            params["is_private"],
            params["s_id"],
            params["e_id"],
            params["bot_chat"],
            params["fwd"],
        )
        return

    # ── Help ──────────────────────────────────────────────────────
    if data == "help":
        await q.edit_message_text(
            "📖 *Help*\n\n"
            "📎 `/test <link>` – Single message\n"
            "📦 `/batch <s> <e>` – Range save\n"
            "📢 `/forward <ch>` – Forward channel\n"
            "🔄 `/reconnect` – Userbot restart\n"
            "📊 `/stats` – Statistics\n\n"
            "*Source link formats:*\n"
            "• `t.me/c/CHATID/MSGID` (private)\n"
            "• `t.me/USERNAME/MSGID` (public)\n\n"
            "*Forward formats:*\n"
            "• `@username`\n"
            "• `-1001234567890`\n"
            "• `https://t.me/channel`\n"
            "• `https://t.me/c/123/1`\n\n"
            "*Note:* Session account must be\n"
            "member of source channel.",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = InlineKeyboardMarkup(BACK),
        )

    # ── Stats ─────────────────────────────────────────────────────
    elif data == "stats":
        up   = datetime.now() - db.stats["start_time"]
        h, r = divmod(int(up.total_seconds()), 3600)
        m, s = divmod(r, 60)
        ub   = "🟢 Online" if is_ready() else "🔴 Offline"
        uid  = q.from_user.id
        fwd  = db.get_forward(uid)
        await q.edit_message_text(
            f"📊 *Statistics*\n\n"
            f"Userbot      : {ub}\n"
            f"Forward      : {f'`{fwd}`' if fwd else 'Not set'}\n"
            f"Cached peers : `{len(peer_cache)}`\n"
            f"👥 Users     : `{len(db.users)}`\n"
            f"💾 Saved     : `{db.stats['total_saved']}`\n"
            f"📦 Batches   : `{db.stats['total_batches']}`\n"
            f"⏱ Uptime    : `{h}h {m}m {s}s`",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = InlineKeyboardMarkup(BACK),
        )

    # ── Settings ──────────────────────────────────────────────────
    elif data == "settings":
        uid = q.from_user.id
        fwd = db.get_forward(uid)
        await q.edit_message_text(
            f"⚙️ *Settings*\n\n"
            f"📢 Forward : {f'`{fwd}`' if fwd else 'Not set (DM mein)'}\n"
            f"🆔 ID      : `{uid}`\n\n"
            f"`/forward <channel>` – set karo\n"
            f"`/forward off` – hatao",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = InlineKeyboardMarkup(BACK),
        )

    # ── Back ──────────────────────────────────────────────────────
    elif data == "menu_back":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📖 Help",     callback_data="help"),
                InlineKeyboardButton("📊 Stats",    callback_data="stats"),
            ],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        ])
        await q.edit_message_text(
            "🔥 *Restricted Content Saver*\n\nOption chuno:",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = kb,
        )

# ============================================
# MAIN
# ============================================

async def run():
    os.makedirs("downloads", exist_ok=True)

    logger.info("Userbot start ho raha hai …")
    ok = await start_pyro()
    if not ok:
        logger.warning("⚠️ Userbot fail – /reconnect karo")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("stats",     cmd_stats))
    app.add_handler(CommandHandler("reconnect", cmd_reconnect))
    app.add_handler(CommandHandler("forward",   cmd_forward))
    app.add_handler(CommandHandler("test",      cmd_test))
    app.add_handler(CommandHandler("batch",     cmd_batch))
    app.add_handler(CallbackQueryHandler(on_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        allowed_updates      = Update.ALL_TYPES,
        drop_pending_updates = True,
    )
    logger.info("🚀 Bot chal raha hai!")

    stop_event = asyncio.Event()

    def _sig(*_):
        logger.info("Signal → band ho raha hai …")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig)
        except (NotImplementedError, RuntimeError):
            pass

    await stop_event.wait()

    logger.info("Stopping …")
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    await stop_pyro()
    logger.info("✅ Band ho gaya.")


if __name__ == "__main__":
    asyncio.run(run())