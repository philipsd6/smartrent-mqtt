# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "python-telegram-bot",
#   "rich",
#   "smartrent-py",
#   "structlog",
# ]
# ///

import asyncio
import json
import logging
import os
import time

from smartrent import async_login
import structlog
from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# ------------------- Config -------------------

BITWARDEN_ID = os.environ["BITWARDEN_ID"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ADMIN_USER_IDS = set(os.environ.get("ADMIN_USERS_IDS", []))
AUTHORIZED_FILE = os.environ.get("AUTHORIZED_FILE", "authorized_users.json")

MOTION_DEBOUNCE_SECONDS = 15  # suppress repeated motion alerts within this range
_motion_lock = asyncio.Lock()

ECO_DEFAULT_TEMP = 74
ECO_REVERT_DELAY = 120  # 2 minutes
_ecomode_active = False
_ecomode_temp = ECO_DEFAULT_TEMP
_last_override_time = 0
_ecomode_task = None
_ecomode_lock = asyncio.Lock()

# ------------------- Logging Setup -------------------

level = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, level)

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(LOG_LEVEL))
logger = structlog.get_logger()

# ------------------- Persistent Store -------------------
try:
    with open(AUTHORIZED_FILE, "r") as f:
        AUTHORIZED_USERS = json.load(f)  # user_id(str) -> {username, full_name}
except:
    AUTHORIZED_USERS = {}


def save_authorized_users():
    with open(AUTHORIZED_FILE, "w") as f:
        json.dump(AUTHORIZED_USERS, f, indent=2)


# ------------------- User Context -------------------
def get_user_logger(update: Update):
    """
    Bind logger with user/chat info. Returns (logger, is_authorized, is_admin)
    """
    user = update.effective_user
    chat_id = update.effective_chat.id
    user_logger = logger.bind(
        user_id=user.id,
        chat_id=chat_id,
        username=user.username,
        full_name=user.full_name,
    )
    is_admin = str(user.id) in ADMIN_USER_IDS or AUTHORIZED_USERS.get(
        str(user.id), {}
    ).get("is_admin")
    is_authorized = is_admin or str(user.id) in AUTHORIZED_USERS
    return user_logger, is_authorized, is_admin

async def get_smartrent_auth():
    logger.debug("Retrieving username and password")
    proc = await asyncio.create_subprocess_shell(
        f"bw get item {BITWARDEN_ID}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"Failed to get username and password: {stderr.decode().strip()}"
        )
    item = json.loads(stdout.decode())
    auth = {"email": item["login"]["username"], "password": item["login"]["password"]}

    logger.debug("Retrieving 2FA code")
    proc = await asyncio.create_subprocess_shell(
        f"bw get totp {BITWARDEN_ID}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to get 2FA code: {stderr.decode().strip()}")
    auth["tfa_token"] = stdout.decode().strip()
    return auth


async def main():
    auth = await get_smartrent_auth()
    logger.debug("Logging into SmartRent")
    api = await async_login(**auth)

    # Get the lock...
    lock = api.get_locks()[0]
    logger.debug(
        "Found lock",
        name=lock.get_name(),
        online=lock.get_online(),
        battery_level=lock.get_battery_level(),
        locked=lock.get_locked(),
    )

    # Get the thermostat
    thermostat = api.get_thermostats()[0]
    logger.debug(
        "Found thermostat",
        name=thermostat.get_name(),
        online=thermostat.get_online(),
        mode=thermostat.get_mode(),
        state=thermostat.get_operating_state(),
        pct_humidity=thermostat.get_current_humidity(),
        temperature=thermostat.get_current_temp(),
        cooling_setpoint=thermostat.get_cooling_setpoint(),
        heating_setpoint=thermostat.get_heating_setpoint(),
    )

    # Get the motion detector
    motion = api.get_motion_sensors()[0]
    logger.debug(
        "Found motion sensor",
        name=motion.get_name(),
        online=motion.get_online(),
        active=motion.get_active(),
    )

    lock.start_updater()
    thermostat.start_updater()
    motion.start_updater()

    # ----------------- Telegram Commands ----------------
    async def check_auth(update: Update):
        logger, is_authorized, is_admin = get_user_logger(update)
        if not is_authorized:
            logger.warning("Unauthorized access", action="command_attempt")
            await update.message.reply_text(
                "❌ You are not authorized to use this bot."
            )
            return None, None
        return logger, is_authorized, is_admin

    async def check_admin(update: Update):
        logger, is_authorized, is_admin = get_user_logger(update)
        if not is_admin:
            logger.warning("Unauthorized access", action="command_attempt")
            await update.message.reply_text("❌ Admin privileges required.")
            return None, None
        return logger, is_authorized, is_admin

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_authorized:
            return
        msg = (
            "👋 Welcome to your SmartRent Control Bot!\n\n"
            "Available commands:\n"
            "/status — Check lock & thermostat status\n"
            "/lock — Lock the door\n"
            "/unlock — Unlock the door\n"
            "/settemp <°F> — Set thermostat temperature\n"
        )
        await update.message.reply_text(msg)
        logger.info("Start")

    async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_authorized:
            return
        lock_state = "Locked" if lock.get_locked() else "Unlocked"
        temp = thermostat.get_current_temp()
        set_temp = thermostat.get_cooling_setpoint()
        # This might be a good use for template strings!!!
        await update.message.reply_text(
            f"🔐 Lock: {lock_state}\n🌡️ Temp: {temp}°F (Set to {set_temp}°F)"
        )
        logger.info("Status", lock_state=lock_state, temp=temp, set_temp=set_temp)

    async def lock_door(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_authorized:
            return
        logger.debug(
            "Locking door...", lock_state="Locked" if lock.get_locked() else "Unlocked"
        )
        await update.message.reply_text("Locking door...")
        await lock.async_set_locked(True)
        await update.message.reply_text("Door locked ✅")
        logger.info(
            "Door locked", lock_state="Locked" if lock.get_locked() else "Unlocked"
        )

    async def unlock_door(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_authorized:
            return
        logger.debug(
            "Unlocking door...",
            lock_state="Locked" if lock.get_locked() else "Unlocked",
        )
        await update.message.reply_text("Unlocking door...")
        await lock.async_set_locked(False)
        await update.message.reply_text("Door unlocked 🔓")
        logger.info(
            "Door unlocked", lock_state="Locked" if lock.get_locked() else "Unlocked"
        )

    async def set_temp(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_authorized:
            return
        if not context.args:
            logger.warning("Missing temperature argument")
            await update.message.reply_text("Usage: /settemp <temperature>")
            return
        try:
            new_temp = float(context.args[0])
            await thermostat.async_set_cooling_setpoint(new_temp)
            await update.message.reply_text(f"Temperature set to {new_temp}°F 🌡️")
            logger.info("Temperature set", setpoint=new_temp)
        except ValueError:
            logger.error("Invalid temperature", value=context.args[0])
            await update.message.reply_text("Invalid number format.")

    async def watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_admin:
            return
        chat_id = update.effective_chat.id
        AUTHORIZED_USERS[str(chat_id)]["is_watching"] = True
        save_authorized_users()
        await update.message.reply_text("You’ll now receive live SmartRent updates 📡")
        logger.info("Started watching", chat_id=chat_id, is_watching=True)

    async def unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_admin:
            return
        chat_id = update.effective_chat.id
        AUTHORIZED_USERS[str(chat_id)]["is_watching"] = False
        save_authorized_users()
        await update.message.reply_text("You’ve stopped receiving SmartRent updates 🚫")
        logger.info("Stopped watching", chat_id=chat_id, is_watching=False)

    async def adduser(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_admin:
            return
        if not context.args:
            logger.warning("Missing user ID argument")
            await update.message.reply_text("Usage: /adduser <user_id>")
            return
        try:
            target_id = str(int(context.args[0]))
            target = await app.bot.get_chat(target_id)
            AUTHORIZED_USERS.setdefault(target_id, {}).update(
                {
                    "username": target.username or "",
                    "full_name": target.full_name,
                    "added_by": update.effective_user.id,
                }
            )
            save_authorized_users()
            await update.message.reply_text(
                f"✅ User {target_id} added. Name: {target.full_name}, Username: {target.username}"
            )
            logger.info(
                "User added",
                user_id=target.id,
                username=target.username,
                full_name=target.full_name,
            )
        except ValueError:
            logger.error("Invalid user ID", value=context.args[0])
            await update.message.reply_text("Invalid user ID.")

    async def listusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_admin:
            return
        if not AUTHORIZED_USERS:
            await update.message.reply_text("No authorized users.")
            return
        msg = "**Authorized users:**\n"
        for uid, info in AUTHORIZED_USERS.items():
            msg += f"- ID: {uid}, Name: {info['full_name']}, Username: {info['username']}, Is Admin: {info.get('is_admin')}\n"
        await update.message.reply_text(msg)
        logger.info("Users listed", users=[uid for uid in AUTHORIZED_USERS])

    async def ecomode(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enable eco mode enforcement."""
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_admin:
            return

        # Parse optional target temperature
        if context.args:
            try:
                ecomode_temp = int(context.args[0])
            except ValueError:
                logger.error("Invalid ecomode temperature", value=context.args[0])
                await update.message.reply_text(
                    "Invalid temperature. Example: /ecomode 74"
                )
                return
        else:
            ecomode_temp = ECO_DEFAULT_TEMP

        async with _ecomode_lock:
            if getattr(ecomode, "active", False) and getattr(ecomode, "temp") == ecomode_temp:
                await update.message.reply_text(
                    f"🌿 Eco mode already active at {ecomode_temp}°F."
                )
                return

            ecomode.active = True
            ecomode.temp = ecomode_temp
            logger.info("Eco mode enabled", eco_temp=ecomode_temp)
            await update.message.reply_text(
                f"🌿 Eco mode enabled at {ecomode_temp}°F."
            )

    async def noecomode(update, context):
        """Disable eco mode enforcement."""
        logger, is_authorized, is_admin = await check_auth(update)
        if not is_admin:
            return

        global _ecomode_active, _ecomode_task

        async with _ecomode_lock:
            if not getattr(ecomode, "active"):
                await update.message.reply_text("Eco mode is not active.")
                return

            ecomode.active = False
            if _ecomode_task and not _ecomode_task.done():
                _ecomode_task.cancel()
            _ecomode_task = None

            logger.info("Eco mode disabled")
            await update.message.reply_text("Eco mode disabled.")

    # --- Telegram bot setup ---
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).concurrent_updates(True).build()

    async def register_commands(app):
        commands = [
            BotCommand("start", "Welcome and show available commands"),
            BotCommand("status", "Check lock & thermostat status"),
            BotCommand("lock", "Lock the door"),
            BotCommand("unlock", "Unlock the door"),
            BotCommand("settemp", "Set thermostat temperature: /settemp <°F>"),
        ]
        await app.bot.set_my_commands(commands)

    # Register commands so Telegram clients know about them
    await register_commands(app)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("lock", lock_door))
    app.add_handler(CommandHandler("unlock", unlock_door))
    app.add_handler(CommandHandler("settemp", set_temp))
    app.add_handler(CommandHandler("watch", watch))
    app.add_handler(CommandHandler("unwatch", unwatch))
    app.add_handler(CommandHandler("adduser", adduser))
    app.add_handler(CommandHandler("listusers", listusers))
    app.add_handler(CommandHandler("ecomode", ecomode))
    app.add_handler(CommandHandler("noecomode", noecomode))

    # Ensure our admin users are looked up and persisted in the AUTHORIZED_USERS file
    for admin_id in ADMIN_USER_IDS:
        if admin_id not in AUTHORIZED_USERS:
            user = await app.bot.get_chat(admin_id)
            AUTHORIZED_USERS.setdefault(admin_id, {}).update(
                {
                    "username": user.username or "",
                    "full_name": user.full_name,
                    "is_admin": True,
                }
            )
    save_authorized_users()

    # --- SmartRent Event Callbacks ---
    async def notify_watchers(msg: str):
        for chat_id in [
            uid for uid, info in AUTHORIZED_USERS.items() if info.get("is_watching")
        ]:
            try:
                await app.bot.send_message(chat_id, msg)
            except Exception:
                # In case user blocked bot or chat no longer valid
                AUTHORIZED_USERS[chat_id]["is_watching"] = False

    async def on_lock_evt():
        state = "🔐 Locked" if lock.get_locked() else "🔓 Unlocked"
        logger.info("Notifying watchers", action="lock_evt", state=state)
        await notify_watchers(f"Lock status changed: {state}")

    async def on_thermostat_evt():
        global _last_override_time, _ecomode_task

        current_setpoint = thermostat.get_cooling_setpoint()
        if hasattr(on_thermostat_evt, "last_setpoint"):
            if current_setpoint != on_thermostat_evt.last_setpoint:
                temp = thermostat.get_current_temp()
                logger.info(
                    "Notifying watchers",
                    action="thermostat_evt",
                    temp=temp,
                    set_temp=current_setpoint,
                )
                await notify_watchers(
                    f"🌡️ Thermostat updated: {temp}°F (Set {current_setpoint}°F)"
                )
        on_thermostat_evt.last_setpoint = current_setpoint

        # Eco mode enforcement...
        if not _ecomode_active:
            return

        if current_setpoint < _ecomode_temp:
            now = time.monotonic()
            _last_override_time = now

            logger.warning(
                "Eco mode override detected",
                current_setpoint=current_setpoint,
                eco_temp=_ecomode_temp,
                revert_in=ECO_REVERT_DELAY,
            )

            if _ecomode_task and not _ecomode_task.done():
                _ecomode_task.cancel()

            _ecomode_task = asyncio.create_task(_revert_eco_setpoint(now))

    async def _revert_eco_setpoint(trigger_time):
        """Wait ECO_REVERT_DELAY and restore eco temperature if still overridden."""
        await asyncio.sleep(ECO_REVERT_DELAY)
        if not _ecomode_active or trigger_time != _last_override_time:
            return

        current_setpoint = thermostat.get_cooling_setpoint()
        if current_setpoint < _ecomode_temp:
            await thermostat.async_set_cooling_setpoint(_ecomode_temp)
            logger.info(
                "eco_reverted",
                from_temp=current_setpoint,
                to_temp=_ecomode_temp,
            )
            await notify_watchers(
                f"♻️ Eco mode restored to {_ecomode_temp}°F after manual change."
            )

    async def on_motion_evt():
        """Callback for motion sensor update."""

        last_motion_event_time = getattr(on_motion_evt, "last_motion_event_time", MOTION_DEBOUNCE_SECONDS)
        async with _motion_lock:
            if not motion.get_active():
                logger.info("Motion sensor is not active")
                return

            now = time.monotonic()

            # Ignore if we’ve sent an alert recently
            if now - last_motion_event_time < MOTION_DEBOUNCE_SECONDS:
                return

            on_motion_evt.last_motion_event_time = now

            logger.info(
                "Motion detected",
                name=motion.get_name(),
                active=motion.get_active(),
                debounce_seconds=MOTION_DEBOUNCE_SECONDS,
            )

            await notify_watchers(f"Motion detected at {motion.get_name()}")

    lock.set_update_callback(on_lock_evt)
    thermostat.set_update_callback(on_thermostat_evt)
    motion.set_update_callback(on_motion_evt)

    # --- Run both SmartRent updater + Telegram bot concurrently ---
    async def run_telegram():
        await app.initialize()
        await app.start()
        logger.info("Bot running...")
        await app.updater.start_polling()
        await asyncio.Event().wait()  # run forever

    await asyncio.gather(run_telegram())


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
