import asyncio
import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, UTC
from functools import partial
from types import SimpleNamespace

from aiohttp import web
import paho.mqtt.client as mqtt
import pyotp
from smartrent import async_login
import structlog

# ------------------------------------ Config ------------------------------------
config = SimpleNamespace(
    smartrent = SimpleNamespace(
        email = os.environ.get("SMARTRENT_EMAIL", "user@example.com"),
        password = os.environ.get("SMARTRENT_PASSWORD", "secret-password"),
        otpauth = os.environ.get("SMARTRENT_OTPAUTH", "otpauth://totp..."),
    ),
    mqtt = SimpleNamespace(
        broker = os.environ.get("MQTT_BROKER", "localhost"),
        port = int(os.environ.get("MQTT_PORT", 1883)),
        topic_prefix = "smartrent",
    ),
    health = SimpleNamespace(
        bind_address = os.environ.get("HEALTH_BIND_ADDRESS", "0.0.0.0"),
        port = int(os.environ.get("HEALTH_PORT", 8000)),
    ),
    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), 20)
)

# -------------------------------- Logging Setup ---------------------------------
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(config.log_level))
logger = structlog.get_logger()

# -------------------------------- Runtime State ---------------------------------
state = SimpleNamespace(
    ready = False,  # Becomes True after successful login and all device start_updater calls
    shutdown = False,
)


# ---------------------------------- Functions -----------------------------------
# https://www.reddit.com/r/learnpython/comments/12hlv0s/comment/jfpj7n5
def to_snakecase(text: str) -> str:
    if text.islower() or not text:
        return text
    return text[0].lower() + "".join(
        "_" + x.lower() if x.isupper() else x for x in text[1:]
    )


def publish_event(device_type: str, device_id: int, data: dict):
    """Publish event JSON to MQTT broker with timestamp and retain."""
    topic = f"{config.mqtt.topic_prefix}/{device_type}/{device_id}"
    payload = json.dumps({**data, "event_ts": datetime.now(UTC).isoformat() + "Z"})
    mqtt_client.publish(topic, payload, retain=True)
    logger.debug("Publish", **data)


def event_handler(device):
    device_type = to_snakecase(device.__class__.__name__)
    device_id = device._device_id
    data = {"device_type": device_type, "device_id": device_id}
    for attr in dir(device):
        if attr.startswith("get_"):
            fn = getattr(device, attr)
            if callable(fn):
                key = attr[4:]
                try:
                    val = fn()
                except Exception as e:
                    logger.error("Error calling `%s': %s" % (attr, e), **data)
                    continue
                data[key] = val
    publish_event(device_type, device_id, data)


def on_disconnect(client, userdata, rc):
    logger.warning("MQTT disconnected, attempting reconnect...", rc=rc)

    def _reconnect_loop():
        while not state.shutdown:
            try:
                client.reconnect()
                logger.info("MQTT reconnected")
                break
            except Exception:
                logger.warning("MQTT reconnect failed, retrying in 5s...")
                time.sleep(5)

    threading.Thread(target=_reconnect_loop, daemon=True).start()


# ------------------------------------- MQTT -------------------------------------
mqtt_client = mqtt.Client()
mqtt_client.on_disconnect = on_disconnect
try:
    mqtt_client.connect(config.mqtt.broker, config.mqtt.port)
    mqtt_client.loop_start()
    logger.info("MQTT connected", broker=config.mqtt.broker, port=config.mqtt.port)
except Exception as e:
    logger.warning("MQTT connect failed", error=str(e))

# -------------------------------- Health Server ---------------------------------
app = web.Application()
routes = web.RouteTableDef()


@routes.get("/healthz")
async def healthz(request):
    # fast liveness: process and loop are responsive
    return web.json_response({"alive": True, "ts": datetime.now(UTC).isoformat() + "Z"})


@routes.get("/ready")
async def ready(request):
    # readiness: True only once we've logged in and started the device updaters
    return web.json_response(
        {"ready": state.ready, "ts": datetime.now(UTC).isoformat() + "Z"}
    )


@routes.get("/metrics")
async def metrics(request):
    mqtt_conn = getattr(mqtt_client.is_connected, lambda: False)()
    return web.json_response(
        {
            "ready": state.ready,
            "mqtt_connected": mqtt_conn,
            "ts": datetime.now(UTC).isoformat() + "Z",
        }
    )


app.add_routes(routes)


async def start_health_server(loop):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.health.bind_address, config.health.port)
    await site.start()
    logger.info("Health server started", bind=config.health.bind_address, port=config.health.port)
    return runner


async def stop_health_server(runner):
    try:
        await runner.cleanup()
    except Exception as e:
        logger.warning("Health server stop failed", error=str(e))


# ------------------------------------- Main -------------------------------------
async def main():
    global state

    loop = asyncio.get_running_loop()
    health_runner = await start_health_server(loop)

    # graceful shutdown helper
    stop_event = asyncio.Event()

    def _on_sig(signame):
        logger.info("Signal received", signal=signame)
        state.shutdown = True
        stop_event.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, partial(_on_sig, s.name))
        except NotImplementedError:
            # Windows or uv loop may not support this
            pass

    try:
        logger.info("Logging into SmartRent...")
        totp = pyotp.parse_uri(config.smartrent.otpauth)
        api = await async_login(
            email=config.smartrent.email,
            password=config.smartrent.password,
            tfa_token=totp.at(datetime.now()),
        )
        logger.info("Logged in")
    except Exception as e:
        logger.exception("Smartrent login failed", error=str(e))
        # Keep readiness false and block; k8s should use startup probe...
        await stop_event.wait()
        await stop_health_server(health_runner)
        return

    # Register all devices with universal handler
    try:
        devices = api.get_device_list()
        logger.info("Devices discovered", count=len(devices))
    except Exception as e:
        logger.exception("Device discovery failed", error=str(e))
        raise

    for device in devices:
        try:
            device.start_updater()
            device.set_update_callback(partial(event_handler, device))
        except Exception as e:
            logger.warning(
                "Device start failed",
                device=getattr(device, "_device_id", None),
                error=str(e),
            )

    # Mark service ready now!
    state.ready = True
    logger.info("Service is ready")

    # Block until signal
    await stop_event.wait()
    logger.info("Shutdown initiated")

    # Graceful teardown
    state.ready = False

    # Stop device updaters (best effort)
    for device in devices:
        try:
            fn = getattr(device, "stop_updater")
            if callable(fn):
                fn()
        except Exception:
            continue

    # Stop mqtt (best effort)
    try:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception:
        pass

    # Stop the health server
    await stop_health_server(health_runner)
    logger.info("Shutdown complete")
    return


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Main failed")
