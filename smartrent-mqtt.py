import asyncio
import json
import logging
import os
import signal
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
        client_id = "smartrent"
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
    smartrent_ready = False,  # Becomes True after successful login and all device start_updater calls
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

# ------------------------------------- MQTT -------------------------------------
loglevel_map = {
    mqtt.LogLevel.MQTT_LOG_INFO: logging.INFO,
    mqtt.LogLevel.MQTT_LOG_NOTICE: logging.INFO,
    mqtt.LogLevel.MQTT_LOG_WARNING: logging.WARNING,
    mqtt.LogLevel.MQTT_LOG_ERR: logging.ERROR,
    mqtt.LogLevel.MQTT_LOG_DEBUG: logging.DEBUG,
}

def on_log(client, userdata, paho_level, message):
    py_level = loglevel_map.get(paho_level, logging.INFO)
    logger.log(py_level, message)

def on_subscribe(client, userdata, mid, reason_code_list, properties):
    # Since we subscribed only for a single channel, reason_code_list contains
    # a single entry
    if reason_code_list[0].is_failure:
        logger.error(f"Broker rejected subscription: {reason_code_list[0]}")
    else:
        logger.info(f"Broker granted the following QoS: {reason_code_list[0].value}")

def on_unsubscribe(client, userdata, mid, reason_code_list, properties):
    # Be careful, the reason_code_list is only present in MQTTv5.
    # In MQTTv3 it will always be empty
    if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
        logger.info("Unsubscribe succeeded")
    else:
        logger.error(f"Broker replied with failure: {reason_code_list[0]}")

def on_connect(client, userdata, flags, reason_code, properties):
    logger.info("Connected to MQTT Broker", reason_code=reason_code)
    client.subscribe(config.mqtt.topic_prefix)

def on_disconnect(client, obj, flags, reason_code, properties):
    logger.warning("MQTT disconnected, attempting reconnect...", reason_code=reason_code)

def on_message(client, userdata, message):
    try:
        payload=json.loads(message.payload)
    except Exception:age):
    try:
        payload=json.loads(message.payload)
    except Exception:
        payload={"payload": message.payload}
    logger.info(message.topic, **payload, message_state=message.state)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_log = on_log
mqtt_client.on_connect = on_connectte)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_log = on_log
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_subscribe = on_subscribe
mqtt_client.on_unsubscribe = on_unsubscribe
mqtt_client.on_message = on_message

mqtt_client.loop_start()

try:
    mqtt_client.connect(config.mqtt.broker, config.mqtt.port, client=id)
except Exception as e:
    logger.critical("MQTT connect failed", errno=e.errno, error=e.strerror)
    raise

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
    # *and* connected to MQTT
    ready = state.smartrent_ready and mqtt_client.is_connected()
    return web.json_response(
        {"ready": ready, "ts": datetime.now(UTC).isoformat() + "Z"}
    )


@routes.get("/metrics")
async def metrics(request):
    return web.json_response(
        {
            "smartrent_ready": state.smartrent_ready,
            "mqtt_connected": mqtt_client.is_connected(),
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
    state.smartrent_ready = True
    logger.info("SmartRent is ready")

    # Block until signal
    await stop_event.wait()
    logger.info("Shutdown initiated")

    # Stop device updaters (best effort)
    for device in devices:
        try:
            fn = getattr(device, "stop_updater")
            if callable(fn):
                fn()
        except Exception:
            continue
    state.smartrent_ready = False

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
