# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "smartrent-py",
#     "gmqtt",
#     "pyotp",
#     "structlog",
# ]
# ///
import asyncio
import json
import logging
import os
import signal
from datetime import date, datetime, UTC
from functools import partial
from types import SimpleNamespace
from typing import Optional

from aiohttp import web
import gmqtt
from gmqtt import Client as MQTTClient
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
timestamper = structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S")
shared_processors = [
    structlog.stdlib.add_log_level,
    timestamper,
]

structlog.configure(
    processors=shared_processors + [
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
    wrapper_class=structlog.make_filtering_bound_logger(config.log_level)
)

formatter = structlog.stdlib.ProcessorFormatter(
    # These run ONLY on `logging` entries that do NOT originate within
    # structlog.
    foreign_pre_chain=shared_processors,
    # These run on ALL entries after the pre_chain is done.
    processors=[
        # Remove _record & _from_structlog.
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        structlog.dev.ConsoleRenderer(),
    ],
)

handler = logging.StreamHandler()
# Use OUR `ProcessorFormatter` to format all `logging` entries.
handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.addHandler(handler)
root_logger.setLevel(config.log_level)

logger = structlog.get_logger()

# -------------------------------- Runtime State ---------------------------------
state = SimpleNamespace(
    smartrent_ready = False,  # Becomes True after successful login and all device start_updater calls
    shutdown = False,
)

# --------------------------------- MQTT Pub/Sub ---------------------------------
class MQTTPublisher(object):
    def __init__(self, client: MQTTClient, base_topic: str):
        self.client = client
        self.base = base_topic.rstrip("/")
        self.subscriptions = {}

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_unsubscribe = self.on_unsubscribe

    def infer_dtype_and_payload(self, value):
        """
        Infer MQTT-friendly dtype and serialized payload from a Python value.
        """
        # Order matters!
        if isinstance(value, bool):
            return "boolean", json.dumps(value)

        if isinstance(value, (int, float)):
            return "number", str(value)

        if isinstance(value, (datetime, date)):
            return "datetime", value.astimezone(UTC).replace(tzinfo=None).isoformat() + "Z"

        if isinstance(value, str):
            return "string", value

        if isinstance(value, (dict, list, tuple, set)):
            return "json", json.dumps(value)

        # Fallback
        return "string", str(value)

    def publish(self, name: str, value, *, set_func: Optional[function] = None, dtype: Optional[str] = None, unit: Optional[str] = None, retain: bool = True, qos: int=1):
        topic = f"{self.base}/{name}"

        if dtype is None:
            dtype, value = self.infer_dtype_and_payload(value)

        self.client.publish(topic, payload=value, retain=retain, qos=qos)
        self.client.publish(f"{topic}/$type", payload=dtype, retain=True, qos=1)
        self.client.publish(f"{topic}/$writable", payload=json.dumps(set_func is not None), retain=True, qos=1)
        if unit is not None:
            self.client.publish(f"{topic}/$unit", unit, retain=True, qos=1)

        if set_func:
            set_topic = f"{topic}/set"
            self.subscriptions[set_topic] = set_func
            self.client.subscribe(set_topic, qos=1, no_local=True)

    async def on_connect(self, client, flags, rc, properties):
        logger.info("Connected to MQTT", client_id=client._client_id)

    async def on_disconnect(self, client, packet, exc=None):
        if exc is not None:
            logger.warning("Disconnected from MQTT", client_id=client._client_id, err=str(exc))
        else:
            logger.info("Disconnected from MQTT", client_id=client._client_id)

    async def on_message(self, client, topic, payload, qos, properties):
        logger.info("Payload", payload=payload)
        try:
            val = payload.decode("UTF-8").lower()
            fn = self.subscriptions[topic]
            try:
                res = await fn(val)
                logger.debug(f"Called {fn.__name__}({val}) => {res}")
            except ValueError as e:
                logger.warning("Invalid value", client_id=client._client_id, topic=topic, qos=qos, properties=properties, value=val, e=str(e))
                self.client.publish(f"{topic.rstrip('/set')}/$error", str(e), retain=False, qos=1)
        except KeyError:
            # Couldn't find subscription -- this shouldn't happen!
            logger.warning("Missing subscription", client_id=client._client_id, topic=topic, qos=qos, properties=properties, value=val)

    def on_subscribe(self, client, mid, qos, properties):
        # in order to check if all the subscriptions were successful, we should first get all subscriptions with this
        # particular mid (from one subscription request)
        subscriptions = client.get_subscriptions_by_mid(mid)
        for subscription, granted_qos in zip(subscriptions, qos):
            # in case of bad suback code, we can resend subscription
            if granted_qos >= gmqtt.constants.SubAckReasonCode.UNSPECIFIED_ERROR.value:
                logger.warning("Retrying subscription", client_id=client._client_id, mid=mid, reason_code=granted_qos, properties=properties)
                client.resubscribe(subscription)
            logger.info("Subscribed", client_id=client._client_id, mid=mid, qos=granted_qos, properties=properties)

    def on_unsubscribe(self, client, userdata, mid, reason_code_list, properties):
        # Be careful, the reason_code_list is only present in MQTTv5.
        # In MQTTv3 it will always be empty
        if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
            logger.info("Unsubscribe succeeded")
        else:
            logger.error(f"Broker replied with failure: {reason_code_list[0]}")

# ---------------------------------- Functions -----------------------------------
# https://www.reddit.com/r/learnpython/comments/12hlv0s/comment/jfpj7n5
def to_snakecase(text: str) -> str:
    if text.islower() or not text:
        return text
    return text[0].lower() + "".join(
        "_" + x.lower() if x.isupper() else x for x in text[1:]
    )

def event_handler(mqtt_client, device):
    event_ts = datetime.now(UTC)

    device_type = to_snakecase(device.__class__.__name__)
    short_name = to_snakecase(device.get_name().split("-")[0].replace(" ", ""))
    topic=f"{device_type}/{short_name}"

    device_id = device._device_id

    mqtt_client.publish(f"{topic}/id", device_id)

    getters = {}
    setters = {}
    for attr in dir(device):
        fn = getattr(device, attr)
        if callable(fn):
            if attr.startswith("get"):
                getters[attr[4:]] = fn
            if attr.startswith("async_set"):
                setters[attr[10:]] = fn

    for key, fn in getters.items():
        value = fn()
        unit = None
        if key.endswith("_level") or key == "current_humidity":
            unit = "%"
        if key.endswith("_setpoint") or key.endswith("_temp"):
            unit = "\N{DEGREE SIGN}F"
        curval = fn()
        setter = setters.get(key, None)
        mqtt_client.publish(f"{topic}/{key}", value, set_func=setter, unit=unit)

    mqtt_client.publish(f"{topic}/last_update", event_ts)

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
    ready = state.smartrent_ready and mqtt_client.is_connected
    status_code = 200 if ready else 503
    return web.json_response(
        {"ready": ready, "ts": datetime.now(UTC).isoformat() + "Z",  "status_code": status_code}
    )


@routes.get("/metrics")
async def metrics(request):
    return web.json_response(
        {
            "smartrent_ready": state.smartrent_ready,
            "mqtt_connected": mqtt_client.is_connected,
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

    c = MQTTClient(client_id=config.mqtt.client_id)
    await c.connect(config.mqtt.broker, config.mqtt.port)
    mqtt_client = MQTTPublisher(c, config.mqtt.topic_prefix)
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
            event_handler(mqtt_client, device)
            device.start_updater()
            device.set_update_callback(partial(event_handler, mqtt_client, device))
        except Exception as e:
            raise
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
        await mqtt_client.disconnect()
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
