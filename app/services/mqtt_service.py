"""
MQTT Bridge Service
===================

Connects to the local Mosquitto broker (plain MQTT, port 1883) and provides:

  1. **State listener** — subscribes to ``smarthome/device/+/state`` and
     persists ESP32-reported endpoint states to MongoDB.

  2. **Registration listener** — subscribes to ``smarthome/register`` and
     handles the MQTT Binding Token device provisioning flow:
       ESP32 publishes → {"mac": "...", "binding_token": "...", "device_model": "..."}
       Backend validates the token, fetches the device model endpoints from
       the catalog, and creates the device document linked to the token's user.

  3. **Command publisher** — ``publish_command()`` formats and enqueues an
     ON/OFF command to ``smarthome/device/<mac>/cmd``.

Topic conventions (must match ESP32 firmware exactly):
  Subscribe: smarthome/device/+/state
  Subscribe: smarthome/register
  Publish:   smarthome/device/<mac>/cmd

State payload format (string states, NOT booleans):
  {"device": "<endpoint_id>", "state": "on"|"off"}

Registration payload format:
  {"mac": "<12-char-hex>", "binding_token": "<token>", "device_model": "<model_id>"}
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import aiomqtt

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

_STATE_SUBSCRIBE = "smarthome/device/+/state"
_REGISTER_TOPIC = "smarthome/register"
_STATUS_SUBSCRIBE = "smarthome/device/+/status"
_CMD_TOPIC_TEMPLATE = "smarthome/device/{mac}/cmd"


class MQTTService:
    """Async MQTT bridge — long-lived connection to the Mosquitto broker."""

    def __init__(self, *, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._db: "AsyncIOMotorDatabase | None" = None
        self._listen_task: asyncio.Task | None = None
        self._publish_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

    async def start(self, db: "AsyncIOMotorDatabase") -> None:
        self._db = db
        self._listen_task = asyncio.create_task(
            self._run(), name="mqtt_bridge"
        )
        logger.info("mqtt_bridge_starting host=%s port=%d", self._host, self._port)

    async def stop(self) -> None:
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        logger.info("mqtt_bridge_stopped")

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def publish_command(
        self, mac_address: str, endpoint: str, target_state: bool
    ) -> None:
        """Enqueue an ON/OFF command to a specific device endpoint.

        Translates the boolean ``target_state`` to the "on"/"off" string
        required by the ESP32 firmware.
        """
        topic = _CMD_TOPIC_TEMPLATE.format(mac=mac_address)
        payload = json.dumps(
            {"device": endpoint, "state": "on" if target_state else "off"}
        )
        await self._publish_queue.put((topic, payload))
        logger.debug(
            "mqtt_command_enqueued mac=%s endpoint=%s state=%s",
            mac_address, endpoint, target_state,
        )

    # -------------------------------------------------------------------------
    # Internal reconnecting loop
    # -------------------------------------------------------------------------

    async def _run(self) -> None:
        """Persistent reconnecting loop — runs for the lifetime of the app."""
        reconnect_delay = 1

        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self._host,
                    port=self._port,
                    identifier="hellum-iot-bridge",
                ) as client:
                    reconnect_delay = 1
                    logger.info(
                        "mqtt_bridge_connected host=%s port=%d",
                        self._host, self._port,
                    )

                    await client.subscribe(_STATE_SUBSCRIBE, qos=1)
                    await client.subscribe(_REGISTER_TOPIC, qos=1)
                    await client.subscribe(_STATUS_SUBSCRIBE, qos=1)
                    logger.info(
                        "mqtt_bridge_subscribed state=%s register=%s status=%s",
                        _STATE_SUBSCRIBE, _REGISTER_TOPIC, _STATUS_SUBSCRIBE,
                    )

                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._listen_messages(client))
                        tg.create_task(self._drain_publish_queue(client))

            except aiomqtt.MqttError as exc:
                logger.warning(
                    "mqtt_bridge_disconnected error=%s retry_in=%ds",
                    exc, reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
            except asyncio.CancelledError:
                logger.info("mqtt_bridge_cancelled")
                raise

    async def _listen_messages(self, client: aiomqtt.Client) -> None:
        async for message in client.messages:
            topic_str = str(message.topic)
            try:
                if topic_str == _REGISTER_TOPIC:
                    await self._handle_registration_message(message.payload)
                elif topic_str.endswith("/status"):
                    await self._handle_status_message(topic_str, message.payload)
                else:
                    await self._handle_state_message(topic_str, message.payload)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "mqtt_message_error topic=%s error=%s",
                    topic_str, exc, exc_info=True,
                )

    async def _drain_publish_queue(self, client: aiomqtt.Client) -> None:
        while True:
            topic, payload = await self._publish_queue.get()
            try:
                await client.publish(topic, payload=payload, qos=1)
                logger.debug("mqtt_published topic=%s payload=%s", topic, payload)
            except aiomqtt.MqttError as exc:
                logger.error("mqtt_publish_failed topic=%s error=%s", topic, exc)
                await self._publish_queue.put((topic, payload))
                raise
            finally:
                self._publish_queue.task_done()

    # -------------------------------------------------------------------------
    # State message handler  (smarthome/device/<mac>/state)
    # -------------------------------------------------------------------------

    async def _handle_state_message(self, topic: str, payload: bytes | str) -> None:
        parts = topic.split("/")
        if len(parts) != 4:
            logger.warning("mqtt_unexpected_topic topic=%s", topic)
            return

        mac = parts[2]

        try:
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8")
            data = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("mqtt_invalid_payload topic=%s error=%s", topic, exc)
            return

        endpoint_id: str = data.get("device", "")
        raw_state: str = data.get("state", "")

        if raw_state == "on":
            bool_state = True
        elif raw_state == "off":
            bool_state = False
        else:
            logger.warning("mqtt_invalid_state mac=%s endpoint=%s state=%r", mac, endpoint_id, raw_state)
            return

        if self._db is None:
            logger.error("mqtt_no_db — cannot persist state")
            return

        from app.repositories.smarthome_device_repository import SmartHomeDeviceRepository

        repo = SmartHomeDeviceRepository(self._db)
        updated = await repo.update_endpoint_state(mac, endpoint_id, bool_state)
        if not updated:
            logger.warning(
                "mqtt_state_unmatched mac=%s endpoint=%s — endpoint not found on device",
                mac, endpoint_id,
            )
        else:
            logger.info(
                "mqtt_state_persisted mac=%s endpoint=%s state=%s",
                mac, endpoint_id, bool_state,
            )

    # -------------------------------------------------------------------------
    # Registration message handler  (smarthome/register)
    # -------------------------------------------------------------------------

    async def _handle_registration_message(self, payload: bytes | str) -> None:
        """Process an MQTT Binding Token device registration request from an ESP32.

        Expected payload:
            {"mac": "aabbccddeeff", "binding_token": "Token_XYZ", "device_model": "4-switch-board"}
        """
        try:
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8")
            data = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("mqtt_registration_invalid_payload error=%s", exc)
            return

        mac: str = data.get("mac", "").lower().strip()
        binding_token: str = data.get("binding_token", "").strip()
        device_model_id: str = data.get("device_model", "").strip()

        if not mac or not binding_token or not device_model_id:
            logger.warning(
                "mqtt_registration_missing_fields mac=%s model=%s token_present=%s",
                mac, device_model_id, bool(binding_token),
            )
            return

        if self._db is None:
            logger.error("mqtt_no_db — cannot process registration")
            return

        # 1. Atomically validate + consume the binding token (one-time use)
        token_doc = await self._db.binding_tokens.find_one_and_update(
            {"token": binding_token, "used": False},
            {"$set": {"used": True}},
        )

        if not token_doc:
            logger.warning(
                "mqtt_registration_invalid_token mac=%s token=%s",
                mac, binding_token,
            )
            return

        # Double-check expiry (TTL index may have slight lag)
        expires_at = token_doc.get("expires_at")
        if expires_at and expires_at.replace(tzinfo=UTC) < datetime.now(UTC):
            logger.warning(
                "mqtt_registration_token_expired mac=%s user_id=%s",
                mac, token_doc["user_id"],
            )
            return

        user_id = str(token_doc["user_id"])

        # 2. Fetch device model endpoints from the catalog
        from app.repositories.device_model_repository import DeviceModelRepository
        from app.repositories.smarthome_device_repository import SmartHomeDeviceRepository

        model_repo = DeviceModelRepository(self._db)
        model_doc = await model_repo.get_by_model_id(device_model_id)
        if not model_doc:
            logger.error(
                "mqtt_registration_unknown_model mac=%s model=%s",
                mac, device_model_id,
            )
            return

        endpoints = model_doc.get("endpoints", [])

        # 3. Create the device document
        device_repo = SmartHomeDeviceRepository(self._db)
        try:
            await device_repo.create_device(
                mac=mac,
                user_id=user_id,
                name=model_doc.get("display_name", device_model_id),
                device_model=device_model_id,
                endpoints=endpoints,
            )
            logger.info(
                "mqtt_device_provisioned mac=%s model=%s user_id=%s",
                mac, device_model_id, user_id,
            )
        except ValueError:
            logger.warning(
                "mqtt_registration_mac_already_claimed mac=%s",
                mac,
            )

    # -------------------------------------------------------------------------
    # Status message handler  (smarthome/device/<mac>/status)
    # -------------------------------------------------------------------------

    async def _handle_status_message(self, topic: str, payload: bytes | str) -> None:
        """Process an MQTT connection status message (online/offline) from an ESP32 or LWT."""
        parts = topic.split("/")
        if len(parts) != 4:
            logger.warning("mqtt_unexpected_status_topic topic=%s", topic)
            return

        mac = parts[2]
        try:
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8")
            status_str = payload.strip().lower()
        except UnicodeDecodeError as exc:
            logger.warning("mqtt_invalid_status_payload topic=%s error=%s", topic, exc)
            return

        if status_str == "online":
            is_online = True
        elif status_str == "offline":
            is_online = False
        else:
            logger.warning("mqtt_invalid_status mac=%s status=%r", mac, status_str)
            return

        if self._db is None:
            logger.error("mqtt_no_db — cannot persist status")
            return

        from app.repositories.smarthome_device_repository import SmartHomeDeviceRepository

        repo = SmartHomeDeviceRepository(self._db)
        updated = await repo.update_device_online_status(mac, is_online)
        if not updated:
            logger.warning(
                "mqtt_status_unmatched mac=%s status=%s — device not found in db",
                mac, is_online,
            )


# ---------------------------------------------------------------------------
# Module-level singleton — imported by main.py and route handlers
# ---------------------------------------------------------------------------
mqtt_service = MQTTService(host="localhost", port=1883)
