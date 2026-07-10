"""
Google Home Smart Home Fulfillment endpoint.

Handles the three core intents Google Home sends:

  SYNC    — Dynamically list all logical devices owned by the authenticated user.
            Each endpoint on each physical device becomes a separate Google Home
            device. Endpoint definitions are read directly from the smarthome_devices
            document (copied from the device_models catalog at provisioning time).

  QUERY   — Return the current ON/OFF state of requested devices from MongoDB.
            No MQTT round-trip needed — state is kept live in the DB by the
            MQTT bridge.

  EXECUTE — Send an ON/OFF command via MQTT. MongoDB is updated strictly when
            the ESP32 publishes its confirmed state back (handled by the MQTT
            bridge). No optimistic writes are performed here.

Device ID format: "<12-char-mac>_<endpoint_id>"   e.g. "aabbccddeeff_light1"

Reference:
  https://developers.google.com/assistant/smarthome/reference/rest/v1/fulfill
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.deps import get_current_consumer_user, get_smarthome_device_repo
from app.models.smarthome_device import SmartHomeDeviceDoc
from app.repositories.smarthome_device_repository import SmartHomeDeviceRepository
from app.schemas.smarthome import FulfillmentRequest, FulfillmentResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["smart_home"])

_FALLBACK_GOOGLE_TYPE = "action.devices.types.OUTLET"


# ---------------------------------------------------------------------------
# Fulfillment entry point
# ---------------------------------------------------------------------------

@router.post("/smarthome/fulfillment", response_model=FulfillmentResponse)
async def fulfillment(
    request: Request,
    body: FulfillmentRequest,
    user_id: str = Depends(get_current_consumer_user),
    device_repo: SmartHomeDeviceRepository = Depends(get_smarthome_device_repo),
) -> FulfillmentResponse:
    """Google Home sends all smart home intents to this single endpoint."""
    if not body.inputs:
        raise HTTPException(status_code=400, detail="empty_inputs")

    intent_input = body.inputs[0]
    intent = intent_input.intent

    if intent == "action.devices.SYNC":
        payload = await _handle_sync(user_id, device_repo)
    elif intent == "action.devices.QUERY":
        payload = await _handle_query(intent_input.payload, user_id, device_repo)
    elif intent == "action.devices.EXECUTE":
        payload = await _handle_execute(intent_input.payload, user_id, device_repo)
    elif intent == "action.devices.DISCONNECT":
        payload = {}
    else:
        logger.warning("fulfillment_unknown_intent intent=%s", intent)
        raise HTTPException(status_code=400, detail=f"unknown_intent: {intent}")

    return FulfillmentResponse(requestId=body.requestId, payload=payload)


# ---------------------------------------------------------------------------
# SYNC — dynamically enumerate all logical devices for this user
# ---------------------------------------------------------------------------

async def _handle_sync(
    user_id: str,
    device_repo: SmartHomeDeviceRepository,
) -> dict[str, Any]:
    docs = await device_repo.get_devices_for_user(user_id)
    google_devices: list[dict[str, Any]] = []

    for raw in docs:
        dev = SmartHomeDeviceDoc.from_doc(raw)
        for ep in dev.endpoints:
            device_id = f"{dev.mac}_{ep.id}"
            google_devices.append(
                {
                    "id": device_id,
                    "type": ep.google_type or _FALLBACK_GOOGLE_TYPE,
                    "traits": ["action.devices.traits.OnOff"],
                    "name": {
                        "defaultNames": [ep.name],
                        "name": f"{dev.name} — {ep.name}",
                        "nicknames": [f"{dev.name} {ep.name}"],
                    },
                    "willReportState": True,
                    "deviceInfo": {
                        "manufacturer": "Hellum",
                        "model": dev.device_model,
                        "hwVersion": "1.0",
                        "swVersion": "1.0",
                    },
                    "customData": {
                        "mac": dev.mac,
                        "endpoint": ep.id,
                    },
                }
            )

    logger.info("fulfillment_sync user_id=%s devices=%d", user_id, len(google_devices))
    return {"agentUserId": user_id, "devices": google_devices}


# ---------------------------------------------------------------------------
# QUERY — return current state for requested device IDs
# ---------------------------------------------------------------------------

async def _handle_query(
    payload: dict[str, Any],
    user_id: str,
    device_repo: SmartHomeDeviceRepository,
) -> dict[str, Any]:
    requested_devices: list[dict] = payload.get("devices", [])
    device_states: dict[str, Any] = {}

    # Group device IDs by MAC to minimise DB round-trips
    mac_map: dict[str, list[tuple[str, str]]] = {}
    for d in requested_devices:
        device_id: str = d.get("id", "")
        try:
            mac, endpoint_id = _parse_device_id(device_id)
        except ValueError:
            device_states[device_id] = {"online": False, "errorCode": "deviceNotFound"}
            continue
        mac_map.setdefault(mac, []).append((device_id, endpoint_id))

    for mac, pairs in mac_map.items():
        doc = await device_repo.get_by_mac(mac)
        if not doc or str(doc.get("user_id")) != user_id:
            for device_id, _ in pairs:
                device_states[device_id] = {"online": False, "errorCode": "deviceNotFound"}
            continue

        dev = SmartHomeDeviceDoc.from_doc(doc)
        state_dict = dev.get_state_dict()
        valid_ids = dev.get_endpoint_ids()
        is_online = bool(doc.get("online", False))
 
        for device_id, endpoint_id in pairs:
            if endpoint_id not in valid_ids:
                device_states[device_id] = {"online": False, "errorCode": "deviceNotFound"}
            else:
                device_states[device_id] = {
                    "online": is_online,
                    "on": state_dict.get(endpoint_id, False),
                }

    logger.info("fulfillment_query user_id=%s queried=%d", user_id, len(requested_devices))
    return {"devices": device_states}


# ---------------------------------------------------------------------------
# EXECUTE — send command via MQTT; DB updated on ESP32 confirmation only
# ---------------------------------------------------------------------------

async def _handle_execute(
    payload: dict[str, Any],
    user_id: str,
    device_repo: SmartHomeDeviceRepository,
) -> dict[str, Any]:
    from app.services.mqtt_service import mqtt_service

    commands: list[dict] = payload.get("commands", [])
    results: list[dict[str, Any]] = []

    for command_group in commands:
        target_devices: list[dict] = command_group.get("devices", [])
        executions: list[dict] = command_group.get("execution", [])

        for execution in executions:
            cmd_name: str = execution.get("command", "")
            cmd_params: dict = execution.get("params", {})

            if cmd_name != "action.devices.commands.OnOff":
                results.append(
                    {
                        "ids": [d["id"] for d in target_devices],
                        "status": "ERROR",
                        "errorCode": "notSupported",
                    }
                )
                continue

            target_on: bool = bool(cmd_params.get("on", False))
            success_ids: list[str] = []
            error_ids: list[str] = []
            offline_ids: list[str] = []
 
            for d in target_devices:
                device_id: str = d.get("id", "")
                try:
                    mac, endpoint_id = _parse_device_id(device_id)
                except ValueError:
                    error_ids.append(device_id)
                    continue
 
                doc = await device_repo.get_by_mac(mac)
                if not doc or str(doc.get("user_id")) != user_id:
                    error_ids.append(device_id)
                    continue
 
                # Validate endpoint exists on this specific device
                dev = SmartHomeDeviceDoc.from_doc(doc)
                if endpoint_id not in dev.get_endpoint_ids():
                    error_ids.append(device_id)
                    continue
 
                await mqtt_service.publish_command(mac, endpoint_id, target_on)
                # NOTE: No optimistic DB update here.  MongoDB is updated
                # strictly when the ESP32 publishes its confirmed state back
                # via MQTT (handled in mqtt_service.py).  Writing the assumed
                # state here would leave the DB out of sync if the device is
                # offline or drops the message.
 
                is_online = bool(doc.get("online", False))
                if is_online:
                    success_ids.append(device_id)
                    logger.info(
                        "fulfillment_execute mac=%s endpoint=%s on=%s user_id=%s",
                        mac, endpoint_id, target_on, user_id,
                    )
                else:
                    offline_ids.append(device_id)
                    logger.info(
                        "fulfillment_execute_offline mac=%s endpoint=%s on=%s user_id=%s (enqueued)",
                        mac, endpoint_id, target_on, user_id,
                    )
 
            if success_ids:
                results.append(
                    {
                        "ids": success_ids,
                        "status": "SUCCESS",
                        "states": {"on": target_on, "online": True},
                    }
                )
            if offline_ids:
                results.append(
                    {
                        "ids": offline_ids,
                        "status": "OFFLINE",
                    }
                )
            if error_ids:
                results.append(
                    {
                        "ids": error_ids,
                        "status": "ERROR",
                        "errorCode": "deviceNotFound",
                    }
                )

    return {"commands": results}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_device_id(device_id: str) -> tuple[str, str]:
    """Parse a Google Home device ID into (mac, endpoint_id).

    Format: "<12-char-mac>_<endpoint_id>"  e.g. "aabbccddeeff_light1"

    Note: Unlike the previous MVP implementation, we no longer validate
    endpoint_id against a hardcoded VALID_ENDPOINTS constant. Validation is
    done against the device's own endpoint list fetched from MongoDB.
    """
    parts = device_id.rsplit("_", 1)
    if len(parts) != 2 or len(parts[0]) != 12:
        raise ValueError(f"malformed device_id: {device_id}")
    return parts[0], parts[1]
