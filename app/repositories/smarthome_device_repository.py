"""Repository for Smart Home device documents — dynamic endpoint architecture."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)


class SmartHomeDeviceRepository:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self.collection = db.smarthome_devices

    async def create_device(
        self,
        *,
        mac: str,
        user_id: str,
        name: str,
        device_model: str,
        endpoints: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Create a new provisioned Smart Home device.

        Args:
            mac:          12-char lowercase hex MAC.
            user_id:      ObjectId string of the owning consumer user.
            name:         Human-readable device name.
            device_model: Model slug from the device_models catalog.
            endpoints:    List of endpoint dicts copied from the device model,
                          each with {id, name, google_type} — ``state`` is
                          initialised to False here.

        Raises:
            ValueError: If the MAC is already registered.
        """
        now = datetime.now(UTC)
        # Inject initial state=False into each endpoint copy
        endpoints_with_state = [
            {**ep, "state": False}
            for ep in endpoints
        ]
        doc: dict[str, Any] = {
            "mac": mac,
            "user_id": ObjectId(user_id),
            "name": name,
            "device_model": device_model,
            "endpoints": endpoints_with_state,
            "online": False,
            "created_at": now,
            "updated_at": now,
        }
        try:
            result = await self.collection.insert_one(doc)
        except DuplicateKeyError as exc:
            raise ValueError(f"Device with MAC '{mac}' is already registered.") from exc
        doc["_id"] = result.inserted_id
        return doc

    async def get_by_mac(self, mac: str) -> dict[str, Any] | None:
        """Return a device document by its MAC address."""
        return await self.collection.find_one({"mac": mac})

    async def get_devices_for_user(self, user_id: str) -> list[dict[str, Any]]:
        """Return all devices owned by a specific consumer user."""
        try:
            oid = ObjectId(user_id)
        except Exception:
            return []
        cursor = self.collection.find({"user_id": oid})
        return await cursor.to_list(length=None)

    async def list_all(self) -> list[dict[str, Any]]:
        """Return all registered devices (admin use)."""
        cursor = self.collection.find({})
        return await cursor.to_list(length=None)

    async def update_endpoint_state(self, mac: str, endpoint_id: str, value: bool) -> bool:
        """Update a single endpoint's boolean state using the positional operator.

        Uses MongoDB's filtered positional operator (``$``) to update only the
        array element whose ``id`` matches ``endpoint_id``.

        Returns:
            True if the endpoint was found and updated, False if the endpoint
            does not exist on this device (caller should log and discard).
        """
        result = await self.collection.update_one(
            {"mac": mac, "endpoints.id": endpoint_id},
            {
                "$set": {
                    "endpoints.$.state": value,
                    "updated_at": datetime.now(UTC),
                }
            },
        )
        updated = result.matched_count > 0
        if updated:
            logger.debug(
                "smarthome_state_updated mac=%s endpoint=%s value=%s",
                mac, endpoint_id, value,
            )
        return updated

    async def update_device_online_status(self, mac: str, online: bool) -> bool:
        """Update the device's online status in MongoDB."""
        result = await self.collection.update_one(
            {"mac": mac},
            {
                "$set": {
                    "online": online,
                    "updated_at": datetime.now(UTC),
                }
            },
        )
        updated = result.matched_count > 0
        if updated:
            logger.info(
                "smarthome_online_updated mac=%s online=%s",
                mac, online,
            )
        return updated

    async def rename_device(self, mac: str, user_id: str, name: str) -> bool:
        """Rename a device. Verifies ownership before updating."""
        try:
            oid = ObjectId(user_id)
        except Exception:
            return False
        result = await self.collection.update_one(
            {"mac": mac, "user_id": oid},
            {"$set": {"name": name, "updated_at": datetime.now(UTC)}},
        )
        return result.matched_count > 0

    async def delete_device(self, mac: str, user_id: str) -> bool:
        """Release a device from its owner. Verifies ownership before deleting."""
        try:
            oid = ObjectId(user_id)
        except Exception:
            return False
        result = await self.collection.delete_one({"mac": mac, "user_id": oid})
        return result.deleted_count > 0
