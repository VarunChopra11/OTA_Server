import pytest
from unittest.mock import AsyncMock, MagicMock
from app.repositories.smarthome_device_repository import SmartHomeDeviceRepository

@pytest.mark.asyncio
async def test_update_device_online_status() -> None:
    # Mock MongoDB database and collection
    db_mock = MagicMock()
    collection_mock = MagicMock()
    db_mock.smarthome_devices = collection_mock
    
    # Mock update_one response
    update_result_mock = MagicMock()
    update_result_mock.matched_count = 1
    collection_mock.update_one = AsyncMock(return_value=update_result_mock)
    
    repo = SmartHomeDeviceRepository(db_mock)
    
    # Call the method
    success = await repo.update_device_online_status("aabbccddeeff", True)
    
    # Assertions
    assert success is True
    collection_mock.update_one.assert_called_once()
    call_args = collection_mock.update_one.call_args[0]
    
    # Verify MAC filtering and set statement
    assert call_args[0] == {"mac": "aabbccddeeff"}
    assert "$set" in call_args[1]
    assert call_args[1]["$set"]["online"] is True
