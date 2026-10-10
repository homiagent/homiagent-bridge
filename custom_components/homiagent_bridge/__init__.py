"""The HomiAgent Bridge integration.

Connects this Home Assistant instance to your HomiAgent account so the assistant
can see and control your devices.
"""
from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers.instance_id import async_get as async_get_instance_id

from .const import BACKEND_WS_URL, CONF_CREDENTIAL, DOMAIN
from .websocket_client import HomiAgentBridgeClient

PLATFORMS: list[Platform] = [Platform.BINARY_SENSOR, Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up HomiAgent Bridge from a config entry."""

    async def _persist_rotated_credential(new_credential: str) -> None:
        hass.config_entries.async_update_entry(
            entry, data={**entry.data, CONF_CREDENTIAL: new_credential}
        )

    client = HomiAgentBridgeClient(
        hass,
        BACKEND_WS_URL,
        credential=entry.data[CONF_CREDENTIAL],
        ha_instance_uuid=await async_get_instance_id(hass),
        entry_id=entry.entry_id,
        on_credential_rotated=_persist_rotated_credential,
    )
    client.start()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = client

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        client: HomiAgentBridgeClient = hass.data[DOMAIN].pop(entry.entry_id)
        await client.stop()
    return unload_ok
