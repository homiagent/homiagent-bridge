"""Connectivity status sensor for HomiAgent Bridge."""
from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, connection_status_signal
from .entity import build_device_info
from .websocket_client import HomiAgentBridgeClient


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the connectivity binary sensor."""
    client: HomiAgentBridgeClient = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([HomiAgentBridgeConnectivitySensor(entry, client)])


class HomiAgentBridgeConnectivitySensor(BinarySensorEntity):
    """Reports whether the connection to HomiAgent is up."""

    _attr_has_entity_name = True
    _attr_name = "Connection"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_should_poll = False

    def __init__(self, entry: ConfigEntry, client: HomiAgentBridgeClient) -> None:
        self._client = client
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_connectivity"
        self._attr_device_info = build_device_info(entry)

    @property
    def is_on(self) -> bool:
        return self._client.connected

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                connection_status_signal(self._entry.entry_id),
                self._handle_status_update,
            )
        )

    @callback
    def _handle_status_update(self, _connected: bool) -> None:
        self.async_write_ha_state()
