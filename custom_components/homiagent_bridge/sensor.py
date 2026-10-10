"""Last-command sensor for HomiAgent Bridge.

Exists purely so each command executed via the API shows up as Activity on
this integration's own device page, not just in the target entity's Logbook.
"""
from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import last_command_signal
from .entity import build_device_info


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the last-command sensor."""
    async_add_entities([HomiAgentBridgeLastCommandSensor(entry)])


class HomiAgentBridgeLastCommandSensor(SensorEntity):
    """Reflects the most recent command executed via the API."""

    _attr_has_entity_name = True
    _attr_name = "Last command"
    _attr_should_poll = False
    _attr_icon = "mdi:console-line"

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_last_command"
        self._attr_device_info = build_device_info(entry)
        self._attr_native_value: str | None = None
        self._attr_extra_state_attributes: dict[str, Any] = {}

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                last_command_signal(self._entry.entry_id),
                self._handle_command,
            )
        )

    @callback
    def _handle_command(self, info: dict[str, Any]) -> None:
        action = f"{info['domain']}.{info['service']}"
        target = f" on {info['entity_id']}" if info.get("entity_id") else ""
        self._attr_native_value = (
            f"{action}{target}" if info["success"] else f"{action}{target} (failed)"
        )
        self._attr_extra_state_attributes = {
            "domain": info["domain"],
            "service": info["service"],
            "entity_id": info.get("entity_id"),
            "success": info["success"],
            "error": info.get("error"),
            "timestamp": info["timestamp"],
        }
        self.async_write_ha_state()
