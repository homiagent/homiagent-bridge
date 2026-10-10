"""Shared helpers for HomiAgent Bridge entities."""
from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import DeviceInfo

from .const import DOMAIN


def build_device_info(entry: ConfigEntry) -> DeviceInfo:
    """Build the DeviceInfo shared by every entity of a config entry."""
    return DeviceInfo(
        identifiers={(DOMAIN, entry.entry_id)},
        name=entry.title,
        manufacturer="HomiAgent",
        model="HomiAgent Bridge",
    )
