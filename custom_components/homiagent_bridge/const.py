"""Constants for the HomiAgent Bridge integration."""

import os

DOMAIN = "homiagent_bridge"

CONF_CREDENTIAL = "credential"

# HomiAgent is a hosted product — every install talks to the same company-operated
# backend, so this is a constant, not a form field (unlike a self-hosted server address
# would be). This file is public (HACS pulls it into every user's Home Assistant), so
# the checked-in default must be the real production endpoint, never a developer's own
# machine. Local development overrides it via env var instead of editing this line, so
# a personal IP never ends up in a commit.
BACKEND_WS_URL = os.environ.get(
    "HOMIAGENT_BRIDGE_BACKEND_WS_URL", "wss://api.homiagent.app/ws/ha"
)

# Reconnect backoff bounds, in seconds.
RECONNECT_MIN_DELAY = 2
RECONNECT_MAX_DELAY = 180

# How often aiohttp sends a WebSocket ping to keep the connection alive / detect drops.
HEARTBEAT_SECONDS = 60

# Applying this label to an entity in HA's own UI excludes it from the entity cache this
# bridge sends — see websocket_client.py's _is_ignored.
IGNORE_LABEL = "homi_ignore"

# HA's own "Show as" device_class override re-creates the entity under the new domain
# instead of just relabeling it — a real switch/outlet asked to "show as" a light doesn't
# become a light, it gets duplicated (old entity_id orphaned, new one created), which
# causes more problems than it solves for anything already relying on the old entity_id.
# These labels are HomiAgent's own parallel "what kind of thing is this, really" tag —
# applying one never touches device_class, so there's no duplication. At most one is ever
# on an entity at a time (see websocket_client.py's _resolve_entity_type/_set_entity_type).
ENTITY_TYPE_LABELS: dict[str, str] = {
    "switch": "homi_entityType_switch",
    "outlet": "homi_entityType_outlet",
    "cover": "homi_entityType_cover",
    "fan": "homi_entityType_fan",
    "light": "homi_entityType_light",
    "lock": "homi_entityType_lock",
    "siren": "homi_entityType_siren",
    "valve": "homi_entityType_valve",
}

_SIGNAL_CONNECTION_STATUS = f"{DOMAIN}_connection_status"
_SIGNAL_LAST_COMMAND = f"{DOMAIN}_last_command"


def connection_status_signal(entry_id: str) -> str:
    """Per-entry dispatcher signal for connectivity changes."""
    return f"{_SIGNAL_CONNECTION_STATUS}_{entry_id}"


def last_command_signal(entry_id: str) -> str:
    """Per-entry dispatcher signal for the most recently executed command."""
    return f"{_SIGNAL_LAST_COMMAND}_{entry_id}"
