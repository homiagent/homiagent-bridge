"""WebSocket client that connects this Home Assistant instance to HomiAgent and
executes incoming `command` messages against it.

Wire protocol:
  -> hello       {credential, ha_instance_uuid, ha_version, snapshot_hash, time_zone}
                  (snapshot_hash: see _compute_snapshot_hash — the backend compares it
                  with the hash of what it has stored for this connection. time_zone is
                  this HA's configured IANA zone, which the backend uses to tell where a
                  day starts for the user)
  <- hello_ack   {connection_id, session_credential?, snapshot_required}
                  (session_credential only right after a Pairing credential's first
                  successful use — must be persisted and used for every reconnect from
                  then on, the Pairing key gets revoked server-side the moment this
                  happens. snapshot_required false means the backend already holds
                  exactly this area/entity state, so both snapshots are skipped; treated
                  as true when absent)
  <- hello_reject {reason}                               (bad/expired/revoked credential
                  — not something a reconnect retry will fix by itself)
  <- command     {command_id, domain, service, entity_id, service_data}
  -> result      {command_id, success, data, error}
  -> entity_snapshot        {entities: [{entity_id, friendly_name, domain,
                              device_class, area_id, device_name, device_id,
                              ha_registry_id, is_ignored, entity_type}, ...]}
                              (sent once, right after hello_ack when snapshot_required —
                              the full current entity list; the backend treats this as a
                              mark-and-sweep reconciliation)
  -> entity_registry_event  {action: "create"|"update"|"remove", entity_id,
                              friendly_name, domain, device_class, area_id,
                              device_name, device_id, ha_registry_id, is_ignored,
                              entity_type}
                              (sent whenever HA's entity registry changes, near-real-time)
  -> area_snapshot          {areas: [{area_id, name}, ...]}   (sent once, right after
                              hello_ack, same mark-and-sweep reconciliation as
                              entity_snapshot)
  -> area_registry_event    {action: "create"|"update"|"delete", area_id, name}
                              (sent whenever HA's area registry changes)
  <- set_ignore_label       {request_id, entity_id, ignored}   (adds/removes the
                              IGNORE_LABEL on that entity, preserving any other labels)
  -> set_ignore_label_result {request_id, success, error}
  <- get_state              {request_id, entity_id}   (a pure read — unlike `command`,
                              never calls a service)
  -> state_result           {request_id, success, state, attributes, error}
  <- get_states             {request_id, entity_ids}  (batched get_state — one request_id,
                              many entity_ids, one reply; saves N separate round trips
                              for what's the same trivial synchronous lookup done N times)
  -> states_result          {request_id, states: [{entity_id, success, state, attributes,
                              error}, ...]}
  <- get_history            {request_id, entity_id, hours_back}   (a pure read of past
                              state changes — hours_back rather than absolute timestamps
                              since neither side has a reliable notion of "now" in the
                              other's timezone)
  -> history_result         {request_id, success, points: [{timestamp, state}, ...],
                              summary: {count, first_state, last_state, min_state,
                              max_state, is_numeric, delta} | null, error}
  -> error_report           {source, message, occurred_at}   (an error this client hit —
                              a failed command, a dropped connection, a malformed message.
                              Queued locally (HA's own Store helper) whenever there's no
                              open connection to send over, and flushed in order right
                              after the next hello_ack — occurred_at (this client's own
                              clock) can therefore be well before the time the backend
                              actually receives it.)
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import timedelta
from functools import partial
from typing import Any

import aiohttp
from homeassistant.components import recorder
from homeassistant.components.recorder import history
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import Event, HomeAssistant
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import label_registry as lr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.json import JSONEncoder as HAJSONEncoder
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util
from homeassistant.util import slugify

# The backend's tool result goes straight into an LLM's context — a sensor that updates
# every few minutes over a week can produce thousands of raw state-change rows, far more
# than useful (or affordable) to hand to a model. Downsampled to this many points, always
# keeping the first and last.
MAX_HISTORY_POINTS = 200

# Device-registry fields mirrored into each HaEntity through _build_entity_item.
# Home Assistant's device_registry_updated event reports the previous values in
# "changes", so handlers must only use the keys as a relevance filter and then
# read the registry again for the current values.
MIRRORED_DEVICE_FIELDS = frozenset({"area_id", "name", "name_by_user"})

# Bounds how much a permanently-unreachable backend (or a rejected credential, which keeps
# retrying forever — see HelloRejected) can make the on-disk error queue grow while this
# client has nothing to flush it to. Oldest entries are dropped first once this is hit.
MAX_QUEUED_ERRORS = 200

# A caught exception's str() is occasionally huge (a full HTTP response body, a stack
# dump embedded in the message) — capped so one bad error report can't balloon the queued
# payload or the eventual DB row.
MAX_ERROR_MESSAGE_LENGTH = 2000

# Storage version for the persisted error queue (Store's own schema-migration hook,
# unrelated to the backend's FluentMigrator numbering) — bump only if the stored shape
# (a list of {source, message, occurred_at} dicts) ever changes incompatibly.
ERROR_STORE_VERSION = 1

# Must match HaSnapshotHasher.Version on the backend. Bump both together whenever the
# canonical format in _compute_snapshot_hash changes — old stored hashes then simply
# stop matching and the next connect falls back to a full snapshot.
SNAPSHOT_HASH_VERSION = "homi-snapshot-v1"

from .const import (
    DOMAIN,
    ENTITY_TYPE_LABELS,
    HEARTBEAT_SECONDS,
    IGNORE_LABEL,
    RECONNECT_MAX_DELAY,
    RECONNECT_MIN_DELAY,
    connection_status_signal,
    last_command_signal,
)

_LOGGER = logging.getLogger(__name__)


class HelloRejected(Exception):
    """The server rejected our credential — retrying with the same one won't help."""


class HomiAgentBridgeClient:
    """Maintains a persistent connection to the HomiAgent server."""

    def __init__(
        self,
        hass: HomeAssistant,
        url: str,
        credential: str,
        ha_instance_uuid: str,
        entry_id: str,
        on_credential_rotated: Callable[[str], Awaitable[None]],
    ) -> None:
        self.hass = hass
        self._url = url
        self._credential = credential
        self._ha_instance_uuid = ha_instance_uuid
        # Only for routing dispatcher signals to this entry's own sensors — a purely
        # local HA concept, unrelated to ha_instance_uuid (which identifies us to the
        # backend and is what survives a from-scratch re-pair; entry_id doesn't).
        self._entry_id = entry_id
        self._on_credential_rotated = on_credential_rotated
        self._session: aiohttp.ClientSession = async_get_clientsession(hass)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False
        self.connected = False
        self.connection_id: str | None = None
        # Persists queued error reports across a HA restart, not just a dropped
        # connection — see _report_error/_flush_error_queue. Loaded into
        # _error_queue once, at the start of _run_forever.
        self._error_store: Store[list[dict[str, Any]]] = Store(
            hass, ERROR_STORE_VERSION, f"{DOMAIN}_{entry_id}_errors"
        )
        self._error_queue: list[dict[str, Any]] = []
        self._unsub_registry_listener: Callable[[], None] | None = None
        self._unsub_device_registry_listener: Callable[[], None] | None = None
        self._unsub_area_registry_listener: Callable[[], None] | None = None
        self._unsub_started_listener: Callable[[], None] | None = None
        # One-shot state listeners for entities whose registry create/update arrived
        # before HA wrote their first state — see _resend_when_state_appears.
        self._pending_state_listeners: dict[str, Callable[[], None]] = {}
        self._snapshot_required = True

    def start(self) -> None:
        """Start the background connect/listen loop — deferred until Home Assistant
        itself has finished starting when it hasn't yet (not just this integration).
        Without this, a full HA restart can race: config entries are set up in
        parallel, so this integration could open its connection and send the very
        first entity_snapshot (a mark-and-sweep — anything not in it gets deleted)
        before a slower-loading integration (e.g. a Zigbee coordinator) has populated
        its entities in hass.states, wiping out everything that hadn't loaded yet."""
        self._stopping = False
        if self.hass.is_running:
            self._task = self.hass.loop.create_task(self._run_forever())
        else:
            self._unsub_started_listener = self.hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STARTED, self._start_after_ha_started
            )
        # Registered once, independent of connect/reconnect cycles — a change that
        # fires while briefly disconnected is simply dropped (self._send no-ops), the
        # next reconnect's entity_snapshot/area_snapshot reconciles it instead.
        self._unsub_registry_listener = self.hass.bus.async_listen(
            er.EVENT_ENTITY_REGISTRY_UPDATED, self._handle_registry_event
        )
        self._unsub_device_registry_listener = self.hass.bus.async_listen(
            dr.EVENT_DEVICE_REGISTRY_UPDATED, self._handle_device_registry_event
        )
        self._unsub_area_registry_listener = self.hass.bus.async_listen(
            ar.EVENT_AREA_REGISTRY_UPDATED, self._handle_area_registry_event
        )

    def _start_after_ha_started(self, event: Event) -> None:
        self._unsub_started_listener = None
        if not self._stopping:
            self._task = self.hass.loop.create_task(self._run_forever())

    async def stop(self) -> None:
        """Stop the client and close the connection."""
        self._stopping = True
        if self._unsub_started_listener:
            self._unsub_started_listener()
            self._unsub_started_listener = None
        if self._unsub_registry_listener:
            self._unsub_registry_listener()
            self._unsub_registry_listener = None
        if self._unsub_device_registry_listener:
            self._unsub_device_registry_listener()
            self._unsub_device_registry_listener = None
        if self._unsub_area_registry_listener:
            self._unsub_area_registry_listener()
            self._unsub_area_registry_listener = None
        for unsub in self._pending_state_listeners.values():
            unsub()
        self._pending_state_listeners.clear()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._set_connected(False)

    def _set_connected(self, connected: bool) -> None:
        if self.connected != connected:
            self.connected = connected
            async_dispatcher_send(
                self.hass, connection_status_signal(self._entry_id), connected
            )

    async def _run_forever(self) -> None:
        self._error_queue = await self._load_error_queue()
        delay = RECONNECT_MIN_DELAY
        while not self._stopping:
            try:
                await self._connect_and_listen()
                delay = RECONNECT_MIN_DELAY
            except HelloRejected as err:
                _LOGGER.error(
                    "HomiAgent server rejected our credential (%s) — fix this from "
                    "Settings > Devices & Services > HomiAgent Bridge > Reconfigure "
                    "instead of waiting; this client will keep retrying but a rejected "
                    "credential won't fix itself.",
                    err,
                )
                await self._report_error("hello_reject", str(err))
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("HomiAgent Bridge connection error: %s", err)
                await self._report_error("connection", str(err))
            finally:
                self._set_connected(False)

            if self._stopping:
                return

            jitter = random.uniform(0, delay * 0.25)
            _LOGGER.debug("Reconnecting to %s in %.1fs", self._url, delay + jitter)
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    async def _connect_and_listen(self) -> None:
        _LOGGER.debug("Connecting to %s", self._url)
        async with self._session.ws_connect(
            self._url, heartbeat=HEARTBEAT_SECONDS
        ) as ws:
            self._ws = ws

            # Built once, before hello: the hash goes in hello itself, and the very same
            # lists are what gets sent if the backend says its copy differs.
            areas = self._build_area_items()
            entities = self._build_entity_items()

            await self._send_hello(_compute_snapshot_hash(areas, entities))
            if not await self._await_hello_response(ws):
                return

            self._set_connected(True)
            _LOGGER.info("Connected to HomiAgent server at %s", self._url)
            await self._flush_error_queue()
            self._ensure_ignore_label_exists()
            self._ensure_entity_type_labels_exist()
            if self._snapshot_required:
                await self._send({"type": "area_snapshot", "areas": areas})
                await self._send({"type": "entity_snapshot", "entities": entities})
            else:
                _LOGGER.debug("Backend already has this area/entity state, skipping snapshots")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(msg.data)
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.ERROR,
                ):
                    break

        self._ws = None
        _LOGGER.info("Disconnected from HomiAgent server at %s", self._url)

    async def _send_hello(self, snapshot_hash: str) -> None:
        await self._send(
            {
                "type": "hello",
                "credential": self._credential,
                "ha_instance_uuid": self._ha_instance_uuid,
                "ha_version": HA_VERSION,
                "snapshot_hash": snapshot_hash,
                "time_zone": self.hass.config.time_zone,
            }
        )

    async def _await_hello_response(self, ws: aiohttp.ClientWebSocketResponse) -> bool:
        """Waits for hello_ack/hello_reject. Returns False if the caller should give up
        on this connection attempt (already logged/handled); raises HelloRejected if the
        credential itself was the problem."""
        msg = await ws.receive(timeout=10)

        if msg.type != aiohttp.WSMsgType.TEXT:
            _LOGGER.warning("Connection closed before a hello response arrived")
            await self._report_error("hello", "Connection closed before a hello response arrived")
            return False

        try:
            response = json.loads(msg.data)
        except ValueError:
            _LOGGER.warning("Malformed hello response: %s", msg.data)
            await self._report_error("hello", f"Malformed hello response: {msg.data}")
            return False

        response_type = response.get("type")

        if response_type == "hello_reject":
            raise HelloRejected(response.get("reason", "no reason given"))

        if response_type != "hello_ack":
            _LOGGER.warning("Unexpected first message from server: %s", response_type)
            await self._report_error("hello", f"Unexpected first message from server: {response_type}")
            return False

        self.connection_id = response.get("connection_id")
        self._snapshot_required = response.get("snapshot_required", True) is not False

        new_credential = response.get("session_credential")
        if new_credential:
            self._credential = new_credential
            await self._on_credential_rotated(new_credential)
            _LOGGER.debug("Pairing key promoted to a session credential")

        return True

    async def _handle_message(self, raw: str) -> None:
        try:
            message = json.loads(raw)
        except ValueError:
            _LOGGER.warning("Received malformed message: %s", raw)
            await self._report_error("message_parse", f"Malformed message: {raw}")
            return

        message_type = message.get("type")
        if message_type == "command":
            await self._execute_command(message)
        elif message_type == "set_ignore_label":
            await self._set_ignore_label(message)
        elif message_type == "get_state":
            await self._get_state(message)
        elif message_type == "get_states":
            await self._get_states(message)
        elif message_type == "get_history":
            await self._get_history(message)
        else:
            _LOGGER.debug("Ignoring message of type %s", message_type)

    async def _execute_command(self, message: dict[str, Any]) -> None:
        command_id = message.get("command_id")
        domain = message.get("domain")
        service = message.get("service")
        entity_id = message.get("entity_id")
        service_data = dict(message.get("service_data") or {})

        if entity_id:
            service_data["entity_id"] = entity_id

        success = True
        error: str | None = None

        try:
            await self.hass.services.async_call(
                domain, service, service_data, blocking=True
            )
            _LOGGER.info(
                "Executed %s.%s (entity_id=%s) via HomiAgent Bridge",
                domain, service, entity_id,
            )
        except Exception as err:  # noqa: BLE001
            success = False
            error = str(err)
            _LOGGER.warning(
                "Failed to execute %s.%s (entity_id=%s): %s",
                domain, service, entity_id, err,
            )
            await self._report_error("execute_command", f"{domain}.{service} (entity_id={entity_id}): {error}")

        await self._log_activity(domain, service, entity_id, success, error)
        self._notify_last_command(domain, service, entity_id, success, error)

        await self._send(
            {
                "type": "result",
                "command_id": command_id,
                "success": success,
                "data": None,
                "error": error,
            }
        )

    def _notify_last_command(
        self,
        domain: str,
        service: str,
        entity_id: str | None,
        success: bool,
        error: str | None,
    ) -> None:
        """Push this command to the 'Last command' sensor so it shows in the device's Activity card."""
        async_dispatcher_send(
            self.hass,
            last_command_signal(self._entry_id),
            {
                "domain": domain,
                "service": service,
                "entity_id": entity_id,
                "success": success,
                "error": error,
                "timestamp": dt_util.utcnow().isoformat(),
            },
        )

    async def _log_activity(
        self,
        domain: str,
        service: str,
        entity_id: str | None,
        success: bool,
        error: str | None,
    ) -> None:
        """Write a Logbook entry so the action shows up as HA activity, not just a log line."""
        action = f"{domain}.{service}"
        target = f" on {entity_id}" if entity_id else ""
        message = (
            f"called {action}{target} via HomiAgent Bridge"
            if success
            else f"failed to call {action}{target} via HomiAgent Bridge: {error}"
        )

        service_data: dict[str, Any] = {"name": "HomiAgent Bridge", "message": message}
        if entity_id:
            service_data["entity_id"] = entity_id

        try:
            await self.hass.services.async_call("logbook", "log", service_data)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Could not write Logbook entry: %s", err)

    def _build_entity_items(self) -> list[dict[str, Any]]:
        """The entity_snapshot payload — sent right after hello_ack when the backend's
        copy differs; the backend reconciles its cache against this full list
        (mark-and-sweep), which is also what self-heals any entity_registry_event this
        client failed to deliver while disconnected. Every entity is included,
        IGNORE_LABEL ones too (as is_ignored=True in the payload) — the backend mirrors
        that into its own IsIgnored so the user can review/reconsider an ignored device
        from its own devices page instead of only from HA's UI."""
        return [
            self._build_entity_item(state.entity_id)
            for state in self.hass.states.async_all()
        ]

    def _build_entity_item(self, entity_id: str) -> dict[str, Any]:
        state = self.hass.states.get(entity_id)
        return {
            "entity_id": entity_id,
            "friendly_name": state.attributes.get("friendly_name") if state else None,
            "domain": entity_id.split(".", 1)[0],
            "device_class": self._resolve_device_class(entity_id),
            "area_id": self._resolve_area_id(entity_id),
            "device_name": self._resolve_device_name(entity_id),
            "device_id": self._resolve_device_id(entity_id),
            "ha_registry_id": self._resolve_registry_id(entity_id),
            "is_ignored": self._is_ignored(entity_id),
            "entity_type": self._resolve_entity_type(entity_id),
        }

    def _resolve_device_class(self, entity_id: str) -> str | None:
        """The user's own "Show as" choice lands in the entity registry immediately,
        while the state attribute only catches up once the entity rewrites its state —
        which can happen after entity_registry_updated reaches us. Reading the registry
        override first is what makes a "Show as" change arrive with the right value;
        the state attribute (the integration's own device_class) covers everything
        the user never overrode."""
        entry = er.async_get(self.hass).async_get(entity_id)
        if entry is not None and entry.device_class:
            return entry.device_class

        state = self.hass.states.get(entity_id)
        return state.attributes.get("device_class") if state else None

    def _resolve_registry_id(self, entity_id: str) -> str | None:
        """The entity registry's own stable id — unlike entity_id, this is generated by
        HA itself (not the integration, not the user) and never changes, including across
        an entity_id rename. The backend uses this instead of entity_id to recognize "the
        same entity" so a rename doesn't orphan display_name/device_class_override/
        is_ignored. None only for entities with no registry entry at all — which HA itself
        also never lets the user rename, so matching by entity_id alone is safe for
        those."""
        entry = er.async_get(self.hass).async_get(entity_id)
        return entry.id if entry else None

    def _resolve_area_id(self, entity_id: str) -> str | None:
        """Same resolution as _resolve_area (entity's own area, falling back to its
        device's), but the area registry's own stable id rather than its display name —
        unlike the name, HA never lets this change, including when the user renames the
        area. The backend uses this (not the name) to key HaArea and any area-scoped
        matching, for the same reason it uses ha_registry_id instead of entity_id."""
        entry = er.async_get(self.hass).async_get(entity_id)
        if entry is None:
            return None

        area_id = entry.area_id
        if area_id is None and entry.device_id:
            device = dr.async_get(self.hass).async_get(entry.device_id)
            area_id = device.area_id if device else None

        return area_id

    def _resolve_device_name(self, entity_id: str) -> str | None:
        """The parent device's own DISPLAY name — distinct from the entity's own
        friendly_name, which is often a generic per-entity label (e.g. "Switch 1")
        repeated across every multi-gang device in the house. Also distinct from
        _resolve_device_id below: this can change any time the user renames the device
        from HA's own UI, so the backend must never use it as a grouping/identity key —
        only to display. None when the entity has no parent device or the device has no
        name of its own."""
        entry = er.async_get(self.hass).async_get(entity_id)
        if entry is None or entry.device_id is None:
            return None

        device = dr.async_get(self.hass).async_get(entry.device_id)
        return device.name_by_user or device.name if device else None

    def _resolve_device_id(self, entity_id: str) -> str | None:
        """The device registry's own STABLE id (what HA's own device page URL is keyed
        on) — unlike _resolve_device_name above, this never changes even when the user
        renames the device, so it's what the backend groups an area's entities by (a
        Tuya AC unit split into a dozen separate switch/sensor entities all share one of
        these). None when the entity has no parent device at all — HA also never lets a
        device-less entity's grouping change, so falling back to the entity's own
        entity_id there is safe."""
        entry = er.async_get(self.hass).async_get(entity_id)
        return entry.device_id if entry is not None else None

    def _is_ignored(self, entity_id: str) -> bool:
        """Whether the "homi_ignore" label is on this entity — reported to the backend as
        is_ignored rather than used to exclude the entity from sync entirely, so the user
        can review/reconsider it from the devices page instead of only from HA's own UI
        (Settings > Entities > pick entity > Labels). Chosen over hidden_by (HA's existing
        "hide from dashboards" flag) because that already means something else to the
        user; this is a dedicated, explicit tag for this one purpose."""
        entry = er.async_get(self.hass).async_get(entity_id)
        return entry is not None and slugify(IGNORE_LABEL) in entry.labels

    def _resolve_entity_type(self, entity_id: str) -> str | None:
        """HomiAgent's own "what kind of thing is this, really" tag, read from whichever
        homi_entityType_* label (if any) is on this entity — see ENTITY_TYPE_LABELS' own
        comment for why this exists instead of overriding device_class directly. At most
        one of these labels is ever expected on an entity; if more than one somehow ended
        up applied (e.g. the user added a second one by hand), the first match wins rather
        than erroring — same fail-soft spirit as the rest of this best-effort resolution
        chain. None when no such label is present, which is the common case until the user
        picks a type for this specific entity. entry.labels holds label_ids (HA's own
        slugify(name)), not display names — comparing the mixed-case ENTITY_TYPE_LABELS
        value directly never matched, so entity_type silently stayed None."""
        entry = er.async_get(self.hass).async_get(entity_id)
        if entry is None:
            return None

        for entity_type, label in ENTITY_TYPE_LABELS.items():
            if slugify(label) in entry.labels:
                return entity_type

        return None

    async def _handle_registry_event(self, event: Event) -> None:
        """HA's own entity_registry_updated event — fires on create/update/remove,
        which is exactly the create/update/remove split entity_registry_event needs.
        Labeling/unlabeling IGNORE_LABEL fires as an ordinary "update" here — is_ignored
        in the payload carries that, same as a full snapshot."""
        action = event.data.get("action")
        entity_id = event.data.get("entity_id")
        if action not in ("create", "update", "remove") or not entity_id:
            return

        # HA starts coroutine listeners eagerly, so without yielding first this runs
        # before the entity's own registry callback has rewritten its state — the
        # payload would carry the pre-change friendly_name.
        await asyncio.sleep(0)

        if action == "remove":
            self._cancel_pending_state_listener(entity_id)
            payload: dict[str, Any] = {
                "entity_id": entity_id,
                "friendly_name": None,
                "domain": None,
                "device_class": None,
                "area_id": None,
                "device_name": None,
                "device_id": None,
                "ha_registry_id": None,
                "is_ignored": False,
                "entity_type": None,
            }
        else:
            payload = self._build_entity_item(entity_id)
            entry = er.async_get(self.hass).async_get(entity_id)
            # A disabled entity never gets a state, so waiting for one would just leak.
            if self.hass.states.get(entity_id) is None and (entry is None or entry.disabled_by is None):
                self._resend_when_state_appears(entity_id)

        _LOGGER.debug("Sending entity_registry_event %s for %s", action, entity_id)
        await self._send({"type": "entity_registry_event", "action": action, **payload})

    def _resend_when_state_appears(self, entity_id: str) -> None:
        """A registry create (e.g. "Show as" wrapping a switch into a brand new light
        entity) or an entity_id rename reaches us before the entity has written any
        state, so friendly_name/device_class go out as None. Waits for that entity's
        first state and re-sends it as an ordinary update, instead of leaving the
        backend with the empty values until the next reconnect's snapshot."""
        if entity_id in self._pending_state_listeners:
            return

        async def _on_state(event: Event) -> None:
            if event.data.get("new_state") is None:
                return
            self._cancel_pending_state_listener(entity_id)
            _LOGGER.debug("First state for %s arrived, re-sending entity_registry_event", entity_id)
            await self._send(
                {
                    "type": "entity_registry_event",
                    "action": "update",
                    **self._build_entity_item(entity_id),
                }
            )

        self._pending_state_listeners[entity_id] = async_track_state_change_event(
            self.hass, [entity_id], _on_state
        )

    def _cancel_pending_state_listener(self, entity_id: str) -> None:
        unsub = self._pending_state_listeners.pop(entity_id, None)
        if unsub is not None:
            unsub()

    async def _handle_device_registry_event(self, event: Event) -> None:
        """Mirror device-owned area/name changes into every attached HaEntity.

        An entity with no explicit area inherits its device's area, but changing the
        device fires device_registry_updated rather than entity_registry_updated. Reuse
        the existing entity event wire protocol so the backend resolves and persists
        the new HaAreaId/DeviceName without needing a second update path.
        """
        action = event.data.get("action")
        device_id = event.data.get("device_id")
        changes = event.data.get("changes")
        if (
            action != "update"
            or not device_id
            or not isinstance(changes, dict)
            or MIRRORED_DEVICE_FIELDS.isdisjoint(changes)
        ):
            return

        # Same reason as _handle_registry_event: let the entities rewrite their state
        # (a device rename changes their friendly_name) before reading it.
        await asyncio.sleep(0)

        registry = er.async_get(self.hass)
        for entry in er.async_entries_for_device(registry, device_id):
            _LOGGER.debug("Sending entity_registry_event update for %s (device change)", entry.entity_id)
            await self._send(
                {
                    "type": "entity_registry_event",
                    "action": "update",
                    **self._build_entity_item(entry.entity_id),
                }
            )

    async def _set_ignore_label(self, message: dict[str, Any]) -> None:
        """Backend-initiated: applies/removes IGNORE_LABEL from the devices page's
        IsIgnored toggle. Reads the entity's current labels first and only adds/removes
        our one label — entity_registry.labels is every label on the entity, not just
        ours, so overwriting it outright would silently drop any other label the user
        put there for their own reasons. Updating the registry here fires HA's own
        entity_registry_updated, which _handle_registry_event forwards back as an
        ordinary update — an idempotent echo, not a loop, since this method is only ever
        called from an incoming backend message, never from _handle_registry_event
        itself."""
        request_id = message.get("request_id")
        entity_id = message.get("entity_id")
        ignored = bool(message.get("ignored"))

        success = True
        error: str | None = None

        try:
            registry = er.async_get(self.hass)
            entry = registry.async_get(entity_id)
            if entry is None:
                raise ValueError(f"Unknown entity {entity_id}")

            labels = set(entry.labels)
            if ignored:
                labels.add(slugify(IGNORE_LABEL))
            else:
                labels.discard(slugify(IGNORE_LABEL))

            registry.async_update_entity(entity_id, labels=labels)
        except Exception as err:  # noqa: BLE001
            success = False
            error = str(err)
            _LOGGER.warning("Failed to set ignore label for %s: %s", entity_id, err)
            await self._report_error("set_ignore_label", f"{entity_id}: {error}")

        await self._send(
            {
                "type": "set_ignore_label_result",
                "request_id": request_id,
                "success": success,
                "error": error,
            }
        )

    async def _get_state(self, message: dict[str, Any]) -> None:
        """A pure read — unlike _execute_command, never calls a service. Used by the
        backend's LLM-with-tools fallback (get_entity_state) for questions like "what's
        the temperature in the bedroom", which isn't a command at all."""
        request_id = message.get("request_id")
        entity_id = message.get("entity_id")

        state_obj = self.hass.states.get(entity_id) if entity_id else None

        if state_obj is None:
            await self._send(
                {
                    "type": "state_result",
                    "request_id": request_id,
                    "success": False,
                    "state": None,
                    "attributes": None,
                    "error": f"Unknown entity {entity_id}",
                }
            )
            return

        await self._send(
            {
                "type": "state_result",
                "request_id": request_id,
                "success": True,
                "state": state_obj.state,
                "attributes": dict(state_obj.attributes),
                "error": None,
            }
        )

    async def _get_states(self, message: dict[str, Any]) -> None:
        """Batched counterpart to _get_state — one request_id, many entity_ids, one
        states_result reply. hass.states.get() is a synchronous in-memory lookup, so
        batching these saves N separate WebSocket round trips (each with its own
        request_id correlation and JSON encode/decode) for what's otherwise the same
        trivial work done N times — /devices' device detail view is the first caller (a
        device split into a couple dozen HA entities used to fetch each one's state with
        its own round trip, which was exactly why opening a 26-entity device group took
        so much longer than a 4-entity one)."""
        request_id = message.get("request_id")
        entity_ids = message.get("entity_ids") or []

        states = []
        for entity_id in entity_ids:
            state_obj = self.hass.states.get(entity_id)
            if state_obj is None:
                states.append(
                    {
                        "entity_id": entity_id,
                        "success": False,
                        "state": None,
                        "attributes": None,
                        "error": f"Unknown entity {entity_id}",
                    }
                )
            else:
                states.append(
                    {
                        "entity_id": entity_id,
                        "success": True,
                        "state": state_obj.state,
                        "attributes": dict(state_obj.attributes),
                        "error": None,
                    }
                )

        await self._send({"type": "states_result", "request_id": request_id, "states": states})

    async def _get_history(self, message: dict[str, Any]) -> None:
        """A pure read of past state changes — the generic counterpart to _get_state,
        used by the backend's get_entity_history tool for any question about the past
        (trends, "was X on at some point", consumption over a period for a
        strictly-increasing counter like an energy meter). hours_back rather than an
        absolute start/end, same reasoning as the module docstring's protocol note."""
        request_id = message.get("request_id")
        entity_id = message.get("entity_id")
        hours_back = message.get("hours_back") or 24

        if not entity_id:
            await self._send(
                {
                    "type": "history_result",
                    "request_id": request_id,
                    "success": False,
                    "points": None,
                    "summary": None,
                    "error": "Missing entity_id",
                }
            )
            return

        end_time = dt_util.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        try:
            recorder_instance = recorder.get_instance(self.hass)
            states_by_entity = await recorder_instance.async_add_executor_job(
                partial(
                    history.state_changes_during_period,
                    self.hass,
                    start_time,
                    end_time,
                    entity_id,
                    no_attributes=True,
                    include_start_time_state=True,
                )
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Failed to read history for %s: %s", entity_id, err)
            await self._report_error("get_history", f"{entity_id}: {err}")
            await self._send(
                {
                    "type": "history_result",
                    "request_id": request_id,
                    "success": False,
                    "points": None,
                    "summary": None,
                    "error": str(err),
                }
            )
            return

        points = [
            {"timestamp": dt_util.as_local(state.last_changed).isoformat(), "state": state.state}
            for state in states_by_entity.get(entity_id, [])
        ]
        points = _downsample_history_points(points, MAX_HISTORY_POINTS)

        await self._send(
            {
                "type": "history_result",
                "request_id": request_id,
                "success": True,
                "points": points,
                "summary": _summarize_history_points(points),
                "error": None,
            }
        )

    def _ensure_ignore_label_exists(self) -> None:
        """Runs on every (re)connect, before any snapshot — so IGNORE_LABEL always has a
        real LabelEntry (color/description slot) instead of depending on the user having
        created it by hand first, which is what this addon required before. Idempotent:
        a no-op once the label exists. async_get_label looks up by label_id, which HA
        derives from the given name via slugify — checking with the raw name instead
        (as this used to) only happens to work here because IGNORE_LABEL is already a
        valid slug; see _ensure_entity_type_labels_exist for why that assumption breaks
        for a mixed-case name."""
        registry = lr.async_get(self.hass)
        if registry.async_get_label(slugify(IGNORE_LABEL)) is None:
            registry.async_create(name=IGNORE_LABEL)

    def _ensure_entity_type_labels_exist(self) -> None:
        """Same reasoning as _ensure_ignore_label_exists, one per ENTITY_TYPE_LABELS value
        — so a user picking "Luz" for an entity (once the picker exists) always finds a
        real label waiting, never has to create it by hand first. Idempotent — but only
        once the existence check actually looks up the real label_id: async_get_label
        takes a label_id (HA's own slugify(name), e.g. "homi_entitytype_switch"), not the
        display name — passing the mixed-case ENTITY_TYPE_LABELS value straight through
        (as this used to) never matched the already-created label on a later reconnect,
        so this ran async_create again every time and HA rejected the now-duplicate slug
        with "The name ... is already in use"."""
        registry = lr.async_get(self.hass)
        for label in ENTITY_TYPE_LABELS.values():
            if registry.async_get_label(slugify(label)) is None:
                registry.async_create(name=label)

    def _build_area_items(self) -> list[dict[str, Any]]:
        """The area_snapshot payload — sent right before entity_snapshot when the
        backend's copy differs, same mark-and-sweep reconciliation shape, just for HaArea
        instead of ha_entity. Areas with zero entities still show up here, unlike a
        "distinct area" derived from ha_entity would."""
        return [
            {"area_id": area.id, "name": area.name}
            for area in ar.async_get(self.hass).async_list_areas()
        ]

    async def _handle_area_registry_event(self, event: Event) -> None:
        """HA's own area_registry_updated event — same create/update/delete shape as
        entity registry events, just for areas."""
        action = event.data.get("action")
        area_id = event.data.get("area_id")
        if action not in ("create", "update", "delete") or not area_id:
            return

        if action == "delete":
            payload: dict[str, Any] = {"area_id": area_id, "name": None}
        else:
            area = ar.async_get(self.hass).async_get_area(area_id)
            payload = {"area_id": area_id, "name": area.name if area else None}

        await self._send({"type": "area_registry_event", "action": action, **payload})

    async def _report_error(self, source: str, message: str) -> None:
        """Queues an error for delivery to the backend as an error_report — persisted to
        disk immediately (survives a HA restart, not just a dropped connection) and
        flushed right away if a connection happens to be open. Never raises: this is
        called from exception handlers all over this module, and a failure here (a disk
        write error, say) must not mask or replace the original error being reported."""
        self._error_queue.append(
            {
                "source": source,
                "message": message[:MAX_ERROR_MESSAGE_LENGTH],
                "occurred_at": dt_util.utcnow().isoformat(),
            }
        )
        if len(self._error_queue) > MAX_QUEUED_ERRORS:
            self._error_queue = self._error_queue[-MAX_QUEUED_ERRORS:]

        try:
            await self._save_error_queue()
            await self._flush_error_queue()
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Could not persist/flush the error queue: %s", err)

    async def _load_error_queue(self) -> list[dict[str, Any]]:
        data = await self._error_store.async_load()
        return data or []

    async def _save_error_queue(self) -> None:
        await self._error_store.async_save(self._error_queue)

    async def _flush_error_queue(self) -> None:
        """Sends every queued error_report in order, oldest first. Stops (leaving the
        remainder queued for the next reconnect) the moment the socket isn't open for
        writing — checked before every single send, not just once, since _send() itself
        silently no-ops on a closed socket instead of raising: without this check here, a
        mid-flush disconnect would still let this loop "pop" every remaining entry as if
        it had been sent, silently losing them instead of keeping them queued. Each
        actually-sent entry is popped and persisted immediately so a partial flush
        followed by another drop doesn't resend what already made it out."""
        while self._error_queue:
            if self._ws is None or self._ws.closed:
                return
            entry = self._error_queue[0]
            await self._send({"type": "error_report", **entry})
            self._error_queue.pop(0)
            await self._save_error_queue()

    async def _send(self, payload: dict[str, Any]) -> None:
        if self._ws is None or self._ws.closed:
            return
        # Plain json.dumps chokes on the non-stdlib-JSON types HA entities routinely put
        # in state.attributes (datetime, Decimal, set/frozenset, Enum members, etc.) —
        # state_result is the payload most likely to carry one straight from
        # state_obj.attributes. An uncaught TypeError here propagates out of the message
        # handler and up through the `async for msg in ws` loop, which _run_forever's
        # broad except then treats as a connection error: the whole socket drops and
        # reconnects, so the pending get_state request on the backend side genuinely never
        # gets a reply and times out (the read wasn't lying about failing — this is why).
        # HA's own JSONEncoder is what the WebSocket API and recorder use for exactly this.
        await self._ws.send_str(json.dumps(payload, cls=HAJSONEncoder))


def _compute_snapshot_hash(
    areas: list[dict[str, Any]], entities: list[dict[str, Any]]
) -> str:
    """SHA-256 of a canonical text form of the area + entity snapshot, byte-for-byte
    identical to HaSnapshotHasher.Compute on the backend (which hashes what it has
    stored). Any change here must be mirrored there and SNAPSHOT_HASH_VERSION bumped.

    Every string field is length-prefixed with its UTF-8 byte count ("s<n>:<value>",
    "n" for null) so no separator can be ambiguous, and byte count rather than len()
    because Python counts code points while .NET counts UTF-16 units. Records are
    ordered by area_id/entity_id — ASCII slugs, so Python's and .NET's ordinal sorts
    agree."""

    def _field(value: Any) -> str:
        if value is None:
            return "n"
        text = str(value)
        return f"s{len(text.encode('utf-8'))}:{text}"

    lines = [SNAPSHOT_HASH_VERSION, f"A{len(areas)}"]
    for area in sorted(areas, key=lambda a: a["area_id"]):
        lines.append(_field(area["area_id"]) + _field(area["name"]))

    lines.append(f"E{len(entities)}")
    for entity in sorted(entities, key=lambda e: e["entity_id"]):
        lines.append(
            _field(entity["entity_id"])
            + _field(entity["friendly_name"])
            + _field(entity["domain"])
            + _field(entity["device_class"])
            + _field(entity["area_id"])
            + _field(entity["device_name"])
            + _field(entity["device_id"])
            + _field(entity["ha_registry_id"])
            + ("t" if entity["is_ignored"] else "f")
            + _field(entity["entity_type"])
        )

    canonical = "\n".join(lines) + "\n"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _downsample_history_points(points: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
    """Evenly thins a long history list down to at most max_points, always keeping the
    first and last point. See MAX_HISTORY_POINTS' own comment for why."""
    if len(points) <= max_points:
        return points

    step = (len(points) - 1) / (max_points - 1)
    keep_indices = {round(i * step) for i in range(max_points)}
    return [point for i, point in enumerate(points) if i in keep_indices]


def _summarize_history_points(points: list[dict[str, Any]]) -> dict[str, Any] | None:
    """First/last/min/max plus delta (last minus first) for a numeric series — the one
    thing that directly answers "how much did X consume this window" for a
    strictly-increasing counter like an energy meter, without the LLM having to reduce a
    raw list itself. Falls back to a non-numeric summary (no min/max/delta) for anything
    that isn't a plain number, e.g. "on"/"off" or "unavailable"."""
    if not points:
        return None

    numeric_values: list[float] = []
    is_numeric = True
    for point in points:
        try:
            numeric_values.append(float(point["state"]))
        except (TypeError, ValueError):
            is_numeric = False
            break

    if not is_numeric or not numeric_values:
        return {
            "count": len(points),
            "first_state": points[0]["state"],
            "last_state": points[-1]["state"],
            "min_state": None,
            "max_state": None,
            "is_numeric": False,
            "delta": None,
        }

    return {
        "count": len(points),
        "first_state": points[0]["state"],
        "last_state": points[-1]["state"],
        "min_state": str(min(numeric_values)),
        "max_state": str(max(numeric_values)),
        "is_numeric": True,
        "delta": numeric_values[-1] - numeric_values[0],
    }
