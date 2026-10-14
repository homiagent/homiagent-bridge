"""Config flow for HomiAgent Bridge.

Only asks for the pairing key — there's one HomiAgent backend, not a server address
the user picks, so BACKEND_WS_URL is fixed in code (see const.py).
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import aiohttp
import voluptuous as vol
from homeassistant.config_entries import ConfigFlow
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.instance_id import async_get as async_get_instance_id

from .const import BACKEND_WS_URL, CONF_CREDENTIAL, CONF_PREVIOUS_CREDENTIAL, DOMAIN


def _schema() -> vol.Schema:
    """Build the form schema. Never pre-filled — a pairing key is single-use and the
    one that created this entry has already been consumed (promoted to a session
    credential), so re-showing it would be stale."""
    return vol.Schema({vol.Required(CONF_CREDENTIAL): str})


class HelloRejectedDuringSetup(Exception):
    """The server understood the handshake but rejected the credential."""


async def _test_connection(hass: HomeAssistant, credential: str) -> tuple[str, str | None]:
    """Opens a WebSocket, runs the real hello handshake, then closes it. Returns
    (connection_id, session_credential) on success — session_credential is only set
    when this handshake just promoted a one-shot pairing key, and the caller must
    persist it as the new credential (the pairing key is revoked server-side the
    moment this handshake succeeds, so re-using it afterward would fail). Raises
    HelloRejectedDuringSetup if the credential itself is the problem, otherwise lets
    aiohttp/asyncio errors propagate for the generic "cannot_connect" case."""
    session = async_get_clientsession(hass)
    async with asyncio.timeout(10):
        async with session.ws_connect(BACKEND_WS_URL) as ws:
            await ws.send_str(
                json.dumps(
                    {
                        "type": "hello",
                        "credential": credential,
                        "ha_instance_uuid": await async_get_instance_id(hass),
                        "ha_version": HA_VERSION,
                    }
                )
            )

            msg = await ws.receive()
            if msg.type != aiohttp.WSMsgType.TEXT:
                raise ConnectionError("Connection closed before a hello response arrived")

            response = json.loads(msg.data)
            if response.get("type") == "hello_reject":
                raise HelloRejectedDuringSetup(response.get("reason", "no reason given"))
            if response.get("type") != "hello_ack":
                raise ConnectionError(f"Unexpected response type: {response.get('type')}")

            await ws.close()
            return response["connection_id"], response.get("session_credential")


class HomiAgentBridgeConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for HomiAgent Bridge."""

    VERSION = 1

    async def _async_validate(
        self, credential: str
    ) -> tuple[str | None, str | None, str | None]:
        """Returns (connection_id, session_credential, None) on success, or
        (None, None, error_code)."""
        try:
            connection_id, session_credential = await _test_connection(self.hass, credential)
        except HelloRejectedDuringSetup:
            return None, None, "invalid_credential"
        except (aiohttp.ClientError, ConnectionError, TimeoutError, asyncio.TimeoutError):
            return None, None, "cannot_connect"
        except Exception:  # noqa: BLE001
            return None, None, "unknown"
        return connection_id, session_credential, None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial setup step (Add Integration)."""
        errors: dict[str, str] = {}

        if user_input is not None:
            connection_id, session_credential, error = await self._async_validate(
                user_input[CONF_CREDENTIAL]
            )
            if error:
                errors["base"] = error
            else:
                # Using the server's own id for this connection as our unique_id stops
                # it from being paired twice as two separate HA config entries.
                await self.async_set_unique_id(connection_id)
                self._abort_if_unique_id_configured()
                # The pairing key just typed was revoked the moment _test_connection's
                # handshake succeeded — store the session credential it was promoted
                # to, not the now-dead pairing key, or the real connection in
                # __init__.py would be rejected from the very first attempt.
                data = {
                    **user_input,
                    CONF_CREDENTIAL: session_credential or user_input[CONF_CREDENTIAL],
                }
                return self.async_create_entry(title="HomiAgent Bridge", data=data)

        return self.async_show_form(
            step_id="user", data_schema=_schema(), errors=errors
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle editing an existing entry (the "Configure" gear icon) — how the user
        recovers from a revoked/expired credential by pasting a fresh pairing key from
        "Regenerate" on the web."""
        errors: dict[str, str] = {}

        if user_input is not None:
            _connection_id, session_credential, error = await self._async_validate(
                user_input[CONF_CREDENTIAL]
            )
            if error:
                errors["base"] = error
            else:
                data_updates = {
                    **user_input,
                    CONF_CREDENTIAL: session_credential or user_input[CONF_CREDENTIAL],
                    # A fresh pairing starts a new credential lineage; the old pair
                    # belongs to the connection that was just replaced.
                    CONF_PREVIOUS_CREDENTIAL: None,
                }
                return self.async_update_reload_and_abort(
                    self._get_reconfigure_entry(),
                    data_updates=data_updates,
                )

        return self.async_show_form(
            step_id="reconfigure", data_schema=_schema(), errors=errors
        )
