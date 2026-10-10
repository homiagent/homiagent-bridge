# HomiAgent Bridge

Custom Home Assistant integration, installable via [HACS](https://hacs.xyz) as a
custom repository, that connects this Home Assistant instance to your HomiAgent
account so the assistant can see and control your devices.

## How it works

- On setup, the integration connects to HomiAgent using the pairing key you provide.
- It then executes commands sent by HomiAgent via `hass.services.async_call`,
  reporting success/failure back.
- If the connection drops, it reconnects automatically.
- A `binary_sensor.<entry>_connection` entity (device class `connectivity`) reflects
  whether the link is currently up.

## Installation

### Via HACS (custom repository)

1. In HACS, open the **⋮** menu (top right) and choose **Custom repositories**.
2. Add `https://github.com/homiagent/homiagent-bridge` with category **Integration**.
3. Install **HomiAgent Bridge**, then restart Home Assistant.

### Manual

Copy `custom_components/homiagent_bridge` into your Home Assistant `config/custom_components/`
directory and restart Home Assistant.

## Configuration

Add the integration from **Settings → Devices & Services → Add Integration → HomiAgent Bridge**
and paste the pairing key from the "Connect a home" screen in your HomiAgent dashboard.

If the connection is ever lost and the key has expired, use **Reconfigure** on this
integration and paste a fresh one.

