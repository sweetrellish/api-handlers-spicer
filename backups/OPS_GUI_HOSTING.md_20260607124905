# Ops GUI Hosting

This project can now serve the interactive Ops GUI directly from the Flask service under `/ops-gui`.

## Overview

- Frontend source: `API Handler Interactive GUI/`
- Built frontend output: `API Handler Interactive GUI/dist/`
- Flask serving route: `/ops-gui`
- Backend API used by GUI: `/ops/*`

Because both the frontend and API are on the same service origin when hosted this way, no extra CORS setup is required.

## Local Build

From repository root:

```bash
./scripts/build_ops_gui.sh
```

This runs a production build with base path `/ops-gui/` so static assets resolve correctly when served from that subpath.

## Local Run

From repository root:

```bash
python3 src/app.py
```

Then open:

- `http://127.0.0.1:5001/ops-gui`

## Server Deployment Flow

Use your normal server deployment path for this repo. After code sync on server:

```bash
cd /home/rellis/spicer
./scripts/build_ops_gui.sh
```

Then restart the Flask service so the running process serves the latest build artifacts.

## Root-required steps (run manually)

If your Flask service is systemd-managed, run this yourself on server:

```bash
sudo systemctl restart spicer-flask-api.service
sudo systemctl status spicer-flask-api.service --no-pager
```

## Verification

1. `curl -I http://127.0.0.1:5001/ops-gui`
2. Open `/ops-gui` in browser.
3. Trigger one safe read action (for example Queue Status) and verify output panel updates.

## Notes

- If `/ops-gui` returns a "build not found" JSON message, rebuild with `./scripts/build_ops_gui.sh`.
- Dev mode (`npm run dev`) still uses Vite and proxies `/ops` to `127.0.0.1:5001`.
