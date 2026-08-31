
# API Handler GUI

  This is a code bundle for API Handler GUI. The original project is available at <https://www.figma.com/design/lYGBuwqNuldOQJAhAzUK8p/API-Handler-GUI>.

## Running the code

  Run `npm i` to install the dependencies.

  Start the Flask backend from the repository root so the GUI can call real menu functionality:

  `python3 src/app.py`

  Run `npm run dev` to start the development server.

  The Vite dev server proxies `/ops/*` requests to `http://127.0.0.1:5001`, so operation buttons in the GUI execute real `spicer_ops_menu.py` logic instead of placeholder alerts.

## Prompt-Free Actions

All interactive action inputs now use in-app modal dialogs. Browser `prompt` / `confirm` popups are no longer used.

## Hosting On Your Server

The Flask backend now serves this GUI at `/ops-gui` when a production build exists.

From repo root, build the hosted frontend bundle:

`./scripts/build_ops_gui.sh`

Then run or restart Flask and open:

`http://<your-server>:5001/ops-gui`

Full hosting notes:

`docs/OPS_GUI_HOSTING.md`
  