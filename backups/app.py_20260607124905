#!/usr/bin/env python3
"""Runtime wrapper that loads Flask app from src/app.py for Gunicorn app:app."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / 'src'

for candidate in (str(SRC), str(ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

SRC_APP = SRC / 'app.py'
spec = importlib.util.spec_from_file_location('spicer_src_app', SRC_APP)
if spec is None or spec.loader is None:
    raise RuntimeError(f'Unable to load Flask app from {SRC_APP}')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
app = module.app

if __name__ == '__main__':
    from config import Config
    app.run(host='0.0.0.0', port=Config.FLASK_PORT, debug=Config.FLASK_DEBUG)
