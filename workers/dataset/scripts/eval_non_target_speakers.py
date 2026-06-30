#!/usr/bin/env python3
"""CLI wrapper for non-target speaker centroid validation on mb/mc."""

from __future__ import annotations

import sys
from pathlib import Path

WORKERS_DATASET = Path(__file__).resolve().parents[1]
if str(WORKERS_DATASET) not in sys.path:
    sys.path.insert(0, str(WORKERS_DATASET))

from speechcraft_dataset.eval_non_target_speakers import main

if __name__ == "__main__":
    raise SystemExit(main())
