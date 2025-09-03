# -*- coding: utf-8 -*-
from __future__ import annotations
import sys

from .config import load_config
from .pipeline import run_once

def main() -> int:
    cfg = load_config()
    run_once(cfg)
    return 0

if __name__ == "__main__":
    sys.exit(main())
