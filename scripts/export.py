# -*- coding: utf-8 -*-
from iptvtester.config import load_config
from iptvtester.pipeline import run_once

def main():
    cfg = load_config()
    run_once(cfg)

if __name__ == "__main__":
    main()
