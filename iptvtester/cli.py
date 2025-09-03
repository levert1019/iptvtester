from .config import load_config
from .pipeline import run_once

def main():
    cfg = load_config()
    run_once(cfg)
