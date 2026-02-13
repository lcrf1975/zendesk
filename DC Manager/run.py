#!/usr/bin/env python3
"""
Entry point for Zendesk DC Manager.

This is the main entry script that launches the application.
Can be run directly: python run.py
"""

import sys

from zendesk_dc_manager.main import main


if __name__ == "__main__":
    sys.exit(main())