"""
Start the QuickBooks Web Connector middleware service.

Usage:
  python execution/start_qbwc_service.py
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from qb_sync_service.app import run


if __name__ == "__main__":
    run()
