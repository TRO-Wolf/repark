"""``python -m compat`` → census runner CLI."""

from __future__ import annotations

import sys

from compat.runner import main

if __name__ == "__main__":
    sys.exit(main())
