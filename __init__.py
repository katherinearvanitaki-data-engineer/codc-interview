"""
Init file
"""

import sys
import os
import pkgutil

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
)

__all__ = list(
    module for _, module, _ in pkgutil.iter_modules([os.path.dirname(__file__)])
)
__version__ = "1.2.0-beta.0"
