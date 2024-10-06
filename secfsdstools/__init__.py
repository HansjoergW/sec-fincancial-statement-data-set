"""
main __init__.py

Defines the version attribut of the library
"""
import logging
import sys
from importlib.metadata import version, PackageNotFoundError

import secfsdstools.update

try:
    __version__ = version("secfsdstools")
except PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback-Version, falls das Paket nicht installiert ist


def is_running_in_pytest():
    """ Check if we are running as a test """
    return 'pytest' in sys.argv[0]


# ensure only execute if not pytest is running
if not is_running_in_pytest():
    logging.getLogger().info("loading secfsdstools ...")
    secfsdstools.update.update()
