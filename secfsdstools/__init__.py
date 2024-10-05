"""
main __init__.py

Defines the version attribut of the library
"""
from importlib.metadata import version, PackageNotFoundError
import logging

import secfsdstools.update

try:
    __version__ = version("secfsdstools")
except PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback-Version, falls das Paket nicht installiert ist

logging.getLogger().info("log: loading secfsdstools")
print("print: loading secfsdstools ... ")
secfsdstools.update.update()