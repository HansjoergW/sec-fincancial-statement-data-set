"""
main __init__.py

Defines the version attribut of the library
"""
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("secfsdstools")
except PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback-Version, falls das Paket nicht installiert ist
