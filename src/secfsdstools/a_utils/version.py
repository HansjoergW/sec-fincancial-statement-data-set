"""Helper method to read the currently available version of the library from pipy.org."""

import requests
from packaging import version

import secfsdstools


def get_latest_pypi_version():
    """ Reads the latest version of the library from pypi.org
    Returns:
        str: the latest version as string
    """
    url = "https://pypi.org/pypi/secfsdstools/json"
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        data = response.json()
        return data["info"]["version"]
    return ""


def is_newer_version_available():
    """Checks if a newer version of the library is available on pypi.org
    Returns:
        bool: True if a newer version is available
    """
    pypi_version = get_latest_pypi_version()
    if pypi_version == "":
        return False

    current_version = secfsdstools.__version__

    return version.parse(pypi_version) > version.parse(current_version)
