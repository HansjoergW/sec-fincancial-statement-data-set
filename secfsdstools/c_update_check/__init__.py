"""
check-for-update launcher.
helping to ensure that the check is executed when main functionality is used:
d_container, e_collector, e_filter, e_presenter, f_standardize.
This is simply done by importing this module in the __init__.py of the above mentioned modules.
"""

import sys


def is_running_in_pytest_or_pdoc():
    """ Check if we are running as a test or inside the build process """
    return ('pytest' in sys.argv[0]) or ('pdoc3' in sys.argv[0])


# ensure only execute if not pytest is running
if not is_running_in_pytest_or_pdoc():
    import logging
    import secfsdstools

    logging.getLogger().info("loading secfsdstools ...")
    secfsdstools.update.update()
