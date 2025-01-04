"""
check-for-update launcher.
helping to ensure that the check is executed when main functionality is used:
d_container, e_collector, e_filter, e_presenter, f_standardize.
This is simply done by importing this module in the __init__.py of the above mentioned modules.
"""

import sys
from inspect import FrameInfo


def is_running_in_pytest_or_pdoc():
    """ Check if we are running as a test or inside the build process """
    return ('pytest' in sys.argv[0]) or ('pdoc3' in sys.argv[0])


def is_update_frame(frameinfo: FrameInfo):
    return (frameinfo.function == "update") and (frameinfo.filename.endswith("updateprocess.py"))


def is_check_configuration(frameinfo: FrameInfo):
    return ((frameinfo.function == "check_basic_configuration") and
            (frameinfo.filename.endswith("configmgt.py")))


def is_already_in_update_or_check_config_process():
    import inspect

    stack = inspect.stack()
    return any(is_update_frame(frame) or is_check_configuration(frame) for frame in stack)


# ensure only execute if not pytest is running or if we are already in
# the update process or the check_config_process
if not is_running_in_pytest_or_pdoc() and not is_already_in_update_or_check_config_process():
    import logging
    import secfsdstools

    logging.getLogger().info("loading secfsdstools ...")
    secfsdstools.update.update()
