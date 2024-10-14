"""
This module shows the automation possibilities.
These are mainly providing a hook method, which can create additional steps that are executed
during the "check-for-update" (checking whether a new quarterly zip file is available), and
providing another hook-method, that is simply called at the end of the whole update process.

As a remainder, the framework executes all update steps (check and downloading new zip files,
transforming them to parquet, indexing the reports, and any additional steps that you define
as shown here.

Usually, you would configure these methods here in the secfsdstools configuration file, like
<pre>
[DEFAULT]
downloaddirectory = ...
dbdirectory = ...
parquetdirectory = ...
useragentemail = ...
autoupdate = True
keepzipfiles = False
postppdatehook=secfsdstools.x_examples.automation.automation.after_update
postupdateprocesses=secfsdstools.x_examples.automation.automation.define_extra_processes
</pre>

But you can also "force" it to run, for instance using the logic that is defined in the
__main__ section of this module.
"""

from typing import List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_automation.task_framework import AbstractProcess


def define_extra_processes(config: Configuration) -> List[AbstractProcess]:
    return []


def after_update(config: Configuration):
    pass


if __name__ == '__main__':
    from secfsdstools.a_config.configmgt import ConfigurationManager
    from secfsdstools.c_update.updateprocess import Updater

    config = ConfigurationManager.read_config_file()
    config.post_update_hook = "secfsdstools.x_examples.automation.automation.after_update"
    config.post_update_processes = "secfsdstools.x_examples.automation.automation.define_extra_processes"

    updater = Updater.get_instance(config)
    # if we call this method, we really want to run it, so we set force_update=True.
    # this will disable the "24"-hour check, so it will run all update tasks everytime it is
    # called
    updater.update(force_update=True)
