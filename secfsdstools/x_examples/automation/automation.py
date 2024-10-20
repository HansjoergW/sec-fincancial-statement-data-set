"""
This module shows the automation possibilities.
These are mainly providing a hook method, which can create additional steps that are executed
during the "check-for-update" (checking whether a new quarterly zip file is available), and
providing another hook-method, that is simply called at the end of the whole update process.

As a remainder, the framework executes all update steps (check and downloading new zip files,
transforming them to parquet, indexing the reports, and any additional steps that you define
as shown here.

Usually, you can configure these methods here in the secfsdstools configuration file, like
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

In this example, we have a config file "automation_config.cfg" which we are going to use in the
__main__ section of this module.
"""
import logging
import os
import shutil
from pathlib import Path
from typing import List

from secfsdstools.a_config.configmodel import Configuration
from secfsdstools.c_automation.task_framework import AbstractProcess, Task
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag

CURRENT_DIR, _ = os.path.split(__file__)


class CombineTask:

    # todo:
    # wir müssen noch schauen, was für files bereits in all vorhanden sind, bzw.
    # was das letzte war, das verarbeitet worden ist...
    def __init__(self,
                 root_path: Path,
                 filter: str,
                 bag_type: str,  # raw or joined
                 target_path: Path):
        self.root_path = root_path
        self.filter = filter
        self.all_dirs = list(self.root_path.glob(self.filter))
        self.all_names = {f.name: f for f in self.all_dirs}

        self.target_path = target_path
        self.bag_type = bag_type

        self.dir_name = target_path.name
        self.tmp_path = target_path.parent / f"tmp_{self.dir_name}"
        self.meta_inf_file: Path = self.target_path / "meta.inf"

        self.missing_paths = self.all_dirs

        if self.meta_inf_file.exists():
            meta_inf_content = self.meta_inf_file.read_text(encoding="utf-8")
            containing_names = meta_inf_content.split("\n")

            missing = set(self.all_names.keys()) - set(containing_names)
            self.missing_paths = [self.all_names[name] for name in missing]

    def prepare(self):
        """ prepare Task. """
        if len(self.missing_paths) == 0:
            return

        self.tmp_path.mkdir(parents=True, exist_ok=False)

    def commit(self):
        """ we commit by renaming the tmp_path. """
        if len(self.missing_paths) == 0:
            return "success"

        self.tmp_path.rename(self.target_path)
        return "success"

    def exception(self, exception) -> str:
        """ delete the temp folder. """
        shutil.rmtree(self.tmp_path, ignore_errors=True)
        return f"failed {exception}"

    def __str__(self) -> str:
        return f"CombineTask(root_path: {self.root_path}, filter: {self.filter})"

    def execute(self):
        if len(self.missing_paths) == 0:
            return

        paths = self.missing_paths
        if self.target_path.exists():
            paths.append(self.target_path)

        if self.bag_type.lower() == "raw":
            self._execute_raw(paths)
        elif self.bag_type.lower() == "joined":
            self._execute_joined(paths)
        else:
            raise ValueError("bag_type must be either raw or joined")

        temp_meta_inf = self.tmp_path / "meta.inf"
        meta_inf_content = "\n".join([f.name for f in self.missing_paths])
        temp_meta_inf.write_text(data=meta_inf_content, encoding="utf-8")

    def _execute_raw(self, paths: List[Path]):
        all_bags = [RawDataBag.load(str(path)) for path in paths]

        all_bag: RawDataBag = RawDataBag.concat(all_bags)
        all_bag.save(target_path=str(self.tmp_path))

    def _execute_joined(self, paths: List[Path]):
        all_bags = [JoinedDataBag.load(str(path)) for path in paths]

        all_bag: JoinedDataBag = JoinedDataBag.concat(all_bags)
        all_bag.save(target_path=str(self.tmp_path))


class CombineProcess(AbstractProcess):

    def __init__(self,
                 filtered_dir: str,
                 bag_type: str,  # raw or joined
                 file_type: str = "quarter",
                 ):
        super().__init__(execute_serial=False,
                         chunksize=0)

        self.filtered_path = Path(filtered_dir)
        self.bag_type = bag_type
        self.file_type = file_type

    def calculate_tasks(self) -> List[Task]:
        task = CombineTask(
            root_path=self.filtered_path,
            bag_type=self.bag_type,
            filter=f"{self.file_type}/*",
            target_path=self.filtered_path / "all"
        )
        # since this is a one task process, we just check if there is really something to do
        if len(task.missing_paths) > 0:
            return [task]
        return []


def define_extra_processes(config: Configuration) -> List[AbstractProcess]:
    from secfsdstools.x_examples.automation.filter_process import FilterProcess

    return [
        FilterProcess(parquet_dir=config.parquet_dir,
                      filtered_dir=config.config_parser.get(section="Filter",
                                                            option="filtered_dir_raw"),
                      bag_type="raw",
                      save_by_stmt=False,
                      execute_serial=False  # switch to true in case of memory problems
                      ),
        CombineProcess(filtered_dir=config.config_parser.get(section="Filter",
                                                             option="filtered_dir_raw"),
                       bag_type="raw"
                       ),
        FilterProcess(parquet_dir=config.parquet_dir,
                      filtered_dir=config.config_parser.get(section="Filter",
                                                            option="filtered_dir_joined"),
                      bag_type="joined",
                      save_by_stmt=False,
                      execute_serial=False  # switch to true in case of memory problems
                      ),
        CombineProcess(filtered_dir=config.config_parser.get(section="Filter",
                                                             option="filtered_dir_joined"),
                       bag_type="joined"
                       )
    ]


def after_update(config: Configuration):
    pass


if __name__ == '__main__':
    from secfsdstools.a_config.configmgt import ConfigurationManager, SECFSDSTOOLS_ENV_VAR_NAME
    from secfsdstools.c_update.updateprocess import Updater

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s  %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    # define which configuration file to use
    os.environ[SECFSDSTOOLS_ENV_VAR_NAME] = f"{CURRENT_DIR}/automation_config.cfg"

    config = ConfigurationManager.read_config_file()

    updater = Updater.get_instance(config)

    # We call this method mainly for demonstration purpose. Therefore, we also set
    # force_update=True, so that update process is being executed, regardless if the last
    # update process run less than 24 hours before.

    # You could also just start to use any feature of the framework. This would also trigger the
    # update process to run, but at most once every 24 hours.
    updater.update(force_update=True)
