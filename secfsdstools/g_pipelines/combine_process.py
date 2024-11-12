import shutil
from pathlib import Path
from typing import List

from secfsdstools.c_automation.task_framework import AbstractProcess, Task, delete_temp_folders
from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag


class CombineTask:

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
            containing_names = self.read_metainf_content()

            missing = set(self.all_names.keys()) - set(containing_names)
            self.missing_paths = [self.all_names[name] for name in missing]

    def read_metainf_content(self) -> List[str]:
        meta_inf_content = self.meta_inf_file.read_text(encoding="utf-8")
        return meta_inf_content.split("\n")

    def prepare(self):
        """ prepare Task. """
        if len(self.missing_paths) == 0:
            return

        self.tmp_path.mkdir(parents=True, exist_ok=False)

    def commit(self):
        """ we commit by renaming the tmp_path. """
        if len(self.missing_paths) == 0:
            return "success"

        # alten Inhalt entfernen
        if self.target_path.exists():
            shutil.rmtree(self.target_path)

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

        paths = self.missing_paths.copy()
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

        all_bag: RawDataBag = RawDataBag.concat(all_bags, drop_duplicates_sub_df=True)
        all_bag.save(target_path=str(self.tmp_path))

    def _execute_joined(self, paths: List[Path]):
        all_bags = [JoinedDataBag.load(str(path)) for path in paths]

        all_bag: JoinedDataBag = JoinedDataBag.concat(all_bags, drop_duplicates_sub_df=True)
        all_bag.save(target_path=str(self.tmp_path))


class CombineProcess(AbstractProcess):

    def __init__(self,
                 root_dir: str,
                 target_dir: str,
                 bag_type: str,  # raw or joined
                 filter: str = "*"
                 ):
        super().__init__(execute_serial=False,
                         chunksize=0)

        self.root_path = Path(root_dir)
        self.target_path = Path(target_dir)
        self.bag_type = bag_type
        self.filter = filter

    def pre_process(self):
        delete_temp_folders(root_path=self.target_path.parent)

    def calculate_tasks(self) -> List[Task]:
        task = CombineTask(
            root_path=self.root_path,
            bag_type=self.bag_type,
            filter=self.filter,
            target_path=self.target_path
        )
        # since this is a one task process, we just check if there is really something to do
        if len(task.missing_paths) > 0:
            return [task]
        return []
