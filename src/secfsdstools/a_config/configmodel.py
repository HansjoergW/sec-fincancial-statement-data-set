"""
Base Model for configuration.
"""

import configparser
import os
from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class Configuration:
    """Basic configuration settings"""

    download_dir: str
    db_dir: str
    parquet_dir: str
    user_agent_email: str
    auto_update: bool = True
    keep_zip_files: bool = False
    no_parallel_processing: bool = False

    config_parser: Optional[configparser.ConfigParser] = None
    post_update_hook: Optional[str] = None
    post_update_processes: Optional[str] = None

    daily_download_dir: str = ""
    daily_processing: bool = False

    def __post_init__(self):
        if self.daily_download_dir == "":
            self.daily_download_dir = os.path.join(self.download_dir, "daily")

    def get_dict(self):
        """
        returns the configuration as a dictionary
        Returns:
            the configuration as a dictionary
        """
        return dict(asdict(self))
