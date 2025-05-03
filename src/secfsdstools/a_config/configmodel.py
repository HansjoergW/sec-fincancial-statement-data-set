"""
Base Model for configuration.
"""

import os
from dataclasses import dataclass, asdict
from typing import Optional
import configparser


@dataclass
class Configuration:
    """ Basic configuration settings """
    download_dir: str
    db_dir: str
    parquet_dir: str
    user_agent_email: str
    rapid_api_key: Optional[str] = None
    rapid_api_plan: Optional[str] = 'basic'
    daily_download_dir: Optional[str] = None
    auto_update: Optional[bool] = True
    keep_zip_files: Optional[bool] = False
    no_parallel_processing: Optional[bool] = False

    config_parser: Optional[configparser.ConfigParser] = None
    post_update_hook: Optional[str] = None
    post_update_processes: Optional[str] = None

    def __post_init__(self):
        self.daily_download_dir = os.path.join(self.download_dir, "daily")

    def get_dict(self):
        """
        returns the configuration as a dictionary
        Returns:
            the configuration as a dictionary
        """
        return dict(asdict(self))
