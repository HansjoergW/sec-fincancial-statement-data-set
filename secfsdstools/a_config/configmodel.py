"""
Base Model for configuration.
"""

import os
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional


class AccessorType(Enum):
    """
    Defines the AccessType which depends on how the data is stored
    """
    ZIP = 1
    PARQUET = 2


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
    use_parquet: Optional[bool] = True
    auto_update: Optional[bool] = True
    keep_zip_files: Optional[bool] = False

    def __post_init__(self):
        self.daily_download_dir = os.path.join(self.download_dir, "daily")

    def get_accessor_type(self) -> AccessorType:
        """
        returns the access type, depending on how the flag use_parquet is set
        Returns:
            AccessorType: accessor type, whether to use parquet or csv in zip

        """
        return AccessorType.PARQUET if self.use_parquet else AccessorType.ZIP

    def get_dict(self):
        """
        returns the configuration as a dictionary
        Returns:
            the configuration as a dictionary
        """
        return dict(asdict(self))
