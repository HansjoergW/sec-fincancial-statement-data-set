from typing import Set

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_rules import CopyTagRule


def test_rename_rule():
    rename_rule = CopyTagRule(original='original', target='target')

    print(rename_rule.get_description())
