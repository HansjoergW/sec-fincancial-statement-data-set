import re
from io import StringIO

import pandas as pd

from secfsdstools._0_utils.fileutils import read_df_from_file_in_zip, read_content_from_file_in_zip

TEST_FILE = '../data/dld/2022q3.zip'
TEST_REPORT = '0000320193-22-000070'  # APPLE INC 10-Q  2022-06-30


def test_find_report():
    sub_df = read_df_from_file_in_zip(TEST_FILE, 'sub.txt')
    sub_df_apple = sub_df[sub_df.cik == 320193][['adsh', 'name', 'form', 'period']]

    print(sub_df.columns)
    print(sub_df_apple)


def match_group_iter(match_iter):
    for match in match_iter:
        yield match.group()

def test_read_content_with_re():
    content = read_content_from_file_in_zip(TEST_FILE, "pre.txt")

    first_line_pattern = re.compile(f"^.*$", re.MULTILINE)
    first_line = re.search(first_line_pattern, content)
    print(first_line.group())

    matches_pattern = re.compile(f"^{TEST_REPORT}.*$", re.MULTILINE)

    lines = "\n".join(match_group_iter(matches_pattern.finditer(content)))
    df = pd.read_csv(StringIO(lines), sep="\t", header=None,
                     names=['adsh','tag','version','coreg','ddate','qtrs','uom','value','footnote'])


    print(lines)


# df = pd.read_csv(StringIO(csvString), sep=","

if __name__ == '__main__':
    pass
