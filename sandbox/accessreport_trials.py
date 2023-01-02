import re

from secfsdstools._0_utils.fileutils import read_df_from_file_in_zip, read_content_from_file_in_zip

TEST_FILE = '../data/dld/2022q3.zip'
TEST_REPORT = '0000320193-22-000070'  # APPLE INC 10-Q  2022-06-30


def test_find_report():
    sub_df = read_df_from_file_in_zip(TEST_FILE, 'sub.txt')
    sub_df_apple = sub_df[sub_df.cik == 320193][['adsh', 'name', 'form', 'period']]

    print(sub_df.columns)
    print(sub_df_apple)


def test_read_content_with_re():
    content = read_content_from_file_in_zip(TEST_FILE, "num.txt")

    matches_pattern = re.compile(f"^{TEST_REPORT}.*$", re.MULTILINE)

    for match in matches_pattern.finditer(content):
        result = match.group()
        print(result)


if __name__ == '__main__':
    pass
