"""
helper utils condhandling compressed files.
"""

import zipfile
from pathlib import Path


# def _check_if_zipped(path: str) -> bool:
#     return os.path.isfile(path + ".zip")
#
#
# def write_df_to_zip(df: pd.DataFrame, filename: str):
#     csv_content = df.to_csv(sep='\t', header=True)
#     write_content_to_zip(csv_content, filename)
#
#
# def read_df_from_zip(filename: str) -> pd.DataFrame:
#     if _check_if_zipped(filename):
#         with zipfile.ZipFile(filename + ".zip", "r") as zf:
#             file = Path(filename).name
#             return pd.read_csv(zf.open(file), header=0, delimiter="\t")
#     else:
#         return pd.read_csv(filename, header=0, delimiter="\t")
#

def write_content_to_zip(content: str, filename: str) -> str:
    """
    write the content str into the zip file. compression is set to zipfile.ZIP_DEFLATED
    :param content: string
    :param filename: string name of the target zipfile, withouit the ending ".zip"
    :return: written zipfilename
    """
    zip_filename = filename + ".zip"
    with zipfile.ZipFile(zip_filename, mode="w", compression=zipfile.ZIP_DEFLATED) as zf_fp:
        file = Path(filename).name
        zf_fp.writestr(file, content)
    return zip_filename


def read_content_from_zip(filename: str) -> str:
    """
    returns the content of the provided zipfile (ending ".zip)
    :param filename: zipfilename without the ending ".zip"
    :return:
    """
    with zipfile.ZipFile(filename + ".zip", mode="r") as zf_fp:
        file = Path(filename).name
        return zf_fp.read(file).decode("utf-8")
