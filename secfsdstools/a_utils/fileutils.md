Module secfsdstools.a_utils.fileutils
=====================================
helper utils condhandling compressed files.

Functions
---------

    
`get_filenames_in_directory(filter_string: str) ‑> List[str]`
:   returns a list with files matching the filter.
    the filter can also contain a folder structure.
    :return: list files in the directory

    
`read_content_from_file_in_zip(zip_file: str, file_to_extract: str) ‑> str`
:   reads the text content of a file inside a zip file
    :param zip_file: the zip file containing the data file
    :param file_to_extract: the file with the data
    :return: the content as string

    
`read_content_from_zip(filename: str) ‑> str`
:   returns the content of the provided zipfile (ending ".zip)
    :param filename: zipfilename without the ending ".zip"
    :return:

    
`read_df_from_file_in_zip(zip_file: str, file_to_extract: str, dtype=None, usecols=None) ‑> pandas.core.frame.DataFrame`
:   reads the content of a file inside a zip file directly into dataframe
    :param zip_file: the zip file containing the data file
    :param file_to_extract: the file with the data
    :param dtype: column type array or None
    :param usecols: list with all the columns that should be read or None
    :return: the pandas dataframe

    
`write_content_to_zip(content: str, filename: str) ‑> str`
:   write the content str into the zip file. compression is set to zipfile.ZIP_DEFLATED
    :param content: string
    :param filename: string name of the target zipfile, withouit the ending ".zip"
    :return: written zipfilename