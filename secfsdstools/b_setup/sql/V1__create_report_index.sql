CREATE TABLE IF NOT EXISTS index_reports
(
    adsh,
    cik,
    name,
    form,
    filed,
    period,
    fullPath,
    originFile,
    originFileType,
    url,
    PRIMARY KEY (adsh, originFile)
);

CREATE TABLE IF NOT EXISTS index_file_processing_state
(
    fileName,
    fullPath,
    status,
    entries,
    processTime,
    PRIMARY KEY (fileName)
);

