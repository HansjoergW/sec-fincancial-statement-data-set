CREATE TABLE IF NOT EXISTS index_reports
(
    accessionNumber,
    cik,
    form,
    filed,
    period,
    originFile,
    originFileType,
    PRIMARY KEY (accessionNumber, originFile)
);

CREATE TABLE IF NOT EXISTS index_file_processing_state
(
    fileName,
    status,
    processTime,
    PRIMARY KEY (fileName)
);

