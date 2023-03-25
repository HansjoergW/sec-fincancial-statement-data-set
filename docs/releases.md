# Release Notes

## Upcoming
* more example notebooks
* use parquet as storing format instead of zipfiles with csv files

## 0.4.0 2023-Mar-25
* new MultiReportReader - reads reports from different zipfiles at once
* new CompanyCollector - reads reports for one company from different zipfiles at once
* new merge_pre_and_num() method which only merges the pre and num data but does not pivot it
* new notebook that shows how the data can be analyzed with an interactive jupyter notebook 

BugFixes:
* coreg was not considered correctly when merging the data

## 0.3.0 2023-Feb-04
* integration of https://rapidapi.com/hansjoerg.wingeier/api/daily-sec-financial-statement-dataset. Daily updates instead of quarterly updates.

## 0.2.1 2023-Jan-21
* class ZipReportReader: helps to read data from a whole zip file; has the same interface as report reader
* class IndexSearch: helps with searching the index_report table
* added a getting started notebook: https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/01_quickstart.ipynb
* ensure runs also with python 3.7
* improvements in the API documentation

## 0.2.0 2023-Jan-14
* first simple APi docu on githubpages https://hansjoergw.github.io/sec-fincancial-statement-data-set/secfsdstools/
* renaming of internal package structure

## 0.1.3 2023-Jan-08
* dependencies added into pyproject.toml

## 0.1.1 2023-Jan-07
* first version