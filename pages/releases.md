---
permalink: /releasenotes/
---


# Release Notes

## Upcoming
* 

## 1.4.2 2024-Mar-29
* Fix
  - The StandardStatementPresenter didn't consider `qtrs` when displaying the data. This was a problem for the 
    Income Statement and the Cash Flow.
* Improvements
  - Several in the `Standardizer` as preparation to implement the Income Statement and Cash Flow `Standardizer`. 


## 1.4.0 2024-Feb-02
* New
  - Introducing the Standardizer Framework and the **Balance Sheet Standardizer** as a first implementation.<br>
    The Balance Sheet Standardizer makes the balance sheets easily comparable.<br>
    Check out the following notebooks: <br>
    [07_00_standardizer_basics](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_00_standardizer_basics.ipynb) <br>
    [07_01_BS_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_01_BS_standardizer.ipynb) <br>
* Improvements
  - Efficiency improvements for `MultiReportCollector`: Every zip file is opened just once if there are multiple reports
    to load from the same zip file.


## 1.3.0 2023-Dec-28
* New
  - Notebook [06_bulk_data_processing_deep_dive](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/06_bulk_data_processing_deep_dive.ipynb)<br>
    This first version shows how datasets can be created with data from all available zip files. It shows a faster
    parallel approach which uses more memory and cpu resources and a slower serial approach which uses significant
    less resources.
  - Package `u_usecases` introduced. <br>
    This package is a place to provide concrete examples showing what you can do
    with the `secfsdstools` library. As a first usecase, the logic shown and explained in the `06_bulk_data_processing_deep_dive`
    is provided as logic within the modul `bulk_loading`.


## 1.2.0 2023-Dec-02
* API Changes
  - `MainCoregFilter` was renamed to `MainCoregRawFilter`
  - `OfficialTagsOnlyFilter` was renamed to `OfficialTagsOnlyRawFilter`
* New
  - `secfsdstools.e_filter.rawfiltering.USDOnlyRawFilter` is new and removes none USD currency datapoints
  - All filters have been implemented for the JoinedDataBag as well: `secfsdstools.e_filter.joinedfiltering`
  - Notebook [05_filter_deep_dive notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/05_filter_deep_dive.ipynb).

## 1.1.0 2023-Oct-28
* API Changes
  - Zipcollector has now a factory method that can load multiple zip files as one
  - Zipcollector has now a factory method that can load all zip files at one
  - Zipcollector factory methods have a new filter parameter "post_load_filter"
* New
  - Filter for official tags only -> company specific tags are removed
  - RawDataBag and JoinedDataBag have now copy_bag method
  - Notebook [04_collector_deep_dive](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/04_collector_deep_dive.ipynb)

## 1.0.1 2023-Oct-16
* README.md adpated <br>
  Added information about using the library on windows because the multiprocessing package is used<br>
  https://docs.python.org/3.10/library/multiprocessing.html#the-process-class

## 1.0.0 2023-Sep-28
ApiChanges:
* The API has completely changed, it should be more structured now. <br> 
  Please check out the README.md and the 01_quickstart notebook for details

## 0.5.0 2023-Jun-02
* use parquet as storing format instead of zipfiles with csv files -> 5-10x faster access to data
* auto discover of new zip files on sec.gov
* launch first time download of zip files without calling the update method

ApiChanges:
* package secfsdstools.d_index was renamed into secfsdstools.c_index


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