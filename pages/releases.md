---
permalink: /releasenotes/
---


# Release Notes

## 1.7.0 -> 1.8.0 2025-January-10
* Fix in OfficialTagsOnlyJoinedFilter: did only filter unofficial tags instead of vice versa

* Major changes
  * Check for update is always executed regardless which feature of the framework is being used.
    Previously, this just happened if a collector had been used.

* Minor changes
  * The interface of the classmethod  Updater.get_instance was changed and takes now a
    Configuration instance instead of the individual attributes of the object.
  * The concat methods of the JoineDataBag and RawDataBag classes have a new parameter "drop_duplicates_sub_df", which
    drops duplicated entries from the sub_df dataframe. Default is set to False. This is being used when
    concatenating data from the same reports. E.g., if you have a bag with all the balance sheet data and another bag with
    all the income sheet data, but from the same reports, you should set that parameter to True, otherwise you would
    have duplicated entries in the sub_df.

* New
  * Introducing automation pipeline framework in package secfsdestools.c_automation.
    This framework can be used as standalone, or it can be used to implement additional steps that can be added to the
    update process.
    * Checkout the documentation for the package secfsdstools.c_automation
    * Checkout example implementations of pipeline steps that can be directly used in your own pipelines:
      secfsdstools.g_pipeline
    * Checkout the example implementation on how you can add additional processing step to the default update process:
      see package secfsdstools.x_examples.automation and notebook 08_automation_basics.
  * Two hook function that can be implemented and configured that run after the default update process.
    * One hook function to provide additional processing steps that are implemented with the automation pipeline framework described above
    * One hook function that is called at the end of the update process and were you can freely implement any logic you want
    * Both hook functions are configured in the configuration file. Have a look at the notebook 08_00_automation_basics.


## 1.6.2 -> 1.7.0 2024-December-22
* Fix for new path to zip files on SEC.gov
  * The SEC did change the location of the zip files and this latest version fixes the path to them

## 1.6.1 -> 1.6.2 2024-September-15
* Major changes
  * Compatibility for Python 3.7 is no longer checked
  * Compatibility for Python 3.11 was added
* Minor changes
  * `secfsdstools.__version__` now returns the version of the library
  * `IncomeStatementStandardizer`
    * Calculation for `OutstandingShares` and `EarningsPerShare` was simplified and improved
    * Validation rule for `EarningsPerShare` was added
    * Please have a look at the comments in [07_02_IS_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_02_IS_standardizer.ipynb) <br>
  * Ability to customize the standardizer was improved
    * Configure the columns that are merged from sub_df into the final results can be extended
    * Configure additional tags that should appear in the final result can be defined
    * All constructor parameters of the `Standardizer` base class can be overwritten via the constructor of the three standardizer classes
    * New notebook that shows the different possibilities for customization: [07_04_customize_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_04_customize_standardizer.ipynb) <br>


## 1.6 -> 1.6.1 2024-August-20
* Minor improvements
  * `filed` column added to result of present method of standardizer
  * `StandardizedBag` now has a `concat()` method to concat multiple instances into one
  * `Standardizer` checks that the data contains just one currency
  * `IncomeStatementStandardizer` now also returns OustandingShares and EarningsPerShare tags
  * [03_explore_with_interactive_notebook.ipynb](notebooks/03_explore_with_interactive_notebook.ipynb) includes use of the `CashFlowStandardizer`
  * improvements in the READMD.md -> thanks to [Hamid Ebadi](https://github.com/ebadi)
* Documentation
  *  Medium article [Understanding the the SEC Financial Statement Data Sets](https://medium.com/@hansjoerg.wingeier/understanding-the-sec-financial-statement-data-sets-6148e07d1715)


## 1.6.0 2024-July-12
* New
  - Introducing **Cash Flow Standardizer**<br>
    The Cash Flow Standardizer makes the cash flow statements easily comparable.<br>
    [07_03_CF_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_03_CF_standardizer.ipynb) <br>
* Improvements
  - Small improvements in the Standardizer framework and rules

## 1.5.0 2024-May-18_
* New
  - Introducing **Income Statement Standardizer**<br>
    The Income Statement Standardizer makes the income statements easily comparable.<br>
    [07_02_IS_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_02_IS_standardizer.ipynb) <br>
* Improvements
  - Small improvements in the Standardizer framework and rules


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