
# Changelog
See the [Release Notes](https://hansjoergw.github.io/sec-fincancial-statement-data-set/releasenotes/) for details.


## 2.3.4 -> 2.4.1
* Fixes
  * update to secdaily 0.2.2 (more robustness / prevent name clashes)
  * check for daily data cleanup if needed by secdaily

## 2.3.0 -> 2.4.0
* New (Experimental)
  * Integration of [secdaily](https://github.com/HansjoergW/sec-financial-statement-data-set-daily-processing) to provide daily report updates. (Experimental)
    * You have to turn on this feature by adding `dailyprocessing = True` in the `DEFAULT` section of the configuration file.
    * Please have a look at the notebook [10_00_daily_financial_report_updates](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/10_00_daily_financial_report_updates.ipynb) for details.
      

## 2.2.0 -> 2.3.0
Maintenance release.
   * changed development environment to vscode as ide
   * changed to use poetry as dependency management tool
   * inform user if a newer version is available on pypi.org

## 2.2.1 -> 2.2.0

* New
  * `CIKXXFilter` was introduced for `RawDataBag` and `JoinedDataBag`
  * ciks_filter parameter was added to the load methods of `RawDataBag` and `JoinedDataBag`
  * The notebook 09_00_segments_basics gives an idea how you can work with the information in the segment column.
    [segment_basics](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/09_00_segments_basics.ipynb)
  * The concat processes `ConcatByChangedTimestampProcess` and `ConcatByNewSubfoldersProcess` now have a switch to choose whether in_memory or file_based concatenation should be used
  * `ConcatByChangedTimestampProcess` and `ConcatByNewSubfoldersProcess` now also support the concatenation of `StandardizedBag`
  * `StandardizeProcess` now also works with multiple subfolders where each contains BS, CF, and IS folders
  * A new example of a memory optimized pipeline was introduced:  `secfsdstools.x_examples.automation.memory_optimized_automation.define_extra_processes`. 
    Have a look at the description of this pipeline in [08_02_automation_a_memory_optimized_example_2.2.0](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/08_02_automation_a_memory_optimized_example_2.2.0.ipynb) 

* Changes
  * The `is_xxx_bag_path` methods in the module `secfsdstools.d_container.databagmodel` have been moved into `RawDataBag`, resp. `JoinedDataBag` classes in the same module.
    The `StandardizedBag` now also has a `is_xxx_bag_path` method. 

* Other
  * GitHub sponsoring account was activated: https://github.com/sponsors/HansjoergW
  * GitHub Discussions was activated: https://github.com/HansjoergW/sec-fincancial-statement-data-set/discussions

## 2.0.0 -> 2.1.0
Main goal: improving memory footprint. Checkout the notebook:  [bulk_data_processing_memory_efficiency](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/06_01_bulk_data_memory_efficiency.ipynb)

* New 
  * Predicate Pushdown in `load` methods of `RawDataBag` and `JoinedDataBag`  
    Directly apply filters for adshs, statements, forms, and tags during loading of the data
  * `concat_filebased` concatenates `RawDataBag` and `JoinedDataBag` folders without loading them into memory  
  * `ConcatByChangedTimestampProcess` and `ConcatByNewSubfoldersProcess` use `concat_filebased`
  * `save` for `RawDataBag`, `JoinedDataBag`, and `StandardizedBag` create new the target folder if it does not exist. 


## 1.8.2 -> 2.0.0
Introducing the new version of the datasets that includes the "segments" column in then num tables.
The main purpose of this version is to ensure that the new "segments" colomn does not interfere with existing logic.
The following things did change:
* Checks during starting if only data from the new datasets is present. If not, data have to be reloaded
* New NoSegmentInfo filter for raw and joined bags: removes datapoints with non-empty `segment` info
* `StandardPresenter` has a new `show_segments` flag. If True, datapoints with segments information are displayed as well
* Notebook 03_explore_with_interactive_notebook has new option `show_segments` for displaying the details of a report
* Support for Daily-Datasets has been removed

## 1.8.1 -> 1.8.2
* Ensures data is read only from the archived version of the datasets without the segments column in num.

## 1.8.0 -> 1.8.1
* Fix problem with circular import when using the new FilterProcess module in secfsdstools.g_pipeline

## 1.7.0 -> 1.8.0
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
    have duplicated enries in the sub_df.

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
    


## 1.6.2 -> 1.7.0
* Fix for new path to zip files on SEC.gov
  * The SEC did change the location of the zip files and this latest version fixes the path to them
  

## 1.6.1 -> 1.6.2
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

## 1.6 -> 1.6.1
* Minor improvements
  * `filed` column added to result of present method of standardizer
  * `StandardizedBag` now has a `concat()` method to concat multiple instances into one
  * `Standardizer` checks that the data contains just one currency
  * `IncomeStatementStandardizer` now also returns OustandingShares and EarningsPerShare tags
  * [03_explore_with_interactive_notebook.ipynb](notebooks/03_explore_with_interactive_notebook.ipynb) includes use of the `CashFlowStandardizer`
  * improvements in the READMD.md -> thanks to [Hamid Ebadi](https://github.com/ebadi)
* Documentation
  *  Medium article [Understanding the the SEC Financial Statement Data Sets](https://medium.com/@hansjoerg.wingeier/understanding-the-sec-financial-statement-data-sets-6148e07d1715)

## 1.5 -> 1.6
* Introducing **Cash Flow Standardizer**<br>
  The Cash Flow Standardizer makes the cash flow statements easily comparable.<br>
  [07_03_CF_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_03_CF_standardizer.ipynb) <br>

## 1.4.2 -> 1.5
* Introducing **Income Statement Standardizer**<br>
  The Income Statement Standardizer makes the income statements easily comparable.<br>
  [07_02_IS_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_02_IS_standardizer.ipynb) <br>
* Small improvements in the Standardizer framework and rules

## 1.4 -> 1.4.2
* Fix in `StandardStatementPresenter`: <br>
  The `StandardStatementPresenter` also considers `qtrs` when displaying the information.
  This was a problem when displaying information for income statements and cash flows, since they often show
  data for different periods.
* Improvements in the Standardizer framework as preparation to implement the income statement and cash flow standardizer.

## 1.3 -> 1.4
* Introducing the Standardizer Framework and the **Balance Sheet Standardizer** as a first implementation.<br>
  The Balance Sheet Standardizer makes the balance sheets easily comparable.<br>
  Check out the following notebooks: <br>
  [07_00_standardizer_basics](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_00_standardizer_basics.ipynb) <br>
  [07_01_BS_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_01_BS_standardizer.ipynb) <br>
* Efficiency improvements for `MultiReportCollector`. 

## 1.2 -> 1.3
* New notebook [06_bulk_data_processing_deep_dive](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/06_bulk_data_processing_deep_dive.ipynb)<br>
  This first version shows how datasets can be created with data from all available zip files. It shows a faster
  parallel approach which uses more memory and cpu resources and a slower serial approach which uses significant
  less resources.
* New package `u_usecases` introduced. This package is a place to provide concrete examples what you can do
  with the `secfsdstools` library. As a first usecase, the logic shown and explained in the `06_bulk_data_processing_deep_dive`
  is provided as logic within the modul `bulk_loading`.
