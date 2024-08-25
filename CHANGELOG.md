
# Changelog
See the [Release Notes](https://hansjoergw.github.io/sec-fincancial-statement-data-set/releasenotes/) for details.

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
