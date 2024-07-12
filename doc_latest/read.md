# sec-fincancial-statement-data-set

Helper tools to analyze the [Financial Statement Data Sets](https://www.sec.gov/dera/data/financial-statement-data-sets)
from the U.S. securities and exchange commission (sec.gov).

For a detail description of the content and the structure of the dataset, see https://www.sec.gov/files/aqfs.pdf.

> The SEC financial statement datasets contain financial information that companies are required to disclose to the US
> Securities and Exchange Commission (SEC). These financial statements include the balance sheet, income statement,
> statement of cash flows, and statement of stockholders' equity. The datasets also include footnotes and other
> disclosures that provide additional information about a company's financial position and performance. The financial
> statements are typically presented in a standardized format, making it easier to compare the financial performance of
> different companies. The datasets are useful for a wide range of purposes, including financial analysis, credit
> analysis, and investment research.
>
> *chat.openai.com*

# TL;DR

The SEC releases quarterly zip files, each containing four CSV files with numerical data from all financial reports
filed within that quarter.

However, accessing data from the past 12 years can be time-consuming due to the large amount
of data - over 120 million data points in over 2GB of zip files by 2023.

This library simplifies the process of working with this data and provides a
convenient way to extract information from the primary financial statements - the balance sheet, income statement, and
statement of cash flows.

The main features include:
- all data is on your local hard drive, no need for numerous API calls
- data is loaded as pandas files
- fast an efficient reading of a single report, all reports of one or multiple companies, or even all available reports
  ```python
  single_collector: SingleReportCollector = SingleReportCollector.get_report_by_adsh(adsh="0000320193-22-000108")
  company_collector = CompanyReportCollector.get_company_collector(ciks=[320193, 789019]) # Apple, Microsoft
  all_2022_collector: ZipCollector = ZipCollector.get_zip_by_names(names=["2022q1.zip", "2022q2.zip", "2022q3.zip", "2022q4.zip"])
  ```
- filter framework with predefined filters, easy to extend, supports easy way of saving, loading, and combining filtered data
  ```python
  (collector.collect().filter(USDOnlyRawFilter()) # only data in USD
                     .filter(StmtRawFilter(['BS']) # only data for balance sheets
                     .filter(...) #  
                     .save("./preparedset")) # save the filtered data 
  ```
- standardize the data for balance sheets, income statements, and cash flow statements to make reports easily comparable
  ```python
  company_collector = CompanyReportCollector.get_company_collector(ciks=[320193, 789019], # Apple, Microsoft
                                                                   post_load_filter=default_postloadfilter) # use predefined filter
  joined_bag = company_collector.collect().join() # load the sub_df, pre_df, and num_df from the zipfiles and join num and pre
  is_standardizer = IncomeStatementStandardizer()
  result_df = joined_bag.present(is_standardizer) # 
  ```


# Latest news / most important changes from previous versions
See the [Release Notes](https://hansjoergw.github.io/sec-fincancial-statement-data-set/releasenotes/) for details.
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


# Principles

The goal is to be able to do bulk processing of the data without the need to do countless API calls to sec.gov.

Therefore, the quarterly zip files are downloaded and indexed using a SQLite database table.
The index table contains information on all filed reports since about 2010, over 500,000 in total. The first
download will take a couple of minutes but after that, all the data is on your local harddisk.

Using the index in the sqlite db allows for direct extraction of data for a specific report from the
appropriate zip file, reducing the need to open and search through each zip file.

Moreover, the downloaded zip files are converted to the parquet format which provides faster read access
to the data compared to reading the csv files inside the zip files.

The library is designed to have a low memory footprint, only parsing and reading the data for a specific
report into pandas dataframe tables.


# Links

* [Release Notes](https://hansjoergw.github.io/sec-fincancial-statement-data-set/releasenotes/)
* [Documentation](https://hansjoergw.github.io/sec-fincancial-statement-data-set/)
* [QuickStart Jupyter Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/01_quickstart.ipynb)
* [Connect to the daily-sec-financial-statement-dataset Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/02_connect_rapidapi.ipynb)
* [Explore the data with an interactive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/03_explore_with_interactive_notebook.ipynb)
* [collector_deep_dive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/04_collector_deep_dive.ipynb)
* [filter_deep_dive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/05_filter_deep_dive.ipynb).
* [bulk_data_processing_deep_dive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/06_bulk_data_processing_deep_dive.ipynb)
* [standardizer_basics](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_00_standardizer_basics.ipynb)
* [BS_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_01_BS_standardizer.ipynb)
* [IS_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_02_IS_standardizer.ipynb)
* [CF_standardizer](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_03_CF_standardizer.ipynb) <br>



# Installation

The project is published on pypi.org. Simply use pip install to install it:

```
pip install secfsdstools
```

The library has been tested for python version 3.7, 3.8, 3.9, and 3.10

If you want to contribute, just clone the project and use a python 3.7 environment.
The dependencies are defined in the requirements.txt file or use the pyproject.toml to install them.



# Configuration

To configure the library, create a file called ".secfsdstools.cfg" in your home directory. The file only requires 
the following entries:

```
[DEFAULT]
downloaddirectory = c:/users/me/secfsdstools/data/dld
parquetdirectory = c:/users/me/secfsdstools/data/parquet
dbdirectory = c:/users/me/secfsdstools/data/db
useragentemail = your.email@goeshere.com
```

If you don't provide a config file, one will be created the first time you use the api and put it inside your home
directory. You can then change the content of it or directly start with the downloading of the data.

The download directory is the place where quarterly zip files from the sec.gov are downloaded to.
The parquet directory is the folder where the data is stored in parquet format.
The db directory is the directory in which the sqllite db is created.
The useragentemail is used in the requests made to the sec.gov website. Since we only make limited calls to the sec.gov,
you can leave the example "your.email@goeshere.com". 

# Attention when using on Windows
In order to support parallel processing, this library uses the multiprocessing package. For instance when transforming the
zip files to the parquet format or when reading data from different files.

However, in order for it to work on Windows when calling `python yourscript.py`, it is necessary that the logic
is started within the "main block" (`if __name__ == '__main__':`).

Of course, your main logic can be in another package that you import, but the "entry point" needs to be a "main block":

yourscript.py:
```
import yourpackage as yp

if __name__ == '__main__':
  yp.run()
```

Otherwise, you will observe the following kind of error messages:
```
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 116, in spawn_main
    exitcode = _main(fd, parent_sentinel)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 125, in _main
    prepare(preparation_data)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 236, in prepare
    _fixup_main_from_path(data['init_main_from_path'])
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 287, in _fixup_main_from_path
    main_content = runpy.run_path(main_path,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 269, in run_path
    return _run_module_code(code, init_globals, run_name,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 96, in _run_module_code
    _run_code(code, mod_globals, init_globals,
  ...
```

For details have a look at the python documentation:
- https://docs.python.org/3.10/library/multiprocessing.html#the-process-class
- https://docs.python.org/3.10/library/multiprocessing.html#multiprocessing-programming

It is not a problem if you run it inside Jupyter.


# Downloading the data files from sec and index the content

In order to download the data files and create the index, just call the `update()` method:

```
from secfsdstools.update import update

if __name__ == '__main__':
    update()
```

The following tasks will be executed:
1. All currently available zip-files are downloaded form sec.gov (these are over 50 files that will need over 2 GB of space on your local drive)
2. All the zipfiles are transformed and stored as parquet files. Per default, the zipfile is deleted afterwards. If you want to keep the zip files, set the parameter 'KeepZipFiles' in the config file to True.
3. An index inside a sqlite db file is created

If you don't call update "manually", then the first time you call a function from the library, a download will be triggered.

Moreover, at most once a day, it is checked if there is a new zip file available on sec.gov. If there is, a download will be started automatically. 
If you don't want 'auto-update', set the 'AutoUpdate' in your config file to False.


# Using the index db with a db browser in order to get an overview of all available report
___
**Note:** This is just if you are curious about the content of the database file. The library itself also contains functions to analyze the content of the database file.
___

The "index of reports" that was created in the previous step can be viewed using a database viewer that supports the SQLite format,
such as [DB Browser for SQLite](https://sqlitebrowser.org/).

(The location of the SQLite database file is specified in the "dbdirectory" field of the config file, which is set to
"<home>/secfsdstools/data/db" in the default configuration. The database file is named "secfsdstools.db".)

There are only two relevant tables in the database: "index_parquet_reports" and "index_parquet_processing_state".

The "index_parquet_reports" table provides an overview of all available reports in the downloaded
data and includes the following relevant columns:

* **adsh** <br>The unique id of the report (a string).
* **cik** <br>The unique id of the company (an int).
* **name** <br>The name of the company in uppercases.
* **form** <br>The type of the report (e.g.: annual: 10-K, quarterly: 10-Q).
* **filed** <br>The date when the report has been filed in the format YYYYMMDD (Note: this is stored as a number).
* **period** <br>The date for which the report was created (the date on the balancesheet). Also in the format YYYYMMDD.
* **fullPath** <br>The path to the downloaded zip files that contains the details of that report.
* **url** <br>The url which takes you directly to the filing of this report on the sec.gov website.

For instance, if you want to have an overview of all reports that Apple has filed since 2010,
just search for "%APPLE INC%" in the name column.

Searching for "%APPLE INC%" will also reveal its cik: 320193

If you accidentally delete data in the database file, don't worry. Just delete the database file
and run `update()` again (see previous chapter).


# Working with the SECFSDSTools library
Note: the code within this chapter is also contained in the "01_quickstart.ipynb" notebook. 
If you want to follow along, just open the notebook.

## A first simple example
Goal: present the information in the balance sheet of Apple's 2022 10-K report in the same way as it appears in the
original report on page 31 ("CONSOLIDATED BALANCE SHEETS"): https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm

````
from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_filter.rawfiltering import ReportPeriodAndPreviousPeriodRawFilter
from secfsdstools.e_presenter.presenting import StandardStatementPresenter

if __name__ == '__main__':
    # the unique identifier for apple's 10-K report of 2022
    apple_10k_2022_adsh = "0000320193-22-000108"
  
    # us a Collector to grab the data of the 10-K report. an filter for balancesheet information
    collector: SingleReportCollector = SingleReportCollector.get_report_by_adsh(
          adsh=apple_10k_2022_adsh,
          stmt_filter=["BS"]
    )  
    rawdatabag = collector.collect() # load the data from the disk
    
   
    bs_df = (rawdatabag
                       # ensure only data from the period (2022) and the previous period (2021) is in the data
                       .filter(ReportPeriodAndPreviousPeriodRawFilter())
                       # join the the content of the pre_txt and num_txt together
                       .join()  
                       # format the data in the same way as it appears in the report
                       .present(StandardStatementPresenter())) 
    print(bs_df) 
````

## Overview
The following diagram gives an overview on SECFSDSTools library.

![Overview](https://github.com/HansjoergW/sec-fincancial-statement-data-set/raw/main/docs/images/overview.png)

It mainly exists out of two main processes. The first one ist the "Date Update Process" wich is responsible for the
download of the Financial Statement Data Sets zip files from the sec.gov website, transforming the content into parquet
format, and indexing the content of these files in a simple SQLite database. Again, this whole process can be started
"manually" by calling the update method, or it is done automatically, as it described above.

The second main process is the "Data Processing Process", which is working with the data that is stored inside the
sub.txt, pre.txt, and num.txt files from the zip files. The "Data Processing Process" mainly exists out of four steps:

* **Collect** <br/> Collect the rawdata from one or more different zip files. For instance, get all the data for a single
report, or get the data for all 10-K reports of a single or multiple companies from several zip files.
* **Raw Processing** <br/> Once the data is collected, the collected data for sub.txt, pre.txt, and num.txt is available
as a pandas dataframe. Filters can be applied, the content can directly be saved and loaded.
* **Joined Processing** <br/> From the "Raw Data", a "joined" representation can be created. This joins the data from
the pre.txt and num.txt content together based on the "adhs", "tag", and "version" attributes. "Joined data" can also be
filtered, concatenated, directly saved and loaded.
* **Present** <br/> Produce a single pandas dataframe out of the data and use it for further processing or use the standardizers
 to create comparable data for the balance sheet, the income statement, and the cash flow statement.

The diagramm also shows the main classes with which a user interacts. The use of them  is described in the following chapters.

## General
Most of the classes you can interact with have a factory method which name starts with "get_". All this factory method
take at least one **optional** parameter called configuration which is of type "Configuration".

If you do not provide this parameter, the class will read the configuration info from you configuration file in your home
directory. If, for whatever reason, you do want to provide an alternative configuration, you can overwrite it.

However, normally you do not have to provide the "configuration" parameter.

## Index: working with the index
The first class that interacts with the index is the `IndexSearch` class. It provides a single method `find_company_by_name`
which executes a SQL Like search on the name of the available companies and returns a pandas dataframe with the columns
'name' and 'cik' (the central index key, or the unique id of a company in the financial statements data sets).
The main purpose of this class is to find the cik for a company (of course, you can also directly search the cik on https://www.sec.gov/edgar/searchedgar/companysearch).


```
from secfsdstools.c_index.searching import IndexSearch

index_search = IndexSearch.get_index_search()
results = index_search.find_company_by_name("apple")
print(results)
```

*Output:*
````
                           name      cik
      APPLE GREEN HOLDING, INC.  1510976
   APPLE HOSPITALITY REIT, INC.  1418121
                      APPLE INC   320193
         APPLE REIT EIGHT, INC.  1387361
          APPLE REIT NINE, INC.  1418121
         APPLE REIT SEVEN, INC.  1329011
             APPLE REIT SIX INC  1277151
           APPLE REIT TEN, INC.  1498864
         APPLETON PAPERS INC/WI  1144326
  DR PEPPER SNAPPLE GROUP, INC.  1418135
   MAUI LAND & PINEAPPLE CO INC    63330
          PINEAPPLE ENERGY INC.    22701
  PINEAPPLE EXPRESS CANNABIS CO  1710495
        PINEAPPLE EXPRESS, INC.  1654672
       PINEAPPLE HOLDINGS, INC.    22701
                PINEAPPLE, INC.  1654672
````


Once you have the cik of a company, you can use the `CompanyIndexReader` to get information on available reports of a company.
To get an instance of the class, you use the get `get_company_index_reader` method and provide the cik parameter.

````
from secfsdstools.c_index.companyindexreading import CompanyIndexReader

apple_cik = 320193
apple_index_reader = CompanyIndexReader.get_company_index_reader(cik=apple_cik)
````

First, you could use the method `get_latest_company_filing` which returns a dictionary with the latest filing of the company:

````
print(apple_index_reader.get_latest_company_filing())
````
*Output:*
````
{'adsh': '0001140361-23-023909', 'cik': 320193, 'name': 'APPLE INC', 'sic': 3571.0, 'countryba': 'US', 'stprba': 'CA', 'cityba': 'CUPERTINO', 
'zipba': '95014', 'bas1': 'ONE APPLE PARK WAY', 'bas2': None, 'baph': '(408) 996-1010', 'countryma': 'US', 'stprma': 'CA', 
'cityma': 'CUPERTINO', 'zipma': '95014', 'mas1': 'ONE APPLE PARK WAY', 'mas2': None, 'countryinc': 'US', 'stprinc': 'CA', 
'ein': 942404110, 'former': 'APPLE INC', 'changed': 20070109.0, 'afs': '1-LAF', 'wksi': 0, 'fye': '0930', 'form': '8-K', 
'period': 20230430, 'fy': nan, 'fp': None, 'filed': 20230510, 'accepted': '2023-05-10 16:31:00.0', 'prevrpt': 0, 'detail': 0, 
'instance': 'ny20007635x4_8k_htm.xml', 'nciks': 1, 'aciks': None}
````

Next there are two methods which return the metadata of the reports that a company has filed. The result is either
returned as a list of `IndexReport` instances, if you use the method `get_all_company_reports` or as pandas dataframe if
you use the method `get_all_company_reports_df`. Both method can take an optional parameter forms, which defines the
type of the report that shall be returned. For instance, if you are only interested in the annual and quarterly report,
set forms to `["10-K", "10-Q"]`.

````
# only show the annual reports of apple
print(apple_index_reader.get_all_company_reports_df(forms=["10-K"]))
````

*Output:*
````
                 adsh     cik       name  form     filed    period                                           fullPath  originFile originFileType                                                url
 0000320193-22-000108  320193  APPLE INC  10-K  20221028  20220930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2022q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0000320193-21-000105  320193  APPLE INC  10-K  20211029  20210930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2021q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0000320193-20-000096  320193  APPLE INC  10-K  20201030  20200930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2020q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0000320193-19-000119  320193  APPLE INC  10-K  20191031  20190930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2019q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0000320193-18-000145  320193  APPLE INC  10-K  20181105  20180930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2018q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0000320193-17-000070  320193  APPLE INC  10-K  20171103  20170930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2017q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001628280-16-020309  320193  APPLE INC  10-K  20161026  20160930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2016q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001193125-15-356351  320193  APPLE INC  10-K  20151028  20150930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2015q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001193125-14-383437  320193  APPLE INC  10-K  20141027  20140930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2014q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001193125-13-416534  320193  APPLE INC  10-K  20131030  20130930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2013q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001193125-12-444068  320193  APPLE INC  10-K  20121031  20120930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2012q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001193125-11-282113  320193  APPLE INC  10-K  20111026  20110930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2011q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001193125-10-238044  320193  APPLE INC  10-K  20101027  20100930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2010q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
 0001193125-09-214859  320193  APPLE INC  10-K  20091027  20090930  C:\Users\hansj\secfsdstools\data\parquet\quart...  2009q4.zip        quarter  https://www.sec.gov/Archives/edgar/data/320193...
````

## Collect: collecting the data for reports
The previously introduced `IndexSearch` and `CompanyIndexReader` let you know what data is available, but they do not
return the real data of the financial statements. This is what the `Collector` classes are used for.

All the `Collector` classes have their own factory method(s) which instantiates the class. Most of these factory methods
also provide parameters to filter the data directly when being loaded from the parquet files.
These are
* the `forms_filter` <br> lets you select which report type should be loaded (e.g. "10-K" or "10-Q").<br>
  Note: the fomrs filter affects all dataframes (sub, pre, num).
* the `stmt_filter` <br> defines the statements that should be loaded (e.g., "BS" if only "Balance Sheet" data should be loaded) <br>
  Note: the stmt filter only affects the pre dataframe.
* the `tag_filter` <br> defines the tags, that should be loaded (e.g., "Assets" if only the "Assets" tag should be loaded) <br>
  Note: the tag filter affects the pre and num dataframes.

It is also possible to apply filter for these attributes after the data is loaded, but since the `Collector` classes
apply this filters directly during the load process from the parquet files (which means that fewer data is loaded from
the disk) this is generally more efficient.

All `Collector` classes have a `collect` method which then loads the data from the parquet files and returns an instance
of `RawDataBag`. The `RawDataBag` instance contains then a pandas dataframe for the `sub` (subscription) data,
`pre` (presentation) data, and `num` (the numeric values) data.

The framework provides the following collectors:
* `SingleReportCollector` <br> As the name suggests, this `Collector` returns the data of a single report. It is 
  instantiated by providing the `adsh` of the desired report as parameter of the `get_report_by_adsh` factory method, 
  or by using an instance of the `IndexReport` as parameter of the `get_report_by_indexreport`. (As a reminder: 
  instances of `IndexReport` are returned by the `CompanyIndexReader` class).
  <br><br>*Example:*
    ````
    from secfsdstools.e_collector.reportcollecting import SingleReportCollector

    apple_10k_2022_adsh = "0000320193-22-000108"

    collector: SingleReportCollector = SingleReportCollector.get_report_by_adsh(adsh=apple_10k_2022_adsh)
    rawdatabag = collector.collect()

    # as expected, there is just one entry in the submission dataframe
    print(rawdatabag.sub_df)
    # just print the size of the pre and num dataframes
    print(rawdatabag.pre_df.shape)
    print(rawdatabag.num_df.shape)
    ````
    <br>*Output*:
    ````
                       adsh     cik       name     sic countryba stprba     cityba  ...
    0  0000320193-22-000108  320193  APPLE INC  3571.0        US     CA  CUPERTINO  ...
    (185, 10)
    (503, 9)  
    ````
    <br>

* `MultiReportCollector` <br> Contrary to the `SingleReportCollector`, this `Collector` can collect data from several
  reports. Moreover, the data of the reports are loaded in parallel, this  especially improves the performance if the
  reports are from different quarters (resp. are in different zip files). The class provides the factory methods 
  `get_reports_by_adshs` and `get_reports_by_indexreports`. The first takes a list of adsh strings, the second a list
  of `IndexReport` instances.
  <br><br>*Example:*
    ````
    from secfsdstools.e_collector.multireportcollecting import MultiReportCollector
    apple_10k_2022_adsh = "0000320193-22-000108"
    apple_10k_2012_adsh = "0001193125-12-444068"

    if __name__ == '__main__':
        # load only the assets tags that are present in the 10-K report of apple in the years
        # 2022 and 2012
        collector: MultiReportCollector = \
            MultiReportCollector.get_reports_by_adshs(adshs=[apple_10k_2022_adsh,
                                                             apple_10k_2012_adsh],
                                                      tag_filter=['Assets'])
        rawdatabag = collector.collect()
        # as expected, there are just two entries in the submission dataframe
        print(rawdatabag.sub_df)
        print(rawdatabag.num_df)  
    ```` 
  <br>*Output*:
    ````
                       adsh     cik       name     sic countryba stprba     cityba  ...          
    0  0000320193-22-000108  320193  APPLE INC  3571.0        US     CA  CUPERTINO  ...
    1  0001193125-12-444068  320193  APPLE INC  3571.0        US     CA  CUPERTINO  ...
    
                       adsh     tag       version coreg     ddate  qtrs  uom         value footnote
    0  0000320193-22-000108  Assets  us-gaap/2022        20210930     0  USD  3.510020e+11     None
    1  0000320193-22-000108  Assets  us-gaap/2022        20220930     0  USD  3.527550e+11     None
    2  0001193125-12-444068  Assets  us-gaap/2012        20110930     0  USD  1.163710e+11     None
    3  0001193125-12-444068  Assets  us-gaap/2012        20120930     0  USD  1.760640e+11     None  
    ````
    <br>
* `ZipCollector` <br> This `Collector` collects the data of one or more zip (resp. the folders that contain the parquet
  files of this zip files). And since every of the original zip files contains the data for one quarter, the names you provide
  in the `get_zip_by_name` or `get_zip_by_names` factory methods reflect the quarter which data you want to load: 
  e.g. `2022q1.zip`.
 
  <br><br>*Example:*
    ````
    from secfsdstools.e_collector.zipcollecting import ZipCollector

    # only collect the Balance Sheet of annual reports that
    # were filed during the first quarter in 2022
    if __name__ == '__main__':
        collector: ZipCollector = ZipCollector.get_zip_by_name(name="2022q1.zip",
                                                               forms_filter=["10-K"],
                                                               stmt_filter=["BS"])
    
        rawdatabag = collector.collect()
    
        # only show the size of the data frame
        # .. over 4000 companies filed a 10 K report in q1 2022
        print(rawdatabag.sub_df.shape)
        print(rawdatabag.pre_df.shape)
        print(rawdatabag.num_df.shape)    
    ```` 
  <br>*Output*:
    ````
    (4875, 36)
    (232863, 10)
    (2404949, 9)
    ````

* `CompanyReportCollector` <br> This class returns reports for one or more companies. The factory method 
  `get_company_collector` provides the parameter `ciks` which takes a list of cik numbers.
  <br><br>*Example:*
    ````
    from secfsdstools.e_collector.companycollecting import CompanyReportCollector
    
    if __name__ == '__main__':
        apple_cik = 320193
        collector = CompanyReportCollector.get_company_collector(ciks=[apple_cik],
                                                                 forms_filter=["10-K"])
    
        rawdatabag = collector.collect()
    
        # all filed 10-K reports for apple since 2010 are in the databag
        print(rawdatabag.sub_df)
    
        print(rawdatabag.pre_df.shape)
        print(rawdatabag.num_df.shape)    
    ```` 
  <br>*Output*:
    ````
                        adsh     cik       name     sic ...
    0   0000320193-22-000108  320193  APPLE INC  3571.0 ...
    1   0000320193-21-000105  320193  APPLE INC  3571.0 ...
    2   0000320193-20-000096  320193  APPLE INC  3571.0 ...
    3   0000320193-19-000119  320193  APPLE INC  3571.0 ...
    4   0000320193-18-000145  320193  APPLE INC  3571.0 ...
    5   0000320193-17-000070  320193  APPLE INC  3571.0 ...
    6   0001628280-16-020309  320193  APPLE INC  3571.0 ...
    7   0001193125-15-356351  320193  APPLE INC  3571.0 ...
    8   0001193125-14-383437  320193  APPLE INC  3571.0 ...
    9   0001193125-13-416534  320193  APPLE INC  3571.0 ...
    10  0001193125-12-444068  320193  APPLE INC  3571.0 ...
    11  0001193125-11-282113  320193  APPLE INC  3571.0 ...
    12  0001193125-10-238044  320193  APPLE INC  3571.0 ...
    13  0001193125-09-214859  320193  APPLE INC  3571.0 ...
    (2246, 10)
    (7925, 9)
    Process finished with exit code 0  
    ````

Have a look at the [collector_deep_dive notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/04_collector_deep_dive.ipynb).



## Raw Processing: working with the raw data
When the `collect` method of a `Collector` class is called, the data for the sub, pre, and num dataframes are loaded
and being stored in the sub_df, pre_df, and num_df attributes inside an instance of `RawDataBag`.

The `RawDataBag` provides the following methods:
* `save`, `load`<br> The content of a `RawDataBag` can be saved into a directory. Within that directory, 
   parquet files are stored for the content of the sub_df, pre_df, and num_df. In order to load this 
   data directly, the static method `RawDataBag.load()` can be used.
* `concat`<br> Several instances of a `RawDataBag` can be concatenated into one single instance. In order to do 
   that, the static method `RawDataBag.concat()` takes a list of RawDataBag as parameter.
* `join` <br> This method produces a `JoinedRawDataBag` by joining the content of the pre_df and num_df
   based on the columns adsh, tag, and version. It is an inner join. The joined dataframe appears as pre_num_df in
   the `JoinedRawDataBag`.
* `filter` <br> The filter method takes a parameter of the type `FilterRaw`, applies it to the data and
   produces a new instance of `RawDataBag` with the filtered data. Therefore, filters can also be chained like
   `a_filtered_RawDataBag = a_RawDataBag.filter(filter1).filter(filter2)`. Moreover, the `__get__item` method
   is forwarded to the filter method, so you can also write `a_filtered_RawDataBag = a_RawDataBag[filter1][filter2]`.

It is simple to write your own filters, just get some inspiration from the once that are already present in the
Framework (module `secfsdstools.e_filter.rawfiltering`:

* `AdshRawFilter` <br> Filters the `RawDataBag` instance based on the list of adshs that were provided in the constructor. <br>
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(AdshRawFilter(adshs=['0001193125-09-214859', '0001193125-10-238044']))
   ````
* `StmtRawFilter` <br> Filters the `RawDataBag`instance based on the list of statements ('BS', 'CF', 'IS', ...). <br>
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(StmtRawFilter(stmts=['BS', 'CF']))
   ````
* `TagRawFilter` <br> Filters the `RawDataBag`instance based on the list of tags that is provided. <br>
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(TagRawFilter(tags=['Assets', 'Liabilities']))
   ````
* `MainCoregRawFilter` <br> Filters the `RawDataBag` so that data of subsidiaries are removed.
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(MainCoregRawFilter()) 
   ````
* `ReportPeriodAndPreviousPeriodRawFilter` <br> The data of a report usually also contains data from previous years.
  However, often you want just to analyze the data of the current and the previous year. This filter ensures that
  only data for the current period and the previous period are contained in the data.
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(ReportPeriodAndPreviousPeriodRawFilter()) 
   ````
* `ReportPeriodRawFilter` <br> If you are just interested in the data of a report that is from the current period
  of the report then you can use this filter. For instance, if you use a `CompanyReportCollector` to collect all
  10-K reports of this company, you want to ensure that every report only contains data for its own period and not for
  previous periods.
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(ReportPeriodRawFilter()) 
   ````
* `OfficialTagsOnlyRawFilter` <br> Sometimes company provide their own tags, which are not defined by the us-gaap XBRL
  definition. In such cases, the version columns contains the value of the adsh instead of something like us-gab/2022.
  This filter removes unofficial tags.
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(OfficialTagsOnlyRawFilter()) 
   ````  
* `USDOnlyRawFilter` <br> Reports often also contain datapoints in other currency than USD. So it might happen that
  the same datapoint in a balance sheet is present in different currencies. If you are just interested in the USD
  currency, then we can use this filter.
   ````
   a_filtered_RawDataBag = a_RawDataBag.filter(USDOnlyRawFilter()) 
   ````  

Have a look at the [filter_deep_dive notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/05_filter_deep_dive.ipynb).



## Joined Processing: working with joined data
When the `join` method of a `RawDataBag` instance is called an instance of `JoinedDataBag` is returned. The returned
instance contains an attribute sub_df, which is a reference to the same sub_df that is in the `RawDataBag`.
In addition to that, the `JoinedDataBag` contains an attribut pre_num_df, which is an inner join of the pre_df and 
the num_df based on the columns adsh, tag, and version. Note that an entry in the pre_df can be joined with more than 
one entry in the num_df.

The `JoinedDataBag` provides the following methods:
* `save`, `load`<br> The content of a `JoinedDataBag` can be saved into a directory. Within that directory,
  parquet files are stored for the content of the sub_df, pre_df, and num_df. In order to load this
  data directly, the static method `JoinedDataBag.save()` can be used.
* `concat`<br> Several instances of a `JoinedDataBag` can be concatenated in one single instance. In order to do
  that, the static method `JoinedDataBag.concat()` takes a list of RawDataBag as parameter.
* `filter` <br> The filter method takes a parameter of the type `FilterJoined`, applies it to the data and
  produces a new instance of `JoinedDataBag` with the filtered data. Therefore, filters can also be chained like
  `a_filtered_JoinedDataBag = a_JoinedDataBag.filter(filter1).filter(filter2)`. Moreover, the `__get__item` method
  is forwarded to the filter method, so you can also write `a_filtered_JoinedDataBag = a_JoinedDataBag[filter1][filter2]`.
  **Note**: The same filters that are present for the `RawDataBag` are also available for the `JoinedDataBag`. Just
  look into the module `secfsdstools.e_filter.joinedfiltering`
* `present` <br> The idea of the present method is to make a final presentation of the data as pandas dataframe. 
  The method has a parameter presenter of type Presenter.

## Present
It is simple to write your own presenter classes. So far, the framework provides the following Presenter 
implementations (module `secfsdstools.e_presenter.presenting`):

* `StandardStatementPresenter` <br> This presenter provides the data in the same form, as you are used to see in
  the reports itself. For instance, the primary financial statements balance sheet, income statement, and cash flow
  display the different positions in rows and the columns contain the different dates/periods of the data.
  Let us say you want to recreate the BS information of the apples 10-K report of 2022, you would write:
  ````
  apple_10k_2022_adsh = "0000320193-22-000108"

  collector: SingleReportCollector = SingleReportCollector.get_report_by_adsh(
        adsh=apple_10k_2022_adsh,
        stmt_filter=["BS"]
  )
  rawdatabag = collector.collect()
  bs_df = rawdatabag.filter(ReportPeriodAndPreviousPeriodRawFilter())
                    .join()
                    .present(StandardStatementPresenter())
  print(bs_df) 
  ````
  <br>*Output*:
  ````  
                        adsh coreg                                              tag       version stmt  report  line     uom  negating  inpth  qrtrs_0/20220930  qrtrs_0/20210930
   0   0000320193-22-000108                  CashAndCashEquivalentsAtCarryingValue  us-gaap/2022   BS       5     3     USD         0      0        2.364600e+10        3.494000e+10
   1   0000320193-22-000108                            MarketableSecuritiesCurrent  us-gaap/2022   BS       5     4     USD         0      0        2.465800e+10        2.769900e+10
   2   0000320193-22-000108                           AccountsReceivableNetCurrent  us-gaap/2022   BS       5     5     USD         0      0        2.818400e+10        2.627800e+10
   3   0000320193-22-000108                                           InventoryNet  us-gaap/2022   BS       5     6     USD         0      0        4.946000e+09        6.580000e+09
   4   0000320193-22-000108                             NontradeReceivablesCurrent  us-gaap/2022   BS       5     7     USD         0      0        3.274800e+10        2.522800e+10
   5   0000320193-22-000108                                     OtherAssetsCurrent  us-gaap/2022   BS       5     8     USD         0      0        2.122300e+10        1.411100e+10
   6   0000320193-22-000108                                          AssetsCurrent  us-gaap/2022   BS       5     9     USD         0      0        1.354050e+11        1.348360e+11
   7   0000320193-22-000108                         MarketableSecuritiesNoncurrent  us-gaap/2022   BS       5    11     USD         0      0        1.208050e+11        1.278770e+11
   8   0000320193-22-000108                           PropertyPlantAndEquipmentNet  us-gaap/2022   BS       5    12     USD         0      0        4.211700e+10        3.944000e+10
   9   0000320193-22-000108                                  OtherAssetsNoncurrent  us-gaap/2022   BS       5    13     USD         0      0        5.442800e+10        4.884900e+10
   10  0000320193-22-000108                                       AssetsNoncurrent  us-gaap/2022   BS       5    14     USD         0      0        2.173500e+11        2.161660e+11
   11  0000320193-22-000108                                                 Assets  us-gaap/2022   BS       5    15     USD         0      0        3.527550e+11        3.510020e+11
   12  0000320193-22-000108                                 AccountsPayableCurrent  us-gaap/2022   BS       5    18     USD         0      0        6.411500e+10        5.476300e+10
   13  0000320193-22-000108                                OtherLiabilitiesCurrent  us-gaap/2022   BS       5    19     USD         0      0        6.084500e+10        4.749300e+10
   14  0000320193-22-000108                   ContractWithCustomerLiabilityCurrent  us-gaap/2022   BS       5    20     USD         0      0        7.912000e+09        7.612000e+09
   15  0000320193-22-000108                                        CommercialPaper  us-gaap/2022   BS       5    21     USD         0      0        9.982000e+09        6.000000e+09
   16  0000320193-22-000108                                    LongTermDebtCurrent  us-gaap/2022   BS       5    22     USD         0      0        1.112800e+10        9.613000e+09
   17  0000320193-22-000108                                     LiabilitiesCurrent  us-gaap/2022   BS       5    23     USD         0      0        1.539820e+11        1.254810e+11
   18  0000320193-22-000108                                 LongTermDebtNoncurrent  us-gaap/2022   BS       5    25     USD         0      0        9.895900e+10        1.091060e+11
   19  0000320193-22-000108                             OtherLiabilitiesNoncurrent  us-gaap/2022   BS       5    26     USD         0      0        4.914200e+10        5.332500e+10
   20  0000320193-22-000108                                  LiabilitiesNoncurrent  us-gaap/2022   BS       5    27     USD         0      0        1.481010e+11        1.624310e+11
   21  0000320193-22-000108                                            Liabilities  us-gaap/2022   BS       5    28     USD         0      0        3.020830e+11        2.879120e+11
   22  0000320193-22-000108           CommonStocksIncludingAdditionalPaidInCapital  us-gaap/2022   BS       5    31     USD         0      0        6.484900e+10        5.736500e+10
   23  0000320193-22-000108                     RetainedEarningsAccumulatedDeficit  us-gaap/2022   BS       5    32     USD         0      0       -3.068000e+09        5.562000e+09
   24  0000320193-22-000108        AccumulatedOtherComprehensiveIncomeLossNetOfTax  us-gaap/2022   BS       5    33     USD         0      0       -1.110900e+10        1.630000e+08
   25  0000320193-22-000108                                     StockholdersEquity  us-gaap/2022   BS       5    34     USD         0      0        5.067200e+10        6.309000e+10
   26  0000320193-22-000108                       LiabilitiesAndStockholdersEquity  us-gaap/2022   BS       5    35     USD         0      0        3.527550e+11        3.510020e+11
   27  0000320193-22-000108                    CommonStockParOrStatedValuePerShare  us-gaap/2022   BS       6     1     USD         0      1        0.000000e+00        0.000000e+00
   28  0000320193-22-000108                            CommonStockSharesAuthorized  us-gaap/2022   BS       6     2  shares         0      1        5.040000e+10        5.040000e+10
   29  0000320193-22-000108                                CommonStockSharesIssued  us-gaap/2022   BS       6     3  shares         0      1        1.594342e+10        1.642679e+10
   30  0000320193-22-000108                           CommonStockSharesOutstanding  us-gaap/2022   BS       6     4  shares         0      1        1.594342e+10        1.642679e+10  
  ````  
  If you compare this with the real report at https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm
  you will notice, that order of the tags and the values are the same.


* `Standardizer` <br>
  Even if xbrl is a standard on how to tag positions and numbers in financial statements, that doesn't mean that financial
  statements can then be compared easily. For instance, there are over 3000 tags which can be used in a balance sheet.
  Moreover, some tags can mean similar things or can be grouped behind a "parent" tag, which itself might not be present.
  For instance, "AccountsNoncurrent" is often not shown in statements. So you would find the position for "Accounts"
  and "AccountsCurrent", but not for "AccountsNoncurrent". Instead, only child tags for "AccountsNoncurrent" might be
  present.<br><br>
  The standardizer helps to solve these problems by unifying the information of financial statements.<br> <br>
  With the standardized financial statements, you can then actually compare the statements between different
  companies or different years, and you can use the dataset for ML. <br><br>
  Have a look at [standardizer_basics](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_00_standardizer_basics.ipynb) which explains it in more details.<br><br>

  * `BalanceSheetStandardizer` <br>
  The `BalanceSheetStandardizer` collects and/or calculates the following positions of balance sheets:  

    ````
    - Assets
      - AssetsCurrent
        - Cash
      - AssetsNoncurrent
    - Liabilities
      - LiabilitiesCurrent
      - LiabilitiesNoncurrent
    - Equity
      - HolderEquity (mainly StockholderEquity or PartnerCapital)
        - RetainedEarnings
        - AdditionalPaidInCapital
        - TreasuryStockValue
      - TemporaryEquity
      - RedeemableEquity
    - LiabilitiesAndEquity
    ````

    With just a few lines of code, you'll get a comparable dataset with the main positions of a balance sheet for Microsoft, Alphabet, and Amazon:
    (see the [stanardize the balance sheets and make them comparable notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_01_BS_standardizer.ipynb) for details)
     ````python
     from secfsdstools.e_collector.companycollecting import CompanyReportCollector
     from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, OfficialTagsOnlyRawFilter, USDOnlyRawFilter
     from secfsdstools.f_standardize.bs_standardize import BalanceSheetStandardizer
   
     bag = CompanyReportCollector.get_company_collector(ciks=[789019, 1652044,1018724]).collect() #Microsoft, Alphabet, Amazon
     filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][USDOnlyRawFilter()]
     joined_bag = filtered_bag.join()
   
     standardizer = BalanceSheetStandardizer()
   
     standardized_bs_df = joined_bag.present(standardizer)
   
     import matplotlib.pyplot as plt
     # Group by 'name' and plot equity for each group
     # Note: using the `present` method ensured that the same cik has always the same name even if the company name did change in the past
     for name, group in standardized_bs_df.groupby('name'):
       plt.plot(group['date'], group['Equity'], label=name, linestyle='-')
   
     # Add labels and title
     plt.xlabel('Date')
     plt.ylabel('Equity')
     plt.title('Equity Over Time for Different Companies (CIKs)')
   
     # Display legend
     plt.legend()
     ````
     ![Equity Compare](https://github.com/HansjoergW/sec-fincancial-statement-data-set/raw/main/docs/images/equity_compare.png)

  * `IncomeStatementStandardizer` <br>
  The `IncomeStatementStandardizer` collects and/or calculates the following positions of balance sheets:
    
    ````  
      Revenues
      - CostOfRevenue
      ---------------
      = GrossProfit
      - OperatingExpenses
      -------------------
      = OperatingIncomeLoss
        
      IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit
      - AllIncomeTaxExpenseBenefit
      ----------------------------
      = IncomeLossFromContinuingOperations
      + IncomeLossFromDiscontinuedOperationsNetOfTax
      -----------------------------------------------
      = ProfitLoss
      - NetIncomeLossAttributableToNoncontrollingInterest
      ---------------------------------------------------
      = NetIncomeLoss
    ````
  
    With just a few lines of code, you'll get a comparable dataset with the main positions of an income statement for Microsoft, Alphabet, and Amazon:
  (see the [standardize the income statement and make them comparable notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_02_IS_standardizer.ipynb) for details)
   
    ````python
    from secfsdstools.e_collector.companycollecting import CompanyReportCollector
    from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, OfficialTagsOnlyRawFilter, USDOnlyRawFilter
    from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer
      
    bag = CompanyReportCollector.get_company_collector(ciks=[789019, 1652044,1018724]).collect() #Microsoft, Alphabet, Amazon
    filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][USDOnlyRawFilter()]
    joined_bag = filtered_bag.join()
      
    standardizer = IncomeStatementStandardizer()
      
    standardized_is_df = joined_bag.present(standardizer)
    # just use the yearly reports with data for the whole year
    standardized_is_df = standardized_is_df[(standardized_is_df.fp=="FY") & (standardized_is_df.qtrs==4)].copy()
      
    import matplotlib.pyplot as plt
    # Group by 'name' and plot equity for each group
    # Note: using the `present` method ensured that the same cik has always the same name even if the company name did change in the past
    for name, group in standardized_is_df.groupby('name'):
      plt.plot(group['date'], group['GrossProfit'], label=name, linestyle='-')
      
    # Add labels and title
    plt.xlabel('Date')
    plt.ylabel('GrossProfit')
    plt.title('GrossProfit Over Time for Different Companies (CIKs)')
      
    # Display legend
    plt.legend()
     ````

  ![GrossProfit Compare](https://github.com/HansjoergW/sec-fincancial-statement-data-set/raw/main/docs/images/grossprofit_compare.png)

  * `CashFlowStandardizer` <br>
   The `CashFlowStandardizer` collects and/or calculates the following positions of cash flow statements:
     
    ````
     NetCashProvidedByUsedInOperatingActivities
       CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations
       NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
           DepreciationDepletionAndAmortization
           DeferredIncomeTaxExpenseBenefit
           ShareBasedCompensation
           IncreaseDecreaseInAccountsPayable
           IncreaseDecreaseInAccruedLiabilities
           InterestPaidNet
           IncomeTaxesPaidNet
    
     NetCashProvidedByUsedInInvestingActivities
         CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations
         NetCashProvidedByUsedInInvestingActivitiesContinuingOperations
           PaymentsToAcquirePropertyPlantAndEquipment
           ProceedsFromSaleOfPropertyPlantAndEquipment
           PaymentsToAcquireInvestments
           ProceedsFromSaleOfInvestments
           PaymentsToAcquireBusinessesNetOfCashAcquired
           ProceedsFromDivestitureOfBusinessesNetOfCashDivested
           PaymentsToAcquireIntangibleAssets
           ProceedsFromSaleOfIntangibleAssets
    
     NetCashProvidedByUsedInFinancingActivities
         CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations
         NetCashProvidedByUsedInFinancingActivitiesContinuingOperations
           ProceedsFromIssuanceOfCommonStock
           ProceedsFromStockOptionsExercised
           PaymentsForRepurchaseOfCommonStock
           ProceedsFromIssuanceOfDebt
           RepaymentsOfDebt
           PaymentsOfDividends
    
    
     EffectOfExchangeRateFinal
     CashPeriodIncreaseDecreaseIncludingExRateEffectFinal
    
     CashAndCashEquivalentsEndOfPeriod
    ````

     With just a few lines of code, you'll get a comparable dataset with the main positions of an cash flow statement for Microsoft, Alphabet, and Amazon:
(see the [standardize the cash flow statements and make them comparable](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_03_CF_standardizer.ipynb) for details)
    ````python
    from secfsdstools.e_collector.companycollecting import CompanyReportCollector
    from secfsdstools.e_filter.rawfiltering import ReportPeriodRawFilter, MainCoregRawFilter, OfficialTagsOnlyRawFilter, USDOnlyRawFilter
    from secfsdstools.f_standardize.cf_standardize import CashFlowStandardizer
    
    bag = CompanyReportCollector.get_company_collector(ciks=[789019, 1652044,1018724]).collect() #Microsoft, Alphabet, Amazon
    filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][USDOnlyRawFilter()]
    joined_bag = filtered_bag.join()
    
    standardizer = CashFlowStandardizer()
    
    standardized_cf_df = joined_bag.present(standardizer)
    standardized_cf_df = standardized_cf_df[(standardized_cf_df.fp=="FY") & (standardized_cf_df.qtrs==4)].copy()
    
    import matplotlib.pyplot as plt
    # Group by 'name' and plot NetCashProvidedByUsedInOperatingActivities for each group
    # Note: using the `present` method ensured that the same cik has always the same name even if the company name did change in the past
    for name, group in standardized_cf_df.groupby('name'):
        plt.plot(group['date'], group['NetCashProvidedByUsedInOperatingActivities'], label=name, linestyle='-')
    
    # Add labels and title
    plt.xlabel('Date')
    plt.ylabel('NetCashProvidedByUsedInOperatingActivities')
    plt.title('NetCashProvidedByUsedInOperatingActivities Over Time for Different Companies (CIKs)')
    
    # Display legend
    plt.legend()
    ````
  ![NetCashOperating Compare](https://github.com/HansjoergW/sec-fincancial-statement-data-set/raw/main/docs/images/netcashoperating_compare.png)


# What to explore further

* [QuickStart Jupyter Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/01_quickstart.ipynb)
* [Explore the data with an interactive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/03_explore_with_interactive_notebook.ipynb)
* [Connect to the daily-sec-financial-statement-dataset Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/02_connect_rapidapi.ipynb) 
* [collector_deep_dive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/04_collector_deep_dive.ipynb)
* [filter_deep_dive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/05_filter_deep_dive.ipynb).
* [bulk_data_processing_deep_dive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/06_bulk_data_processing_deep_dive.ipynb)
* [checkout the `u_usecases` package](https://hansjoergw.github.io/sec-fincancial-statement-data-set/doc_latest/api/secfsdstools/u_usecases/index.html)
* [standardize the balance sheets and make them comparable](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_01_BS_standardizer.ipynb)
* [standardize the income statements and make them comparable](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_02_IS_standardizer.ipynb)
* [standardize the cash flow statements and make them comparable](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/07_03_CF_standardizer.ipynb) <br>



# Troubleshooting

----
**Problem:** I receive error messages like the following when I try to start a script on windows:
````
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 116, in spawn_main
    exitcode = _main(fd, parent_sentinel)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 125, in _main
    prepare(preparation_data)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 236, in prepare
    _fixup_main_from_path(data['init_main_from_path'])
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 287, in _fixup_main_from_path
    main_content = runpy.run_path(main_path,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 269, in run_path
    return _run_module_code(code, init_globals, run_name,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 96, in _run_module_code
    _run_code(code, mod_globals, init_globals,
 ...
````

**Solution:** 
This library uses the multiprocessing package. However, on Windows this works only correctly if the "entry point" of the
script is within a `if __name__ == '__main__':` block.

Therefore, change your scripts from
````python
import xy

your code goes here
````

to 
````python
import xy

if __name__ == '__main__':
    your code goes here
````

For details have a look at the python documentation:
- https://docs.python.org/3.10/library/multiprocessing.html#the-process-class
- https://docs.python.org/3.10/library/multiprocessing.html#multiprocessing-programming














