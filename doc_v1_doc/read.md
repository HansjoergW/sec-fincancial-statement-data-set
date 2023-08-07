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
of data - over 120 million data points in over 2GB of zip files.

This library simplifies the process of working with this data and provides a
convenient way to extract information from the primary financial statements - the balance sheet, income statement, and
statement of cash flows.

It also provides an integration with
the https://rapidapi.com/hansjoerg.wingeier/api/daily-sec-financial-statement-dataset API
and therefore providing a possibility to receive the latest filings on a daily basis and not just every three months.

# Important: The API was redesigned completely from version 0.5 to version 1.0  

Please read the chapter "Working with the SEFSDSTools library" carefully to understand how the API is working now.


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

# Downloading the data files from sec and index the content

In order to download the data files and create the index, just call the `update()` method:

```
from secfsdstools.update import update

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


# Working with the SEFSDSTools library
## Overview
The following diagram gives an overview on SECFSDSTools library.

link to diagramm had to be added

It mainly exists out of two main processes. The first one ist the "Date Update Process" wich is responsible for the
download of the Financial Statement Data Sets zip files from the sec.gov website, transforming the content into parquet
format, and indexing the content of these files in a simple SQLite database. Again, this whole process can be started
"manually" by calling the update method, or it is done automatically, as it described above.

The second main process is the "Data Processing Process", which is working with the data that is stored inside the
sub.txt, pre.txt, and num.txt files from the zip files. The "Data Processing Process" mainly exists out of four steps:

* **Collect** <br/> Collect the rawdata from one or more different zip files. For instance, get all the data for a single
report, or get the data for all 10-K reports of a single report from several zip files.
* **Raw Processing** <br/> Once the data is collected, the collected data for sub.txt, pre.txt, and num.txt is available
as a pandas dataframe. Filters can be applied, the content can directly be saved and loaded.
* **Joined Processing** <br/> From the "Raw Data", a "joined" representation can be created. This joins the data from
the pre.txt and num.txt content together based on the "adhs", "tag", and "version" attributes. "Joined data" can also be
filtered, concatenated, directly saved and loaded.
* **Present** <br/> Produce a single pandas dataframe out of the data and use it for further processing.

The diagramm also shows the main classes with which a user interacts. The use of them  is described in the following chapters.

## Index: working with the index

## Collect: collecting the data for reports

## Raw Processing: working with the raw data

## Joined Processing: working with joined data

## Present: 


## Final remarks
no copy of the data, always just filters, no copy

not static, you can access the raw or joined data always directly






Overview - Graphic
- Index
- Collectors
  - type of collectors 
- RawDataBag
    - does not produce copies
    - filter can be cascaded (also with shortcut)
    - pure dataframes are accessible
- JoinedDataBag
  -- does not produce copies


## Browsing the index 

Searching companies -> cik -> simple like statement
finding reports of a company, filter by name

## Analyzing the data
### Concept




tips
- generally more performant to apply filters already on collectors






## Getting the data in your python code

___
**Note:** The following code works only after the `update()` method was successfully executed and the quarterly zip
files from the sec.gov were downloaded and indexed
___

Inside the package `secfsdstools.e_read` are several modules that help to read the detail information from the zip
files.

### Module `companyreading`

___
**Note:** the code in this chapter is available in the module `secfsdstools.x_examples.examplecompanyreading`
___

Inside the module `secfsdstools.e_read.companyreading` the `CompanyReader` class is defined.

You will need the cik-number to get an instance for a certain company. The cik can be found either by searching in the
index_reports table or on the [sec.gov website](https://www.sec.gov/edgar/searchedgar/companysearch).

The following example shows how to create a `CompanyReader` instance for apple (which cik is 320193):

```
from typing import Dict, List

from secfsdstools.c_index.indexdataaccess import IndexReport
from secfsdstools.e_read.companyreading import CompanyReader


if __name__ == '__main__':
    apple_cik: int = 320193
    apple_reader = CompanyReader.get_company_reader(apple_cik)
```

Next, you can get the data of the latest filing of the company. This is the content of the entry in the sub.txt file
inside the zipped data. Besides basic information about the report, it contains also basic information of the company,
like the address.

For details about the fields, see https://www.sec.gov/files/aqfs.pdf.

```
    latest_filing: Dict[str, str] = apple_reader.get_latest_company_filing()
    print(latest_filing) 
```

Now, lets have a look at all the reports apple has filed. There are two methods, one of them returning a pandas
dataframe and the other a list of `secfsdstools.c_index.indexdataaccess.IndexReport` instances.

```
    # get basic infos of all the reports the company has filed.
    # ... first as a pandas DataFrame
    apple_all_reports_df = apple_reader.get_all_company_reports_df()

    # ... second as list of IndexReport instances
    apple_all_reports: List[IndexReport] = apple_reader.get_all_company_reports()
    print("first entry: ", apple_all_reports[0])

    # both method can also be used with filters for the form, the report type.
    # for instance, if you are only interested in annual and quarter reports, you can use
    apple_10k_and_10q_reports_df = apple_reader.get_all_company_reports_df(forms=['10-K','10-Q'])
    print(apple_10k_and_10q_reports_df)
    
```

This is the same information that you see when you browse the "index_reports" table as described above
under **Using the index db with a db browser**.

Next, we will see how we can read the detailed information for a report. For instance, how you can
reproduce the content of the primary financial statements of a report (BalanceSheet, IncomeStatement, CashFlow).

### Module `reportreading`

___
**Note:** the code in this chapter is available in the module `secfsdstools.x_examples.examplecreportreading`.
___

The ReportReader class enables us to access the real data of a single report. It provides two class methods which
help to create a ReportReader either by the unique report id "adsh" or by an instance of IndexReport
(which is returned by one of the methods shown in the last section).

in order to create an instance based on the adsh itself, you can use the following code:

```
from secfsdstools.e_read.reportreading import ReportReader

if __name__ == '__main__':
    # id apples 10k report from september 2022
    adsh_apple_10k_2022 = '0000320193-22-000108'
    
    apple_10k_2022_reader = ReportReader.get_report_by_adsh(adsh=adsh_apple_10k_2022)
```

The data of the report is split up in "pre.txt" and the "num.txt" files inside the zip file.
In order to get the raw content of them, there are the following methods available which return a pandas dataframes.

```
    # reading the raw content of the num and pre files
    raw_pre_df = apple_10k_2022_reader.get_raw_pre_data()
    raw_num_df = apple_10k_2022_reader.get_raw_num_data()
```

However, the data is more useful if the data of these two datasets is merged together, so that
the primary financial statements (BalanceSheet, IncomeStatement, CashFlow) can be reproduced.

There are several methods that can be used. First, let's have a look at the `merge_pre_and_num` method.

```
    # just merge the data of the num and pre dataframes, without pivoting the data -> the ddate stays as column
    # setting the use_period parameter to true, we will just keep the data for the current year.
    # if we also set the use_previous_period parameter to True, we would also keep the data of the previous year.
    apple_10k_2020_current_year_merged = apple_10k_2022_reader.merge_pre_and_num(use_period=True)
```

Second, let's hava a look at the methods, which also pivot the data. This means that every ddate value has its own
column.

There are two methods, which do exactly do that. The first one returns only the data of the current period and the
second also returns the content for the previous year. (provided that this information is present in the report)

```
    # merging the data from num and pre together and produce the primary financial statements
    apple_10k_2022_current_year_df = apple_10k_2022_reader.financial_statements_for_period()
    apple_10k_2022_current_and_previous_year_df = \
        apple_10k_2022_reader.financial_statements_for_period_and_previous_period()

```

Now lets filter for the BalanceSheet, IncomeStatement, and CashFlow for the current and previous year:

```
    # Filter for BalanceSheet
    apple_10k_2022_bs_df = apple_10k_2022_current_and_previous_year_df[
        apple_10k_2022_current_and_previous_year_df.stmt == 'BS']

    # Filter for IncomeStatement
    apple_10k_2022_is_df = apple_10k_2022_current_and_previous_year_df[
        apple_10k_2022_current_and_previous_year_df.stmt == 'IS']

    # Filter for CashFlow
    apple_10k_2022_cf_df = apple_10k_2022_current_and_previous_year_df[
        apple_10k_2022_current_and_previous_year_df.stmt == 'CF']  
```

If you compare the content of the balance sheet dataframe with
[apple's 10-K report from 2022](https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm#ief5efb7a728d4285b6b4af1e880101bc_85)
you see that the structure and the content is indeed the same.

The following readers, which share the basic interface of the ReportReader (methods `merge_pre_and_num`
, `financial_statements_for_period`,
`financial_statements_for_period_and_previous_period`), are also available. Please have a look at the Quickstart Jupyter
Notebook to get an idea about how they can be used.

* ZipReportReader <br> Reads all reports of a single zipfile at once
* MultiReportReader <br> Reads several reports of different zipfiles and concats their data in the same dataframes.
* CompanyCollector <br> Reads all reports of one company from different zipfile and concats the data into the same dataframes.


Also checkout the example Jupyter Notebooks:

* [QuickStart Jupyter Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/01_quickstart.ipynb)
* [Connect to the daily-sec-financial-statement-dataset Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/02_connect_rapidapi.ipynb) 
* [Explore the data with an interactive Notebook](https://nbviewer.org/github/HansjoergW/sec-fincancial-statement-data-set/blob/main/notebooks/03_explore_with_interactive_notebook.ipynb)


















