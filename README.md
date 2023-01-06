# sec-fincancial-statement-data-set

Helper tools to analyze the [Financial Statement Data Sets](https://www.sec.gov/dera/data/financial-statement-data-sets)
from the U.S. securities and exchange commission (sec.gov).

For a detail description of the content and the structure of the dataset, see https://www.sec.gov/files/aqfs.pdf.

# Installation

The project is published on pypi.org. Simply use pip install to install it:

```
pip install secfsdstools
```

If you want to contribute, just clone the project and use a python 3.8 environment.
The dependencies are defined in the requirements.txt file.

# Principles

In order to be able to work with the data, they first have to be downloaded and
indexed. The index is created in a simple sqlite db.

# Configuration

In order to do that, a configuration file has to be crated.
The easiest thing to do is to create the file ".secfsdstools.cfg" within your home directory.

As an alternative, you can also create it in your working dir where you plan to launch the code.

The file only has 3 entries:

```
[DEFAULT]
downloaddirectory = c:/users/me/secfsdstools/data/dld
dbdirectory = c:/users/me/secfsdstools/data/db
useragentemail = your.email@goeshere.com
```

The download directory is the place where financial data sets files are downloaded to.
The db directory is the directory in which the sqllite db is created.
When connecting to sec site, you should provide the user-agent email.

Note, if you call `update()` (see the next chapter), without having an existing configuration
file, it will fail, but it will create one in the user home directory with default settings.

If the default settings are ok, just run `update()` again.

# Downloading the data files from sec and index the content

In order to download the data files and create the indexes, you just need to call

```
from secfsdstools.update import update

update()
```

# How to work with the data

## Using the index db with a db browser

Since an index of the reports was created in the previous step, you can also have a look at that.
All you need to do is to have Database Viewer that supports the sqlite format.

For instance, I use the [DB Browser for SQLite](https://sqlitebrowser.org/).

The location of the sqlite database file is configured as "dbdirectory" in the config file.
If you use the default settings, it will be in your user home directory under "secfsdstools/data/db".
The name of the file is "secfsdstools.db".

There are only two tables within the db: "index_reports" and "index_file_processing_state".

The interesting one of these is "index_reports".
It gives an overview of all the available reports in the downloaded data and contains the
following relevant columns:

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

## Getting the data in your python code

___
**Note:** The following cod is working only after `update()` method was called and therefore the quarterly zip files
from the sec.gov have been downloaded and indexed
___

Inside the package `secfsdstools._4_read` are several modules that help to read the detail information from zip files.

### Module `companyreading`

Inside the module `secfsdstools._4_read.companyreading` the `CompanyReader` class is defined.
You will need the cik-number to get an instance for a certain company. The cik can be find either by searching in the
index_reports table or on the [sec.gov website](https://www.sec.gov/edgar/searchedgar/companysearch).

The following example shows how to create a `CompanyReader` instance for apple (which has the cik 320193):

```
from secfsdstools._4_read.companyreading import CompanyReader


if __name__ == '__main__':
    apple_cik: int = 320193
    apple_reader = CompanyReader.get_company_reader(apple_cik)
```

details company laden (von letztem filling)
alle reports einer company laden
nur die jahresreports laden
daraus die detail infos laden

details eines reports laden über adsh
erzeugen der financial statements
Filter für Bilanz, IS, Cashflow















