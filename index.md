---
layout: default
---

# Sec Financial Statement Data Sets Tools

Helper tools to analyze the Financial Statement Data Sets from the U.S. securities and exchange commission (sec.gov).

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


# Links
## General
* [ReleaseNotes](releasenotes/)
* [Github project](https://github.com/HansjoergW/sec-fincancial-statement-data-set)
* [Financial Statement Data Sets @ sec.gov](https://www.sec.gov/dera/data/financial-statement-data-sets)

## Versions
* [in development (main branch in Github repository)](doc_main/index.html)
* [latest stable version](doc_latest/index.html)
* [0.5.0](doc_v0.5.1/index.html)
