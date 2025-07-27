---
type: "agent_requested"
description: "Architecture patterns and design conventions for the secfsdstools project"
---

# Architecture Patterns - secfsdstools Project

## Overview
This document defines the architectural patterns and design conventions used throughout the secfsdstools project. Following these patterns ensures consistency and maintainability.

## Core Architecture Patterns

### 1. Factory Method Pattern
**Convention**: Use `get_*` class methods for object creation
```python
# ✅ Correct pattern
collector = SingleReportCollector.get_report_by_adsh(adsh=apple_10k_2022_adsh)
index_search = IndexSearch.get_index_search()
company_reader = CompanyIndexReader.get_company_index_reader(cik=apple_cik)

# ❌ Avoid direct instantiation for main API classes
collector = SingleReportCollector(...)  # Don't do this
```

### 2. Collector Pattern
**Purpose**: Collect and aggregate data from various sources
**Naming**: `*Collector` classes with `collect()` method
```python
# All collectors follow this pattern:
collector = SomeCollector.get_*(...) 
rawdatabag = collector.collect()  # Always returns RawDataBag or similar
```

**Key Collectors**:
- `SingleReportCollector` - Single report data
- `MultiReportCollector` - Multiple reports
- `CompanyReportCollector` - All reports for companies
- `ZipCollector` - Data from zip files

### 3. Filter Pattern
**Purpose**: Filter data at different processing stages
**Naming**: `*Filter` classes, chainable with `[]` operator
```python
# Raw data filters (before joining)
filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][USDOnlyRawFilter()]

# Joined data filters (after joining)
filtered_joined = joined_bag[SomeJoinedFilter()]
```

**Filter Types**:
- `*RawFilter` - Applied to RawDataBag
- `*JoinedFilter` - Applied to JoinedDataBag

### 4. Presenter Pattern
**Purpose**: Format data for display/output
**Usage**: Called via `.present()` method on JoinedDataBag
```python
presentation = joined_bag.present(StandardStatementPresenter())
```

### 5. Standardizer Pattern
**Purpose**: Standardize financial statement data
**Naming**: `*Standardizer` classes
```python
standardizer = BalanceSheetStandardizer()
standardized_bag = standardizer.standardize(joined_bag)
```

## Data Flow Architecture

### Standard Processing Pipeline
```python
# 1. Collect raw data
collector = SomeCollector.get_*(...) 
rawdatabag = collector.collect()

# 2. Filter raw data
filtered_bag = rawdatabag[Filter1()][Filter2()]

# 3. Join data
joined_bag = filtered_bag.join()

# 4. Present or standardize
result = joined_bag.present(SomePresenter())
# OR
standardized = SomeStandardizer().standardize(joined_bag)
```

### Data Container Hierarchy
- `RawDataBag` - Contains sub_df, pre_df, num_df
- `JoinedDataBag` - Joined/pivoted data ready for analysis
- `StandardizedBag` - Standardized financial statement data

## Configuration Management

### Configuration Pattern
```python
# ✅ Preferred: Use ConfigurationManager
configuration = ConfigurationManager.read_config_file()

# ✅ Alternative: Pass configuration explicitly
def some_method(configuration: Optional[Configuration] = None):
    if configuration is None:
        configuration = ConfigurationManager.read_config_file()
```

### Database Access Pattern
```python
# ✅ Standard pattern for database access
dbaccessor = ParquetDBIndexingAccessor(db_dir=configuration.db_dir)
```

## Error Handling Patterns

### Logging Pattern
```python
import logging

LOGGER = logging.getLogger(__name__)  # Always at module level

def some_method():
    LOGGER.info("Starting process")
    try:
        # ... processing
        LOGGER.debug("Detailed debug info")
    except SpecificException as ex:
        LOGGER.error("Specific error occurred: %s", ex)
        raise
    except Exception as ex:
        LOGGER.error("Unexpected error in %s: %s", __name__, ex)
        raise
```

### Exception Handling
```python
# ✅ Use specific exceptions
if not file_exists:
    raise FileNotFoundError(f"File not found: {file_path}")

# ✅ Provide context in error messages
raise ValueError(f"Invalid CIK {cik}. Must be positive integer.")

# ✅ Re-raise with context
try:
    process_data()
except DataProcessingError as ex:
    LOGGER.error("Failed to process data for company %s: %s", company_name, ex)
    raise
```

## Class Design Patterns

### Abstract Base Classes
```python
# Use ABC for base classes that define interfaces
from abc import ABC, abstractmethod

class BaseCollector(ABC):
    @abstractmethod
    def collect(self) -> RawDataBag:
        """Collect data and return RawDataBag"""
```

### Rule-Based Processing
**Used in**: Standardization framework
```python
class SomeRule(AbstractRule):
    def get_input_tags(self) -> Set[str]:
        return {"tag1", "tag2"}
    
    def process(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # Apply rule logic
        return data_df
```

### Task Framework Pattern
**Used in**: Automation and pipeline processing
```python
class SomeProcess(AbstractProcess):
    def create_tasks(self) -> List[Task]:
        # Create list of tasks to execute
        pass
    
    def process(self):
        # Execute all tasks
        pass
```

## Naming Conventions

### Module Organization
- `a_*` - Utilities and configuration
- `b_*` - Setup and initialization
- `c_*` - Core functionality (index, automation)
- `d_*` - Data containers and models
- `e_*` - Data collection, filtering, presentation
- `f_*` - Data standardization
- `g_*` - Processing pipelines
- `x_*` - Examples and demonstrations

### Class Naming Patterns
- `*Collector` - Data collection classes
- `*Filter` - Data filtering classes
- `*Presenter` - Data presentation classes
- `*Standardizer` - Data standardization classes
- `*Accessor` - Database access classes
- `*Process` - Pipeline/automation processes
- `*Rule` - Business rule implementations

### Method Naming Patterns
- `get_*` - Factory methods (class methods)
- `create_*` - Object creation methods
- `process_*` - Data processing methods
- `read_*` - Data reading methods
- `write_*` - Data writing methods

## Financial Domain Conventions

### Common Identifiers
- `cik` - Central Index Key (company identifier)
- `adsh` - Accession Number (filing identifier)
- `tag` - XBRL tag name
- `stmt` - Statement type ("BS", "IS", "CF", "EQ")
- `form` - SEC form type ("10-K", "10-Q", "8-K")

### Data Structure Conventions
- `sub_df` - Submission data (company/filing metadata)
- `pre_df` - Presentation data (how data is presented)
- `num_df` - Numerical data (actual financial numbers)

### Financial Statement Types
- `BS` - Balance Sheet
- `IS` - Income Statement  
- `CF` - Cash Flow Statement
- `EQ` - Statement of Equity

## Performance Considerations

### Pandas Operations
```python
# ✅ Use vectorized operations
df['new_col'] = df['col1'] * df['col2']

# ❌ Avoid loops when possible
for idx, row in df.iterrows():  # Slow
    df.at[idx, 'new_col'] = row['col1'] * row['col2']
```

### Memory Management
```python
# ✅ Use appropriate data types
df['category_col'] = df['category_col'].astype('category')

# ✅ Filter early to reduce memory usage
filtered_df = df[df['relevant_column'] == target_value]
```

### Parallel Processing
```python
# ✅ Use ParallelExecutor for CPU-intensive tasks
executor = ParallelExecutor(parallel=True)
results = executor.execute(tasks)
```

## Common Code Examples

### Typical Data Processing Workflow
```python
def process_company_data(cik: int, forms: List[str] = None) -> pd.DataFrame:
    """Process financial data for a company following standard patterns."""
    # 1. Get configuration
    configuration = ConfigurationManager.read_config_file()

    # 2. Collect data using factory method
    collector = CompanyReportCollector.get_company_collector(
        ciks=[cik],
        forms_filter=forms or ["10-K", "10-Q"]
    )
    rawdatabag = collector.collect()

    # 3. Apply filters in chain
    filtered_bag = (rawdatabag
                   [ReportPeriodRawFilter()]
                   [MainCoregRawFilter()]
                   [USDOnlyRawFilter()])

    # 4. Join and present
    joined_bag = filtered_bag.join()
    return joined_bag.present(StandardStatementPresenter())
```

### Error Handling with Context
```python
def safe_data_collection(adsh: str) -> Optional[RawDataBag]:
    """Collect data with proper error handling."""
    try:
        collector = SingleReportCollector.get_report_by_adsh(adsh=adsh)
        return collector.collect()
    except FileNotFoundError:
        LOGGER.warning("Report data not found for adsh: %s", adsh)
        return None
    except Exception as ex:
        LOGGER.error("Failed to collect data for adsh %s: %s", adsh, ex)
        raise DataCollectionError(f"Unable to process report {adsh}") from ex
```

This architecture guide should be used alongside the Python coding rules and testing guidelines to ensure consistent, maintainable code throughout the project.
