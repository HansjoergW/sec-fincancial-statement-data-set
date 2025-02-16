"""
base constant values
"""
from typing import Dict

import pyarrow as pa

NUM_TXT = "num.txt"
PRE_TXT = "pre.txt"
SUB_TXT = "sub.txt"
PRE_NUM_TXT = "pre_num.txt"

NUM_COLS = ['adsh', 'tag', 'version', 'coreg', 'ddate', 'qtrs', 'uom',
            'segments', 'value', 'footnote']
PRE_COLS = ['adsh', 'report', 'line', 'stmt', 'inpth', 'rfile',
            'tag', 'version', 'plabel', 'negating']

SUB_COLS = ['adsh', 'form', 'period', 'filed', 'cik']

# period, filed, ddate as float, since period could contain NAs, which are not supported for int

SUB_DTYPE = {'adsh': str,
             'cik': int,
             'name': str,
             'sic': float,  # has to be read as float, since it could be empty in quarterly zips
             'fye': str,
             'form': str,
             'period': float,
             'filed': int,
             'accepted': str,
             'fy': float,  # has to be read as float, since it could be empty in quarterly zips
             'fp': str,
             'aciks': str}

NUM_DTYPE = {'adsh': str,
             'tag': str,
             'version': str,
             'coreg': str,
             'ddate': int,
             'qtrs': int,
             'uom': str,
             'segments': str,
             'value': str,  # daily files can also contain ticker name and stockexchange as strings
             'footnote': str}

PRE_DTYPE = {'adsh': str,
             'report': int,
             'line': float,  # may be nan in some entries
             'stmt': str,
             'inpth': int,
             'tag': str,
             'version': str,
             'negating': int}

# pyarrow schema map
PA_SCHEMA_MAP: Dict[str, pa.Schema] = {
    SUB_TXT: pa.schema([
        ("adsh", pa.string()),
        ('cik', pa.int32()),
        ('name', pa.string()),
        ('sic', pa.float64()),
        ('countryba', pa.string()),
        ('stprba', pa.string()),
        ('cityba', pa.string()),
        ('zipba', pa.string()),
        ('bas1', pa.string()),
        ('bas2', pa.string()),
        ('baph', pa.string()),
        ('countryma', pa.string()),
        ('stprma', pa.string()),
        ('cityma', pa.string()),
        ('zipma', pa.string()),
        ('mas1', pa.string()),
        ('mas2', pa.string()),
        ('countryinc', pa.string()),
        ('stprinc', pa.string()),
        ('ein', pa.float64()),
        ('former', pa.string()),
        ('changed', pa.float64()),
        ('afs', pa.string()),
        ('wksi', pa.int64()),
        ('fye', pa.string()),
        ('form', pa.string()),
        ('period', pa.int32()),
        ('fy', pa.float64()),
        ('fp', pa.string()),
        ('filed', pa.int32()),
        ('accepted', pa.string()),
        ('prevrpt', pa.int64()),
        ('detail', pa.int64()),
        ('instance', pa.string()),
        ('nciks', pa.int64()),
        ('aciks', pa.string()),
        ('__index__level_0__', pa.int64())

    ]),
    PRE_TXT: pa.schema([
        ("adsh", pa.string()),
        ("tag", pa.string()),
        ("version", pa.string()),
        ("report", pa.int32()),
        ("line", pa.int32()),
        ("stmt", pa.string()),
        ("inpth", pa.int32()),
        ("rfile", pa.string()),
        ("plabel", pa.string()),
        ("negating", pa.int32()),
        ('__index__level_0__', pa.int64())
    ]),
    NUM_TXT: pa.schema([
        ("adsh", pa.string()),
        ("tag", pa.string()),
        ("version", pa.string()),
        ("ddate", pa.int32()),
        ("qtrs", pa.int32()),
        ("uom", pa.string()),
        ("segments", pa.string()),
        ("coreg", pa.string()),
        ("value", pa.float64()),
        ('__index__level_0__', pa.int64())
    ]),
    PRE_NUM_TXT: pa.schema([
        ("adsh", pa.string()),
        ("tag", pa.string()),
        ("version", pa.string()),
        ("ddate", pa.int32()),
        ("qtrs", pa.int32()),
        ("uom", pa.string()),
        ("segments", pa.string()),
        ("coreg", pa.string()),
        ("value", pa.float64()),
        ("footnote", pa.string()),
        ("report", pa.int32()),
        ("line", pa.int32()),
        ("stmt", pa.string()),
        ("inpth", pa.int32()),
        ("rfile", pa.string()),
        ("plabel", pa.string()),
        ("negating", pa.int32()),
        ('__index_level_0__', pa.int64())
    ])
}
