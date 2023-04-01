"""
base constant values
"""

NUM_TXT = "num.txt"
PRE_TXT = "pre.txt"
SUB_TXT = "sub.txt"

NUM_COLS = ['adsh', 'tag', 'version', 'coreg', 'ddate', 'qtrs', 'uom', 'value', 'footnote']
PRE_COLS = ['adsh', 'report', 'line', 'stmt', 'inpth', 'rfile',
            'tag', 'version', 'plabel', 'negating']

SUB_COLS = ['adsh', 'form', 'period', 'filed', 'cik']

# period, filed, ddate as float, since period could contain NAs, which are not supported for int

SUB_DTYPE = {'adsh': str,
             'cik': int,
             'name': str,
             'sic':float, # has to be read as float, since it could be empty in quarterly zips
             'fye': str,
             'form':str,
             'period':float,
             'filed':float,
             'accepted':str,
             'fy':float, # has to be read as float, since it could be empty in quarterly zips
             'fp':str,
             'aciks':str}

NUM_DTYPE = {'adsh': str,
             'tag': str,
             'version':str,
             'coreg':str,
             'ddate':float,
             'qtrs':int,
             'uom':str,
             'value': str,
             'footnote': str}

PRE_DTYPE = {'adsh':str,
             'report':int,
             'line':float, # todo: ist in den daten falsch
             'stmt':str,
             'inpth':int,
             'tag':str,
             'version':str,
             'negating': int}