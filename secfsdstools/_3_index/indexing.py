"""Indexing the downloaded to data"""

# DB-Accessor
# Dataclass für db content
# dataaccess in eigene classe auslagern, da die von mehreren stellen verwendet werden muss.
#
# logik
# - read all available quarterfiles
# - read index state
# - calculate which files have to be indexed
# - index missing
