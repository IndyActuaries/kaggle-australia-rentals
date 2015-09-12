"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Tooling to import csv files into Spark DataFrames.

### DEVELOPER NOTES:
  CSV files are second class citizens, so this is somewhat painful.
  There are some third party packages that provide much of this functionality,
    but they aren't perfect and I want to learn, so here we go.
  Most of the munging should be done within the Spark engine if possible.
"""

import csv

import pyspark.sql.types as types

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


def import_csv(
        sqlcon,
        path_csv,
        dict_schema,
        *,
        header=True,
        delimiter=','
    ):
    """
    Read in a CSV to a rich Spark DataFrame.
    """
    lines = sqlcon._sc.textFile(str(path_csv))
    if header:
        header_line = lines.first()
        lines = lines.filter(lambda l: l != header_line)

    return lines


if __name__ == '__main__':
    pass
