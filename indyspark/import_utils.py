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
        schema,
        *,
        header=True,
        delimiter=','
    ):
    """
    Read in a CSV to a rich Spark DataFrame.
    """
    assert isinstance(schema, types.StructType), '{} is not a pyspark StructType'.format(schema)

    lines = sqlcon._sc.textFile(str(path_csv))
    if header:
        header_line = lines.first()
        lines = lines.filter(lambda l: l != header_line)

    def _enrich_field(field_raw_value, field_type):
        """Convert a single raw string into the anticipated Python datatype for the field"""
        if isinstance(field_type, types.StringType):
            return field_raw_value
        if isinstance(field_type, (types.IntegerType, types.LongType, types.ShortType)):
            return int(field_raw_value)
        if isinstance(field_type, (types.FloatType, types.DoubleType)):
            return float(field_raw_value)

    def _parse_line(line, delimiter=delimiter, schema=schema):
        """Parse a single line (raw string) into a list of rich data types"""
        # Wastefully utilize a csv.reader object for a single line to handle messy csv nuances
        for row in csv.reader([line], delimiter=delimiter):
            return [
                _enrich_field(field_raw_value, field_struct.dataType)
                for field_raw_value, field_struct in zip(row, schema.fields)
                ]

    parts_enriched = lines.map(_parse_line)

    return sqlcon.createDataFrame(parts_enriched, schema)


if __name__ == '__main__':
    pass
