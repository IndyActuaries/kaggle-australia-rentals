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
from datetime import datetime

import pyspark.sql.types as types
from pyspark import StorageLevel

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


def import_csv(
        sqlcon,
        path_csv,
        schema,
        *,
        header=True,
        delimiter=',',
        na_strings={'na', 'n/a', 'null', ''}
    ):
    """Read in a CSV to a rich Spark DataFrame."""
    assert isinstance(schema, types.StructType), '{} is not a pyspark StructType'.format(schema)

    def _enrich_field(field_raw_value, field_type):
        """Convert a single raw string into the anticipated Python datatype for the field"""
        if field_raw_value is None:
            return None
        if field_raw_value.lower() in na_strings:
            return None
        if isinstance(field_type, types.StringType):
            return field_raw_value
        if isinstance(field_type, (types.IntegerType, types.LongType, types.ShortType)):
            return int(field_raw_value)
        if isinstance(field_type, (types.FloatType, types.DoubleType)):
            return float(field_raw_value)
        if isinstance(field_type, types.DateType):
            try:
                return datetime.strptime(field_raw_value, '%Y-%m-%d').date()
            except ValueError:
                return datetime.strptime(field_raw_value, '%d/%m/%Y').date()
        if isinstance(field_type, types.TimestampType):
            try:
                return datetime.strptime(field_raw_value, '%d/%m/%Y %H:%M:%S')
            except ValueError:
                return datetime.strptime(field_raw_value, '%Y-%m-%d %H:%M:%S')

    _field_types = [field.dataType for field in schema.fields]

    def _parse_lines(iterator):
        """Parse an iterator of lines (raw strings) into lists of rich data types"""
        # Utilize a csv.reader object to handle messy csv nuances
        for row in csv.reader(iterator, delimiter=delimiter):
            yield [
                _enrich_field(field_raw_value, field_type)
                for field_raw_value, field_type in zip(row, _field_types)
                ]

    # Start defining the data pipeline
    lines = sqlcon._sc.textFile(str(path_csv))

    if header:
        header_line = lines.first()
        lines = lines.filter(lambda l: l != header_line)

    parts_enriched = lines.mapPartitions(_parse_lines)

    typed_dataframe = sqlcon.createDataFrame(parts_enriched, schema)
    typed_dataframe.persist(StorageLevel.MEMORY_AND_DISK_SER)

    return typed_dataframe


if __name__ == '__main__':
    pass
