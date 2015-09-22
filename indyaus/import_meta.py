"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Tooling to import metadata for Australia Rentals competition

### DEVELOPER NOTES:
  Depends on pyspark being available
"""
import csv
from pathlib import Path
from collections import defaultdict

from openpyxl import load_workbook

import pyspark.sql.types as types

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


def _determine_type(row_dict):
    """Determine the Spark.DataType required for the field described by this row"""
    if row_dict['data_type'] is None:
        return types.StringType
    if row_dict['data_type'].lower() == 'varchar2':
        return types.StringType
    if row_dict['data_type'].lower() == 'date':
        return types.DateType
    if row_dict['data_type'].lower() == 'number':
        if row_dict['data_scale'] is None:
            return types.IntegerType
        if row_dict['data_scale'] == 0:
            return types.IntegerType
        else:
            return types.FloatType


def import_meta(path_meta, name_ws='DataDict'):
    """Read the contest metadata into a Dict(Files) of Dict(Fields) of StructField objects"""

    # Load the workbook and then the worksheet
    wb_meta = load_workbook(
        str(path_meta),
        read_only=True,
        keep_vba=False,
        data_only=True,
        )
    ws_meta = wb_meta[name_ws]

    # Store the contents into a list of dicts
    #  - i.e. push the header into the rows
    row_dicts = []
    for i_row, row in enumerate(ws_meta.rows):
        if i_row == 0:
            field_names = [
                cell.value.lower()
                for cell in row
                if isinstance(cell.value, str)
                ]
            continue
        row_dicts.append({
            field_name: cell.value
            for field_name, cell in zip(field_names, row)
            })

    # Transform into a dict of dict of StructTypes
    table_schemas = defaultdict(dict)
    for row in row_dicts:
        table_schemas[row['table_name'].lower()][row['column_name'].lower()] = types.StructField(
            row['column_name'].lower(),
            _determine_type(row)(),
            nullable=True,
            metadata={'comment': row['comments']},
            )

    return table_schemas


def import_csv_headers(path_csvs):
    """Sniff the order of fields from the headers of the CSVs"""

    _headers = dict()
    for path_csv in path_csvs.glob('*.csv'):
        with path_csv.open() as fh_header:
            _wasted_reader = csv.DictReader(fh_header)
            _headers[path_csv.stem.lower()] = [
                fieldname.lower()
                for fieldname in _wasted_reader.fieldnames
                ]

    return _headers

def order_meta(meta_unordered, csv_headers):
    """Create the proper StructType objects for each CSV"""
    _ordered_meta = dict()

    for name_csv in csv_headers:
        try:
            _ordered_meta[name_csv] = types.StructType([
                meta_unordered[name_csv][name_field]
                for name_field in csv_headers[name_csv]
                ])
        except KeyError:
            print('MetaData mismatch for {}'.format(name_csv))

    return _ordered_meta

if __name__ == '__main__':

    path_test = Path(r'W:\NWS\Australia_Rentals\005_Raw_Data\data_dictionary.xlsx')
    print(import_meta(path_test))



