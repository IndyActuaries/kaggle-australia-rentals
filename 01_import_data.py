"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Drive the import process.

### DEVELOPER NOTES:
  Likely just want to serialize the raw data into a clean, rich typed format.
  Is written as a procedure, not a library.
"""

from pathlib import Path

import indyspark

indyspark.setup_spark_env()

from indyaus import get_spark_pieces, PATH_DATA

import indyspark.import_utils
import indyaus.import_meta as import_meta

import pyspark.sql.types as types

PATH_RAW = PATH_DATA / '005_Raw_Data'

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


superman, sc, sqlContext = get_spark_pieces('aus_import')

# Import the metadata (without proper field ordering)
meta_unordered = import_meta.import_meta(PATH_RAW / 'data_dictionary.xlsx')

# Apply hacky overrides for where the sources don't match the data dictionary
meta_unordered['valuation_entities']['ve_date_created'].dataType = types.TimestampType()
meta_unordered['valuation_entities']['ve_date_modified'].dataType = types.TimestampType()
meta_unordered['train'] = meta_unordered['wrk_train'].copy()
meta_unordered['test'] = meta_unordered['wrk_train'].copy()
del meta_unordered['test']['ren_base_rent']

meta_unordered['sample_submission'] = {
    'ren_id': types.StructField(
        'ren_id',
        types.StringType(),
        nullable=False,
        ),
    'ren_base_rent': types.StructField(
        'ren_base_rent',
        types.FloatType(),
        nullable=False,
        ),
    }

# Sniff the CSV files to get the field metadata into the correct order
csv_headers = import_meta.import_csv_headers(PATH_RAW)
meta_ordered = import_meta.order_meta(meta_unordered, csv_headers)

# Loop and import into a cleaner/richer storage format
for filename, filemeta in meta_ordered.items():
    print('\n**** Processing {} ****\n'.format(filename))
    indyspark.import_utils.import_csv(
        sqlContext,
        PATH_RAW / '{}.csv'.format(filename),
        filemeta,
        ).write.parquet(
            (PATH_DATA / '010_Imported_Data' / '{}.parquet'.format(filename)).as_uri(),
            mode='overwrite',
        )

superman.stop_cluster()
