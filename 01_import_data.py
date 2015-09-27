"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Drive the import process.

### DEVELOPER NOTES:
  Likely just want to serialize the raw data into a clean, rich typed format.
"""

from pathlib import Path

import indyspark

indyspark.setup_spark_env()

import indyspark.import_utils
import indyaus.import_meta as import_meta

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.types as types

PATH_DATA = Path(r'W:\NWS\Australia_Rentals')
PATH_RAW = PATH_DATA / '005_Raw_Data'

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================

superman = indyspark.SparkCluster(n_workers=6, spark_worker_memory='4G')
superman.start_cluster()


conf = SparkConf().setAppName('playground').setMaster(superman.url_master)
conf = conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
# conf = conf.set('spark.driver.memory', '2g') ## Too late to set this
conf = conf.set('spark.executor.memory', '4g')
conf = conf.set('spark.python.worker.memory', '4g')
# conf = conf.set('spark.python.worker.reuse', 'false')
# conf = conf.set('spark.eventlog.enabled', 'true')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')


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

csv_headers = import_meta.import_csv_headers(PATH_RAW)
meta_ordered = import_meta.order_meta(meta_unordered, csv_headers)


for filename, filemeta in meta_ordered.items():
    print('\n**** Processing {} ****\n'.format(filename))
    if filename in {
            'land',
        }:
        print('\n**** Skipping  ****\n'.format(filename))
        continue
    indyspark.import_utils.import_csv(
        sqlContext,
        PATH_RAW / '{}.csv'.format(filename),
        filemeta,
        ).write.parquet(
            (PATH_DATA / '010_Imported_Data' / '{}.parquet'.format(filename)).as_uri(),
            mode='overwrite',
        )

superman.stop_cluster()
