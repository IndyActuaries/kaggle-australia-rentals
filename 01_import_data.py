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
import indyaus.import_meta

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.types as types

PATH_RAW = Path(r'W:\NWS\Australia_Rentals\005_Raw_Data')

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================

superman = indyspark.SparkCluster(n_workers=3)
superman.start_cluster()


conf = SparkConf().setAppName('playground').setMaster(superman.url_master)
# conf = conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
# conf = conf.set('spark.driver.memory', '2g')
# conf = conf.set('spark.executor.memory', '2g')
# conf = conf.set('spark.python.worker.memory', '2g')
# conf = conf.set('spark.python.worker.reuse', 'false')
# conf = conf.set('spark.eventlog.enabled', 'true')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


all_schemas = indyaus.import_meta.import_meta(PATH_RAW / 'data_dictionary.xlsx')
print(all_schemas['land_pins'])

sample_dataframe = indyspark.import_utils.import_csv(
    sqlContext,
    PATH_RAW / 'land_valuation_key.csv',
    all_schemas['land_valuation_key'],
    )

sample_dataframe.show()



sample_schema = types.StructType([
        types.StructField(
            "REN_ID",
            types.StringType(),
            nullable=False,
            ),
        types.StructField(
            "REN_BASE_RENT",
            types.FloatType(),
            nullable=False,
            ),
        ])

sample_dataframe = indyspark.import_utils.import_csv(
    sqlContext,
    r'E:\kaggle_play\australia-rentals\005_Raw_Data\sample_submission.csv',
    sample_schema,
    )

sample_dataframe.show()
sample_dataframe.describe().show()

superman.stop_cluster()
