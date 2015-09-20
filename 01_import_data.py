"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Drive the import process.

### DEVELOPER NOTES:
  Likely just want to serialize the raw data into a clean, rich typed format.
"""

import indyspark

indyspark.setup_spark_env()

import indyspark.import_utils

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

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

print(indyspark.import_utils.import_csv(sqlContext, r'W:\NWS\Australia_Rentals\005_Raw_Data\sample_submission.csv', {'bob': 2}).take(42))

superman.stop_cluster()
