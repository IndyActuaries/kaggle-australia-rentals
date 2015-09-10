"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Drive the import process.

### DEVELOPER NOTES:
  Likely just want to serialize the raw data into a clean, rich typed format.
"""

import indyspark

indyspark.setup_spark_env()

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.types as types

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


conf = SparkConf().setAppName('playground').setMaster('local[3]')
conf = conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
