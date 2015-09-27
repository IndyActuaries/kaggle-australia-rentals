"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Tooling to initialize an actual Spark interface.

### DEVELOPER NOTES:
  This makes use of the `indyspark` tooling,
    but goes further in returning a running cluster and application.
  Depends on pyspark and indyspark being importable.
  Decided not to make this an object itself since it's already returning three
    powerful objects.
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import indyspark

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================

def get_spark_pieces(name_app, *, spark_cluster=None):
    """Return a tuple of (SparkCluster, SparkContext, SQLContext)"""

    # Start up a cluster if one has not been provided
    if not spark_cluster:
        spark_cluster = indyspark.SparkCluster(
            n_workers=6,
            spark_worker_memory='4G',
            )
        spark_cluster.start_cluster()

    spark_conf = SparkConf().setAppName(name_app).setMaster(spark_cluster.url_master)
    spark_conf = spark_conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    # spark_conf = spark_conf.set('spark.driver.memory', '2g') ## Too late to set this
    spark_conf = spark_conf.set('spark.executor.memory', '4g')
    spark_conf = spark_conf.set('spark.python.worker.memory', '4g')

    sc = SparkContext(conf=spark_conf)

    sqlContext = SQLContext(sc)
    sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')

    return spark_cluster, sc, sqlContext


if __name__ == '__main__':
    pass
