"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Bury all the environment hacking.

### DEVELOPER NOTES:
  Currently requires backporting bugfix in `pyspark/worker.py`:
    Line 149 - Change "a+" to "rwb"
"""

import os
import sys
from pathlib import Path


PATH_SPARK = Path(r'S:\ZQL\Software\Hotware\spark-1.4.1-bin-hadoop2.6')
PATH_HADOOP_FAKE = Path(r'S:\ZQL\Software\Hotware\fake-hadoop-for-spark')

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


def setup_spark_env(path_spark=PATH_SPARK, path_hadoop=PATH_HADOOP_FAKE):
    """Munge all necessary environment bits to get pyspark working"""

    # Munge System PATH and Python PATH for Spark
    os.environ['SPARK_HOME'] = str(path_spark)
    sys.path.append(str(path_spark / 'python'))
    for path_py4j in (path_spark / 'python' / 'lib').glob('py4j*.zip'):
        sys.path.append(str(path_py4j))

    # Inform Spark of current Python environment
    os.environ['PYSPARK_PYTHON'] = sys.executable

    # Point Spark to a mostly fake Hadoop install to supress a couple warnings
    os.environ['HADOOP_HOME'] = str(path_hadoop)

if __name__ == '__main__':

    setup_spark_env()

    import pyspark
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext
    import pyspark.sql.types as types

    conf = SparkConf().setAppName('playground').setMaster('local[3]')
    conf = conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    print('Spark Context (and SQL Context) successfuly created.')
    sys.exit(0)
