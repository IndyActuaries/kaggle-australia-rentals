"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Learn what can be done with pyspark.

### DEVELOPER NOTES:
  Likely just a throw-away playground.
"""

import os
import sys
from pathlib import Path

# Temporary hack for dealing with SublimeREPL
os.chdir(os.environ['UserProfile'])


# Munge System PATH and Python PATH for Spark
PATH_SPARK = Path(r'S:\ZQL\Software\Hotware\spark-1.4.1-bin-hadoop2.6')
os.environ['SPARK_HOME'] = str(PATH_SPARK)
sys.path.append(str(PATH_SPARK / 'python'))
for path_py4j in (PATH_SPARK / 'python' / 'lib').glob('py4j*.zip'):
    sys.path.append(str(path_py4j))

# Inform Spark of current Python environment
PATH_CURRENT_ENV = Path(sys.executable).parent
os.environ['PYSPARK_PYTHON'] = str(PATH_CURRENT_ENV)

# Point Spark to a mostly fake Hadoop install to supress a couple warnings
os.environ['HADOOP_HOME'] = r'S:\ZQL\Software\Hotware\fake-hadoop-for-spark'

# Now we're ready to do the real import
import pyspark
from pyspark import SparkContext, SparkConf


#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


conf = SparkConf().setAppName('playground').setMaster('local[3]')
sc = SparkContext(conf=conf)
