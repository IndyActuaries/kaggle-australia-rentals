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
PATH_SPARK = Path(r'S:\ZQL\Software\Hotware\spark-1.4.1-bin-without-hadoop')
os.environ['SPARK_HOME'] = str(PATH_SPARK)
sys.path.append(str(PATH_SPARK / 'python'))
for path_py4j in (PATH_SPARK / 'python' / 'lib').glob('py4j*.zip'):
    sys.path.append(str(path_py4j))


import pyspark
from pyspark import SparkContext, SparkConf


#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


conf = SparkConf().setAppName('playground').setMaster('local[3]')
sc = SparkContext(conf=conf)
