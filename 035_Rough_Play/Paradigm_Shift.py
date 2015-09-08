"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Learn what can be done with pyspark.

### DEVELOPER NOTES:
  Likely just a throw-away playground.
  Currently requires backporting bugfix in `pyspark/worker.py`:
    Line 149 - Change "a+" to "rwb"
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
os.environ['PYSPARK_PYTHON'] = sys.executable

# Point Spark to a mostly fake Hadoop install to supress a couple warnings
os.environ['HADOOP_HOME'] = r'S:\ZQL\Software\Hotware\fake-hadoop-for-spark'

# Now we're ready to do the real import
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

# Show converting a simple text file into an RDD
lines = sc.textFile(r'W:\NWS\Australia_Rentals\005_Raw_Data\sample_submission.csv')
header = lines.first()
lines = lines.filter(lambda l: l != header)
print(lines.take(42))
parts = lines.map(lambda l: l.split(","))
parts = parts.map(lambda p: [p[0], float(p[1])])
print(parts.take(42))

# Now confert the RDD into a richer DataFrame
df = sqlContext.createDataFrame(
    parts,
    schema=types.StructType([
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
        ]),
    )
print(df.schema)
df.show()
df.describe().show()

# Serialize somewhere silly as a parquet file
df.write.save(r'C:\repos\trash\mysave.parquet')
