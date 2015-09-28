"""
## CODE OWNERS: Shea Parkes

### OBJECTIVE:
  Explore this data.

### DEVELOPER NOTES:
  Is written as a procedure, not a library.
"""

import indyspark

indyspark.setup_spark_env()

from pyspark import StorageLevel
import pyspark.sql.functions as funcs

from indyaus import get_spark_pieces, PATH_DATA

PATH_IMPORTED = PATH_DATA / '010_Imported_Data'
PATH_EDA = PATH_DATA / '020_EDA_Play'

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


superman, sc, sql_con = get_spark_pieces('aus_eda_play')


train = sql_con.read.parquet(str(PATH_IMPORTED / 'train.parquet'))
train.persist(StorageLevel.MEMORY_AND_DISK_SER)
train.show()
print(train.count())

train.groupby(
        'ren_lease_length'
    ).agg(
        funcs.count(train[0]).alias('rowcnt'),
        funcs.avg(train.ren_base_rent).alias('rent_avg'),
    ).sort(
        'rowcnt',
        ascending=False,
    ).show(42)

train.filter(
        've_number = 219069'
    ).show()

sql_con.read.parquet(str(PATH_IMPORTED / 'test.parquet')).show()

land_valuation_key = sql_con.read.parquet(str(PATH_IMPORTED / 'land_valuation_key.parquet'))
land_valuation_key.persist(StorageLevel.MEMORY_AND_DISK_SER)
print(land_valuation_key.count())
print(land_valuation_key.select(funcs.countDistinct('ve_number').alias('cnt')).show())
print(land_valuation_key.select(funcs.countDistinct('lan_id','ve_number').alias('cnt')).show())

print(land_valuation_key.freqItems(['lan_id']))
print(land_valuation_key.schema.fields[1].metadata)

superman.stop_cluster()
