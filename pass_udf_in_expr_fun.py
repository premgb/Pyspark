#######################################################################################
# pass udf in expr function                                                           #
#######################################################################################
#
from pyspark.sql.functions import udf, expr, concat, col
from pyspark.sql.types import StringType

ac = sc.accumulator(0)

def incrementAC():
  ac.add(1)
  return str(ac)

#sample data
df = sc.parallelize([('Prem',10),('John',20),('Jack',30)]).toDF(["name","id"])

sqlContext.udf.register("myudf", incrementAC, StringType())
df = df.withColumn("name_with_newid", expr("concat(name, myudf())"))
df.show()

####
#alternate solution (to use udf without 'expr()')
####
myudf = udf(incrementAC, StringType())
df.withColumn("name_with_newid", concat(col('name'), myudf())).show()
