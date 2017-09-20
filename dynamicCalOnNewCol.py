#######################################################################################################################################
# Dynamic calcuation on the new column in a Spark dataframe- 
#     Add new column to a Spark dataframe and at the same time apply calculation on it based on the value in previous row (i.e. lag)
#######################################################################################################################################
#
import pyspark.sql.functions as f
from pyspark.sql.window import Window

df = sc.parallelize([
    [1,2],
    [3,4],
    [5,6],
    [7,8]
]).toDF(('a', 'b'))

df1 = df.withColumn("row_id", f.monotonically_increasing_id())
w = Window.partitionBy().orderBy(f.col("row_id"))
df1 = df1.withColumn("c_temp", f.when(f.col("row_id")==0, f.lit(3)).otherwise(f.col("a") - f.col("b")))
df1 = df1.withColumn("c", f.sum(f.col("c_temp")).over(w)).drop("c_temp","row_id")
df1.show()
