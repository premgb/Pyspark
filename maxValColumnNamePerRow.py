###############################################################################################################################
# Apply a function to every row in a Spark DataFrame to find 
#     1. the maximum value, and
#     2. the column name having this maximum value
###############################################################################################################################
#
from pyspark.sql.functions import col, greatest, udf, array
from pyspark.sql.types import StringType

df = sc.parallelize([(10, 20, 30),
                     (40, 60, 50),
                     (11, 5,  6)]).\
    toDF(["col1", "col2","col3"])

df1 = df.withColumn("maxValue", greatest(*[col(x) for x in df.columns]))
col_arr = df1.columns

def modify_values(r):
    for i in range(len(r[:-1])):
        if r[i]==r[-1]:
            return col_arr[i]
modify_values_udf = udf(modify_values, StringType())
df1 = df1.withColumn("maxColumn", modify_values_udf(array(df1.columns)))
df1.show()
