###############################################################################################################################
# Apply a function to every row in a Spark DataFrame to find 
#     1. top3 values, and
#     2. the column name having these values
###############################################################################################################################
#
from pyspark.sql.functions import col, udf, array, sort_array
from pyspark.sql.types import StringType

df = sc.parallelize([(100, 2, 3, 4, 5, 4, 1),
                     (200, 60, 40, 20, 20, 98, 2),
                     (300, 30, 16, 31, 12, 65, 30),
                     (400, 15, 16, 17, 12, 17, 15)]).\
    toDF(["id","col1","col2","col3","col4","col5","col6"])
df_col = df.columns

df = df.\
    withColumn("top1_val", sort_array(array([col(x) for x in df_col[1:]]), asc=False)[0]).\
    withColumn("top2_val", sort_array(array([col(x) for x in df_col[1:]]), asc=False)[1]).\
    withColumn("top3_val", sort_array(array([col(x) for x in df_col[1:]]), asc=False)[2])

def modify_values(r, max_col):
    l = []
    for i in range(len(df_col[1:])):
        if r[i]== max_col:
            l.append(df_col[i+1])
    return l
modify_values_udf = udf(modify_values, StringType())

df1 = df.\
    withColumn("top1", modify_values_udf(array(df.columns[1:-3]), "top1_val")).\
    withColumn("top2", modify_values_udf(array(df.columns[1:-3]), "top2_val")).\
    withColumn("top3", modify_values_udf(array(df.columns[1:-3]), "top3_val"))
df1.show()
