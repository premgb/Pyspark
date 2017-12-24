#######################################################################################
# Pass list as an argument to UDF                                                     #
#######################################################################################
#
from pyspark.sql.functions import udf, col

#sample data
df= sqlContext.createDataFrame([("A", 10), ("B", 20), ("C", 30)],["student", "marks"])
label_list = ["Not Ok!", "OK", "Good"]

#dummy function
def marks_fun(col_val, label):
    if col_val == 20:
        return label[1]
    else:
        return "Grading will soon appear!"

def marks_udf(label_list):
    return udf(lambda l: marks_fun(l, label_list))
df = df.withColumn("grade", marks_udf(label_list)(col("marks")))
df.show()
