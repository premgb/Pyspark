# Pyspark
<b><u>Pyspark related projects:</b></u>

<h3>1. maxValColumnNamePerRow.py</h3>
Apply a function to every row in a Spark DataFrame to find the maximum value, and the column name having this maximum value.

&nbsp;&nbsp;&nbsp;&nbsp;<b>Input:</b>

```

+----+----+----+
|col1|col2|col3|
+----+----+----+
|  10|  20|  30|
|  40|  60|  50|
|  11|   5|   6|
+----+----+----+

```
&nbsp;&nbsp;&nbsp;&nbsp;<b>Output:</b>

```

+----+----+----+--------+---------+
|col1|col2|col3|maxValue|maxColumn|
+----+----+----+--------+---------+
|  10|  20|  30|      30|     col3|
|  40|  60|  50|      60|     col2|
|  11|   5|   6|      11|     col1|
+----+----+----+--------+---------+        

```
<br>
<h3>2. top3ValColumnNamePerRow.py</h3>
Apply a function to every row in a Spark DataFrame to find top3 values, and the column name having these values.

&nbsp;&nbsp;&nbsp;&nbsp;<b>Input:</b>

```

+---+----+----+----+----+----+----+
| id|col1|col2|col3|col4|col5|col6|
+---+----+----+----+----+----+----+
|100|   2|   3|   4|   5|   4|   1|
|200|  60|  40|  20|  20|  98|   2|
|300|  30|  16|  31|  12|  65|  30|
|400|  15|  16|  17|  12|  17|  15|
+---+----+----+----+----+----+----+

```
&nbsp;&nbsp;&nbsp;&nbsp;<b>Output:</b>

```

+---+----+----+----+----+----+----+--------+--------+--------+------------+------------+------------+
| id|col1|col2|col3|col4|col5|col6|top1_val|top2_val|top3_val|        top1|        top2|        top3|
+---+----+----+----+----+----+----+--------+--------+--------+------------+------------+------------+
|100|   2|   3|   4|   5|   4|   1|       5|       4|       4|      [col4]|[col3, col5]|[col3, col5]|
|200|  60|  40|  20|  20|  98|   2|      98|      60|      40|      [col5]|      [col1]|      [col2]|
|300|  30|  16|  31|  12|  65|  30|      65|      31|      30|      [col5]|      [col3]|[col1, col6]|
|400|  15|  16|  17|  12|  17|  15|      17|      17|      16|[col3, col5]|[col3, col5]|      [col2]|
+---+----+----+----+----+----+----+--------+--------+--------+------------+------------+------------+

```
