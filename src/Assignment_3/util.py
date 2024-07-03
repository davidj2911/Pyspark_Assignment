from pyspark.sql.functions import *
from pyspark.sql.types import *

def assign_3(spark):

    data = [(1, 101, 'login', '2023-09-05 08:30:00'),
 (2, 102, 'click', '2023-09-06 12:45:00'),
 (3, 101, 'click', '2023-09-07 14:15:00'),
 (4, 103, 'login', '2023-09-08 09:00:00'),
 (5, 102, 'logout', '2023-09-09 17:30:00'),
 (6, 101, 'click', '2023-09-10 11:20:00'),
 (7, 103, 'click', '2023-09-11 10:15:00'),
 (8, 102, 'click', '2023-09-12 13:10:00')
]

    columns = ['log_id', 'user_id', 'user_activity', 'time_stamp' ]

    return spark.createDataFrame(data, schema=columns)


def num_of_actions(df):
    time_line = df.filter(datediff(to_date(expr("date('2023-09-12')")), to_date(col('time_stamp'))) <= 7)
    result = time_line.groupBy("user_id").agg(count("user_activity"))
    return result

def change_datatype(df):
    name_change = df.withColumnRenamed('time_stamp','login_date')
    data_type_change = name_change.withColumn(colName='login_date',col = col('login_date').cast(DateType()))
    return data_type_change