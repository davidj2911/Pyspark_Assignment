from pyspark.sql.functions import *
from pyspark.sql.types import *

def data_frame_create(spark,data,schema):
    return spark.createDataFrame(data,schema=schema)

employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]

employee_schema = StructType([
                              StructField("employee_id", IntegerType()),
                              StructField("employee_name", StringType()),
                              StructField("department", StringType()),
                              StructField("state", StringType()),
                              StructField("salary", IntegerType()),
                              StructField("age", IntegerType()),
                              ])

department_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")

]

department_schema = StructType([
                                StructField("dept_id", StringType()),
                                StructField("dept_name", StringType())
                               ])

country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]

country_schema = StructType([
                                 StructField("country_code", StringType()),
                                 StructField("country_name", StringType())
                             ])

def find_avg_salary(df):
    return df.groupBy("department").avg("salary")

def name_starts_with(df, df1):
    name_filter = df.filter(col("employee_name").startswith('m'))
    result = name_filter.join(df1, name_filter["department"] == df1["dept_id"],
                              "inner").select(name_filter["employee_name"], df1["dept_name"])
    return result

def bonus_column(df):
    return df.withColumn("bonus", df["salary"] * 2)

def column_order(df):
    return df.select("employee_id", "employee_name", "salary", "state", "age", "department")

def join_types(df,df1,type_name):
    result = df.join(df1, df["department"] == df1["dept_id"],type_name)
    return result

def derive_new_df(df,df1):
    result = df.join(df1, df["state"] == df1["country_code"],"left").select(df["employee_id"],
                        df["employee_name"],df["department"], df1["country_name"],
                        df["salary"],df["age"])
    return result

def col_to_lowercase(df):
    for column in df.columns:
        df = df.withColumnRenamed(column,column.lower())

    df_new = df.withColumn("load_date",current_date())
    return df_new
