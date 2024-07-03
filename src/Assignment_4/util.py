from pyspark.sql.functions import *
from pyspark.sql.types import *

#Read the json file
def read_json_file(spark,path):
    json_df = spark.read.json(path, multiLine = True)
    return json_df


#Flatten the data frame
def flatten_df(df):
    flatten = df.select("*",explode("employees").alias("emp"),"properties.*").drop("employees","properties")
    return flatten

#Difference in count before and after Flattening
def count_diff(df,flatten):
    print("count before flattening: ",df.count())
    print("count after flattening: ",flatten.count())


#Difference between explode, explode_outer and posexplode

def diff_explode_types(spark):
    data = [
    (1, ["apple", "banana", "cherry"]),
    (2, ["grape", "orange"]),
    (3, None),
    (4, [])
]
    columns = ["id","fruits"]

    df = spark.createDataFrame(data,schema=columns)
    df_exp = df.select("*",explode("fruits"))
    print("DF with explode :")
    df_exp.show()
    df_exp_outer = df.select("*",explode_outer("fruits"))
    print("DF with explode_outer :")
    df_exp_outer.show()
    df_posexplode = df.select("*",posexplode("fruits"))
    print("DF with pos explode :")
    df_posexplode.show()

#adding new column

def add_new_column(df):
    new_df = df.withColumn("load_date",current_date())
    return new_df

#from the new column with current date, create 3 columns - year,month and day

def create_new_columns(df):
    result = df.withColumn("year",year(df.load_date))\
                .withColumn("month",month(df.load_date))\
                 .withColumn("day",day(df.load_date))
    return result