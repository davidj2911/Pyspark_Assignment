from pyspark.sql import SparkSession
from Pyspark_Assignment.src.Assignment_4.util import *

def main():

    spark = SparkSession.builder.appName("Assignment_4").getOrCreate()

    print("Reading from json file:")

    json_df = read_json_file(spark,r'C:\Users\DavidJacob\PycharmProjects\Pyspark_Assignment\Nested_json_file.json')
    json_df.show()

#Flatten
    df = flatten_df(json_df)
    df.show()

#Difference in count before and after Flattening

    count_diff(json_df,df)

#Different types of explode

    diff_explode_types(spark)

#new_column with current date

    df1 = add_new_column(df)
    df1.show()

#from the new column with current date, create 3 columns - year,month and day
    df2 = create_new_columns(df1)
    df2.show()



    spark.stop()


if __name__ == "__main__":
    main()