from pyspark.sql import SparkSession
from Pyspark_Assignment.src.Assignment_3.util import *

def main():

    spark = SparkSession.builder.appName("Assignment_3").getOrCreate()

    df = assign_3(spark)
    df.show()

    df1 = num_of_actions(df)
    df1.show()

    df2 = change_datatype(df)
    df2.show()








if __name__ == "__main__":
    main()