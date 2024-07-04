from pyspark.sql import SparkSession
from Pyspark_Assignment.src.Assignment_5.util import *

def main():

    spark = SparkSession.builder.appName("Assignment_5").getOrCreate()

    print("Employee DataFrame:")

    employee_df = data_frame_create(spark,employee_data,employee_schema)
    employee_df.show()

    print("Department DataFrame:")

    department_df = data_frame_create(spark, department_data,department_schema)
    department_df.show()

    print("Country DataFrame:")

    country_df = data_frame_create(spark,country_data,country_schema)
    country_df.show()

    print("Average Salary of each department :")
    avg_salary = find_avg_salary(employee_df)
    avg_salary.show()

    print("Name and dept name of employee whose name starts with m :")
    df1 = name_starts_with(employee_df,department_df)
    df1.show()

    print("Creating a new column named Bonus :")
    df2 = bonus_column(employee_df)
    df2.show()

    print("Reordering Columns :")
    df3 = column_order(employee_df)
    df3.show()

    print("Results for different types of join :")

    inner_join = join_types(employee_df,department_df,"inner")
    print("Inner Join :")
    inner_join.show()
    outer_join = join_types(employee_df,department_df,"outer")
    print("Outer Join :")
    outer_join.show()
    left_join = join_types(employee_df,department_df,"left")
    print("Left Join :")
    left_join.show()
    right_join = join_types(employee_df,department_df,"right")
    print("Right Join :")
    right_join.show()

    print("Deriving a new DataFrame with country name :")
    df4 = derive_new_df(employee_df,country_df)
    df4.show()

    print("Column names in lower case and adding new column load_date:")
    df5 = col_to_lowercase(df4)
    df5.show()

if __name__ == "__main__":
    main()