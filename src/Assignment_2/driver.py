from pyspark.sql import SparkSession
from src.Assignment_2.util import *

def main():

    spark = SparkSession.builder.appName("Assignment 2 - credit_card").getOrCreate()

    cc_df = credit_card_df(spark)
    cc_df.show()

    result = num_of_partitions(cc_df)
    print("\n The original number of partitions are :", result)

    result = increase_partitions(cc_df,5)
    print("\nNew number of partitions are : ",result.rdd.getNumPartitions())

    result_df = original_partitions(cc_df,8)
    print("\nBack to number of original partitions = ", result_df.rdd.getNumPartitions())

    print("\nThe unmasked and masked credit card numbers are :")
    final_result = mask_credit_card(cc_df)
    final_result.show()



if __name__ == "__main__":
    main()