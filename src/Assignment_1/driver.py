from pyspark.sql import SparkSession
from src.Assignment_1.util import *


def main():
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Purchase and Product DataFrame").getOrCreate()

    # Create DataFrame for purchase data and product data
    purchase_df = purchase_data_df(spark)

    product_df = product_data_df(spark)

    # Find customers who have bought only product A
    print("\nCustomers who have bought only product A are :")
    result_df = bought_A_only(purchase_df,product_df)
    result_df.show()

    # Find customers who upgraded from product B to product E

    print("\nCustomers who upgraded from B to E are :")
    result_df = upgrade_from_B_to_E(purchase_df)
    result_df.show()

    # Find customers who have bought all models in the new Product Data

    print("\nCustomers who have bought all models are :")
    result_df = bought_all(purchase_df,product_df)
    result_df.show()


    spark.stop()

if __name__ == "__main__":
    main()