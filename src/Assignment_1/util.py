from pyspark.sql.functions import *
from pyspark.sql.types import *



def purchase_data_df(spark):
    purchase_schema = StructType([StructField("customer", IntegerType()),
                                  StructField("product_model", StringType())
                                 ])

    purchase_data = [(1, "A"),(1, "B"),(2, "A"),(2, "B"),(3, "A"),
                    (3, "B"),(1, "C"),(1, "D"),(1, "E"),(3, "E"),(4, "A")]

    return spark.createDataFrame(purchase_data,schema=purchase_schema)


def product_data_df(spark):
    product_schema = StructType([StructField("product_model", StringType())])

    product_data = [("A",),("B",),("C",),("D",),("E",)]

    return spark.createDataFrame(product_data, schema=product_schema)

#Find the customers who have bought only product A

def bought_A_only(purchase_data_df,product_data_df):
    prod_count = purchase_data_df.groupBy("customer").count()
    filter_count= prod_count.filter("count = 1")
    prod_A_buyer = (filter_count.join(purchase_data_df,"customer").join(product_data_df,"product_model")
                    .filter("product_model = 'A'")).select("customer")
    return prod_A_buyer

#Find customers who upgraded from product B to product E

def upgrade_from_B_to_E(purchase_data_df):
    upgraded_customers = purchase_data_df.alias("p1").join(
        purchase_data_df.alias("p2"),
        (col("p1.customer") == col("p2.customer")) &
        (col("p1.product_model") == "B") & (col("p2.product_model") == "E")
    ).select(col("p1.customer"))
    return upgraded_customers

#Find customers who have bought all models in the new Product Data

def bought_all(purchase_data_df,product_data_df):

    customer_prod_count = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("total_prod_bought"))
    total_unique = product_data_df.select("product_model").distinct()
    bought_all_prod = customer_prod_count.filter(customer_prod_count["total_prod_bought"] ==
                                                 total_unique.count()).select("customer")
    return bought_all_prod