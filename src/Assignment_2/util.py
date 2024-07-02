from pyspark.sql.functions import *
from pyspark.sql.types import *

def credit_card_df(spark):

    data = [("1234567891234567",),("5678912345671234",),("9123456712345678",),
             ("1234567812341122",), ("1234567812341342",)
           ]

    credit_card_schema = StructType([StructField("card_num",StringType(),True)])

    return spark.createDataFrame(data,schema=credit_card_schema)

#print number of partitions

def num_of_partitions(df):
    return df.rdd.getNumPartitions()


#Increase the partition size to 5

def increase_partitions(df,num_of_parts):
    return df.repartition(df.rdd.getNumPartitions()+num_of_parts)

#Decrease the partition size back to its original partition size

def original_partitions(df,num_partitions):
    return df.coalesce(num_partitions)

''' 1.Create a Udf to print only last 4 digits marking remaining digits as *

     2. output should have 2 columns as card_number, masked_card_number'''

def mask_credit_card(df):
    def last_4_digits(card_num):
        return "*" * (len(card_num) - 4) + card_num[-4:]

    cc_udf = udf(last_4_digits, StringType())
    return df.withColumn("masked_card_number", cc_udf(df["card_num"]))

