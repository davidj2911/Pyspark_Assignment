import unittest
from pyspark.sql import SparkSession
from Pyspark_Assignment.src.Assignment_1.util import *
import pytest



class Assignment_1_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Assignment_1_Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()



    def test_bought_A_only(self):
        purchase_df = purchase_data_df(self.spark)
        product_df = product_data_df(self.spark)
        result = bought_A_only(purchase_df,product_df)
        result_value = [row['customer'] for row in result.collect()]
        expected_result = [4]

        self.assertEqual(result_value,expected_result)  # add assertion here

    def test_upgrade_from_B_to_E(self):
        purchase_df = purchase_data_df(self.spark)
        result = upgrade_from_B_to_E(purchase_df)
        result_value = [row['customer'] for row in result.collect()]
        expected_result = [1,3]
        self.assertEqual(result_value, expected_result)  # add assertion here


    def test_bought_all(self):
        purchase_df = purchase_data_df(self.spark)
        product_df = product_data_df(self.spark)
        result = bought_all(purchase_df,product_df)
        result_value = [row['customer'] for row in result.collect()]
        expected_result = [1]
        self.assertEqual(result_value, expected_result)



if __name__ == '__main__':
    unittest.main()
