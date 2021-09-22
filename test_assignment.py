import unittest
from pyspark.sql import SparkSession
import assignment
from pandas._testing import assert_frame_equal


class SimpleTest(unittest.TestCase):
     spark=SparkSession.builder.master("local[*]").getOrCreate()
	 def test_use_case1(self):
	     input_dataframe=self.spark.createDataFrame([["abc.pdf"],["abc.doc"],["abc.txt"]],["nm"])
		 expected_dataframe=self.spark.createDataFrame([["abc.pdf","pdf"], ["abc.doc","doc"],["abc.txt","txt"]],["nm","extention"]).toPandas()
		 actual_output=assignment.use_case_1(input_dataframe).toPandas()
		 assert_frame_equal(expected_dataframe,actual_output)
	 def test_use_case2(self):
	    input_dataframe =self.spark.createDataFrame([["abc.pdf","pdf"],["abc.pdf","pdf"],["cab.pdf", "pdf"],["abc.doc","doc"]],["nm", "extention"]).toPandas()
		expected_dataframe=self.spark.createdataFrame([["pdf",2],["doc", 1]],["nm","file_count"]).toPandas()
		actual_ouput=assignment.use_case_2(input_dataframe).toPandas()
		assert_frame_equal(expected_dataframe,actual_output)